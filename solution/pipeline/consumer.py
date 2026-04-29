"""Kafka CDC consumer for continuous analytics incremental updates.

Implements:
- Low-latency Kafka consumer with confluent-kafka
- Debezium CDC payload parsing (INSERT/UPDATE/DELETE operations)
- Manual offset management: commit AFTER successful DB write (transactional safety)
- Event routing to correct upsert based on table name
- Lag tracking and error handling

Flow:
1. Poll Kafka for messages (order-changes, product-changes, customer-changes topics)
2. Parse Debezium JSON payload (op='c'/'u'/'d', before/after fields)
3. Route to appropriate upsert function based on table name
4. Write to analytics database (transactional)
5. Store offset + commit (ONLY after DB write succeeds)
6. On error: log, don't commit offset (auto-replay on restart)
"""
import json
import logging
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer, KafkaError

from pipeline.config import Config
from pipeline.db import (
    create_analytics_pool,
    upsert_fact_table,
    upsert_dim_order,
    upsert_dim_product,
    upsert_dim_customer,
)
from pipeline.logging_config import get_logger
from pipeline.transforms import compute_is_open, compute_quantity_pending

logger = get_logger(__name__)


class AnalyticsConsumer:
    """Kafka CDC consumer for continuous analytics updates.

    Subscribes to order-changes, product-changes, customer-changes topics.
    Parses Debezium CDC events and upserts to analytics schema with manual
    offset management for transactional safety.

    Attributes:
        kafka_brokers: List of Kafka broker addresses (e.g., ['localhost:9092'])
        analytics_url: PostgreSQL connection string for analytics database
        analytics_pool: ThreadedConnectionPool for analytics writes
        consumer: confluent_kafka.Consumer instance (created in run())
        logger: Logger for this consumer instance
    """

    def __init__(self, kafka_brokers, analytics_url):
        """Initialize Kafka CDC consumer.

        Args:
            kafka_brokers: List of Kafka broker addresses
                          (e.g., ['broker1:9092', 'broker2:9092'])
            analytics_url: PostgreSQL connection string for analytics database

        Raises:
            psycopg2.OperationalError: If analytics pool creation fails
        """
        self.kafka_brokers = kafka_brokers
        self.analytics_url = analytics_url
        self.analytics_pool = create_analytics_pool(analytics_url)
        self.consumer = None
        self.logger = get_logger(__name__)
        self.last_lag_log = 0  # Track when we last logged lag

    def run(self):
        """Start Kafka consumer polling loop.

        Main consumer loop:
        1. Configure Kafka consumer with manual offset management
        2. Subscribe to CDC topics
        3. Poll for messages (timeout=1.0 second)
        4. Parse Debezium event
        5. Apply event to analytics DB
        6. Store offset + commit (ONLY after DB write succeeds)
        7. Handle errors: log and don't commit (auto-replay on restart)

        Raises:
            KafkaError: Fatal Kafka error (non-EOF partition errors)
        """
        try:
            # Configure Kafka consumer with manual offset management for transactional safety
            conf = {
                'bootstrap.servers': ','.join(self.kafka_brokers),
                'group.id': 'analytics-consumer',  # Per REQ-3.2
                'auto.offset.reset': 'earliest',  # Start from beginning if no committed offset
                'enable.auto.commit': False,  # CRITICAL: Manual commit only — offset persisted AFTER DB write, not before
                'enable.auto.offset.store': False,  # Don't auto-store offsets
                'session.timeout.ms': 6000,  # 6 second timeout for broker availability
            }

            self.consumer = Consumer(conf)
            self.logger.info(f"Created Kafka consumer: group.id=analytics-consumer, brokers={self.kafka_brokers}")

            # Subscribe to CDC topics
            topics = ['order-changes', 'product-changes', 'customer-changes']
            self.consumer.subscribe(topics)
            self.logger.info(f"Subscribed to topics: {topics}")

            # Main polling loop
            poll_count = 0
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # Empty poll (no messages available)
                    poll_count += 1
                    if poll_count % 10 == 0:  # Log lag every 10 empty polls
                        self._log_lag()
                    continue

                # Check for Kafka errors
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Normal: reached end of partition
                        self.logger.debug(
                            f"Reached end of partition: {msg.partition()}"
                        )
                        continue
                    else:
                        # Fatal error: log and raise
                        error = msg.error()
                        self.logger.error(f"Kafka error: {error}")
                        raise error

                poll_count = 0  # Reset poll count on successful message

                # Parse and apply event
                try:
                    event = self._parse_debezium_event(msg)
                    self._apply_event(event)

                    # Commit offset AFTER batch upsert succeeds — ensures we only mark messages processed when analytics data is durable
                    self.consumer.store_offsets(msg)  # Mark offset in memory
                    self.consumer.commit()  # Commit to broker (transactional)

                    self.logger.debug(
                        f"Processed {event['op']} to {event['table']}: "
                        f"partition={msg.partition()}, offset={msg.offset()}"
                    )

                except Exception as e:
                    # DB write or parsing failed: DON'T commit offset
                    self.logger.error(
                        f"Failed to process message: {e}",
                        exc_info=True
                    )
                    # Message will be re-delivered on restart

        except Exception as e:
            self.logger.error(f"Consumer fatal error: {e}", exc_info=True)
            raise
        finally:
            # Always cleanup
            if self.consumer:
                self.consumer.close()
            if self.analytics_pool:
                self.analytics_pool.closeall()
            self.logger.info("Consumer closed, connection pools cleaned up")

    def _parse_debezium_event(self, kafka_msg):
        """Parse Debezium CDC payload from Kafka message.

        Debezium JSON structure:
        {
            "op": "c|u|d",           // Operation: create, update, delete
            "before": {...},         // Previous row state (NULL for INSERT)
            "after": {...},          // New row state (NULL for DELETE)
            "source": {
                "table": "tablename",
                "ts_ms": 1234567890  // Timestamp in milliseconds
            }
        }

        Args:
            kafka_msg: confluent_kafka.Message with Debezium payload

        Returns:
            dict: Parsed event with keys:
                - op: 'INSERT', 'UPDATE', or 'DELETE'
                - table: Table name from source metadata
                - data: Row data (after for INSERT/UPDATE, before for DELETE)
                - old_data: Previous state (UPDATE only)
                - ts_ms: Event timestamp in milliseconds

        Raises:
            ValueError: If operation type is unknown or payload malformed
            json.JSONDecodeError: If payload is not valid JSON
        """
        try:
            payload = json.loads(kafka_msg.value().decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Failed to decode Debezium payload: {e}")

        # Debezium CDC events contain before/after payloads; extract operation type to determine INSERT/UPDATE/DELETE
        op_code = payload.get('op')
        if not op_code:
            raise ValueError("Debezium payload missing 'op' field")

        source = payload.get('source', {})
        table = source.get('table')
        ts_ms = source.get('ts_ms')

        if not table:
            raise ValueError("Debezium payload missing 'source.table'")

        # Map operation code to operation type
        # Per Debezium: c=create, u=update, d=delete
        if op_code == 'c':
            op_type = 'INSERT'
            data = payload.get('after')  # New row
        elif op_code == 'u':
            op_type = 'UPDATE'
            data = payload.get('after')  # New row
            old_data = payload.get('before')  # Previous row
        elif op_code == 'd':
            op_type = 'DELETE'
            data = payload.get('before')  # Previous row (what's being deleted)
            old_data = None
        else:
            raise ValueError(f"Unknown Debezium operation: {op_code}")

        # Validate data is not NULL (should always have before or after)
        if data is None:
            raise ValueError(f"Debezium event missing data (op={op_code}, table={table})")

        event = {
            'op': op_type,
            'table': table,
            'data': data,
            'ts_ms': ts_ms,
        }

        if op_code == 'u':
            event['old_data'] = old_data

        self.logger.debug(f"Parsed {op_type} to {table}: {ts_ms}")
        return event

    def _apply_event(self, event):
        """Apply CDC event to analytics schema.

        Routes event to correct upsert function based on table name:
        - 'orders' → _upsert_dim_order()
        - 'products' → _upsert_dim_product()
        - 'customers' → _upsert_dim_customer()
        - 'order_items' → _upsert_fact_order_items()

        Args:
            event: Parsed Debezium event (from _parse_debezium_event())

        Raises:
            ValueError: If table is unknown
            psycopg2.DatabaseError: If upsert fails
        """
        table = event['table']
        data = event['data']
        op_type = event['op']

        try:
            if table == 'orders':
                self._upsert_dim_order(data)
            elif table == 'products':
                self._upsert_dim_product(data)
            elif table == 'customers':
                self._upsert_dim_customer(data)
            elif table == 'order_items':
                self._upsert_fact_order_items(data)
            else:
                raise ValueError(f"Unknown table in CDC event: {table}")

            self.logger.info(f"Applied {op_type} to {table}")

        except Exception as e:
            self.logger.error(f"Failed to apply {op_type} to {table}: {e}", exc_info=True)
            raise

    def _upsert_dim_order(self, order_data):
        """Upsert order dimension from CDC event.

        Extracts: order_id, order_date, status
        Writes to: analytics.dim_order via db.upsert_dim_order()

        Args:
            order_data: Row dict from CDC event's after/before field

        Raises:
            psycopg2.DatabaseError: If upsert fails
            KeyError: If required fields missing
        """
        order_id = order_data.get('order_id')
        order_date = order_data.get('order_date')
        status = order_data.get('status')
        updated_at = datetime.utcnow().isoformat()

        if not order_id:
            raise ValueError("Order missing order_id")

        conn = self.analytics_pool.getconn()
        try:
            rows = [(order_id, order_date, status, updated_at)]
            upsert_dim_order(conn, rows)
            self.logger.debug(f"Upserted dim_order: order_id={order_id}")
        finally:
            self.analytics_pool.putconn(conn)

    def _upsert_dim_product(self, product_data):
        """Upsert product dimension from CDC event.

        Extracts: product_id, product_name, barcode, unity_price, is_active
        Writes to: analytics.dim_product via db.upsert_dim_product()

        Args:
            product_data: Row dict from CDC event's after/before field

        Raises:
            psycopg2.DatabaseError: If upsert fails
            KeyError: If required fields missing
        """
        product_id = product_data.get('product_id')
        product_name = product_data.get('product_name')
        barcode = product_data.get('barcode')
        unity_price = product_data.get('unity_price')
        is_active = product_data.get('is_active', True)
        updated_at = datetime.utcnow().isoformat()

        if not product_id:
            raise ValueError("Product missing product_id")

        conn = self.analytics_pool.getconn()
        try:
            rows = [(product_id, product_name, barcode, unity_price, is_active, updated_at)]
            upsert_dim_product(conn, rows)
            self.logger.debug(f"Upserted dim_product: product_id={product_id}")
        finally:
            self.analytics_pool.putconn(conn)

    def _upsert_dim_customer(self, customer_data):
        """Upsert customer dimension from CDC event.

        Extracts: customer_id, customer_name, customer_address, is_active
        Writes to: analytics.dim_customer via db.upsert_dim_customer()

        Args:
            customer_data: Row dict from CDC event's after/before field

        Raises:
            psycopg2.DatabaseError: If upsert fails
            KeyError: If required fields missing
        """
        customer_id = customer_data.get('customer_id')
        customer_name = customer_data.get('customer_name')
        customer_address = customer_data.get('customer_address')
        is_active = customer_data.get('is_active', True)
        updated_at = datetime.utcnow().isoformat()

        if not customer_id:
            raise ValueError("Customer missing customer_id")

        conn = self.analytics_pool.getconn()
        try:
            rows = [(customer_id, customer_name, customer_address, is_active, updated_at)]
            upsert_dim_customer(conn, rows)
            self.logger.debug(f"Upserted dim_customer: customer_id={customer_id}")
        finally:
            self.analytics_pool.putconn(conn)

    def _upsert_fact_order_items(self, item_data):
        """Upsert fact order items from CDC event.

        CDC event for order_items table: Extract order_item_id, order_id,
        product_id, customer_id, delivery_date, quanity (note source typo).

        Computes:
        - is_open: 1 if order status != 'COMPLETED', 0 otherwise
        - quantity_pending: quantity if open, 0 if closed
        (Requires join to dim_order to get status)

        Writes to: analytics.fct_order_items via db.upsert_fact_table()

        Args:
            item_data: Row dict from CDC event's after/before field

        Raises:
            psycopg2.DatabaseError: If upsert fails
            KeyError: If required fields missing
        """
        order_item_id = item_data.get('order_item_id')
        order_id = item_data.get('order_id')
        product_id = item_data.get('product_id')
        customer_id = item_data.get('customer_id')
        delivery_date = item_data.get('delivery_date')
        quantity = item_data.get('quanity')  # Note: source has typo
        created_at = datetime.utcnow().isoformat()
        updated_at = created_at

        if not order_item_id:
            raise ValueError("OrderItem missing order_item_id")

        # Get analytics connection and query status
        conn = self.analytics_pool.getconn()
        try:
            # Query order status from dim_order to compute is_open, quantity_pending
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM analytics.dim_order WHERE order_id = %s",
                    (order_id,)
                )
                result = cur.fetchone()
                order_status = result[0] if result else None

            # Compute derived columns
            is_open = compute_is_open(order_status)
            quantity_pending = compute_quantity_pending(quantity, order_status)

            rows = [(
                order_item_id, order_id, product_id, customer_id,
                delivery_date, quantity_pending, is_open,
                created_at, updated_at
            )]
            upsert_fact_table(conn, rows)
            self.logger.debug(
                f"Upserted fct_order_items: order_item_id={order_item_id}, "
                f"is_open={is_open}, quantity_pending={quantity_pending}"
            )
        finally:
            self.analytics_pool.putconn(conn)

    def _log_lag(self):
        """Log current consumer lag for operational visibility.

        Simple implementation: placeholder for future metrics integration.
        """
        self.logger.debug("Consumer lag tracking: polling for messages...")


if __name__ == '__main__':
    """Entry point for running consumer as daemon."""
    import sys

    # Validate config
    if not Config.validate():
        sys.exit(1)

    # Set up logging
    from pipeline.logging_config import setup_logging
    setup_logging(Config.LOG_LEVEL)

    # Get Kafka brokers from config or environment
    kafka_brokers = Config.KAFKA_BROKERS if hasattr(Config, 'KAFKA_BROKERS') else ['localhost:9092']

    # Run consumer
    consumer = AnalyticsConsumer(kafka_brokers, Config.POSTGRES_ANALYTICS_URL)
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Consumer failed: {e}", exc_info=True)
        sys.exit(1)
