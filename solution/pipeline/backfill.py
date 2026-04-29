"""Backfill job for analytics pipeline.

Orchestrates full historical data load from source database to analytics schema.

Flow:
1. Read all dimension data from source
2. Upsert dimensions to analytics schema
3. Read all fact data (order_items with order joins) from source
4. Transform each row (compute is_open, quantity_pending)
5. Batch upsert to fact table using ON CONFLICT semantics

Idempotent: Safe to re-run multiple times without creating duplicates.
"""
import logging
import psycopg2
from datetime import datetime

from pipeline.config import Config
from pipeline.db import (
    create_source_pool,
    create_analytics_pool,
    upsert_fact_table,
    upsert_dim_order,
    upsert_dim_product,
    upsert_dim_customer,
)
from pipeline.transforms import compute_is_open, compute_quantity_pending

logger = logging.getLogger(__name__)


class BackfillJob:
    """Orchestrates idempotent backfill of analytics schema from source."""

    def __init__(self, source_url, analytics_url, batch_size=1000):
        """Initialize backfill job with database connections.

        Args:
            source_url: PostgreSQL connection string for source database
            analytics_url: PostgreSQL connection string for analytics database
            batch_size: Number of rows to batch per upsert (default 1000)
        """
        self.source_url = source_url
        self.analytics_url = analytics_url
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)

        # Create connection pools
        self.source_pool = create_source_pool(
            source_url,
            min_connections=Config.DB_POOL_MIN_CONNECTIONS,
            max_connections=Config.DB_POOL_MAX_CONNECTIONS,
        )
        self.analytics_pool = create_analytics_pool(
            analytics_url,
            min_connections=Config.DB_POOL_MIN_CONNECTIONS,
            max_connections=Config.DB_POOL_MAX_CONNECTIONS,
        )

    def run(self):
        """Execute full backfill: dimensions first, then fact table.

        Logs progress at key stages.

        Raises:
            Exception: Any database error (not caught; caller responsible for handling)
        """
        try:
            self.logger.info("Backfill started")

            # Backfill dimensions first (foreign key dependencies)
            self.backfill_dimensions()
            self.logger.info("Dimensions complete")

            # Then backfill fact table
            self.backfill_fact_table()
            self.logger.info("Fact table backfill complete")

        except Exception as e:
            self.logger.error(f"Backfill failed: {e}", exc_info=True)
            raise
        finally:
            # Always close pools
            self.source_pool.closeall()
            self.analytics_pool.closeall()
            self.logger.info("Connection pools closed")

    def backfill_dimensions(self):
        """Read and upsert dimension tables (customers, products, orders).

        Order matters: customers and products first (no dependencies);
        orders after (depends on customers).
        """
        source_conn = self.source_pool.getconn()
        analytics_conn = self.analytics_pool.getconn()

        try:
            # Backfill customers
            self.logger.info("Backfilling dim_customer...")
            self._backfill_dim_customer(source_conn, analytics_conn)

            # Backfill products
            self.logger.info("Backfilling dim_product...")
            self._backfill_dim_product(source_conn, analytics_conn)

            # Backfill orders
            self.logger.info("Backfilling dim_order...")
            self._backfill_dim_order(source_conn, analytics_conn)

        finally:
            self.source_pool.putconn(source_conn)
            self.analytics_pool.putconn(analytics_conn)

    def _backfill_dim_customer(self, source_conn, analytics_conn):
        """Backfill dim_customer from source.customers."""
        source_cur = source_conn.cursor()
        batch = []

        try:
            source_cur.execute("""
                SELECT customer_id, customer_name, customer_address, is_active, updated_at
                FROM public.customers
                ORDER BY customer_id
            """)

            while True:
                rows = source_cur.fetchmany(self.batch_size)
                if not rows:
                    break

                batch.extend(rows)
                if len(batch) >= self.batch_size:
                    upsert_dim_customer(analytics_conn, batch)
                    batch = []

            # Final batch
            if batch:
                upsert_dim_customer(analytics_conn, batch)

        finally:
            source_cur.close()

    def _backfill_dim_product(self, source_conn, analytics_conn):
        """Backfill dim_product from source.products."""
        source_cur = source_conn.cursor()
        batch = []

        try:
            source_cur.execute("""
                SELECT product_id, product_name, barcode, unity_price, is_active, updated_at
                FROM public.products
                ORDER BY product_id
            """)

            while True:
                rows = source_cur.fetchmany(self.batch_size)
                if not rows:
                    break

                batch.extend(rows)
                if len(batch) >= self.batch_size:
                    upsert_dim_product(analytics_conn, batch)
                    batch = []

            # Final batch
            if batch:
                upsert_dim_product(analytics_conn, batch)

        finally:
            source_cur.close()

    def _backfill_dim_order(self, source_conn, analytics_conn):
        """Backfill dim_order from source.orders."""
        source_cur = source_conn.cursor()
        batch = []

        try:
            source_cur.execute("""
                SELECT order_id, order_date, status, updated_at
                FROM public.orders
                ORDER BY order_id
            """)

            while True:
                rows = source_cur.fetchmany(self.batch_size)
                if not rows:
                    break

                batch.extend(rows)
                if len(batch) >= self.batch_size:
                    upsert_dim_order(analytics_conn, batch)
                    batch = []

            # Final batch
            if batch:
                upsert_dim_order(analytics_conn, batch)

        finally:
            source_cur.close()

    def backfill_fact_table(self):
        """Read and transform fact table from source order_items + orders."""
        source_conn = self.source_pool.getconn()
        analytics_conn = self.analytics_pool.getconn()

        try:
            source_cur = source_conn.cursor()
            batch = []
            rows_total = 0

            # Query: order_items joined with orders to get status
            # Note: source column is 'quanity' (misspelled); normalize in SELECT
            query = """
                SELECT
                    oi.order_item_id,
                    o.order_id,
                    oi.product_id,
                    o.customer_id,
                    o.delivery_date,
                    oi.quanity,
                    o.status,
                    oi.created_at,
                    oi.updated_at
                FROM public.order_items oi
                JOIN public.orders o ON oi.order_id = o.order_id
                ORDER BY oi.order_item_id
            """

            source_cur.execute(query)

            while True:
                rows = source_cur.fetchmany(self.batch_size)
                if not rows:
                    break

                # Transform rows: compute is_open, quantity_pending
                transformed_rows = []
                for row in rows:
                    (
                        order_item_id,
                        order_id,
                        product_id,
                        customer_id,
                        delivery_date,
                        quantity,
                        order_status,
                        created_at,
                        updated_at,
                    ) = row

                    # Apply transformations
                    is_open = compute_is_open(order_status)
                    quantity_pending = compute_quantity_pending(quantity, order_status)

                    # Build fact row
                    fact_row = (
                        order_item_id,
                        order_id,
                        product_id,
                        customer_id,
                        delivery_date,
                        quantity_pending,
                        is_open,
                        created_at,
                        updated_at,
                    )
                    transformed_rows.append(fact_row)

                batch.extend(transformed_rows)
                rows_total += len(transformed_rows)

                # Batch upsert
                if len(batch) >= self.batch_size:
                    upsert_fact_table(analytics_conn, batch)
                    self.logger.info(f"Processed {rows_total} rows")
                    batch = []

            # Final batch
            if batch:
                upsert_fact_table(analytics_conn, batch)
                rows_total += len(batch)

            self.logger.info(f"Fact table backfill complete: {rows_total} total rows")

        finally:
            source_cur.close()
            self.source_pool.putconn(source_conn)
            self.analytics_pool.putconn(analytics_conn)


def main():
    """Entrypoint for backfill job.

    Usage: python -m pipeline.backfill
    """
    import sys

    # Configure logging
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    # Validate config
    if not Config.validate():
        logger.error("Configuration validation failed")
        sys.exit(1)

    # Create and run backfill job
    job = BackfillJob(
        source_url=Config.POSTGRES_SOURCE_URL,
        analytics_url=Config.POSTGRES_ANALYTICS_URL,
        batch_size=Config.BATCH_SIZE,
    )

    try:
        job.run()
        logger.info("Backfill job completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Backfill job failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
