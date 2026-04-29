"""Database connection pooling and utilities for analytics pipeline.

Provides:
- Connection pooling for both source and analytics databases
- Idempotent upsert helpers for fact and dimension tables
- Batch insert via execute_values for performance
"""
import logging
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def create_source_pool(source_url, min_connections=1, max_connections=5):
    """Create a ThreadedConnectionPool for source database.

    Args:
        source_url: PostgreSQL connection string for source database
        min_connections: Minimum connections in pool (default 1)
        max_connections: Maximum connections in pool (default 5)

    Returns:
        ThreadedConnectionPool instance for source database

    Raises:
        psycopg2.OperationalError: If connection fails
    """
    try:
        source_pool = pool.ThreadedConnectionPool(
            min_connections, max_connections, source_url
        )
        logger.info(
            f"Created source pool: min={min_connections}, max={max_connections}"
        )
        return source_pool
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to create source pool: {e}")
        raise


def create_analytics_pool(analytics_url, min_connections=1, max_connections=5):
    """Create a ThreadedConnectionPool for analytics database.

    Args:
        analytics_url: PostgreSQL connection string for analytics database
        min_connections: Minimum connections in pool (default 1)
        max_connections: Maximum connections in pool (default 5)

    Returns:
        ThreadedConnectionPool instance for analytics database

    Raises:
        psycopg2.OperationalError: If connection fails
    """
    try:
        analytics_pool = pool.ThreadedConnectionPool(
            min_connections, max_connections, analytics_url
        )
        logger.info(
            f"Created analytics pool: min={min_connections}, max={max_connections}"
        )
        return analytics_pool
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to create analytics pool: {e}")
        raise


def upsert_fact_table(conn, rows):
    """Idempotent upsert of fact table (fct_order_items).

    Uses ON CONFLICT DO UPDATE to safely handle re-runs.
    Safe to call multiple times without creating duplicates.

    Args:
        conn: psycopg2 database connection
        rows: List of tuples:
            (order_item_id, order_id, product_id, customer_id, delivery_date,
             quantity_pending, is_open, created_at, updated_at)

    Raises:
        psycopg2.DatabaseError: If upsert fails
    """
    query = """
    INSERT INTO analytics.fct_order_items
      (order_item_id, order_id, product_id, customer_id, delivery_date,
       quantity_pending, is_open, created_at, updated_at)
    VALUES %s
    ON CONFLICT (order_item_id) DO UPDATE SET
      quantity_pending = EXCLUDED.quantity_pending,
      is_open = EXCLUDED.is_open,
      updated_at = EXCLUDED.updated_at
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
        conn.commit()
        logger.info(f"Upserting {len(rows)} rows into analytics.fct_order_items")
    except psycopg2.DatabaseError as e:
        conn.rollback()
        logger.error(f"Failed to upsert fact table: {e}")
        raise


def upsert_dim_order(conn, rows):
    """Idempotent upsert of order dimension (dim_order).

    Args:
        conn: psycopg2 database connection
        rows: List of tuples:
            (order_id, order_date, status, updated_at)

    Raises:
        psycopg2.DatabaseError: If upsert fails
    """
    query = """
    INSERT INTO analytics.dim_order
      (order_id, order_date, status, updated_at)
    VALUES %s
    ON CONFLICT (order_id) DO UPDATE SET
      order_date = EXCLUDED.order_date,
      status = EXCLUDED.status,
      updated_at = EXCLUDED.updated_at
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
        conn.commit()
        logger.info(f"Upserting {len(rows)} rows into analytics.dim_order")
    except psycopg2.DatabaseError as e:
        conn.rollback()
        logger.error(f"Failed to upsert dim_order: {e}")
        raise


def upsert_dim_product(conn, rows):
    """Idempotent upsert of product dimension (dim_product).

    Args:
        conn: psycopg2 database connection
        rows: List of tuples:
            (product_id, product_name, barcode, unity_price, is_active, updated_at)

    Raises:
        psycopg2.DatabaseError: If upsert fails
    """
    query = """
    INSERT INTO analytics.dim_product
      (product_id, product_name, barcode, unity_price, is_active, updated_at)
    VALUES %s
    ON CONFLICT (product_id) DO UPDATE SET
      product_name = EXCLUDED.product_name,
      barcode = EXCLUDED.barcode,
      unity_price = EXCLUDED.unity_price,
      is_active = EXCLUDED.is_active,
      updated_at = EXCLUDED.updated_at
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
        conn.commit()
        logger.info(f"Upserting {len(rows)} rows into analytics.dim_product")
    except psycopg2.DatabaseError as e:
        conn.rollback()
        logger.error(f"Failed to upsert dim_product: {e}")
        raise


def upsert_dim_customer(conn, rows):
    """Idempotent upsert of customer dimension (dim_customer).

    Args:
        conn: psycopg2 database connection
        rows: List of tuples:
            (customer_id, customer_name, customer_address, is_active, updated_at)

    Raises:
        psycopg2.DatabaseError: If upsert fails
    """
    query = """
    INSERT INTO analytics.dim_customer
      (customer_id, customer_name, customer_address, is_active, updated_at)
    VALUES %s
    ON CONFLICT (customer_id) DO UPDATE SET
      customer_name = EXCLUDED.customer_name,
      customer_address = EXCLUDED.customer_address,
      is_active = EXCLUDED.is_active,
      updated_at = EXCLUDED.updated_at
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
        conn.commit()
        logger.info(f"Upserting {len(rows)} rows into analytics.dim_customer")
    except psycopg2.DatabaseError as e:
        conn.rollback()
        logger.error(f"Failed to upsert dim_customer: {e}")
        raise
