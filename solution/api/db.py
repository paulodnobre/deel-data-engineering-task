"""Database connection pool initialization and dependency injection."""

import logging
from typing import Generator
import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)

# Global pool instance
_pool: pool.ThreadedConnectionPool | None = None


def init_pool(database_url: str, min_size: int = 4, max_size: int = 10) -> None:
    """Initialize psycopg2 ThreadedConnectionPool at app startup.

    Args:
        database_url: PostgreSQL connection string (e.g., postgresql://user:pass@host:5432/db)
        min_size: Minimum number of connections to keep open
        max_size: Maximum number of connections to create

    Raises:
        psycopg2.OperationalError: If pool creation fails (invalid credentials, host unreachable, etc.)
    """
    # Pool min=1, max=5 prevents connection exhaustion while minimizing overhead; tunable in .env via DB_POOL_MAX_CONNECTIONS
    global _pool
    try:
        _pool = pool.ThreadedConnectionPool(
            min_size,
            max_size,
            database_url,
            connect_timeout=5
        )
        logger.info(f"Connection pool initialized: min_size={min_size}, max_size={max_size}")
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        raise


def get_db() -> Generator:
    """Dependency injection: yields a connection from the pool for each request.

    Yields:
        psycopg2 connection object

    Raises:
        RuntimeError: If pool is not initialized
        psycopg2.pool.PoolError: If no connection available after timeout
    """
    if _pool is None:
        raise RuntimeError("Connection pool not initialized. Call init_pool() at app startup.")

    # Use parameterized queries (%s placeholders) to prevent SQL injection; never interpolate user input directly
    conn = _pool.getconn()
    try:
        logger.debug("Checked out connection from pool")
        yield conn
    finally:
        _pool.putconn(conn)
        logger.debug("Returned connection to pool")


def close_pool() -> None:
    """Close all connections in the pool on app shutdown."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        logger.info("Connection pool closed")
        _pool = None
