"""Shared pytest fixtures for analytics pipeline tests.

Provides:
- Database connection URLs (source and analytics)
- Sample Debezium CDC events (INSERT/UPDATE/DELETE operations)
- Test database schema and connection management
- FastAPI TestClient and test database fixtures for API testing
"""
import json
import pytest
import os
import psycopg2
from fastapi.testclient import TestClient


@pytest.fixture
def source_db_url():
    """Connection string for source database.

    Used in integration tests to verify backfill reads correct data.
    Returns PostgreSQL connection URL from POSTGRES_SOURCE_URL env var
    or default localhost connection.

    Example:
        postgresql://postgres:password@localhost:5432/deel

    Returns:
        str: PostgreSQL connection URL for source database
    """
    return os.getenv(
        'POSTGRES_SOURCE_URL',
        'postgresql://postgres:password@localhost:5432/deel'
    )


@pytest.fixture
def analytics_db_url():
    """Connection string for analytics database.

    Used in integration tests to verify upserts and data integrity.
    Returns PostgreSQL connection URL from POSTGRES_ANALYTICS_URL env var
    or default localhost connection.

    Example:
        postgresql://postgres:password@localhost:5432/deel

    Returns:
        str: PostgreSQL connection URL for analytics database
    """
    return os.getenv(
        'POSTGRES_ANALYTICS_URL',
        'postgresql://postgres:password@localhost:5432/deel'
    )


@pytest.fixture
def sample_debezium_insert_event():
    """Sample Debezium INSERT event (op='c').

    Represents a new product being created in the source database.
    Used for unit testing Debezium payload parsing.

    Debezium operation codes:
    - 'c' = CREATE (INSERT)
    - 'u' = UPDATE
    - 'd' = DELETE

    Returns:
        dict: Debezium CDC event with INSERT operation
    """
    return {
        "op": "c",
        "before": None,
        "after": {
            "product_id": 999,
            "product_name": "Test Product",
            "barcode": "TEST123",
            "unity_price": 49.99,
            "is_active": True,
            "updated_at": "2026-04-28T10:00:00Z"
        },
        "source": {
            "table": "products",
            "ts_ms": 1714312800000
        }
    }


@pytest.fixture
def sample_debezium_update_event():
    """Sample Debezium UPDATE event (op='u').

    Represents an existing product being modified in the source database.
    Includes both 'before' (previous state) and 'after' (new state) fields.

    Returns:
        dict: Debezium CDC event with UPDATE operation
    """
    return {
        "op": "u",
        "before": {
            "product_id": 999,
            "product_name": "Test Product",
            "barcode": "TEST123",
            "unity_price": 49.99,
            "is_active": True,
            "updated_at": "2026-04-28T10:00:00Z"
        },
        "after": {
            "product_id": 999,
            "product_name": "Test Product (Updated)",
            "barcode": "TEST123",
            "unity_price": 59.99,
            "is_active": True,
            "updated_at": "2026-04-29T11:30:00Z"
        },
        "source": {
            "table": "products",
            "ts_ms": 1714399800000
        }
    }


@pytest.fixture
def sample_debezium_delete_event():
    """Sample Debezium DELETE event (op='d').

    Represents a product being deleted in the source database.
    'before' field contains the deleted row; 'after' is null.

    Returns:
        dict: Debezium CDC event with DELETE operation
    """
    return {
        "op": "d",
        "before": {
            "product_id": 999,
            "product_name": "Test Product (Updated)",
            "barcode": "TEST123",
            "unity_price": 59.99,
            "is_active": False,
            "updated_at": "2026-04-29T11:30:00Z"
        },
        "after": None,
        "source": {
            "table": "products",
            "ts_ms": 1714400000000
        }
    }


@pytest.fixture
def sample_debezium_order_event():
    """Sample Debezium ORDER INSERT event.

    Represents a new order being created in the source database.
    Used for testing order dimension upserts.

    Returns:
        dict: Debezium CDC event for order INSERT
    """
    return {
        "op": "c",
        "before": None,
        "after": {
            "order_id": 5001,
            "order_date": "2026-04-28",
            "status": "PENDING",
            "customer_id": 101,
            "updated_at": "2026-04-28T14:00:00Z"
        },
        "source": {
            "table": "orders",
            "ts_ms": 1714312800000
        }
    }


@pytest.fixture
def sample_debezium_order_item_event():
    """Sample Debezium ORDER_ITEM INSERT event.

    Represents a new order item (line item) being created.
    Used for testing fact table upserts.

    Returns:
        dict: Debezium CDC event for order_item INSERT
    """
    return {
        "op": "c",
        "before": None,
        "after": {
            "order_item_id": 8001,
            "order_id": 5001,
            "product_id": 999,
            "customer_id": 101,
            "quanity": 5,
            "delivery_date": "2026-05-05",
            "updated_at": "2026-04-28T14:00:00Z"
        },
        "source": {
            "table": "order_items",
            "ts_ms": 1714312800000
        }
    }


@pytest.fixture
def sample_debezium_customer_event():
    """Sample Debezium CUSTOMER INSERT event.

    Represents a new customer being created in the source database.
    Used for testing customer dimension upserts.

    Returns:
        dict: Debezium CDC event for customer INSERT
    """
    return {
        "op": "c",
        "before": None,
        "after": {
            "customer_id": 101,
            "customer_name": "Test Customer",
            "customer_address": "123 Test St, City, State 12345",
            "is_active": True,
            "updated_at": "2026-04-28T14:00:00Z"
        },
        "source": {
            "table": "customers",
            "ts_ms": 1714312800000
        }
    }


# ============================================================================
# API Test Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def api_db_connection():
    """Create test database connection for API tests (session-scoped).

    Uses the analytics database URL from environment or defaults to localhost.
    This connection is reused across all tests in the session for efficiency.

    Yields:
        psycopg2 connection object

    Raises:
        psycopg2.OperationalError: If database connection fails
    """
    db_url = os.getenv(
        'POSTGRES_ANALYTICS_URL',
        'postgresql://postgres:password@localhost:5432/deel'
    )
    try:
        conn = psycopg2.connect(db_url, connect_timeout=5)
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope="function")
def test_client(api_db_connection):
    """Create FastAPI TestClient with overridden database dependency.

    Imports the app and overrides the get_db dependency to use the test
    database connection. This fixture is function-scoped to ensure fresh
    dependency overrides for each test.

    Args:
        api_db_connection: Session-scoped database connection

    Yields:
        TestClient instance with dependency overrides active

    Note:
        Clears dependency_overrides after yielding to ensure clean state.
    """
    from api.main import app
    from api.db import get_db

    def override_get_db():
        """Override dependency to yield test connection."""
        try:
            yield api_db_connection
        finally:
            pass  # Don't close; connection is managed by session fixture

    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def sample_data(api_db_connection):
    """Insert and clean up test data for API tests.

    Runs before and after each test function to:
    1. Insert minimal test data (2-3 orders, products, customers)
    2. Clean up after test completes

    This ensures isolated, repeatable tests with known data.

    Args:
        api_db_connection: Session-scoped database connection

    Yields:
        None (fixture is autouse)
    """
    # Setup: Insert test data
    cursor = api_db_connection.cursor()
    try:
        # Insert test customer
        cursor.execute("""
            INSERT INTO analytics.dim_customer (customer_id, customer_name, is_current, created_at, updated_at)
            VALUES (%s, %s, true, now(), now())
            ON CONFLICT (customer_id) DO NOTHING
        """, (9999, 'Test Customer'))

        # Insert test product
        cursor.execute("""
            INSERT INTO analytics.dim_product (product_id, product_name, barcode, unity_price, is_active, is_current, created_at, updated_at)
            VALUES (%s, %s, %s, %s, true, true, now(), now())
            ON CONFLICT (product_id) DO NOTHING
        """, (8888, 'Test Product', '123456789', 10.00))

        # Insert test order
        cursor.execute("""
            INSERT INTO analytics.dim_order (order_id, order_date, status, is_current, created_at, updated_at)
            VALUES (%s, %s, %s, true, now(), now())
            ON CONFLICT (order_id) DO NOTHING
        """, (9999, '2026-04-29', 'PENDING'))

        # Insert test order item (fact table)
        cursor.execute("""
            INSERT INTO analytics.fct_order_items (order_id, product_id, customer_id, delivery_date, quantity_pending, is_open, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, true, now(), now())
            ON CONFLICT (order_id, product_id) DO NOTHING
        """, (9999, 8888, 9999, '2026-05-01', 5))

        api_db_connection.commit()
    except Exception as e:
        api_db_connection.rollback()
        # Continue even if insert fails (data may already exist)
        print(f"Note: Test data insertion encountered: {e}")
    finally:
        cursor.close()

    yield  # Run the test

    # Cleanup: Delete test data
    cursor = api_db_connection.cursor()
    try:
        cursor.execute("DELETE FROM analytics.fct_order_items WHERE order_id = %s", (9999,))
        cursor.execute("DELETE FROM analytics.dim_order WHERE order_id = %s", (9999,))
        cursor.execute("DELETE FROM analytics.dim_product WHERE product_id = %s", (8888,))
        cursor.execute("DELETE FROM analytics.dim_customer WHERE customer_id = %s", (9999,))
        api_db_connection.commit()
    except Exception as e:
        api_db_connection.rollback()
        print(f"Note: Test data cleanup encountered: {e}")
    finally:
        cursor.close()
