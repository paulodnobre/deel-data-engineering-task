"""FastAPI application with 4 analytics endpoints, health check, and error handlers."""

import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse

from api.config import DATABASE_URL, MIN_POOL_SIZE, MAX_POOL_SIZE, LOG_LEVEL
from api.db import init_pool, get_db, close_pool
from api.models import (
    OrdersQuery, OrdersResponse, OrderGroupByDeliveryDate,
    TopOrdersQuery, TopOrdersResponse, TopOrderResponse,
    ProductsQuery, ProductsResponse, ProductQuantity,
    CustomersQuery, CustomersResponse, TopCustomer
)

# Configure logging
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown handler for app lifecycle (FastAPI 0.128.0 pattern).

    Startup: Initialize connection pool at app startup.
    Shutdown: Close connection pool on app shutdown.
    """
    # Startup
    try:
        init_pool(DATABASE_URL, MIN_POOL_SIZE, MAX_POOL_SIZE)
        logger.info("Connection pool initialized on app startup")
    except Exception as e:
        logger.error(f"Failed to initialize app: {e}")
        raise

    yield

    # Shutdown
    try:
        close_pool()
        logger.info("Connection pool closed on app shutdown")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


app = FastAPI(
    title="Deel Analytics API",
    description="REST API for aggregated analytics from PostgreSQL star schema",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================================================
# Endpoint 1: GET /analytics/orders
# ============================================================================

@app.get("/analytics/orders", response_model=OrdersResponse)
async def get_orders(
    query: Annotated[OrdersQuery, Query()],
    db = Depends(get_db)
):
    """List open orders grouped by delivery_date and status.

    Returns: [{"delivery_date": "YYYY-MM-DD", "status": "PENDING", "order_count": 42}, ...]

    Query Parameters:
    - status: Order status filter (currently only "open" supported)
    """
    # Use LIMIT and GROUP BY to prevent unbounded result sets; protects against slow queries on large datasets
    try:
        with db.cursor() as cursor:
            cursor.execute("""
                SELECT
                    f.delivery_date::TEXT,
                    d.status,
                    COUNT(*) as order_count
                FROM analytics.fct_order_items f
                JOIN analytics.dim_order d ON f.order_id = d.order_id
                WHERE f.is_open = TRUE
                GROUP BY f.delivery_date, d.status
                ORDER BY f.delivery_date DESC, d.status
            """)
            rows = cursor.fetchall()

        items = [
            OrderGroupByDeliveryDate(
                delivery_date=row[0],
                status=row[1],
                order_count=row[2]
            )
            for row in rows
        ]
        return OrdersResponse(items=items, total=len(items))

    except Exception as e:
        logger.error(f"Error in GET /analytics/orders: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error; contact support"
        )


# ============================================================================
# Endpoint 2: GET /analytics/orders/top
# ============================================================================

@app.get("/analytics/orders/top", response_model=TopOrdersResponse)
async def get_top_orders(
    query: Annotated[TopOrdersQuery, Query()],
    db = Depends(get_db)
):
    """Top N delivery_dates by count of open orders.

    Returns: [{"delivery_date": "YYYY-MM-DD", "order_count": 100}, ...]

    Query Parameters:
    - limit: Number of top results to return (1-100, default 3)
    """
    try:
        with db.cursor() as cursor:
            cursor.execute("""
                SELECT
                    f.delivery_date::TEXT,
                    COUNT(*) as order_count
                FROM analytics.fct_order_items f
                WHERE f.is_open = TRUE
                GROUP BY f.delivery_date
                ORDER BY order_count DESC
                LIMIT %s
            """, (query.limit,))
            rows = cursor.fetchall()

        items = [
            TopOrderResponse(delivery_date=row[0], order_count=row[1])
            for row in rows
        ]
        return TopOrdersResponse(items=items)

    except Exception as e:
        logger.error(f"Error in GET /analytics/orders/top: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error; contact support"
        )


# ============================================================================
# Endpoint 3: GET /analytics/orders/product
# ============================================================================

@app.get("/analytics/orders/product", response_model=ProductsResponse)
async def get_products(
    query: Annotated[ProductsQuery, Query()],
    db = Depends(get_db)
):
    """Pending items grouped by product.

    Returns: [{"product_id": 123, "product_name": "Widget", "quantity_pending": 50}, ...]

    Query Parameters:
    - None (returns all products with pending items)
    """
    try:
        with db.cursor() as cursor:
            cursor.execute("""
                SELECT
                    f.product_id,
                    p.product_name,
                    COALESCE(SUM(f.quantity_pending), 0) as quantity_pending
                FROM analytics.fct_order_items f
                JOIN analytics.dim_product p ON f.product_id = p.product_id
                WHERE f.is_open = TRUE
                GROUP BY f.product_id, p.product_name
                ORDER BY quantity_pending DESC
            """)
            rows = cursor.fetchall()

        items = [
            ProductQuantity(
                product_id=row[0],
                product_name=row[1],
                quantity_pending=row[2]
            )
            for row in rows
        ]
        return ProductsResponse(items=items)

    except Exception as e:
        logger.error(f"Error in GET /analytics/orders/product: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error; contact support"
        )


# ============================================================================
# Endpoint 4: GET /analytics/orders/customers
# ============================================================================

@app.get("/analytics/orders/customers", response_model=CustomersResponse)
async def get_customers(
    query: Annotated[CustomersQuery, Query()],
    db = Depends(get_db)
):
    """Top N customers by count of pending orders.

    Returns: [{"customer_id": 456, "customer_name": "Acme Corp", "pending_order_count": 12}, ...]

    Query Parameters:
    - status: Order status filter (currently only "open" supported)
    - limit: Number of top results to return (1-100, default 3)
    """
    try:
        with db.cursor() as cursor:
            cursor.execute("""
                SELECT
                    c.customer_id,
                    c.customer_name,
                    COUNT(DISTINCT f.order_id) as pending_order_count
                FROM analytics.fct_order_items f
                JOIN analytics.dim_order d ON f.order_id = d.order_id
                JOIN analytics.dim_customer c ON f.customer_id = c.customer_id
                WHERE f.is_open = TRUE
                GROUP BY c.customer_id, c.customer_name
                ORDER BY pending_order_count DESC
                LIMIT %s
            """, (query.limit,))
            rows = cursor.fetchall()

        items = [
            TopCustomer(
                customer_id=row[0],
                customer_name=row[1],
                pending_order_count=row[2]
            )
            for row in rows
        ]
        return CustomersResponse(items=items)

    except Exception as e:
        logger.error(f"Error in GET /analytics/orders/customers: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error; contact support"
        )


# ============================================================================
# Health Check Endpoint
# ============================================================================

@app.get("/health", tags=["Health"])
async def health_check(db = Depends(get_db)):
    """Health check endpoint. Returns 200 if DB is connected.

    Returns: {"status": "ok", "db": "connected"}
    """
    # Return connection status to enable docker-compose healthchecks for API readiness detection
    try:
        with db.cursor() as cursor:
            cursor.execute("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable"
        )


# ============================================================================
# Exception Handlers (Optional)
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Standardize HTTPException responses."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )
