"""Pydantic request and response models for all 4 API endpoints."""

from typing import Annotated, List, Literal
from fastapi import Query
from pydantic import BaseModel, Field


# ============================================================================
# Request Models (Query Parameters)
# ============================================================================

class OrdersQuery(BaseModel):
    """Query parameters for GET /analytics/orders.

    Filters open orders by status (currently only 'open' is accepted).
    Future: Can extend to support 'pending', 'processing', 'reprocessing'.
    """
    status: Literal["open"] = Field(default="open", description="Order status filter (open only)")


class TopOrdersQuery(BaseModel):
    """Query parameters for GET /analytics/orders/top.

    Limits the number of results returned (must be positive, max 100).
    """
    limit: Annotated[int, Query(gt=0, le=100)] = Field(
        default=3,
        description="Maximum number of results (1-100, default 3)"
    )


class ProductsQuery(BaseModel):
    """Query parameters for GET /analytics/orders/product.

    No parameters required; returns all products with pending items.
    """
    pass


class CustomersQuery(BaseModel):
    """Query parameters for GET /analytics/orders/customers.

    Filters by status and limits the number of results.
    """
    status: Literal["open"] = Field(default="open", description="Order status filter (open only)")
    limit: Annotated[int, Query(gt=0, le=100)] = Field(
        default=3,
        description="Maximum number of results (1-100, default 3)"
    )


# ============================================================================
# Response Models
# ============================================================================

class OrderGroupByDeliveryDate(BaseModel):
    """Single row response for GET /analytics/orders.

    Represents orders grouped by delivery_date and status.
    """
    delivery_date: str = Field(
        description="ISO 8601 date (YYYY-MM-DD) of delivery"
    )
    status: str = Field(
        description="Order status (e.g., 'PENDING', 'PROCESSING', 'REPROCESSING')"
    )
    order_count: int = Field(
        description="Count of orders with this delivery_date and status"
    )


class OrdersResponse(BaseModel):
    """Wrapper response for GET /analytics/orders.

    Contains list of orders grouped by delivery_date and status.
    """
    items: List[OrderGroupByDeliveryDate] = Field(
        description="List of orders grouped by delivery_date and status"
    )
    total: int = Field(
        description="Total count of result rows"
    )


class TopOrderResponse(BaseModel):
    """Single row response for GET /analytics/orders/top.

    Represents the top delivery dates by order count.
    """
    delivery_date: str = Field(
        description="ISO 8601 date (YYYY-MM-DD) of delivery"
    )
    order_count: int = Field(
        description="Count of open orders with this delivery_date"
    )


class TopOrdersResponse(BaseModel):
    """Wrapper response for GET /analytics/orders/top.

    Contains list of top delivery dates by order count.
    """
    items: List[TopOrderResponse] = Field(
        description="List of top delivery dates by order count, limited to N results"
    )


class ProductQuantity(BaseModel):
    """Single row response for GET /analytics/orders/product.

    Represents pending items grouped by product.
    """
    product_id: int = Field(
        description="Product ID from dim_product"
    )
    product_name: str = Field(
        description="Product name from dim_product"
    )
    quantity_pending: int = Field(
        description="Total quantity of pending items for this product (SUM aggregation)"
    )


class ProductsResponse(BaseModel):
    """Wrapper response for GET /analytics/orders/product.

    Contains list of products with total pending quantity.
    """
    items: List[ProductQuantity] = Field(
        description="List of products with pending quantity"
    )


class TopCustomer(BaseModel):
    """Single row response for GET /analytics/orders/customers.

    Represents top customers by count of open orders.
    """
    customer_id: int = Field(
        description="Customer ID from dim_customer"
    )
    customer_name: str = Field(
        description="Customer name from dim_customer"
    )
    pending_order_count: int = Field(
        description="Count of distinct open orders for this customer"
    )


class CustomersResponse(BaseModel):
    """Wrapper response for GET /analytics/orders/customers.

    Contains list of top customers by pending order count.
    """
    items: List[TopCustomer] = Field(
        description="List of top customers by pending order count, limited to N results"
    )
