"""Unit tests for Pydantic model validation.

Tests cover:
- OrdersQuery: status field validation
- TopOrdersQuery: limit field range validation (gt=0, le=100)
- CustomersQuery: status and limit validation
- ProductsQuery: no required parameters
- Response models: schema validation

All tests verify both valid inputs are accepted and invalid inputs raise ValidationError.
"""
import pytest
from pydantic import ValidationError

from api.models import (
    OrdersQuery, TopOrdersQuery, CustomersQuery, ProductsQuery,
    OrderGroupByDeliveryDate, OrdersResponse,
    TopOrderResponse, TopOrdersResponse,
    ProductQuantity, ProductsResponse,
    TopCustomer, CustomersResponse
)


class TestOrdersQueryValidation:
    """Unit tests for OrdersQuery Pydantic model."""

    def test_orders_query_valid_status_open(self):
        """Test: valid status='open' accepted"""
        q = OrdersQuery(status="open")
        assert q.status == "open"

    def test_orders_query_default_status(self):
        """Test: status defaults to 'open' if not provided"""
        q = OrdersQuery()
        assert q.status == "open"

    def test_orders_query_invalid_status(self):
        """Test: invalid status raises ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            OrdersQuery(status="invalid")
        assert "Input should be 'open'" in str(exc_info.value) or "open" in str(exc_info.value)

    def test_orders_query_status_pending_rejected(self):
        """Test: status='pending' rejected (only 'open' allowed)"""
        with pytest.raises(ValidationError):
            OrdersQuery(status="pending")

    def test_orders_query_status_processing_rejected(self):
        """Test: status='processing' rejected"""
        with pytest.raises(ValidationError):
            OrdersQuery(status="processing")


class TestTopOrdersQueryValidation:
    """Unit tests for TopOrdersQuery Pydantic model."""

    def test_top_orders_query_valid_limit(self):
        """Test: valid limit=10 accepted"""
        q = TopOrdersQuery(limit=10)
        assert q.limit == 10

    def test_top_orders_query_limit_boundary_1(self):
        """Test: limit=1 (minimum) accepted"""
        q = TopOrdersQuery(limit=1)
        assert q.limit == 1

    def test_top_orders_query_limit_boundary_100(self):
        """Test: limit=100 (maximum) accepted"""
        q = TopOrdersQuery(limit=100)
        assert q.limit == 100

    def test_top_orders_query_default_limit(self):
        """Test: limit defaults to 3 if not provided"""
        q = TopOrdersQuery()
        assert q.limit == 3

    def test_top_orders_query_limit_zero_rejected(self):
        """Test: limit=0 rejected (must be > 0)"""
        with pytest.raises(ValidationError) as exc_info:
            TopOrdersQuery(limit=0)
        error_msg = str(exc_info.value).lower()
        assert "greater than 0" in error_msg or "must be" in error_msg or "gt" in error_msg

    def test_top_orders_query_limit_negative_rejected(self):
        """Test: limit=-1 rejected (must be > 0)"""
        with pytest.raises(ValidationError) as exc_info:
            TopOrdersQuery(limit=-1)
        error_msg = str(exc_info.value).lower()
        assert "greater than 0" in error_msg or "must be" in error_msg

    def test_top_orders_query_limit_101_rejected(self):
        """Test: limit > 100 rejected"""
        with pytest.raises(ValidationError) as exc_info:
            TopOrdersQuery(limit=101)
        error_msg = str(exc_info.value).lower()
        assert "less than or equal to 100" in error_msg or "maximum" in error_msg or "le" in error_msg

    def test_top_orders_query_limit_999_rejected(self):
        """Test: limit=999 rejected (exceeds max)"""
        with pytest.raises(ValidationError):
            TopOrdersQuery(limit=999)

    def test_top_orders_query_limit_string_rejected(self):
        """Test: limit='abc' rejected (must be int)"""
        with pytest.raises(ValidationError) as exc_info:
            TopOrdersQuery(limit="abc")
        # Pydantic should reject non-integer string
        assert "abc" in str(exc_info.value) or "int" in str(exc_info.value).lower()


class TestCustomersQueryValidation:
    """Unit tests for CustomersQuery Pydantic model."""

    def test_customers_query_valid_defaults(self):
        """Test: CustomersQuery with default values"""
        q = CustomersQuery()
        assert q.status == "open"
        assert q.limit == 3

    def test_customers_query_valid_status_and_limit(self):
        """Test: valid status='open', limit=5"""
        q = CustomersQuery(status="open", limit=5)
        assert q.status == "open"
        assert q.limit == 5

    def test_customers_query_invalid_status(self):
        """Test: invalid status rejected"""
        with pytest.raises(ValidationError):
            CustomersQuery(status="invalid", limit=3)

    def test_customers_query_invalid_limit_too_high(self):
        """Test: limit=1000 rejected (max 100)"""
        with pytest.raises(ValidationError) as exc_info:
            CustomersQuery(status="open", limit=1000)
        error_msg = str(exc_info.value).lower()
        assert "less than or equal to 100" in error_msg or "maximum" in error_msg

    def test_customers_query_invalid_limit_zero(self):
        """Test: limit=0 rejected"""
        with pytest.raises(ValidationError):
            CustomersQuery(status="open", limit=0)

    def test_customers_query_limit_boundary_100(self):
        """Test: limit=100 accepted"""
        q = CustomersQuery(status="open", limit=100)
        assert q.limit == 100


class TestProductsQueryValidation:
    """Unit tests for ProductsQuery Pydantic model."""

    def test_products_query_no_params(self):
        """Test: ProductsQuery with no parameters"""
        q = ProductsQuery()
        # ProductsQuery has no fields; just verify it instantiates
        assert isinstance(q, ProductsQuery)

    def test_products_query_empty_instantiation(self):
        """Test: ProductsQuery() instantiates successfully"""
        q = ProductsQuery()
        assert q is not None


class TestResponseModelValidation:
    """Unit tests for response model validation."""

    def test_order_group_by_delivery_date_valid(self):
        """Test: OrderGroupByDeliveryDate with valid fields"""
        item = OrderGroupByDeliveryDate(
            delivery_date="2026-04-29",
            status="PENDING",
            order_count=42
        )
        assert item.delivery_date == "2026-04-29"
        assert item.status == "PENDING"
        assert item.order_count == 42

    def test_order_group_by_delivery_date_zero_count(self):
        """Test: OrderGroupByDeliveryDate with zero count"""
        item = OrderGroupByDeliveryDate(
            delivery_date="2026-04-29",
            status="PROCESSING",
            order_count=0
        )
        assert item.order_count == 0

    def test_orders_response_with_items(self):
        """Test: OrdersResponse with list of items"""
        response = OrdersResponse(
            items=[
                OrderGroupByDeliveryDate(delivery_date="2026-04-29", status="PENDING", order_count=10)
            ],
            total=1
        )
        assert len(response.items) == 1
        assert response.total == 1

    def test_orders_response_empty_items(self):
        """Test: OrdersResponse with empty items list"""
        response = OrdersResponse(items=[], total=0)
        assert len(response.items) == 0
        assert response.total == 0

    def test_orders_response_multiple_items(self):
        """Test: OrdersResponse with multiple items"""
        items = [
            OrderGroupByDeliveryDate(delivery_date="2026-04-29", status="PENDING", order_count=10),
            OrderGroupByDeliveryDate(delivery_date="2026-04-30", status="PROCESSING", order_count=5),
        ]
        response = OrdersResponse(items=items, total=2)
        assert len(response.items) == 2
        assert response.total == 2

    def test_top_order_response_valid(self):
        """Test: TopOrderResponse with valid fields"""
        item = TopOrderResponse(delivery_date="2026-04-29", order_count=100)
        assert item.delivery_date == "2026-04-29"
        assert item.order_count == 100

    def test_top_orders_response_with_items(self):
        """Test: TopOrdersResponse with items"""
        items = [
            TopOrderResponse(delivery_date="2026-04-29", order_count=100),
            TopOrderResponse(delivery_date="2026-04-30", order_count=80),
        ]
        response = TopOrdersResponse(items=items)
        assert len(response.items) == 2

    def test_top_orders_response_empty_items(self):
        """Test: TopOrdersResponse with empty items"""
        response = TopOrdersResponse(items=[])
        assert len(response.items) == 0

    def test_product_quantity_valid(self):
        """Test: ProductQuantity with valid fields"""
        item = ProductQuantity(
            product_id=123,
            product_name="Widget",
            quantity_pending=50
        )
        assert item.product_id == 123
        assert item.product_name == "Widget"
        assert item.quantity_pending == 50

    def test_products_response_with_items(self):
        """Test: ProductsResponse with items"""
        items = [
            ProductQuantity(product_id=123, product_name="Widget", quantity_pending=50),
            ProductQuantity(product_id=124, product_name="Gadget", quantity_pending=30),
        ]
        response = ProductsResponse(items=items)
        assert len(response.items) == 2

    def test_products_response_empty_items(self):
        """Test: ProductsResponse with empty items"""
        response = ProductsResponse(items=[])
        assert len(response.items) == 0

    def test_top_customer_valid(self):
        """Test: TopCustomer with valid fields"""
        item = TopCustomer(
            customer_id=456,
            customer_name="Acme Corp",
            pending_order_count=12
        )
        assert item.customer_id == 456
        assert item.customer_name == "Acme Corp"
        assert item.pending_order_count == 12

    def test_customers_response_with_items(self):
        """Test: CustomersResponse with items"""
        items = [
            TopCustomer(customer_id=456, customer_name="Acme Corp", pending_order_count=12),
            TopCustomer(customer_id=457, customer_name="Global Inc", pending_order_count=8),
        ]
        response = CustomersResponse(items=items)
        assert len(response.items) == 2

    def test_customers_response_empty_items(self):
        """Test: CustomersResponse with empty items"""
        response = CustomersResponse(items=[])
        assert len(response.items) == 0
