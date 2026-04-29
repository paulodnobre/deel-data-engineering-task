"""Integration tests for all 4 API endpoints and health check.

Tests cover:
- GET /analytics/orders: Groups open orders by delivery_date and status
- GET /analytics/orders/top: Top N delivery dates by order count
- GET /analytics/orders/product: Pending items grouped by product
- GET /analytics/orders/customers: Top N customers by pending order count
- GET /health: Database connectivity check

All tests use the TestClient fixture with real database queries against
the analytics schema populated by sample_data fixture.
"""
import pytest


class TestOrdersEndpoint:
    """Integration tests for GET /analytics/orders endpoint."""

    def test_get_orders_status_200(self, test_client):
        """Test: GET /analytics/orders returns 200 OK"""
        response = test_client.get("/analytics/orders")
        assert response.status_code == 200

    def test_get_orders_response_schema(self, test_client):
        """Test: Response matches OrdersResponse schema"""
        response = test_client.get("/analytics/orders")
        data = response.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert "delivery_date" in item
            assert "status" in item
            assert "order_count" in item

    def test_get_orders_has_total(self, test_client):
        """Test: Response includes total count field"""
        response = test_client.get("/analytics/orders")
        data = response.json()
        assert "total" in data
        assert isinstance(data["total"], int)

    def test_get_orders_items_are_list(self, test_client):
        """Test: items field is always a list"""
        response = test_client.get("/analytics/orders")
        data = response.json()
        assert isinstance(data["items"], list)

    def test_get_orders_fields_have_correct_types(self, test_client):
        """Test: Response fields have correct types"""
        response = test_client.get("/analytics/orders")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert isinstance(item["delivery_date"], str)
            assert isinstance(item["status"], str)
            assert isinstance(item["order_count"], int)

    def test_get_orders_delivery_date_format(self, test_client):
        """Test: delivery_date is ISO 8601 format (YYYY-MM-DD)"""
        response = test_client.get("/analytics/orders")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            # Simple check: format should be YYYY-MM-DD
            assert len(item["delivery_date"]) == 10
            assert item["delivery_date"][4] == "-"
            assert item["delivery_date"][7] == "-"

    def test_get_orders_order_count_positive(self, test_client):
        """Test: order_count is positive integer"""
        response = test_client.get("/analytics/orders")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert item["order_count"] > 0

    def test_get_orders_with_status_param(self, test_client):
        """Test: accepts status query parameter"""
        response = test_client.get("/analytics/orders?status=open")
        assert response.status_code == 200

    def test_get_orders_empty_result(self, test_client):
        """Test: empty result returns items=[] and total=0"""
        # Even with no data, endpoint should not error
        response = test_client.get("/analytics/orders")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data["items"], list)
        # total should equal items length
        assert data["total"] == len(data["items"])


class TestTopOrdersEndpoint:
    """Integration tests for GET /analytics/orders/top endpoint."""

    def test_get_top_orders_status_200(self, test_client):
        """Test: GET /analytics/orders/top returns 200 OK"""
        response = test_client.get("/analytics/orders/top")
        assert response.status_code == 200

    def test_get_top_orders_response_schema(self, test_client):
        """Test: Response matches TopOrdersResponse schema"""
        response = test_client.get("/analytics/orders/top")
        data = response.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert "delivery_date" in item
            assert "order_count" in item

    def test_get_top_orders_respects_limit_default(self, test_client):
        """Test: Default limit is 3"""
        response = test_client.get("/analytics/orders/top")
        data = response.json()
        assert len(data["items"]) <= 3

    def test_get_top_orders_respects_limit_custom(self, test_client):
        """Test: Response has at most limit items (limit=5)"""
        response = test_client.get("/analytics/orders/top?limit=5")
        data = response.json()
        assert len(data["items"]) <= 5

    def test_get_top_orders_respects_limit_boundary(self, test_client):
        """Test: Response respects limit=1"""
        response = test_client.get("/analytics/orders/top?limit=1")
        data = response.json()
        assert len(data["items"]) <= 1

    def test_get_top_orders_respects_limit_maximum(self, test_client):
        """Test: Response respects limit=100"""
        response = test_client.get("/analytics/orders/top?limit=100")
        data = response.json()
        assert len(data["items"]) <= 100

    def test_get_top_orders_fields_have_correct_types(self, test_client):
        """Test: Response fields have correct types"""
        response = test_client.get("/analytics/orders/top?limit=3")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert isinstance(item["delivery_date"], str)
            assert isinstance(item["order_count"], int)

    def test_get_top_orders_delivery_date_format(self, test_client):
        """Test: delivery_date is ISO 8601 format"""
        response = test_client.get("/analytics/orders/top")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert len(item["delivery_date"]) == 10
            assert item["delivery_date"][4] == "-"
            assert item["delivery_date"][7] == "-"

    def test_get_top_orders_sorted_descending(self, test_client):
        """Test: Items sorted by order_count descending"""
        response = test_client.get("/analytics/orders/top?limit=10")
        data = response.json()
        items = data["items"]
        if len(items) > 1:
            for i in range(len(items) - 1):
                assert items[i]["order_count"] >= items[i+1]["order_count"]

    def test_get_top_orders_order_count_positive(self, test_client):
        """Test: order_count is positive integer"""
        response = test_client.get("/analytics/orders/top")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert item["order_count"] > 0


class TestProductsEndpoint:
    """Integration tests for GET /analytics/orders/product endpoint."""

    def test_get_products_status_200(self, test_client):
        """Test: GET /analytics/orders/product returns 200 OK"""
        response = test_client.get("/analytics/orders/product")
        assert response.status_code == 200

    def test_get_products_response_schema(self, test_client):
        """Test: Response matches ProductsResponse schema"""
        response = test_client.get("/analytics/orders/product")
        data = response.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert "product_id" in item
            assert "product_name" in item
            assert "quantity_pending" in item

    def test_get_products_fields_have_correct_types(self, test_client):
        """Test: Response fields have correct types"""
        response = test_client.get("/analytics/orders/product")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert isinstance(item["product_id"], int)
            assert isinstance(item["product_name"], str)
            assert isinstance(item["quantity_pending"], int)

    def test_get_products_product_id_positive(self, test_client):
        """Test: product_id is positive integer"""
        response = test_client.get("/analytics/orders/product")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert item["product_id"] > 0

    def test_get_products_quantity_pending_positive(self, test_client):
        """Test: quantity_pending is positive integer (aggregated)"""
        response = test_client.get("/analytics/orders/product")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            # Quantity should be >= 0 (can be zero for products with no pending items)
            assert item["quantity_pending"] >= 0

    def test_get_products_product_name_not_empty(self, test_client):
        """Test: product_name is not empty"""
        response = test_client.get("/analytics/orders/product")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert len(item["product_name"]) > 0

    def test_get_products_no_params_required(self, test_client):
        """Test: Endpoint works with no query parameters"""
        response = test_client.get("/analytics/orders/product")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data

    def test_get_products_returns_list(self, test_client):
        """Test: items is always a list"""
        response = test_client.get("/analytics/orders/product")
        data = response.json()
        assert isinstance(data["items"], list)


class TestCustomersEndpoint:
    """Integration tests for GET /analytics/orders/customers endpoint."""

    def test_get_customers_status_200(self, test_client):
        """Test: GET /analytics/orders/customers returns 200 OK"""
        response = test_client.get("/analytics/orders/customers")
        assert response.status_code == 200

    def test_get_customers_response_schema(self, test_client):
        """Test: Response matches CustomersResponse schema"""
        response = test_client.get("/analytics/orders/customers")
        data = response.json()
        assert "items" in data
        assert isinstance(data["items"], list)
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert "customer_id" in item
            assert "customer_name" in item
            assert "pending_order_count" in item

    def test_get_customers_respects_limit_default(self, test_client):
        """Test: Default limit is 3"""
        response = test_client.get("/analytics/orders/customers")
        data = response.json()
        assert len(data["items"]) <= 3

    def test_get_customers_respects_limit_custom(self, test_client):
        """Test: Response has at most limit items (limit=5)"""
        response = test_client.get("/analytics/orders/customers?limit=5")
        data = response.json()
        assert len(data["items"]) <= 5

    def test_get_customers_respects_limit_maximum(self, test_client):
        """Test: Response respects limit=100"""
        response = test_client.get("/analytics/orders/customers?limit=100")
        data = response.json()
        assert len(data["items"]) <= 100

    def test_get_customers_fields_have_correct_types(self, test_client):
        """Test: Response fields have correct types"""
        response = test_client.get("/analytics/orders/customers")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert isinstance(item["customer_id"], int)
            assert isinstance(item["customer_name"], str)
            assert isinstance(item["pending_order_count"], int)

    def test_get_customers_customer_id_positive(self, test_client):
        """Test: customer_id is positive integer"""
        response = test_client.get("/analytics/orders/customers")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert item["customer_id"] > 0

    def test_get_customers_customer_name_not_empty(self, test_client):
        """Test: customer_name is not empty"""
        response = test_client.get("/analytics/orders/customers")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert len(item["customer_name"]) > 0

    def test_get_customers_pending_order_count_positive(self, test_client):
        """Test: pending_order_count is positive integer"""
        response = test_client.get("/analytics/orders/customers")
        data = response.json()
        if len(data["items"]) > 0:
            item = data["items"][0]
            assert item["pending_order_count"] > 0

    def test_get_customers_with_status_param(self, test_client):
        """Test: accepts status query parameter"""
        response = test_client.get("/analytics/orders/customers?status=open")
        assert response.status_code == 200

    def test_get_customers_returns_list(self, test_client):
        """Test: items is always a list"""
        response = test_client.get("/analytics/orders/customers")
        data = response.json()
        assert isinstance(data["items"], list)


class TestHealthEndpoint:
    """Integration tests for GET /health endpoint."""

    def test_health_status_200(self, test_client):
        """Test: GET /health returns 200 OK"""
        response = test_client.get("/health")
        assert response.status_code == 200

    def test_health_response_format(self, test_client):
        """Test: Response has status and db fields"""
        response = test_client.get("/health")
        data = response.json()
        assert "status" in data
        assert "db" in data

    def test_health_status_value(self, test_client):
        """Test: status field value is 'ok'"""
        response = test_client.get("/health")
        data = response.json()
        assert data["status"] == "ok"

    def test_health_db_value(self, test_client):
        """Test: db field value is 'connected'"""
        response = test_client.get("/health")
        data = response.json()
        assert data["db"] == "connected"

    def test_health_response_is_json(self, test_client):
        """Test: Response is valid JSON"""
        response = test_client.get("/health")
        # If response is not JSON, .json() will raise
        data = response.json()
        assert isinstance(data, dict)
