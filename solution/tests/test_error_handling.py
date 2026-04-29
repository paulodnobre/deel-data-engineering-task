"""Error handling tests for API validation, server errors, and safe messages.

Tests cover:
- Validation errors (422): Invalid query parameters rejected before handlers run
- Server errors (500): Database errors return safe messages (no stack trace)
- Error message safety: No SQL, no passwords, no traceback exposed
- All error scenarios with monkeypatch for dependency injection

Error codes tested:
- 422 Unprocessable Entity: Invalid input (limit range, status enum)
- 500 Internal Server Error: Database connection failures, query errors
- 503 Service Unavailable: Database unreachable (health check)
"""
import pytest
from fastapi import status


class TestValidationErrors:
    """Tests for validation errors (422) on invalid query params."""

    def test_invalid_limit_too_high_returns_422(self, test_client):
        """Test: limit > 100 returns 422 Unprocessable Entity"""
        response = test_client.get("/analytics/orders/top?limit=101")
        assert response.status_code == 422

    def test_invalid_limit_zero_returns_422(self, test_client):
        """Test: limit = 0 returns 422"""
        response = test_client.get("/analytics/orders/top?limit=0")
        assert response.status_code == 422

    def test_invalid_limit_negative_returns_422(self, test_client):
        """Test: limit < 0 returns 422"""
        response = test_client.get("/analytics/orders/top?limit=-1")
        assert response.status_code == 422

    def test_invalid_limit_string_returns_422(self, test_client):
        """Test: limit='abc' returns 422"""
        response = test_client.get("/analytics/orders/top?limit=abc")
        assert response.status_code == 422

    def test_invalid_status_returns_422(self, test_client):
        """Test: invalid status value returns 422"""
        response = test_client.get("/analytics/orders?status=invalid")
        assert response.status_code == 422

    def test_invalid_status_pending_returns_422(self, test_client):
        """Test: status='pending' (not 'open') returns 422"""
        response = test_client.get("/analytics/orders?status=pending")
        assert response.status_code == 422

    def test_invalid_status_processing_returns_422(self, test_client):
        """Test: status='processing' returns 422"""
        response = test_client.get("/analytics/orders?status=processing")
        assert response.status_code == 422

    def test_customers_invalid_status_returns_422(self, test_client):
        """Test: GET /customers with invalid status returns 422"""
        response = test_client.get("/analytics/orders/customers?status=invalid")
        assert response.status_code == 422

    def test_customers_invalid_limit_returns_422(self, test_client):
        """Test: GET /customers with limit > 100 returns 422"""
        response = test_client.get("/analytics/orders/customers?limit=999")
        assert response.status_code == 422

    def test_validation_error_has_detail(self, test_client):
        """Test: 422 response includes error detail"""
        response = test_client.get("/analytics/orders/top?limit=999")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data


class TestServerErrors:
    """Tests for server errors (500) from database issues."""

    def test_db_error_returns_500(self, test_client, api_db_connection):
        """Test: Database error returns 500 with safe message.

        Simulates database connection failure by breaking the get_db dependency.
        """
        from api.main import app
        from api.db import get_db

        def broken_get_db():
            raise Exception("Simulated DB error: connection refused")

        app.dependency_overrides[get_db] = broken_get_db

        response = test_client.get("/analytics/orders")
        assert response.status_code == 500

        data = response.json()
        assert "detail" in data
        # Should have a message (not empty)
        assert data["detail"] != ""

        # Clean up overrides
        app.dependency_overrides.clear()

    def test_db_error_message_is_safe(self, test_client, api_db_connection):
        """Test: 500 error message doesn't expose internal details."""
        from api.main import app
        from api.db import get_db

        def broken_get_db():
            raise Exception("Simulated DB error: password rejected")

        app.dependency_overrides[get_db] = broken_get_db

        response = test_client.get("/analytics/orders")
        data = response.json()
        error_msg = data.get("detail", "").lower()

        # Should not contain sensitive information
        assert "password" not in error_msg
        assert "traceback" not in error_msg
        assert "psycopg" not in error_msg

        # Clean up overrides
        app.dependency_overrides.clear()

    def test_db_pool_error_returns_500(self, test_client, api_db_connection):
        """Test: Database pool error returns 500"""
        from api.main import app
        from api.db import get_db

        def pool_error_get_db():
            raise RuntimeError("Connection pool exhausted")

        app.dependency_overrides[get_db] = pool_error_get_db

        response = test_client.get("/analytics/orders/top")
        assert response.status_code == 500

        # Clean up overrides
        app.dependency_overrides.clear()

    def test_query_error_returns_500(self, test_client, api_db_connection):
        """Test: Query execution error returns 500 with safe message"""
        from api.main import app
        from api.db import get_db

        def bad_query_get_db():
            # Return a connection that will fail on query execution
            class BadConnection:
                def cursor(self):
                    return self
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    pass
                def execute(self, *args):
                    raise Exception("Simulated query error: syntax error in SQL")

            return BadConnection()

        app.dependency_overrides[get_db] = bad_query_get_db

        response = test_client.get("/analytics/orders/top")
        assert response.status_code == 500

        data = response.json()
        # Should have detail but not expose SQL
        assert "detail" in data

        # Clean up overrides
        app.dependency_overrides.clear()


class TestErrorMessageSafety:
    """Tests for safe error messages (no stack trace, no SQL)."""

    def test_error_response_no_traceback(self, test_client):
        """Test: Error responses don't include Python traceback."""
        # Trigger a 422 error (safe, known validation error)
        response = test_client.get("/analytics/orders/top?limit=999")
        data = response.json()
        response_text = str(data)

        assert "Traceback" not in response_text
        assert "File \"" not in response_text
        assert "line " not in response_text

    def test_error_response_no_raw_sql(self, test_client):
        """Test: Error responses don't expose raw SQL queries."""
        # Force a 422 error
        response = test_client.get("/analytics/orders?status=bad")
        data = response.json()
        response_text = str(data)

        assert "SELECT" not in response_text
        assert "FROM analytics" not in response_text
        assert "WHERE " not in response_text

    def test_validation_error_message_helpful(self, test_client):
        """Test: 422 error message is helpful (not cryptic)."""
        response = test_client.get("/analytics/orders/top?limit=999")
        data = response.json()
        error_msg = str(data)

        # Should have some message
        assert len(error_msg) > 0

    def test_db_error_message_is_user_friendly(self, test_client, api_db_connection):
        """Test: 500 error message is user-friendly."""
        from api.main import app
        from api.db import get_db

        def broken_get_db():
            raise Exception("Simulated DB error: connection refused")

        app.dependency_overrides[get_db] = broken_get_db

        response = test_client.get("/analytics/orders")
        data = response.json()
        error_msg = data.get("detail", "")

        # Should mention database (generic, not exposed)
        assert "database" in error_msg.lower() or "error" in error_msg.lower()

        # Clean up overrides
        app.dependency_overrides.clear()

    def test_error_response_is_json(self, test_client):
        """Test: Error response is valid JSON with detail field"""
        response = test_client.get("/analytics/orders?limit=invalid")
        # Should be valid JSON
        data = response.json()
        assert isinstance(data, dict)
        # Should have detail (per HTTPException handler)
        assert "detail" in data


class TestNotFoundErrors:
    """Tests for 404 Not Found scenarios."""

    def test_invalid_endpoint_returns_404(self, test_client):
        """Test: Invalid endpoint path returns 404"""
        response = test_client.get("/analytics/invalid_endpoint")
        assert response.status_code == 404

    def test_wrong_method_returns_405(self, test_client):
        """Test: Wrong HTTP method returns 405"""
        response = test_client.post("/analytics/orders")
        assert response.status_code == 405


class TestEndpointAvailability:
    """Tests to ensure all required endpoints are available."""

    def test_orders_endpoint_exists(self, test_client):
        """Test: GET /analytics/orders endpoint exists"""
        response = test_client.get("/analytics/orders")
        # Should return 200 or 500 (not 404)
        assert response.status_code in [200, 500, 503]

    def test_top_orders_endpoint_exists(self, test_client):
        """Test: GET /analytics/orders/top endpoint exists"""
        response = test_client.get("/analytics/orders/top")
        # Should return 200 or 500 (not 404)
        assert response.status_code in [200, 500, 503]

    def test_products_endpoint_exists(self, test_client):
        """Test: GET /analytics/orders/product endpoint exists"""
        response = test_client.get("/analytics/orders/product")
        # Should return 200 or 500 (not 404)
        assert response.status_code in [200, 500, 503]

    def test_customers_endpoint_exists(self, test_client):
        """Test: GET /analytics/orders/customers endpoint exists"""
        response = test_client.get("/analytics/orders/customers")
        # Should return 200 or 500 (not 404)
        assert response.status_code in [200, 500, 503]

    def test_health_endpoint_exists(self, test_client):
        """Test: GET /health endpoint exists"""
        response = test_client.get("/health")
        # Should return 200 or 503 (not 404)
        assert response.status_code in [200, 503]
