"""
Health Check Endpoint Tests

Tests for application health and root endpoints.
"""

import pytest
from fastapi.testclient import TestClient


class TestHealthEndpoints:
    """Test health check endpoints"""

    def test_root_endpoint(self, client: TestClient):
        """Test root endpoint returns app info"""
        response = client.get("/")

        assert response.status_code == 200
        data = response.json()

        assert "name" in data
        assert "version" in data
        assert "status" in data
        assert data["status"] == "operational"
        assert "docs" in data

    def test_health_endpoint(self, client: TestClient):
        """Test health check endpoint"""
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()

        assert data["status"] == "healthy"
        assert "environment" in data

    def test_docs_endpoint_accessible(self, client: TestClient):
        """Test that API docs are accessible"""
        response = client.get("/api/docs")

        # Should return HTML or redirect
        assert response.status_code in [200, 307]

    def test_openapi_schema(self, client: TestClient):
        """Test OpenAPI schema is accessible"""
        response = client.get("/api/openapi.json")

        assert response.status_code == 200
        data = response.json()

        assert "openapi" in data
        assert "info" in data
        assert "paths" in data
