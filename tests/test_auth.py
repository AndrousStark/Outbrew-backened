"""
Authentication Endpoint Tests

Tests for login, registration, and password management.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.candidate import Candidate


class TestRegistration:
    """Test user registration"""

    def test_register_success(self, client: TestClient, db_session: Session):
        """Test successful user registration"""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "newuser",
                "email": "newuser@example.com",
                "password": "securepassword123",
                "full_name": "New User",
                "email_account": "newuser@example.com",
                "email_password": "app_password_123"
            }
        )

        assert response.status_code == 201
        data = response.json()

        assert data["username"] == "newuser"
        assert data["email"] == "newuser@example.com"
        assert data["full_name"] == "New User"
        assert "id" in data

    def test_register_duplicate_username(self, client: TestClient, test_user: Candidate):
        """Test registration with existing username fails"""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "testuser",  # Already exists
                "email": "another@example.com",
                "password": "password123",
                "full_name": "Another User",
                "email_account": "another@example.com",
                "email_password": "app_password_123"
            }
        )

        assert response.status_code == 400
        assert "already registered" in response.json()["detail"].lower()

    def test_register_duplicate_email(self, client: TestClient, test_user: Candidate):
        """Test registration with existing email fails"""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "anotheruser",
                "email": "test@example.com",  # Already exists
                "password": "password123",
                "full_name": "Another User",
                "email_account": "anotheruser@example.com",
                "email_password": "app_password_123"
            }
        )

        assert response.status_code == 400
        assert "already registered" in response.json()["detail"].lower()

    def test_register_invalid_email(self, client: TestClient):
        """Test registration with invalid email format"""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "newuser",
                "email": "invalid-email",  # Invalid format
                "password": "password123",
                "full_name": "New User",
                "email_account": "newuser@example.com",
                "email_password": "app_password_123"
            }
        )

        assert response.status_code == 422  # Validation error


class TestLogin:
    """Test user login"""

    def test_login_success_json(self, client: TestClient, test_user: Candidate):
        """Test successful JSON login"""
        response = client.post(
            "/api/v1/auth/login/json",
            json={
                "username": "testuser",
                "password": "testpassword123"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert len(data["access_token"]) > 0

    def test_login_success_form(self, client: TestClient, test_user: Candidate):
        """Test successful OAuth2 form login"""
        response = client.post(
            "/api/v1/auth/login",
            data={
                "username": "testuser",
                "password": "testpassword123"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert "access_token" in data
        assert data["token_type"] == "bearer"

    def test_login_wrong_password(self, client: TestClient, test_user: Candidate):
        """Test login with wrong password"""
        response = client.post(
            "/api/v1/auth/login/json",
            json={
                "username": "testuser",
                "password": "wrongpassword"
            }
        )

        assert response.status_code == 401
        assert "incorrect" in response.json()["detail"].lower()

    def test_login_nonexistent_user(self, client: TestClient):
        """Test login with non-existent user"""
        response = client.post(
            "/api/v1/auth/login/json",
            json={
                "username": "nonexistent",
                "password": "password123"
            }
        )

        assert response.status_code == 401


class TestCurrentUser:
    """Test current user endpoint"""

    def test_get_current_user(self, client: TestClient, auth_headers: dict, test_user: Candidate):
        """Test getting current user info"""
        response = client.get("/api/v1/auth/me", headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert data["username"] == "testuser"
        assert data["email"] == "test@example.com"

    def test_get_current_user_unauthorized(self, client: TestClient):
        """Test accessing current user without auth"""
        response = client.get("/api/v1/auth/me")

        assert response.status_code == 401


class TestChangePassword:
    """Test password change functionality"""

    def test_change_password_success(
        self, client: TestClient, auth_headers: dict, test_user: Candidate
    ):
        """Test successful password change"""
        response = client.post(
            "/api/v1/auth/change-password",
            headers=auth_headers,
            json={
                "current_password": "testpassword123",
                "new_password": "newpassword456"
            }
        )

        assert response.status_code == 200

        # Verify new password works
        login_response = client.post(
            "/api/v1/auth/login/json",
            json={
                "username": "testuser",
                "password": "newpassword456"
            }
        )
        assert login_response.status_code == 200

    def test_change_password_wrong_current(
        self, client: TestClient, auth_headers: dict, test_user: Candidate
    ):
        """Test password change with wrong current password"""
        response = client.post(
            "/api/v1/auth/change-password",
            headers=auth_headers,
            json={
                "current_password": "wrongpassword",
                "new_password": "newpassword456"
            }
        )

        assert response.status_code == 400
        assert "incorrect" in response.json()["detail"].lower()

    def test_change_password_unauthorized(self, client: TestClient):
        """Test password change without authentication"""
        response = client.post(
            "/api/v1/auth/change-password",
            json={
                "current_password": "testpassword123",
                "new_password": "newpassword456"
            }
        )

        assert response.status_code == 401
