"""
Applications Endpoint Tests

Tests for job application CRUD operations and related functionality.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.application import Application, ApplicationStatusEnum
from app.models.candidate import Candidate
from app.models.company import Company


@pytest.fixture
def test_company(db_session: Session) -> Company:
    """Create a test company"""
    company = Company(
        name="Test Company Inc.",
        domain="testcompany.com",
        industry="Technology",
        headquarters_country="USA"
    )
    db_session.add(company)
    db_session.commit()
    db_session.refresh(company)
    return company


@pytest.fixture
def test_application(
    db_session: Session, test_user: Candidate, test_company: Company
) -> Application:
    """Create a test application"""
    application = Application(
        candidate_id=test_user.id,
        company_id=test_company.id,
        recruiter_email="recruiter@testcompany.com",
        recruiter_name="John Doe",
        position_title="Software Engineer",
        status=ApplicationStatusEnum.DRAFT
    )
    db_session.add(application)
    db_session.commit()
    db_session.refresh(application)
    return application


class TestListApplications:
    """Test listing applications"""

    def test_list_applications_empty(self, client: TestClient, auth_headers: dict):
        """Test listing when no applications exist"""
        response = client.get("/api/v1/applications", headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert "items" in data
        assert isinstance(data["items"], list)

    def test_list_applications_with_data(
        self, client: TestClient, auth_headers: dict, test_application: Application
    ):
        """Test listing applications with existing data"""
        response = client.get("/api/v1/applications", headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert "items" in data
        assert len(data["items"]) >= 1

    def test_list_applications_pagination(
        self, client: TestClient, auth_headers: dict, test_application: Application
    ):
        """Test applications pagination"""
        response = client.get(
            "/api/v1/applications",
            params={"page": 1, "limit": 10},
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        assert "items" in data
        assert "total" in data
        assert "page" in data

    def test_list_applications_unauthorized(self, client: TestClient):
        """Test listing without authentication"""
        response = client.get("/api/v1/applications")

        assert response.status_code == 401


class TestGetApplication:
    """Test getting single application"""

    def test_get_application_success(
        self, client: TestClient, auth_headers: dict, test_application: Application
    ):
        """Test getting a specific application"""
        response = client.get(
            f"/api/v1/applications/{test_application.id}",
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        assert data["id"] == test_application.id
        assert data["company_name"] == "Test Company Inc."
        assert data["position_title"] == "Software Engineer"

    def test_get_application_not_found(self, client: TestClient, auth_headers: dict):
        """Test getting non-existent application"""
        response = client.get("/api/v1/applications/99999", headers=auth_headers)

        assert response.status_code == 404


class TestCreateApplication:
    """Test creating applications"""

    def test_create_application_success(
        self, client: TestClient, auth_headers: dict, test_company: Company
    ):
        """Test creating a new application"""
        response = client.post(
            "/api/v1/applications",
            headers=auth_headers,
            json={
                "company_name": "New Company",
                "recruiter_email": "hr@newcompany.com",
                "recruiter_name": "Jane Smith",
                "position_title": "Backend Developer"
            }
        )

        assert response.status_code == 201
        data = response.json()

        assert data["company_name"] == "New Company"
        assert data["recruiter_email"] == "hr@newcompany.com"
        assert data["status"] == "draft"

    def test_create_application_minimal(self, client: TestClient, auth_headers: dict):
        """Test creating application with minimal data"""
        response = client.post(
            "/api/v1/applications",
            headers=auth_headers,
            json={
                "company_name": "Minimal Company",
                "recruiter_email": "contact@minimal.com"
            }
        )

        assert response.status_code == 201

    def test_create_application_invalid_email(self, client: TestClient, auth_headers: dict):
        """Test creating application with invalid email"""
        response = client.post(
            "/api/v1/applications",
            headers=auth_headers,
            json={
                "company_name": "Test Company",
                "recruiter_email": "invalid-email"
            }
        )

        assert response.status_code == 422


class TestUpdateApplication:
    """Test updating applications"""

    def test_update_application_success(
        self, client: TestClient, auth_headers: dict, test_application: Application
    ):
        """Test updating an application"""
        response = client.patch(
            f"/api/v1/applications/{test_application.id}",
            headers=auth_headers,
            json={
                "position_title": "Senior Software Engineer",
                "notes": "Updated position"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert data["position_title"] == "Senior Software Engineer"

    def test_update_application_status(
        self, client: TestClient, auth_headers: dict, test_application: Application
    ):
        """Test updating application status"""
        response = client.patch(
            f"/api/v1/applications/{test_application.id}/status",
            headers=auth_headers,
            json={"status": "sent"}
        )

        assert response.status_code == 200
        data = response.json()

        assert data["status"] == "sent"


class TestDeleteApplication:
    """Test deleting applications"""

    def test_delete_application_success(
        self, client: TestClient, auth_headers: dict, test_application: Application
    ):
        """Test soft deleting an application"""
        response = client.delete(
            f"/api/v1/applications/{test_application.id}",
            headers=auth_headers
        )

        assert response.status_code == 200

        # Verify it's not returned in list
        list_response = client.get("/api/v1/applications", headers=auth_headers)
        app_ids = [app["id"] for app in list_response.json()["items"]]
        assert test_application.id not in app_ids

    def test_delete_application_not_found(self, client: TestClient, auth_headers: dict):
        """Test deleting non-existent application"""
        response = client.delete("/api/v1/applications/99999", headers=auth_headers)

        assert response.status_code == 404


class TestApplicationStats:
    """Test application statistics"""

    def test_get_stats(
        self, client: TestClient, auth_headers: dict, test_application: Application
    ):
        """Test getting application statistics"""
        response = client.get(
            "/api/v1/applications/stats/summary",
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        assert "total" in data
        assert "by_status" in data
