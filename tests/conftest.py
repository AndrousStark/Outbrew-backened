"""
Pytest Configuration and Fixtures

Provides test database, client, and common fixtures for all tests.
"""

import os
import pytest
import asyncio
from typing import Generator, AsyncGenerator
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

# Set test environment variables BEFORE importing app modules
os.environ.setdefault("ENCRYPTION_KEY", "test-encryption-key-for-pytest-only")

from app.main import app
from app.core.database import Base, get_db, get_database_session
from app.core.auth import get_password_hash
from app.core.rate_limiter import limiter
from app.models.candidate import Candidate, UserRole


# Disable rate limiting in tests
limiter.enabled = False

# Test database URL (in-memory SQLite)
TEST_DATABASE_URL = "sqlite:///:memory:"

# Create test engine with StaticPool for thread safety
test_engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

# Test session factory
TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


def override_get_db() -> Generator[Session, None, None]:
    """Override database dependency for testing"""
    db = TestSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
def db_session() -> Generator[Session, None, None]:
    """
    Create a fresh database session for each test.
    Creates all tables before test and drops after.
    """
    # Create all tables
    Base.metadata.create_all(bind=test_engine)

    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.close()
        # Drop all tables after test
        Base.metadata.drop_all(bind=test_engine)


@pytest.fixture(scope="function")
def client(db_session: Session) -> Generator[TestClient, None, None]:
    """
    Create test client with overridden database dependency.
    """
    # Override both get_db and get_database_session since endpoints use either
    app.dependency_overrides[get_db] = lambda: db_session
    app.dependency_overrides[get_database_session] = lambda: db_session

    with TestClient(app) as test_client:
        yield test_client

    # Clear overrides after test
    app.dependency_overrides.clear()


@pytest.fixture(scope="function")
async def async_client(db_session: Session) -> AsyncGenerator[AsyncClient, None]:
    """
    Create async test client for async endpoint testing.
    """
    app.dependency_overrides[get_db] = lambda: db_session
    app.dependency_overrides[get_database_session] = lambda: db_session

    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
def test_user(db_session: Session) -> Candidate:
    """
    Create a test user for authentication tests.
    """
    user = Candidate(
        username="testuser",
        email="test@example.com",
        hashed_password=get_password_hash("testpassword123"),
        full_name="Test User",
        role=UserRole.PRAGYA,
        email_account="test@example.com",
        email_password="encrypted_test_password",
        is_active=True
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def test_admin(db_session: Session) -> Candidate:
    """
    Create a test admin user.
    """
    admin = Candidate(
        username="admin",
        email="admin@example.com",
        hashed_password=get_password_hash("adminpassword123"),
        full_name="Admin User",
        role=UserRole.SUPER_ADMIN,
        email_account="admin@example.com",
        email_password="encrypted_admin_password",
        is_active=True
    )
    db_session.add(admin)
    db_session.commit()
    db_session.refresh(admin)
    return admin


@pytest.fixture
def auth_headers(client: TestClient, test_user: Candidate) -> dict:
    """
    Get authentication headers for protected endpoints.
    """
    response = client.post(
        "/api/v1/auth/login/json",
        json={"username": "testuser", "password": "testpassword123"}
    )
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def admin_auth_headers(client: TestClient, test_admin: Candidate) -> dict:
    """
    Get admin authentication headers.
    """
    response = client.post(
        "/api/v1/auth/login/json",
        json={"username": "admin", "password": "adminpassword123"}
    )
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}
