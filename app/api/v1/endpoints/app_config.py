"""
Application Configuration Endpoint

Provides public (non-sensitive) configuration values to the frontend,
eliminating the need for hardcoded constants on the client side.
"""
from fastapi import APIRouter

from app.core.config import settings
from app.schemas.app_config import PublicAppConfig, HealthScoreThresholds

router = APIRouter()


@router.get("/public", response_model=PublicAppConfig)
def get_public_config():
    """
    Get public application configuration.

    Returns non-sensitive configuration values that the frontend
    needs for validation, UI constraints, and feature settings.

    No authentication required.
    """
    return PublicAppConfig(
        max_file_size_mb=settings.MAX_FILE_SIZE_MB,
        max_storage_quota_mb=settings.MAX_STORAGE_QUOTA_MB,
        max_page_size=settings.MAX_PAGE_SIZE,
        default_page_size=settings.DEFAULT_PAGE_SIZE,
        allowed_resume_extensions=settings.ALLOWED_RESUME_EXTENSIONS,
        allowed_attachment_extensions=settings.ALLOWED_ATTACHMENT_EXTENSIONS,
        health_score_thresholds=HealthScoreThresholds(
            excellent=90,
            good=75,
            fair=60,
            poor=40,
        ),
        max_daily_emails_recommended=50,
        max_followup_days=30,
        rate_limit_presets=[
            "conservative",
            "moderate",
            "aggressive",
            "gmail_free",
            "gmail_workspace",
            "outlook",
            "custom",
        ],
    )
