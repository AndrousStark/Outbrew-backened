"""
Application Config Schemas

Public configuration response schema for frontend consumption.
"""
from pydantic import BaseModel
from typing import Dict, List


class HealthScoreThresholds(BaseModel):
    """Thresholds for health score categorization."""
    excellent: int = 90
    good: int = 75
    fair: int = 60
    poor: int = 40


class PublicAppConfig(BaseModel):
    """
    Public application configuration.

    Contains non-sensitive settings that the frontend needs
    to avoid hardcoding values.
    """
    max_file_size_mb: int
    max_storage_quota_mb: int
    max_page_size: int
    default_page_size: int
    allowed_resume_extensions: List[str]
    allowed_attachment_extensions: List[str]
    health_score_thresholds: HealthScoreThresholds
    max_daily_emails_recommended: int
    max_followup_days: int
    rate_limit_presets: List[str]
