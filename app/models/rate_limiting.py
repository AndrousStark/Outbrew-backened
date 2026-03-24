"""Rate Limiting Models

Rate limiting controls the maximum number of emails sent per time period
to avoid hitting provider limits and maintain sender reputation.

Common limits:
- Gmail Free: 500/day, 100/hour
- Gmail Workspace: 2000/day, 500/hour
- Outlook: 300/day, 100/hour
- Custom SMTP: Varies by provider
"""

import enum
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class RateLimitPresetEnum(str, enum.Enum):
    """Predefined rate limit presets"""
    CONSERVATIVE = "conservative"  # 50/day, safe for warming
    MODERATE = "moderate"          # 100/day, balanced
    AGGRESSIVE = "aggressive"      # 200/day, established accounts
    GMAIL_FREE = "gmail_free"      # 500/day (Gmail limit)
    GMAIL_WORKSPACE = "gmail_workspace"  # 2000/day
    OUTLOOK = "outlook"            # 300/day
    CUSTOM = "custom"              # User-defined


class RateLimitPeriodEnum(str, enum.Enum):
    """Time period for rate limiting"""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


# Predefined rate limit presets
RATE_LIMIT_PRESETS = {
    RateLimitPresetEnum.CONSERVATIVE: {
        "daily_limit": 50,
        "hourly_limit": 15,
        "description": "Safe for new accounts during warming",
        "recommended_for": "New email accounts (0-2 weeks)"
    },
    RateLimitPresetEnum.MODERATE: {
        "daily_limit": 100,
        "hourly_limit": 25,
        "description": "Balanced sending for established accounts",
        "recommended_for": "Warmed accounts (2-4 weeks)"
    },
    RateLimitPresetEnum.AGGRESSIVE: {
        "daily_limit": 200,
        "hourly_limit": 50,
        "description": "High volume for fully warmed accounts",
        "recommended_for": "Fully warmed accounts (4+ weeks)"
    },
    RateLimitPresetEnum.GMAIL_FREE: {
        "daily_limit": 500,
        "hourly_limit": 100,
        "description": "Gmail free account limits",
        "recommended_for": "Gmail free accounts"
    },
    RateLimitPresetEnum.GMAIL_WORKSPACE: {
        "daily_limit": 2000,
        "hourly_limit": 500,
        "description": "Gmail Workspace (paid) limits",
        "recommended_for": "Google Workspace accounts"
    },
    RateLimitPresetEnum.OUTLOOK: {
        "daily_limit": 300,
        "hourly_limit": 100,
        "description": "Outlook/Hotmail limits",
        "recommended_for": "Outlook.com accounts"
    }
}


class RateLimitConfig(Base):
    """Rate limiting configuration for a candidate"""
    __tablename__ = "rate_limit_configs"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), unique=True, nullable=False)

    # Rate limit settings
    preset = Column(String(50), default=RateLimitPresetEnum.MODERATE.value, nullable=False)

    # Custom limits (if preset is CUSTOM)
    daily_limit = Column(Integer, default=100, nullable=False)
    hourly_limit = Column(Integer, default=25, nullable=False)
    weekly_limit = Column(Integer, nullable=True)    # Optional
    monthly_limit = Column(Integer, nullable=True)   # Optional

    # Current usage tracking
    emails_sent_today = Column(Integer, default=0, nullable=False)
    emails_sent_this_hour = Column(Integer, default=0, nullable=False)
    emails_sent_this_week = Column(Integer, default=0, nullable=False)
    emails_sent_this_month = Column(Integer, default=0, nullable=False)

    # Last reset timestamps
    last_hourly_reset = Column(DateTime(timezone=True), nullable=True)
    last_daily_reset = Column(DateTime(timezone=True), nullable=True)
    last_weekly_reset = Column(DateTime(timezone=True), nullable=True)
    last_monthly_reset = Column(DateTime(timezone=True), nullable=True)

    # Behavior settings
    enabled = Column(Boolean, default=True, nullable=False)
    pause_on_limit = Column(Boolean, default=True)  # Pause sending when limit hit
    notify_on_limit = Column(Boolean, default=True)  # Notify user when limit hit
    auto_reset = Column(Boolean, default=True)       # Auto-reset counters daily

    # Warning thresholds (percentage of limit)
    warning_threshold_daily = Column(Integer, default=80)   # Warn at 80% of daily limit
    warning_threshold_hourly = Column(Integer, default=80)  # Warn at 80% of hourly limit

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", back_populates="rate_limit_config")
    usage_logs = relationship("RateLimitUsageLog", back_populates="config", cascade="all, delete-orphan")


class RateLimitUsageLog(Base):
    """Log of rate limit usage over time"""
    __tablename__ = "rate_limit_usage_logs"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key
    config_id = Column(Integer, ForeignKey("rate_limit_configs.id", ondelete="CASCADE"), nullable=False)

    # Usage data
    period_type = Column(String(20), nullable=False)  # hourly, daily, weekly, monthly
    period_start = Column(DateTime(timezone=True), nullable=False)
    period_end = Column(DateTime(timezone=True), nullable=False)

    # Stats
    limit_value = Column(Integer, nullable=False)  # What was the limit?
    emails_sent = Column(Integer, default=0)       # How many were sent?
    limit_reached = Column(Boolean, default=False)  # Did we hit the limit?
    limit_exceeded = Column(Boolean, default=False) # Did we exceed it?

    # Calculated
    usage_percentage = Column(Integer, default=0)  # (sent / limit) * 100

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    config = relationship("RateLimitConfig", back_populates="usage_logs")
