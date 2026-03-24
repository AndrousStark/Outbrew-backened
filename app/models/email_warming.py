"""Email Warming Models

Email warming is the process of gradually increasing email sending volume
to build sender reputation and avoid spam filters.

Best practices:
- Start with 5-10 emails per day
- Increase by 50-100% every 2-3 days
- Typical warm-up period: 2-4 weeks
- Target: 50-100+ emails per day
"""

import enum
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class WarmingStrategyEnum(str, enum.Enum):
    """Predefined warming strategies"""
    CONSERVATIVE = "conservative"  # 5→10→15→20→30→40→50 (slow, 14 days)
    MODERATE = "moderate"          # 5→10→20→35→50→75→100 (medium, 14 days)
    AGGRESSIVE = "aggressive"      # 10→20→40→70→100→150 (fast, 12 days)
    CUSTOM = "custom"              # User-defined schedule


class WarmingStatusEnum(str, enum.Enum):
    """Warming campaign status"""
    NOT_STARTED = "not_started"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


# Predefined warming schedules (day: max_emails)
WARMING_SCHEDULES = {
    WarmingStrategyEnum.CONSERVATIVE: {
        # 14 days, very safe for new accounts
        1: 5, 2: 5, 3: 10, 4: 10,
        5: 15, 6: 15, 7: 20, 8: 20,
        9: 30, 10: 30, 11: 40, 12: 40,
        13: 50, 14: 50
    },
    WarmingStrategyEnum.MODERATE: {
        # 14 days, balanced approach
        1: 5, 2: 10, 3: 10, 4: 20,
        5: 20, 6: 35, 7: 35, 8: 50,
        9: 50, 10: 75, 11: 75, 12: 100,
        13: 100, 14: 100
    },
    WarmingStrategyEnum.AGGRESSIVE: {
        # 12 days, faster for established domains
        1: 10, 2: 20, 3: 20, 4: 40,
        5: 40, 6: 70, 7: 70, 8: 100,
        9: 100, 10: 150, 11: 150, 12: 150
    }
}


class EmailWarmingConfig(Base):
    """Email warming configuration for a candidate"""
    __tablename__ = "email_warming_configs"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), unique=True, nullable=False)

    # Warming settings
    strategy = Column(String(50), default=WarmingStrategyEnum.CONSERVATIVE.value, nullable=False)
    status = Column(String(50), default=WarmingStatusEnum.NOT_STARTED.value, nullable=False)

    # Custom schedule (if strategy is CUSTOM)
    # Format: {"1": 5, "2": 10, "3": 15, ...}
    custom_schedule = Column(JSON, nullable=True)

    # Current progress
    current_day = Column(Integer, default=0, nullable=False)
    emails_sent_today = Column(Integer, default=0, nullable=False)
    total_emails_sent = Column(Integer, default=0, nullable=False)

    # Tracking
    start_date = Column(DateTime(timezone=True), nullable=True)
    completion_date = Column(DateTime(timezone=True), nullable=True)
    last_reset_date = Column(DateTime(timezone=True), nullable=True)  # Daily reset

    # Success metrics
    success_rate = Column(Float, default=0.0)  # Percentage of emails delivered
    bounce_rate = Column(Float, default=0.0)   # Percentage of bounced emails

    # Settings
    auto_progress = Column(Boolean, default=True)  # Auto-advance to next day
    pause_on_high_bounce = Column(Boolean, default=True)  # Pause if bounce rate > 5%
    bounce_threshold = Column(Float, default=2.0)  # Bounce rate % threshold for auto-pause
    daily_reset_hour = Column(Integer, default=0)  # Hour to reset daily count (0-23)

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", back_populates="warming_config")
    daily_logs = relationship("EmailWarmingDailyLog", back_populates="config", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        """String representation for debugging"""
        return (
            f"<EmailWarmingConfig(id={self.id}, candidate_id={self.candidate_id}, "
            f"strategy={self.strategy}, status={self.status}, day={self.current_day})>"
        )

    def get_daily_limit(self, day: int = None) -> int:
        """
        Get the email limit for a specific day.

        Args:
            day: Day number (1-indexed). If None, uses current_day.

        Returns:
            Maximum emails allowed for that day.
        """
        if day is None:
            day = self.current_day or 1

        # Use custom schedule if strategy is CUSTOM
        if self.strategy == WarmingStrategyEnum.CUSTOM.value and self.custom_schedule:
            return self.custom_schedule.get(str(day), 5)

        # Use predefined schedule
        strategy_enum = WarmingStrategyEnum(self.strategy)
        schedule = WARMING_SCHEDULES.get(strategy_enum, {})

        # If day exceeds schedule, return the last day's limit
        if day in schedule:
            return schedule[day]
        elif schedule:
            return max(schedule.values())
        return 5  # Default fallback

    @property
    def is_active(self) -> bool:
        """Check if warming is currently active"""
        return self.status == WarmingStatusEnum.ACTIVE.value

    @property
    def is_complete(self) -> bool:
        """Check if warming is completed"""
        return self.status == WarmingStatusEnum.COMPLETED.value

    @property
    def can_send_today(self) -> bool:
        """Check if more emails can be sent today"""
        if not self.is_active:
            return False
        daily_limit = self.get_daily_limit()
        return self.emails_sent_today < daily_limit

    @property
    def remaining_today(self) -> int:
        """Get remaining emails that can be sent today"""
        if not self.is_active:
            return 0
        daily_limit = self.get_daily_limit()
        return max(0, daily_limit - self.emails_sent_today)

    @property
    def progress_percentage(self) -> float:
        """Get warming progress as percentage (0-100)"""
        strategy_enum = WarmingStrategyEnum(self.strategy)
        schedule = WARMING_SCHEDULES.get(strategy_enum, {})
        total_days = len(schedule) if schedule else 14
        if total_days == 0:
            return 0.0
        return min(100.0, (self.current_day / total_days) * 100)

    def to_dict(self, include_stats: bool = False) -> dict:
        """
        Convert to dictionary for API responses.

        Args:
            include_stats: Include computed statistics

        Returns:
            Dictionary representation of the config
        """
        result = {
            "id": self.id,
            "candidate_id": self.candidate_id,
            "strategy": self.strategy,
            "status": self.status,
            "current_day": self.current_day,
            "emails_sent_today": self.emails_sent_today,
            "total_emails_sent": self.total_emails_sent,
            "success_rate": round(self.success_rate, 2),
            "bounce_rate": round(self.bounce_rate, 2),
            "auto_progress": self.auto_progress,
            "pause_on_high_bounce": self.pause_on_high_bounce,
            "daily_reset_hour": self.daily_reset_hour,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "completion_date": self.completion_date.isoformat() if self.completion_date else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

        if include_stats:
            result.update({
                "daily_limit": self.get_daily_limit(),
                "remaining_today": self.remaining_today,
                "can_send_today": self.can_send_today,
                "is_active": self.is_active,
                "is_complete": self.is_complete,
                "progress_percentage": round(self.progress_percentage, 1),
            })

        return result


class EmailWarmingDailyLog(Base):
    """Daily log of email warming progress"""
    __tablename__ = "email_warming_daily_logs"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key
    config_id = Column(Integer, ForeignKey("email_warming_configs.id", ondelete="CASCADE"), nullable=False)

    # Daily stats
    day_number = Column(Integer, nullable=False)  # Day 1, 2, 3, etc.
    date = Column(DateTime(timezone=True), nullable=False)

    # Limits
    daily_limit = Column(Integer, nullable=False)  # Max allowed for this day

    # Actual performance
    emails_sent = Column(Integer, default=0)
    emails_delivered = Column(Integer, default=0)
    emails_bounced = Column(Integer, default=0)
    emails_failed = Column(Integer, default=0)

    # Calculated metrics
    delivery_rate = Column(Float, default=0.0)  # delivered / sent
    bounce_rate = Column(Float, default=0.0)    # bounced / sent

    # Status
    limit_reached = Column(Boolean, default=False)
    notes = Column(String(500), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    config = relationship("EmailWarmingConfig", back_populates="daily_logs")

    def __repr__(self) -> str:
        """String representation for debugging"""
        return (
            f"<EmailWarmingDailyLog(id={self.id}, config_id={self.config_id}, "
            f"day={self.day_number}, sent={self.emails_sent}/{self.daily_limit})>"
        )

    def recalculate_rates(self) -> None:
        """Recalculate delivery and bounce rates based on current counts"""
        if self.emails_sent > 0:
            self.delivery_rate = (self.emails_delivered / self.emails_sent) * 100
            self.bounce_rate = (self.emails_bounced / self.emails_sent) * 100
        else:
            self.delivery_rate = 0.0
            self.bounce_rate = 0.0

    @property
    def utilization_percentage(self) -> float:
        """Get how much of the daily limit was used (0-100)"""
        if self.daily_limit == 0:
            return 0.0
        return min(100.0, (self.emails_sent / self.daily_limit) * 100)

    @property
    def is_healthy(self) -> bool:
        """Check if this day had healthy metrics (bounce rate < 5%)"""
        return self.bounce_rate < 5.0

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "config_id": self.config_id,
            "day_number": self.day_number,
            "date": self.date.isoformat() if self.date else None,
            "daily_limit": self.daily_limit,
            "emails_sent": self.emails_sent,
            "emails_delivered": self.emails_delivered,
            "emails_bounced": self.emails_bounced,
            "emails_failed": self.emails_failed,
            "delivery_rate": round(self.delivery_rate, 2),
            "bounce_rate": round(self.bounce_rate, 2),
            "utilization_percentage": round(self.utilization_percentage, 1),
            "limit_reached": self.limit_reached,
            "is_healthy": self.is_healthy,
            "notes": self.notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
