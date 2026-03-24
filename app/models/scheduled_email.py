"""
Scheduled Email Model

Stores emails that are scheduled to be sent at optimal times.
Uses the SendTimeOptimizer service to determine when to send.

Features:
- Queue emails for optimal send times
- Track scheduling metadata (industry, timezone, expected boost)
- Handle retries for failed sends
- Support cancellation
"""

import enum
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Text, Enum, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class ScheduledEmailStatus(str, enum.Enum):
    """Status of a scheduled email."""
    PENDING = "pending"       # Waiting to be sent
    PROCESSING = "processing" # Currently being processed
    SENT = "sent"            # Successfully sent
    CANCELLED = "cancelled"  # User cancelled
    FAILED = "failed"        # Failed after max retries


class ScheduledEmail(Base):
    """
    Emails scheduled to be sent at optimal times.

    When a user clicks "Send" and the time isn't optimal,
    we create a ScheduledEmail record and the scheduler
    sends it at the right time.

    Flow:
    1. User creates application and clicks "Schedule Send"
    2. SendTimeOptimizer calculates optimal time
    3. ScheduledEmail record created with scheduled_for time
    4. APScheduler job processes pending emails every 5 minutes
    5. When scheduled_for <= now, email is sent via EmailService
    """
    __tablename__ = "scheduled_emails"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys - which application and candidate
    application_id = Column(
        Integer,
        ForeignKey("applications.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    candidate_id = Column(
        Integer,
        ForeignKey("candidates.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    # Scheduling information
    scheduled_for = Column(DateTime(timezone=True), nullable=False, index=True)
    timezone = Column(String(100), default="UTC")

    # Send time optimization metadata
    industry = Column(String(50))               # Industry used for optimization
    recipient_country = Column(String(100))     # Recipient's country
    expected_boost = Column(String(50))         # Expected open rate boost
    optimization_reason = Column(Text)          # Why this time was chosen
    is_optimal_time = Column(Boolean, default=True)  # Was optimal time used?
    original_request_time = Column(DateTime(timezone=True))  # When user clicked send

    # Status tracking
    status = Column(
        Enum(ScheduledEmailStatus),
        default=ScheduledEmailStatus.PENDING,
        index=True
    )

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    sent_at = Column(DateTime(timezone=True), nullable=True)  # When actually sent
    cancelled_at = Column(DateTime(timezone=True), nullable=True)

    # Error handling
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    last_retry_at = Column(DateTime(timezone=True), nullable=True)

    # User preferences
    send_immediately_if_optimal = Column(Boolean, default=True)  # Send now if time is optimal
    user_confirmed_schedule = Column(Boolean, default=False)     # User confirmed the schedule

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Relationships
    application = relationship(
        "Application",
        backref="scheduled_emails",
        foreign_keys=[application_id]
    )
    candidate = relationship(
        "Candidate",
        backref="scheduled_emails",
        foreign_keys=[candidate_id]
    )

    # Composite indexes for common queries
    __table_args__ = (
        # For scheduler job: find pending emails due to send
        Index('ix_sched_status_scheduled', 'status', 'scheduled_for'),
        # For candidate dashboard: see their scheduled emails
        Index('ix_sched_candidate_status', 'candidate_id', 'status'),
        # For cleanup: find old completed records
        Index('ix_sched_status_sent', 'status', 'sent_at'),
    )

    def __repr__(self):
        return f"<ScheduledEmail(id={self.id}, app_id={self.application_id}, status={self.status}, scheduled_for={self.scheduled_for})>"

    @property
    def is_due(self) -> bool:
        """Check if this email is due to be sent."""
        from datetime import datetime
        import pytz
        now = datetime.now(pytz.UTC)
        return (
            self.status == ScheduledEmailStatus.PENDING and
            self.scheduled_for <= now
        )

    @property
    def can_cancel(self) -> bool:
        """Check if this scheduled email can be cancelled."""
        return self.status == ScheduledEmailStatus.PENDING

    @property
    def can_retry(self) -> bool:
        """Check if this email can be retried."""
        return (
            self.status == ScheduledEmailStatus.FAILED and
            self.retry_count < self.max_retries
        )

    def mark_sent(self):
        """Mark this email as sent."""
        from datetime import datetime
        import pytz
        self.status = ScheduledEmailStatus.SENT
        self.sent_at = datetime.now(pytz.UTC)

    def mark_failed(self, error_message: str):
        """Mark this email as failed."""
        from datetime import datetime
        import pytz
        self.error_message = error_message
        self.retry_count += 1
        self.last_retry_at = datetime.now(pytz.UTC)

        if self.retry_count >= self.max_retries:
            self.status = ScheduledEmailStatus.FAILED
        else:
            self.status = ScheduledEmailStatus.PENDING

    def cancel(self):
        """Cancel this scheduled email."""
        from datetime import datetime
        import pytz
        if self.can_cancel:
            self.status = ScheduledEmailStatus.CANCELLED
            self.cancelled_at = datetime.now(pytz.UTC)
            return True
        return False


class SendTimePreference(Base):
    """
    User preferences for send time optimization.

    Stores candidate-specific preferences for email scheduling,
    allowing customization of the default optimization behavior.
    """
    __tablename__ = "send_time_preferences"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key - one preference per candidate
    candidate_id = Column(
        Integer,
        ForeignKey("candidates.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True
    )

    # Default settings
    default_industry = Column(String(50), default="default")
    default_timezone = Column(String(100), default="Asia/Kolkata")
    auto_schedule_enabled = Column(Boolean, default=True)

    # Custom schedule (if user wants to override)
    use_custom_schedule = Column(Boolean, default=False)
    custom_days = Column(String(50))       # JSON list: "[1, 2, 3]"
    custom_hours = Column(String(50))      # JSON list: "[10, 11, 14]"

    # Preferences
    tolerance_hours = Column(Integer, default=2)  # How far from optimal is acceptable
    prefer_morning = Column(Boolean, default=True)
    prefer_afternoon = Column(Boolean, default=False)
    avoid_mondays = Column(Boolean, default=True)
    avoid_fridays = Column(Boolean, default=True)

    # Stats
    total_scheduled = Column(Integer, default=0)
    total_sent_optimal = Column(Integer, default=0)
    average_boost_achieved = Column(String(20))  # e.g., "+25%"

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationship
    candidate = relationship("Candidate", backref="send_time_preference")

    def __repr__(self):
        return f"<SendTimePreference(candidate_id={self.candidate_id}, industry={self.default_industry})>"

    def get_custom_days(self) -> list:
        """Parse custom days from JSON string."""
        import json
        if self.custom_days:
            try:
                return json.loads(self.custom_days)
            except Exception:
                pass
        return [1, 2, 3]  # Default: Tue, Wed, Thu

    def get_custom_hours(self) -> list:
        """Parse custom hours from JSON string."""
        import json
        if self.custom_hours:
            try:
                return json.loads(self.custom_hours)
            except Exception:
                pass
        return [10, 11, 14]  # Default: 10am, 11am, 2pm

    def set_custom_days(self, days: list):
        """Set custom days as JSON string."""
        import json
        self.custom_days = json.dumps(days)

    def set_custom_hours(self, hours: list):
        """Set custom hours as JSON string."""
        import json
        self.custom_hours = json.dumps(hours)
