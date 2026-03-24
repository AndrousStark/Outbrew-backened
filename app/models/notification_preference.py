"""
Notification Preference Model

Stores user preferences for:
- Which notification types to receive
- In-app vs email delivery
- Quiet hours/do-not-disturb settings
- Notification channels (push, email, in-app)
"""

from datetime import time
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Boolean,
    Time,
    JSON,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class NotificationPreference(Base):
    """User notification preferences."""

    __tablename__ = "notification_preferences"

    id = Column(Integer, primary_key=True, index=True)

    # User reference
    candidate_id = Column(
        Integer,
        ForeignKey("candidates.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    candidate = relationship("Candidate", lazy="select")

    # General settings
    notifications_enabled = Column(Boolean, default=True)
    email_notifications_enabled = Column(Boolean, default=True)
    push_notifications_enabled = Column(Boolean, default=True)

    # Notification type preferences (enabled/disabled for each type)
    # Stored as JSON for flexibility: { "campaign_created": true, "email_replied": true, ... }
    enabled_types = Column(
        JSON,
        default={
            "campaign_created": True,
            "campaign_sending": True,
            "campaign_completed": True,
            "campaign_failed": True,
            "campaign_paused": True,
            "reply_detected": True,
            "email_opened": True,
            "email_replied": True,
            "application_update": False,
            "warming_alert": True,
            "rate_limit": True,
            "system": True,
        },
    )

    # Delivery preference per type (in_app, email, both, none)
    # Stored as JSON: { "reply_detected": "both", "campaign_completed": "in_app", ... }
    delivery_preferences = Column(
        JSON,
        default={
            "campaign_created": "in_app",
            "campaign_sending": "in_app",
            "campaign_completed": "both",
            "campaign_failed": "both",
            "campaign_paused": "in_app",
            "reply_detected": "both",
            "email_opened": "in_app",
            "email_replied": "both",
            "application_update": "email",
            "warming_alert": "both",
            "rate_limit": "email",
            "system": "both",
        },
    )

    # Quiet hours
    quiet_hours_enabled = Column(Boolean, default=False)
    quiet_start_time = Column(Time, default=time(22, 0))  # 10 PM
    quiet_end_time = Column(Time, default=time(8, 0))  # 8 AM

    # Do not disturb mode
    dnd_enabled = Column(Boolean, default=False)
    dnd_until = Column(
        DateTime(timezone=True), nullable=True
    )  # Temp DND until this time

    # Email digest settings
    digest_enabled = Column(Boolean, default=False)
    digest_frequency = Column(String(20), default="daily")  # daily, weekly, never

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self):
        return f"<NotificationPreference {self.candidate_id}>"

    def is_type_enabled(self, notification_type: str) -> bool:
        """Check if notification type is enabled."""
        return self.enabled_types.get(notification_type, True)

    def get_delivery_method(self, notification_type: str) -> str:
        """Get delivery method for notification type."""
        return self.delivery_preferences.get(notification_type, "in_app")

    def should_send(self, notification_type: str) -> bool:
        """Determine if notification should be sent based on preferences."""
        if not self.notifications_enabled:
            return False

        if not self.is_type_enabled(notification_type):
            return False

        delivery = self.get_delivery_method(notification_type)
        if delivery == "none":
            return False

        return True

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "id": self.id,
            "notifications_enabled": self.notifications_enabled,
            "email_notifications_enabled": self.email_notifications_enabled,
            "push_notifications_enabled": self.push_notifications_enabled,
            "enabled_types": self.enabled_types,
            "delivery_preferences": self.delivery_preferences,
            "quiet_hours_enabled": self.quiet_hours_enabled,
            "quiet_start_time": self.quiet_start_time.isoformat()
            if self.quiet_start_time
            else None,
            "quiet_end_time": self.quiet_end_time.isoformat()
            if self.quiet_end_time
            else None,
            "dnd_enabled": self.dnd_enabled,
            "dnd_until": self.dnd_until.isoformat() if self.dnd_until else None,
            "digest_enabled": self.digest_enabled,
            "digest_frequency": self.digest_frequency,
        }
