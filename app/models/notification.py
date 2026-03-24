"""
Notification Model

Stores user notifications with support for:
- Different notification types
- Read/unread status
- Action URLs
- Expiration
"""

from datetime import datetime, timedelta, timezone
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Boolean,
    Enum,
    ForeignKey,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
import logging

from app.core.database import Base

# Setup logger for debugging
logger = logging.getLogger(__name__)


class NotificationType(str, enum.Enum):
    """Types of notifications."""

    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    EMAIL_SENT = "email_sent"
    EMAIL_OPENED = "email_opened"
    EMAIL_REPLIED = "email_replied"
    APPLICATION_UPDATE = "application_update"
    WARMING_ALERT = "warming_alert"
    RATE_LIMIT = "rate_limit"
    SYSTEM = "system"
    CAMPAIGN_CREATED = "campaign_created"
    CAMPAIGN_SENDING = "campaign_sending"
    CAMPAIGN_COMPLETED = "campaign_completed"
    CAMPAIGN_FAILED = "campaign_failed"
    CAMPAIGN_PAUSED = "campaign_paused"
    REPLY_DETECTED = "reply_detected"


class Notification(Base):
    """Notification model for storing user notifications."""

    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True)

    # Notification content
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False)
    notification_type = Column(
        String(50), default=NotificationType.INFO.value, nullable=False
    )

    # Related entities (optional) - with proper ForeignKey constraints
    application_id = Column(
        Integer,
        ForeignKey("applications.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    company_id = Column(
        Integer,
        ForeignKey("companies.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    candidate_id = Column(
        Integer,
        ForeignKey("candidates.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    group_campaign_id = Column(
        Integer,
        ForeignKey("group_campaigns.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Relationships for easier querying
    application = relationship(
        "Application", foreign_keys=[application_id], lazy="select"
    )
    company = relationship("Company", foreign_keys=[company_id], lazy="select")
    candidate = relationship("Candidate", foreign_keys=[candidate_id], lazy="select")
    group_campaign = relationship(
        "GroupCampaign", foreign_keys=[group_campaign_id], lazy="select"
    )

    # Action/navigation
    action_url = Column(String(500), nullable=True)
    action_text = Column(String(100), nullable=True)

    # Status
    is_read = Column(Boolean, default=False, index=True)
    is_archived = Column(Boolean, default=False, index=True)

    # Metadata
    icon = Column(String(50), nullable=True)  # Icon name for frontend
    priority = Column(Integer, default=0)  # Higher = more important

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    read_at = Column(DateTime(timezone=True), nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self):
        return f"<Notification {self.id}: {self.title[:30]}>"

    @property
    def is_expired(self) -> bool:
        """Check if notification has expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    def mark_as_read(self):
        """Mark notification as read."""
        logger.debug(f"[Notification] Marking notification {self.id} as read")
        self.is_read = True
        self.read_at = datetime.now(timezone.utc)
        logger.info(
            f"[Notification] Notification {self.id} marked as read at {self.read_at}"
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "id": self.id,
            "title": self.title,
            "message": self.message,
            "type": self.notification_type,
            "application_id": self.application_id,
            "company_id": self.company_id,
            "action_url": self.action_url,
            "action_text": self.action_text,
            "is_read": self.is_read,
            "is_archived": self.is_archived,
            "icon": self.icon,
            "priority": self.priority,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "read_at": self.read_at.isoformat() if self.read_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "is_expired": self.is_expired,
        }
