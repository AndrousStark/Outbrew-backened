"""Group Campaign Recipient Model - Individual recipient tracking within campaigns"""
import enum
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Enum as SQLEnum, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class RecipientStatusEnum(str, enum.Enum):
    """Per-recipient status enumeration"""
    PENDING = "pending"  # Queued for sending
    SENT = "sent"  # Successfully sent
    FAILED = "failed"  # Failed to send
    SKIPPED = "skipped"  # Skipped (inactive, unsubscribed, etc.)
    OPENED = "opened"  # Email opened
    REPLIED = "replied"  # Recipient replied
    BOUNCED = "bounced"  # Email bounced


class GroupCampaignRecipient(Base):
    """
    GroupCampaignRecipient model tracks individual email sends within a group campaign.

    For each recipient in a campaign, this model stores:
    - Personalized rendered content (subject + body with recipient's variables)
    - Send status and timestamps
    - Email tracking (opens, replies, bounces)
    - Error messages if send failed
    - Link to email_logs for detailed tracking
    """
    __tablename__ = "group_campaign_recipients"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign Keys
    campaign_id = Column(Integer, ForeignKey("group_campaigns.id", ondelete="CASCADE"), nullable=False, index=True)
    recipient_id = Column(Integer, ForeignKey("recipients.id", ondelete="CASCADE"), nullable=False, index=True)

    # Personalized Content (rendered for this specific recipient)
    rendered_subject = Column(String(500))
    rendered_body_html = Column(Text)

    # Status Tracking
    status = Column(SQLEnum(RecipientStatusEnum), default=RecipientStatusEnum.PENDING, index=True)

    # Email Tracking
    tracking_id = Column(String(100), unique=True, index=True)  # Unique ID for tracking pixel/links

    # Timestamps
    sent_at = Column(DateTime(timezone=True))
    delivered_at = Column(DateTime(timezone=True))
    opened_at = Column(DateTime(timezone=True))
    replied_at = Column(DateTime(timezone=True))
    bounced_at = Column(DateTime(timezone=True))

    # Error Handling
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)

    # Email Log Reference (links to EmailLog for detailed tracking)
    email_log_id = Column(Integer, ForeignKey("email_logs.id", ondelete="SET NULL"))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    campaign = relationship("GroupCampaign", back_populates="campaign_recipients")
    recipient = relationship("Recipient", back_populates="campaign_sends")
    email_log = relationship("EmailLog")

    # Composite Indexes
    __table_args__ = (
        # Ensure one email per recipient per campaign
        Index('ix_campaign_recipient_unique', 'campaign_id', 'recipient_id', unique=True),
        # Query optimization for campaign status tracking
        Index('ix_campaign_recipient_status', 'campaign_id', 'status'),
        # Recipient history lookup
        Index('ix_recipient_campaign_sent', 'recipient_id', 'sent_at'),
    )

    def __repr__(self):
        return f"<GroupCampaignRecipient(campaign_id={self.campaign_id}, recipient_id={self.recipient_id}, status={self.status})>"

    @property
    def was_successful(self) -> bool:
        """Check if email was successfully sent"""
        return self.status in [RecipientStatusEnum.SENT, RecipientStatusEnum.OPENED, RecipientStatusEnum.REPLIED]

    @property
    def engagement_level(self) -> str:
        """Get engagement level description"""
        if self.status == RecipientStatusEnum.REPLIED:
            return "high"
        elif self.status == RecipientStatusEnum.OPENED:
            return "medium"
        elif self.status == RecipientStatusEnum.SENT:
            return "low"
        else:
            return "none"
