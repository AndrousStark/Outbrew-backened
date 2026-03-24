"""Group Campaign Model - Email campaigns sent to recipient groups"""
import enum
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Enum as SQLEnum, Index, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class CampaignStatusEnum(str, enum.Enum):
    """Campaign status enumeration"""
    DRAFT = "draft"  # Created but not sent
    SCHEDULED = "scheduled"  # Scheduled for future send
    SENDING = "sending"  # Currently being sent
    COMPLETED = "completed"  # Successfully completed
    FAILED = "failed"  # Failed to send
    PAUSED = "paused"  # Paused mid-send
    CANCELLED = "cancelled"  # Cancelled by user


class GroupCampaign(Base):
    """
    GroupCampaign model represents an email campaign sent to a recipient group.

    Each campaign:
    - Is linked to a specific group
    - Uses a template (or custom content)
    - Tracks send progress in real-time
    - Provides per-recipient personalization
    - Can be scheduled, paused, or resumed
    """
    __tablename__ = "group_campaigns"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Ownership (multi-tenant isolation)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Group Link (nullable to preserve campaign if group deleted)
    group_id = Column(Integer, ForeignKey("recipient_groups.id", ondelete="SET NULL"), index=True)

    # Campaign Details
    campaign_name = Column(String(255), nullable=False, index=True)

    # Template Reference (optional, nullable if custom content)
    email_template_id = Column(Integer, ForeignKey("email_templates.id", ondelete="SET NULL"))

    # Email Content (snapshot at send time)
    subject_template = Column(String(500), nullable=False)
    body_template_html = Column(Text, nullable=False)

    # Send Configuration
    send_delay_seconds = Column(Integer, default=60)  # Delay between each email send

    # Scheduling
    scheduled_at = Column(DateTime(timezone=True))  # NULL = send immediately

    # Follow-Up Configuration
    enable_follow_up = Column(Boolean, default=False)
    follow_up_sequence_id = Column(Integer, ForeignKey("follow_up_sequences.id", ondelete="SET NULL"), nullable=True)
    follow_up_stop_on_reply = Column(Boolean, default=True)
    follow_up_stop_on_bounce = Column(Boolean, default=False)

    # Status
    status = Column(SQLEnum(CampaignStatusEnum), default=CampaignStatusEnum.DRAFT, index=True)

    # Progress Tracking
    total_recipients = Column(Integer, default=0)
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    skipped_count = Column(Integer, default=0)
    opened_count = Column(Integer, default=0)
    replied_count = Column(Integer, default=0)
    bounced_count = Column(Integer, default=0)

    # Timing
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    paused_at = Column(DateTime(timezone=True))

    # Error Tracking
    error_message = Column(Text)

    # Soft Delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", back_populates="group_campaigns")
    group = relationship("RecipientGroup", back_populates="campaigns")
    email_template = relationship("EmailTemplate")
    follow_up_sequence = relationship("FollowUpSequence", foreign_keys=[follow_up_sequence_id])
    campaign_recipients = relationship(
        "GroupCampaignRecipient",
        back_populates="campaign",
        cascade="all, delete-orphan"
    )

    # Composite Indexes
    __table_args__ = (
        # Query optimization
        Index('ix_campaign_candidate_status', 'candidate_id', 'status', 'deleted_at'),
        Index('ix_campaign_group_created', 'group_id', 'created_at'),
        Index('ix_campaign_scheduled', 'scheduled_at', 'status'),
    )

    def __repr__(self):
        return f"<GroupCampaign(id={self.id}, name={self.campaign_name}, status={self.status}, sent={self.sent_count}/{self.total_recipients})>"

    @property
    def success_rate(self) -> float:
        """Calculate success rate (sent / total)"""
        if self.total_recipients == 0:
            return 0.0
        return round((self.sent_count / self.total_recipients) * 100, 2)

    @property
    def open_rate(self) -> float:
        """Calculate open rate (opened / sent)"""
        if self.sent_count == 0:
            return 0.0
        return round((self.opened_count / self.sent_count) * 100, 2)

    @property
    def reply_rate(self) -> float:
        """Calculate reply rate (replied / sent)"""
        if self.sent_count == 0:
            return 0.0
        return round((self.replied_count / self.sent_count) * 100, 2)
