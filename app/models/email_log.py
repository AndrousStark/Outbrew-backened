"""Email Log Model"""
import enum
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, Enum, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class EmailStatusEnum(str, enum.Enum):
    """Email status enum"""
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    BOUNCED = "bounced"


class EmailLog(Base):
    __tablename__ = "email_logs"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)
    application_id = Column(Integer, ForeignKey("applications.id", ondelete="CASCADE"), index=True)

    # Email details
    from_email = Column(String(255), nullable=False)
    to_email = Column(String(255), nullable=False, index=True)
    subject = Column(String(500))
    body_html = Column(Text, nullable=True)

    # Tracking
    tracking_id = Column(String(100), unique=True, nullable=True, index=True)
    campaign_id = Column(Integer, ForeignKey("group_campaigns.id", ondelete="SET NULL"), nullable=True, index=True)

    # Status
    status = Column(Enum(EmailStatusEnum), default=EmailStatusEnum.PENDING, nullable=False, index=True)
    error_message = Column(Text)

    # Tracking
    opened = Column(Boolean, default=False)
    clicked = Column(Boolean, default=False)

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    sent_at = Column(DateTime(timezone=True))
    opened_at = Column(DateTime(timezone=True))

    # Composite indexes for common queries
    __table_args__ = (
        Index('ix_email_candidate_created', 'candidate_id', 'created_at'),
        Index('ix_email_status_created', 'status', 'created_at'),
    )

    # Relationships
    candidate = relationship("Candidate", back_populates="email_logs")
    application = relationship("Application", back_populates="email_logs")
    campaign = relationship("GroupCampaign", backref="email_logs")
