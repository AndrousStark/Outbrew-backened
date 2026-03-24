"""Recipient Model - Normalized contact directory for email campaigns"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey, Text, JSON, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Recipient(Base):
    """
    Recipient model represents a contact in the candidate's recipient directory.

    Recipients can be added to multiple groups and receive personalized emails
    through group campaigns. This provides a normalized contact directory
    separate from the Application model.
    """
    __tablename__ = "recipients"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Ownership (multi-tenant isolation)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Core Contact Fields
    email = Column(String(255), nullable=False, index=True)
    name = Column(String(255))
    company = Column(String(255), index=True)
    position = Column(String(255))

    # Localization
    country = Column(String(100), index=True)
    language = Column(String(50))  # en, hi, es, fr, de, zh, ja, ko

    # Categorization
    tags = Column(String(500))  # Comma-separated: "senior,ml-engineer,usa"
    source = Column(String(100))  # manual, csv_import, linkedin, application, api

    # Custom Fields (JSON for extensibility)
    custom_fields = Column(JSON)  # {"linkedin_url": "...", "phone": "...", "notes": "..."}

    # Status
    is_active = Column(Boolean, default=True, index=True)
    unsubscribed = Column(Boolean, default=False, index=True)
    bounce_count = Column(Integer, default=0)
    last_contacted_at = Column(DateTime(timezone=True))

    # Engagement Tracking
    total_emails_sent = Column(Integer, default=0)
    total_emails_opened = Column(Integer, default=0)
    total_emails_replied = Column(Integer, default=0)
    engagement_score = Column(Float, default=0.0)  # 0-100 calculated score

    # Soft Delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", back_populates="recipients")
    applications = relationship("Application", back_populates="recipient")
    group_memberships = relationship(
        "GroupRecipient",
        back_populates="recipient",
        cascade="all, delete-orphan"
    )
    campaign_sends = relationship(
        "GroupCampaignRecipient",
        back_populates="recipient",
        cascade="all, delete-orphan"
    )

    # Composite Indexes
    __table_args__ = (
        # Unique email per candidate (soft delete aware)
        Index('ix_recipient_candidate_email_deleted',
              'candidate_id', 'email', 'deleted_at',
              unique=True),
        # Search optimization
        Index('ix_recipient_search', 'candidate_id', 'company', 'tags'),
    )

    def __repr__(self):
        return f"<Recipient(id={self.id}, email={self.email}, name={self.name})>"
