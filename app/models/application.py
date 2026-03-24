"""Application Model"""
import enum
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, ForeignKey, Enum, Boolean, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class ApplicationStatusEnum(str, enum.Enum):
    DRAFT = "draft"
    SENT = "sent"
    OPENED = "opened"
    RESPONDED = "responded"  # Recruiter replied
    REPLIED = "replied"
    INTERVIEW = "interview"  # Interview scheduled
    WAITING = "waiting"  # Waiting for decision
    OFFER = "offer"  # Offer received
    REJECTED = "rejected"
    ACCEPTED = "accepted"  # Offer accepted
    DECLINED = "declined"  # Declined offer


class ApplicationType(str, enum.Enum):
    """Type of application"""
    INITIAL = "initial"
    FOLLOW_UP = "follow_up"
    REAPPLICATION = "reapplication"
    REFERRAL = "referral"


class Application(Base):
    __tablename__ = "applications"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)
    resume_version_id = Column(Integer, ForeignKey("resume_versions.id", ondelete="SET NULL"), index=True)
    email_template_id = Column(Integer, ForeignKey("email_templates.id", ondelete="SET NULL"), index=True)
    parent_application_id = Column(Integer, ForeignKey("applications.id", ondelete="SET NULL"), index=True)
    recipient_id = Column(Integer, ForeignKey("recipients.id", ondelete="SET NULL"), nullable=True, index=True)

    # Application type
    application_type = Column(Enum(ApplicationType), default=ApplicationType.INITIAL, nullable=False, index=True)

    # Recruiter
    recruiter_name = Column(String(255))
    recruiter_email = Column(String(255), index=True, nullable=False)
    recruiter_country = Column(String(100))  # Recruiter's country
    recruiter_language = Column(String(50))  # Preferred language

    # Position
    position_title = Column(String(255))
    position_level = Column(String(50))
    position_country = Column(String(100), index=True)  # Job location country
    position_language = Column(String(50))  # Required language for position
    job_posting_url = Column(String(500))

    # Email
    email_subject = Column(String(500))
    email_body_html = Column(Text)
    alignment_text = Column(Text)
    alignment_score = Column(Float, default=0.0)

    # Status
    status = Column(Enum(ApplicationStatusEnum), default=ApplicationStatusEnum.DRAFT, index=True)

    # Tracking
    tracking_id = Column(String(100), unique=True, index=True)

    # Dates
    sent_at = Column(DateTime(timezone=True))
    opened_at = Column(DateTime(timezone=True))
    replied_at = Column(DateTime(timezone=True))

    # Response
    response_received = Column(Boolean, default=False)
    response_content = Column(Text)

    # Metadata (keeping old notes column for backward compatibility)
    notes = Column(Text)
    tags = Column(String(500))
    priority = Column(Integer, default=0)
    is_starred = Column(Boolean, default=False)

    # Interview details
    interview_date = Column(DateTime(timezone=True))
    interview_type = Column(String(50))  # "phone", "video", "onsite"
    interview_notes = Column(Text)

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", back_populates="applications")
    company = relationship("Company", back_populates="applications")
    resume_version = relationship("ResumeVersion", back_populates="applications")
    email_template = relationship("EmailTemplate", back_populates="applications")
    recipient = relationship("Recipient", back_populates="applications")
    email_logs = relationship("EmailLog", back_populates="application", cascade="all, delete-orphan")

    # Self-referential relationship for follow-ups
    parent_application = relationship("Application", remote_side=[id], backref="follow_ups")

    # New relationships for history, notes, and attachments
    history = relationship("ApplicationHistory", back_populates="application", cascade="all, delete-orphan")
    application_notes_list = relationship("ApplicationNote", back_populates="application", cascade="all, delete-orphan")
    attachments = relationship("ApplicationAttachment", back_populates="application", cascade="all, delete-orphan")

    # Composite indexes for common queries
    __table_args__ = (
        Index('ix_app_candidate_status', 'candidate_id', 'status'),
        Index('ix_app_candidate_created', 'candidate_id', 'created_at'),
        Index('ix_app_status_created', 'status', 'created_at'),
        Index('ix_app_candidate_recipient', 'candidate_id', 'recipient_id'),
    )
