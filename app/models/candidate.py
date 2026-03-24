"""Candidate Model"""
import re
import logging
from sqlalchemy import Column, Integer, String, DateTime, Float, JSON, Boolean, Enum, event
from sqlalchemy.orm import relationship, validates
from sqlalchemy.sql import func
import enum

from app.core.database import Base

logger = logging.getLogger(__name__)

# Email validation regex pattern (RFC 5322 compliant simplified version)
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


class UserRole(str, enum.Enum):
    """User roles for different access levels"""
    PRAGYA = "pragya"
    ANIRUDDH = "aniruddh"
    SUPER_ADMIN = "super_admin"


class Candidate(Base):
    __tablename__ = "candidates"

    id = Column(Integer, primary_key=True, index=True)

    # User identification
    username = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=False)
    role = Column(Enum(UserRole), default=UserRole.PRAGYA, nullable=False, index=True)

    # Email config
    email_account = Column(String(255), nullable=False)
    email_password = Column(String(255), nullable=False)
    smtp_host = Column(String(255), default="smtp.gmail.com")
    smtp_port = Column(Integer, default=587)

    # Resume
    resume_filename = Column(String(255))
    resume_path = Column(String(500))

    # Profile
    title = Column(String(255))
    skills = Column(JSON)

    # Stats
    total_applications_sent = Column(Integer, default=0)
    total_responses_received = Column(Integer, default=0)
    response_rate = Column(Float, default=0.0)

    # Status
    is_active = Column(Boolean, default=True)

    # Token revocation: all tokens issued before this timestamp are invalid
    tokens_invalid_before = Column(DateTime(timezone=True), nullable=True)

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    applications = relationship("Application", back_populates="candidate", cascade="all, delete-orphan")
    email_logs = relationship("EmailLog", back_populates="candidate", cascade="all, delete-orphan")
    resume_versions = relationship("ResumeVersion", back_populates="candidate", cascade="all, delete-orphan")
    email_templates = relationship("EmailTemplate", back_populates="candidate", cascade="all, delete-orphan")
    warming_config = relationship("EmailWarmingConfig", back_populates="candidate", uselist=False, cascade="all, delete-orphan")
    rate_limit_config = relationship("RateLimitConfig", back_populates="candidate", uselist=False, cascade="all, delete-orphan")

    # Company Intelligence relationships
    skill_profile = relationship("CandidateSkillProfile", back_populates="candidate", uselist=False, cascade="all, delete-orphan")
    skill_matches = relationship("SkillMatch", back_populates="candidate", cascade="all, delete-orphan")
    email_drafts = relationship("PersonalizedEmailDraft", back_populates="candidate", cascade="all, delete-orphan")

    # Recipient Groups relationships (NEW)
    recipients = relationship("Recipient", back_populates="candidate", cascade="all, delete-orphan")
    recipient_groups = relationship("RecipientGroup", back_populates="candidate", cascade="all, delete-orphan")
    group_campaigns = relationship("GroupCampaign", back_populates="candidate", cascade="all, delete-orphan")

    # ULTRA PRO MAX EXTRACTION ENGINE relationships
    extraction_jobs = relationship("ExtractionJob", back_populates="candidate", cascade="all, delete-orphan")
    extraction_templates = relationship("ExtractionTemplate", back_populates="candidate", cascade="all, delete-orphan")

    # Email validation
    @validates('email')
    def validate_email(self, key: str, email: str) -> str:
        """Validate email format."""
        if email and not EMAIL_REGEX.match(email):
            logger.warning(f"[Candidate] Invalid email format: {email}")
            raise ValueError(f"Invalid email format: {email}")
        return email.lower() if email else email

    @validates('email_account')
    def validate_email_account(self, key: str, email_account: str) -> str:
        """Validate email account format."""
        if email_account and not EMAIL_REGEX.match(email_account):
            logger.warning(f"[Candidate] Invalid email_account format: {email_account}")
            raise ValueError(f"Invalid email account format: {email_account}")
        return email_account.lower() if email_account else email_account

    def __repr__(self) -> str:
        return f"<Candidate {self.username} ({self.email})>"
