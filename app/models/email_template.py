"""Email Template Model"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum

from app.core.database import Base


class EmailLanguage(str, enum.Enum):
    """Email template languages"""
    ENGLISH = "english"
    HINDI = "hindi"
    SPANISH = "spanish"
    FRENCH = "french"
    GERMAN = "german"
    CHINESE = "chinese"
    JAPANESE = "japanese"
    KOREAN = "korean"


class TemplateCategory(str, enum.Enum):
    """Email template categories"""
    # New frontend-aligned categories
    APPLICATION = "application"  # Job applications
    REPLY = "reply"  # Responses to recruiters
    FOLLOWUP = "followup"  # Follow-up emails
    OUTREACH = "outreach"  # Cold outreach
    CUSTOM = "custom"  # Custom user templates

    # Legacy categories (kept for backwards compatibility)
    INITIAL_APPLICATION = "initial_application"
    FOLLOW_UP = "follow_up"
    THANK_YOU = "thank_you"
    INQUIRY = "inquiry"
    NETWORKING = "networking"
    REFERRAL = "referral"
    REAPPLICATION = "reapplication"
    AI_GENERATED = "ai_generated"  # For ULTRA AI generated templates


class EmailTone(str, enum.Enum):
    """Email tones for AI-generated templates"""
    PROFESSIONAL = "professional"
    ENTHUSIASTIC = "enthusiastic"
    STORY_DRIVEN = "story_driven"
    VALUE_FIRST = "value_first"
    CONSULTANT = "consultant"
    FRIENDLY = "friendly"
    FORMAL = "formal"
    CASUAL = "casual"


class EmailTemplate(Base):
    """Email templates for different scenarios and languages"""
    __tablename__ = "email_templates"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Template details
    name = Column(String(255), nullable=False)  # e.g., "Professional ML Engineer Application"
    description = Column(Text)
    category = Column(Enum(TemplateCategory), nullable=False, index=True)
    language = Column(Enum(EmailLanguage), default=EmailLanguage.ENGLISH, nullable=False, index=True)
    tone = Column(Enum(EmailTone), nullable=True, index=True)  # AI-generated email tone

    # Email content
    subject_template = Column(String(500), nullable=False)
    body_template_html = Column(Text, nullable=False)
    body_template_text = Column(Text)  # Plain text version

    # Targeting
    target_position = Column(String(255))  # e.g., "Data Scientist"
    target_industry = Column(String(255))  # e.g., "Finance"
    target_country = Column(String(100))  # e.g., "USA"
    target_company_size = Column(String(50))  # e.g., "Startup", "Enterprise"

    # Template variables
    # Supported variables: {{recruiter_name}}, {{company_name}}, {{position_title}},
    # {{candidate_name}}, {{candidate_email}}, {{skills}}, {{experience}}, etc.
    available_variables = Column(Text)  # JSON list of available variables

    # Status
    is_default = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)

    # Usage tracking
    times_used = Column(Integer, default=0)
    last_used_at = Column(DateTime(timezone=True))

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", back_populates="email_templates")
    applications = relationship("Application", back_populates="email_template")
