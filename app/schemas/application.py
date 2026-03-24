"""Application Schemas

Pydantic schemas for application CRUD operations with validation.
"""
from pydantic import BaseModel, EmailStr, field_validator, Field
from typing import Optional, List, Any
from datetime import datetime
from enum import Enum


class ApplicationStatus(str, Enum):
    """Application status enum matching the database model."""
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


class ApplicationType(str, Enum):
    """Type of application."""
    INITIAL = "initial"
    FOLLOW_UP = "follow_up"
    REAPPLICATION = "reapplication"
    REFERRAL = "referral"


class ApplicationCreate(BaseModel):
    """Schema for creating a new application."""
    company_name: str = Field(..., min_length=1, max_length=255)
    recruiter_name: Optional[str] = Field(None, max_length=255)
    recruiter_email: EmailStr
    position_title: Optional[str] = Field(None, max_length=255)
    position_level: Optional[str] = Field(None, max_length=50)
    job_posting_url: Optional[str] = Field(None, max_length=500)
    notes: Optional[str] = None
    tags: Optional[str] = Field(None, max_length=500)
    priority: Optional[int] = Field(0, ge=0, le=10)
    application_type: Optional[ApplicationType] = ApplicationType.INITIAL

    @field_validator("company_name")
    @classmethod
    def validate_company_name(cls, v: str) -> str:
        """Ensure company name is not empty after stripping."""
        v = v.strip()
        if not v:
            raise ValueError("Company name cannot be empty")
        return v

    @field_validator("recruiter_name")
    @classmethod
    def validate_recruiter_name(cls, v: Optional[str]) -> Optional[str]:
        """Clean recruiter name."""
        if v is None:
            return None
        v = v.strip()
        return v if v else None

    @field_validator("job_posting_url")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        """Basic URL validation."""
        if v is None or not v.strip():
            return None
        v = v.strip()
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v


class ApplicationUpdate(BaseModel):
    """Schema for updating an existing application."""
    recruiter_name: Optional[str] = Field(None, max_length=255)
    recruiter_email: Optional[EmailStr] = None
    position_title: Optional[str] = Field(None, max_length=255)
    position_level: Optional[str] = Field(None, max_length=50)
    job_posting_url: Optional[str] = Field(None, max_length=500)
    status: Optional[ApplicationStatus] = None
    notes: Optional[str] = None
    tags: Optional[str] = Field(None, max_length=500)
    priority: Optional[int] = Field(None, ge=0, le=10)
    is_starred: Optional[bool] = None
    email_subject: Optional[str] = Field(None, max_length=500)
    email_body_html: Optional[str] = None
    interview_date: Optional[datetime] = None
    interview_type: Optional[str] = Field(None, max_length=50)
    interview_notes: Optional[str] = None

    @field_validator("interview_type")
    @classmethod
    def validate_interview_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate interview type."""
        if v is None:
            return None
        valid_types = ["phone", "video", "onsite", "technical", "behavioral", "panel", "other"]
        v_lower = v.lower().strip()
        if v_lower not in valid_types:
            # Allow custom types, just clean them
            return v.strip()
        return v_lower


class ApplicationStatusUpdate(BaseModel):
    """Schema for updating just the status with optional note."""
    status: ApplicationStatus
    note: Optional[str] = None


class ApplicationResponse(BaseModel):
    """Schema for application response."""
    id: int
    candidate_id: int
    company_id: int
    company_name: Optional[str] = None
    recruiter_name: Optional[str] = None
    recruiter_email: str
    recruiter_country: Optional[str] = None
    recruiter_language: Optional[str] = None
    position_title: Optional[str] = None
    position_level: Optional[str] = None
    position_country: Optional[str] = None
    position_language: Optional[str] = None
    job_posting_url: Optional[str] = None
    email_subject: Optional[str] = None
    email_body_html: Optional[str] = None
    alignment_text: Optional[str] = None
    alignment_score: Optional[float] = 0.0
    status: ApplicationStatus
    application_type: Optional[str] = None
    tracking_id: Optional[str] = None
    sent_at: Optional[datetime] = None
    opened_at: Optional[datetime] = None
    replied_at: Optional[datetime] = None
    response_received: Optional[bool] = False
    response_content: Optional[str] = None
    notes: Optional[str] = None
    tags: Optional[str] = None
    priority: Optional[int] = 0
    is_starred: Optional[bool] = False
    interview_date: Optional[datetime] = None
    interview_type: Optional[str] = None
    interview_notes: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ApplicationNoteCreate(BaseModel):
    """Schema for creating an application note."""
    content: str = Field(..., min_length=1)
    note_type: Optional[str] = Field("general", max_length=50)

    @field_validator("note_type")
    @classmethod
    def validate_note_type(cls, v: str) -> str:
        valid_types = ["general", "interview", "follow_up", "feedback", "reminder"]
        if v.lower() not in valid_types:
            return "general"
        return v.lower()


class ApplicationNoteResponse(BaseModel):
    """Schema for application note response."""
    id: int
    application_id: int
    candidate_id: int
    content: str
    note_type: str
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ApplicationHistoryResponse(BaseModel):
    """Schema for application history response."""
    id: int
    application_id: int
    changed_by: Optional[int] = None
    field_name: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    note: Optional[str] = None
    change_type: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True
