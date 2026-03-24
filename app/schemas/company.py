"""Company Schemas"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Any
from datetime import datetime
import json


class CompanyCreate(BaseModel):
    """Schema for creating a company."""
    name: str = Field(..., min_length=1, max_length=255)
    domain: Optional[str] = Field(None, max_length=255)
    industry: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    tech_stack: Optional[List[str]] = None
    company_size: Optional[str] = Field(None, max_length=50)
    website_url: Optional[str] = Field(None, max_length=500)
    linkedin_url: Optional[str] = Field(None, max_length=500)
    careers_url: Optional[str] = Field(None, max_length=500)
    headquarters_country: Optional[str] = Field(None, max_length=100)
    headquarters_city: Optional[str] = Field(None, max_length=255)
    primary_language: Optional[str] = Field(None, max_length=50)

    @field_validator("tech_stack", mode="before")
    @classmethod
    def validate_tech_stack(cls, v: Any) -> Optional[List[str]]:
        """Validate and normalize tech_stack field."""
        if v is None:
            return None
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                return [v.strip()] if v.strip() else None
        if not isinstance(v, list):
            raise ValueError("tech_stack must be a list of strings")
        return [str(s).strip() for s in v if s and str(s).strip()]


class CompanyUpdate(BaseModel):
    """Schema for updating a company."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    domain: Optional[str] = Field(None, max_length=255)
    industry: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    tech_stack: Optional[List[str]] = None
    company_size: Optional[str] = Field(None, max_length=50)
    website_url: Optional[str] = Field(None, max_length=500)
    linkedin_url: Optional[str] = Field(None, max_length=500)
    careers_url: Optional[str] = Field(None, max_length=500)
    headquarters_country: Optional[str] = Field(None, max_length=100)
    headquarters_city: Optional[str] = Field(None, max_length=255)
    primary_language: Optional[str] = Field(None, max_length=50)

    @field_validator("tech_stack", mode="before")
    @classmethod
    def validate_tech_stack(cls, v: Any) -> Optional[List[str]]:
        """Validate and normalize tech_stack field."""
        if v is None:
            return None
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                return [v.strip()] if v.strip() else None
        if not isinstance(v, list):
            raise ValueError("tech_stack must be a list of strings")
        return [str(s).strip() for s in v if s and str(s).strip()]


class CompanyResponse(BaseModel):
    """Schema for company response."""
    id: int
    name: str
    domain: Optional[str] = None
    industry: Optional[str] = None
    description: Optional[str] = None
    tech_stack: Optional[List[str]] = None
    company_size: Optional[str] = None
    website_url: Optional[str] = None
    linkedin_url: Optional[str] = None
    careers_url: Optional[str] = None
    headquarters_country: Optional[str] = None
    headquarters_city: Optional[str] = None
    primary_language: Optional[str] = None
    alignment_pragya_text: Optional[str] = None
    alignment_pragya_score: Optional[float] = 0.0
    alignment_aniruddh_text: Optional[str] = None
    alignment_aniruddh_score: Optional[float] = 0.0
    job_postings_pragya: Optional[List[dict]] = None
    job_postings_aniruddh: Optional[List[dict]] = None
    total_applications: Optional[int] = 0
    total_responses: Optional[int] = 0
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class CompanyResearchRequest(BaseModel):
    """Request to research a company."""
    company_name: str = Field(..., min_length=1, max_length=255)
    candidate: str = Field(..., pattern="^(pragya|aniruddh)$")
