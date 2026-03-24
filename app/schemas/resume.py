"""Resume Version Schemas"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from app.models.resume import ResumeLanguage


class ResumeVersionBase(BaseModel):
    """Base resume version schema"""
    name: str = Field(..., min_length=1, max_length=255, description="Resume name (e.g., 'ML Engineer Resume')")
    description: Optional[str] = Field(None, description="Resume description")
    language: ResumeLanguage = Field(default=ResumeLanguage.ENGLISH, description="Resume language")
    target_position: Optional[str] = Field(None, max_length=255, description="Target position (e.g., 'Machine Learning Engineer')")
    target_industry: Optional[str] = Field(None, max_length=255, description="Target industry (e.g., 'Tech', 'Finance')")
    target_country: Optional[str] = Field(None, max_length=100, description="Target country (e.g., 'USA', 'India')")
    is_default: bool = Field(default=False, description="Set as default resume")
    is_active: bool = Field(default=True, description="Resume is active")


class ResumeVersionCreate(ResumeVersionBase):
    """Schema for creating a resume version"""
    pass


class ResumeVersionUpdate(BaseModel):
    """Schema for updating a resume version"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    language: Optional[ResumeLanguage] = None
    target_position: Optional[str] = Field(None, max_length=255)
    target_industry: Optional[str] = Field(None, max_length=255)
    target_country: Optional[str] = Field(None, max_length=100)
    is_default: Optional[bool] = None
    is_active: Optional[bool] = None


class ResumeVersionResponse(ResumeVersionBase):
    """Schema for resume version response"""
    id: int
    candidate_id: int
    filename: str
    file_path: str
    file_size: Optional[int] = None
    times_used: int = 0
    last_used_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ResumeVersionListResponse(BaseModel):
    """Schema for list of resume versions"""
    total: int
    items: list[ResumeVersionResponse]
