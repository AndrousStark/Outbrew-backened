"""Email Template Schemas"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from app.models.email_template import EmailLanguage, TemplateCategory


class EmailTemplateBase(BaseModel):
    """Base email template schema"""
    name: str = Field(..., min_length=1, max_length=255, description="Template name")
    description: Optional[str] = Field(None, description="Template description")
    category: TemplateCategory = Field(..., description="Template category")
    language: EmailLanguage = Field(default=EmailLanguage.ENGLISH, description="Template language")
    subject_template: str = Field(..., min_length=1, max_length=500, description="Email subject template with {{variables}}")
    body_template_html: str = Field(..., min_length=1, description="HTML email body template with {{variables}}")
    body_template_text: Optional[str] = Field(None, description="Plain text email body template")
    target_position: Optional[str] = Field(None, max_length=255, description="Target position")
    target_industry: Optional[str] = Field(None, max_length=255, description="Target industry")
    target_country: Optional[str] = Field(None, max_length=100, description="Target country")
    target_company_size: Optional[str] = Field(None, max_length=50, description="Target company size")
    available_variables: Optional[str] = Field(
        None,
        description="JSON string of available variables (e.g., ['candidate_name', 'company_name', 'position_title'])"
    )
    is_default: bool = Field(default=False, description="Set as default template for category")
    is_active: bool = Field(default=True, description="Template is active")


class EmailTemplateCreate(EmailTemplateBase):
    """Schema for creating an email template"""
    pass


class EmailTemplateUpdate(BaseModel):
    """Schema for updating an email template"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    category: Optional[TemplateCategory] = None
    language: Optional[EmailLanguage] = None
    subject_template: Optional[str] = Field(None, min_length=1, max_length=500)
    body_template_html: Optional[str] = Field(None, min_length=1)
    body_template_text: Optional[str] = None
    target_position: Optional[str] = Field(None, max_length=255)
    target_industry: Optional[str] = Field(None, max_length=255)
    target_country: Optional[str] = Field(None, max_length=100)
    target_company_size: Optional[str] = Field(None, max_length=50)
    available_variables: Optional[str] = None
    is_default: Optional[bool] = None
    is_active: Optional[bool] = None


class EmailTemplateResponse(EmailTemplateBase):
    """Schema for email template response"""
    id: int
    candidate_id: int
    times_used: int = 0
    last_used_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class EmailTemplateListResponse(BaseModel):
    """Schema for list of email templates"""
    total: int
    items: list[EmailTemplateResponse]


class EmailTemplatePreviewRequest(BaseModel):
    """Schema for previewing a template with test variables"""
    template_id: int
    variables: dict = Field(
        default_factory=dict,
        description="Variables to use for preview",
        examples=[{
            "candidate_name": "John Doe",
            "company_name": "Acme Corp",
            "position_title": "Software Engineer",
            "recruiter_name": "Jane Smith"
        }]
    )


class EmailTemplatePreviewResponse(BaseModel):
    """Schema for template preview response"""
    subject: str
    body_html: str
    body_text: Optional[str] = None
