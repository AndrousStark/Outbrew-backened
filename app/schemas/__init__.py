"""Pydantic Schemas

All Pydantic schemas for request/response validation.
"""
# Common schemas and validators
from app.schemas.common import (
    PaginatedResponse,
    StatusResponse,
    SkillsValidator,
    WarmingScheduleValidator,
    RateLimitValidator,
    WarmingConfigCreate,
    WarmingConfigUpdate,
    RateLimitConfigCreate,
    RateLimitConfigUpdate,
)

# Application schemas
from app.schemas.application import (
    ApplicationStatus,
    ApplicationType,
    ApplicationCreate,
    ApplicationUpdate,
    ApplicationStatusUpdate,
    ApplicationResponse,
    ApplicationNoteCreate,
    ApplicationNoteResponse,
    ApplicationHistoryResponse,
)

# Auth schemas
from app.schemas.auth import (
    Token,
    TokenData,
    LoginRequest,
    RegisterRequest,
    UserResponse,
    ChangePasswordRequest,
)

# Company schemas
from app.schemas.company import (
    CompanyCreate,
    CompanyUpdate,
    CompanyResponse,
)

# Resume schemas
from app.schemas.resume import (
    ResumeVersionCreate,
    ResumeVersionUpdate,
    ResumeVersionResponse,
)

# Email template schemas
from app.schemas.email_template import (
    EmailTemplateCreate,
    EmailTemplateUpdate,
    EmailTemplateResponse,
)

# Recipient schemas (Recipient Groups feature)
from app.schemas.recipient import (
    # Enums
    GroupType,
    CampaignStatus,
    RecipientStatus,
    # Recipient schemas
    RecipientCreate,
    RecipientUpdate,
    RecipientResponse,
    RecipientListResponse,
    RecipientSearchRequest,
    RecipientCSVImportRequest,
    RecipientCSVImportResponse,
    RecipientStatistics,
    # Group schemas
    DynamicFilterCriteria,
    RecipientGroupCreate,
    RecipientGroupUpdate,
    RecipientGroupResponse,
    RecipientGroupListResponse,
    RecipientGroupWithRecipientsResponse,
    AddRecipientsToGroupRequest,
    RemoveRecipientsFromGroupRequest,
    RefreshDynamicGroupResponse,
    GroupStatistics,
    # Campaign schemas
    GroupCampaignCreate,
    GroupCampaignUpdate,
    GroupCampaignResponse,
    GroupCampaignListResponse,
    CampaignRecipientResponse,
    CampaignRecipientsListResponse,
    SendCampaignRequest,
    PauseCampaignResponse,
    ResumeCampaignResponse,
    # Template preview
    TemplatePreviewRequest,
    TemplatePreviewResponse,
)

__all__ = [
    # Common
    "PaginatedResponse",
    "StatusResponse",
    "SkillsValidator",
    "WarmingScheduleValidator",
    "RateLimitValidator",
    "WarmingConfigCreate",
    "WarmingConfigUpdate",
    "RateLimitConfigCreate",
    "RateLimitConfigUpdate",
    # Application
    "ApplicationStatus",
    "ApplicationType",
    "ApplicationCreate",
    "ApplicationUpdate",
    "ApplicationStatusUpdate",
    "ApplicationResponse",
    "ApplicationNoteCreate",
    "ApplicationNoteResponse",
    "ApplicationHistoryResponse",
    # Auth
    "Token",
    "TokenData",
    "LoginRequest",
    "RegisterRequest",
    "UserResponse",
    "ChangePasswordRequest",
    # Company
    "CompanyCreate",
    "CompanyUpdate",
    "CompanyResponse",
    # Resume
    "ResumeVersionCreate",
    "ResumeVersionUpdate",
    "ResumeVersionResponse",
    # Email Template
    "EmailTemplateCreate",
    "EmailTemplateUpdate",
    "EmailTemplateResponse",
    # Recipient Groups Feature
    # Enums
    "GroupType",
    "CampaignStatus",
    "RecipientStatus",
    # Recipient
    "RecipientCreate",
    "RecipientUpdate",
    "RecipientResponse",
    "RecipientListResponse",
    "RecipientSearchRequest",
    "RecipientCSVImportRequest",
    "RecipientCSVImportResponse",
    "RecipientStatistics",
    # Group
    "DynamicFilterCriteria",
    "RecipientGroupCreate",
    "RecipientGroupUpdate",
    "RecipientGroupResponse",
    "RecipientGroupListResponse",
    "RecipientGroupWithRecipientsResponse",
    "AddRecipientsToGroupRequest",
    "RemoveRecipientsFromGroupRequest",
    "RefreshDynamicGroupResponse",
    "GroupStatistics",
    # Campaign
    "GroupCampaignCreate",
    "GroupCampaignUpdate",
    "GroupCampaignResponse",
    "GroupCampaignListResponse",
    "CampaignRecipientResponse",
    "CampaignRecipientsListResponse",
    "SendCampaignRequest",
    "PauseCampaignResponse",
    "ResumeCampaignResponse",
    # Template Preview
    "TemplatePreviewRequest",
    "TemplatePreviewResponse",
]
