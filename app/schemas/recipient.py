"""Recipient, Recipient Group, and Campaign Schemas

Pydantic schemas for the Recipient Groups feature with validation.
"""
from pydantic import BaseModel, EmailStr, field_validator, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from enum import Enum

from app.models.group_campaign import CampaignStatusEnum as CampaignStatus


# ==================== ENUMS ====================

class GroupType(str, Enum):
    """Group type enum matching the database model."""
    STATIC = "static"
    DYNAMIC = "dynamic"


class RecipientStatus(str, Enum):
    """Per-recipient status within a campaign."""
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    SKIPPED = "skipped"
    OPENED = "opened"
    REPLIED = "replied"
    BOUNCED = "bounced"


# ==================== RECIPIENT SCHEMAS ====================

class RecipientCreate(BaseModel):
    """Schema for creating a new recipient."""
    email: EmailStr
    name: Optional[str] = Field(None, max_length=255)
    company: Optional[str] = Field(None, max_length=255)
    position: Optional[str] = Field(None, max_length=255)
    country: Optional[str] = Field(None, max_length=100)
    language: Optional[str] = Field("en", max_length=50)
    tags: Optional[str] = Field(None, max_length=500)
    source: Optional[str] = Field(None, max_length=100)
    custom_fields: Optional[Dict[str, Any]] = None
    is_active: bool = True

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate and normalize email: reject disposable domains, consecutive dots, and invalid TLDs."""
        v = v.lower().strip()
        DISPOSABLE_DOMAINS = {
            "mailinator.com", "guerrillamail.com", "tempmail.com",
            "throwaway.email", "yopmail.com", "sharklasers.com",
            "guerrillamailblock.com", "10minutemail.com", "trashmail.com",
        }
        domain = v.rsplit("@", 1)[-1]
        if domain in DISPOSABLE_DOMAINS:
            raise ValueError(f"Disposable email domains are not allowed: {domain}")
        if ".." in domain:
            raise ValueError("Invalid email domain: consecutive dots are not allowed")
        tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
        if len(tld) < 2:
            raise ValueError("Invalid email domain: TLD must be at least 2 characters")
        return v

    @field_validator("name", "company", "position", "country")
    @classmethod
    def clean_string_fields(cls, v: Optional[str]) -> Optional[str]:
        """Strip whitespace from string fields."""
        if v is None:
            return None
        v = v.strip()
        return v if v else None

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: Optional[str]) -> Optional[str]:
        """Clean tags (comma-separated)."""
        if v is None:
            return None
        # Remove extra spaces around commas
        tags = [tag.strip() for tag in v.split(",") if tag.strip()]
        return ",".join(tags) if tags else None


class RecipientUpdate(BaseModel):
    """Schema for updating an existing recipient."""
    email: Optional[EmailStr] = None
    name: Optional[str] = Field(None, max_length=255)
    company: Optional[str] = Field(None, max_length=255)
    position: Optional[str] = Field(None, max_length=255)
    country: Optional[str] = Field(None, max_length=100)
    language: Optional[str] = Field(None, max_length=50)
    tags: Optional[str] = Field(None, max_length=500)
    custom_fields: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None
    unsubscribed: Optional[bool] = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """Validate and normalize email: reject disposable domains, consecutive dots, and invalid TLDs."""
        if v is None:
            return None
        v = v.lower().strip()
        DISPOSABLE_DOMAINS = {
            "mailinator.com", "guerrillamail.com", "tempmail.com",
            "throwaway.email", "yopmail.com", "sharklasers.com",
            "guerrillamailblock.com", "10minutemail.com", "trashmail.com",
        }
        domain = v.rsplit("@", 1)[-1]
        if domain in DISPOSABLE_DOMAINS:
            raise ValueError(f"Disposable email domains are not allowed: {domain}")
        if ".." in domain:
            raise ValueError("Invalid email domain: consecutive dots are not allowed")
        tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
        if len(tld) < 2:
            raise ValueError("Invalid email domain: TLD must be at least 2 characters")
        return v

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: Optional[str]) -> Optional[str]:
        """Clean tags (comma-separated)."""
        if v is None:
            return None
        tags = [tag.strip() for tag in v.split(",") if tag.strip()]
        return ",".join(tags) if tags else None


class RecipientResponse(BaseModel):
    """Schema for recipient response."""
    id: int
    candidate_id: int
    email: str
    name: Optional[str] = None
    company: Optional[str] = None
    position: Optional[str] = None
    country: Optional[str] = None
    language: Optional[str] = "en"
    tags: Optional[str] = None
    source: Optional[str] = None
    custom_fields: Optional[Dict[str, Any]] = None
    is_active: bool = True
    unsubscribed: bool = False
    total_emails_sent: int = 0
    total_emails_opened: int = 0
    total_emails_replied: int = 0
    engagement_score: float = 0.0
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class RecipientListResponse(BaseModel):
    """Paginated list of recipients."""
    recipients: List[RecipientResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class RecipientSearchRequest(BaseModel):
    """Schema for recipient search."""
    search_term: Optional[str] = None
    company: Optional[str] = None
    tags: Optional[List[str]] = None
    country: Optional[str] = None
    is_active: Optional[bool] = True
    include_unsubscribed: bool = False
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)
    order_by: str = "created_at"
    order_desc: bool = True


class RecipientCSVImportRequest(BaseModel):
    """Schema for CSV import."""
    csv_content: str
    source: str = Field("csv_import", max_length=100)
    skip_duplicates: bool = True


class RecipientCSVImportResponse(BaseModel):
    """Response from CSV import."""
    created: int
    skipped: int
    errors: int
    total_processed: int
    error_details: Optional[List[str]] = None


class RecipientStatistics(BaseModel):
    """Recipient statistics for a candidate."""
    total: int
    active: int
    unsubscribed: int
    never_contacted: int
    avg_engagement_score: float
    top_companies: List[Dict[str, Any]]


# ==================== RECIPIENT GROUP SCHEMAS ====================

class DynamicFilterCriteria(BaseModel):
    """Schema for dynamic group filter criteria."""
    companies: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    countries: Optional[List[str]] = None
    positions: Optional[List[str]] = None
    min_engagement_score: Optional[float] = Field(None, ge=0.0, le=100.0)
    is_active: bool = True
    exclude_unsubscribed: bool = True

    @field_validator("companies", "tags", "countries", "positions")
    @classmethod
    def clean_string_lists(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Remove empty strings from lists."""
        if v is None:
            return None
        cleaned = [item.strip() for item in v if item and item.strip()]
        return cleaned if cleaned else None


class RecipientGroupCreate(BaseModel):
    """Schema for creating a new recipient group."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    group_type: GroupType = GroupType.STATIC
    filter_criteria: Optional[DynamicFilterCriteria] = None
    auto_refresh: bool = True
    color: Optional[str] = Field(None, max_length=50)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Ensure group name is not empty after stripping."""
        v = v.strip()
        if not v:
            raise ValueError("Group name cannot be empty")
        return v

    @field_validator("filter_criteria")
    @classmethod
    def validate_filter_criteria(cls, v: Optional[DynamicFilterCriteria], info) -> Optional[DynamicFilterCriteria]:
        """Ensure dynamic groups have filter criteria."""
        group_type = info.data.get("group_type")
        if group_type == GroupType.DYNAMIC and not v:
            raise ValueError("Dynamic groups must have filter_criteria")
        if group_type == GroupType.STATIC and v:
            raise ValueError("Static groups cannot have filter_criteria")
        return v


class RecipientGroupUpdate(BaseModel):
    """Schema for updating an existing recipient group."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    filter_criteria: Optional[DynamicFilterCriteria] = None
    auto_refresh: Optional[bool] = None
    color: Optional[str] = Field(None, max_length=50)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        """Clean group name."""
        if v is None:
            return None
        v = v.strip()
        if not v:
            raise ValueError("Group name cannot be empty")
        return v


class RecipientGroupResponse(BaseModel):
    """Schema for recipient group response."""
    id: int
    candidate_id: int
    name: str
    description: Optional[str] = None
    group_type: GroupType
    filter_criteria: Optional[Dict[str, Any]] = None
    auto_refresh: bool = True
    last_refreshed_at: Optional[datetime] = None
    total_recipients: int = 0
    active_recipients: int = 0
    color: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class RecipientGroupListResponse(BaseModel):
    """Paginated list of recipient groups."""
    groups: List[RecipientGroupResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class RecipientGroupWithRecipientsResponse(RecipientGroupResponse):
    """Group response with recipient details."""
    recipients: List[RecipientResponse]


class AddRecipientsToGroupRequest(BaseModel):
    """Schema for adding recipients to a group."""
    recipient_ids: List[int] = Field(..., min_length=1)

    @field_validator("recipient_ids")
    @classmethod
    def validate_recipient_ids(cls, v: List[int]) -> List[int]:
        """Ensure unique IDs."""
        return list(set(v))


class RemoveRecipientsFromGroupRequest(BaseModel):
    """Schema for removing recipients from a group."""
    recipient_ids: List[int] = Field(..., min_length=1)


class RefreshDynamicGroupResponse(BaseModel):
    """Response from refreshing a dynamic group."""
    refreshed: bool
    total_recipients: Optional[int] = None
    active_recipients: Optional[int] = None
    added: Optional[int] = None
    removed: Optional[int] = None
    last_refresh: Optional[datetime] = None
    reason: Optional[str] = None


class GroupStatistics(BaseModel):
    """Group statistics for a candidate."""
    total_groups: int
    static_groups: int
    dynamic_groups: int
    unique_recipients: int
    avg_group_size: float


# ==================== GROUP CAMPAIGN SCHEMAS ====================

class GroupCampaignCreate(BaseModel):
    """Schema for creating a new group campaign."""
    group_id: int
    campaign_name: str = Field(..., min_length=1, max_length=255)
    email_template_id: Optional[int] = None
    subject_template: str = Field(..., min_length=1, max_length=500)
    body_template_html: str = Field(..., min_length=1)
    send_delay_seconds: int = Field(60, ge=10, le=3600)
    scheduled_at: Optional[datetime] = None

    # Follow-up configuration
    enable_follow_up: bool = False
    follow_up_sequence_id: Optional[int] = None
    follow_up_stop_on_reply: bool = True
    follow_up_stop_on_bounce: bool = False

    @field_validator("campaign_name")
    @classmethod
    def validate_campaign_name(cls, v: str) -> str:
        """Ensure campaign name is not empty after stripping."""
        v = v.strip()
        if not v:
            raise ValueError("Campaign name cannot be empty")
        return v

    @field_validator("subject_template", "body_template_html")
    @classmethod
    def validate_templates(cls, v: str) -> str:
        """Ensure templates are not empty."""
        v = v.strip()
        if not v:
            raise ValueError("Template content cannot be empty")
        return v

    @field_validator("follow_up_sequence_id")
    @classmethod
    def validate_follow_up_sequence(cls, v: Optional[int], info) -> Optional[int]:
        """Ensure follow_up_sequence_id is provided when follow-up is enabled."""
        enable_follow_up = info.data.get("enable_follow_up", False)
        if enable_follow_up and not v:
            raise ValueError("follow_up_sequence_id is required when enable_follow_up is True")
        return v


class GroupCampaignUpdate(BaseModel):
    """Schema for updating a campaign (draft only)."""
    campaign_name: Optional[str] = Field(None, max_length=255)
    subject_template: Optional[str] = Field(None, max_length=500)
    body_template_html: Optional[str] = None
    send_delay_seconds: Optional[int] = Field(None, ge=10, le=3600)
    scheduled_at: Optional[datetime] = None

    # Follow-up configuration
    enable_follow_up: Optional[bool] = None
    follow_up_sequence_id: Optional[int] = None
    follow_up_stop_on_reply: Optional[bool] = None
    follow_up_stop_on_bounce: Optional[bool] = None


class GroupCampaignResponse(BaseModel):
    """Schema for campaign response."""
    id: int
    candidate_id: int
    group_id: Optional[int] = None
    campaign_name: str
    email_template_id: Optional[int] = None
    subject_template: str
    body_template_html: str
    send_delay_seconds: int = 60
    scheduled_at: Optional[datetime] = None
    status: CampaignStatus

    # Follow-up configuration
    enable_follow_up: bool = False
    follow_up_sequence_id: Optional[int] = None
    follow_up_stop_on_reply: bool = True
    follow_up_stop_on_bounce: bool = False

    # Counts
    total_recipients: int = 0
    sent_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    opened_count: int = 0
    replied_count: int = 0
    bounced_count: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    paused_at: Optional[datetime] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    # Computed properties
    success_rate: float = 0.0
    open_rate: float = 0.0
    reply_rate: float = 0.0

    model_config = ConfigDict(from_attributes=True)


class GroupCampaignListResponse(BaseModel):
    """Paginated list of campaigns."""
    campaigns: List[GroupCampaignResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class CampaignRecipientResponse(BaseModel):
    """Per-recipient campaign status."""
    id: int
    campaign_id: int
    recipient_id: int
    recipient_email: str
    recipient_name: Optional[str] = None
    rendered_subject: Optional[str] = None
    rendered_body_html: Optional[str] = None
    status: RecipientStatus
    tracking_id: Optional[str] = None
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    opened_at: Optional[datetime] = None
    replied_at: Optional[datetime] = None
    bounced_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    email_log_id: Optional[int] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class CampaignRecipientsListResponse(BaseModel):
    """List of campaign recipients with status."""
    recipients: List[CampaignRecipientResponse]
    total: int
    page: int
    page_size: int


class SendCampaignRequest(BaseModel):
    """Request to send a campaign."""
    send_immediately: bool = True
    scheduled_at: Optional[datetime] = None

    @field_validator("scheduled_at")
    @classmethod
    def validate_scheduled_at(cls, v: Optional[datetime], info) -> Optional[datetime]:
        """Validate scheduled_at."""
        send_immediately = info.data.get("send_immediately")
        if not send_immediately and not v:
            raise ValueError("scheduled_at is required when send_immediately is False")
        if send_immediately and v:
            raise ValueError("Cannot set scheduled_at when send_immediately is True")
        if v and v <= datetime.now(timezone.utc):
            raise ValueError("scheduled_at must be in the future")
        return v


class PauseCampaignResponse(BaseModel):
    """Response from pausing a campaign."""
    campaign_id: int
    status: CampaignStatus
    paused_at: datetime
    message: str


class ResumeCampaignResponse(BaseModel):
    """Response from resuming a campaign."""
    campaign_id: int
    status: CampaignStatus
    resumed_at: datetime
    message: str


# ==================== TEMPLATE PREVIEW SCHEMAS ====================

class TemplatePreviewRequest(BaseModel):
    """Request to preview a template with recipient data."""
    subject_template: str
    body_template_html: str
    recipient_id: Optional[int] = None
    sample_data: Optional[Dict[str, str]] = None


class TemplatePreviewResponse(BaseModel):
    """Response with rendered template."""
    rendered_subject: str
    rendered_body_html: str
    recipient_data: Dict[str, Any]
    variables_used: List[str]
