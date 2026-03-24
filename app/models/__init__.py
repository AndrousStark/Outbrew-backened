"""Database Models

All SQLAlchemy models for the Outbrew application.
"""

from app.models.candidate import Candidate, UserRole
from app.models.company import Company
from app.models.application import Application, ApplicationStatusEnum, ApplicationType
from app.models.email_log import EmailLog
from app.models.resume import ResumeVersion, ResumeLanguage
from app.models.email_template import EmailTemplate, EmailLanguage, TemplateCategory
from app.models.application_history import (
    ApplicationHistory,
    ApplicationNote,
    ApplicationAttachment,
)
from app.models.email_warming import (
    EmailWarmingConfig,
    EmailWarmingDailyLog,
    WarmingStrategyEnum,
    WarmingStatusEnum,
    WARMING_SCHEDULES,
)
from app.models.rate_limiting import (
    RateLimitConfig,
    RateLimitUsageLog,
    RateLimitPresetEnum,
    RateLimitPeriodEnum,
    RATE_LIMIT_PRESETS,
)
from app.models.notification import Notification, NotificationType
from app.models.notification_preference import NotificationPreference
from app.models.scheduled_email import (
    ScheduledEmail,
    SendTimePreference,
    ScheduledEmailStatus,
)
from app.models.email_inbox import (
    EmailAccount,
    EmailMessage,
    EmailThread,
    StorageQuota,
    EmailDirection,
    EmailSyncStatus,
    EmailAccountType,
)
from app.models.template_marketplace import (
    PublicTemplate,
    TemplateRating,
    TemplateReview,
    TemplateUsageReport,
    TemplateFavorite,
    TemplateCollection,
    TemplateVisibility,
    TemplateLanguage as MarketplaceTemplateLanguage,
)

# Company Intelligence (Skill Matching, Research, Email Drafts)
from app.models.company_intelligence import (
    CompanyProject,
    CompanyResearchCache,
    SkillMatch,
    PersonalizedEmailDraft,
    CandidateSkillProfile,
)

# Follow-Up Sequences & Campaigns
from app.models.follow_up import (
    FollowUpSequence,
    FollowUpStep,
    FollowUpCampaign,
    FollowUpEmail,
    FollowUpLog,
    CandidateProfile,
    ABTest,
    ABTestVariant,
    ABTestAssignment,
)

# Documents (Resume & Info Doc Management)
from app.models.documents import ParsedResume, CompanyInfoDoc

# Warmup Health Monitoring
from app.models.warmup_health import (
    WarmupHealthScore,
    WarmupHealthAlert,
    DomainReputation,
    WarmupMilestone,
)

# Recipient Groups (NEW)
from app.models.recipient import Recipient
from app.models.recipient_group import RecipientGroup, GroupTypeEnum
from app.models.group_recipient import GroupRecipient
from app.models.group_campaign import GroupCampaign, CampaignStatusEnum
from app.models.group_campaign_recipient import (
    GroupCampaignRecipient,
    RecipientStatusEnum,
)

# Enrichment Job Tracking (NEW)
from app.models.enrichment_job import EnrichmentJob, EnrichmentJobStatus

# Merge History Tracking (NEW)
from app.models.merge_history import MergeHistory, MergeStrategyEnum

# ULTRA PRO MAX EXTRACTION ENGINE (GOD-TIER)
from app.models.extraction import (
    ExtractionJob,
    ExtractionResult,
    ExtractionProgress,
    ExtractionTemplate,
    SectorEnum,
    JobStatusEnum,
    ExtractionStageEnum,
)

# ADVANCED EMAIL WARMUP POOL SYSTEM (Smartlead/Instantly Competitor)
from app.models.warmup_pool import (
    WarmupPoolMember,
    WarmupConversation,
    InboxPlacementTest,
    BlacklistStatus,
    WarmupSchedule,
    PoolTierEnum,
    PoolMemberStatusEnum,
    ConversationStatusEnum,
    PlacementResultEnum,
    BlacklistStatusEnum,
)

# ULTRA FOLLOW-UP AI/ML SYSTEM V2.0
from app.models.follow_up_ml import (
    FollowUpPrediction,
    AIGeneratedContent,
    SendTimeAnalytics,
    SequenceBranch,
    ReplyIntent,
    BranchConditionType,
    IntentType,
    AIModelType,
    PredictionConfidence,
)

__all__ = [
    # User/Candidate
    "Candidate",
    "UserRole",
    # Company
    "Company",
    # Application
    "Application",
    "ApplicationStatusEnum",
    "ApplicationType",
    # Application related
    "ApplicationHistory",
    "ApplicationNote",
    "ApplicationAttachment",
    # Email
    "EmailLog",
    "EmailTemplate",
    "EmailLanguage",
    "TemplateCategory",
    # Resume
    "ResumeVersion",
    "ResumeLanguage",
    # Email Warming
    "EmailWarmingConfig",
    "EmailWarmingDailyLog",
    "WarmingStrategyEnum",
    "WarmingStatusEnum",
    "WARMING_SCHEDULES",
    # Rate Limiting
    "RateLimitConfig",
    "RateLimitUsageLog",
    "RateLimitPresetEnum",
    "RateLimitPeriodEnum",
    "RATE_LIMIT_PRESETS",
    # Notifications
    "Notification",
    "NotificationType",
    # Scheduled Emails (Send Time Optimization)
    "ScheduledEmail",
    "SendTimePreference",
    "ScheduledEmailStatus",
    # Email Inbox
    "EmailAccount",
    "EmailMessage",
    "EmailThread",
    "StorageQuota",
    "EmailDirection",
    "EmailSyncStatus",
    "EmailAccountType",
    # Template Marketplace
    "PublicTemplate",
    "TemplateRating",
    "TemplateReview",
    "TemplateUsageReport",
    "TemplateFavorite",
    "TemplateCollection",
    "TemplateVisibility",
    "MarketplaceTemplateLanguage",
    # Company Intelligence
    "CompanyProject",
    "CompanyResearchCache",
    "SkillMatch",
    "PersonalizedEmailDraft",
    "CandidateSkillProfile",
    # Follow-Up Sequences & Campaigns
    "FollowUpSequence",
    "FollowUpStep",
    "FollowUpCampaign",
    "FollowUpEmail",
    "FollowUpLog",
    "CandidateProfile",
    "ABTest",
    "ABTestVariant",
    "ABTestAssignment",
    # Documents
    "ParsedResume",
    "CompanyInfoDoc",
    # Warmup Health
    "WarmupHealthScore",
    "WarmupHealthAlert",
    "DomainReputation",
    "WarmupMilestone",
    # Recipient Groups (NEW)
    "Recipient",
    "RecipientGroup",
    "GroupTypeEnum",
    "GroupRecipient",
    "GroupCampaign",
    "CampaignStatusEnum",
    "GroupCampaignRecipient",
    "RecipientStatusEnum",
    # Enrichment Job Tracking (NEW)
    "EnrichmentJob",
    "EnrichmentJobStatus",
    # Merge History Tracking (NEW)
    "MergeHistory",
    "MergeStrategyEnum",
    # ULTRA PRO MAX EXTRACTION ENGINE
    "ExtractionJob",
    "ExtractionResult",
    "ExtractionProgress",
    "ExtractionTemplate",
    "SectorEnum",
    "JobStatusEnum",
    "ExtractionStageEnum",
    # ADVANCED EMAIL WARMUP POOL SYSTEM
    "WarmupPoolMember",
    "WarmupConversation",
    "InboxPlacementTest",
    "BlacklistStatus",
    "WarmupSchedule",
    "PoolTierEnum",
    "PoolMemberStatusEnum",
    "ConversationStatusEnum",
    "PlacementResultEnum",
    "BlacklistStatusEnum",
    # ULTRA FOLLOW-UP AI/ML SYSTEM V2.0
    "FollowUpPrediction",
    "AIGeneratedContent",
    "SendTimeAnalytics",
    "SequenceBranch",
    "ReplyIntent",
    "BranchConditionType",
    "IntentType",
    "AIModelType",
    "PredictionConfidence",
]
