"""
Follow-Up Sequence Models - Comprehensive Automated Follow-Up System

Features:
- Reusable sequence templates with multiple steps
- Smart email generation using candidate + company + original email context
- Auto-mode with user approval flow
- Draft management for user editing
- Reply detection and auto-stop
- Full tracking and analytics
"""

from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Boolean,
    Text, Float, JSON, Enum as SQLEnum, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Any, Optional

from app.core.database import Base


# ============= ENUMS =============

class SequenceStatus(str, Enum):
    """Status of a follow-up sequence template"""
    ACTIVE = "active"
    DRAFT = "draft"
    ARCHIVED = "archived"


class CampaignStatus(str, Enum):
    """Status of an active follow-up campaign"""
    PENDING_APPROVAL = "pending_approval"  # Auto-mode waiting for user approval
    ACTIVE = "active"                      # Running automatically
    PAUSED = "paused"                      # Paused by user
    MANUAL = "manual"                      # User sending manually
    COMPLETED = "completed"                # All steps sent
    REPLIED = "replied"                    # Stopped - recipient replied
    CANCELLED = "cancelled"                # Cancelled by user
    BOUNCED = "bounced"                    # Stopped - email bounced


class FollowUpEmailStatus(str, Enum):
    """Status of individual follow-up emails"""
    DRAFT = "draft"                  # Generated, pending user review
    APPROVED = "approved"            # User approved, waiting to send
    SCHEDULED = "scheduled"          # Scheduled for sending
    SENDING = "sending"              # Currently being sent
    SENT = "sent"                    # Successfully sent
    FAILED = "failed"                # Failed to send
    SKIPPED = "skipped"              # User skipped this step
    EDITED = "edited"                # User edited and saved


class FollowUpTone(str, Enum):
    """Tone options for follow-up emails"""
    PROFESSIONAL = "professional"
    FRIENDLY = "friendly"
    PERSISTENT = "persistent"
    VALUE_ADD = "value_add"
    BREAKUP = "breakup"
    URGENT = "urgent"


class FollowUpStrategy(str, Enum):
    """Strategy for the follow-up step"""
    SOFT_BUMP = "soft_bump"              # Gentle reminder
    ADD_VALUE = "add_value"              # Share additional value/info
    SOCIAL_PROOF = "social_proof"        # Case studies, testimonials
    QUESTION = "question"                 # Ask engaging question
    REFERENCE_ORIGINAL = "reference_original"  # Reference original email
    BREAKUP = "breakup"                   # Final "closing the loop" email
    CUSTOM = "custom"                     # User's custom content


# ============= TEMPLATE CONSTANTS =============

FOLLOW_UP_TEMPLATES = {
    "soft_bump": {
        "name": "Gentle Reminder",
        "description": "A friendly bump referencing the original email",
        "subject_prefix": "Re: ",
        "tone": FollowUpTone.FRIENDLY,
        "typical_delay_days": 2
    },
    "add_value": {
        "name": "Value Addition",
        "description": "Share relevant article, project, or insight",
        "subject_prefix": "Quick thought: ",
        "tone": FollowUpTone.VALUE_ADD,
        "typical_delay_days": 3
    },
    "social_proof": {
        "name": "Social Proof",
        "description": "Share achievements, testimonials, or portfolio highlights",
        "subject_prefix": "Re: ",
        "tone": FollowUpTone.PROFESSIONAL,
        "typical_delay_days": 4
    },
    "question": {
        "name": "Engaging Question",
        "description": "Ask a thought-provoking question related to their work",
        "subject_prefix": "Quick question about ",
        "tone": FollowUpTone.FRIENDLY,
        "typical_delay_days": 3
    },
    "breakup": {
        "name": "Breakup Email",
        "description": "Final email closing the loop professionally",
        "subject_prefix": "Closing the loop: ",
        "tone": FollowUpTone.BREAKUP,
        "typical_delay_days": 5
    }
}

DEFAULT_SEQUENCE_PRESETS = [
    {
        "name": "Standard Job Application",
        "description": "4-email sequence over 14 days - balanced and professional",
        "steps": [
            {"delay_days": 2, "strategy": "soft_bump", "tone": "friendly"},
            {"delay_days": 3, "strategy": "add_value", "tone": "value_add"},
            {"delay_days": 4, "strategy": "social_proof", "tone": "professional"},
            {"delay_days": 5, "strategy": "breakup", "tone": "breakup"}
        ]
    },
    {
        "name": "Aggressive Outreach",
        "description": "6-email sequence over 10 days - for time-sensitive opportunities",
        "steps": [
            {"delay_days": 1, "strategy": "soft_bump", "tone": "friendly"},
            {"delay_days": 2, "strategy": "add_value", "tone": "value_add"},
            {"delay_days": 2, "strategy": "question", "tone": "friendly"},
            {"delay_days": 2, "strategy": "social_proof", "tone": "professional"},
            {"delay_days": 2, "strategy": "reference_original", "tone": "persistent"},
            {"delay_days": 1, "strategy": "breakup", "tone": "breakup"}
        ]
    },
    {
        "name": "Gentle Persistence",
        "description": "3-email sequence over 21 days - respectful long-term follow-up",
        "steps": [
            {"delay_days": 5, "strategy": "soft_bump", "tone": "friendly"},
            {"delay_days": 7, "strategy": "add_value", "tone": "value_add"},
            {"delay_days": 9, "strategy": "breakup", "tone": "breakup"}
        ]
    },
    {
        "name": "Value-First Approach",
        "description": "5-email sequence focusing on providing value",
        "steps": [
            {"delay_days": 2, "strategy": "add_value", "tone": "value_add"},
            {"delay_days": 3, "strategy": "add_value", "tone": "value_add"},
            {"delay_days": 4, "strategy": "social_proof", "tone": "professional"},
            {"delay_days": 3, "strategy": "question", "tone": "friendly"},
            {"delay_days": 5, "strategy": "breakup", "tone": "breakup"}
        ]
    }
]


# ============= MODELS =============

class FollowUpSequence(Base):
    """
    Reusable follow-up sequence template.

    Users can create and save sequences like:
    - "Standard Job Application" (4 emails over 14 days)
    - "Aggressive Outreach" (6 emails over 10 days)
    - Custom sequences with any configuration
    """
    __tablename__ = "follow_up_sequences"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)

    # Sequence info
    name = Column(String(200), nullable=False)
    description = Column(Text)
    status = Column(SQLEnum(SequenceStatus), default=SequenceStatus.ACTIVE)

    # Settings
    is_system_preset = Column(Boolean, default=False)  # Built-in templates
    stop_on_reply = Column(Boolean, default=True)
    stop_on_bounce = Column(Boolean, default=True)
    use_threading = Column(Boolean, default=True)  # Send as reply thread
    respect_business_hours = Column(Boolean, default=True)  # Only send 9-5
    business_hours_start = Column(Integer, default=9)  # 9 AM
    business_hours_end = Column(Integer, default=18)  # 6 PM (18:00)

    # Personalization settings
    include_candidate_links = Column(Boolean, default=True)  # Add LinkedIn, GitHub, etc.
    include_portfolio = Column(Boolean, default=True)  # Reference projects
    include_signature = Column(Boolean, default=True)
    custom_signature = Column(Text)

    # Preferred sending time
    preferred_send_hour = Column(Integer, default=10)  # 10 AM default
    preferred_timezone = Column(String(50), default="UTC")

    # Stats
    times_used = Column(Integer, default=0)
    total_campaigns = Column(Integer, default=0)
    successful_replies = Column(Integer, default=0)
    reply_rate = Column(Float, default=0.0)

    # ============= AI/ML FIELDS (ULTRA V2.0) =============

    # Branching support
    has_branches = Column(Boolean, default=False)  # Does this sequence have conditional branches?

    # AI Copilot
    ai_copilot_generated = Column(Boolean, default=False)  # Was this sequence created by AI Copilot?
    ai_generation_prompt = Column(Text)  # Original user prompt that created this sequence

    # Performance score (ML-calculated)
    performance_score = Column(Float)  # 0-1, based on historical success

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    candidate = relationship("Candidate", backref="follow_up_sequences")
    steps = relationship("FollowUpStep", back_populates="sequence",
                        cascade="all, delete-orphan", order_by="FollowUpStep.step_number")
    campaigns = relationship("FollowUpCampaign", back_populates="sequence")


class FollowUpStep(Base):
    """
    A single step in a follow-up sequence.

    Each step defines:
    - When to send (delay_days after previous)
    - What strategy to use
    - Content template with placeholders
    """
    __tablename__ = "follow_up_steps"

    id = Column(Integer, primary_key=True, index=True)
    sequence_id = Column(Integer, ForeignKey("follow_up_sequences.id", ondelete="CASCADE"), nullable=False, index=True)

    # Step position
    step_number = Column(Integer, nullable=False)  # 1, 2, 3, ...

    # Timing
    delay_days = Column(Integer, nullable=False)  # Days after previous step
    delay_hours = Column(Integer, default=0)  # Additional hours

    # Strategy & Tone
    strategy = Column(SQLEnum(FollowUpStrategy), default=FollowUpStrategy.SOFT_BUMP)
    tone = Column(SQLEnum(FollowUpTone), default=FollowUpTone.PROFESSIONAL)

    # Content templates (with placeholders)
    subject_template = Column(String(500))  # Can use {original_subject}, {company_name}, etc.
    body_template = Column(Text, nullable=False)

    # AI Generation hints
    generation_hints = Column(JSON, default=dict)  # {"focus": "skills", "mention_project": true}

    # What to include
    include_original_context = Column(Boolean, default=True)
    include_value_proposition = Column(Boolean, default=False)
    include_portfolio_link = Column(Boolean, default=False)
    include_call_to_action = Column(Boolean, default=True)

    # Status
    is_active = Column(Boolean, default=True)

    # Relationships
    sequence = relationship("FollowUpSequence", back_populates="steps")

    __table_args__ = (
        UniqueConstraint('sequence_id', 'step_number', name='unique_step_in_sequence'),
    )


class FollowUpCampaign(Base):
    """
    An active follow-up campaign for a specific recipient.

    Links a sequence to either:
    - An application (traditional job application follow-up)
    - A group campaign recipient (bulk campaign follow-up)

    Tracks progress through the sequence steps.
    """
    __tablename__ = "follow_up_campaigns"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys - either application_id OR group_campaign_recipient_id must be set
    sequence_id = Column(Integer, ForeignKey("follow_up_sequences.id", ondelete="SET NULL"), nullable=True)
    application_id = Column(Integer, ForeignKey("applications.id", ondelete="CASCADE"), nullable=True)
    group_campaign_recipient_id = Column(Integer, ForeignKey("group_campaign_recipients.id", ondelete="CASCADE"), nullable=True, index=True)
    group_campaign_id = Column(Integer, ForeignKey("group_campaigns.id", ondelete="CASCADE"), nullable=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)

    # Campaign state
    status = Column(SQLEnum(CampaignStatus), default=CampaignStatus.PENDING_APPROVAL, index=True)
    is_auto_mode = Column(Boolean, default=False)  # True = send automatically
    auto_mode_approved = Column(Boolean, default=False)  # User confirmed auto-mode
    auto_mode_approved_at = Column(DateTime)

    # Progress tracking
    current_step = Column(Integer, default=0)  # 0 = initial email sent, 1 = first follow-up, etc.
    total_steps = Column(Integer, default=0)

    # Scheduling
    next_send_date = Column(DateTime, index=True)
    last_sent_date = Column(DateTime)

    # Context from original email (cached for reference)
    original_email_context = Column(JSON, default=dict)
    """
    {
        "subject": "Original subject line",
        "body_preview": "First 500 chars...",
        "sent_at": "2025-01-01T10:00:00",
        "template_used": "template_name",
        "personalization_used": {...}
    }
    """

    # Company context (cached)
    company_context = Column(JSON, default=dict)
    """
    {
        "name": "Company Name",
        "industry": "Tech",
        "position": "Software Engineer",
        "tech_stack": ["Python", "React"],
        "about": "Company description...",
        "research_highlights": [...]
    }
    """

    # Candidate context (cached for quick access)
    candidate_context = Column(JSON, default=dict)
    """
    {
        "name": "John Doe",
        "email": "john@email.com",
        "phone": "+1234567890",
        "linkedin": "linkedin.com/in/johndoe",
        "github": "github.com/johndoe",
        "website": "johndoe.dev",
        "skills": ["Python", "React"],
        "experience_summary": "...",
        "portfolio_highlights": [...]
    }
    """

    # Reply tracking
    reply_detected = Column(Boolean, default=False)
    reply_detected_at = Column(DateTime)
    reply_source = Column(String(50))  # "manual", "auto", "webhook"

    # Stats
    emails_sent = Column(Integer, default=0)
    emails_opened = Column(Integer, default=0)
    emails_clicked = Column(Integer, default=0)

    # Notes
    user_notes = Column(Text)
    cancellation_reason = Column(String(500))

    # ============= AI/ML FIELDS (ULTRA V2.0) =============

    # ML Predictions
    reply_probability = Column(Float)  # 0-1, predicted likelihood of reply
    priority_score = Column(Integer, default=50)  # 0-100, higher = more important to send

    # Out-of-office handling
    is_ooo_paused = Column(Boolean, default=False)  # Paused due to OOO detection
    ooo_return_date = Column(DateTime)  # When to auto-resume

    # AI Reply Agent
    auto_reply_enabled = Column(Boolean, default=False)  # Allow AI to auto-respond

    # Branching history (track which branches were taken)
    branch_history = Column(JSON, default=list)
    """
    [
        {
            "from_step": 1,
            "to_step": 3,
            "condition": "opened_not_replied",
            "triggered_at": "2025-01-15T10:00:00"
        }
    ]
    """

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime)

    # Properties for API response compatibility
    @property
    def total_emails_sent(self) -> int:
        """Alias for emails_sent to match API response schema"""
        return self.emails_sent or 0

    @property
    def total_emails_opened(self) -> int:
        """Alias for emails_opened to match API response schema"""
        return self.emails_opened or 0

    @property
    def total_replies(self) -> int:
        """Count of replies (1 if reply detected, 0 otherwise)"""
        return 1 if self.reply_detected else 0

    @property
    def started_at(self) -> datetime:
        """Alias for created_at to match API response schema"""
        return self.created_at

    @property
    def last_reply_at(self) -> Optional[datetime]:
        """Alias for reply_detected_at to match API response schema"""
        return self.reply_detected_at

    @property
    def next_email_at(self) -> Optional[datetime]:
        """Alias for next_send_date to match API response schema"""
        return self.next_send_date

    # Relationships
    sequence = relationship("FollowUpSequence", back_populates="campaigns")
    application = relationship("Application", backref="follow_up_campaigns")
    group_campaign_recipient = relationship("GroupCampaignRecipient", backref="follow_up_campaigns")
    group_campaign = relationship("GroupCampaign", backref="follow_up_campaigns")
    candidate = relationship("Candidate", backref="follow_up_campaigns")
    emails = relationship("FollowUpEmail", back_populates="campaign",
                         cascade="all, delete-orphan", order_by="FollowUpEmail.step_number")
    logs = relationship("FollowUpLog", back_populates="campaign",
                       cascade="all, delete-orphan", order_by="FollowUpLog.created_at.desc()")


class FollowUpEmail(Base):
    """
    A generated follow-up email (draft or sent).

    Each email can be:
    - Auto-generated based on templates and context
    - Edited by user before sending
    - Completely custom-written by user
    """
    __tablename__ = "follow_up_emails"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys
    campaign_id = Column(Integer, ForeignKey("follow_up_campaigns.id", ondelete="CASCADE"), nullable=False, index=True)
    step_id = Column(Integer, ForeignKey("follow_up_steps.id", ondelete="SET NULL"), nullable=True)
    email_log_id = Column(Integer, ForeignKey("email_logs.id", ondelete="SET NULL"), nullable=True)

    # Position
    step_number = Column(Integer, nullable=False)

    # Status
    status = Column(SQLEnum(FollowUpEmailStatus), default=FollowUpEmailStatus.DRAFT, index=True)

    # Email content
    subject = Column(String(500), nullable=False)
    body_text = Column(Text, nullable=False)  # Plain text version
    body_html = Column(Text)  # HTML version

    # Generation info
    is_auto_generated = Column(Boolean, default=True)
    is_user_edited = Column(Boolean, default=False)
    is_custom_written = Column(Boolean, default=False)  # User wrote from scratch

    # ============= AI/ML FIELDS (ULTRA V2.0) =============

    # AI generation tracking
    ai_generated = Column(Boolean, default=False)  # Was this generated by AI Copilot?
    ai_model = Column(String(50))  # "gpt-4", "gpt-4o", etc.
    ai_content_id = Column(Integer, ForeignKey("ai_generated_content.id", ondelete="SET NULL"))

    # ML predictions for this email
    predicted_open_rate = Column(Float)  # 0-1, predicted open rate
    predicted_reply_rate = Column(Float)  # 0-1, predicted reply rate
    optimal_send_time = Column(DateTime)  # ML-predicted optimal time

    # Spintax support (for deliverability)
    spintax_original = Column(Text)  # Original with spintax: {Hi|Hello|Hey}
    spintax_rendered = Column(Text)  # Rendered version that was sent

    # Original generated content (preserved if user edits)
    original_subject = Column(String(500))
    original_body = Column(Text)

    # Strategy used
    strategy_used = Column(SQLEnum(FollowUpStrategy))
    tone_used = Column(SQLEnum(FollowUpTone))

    # Personalization details
    personalization_data = Column(JSON, default=dict)
    """
    {
        "placeholders_replaced": ["company_name", "candidate_name", ...],
        "links_included": {"linkedin": true, "github": true},
        "portfolio_mentioned": true,
        "skills_highlighted": ["Python", "React"],
        "company_reference": "Their recent project X..."
    }
    """

    # Scheduling
    scheduled_for = Column(DateTime)
    timezone = Column(String(50), default="UTC")

    # Sending results
    sent_at = Column(DateTime)
    delivered_at = Column(DateTime)
    opened_at = Column(DateTime)
    clicked_at = Column(DateTime)
    replied_at = Column(DateTime)  # When recipient replied to this email

    # Error tracking
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    # User approval tracking
    approved_at = Column(DateTime)
    approved_by = Column(String(100))  # "auto" or user info

    # Edit tracking
    edit_count = Column(Integer, default=0)
    last_edited_at = Column(DateTime)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    campaign = relationship("FollowUpCampaign", back_populates="emails")
    step = relationship("FollowUpStep")
    email_log = relationship("EmailLog")


class FollowUpLog(Base):
    """
    Activity log for follow-up campaigns.

    Tracks all actions: creation, edits, sends, replies, etc.
    """
    __tablename__ = "follow_up_logs"

    id = Column(Integer, primary_key=True, index=True)
    campaign_id = Column(Integer, ForeignKey("follow_up_campaigns.id", ondelete="CASCADE"), nullable=False, index=True)

    # Log details
    action = Column(String(100), nullable=False)
    """
    Actions: created, approved, step_generated, step_edited, step_sent,
             step_failed, reply_detected, paused, resumed, cancelled, completed
    """

    step_number = Column(Integer)  # Which step this relates to (if applicable)

    # Details
    details = Column(JSON, default=dict)
    """
    {
        "old_status": "draft",
        "new_status": "sent",
        "changes_made": {...},
        "error_details": {...}
    }
    """

    # Actor
    actor = Column(String(100), default="system")  # "system", "user", "scheduler"

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    campaign = relationship("FollowUpCampaign", back_populates="logs")


class CandidateProfile(Base):
    """
    Extended candidate profile for follow-up personalization.

    Stores additional contact info and portfolio details used in follow-ups.
    """
    __tablename__ = "candidate_profiles"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, unique=True)

    # Contact info
    phone_number = Column(String(50))
    personal_email = Column(String(255))  # Different from login email

    # Social & Professional links
    linkedin_url = Column(String(500))
    github_url = Column(String(500))
    twitter_url = Column(String(500))
    website_url = Column(String(500))
    portfolio_url = Column(String(500))
    behance_url = Column(String(500))
    dribbble_url = Column(String(500))
    medium_url = Column(String(500))
    stackoverflow_url = Column(String(500))
    other_links = Column(JSON, default=list)  # [{name, url}, ...]

    # Professional summary
    headline = Column(String(255))  # "Senior Software Engineer | React & Node.js"
    bio = Column(Text)  # Short professional bio
    years_experience = Column(Integer)
    current_company = Column(String(255))
    current_title = Column(String(255))

    # Portfolio highlights (for follow-up mentions)
    portfolio_projects = Column(JSON, default=list)
    """
    [
        {
            "name": "Project Name",
            "description": "Brief description",
            "url": "project-url.com",
            "technologies": ["React", "Node"],
            "highlights": ["50k users", "Featured on HN"]
        }
    ]
    """

    # Achievements (for social proof emails)
    achievements = Column(JSON, default=list)
    """
    [
        {
            "title": "AWS Certified",
            "description": "Solutions Architect",
            "date": "2024",
            "url": "credential-url"
        }
    ]
    """

    # Value propositions (pre-written for follow-ups)
    value_propositions = Column(JSON, default=list)
    """
    [
        "Reduced deployment time by 60% at previous company",
        "Led team of 5 engineers to deliver project ahead of schedule"
    ]
    """

    # Custom signature for emails
    email_signature = Column(Text)
    signature_html = Column(Text)

    # Follow-up preferences
    preferred_follow_up_tone = Column(SQLEnum(FollowUpTone), default=FollowUpTone.PROFESSIONAL)
    default_sequence_id = Column(Integer, ForeignKey("follow_up_sequences.id", ondelete="SET NULL"))

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    candidate = relationship("Candidate", backref="profile", uselist=False)
    default_sequence = relationship("FollowUpSequence")


# ============= A/B TESTING SYSTEM =============

class ABTestStatus(str, Enum):
    """Status of an A/B test"""
    DRAFT = "draft"              # Test created but not started
    RUNNING = "running"          # Currently collecting data
    PAUSED = "paused"           # Temporarily paused
    COMPLETED = "completed"      # Enough data, ready for analysis
    WINNER_SELECTED = "winner_selected"  # Winner chosen
    CANCELLED = "cancelled"      # Test cancelled


class ABTest(Base):
    """
    A/B test configuration for testing different follow-up sequences

    Tests can compare:
    - Different sequences (3-email vs 5-email)
    - Different tones (friendly vs professional)
    - Different timing (2-day vs 3-day intervals)
    - Different subject lines
    - Different body content
    """
    __tablename__ = "ab_tests"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)

    # Test info
    name = Column(String(200), nullable=False)
    description = Column(Text)
    hypothesis = Column(Text)  # What are we testing and why?

    # Status
    status = Column(SQLEnum(ABTestStatus), default=ABTestStatus.DRAFT, index=True)

    # Test configuration
    allocation_strategy = Column(String(50), default="even")  # even, weighted, contextual
    minimum_sample_size = Column(Integer, default=30)  # Min campaigns per variant
    confidence_level = Column(Float, default=0.95)  # 95% confidence

    # Success metrics (primary and secondary)
    primary_metric = Column(String(50), default="reply_rate")  # reply_rate, open_rate, click_rate
    secondary_metrics = Column(JSON, default=list)  # ["open_rate", "time_to_reply"]

    # Winner selection
    winner_variant_id = Column(Integer, ForeignKey("ab_test_variants.id", ondelete="SET NULL"))
    winner_selected_at = Column(DateTime)
    winner_selection_method = Column(String(50))  # auto, manual

    # Statistics
    total_campaigns = Column(Integer, default=0)
    statistical_significance = Column(Float)  # p-value
    confidence_interval = Column(JSON, default=dict)  # {"lower": 0.15, "upper": 0.25}

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    candidate = relationship("Candidate", backref="ab_tests")
    variants = relationship("ABTestVariant", back_populates="test",
                          cascade="all, delete-orphan", foreign_keys="ABTestVariant.test_id")
    winner_variant = relationship("ABTestVariant", foreign_keys=[winner_variant_id],
                                 post_update=True, uselist=False)
    assignments = relationship("ABTestAssignment", back_populates="test")


class ABTestVariant(Base):
    """
    A variant (version) in an A/B test

    Each variant represents a different approach to test
    """
    __tablename__ = "ab_test_variants"

    id = Column(Integer, primary_key=True, index=True)
    test_id = Column(Integer, ForeignKey("ab_tests.id", ondelete="CASCADE"), nullable=False, index=True)
    sequence_id = Column(Integer, ForeignKey("follow_up_sequences.id", ondelete="SET NULL"))

    # Variant info
    name = Column(String(100), nullable=False)  # "Control", "Variant A", "Shorter Version"
    description = Column(Text)
    is_control = Column(Boolean, default=False)  # Control group baseline

    # Allocation
    allocation_weight = Column(Float, default=1.0)  # 1.0 = equal, 2.0 = 2x traffic

    # Performance metrics
    total_campaigns = Column(Integer, default=0)
    total_emails_sent = Column(Integer, default=0)
    total_opens = Column(Integer, default=0)
    total_clicks = Column(Integer, default=0)
    total_replies = Column(Integer, default=0)

    # Calculated rates (cached for performance)
    open_rate = Column(Float, default=0.0)
    click_rate = Column(Float, default=0.0)
    reply_rate = Column(Float, default=0.0)

    # Timing metrics
    avg_time_to_reply_hours = Column(Float)  # Average time to get reply
    avg_emails_to_reply = Column(Float)  # How many emails before reply

    # Statistical analysis
    conversion_rate = Column(Float, default=0.0)  # Primary metric rate
    std_error = Column(Float)  # Standard error
    confidence_interval_lower = Column(Float)
    confidence_interval_upper = Column(Float)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    test = relationship("ABTest", back_populates="variants", foreign_keys=[test_id])
    sequence = relationship("FollowUpSequence")
    assignments = relationship("ABTestAssignment", back_populates="variant")

    def calculate_rates(self):
        """Calculate and cache performance rates"""
        if self.total_emails_sent > 0:
            self.open_rate = self.total_opens / self.total_emails_sent
            self.click_rate = self.total_clicks / self.total_emails_sent

        if self.total_campaigns > 0:
            self.reply_rate = self.total_replies / self.total_campaigns
            self.conversion_rate = self.reply_rate  # Primary metric


class ABTestAssignment(Base):
    """
    Tracks which campaign was assigned to which variant

    This ensures consistent variant assignment per campaign
    """
    __tablename__ = "ab_test_assignments"

    id = Column(Integer, primary_key=True, index=True)
    test_id = Column(Integer, ForeignKey("ab_tests.id", ondelete="CASCADE"), nullable=False, index=True)
    variant_id = Column(Integer, ForeignKey("ab_test_variants.id", ondelete="CASCADE"), nullable=False, index=True)
    campaign_id = Column(Integer, ForeignKey("follow_up_campaigns.id", ondelete="CASCADE"), nullable=False, index=True)

    # Assignment details
    assigned_at = Column(DateTime, default=datetime.utcnow)
    assignment_method = Column(String(50))  # random, weighted, contextual

    # Outcome tracking (denormalized for fast queries)
    has_reply = Column(Boolean, default=False)
    reply_received_at = Column(DateTime)
    emails_sent = Column(Integer, default=0)
    emails_opened = Column(Integer, default=0)
    time_to_reply_hours = Column(Float)

    # Relationships
    test = relationship("ABTest", back_populates="assignments")
    variant = relationship("ABTestVariant", back_populates="assignments")
    campaign = relationship("FollowUpCampaign", backref="ab_test_assignment", uselist=False)

    __table_args__ = (
        UniqueConstraint('campaign_id', name='unique_campaign_assignment'),
        Index('ix_ab_test_assignment_test_variant', 'test_id', 'variant_id'),
    )
