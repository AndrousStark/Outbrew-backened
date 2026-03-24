"""
Template Marketplace Models - Share and discover successful email templates

Features:
- Public template library
- Template ratings and reviews
- Usage statistics and performance metrics
- Template cloning to personal library
- Categories and tags for discovery
"""

from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Boolean,
    Text, Float, JSON, Enum as SQLEnum, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from enum import Enum
from typing import Optional

from app.core.database import Base


class TemplateVisibility(str, Enum):
    """Template visibility levels"""
    PRIVATE = "private"
    PUBLIC = "public"
    UNLISTED = "unlisted"  # Public but not in browse


class TemplateCategory(str, Enum):
    """Template categories"""
    COLD_OUTREACH = "cold_outreach"
    FOLLOW_UP = "follow_up"
    THANK_YOU = "thank_you"
    NETWORKING = "networking"
    REFERRAL_REQUEST = "referral_request"
    REAPPLICATION = "reapplication"
    INTERVIEW_PREP = "interview_prep"
    OFFER_ACCEPTANCE = "offer_acceptance"
    OFFER_NEGOTIATION = "offer_negotiation"
    OTHER = "other"


class TemplateLanguage(str, Enum):
    """Template language"""
    ENGLISH = "en"
    SPANISH = "es"
    FRENCH = "fr"
    GERMAN = "de"
    JAPANESE = "ja"
    CHINESE = "zh"
    HINDI = "hi"
    PORTUGUESE = "pt"
    OTHER = "other"


class PublicTemplate(Base):
    """
    Public templates shared in the marketplace
    """
    __tablename__ = "public_templates"

    id = Column(Integer, primary_key=True, index=True)

    # Creator
    creator_id = Column(Integer, ForeignKey("candidates.id", ondelete="SET NULL"), index=True)
    creator_name = Column(String(255))  # Cached for display

    # Template info
    title = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    category = Column(SQLEnum(TemplateCategory), nullable=False, index=True)
    language = Column(SQLEnum(TemplateLanguage), default=TemplateLanguage.ENGLISH, index=True)

    # Content
    subject_template = Column(String(500), nullable=False)
    body_template_text = Column(Text, nullable=False)
    body_template_html = Column(Text)

    # Preview
    preview_text = Column(Text)  # First 500 chars
    thumbnail_url = Column(String(500))  # Optional screenshot

    # Metadata
    tags = Column(JSON, default=list)  # ["tech", "cold-email", "engineering"]
    variables = Column(JSON, default=list)  # ["{company_name}", "{recruiter_name}"]

    # Target audience
    target_industry = Column(String(100))  # "Technology", "Finance", etc.
    target_position_level = Column(String(50))  # "Entry", "Mid", "Senior"
    target_role = Column(String(100))  # "Software Engineer", "Product Manager"

    # Visibility
    visibility = Column(SQLEnum(TemplateVisibility), default=TemplateVisibility.PRIVATE, index=True)
    is_featured = Column(Boolean, default=False, index=True)  # Admin-curated
    is_verified = Column(Boolean, default=False)  # Admin-verified quality

    # Performance metrics
    total_clones = Column(Integer, default=0)
    total_uses = Column(Integer, default=0)
    total_views = Column(Integer, default=0)

    # User-reported success metrics
    avg_response_rate = Column(Float, default=0.0)  # Percentage
    avg_rating = Column(Float, default=0.0)  # 0-5 stars
    total_ratings = Column(Integer, default=0)

    # Stats breakdown
    successful_uses = Column(Integer, default=0)  # Users reported positive outcome
    total_opens = Column(Integer, default=0)
    total_clicks = Column(Integer, default=0)
    total_replies = Column(Integer, default=0)

    # Moderation
    is_approved = Column(Boolean, default=False)
    is_flagged = Column(Boolean, default=False)
    flag_reason = Column(Text)
    moderated_by = Column(Integer, ForeignKey("candidates.id", ondelete="SET NULL"))
    moderated_at = Column(DateTime(timezone=True))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    published_at = Column(DateTime(timezone=True), index=True)
    deleted_at = Column(DateTime(timezone=True), index=True)

    # Relationships
    creator = relationship("Candidate", foreign_keys=[creator_id], backref="public_templates")
    moderator = relationship("Candidate", foreign_keys=[moderated_by])
    ratings = relationship("TemplateRating", back_populates="template", cascade="all, delete-orphan")
    reviews = relationship("TemplateReview", back_populates="template", cascade="all, delete-orphan")
    usage_reports = relationship("TemplateUsageReport", back_populates="template", cascade="all, delete-orphan")

    __table_args__ = (
        Index('ix_public_template_category_rating', 'category', 'avg_rating'),
        Index('ix_public_template_featured_published', 'is_featured', 'published_at'),
        Index('ix_public_template_visibility_approved', 'visibility', 'is_approved'),
    )

    @property
    def response_rate_display(self) -> str:
        """Format response rate for display"""
        return f"{self.avg_response_rate:.1f}%"

    @property
    def rating_display(self) -> str:
        """Format rating for display"""
        return f"{self.avg_rating:.1f}/5.0"


class TemplateRating(Base):
    """
    User ratings for public templates
    """
    __tablename__ = "template_ratings"

    id = Column(Integer, primary_key=True, index=True)

    # References
    template_id = Column(Integer, ForeignKey("public_templates.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Rating
    rating = Column(Integer, nullable=False)  # 1-5 stars

    # Usage outcome
    was_successful = Column(Boolean)  # Did they get a response?
    response_time_hours = Column(Integer)  # How long until response?

    # Context
    used_for_industry = Column(String(100))
    used_for_role = Column(String(100))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    template = relationship("PublicTemplate", back_populates="ratings")
    candidate = relationship("Candidate", backref="template_ratings")

    __table_args__ = (
        UniqueConstraint('template_id', 'candidate_id', name='unique_rating_per_user'),
        Index('ix_template_rating_template_created', 'template_id', 'created_at'),
    )


class TemplateReview(Base):
    """
    User reviews for public templates
    """
    __tablename__ = "template_reviews"

    id = Column(Integer, primary_key=True, index=True)

    # References
    template_id = Column(Integer, ForeignKey("public_templates.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Review content
    review_text = Column(Text, nullable=False)
    pros = Column(Text)
    cons = Column(Text)

    # Stats reported by user
    emails_sent = Column(Integer)
    responses_received = Column(Integer)

    # Helpful votes
    helpful_count = Column(Integer, default=0)
    not_helpful_count = Column(Integer, default=0)

    # Moderation
    is_verified_use = Column(Boolean, default=False)  # Admin verified they actually used it
    is_flagged = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    template = relationship("PublicTemplate", back_populates="reviews")
    candidate = relationship("Candidate", backref="template_reviews")

    __table_args__ = (
        UniqueConstraint('template_id', 'candidate_id', name='unique_review_per_user'),
        Index('ix_template_review_template_helpful', 'template_id', 'helpful_count'),
    )


class TemplateUsageReport(Base):
    """
    Anonymous usage statistics for public templates
    """
    __tablename__ = "template_usage_reports"

    id = Column(Integer, primary_key=True, index=True)

    # References
    template_id = Column(Integer, ForeignKey("public_templates.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Usage details
    times_used = Column(Integer, default=1)

    # Performance (aggregated from user's actual emails)
    emails_sent = Column(Integer, default=0)
    emails_opened = Column(Integer, default=0)
    emails_clicked = Column(Integer, default=0)
    emails_replied = Column(Integer, default=0)

    # Success metrics
    got_interview = Column(Boolean, default=False)
    got_response = Column(Boolean, default=False)

    # Context
    industry_used = Column(String(100))
    role_used = Column(String(100))

    # Timestamps
    first_used_at = Column(DateTime(timezone=True), server_default=func.now())
    last_used_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    template = relationship("PublicTemplate", back_populates="usage_reports")
    candidate = relationship("Candidate", backref="template_usage_reports")

    __table_args__ = (
        UniqueConstraint('template_id', 'candidate_id', name='unique_usage_per_user'),
        Index('ix_template_usage_template_used', 'template_id', 'last_used_at'),
    )


class TemplateFavorite(Base):
    """
    User favorites/bookmarks for templates
    """
    __tablename__ = "template_favorites"

    id = Column(Integer, primary_key=True, index=True)

    # References
    template_id = Column(Integer, ForeignKey("public_templates.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Notes
    notes = Column(Text)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    template = relationship("PublicTemplate", backref="favorites")
    candidate = relationship("Candidate", backref="favorite_templates")

    __table_args__ = (
        UniqueConstraint('template_id', 'candidate_id', name='unique_favorite_per_user'),
        Index('ix_template_favorite_candidate_created', 'candidate_id', 'created_at'),
    )


class TemplateCollection(Base):
    """
    Curated collections of templates
    """
    __tablename__ = "template_collections"

    id = Column(Integer, primary_key=True, index=True)

    # Creator (can be admin or user)
    creator_id = Column(Integer, ForeignKey("candidates.id", ondelete="SET NULL"), index=True)
    creator_name = Column(String(255))

    # Collection info
    name = Column(String(255), nullable=False)
    description = Column(Text)
    thumbnail_url = Column(String(500))

    # Templates in collection (JSON array of template IDs)
    template_ids = Column(JSON, default=list)

    # Stats
    total_templates = Column(Integer, default=0)
    total_views = Column(Integer, default=0)
    total_followers = Column(Integer, default=0)

    # Visibility
    is_public = Column(Boolean, default=False)
    is_featured = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    creator = relationship("Candidate", backref="template_collections")

    __table_args__ = (
        Index('ix_template_collection_public_featured', 'is_public', 'is_featured'),
    )


class TemplateVersion(Base):
    """
    Version history for public templates - track changes over time
    """
    __tablename__ = "template_versions"

    id = Column(Integer, primary_key=True, index=True)

    # Template reference
    template_id = Column(Integer, ForeignKey("public_templates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Version info
    version_number = Column(Integer, nullable=False)  # 1, 2, 3, etc.
    version_name = Column(String(100))  # Optional: "v2.0", "Major Update", etc.
    change_description = Column(Text)  # What changed in this version

    # Content snapshot
    subject_template = Column(String(500), nullable=False)
    body_template_text = Column(Text, nullable=False)
    body_template_html = Column(Text)

    # Metadata snapshot
    tags = Column(JSON, default=list)
    variables = Column(JSON, default=list)

    # Who made the change
    changed_by_id = Column(Integer, ForeignKey("candidates.id", ondelete="SET NULL"))
    changed_by_name = Column(String(255))  # Cached

    # Statistics (at time of version creation)
    total_uses_at_version = Column(Integer, default=0)
    avg_rating_at_version = Column(Float, default=0.0)

    # Version status
    is_current = Column(Boolean, default=False, index=True)  # Only one current version per template
    is_published = Column(Boolean, default=True)  # Can save draft versions

    # Diff summary (for display)
    changes_summary = Column(JSON, default=dict)
    # Example: {"subject_changed": true, "body_changed": true, "lines_added": 5, "lines_removed": 2}

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    template = relationship("PublicTemplate", backref="versions")
    changed_by = relationship("Candidate")

    __table_args__ = (
        Index('ix_template_version_template_id', 'template_id'),
        Index('ix_template_version_template_version', 'template_id', 'version_number', unique=True),
        Index('ix_template_version_current', 'template_id', 'is_current'),
    )

    @property
    def version_label(self) -> str:
        """Human-readable version label"""
        if self.version_name:
            return f"v{self.version_number} - {self.version_name}"
        return f"v{self.version_number}"


class TemplateAnalyticsEvent(Base):
    """
    Track individual analytics events for templates

    Captures every interaction with templates for detailed analysis
    """
    __tablename__ = "template_analytics_events"

    id = Column(Integer, primary_key=True, index=True)

    # Template reference
    template_id = Column(Integer, ForeignKey("public_templates.id", ondelete="CASCADE"), nullable=False, index=True)
    template_version_id = Column(Integer, ForeignKey("template_versions.id", ondelete="SET NULL"))

    # User who performed action
    user_id = Column(Integer, ForeignKey("candidates.id", ondelete="SET NULL"), index=True)

    # Event info
    event_type = Column(String(50), nullable=False, index=True)
    # Event types: view, clone, use, rate, favorite, share, report

    # Event metadata
    event_metadata = Column(JSON, default=dict)
    # Example: {"source": "search", "category": "cold_outreach", "rating": 5}

    # Session tracking
    session_id = Column(String(100))  # Track user sessions
    referrer = Column(String(500))  # Where did they come from?

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    # Relationships
    template = relationship("PublicTemplate")
    template_version = relationship("TemplateVersion")
    user = relationship("Candidate")

    __table_args__ = (
        Index('ix_template_analytics_template_event', 'template_id', 'event_type'),
        Index('ix_template_analytics_user_event', 'user_id', 'event_type'),
        Index('ix_template_analytics_created', 'created_at'),
    )


class TemplatePerformanceSnapshot(Base):
    """
    Periodic snapshots of template performance metrics

    Allows tracking trends over time without scanning all events
    """
    __tablename__ = "template_performance_snapshots"

    id = Column(Integer, primary_key=True, index=True)

    # Template reference
    template_id = Column(Integer, ForeignKey("public_templates.id", ondelete="CASCADE"), nullable=False, index=True)
    template_version_id = Column(Integer, ForeignKey("template_versions.id", ondelete="SET NULL"))

    # Time period
    snapshot_date = Column(DateTime(timezone=True), nullable=False, index=True)
    period_type = Column(String(20), nullable=False)  # daily, weekly, monthly
    period_start = Column(DateTime(timezone=True), nullable=False)
    period_end = Column(DateTime(timezone=True), nullable=False)

    # Engagement metrics
    total_views = Column(Integer, default=0)
    unique_viewers = Column(Integer, default=0)
    total_clones = Column(Integer, default=0)
    total_uses = Column(Integer, default=0)
    total_favorites = Column(Integer, default=0)

    # Rating metrics
    new_ratings = Column(Integer, default=0)
    new_reviews = Column(Integer, default=0)
    avg_rating_period = Column(Float)
    cumulative_avg_rating = Column(Float)

    # Performance metrics (from actual email campaigns using this template)
    emails_sent = Column(Integer, default=0)
    emails_opened = Column(Integer, default=0)
    emails_replied = Column(Integer, default=0)
    open_rate = Column(Float)
    reply_rate = Column(Float)

    # Engagement rates
    view_to_clone_rate = Column(Float)  # What % of viewers clone?
    clone_to_use_rate = Column(Float)   # What % of cloners actually use?
    view_to_favorite_rate = Column(Float)

    # Growth metrics
    views_growth_pct = Column(Float)  # % change from previous period
    uses_growth_pct = Column(Float)
    rating_growth_pct = Column(Float)

    # Rankings
    rank_in_category = Column(Integer)  # Rank within its category
    rank_overall = Column(Integer)      # Overall marketplace rank

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    template = relationship("PublicTemplate")
    template_version = relationship("TemplateVersion")

    __table_args__ = (
        Index('ix_template_snapshot_template_date', 'template_id', 'snapshot_date'),
        Index('ix_template_snapshot_period', 'period_type', 'snapshot_date'),
        UniqueConstraint('template_id', 'period_type', 'snapshot_date', name='unique_template_period_snapshot'),
    )


class TemplateABTestResult(Base):
    """
    A/B test results for template variations

    Tracks comparative performance when testing different versions
    """
    __tablename__ = "template_ab_test_results"

    id = Column(Integer, primary_key=True, index=True)

    # Template reference
    template_id = Column(Integer, ForeignKey("public_templates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Test info
    test_name = Column(String(200), nullable=False)
    test_description = Column(Text)
    test_hypothesis = Column(Text)

    # Variants being tested
    variant_a_version_id = Column(Integer, ForeignKey("template_versions.id", ondelete="SET NULL"))
    variant_b_version_id = Column(Integer, ForeignKey("template_versions.id", ondelete="SET NULL"))

    # What's being tested
    test_dimension = Column(String(50))  # subject, body, tone, length, cta

    # Sample sizes
    variant_a_uses = Column(Integer, default=0)
    variant_b_uses = Column(Integer, default=0)

    # Performance metrics
    variant_a_reply_rate = Column(Float)
    variant_b_reply_rate = Column(Float)
    variant_a_open_rate = Column(Float)
    variant_b_open_rate = Column(Float)

    # Statistical analysis
    p_value = Column(Float)  # Statistical significance
    confidence_level = Column(Float, default=0.95)
    is_significant = Column(Boolean, default=False)
    winner = Column(String(10))  # "A", "B", or "inconclusive"

    # Effect size
    relative_improvement = Column(Float)  # % improvement of winner
    absolute_difference = Column(Float)   # Absolute difference in rates

    # Test period
    started_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True))

    # Status
    status = Column(String(20), default="running")  # running, completed, cancelled

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    template = relationship("PublicTemplate")
    variant_a = relationship("TemplateVersion", foreign_keys=[variant_a_version_id])
    variant_b = relationship("TemplateVersion", foreign_keys=[variant_b_version_id])

    __table_args__ = (
        Index('ix_template_abtest_template_status', 'template_id', 'status'),
        Index('ix_template_abtest_completed', 'completed_at'),
    )
