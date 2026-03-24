"""
MobiAdz Extraction Job - Database Models
Persists extraction jobs, contacts, and live feed to survive server restarts.
"""

from sqlalchemy import (
    Column, Integer, String, Text, Float, Boolean, DateTime, JSON,
    ForeignKey, Index
)
from sqlalchemy.orm import relationship
from datetime import datetime

from app.core.database import Base


class MobiAdzJob(Base):
    """
    MobiAdz extraction job - tracks the full lifecycle of a data extraction.
    Replaces the in-memory active_jobs dict for persistence.
    """
    __tablename__ = "mobiadz_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(36), unique=True, nullable=False, index=True)  # UUID

    # Status
    status = Column(String(20), default="pending", nullable=False, index=True)
    # pending, running, completed, failed, cancelled

    # Configuration (stored as JSON for flexibility)
    config = Column(JSON, default={})
    # demographics, categories, max_companies, scrape_depth, etc.

    # Progress tracking (updated during extraction)
    progress = Column(JSON, default={
        "stage": "initializing",
        "stage_progress": 0,
        "total_progress": 0,
        "message": "Starting..."
    })

    # Stats (updated during extraction)
    stats = Column(JSON, default={
        "apps_found": 0,
        "companies_found": 0,
        "emails_found": 0,
        "emails_verified": 0,
        "pages_scraped": 0,
        "api_calls": 0,
        "bloom_filter_hits": 0,
        "cache_hits": 0,
        "nlp_entities_extracted": 0,
        "email_permutations_generated": 0,
        "osint_leadership_found": 0,
        "osint_employees_found": 0,
        "osint_phones_found": 0,
        "osint_social_profiles_found": 0
    })

    # Live contacts feed (JSON array, kept for completed jobs)
    live_contacts = Column(JSON, default=[])

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    contacts = relationship(
        "MobiAdzContact",
        back_populates="job",
        cascade="all, delete-orphan",
        lazy="dynamic"
    )

    def to_dict(self):
        """Convert job to dict for API response."""
        return {
            "job_id": self.job_id,
            "status": self.status,
            "progress": self.progress or {},
            "stats": self.stats or {},
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "results_count": self.contacts.count() if self.contacts else 0,
            "live_contacts": self.live_contacts or [],
            "config": self.config or {},
        }


class MobiAdzContact(Base):
    """
    Individual company contact extracted by MobiAdz.
    Replaces the in-memory results list for persistence.
    """
    __tablename__ = "mobiadz_contacts"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(36), ForeignKey("mobiadz_jobs.job_id"), nullable=False, index=True)

    # Company info
    company_name = Column(String(300), nullable=False)
    app_or_product = Column(String(300), nullable=True)
    product_category = Column(String(100), nullable=True)
    demographic = Column(String(50), nullable=True)
    company_website = Column(String(500), nullable=True)
    company_domain = Column(String(300), nullable=True, index=True)
    company_description = Column(Text, nullable=True)
    company_linkedin = Column(String(500), nullable=True)
    company_size = Column(String(100), nullable=True)
    company_industry = Column(String(200), nullable=True)
    company_founded = Column(String(50), nullable=True)
    company_location = Column(String(300), nullable=True)
    company_phones = Column(JSON, default=[])

    # Emails
    contact_email = Column(String(300), nullable=True, index=True)
    marketing_email = Column(String(300), nullable=True)
    sales_email = Column(String(300), nullable=True)
    support_email = Column(String(300), nullable=True)
    press_email = Column(String(300), nullable=True)

    # App store URLs
    playstore_url = Column(String(500), nullable=True)
    appstore_url = Column(String(500), nullable=True)

    # People (JSON array of person dicts)
    people = Column(JSON, default=[])

    # Scoring
    confidence_score = Column(Integer, default=0)
    data_sources = Column(JSON, default=[])

    # Email verification fields (Layer 6)
    email_verification_status = Column(String(50), default="not_verified")
    email_verification_confidence = Column(Integer, default=0)
    email_mx_valid = Column(Boolean, default=False)
    email_is_disposable = Column(Boolean, default=False)
    email_is_role_based = Column(Boolean, default=False)

    # Layer 9: Enhanced scoring fields
    email_sources = Column(JSON, default={})  # Per-email source tracking
    role_engagement_score = Column(Float, default=0.5)
    domain_reputation_score = Column(Integer, default=0)
    email_freshness_score = Column(Float, default=1.0)
    last_verified_at = Column(DateTime, nullable=True)

    # Layer 15: Email warmth and catch-all
    email_warmth_score = Column(Integer, default=0)
    domain_is_catchall = Column(Boolean, default=False)

    # Timestamps
    extracted_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    job = relationship("MobiAdzJob", back_populates="contacts")

    # Composite index for deduplication
    __table_args__ = (
        Index("ix_mobiadz_contacts_job_domain", "job_id", "company_domain"),
        Index("ix_mobiadz_contacts_job_email", "job_id", "contact_email"),
    )

    def to_dict(self):
        """Convert contact to dict for API response."""
        return {
            "company_name": self.company_name,
            "app_or_product": self.app_or_product,
            "product_category": self.product_category,
            "demographic": self.demographic,
            "company_website": self.company_website,
            "company_domain": self.company_domain,
            "company_description": self.company_description,
            "company_linkedin": self.company_linkedin,
            "company_size": self.company_size,
            "company_industry": self.company_industry,
            "company_founded": self.company_founded,
            "company_location": self.company_location,
            "company_phones": self.company_phones or [],
            "contact_email": self.contact_email,
            "marketing_email": self.marketing_email,
            "sales_email": self.sales_email,
            "support_email": self.support_email,
            "press_email": self.press_email,
            "playstore_url": self.playstore_url,
            "appstore_url": self.appstore_url,
            "people": self.people or [],
            "confidence_score": self.confidence_score,
            "data_sources": self.data_sources or [],
            "email_verification_status": self.email_verification_status or "not_verified",
            "email_verification_confidence": self.email_verification_confidence or 0,
            "email_mx_valid": self.email_mx_valid or False,
            "email_is_disposable": self.email_is_disposable or False,
            "email_is_role_based": self.email_is_role_based or False,
            "email_sources": self.email_sources or {},
            "role_engagement_score": self.role_engagement_score,
            "domain_reputation_score": self.domain_reputation_score,
            "email_freshness_score": self.email_freshness_score,
            "email_warmth_score": self.email_warmth_score or 0,
            "domain_is_catchall": self.domain_is_catchall or False,
            "last_verified_at": self.last_verified_at.isoformat() if self.last_verified_at else None,
        }


class MobiAdzDomainBounceHistory(Base):
    """
    Layer 9: Historical bounce rate tracking per domain.
    Persists across extraction runs for confidence adjustment on future campaigns.

    Bounce rates are used to adjust email confidence:
      < 2% → +5 confidence boost (good domain)
      2-5% → neutral
      5-10% → -15 confidence penalty
      > 20% → -50 penalty (avoid this domain)

    Records decay over time — recent bounces matter more than old ones.
    """
    __tablename__ = "mobiadz_domain_bounce_history"

    id = Column(Integer, primary_key=True, index=True)
    domain = Column(String(300), nullable=False, index=True, unique=True)

    # Aggregate counts
    total_emails_sent = Column(Integer, default=0)
    hard_bounces = Column(Integer, default=0)
    soft_bounces = Column(Integer, default=0)
    successful_deliveries = Column(Integer, default=0)

    # Calculated rates (updated on each extraction)
    hard_bounce_rate = Column(Float, default=0.0)  # 0.0-1.0
    soft_bounce_rate = Column(Float, default=0.0)
    weighted_bounce_rate = Column(Float, default=0.0)  # hard + soft*0.33

    # Domain email infrastructure info (cached)
    mx_provider = Column(String(100), nullable=True)
    has_spf = Column(Boolean, default=False)
    has_dkim = Column(Boolean, default=False)
    has_dmarc = Column(Boolean, default=False)
    is_catchall = Column(Boolean, default=False)
    domain_age_days = Column(Integer, nullable=True)

    # Confidence adjustment applied to emails from this domain
    confidence_adjustment = Column(Integer, default=0)  # -50 to +5

    # Timestamps
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_bounce_at = Column(DateTime, nullable=True)

    def update_rates(self):
        """Recalculate bounce rates from counts."""
        if self.total_emails_sent > 0:
            self.hard_bounce_rate = self.hard_bounces / self.total_emails_sent
            self.soft_bounce_rate = self.soft_bounces / self.total_emails_sent
            self.weighted_bounce_rate = self.hard_bounce_rate + (self.soft_bounce_rate * 0.33)
        else:
            self.hard_bounce_rate = 0.0
            self.soft_bounce_rate = 0.0
            self.weighted_bounce_rate = 0.0

        # Calculate confidence adjustment based on weighted rate
        if self.total_emails_sent < 10:
            self.confidence_adjustment = 0  # Insufficient data
        elif self.weighted_bounce_rate < 0.02:
            self.confidence_adjustment = 5  # Good domain
        elif self.weighted_bounce_rate < 0.05:
            self.confidence_adjustment = 0  # Neutral
        elif self.weighted_bounce_rate < 0.10:
            self.confidence_adjustment = -15  # Concerning
        elif self.weighted_bounce_rate < 0.20:
            self.confidence_adjustment = -30  # Bad domain
        else:
            self.confidence_adjustment = -50  # Blacklist-level

    def to_dict(self):
        return {
            "domain": self.domain,
            "total_emails_sent": self.total_emails_sent,
            "hard_bounces": self.hard_bounces,
            "soft_bounces": self.soft_bounces,
            "hard_bounce_rate": round(self.hard_bounce_rate, 4),
            "soft_bounce_rate": round(self.soft_bounce_rate, 4),
            "weighted_bounce_rate": round(self.weighted_bounce_rate, 4),
            "confidence_adjustment": self.confidence_adjustment,
            "mx_provider": self.mx_provider,
            "has_spf": self.has_spf,
            "has_dkim": self.has_dkim,
            "has_dmarc": self.has_dmarc,
            "is_catchall": self.is_catchall,
            "domain_age_days": self.domain_age_days,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
        }
