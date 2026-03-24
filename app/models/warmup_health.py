"""Warmup Health Tracking Models

Advanced health monitoring for email sender reputation.
Tracks domain health, deliverability scores, and alerts.

Based on 2025 email deliverability best practices:
- Sender reputation is #1 factor in deliverability
- Bounce rates > 2% damage reputation
- Spam complaints > 0.1% are critical
- Consistent sending patterns improve reputation
"""

import enum
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, JSON, Float, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class HealthStatusEnum(str, enum.Enum):
    """Overall health status"""
    EXCELLENT = "excellent"  # Score 90-100
    GOOD = "good"           # Score 70-89
    FAIR = "fair"           # Score 50-69
    POOR = "poor"           # Score 30-49
    CRITICAL = "critical"   # Score 0-29


class AlertSeverityEnum(str, enum.Enum):
    """Alert severity levels"""
    INFO = "info"           # Informational
    WARNING = "warning"     # Needs attention
    CRITICAL = "critical"   # Immediate action required
    RESOLVED = "resolved"   # Previously triggered, now resolved


class AlertTypeEnum(str, enum.Enum):
    """Types of health alerts"""
    HIGH_BOUNCE_RATE = "high_bounce_rate"
    SPAM_COMPLAINT = "spam_complaint"
    BLACKLIST_DETECTED = "blacklist_detected"
    LOW_OPEN_RATE = "low_open_rate"
    SENDING_PATTERN_IRREGULAR = "sending_pattern_irregular"
    AUTHENTICATION_ISSUE = "authentication_issue"
    REPUTATION_DROP = "reputation_drop"
    DAILY_LIMIT_EXCEEDED = "daily_limit_exceeded"
    WARMING_STALLED = "warming_stalled"
    DELIVERY_FAILURE_SPIKE = "delivery_failure_spike"


# Health score weights
HEALTH_SCORE_WEIGHTS = {
    "delivery_rate": 0.30,      # 30% weight
    "bounce_rate": 0.25,        # 25% weight (inverted - lower is better)
    "open_rate": 0.15,          # 15% weight
    "spam_rate": 0.20,          # 20% weight (inverted - lower is better)
    "consistency": 0.10,        # 10% weight
}

# Thresholds for alerts
ALERT_THRESHOLDS = {
    "bounce_rate_warning": 2.0,     # 2% bounce rate triggers warning
    "bounce_rate_critical": 5.0,    # 5% bounce rate is critical
    "spam_rate_warning": 0.05,      # 0.05% spam complaints
    "spam_rate_critical": 0.1,      # 0.1% is critical
    "open_rate_low": 10.0,          # Below 10% is concerning
    "delivery_rate_warning": 95.0,  # Below 95% needs attention
    "delivery_rate_critical": 90.0, # Below 90% is critical
}


class WarmupHealthScore(Base):
    """Daily health score snapshot for a candidate's email account"""
    __tablename__ = "warmup_health_scores"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Date of this score
    score_date = Column(DateTime(timezone=True), nullable=False, index=True)

    # Overall health score (0-100)
    overall_score = Column(Float, default=100.0, nullable=False)
    health_status = Column(String(20), default=HealthStatusEnum.EXCELLENT.value, nullable=False)

    # Component scores (0-100)
    delivery_score = Column(Float, default=100.0)
    bounce_score = Column(Float, default=100.0)
    open_score = Column(Float, default=50.0)  # Start neutral since we may not track opens
    spam_score = Column(Float, default=100.0)
    consistency_score = Column(Float, default=100.0)

    # Raw metrics (percentages)
    delivery_rate = Column(Float, default=100.0)
    bounce_rate = Column(Float, default=0.0)
    open_rate = Column(Float, default=0.0)
    spam_rate = Column(Float, default=0.0)
    click_rate = Column(Float, default=0.0)

    # Volume metrics
    emails_sent = Column(Integer, default=0)
    emails_delivered = Column(Integer, default=0)
    emails_bounced = Column(Integer, default=0)
    emails_opened = Column(Integer, default=0)
    spam_complaints = Column(Integer, default=0)

    # Trend indicators (-1 = declining, 0 = stable, 1 = improving)
    score_trend = Column(Integer, default=0)
    delivery_trend = Column(Integer, default=0)
    bounce_trend = Column(Integer, default=0)

    # 7-day rolling averages
    avg_7day_score = Column(Float, nullable=True)
    avg_7day_delivery = Column(Float, nullable=True)
    avg_7day_bounce = Column(Float, nullable=True)

    # Recommendations JSON
    # Format: [{"priority": 1, "action": "Reduce sending volume", "reason": "Bounce rate spike"}]
    recommendations = Column(JSON, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    candidate = relationship("Candidate", backref="health_scores")


class WarmupHealthAlert(Base):
    """Health alerts for email warming issues"""
    __tablename__ = "warmup_health_alerts"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Alert details
    alert_type = Column(String(50), nullable=False, index=True)
    severity = Column(String(20), nullable=False, index=True)
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False)

    # Context data
    # Format: {"metric": "bounce_rate", "current_value": 5.2, "threshold": 5.0, "previous_value": 2.1}
    context = Column(JSON, nullable=True)

    # Recommended actions
    # Format: [{"action": "Pause warming", "priority": 1}, {"action": "Check email list quality", "priority": 2}]
    recommended_actions = Column(JSON, nullable=True)

    # Status
    is_read = Column(Boolean, default=False, index=True)
    is_resolved = Column(Boolean, default=False, index=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by = Column(String(100), nullable=True)  # "system" or "user"
    resolution_note = Column(Text, nullable=True)

    # Auto-resolve settings
    auto_resolve_on_improvement = Column(Boolean, default=True)

    # Timestamps
    triggered_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    candidate = relationship("Candidate", backref="health_alerts")


class DomainReputation(Base):
    """Track domain reputation from various sources"""
    __tablename__ = "domain_reputations"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Domain info
    domain = Column(String(255), nullable=False, index=True)
    email_address = Column(String(255), nullable=False)

    # Reputation scores (0-100, higher is better)
    overall_reputation = Column(Float, default=50.0)

    # Authentication status
    spf_configured = Column(Boolean, default=False)
    dkim_configured = Column(Boolean, default=False)
    dmarc_configured = Column(Boolean, default=False)
    authentication_score = Column(Float, default=0.0)  # 0-100 based on SPF/DKIM/DMARC

    # Blacklist status
    is_blacklisted = Column(Boolean, default=False)
    blacklist_sources = Column(JSON, nullable=True)  # List of blacklists
    last_blacklist_check = Column(DateTime(timezone=True), nullable=True)

    # Sending history summary
    total_emails_sent = Column(Integer, default=0)
    total_bounces = Column(Integer, default=0)
    total_spam_reports = Column(Integer, default=0)
    lifetime_delivery_rate = Column(Float, default=100.0)
    lifetime_bounce_rate = Column(Float, default=0.0)

    # Age and consistency
    first_email_date = Column(DateTime(timezone=True), nullable=True)
    domain_age_days = Column(Integer, default=0)
    sending_consistency_score = Column(Float, default=50.0)  # Based on regular sending patterns

    # Provider-specific notes
    provider_warnings = Column(JSON, nullable=True)  # Any warnings from email providers

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationship
    candidate = relationship("Candidate", backref="domain_reputation")


class WarmupMilestone(Base):
    """Track warming milestones and achievements"""
    __tablename__ = "warmup_milestones"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Milestone details
    milestone_type = Column(String(50), nullable=False)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)

    # Achievement data
    # Format: {"emails_sent": 100, "day_reached": 7, "health_score": 95}
    achievement_data = Column(JSON, nullable=True)

    # Badge/Icon
    badge_icon = Column(String(50), nullable=True)  # e.g., "trophy", "star", "rocket"
    badge_color = Column(String(20), nullable=True)  # e.g., "gold", "silver", "bronze"

    # Timestamps
    achieved_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    candidate = relationship("Candidate", backref="warmup_milestones")


# Predefined milestones
MILESTONE_DEFINITIONS = {
    "first_email": {
        "title": "First Email Sent",
        "description": "You've sent your first email! The warming journey begins.",
        "badge_icon": "rocket",
        "badge_color": "blue",
        "condition": lambda stats: stats.get("total_sent", 0) >= 1
    },
    "day_3_complete": {
        "title": "3 Days Strong",
        "description": "Completed 3 days of warming with good health.",
        "badge_icon": "calendar",
        "badge_color": "green",
        "condition": lambda stats: stats.get("current_day", 0) >= 3
    },
    "day_7_complete": {
        "title": "Week One Champion",
        "description": "First week of warming complete! You're building reputation.",
        "badge_icon": "trophy",
        "badge_color": "bronze",
        "condition": lambda stats: stats.get("current_day", 0) >= 7
    },
    "day_14_complete": {
        "title": "Two Week Warrior",
        "description": "Two weeks of consistent warming. Reputation growing strong!",
        "badge_icon": "trophy",
        "badge_color": "silver",
        "condition": lambda stats: stats.get("current_day", 0) >= 14
    },
    "warming_complete": {
        "title": "Warming Complete",
        "description": "Congratulations! Your email account is fully warmed up.",
        "badge_icon": "trophy",
        "badge_color": "gold",
        "condition": lambda stats: stats.get("status") == "completed"
    },
    "50_emails": {
        "title": "Half Century",
        "description": "50 emails sent successfully!",
        "badge_icon": "mail",
        "badge_color": "purple",
        "condition": lambda stats: stats.get("total_sent", 0) >= 50
    },
    "100_emails": {
        "title": "Century Club",
        "description": "100 emails sent! You're a pro.",
        "badge_icon": "star",
        "badge_color": "gold",
        "condition": lambda stats: stats.get("total_sent", 0) >= 100
    },
    "perfect_week": {
        "title": "Perfect Week",
        "description": "7 days with 100% delivery rate!",
        "badge_icon": "sparkles",
        "badge_color": "cyan",
        "condition": lambda stats: stats.get("perfect_days", 0) >= 7
    },
    "high_health_score": {
        "title": "Health Hero",
        "description": "Achieved 95+ health score. Excellent sender reputation!",
        "badge_icon": "heart",
        "badge_color": "red",
        "condition": lambda stats: stats.get("health_score", 0) >= 95
    },
    "zero_bounces_week": {
        "title": "Bounce-Free Week",
        "description": "A full week with zero bounces!",
        "badge_icon": "shield",
        "badge_color": "green",
        "condition": lambda stats: stats.get("bounce_free_days", 0) >= 7
    }
}
