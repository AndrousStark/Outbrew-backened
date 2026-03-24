"""
Warmup Pool Models - ULTRA EMAIL WARMUP SYSTEM V1.0

Database models for the peer-to-peer email warmup network.
Implements tiered pool system with quality scoring and intelligent pairing.

Features:
- Pool member management with quality metrics
- Conversation thread tracking
- Inbox placement test results
- Blacklist monitoring status
- Warmup statistics and analytics

Author: Metaminds AI
Version: 1.0.0
"""

import enum
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime,
    ForeignKey, Text, JSON, Enum, Index, UniqueConstraint,
    CheckConstraint, event
)
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.hybrid import hybrid_property

from app.core.database import Base

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS - Pool Tiers and Status Types
# ============================================================================

class PoolTierEnum(str, enum.Enum):
    """
    Warmup pool tiers based on account quality.

    STANDARD: Default tier for all users (free)
    PREMIUM: Aged Google/Microsoft accounts with higher engagement
    PRIVATE: Dedicated enterprise pools for large organizations
    PROBATION: Accounts with quality issues, restricted interactions
    """
    STANDARD = "standard"
    PREMIUM = "premium"
    PRIVATE = "private"
    PROBATION = "probation"


class PoolMemberStatusEnum(str, enum.Enum):
    """Pool member activity status"""
    ACTIVE = "active"
    PAUSED = "paused"
    SUSPENDED = "suspended"
    PENDING_VERIFICATION = "pending_verification"


class ConversationStatusEnum(str, enum.Enum):
    """Warmup conversation lifecycle status"""
    SCHEDULED = "scheduled"
    SENT = "sent"
    DELIVERED = "delivered"
    OPENED = "opened"
    READ = "read"
    REPLIED = "replied"
    SPAM_DETECTED = "spam_detected"
    SPAM_RESCUED = "spam_rescued"
    BOUNCED = "bounced"
    FAILED = "failed"


class PlacementResultEnum(str, enum.Enum):
    """Email placement result categories"""
    INBOX = "inbox"
    SPAM = "spam"
    PROMOTIONS = "promotions"
    UPDATES = "updates"
    SOCIAL = "social"
    QUARANTINE = "quarantine"
    NOT_DELIVERED = "not_delivered"


class BlacklistStatusEnum(str, enum.Enum):
    """Blacklist check result"""
    CLEAN = "clean"
    LISTED = "listed"
    PENDING = "pending"
    ERROR = "error"


# ============================================================================
# TIER CONFIGURATION - Pool tier settings and limits
# ============================================================================

POOL_TIER_CONFIG: Dict[str, Dict[str, Any]] = {
    PoolTierEnum.STANDARD.value: {
        "name": "Standard Pool",
        "description": "Default warmup pool with balanced mix of accounts",
        "icon": "🟢",
        "color": "#22c55e",
        "max_daily_sends": 50,
        "max_daily_receives": 100,
        "min_quality_score": 0,
        "reply_rate_boost": 0,
        "priority_matching": False,
        "features": [
            "Basic warmup interactions",
            "Standard engagement signals",
            "Daily progress tracking",
            "Basic spam rescue",
        ],
    },
    PoolTierEnum.PREMIUM.value: {
        "name": "Premium Pool",
        "description": "Aged Google & Microsoft accounts with 9% higher reply rates",
        "icon": "🔵",
        "color": "#3b82f6",
        "max_daily_sends": 100,
        "max_daily_receives": 200,
        "min_quality_score": 70,
        "reply_rate_boost": 9,
        "priority_matching": True,
        "features": [
            "Premium aged accounts only",
            "9% higher reply rates",
            "Priority matching algorithm",
            "Advanced spam rescue",
            "Dedicated support",
        ],
    },
    PoolTierEnum.PRIVATE.value: {
        "name": "Private Pool",
        "description": "Dedicated enterprise pool with custom configuration",
        "icon": "🟣",
        "color": "#a855f7",
        "max_daily_sends": 200,
        "max_daily_receives": 500,
        "min_quality_score": 80,
        "reply_rate_boost": 15,
        "priority_matching": True,
        "features": [
            "Dedicated private network",
            "Custom warmup schedules",
            "Enterprise-grade security",
            "White-glove support",
            "Custom domain pools",
        ],
    },
    PoolTierEnum.PROBATION.value: {
        "name": "Probation Pool",
        "description": "Restricted pool for accounts with quality issues",
        "icon": "🟡",
        "color": "#eab308",
        "max_daily_sends": 10,
        "max_daily_receives": 20,
        "min_quality_score": 0,
        "reply_rate_boost": -10,
        "priority_matching": False,
        "features": [
            "Limited interactions",
            "Quality improvement focus",
            "Monitoring period",
        ],
    },
}


# ============================================================================
# QUALITY SCORING WEIGHTS - ML-inspired scoring algorithm
# ============================================================================

QUALITY_SCORE_WEIGHTS: Dict[str, float] = {
    "response_rate": 0.25,          # How often they respond to warmup emails
    "open_rate": 0.15,              # Email open rate
    "spam_rescue_rate": 0.10,       # How often they rescue from spam
    "consistency_score": 0.15,      # Daily activity consistency
    "domain_age_score": 0.10,       # Age of email domain
    "provider_score": 0.10,         # Gmail/Outlook score higher
    "engagement_depth": 0.10,       # Thread continuation rate
    "blacklist_clean": 0.05,        # No blacklist issues
}


# ============================================================================
# MODEL: WarmupPoolMember
# ============================================================================

class WarmupPoolMember(Base):
    """
    Represents a member in the warmup pool network.

    Each user who enables warmup becomes a pool member, participating
    in the peer-to-peer email exchange network. Quality scores determine
    pool tier placement and matching priority.

    Attributes:
        candidate_id: Link to the user account
        pool_tier: Current tier (standard/premium/private/probation)
        quality_score: ML-computed quality rating (0-100)
        engagement_score: Recent engagement metric
        status: Active/paused/suspended

    Statistics:
        total_sends: Lifetime warmup emails sent
        total_receives: Lifetime warmup emails received
        total_replies: Total replies generated
        spam_rescues: Times rescued email from spam

    Quality Metrics:
        response_rate: % of received emails replied to
        open_rate: % of received emails opened
        avg_response_time: Average time to respond (seconds)
        consistency_score: Daily activity consistency (0-100)
    """

    __tablename__ = "warmup_pool_members"
    __table_args__ = (
        Index("ix_warmup_pool_candidate", "candidate_id"),
        Index("ix_warmup_pool_tier_quality", "pool_tier", "quality_score"),
        Index("ix_warmup_pool_active", "status", "is_active"),
        CheckConstraint("quality_score >= 0 AND quality_score <= 100", name="quality_score_range"),
        CheckConstraint("response_rate >= 0 AND response_rate <= 100", name="response_rate_range"),
    )

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign Keys
    candidate_id = Column(
        Integer,
        ForeignKey("candidates.id", ondelete="CASCADE"),
        nullable=False,
        unique=True  # One pool membership per user
    )

    # Pool Configuration
    pool_tier = Column(
        String(20),
        default=PoolTierEnum.STANDARD.value,
        nullable=False,
        index=True
    )
    status = Column(
        String(30),
        default=PoolMemberStatusEnum.ACTIVE.value,
        nullable=False
    )
    is_active = Column(Boolean, default=True, nullable=False)

    # Quality Metrics (0-100 scale)
    quality_score = Column(Float, default=50.0, nullable=False)
    engagement_score = Column(Float, default=50.0, nullable=False)
    consistency_score = Column(Float, default=50.0, nullable=False)

    # Lifetime Statistics
    total_sends = Column(Integer, default=0, nullable=False)
    total_receives = Column(Integer, default=0, nullable=False)
    total_replies = Column(Integer, default=0, nullable=False)
    total_opens = Column(Integer, default=0, nullable=False)
    spam_rescues = Column(Integer, default=0, nullable=False)
    bounces = Column(Integer, default=0, nullable=False)

    # Rate Metrics (percentages)
    response_rate = Column(Float, default=0.0, nullable=False)
    open_rate = Column(Float, default=0.0, nullable=False)
    bounce_rate = Column(Float, default=0.0, nullable=False)

    # Timing Metrics
    avg_response_time = Column(Integer, default=0)  # seconds
    avg_open_time = Column(Integer, default=0)  # seconds

    # Daily Limits & Usage
    daily_send_limit = Column(Integer, default=50)
    daily_receive_limit = Column(Integer, default=100)
    sends_today = Column(Integer, default=0)
    receives_today = Column(Integer, default=0)
    last_reset_date = Column(DateTime, default=datetime.utcnow)

    # Email Account Details
    email_provider = Column(String(50))  # gmail, outlook, yahoo, other
    domain = Column(String(255))
    domain_age_days = Column(Integer, default=0)

    # Timestamps
    joined_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_activity_at = Column(DateTime, default=datetime.utcnow)
    tier_changed_at = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Metadata
    settings = Column(JSON, default=dict)  # User preferences
    notes = Column(Text)  # Admin notes

    # Relationships
    candidate = relationship("Candidate", backref=backref("warmup_pool_member", uselist=False))
    sent_conversations = relationship(
        "WarmupConversation",
        foreign_keys="WarmupConversation.sender_id",
        back_populates="sender",
        lazy="dynamic"
    )
    received_conversations = relationship(
        "WarmupConversation",
        foreign_keys="WarmupConversation.receiver_id",
        back_populates="receiver",
        lazy="dynamic"
    )

    def __repr__(self) -> str:
        return f"<WarmupPoolMember(id={self.id}, tier={self.pool_tier}, quality={self.quality_score:.1f})>"

    # ============ Hybrid Properties ============

    @hybrid_property
    def tier_config(self) -> Dict[str, Any]:
        """Get configuration for current tier"""
        return POOL_TIER_CONFIG.get(self.pool_tier, POOL_TIER_CONFIG[PoolTierEnum.STANDARD.value])

    @hybrid_property
    def can_send_today(self) -> bool:
        """Check if member can send more warmup emails today"""
        self._check_daily_reset()
        return self.sends_today < self.daily_send_limit and self.is_active

    @hybrid_property
    def can_receive_today(self) -> bool:
        """Check if member can receive more warmup emails today"""
        self._check_daily_reset()
        return self.receives_today < self.daily_receive_limit and self.is_active

    @hybrid_property
    def remaining_sends_today(self) -> int:
        """Remaining send quota for today"""
        self._check_daily_reset()
        return max(0, self.daily_send_limit - self.sends_today)

    @hybrid_property
    def remaining_receives_today(self) -> int:
        """Remaining receive quota for today"""
        self._check_daily_reset()
        return max(0, self.daily_receive_limit - self.receives_today)

    @hybrid_property
    def health_status(self) -> str:
        """Overall health status based on quality score"""
        if self.quality_score >= 90:
            return "excellent"
        elif self.quality_score >= 70:
            return "good"
        elif self.quality_score >= 50:
            return "fair"
        elif self.quality_score >= 30:
            return "poor"
        else:
            return "critical"

    # ============ Instance Methods ============

    def _check_daily_reset(self) -> None:
        """Reset daily counters if it's a new day"""
        if self.last_reset_date:
            today = datetime.now(timezone.utc).date()
            last_reset = self.last_reset_date.date()
            if today > last_reset:
                logger.debug(f"[WarmupPool] Resetting daily counters for member {self.id}")
                self.sends_today = 0
                self.receives_today = 0
                self.last_reset_date = datetime.now(timezone.utc)

    def increment_send(self) -> bool:
        """
        Increment send counter and update statistics.

        Returns:
            bool: True if send was recorded, False if limit reached
        """
        self._check_daily_reset()

        if self.sends_today >= self.daily_send_limit:
            logger.warning(f"[WarmupPool] Member {self.id} reached daily send limit")
            return False

        self.sends_today += 1
        self.total_sends += 1
        self.last_activity_at = datetime.now(timezone.utc)

        logger.debug(f"[WarmupPool] Member {self.id} send count: {self.sends_today}/{self.daily_send_limit}")
        return True

    def increment_receive(self) -> bool:
        """
        Increment receive counter and update statistics.

        Returns:
            bool: True if receive was recorded, False if limit reached
        """
        self._check_daily_reset()

        if self.receives_today >= self.daily_receive_limit:
            logger.warning(f"[WarmupPool] Member {self.id} reached daily receive limit")
            return False

        self.receives_today += 1
        self.total_receives += 1
        self.last_activity_at = datetime.now(timezone.utc)

        logger.debug(f"[WarmupPool] Member {self.id} receive count: {self.receives_today}/{self.daily_receive_limit}")
        return True

    def record_reply(self, response_time_seconds: int = 0) -> None:
        """Record a reply action and update response metrics"""
        self.total_replies += 1

        # Update response rate using exponential moving average
        if self.total_receives > 0:
            self.response_rate = (self.total_replies / self.total_receives) * 100

        # Update average response time
        if response_time_seconds > 0:
            if self.avg_response_time == 0:
                self.avg_response_time = response_time_seconds
            else:
                # Exponential moving average with alpha=0.1
                self.avg_response_time = int(0.9 * self.avg_response_time + 0.1 * response_time_seconds)

        self.last_activity_at = datetime.now(timezone.utc)
        logger.debug(f"[WarmupPool] Member {self.id} reply recorded, response_rate: {self.response_rate:.1f}%")

    def record_open(self, open_time_seconds: int = 0) -> None:
        """Record an email open action"""
        self.total_opens += 1

        # Update open rate
        if self.total_receives > 0:
            self.open_rate = (self.total_opens / self.total_receives) * 100

        # Update average open time
        if open_time_seconds > 0:
            if self.avg_open_time == 0:
                self.avg_open_time = open_time_seconds
            else:
                self.avg_open_time = int(0.9 * self.avg_open_time + 0.1 * open_time_seconds)

        self.last_activity_at = datetime.now(timezone.utc)

    def record_spam_rescue(self) -> None:
        """Record a spam rescue action"""
        self.spam_rescues += 1
        self.last_activity_at = datetime.now(timezone.utc)
        logger.info(f"[WarmupPool] Member {self.id} spam rescue #{self.spam_rescues}")

    def record_bounce(self) -> None:
        """Record a bounce"""
        self.bounces += 1
        if self.total_sends > 0:
            self.bounce_rate = (self.bounces / self.total_sends) * 100
        self.last_activity_at = datetime.now(timezone.utc)
        logger.warning(f"[WarmupPool] Member {self.id} bounce recorded, rate: {self.bounce_rate:.1f}%")

    def calculate_quality_score(self) -> float:
        """
        Calculate quality score using weighted metrics.

        Uses ML-inspired weighted average algorithm with the following components:
        - Response rate (25%)
        - Open rate (15%)
        - Spam rescue rate (10%)
        - Consistency score (15%)
        - Domain age (10%)
        - Provider score (10%)
        - Engagement depth (10%)
        - Blacklist clean (5%)

        Returns:
            float: Quality score between 0-100
        """
        scores = {}

        # Response rate score (higher is better)
        scores["response_rate"] = min(100, self.response_rate * 1.25)  # 80% response = 100 score

        # Open rate score
        scores["open_rate"] = min(100, self.open_rate * 1.1)  # 90% open = 100 score

        # Spam rescue rate (if receiving spam, how often do they rescue)
        if self.total_receives > 0:
            spam_rescue_rate = (self.spam_rescues / self.total_receives) * 100
            scores["spam_rescue_rate"] = min(100, spam_rescue_rate * 10)  # 10% rescue rate = 100
        else:
            scores["spam_rescue_rate"] = 50  # Neutral if no data

        # Consistency score (use existing or calculate)
        scores["consistency_score"] = self.consistency_score

        # Domain age score (older domains score higher)
        if self.domain_age_days >= 365:
            scores["domain_age_score"] = 100
        elif self.domain_age_days >= 180:
            scores["domain_age_score"] = 80
        elif self.domain_age_days >= 90:
            scores["domain_age_score"] = 60
        elif self.domain_age_days >= 30:
            scores["domain_age_score"] = 40
        else:
            scores["domain_age_score"] = 20

        # Provider score (Gmail/Outlook higher)
        provider_scores = {
            "gmail": 100,
            "outlook": 95,
            "microsoft": 95,
            "yahoo": 70,
            "icloud": 75,
            "other": 50,
        }
        scores["provider_score"] = provider_scores.get(
            (self.email_provider or "other").lower(), 50
        )

        # Engagement depth (thread continuation)
        if self.total_replies > 0 and self.total_receives > 0:
            engagement = (self.total_replies / self.total_receives) * 100
            scores["engagement_depth"] = min(100, engagement * 1.5)
        else:
            scores["engagement_depth"] = 50

        # Blacklist clean (assume clean if bounce rate is low)
        scores["blacklist_clean"] = 100 if self.bounce_rate < 5 else (50 if self.bounce_rate < 10 else 0)

        # Calculate weighted average
        total_score = 0.0
        for metric, weight in QUALITY_SCORE_WEIGHTS.items():
            metric_score = scores.get(metric, 50)
            total_score += metric_score * weight

        # Apply bounds
        self.quality_score = max(0, min(100, total_score))

        logger.debug(
            f"[WarmupPool] Member {self.id} quality score calculated: {self.quality_score:.1f} "
            f"(components: {scores})"
        )

        return self.quality_score

    def update_tier_based_on_quality(self) -> Optional[str]:
        """
        Update pool tier based on quality score.

        Returns:
            str or None: New tier if changed, None if unchanged
        """
        old_tier = self.pool_tier

        # Don't auto-upgrade to private (requires manual assignment)
        if self.pool_tier == PoolTierEnum.PRIVATE.value:
            return None

        # Check for probation
        if self.quality_score < 30 or self.bounce_rate > 15:
            new_tier = PoolTierEnum.PROBATION.value
        # Check for premium eligibility
        elif self.quality_score >= 70 and self.bounce_rate < 3:
            if self.email_provider in ["gmail", "outlook", "microsoft"]:
                new_tier = PoolTierEnum.PREMIUM.value
            else:
                new_tier = PoolTierEnum.STANDARD.value
        # Standard tier
        else:
            new_tier = PoolTierEnum.STANDARD.value

        if new_tier != old_tier:
            self.pool_tier = new_tier
            self.tier_changed_at = datetime.now(timezone.utc)

            # Update daily limits based on new tier
            tier_config = POOL_TIER_CONFIG.get(new_tier, POOL_TIER_CONFIG[PoolTierEnum.STANDARD.value])
            self.daily_send_limit = tier_config["max_daily_sends"]
            self.daily_receive_limit = tier_config["max_daily_receives"]

            logger.info(
                f"[WarmupPool] Member {self.id} tier changed: {old_tier} -> {new_tier} "
                f"(quality: {self.quality_score:.1f})"
            )
            return new_tier

        return None

    def to_dict(self, include_stats: bool = True) -> Dict[str, Any]:
        """Convert to dictionary for API response"""
        data = {
            "id": self.id,
            "candidate_id": self.candidate_id,
            "pool_tier": self.pool_tier,
            "tier_config": self.tier_config,
            "status": self.status,
            "is_active": self.is_active,
            "quality_score": round(self.quality_score, 1),
            "health_status": self.health_status,
            "email_provider": self.email_provider,
            "domain": self.domain,
            "joined_at": self.joined_at.isoformat() if self.joined_at else None,
            "last_activity_at": self.last_activity_at.isoformat() if self.last_activity_at else None,
        }

        if include_stats:
            data.update({
                "statistics": {
                    "total_sends": self.total_sends,
                    "total_receives": self.total_receives,
                    "total_replies": self.total_replies,
                    "total_opens": self.total_opens,
                    "spam_rescues": self.spam_rescues,
                    "bounces": self.bounces,
                },
                "rates": {
                    "response_rate": round(self.response_rate, 1),
                    "open_rate": round(self.open_rate, 1),
                    "bounce_rate": round(self.bounce_rate, 2),
                },
                "daily_usage": {
                    "sends_today": self.sends_today,
                    "receives_today": self.receives_today,
                    "send_limit": self.daily_send_limit,
                    "receive_limit": self.daily_receive_limit,
                    "remaining_sends": self.remaining_sends_today,
                    "remaining_receives": self.remaining_receives_today,
                },
                "timing": {
                    "avg_response_time_seconds": self.avg_response_time,
                    "avg_open_time_seconds": self.avg_open_time,
                },
            })

        return data


# ============================================================================
# MODEL: WarmupConversation
# ============================================================================

class WarmupConversation(Base):
    """
    Represents a warmup email conversation between pool members.

    Tracks the full lifecycle of warmup interactions:
    - Scheduling and sending
    - Delivery confirmation
    - Open tracking with timing
    - Reply generation and tracking
    - Spam detection and rescue

    Attributes:
        sender_id: Pool member who sent the email
        receiver_id: Pool member who receives the email
        thread_id: Unique thread identifier for conversation threading
        parent_conversation_id: Link to parent for threaded replies
    """

    __tablename__ = "warmup_conversations"
    __table_args__ = (
        Index("ix_warmup_conv_sender", "sender_id"),
        Index("ix_warmup_conv_receiver", "receiver_id"),
        Index("ix_warmup_conv_thread", "thread_id"),
        Index("ix_warmup_conv_status", "status"),
        Index("ix_warmup_conv_scheduled", "scheduled_at"),
        Index("ix_warmup_conv_created", "created_at"),
    )

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign Keys
    sender_id = Column(
        Integer,
        ForeignKey("warmup_pool_members.id", ondelete="CASCADE"),
        nullable=False
    )
    receiver_id = Column(
        Integer,
        ForeignKey("warmup_pool_members.id", ondelete="CASCADE"),
        nullable=False
    )
    parent_conversation_id = Column(
        Integer,
        ForeignKey("warmup_conversations.id", ondelete="SET NULL"),
        nullable=True
    )

    # Thread Tracking
    thread_id = Column(String(100), nullable=False, index=True)
    thread_depth = Column(Integer, default=0)  # 0 = initial, 1 = first reply, etc.

    # Email Content
    subject = Column(String(500), nullable=False)
    body_text = Column(Text)
    body_html = Column(Text)
    message_id = Column(String(255))  # Email message ID
    in_reply_to = Column(String(255))  # Parent message ID

    # Status Tracking
    status = Column(
        String(30),
        default=ConversationStatusEnum.SCHEDULED.value,
        nullable=False,
        index=True
    )

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    scheduled_at = Column(DateTime)  # When to send
    sent_at = Column(DateTime)
    delivered_at = Column(DateTime)
    opened_at = Column(DateTime)
    read_completed_at = Column(DateTime)  # When read emulation completed
    replied_at = Column(DateTime)

    # Engagement Metrics
    time_to_open = Column(Integer)  # Seconds from delivery to open
    time_to_reply = Column(Integer)  # Seconds from delivery to reply
    read_duration = Column(Integer)  # Seconds spent "reading"
    scroll_percentage = Column(Integer)  # How far they scrolled (0-100)
    marked_important = Column(Boolean, default=False)
    marked_not_spam = Column(Boolean, default=False)

    # Spam Tracking
    was_in_spam = Column(Boolean, default=False)
    spam_detected_at = Column(DateTime)
    spam_rescued_at = Column(DateTime)

    # Error Tracking
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)

    # AI Generation Metadata
    ai_model_used = Column(String(50))
    ai_generation_params = Column(JSON)
    content_category = Column(String(50))  # business, tech, casual, etc.
    sentiment = Column(String(20))  # positive, neutral, professional

    # Relationships
    sender = relationship(
        "WarmupPoolMember",
        foreign_keys=[sender_id],
        back_populates="sent_conversations"
    )
    receiver = relationship(
        "WarmupPoolMember",
        foreign_keys=[receiver_id],
        back_populates="received_conversations"
    )
    replies = relationship(
        "WarmupConversation",
        backref=backref("parent", remote_side=[id]),
        lazy="dynamic"
    )

    def __repr__(self) -> str:
        return f"<WarmupConversation(id={self.id}, thread={self.thread_id}, status={self.status})>"

    # ============ Status Transition Methods ============

    def mark_sent(self, message_id: Optional[str] = None) -> None:
        """Mark conversation as sent"""
        self.status = ConversationStatusEnum.SENT.value
        self.sent_at = datetime.now(timezone.utc)
        if message_id:
            self.message_id = message_id
        logger.debug(f"[WarmupConv] Conversation {self.id} marked as sent")

    def mark_delivered(self) -> None:
        """Mark conversation as delivered"""
        self.status = ConversationStatusEnum.DELIVERED.value
        self.delivered_at = datetime.now(timezone.utc)
        logger.debug(f"[WarmupConv] Conversation {self.id} marked as delivered")

    def mark_opened(self, read_duration: int = 0, scroll_percentage: int = 100) -> None:
        """
        Mark conversation as opened with read emulation metrics.

        Args:
            read_duration: Simulated reading time in seconds
            scroll_percentage: How far "scrolled" through email (0-100)
        """
        now = datetime.now(timezone.utc)
        self.status = ConversationStatusEnum.OPENED.value
        self.opened_at = now
        self.read_duration = read_duration
        self.scroll_percentage = scroll_percentage

        if self.delivered_at:
            self.time_to_open = int((now - self.delivered_at).total_seconds())

        logger.debug(
            f"[WarmupConv] Conversation {self.id} opened, "
            f"read_duration={read_duration}s, scroll={scroll_percentage}%"
        )

    def mark_read_complete(self) -> None:
        """Mark read emulation as complete"""
        self.status = ConversationStatusEnum.READ.value
        self.read_completed_at = datetime.now(timezone.utc)

    def mark_replied(self, reply_conversation_id: int) -> None:
        """Mark conversation as replied"""
        now = datetime.now(timezone.utc)
        self.status = ConversationStatusEnum.REPLIED.value
        self.replied_at = now

        if self.delivered_at:
            self.time_to_reply = int((now - self.delivered_at).total_seconds())

        logger.debug(f"[WarmupConv] Conversation {self.id} replied with conversation {reply_conversation_id}")

    def mark_spam_detected(self) -> None:
        """Mark that email was found in spam folder"""
        self.was_in_spam = True
        self.status = ConversationStatusEnum.SPAM_DETECTED.value
        self.spam_detected_at = datetime.now(timezone.utc)
        logger.warning(f"[WarmupConv] Conversation {self.id} detected in spam folder")

    def mark_spam_rescued(self) -> None:
        """Mark that email was rescued from spam"""
        self.status = ConversationStatusEnum.SPAM_RESCUED.value
        self.spam_rescued_at = datetime.now(timezone.utc)
        self.marked_not_spam = True
        logger.info(f"[WarmupConv] Conversation {self.id} rescued from spam")

    def mark_important(self) -> None:
        """Mark email as important"""
        self.marked_important = True
        logger.debug(f"[WarmupConv] Conversation {self.id} marked as important")

    def mark_bounced(self, error_message: Optional[str] = None) -> None:
        """Mark conversation as bounced"""
        self.status = ConversationStatusEnum.BOUNCED.value
        if error_message:
            self.error_message = error_message
        logger.warning(f"[WarmupConv] Conversation {self.id} bounced: {error_message}")

    def mark_failed(self, error_message: str) -> None:
        """Mark conversation as failed"""
        self.status = ConversationStatusEnum.FAILED.value
        self.error_message = error_message
        self.retry_count += 1
        logger.error(f"[WarmupConv] Conversation {self.id} failed: {error_message}")

    # ============ Utility Methods ============

    @hybrid_property
    def is_complete(self) -> bool:
        """Check if conversation cycle is complete"""
        return self.status in [
            ConversationStatusEnum.REPLIED.value,
            ConversationStatusEnum.SPAM_RESCUED.value,
            ConversationStatusEnum.BOUNCED.value,
            ConversationStatusEnum.FAILED.value,
        ]

    @hybrid_property
    def engagement_score(self) -> float:
        """Calculate engagement score for this conversation (0-100)"""
        score = 0.0

        if self.status in [ConversationStatusEnum.DELIVERED.value]:
            score += 20
        if self.opened_at:
            score += 25
        if self.read_duration and self.read_duration > 5:
            score += 15
        if self.marked_important:
            score += 10
        if self.replied_at:
            score += 25
        if self.was_in_spam and self.spam_rescued_at:
            score += 5  # Bonus for spam rescue

        return min(100, score)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response"""
        return {
            "id": self.id,
            "thread_id": self.thread_id,
            "thread_depth": self.thread_depth,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "subject": self.subject,
            "status": self.status,
            "engagement_score": self.engagement_score,
            "timestamps": {
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
                "sent_at": self.sent_at.isoformat() if self.sent_at else None,
                "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
                "opened_at": self.opened_at.isoformat() if self.opened_at else None,
                "replied_at": self.replied_at.isoformat() if self.replied_at else None,
            },
            "metrics": {
                "time_to_open": self.time_to_open,
                "time_to_reply": self.time_to_reply,
                "read_duration": self.read_duration,
                "scroll_percentage": self.scroll_percentage,
            },
            "flags": {
                "marked_important": self.marked_important,
                "was_in_spam": self.was_in_spam,
                "spam_rescued": self.spam_rescued_at is not None,
            },
            "ai_metadata": {
                "model": self.ai_model_used,
                "category": self.content_category,
                "sentiment": self.sentiment,
            },
        }


# ============================================================================
# MODEL: InboxPlacementTest
# ============================================================================

class InboxPlacementTest(Base):
    """
    Stores results of inbox placement tests.

    Tests are run by sending emails to seed accounts at various
    providers and checking where they land (inbox, spam, promotions).
    """

    __tablename__ = "inbox_placement_tests"
    __table_args__ = (
        Index("ix_placement_test_candidate", "candidate_id"),
        Index("ix_placement_test_date", "test_date"),
    )

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign Keys
    candidate_id = Column(
        Integer,
        ForeignKey("candidates.id", ondelete="CASCADE"),
        nullable=False
    )

    # Test Metadata
    test_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    test_type = Column(String(50), default="standard")  # standard, deep, custom
    emails_sent = Column(Integer, default=0)
    emails_delivered = Column(Integer, default=0)

    # Overall Scores
    overall_inbox_rate = Column(Float, default=0.0)  # Percentage in inbox
    overall_spam_rate = Column(Float, default=0.0)  # Percentage in spam
    overall_score = Column(Float, default=0.0)  # Composite score 0-100

    # Provider-specific Results (JSON for flexibility)
    gmail_results = Column(JSON, default=dict)
    outlook_results = Column(JSON, default=dict)
    yahoo_results = Column(JSON, default=dict)
    icloud_results = Column(JSON, default=dict)
    other_results = Column(JSON, default=dict)

    # Detailed breakdown (for analytics)
    results_by_category = Column(JSON, default=dict)  # inbox, spam, promotions, etc.

    # Issues detected
    issues_detected = Column(JSON, default=list)  # List of issues found
    recommendations = Column(JSON, default=list)  # Suggested fixes

    # Status
    status = Column(String(30), default="completed")  # pending, running, completed, failed
    error_message = Column(Text)

    # Timestamps
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    # Relationship
    candidate = relationship("Candidate", backref="inbox_placement_tests")

    def __repr__(self) -> str:
        return f"<InboxPlacementTest(id={self.id}, score={self.overall_score:.1f}, date={self.test_date})>"

    def calculate_overall_score(self) -> float:
        """
        Calculate overall inbox placement score.

        Weighted by provider importance:
        - Gmail: 40%
        - Outlook: 30%
        - Yahoo: 15%
        - Other: 15%
        """
        weights = {
            "gmail": 0.40,
            "outlook": 0.30,
            "yahoo": 0.15,
            "other": 0.15,
        }

        scores = {
            "gmail": self.gmail_results.get("inbox_rate", 0) if self.gmail_results else 0,
            "outlook": self.outlook_results.get("inbox_rate", 0) if self.outlook_results else 0,
            "yahoo": self.yahoo_results.get("inbox_rate", 0) if self.yahoo_results else 0,
            "other": self.other_results.get("inbox_rate", 0) if self.other_results else 0,
        }

        total_score = sum(scores[provider] * weight for provider, weight in weights.items())
        self.overall_score = round(total_score, 1)

        # Calculate overall rates
        total_inbox = sum(
            r.get("inbox_count", 0) for r in [
                self.gmail_results or {},
                self.outlook_results or {},
                self.yahoo_results or {},
                self.other_results or {},
            ]
        )
        total_spam = sum(
            r.get("spam_count", 0) for r in [
                self.gmail_results or {},
                self.outlook_results or {},
                self.yahoo_results or {},
                self.other_results or {},
            ]
        )

        if self.emails_delivered > 0:
            self.overall_inbox_rate = round((total_inbox / self.emails_delivered) * 100, 1)
            self.overall_spam_rate = round((total_spam / self.emails_delivered) * 100, 1)

        logger.debug(f"[PlacementTest] Test {self.id} score calculated: {self.overall_score}")
        return self.overall_score

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response"""
        return {
            "id": self.id,
            "candidate_id": self.candidate_id,
            "test_date": self.test_date.isoformat() if self.test_date else None,
            "test_type": self.test_type,
            "status": self.status,
            "scores": {
                "overall": self.overall_score,
                "inbox_rate": self.overall_inbox_rate,
                "spam_rate": self.overall_spam_rate,
            },
            "by_provider": {
                "gmail": self.gmail_results,
                "outlook": self.outlook_results,
                "yahoo": self.yahoo_results,
                "icloud": self.icloud_results,
                "other": self.other_results,
            },
            "emails": {
                "sent": self.emails_sent,
                "delivered": self.emails_delivered,
            },
            "issues": self.issues_detected,
            "recommendations": self.recommendations,
            "timestamps": {
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            },
        }


# ============================================================================
# MODEL: BlacklistStatus
# ============================================================================

class BlacklistStatus(Base):
    """
    Tracks blacklist status across major blacklist providers.

    Monitors:
    - Spamhaus
    - Barracuda
    - SORBS
    - SpamCop
    - And 50+ other blacklists
    """

    __tablename__ = "blacklist_status"
    __table_args__ = (
        Index("ix_blacklist_candidate", "candidate_id"),
        Index("ix_blacklist_check_date", "check_date"),
    )

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign Keys
    candidate_id = Column(
        Integer,
        ForeignKey("candidates.id", ondelete="CASCADE"),
        nullable=False
    )

    # Check Metadata
    check_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    ip_address = Column(String(45))  # IPv4 or IPv6
    domain = Column(String(255))

    # Overall Status
    is_listed_anywhere = Column(Boolean, default=False, nullable=False)
    total_blacklists_checked = Column(Integer, default=0)
    total_listings = Column(Integer, default=0)

    # Major Blacklist Status
    spamhaus = Column(String(20), default=BlacklistStatusEnum.PENDING.value)
    spamhaus_details = Column(JSON)

    barracuda = Column(String(20), default=BlacklistStatusEnum.PENDING.value)
    barracuda_details = Column(JSON)

    sorbs = Column(String(20), default=BlacklistStatusEnum.PENDING.value)
    sorbs_details = Column(JSON)

    spamcop = Column(String(20), default=BlacklistStatusEnum.PENDING.value)
    spamcop_details = Column(JSON)

    # All blacklist results
    all_results = Column(JSON, default=dict)

    # Alerts
    new_listings = Column(JSON, default=list)  # New listings since last check
    removed_listings = Column(JSON, default=list)  # Removed since last check

    # Status
    status = Column(String(30), default="completed")
    error_message = Column(Text)

    # Relationship
    candidate = relationship("Candidate", backref="blacklist_checks")

    def __repr__(self) -> str:
        return f"<BlacklistStatus(id={self.id}, listed={self.is_listed_anywhere}, date={self.check_date})>"

    @hybrid_property
    def severity(self) -> str:
        """Get severity level based on listings"""
        if not self.is_listed_anywhere:
            return "clean"

        # Check major blacklists
        major_listed = sum([
            1 for status in [self.spamhaus, self.barracuda, self.sorbs, self.spamcop]
            if status == BlacklistStatusEnum.LISTED.value
        ])

        if major_listed >= 2:
            return "critical"
        elif major_listed == 1:
            return "warning"
        elif self.total_listings > 0:
            return "minor"

        return "clean"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response"""
        return {
            "id": self.id,
            "candidate_id": self.candidate_id,
            "check_date": self.check_date.isoformat() if self.check_date else None,
            "ip_address": self.ip_address,
            "domain": self.domain,
            "summary": {
                "is_listed": self.is_listed_anywhere,
                "severity": self.severity,
                "total_checked": self.total_blacklists_checked,
                "total_listings": self.total_listings,
            },
            "major_blacklists": {
                "spamhaus": {
                    "status": self.spamhaus,
                    "details": self.spamhaus_details,
                },
                "barracuda": {
                    "status": self.barracuda,
                    "details": self.barracuda_details,
                },
                "sorbs": {
                    "status": self.sorbs,
                    "details": self.sorbs_details,
                },
                "spamcop": {
                    "status": self.spamcop,
                    "details": self.spamcop_details,
                },
            },
            "all_results": self.all_results,
            "changes": {
                "new_listings": self.new_listings,
                "removed_listings": self.removed_listings,
            },
            "status": self.status,
        }


# ============================================================================
# MODEL: WarmupSchedule
# ============================================================================

class WarmupSchedule(Base):
    """
    Stores warmup schedules and timing preferences.

    Allows users to customize when warmup emails are sent
    to match their business hours and timezone.
    """

    __tablename__ = "warmup_schedules"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign Keys
    candidate_id = Column(
        Integer,
        ForeignKey("candidates.id", ondelete="CASCADE"),
        nullable=False,
        unique=True
    )

    # Timezone
    timezone = Column(String(50), default="UTC")

    # Active Hours (24-hour format)
    start_hour = Column(Integer, default=9)  # 9 AM
    end_hour = Column(Integer, default=17)  # 5 PM

    # Active Days (bit flags: Mon=1, Tue=2, Wed=4, Thu=8, Fri=16, Sat=32, Sun=64)
    active_days = Column(Integer, default=31)  # Mon-Fri by default

    # Sending Pattern
    min_delay_between_sends = Column(Integer, default=300)  # 5 minutes
    max_delay_between_sends = Column(Integer, default=3600)  # 1 hour

    # Response Pattern
    min_delay_before_open = Column(Integer, default=60)  # 1 minute
    max_delay_before_open = Column(Integer, default=14400)  # 4 hours
    min_delay_before_reply = Column(Integer, default=300)  # 5 minutes
    max_delay_before_reply = Column(Integer, default=86400)  # 24 hours

    # Preferences
    weekdays_only = Column(Boolean, default=True)
    randomize_timing = Column(Boolean, default=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship
    candidate = relationship("Candidate", backref=backref("warmup_schedule", uselist=False))

    def is_day_active(self, weekday: int) -> bool:
        """
        Check if a specific weekday is active.

        Args:
            weekday: 0=Monday, 6=Sunday

        Returns:
            bool: True if day is active
        """
        day_bit = 1 << weekday
        return bool(self.active_days & day_bit)

    def get_random_send_delay(self) -> int:
        """Get random delay between sends"""
        import random
        return random.randint(self.min_delay_between_sends, self.max_delay_between_sends)

    def get_random_open_delay(self) -> int:
        """Get random delay before opening"""
        import random
        return random.randint(self.min_delay_before_open, self.max_delay_before_open)

    def get_random_reply_delay(self) -> int:
        """Get random delay before replying"""
        import random
        return random.randint(self.min_delay_before_reply, self.max_delay_before_reply)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "candidate_id": self.candidate_id,
            "timezone": self.timezone,
            "active_hours": {
                "start": self.start_hour,
                "end": self.end_hour,
            },
            "active_days": {
                "monday": self.is_day_active(0),
                "tuesday": self.is_day_active(1),
                "wednesday": self.is_day_active(2),
                "thursday": self.is_day_active(3),
                "friday": self.is_day_active(4),
                "saturday": self.is_day_active(5),
                "sunday": self.is_day_active(6),
            },
            "delays": {
                "between_sends": {
                    "min": self.min_delay_between_sends,
                    "max": self.max_delay_between_sends,
                },
                "before_open": {
                    "min": self.min_delay_before_open,
                    "max": self.max_delay_before_open,
                },
                "before_reply": {
                    "min": self.min_delay_before_reply,
                    "max": self.max_delay_before_reply,
                },
            },
            "preferences": {
                "weekdays_only": self.weekdays_only,
                "randomize_timing": self.randomize_timing,
            },
        }


# ============================================================================
# EVENT LISTENERS - Auto-update timestamps and quality scores
# ============================================================================

@event.listens_for(WarmupPoolMember, "before_update")
def update_pool_member_timestamp(mapper, connection, target):
    """Auto-update timestamp on pool member changes"""
    target.updated_at = datetime.now(timezone.utc)


@event.listens_for(WarmupConversation, "after_update")
def log_conversation_status_change(mapper, connection, target):
    """Log conversation status changes for debugging"""
    logger.debug(
        f"[WarmupConv] Conversation {target.id} updated: status={target.status}"
    )


# ============================================================================
# EXPORT ALL MODELS
# ============================================================================

__all__ = [
    # Enums
    "PoolTierEnum",
    "PoolMemberStatusEnum",
    "ConversationStatusEnum",
    "PlacementResultEnum",
    "BlacklistStatusEnum",
    # Config
    "POOL_TIER_CONFIG",
    "QUALITY_SCORE_WEIGHTS",
    # Models
    "WarmupPoolMember",
    "WarmupConversation",
    "InboxPlacementTest",
    "BlacklistStatus",
    "WarmupSchedule",
]
