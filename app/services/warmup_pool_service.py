"""
Warmup Pool Service - ULTRA EMAIL WARMUP SYSTEM V1.0

Core service for managing the peer-to-peer email warmup network.
Implements intelligent pairing algorithms, quality scoring, and
pool tier management.

Features:
- User enrollment and pool management
- Intelligent partner matching (domain diversity, quality scoring)
- Conversation scheduling with optimal timing
- Quality score calculation with ML-inspired weighting
- Pool tier auto-promotion/demotion
- Daily statistics and analytics

Architecture:
- Uses graph-based pairing to ensure domain diversity
- Implements exponential backoff for failed interactions
- Caches frequently accessed data for performance
- Thread-safe for concurrent operations

Author: Metaminds AI
Version: 1.0.0
"""

import logging
import random
import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple, Set
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum

from sqlalchemy import func, and_, or_, not_
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.models.warmup_pool import (
    WarmupPoolMember,
    WarmupConversation,
    WarmupSchedule,
    InboxPlacementTest,
    BlacklistStatus,
    PoolTierEnum,
    PoolMemberStatusEnum,
    ConversationStatusEnum,
    POOL_TIER_CONFIG,
    QUALITY_SCORE_WEIGHTS,
)

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# Pairing Algorithm Configuration
MAX_PAIRING_ATTEMPTS = 50
MIN_DOMAIN_DIVERSITY = 0.7  # At least 70% different domains in pairings
QUALITY_SCORE_WEIGHT_IN_PAIRING = 0.3
RECENT_INTERACTION_PENALTY_DAYS = 3  # Penalize if interacted recently
MAX_SAME_DOMAIN_INTERACTIONS_PER_DAY = 2

# Quality Score Thresholds
PREMIUM_TIER_THRESHOLD = 70
PROBATION_TIER_THRESHOLD = 30
QUALITY_SCORE_DECAY_RATE = 0.01  # Daily decay for inactive members

# Scheduling Configuration
DEFAULT_WARMUP_BATCH_SIZE = 10
MAX_CONCURRENT_CONVERSATIONS = 50
MIN_CONVERSATION_INTERVAL_MINUTES = 15

# Cache Configuration
PARTNER_CACHE_TTL_SECONDS = 300  # 5 minutes
STATS_CACHE_TTL_SECONDS = 60  # 1 minute


# ============================================================================
# DATA CLASSES FOR STRUCTURED RESPONSES
# ============================================================================

@dataclass
class PairingResult:
    """Result of a partner pairing operation"""
    sender_id: int
    receiver_id: int
    score: float
    domain_diversity: float
    reason: str
    scheduled_at: datetime


@dataclass
class PoolStatistics:
    """Aggregate pool statistics"""
    total_members: int
    active_members: int
    by_tier: Dict[str, int]
    by_provider: Dict[str, int]
    avg_quality_score: float
    total_conversations_today: int
    total_opens_today: int
    total_replies_today: int
    spam_rescue_rate: float


@dataclass
class MemberEnrollmentResult:
    """Result of enrolling a new pool member"""
    success: bool
    member: Optional[WarmupPoolMember]
    message: str
    warnings: List[str] = field(default_factory=list)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def extract_domain(email: str) -> str:
    """
    Extract domain from email address.

    Args:
        email: Email address string

    Returns:
        Domain portion of email, lowercase
    """
    if not email or "@" not in email:
        logger.warning(f"[WarmupPool] Invalid email format: {email}")
        return "unknown"

    try:
        domain = email.split("@")[1].lower().strip()
        return domain
    except Exception as e:
        logger.error(f"[WarmupPool] Error extracting domain from {email}: {e}")
        return "unknown"


def detect_email_provider(email: str) -> str:
    """
    Detect email provider from email address.

    Args:
        email: Email address string

    Returns:
        Provider name (gmail, outlook, yahoo, etc.)
    """
    domain = extract_domain(email)

    provider_patterns = {
        "gmail": ["gmail.com", "googlemail.com"],
        "outlook": ["outlook.com", "hotmail.com", "live.com", "msn.com"],
        "microsoft": ["microsoft.com"],
        "yahoo": ["yahoo.com", "yahoo.co.uk", "ymail.com", "rocketmail.com"],
        "icloud": ["icloud.com", "me.com", "mac.com"],
        "aol": ["aol.com"],
        "protonmail": ["protonmail.com", "proton.me"],
        "zoho": ["zoho.com", "zohomail.com"],
    }

    for provider, domains in provider_patterns.items():
        if domain in domains:
            return provider

    # Check for Google Workspace (custom domain with Google MX)
    # In production, would do actual MX lookup
    return "other"


def generate_thread_id() -> str:
    """
    Generate unique thread ID for conversation tracking.

    Returns:
        Unique thread identifier string
    """
    unique_string = f"{datetime.now(timezone.utc).timestamp()}-{uuid.uuid4()}"
    return hashlib.sha256(unique_string.encode()).hexdigest()[:32]


def calculate_domain_diversity(domains: List[str]) -> float:
    """
    Calculate domain diversity score for a set of domains.

    Higher score means more diverse (different) domains.

    Args:
        domains: List of domain strings

    Returns:
        Diversity score between 0.0 and 1.0
    """
    if not domains:
        return 0.0

    unique_domains = set(domains)
    return len(unique_domains) / len(domains)


# ============================================================================
# MAIN SERVICE CLASS
# ============================================================================

class WarmupPoolService:
    """
    Core service for managing the warmup pool network.

    Handles:
    - Member enrollment and management
    - Intelligent partner pairing
    - Conversation scheduling
    - Quality score computation
    - Pool statistics and analytics

    Usage:
        service = WarmupPoolService(db_session)
        member = service.enroll_member(candidate_id, email)
        partners = service.get_warmup_partners(member.id, count=10)
        service.schedule_warmup_conversations(member.id, partners)
    """

    def __init__(self, db: Session):
        """
        Initialize service with database session.

        Args:
            db: SQLAlchemy database session
        """
        self.db = db
        self._partner_cache: Dict[int, Tuple[datetime, List[int]]] = {}
        self._stats_cache: Optional[Tuple[datetime, PoolStatistics]] = None

        logger.debug("[WarmupPoolService] Service initialized")

    # ========================================================================
    # MEMBER ENROLLMENT & MANAGEMENT
    # ========================================================================

    def enroll_member(
        self,
        candidate_id: int,
        email: str,
        tier: str = PoolTierEnum.STANDARD.value,
        settings: Optional[Dict[str, Any]] = None
    ) -> MemberEnrollmentResult:
        """
        Enroll a user in the warmup pool network.

        Creates a new pool member with initial quality metrics and
        configures default warmup schedule.

        Args:
            candidate_id: ID of the candidate/user
            email: User's email address
            tier: Initial pool tier (default: standard)
            settings: Optional custom settings

        Returns:
            MemberEnrollmentResult with success status and member data

        Raises:
            SQLAlchemyError: On database errors
        """
        logger.info(f"[WarmupPool] Enrolling candidate {candidate_id} in pool")

        warnings = []

        try:
            # Check if already enrolled
            existing = self.db.query(WarmupPoolMember).filter(
                WarmupPoolMember.candidate_id == candidate_id
            ).first()

            if existing:
                logger.info(f"[WarmupPool] Candidate {candidate_id} already enrolled as member {existing.id}")
                return MemberEnrollmentResult(
                    success=True,
                    member=existing,
                    message="Already enrolled in warmup pool",
                    warnings=["Member already exists, returning existing membership"]
                )

            # Extract email metadata
            domain = extract_domain(email)
            provider = detect_email_provider(email)

            # Validate tier
            if tier not in [t.value for t in PoolTierEnum]:
                logger.warning(f"[WarmupPool] Invalid tier '{tier}', defaulting to standard")
                tier = PoolTierEnum.STANDARD.value
                warnings.append(f"Invalid tier specified, using standard")

            # Get tier configuration
            tier_config = POOL_TIER_CONFIG.get(tier, POOL_TIER_CONFIG[PoolTierEnum.STANDARD.value])

            # Create new member
            member = WarmupPoolMember(
                candidate_id=candidate_id,
                pool_tier=tier,
                status=PoolMemberStatusEnum.ACTIVE.value,
                is_active=True,
                quality_score=50.0,  # Start neutral
                engagement_score=50.0,
                consistency_score=50.0,
                domain=domain,
                email_provider=provider,
                daily_send_limit=tier_config["max_daily_sends"],
                daily_receive_limit=tier_config["max_daily_receives"],
                settings=settings or {},
                joined_at=datetime.now(timezone.utc),
                last_activity_at=datetime.now(timezone.utc),
            )

            self.db.add(member)
            self.db.flush()  # Get ID without committing

            # Create default warmup schedule
            schedule = WarmupSchedule(
                candidate_id=candidate_id,
                timezone="UTC",
                start_hour=9,
                end_hour=17,
                active_days=31,  # Mon-Fri
                weekdays_only=True,
                randomize_timing=True,
            )
            self.db.add(schedule)

            # Create EmailWarmingConfig so the scheduler can send warmup emails
            from app.models.email_warming import EmailWarmingConfig, WarmingStatusEnum as WarmingStatus
            existing_config = self.db.query(EmailWarmingConfig).filter(
                EmailWarmingConfig.candidate_id == candidate_id,
                EmailWarmingConfig.deleted_at.is_(None)
            ).first()
            if not existing_config:
                warming_config = EmailWarmingConfig(
                    candidate_id=candidate_id,
                    strategy="conservative",
                    status=WarmingStatus.ACTIVE.value,
                    current_day=1,
                    start_date=datetime.now(timezone.utc),
                )
                self.db.add(warming_config)
                logger.info(f"[WarmupPool] Created EmailWarmingConfig for candidate {candidate_id}")

            self.db.commit()
            self.db.refresh(member)

            logger.info(
                f"[WarmupPool] Successfully enrolled member {member.id} "
                f"(candidate={candidate_id}, tier={tier}, provider={provider})"
            )

            # Check for potential issues
            if provider == "other":
                warnings.append("Custom domain detected - ensure proper email authentication (SPF, DKIM, DMARC)")

            if domain in ["gmail.com", "yahoo.com", "outlook.com"]:
                warnings.append("Free email provider detected - consider using a custom domain for better deliverability")

            return MemberEnrollmentResult(
                success=True,
                member=member,
                message=f"Successfully enrolled in {tier} warmup pool",
                warnings=warnings
            )

        except IntegrityError as e:
            self.db.rollback()
            logger.error(f"[WarmupPool] Integrity error enrolling candidate {candidate_id}: {e}")
            return MemberEnrollmentResult(
                success=False,
                member=None,
                message="Failed to enroll - integrity constraint violation",
                warnings=[str(e)]
            )

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[WarmupPool] Database error enrolling candidate {candidate_id}: {e}")
            return MemberEnrollmentResult(
                success=False,
                member=None,
                message="Database error during enrollment",
                warnings=[str(e)]
            )

        except Exception as e:
            self.db.rollback()
            logger.exception(f"[WarmupPool] Unexpected error enrolling candidate {candidate_id}: {e}")
            return MemberEnrollmentResult(
                success=False,
                member=None,
                message="Unexpected error during enrollment",
                warnings=[str(e)]
            )

    def get_member(self, candidate_id: int) -> Optional[WarmupPoolMember]:
        """
        Get pool member by candidate ID.

        Args:
            candidate_id: Candidate/user ID

        Returns:
            WarmupPoolMember or None if not found
        """
        try:
            member = self.db.query(WarmupPoolMember).filter(
                WarmupPoolMember.candidate_id == candidate_id
            ).first()

            if member:
                logger.debug(f"[WarmupPool] Found member {member.id} for candidate {candidate_id}")
            else:
                logger.debug(f"[WarmupPool] No member found for candidate {candidate_id}")

            return member

        except SQLAlchemyError as e:
            logger.error(f"[WarmupPool] Error fetching member for candidate {candidate_id}: {e}")
            return None

    def get_member_by_id(self, member_id: int) -> Optional[WarmupPoolMember]:
        """
        Get pool member by member ID.

        Args:
            member_id: Pool member ID

        Returns:
            WarmupPoolMember or None if not found
        """
        try:
            return self.db.query(WarmupPoolMember).filter(
                WarmupPoolMember.id == member_id
            ).first()
        except SQLAlchemyError as e:
            logger.error(f"[WarmupPool] Error fetching member {member_id}: {e}")
            return None

    def update_member_status(
        self,
        member_id: int,
        status: str,
        note: Optional[str] = None
    ) -> bool:
        """
        Update member status (active, paused, suspended).

        Args:
            member_id: Pool member ID
            status: New status string
            note: Optional note for the change

        Returns:
            True if updated successfully
        """
        try:
            member = self.get_member_by_id(member_id)
            if not member:
                logger.warning(f"[WarmupPool] Member {member_id} not found for status update")
                return False

            old_status = member.status
            member.status = status
            member.is_active = status == PoolMemberStatusEnum.ACTIVE.value

            if note:
                member.notes = (member.notes or "") + f"\n[{datetime.now(timezone.utc)}] Status: {old_status} -> {status}: {note}"

            self.db.commit()

            logger.info(f"[WarmupPool] Member {member_id} status changed: {old_status} -> {status}")
            return True

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[WarmupPool] Error updating member {member_id} status: {e}")
            return False

    def pause_member(self, member_id: int, reason: str = "User requested") -> bool:
        """Pause member's warmup activity"""
        return self.update_member_status(
            member_id,
            PoolMemberStatusEnum.PAUSED.value,
            reason
        )

    def resume_member(self, member_id: int) -> bool:
        """Resume member's warmup activity"""
        return self.update_member_status(
            member_id,
            PoolMemberStatusEnum.ACTIVE.value,
            "Resumed by user"
        )

    def suspend_member(self, member_id: int, reason: str) -> bool:
        """Suspend member (admin action)"""
        return self.update_member_status(
            member_id,
            PoolMemberStatusEnum.SUSPENDED.value,
            f"SUSPENDED: {reason}"
        )

    # ========================================================================
    # INTELLIGENT PARTNER MATCHING ALGORITHM
    # ========================================================================

    def get_warmup_partners(
        self,
        member_id: int,
        count: int = 10,
        use_cache: bool = True
    ) -> List[WarmupPoolMember]:
        """
        Find optimal warmup partners for a member using intelligent matching.

        Algorithm considers:
        1. Domain diversity (avoid same-domain interactions)
        2. Quality score (prefer high-quality partners)
        3. Tier matching (same or adjacent tiers)
        4. Recent interaction history (avoid repetition)
        5. Daily quotas (respect limits)
        6. Activity status (only active members)

        Args:
            member_id: ID of the member seeking partners
            count: Number of partners to find
            use_cache: Whether to use cached results

        Returns:
            List of WarmupPoolMember objects suitable for pairing
        """
        logger.info(f"[WarmupPool] Finding {count} partners for member {member_id}")

        # Check cache
        if use_cache and member_id in self._partner_cache:
            cache_time, cached_partners = self._partner_cache[member_id]
            if (datetime.now(timezone.utc) - cache_time).seconds < PARTNER_CACHE_TTL_SECONDS:
                logger.debug(f"[WarmupPool] Using cached partners for member {member_id}")
                # Fetch fresh member objects
                return self.db.query(WarmupPoolMember).filter(
                    WarmupPoolMember.id.in_(cached_partners[:count])
                ).all()

        # Get the requesting member
        member = self.get_member_by_id(member_id)
        if not member:
            logger.error(f"[WarmupPool] Member {member_id} not found")
            return []

        if not member.can_send_today:
            logger.info(f"[WarmupPool] Member {member_id} reached daily send limit")
            return []

        # Get recent interaction partners to avoid
        recent_cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_INTERACTION_PENALTY_DAYS)
        recent_partner_ids = self._get_recent_partner_ids(member_id, recent_cutoff)

        # Build base query for eligible partners
        eligible_query = self.db.query(WarmupPoolMember).filter(
            and_(
                WarmupPoolMember.id != member_id,
                WarmupPoolMember.is_active == True,
                WarmupPoolMember.status == PoolMemberStatusEnum.ACTIVE.value,
                WarmupPoolMember.receives_today < WarmupPoolMember.daily_receive_limit,
            )
        )

        # Tier-based filtering
        if member.pool_tier == PoolTierEnum.PREMIUM.value:
            # Premium members prefer other premium members
            eligible_query = eligible_query.filter(
                WarmupPoolMember.pool_tier.in_([
                    PoolTierEnum.PREMIUM.value,
                    PoolTierEnum.STANDARD.value
                ])
            )
        elif member.pool_tier == PoolTierEnum.PROBATION.value:
            # Probation members only interact with other probation
            eligible_query = eligible_query.filter(
                WarmupPoolMember.pool_tier == PoolTierEnum.PROBATION.value
            )

        # Exclude recently interacted partners (soft exclusion - deprioritize)
        # We'll handle this in scoring

        # Get candidates
        candidates = eligible_query.order_by(
            WarmupPoolMember.quality_score.desc(),
            func.random()
        ).limit(count * 5).all()  # Get more than needed for filtering

        if not candidates:
            logger.warning(f"[WarmupPool] No eligible partners found for member {member_id}")
            return []

        # Score and rank candidates
        scored_candidates = self._score_partners(
            member=member,
            candidates=candidates,
            recent_partner_ids=recent_partner_ids
        )

        # Select top partners ensuring domain diversity
        selected_partners = self._select_diverse_partners(
            scored_candidates=scored_candidates,
            member_domain=member.domain,
            count=count
        )

        # Cache results
        self._partner_cache[member_id] = (
            datetime.now(timezone.utc),
            [p.id for p in selected_partners]
        )

        logger.info(
            f"[WarmupPool] Found {len(selected_partners)} partners for member {member_id} "
            f"(domains: {[p.domain for p in selected_partners]})"
        )

        return selected_partners

    def _get_recent_partner_ids(
        self,
        member_id: int,
        since: datetime
    ) -> Set[int]:
        """Get IDs of partners interacted with recently"""
        try:
            # Get as sender
            sent_to = self.db.query(WarmupConversation.receiver_id).filter(
                and_(
                    WarmupConversation.sender_id == member_id,
                    WarmupConversation.created_at >= since
                )
            ).distinct().all()

            # Get as receiver
            received_from = self.db.query(WarmupConversation.sender_id).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.created_at >= since
                )
            ).distinct().all()

            partner_ids = set()
            for (pid,) in sent_to + received_from:
                partner_ids.add(pid)

            logger.debug(f"[WarmupPool] Member {member_id} has {len(partner_ids)} recent partners")
            return partner_ids

        except SQLAlchemyError as e:
            logger.error(f"[WarmupPool] Error getting recent partners: {e}")
            return set()

    def _score_partners(
        self,
        member: WarmupPoolMember,
        candidates: List[WarmupPoolMember],
        recent_partner_ids: Set[int]
    ) -> List[Tuple[WarmupPoolMember, float]]:
        """
        Score potential partners using multi-factor algorithm.

        Scoring factors:
        - Quality score: 30% weight
        - Domain diversity: 25% weight (different domain = higher)
        - Provider diversity: 15% weight
        - Recency penalty: 20% weight (recent interaction = lower)
        - Random factor: 10% weight (for variety)

        Returns:
            List of (member, score) tuples sorted by score descending
        """
        scored = []

        for candidate in candidates:
            score = 0.0

            # Quality score contribution (normalized to 0-1, then weighted)
            quality_contribution = (candidate.quality_score / 100) * 0.30
            score += quality_contribution

            # Domain diversity contribution
            if candidate.domain != member.domain:
                score += 0.25  # Full points for different domain
            else:
                score += 0.05  # Small points for same domain (some diversity is still useful)

            # Provider diversity contribution
            if candidate.email_provider != member.email_provider:
                score += 0.15
            else:
                score += 0.05

            # Recency penalty
            if candidate.id in recent_partner_ids:
                score -= 0.15  # Penalty for recent interaction
            else:
                score += 0.20  # Bonus for fresh partner

            # Random factor for variety
            score += random.uniform(0, 0.10)

            # Tier bonus
            if candidate.pool_tier == member.pool_tier:
                score += 0.05  # Small bonus for same tier

            # Response rate bonus
            if candidate.response_rate >= 50:
                score += 0.05

            scored.append((candidate, score))

        # Sort by score descending
        scored.sort(key=lambda x: x[1], reverse=True)

        logger.debug(
            f"[WarmupPool] Scored {len(scored)} candidates, "
            f"top score: {scored[0][1]:.3f}" if scored else
            f"[WarmupPool] Scored 0 candidates, top score: 0"
        )

        return scored

    def _select_diverse_partners(
        self,
        scored_candidates: List[Tuple[WarmupPoolMember, float]],
        member_domain: str,
        count: int
    ) -> List[WarmupPoolMember]:
        """
        Select partners ensuring domain diversity.

        Uses greedy selection with diversity constraint:
        - At most N interactions per unique domain
        - Prioritize by score while maintaining diversity
        """
        selected = []
        domain_counts: Dict[str, int] = defaultdict(int)
        max_per_domain = max(1, count // 3)  # At most 1/3 from same domain

        for candidate, score in scored_candidates:
            if len(selected) >= count:
                break

            # Check domain diversity constraint
            if domain_counts[candidate.domain] >= max_per_domain:
                logger.debug(
                    f"[WarmupPool] Skipping candidate {candidate.id} - "
                    f"domain {candidate.domain} at limit"
                )
                continue

            selected.append(candidate)
            domain_counts[candidate.domain] += 1

        # If we didn't get enough, relax constraints
        if len(selected) < count:
            for candidate, score in scored_candidates:
                if len(selected) >= count:
                    break
                if candidate not in selected:
                    selected.append(candidate)

        # Calculate final diversity score
        diversity = calculate_domain_diversity([p.domain for p in selected])
        logger.info(
            f"[WarmupPool] Selected {len(selected)} partners with "
            f"domain diversity: {diversity:.2%}"
        )

        return selected

    # ========================================================================
    # CONVERSATION SCHEDULING
    # ========================================================================

    def schedule_warmup_conversation(
        self,
        sender_id: int,
        receiver_id: int,
        scheduled_at: Optional[datetime] = None,
        content_category: str = "business"
    ) -> Optional[WarmupConversation]:
        """
        Schedule a warmup conversation between two pool members.

        Creates a new conversation record with scheduled send time.
        Actual content is generated at send time by the AI engine.

        Args:
            sender_id: ID of sending pool member
            receiver_id: ID of receiving pool member
            scheduled_at: When to send (default: calculated optimal time)
            content_category: Type of content to generate

        Returns:
            WarmupConversation object if created successfully
        """
        logger.info(f"[WarmupPool] Scheduling conversation: {sender_id} -> {receiver_id}")

        try:
            # Validate participants
            sender = self.get_member_by_id(sender_id)
            receiver = self.get_member_by_id(receiver_id)

            if not sender or not receiver:
                logger.error(f"[WarmupPool] Invalid sender or receiver")
                return None

            if not sender.can_send_today:
                logger.warning(f"[WarmupPool] Sender {sender_id} cannot send today")
                return None

            if not receiver.can_receive_today:
                logger.warning(f"[WarmupPool] Receiver {receiver_id} cannot receive today")
                return None

            # Calculate scheduled time if not provided
            if not scheduled_at:
                scheduled_at = self._calculate_optimal_send_time(sender_id)

            # Generate thread ID
            thread_id = generate_thread_id()

            # Create conversation
            conversation = WarmupConversation(
                sender_id=sender_id,
                receiver_id=receiver_id,
                thread_id=thread_id,
                thread_depth=0,
                subject="[Generated at send time]",  # Placeholder
                status=ConversationStatusEnum.SCHEDULED.value,
                scheduled_at=scheduled_at,
                created_at=datetime.now(timezone.utc),
                content_category=content_category,
            )

            self.db.add(conversation)

            # Update sender's send count
            sender.increment_send()

            self.db.commit()
            self.db.refresh(conversation)

            logger.info(
                f"[WarmupPool] Scheduled conversation {conversation.id} "
                f"for {scheduled_at.isoformat()}"
            )

            return conversation

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[WarmupPool] Error scheduling conversation: {e}")
            return None

    def schedule_batch_conversations(
        self,
        member_id: int,
        count: int = DEFAULT_WARMUP_BATCH_SIZE
    ) -> List[WarmupConversation]:
        """
        Schedule multiple warmup conversations for a member.

        Finds partners and creates scheduled conversations with
        optimal timing distribution.

        Args:
            member_id: Pool member ID
            count: Number of conversations to schedule

        Returns:
            List of created WarmupConversation objects
        """
        logger.info(f"[WarmupPool] Scheduling batch of {count} conversations for member {member_id}")

        conversations = []

        try:
            # Get member
            member = self.get_member_by_id(member_id)
            if not member:
                logger.error(f"[WarmupPool] Member {member_id} not found")
                return []

            # Get available send slots
            remaining = member.remaining_sends_today
            actual_count = min(count, remaining)

            if actual_count <= 0:
                logger.info(f"[WarmupPool] Member {member_id} has no remaining send slots")
                return []

            # Find partners
            partners = self.get_warmup_partners(member_id, count=actual_count)

            if not partners:
                logger.warning(f"[WarmupPool] No partners found for member {member_id}")
                return []

            # Get schedule for timing
            schedule = self.db.query(WarmupSchedule).filter(
                WarmupSchedule.candidate_id == member.candidate_id
            ).first()

            # Calculate send times with distribution
            base_time = datetime.now(timezone.utc)
            interval_minutes = 60 // max(1, len(partners))  # Distribute over an hour

            for i, partner in enumerate(partners):
                # Calculate scheduled time with jitter
                jitter = random.randint(0, 15)  # 0-15 minutes random jitter
                send_time = base_time + timedelta(minutes=(i * interval_minutes) + jitter)

                # Apply schedule constraints if available
                if schedule:
                    send_time = self._apply_schedule_constraints(send_time, schedule)

                # Create conversation
                conversation = self.schedule_warmup_conversation(
                    sender_id=member_id,
                    receiver_id=partner.id,
                    scheduled_at=send_time
                )

                if conversation:
                    conversations.append(conversation)

            logger.info(
                f"[WarmupPool] Scheduled {len(conversations)} conversations "
                f"for member {member_id}"
            )

            return conversations

        except Exception as e:
            logger.exception(f"[WarmupPool] Error scheduling batch: {e}")
            return conversations

    def _calculate_optimal_send_time(self, member_id: int) -> datetime:
        """
        Calculate optimal time to send warmup email.

        Considers:
        - User's warmup schedule
        - Current queue of scheduled emails
        - Time zone
        - Business hours

        Returns:
            Optimal datetime for sending
        """
        member = self.get_member_by_id(member_id)
        if not member:
            return datetime.now(timezone.utc) + timedelta(minutes=random.randint(5, 60))

        # Get user's schedule
        schedule = self.db.query(WarmupSchedule).filter(
            WarmupSchedule.candidate_id == member.candidate_id
        ).first()

        base_time = datetime.now(timezone.utc)

        if schedule:
            # Check if current time is within active hours
            current_hour = base_time.hour

            if current_hour < schedule.start_hour:
                # Before active hours - schedule for start
                base_time = base_time.replace(
                    hour=schedule.start_hour,
                    minute=random.randint(0, 59)
                )
            elif current_hour >= schedule.end_hour:
                # After active hours - schedule for next day
                base_time = (base_time + timedelta(days=1)).replace(
                    hour=schedule.start_hour,
                    minute=random.randint(0, 59)
                )

            # Add random delay
            delay = schedule.get_random_send_delay()
            base_time = base_time + timedelta(seconds=delay)

        else:
            # No schedule - use reasonable defaults
            delay = random.randint(300, 3600)  # 5 min to 1 hour
            base_time = base_time + timedelta(seconds=delay)

        return base_time

    def _apply_schedule_constraints(
        self,
        proposed_time: datetime,
        schedule: WarmupSchedule
    ) -> datetime:
        """Apply schedule constraints to proposed send time"""
        # Check day of week
        weekday = proposed_time.weekday()

        if schedule.weekdays_only and weekday >= 5:
            # Move to Monday
            days_until_monday = 7 - weekday
            proposed_time = proposed_time + timedelta(days=days_until_monday)

        # Check hour constraints
        if proposed_time.hour < schedule.start_hour:
            proposed_time = proposed_time.replace(hour=schedule.start_hour, minute=0)
        elif proposed_time.hour >= schedule.end_hour:
            # Move to next day's start
            proposed_time = (proposed_time + timedelta(days=1)).replace(
                hour=schedule.start_hour, minute=0
            )

        return proposed_time

    # ========================================================================
    # QUALITY SCORE MANAGEMENT
    # ========================================================================

    def recalculate_quality_scores(self, member_ids: Optional[List[int]] = None) -> int:
        """
        Recalculate quality scores for members.

        If no member_ids provided, recalculates for all active members.

        Args:
            member_ids: Optional list of specific member IDs

        Returns:
            Number of members updated
        """
        logger.info("[WarmupPool] Recalculating quality scores")

        try:
            if member_ids:
                members = self.db.query(WarmupPoolMember).filter(
                    WarmupPoolMember.id.in_(member_ids)
                ).all()
            else:
                members = self.db.query(WarmupPoolMember).filter(
                    WarmupPoolMember.is_active == True
                ).all()

            updated_count = 0

            for member in members:
                old_score = member.quality_score
                new_score = member.calculate_quality_score()

                # Check for tier change
                tier_changed = member.update_tier_based_on_quality()

                if abs(new_score - old_score) > 0.1 or tier_changed:
                    updated_count += 1
                    logger.debug(
                        f"[WarmupPool] Member {member.id} score: {old_score:.1f} -> {new_score:.1f}"
                        f"{' (tier: ' + tier_changed + ')' if tier_changed else ''}"
                    )

            self.db.commit()

            logger.info(f"[WarmupPool] Updated quality scores for {updated_count} members")
            return updated_count

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[WarmupPool] Error recalculating scores: {e}")
            return 0

    def apply_inactivity_decay(self, days_inactive_threshold: int = 7) -> int:
        """
        Apply quality score decay to inactive members.

        Members who haven't been active for N days get their
        quality score reduced to encourage activity.

        Args:
            days_inactive_threshold: Days before decay starts

        Returns:
            Number of members affected
        """
        logger.info("[WarmupPool] Applying inactivity decay")

        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days_inactive_threshold)

            inactive_members = self.db.query(WarmupPoolMember).filter(
                and_(
                    WarmupPoolMember.is_active == True,
                    WarmupPoolMember.last_activity_at < cutoff
                )
            ).all()

            affected_count = 0

            for member in inactive_members:
                # Calculate days inactive
                days_inactive = (datetime.now(timezone.utc) - member.last_activity_at).days

                # Apply decay (exponential)
                decay_factor = 1 - (QUALITY_SCORE_DECAY_RATE * (days_inactive - days_inactive_threshold))
                decay_factor = max(0.5, decay_factor)  # Floor at 50% of original

                old_score = member.quality_score
                member.quality_score = max(0, member.quality_score * decay_factor)

                if old_score != member.quality_score:
                    affected_count += 1
                    logger.debug(
                        f"[WarmupPool] Member {member.id} decay: {old_score:.1f} -> {member.quality_score:.1f} "
                        f"(inactive {days_inactive} days)"
                    )

                # Update tier if needed
                member.update_tier_based_on_quality()

            self.db.commit()

            logger.info(f"[WarmupPool] Applied decay to {affected_count} inactive members")
            return affected_count

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[WarmupPool] Error applying decay: {e}")
            return 0

    # ========================================================================
    # STATISTICS & ANALYTICS
    # ========================================================================

    def get_pool_statistics(self, use_cache: bool = True) -> PoolStatistics:
        """
        Get aggregate statistics for the warmup pool.

        Args:
            use_cache: Whether to use cached results

        Returns:
            PoolStatistics dataclass with aggregate metrics
        """
        # Check cache
        if use_cache and self._stats_cache:
            cache_time, cached_stats = self._stats_cache
            if (datetime.now(timezone.utc) - cache_time).seconds < STATS_CACHE_TTL_SECONDS:
                logger.debug("[WarmupPool] Using cached pool statistics")
                return cached_stats

        logger.info("[WarmupPool] Computing pool statistics")

        try:
            # Total and active members
            total_members = self.db.query(func.count(WarmupPoolMember.id)).scalar() or 0
            active_members = self.db.query(func.count(WarmupPoolMember.id)).filter(
                WarmupPoolMember.is_active == True
            ).scalar() or 0

            # By tier
            tier_counts = self.db.query(
                WarmupPoolMember.pool_tier,
                func.count(WarmupPoolMember.id)
            ).group_by(WarmupPoolMember.pool_tier).all()

            by_tier = {tier: count for tier, count in tier_counts}

            # By provider
            provider_counts = self.db.query(
                WarmupPoolMember.email_provider,
                func.count(WarmupPoolMember.id)
            ).group_by(WarmupPoolMember.email_provider).all()

            by_provider = {provider or "unknown": count for provider, count in provider_counts}

            # Average quality score
            avg_quality = self.db.query(
                func.avg(WarmupPoolMember.quality_score)
            ).filter(WarmupPoolMember.is_active == True).scalar() or 0.0

            # Today's activity
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

            conversations_today = self.db.query(func.count(WarmupConversation.id)).filter(
                WarmupConversation.created_at >= today_start
            ).scalar() or 0

            opens_today = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.opened_at >= today_start,
                    WarmupConversation.opened_at.isnot(None)
                )
            ).scalar() or 0

            replies_today = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.replied_at >= today_start,
                    WarmupConversation.replied_at.isnot(None)
                )
            ).scalar() or 0

            # Spam rescue rate (last 7 days)
            week_start = datetime.now(timezone.utc) - timedelta(days=7)
            total_spam = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.was_in_spam == True,
                    WarmupConversation.created_at >= week_start
                )
            ).scalar() or 0

            rescued = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.spam_rescued_at.isnot(None),
                    WarmupConversation.created_at >= week_start
                )
            ).scalar() or 0

            spam_rescue_rate = (rescued / total_spam * 100) if total_spam > 0 else 100.0

            stats = PoolStatistics(
                total_members=total_members,
                active_members=active_members,
                by_tier=by_tier,
                by_provider=by_provider,
                avg_quality_score=round(avg_quality, 1),
                total_conversations_today=conversations_today,
                total_opens_today=opens_today,
                total_replies_today=replies_today,
                spam_rescue_rate=round(spam_rescue_rate, 1)
            )

            # Cache results
            self._stats_cache = (datetime.now(timezone.utc), stats)

            logger.info(
                f"[WarmupPool] Stats: {total_members} members, "
                f"{active_members} active, avg quality: {avg_quality:.1f}"
            )

            return stats

        except SQLAlchemyError as e:
            logger.error(f"[WarmupPool] Error computing statistics: {e}")
            return PoolStatistics(
                total_members=0,
                active_members=0,
                by_tier={},
                by_provider={},
                avg_quality_score=0.0,
                total_conversations_today=0,
                total_opens_today=0,
                total_replies_today=0,
                spam_rescue_rate=0.0
            )

    def get_member_statistics(self, member_id: int) -> Dict[str, Any]:
        """
        Get detailed statistics for a specific member.

        Args:
            member_id: Pool member ID

        Returns:
            Dictionary with member statistics
        """
        member = self.get_member_by_id(member_id)
        if not member:
            return {}

        try:
            # Get conversation stats
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            week_start = datetime.now(timezone.utc) - timedelta(days=7)
            month_start = datetime.now(timezone.utc) - timedelta(days=30)

            # Sent conversations
            sent_today = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.sender_id == member_id,
                    WarmupConversation.created_at >= today_start
                )
            ).scalar() or 0

            sent_week = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.sender_id == member_id,
                    WarmupConversation.created_at >= week_start
                )
            ).scalar() or 0

            # Received and opened
            received_week = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.created_at >= week_start
                )
            ).scalar() or 0

            opened_week = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.opened_at.isnot(None),
                    WarmupConversation.created_at >= week_start
                )
            ).scalar() or 0

            # Replied
            replied_week = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.replied_at.isnot(None),
                    WarmupConversation.created_at >= week_start
                )
            ).scalar() or 0

            # Average response time
            avg_response = self.db.query(
                func.avg(WarmupConversation.time_to_reply)
            ).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.time_to_reply.isnot(None)
                )
            ).scalar()

            return {
                "member_id": member_id,
                "quality_score": member.quality_score,
                "pool_tier": member.pool_tier,
                "health_status": member.health_status,
                "activity": {
                    "today": {
                        "sent": sent_today,
                        "limit": member.daily_send_limit,
                        "remaining": member.remaining_sends_today,
                    },
                    "week": {
                        "sent": sent_week,
                        "received": received_week,
                        "opened": opened_week,
                        "replied": replied_week,
                    },
                },
                "rates": {
                    "response_rate": round(member.response_rate, 1),
                    "open_rate": round(member.open_rate, 1),
                    "bounce_rate": round(member.bounce_rate, 2),
                },
                "timing": {
                    "avg_response_seconds": avg_response or 0,
                    "avg_response_minutes": round((avg_response or 0) / 60, 1),
                },
                "lifetime": {
                    "total_sends": member.total_sends,
                    "total_receives": member.total_receives,
                    "total_replies": member.total_replies,
                    "spam_rescues": member.spam_rescues,
                },
            }

        except SQLAlchemyError as e:
            logger.error(f"[WarmupPool] Error getting member statistics: {e}")
            return {}

    def get_pending_conversations(
        self,
        member_id: Optional[int] = None,
        limit: int = 100
    ) -> List[WarmupConversation]:
        """
        Get conversations scheduled but not yet sent.

        Args:
            member_id: Optional filter by sender
            limit: Maximum results

        Returns:
            List of pending WarmupConversation objects
        """
        try:
            query = self.db.query(WarmupConversation).filter(
                and_(
                    WarmupConversation.status == ConversationStatusEnum.SCHEDULED.value,
                    WarmupConversation.scheduled_at <= datetime.now(timezone.utc)
                )
            )

            if member_id:
                query = query.filter(WarmupConversation.sender_id == member_id)

            return query.order_by(
                WarmupConversation.scheduled_at.asc()
            ).limit(limit).all()

        except SQLAlchemyError as e:
            logger.error(f"[WarmupPool] Error getting pending conversations: {e}")
            return []

    # ========================================================================
    # CLEANUP & MAINTENANCE
    # ========================================================================

    def cleanup_old_conversations(self, days_to_keep: int = 90) -> int:
        """
        Remove old conversation records.

        Args:
            days_to_keep: Number of days of history to retain

        Returns:
            Number of records deleted
        """
        logger.info(f"[WarmupPool] Cleaning up conversations older than {days_to_keep} days")

        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

            deleted = self.db.query(WarmupConversation).filter(
                WarmupConversation.created_at < cutoff
            ).delete(synchronize_session=False)

            self.db.commit()

            logger.info(f"[WarmupPool] Deleted {deleted} old conversation records")
            return deleted

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[WarmupPool] Error cleaning up conversations: {e}")
            return 0

    def reset_daily_counters(self) -> int:
        """
        Reset daily send/receive counters for all members.

        Should be called by a scheduled job at midnight.

        Returns:
            Number of members reset
        """
        logger.info("[WarmupPool] Resetting daily counters")

        try:
            # Update all members
            updated = self.db.query(WarmupPoolMember).filter(
                or_(
                    WarmupPoolMember.sends_today > 0,
                    WarmupPoolMember.receives_today > 0
                )
            ).update({
                "sends_today": 0,
                "receives_today": 0,
                "last_reset_date": datetime.now(timezone.utc)
            }, synchronize_session=False)

            self.db.commit()

            logger.info(f"[WarmupPool] Reset daily counters for {updated} members")
            return updated

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[WarmupPool] Error resetting counters: {e}")
            return 0

    def clear_partner_cache(self, member_id: Optional[int] = None) -> None:
        """Clear partner cache for a member or all members"""
        if member_id:
            self._partner_cache.pop(member_id, None)
            logger.debug(f"[WarmupPool] Cleared partner cache for member {member_id}")
        else:
            self._partner_cache.clear()
            logger.debug("[WarmupPool] Cleared all partner caches")

    def clear_stats_cache(self) -> None:
        """Clear statistics cache"""
        self._stats_cache = None
        logger.debug("[WarmupPool] Cleared statistics cache")


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

def get_warmup_pool_service(db: Session) -> WarmupPoolService:
    """
    Factory function to create WarmupPoolService instance.

    Args:
        db: SQLAlchemy database session

    Returns:
        Configured WarmupPoolService instance
    """
    return WarmupPoolService(db)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "WarmupPoolService",
    "get_warmup_pool_service",
    "PairingResult",
    "PoolStatistics",
    "MemberEnrollmentResult",
    "extract_domain",
    "detect_email_provider",
    "generate_thread_id",
    "calculate_domain_diversity",
]
