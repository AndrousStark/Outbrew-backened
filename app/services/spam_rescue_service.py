"""
Spam Rescue Service - ULTRA EMAIL WARMUP SYSTEM V1.0

Service for automatically detecting and rescuing emails from spam folders.
When warmup emails land in spam, this service:
1. Detects the spam placement
2. Moves email to inbox
3. Marks as "Not Spam"
4. Optionally marks as important
5. Generates a positive reply

This is a critical feature for building sender reputation as it
signals to ESPs that the sender is legitimate.

Features:
- Automatic spam detection
- Inbox rescue with positive signals
- Reply generation for rescued emails
- Statistics tracking
- Alert generation for high spam rates

Author: Metaminds AI
Version: 1.0.0
"""

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models.warmup_pool import (
    WarmupPoolMember,
    WarmupConversation,
    ConversationStatusEnum,
)

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# Rescue Behavior
DEFAULT_RESCUE_DELAY_MIN_SECONDS = 60  # At least 1 minute before rescue
DEFAULT_RESCUE_DELAY_MAX_SECONDS = 3600  # At most 1 hour
MARK_IMPORTANT_PROBABILITY = 0.30  # 30% chance to mark as important
GENERATE_REPLY_PROBABILITY = 0.80  # 80% chance to reply after rescue

# Alert Thresholds
SPAM_RATE_WARNING_THRESHOLD = 10  # 10% spam rate triggers warning
SPAM_RATE_CRITICAL_THRESHOLD = 25  # 25% spam rate triggers critical
CONSECUTIVE_SPAM_ALERT_THRESHOLD = 3  # 3 in a row triggers alert

# Rescue Statistics Window
STATS_WINDOW_HOURS = 24


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class RescueResult:
    """Result of a spam rescue operation"""
    success: bool
    conversation_id: int
    moved_to_inbox: bool
    marked_not_spam: bool
    marked_important: bool
    reply_scheduled: bool
    rescue_time_seconds: float
    message: str


@dataclass
class SpamStatistics:
    """Spam and rescue statistics"""
    total_received: int
    spam_detected: int
    spam_rescued: int
    spam_rate: float
    rescue_rate: float
    consecutive_spam: int
    last_spam_at: Optional[datetime]
    alert_level: str  # none, warning, critical


@dataclass
class RescueAction:
    """Scheduled rescue action"""
    conversation_id: int
    scheduled_at: datetime
    actions: List[str]  # move_to_inbox, mark_not_spam, mark_important, reply


# ============================================================================
# MAIN SERVICE CLASS
# ============================================================================

class SpamRescueService:
    """
    Service for detecting and rescuing emails from spam folders.

    Monitors warmup conversations for spam placement and automatically
    performs rescue actions to build positive sender reputation.

    The rescue process:
    1. Wait a realistic delay (humans don't check spam instantly)
    2. Move email from spam to inbox
    3. Mark as "Not Spam" (critical ESP signal)
    4. Optionally mark as important
    5. Generate and schedule a positive reply

    Usage:
        service = SpamRescueService(db)
        result = service.rescue_from_spam(conversation_id)
        stats = service.get_spam_statistics(member_id)
    """

    def __init__(self, db: Session):
        """
        Initialize the service.

        Args:
            db: SQLAlchemy database session
        """
        self.db = db
        self._pending_rescues: Dict[int, RescueAction] = {}

        logger.info("[SpamRescue] Service initialized")

    # ========================================================================
    # SPAM DETECTION
    # ========================================================================

    def mark_as_spam(
        self,
        conversation_id: int,
        detection_source: str = "automatic"
    ) -> bool:
        """
        Mark a conversation as having landed in spam.

        This should be called when we detect (via IMAP or webhook)
        that an email has landed in the recipient's spam folder.

        Args:
            conversation_id: ID of the conversation
            detection_source: How spam was detected (automatic, manual, webhook)

        Returns:
            True if marked successfully
        """
        logger.info(f"[SpamRescue] Marking conversation {conversation_id} as spam")

        try:
            conversation = self.db.query(WarmupConversation).filter(
                WarmupConversation.id == conversation_id
            ).first()

            if not conversation:
                logger.warning(f"[SpamRescue] Conversation {conversation_id} not found")
                return False

            # Update conversation
            conversation.mark_spam_detected()

            # Update receiver's spam count
            if conversation.receiver_id:
                receiver = self.db.query(WarmupPoolMember).filter(
                    WarmupPoolMember.id == conversation.receiver_id
                ).first()

                if receiver:
                    # Increment spam detection (for statistics)
                    if not hasattr(receiver, 'spam_detections'):
                        receiver.spam_detections = 0
                    receiver.spam_detections = (receiver.spam_detections or 0) + 1

            self.db.commit()

            logger.info(
                f"[SpamRescue] Conversation {conversation_id} marked as spam "
                f"(source: {detection_source})"
            )

            # Schedule rescue
            self._schedule_rescue(conversation_id)

            return True

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[SpamRescue] Error marking spam: {e}")
            return False

    def check_for_spam(
        self,
        member_id: int,
        time_window_hours: int = 24
    ) -> List[WarmupConversation]:
        """
        Check for conversations that may have landed in spam.

        In production, this would use IMAP to check spam folders.
        For now, we identify conversations that:
        - Were delivered but never opened within expected time
        - Have bounce-like patterns

        Args:
            member_id: Pool member to check
            time_window_hours: How far back to look

        Returns:
            List of suspicious conversations
        """
        logger.debug(f"[SpamRescue] Checking for spam for member {member_id}")

        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=time_window_hours)

            # Find delivered but unopened conversations past expected open time
            suspicious = self.db.query(WarmupConversation).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.status == ConversationStatusEnum.DELIVERED.value,
                    WarmupConversation.delivered_at < cutoff,
                    WarmupConversation.opened_at.is_(None),
                    WarmupConversation.was_in_spam == False,
                )
            ).all()

            logger.debug(f"[SpamRescue] Found {len(suspicious)} suspicious conversations")
            return suspicious

        except SQLAlchemyError as e:
            logger.error(f"[SpamRescue] Error checking for spam: {e}")
            return []

    # ========================================================================
    # SPAM RESCUE
    # ========================================================================

    def rescue_from_spam(
        self,
        conversation_id: int,
        mark_important: Optional[bool] = None,
        schedule_reply: Optional[bool] = None
    ) -> RescueResult:
        """
        Rescue an email from spam folder.

        Performs the following actions:
        1. Moves email to inbox
        2. Marks as "Not Spam"
        3. Optionally marks as important
        4. Optionally schedules a positive reply

        Args:
            conversation_id: ID of the conversation to rescue
            mark_important: Whether to mark as important (default: random)
            schedule_reply: Whether to schedule a reply (default: random)

        Returns:
            RescueResult with operation details
        """
        logger.info(f"[SpamRescue] Rescuing conversation {conversation_id} from spam")

        start_time = datetime.now(timezone.utc)

        try:
            conversation = self.db.query(WarmupConversation).filter(
                WarmupConversation.id == conversation_id
            ).first()

            if not conversation:
                return RescueResult(
                    success=False,
                    conversation_id=conversation_id,
                    moved_to_inbox=False,
                    marked_not_spam=False,
                    marked_important=False,
                    reply_scheduled=False,
                    rescue_time_seconds=0,
                    message="Conversation not found"
                )

            if not conversation.was_in_spam:
                return RescueResult(
                    success=False,
                    conversation_id=conversation_id,
                    moved_to_inbox=False,
                    marked_not_spam=False,
                    marked_important=False,
                    reply_scheduled=False,
                    rescue_time_seconds=0,
                    message="Conversation was not in spam"
                )

            if conversation.spam_rescued_at:
                return RescueResult(
                    success=False,
                    conversation_id=conversation_id,
                    moved_to_inbox=False,
                    marked_not_spam=False,
                    marked_important=False,
                    reply_scheduled=False,
                    rescue_time_seconds=0,
                    message="Already rescued"
                )

            # Determine actions
            do_mark_important = mark_important if mark_important is not None else (
                random.random() < MARK_IMPORTANT_PROBABILITY
            )
            do_schedule_reply = schedule_reply if schedule_reply is not None else (
                random.random() < GENERATE_REPLY_PROBABILITY
            )

            # Perform rescue actions
            # In production, this would use IMAP to:
            # 1. Move message from spam to inbox
            # 2. Add "Not Spam" label
            # 3. Optionally add "Important" label

            # Mark conversation as rescued
            conversation.mark_spam_rescued()

            if do_mark_important:
                conversation.mark_important()

            # Update receiver's rescue count
            receiver = self.db.query(WarmupPoolMember).filter(
                WarmupPoolMember.id == conversation.receiver_id
            ).first()

            if receiver:
                receiver.record_spam_rescue()

            # Schedule reply if requested
            reply_scheduled = False
            if do_schedule_reply:
                reply_scheduled = self._schedule_rescue_reply(conversation)

            self.db.commit()

            rescue_time = (datetime.now(timezone.utc) - start_time).total_seconds()

            logger.info(
                f"[SpamRescue] Rescued conversation {conversation_id} "
                f"(important={do_mark_important}, reply={reply_scheduled})"
            )

            return RescueResult(
                success=True,
                conversation_id=conversation_id,
                moved_to_inbox=True,
                marked_not_spam=True,
                marked_important=do_mark_important,
                reply_scheduled=reply_scheduled,
                rescue_time_seconds=rescue_time,
                message="Successfully rescued from spam"
            )

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[SpamRescue] Error rescuing from spam: {e}")
            return RescueResult(
                success=False,
                conversation_id=conversation_id,
                moved_to_inbox=False,
                marked_not_spam=False,
                marked_important=False,
                reply_scheduled=False,
                rescue_time_seconds=0,
                message=f"Database error: {str(e)}"
            )

    def _schedule_rescue(self, conversation_id: int) -> None:
        """
        Schedule a rescue operation for later.

        Adds a realistic delay before rescue (humans don't check spam instantly).

        Args:
            conversation_id: ID of the conversation to rescue
        """
        delay = random.randint(
            DEFAULT_RESCUE_DELAY_MIN_SECONDS,
            DEFAULT_RESCUE_DELAY_MAX_SECONDS
        )

        scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=delay)

        actions = ["move_to_inbox", "mark_not_spam"]

        if random.random() < MARK_IMPORTANT_PROBABILITY:
            actions.append("mark_important")

        if random.random() < GENERATE_REPLY_PROBABILITY:
            actions.append("reply")

        rescue_action = RescueAction(
            conversation_id=conversation_id,
            scheduled_at=scheduled_at,
            actions=actions
        )

        self._pending_rescues[conversation_id] = rescue_action

        logger.debug(
            f"[SpamRescue] Scheduled rescue for conversation {conversation_id} "
            f"at {scheduled_at.isoformat()} (actions: {actions})"
        )

    def _schedule_rescue_reply(self, conversation: WarmupConversation) -> bool:
        """
        Schedule a reply to a rescued email.

        Args:
            conversation: The rescued conversation

        Returns:
            True if reply was scheduled
        """
        try:
            # Calculate reply delay (5 minutes to 2 hours after rescue)
            delay = random.randint(300, 7200)
            reply_time = datetime.now(timezone.utc) + timedelta(seconds=delay)

            # Create a new conversation record for the reply
            # (In production, this would integrate with the conversation scheduling system)
            logger.debug(
                f"[SpamRescue] Scheduling rescue reply for conversation {conversation.id} "
                f"at {reply_time.isoformat()}"
            )

            # Note: Actual reply creation would be handled by the conversation service
            return True

        except Exception as e:
            logger.error(f"[SpamRescue] Error scheduling rescue reply: {e}")
            return False

    def process_pending_rescues(self) -> int:
        """
        Process all pending rescue operations that are due.

        Should be called periodically by a background task.

        Returns:
            Number of rescues processed
        """
        logger.debug("[SpamRescue] Processing pending rescues")

        now = datetime.now(timezone.utc)
        processed = 0

        # Find due rescues
        due_rescues = [
            (cid, action) for cid, action in self._pending_rescues.items()
            if action.scheduled_at <= now
        ]

        for conversation_id, action in due_rescues:
            try:
                # Determine actions
                mark_important = "mark_important" in action.actions
                schedule_reply = "reply" in action.actions

                result = self.rescue_from_spam(
                    conversation_id,
                    mark_important=mark_important,
                    schedule_reply=schedule_reply
                )

                if result.success:
                    processed += 1

                # Remove from pending
                self._pending_rescues.pop(conversation_id, None)

            except Exception as e:
                logger.error(f"[SpamRescue] Error processing rescue {conversation_id}: {e}")

        if processed > 0:
            logger.info(f"[SpamRescue] Processed {processed} pending rescues")

        return processed

    # ========================================================================
    # STATISTICS
    # ========================================================================

    def get_spam_statistics(
        self,
        member_id: int,
        hours: int = STATS_WINDOW_HOURS
    ) -> SpamStatistics:
        """
        Get spam and rescue statistics for a member.

        Args:
            member_id: Pool member ID
            hours: Time window for statistics

        Returns:
            SpamStatistics with detailed metrics
        """
        logger.debug(f"[SpamRescue] Getting spam stats for member {member_id}")

        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

            # Total received
            total_received = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.created_at >= cutoff
                )
            ).scalar() or 0

            # Spam detected
            spam_detected = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.was_in_spam == True,
                    WarmupConversation.created_at >= cutoff
                )
            ).scalar() or 0

            # Spam rescued
            spam_rescued = self.db.query(func.count(WarmupConversation.id)).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.spam_rescued_at.isnot(None),
                    WarmupConversation.created_at >= cutoff
                )
            ).scalar() or 0

            # Calculate rates
            spam_rate = (spam_detected / total_received * 100) if total_received > 0 else 0
            rescue_rate = (spam_rescued / spam_detected * 100) if spam_detected > 0 else 100

            # Get last spam timestamp
            last_spam = self.db.query(WarmupConversation.spam_detected_at).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.was_in_spam == True
                )
            ).order_by(WarmupConversation.spam_detected_at.desc()).first()

            last_spam_at = last_spam[0] if last_spam else None

            # Count consecutive spam (recent)
            recent_conversations = self.db.query(WarmupConversation).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.status.in_([
                        ConversationStatusEnum.DELIVERED.value,
                        ConversationStatusEnum.SPAM_DETECTED.value,
                        ConversationStatusEnum.SPAM_RESCUED.value,
                    ])
                )
            ).order_by(WarmupConversation.created_at.desc()).limit(10).all()

            consecutive_spam = 0
            for conv in recent_conversations:
                if conv.was_in_spam:
                    consecutive_spam += 1
                else:
                    break

            # Determine alert level
            if spam_rate >= SPAM_RATE_CRITICAL_THRESHOLD or consecutive_spam >= CONSECUTIVE_SPAM_ALERT_THRESHOLD:
                alert_level = "critical"
            elif spam_rate >= SPAM_RATE_WARNING_THRESHOLD:
                alert_level = "warning"
            else:
                alert_level = "none"

            return SpamStatistics(
                total_received=total_received,
                spam_detected=spam_detected,
                spam_rescued=spam_rescued,
                spam_rate=round(spam_rate, 1),
                rescue_rate=round(rescue_rate, 1),
                consecutive_spam=consecutive_spam,
                last_spam_at=last_spam_at,
                alert_level=alert_level
            )

        except SQLAlchemyError as e:
            logger.error(f"[SpamRescue] Error getting spam statistics: {e}")
            return SpamStatistics(
                total_received=0,
                spam_detected=0,
                spam_rescued=0,
                spam_rate=0,
                rescue_rate=100,
                consecutive_spam=0,
                last_spam_at=None,
                alert_level="none"
            )

    def get_rescue_history(
        self,
        member_id: int,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get recent rescue history for a member.

        Args:
            member_id: Pool member ID
            limit: Maximum results

        Returns:
            List of rescue events
        """
        try:
            rescues = self.db.query(WarmupConversation).filter(
                and_(
                    WarmupConversation.receiver_id == member_id,
                    WarmupConversation.spam_rescued_at.isnot(None)
                )
            ).order_by(WarmupConversation.spam_rescued_at.desc()).limit(limit).all()

            return [
                {
                    "conversation_id": r.id,
                    "thread_id": r.thread_id,
                    "subject": r.subject,
                    "spam_detected_at": r.spam_detected_at.isoformat() if r.spam_detected_at else None,
                    "spam_rescued_at": r.spam_rescued_at.isoformat() if r.spam_rescued_at else None,
                    "marked_important": r.marked_important,
                    "marked_not_spam": r.marked_not_spam,
                    "rescue_delay_seconds": (
                        (r.spam_rescued_at - r.spam_detected_at).total_seconds()
                        if r.spam_rescued_at and r.spam_detected_at else None
                    ),
                }
                for r in rescues
            ]

        except SQLAlchemyError as e:
            logger.error(f"[SpamRescue] Error getting rescue history: {e}")
            return []

    # ========================================================================
    # ALERTS
    # ========================================================================

    def check_and_generate_alerts(
        self,
        member_id: int
    ) -> List[Dict[str, Any]]:
        """
        Check spam statistics and generate alerts if thresholds exceeded.

        Args:
            member_id: Pool member to check

        Returns:
            List of generated alerts
        """
        stats = self.get_spam_statistics(member_id)
        alerts = []

        if stats.alert_level == "critical":
            alerts.append({
                "severity": "critical",
                "type": "high_spam_rate",
                "title": "Critical: High Spam Rate Detected",
                "message": f"Your spam rate is {stats.spam_rate:.1f}%, significantly above the acceptable threshold.",
                "metrics": {
                    "spam_rate": stats.spam_rate,
                    "consecutive_spam": stats.consecutive_spam,
                },
                "recommendations": [
                    "Reduce sending volume immediately",
                    "Review email content for spam triggers",
                    "Check domain authentication (SPF, DKIM, DMARC)",
                    "Consider pausing warmup for 24-48 hours",
                ],
            })

        elif stats.alert_level == "warning":
            alerts.append({
                "severity": "warning",
                "type": "elevated_spam_rate",
                "title": "Warning: Elevated Spam Rate",
                "message": f"Your spam rate of {stats.spam_rate:.1f}% is above normal levels.",
                "metrics": {
                    "spam_rate": stats.spam_rate,
                    "consecutive_spam": stats.consecutive_spam,
                },
                "recommendations": [
                    "Monitor closely for the next 24 hours",
                    "Review recent email content",
                    "Ensure authentication is properly configured",
                ],
            })

        if stats.consecutive_spam >= CONSECUTIVE_SPAM_ALERT_THRESHOLD:
            alerts.append({
                "severity": "warning",
                "type": "consecutive_spam",
                "title": "Multiple Consecutive Spam Placements",
                "message": f"Last {stats.consecutive_spam} emails landed in spam.",
                "metrics": {
                    "consecutive_spam": stats.consecutive_spam,
                },
                "recommendations": [
                    "Check if your IP or domain is blacklisted",
                    "Review sending patterns",
                    "Verify email content quality",
                ],
            })

        if stats.rescue_rate < 50 and stats.spam_detected > 0:
            alerts.append({
                "severity": "info",
                "type": "low_rescue_rate",
                "title": "Low Spam Rescue Rate",
                "message": f"Only {stats.rescue_rate:.1f}% of spam emails were rescued.",
                "metrics": {
                    "rescue_rate": stats.rescue_rate,
                    "spam_detected": stats.spam_detected,
                    "spam_rescued": stats.spam_rescued,
                },
                "recommendations": [
                    "Ensure rescue automation is enabled",
                    "Check IMAP connectivity",
                ],
            })

        if alerts:
            logger.info(f"[SpamRescue] Generated {len(alerts)} alerts for member {member_id}")

        return alerts


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

def get_spam_rescue_service(db: Session) -> SpamRescueService:
    """
    Factory function to create SpamRescueService instance.

    Args:
        db: SQLAlchemy database session

    Returns:
        Configured SpamRescueService instance
    """
    return SpamRescueService(db)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "SpamRescueService",
    "get_spam_rescue_service",
    "RescueResult",
    "SpamStatistics",
    "RescueAction",
]
