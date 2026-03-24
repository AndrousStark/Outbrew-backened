"""
Notification Service

Centralized service for creating and managing notifications with:
- Smart notification creation
- WebSocket broadcasting (when integrated)
- Notification grouping and deduplication
- Priority-based delivery
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_, or_

from app.models.notification import Notification, NotificationType
from app.core.database import get_db
import logging

logger = logging.getLogger(__name__)


class NotificationService:
    """Service for managing notifications."""

    @staticmethod
    def _log(action: str, details: dict = None):
        """Helper for consistent logging"""
        logger.info(f"[NotificationService] {action}")
        if details:
            logger.debug(f"[NotificationService] Details: {details}")

    # Default fallback icon
    DEFAULT_ICON = "bell"

    # Icon mapping for notification types
    TYPE_ICONS = {
        NotificationType.INFO: "info",
        NotificationType.SUCCESS: "check-circle",
        NotificationType.WARNING: "alert-triangle",
        NotificationType.ERROR: "x-circle",
        NotificationType.EMAIL_SENT: "send",
        NotificationType.EMAIL_OPENED: "mail-open",
        NotificationType.EMAIL_REPLIED: "message-circle",
        NotificationType.APPLICATION_UPDATE: "briefcase",
        NotificationType.WARMING_ALERT: "thermometer",
        NotificationType.RATE_LIMIT: "clock",
        NotificationType.SYSTEM: "settings",
        NotificationType.CAMPAIGN_CREATED: "plus-circle",
        NotificationType.CAMPAIGN_SENDING: "send",
        NotificationType.CAMPAIGN_COMPLETED: "check-circle",
        NotificationType.CAMPAIGN_FAILED: "x-circle",
        NotificationType.CAMPAIGN_PAUSED: "pause-circle",
        NotificationType.REPLY_DETECTED: "message-square",
    }

    @classmethod
    def get_icon_for_type(cls, notification_type: NotificationType) -> str:
        """Get icon for notification type with fallback to default"""
        icon = cls.TYPE_ICONS.get(notification_type, cls.DEFAULT_ICON)
        if icon == cls.DEFAULT_ICON and notification_type not in cls.TYPE_ICONS:
            logger.warning(
                f"[NotificationService] Unknown notification type for icon mapping: {notification_type}, using default icon"
            )
        return icon

    # Default expiration times (in days)
    DEFAULT_EXPIRATION = {
        NotificationType.INFO: 7,
        NotificationType.SUCCESS: 3,
        NotificationType.WARNING: 14,
        NotificationType.ERROR: 30,
        NotificationType.EMAIL_SENT: 7,
        NotificationType.EMAIL_OPENED: 14,
        NotificationType.EMAIL_REPLIED: 30,
        NotificationType.APPLICATION_UPDATE: 14,
        NotificationType.WARMING_ALERT: 7,
        NotificationType.RATE_LIMIT: 1,
        NotificationType.SYSTEM: 30,
        NotificationType.CAMPAIGN_CREATED: 30,
        NotificationType.CAMPAIGN_SENDING: 30,
        NotificationType.CAMPAIGN_COMPLETED: 30,
        NotificationType.CAMPAIGN_FAILED: 30,
        NotificationType.CAMPAIGN_PAUSED: 30,
        NotificationType.REPLY_DETECTED: 30,
    }

    @classmethod
    def create(
        cls,
        db: Session,
        title: str,
        message: str,
        notification_type: NotificationType = NotificationType.INFO,
        application_id: Optional[int] = None,
        company_id: Optional[int] = None,
        candidate_id: Optional[int] = None,
        group_campaign_id: Optional[int] = None,
        action_url: Optional[str] = None,
        action_text: Optional[str] = None,
        priority: int = 0,
        expires_in_days: Optional[int] = None,
    ) -> Notification:
        """
        Create a new notification.

        Args:
            db: Database session
            title: Notification title
            message: Notification message body
            notification_type: Type of notification
            application_id: Related application ID
            company_id: Related company ID
            candidate_id: Related candidate ID
            group_campaign_id: Related campaign ID
            action_url: URL to navigate to when clicked
            action_text: Text for action button
            priority: Priority level (higher = more important)
            expires_in_days: Days until notification expires

        Returns:
            Created Notification object
        """
        cls._log(
            "create called",
            {
                "title": title,
                "type": notification_type.value
                if isinstance(notification_type, NotificationType)
                else notification_type,
                "application_id": application_id,
                "company_id": company_id,
                "candidate_id": candidate_id,
                "group_campaign_id": group_campaign_id,
                "priority": priority,
            },
        )

        # Calculate expiration
        if expires_in_days is None:
            expires_in_days = cls.DEFAULT_EXPIRATION.get(notification_type, 7)

        expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)
        logger.debug(f"[NotificationService] Notification expires at: {expires_at}")

        # Get icon for type using the helper method with fallback
        icon = cls.get_icon_for_type(notification_type)
        logger.debug(f"[NotificationService] Using icon: {icon}")

        notification = Notification(
            title=title,
            message=message,
            notification_type=notification_type.value
            if isinstance(notification_type, NotificationType)
            else notification_type,
            application_id=application_id,
            company_id=company_id,
            candidate_id=candidate_id,
            group_campaign_id=group_campaign_id,
            action_url=action_url,
            action_text=action_text,
            icon=icon,
            priority=priority,
            expires_at=expires_at,
        )

        try:
            db.add(notification)
            db.commit()
            db.refresh(notification)
            logger.info(
                f"[NotificationService] Created notification: {title} (ID: {notification.id}, Type: {notification.notification_type})"
            )
        except Exception as e:
            db.rollback()
            logger.error(f"[NotificationService] Failed to create notification: {e}")
            raise ValueError(f"Failed to create notification: {e}")

        return notification

    @classmethod
    def get_all(
        cls,
        db: Session,
        include_read: bool = True,
        include_archived: bool = False,
        limit: int = 50,
        offset: int = 0,
        notification_type: Optional[str] = None,
    ) -> List[Notification]:
        """
        Get all notifications with filtering.

        Args:
            db: Database session
            include_read: Include read notifications
            include_archived: Include archived notifications
            limit: Maximum number to return
            offset: Number to skip
            notification_type: Filter by type

        Returns:
            List of Notification objects
        """
        query = db.query(Notification)

        # Apply filters
        if not include_archived:
            query = query.filter(Notification.is_archived == False)

        if not include_read:
            query = query.filter(Notification.is_read == False)

        if notification_type:
            query = query.filter(Notification.notification_type == notification_type)

        # Exclude expired
        query = query.filter(
            or_(
                Notification.expires_at == None,
                Notification.expires_at > datetime.now(timezone.utc),
            )
        )

        # Order by priority (desc), then created_at (desc)
        query = query.order_by(
            desc(Notification.priority), desc(Notification.created_at)
        )

        return query.offset(offset).limit(limit).all()

    @classmethod
    def get_unread_count(cls, db: Session) -> int:
        """Get count of unread, non-archived, non-expired notifications."""
        return (
            db.query(Notification)
            .filter(
                and_(
                    Notification.is_read == False,
                    Notification.is_archived == False,
                    or_(
                        Notification.expires_at == None,
                        Notification.expires_at > datetime.now(timezone.utc),
                    ),
                )
            )
            .count()
        )

    @classmethod
    def mark_as_read(cls, db: Session, notification_id: int) -> Optional[Notification]:
        """Mark a notification as read."""
        notification = (
            db.query(Notification).filter(Notification.id == notification_id).first()
        )

        if notification:
            notification.mark_as_read()
            db.commit()
            db.refresh(notification)

        return notification

    @classmethod
    def mark_all_as_read(cls, db: Session) -> int:
        """Mark all unread notifications as read. Returns count updated."""
        count = (
            db.query(Notification)
            .filter(Notification.is_read == False)
            .update({"is_read": True, "read_at": datetime.now(timezone.utc)})
        )
        db.commit()
        return count

    @classmethod
    def archive(cls, db: Session, notification_id: int) -> Optional[Notification]:
        """Archive a notification."""
        notification = (
            db.query(Notification).filter(Notification.id == notification_id).first()
        )

        if notification:
            notification.is_archived = True
            db.commit()
            db.refresh(notification)

        return notification

    @classmethod
    def delete(cls, db: Session, notification_id: int) -> bool:
        """Delete a notification permanently."""
        logger.debug(f"[NotificationService] Deleting notification {notification_id}")

        notification = (
            db.query(Notification).filter(Notification.id == notification_id).first()
        )

        if notification:
            try:
                db.delete(notification)
                db.commit()
                logger.info(
                    f"[NotificationService] Deleted notification {notification_id}"
                )
                return True
            except Exception as e:
                db.rollback()
                logger.error(
                    f"[NotificationService] Failed to delete notification {notification_id}: {e}"
                )
                raise ValueError(f"Failed to delete notification: {e}")

        logger.warning(
            f"[NotificationService] Notification {notification_id} not found for deletion"
        )
        return False

    @classmethod
    def cleanup_expired(cls, db: Session) -> int:
        """Delete all expired notifications. Returns count deleted."""
        logger.info("[NotificationService] Running cleanup of expired notifications")

        try:
            count = (
                db.query(Notification)
                .filter(
                    and_(
                        Notification.expires_at != None,
                        Notification.expires_at < datetime.now(timezone.utc),
                    )
                )
                .delete()
            )
            db.commit()
            logger.info(
                f"[NotificationService] Cleaned up {count} expired notifications"
            )
            return count
        except Exception as e:
            db.rollback()
            logger.error(
                f"[NotificationService] Failed to cleanup expired notifications: {e}"
            )
            raise ValueError(f"Failed to cleanup notifications: {e}")

    # Convenience methods for common notifications

    @classmethod
    def notify_email_sent(
        cls,
        db: Session,
        application_id: int,
        company_name: str,
        recruiter_name: str,
    ) -> Notification:
        """Create notification for email sent."""
        return cls.create(
            db=db,
            title="Email Sent",
            message=f"Email sent to {recruiter_name} at {company_name}",
            notification_type=NotificationType.EMAIL_SENT,
            application_id=application_id,
            action_url=f"/applications/{application_id}",
            action_text="View Application",
            priority=1,
        )

    @classmethod
    def notify_email_opened(
        cls,
        db: Session,
        application_id: int,
        company_name: str,
    ) -> Notification:
        """Create notification for email opened."""
        return cls.create(
            db=db,
            title="Email Opened!",
            message=f"Your email to {company_name} was opened",
            notification_type=NotificationType.EMAIL_OPENED,
            application_id=application_id,
            action_url=f"/applications/{application_id}",
            action_text="View Details",
            priority=2,
        )

    @classmethod
    def notify_email_replied(
        cls,
        db: Session,
        application_id: int,
        company_name: str,
    ) -> Notification:
        """Create notification for email reply received."""
        return cls.create(
            db=db,
            title="Reply Received!",
            message=f"{company_name} has replied to your email",
            notification_type=NotificationType.EMAIL_REPLIED,
            application_id=application_id,
            action_url=f"/applications/{application_id}",
            action_text="View Reply",
            priority=5,  # High priority
        )

    @classmethod
    def notify_warming_alert(
        cls,
        db: Session,
        email_address: str,
        alert_type: str,
        details: str,
    ) -> Notification:
        """Create notification for email warming alerts."""
        return cls.create(
            db=db,
            title=f"Warming Alert: {alert_type}",
            message=f"{email_address}: {details}",
            notification_type=NotificationType.WARMING_ALERT,
            action_url="/settings?tab=email",
            action_text="View Settings",
            priority=3,
        )

    @classmethod
    def notify_rate_limit_warning(
        cls,
        db: Session,
        email_address: str,
        current_usage: int,
        daily_limit: int,
    ) -> Notification:
        """Create notification for rate limit warnings."""
        logger.info(
            f"[NotificationService] Creating rate limit warning for {email_address}: {current_usage}/{daily_limit}"
        )

        # Division by zero protection
        if daily_limit and daily_limit > 0:
            percentage = int((current_usage / daily_limit) * 100)
        else:
            percentage = 100  # Assume 100% if limit is zero or None
            logger.warning(
                f"[NotificationService] Daily limit is zero or None for {email_address}, assuming 100%"
            )

        return cls.create(
            db=db,
            title="Rate Limit Warning",
            message=f"{email_address} has used {percentage}% of daily limit ({current_usage}/{daily_limit})",
            notification_type=NotificationType.RATE_LIMIT,
            action_url="/settings?tab=email",
            action_text="View Limits",
            priority=2,
        )

    @classmethod
    def notify_application_status_change(
        cls,
        db: Session,
        application_id: int,
        company_name: str,
        old_status: str,
        new_status: str,
    ) -> Notification:
        """Create notification for application status changes."""
        return cls.create(
            db=db,
            title="Application Updated",
            message=f"{company_name}: Status changed from {old_status} to {new_status}",
            notification_type=NotificationType.APPLICATION_UPDATE,
            application_id=application_id,
            action_url=f"/applications/{application_id}",
            action_text="View Details",
            priority=1,
        )

    # ==================== CAMPAIGN NOTIFICATIONS ====================

    @classmethod
    def notify_campaign_created(
        cls,
        db: Session,
        campaign_id: int,
        campaign_name: str,
        recipient_count: int,
        candidate_id: int,
    ) -> Notification:
        """Create notification for campaign creation."""
        return cls.create(
            db=db,
            title="Campaign Created",
            message=f"Campaign '{campaign_name}' created with {recipient_count} recipients",
            notification_type=NotificationType.CAMPAIGN_CREATED,
            group_campaign_id=campaign_id,
            candidate_id=candidate_id,
            action_url=f"/campaigns/{campaign_id}",
            action_text="View Campaign",
            priority=1,
        )

    @classmethod
    def notify_campaign_sending(
        cls,
        db: Session,
        campaign_id: int,
        campaign_name: str,
        candidate_id: int,
    ) -> Notification:
        """Create notification for campaign send start."""
        return cls.create(
            db=db,
            title="Campaign Sending",
            message=f"Campaign '{campaign_name}' is now sending emails",
            notification_type=NotificationType.CAMPAIGN_SENDING,
            group_campaign_id=campaign_id,
            candidate_id=candidate_id,
            action_url=f"/campaigns/{campaign_id}",
            action_text="Track Progress",
            priority=2,
        )

    @classmethod
    def notify_campaign_completed(
        cls,
        db: Session,
        campaign_id: int,
        campaign_name: str,
        sent_count: int,
        candidate_id: int,
    ) -> Notification:
        """Create notification for campaign completion."""
        return cls.create(
            db=db,
            title="Campaign Completed",
            message=f"Campaign '{campaign_name}' sent {sent_count} emails successfully",
            notification_type=NotificationType.CAMPAIGN_COMPLETED,
            group_campaign_id=campaign_id,
            candidate_id=candidate_id,
            action_url=f"/campaigns/{campaign_id}",
            action_text="View Results",
            priority=2,
        )

    @classmethod
    def notify_campaign_failed(
        cls,
        db: Session,
        campaign_id: int,
        campaign_name: str,
        error_message: str,
        candidate_id: int,
    ) -> Notification:
        """Create notification for campaign failure."""
        return cls.create(
            db=db,
            title="Campaign Failed",
            message=f"Campaign '{campaign_name}' failed: {error_message}",
            notification_type=NotificationType.CAMPAIGN_FAILED,
            group_campaign_id=campaign_id,
            candidate_id=candidate_id,
            action_url=f"/campaigns/{campaign_id}",
            action_text="View Details",
            priority=4,  # High priority
        )

    @classmethod
    def notify_campaign_paused(
        cls,
        db: Session,
        campaign_id: int,
        campaign_name: str,
        sent_count: int,
        candidate_id: int,
        reason: str = None,
    ) -> Notification:
        """Create notification for campaign pause (including rate limit pauses)."""
        if reason:
            message = f"Campaign '{campaign_name}' paused after sending {sent_count} emails. Reason: {reason}"
        else:
            message = f"Campaign '{campaign_name}' paused after sending {sent_count} emails"

        return cls.create(
            db=db,
            title="Campaign Paused",
            message=message,
            notification_type=NotificationType.CAMPAIGN_PAUSED,
            group_campaign_id=campaign_id,
            candidate_id=candidate_id,
            action_url=f"/campaigns/{campaign_id}",
            action_text="Resume Campaign",
            priority=2,
        )

    @classmethod
    def notify_reply_detected(
        cls,
        db: Session,
        campaign_id: int,
        sender_name: str,
        candidate_id: int,
    ) -> Notification:
        """Create notification when reply is detected."""
        return cls.create(
            db=db,
            title="Reply Received!",
            message=f"{sender_name} replied to your campaign email",
            notification_type=NotificationType.REPLY_DETECTED,
            group_campaign_id=campaign_id,
            candidate_id=candidate_id,
            action_url=f"/inbox",
            action_text="View Reply",
            priority=5,  # Highest priority
        )


# Singleton instance
notification_service = NotificationService()
