"""
Notifications API Endpoints

Provides REST API for:
- Listing notifications with filtering
- Marking notifications as read
- Archiving and deleting notifications
- Getting notification statistics
- Managing notification preferences
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import time

from app.core.database import get_db
from app.services.notification_service import NotificationService, NotificationType
from app.models.notification_preference import NotificationPreference
from app.api.dependencies import get_current_candidate

router = APIRouter()


class NotificationResponse(BaseModel):
    """Response model for a notification."""

    id: int
    title: str
    message: str
    type: str
    application_id: Optional[int]
    company_id: Optional[int]
    action_url: Optional[str]
    action_text: Optional[str]
    is_read: bool
    is_archived: bool
    icon: Optional[str]
    priority: int
    created_at: Optional[str]
    read_at: Optional[str]
    expires_at: Optional[str]
    is_expired: bool

    class Config:
        from_attributes = True


class NotificationListResponse(BaseModel):
    """Response model for notification list."""

    notifications: list[dict]
    total: int
    unread_count: int


class NotificationStatsResponse(BaseModel):
    """Response model for notification statistics."""

    total: int
    unread: int
    by_type: dict[str, int]


class CreateNotificationRequest(BaseModel):
    """Request model for creating a notification."""

    title: str
    message: str
    notification_type: str = "info"
    application_id: Optional[int] = None
    company_id: Optional[int] = None
    action_url: Optional[str] = None
    action_text: Optional[str] = None
    priority: int = 0


class NotificationPreferenceResponse(BaseModel):
    """Response model for notification preferences."""

    notifications_enabled: bool
    email_notifications_enabled: bool
    push_notifications_enabled: bool
    enabled_types: dict
    delivery_preferences: dict
    quiet_hours_enabled: bool
    quiet_start_time: Optional[str] = None
    quiet_end_time: Optional[str] = None
    dnd_enabled: bool
    dnd_until: Optional[str] = None
    digest_enabled: bool
    digest_frequency: str

    class Config:
        from_attributes = True

    @staticmethod
    def _time_to_str(val):
        """Convert time/datetime to string if needed."""
        if val is None:
            return None
        if hasattr(val, "isoformat"):
            return val.isoformat()
        return str(val)

    @classmethod
    def model_validate(cls, obj, **kwargs):
        """Override model_validate to handle time objects from ORM."""
        if hasattr(obj, "quiet_start_time") and not isinstance(obj, dict):
            import datetime as dt
            data = {}
            for field_name in cls.model_fields:
                val = getattr(obj, field_name, None)
                if isinstance(val, (dt.time, dt.datetime)):
                    val = val.isoformat()
                data[field_name] = val
            return super().model_validate(data, **kwargs)
        return super().model_validate(obj, **kwargs)


class UpdateNotificationPreferenceRequest(BaseModel):
    """Request model for updating notification preferences."""

    notifications_enabled: Optional[bool] = None
    email_notifications_enabled: Optional[bool] = None
    push_notifications_enabled: Optional[bool] = None
    enabled_types: Optional[dict] = None
    delivery_preferences: Optional[dict] = None
    quiet_hours_enabled: Optional[bool] = None
    quiet_start_time: Optional[str] = None
    quiet_end_time: Optional[str] = None
    dnd_enabled: Optional[bool] = None
    dnd_until: Optional[str] = None
    digest_enabled: Optional[bool] = None
    digest_frequency: Optional[str] = None


@router.get("", response_model=NotificationListResponse)
def get_notifications(
    include_read: bool = Query(True, description="Include read notifications"),
    include_archived: bool = Query(False, description="Include archived notifications"),
    notification_type: Optional[str] = Query(None, description="Filter by type"),
    limit: int = Query(50, ge=1, le=100, description="Maximum notifications to return"),
    offset: int = Query(0, ge=0, description="Number to skip"),
    db: Session = Depends(get_db),
):
    """
    Get all notifications with optional filtering.

    Parameters:
    - include_read: Whether to include already-read notifications
    - include_archived: Whether to include archived notifications
    - notification_type: Filter by notification type
    - limit: Maximum number of notifications to return (1-100)
    - offset: Number of notifications to skip for pagination
    """
    notifications = NotificationService.get_all(
        db=db,
        include_read=include_read,
        include_archived=include_archived,
        notification_type=notification_type,
        limit=limit,
        offset=offset,
    )

    unread_count = NotificationService.get_unread_count(db)

    return {
        "notifications": [n.to_dict() for n in notifications],
        "total": len(notifications),
        "unread_count": unread_count,
    }


@router.get("/unread-count")
def get_unread_count(db: Session = Depends(get_db)):
    """Get the count of unread notifications."""
    count = NotificationService.get_unread_count(db)
    return {"unread_count": count}


@router.get("/stats", response_model=NotificationStatsResponse)
def get_notification_stats(db: Session = Depends(get_db)):
    """Get notification statistics."""
    from sqlalchemy import func
    from app.models.notification import Notification

    # Get counts by type
    type_counts = (
        db.query(Notification.notification_type, func.count(Notification.id))
        .filter(Notification.is_archived == False)
        .group_by(Notification.notification_type)
        .all()
    )

    by_type = {t: c for t, c in type_counts}

    # Total and unread
    total = sum(by_type.values())
    unread = NotificationService.get_unread_count(db)

    return {
        "total": total,
        "unread": unread,
        "by_type": by_type,
    }


@router.post("", status_code=201)
def create_notification(
    request: CreateNotificationRequest,
    db: Session = Depends(get_db),
):
    """
    Create a new notification.

    This is primarily used for testing or system-generated notifications.
    """
    try:
        notification_type = NotificationType(request.notification_type)
    except ValueError:
        notification_type = NotificationType.INFO

    notification = NotificationService.create(
        db=db,
        title=request.title,
        message=request.message,
        notification_type=notification_type,
        application_id=request.application_id,
        company_id=request.company_id,
        action_url=request.action_url,
        action_text=request.action_text,
        priority=request.priority,
    )

    return {
        "message": "Notification created",
        "notification": notification.to_dict(),
    }


@router.post("/{notification_id}/read")
def mark_as_read(notification_id: int, db: Session = Depends(get_db)):
    """Mark a specific notification as read."""
    notification = NotificationService.mark_as_read(db, notification_id)

    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")

    return {
        "message": "Notification marked as read",
        "notification": notification.to_dict(),
    }


@router.post("/read-all")
def mark_all_as_read(db: Session = Depends(get_db)):
    """Mark all notifications as read."""
    count = NotificationService.mark_all_as_read(db)

    return {
        "message": f"Marked {count} notifications as read",
        "count": count,
    }


@router.post("/{notification_id}/archive")
def archive_notification(notification_id: int, db: Session = Depends(get_db)):
    """Archive a notification."""
    notification = NotificationService.archive(db, notification_id)

    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")

    return {
        "message": "Notification archived",
        "notification": notification.to_dict(),
    }


@router.delete("/{notification_id}")
def delete_notification(notification_id: int, db: Session = Depends(get_db)):
    """Permanently delete a notification."""
    success = NotificationService.delete(db, notification_id)

    if not success:
        raise HTTPException(status_code=404, detail="Notification not found")

    return {"message": "Notification deleted"}


@router.post("/cleanup")
def cleanup_expired(db: Session = Depends(get_db)):
    """Delete all expired notifications."""
    count = NotificationService.cleanup_expired(db)

    return {
        "message": f"Cleaned up {count} expired notifications",
        "count": count,
    }


# ==================== NOTIFICATION PREFERENCES ====================



def _serialize_preferences(prefs):
    """Convert ORM preferences to dict with time->str conversion."""
    import datetime as dt
    data = {}
    for field_name in NotificationPreferenceResponse.model_fields:
        val = getattr(prefs, field_name, None)
        if isinstance(val, (dt.time,)):
            val = val.isoformat()
        elif isinstance(val, (dt.datetime,)):
            val = val.isoformat()
        data[field_name] = val
    return data


@router.get("/preferences", response_model=NotificationPreferenceResponse)
def get_notification_preferences(
    current_user=Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """Get current user's notification preferences."""
    prefs = (
        db.query(NotificationPreference)
        .filter(NotificationPreference.candidate_id == current_user.id)
        .first()
    )

    if not prefs:
        # Create default preferences if they don't exist
        prefs = NotificationPreference(candidate_id=current_user.id)
        db.add(prefs)
        db.commit()
        db.refresh(prefs)

    return _serialize_preferences(prefs)


@router.put("/preferences", response_model=NotificationPreferenceResponse)
def update_notification_preferences(
    request: UpdateNotificationPreferenceRequest,
    current_user=Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """Update current user's notification preferences."""
    prefs = (
        db.query(NotificationPreference)
        .filter(NotificationPreference.candidate_id == current_user.id)
        .first()
    )

    if not prefs:
        prefs = NotificationPreference(candidate_id=current_user.id)
        db.add(prefs)
        db.flush()

    # Update provided fields
    if request.notifications_enabled is not None:
        prefs.notifications_enabled = request.notifications_enabled

    if request.email_notifications_enabled is not None:
        prefs.email_notifications_enabled = request.email_notifications_enabled

    if request.push_notifications_enabled is not None:
        prefs.push_notifications_enabled = request.push_notifications_enabled

    if request.enabled_types is not None:
        prefs.enabled_types = request.enabled_types

    if request.delivery_preferences is not None:
        prefs.delivery_preferences = request.delivery_preferences

    if request.quiet_hours_enabled is not None:
        prefs.quiet_hours_enabled = request.quiet_hours_enabled

    if request.quiet_start_time is not None:
        prefs.quiet_start_time = time.fromisoformat(request.quiet_start_time)

    if request.quiet_end_time is not None:
        prefs.quiet_end_time = time.fromisoformat(request.quiet_end_time)

    if request.dnd_enabled is not None:
        prefs.dnd_enabled = request.dnd_enabled

    if request.dnd_until is not None:
        from datetime import datetime

        prefs.dnd_until = (
            datetime.fromisoformat(request.dnd_until) if request.dnd_until else None
        )

    if request.digest_enabled is not None:
        prefs.digest_enabled = request.digest_enabled

    if request.digest_frequency is not None:
        prefs.digest_frequency = request.digest_frequency

    db.commit()
    db.refresh(prefs)

    return _serialize_preferences(prefs)
