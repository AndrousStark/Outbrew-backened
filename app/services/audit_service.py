"""
Audit Service — DB-backed audit trail for all security-relevant events.

Writes to both:
1. audit_logs DB table (queryable by admin)
2. File-based logger (for operational monitoring)

Events tracked:
- login_success, login_failed
- logout
- user_registered, registration_failed
- email_verified
- password_changed, password_change_failed
- password_reset_requested, password_reset_completed
- user_created, user_updated, user_deleted, user_activated, user_deactivated
- plan_changed
- permission_denied
- admin_action
"""
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.audit_log import AuditLog

logger = logging.getLogger("security.audit")


def log_audit(
    event_type: str,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    success: bool = True,
) -> None:
    """
    Log an audit event to both DB and file.

    This is fire-and-forget — DB write failures don't raise.
    The file logger always works as fallback.
    """
    # Always log to file (immediate, no DB dependency)
    event_data = {
        "event_type": event_type,
        "user_id": user_id,
        "username": username,
        "success": success,
        "ip_address": ip_address,
    }
    if details:
        event_data["details"] = details

    if success:
        logger.info(f"AUDIT: {event_type} user={username}", extra=event_data)
    else:
        logger.warning(f"AUDIT: {event_type} FAILED user={username}", extra=event_data)

    # Write to DB (best-effort, don't crash on failure)
    try:
        db = SessionLocal()
        try:
            entry = AuditLog(
                event_type=event_type,
                user_id=user_id,
                username=username,
                ip_address=ip_address,
                user_agent=user_agent,
                details=details,
                success=success,
            )
            db.add(entry)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to write audit log to DB: {e}")
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Failed to create DB session for audit log: {e}")


def get_client_ip(request) -> str:
    """Extract client IP from request, respecting X-Forwarded-For."""
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if hasattr(request, "client") and request.client:
        return request.client.host
    return "unknown"
