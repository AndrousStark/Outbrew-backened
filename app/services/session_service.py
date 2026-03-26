"""
Session Service — Manages user login sessions.

Features:
- Track active sessions with IP, device, user-agent
- List active sessions for a user
- Revoke individual sessions or all sessions
- Parse device info from user-agent
- Detect new device logins
"""
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.models.user_session import UserSession
from app.core.auth import blacklist_token, token_blacklist

logger = logging.getLogger(__name__)

# Max concurrent sessions per user (free plan)
MAX_SESSIONS_FREE = 3
MAX_SESSIONS_PRO = 10


def parse_device_info(user_agent: str) -> str:
    """Extract readable device info from user-agent string."""
    if not user_agent:
        return "Unknown device"

    ua = user_agent.lower()

    # OS detection
    if "windows" in ua:
        os_name = "Windows"
    elif "macintosh" in ua or "mac os" in ua:
        os_name = "macOS"
    elif "linux" in ua:
        os_name = "Linux"
    elif "android" in ua:
        os_name = "Android"
    elif "iphone" in ua or "ipad" in ua:
        os_name = "iOS"
    else:
        os_name = "Unknown OS"

    # Browser detection
    if "chrome" in ua and "edg" not in ua:
        browser = "Chrome"
    elif "firefox" in ua:
        browser = "Firefox"
    elif "safari" in ua and "chrome" not in ua:
        browser = "Safari"
    elif "edg" in ua:
        browser = "Edge"
    elif "curl" in ua:
        browser = "curl"
    elif "python" in ua:
        browser = "Python"
    else:
        browser = "Unknown browser"

    return f"{browser} on {os_name}"


def create_session(
    db: Session,
    user_id: int,
    access_jti: str,
    refresh_jti: str,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    expires_at: Optional[datetime] = None,
) -> UserSession:
    """Create a new session record on login."""
    device_info = parse_device_info(user_agent or "")

    session = UserSession(
        user_id=user_id,
        token_jti=access_jti,
        refresh_jti=refresh_jti,
        ip_address=ip_address,
        user_agent=(user_agent or "")[:512],
        device_info=device_info,
        expires_at=expires_at or (datetime.now(timezone.utc) + timedelta(days=7)),
        is_current=True,
    )

    db.add(session)

    # Enforce max sessions — revoke oldest if over limit
    active_count = db.query(UserSession).filter(
        UserSession.user_id == user_id,
        UserSession.revoked_at.is_(None),
    ).count()

    if active_count > MAX_SESSIONS_PRO:
        oldest = db.query(UserSession).filter(
            UserSession.user_id == user_id,
            UserSession.revoked_at.is_(None),
        ).order_by(UserSession.created_at).first()
        if oldest:
            oldest.revoked_at = datetime.now(timezone.utc)
            logger.info(f"Auto-revoked oldest session for user {user_id} (max sessions)")

    try:
        db.flush()
    except Exception as e:
        logger.error(f"Failed to create session: {e}")

    return session


def get_active_sessions(db: Session, user_id: int) -> List[Dict[str, Any]]:
    """Get all active sessions for a user."""
    sessions = db.query(UserSession).filter(
        UserSession.user_id == user_id,
        UserSession.revoked_at.is_(None),
    ).order_by(desc(UserSession.last_active_at)).all()

    return [{
        "id": s.id,
        "ip_address": s.ip_address,
        "device_info": s.device_info,
        "user_agent": s.user_agent,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "last_active_at": s.last_active_at.isoformat() if s.last_active_at else None,
        "is_current": s.is_current,
    } for s in sessions]


def revoke_session(db: Session, session_id: int, user_id: int) -> bool:
    """Revoke a specific session."""
    session = db.query(UserSession).filter(
        UserSession.id == session_id,
        UserSession.user_id == user_id,
        UserSession.revoked_at.is_(None),
    ).first()

    if not session:
        return False

    session.revoked_at = datetime.now(timezone.utc)

    # Blacklist the access token JTI
    if session.token_jti:
        token_blacklist.add(session.token_jti, session.expires_at or datetime.now(timezone.utc) + timedelta(hours=1))
    if session.refresh_jti:
        token_blacklist.add(session.refresh_jti, session.expires_at or datetime.now(timezone.utc) + timedelta(days=7))

    return True


def revoke_all_sessions(db: Session, user_id: int, except_session_id: Optional[int] = None) -> int:
    """Revoke all sessions for a user, optionally keeping one."""
    query = db.query(UserSession).filter(
        UserSession.user_id == user_id,
        UserSession.revoked_at.is_(None),
    )
    if except_session_id:
        query = query.filter(UserSession.id != except_session_id)

    sessions = query.all()
    now = datetime.now(timezone.utc)
    count = 0

    for s in sessions:
        s.revoked_at = now
        if s.token_jti:
            token_blacklist.add(s.token_jti, s.expires_at or now + timedelta(hours=1))
        if s.refresh_jti:
            token_blacklist.add(s.refresh_jti, s.expires_at or now + timedelta(days=7))
        count += 1

    return count


def update_session_activity(db: Session, token_jti: str) -> None:
    """Update last_active_at for a session (called on each authenticated request)."""
    try:
        session = db.query(UserSession).filter(
            UserSession.token_jti == token_jti,
            UserSession.revoked_at.is_(None),
        ).first()
        if session:
            session.last_active_at = datetime.now(timezone.utc)
    except Exception:
        pass  # Don't fail requests for session tracking
