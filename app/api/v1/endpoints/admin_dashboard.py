"""
Super Admin Dashboard & Audit Log Endpoints

All endpoints require SUPER_ADMIN role.
Provides:
- Platform-wide statistics (users, emails, campaigns, plans)
- Audit log viewer with filtering
- Per-user management (set plan, view activity)
"""
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, case, text

from app.core.database import get_database_session
from app.api.dependencies import require_super_admin
from app.models.candidate import Candidate, UserRole, PlanTier
from app.models.audit_log import AuditLog
from app.models.application import Application
from app.models.email_log import EmailLog
from app.models.recipient import Recipient
from app.models.group_campaign import GroupCampaign
from app.services.audit_service import log_audit

router = APIRouter()


# ===================== DASHBOARD STATS =====================

@router.get("/dashboard")
def admin_dashboard(
    db: Session = Depends(get_database_session),
    admin: Candidate = Depends(require_super_admin),
):
    """
    Platform-wide dashboard statistics for Super Admin.

    Returns user counts, plan breakdown, email stats, recent activity.
    """
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)
    seven_days_ago = now - timedelta(days=7)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # User counts
    total_users = db.query(func.count(Candidate.id)).filter(Candidate.deleted_at.is_(None)).scalar() or 0
    active_users = db.query(func.count(Candidate.id)).filter(
        Candidate.deleted_at.is_(None), Candidate.is_active == True
    ).scalar() or 0
    verified_users = db.query(func.count(Candidate.id)).filter(
        Candidate.deleted_at.is_(None), Candidate.email_verified == True
    ).scalar() or 0

    # Plan breakdown
    plan_breakdown = db.query(
        Candidate.plan_tier, func.count(Candidate.id)
    ).filter(Candidate.deleted_at.is_(None)).group_by(Candidate.plan_tier).all()
    plans = {str(tier): count for tier, count in plan_breakdown}

    # Users registered in last 7 days
    new_users_7d = db.query(func.count(Candidate.id)).filter(
        Candidate.created_at >= seven_days_ago, Candidate.deleted_at.is_(None)
    ).scalar() or 0

    # Email stats
    total_emails = db.query(func.count(EmailLog.id)).scalar() or 0
    emails_today = db.query(func.count(EmailLog.id)).filter(
        EmailLog.created_at >= today_start
    ).scalar() or 0
    emails_7d = db.query(func.count(EmailLog.id)).filter(
        EmailLog.created_at >= seven_days_ago
    ).scalar() or 0

    # Application stats
    total_applications = db.query(func.count(Application.id)).filter(
        Application.deleted_at.is_(None)
    ).scalar() or 0

    # Campaign stats
    total_campaigns = db.query(func.count(GroupCampaign.id)).filter(
        GroupCampaign.deleted_at.is_(None)
    ).scalar() or 0

    # Recipient stats
    total_recipients = db.query(func.count(Recipient.id)).filter(
        Recipient.is_active == True
    ).scalar() or 0

    # Recent registrations (last 10)
    recent_registrations = db.query(
        Candidate.id, Candidate.username, Candidate.email, Candidate.full_name,
        Candidate.plan_tier, Candidate.email_verified, Candidate.created_at
    ).filter(
        Candidate.deleted_at.is_(None)
    ).order_by(desc(Candidate.created_at)).limit(10).all()

    recent_regs = [{
        "id": r.id, "username": r.username, "email": r.email,
        "full_name": r.full_name,
        "plan_tier": str(r.plan_tier) if r.plan_tier else "free",
        "email_verified": r.email_verified or False,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    } for r in recent_registrations]

    # Recent login failures (security)
    recent_failures = db.query(AuditLog).filter(
        AuditLog.event_type == "login_failed",
        AuditLog.timestamp >= seven_days_ago,
    ).order_by(desc(AuditLog.timestamp)).limit(10).all()

    failures = [{
        "id": f.id, "username": f.username, "ip_address": f.ip_address,
        "details": f.details, "timestamp": f.timestamp.isoformat() if f.timestamp else None,
    } for f in recent_failures]

    return {
        "users": {
            "total": total_users,
            "active": active_users,
            "verified": verified_users,
            "new_7d": new_users_7d,
            "plans": plans,
        },
        "emails": {
            "total": total_emails,
            "today": emails_today,
            "last_7d": emails_7d,
        },
        "applications": {"total": total_applications},
        "campaigns": {"total": total_campaigns},
        "recipients": {"total": total_recipients},
        "recent_registrations": recent_regs,
        "recent_login_failures": failures,
    }


# ===================== ALL USERS WITH PLAN + USAGE =====================

@router.get("/users")
def admin_list_users(
    db: Session = Depends(get_database_session),
    admin: Candidate = Depends(require_super_admin),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: Optional[str] = None,
    plan_tier: Optional[str] = None,
    role: Optional[str] = None,
    verified: Optional[bool] = None,
):
    """
    List all users with plan, usage, verification status.
    Filterable by plan, role, verified status, and search term.
    """
    query = db.query(Candidate).filter(Candidate.deleted_at.is_(None))

    if search:
        term = f"%{search}%"
        query = query.filter(
            (Candidate.username.ilike(term)) |
            (Candidate.email.ilike(term)) |
            (Candidate.full_name.ilike(term))
        )

    if plan_tier:
        query = query.filter(Candidate.plan_tier == plan_tier)

    if role:
        query = query.filter(Candidate.role == role)

    if verified is not None:
        query = query.filter(Candidate.email_verified == verified)

    total = query.count()
    users = query.order_by(desc(Candidate.created_at)).offset(
        (page - 1) * page_size
    ).limit(page_size).all()

    return {
        "users": [{
            "id": u.id,
            "username": u.username,
            "email": u.email,
            "full_name": u.full_name,
            "role": u.role.value if u.role else "pragya",
            "plan_tier": u.plan_tier.value if hasattr(u.plan_tier, "value") else str(u.plan_tier or "free"),
            "email_verified": u.email_verified or False,
            "is_active": u.is_active,
            "monthly_email_sent": u.monthly_email_sent or 0,
            "monthly_email_limit": u.monthly_email_limit or 100,
            "monthly_campaigns_created": u.monthly_campaigns_created or 0,
            "monthly_campaign_limit": u.monthly_campaign_limit or 3,
            "total_applications_sent": u.total_applications_sent or 0,
            "created_at": u.created_at.isoformat() if u.created_at else None,
        } for u in users],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


# ===================== SET USER PLAN =====================

@router.post("/users/{user_id}/set-plan")
def admin_set_plan(
    user_id: int,
    plan: str = Query(..., description="Plan tier: free or pro"),
    db: Session = Depends(get_database_session),
    admin: Candidate = Depends(require_super_admin),
):
    """Change a user's plan tier (Super Admin only)."""
    candidate = db.query(Candidate).filter(
        Candidate.id == user_id, Candidate.deleted_at.is_(None)
    ).first()
    if not candidate:
        raise HTTPException(status_code=404, detail="User not found")

    if plan not in ("free", "pro"):
        raise HTTPException(status_code=400, detail="Plan must be 'free' or 'pro'")

    old_plan = candidate.plan_tier
    candidate.plan_tier = plan  # VARCHAR column, store as-is

    # Update limits based on plan
    if plan == "pro":
        candidate.monthly_email_limit = 999999
        candidate.monthly_campaign_limit = 999999
        candidate.monthly_recipient_limit = 999999
        candidate.plan_started_at = datetime.now(timezone.utc)
    else:
        candidate.monthly_email_limit = 100
        candidate.monthly_campaign_limit = 3
        candidate.monthly_recipient_limit = 100

    try:
        db.commit()
        db.refresh(candidate)
        log_audit("plan_changed", user_id=admin.id, username=admin.username,
                  details={
                      "target_user_id": user_id,
                      "target_username": candidate.username,
                      "old_plan": str(old_plan),
                      "new_plan": plan,
                  })
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Failed to update plan")

    return {
        "success": True,
        "user_id": user_id,
        "username": candidate.username,
        "plan_tier": plan,
        "monthly_email_limit": candidate.monthly_email_limit,
        "monthly_campaign_limit": candidate.monthly_campaign_limit,
    }


# ===================== AUDIT LOGS =====================

@router.get("/audit-logs")
def admin_audit_logs(
    db: Session = Depends(get_database_session),
    admin: Candidate = Depends(require_super_admin),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    event_type: Optional[str] = None,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    success: Optional[bool] = None,
    ip_address: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
):
    """
    Query audit logs with filtering.

    Supports filtering by event type, user, success status, IP, and date range.
    """
    query = db.query(AuditLog)

    if event_type:
        query = query.filter(AuditLog.event_type == event_type)
    if user_id:
        query = query.filter(AuditLog.user_id == user_id)
    if username:
        query = query.filter(AuditLog.username.ilike(f"%{username}%"))
    if success is not None:
        query = query.filter(AuditLog.success == success)
    if ip_address:
        query = query.filter(AuditLog.ip_address == ip_address)
    if from_date:
        query = query.filter(AuditLog.timestamp >= from_date)
    if to_date:
        query = query.filter(AuditLog.timestamp <= to_date)

    total = query.count()
    logs = query.order_by(desc(AuditLog.timestamp)).offset(
        (page - 1) * page_size
    ).limit(page_size).all()

    # Get distinct event types for filter dropdown
    event_types = db.query(AuditLog.event_type).distinct().all()

    return {
        "logs": [{
            "id": l.id,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
            "event_type": l.event_type,
            "user_id": l.user_id,
            "username": l.username,
            "ip_address": l.ip_address,
            "user_agent": l.user_agent,
            "details": l.details,
            "success": l.success,
        } for l in logs],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "event_types": [t[0] for t in event_types],
    }


# ===================== AUDIT LOG STATS =====================

@router.get("/audit-stats")
def admin_audit_stats(
    db: Session = Depends(get_database_session),
    admin: Candidate = Depends(require_super_admin),
):
    """Audit log summary statistics for the last 30 days."""
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)

    total_events = db.query(func.count(AuditLog.id)).filter(
        AuditLog.timestamp >= thirty_days_ago
    ).scalar() or 0

    failed_events = db.query(func.count(AuditLog.id)).filter(
        AuditLog.timestamp >= thirty_days_ago, AuditLog.success == False
    ).scalar() or 0

    # Events by type
    by_type = db.query(
        AuditLog.event_type, func.count(AuditLog.id)
    ).filter(
        AuditLog.timestamp >= thirty_days_ago
    ).group_by(AuditLog.event_type).order_by(desc(func.count(AuditLog.id))).all()

    # Failed logins by IP (top 10 — potential brute force)
    suspicious_ips = db.query(
        AuditLog.ip_address, func.count(AuditLog.id).label("count")
    ).filter(
        AuditLog.event_type == "login_failed",
        AuditLog.timestamp >= thirty_days_ago,
        AuditLog.ip_address.isnot(None),
    ).group_by(AuditLog.ip_address).order_by(desc("count")).limit(10).all()

    return {
        "period": "30 days",
        "total_events": total_events,
        "failed_events": failed_events,
        "success_rate": round((total_events - failed_events) / total_events * 100, 1) if total_events > 0 else 100,
        "by_type": [{"event_type": t, "count": c} for t, c in by_type],
        "suspicious_ips": [{"ip": ip, "failed_logins": c} for ip, c in suspicious_ips],
    }
