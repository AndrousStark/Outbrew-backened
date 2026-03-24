"""Dashboard Endpoints - User-specific and Super Admin dashboards"""
import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, case, extract
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from app.api.dependencies import get_db, get_current_candidate
from app.core.auth import require_super_admin
from app.models.application import Application, ApplicationStatusEnum
from app.models.company import Company
from app.models.candidate import Candidate, UserRole
from app.models.email_log import EmailLog

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_user_dashboard(db: Session, user: Candidate) -> dict:
    """Build dashboard data for the given authenticated user."""
    # Get all non-deleted applications for the user
    applications = db.query(Application).filter(
        Application.candidate_id == user.id,
        Application.deleted_at.is_(None)
    )

    total_apps = applications.count()
    sent = applications.filter(Application.status != ApplicationStatusEnum.DRAFT).count()
    opened = applications.filter(Application.opened_at.isnot(None)).count()
    replied = applications.filter(Application.replied_at.isnot(None)).count()
    interview = applications.filter(Application.status == ApplicationStatusEnum.INTERVIEW).count()
    accepted = applications.filter(Application.status == ApplicationStatusEnum.ACCEPTED).count()
    rejected = applications.filter(Application.status == ApplicationStatusEnum.REJECTED).count()

    # Status breakdown
    status_breakdown = db.query(
        Application.status,
        func.count(Application.id).label('count')
    ).filter(
        Application.candidate_id == user.id,
        Application.deleted_at.is_(None)
    ).group_by(Application.status).all()

    # Recent applications (last 7 days)
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    recent_apps = applications.filter(Application.created_at >= week_ago).count()

    # Top companies
    top_companies = db.query(
        Company.name,
        func.count(Application.id).label('count')
    ).join(Application).filter(
        Application.candidate_id == user.id,
        Application.deleted_at.is_(None),
        Company.deleted_at.is_(None)
    ).group_by(Company.name).order_by(func.count(Application.id).desc()).limit(5).all()

    # Response rate over time (last 30 days, grouped by week)
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    weekly_stats = db.query(
        func.date(Application.created_at).label('date'),
        func.count(Application.id).label('total'),
        func.sum(case((Application.replied_at.isnot(None), 1), else_=0)).label('replied')
    ).filter(
        Application.candidate_id == user.id,
        Application.created_at >= thirty_days_ago,
        Application.deleted_at.is_(None)
    ).group_by(func.date(Application.created_at)).all()

    return {
        "user_info": {
            "name": user.full_name,
            "email": user.email,
            "role": user.role
        },
        "overview": {
            "total_applications": total_apps,
            "total_sent": sent,
            "total_opened": opened,
            "total_replied": replied,
            "total_interview": interview,
            "total_accepted": accepted,
            "total_rejected": rejected,
            "response_rate": round((replied / sent * 100) if sent > 0 else 0, 1),
            "open_rate": round((opened / sent * 100) if sent > 0 else 0, 1),
            "interview_rate": round((interview / sent * 100) if sent > 0 else 0, 1),
            "acceptance_rate": round((accepted / sent * 100) if sent > 0 else 0, 1),
            "rejection_rate": round((rejected / sent * 100) if sent > 0 else 0, 1),
            "recent_applications_7d": recent_apps
        },
        "status_breakdown": [
            {"status": str(r.status), "count": r.count}
            for r in status_breakdown
        ],
        "top_companies": [
            {"company": r.name, "applications": r.count}
            for r in top_companies
        ],
        "weekly_trend": [
            {
                "date": str(r.date),
                "total": r.total,
                "replied": r.replied,
                "response_rate": round((r.replied / r.total * 100) if r.total > 0 else 0, 1)
            }
            for r in weekly_stats
        ]
    }


@router.get("/me")
def get_my_dashboard(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get the authenticated user's dashboard with comprehensive statistics"""
    logger.debug(f"[Dashboard] Fetching dashboard for user: {current_candidate.username}")
    return _get_user_dashboard(db, current_candidate)


# DEPRECATED: Use GET /me instead. These routes are kept for backward compatibility.
@router.get("/pragya", deprecated=True)
def get_pragya_dashboard(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Deprecated: Use GET /me instead. Returns dashboard for the authenticated user."""
    return _get_user_dashboard(db, current_candidate)


# DEPRECATED: Use GET /me instead. These routes are kept for backward compatibility.
@router.get("/aniruddh", deprecated=True)
def get_aniruddh_dashboard(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Deprecated: Use GET /me instead. Returns dashboard for the authenticated user."""
    return _get_user_dashboard(db, current_candidate)


@router.get("/super-admin")
async def get_super_admin_dashboard(
    db: Session = Depends(get_db),
    current_admin: Candidate = Depends(require_super_admin)
):
    """Get Super Admin dashboard with comparative statistics for all users (requires Super Admin role)"""
    logger.debug(f"[Dashboard] Fetching Super Admin dashboard for {current_admin.username}")

    # Get all users with their stats in a single aggregated query (fixes N+1 issue)
    user_stats_query = db.query(
        Candidate.id.label('user_id'),
        Candidate.username,
        Candidate.full_name,
        Candidate.role,
        Candidate.email,
        func.count(Application.id).label('total_applications'),
        func.sum(case((Application.status != ApplicationStatusEnum.DRAFT, 1), else_=0)).label('sent'),
        func.sum(case((Application.opened_at.isnot(None), 1), else_=0)).label('opened'),
        func.sum(case((Application.replied_at.isnot(None), 1), else_=0)).label('replied'),
        func.sum(case((Application.status == ApplicationStatusEnum.INTERVIEW, 1), else_=0)).label('interview'),
        func.sum(case((Application.status == ApplicationStatusEnum.ACCEPTED, 1), else_=0)).label('accepted')
    ).outerjoin(
        Application,
        and_(
            Application.candidate_id == Candidate.id,
            Application.deleted_at.is_(None)
        )
    ).filter(
        Candidate.deleted_at.is_(None),
        Candidate.role.in_([UserRole.PRAGYA, UserRole.ANIRUDDH])
    ).group_by(
        Candidate.id,
        Candidate.username,
        Candidate.full_name,
        Candidate.role,
        Candidate.email
    ).all()

    logger.debug(f"[Dashboard] Found {len(user_stats_query)} active users")

    user_stats = []
    for row in user_stats_query:
        total_apps = row.total_applications or 0
        sent = row.sent or 0
        opened = row.opened or 0
        replied = row.replied or 0
        interview = row.interview or 0
        accepted = row.accepted or 0

        user_stats.append({
            "user_id": row.user_id,
            "username": row.username,
            "full_name": row.full_name,
            "role": str(row.role),
            "email": row.email,
            "total_applications": total_apps,
            "sent": sent,
            "opened": opened,
            "replied": replied,
            "interview": interview,
            "accepted": accepted,
            "response_rate": round((replied / sent * 100) if sent > 0 else 0, 1),
            "interview_rate": round((interview / sent * 100) if sent > 0 else 0, 1),
            "acceptance_rate": round((accepted / sent * 100) if sent > 0 else 0, 1)
        })

    # Overall system statistics
    total_apps = db.query(Application).filter(Application.deleted_at.is_(None)).count()
    total_companies = db.query(Company).filter(Company.deleted_at.is_(None)).count()
    total_users = db.query(Candidate).filter(
        Candidate.deleted_at.is_(None),
        Candidate.role.in_([UserRole.PRAGYA, UserRole.ANIRUDDH])
    ).count()

    # Applications by status (all users)
    status_breakdown = db.query(
        Application.status,
        func.count(Application.id).label('count')
    ).filter(
        Application.deleted_at.is_(None)
    ).group_by(Application.status).all()

    # Top performing companies
    top_companies = db.query(
        Company.name,
        func.count(Application.id).label('total_applications'),
        func.sum(case((Application.replied_at.isnot(None), 1), else_=0)).label('responses')
    ).join(Application).filter(
        Application.deleted_at.is_(None),
        Company.deleted_at.is_(None)
    ).group_by(Company.name).order_by(func.count(Application.id).desc()).limit(10).all()

    # Daily activity (last 30 days)
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    daily_activity = db.query(
        func.date(Application.created_at).label('date'),
        func.count(Application.id).label('applications_created'),
        func.sum(case((Application.sent_at.isnot(None), 1), else_=0)).label('applications_sent')
    ).filter(
        Application.created_at >= thirty_days_ago,
        Application.deleted_at.is_(None)
    ).group_by(func.date(Application.created_at)).all()

    # Monthly comparison
    this_month_start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_start = (this_month_start - timedelta(days=1)).replace(day=1)

    this_month_apps = db.query(Application).filter(
        Application.created_at >= this_month_start,
        Application.deleted_at.is_(None)
    ).count()

    last_month_apps = db.query(Application).filter(
        Application.created_at >= last_month_start,
        Application.created_at < this_month_start,
        Application.deleted_at.is_(None)
    ).count()

    return {
        "system_overview": {
            "total_applications": total_apps,
            "total_companies": total_companies,
            "total_active_users": total_users,
            "applications_this_month": this_month_apps,
            "applications_last_month": last_month_apps,
            "monthly_growth": round(((this_month_apps - last_month_apps) / last_month_apps * 100) if last_month_apps > 0 else 0, 1)
        },
        "user_comparison": user_stats,
        "status_breakdown": [
            {"status": str(r.status), "count": r.count}
            for r in status_breakdown
        ],
        "top_companies": [
            {
                "name": r.name,
                "total_applications": r.total_applications,
                "responses": r.responses,
                "response_rate": round((r.responses / r.total_applications * 100) if r.total_applications > 0 else 0, 1)
            }
            for r in top_companies
        ],
        "daily_activity": [
            {
                "date": str(r.date),
                "created": r.applications_created,
                "sent": r.applications_sent
            }
            for r in daily_activity
        ]
    }
