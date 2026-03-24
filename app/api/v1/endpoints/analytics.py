"""Analytics API Endpoints"""
import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.api.dependencies import get_db, get_current_candidate
from app.models.application import Application, ApplicationStatusEnum
from app.models.candidate import Candidate
from app.models.company import Company

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/dashboard")
def get_dashboard_stats(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get dashboard statistics (excluding soft-deleted)"""
    logger.debug("[Analytics] Fetching dashboard stats")

    query = db.query(Application).filter(
        Application.candidate_id == current_candidate.id,
        Application.deleted_at.is_(None)
    )

    total_apps = query.count()
    # Count sent as anything that's not draft
    sent = query.filter(Application.status != ApplicationStatusEnum.DRAFT).count()
    opened = query.filter(Application.opened_at.isnot(None)).count()
    replied = query.filter(Application.replied_at.isnot(None)).count()

    # Safe division to prevent division by zero
    response_rate = round((replied / sent * 100), 1) if sent > 0 else 0.0
    open_rate = round((opened / sent * 100), 1) if sent > 0 else 0.0

    logger.debug(f"[Analytics] Dashboard stats: total={total_apps}, sent={sent}, opened={opened}, replied={replied}")

    return {
        "total_applications": total_apps,
        "total_sent": sent,
        "total_opened": opened,
        "total_replied": replied,
        "response_rate": response_rate,
        "open_rate": open_rate
    }


@router.get("/by-status")
def get_applications_by_status(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get application count by status (excluding soft-deleted)"""
    logger.debug("[Analytics] Fetching applications by status")

    results = db.query(
        Application.status,
        func.count(Application.id).label('count')
    ).filter(
        Application.candidate_id == current_candidate.id,
        Application.deleted_at.is_(None)
    ).group_by(Application.status).all()

    status_counts = [{"status": r.status, "count": r.count} for r in results]
    logger.debug(f"[Analytics] Found {len(status_counts)} status groups")

    return status_counts


@router.get("/by-company")
def get_applications_by_company(
    limit: int = Query(10, ge=1, le=50, description="Maximum companies to return"),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get top companies by application count (excluding soft-deleted)"""
    logger.debug(f"[Analytics] Fetching top {limit} companies by application count")

    results = db.query(
        Company.name,
        func.count(Application.id).label('count')
    ).join(Application).filter(
        Application.candidate_id == current_candidate.id,
        Application.deleted_at.is_(None),
        Company.deleted_at.is_(None)
    ).group_by(Company.name).order_by(
        func.count(Application.id).desc()
    ).limit(limit).all()

    companies = [{"company_name": r.name, "count": r.count} for r in results]
    logger.debug(f"[Analytics] Retrieved {len(companies)} companies")

    return companies
