"""Search Endpoints - Advanced search functionality"""
import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from typing import Optional, List
from datetime import datetime, timedelta

from app.api.dependencies import get_db
from app.core.auth import get_current_candidate
from app.models.application import Application, ApplicationStatusEnum
from app.models.company import Company
from app.models.candidate import Candidate
from app.schemas.application import ApplicationResponse
from app.schemas.common import PaginatedResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/applications", response_model=PaginatedResponse[ApplicationResponse])
async def search_applications(
    q: Optional[str] = Query(None, description="Search query (company name, position, recruiter)"),
    status: Optional[str] = Query(None, description="Filter by status"),
    company_name: Optional[str] = Query(None, description="Filter by company name"),
    position_title: Optional[str] = Query(None, description="Filter by position title"),
    recruiter_email: Optional[str] = Query(None, description="Filter by recruiter email"),
    date_from: Optional[datetime] = Query(None, description="Filter by created date from"),
    date_to: Optional[datetime] = Query(None, description="Filter by created date to"),
    min_alignment_score: Optional[float] = Query(None, ge=0, le=100, description="Minimum alignment score"),
    is_starred: Optional[bool] = Query(None, description="Filter starred applications"),
    tags: Optional[str] = Query(None, description="Filter by tags (comma-separated)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Advanced search for applications with multiple filters (authenticated)

    Supports:
    - Full-text search across company, position, and recruiter
    - Status filtering
    - Date range filtering
    - Alignment score filtering
    - Tag filtering
    - Starred filtering

    Note: Only returns applications belonging to the authenticated user.
    """
    # Security: Always filter by the authenticated user's candidate_id
    candidate_id = current_user.id
    logger.info(f"[Search] Searching applications - query: '{q}', status: {status}, candidate_id: {candidate_id}, skip: {skip}, limit: {limit}")

    # Base query - exclude soft-deleted
    query = db.query(Application).join(Company).filter(
        Application.deleted_at.is_(None),
        Company.deleted_at.is_(None)
    )

    # Full-text search
    if q:
        search_term = f"%{q}%"
        query = query.filter(
            or_(
                Company.name.ilike(search_term),
                Application.position_title.ilike(search_term),
                Application.recruiter_name.ilike(search_term),
                Application.recruiter_email.ilike(search_term),
                Application.notes.ilike(search_term)
            )
        )

    # Specific filters
    if status:
        query = query.filter(Application.status == status)

    if company_name:
        query = query.filter(Company.name.ilike(f"%{company_name}%"))

    if position_title:
        query = query.filter(Application.position_title.ilike(f"%{position_title}%"))

    if recruiter_email:
        query = query.filter(Application.recruiter_email.ilike(f"%{recruiter_email}%"))

    if date_from:
        query = query.filter(Application.created_at >= date_from)

    if date_to:
        query = query.filter(Application.created_at <= date_to)

    if min_alignment_score is not None:
        query = query.filter(Application.alignment_score >= min_alignment_score)

    if is_starred is not None:
        query = query.filter(Application.is_starred == is_starred)

    if tags:
        tag_list = [tag.strip() for tag in tags.split(",")]
        tag_filters = [Application.tags.ilike(f"%{tag}%") for tag in tag_list]
        query = query.filter(or_(*tag_filters))

    # Security: Always filter by authenticated user's candidate_id
    query = query.filter(Application.candidate_id == candidate_id)

    # Get total count
    total = query.count()

    # Get paginated results
    applications = query.order_by(Application.created_at.desc()).offset(skip).limit(limit).all()

    logger.info(f"[Search] Found {len(applications)} applications (total: {total}) in {limit} page size")

    # Build response with company data
    items = []
    for app in applications:
        app_dict = {
            "id": app.id,
            "candidate_id": app.candidate_id,
            "company_id": app.company_id,
            "company_name": app.company.name if app.company else None,
            "recruiter_name": app.recruiter_name,
            "recruiter_email": app.recruiter_email,
            "position_title": app.position_title,
            "position_level": app.position_level,
            "alignment_text": app.alignment_text,
            "alignment_score": app.alignment_score,
            "status": app.status,
            "sent_at": app.sent_at,
            "opened_at": app.opened_at,
            "replied_at": app.replied_at,
            "created_at": app.created_at,
            "is_starred": app.is_starred,
            "tags": app.tags,
            "notes": app.notes
        }
        items.append(app_dict)

    return {
        "items": items,
        "total": total,
        "page": (skip // limit) + 1 if limit > 0 else 1,
        "page_size": limit
    }


@router.get("/companies")
def search_companies(
    q: Optional[str] = Query(None, description="Search query (name, industry, description)"),
    industry: Optional[str] = Query(None, description="Filter by industry"),
    min_applications: Optional[int] = Query(None, ge=0, description="Minimum number of applications"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Search companies with filters"""
    logger.info(f"[Search] Searching companies - query: '{q}', industry: {industry}, skip: {skip}, limit: {limit}")

    # Base query - exclude soft-deleted
    query = db.query(Company).filter(Company.deleted_at.is_(None))

    # Full-text search
    if q:
        search_term = f"%{q}%"
        query = query.filter(
            or_(
                Company.name.ilike(search_term),
                Company.industry.ilike(search_term),
                Company.description.ilike(search_term)
            )
        )

    # Specific filters
    if industry:
        query = query.filter(Company.industry.ilike(f"%{industry}%"))

    if min_applications is not None:
        query = query.filter(Company.total_applications >= min_applications)

    # Get total count
    total = query.count()

    # Get paginated results
    companies = query.order_by(Company.total_applications.desc()).offset(skip).limit(limit).all()

    logger.info(f"[Search] Found {len(companies)} companies (total: {total})")

    return {
        "items": companies,
        "total": total,
        "page": (skip // limit) + 1 if limit > 0 else 1,
        "page_size": limit
    }
