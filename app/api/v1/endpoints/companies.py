"""Companies API Endpoints - Refactored with Repository Pattern (Phase 1)"""
import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.dependencies import get_db, get_current_candidate
from app.models.candidate import Candidate
from app.schemas.company import CompanyResponse

# Phase 1: Repository Pattern for automatic caching
from app.repositories.company import CompanyRepository

logger = logging.getLogger(__name__)

router = APIRouter()


# IMPORTANT: Static routes MUST come before parameterized routes to avoid shadowing
@router.get("/search/by-name")
def search_companies(
    name: str = Query(..., min_length=1, max_length=100),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Search companies by name (Phase 1: Using Repository Pattern)"""
    logger.info(f"[Companies] Searching for companies with name containing: '{name}'")

    # Phase 1: Use repository
    repo = CompanyRepository(db)
    companies = repo.search_by_name(name, limit=limit)

    logger.debug(f"[Companies] Search found {len(companies)} companies (cached)")
    return {"companies": companies, "count": len(companies)}


@router.get("/", response_model=list[CompanyResponse])
def list_companies(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """List all non-deleted companies (Phase 1: Using Repository Pattern)"""
    logger.info(f"[Companies] Listing companies - skip: {skip}, limit: {limit}")

    # Phase 1: Use repository
    repo = CompanyRepository(db)
    companies = repo.get_all(skip=skip, limit=limit)
    total = repo.count()

    logger.info(f"[Companies] Retrieved {len(companies)} of {total} total companies (repository pattern)")
    return companies


@router.get("/{company_id}", response_model=CompanyResponse)
def get_company(
    company_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get non-deleted company by ID (Phase 1: Using Repository Pattern)"""
    logger.debug(f"[Companies] Getting company with ID: {company_id}")

    # Phase 1: Use repository (automatic caching!)
    repo = CompanyRepository(db)
    company = repo.get_by_id(company_id)

    if not company:
        logger.warning(f"[Companies] Company not found: {company_id}")
        raise HTTPException(status_code=404, detail="Company not found")

    logger.debug(f"[Companies] Retrieved company: {company.name} (from cache or DB)")
    return company
