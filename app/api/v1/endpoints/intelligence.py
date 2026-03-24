"""
Company Intelligence API - View research data
"""
import logging
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import select, desc
from typing import List, Dict, Any
import json

logger = logging.getLogger(__name__)

from app.core.database import get_db
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.company_intelligence import CompanyResearchCache
from app.models.company import Company

router = APIRouter()


@router.get("/")
async def list_company_research(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
    limit: int = 100
):
    """
    Get all company research for current user
    Shows cached research results from ULTRA AI company intelligence
    """

    # Get all companies that have been researched
    stmt = (
        select(CompanyResearchCache, Company)
        .join(Company, CompanyResearchCache.company_id == Company.id)
        .order_by(desc(CompanyResearchCache.last_refreshed))
        .limit(limit)
    )
    results = db.execute(stmt).all()

    research_cache = []
    for cache, company in results:
        # Parse JSON fields
        tech_stack = {}
        try:
            tech_stack = json.loads(cache.tech_stack_detailed) if isinstance(cache.tech_stack_detailed, str) else cache.tech_stack_detailed or {}
        except Exception as e:
            logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

        company_culture = {}
        try:
            company_culture = json.loads(cache.company_culture) if isinstance(cache.company_culture, str) else cache.company_culture or {}
        except Exception as e:
            logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

        recent_news = []
        try:
            recent_news = json.loads(cache.recent_news) if isinstance(cache.recent_news, str) else cache.recent_news or []
        except Exception as e:
            logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

        job_openings = []
        try:
            job_openings = json.loads(cache.job_openings) if isinstance(cache.job_openings, str) else cache.job_openings or []
        except Exception as e:
            logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

        blog_posts = []
        try:
            blog_posts = json.loads(cache.blog_posts) if isinstance(cache.blog_posts, str) else cache.blog_posts or []
        except Exception as e:
            logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

        data_sources = []
        try:
            data_sources = json.loads(cache.data_sources) if isinstance(cache.data_sources, str) else cache.data_sources or []
        except Exception as e:
            logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

        research_cache.append({
            "id": cache.id,
            "company_id": company.id,
            "company_name": company.name,
            "company_website": company.website_url,
            "research_depth": cache.research_depth.value if cache.research_depth else "standard",
            "about_summary": cache.about_summary,
            "company_culture": company_culture,
            "recent_news": recent_news,
            "job_openings": job_openings,
            "blog_posts": blog_posts,
            "tech_stack": tech_stack,
            "completeness_score": cache.completeness_score,
            "data_sources": data_sources,
            "created_at": cache.created_at.isoformat() if cache.created_at else None,
            "last_refreshed": cache.last_refreshed.isoformat() if cache.last_refreshed else None,
            "expires_at": cache.expires_at.isoformat() if cache.expires_at else None,
        })

    return {
        "research_cache": research_cache,
        "total": len(research_cache)
    }


@router.get("/{company_id}")
async def get_company_research(
    company_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get detailed research for a specific company"""

    stmt = (
        select(CompanyResearchCache, Company)
        .join(Company, CompanyResearchCache.company_id == Company.id)
        .where(CompanyResearchCache.company_id == company_id)
        .order_by(desc(CompanyResearchCache.last_refreshed))
        .limit(1)
    )
    result = db.execute(stmt).first()

    if not result:
        return {
            "error": "No research found for this company",
            "company_id": company_id
        }

    cache, company = result

    # Parse all JSON fields
    tech_stack = {}
    try:
        tech_stack = json.loads(cache.tech_stack_detailed) if isinstance(cache.tech_stack_detailed, str) else cache.tech_stack_detailed or {}
    except Exception as e:
        logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

    company_culture = {}
    try:
        company_culture = json.loads(cache.company_culture) if isinstance(cache.company_culture, str) else cache.company_culture or {}
    except Exception as e:
        logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

    recent_news = []
    try:
        recent_news = json.loads(cache.recent_news) if isinstance(cache.recent_news, str) else cache.recent_news or []
    except Exception as e:
        logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

    job_openings = []
    try:
        job_openings = json.loads(cache.job_openings) if isinstance(cache.job_openings, str) else cache.job_openings or []
    except Exception as e:
        logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

    blog_posts = []
    try:
        blog_posts = json.loads(cache.blog_posts) if isinstance(cache.blog_posts, str) else cache.blog_posts or []
    except Exception as e:
        logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

    data_sources = []
    try:
        data_sources = json.loads(cache.data_sources) if isinstance(cache.data_sources, str) else cache.data_sources or []
    except Exception as e:
        logger.warning(f"[Intelligence] Failed to parse JSON field: {e}")

    return {
        "id": cache.id,
        "company": {
            "id": company.id,
            "name": company.name,
            "website_url": company.website_url,
            "domain": company.domain,
        },
        "research_depth": cache.research_depth.value if cache.research_depth else "standard",
        "about_summary": cache.about_summary,
        "mission_statement": cache.mission_statement,
        "company_culture": company_culture,
        "recent_news": recent_news,
        "job_openings": job_openings,
        "blog_posts": blog_posts,
        "tech_stack": tech_stack,
        "employee_count_estimate": cache.employee_count_estimate,
        "completeness_score": cache.completeness_score,
        "data_sources": data_sources,
        "created_at": cache.created_at.isoformat() if cache.created_at else None,
        "last_refreshed": cache.last_refreshed.isoformat() if cache.last_refreshed else None,
        "expires_at": cache.expires_at.isoformat() if cache.expires_at else None,
    }


@router.get("/stats/overview")
async def get_intelligence_stats(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get statistics about company intelligence"""

    total_research = db.execute(
        select(CompanyResearchCache)
    ).scalars().all()

    companies_researched = len(set(r.company_id for r in total_research))

    avg_completeness = sum(r.completeness_score for r in total_research) / len(total_research) if total_research else 0

    return {
        "total_research_records": len(total_research),
        "companies_researched": companies_researched,
        "average_completeness": round(avg_completeness, 1),
        "research_by_depth": {
            "quick": sum(1 for r in total_research if r.research_depth and r.research_depth.value == "quick"),
            "standard": sum(1 for r in total_research if r.research_depth and r.research_depth.value == "standard"),
            "deep": sum(1 for r in total_research if r.research_depth and r.research_depth.value == "deep"),
            "exhaustive": sum(1 for r in total_research if r.research_depth and r.research_depth.value == "exhaustive"),
        }
    }
