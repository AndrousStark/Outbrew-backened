"""Company Intelligence API Endpoints - Smart Research, Skill Matching & Email Drafting"""
import logging
from fastapi import APIRouter, Depends, HTTPException, Query, Body, Request
from app.core.rate_limiter import limiter
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

logger = logging.getLogger(__name__)

from app.core.database import get_db
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.company import Company
from app.models.company_intelligence import (
    CompanyProject, CompanyResearchCache, SkillMatch,
    PersonalizedEmailDraft, CandidateSkillProfile,
    ResearchDepthEnum, EmailToneEnum, MatchStrengthEnum
)
from app.services.smart_company_research import SmartCompanyResearchService
from app.services.email_drafter import PersonalizedEmailDrafter

router = APIRouter(tags=["company-intelligence"])


# ============= PYDANTIC SCHEMAS =============

class ResearchCompanyRequest(BaseModel):
    company_id: int
    depth: str = "standard"
    force_refresh: bool = False


class SkillMatchRequest(BaseModel):
    company_id: int
    force_refresh: bool = False


class BatchMatchRequest(BaseModel):
    company_ids: List[int]


class GenerateEmailRequest(BaseModel):
    company_id: int
    skill_match_id: Optional[int] = None
    tone: str = "professional"
    include_projects: bool = True
    include_achievements: bool = True
    custom_opening: Optional[str] = None
    job_title: Optional[str] = None


class ExtractSkillsRequest(BaseModel):
    resume_text: Optional[str] = None


class CompanyProjectResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    project_type: str
    url: Optional[str]
    technologies: List[str]
    skills_required: List[str]
    is_active: bool
    confidence_score: float

    class Config:
        from_attributes = True


class SkillMatchResponse(BaseModel):
    id: int
    company_id: int
    company_name: str
    match_strength: str
    overall_score: float
    matched_skills: List[str]
    category_scores: dict
    match_context: str
    talking_points: List[str]
    calculated_at: datetime

    class Config:
        from_attributes = True


class EmailDraftResponse(BaseModel):
    id: int
    company_id: int
    company_name: str
    subject_line: str
    subject_alternatives: List[str]
    email_body: str
    email_html: str
    tone: str
    confidence_score: float
    relevance_score: float
    personalization_level: float
    is_favorite: bool
    is_used: bool
    created_at: datetime

    class Config:
        from_attributes = True


class SkillProfileResponse(BaseModel):
    id: int
    programming_languages: List[str]
    frameworks: List[str]
    databases: List[str]
    cloud_devops: List[str]
    tools: List[str]
    soft_skills: List[str]
    domain_knowledge: List[str]
    primary_expertise: List[str]
    secondary_skills: List[str]
    projects: List[dict]
    work_experience: List[dict]
    education: List[dict]
    achievements: List[str]
    completeness_score: float
    last_analyzed: datetime

    class Config:
        from_attributes = True


# ============= COMPANY RESEARCH ENDPOINTS =============

@router.post("/research")
@limiter.limit("10/minute")
async def research_company(
    http_request: Request,
    request: ResearchCompanyRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Research a company in depth.
    Extracts projects, tech stack, culture, job openings, and more.
    """
    try:
        depth = ResearchDepthEnum(request.depth)
    except ValueError:
        depth = ResearchDepthEnum.STANDARD

    service = SmartCompanyResearchService(db)

    try:
        result = await service.research_company(
            company_id=request.company_id,
            depth=depth,
            force_refresh=request.force_refresh
        )
        return {"success": True, "data": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Research failed: {e}")
        raise HTTPException(status_code=500, detail="Research failed")


@router.get("/research/{company_id}")
async def get_company_research(
    company_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get cached research data for a company"""
    cache = db.query(CompanyResearchCache).filter(
        CompanyResearchCache.company_id == company_id
    ).first()

    if not cache:
        # Trigger research if not exists
        service = SmartCompanyResearchService(db)
        result = await service.research_company(company_id, ResearchDepthEnum.STANDARD)
        return {"success": True, "data": result, "from_cache": False}

    return {
        "success": True,
        "data": {
            "id": cache.id,
            "company_id": cache.company_id,
            "research_depth": cache.research_depth.value,
            "about_summary": cache.about_summary,
            "mission_statement": cache.mission_statement,
            "company_culture": cache.company_culture,
            "recent_news": cache.recent_news,
            "job_openings": cache.job_openings,
            "key_people": cache.key_people,
            "funding_info": cache.funding_info,
            "tech_stack_detailed": cache.tech_stack_detailed,
            "github_repos": cache.github_repos,
            "blog_posts": cache.blog_posts,
            "social_links": cache.social_links,
            "employee_count_estimate": cache.employee_count_estimate,
            "growth_signals": cache.growth_signals,
            "completeness_score": cache.completeness_score,
            "data_sources": cache.data_sources,
            "last_refreshed": cache.last_refreshed.isoformat() if cache.last_refreshed else None,
            "expires_at": cache.expires_at.isoformat() if cache.expires_at else None
        },
        "from_cache": True
    }


@router.get("/projects/{company_id}")
async def get_company_projects(
    company_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get discovered projects for a company"""
    projects = db.query(CompanyProject).filter(
        CompanyProject.company_id == company_id
    ).order_by(CompanyProject.confidence_score.desc()).all()

    return {
        "success": True,
        "count": len(projects),
        "projects": [
            {
                "id": p.id,
                "name": p.name,
                "description": p.description,
                "project_type": p.project_type.value,
                "url": p.url,
                "technologies": p.technologies or [],
                "skills_required": p.skills_required or [],
                "is_active": p.is_active,
                "confidence_score": p.confidence_score,
                "source_url": p.source_url,
                "discovered_at": p.discovered_at.isoformat() if p.discovered_at else None
            }
            for p in projects
        ]
    }


# ============= SKILL PROFILE ENDPOINTS =============

@router.post("/skills/extract")
async def extract_skills(
    request: ExtractSkillsRequest = Body(...),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Extract and categorize candidate skills from resume text"""
    service = SmartCompanyResearchService(db)

    profile = await service.extract_candidate_skills(
        candidate_id=current_candidate.id,
        resume_text=request.resume_text
    )

    return {
        "success": True,
        "profile": {
            "id": profile.id,
            "programming_languages": profile.programming_languages or [],
            "frameworks": profile.frameworks or [],
            "databases": profile.databases or [],
            "cloud_devops": profile.cloud_devops or [],
            "tools": profile.tools or [],
            "soft_skills": profile.soft_skills or [],
            "domain_knowledge": profile.domain_knowledge or [],
            "primary_expertise": profile.primary_expertise or [],
            "secondary_skills": profile.secondary_skills or [],
            "projects": profile.projects or [],
            "work_experience": profile.work_experience or [],
            "education": profile.education or [],
            "achievements": profile.achievements or [],
            "completeness_score": profile.completeness_score,
            "last_analyzed": profile.last_analyzed.isoformat() if profile.last_analyzed else None
        }
    }


@router.get("/skills/profile")
async def get_skill_profile(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get current user's skill profile"""
    profile = db.query(CandidateSkillProfile).filter(
        CandidateSkillProfile.candidate_id == current_candidate.id
    ).first()

    if not profile:
        # Create empty profile
        service = SmartCompanyResearchService(db)
        profile = await service.extract_candidate_skills(current_candidate.id)

    return {
        "success": True,
        "profile": {
            "id": profile.id,
            "programming_languages": profile.programming_languages or [],
            "frameworks": profile.frameworks or [],
            "databases": profile.databases or [],
            "cloud_devops": profile.cloud_devops or [],
            "tools": profile.tools or [],
            "soft_skills": profile.soft_skills or [],
            "domain_knowledge": profile.domain_knowledge or [],
            "primary_expertise": profile.primary_expertise or [],
            "secondary_skills": profile.secondary_skills or [],
            "projects": profile.projects or [],
            "work_experience": profile.work_experience or [],
            "education": profile.education or [],
            "achievements": profile.achievements or [],
            "skill_levels": profile.skill_levels or {},
            "years_experience": profile.years_experience,
            "completeness_score": profile.completeness_score,
            "last_analyzed": profile.last_analyzed.isoformat() if profile.last_analyzed else None
        }
    }


# ============= SKILL MATCHING ENDPOINTS =============

@router.post("/match")
async def match_skills(
    request: SkillMatchRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Match candidate skills to a company"""
    service = SmartCompanyResearchService(db)

    try:
        match = await service.match_candidate_to_company(
            candidate_id=current_candidate.id,
            company_id=request.company_id,
            force_refresh=request.force_refresh
        )

        company = db.query(Company).filter(Company.id == request.company_id).first()

        return {
            "success": True,
            "match": {
                "id": match.id,
                "company_id": match.company_id,
                "company_name": company.name if company else "Unknown",
                "match_strength": match.match_strength.value,
                "overall_score": match.overall_score,
                "matched_skills": match.matched_skills or [],
                "candidate_skills_used": match.candidate_skills_used or [],
                "company_needs": match.company_needs or [],
                "category_scores": match.category_scores or {},
                "match_context": match.match_context,
                "talking_points": match.talking_points or [],
                "calculated_at": match.calculated_at.isoformat() if match.calculated_at else None
            }
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/match/batch")
async def batch_match_skills(
    request: BatchMatchRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Match candidate skills against multiple companies"""
    service = SmartCompanyResearchService(db)

    matches = await service.batch_match_companies(
        candidate_id=current_candidate.id,
        company_ids=request.company_ids
    )

    results = []
    for match in matches:
        company = db.query(Company).filter(Company.id == match.company_id).first()
        results.append({
            "id": match.id,
            "company_id": match.company_id,
            "company_name": company.name if company else "Unknown",
            "match_strength": match.match_strength.value,
            "overall_score": match.overall_score,
            "matched_skills": match.matched_skills or [],
            "talking_points": match.talking_points or []
        })

    # Sort by score
    results.sort(key=lambda x: x["overall_score"], reverse=True)

    return {
        "success": True,
        "count": len(results),
        "matches": results
    }


@router.get("/matches")
async def get_all_matches(
    limit: int = Query(default=20, le=100),
    min_score: float = Query(default=0),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get all skill matches for current user"""
    query = db.query(SkillMatch).filter(
        SkillMatch.candidate_id == current_candidate.id
    )

    if min_score > 0:
        query = query.filter(SkillMatch.overall_score >= min_score)

    matches = query.order_by(SkillMatch.overall_score.desc()).limit(limit).all()

    results = []
    for match in matches:
        company = db.query(Company).filter(Company.id == match.company_id).first()
        results.append({
            "id": match.id,
            "company_id": match.company_id,
            "company_name": company.name if company else "Unknown",
            "industry": company.industry if company else None,
            "match_strength": match.match_strength.value,
            "overall_score": match.overall_score,
            "matched_skills": match.matched_skills or [],
            "category_scores": match.category_scores or {},
            "match_context": match.match_context,
            "talking_points": match.talking_points or [],
            "calculated_at": match.calculated_at.isoformat() if match.calculated_at else None
        })

    return {
        "success": True,
        "count": len(results),
        "matches": results
    }


@router.get("/matches/best")
async def get_best_matches(
    limit: int = Query(default=10, le=50),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get best matching companies for current user"""
    service = SmartCompanyResearchService(db)

    best = await service.get_best_company_matches(
        candidate_id=current_candidate.id,
        limit=limit
    )

    return {
        "success": True,
        "count": len(best),
        "best_matches": best
    }


# ============= EMAIL DRAFT ENDPOINTS =============

@router.post("/email/generate")
async def generate_email_draft(
    request: GenerateEmailRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Generate a personalized email draft"""
    try:
        tone = EmailToneEnum(request.tone)
    except ValueError:
        tone = EmailToneEnum.PROFESSIONAL

    drafter = PersonalizedEmailDrafter(db)

    try:
        draft = await drafter.generate_email_draft(
            candidate_id=current_candidate.id,
            company_id=request.company_id,
            skill_match_id=request.skill_match_id,
            tone=tone,
            include_projects=request.include_projects,
            include_achievements=request.include_achievements,
            custom_opening=request.custom_opening,
            job_title=request.job_title
        )

        company = db.query(Company).filter(Company.id == request.company_id).first()

        return {
            "success": True,
            "draft": {
                "id": draft.id,
                "company_id": draft.company_id,
                "company_name": company.name if company else "Unknown",
                "subject_line": draft.subject_line,
                "subject_alternatives": draft.subject_alternatives or [],
                "email_body": draft.email_body,
                "email_html": draft.email_html,
                "opening": draft.opening,
                "skill_highlights": draft.skill_highlights,
                "company_specific": draft.company_specific,
                "call_to_action": draft.call_to_action,
                "closing": draft.closing,
                "tone": draft.tone.value,
                "confidence_score": draft.confidence_score,
                "relevance_score": draft.relevance_score,
                "personalization_level": draft.personalization_level,
                "is_favorite": draft.is_favorite,
                "is_used": draft.is_used,
                "created_at": draft.created_at.isoformat() if draft.created_at else None
            }
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/email/quick-draft/{company_id}")
async def quick_draft(
    company_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Quick draft generation with default settings"""
    drafter = PersonalizedEmailDrafter(db)

    try:
        result = await drafter.quick_draft(
            candidate_id=current_candidate.id,
            company_id=company_id
        )
        return {"success": True, **result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/email/variations/{company_id}")
async def generate_variations(
    company_id: int,
    count: int = Query(default=3, le=5),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Generate multiple email variations with different tones"""
    drafter = PersonalizedEmailDrafter(db)

    drafts = await drafter.generate_variations(
        candidate_id=current_candidate.id,
        company_id=company_id,
        count=count
    )

    company = db.query(Company).filter(Company.id == company_id).first()

    return {
        "success": True,
        "count": len(drafts),
        "variations": [
            {
                "id": d.id,
                "tone": d.tone.value,
                "subject_line": d.subject_line,
                "email_body": d.email_body,
                "confidence_score": d.confidence_score
            }
            for d in drafts
        ]
    }


@router.get("/email/drafts")
async def get_all_drafts(
    limit: int = Query(default=50, le=200),
    favorites_only: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get all email drafts for current user"""
    query = db.query(PersonalizedEmailDraft).filter(
        PersonalizedEmailDraft.candidate_id == current_candidate.id
    )

    if favorites_only:
        query = query.filter(PersonalizedEmailDraft.is_favorite == True)

    drafts = query.order_by(PersonalizedEmailDraft.created_at.desc()).limit(limit).all()

    results = []
    for d in drafts:
        company = db.query(Company).filter(Company.id == d.company_id).first()
        results.append({
            "id": d.id,
            "company_id": d.company_id,
            "company_name": company.name if company else "Unknown",
            "subject_line": d.subject_line,
            "email_body": d.email_body[:200] + "..." if len(d.email_body) > 200 else d.email_body,
            "tone": d.tone.value,
            "confidence_score": d.confidence_score,
            "is_favorite": d.is_favorite,
            "is_used": d.is_used,
            "created_at": d.created_at.isoformat() if d.created_at else None
        })

    return {
        "success": True,
        "count": len(results),
        "drafts": results
    }


@router.get("/email/drafts/{company_id}")
async def get_company_drafts(
    company_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get all drafts for a specific company"""
    drafter = PersonalizedEmailDrafter(db)

    drafts = await drafter.get_drafts_for_company(
        candidate_id=current_candidate.id,
        company_id=company_id
    )

    company = db.query(Company).filter(Company.id == company_id).first()

    return {
        "success": True,
        "company_name": company.name if company else "Unknown",
        "count": len(drafts),
        "drafts": [
            {
                "id": d.id,
                "subject_line": d.subject_line,
                "subject_alternatives": d.subject_alternatives or [],
                "email_body": d.email_body,
                "email_html": d.email_html,
                "tone": d.tone.value,
                "confidence_score": d.confidence_score,
                "relevance_score": d.relevance_score,
                "personalization_level": d.personalization_level,
                "is_favorite": d.is_favorite,
                "is_used": d.is_used,
                "created_at": d.created_at.isoformat() if d.created_at else None
            }
            for d in drafts
        ]
    }


@router.get("/email/draft/{draft_id}")
async def get_draft(
    draft_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get a specific email draft"""
    draft = db.query(PersonalizedEmailDraft).filter(
        PersonalizedEmailDraft.id == draft_id,
        PersonalizedEmailDraft.candidate_id == current_candidate.id
    ).first()

    if not draft:
        raise HTTPException(status_code=404, detail="Draft not found")

    company = db.query(Company).filter(Company.id == draft.company_id).first()

    return {
        "success": True,
        "draft": {
            "id": draft.id,
            "company_id": draft.company_id,
            "company_name": company.name if company else "Unknown",
            "subject_line": draft.subject_line,
            "subject_alternatives": draft.subject_alternatives or [],
            "email_body": draft.email_body,
            "email_html": draft.email_html,
            "opening": draft.opening,
            "skill_highlights": draft.skill_highlights,
            "company_specific": draft.company_specific,
            "call_to_action": draft.call_to_action,
            "closing": draft.closing,
            "tone": draft.tone.value,
            "confidence_score": draft.confidence_score,
            "relevance_score": draft.relevance_score,
            "personalization_level": draft.personalization_level,
            "generation_params": draft.generation_params,
            "is_favorite": draft.is_favorite,
            "is_used": draft.is_used,
            "used_at": draft.used_at.isoformat() if draft.used_at else None,
            "created_at": draft.created_at.isoformat() if draft.created_at else None
        }
    }


@router.put("/email/draft/{draft_id}/favorite")
async def toggle_favorite_draft(
    draft_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Toggle favorite status of a draft"""
    drafter = PersonalizedEmailDrafter(db)
    draft = await drafter.toggle_favorite(draft_id, candidate_id=current_candidate.id)

    if not draft:
        raise HTTPException(status_code=404, detail="Draft not found")

    return {
        "success": True,
        "draft_id": draft.id,
        "is_favorite": draft.is_favorite
    }


@router.put("/email/draft/{draft_id}/used")
async def mark_draft_used(
    draft_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Mark a draft as used"""
    drafter = PersonalizedEmailDrafter(db)
    draft = await drafter.mark_draft_as_used(draft_id, candidate_id=current_candidate.id)

    if not draft:
        raise HTTPException(status_code=404, detail="Draft not found")

    return {
        "success": True,
        "draft_id": draft.id,
        "is_used": draft.is_used,
        "used_at": draft.used_at.isoformat() if draft.used_at else None
    }


@router.post("/email/draft/{draft_id}/regenerate")
async def regenerate_draft(
    draft_id: int,
    tone: str = Query(default=None),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Regenerate a draft with new parameters"""
    drafter = PersonalizedEmailDrafter(db)

    kwargs = {}
    if tone:
        try:
            kwargs["tone"] = EmailToneEnum(tone)
        except ValueError:
            pass

    try:
        new_draft = await drafter.regenerate_draft(draft_id, **kwargs)

        return {
            "success": True,
            "draft": {
                "id": new_draft.id,
                "subject_line": new_draft.subject_line,
                "email_body": new_draft.email_body,
                "tone": new_draft.tone.value,
                "confidence_score": new_draft.confidence_score
            }
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/email/draft/{draft_id}")
async def delete_draft(
    draft_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Delete an email draft"""
    draft = db.query(PersonalizedEmailDraft).filter(
        PersonalizedEmailDraft.id == draft_id,
        PersonalizedEmailDraft.candidate_id == current_candidate.id
    ).first()

    if not draft:
        raise HTTPException(status_code=404, detail="Draft not found")

    try:
        db.delete(draft)
        db.commit()
        logger.info(f"[CompanyIntelligence] Deleted email draft {draft_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[CompanyIntelligence] Failed to delete draft {draft_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete draft")

    return {"success": True, "message": "Draft deleted"}


# ============= DASHBOARD/SUMMARY ENDPOINTS =============

@router.get("/dashboard")
async def get_intelligence_dashboard(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get company intelligence dashboard data"""
    # Get skill profile
    profile = db.query(CandidateSkillProfile).filter(
        CandidateSkillProfile.candidate_id == current_candidate.id
    ).first()

    # Get matches summary
    total_matches = db.query(SkillMatch).filter(
        SkillMatch.candidate_id == current_candidate.id
    ).count()

    strong_matches = db.query(SkillMatch).filter(
        SkillMatch.candidate_id == current_candidate.id,
        SkillMatch.overall_score >= 60
    ).count()

    # Get drafts summary
    total_drafts = db.query(PersonalizedEmailDraft).filter(
        PersonalizedEmailDraft.candidate_id == current_candidate.id
    ).count()

    used_drafts = db.query(PersonalizedEmailDraft).filter(
        PersonalizedEmailDraft.candidate_id == current_candidate.id,
        PersonalizedEmailDraft.is_used == True
    ).count()

    favorite_drafts = db.query(PersonalizedEmailDraft).filter(
        PersonalizedEmailDraft.candidate_id == current_candidate.id,
        PersonalizedEmailDraft.is_favorite == True
    ).count()

    # Get top matches
    top_matches = db.query(SkillMatch).filter(
        SkillMatch.candidate_id == current_candidate.id
    ).order_by(SkillMatch.overall_score.desc()).limit(5).all()

    # Get recent drafts
    recent_drafts = db.query(PersonalizedEmailDraft).filter(
        PersonalizedEmailDraft.candidate_id == current_candidate.id
    ).order_by(PersonalizedEmailDraft.created_at.desc()).limit(5).all()

    # Batch query all companies needed (fixes N+1 query issue)
    company_ids = set()
    for match in top_matches:
        company_ids.add(match.company_id)
    for draft in recent_drafts:
        company_ids.add(draft.company_id)

    companies_lookup = {}
    if company_ids:
        companies = db.query(Company).filter(Company.id.in_(company_ids)).all()
        companies_lookup = {c.id: c.name for c in companies}

    # Build top matches data using lookup
    top_matches_data = []
    for match in top_matches:
        top_matches_data.append({
            "company_id": match.company_id,
            "company_name": companies_lookup.get(match.company_id, "Unknown"),
            "match_strength": match.match_strength.value,
            "overall_score": match.overall_score
        })

    # Build recent drafts data using lookup
    recent_drafts_data = []
    for draft in recent_drafts:
        recent_drafts_data.append({
            "id": draft.id,
            "company_name": companies_lookup.get(draft.company_id, "Unknown"),
            "subject_line": draft.subject_line,
            "tone": draft.tone.value,
            "created_at": draft.created_at.isoformat() if draft.created_at else None
        })

    return {
        "success": True,
        "dashboard": {
            "skill_profile": {
                "exists": profile is not None,
                "completeness": profile.completeness_score if profile else 0,
                "primary_expertise": profile.primary_expertise if profile else [],
                "total_skills": sum([
                    len(profile.programming_languages or []),
                    len(profile.frameworks or []),
                    len(profile.databases or []),
                    len(profile.cloud_devops or []),
                    len(profile.tools or [])
                ]) if profile else 0
            },
            "matches": {
                "total": total_matches,
                "strong": strong_matches,
                "top_matches": top_matches_data
            },
            "drafts": {
                "total": total_drafts,
                "used": used_drafts,
                "favorites": favorite_drafts,
                "recent": recent_drafts_data
            }
        }
    }
