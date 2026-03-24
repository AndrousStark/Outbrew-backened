"""
Unified Templates API - Shows both EmailTemplates and PersonalizedEmailDrafts
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, desc
from typing import List, Dict, Any
import json
import logging

from app.core.database import get_db
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.email_template import EmailTemplate
from app.models.company_intelligence import PersonalizedEmailDraft
from app.models.company import Company

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
async def list_all_templates(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
    limit: int = 100,
    offset: int = 0
):
    """
    Get all templates and AI-generated drafts for current user.
    Returns unified list combining EmailTemplate and PersonalizedEmailDraft.
    """
    # Get traditional email templates via SQLAlchemy
    templates_query = (
        db.query(EmailTemplate)
        .filter(
            EmailTemplate.candidate_id == current_candidate.id,
            EmailTemplate.is_active == True,
            EmailTemplate.deleted_at.is_(None)
        )
        .order_by(desc(EmailTemplate.created_at))
        .limit(limit)
    )
    templates_rows = templates_query.all()

    # Get AI-generated drafts via SQLAlchemy
    drafts_stmt = (
        select(PersonalizedEmailDraft, Company.name)
        .join(Company, PersonalizedEmailDraft.company_id == Company.id)
        .where(PersonalizedEmailDraft.candidate_id == current_candidate.id)
        .order_by(desc(PersonalizedEmailDraft.created_at))
        .limit(limit)
    )
    drafts_results = db.execute(drafts_stmt).all()

    # Transform templates to unified format
    items = []

    for t in templates_rows:
        items.append({
            "id": t.id,
            "type": "template",
            "name": t.name,
            "subject_template": t.subject_template,
            "body_template_html": t.body_template_html,
            "category": t.category.value if hasattr(t.category, 'value') else (t.category or "general"),
            "language": t.language.value if hasattr(t.language, 'value') else (t.language or "english"),
            "tone": t.tone.value if hasattr(t.tone, 'value') else t.tone,
            "target_position": t.target_position,
            "target_country": t.target_country,
            "is_default": bool(t.is_default),
            "is_active": bool(t.is_active),
            "times_used": t.times_used or 0,
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "last_used_at": t.last_used_at.isoformat() if t.last_used_at else None,
        })

    for draft, company_name in drafts_results:
        gen_params = {}
        if draft.generation_params:
            try:
                gen_params = json.loads(draft.generation_params) if isinstance(draft.generation_params, str) else draft.generation_params
            except Exception:
                pass

        items.append({
            "id": draft.id,
            "type": "ai_draft",
            "name": f"AI Generated - {company_name} ({draft.tone.value if hasattr(draft.tone, 'value') else draft.tone})",
            "subject_template": draft.subject_line,
            "body_template_html": draft.email_html or draft.email_body,
            "category": "ai_generated",
            "language": "english",
            "tone": draft.tone.value if hasattr(draft.tone, 'value') else draft.tone,
            "target_position": gen_params.get("recipient_position", ""),
            "target_company": company_name,
            "target_country": gen_params.get("recipient_country", ""),
            "is_default": False,
            "is_active": True,
            "times_used": 0,
            "is_favorite": bool(draft.is_favorite),
            "is_used": bool(draft.is_used),
            "personalization_level": draft.personalization_level,
            "confidence_score": draft.confidence_score,
            "matched_skills": gen_params.get("matched_skills", []),
            "estimated_response_rate": gen_params.get("estimated_response_rate", ""),
            "created_at": draft.created_at.isoformat() if draft.created_at else None,
            "used_at": draft.used_at.isoformat() if draft.used_at else None,
        })

    # Sort by created_at descending
    items.sort(key=lambda x: x.get("created_at") or "", reverse=True)

    return {
        "items": items,
        "total": len(items),
        "traditional_templates": len(templates_rows),
        "ai_drafts": len(drafts_results),
    }


@router.get("/ai-drafts")
async def list_ai_drafts(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
    limit: int = 100
):
    """Get only AI-generated email drafts"""

    stmt = (
        select(PersonalizedEmailDraft, Company.name)
        .join(Company, PersonalizedEmailDraft.company_id == Company.id)
        .where(PersonalizedEmailDraft.candidate_id == current_candidate.id)
        .order_by(desc(PersonalizedEmailDraft.created_at))
        .limit(limit)
    )
    results = db.execute(stmt).all()

    items = []
    for draft, company_name in results:
        gen_params = {}
        if draft.generation_params:
            try:
                gen_params = json.loads(draft.generation_params) if isinstance(draft.generation_params, str) else draft.generation_params
            except Exception as e:
                logger.warning(f"[Templates] Failed to parse generation_params: {e}")

        items.append({
            "id": draft.id,
            "company_name": company_name,
            "recipient_name": gen_params.get("recipient_name"),
            "recipient_position": gen_params.get("recipient_position"),
            "subject_line": draft.subject_line,
            "email_body": draft.email_body,
            "tone": draft.tone.value if hasattr(draft.tone, 'value') else draft.tone,
            "personalization_level": draft.personalization_level,
            "confidence_score": draft.confidence_score,
            "matched_skills": gen_params.get("matched_skills", []),
            "estimated_response_rate": gen_params.get("estimated_response_rate"),
            "is_favorite": draft.is_favorite,
            "is_used": draft.is_used,
            "created_at": draft.created_at.isoformat() if draft.created_at else None,
        })

    return {
        "items": items,
        "total": len(items)
    }


@router.get("/stats")
async def get_template_stats(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get statistics about templates and drafts"""

    # Count templates
    templates_count = db.execute(
        select(EmailTemplate)
        .where(EmailTemplate.candidate_id == current_candidate.id)
        .where(EmailTemplate.deleted_at.is_(None))
    ).scalars().all()

    # Count AI drafts
    drafts_count = db.execute(
        select(PersonalizedEmailDraft)
        .where(PersonalizedEmailDraft.candidate_id == current_candidate.id)
    ).scalars().all()

    return {
        "total_templates": len(templates_count),
        "total_ai_drafts": len(drafts_count),
        "total": len(templates_count) + len(drafts_count),
        "active_templates": sum(1 for t in templates_count if t.is_active),
        "favorite_drafts": sum(1 for d in drafts_count if d.is_favorite),
        "used_drafts": sum(1 for d in drafts_count if d.is_used),
    }

