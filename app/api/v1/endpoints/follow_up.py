"""
Follow-Up Sequences API - ULTRA V2.0 with AI Copilot

Features:
- Sequence template management with preset bootstrapping
- AI Copilot for intelligent sequence and email generation
- ML predictions for reply probability and optimal send times
- Intent detection and branching support
"""

import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Body
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.dependencies import get_db, get_current_candidate
from app.models.candidate import Candidate
from app.models.follow_up import FollowUpSequence, FollowUpStep
from app.services.follow_up_service import FollowUpService
from datetime import timezone

# Setup logger
logger = logging.getLogger(__name__)

router = APIRouter()


# ============= PYDANTIC SCHEMAS FOR AI COPILOT =============

class CopilotGenerateSequenceRequest(BaseModel):
    """Request body for AI sequence generation"""
    user_request: str = Field(
        ...,
        description="Natural language description of the sequence to generate",
        example="Create a 4-email sequence for software engineering job applications, professional tone"
    )
    num_steps: int = Field(
        default=4,
        ge=1,
        le=10,
        description="Number of follow-up steps"
    )
    default_tone: str = Field(
        default="professional",
        description="Default tone for emails"
    )
    context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional context (company info, candidate info, etc.)"
    )


class CopilotGenerateEmailRequest(BaseModel):
    """Request body for AI email generation"""
    step_number: int = Field(
        ...,
        ge=1,
        description="Step number in the sequence"
    )
    strategy: str = Field(
        default="soft_bump",
        description="Email strategy"
    )
    tone: str = Field(
        default="professional",
        description="Email tone"
    )
    context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Context for personalization"
    )
    previous_emails: Optional[List[Dict[str, str]]] = Field(
        default=None,
        description="Previous emails in the sequence for context"
    )


class CopilotSuggestImprovementsRequest(BaseModel):
    """Request body for sequence improvement suggestions"""
    sequence_id: int = Field(
        ...,
        description="ID of the sequence to analyze"
    )


class CopilotGenerateABVariantsRequest(BaseModel):
    """Request body for A/B variant generation"""
    original_subject: str = Field(
        ...,
        description="Original subject line"
    )
    original_body: str = Field(
        ...,
        description="Original email body"
    )
    num_variants: int = Field(
        default=3,
        ge=2,
        le=5,
        description="Number of variants to generate"
    )


def _serialize_step(step: FollowUpStep) -> Dict[str, Any]:
    return {
        "id": step.id,
        "step_number": step.step_number,
        "delay_days": step.delay_days,
        "delay_hours": step.delay_hours,
        "strategy": step.strategy.value
        if hasattr(step.strategy, "value")
        else step.strategy,
        "tone": step.tone.value if hasattr(step.tone, "value") else step.tone,
        "subject_template": step.subject_template,
        "body_template": step.body_template,
        "generation_hints": step.generation_hints,
        "include_original_context": step.include_original_context,
        "include_value_proposition": step.include_value_proposition,
        "include_portfolio_link": step.include_portfolio_link,
        "include_call_to_action": step.include_call_to_action,
    }


def _serialize_sequence(seq: FollowUpSequence) -> Dict[str, Any]:
    return {
        "id": seq.id,
        "candidate_id": seq.candidate_id,
        "name": seq.name,
        "description": seq.description,
        "status": seq.status.value if hasattr(seq.status, "value") else seq.status,
        "is_system_preset": seq.is_system_preset,
        "stop_on_reply": seq.stop_on_reply,
        "stop_on_bounce": seq.stop_on_bounce,
        "use_threading": seq.use_threading,
        "respect_business_hours": seq.respect_business_hours,
        "business_hours_start": seq.business_hours_start,
        "business_hours_end": seq.business_hours_end,
        "include_candidate_links": seq.include_candidate_links,
        "include_portfolio": seq.include_portfolio,
        "include_signature": seq.include_signature,
        "custom_signature": seq.custom_signature,
        "preferred_send_hour": seq.preferred_send_hour,
        "preferred_timezone": seq.preferred_timezone,
        "times_used": seq.times_used,
        "total_campaigns": seq.total_campaigns,
        "successful_replies": seq.successful_replies,
        "reply_rate": seq.reply_rate,
        "created_at": seq.created_at,
        "updated_at": seq.updated_at,
        "steps": [_serialize_step(s) for s in seq.steps],
    }


@router.get("/sequences")
def list_sequences(
    include_presets: bool = Query(
        True, description="Include system preset sequences when empty"
    ),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """Return follow-up sequences for the authenticated candidate.

    - Bootstraps preset sequences if none exist and include_presets is True
    - Returns lightweight JSON to satisfy consolidated outreach UI
    """
    service = FollowUpService(db)

    sequences: List[FollowUpSequence] = service.get_sequences(
        candidate_id=current_candidate.id,
        include_presets=include_presets,
    )

    if not sequences and include_presets:
        sequences = service.create_preset_sequences(current_candidate.id)

    return {
        "items": [_serialize_sequence(seq) for seq in sequences],
        "count": len(sequences),
    }


# ============= AI COPILOT ENDPOINTS =============

@router.post("/copilot/generate-sequence")
async def generate_sequence_with_ai(
    request: CopilotGenerateSequenceRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Generate a complete follow-up sequence using AI Copilot.

    Takes a natural language description and generates a full sequence
    with email content for each step.

    Example requests:
    - "Create a 4-email sequence for software engineering job applications"
    - "Generate an aggressive 6-email sequence for sales outreach"
    - "Make a gentle 3-email sequence with value-focused content"
    """
    logger.info(f"[AI Copilot] Generating sequence for candidate {current_candidate.id}")
    logger.debug(f"[AI Copilot] Request: {request.user_request[:100]}...")

    try:
        # Import here to avoid circular imports
        from app.services.follow_up_ai_copilot import (
            FollowUpAICopilot,
            CopilotContext,
        )

        copilot = FollowUpAICopilot()

        # Build context from request and candidate
        context = CopilotContext(
            candidate_name=current_candidate.full_name or "User",
            candidate_email=current_candidate.email,
            company_name=request.context.get("company_name") if request.context else None,
            position=request.context.get("position") if request.context else None,
            industry=request.context.get("industry") if request.context else None,
            original_email_subject=request.context.get("original_subject") if request.context else None,
            original_email_body=request.context.get("original_body") if request.context else None,
        )

        # Generate sequence
        generated = await copilot.generate_sequence(
            user_request=request.user_request,
            context=context,
            num_steps=request.num_steps,
            default_tone=request.default_tone,
        )

        logger.info(f"[AI Copilot] Generated sequence '{generated.name}' with {len(generated.steps)} steps")

        # Optionally save to database
        service = FollowUpService(db)
        sequence = service.create_sequence(
            candidate_id=current_candidate.id,
            name=generated.name,
            description=generated.description,
            steps=[
                {
                    "delay_days": step.delay_days,
                    "strategy": step.strategy,
                    "tone": step.tone,
                    "subject_template": step.subject,
                    "body_template": step.body,
                }
                for step in generated.steps
            ],
        )

        # Mark as AI-generated
        sequence.ai_copilot_generated = True
        sequence.ai_generation_prompt = request.user_request
        db.commit()

        return {
            "success": True,
            "sequence": _serialize_sequence(sequence),
            "ai_metadata": {
                "model_used": generated.model_used,
                "tokens_used": generated.tokens_used,
                "generation_time_ms": generated.generation_time_ms,
            },
        }

    except ImportError as e:
        logger.error(f"[AI Copilot] Service not available: {e}")
        raise HTTPException(
            status_code=503,
            detail="AI Copilot service is not available. Please check OpenAI API configuration.",
        )
    except Exception as e:
        logger.error(f"[AI Copilot] Error generating sequence: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to generate sequence",
        )


@router.post("/copilot/generate-email")
async def generate_email_with_ai(
    request: CopilotGenerateEmailRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Generate a single follow-up email using AI Copilot.

    Useful for:
    - Generating individual emails without creating a full sequence
    - Regenerating specific steps in an existing sequence
    - Getting AI suggestions for manual email writing
    """
    logger.info(f"[AI Copilot] Generating email for candidate {current_candidate.id}, step {request.step_number}")

    try:
        from app.services.follow_up_ai_copilot import (
            FollowUpAICopilot,
            CopilotContext,
        )

        copilot = FollowUpAICopilot()

        # Build context
        context = CopilotContext(
            candidate_name=current_candidate.full_name or "User",
            candidate_email=current_candidate.email,
            company_name=request.context.get("company_name") if request.context else None,
            position=request.context.get("position") if request.context else None,
            industry=request.context.get("industry") if request.context else None,
            original_email_subject=request.context.get("original_subject") if request.context else None,
            original_email_body=request.context.get("original_body") if request.context else None,
        )

        # Generate email
        generated = await copilot.generate_email(
            context=context,
            step_number=request.step_number,
            strategy=request.strategy,
            tone=request.tone,
            previous_emails=request.previous_emails,
        )

        logger.info(f"[AI Copilot] Generated email with subject: {generated.subject[:50]}...")

        return {
            "success": True,
            "email": {
                "subject": generated.subject,
                "body": generated.body,
                "html": generated.html,
                "spintax_subject": generated.spintax_subject,
                "spintax_body": generated.spintax_body,
            },
            "ai_metadata": {
                "model_used": generated.model_used,
                "tokens_used": generated.tokens_used,
                "quality_score": generated.quality_score,
                "personalization_score": generated.personalization_score,
            },
        }

    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="AI Copilot service is not available. Please check OpenAI API configuration.",
        )
    except Exception as e:
        logger.error(f"[AI Copilot] Error generating email: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to generate email",
        )


@router.post("/copilot/suggest-improvements")
async def suggest_sequence_improvements(
    request: CopilotSuggestImprovementsRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Analyze a sequence and suggest AI-powered improvements.

    Returns suggestions for:
    - Timing optimization
    - Content improvements
    - Tone adjustments
    - Strategy recommendations
    """
    logger.info(f"[AI Copilot] Suggesting improvements for sequence {request.sequence_id}")

    # Get sequence
    sequence = db.query(FollowUpSequence).filter(
        FollowUpSequence.id == request.sequence_id,
        FollowUpSequence.candidate_id == current_candidate.id,
    ).first()

    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    try:
        from app.services.follow_up_ai_copilot import FollowUpAICopilot

        copilot = FollowUpAICopilot()

        # Get current steps
        current_steps = [
            {
                "step_number": step.step_number,
                "delay_days": step.delay_days,
                "strategy": step.strategy.value if hasattr(step.strategy, "value") else step.strategy,
                "tone": step.tone.value if hasattr(step.tone, "value") else step.tone,
                "subject": step.subject_template,
                "body": step.body_template,
            }
            for step in sequence.steps
        ]

        # Get suggestions
        suggestions = await copilot.suggest_improvements(
            sequence_name=sequence.name,
            current_steps=current_steps,
            performance_data={
                "reply_rate": sequence.reply_rate,
                "total_campaigns": sequence.total_campaigns,
                "successful_replies": sequence.successful_replies,
            },
        )

        logger.info(f"[AI Copilot] Generated {len(suggestions)} improvement suggestions")

        return {
            "success": True,
            "sequence_id": sequence.id,
            "sequence_name": sequence.name,
            "suggestions": [
                {
                    "type": s.suggestion_type,
                    "priority": s.priority,
                    "title": s.title,
                    "description": s.description,
                    "current_value": s.current_value,
                    "suggested_value": s.suggested_value,
                    "expected_improvement": s.expected_improvement,
                }
                for s in suggestions
            ],
        }

    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="AI Copilot service is not available.",
        )
    except Exception as e:
        logger.error(f"[AI Copilot] Error suggesting improvements: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to suggest improvements",
        )


@router.post("/copilot/generate-ab-variants")
async def generate_ab_variants(
    request: CopilotGenerateABVariantsRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Generate A/B test variants for an email.

    Takes an original email and generates alternative versions
    for A/B testing subject lines and content.
    """
    logger.info(f"[AI Copilot] Generating {request.num_variants} A/B variants")

    try:
        from app.services.follow_up_ai_copilot import FollowUpAICopilot

        copilot = FollowUpAICopilot()

        variants = await copilot.generate_ab_variants(
            original_subject=request.original_subject,
            original_body=request.original_body,
            num_variants=request.num_variants,
        )

        logger.info(f"[AI Copilot] Generated {len(variants)} variants")

        return {
            "success": True,
            "original": {
                "subject": request.original_subject,
                "body": request.original_body,
            },
            "variants": variants,
        }

    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="AI Copilot service is not available.",
        )
    except Exception as e:
        logger.error(f"[AI Copilot] Error generating variants: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to generate variants",
        )


@router.get("/copilot/status")
async def get_copilot_status(
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Check if AI Copilot is available and configured.

    Returns configuration status and available features.
    """
    try:
        from app.services.follow_up_ai_copilot import FollowUpAICopilot

        copilot = FollowUpAICopilot()
        is_available = copilot.is_available()

        return {
            "available": is_available,
            "model": copilot.model if is_available else None,
            "features": {
                "sequence_generation": is_available,
                "email_generation": is_available,
                "improvement_suggestions": is_available,
                "ab_variant_generation": is_available,
                "spintax_support": is_available,
            },
        }

    except ImportError:
        return {
            "available": False,
            "model": None,
            "features": {
                "sequence_generation": False,
                "email_generation": False,
                "improvement_suggestions": False,
                "ab_variant_generation": False,
                "spintax_support": False,
            },
            "error": "AI Copilot module not installed",
        }
    except Exception as e:
        logger.error(f"[AI Copilot] Status check failed: {e}")
        return {
            "available": False,
            "error": "AI Copilot status check failed",
        }


# ============= FOLLOW-UP CAMPAIGN MANAGEMENT ENDPOINTS =============
# These endpoints manage follow-up campaigns from the Pipeline view


class StartCampaignRequest(BaseModel):
    """Request body for starting a follow-up campaign"""
    application_id: Optional[int] = Field(None, description="Application ID (for pipeline campaigns)")
    group_campaign_recipient_id: Optional[int] = Field(None, description="Group campaign recipient ID")
    sequence_id: int = Field(..., description="Follow-up sequence to use")
    auto_mode: bool = Field(default=True, description="Enable auto-mode for automatic sending")


class PipelineFollowUpSummary(BaseModel):
    """Summary of follow-up status for a pipeline item"""
    application_id: int
    has_campaign: bool
    campaign_id: Optional[int] = None
    campaign_status: Optional[str] = None
    current_step: int = 0
    total_steps: int = 0
    emails_sent: int = 0
    next_send_date: Optional[str] = None
    last_sent_date: Optional[str] = None
    is_auto_mode: bool = False
    sequence_name: Optional[str] = None


@router.post("/campaigns/start")
async def start_follow_up_campaign(
    request: StartCampaignRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Start a new follow-up campaign for an application or group campaign recipient.

    This creates a FollowUpCampaign record and schedules the first follow-up email
    based on the selected sequence.

    Returns:
        Created campaign details including ID and scheduled send dates
    """
    from datetime import datetime, timedelta
    from app.models.follow_up import (
        FollowUpCampaign, FollowUpSequence,
        CampaignStatus as FollowUpCampaignStatus
    )
    from app.models.application import Application

    logger.info(
        f"[FOLLOW-UP] Starting campaign: app_id={request.application_id}, "
        f"sequence_id={request.sequence_id}, auto_mode={request.auto_mode}"
    )

    try:
        # Validate sequence exists and belongs to candidate
        sequence = db.query(FollowUpSequence).filter(
            FollowUpSequence.id == request.sequence_id
        ).first()

        if not sequence:
            raise HTTPException(status_code=404, detail="Follow-up sequence not found")

        if sequence.candidate_id != current_candidate.id:
            raise HTTPException(status_code=403, detail="Access denied to this sequence")

        if not sequence.steps:
            raise HTTPException(status_code=400, detail="Sequence has no steps defined")

        # Validate application if provided
        application = None
        if request.application_id:
            application = db.query(Application).filter(
                Application.id == request.application_id,
                Application.candidate_id == current_candidate.id
            ).first()

            if not application:
                raise HTTPException(status_code=404, detail="Application not found")

            # Check if campaign already exists
            existing = db.query(FollowUpCampaign).filter(
                FollowUpCampaign.application_id == request.application_id,
                FollowUpCampaign.status.in_([
                    FollowUpCampaignStatus.ACTIVE,
                    FollowUpCampaignStatus.PENDING_APPROVAL,
                    FollowUpCampaignStatus.PAUSED
                ])
            ).first()

            if existing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Active follow-up campaign already exists (ID: {existing.id})"
                )

        # Calculate first send date
        first_step = sequence.steps[0]
        next_send_date = datetime.now(timezone.utc) + timedelta(
            days=first_step.delay_days,
            hours=first_step.delay_hours or 0
        )

        # Create campaign
        campaign = FollowUpCampaign(
            sequence_id=sequence.id,
            application_id=request.application_id,
            group_campaign_recipient_id=request.group_campaign_recipient_id,
            candidate_id=current_candidate.id,
            status=FollowUpCampaignStatus.ACTIVE if request.auto_mode else FollowUpCampaignStatus.PENDING_APPROVAL,
            is_auto_mode=request.auto_mode,
            auto_mode_approved=request.auto_mode,
            auto_mode_approved_at=datetime.now(timezone.utc) if request.auto_mode else None,
            current_step=0,
            total_steps=len(sequence.steps),
            next_send_date=next_send_date,
            original_email_context={
                "application_id": request.application_id,
                "started_at": datetime.now(timezone.utc).isoformat(),
            } if application else {},
            company_context={
                "name": application.company.name if application and application.company else None,
                "recruiter_email": application.recruiter_email if application else None,
            } if application else {},
        )

        db.add(campaign)

        # Update sequence usage count
        sequence.times_used = (sequence.times_used or 0) + 1
        sequence.last_used_at = datetime.now(timezone.utc)

        db.commit()
        db.refresh(campaign)

        logger.info(
            f"✅ [FOLLOW-UP] Created campaign {campaign.id} for application {request.application_id} "
            f"(sequence: {sequence.name}, next send: {next_send_date})"
        )

        return {
            "success": True,
            "campaign_id": campaign.id,
            "status": campaign.status.value,
            "sequence_name": sequence.name,
            "total_steps": len(sequence.steps),
            "next_send_date": next_send_date.isoformat(),
            "is_auto_mode": campaign.is_auto_mode,
            "message": f"Follow-up campaign started with sequence '{sequence.name}'"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Failed to start campaign: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to start follow-up campaign"
        )


@router.get("/pipeline/{application_id}/summary", response_model=PipelineFollowUpSummary)
async def get_pipeline_follow_up_summary(
    application_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get follow-up campaign summary for a pipeline item.

    Used by the Pipeline view to show follow-up status badges on application cards.

    Returns:
        Summary of follow-up campaign status including current step, emails sent, etc.
    """
    from app.models.follow_up import FollowUpCampaign, FollowUpSequence
    from app.models.application import Application

    logger.debug(f"[FOLLOW-UP] Getting pipeline summary for application {application_id}")

    try:
        # Verify application access
        application = db.query(Application).filter(
            Application.id == application_id,
            Application.candidate_id == current_candidate.id
        ).first()

        if not application:
            raise HTTPException(status_code=404, detail="Application not found")

        # Get the most recent campaign for this application
        campaign = db.query(FollowUpCampaign).filter(
            FollowUpCampaign.application_id == application_id
        ).order_by(FollowUpCampaign.created_at.desc()).first()

        if not campaign:
            return PipelineFollowUpSummary(
                application_id=application_id,
                has_campaign=False,
            )

        # Get sequence name
        sequence = db.query(FollowUpSequence).filter(
            FollowUpSequence.id == campaign.sequence_id
        ).first()

        return PipelineFollowUpSummary(
            application_id=application_id,
            has_campaign=True,
            campaign_id=campaign.id,
            campaign_status=campaign.status.value if campaign.status else None,
            current_step=campaign.current_step or 0,
            total_steps=campaign.total_steps or 0,
            emails_sent=campaign.emails_sent or 0,
            next_send_date=campaign.next_send_date.isoformat() if campaign.next_send_date else None,
            last_sent_date=campaign.last_sent_date.isoformat() if campaign.last_sent_date else None,
            is_auto_mode=campaign.is_auto_mode or False,
            sequence_name=sequence.name if sequence else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Failed to get pipeline summary: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to get follow-up summary"
        )


@router.post("/campaigns/{campaign_id}/pause")
async def pause_follow_up_campaign(
    campaign_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Pause an active follow-up campaign.

    Paused campaigns will not send any scheduled emails until resumed.

    Returns:
        Updated campaign status
    """
    from datetime import datetime
    from app.models.follow_up import FollowUpCampaign, CampaignStatus as FollowUpCampaignStatus

    logger.info(f"[FOLLOW-UP] Pausing campaign {campaign_id}")

    try:
        campaign = db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id,
            FollowUpCampaign.candidate_id == current_candidate.id
        ).first()

        if not campaign:
            raise HTTPException(status_code=404, detail="Follow-up campaign not found")

        if campaign.status not in [FollowUpCampaignStatus.ACTIVE, FollowUpCampaignStatus.PENDING_APPROVAL]:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot pause campaign in {campaign.status.value} status"
            )

        campaign.status = FollowUpCampaignStatus.PAUSED
        campaign.paused_at = datetime.now(timezone.utc)

        db.commit()

        logger.info(f"✅ [FOLLOW-UP] Campaign {campaign_id} paused")

        return {
            "success": True,
            "campaign_id": campaign_id,
            "status": "paused",
            "message": "Follow-up campaign paused successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Failed to pause campaign: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to pause campaign"
        )


@router.post("/campaigns/{campaign_id}/resume")
async def resume_follow_up_campaign(
    campaign_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Resume a paused follow-up campaign.

    Recalculates the next send date based on current time and continues the sequence.

    Returns:
        Updated campaign status and next scheduled send
    """
    from datetime import datetime, timedelta
    from app.models.follow_up import (
        FollowUpCampaign, FollowUpSequence, FollowUpStep,
        CampaignStatus as FollowUpCampaignStatus
    )

    logger.info(f"[FOLLOW-UP] Resuming campaign {campaign_id}")

    try:
        campaign = db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id,
            FollowUpCampaign.candidate_id == current_candidate.id
        ).first()

        if not campaign:
            raise HTTPException(status_code=404, detail="Follow-up campaign not found")

        if campaign.status != FollowUpCampaignStatus.PAUSED:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot resume campaign in {campaign.status.value} status"
            )

        # Get next step delay
        sequence = db.query(FollowUpSequence).filter(
            FollowUpSequence.id == campaign.sequence_id
        ).first()

        next_step_number = (campaign.current_step or 0) + 1
        if sequence and next_step_number <= len(sequence.steps):
            step = sequence.steps[next_step_number - 1]
            # Schedule for 1 day from now minimum (don't send immediately after resume)
            next_send = datetime.now(timezone.utc) + timedelta(days=1)
        else:
            next_send = None

        campaign.status = FollowUpCampaignStatus.ACTIVE
        campaign.paused_at = None
        if next_send:
            campaign.next_send_date = next_send

        db.commit()

        logger.info(f"✅ [FOLLOW-UP] Campaign {campaign_id} resumed, next send: {next_send}")

        return {
            "success": True,
            "campaign_id": campaign_id,
            "status": "active",
            "next_send_date": next_send.isoformat() if next_send else None,
            "message": "Follow-up campaign resumed successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Failed to resume campaign: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to resume campaign"
        )


@router.post("/campaigns/{campaign_id}/cancel")
async def cancel_follow_up_campaign(
    campaign_id: int,
    reason: Optional[str] = Query(None, description="Cancellation reason"),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Cancel a follow-up campaign permanently.

    Cancelled campaigns cannot be resumed.

    Returns:
        Confirmation of cancellation
    """
    from datetime import datetime
    from app.models.follow_up import FollowUpCampaign, CampaignStatus as FollowUpCampaignStatus

    logger.info(f"[FOLLOW-UP] Cancelling campaign {campaign_id}")

    try:
        campaign = db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id,
            FollowUpCampaign.candidate_id == current_candidate.id
        ).first()

        if not campaign:
            raise HTTPException(status_code=404, detail="Follow-up campaign not found")

        if campaign.status in [FollowUpCampaignStatus.COMPLETED, FollowUpCampaignStatus.CANCELLED]:
            raise HTTPException(
                status_code=400,
                detail=f"Campaign is already {campaign.status.value}"
            )

        campaign.status = FollowUpCampaignStatus.CANCELLED
        campaign.cancellation_reason = reason or "Cancelled by user"
        campaign.cancelled_at = datetime.now(timezone.utc)
        campaign.next_send_date = None

        db.commit()

        logger.info(f"✅ [FOLLOW-UP] Campaign {campaign_id} cancelled: {reason}")

        return {
            "success": True,
            "campaign_id": campaign_id,
            "status": "cancelled",
            "message": "Follow-up campaign cancelled"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Failed to cancel campaign: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to cancel campaign"
        )


@router.get("/campaigns")
async def list_follow_up_campaigns(
    status: Optional[str] = Query(None, description="Filter by status"),
    application_id: Optional[int] = Query(None, description="Filter by application"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    List all follow-up campaigns for the current candidate.

    Supports filtering by status and application ID.

    Returns:
        Paginated list of follow-up campaigns
    """
    from sqlalchemy import func
    from app.models.follow_up import (
        FollowUpCampaign, FollowUpSequence,
        CampaignStatus as FollowUpCampaignStatus
    )

    logger.debug(f"[FOLLOW-UP] Listing campaigns for candidate {current_candidate.id}")

    try:
        query = db.query(FollowUpCampaign).filter(
            FollowUpCampaign.candidate_id == current_candidate.id
        )

        if status:
            try:
                status_enum = FollowUpCampaignStatus(status)
                query = query.filter(FollowUpCampaign.status == status_enum)
            except ValueError:
                pass

        if application_id:
            query = query.filter(FollowUpCampaign.application_id == application_id)

        # Get total count
        total = query.count()

        # Get paginated results
        campaigns = query.order_by(
            FollowUpCampaign.created_at.desc()
        ).offset((page - 1) * page_size).limit(page_size).all()

        # Batch-fetch all referenced sequences in one query (avoids N+1)
        sequence_ids = {c.sequence_id for c in campaigns if c.sequence_id}
        sequences_map = {}
        if sequence_ids:
            sequences = db.query(FollowUpSequence).filter(
                FollowUpSequence.id.in_(sequence_ids)
            ).all()
            sequences_map = {s.id: s for s in sequences}

        # Build response
        results = []
        for campaign in campaigns:
            sequence = sequences_map.get(campaign.sequence_id)

            results.append({
                "id": campaign.id,
                "application_id": campaign.application_id,
                "group_campaign_id": campaign.group_campaign_id,
                "sequence_id": campaign.sequence_id,
                "sequence_name": sequence.name if sequence else None,
                "status": campaign.status.value if campaign.status else None,
                "current_step": campaign.current_step,
                "total_steps": campaign.total_steps,
                "emails_sent": campaign.emails_sent,
                "next_send_date": campaign.next_send_date.isoformat() if campaign.next_send_date else None,
                "last_sent_date": campaign.last_sent_date.isoformat() if campaign.last_sent_date else None,
                "is_auto_mode": campaign.is_auto_mode,
                "created_at": campaign.created_at.isoformat() if campaign.created_at else None,
            })

        return {
            "campaigns": results,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }

    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Failed to list campaigns: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to list campaigns"
        )


@router.get("/campaigns/{campaign_id}")
async def get_follow_up_campaign(
    campaign_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get detailed information about a specific follow-up campaign.

    Returns:
        Full campaign details including emails sent and scheduled
    """
    from app.models.follow_up import FollowUpCampaign, FollowUpSequence, FollowUpEmail

    logger.debug(f"[FOLLOW-UP] Getting campaign {campaign_id}")

    try:
        campaign = db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id,
            FollowUpCampaign.candidate_id == current_candidate.id
        ).first()

        if not campaign:
            raise HTTPException(status_code=404, detail="Follow-up campaign not found")

        sequence = db.query(FollowUpSequence).filter(
            FollowUpSequence.id == campaign.sequence_id
        ).first()

        # Get emails for this campaign
        emails = db.query(FollowUpEmail).filter(
            FollowUpEmail.campaign_id == campaign_id
        ).order_by(FollowUpEmail.step_number).all()

        return {
            "id": campaign.id,
            "application_id": campaign.application_id,
            "group_campaign_id": campaign.group_campaign_id,
            "group_campaign_recipient_id": campaign.group_campaign_recipient_id,
            "sequence": {
                "id": sequence.id,
                "name": sequence.name,
                "total_steps": len(sequence.steps) if sequence.steps else 0,
            } if sequence else None,
            "status": campaign.status.value if campaign.status else None,
            "current_step": campaign.current_step,
            "total_steps": campaign.total_steps,
            "emails_sent": campaign.emails_sent,
            "next_send_date": campaign.next_send_date.isoformat() if campaign.next_send_date else None,
            "last_sent_date": campaign.last_sent_date.isoformat() if campaign.last_sent_date else None,
            "is_auto_mode": campaign.is_auto_mode,
            "auto_mode_approved": campaign.auto_mode_approved,
            "created_at": campaign.created_at.isoformat() if campaign.created_at else None,
            "original_email_context": campaign.original_email_context,
            "company_context": campaign.company_context,
            "emails": [
                {
                    "id": email.id,
                    "step_number": email.step_number,
                    "status": email.status.value if email.status else None,
                    "subject": email.subject,
                    "scheduled_date": email.scheduled_date.isoformat() if email.scheduled_date else None,
                    "sent_at": email.sent_at.isoformat() if email.sent_at else None,
                }
                for email in emails
            ]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Failed to get campaign: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to get campaign"
        )
