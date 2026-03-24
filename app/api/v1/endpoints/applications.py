"""Applications API Endpoints - Refactored with Repository Pattern"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime, timezone
from email_validator import validate_email, EmailNotValidError

from app.api.dependencies import get_db, get_current_candidate
from app.core.logger import api_logger as logger
from app.models.application import ApplicationStatusEnum, Application
from app.models.candidate import Candidate
from app.models.company import Company
from app.schemas.application import ApplicationCreate, ApplicationUpdate, ApplicationResponse
from app.schemas.common import PaginatedResponse, StatusResponse

# Phase 1: Repository Pattern for automatic caching
from app.repositories.application import ApplicationRepository
from app.repositories.company import CompanyRepository

router = APIRouter()

# Debug logging for all application operations
def log_operation(operation: str, details: dict = None):
    """Helper to log application operations"""
    logger.info(f"[Applications] {operation}")
    if details:
        logger.debug(f"[Applications] Details: {details}")


# IMPORTANT: Static routes MUST come before parameterized routes to avoid shadowing
@router.get("/stats/summary")
def get_application_stats(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get application statistics (Phase 1: Using Repository Pattern)"""
    log_operation("get_application_stats", {"current_user": current_candidate.id})

    # Always use authenticated user's ID (prevents IDOR)
    effective_candidate_id = current_candidate.id
    logger.debug(f"[Applications] Using candidate_id: {effective_candidate_id}")

    # Phase 1: Use repository for stats (will be cached)
    repo = ApplicationRepository(db)
    stats = repo.get_stats_by_candidate(effective_candidate_id)

    logger.info(f"[Applications] Stats from repository (cached): {stats}")

    return stats


@router.get("/", response_model=PaginatedResponse[ApplicationResponse])
def list_applications(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """List all non-deleted applications (Phase 1: Using Repository Pattern)"""
    log_operation("list_applications", {"skip": skip, "limit": limit, "status": status, "current_user": current_candidate.id})

    # Always use authenticated user's ID (prevents IDOR)
    effective_candidate_id = current_candidate.id
    logger.debug(f"[Applications] Listing for candidate_id: {effective_candidate_id}")

    # Phase 1: Use repository
    repo = ApplicationRepository(db)

    # Validate status if provided
    if status:
        valid_statuses = [s.value for s in ApplicationStatusEnum]
        if status not in valid_statuses:
            logger.warning(f"[Applications] Invalid status filter: {status}. Valid statuses: {valid_statuses}")
            raise HTTPException(
                status_code=422,
                detail=f"Invalid status '{status}'. Valid values: {', '.join(valid_statuses)}"
            )
        logger.debug(f"[Applications] Filtering by status: {status}")

    # Get applications using repository (with automatic caching and eager loading)
    filters = {"candidate_id": effective_candidate_id}
    if status:
        filters["status"] = status

    applications = repo.get_all(
        filters=filters,
        skip=skip,
        limit=limit
    )

    # Get total count
    total = repo.count(filters=filters)

    logger.info(f"[Applications] Found {len(applications)} applications (total: {total}) - Repository pattern")

    # Add company name to each application (no extra queries due to eager loading)
    for app in applications:
        if app.company:
            app.company_name = app.company.name

    return {
        "items": applications,
        "total": total,
        "page": (skip // limit) + 1 if limit > 0 else 1,
        "page_size": limit
    }


@router.post("/", response_model=ApplicationResponse, status_code=201)
def create_application(
    application: ApplicationCreate,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Create new application with email validation"""
    log_operation("create_application", {
        "recruiter_email": application.recruiter_email,
        "company_name": application.company_name,
        "position_title": application.position_title,
        "current_user": current_candidate.id
    })

    # Validate email format
    try:
        valid_email = validate_email(application.recruiter_email)
        application.recruiter_email = valid_email.normalized
        logger.debug(f"[Applications] Email validated: {application.recruiter_email}")
    except EmailNotValidError as e:
        logger.warning(f"[Applications] Invalid email: {application.recruiter_email} - {str(e)}")
        raise HTTPException(status_code=422, detail="Invalid recruiter email")

    # Use the authenticated candidate
    candidate = current_candidate
    logger.debug(f"[Applications] Using authenticated candidate: {candidate.id} ({candidate.email})")

    # Check if company exists (exclude soft-deleted)
    company = db.query(Company).filter(
        Company.name == application.company_name,
        Company.deleted_at.is_(None)
    ).first()

    if not company:
        # Create new company
        company = Company(name=application.company_name)
        try:
            db.add(company)
            db.commit()
            db.refresh(company)
            logger.info(f"[Applications] Created new company: {company.name} (ID: {company.id})")
        except Exception as e:
            db.rollback()
            logger.error(f"[Applications] Failed to create company {application.company_name}: {e}")
            raise HTTPException(status_code=500, detail="Failed to create company")

    # Check for duplicate application
    existing = db.query(Application).filter(
        Application.candidate_id == candidate.id,
        Application.recruiter_email == application.recruiter_email,
        Application.position_title == application.position_title,
        Application.deleted_at.is_(None)
    ).first()

    if existing:
        raise HTTPException(
            status_code=409,
            detail="Application already exists for this position and recruiter"
        )

    # Create application
    db_app = Application(
        candidate_id=candidate.id,
        company_id=company.id,
        recruiter_name=application.recruiter_name,
        recruiter_email=application.recruiter_email,
        position_title=application.position_title,
        notes=application.notes,
        status=ApplicationStatusEnum.DRAFT
    )

    try:
        db.add(db_app)
        db.commit()
        db.refresh(db_app)
        logger.info(f"[Applications] Created application {db_app.id} for {application.recruiter_email} at {company.name}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Applications] Failed to create application for {application.recruiter_email}: {e}")
        raise HTTPException(status_code=500, detail="Failed to create application")

    # Add company name
    db_app.company_name = company.name

    return db_app


@router.get("/{application_id}", response_model=ApplicationResponse)
def get_application(
    application_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get non-deleted application by ID"""
    log_operation("get_application", {"application_id": application_id, "current_user": current_candidate.id})

    app = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,  # Security: only own applications
        Application.deleted_at.is_(None)
    ).first()

    if not app:
        logger.warning(f"[Applications] Application {application_id} not found for user {current_candidate.id}")
        raise HTTPException(status_code=404, detail="Application not found")

    logger.debug(f"[Applications] Retrieved application {application_id}: {app.recruiter_email}")

    if app.company:
        app.company_name = app.company.name

    return app


@router.patch("/{application_id}", response_model=ApplicationResponse)
def update_application(
    application_id: int,
    updates: ApplicationUpdate,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Update application with auto-timestamps on status change"""
    log_operation("update_application", {"application_id": application_id, "current_user": current_candidate.id})

    app = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,  # Security: only own applications
        Application.deleted_at.is_(None)
    ).first()

    if not app:
        logger.warning(f"[Applications] Application {application_id} not found for update by user {current_candidate.id}")
        raise HTTPException(status_code=404, detail="Application not found")

    update_data = updates.model_dump(exclude_unset=True)
    logger.debug(f"[Applications] Updating application {application_id} with: {update_data}")

    # Auto-set timestamps based on status changes
    if "status" in update_data:
        new_status = update_data["status"]
        old_status = app.status

        # Set sent_at when status changes from draft to anything else
        if old_status == ApplicationStatusEnum.DRAFT and new_status != ApplicationStatusEnum.DRAFT:
            if not app.sent_at:
                update_data["sent_at"] = datetime.now(timezone.utc)

        # Set opened_at when status becomes opened
        if new_status == ApplicationStatusEnum.OPENED and not app.opened_at:
            update_data["opened_at"] = datetime.now(timezone.utc)

        # Set replied_at when status becomes replied or later stages
        if new_status in [ApplicationStatusEnum.REPLIED, ApplicationStatusEnum.INTERVIEW,
                          ApplicationStatusEnum.ACCEPTED, ApplicationStatusEnum.REJECTED]:
            if not app.replied_at:
                update_data["replied_at"] = datetime.now(timezone.utc)

    # Update fields
    for field, value in update_data.items():
        setattr(app, field, value)

    try:
        db.commit()
        db.refresh(app)
        logger.info(f"[Applications] Updated application {app.id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Applications] Failed to update application {app.id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update application")

    if app.company:
        app.company_name = app.company.name

    return app


@router.delete("/{application_id}", response_model=StatusResponse)
def delete_application(
    application_id: int,
    hard_delete: bool = Query(False, description="Permanently delete (default: soft delete)"),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Soft delete application (or hard delete if specified)"""
    log_operation("delete_application", {"application_id": application_id, "hard_delete": hard_delete, "current_user": current_candidate.id})

    app = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,  # Security: only own applications
        Application.deleted_at.is_(None)
    ).first()

    if not app:
        logger.warning(f"[Applications] Application {application_id} not found for deletion by user {current_candidate.id}")
        raise HTTPException(status_code=404, detail="Application not found")

    if hard_delete:
        # Permanent deletion
        logger.info(f"[Applications] Hard deleting application {application_id}")
        try:
            db.delete(app)
            db.commit()
            logger.info(f"[Applications] Application {application_id} permanently deleted")
            return {"success": True, "message": "Application permanently deleted"}
        except Exception as e:
            db.rollback()
            logger.error(f"[Applications] Failed to hard delete application {application_id}: {e}")
            raise HTTPException(status_code=500, detail="Failed to delete application")
    else:
        # Soft delete
        logger.info(f"[Applications] Soft deleting application {application_id}")
        app.deleted_at = datetime.now(timezone.utc)
        try:
            db.commit()
            logger.info(f"[Applications] Application {application_id} soft deleted")
            return {"success": True, "message": "Application soft deleted"}
        except Exception as e:
            db.rollback()
            logger.error(f"[Applications] Failed to soft delete application {application_id}: {e}")
            raise HTTPException(status_code=500, detail="Failed to delete application")


# ============= STATUS UPDATE & HISTORY ENDPOINTS =============

from app.models.application_history import ApplicationHistory, ApplicationNote
from pydantic import BaseModel as PydanticBase

class StatusUpdateRequest(PydanticBase):
    status: ApplicationStatusEnum
    note: str = None


@router.patch("/{application_id}/status")
def update_application_status(
    application_id: int,
    status_update: StatusUpdateRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Update application status and record history"""
    log_operation("update_application_status", {"application_id": application_id, "new_status": status_update.status.value, "current_user": current_candidate.id})

    app = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,
        Application.deleted_at.is_(None)
    ).first()

    if not app:
        logger.warning(f"[Applications] Application {application_id} not found for status update")
        raise HTTPException(status_code=404, detail="Application not found")

    old_status = app.status.value

    # Update status
    app.status = status_update.status

    # Update timestamps
    if status_update.status == ApplicationStatusEnum.SENT and not app.sent_at:
        app.sent_at = datetime.now(timezone.utc)
    elif status_update.status in [ApplicationStatusEnum.RESPONDED, ApplicationStatusEnum.REPLIED] and not app.replied_at:
        app.replied_at = datetime.now(timezone.utc)
    elif status_update.status == ApplicationStatusEnum.OPENED and not app.opened_at:
        app.opened_at = datetime.now(timezone.utc)

    # Create history record
    history = ApplicationHistory(
        application_id=app.id,
        changed_by=app.candidate_id,  # Use candidate ID for now
        field_name="status",
        old_value=old_status,
        new_value=status_update.status.value,
        note=status_update.note,
        change_type="status_change"
    )
    db.add(history)

    try:
        db.commit()
        db.refresh(app)
        logger.info(f"[Applications] Status updated for {application_id}: {old_status} -> {status_update.status.value}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Applications] Failed to update status for application {application_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update application status")

    return {
        "id": app.id,
        "status": app.status.value,
        "old_status": old_status,
        "message": f"Status updated from {old_status} to {status_update.status.value}",
        "updated_at": app.updated_at
    }


@router.get("/{application_id}/history")
def get_application_history(
    application_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get application change history"""
    log_operation("get_application_history", {"application_id": application_id, "current_user": current_candidate.id})

    app = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,  # Security: only own applications
        Application.deleted_at.is_(None)
    ).first()

    if not app:
        logger.warning(f"[Applications] Application {application_id} not found for history by user {current_candidate.id}")
        raise HTTPException(status_code=404, detail="Application not found")

    history = db.query(ApplicationHistory).filter(
        ApplicationHistory.application_id == application_id
    ).order_by(ApplicationHistory.created_at.desc()).all()

    logger.debug(f"[Applications] Retrieved {len(history)} history entries for application {application_id}")

    return [
        {
            "id": h.id,
            "field_name": h.field_name,
            "old_value": h.old_value,
            "new_value": h.new_value,
            "note": h.note,
            "change_type": h.change_type,
            "created_at": h.created_at,
        }
        for h in history
    ]


class NoteCreate(PydanticBase):
    content: str
    note_type: str = "general"


@router.post("/{application_id}/notes")
def add_application_note(
    application_id: int,
    note_data: NoteCreate,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Add a note to an application"""
    log_operation("add_application_note", {"application_id": application_id, "note_type": note_data.note_type, "current_user": current_candidate.id})

    app = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,  # Security: only own applications
        Application.deleted_at.is_(None)
    ).first()

    if not app:
        logger.warning(f"[Applications] Application {application_id} not found for note by user {current_candidate.id}")
        raise HTTPException(status_code=404, detail="Application not found")

    note = ApplicationNote(
        application_id=application_id,
        candidate_id=current_candidate.id,
        content=note_data.content,
        note_type=note_data.note_type
    )
    db.add(note)

    # Also add to history
    history = ApplicationHistory(
        application_id=application_id,
        changed_by=current_candidate.id,
        field_name="notes",
        new_value=note_data.content[:100] + "..." if len(note_data.content) > 100 else note_data.content,
        change_type="note_added"
    )
    db.add(history)

    try:
        db.commit()
        db.refresh(note)
        logger.info(f"[Applications] Note added to application {application_id}: type={note_data.note_type}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Applications] Failed to add note to application {application_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to add note")

    return {
        "id": note.id,
        "content": note.content,
        "note_type": note.note_type,
        "created_at": note.created_at
    }


@router.get("/{application_id}/notes")
def get_application_notes(
    application_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get all notes for an application"""
    log_operation("get_application_notes", {"application_id": application_id, "current_user": current_candidate.id})

    app = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,  # Security: only own applications
        Application.deleted_at.is_(None)
    ).first()

    if not app:
        logger.warning(f"[Applications] Application {application_id} not found for notes by user {current_candidate.id}")
        raise HTTPException(status_code=404, detail="Application not found")

    notes = db.query(ApplicationNote).filter(
        ApplicationNote.application_id == application_id
    ).order_by(ApplicationNote.created_at.desc()).all()

    logger.debug(f"[Applications] Retrieved {len(notes)} notes for application {application_id}")

    return [
        {
            "id": n.id,
            "content": n.content,
            "note_type": n.note_type,
            "created_at": n.created_at,
            "updated_at": n.updated_at
        }
        for n in notes
    ]


# ============= EMAIL SENDING ENDPOINTS =============

from app.services.email_service import EmailService, EmailServiceError
from app.models.resume import ResumeVersion
from app.models.email_template import EmailTemplate


class SendEmailRequest(PydanticBase):
    resume_version_id: Optional[int] = None
    template_id: Optional[int] = None
    force_send: bool = False


@router.post("/{application_id}/send")
def send_application_email(
    application_id: int,
    request: SendEmailRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Send application email immediately"""

    # Get application (scoped to current user)
    app = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,
        Application.deleted_at.is_(None)
    ).first()

    if not app:
        raise HTTPException(status_code=404, detail="Application not found")

    # Check if already sent (unless force_send)
    if not request.force_send and app.status not in [ApplicationStatusEnum.DRAFT, ApplicationStatusEnum.WAITING]:
        # Check if there's an actual EmailLog proving delivery -- apps with fake SENT
        # status (created before real SMTP wiring) have no EmailLog record
        from app.models.email_log import EmailLog, EmailStatusEnum as ELS
        actual_send = db.query(EmailLog).filter(
            EmailLog.application_id == app.id,
            EmailLog.status == ELS.SENT,
        ).first()
        if actual_send:
            raise HTTPException(
                status_code=400,
                detail=f"Application already sent (status: {app.status.value}). Use force_send=true to resend."
            )
        # No EmailLog = never actually sent via SMTP. Allow sending.

    # Use authenticated candidate (ownership already verified by query filter)
    candidate = current_candidate

    # Verify candidate has email credentials
    if not candidate.email_account or not candidate.email_password:
        raise HTTPException(
            status_code=400,
            detail="Candidate email credentials not configured"
        )

    # Check email warming limits
    logger.info(f"📧 Sending email for application {application_id}")
    logger.debug(f"   To: {app.recruiter_email}, Company ID: {app.company_id}")

    from app.services.email_warming_service import EmailWarmingService
    can_send_warming, warming_reason, warming_remaining = EmailWarmingService.can_send_email(
        db, candidate.id
    )

    if not can_send_warming:
        logger.warning(f"❌ Email BLOCKED by warming limits: {warming_reason}")
        raise HTTPException(
            status_code=429,
            detail=f"Email warming limit: {warming_reason}"
        )

    logger.info(f"✓ Warming check passed - {warming_remaining} emails remaining")

    # Check rate limits
    from app.services.rate_limiting_service import RateLimitingService
    can_send_rate, rate_reason, rate_quota = RateLimitingService.can_send_email(
        db, candidate.id
    )

    if not can_send_rate:
        logger.warning(f"❌ Email BLOCKED by rate limits: {rate_reason}")
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit: {rate_reason}"
        )

    logger.info(f"✓ Rate limit check passed - Hourly: {rate_quota.get('hourly_remaining')}, Daily: {rate_quota.get('daily_remaining')}")

    # Get resume version if specified (fall back to the one already on the application)
    resume = None
    resume_vid = request.resume_version_id or app.resume_version_id
    if resume_vid:
        resume = db.query(ResumeVersion).filter(
            ResumeVersion.id == resume_vid,
            ResumeVersion.candidate_id == candidate.id
        ).first()
        if not resume:
            raise HTTPException(status_code=404, detail="Resume version not found")

    # Get email template if specified
    template = None
    if request.template_id:
        template = db.query(EmailTemplate).filter(
            EmailTemplate.id == request.template_id,
            EmailTemplate.candidate_id == candidate.id
        ).first()
        if not template:
            raise HTTPException(status_code=404, detail="Email template not found")

    # Send email
    email_service = EmailService(db)
    try:
        email_log = email_service.send_application_email(
            application=app,
            candidate=candidate,
            resume_version=resume,
            email_template=template
        )

        # Record email sent for warming and rate limiting
        from app.models.email_log import EmailStatusEnum
        success = email_log.status == EmailStatusEnum.SENT
        bounced = email_log.status == EmailStatusEnum.BOUNCED

        logger.info(f"✅ Email sent successfully! Status: {email_log.status.value}, Log ID: {email_log.id}")

        EmailWarmingService.record_email_sent(db, candidate.id, success=success, bounced=bounced)
        RateLimitingService.record_email_sent(db, candidate.id)

        logger.info(f"📊 Recorded email metrics - Warming remaining: {warming_remaining}")

        return {
            "success": True,
            "message": "Email sent successfully",
            "email_log_id": email_log.id,
            "status": email_log.status.value,
            "sent_at": email_log.sent_at,
            "to_email": email_log.to_email,
            "subject": email_log.subject,
            "warming_remaining": warming_remaining,
            "rate_quota": rate_quota
        }

    except EmailServiceError as e:
        logger.error(f"Failed to send email: {e}")
        raise HTTPException(status_code=500, detail="Failed to send email")
    except Exception as e:
        logger.error(f"Unexpected error sending email: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error")


class BulkSendRequest(PydanticBase):
    application_ids: List[int]
    resume_version_id: Optional[int] = None
    template_id: Optional[int] = None
    delay_seconds: int = 60  # Delay between emails (default: 1 minute)


@router.post("/send-bulk")
async def send_bulk_emails(
    request: BulkSendRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Send multiple application emails with delays between them (async)"""

    if not request.application_ids:
        raise HTTPException(status_code=400, detail="No application IDs provided")

    if len(request.application_ids) > 100:
        raise HTTPException(
            status_code=400,
            detail="Maximum 100 applications per bulk send. Use scheduling for larger batches."
        )

    results = {
        "total": len(request.application_ids),
        "sent": [],
        "failed": [],
        "skipped": []
    }

    email_service = EmailService(db)

    for idx, app_id in enumerate(request.application_ids):
        try:
            # Get application (scoped to current user)
            app = db.query(Application).filter(
                Application.id == app_id,
                Application.candidate_id == current_candidate.id,
                Application.deleted_at.is_(None)
            ).first()

            if not app:
                results["failed"].append({
                    "application_id": app_id,
                    "error": "Application not found"
                })
                continue

            # Skip if already sent
            if app.status not in [ApplicationStatusEnum.DRAFT, ApplicationStatusEnum.WAITING]:
                results["skipped"].append({
                    "application_id": app_id,
                    "reason": f"Already sent (status: {app.status.value})"
                })
                continue

            # Use authenticated candidate (ownership already verified by query filter)
            candidate = current_candidate

            # Check warming and rate limits
            from app.services.email_warming_service import EmailWarmingService
            from app.services.rate_limiting_service import RateLimitingService

            can_send_warming, warming_reason, _ = EmailWarmingService.can_send_email(db, candidate.id)
            can_send_rate, rate_reason, _ = RateLimitingService.can_send_email(db, candidate.id)

            if not can_send_warming:
                results["skipped"].append({
                    "application_id": app_id,
                    "reason": f"Warming limit: {warming_reason}"
                })
                continue

            if not can_send_rate:
                results["skipped"].append({
                    "application_id": app_id,
                    "reason": f"Rate limit: {rate_reason}"
                })
                continue

            # Get resume if specified (scoped to current user)
            resume = None
            if request.resume_version_id:
                resume = db.query(ResumeVersion).filter(
                    ResumeVersion.id == request.resume_version_id,
                    ResumeVersion.candidate_id == candidate.id
                ).first()

            # Get template if specified (scoped to current user)
            template = None
            if request.template_id:
                template = db.query(EmailTemplate).filter(
                    EmailTemplate.id == request.template_id,
                    EmailTemplate.candidate_id == candidate.id
                ).first()

            # Send email
            email_log = email_service.send_application_email(
                application=app,
                candidate=candidate,
                resume_version=resume,
                email_template=template
            )

            # Record for warming and rate limiting
            from app.models.email_log import EmailStatusEnum
            success = email_log.status == EmailStatusEnum.SENT
            bounced = email_log.status == EmailStatusEnum.BOUNCED

            EmailWarmingService.record_email_sent(db, candidate.id, success=success, bounced=bounced)
            RateLimitingService.record_email_sent(db, candidate.id)

            results["sent"].append({
                "application_id": app_id,
                "email_log_id": email_log.id,
                "to_email": app.recruiter_email,
                "status": email_log.status.value
            })

            # Delay before next email (except for last one) - using async sleep
            if idx < len(request.application_ids) - 1 and request.delay_seconds > 0:
                import asyncio
                await asyncio.sleep(request.delay_seconds)

        except EmailServiceError as e:
            results["failed"].append({
                "application_id": app_id,
                "error": str(e)
            })
        except Exception as e:
            results["failed"].append({
                "application_id": app_id,
                "error": f"Unexpected error: {str(e)}"
            })

    return {
        "success": True,
        "summary": {
            "total": results["total"],
            "sent": len(results["sent"]),
            "failed": len(results["failed"]),
            "skipped": len(results["skipped"])
        },
        "results": results
    }


# ============= CSV/EXCEL IMPORT ENDPOINT =============

from fastapi import UploadFile, File
import pandas as pd
import io


@router.post("/import-csv")
async def import_recruiters_csv(
    file: UploadFile = File(...),
    auto_send: bool = False,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Import recruiters from CSV/Excel file

    Expected columns:
    - recruiter_email (required)
    - company_name (required)
    - recruiter_name
    - position_title
    - position_country
    - position_language
    - recruiter_country
    - recruiter_language
    - notes
    """
    log_operation("import_recruiters_csv", {"filename": file.filename, "auto_send": auto_send, "current_user": current_candidate.id})

    # Validate file type
    if not file.filename.endswith(('.csv', '.xlsx', '.xls')):
        logger.warning(f"[Applications] Invalid file type: {file.filename}")
        raise HTTPException(
            status_code=400,
            detail="File must be CSV or Excel (.csv, .xlsx, .xls)"
        )

    # Use authenticated candidate
    candidate = current_candidate
    logger.info(f"[Applications] Importing CSV for candidate {candidate.id}")

    # Read file
    try:
        contents = await file.read()

        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents))
        else:
            df = pd.read_excel(io.BytesIO(contents))

    except Exception as e:
        logger.error(f"Failed to read file: {e}")
        raise HTTPException(status_code=400, detail="Failed to read file")

    # Normalize column names: strip whitespace, lowercase, replace spaces with underscores
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Map common column name variations to expected names
    column_aliases = {
        'recruiter_email': ['recruiter_email', 'email', 'email_address', 'recruiter_email_address', 'contact_email'],
        'company_name': ['company_name', 'company', 'organization', 'org', 'employer'],
        'recruiter_name': ['recruiter_name', 'name', 'contact_name', 'full_name', 'contact'],
        'position_title': ['position_title', 'position', 'title', 'job_title', 'role', 'job_role'],
        'position_country': ['position_country', 'country', 'location', 'job_country', 'job_location'],
        'position_language': ['position_language', 'language', 'job_language'],
        'recruiter_country': ['recruiter_country', 'contact_country'],
        'recruiter_language': ['recruiter_language', 'contact_language'],
        'notes': ['notes', 'note', 'comments', 'comment', 'remarks'],
    }
    for target, aliases in column_aliases.items():
        if target not in df.columns:
            for alias in aliases:
                if alias in df.columns:
                    df.rename(columns={alias: target}, inplace=True)
                    break

    # Validate required columns
    required = ['recruiter_email', 'company_name']
    missing = [col for col in required if col not in df.columns]

    if missing:
        available = ', '.join(df.columns.tolist())
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {', '.join(missing)}. Required: {', '.join(required)}. Found columns: {available}"
        )

    # Process rows
    results = {
        "total_rows": len(df),
        "success": [],
        "failed": [],
        "duplicates": []
    }

    for idx, row in df.iterrows():
        try:
            # Validate email
            recruiter_email = str(row['recruiter_email']).strip()

            if pd.isna(row['recruiter_email']) or not recruiter_email:
                results["failed"].append({
                    "row": idx + 2,  # +2 because: 0-indexed + header row
                    "email": "",
                    "error": "Empty email address"
                })
                continue

            try:
                valid_email = validate_email(recruiter_email)
                recruiter_email = valid_email.normalized
            except EmailNotValidError as e:
                results["failed"].append({
                    "row": idx + 2,
                    "email": recruiter_email,
                    "error": f"Invalid email: {str(e)}"
                })
                continue

            # Get company name
            company_name = str(row['company_name']).strip()

            if pd.isna(row['company_name']) or not company_name:
                results["failed"].append({
                    "row": idx + 2,
                    "email": recruiter_email,
                    "error": "Empty company name"
                })
                continue

            # Check for duplicate application
            position_title = str(row.get('position_title', '')).strip() if not pd.isna(row.get('position_title')) else None

            existing = db.query(Application).filter(
                Application.candidate_id == candidate.id,
                Application.recruiter_email == recruiter_email,
                Application.deleted_at.is_(None)
            )

            if position_title:
                existing = existing.filter(Application.position_title == position_title)

            existing = existing.first()

            if existing:
                results["duplicates"].append({
                    "row": idx + 2,
                    "email": recruiter_email,
                    "company": company_name,
                    "existing_id": existing.id
                })
                continue

            # Create or get company
            company = db.query(Company).filter(
                Company.name == company_name,
                Company.deleted_at.is_(None)
            ).first()

            if not company:
                company = Company(name=company_name)
                db.add(company)
                db.flush()

            # Create application
            app = Application(
                candidate_id=candidate.id,
                company_id=company.id,
                recruiter_email=recruiter_email,
                recruiter_name=str(row.get('recruiter_name', '')).strip() if not pd.isna(row.get('recruiter_name')) else None,
                position_title=position_title,
                position_country=str(row.get('position_country', '')).strip() if not pd.isna(row.get('position_country')) else None,
                position_language=str(row.get('position_language', '')).strip() if not pd.isna(row.get('position_language')) else None,
                recruiter_country=str(row.get('recruiter_country', '')).strip() if not pd.isna(row.get('recruiter_country')) else None,
                recruiter_language=str(row.get('recruiter_language', '')).strip() if not pd.isna(row.get('recruiter_language')) else None,
                notes=str(row.get('notes', '')).strip() if not pd.isna(row.get('notes')) else None,
                status=ApplicationStatusEnum.DRAFT
            )
            db.add(app)
            db.flush()

            results["success"].append({
                "row": idx + 2,
                "application_id": app.id,
                "email": recruiter_email,
                "company": company_name,
                "position": position_title or "Not specified"
            })

        except Exception as e:
            results["failed"].append({
                "row": idx + 2,
                "email": row.get('recruiter_email', ''),
                "error": f"Unexpected error: {str(e)}"
            })

    try:
        db.commit()
        logger.info(f"[Applications] CSV import completed: {len(results['success'])} created, {len(results['failed'])} failed")
    except Exception as e:
        db.rollback()
        logger.error(f"[Applications] Failed to commit CSV import: {e}")
        raise HTTPException(status_code=500, detail="Failed to save imported applications")

    return {
        "success": True,
        "message": f"Import completed: {len(results['success'])} created, {len(results['failed'])} failed, {len(results['duplicates'])} duplicates",
        "summary": {
            "total_rows": results["total_rows"],
            "success_count": len(results["success"]),
            "failed_count": len(results["failed"]),
            "duplicate_count": len(results["duplicates"])
        },
        "results": results
    }
