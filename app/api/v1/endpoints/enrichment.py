"""
Enrichment API Endpoints

Provides enrichment execution and management:
- Execute enrichment jobs
- Check enrichment status
- Retrieve enrichment results
- Persist enrichment data to recipient records

**CRITICAL FIX**: Enrichment results are now auto-persisted to Recipient.custom_fields
instead of only being stored in browser state. This ensures data survives page refresh.
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from uuid import uuid4
from datetime import datetime, timezone
import asyncio
import logging

from app.core.database_async import get_async_db
from app.core.database import SessionLocal
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.enrichment_job import EnrichmentJob, EnrichmentJobStatus
from app.models.recipient import Recipient
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.repositories.recipient import AsyncRecipientRepository

router = APIRouter()
logger = logging.getLogger(__name__)


# ==================== SCHEMAS ====================


class EnrichmentConfig(BaseModel):
    """Configuration for enrichment features"""

    email_verification: bool = True
    phone_discovery: bool = False
    linkedin_profile: bool = False
    job_title_validation: bool = True
    company_info: bool = True
    social_profiles: bool = False
    use_cache: bool = True
    fraud_detection: bool = False


class EnrichmentExecuteRequest(BaseModel):
    """Request schema for executing enrichment"""

    recipient_ids: List[int] = Field(..., min_items=1, max_items=1000)
    config: EnrichmentConfig = Field(default_factory=EnrichmentConfig)
    async_mode: bool = True
    depth: str = Field("standard", pattern="^(quick|standard|deep)$")


class EnrichmentExecuteResponse(BaseModel):
    """Response for enrichment execution"""

    success: bool
    job_id: str
    status: str
    total_recipients: int
    progress_url: Optional[str] = None
    results_url: Optional[str] = None
    message: str


class EnrichmentStatusResponse(BaseModel):
    """Response for enrichment status check"""

    job_id: str
    status: str
    progress: Dict[str, Any]
    results: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    created_at: str
    completed_at: Optional[str] = None


class RecipientEnrichmentUpdate(BaseModel):
    """Single recipient enrichment data to persist"""
    recipient_id: int
    email_verified: Optional[bool] = None
    email_score: Optional[int] = None
    email_result: Optional[str] = None  # deliverable, risky, undeliverable, unknown
    is_disposable: Optional[bool] = None
    is_role_email: Optional[bool] = None
    is_free_email: Optional[bool] = None
    phone: Optional[str] = None
    linkedin_url: Optional[str] = None
    twitter_url: Optional[str] = None
    job_title: Optional[str] = None
    seniority: Optional[str] = None
    department: Optional[str] = None
    company_name: Optional[str] = None
    company_domain: Optional[str] = None
    company_industry: Optional[str] = None
    company_size: Optional[str] = None
    company_linkedin: Optional[str] = None
    enrichment_sources: Optional[List[str]] = None
    enrichment_quality: Optional[float] = None


class BatchEnrichmentUpdateRequest(BaseModel):
    """Request to batch update recipients with enrichment data

    **Reason**: Ensures enrichment data is persisted to database,
    surviving page refresh and browser session loss.
    """
    updates: List[RecipientEnrichmentUpdate] = Field(..., min_items=1, max_items=1000)
    job_id: Optional[str] = None  # Optional: link to enrichment job


class BatchEnrichmentUpdateResponse(BaseModel):
    """Response for batch enrichment update"""
    success: bool
    updated_count: int
    failed_count: int
    failed_ids: List[int] = []
    message: str


# ==================== ENDPOINTS ====================


@router.post("/execute", response_model=EnrichmentExecuteResponse)
async def execute_enrichment(
    request: EnrichmentExecuteRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Execute enrichment job for specified recipients.

    Features:
    - Email verification (Hunter.io)
    - Phone number discovery
    - LinkedIn profile finding
    - Job title validation
    - Company information enrichment
    - Social profile discovery
    - Fraud detection (optional)

    **Async mode (default):**
    - Returns immediately with job_id
    - Poll /enrichment/status/{job_id} for progress
    - Fetch results from /enrichment/results/{job_id}

    **Sync mode:**
    - Waits for completion
    - Returns results directly
    - Use for small batches (<10 recipients)

    **Depth levels:**
    - quick: Email verification only (fast)
    - standard: Email + basic company info (default)
    - deep: All available enrichment (slow)
    """
    try:
        logger.info(
            f"🚀 [ENRICHMENT EXECUTE] Starting enrichment for {len(request.recipient_ids)} recipients "
            f"(depth: {request.depth}, async: {request.async_mode})"
        )

        # Validate recipients belong to user
        repo = AsyncRecipientRepository(db)
        recipients = []

        for recipient_id in request.recipient_ids:
            recipient = await repo.get_by_id(recipient_id)
            if not recipient:
                logger.warning(f"⚠️ [ENRICHMENT] Recipient {recipient_id} not found")
                continue
            if recipient.candidate_id != current_candidate.id:
                logger.warning(
                    f"⚠️ [ENRICHMENT] Recipient {recipient_id} not owned by user"
                )
                continue
            recipients.append(recipient)

        if not recipients:
            raise HTTPException(
                status_code=404, detail="No valid recipients found for enrichment"
            )

        logger.info(f"✅ [ENRICHMENT] Validated {len(recipients)} recipients")

        # Create job ID
        job_id = str(uuid4())

        # Initialize enrichment job in database (using async session)
        enrichment_job = EnrichmentJob(
            job_id=job_id,
            candidate_id=current_candidate.id,
            status=EnrichmentJobStatus.PENDING,
            total_recipients=len(recipients),
            processed_recipients=0,
            successful_enrichments=0,
            failed_enrichments=0,
        )
        db.add(enrichment_job)
        await db.commit()
        logger.info(f"✅ [ENRICHMENT] Created job {job_id}")

        # Execute asynchronously
        if request.async_mode:
            # Use BackgroundTasks for proper lifecycle management
            background_tasks.add_task(
                _run_enrichment_job,
                job_id=job_id,
                recipients=recipients,
                config=request.config,
                depth=request.depth,
                candidate_id=current_candidate.id,
            )

            return EnrichmentExecuteResponse(
                success=True,
                job_id=job_id,
                status="running",
                total_recipients=len(recipients),
                progress_url=f"/enrichment/status/{job_id}",
                results_url=f"/enrichment/results/{job_id}",
                message=f"Enrichment job started for {len(recipients)} recipients",
            )

        # Execute synchronously (for small batches)
        else:
            await _execute_enrichment_job(
                job_id=job_id,
                recipients=recipients,
                config=request.config,
                depth=request.depth,
                candidate_id=current_candidate.id,
            )

            return EnrichmentExecuteResponse(
                success=True,
                job_id=job_id,
                status="completed",
                total_recipients=len(recipients),
                results_url=f"/enrichment/results/{job_id}",
                message=f"Enrichment completed for {len(recipients)} recipients",
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [ENRICHMENT EXECUTE] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Enrichment execution failed",
        )


@router.get("/status/{job_id}", response_model=EnrichmentStatusResponse)
async def get_enrichment_status(
    job_id: str,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get enrichment job status and progress.

    Returns:
    - Current status (pending, running, completed, failed)
    - Progress metrics (processed, successful, failed)
    - Partial results (if available)
    - Error details (if failed)
    """
    try:
        logger.info(f"📊 [ENRICHMENT STATUS] Checking status for job {job_id}")

        from sqlalchemy import select
        result = await db.execute(
            select(EnrichmentJob).where(
                EnrichmentJob.job_id == job_id,
                EnrichmentJob.candidate_id == current_candidate.id,
            )
        )
        job = result.scalars().first()

        if not job:
            raise HTTPException(status_code=404, detail="Enrichment job not found")

        # Calculate progress
        progress_pct = 0
        if job.total_recipients > 0:
            progress_pct = round(
                (job.processed_recipients / job.total_recipients) * 100, 2
            )

        response = EnrichmentStatusResponse(
            job_id=job.job_id,
            status=job.status.value,
            progress={
                "total": job.total_recipients,
                "processed": job.processed_recipients,
                "successful": job.successful_enrichments,
                "failed": job.failed_enrichments,
                "percentage": progress_pct,
            },
            results=job.enrichment_results if job.enrichment_results else None,
            error=job.error_message,
            created_at=job.created_at.isoformat(),
            completed_at=job.completed_at.isoformat() if job.completed_at else None,
        )

        logger.info(
            f"✅ [ENRICHMENT STATUS] Job {job_id}: {job.status.value} "
            f"({job.processed_recipients}/{job.total_recipients})"
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [ENRICHMENT STATUS] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get enrichment status",
        )


@router.get("/stream/{job_id}")
async def stream_enrichment_progress(
    job_id: str,
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    SSE endpoint for real-time enrichment progress updates.

    **Purpose**: Replace polling with Server-Sent Events for better
    real-time updates and reduced server load.

    **Event Types**:
    - connected: Initial connection established
    - progress: Recipient enriched (with running totals)
    - status_changed: Job status transition
    - completed: Job finished (success or failure)
    - error: Error occurred

    **Example Usage** (Frontend):
    ```javascript
    const eventSource = new EventSource('/api/v1/enrichment/stream/job-123');
    eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.type === 'progress') {
            updateProgress(data.processed, data.total);
        }
    };
    ```
    """
    from fastapi.responses import StreamingResponse
    import json

    logger.info(f"📡 [ENRICHMENT SSE] Stream started for job {job_id}")

    # Validate job exists and belongs to user
    sync_db = SessionLocal()
    try:
        job = (
            sync_db.query(EnrichmentJob)
            .filter(
                EnrichmentJob.job_id == job_id,
                EnrichmentJob.candidate_id == current_candidate.id,
            )
            .first()
        )

        if not job:
            raise HTTPException(status_code=404, detail="Enrichment job not found")

        initial_status = job.status.value
    finally:
        sync_db.close()

    async def event_generator():
        """Generate SSE events for enrichment progress"""
        last_processed = 0
        last_status = initial_status
        poll_count = 0
        max_polls = 600  # 10 minutes with 1s polling

        sync_db = SessionLocal()

        try:
            # Send initial connection event
            yield f"data: {json.dumps({'type': 'connected', 'job_id': job_id, 'status': initial_status})}\n\n"

            while poll_count < max_polls:
                await asyncio.sleep(1)  # Poll every 1 second
                poll_count += 1

                # Refresh job data
                job = (
                    sync_db.query(EnrichmentJob)
                    .filter(EnrichmentJob.job_id == job_id)
                    .first()
                )

                if not job:
                    yield f"data: {json.dumps({'type': 'error', 'message': 'Job not found'})}\n\n"
                    break

                # Refresh session to get latest data
                sync_db.refresh(job)

                # Check for status change
                current_status = job.status.value
                if current_status != last_status:
                    yield f"data: {json.dumps({'type': 'status_changed', 'status': current_status, 'previous_status': last_status})}\n\n"
                    last_status = current_status

                # Check for progress update
                current_processed = job.processed_recipients or 0
                if current_processed != last_processed:
                    progress_pct = 0
                    if job.total_recipients > 0:
                        progress_pct = round((current_processed / job.total_recipients) * 100, 2)

                    yield f"data: {json.dumps({'type': 'progress', 'processed': current_processed, 'total': job.total_recipients, 'successful': job.successful_enrichments, 'failed': job.failed_enrichments, 'percentage': progress_pct})}\n\n"
                    last_processed = current_processed

                # If job completed/failed, send final event and close
                if job.status in [
                    EnrichmentJobStatus.COMPLETED,
                    EnrichmentJobStatus.FAILED,
                ]:
                    yield f"data: {json.dumps({'type': 'completed', 'status': job.status.value, 'total': job.total_recipients, 'successful': job.successful_enrichments, 'failed': job.failed_enrichments, 'error_message': job.error_message})}\n\n"
                    break

                # Send heartbeat every 30 seconds to keep connection alive
                if poll_count % 30 == 0:
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"

        except asyncio.CancelledError:
            logger.info(f"📡 [ENRICHMENT SSE] Stream cancelled for job {job_id}")
        except Exception as e:
            logger.error(f"❌ [ENRICHMENT SSE] Error in stream: {str(e)}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            sync_db.close()

        logger.info(f"📡 [ENRICHMENT SSE] Stream ended for job {job_id}")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/results/{job_id}")
async def get_enrichment_results(
    job_id: str,
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get enrichment results for completed job.

    Returns:
    - Enriched recipient data
    - Email verification results
    - Confidence scores
    - Fraud detection results
    - Data sources used

    **Note**: Results are automatically persisted to Recipient.custom_fields
    upon job completion. This endpoint returns cached job results for reference.
    """
    try:
        logger.info(f"📥 [ENRICHMENT RESULTS] Fetching results for job {job_id}")

        sync_db = SessionLocal()
        try:
            job = (
                sync_db.query(EnrichmentJob)
                .filter(
                    EnrichmentJob.job_id == job_id,
                    EnrichmentJob.candidate_id == current_candidate.id,
                )
                .first()
            )

            if not job:
                raise HTTPException(status_code=404, detail="Enrichment job not found")

            if job.status not in [
                EnrichmentJobStatus.COMPLETED,
                EnrichmentJobStatus.FAILED,
            ]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Enrichment job is {job.status.value}. Results not available yet.",
                )

            logger.info(
                f"✅ [ENRICHMENT RESULTS] Retrieved {len(job.enrichment_results or [])} results"
            )

            return {
                "job_id": job.job_id,
                "status": job.status.value,
                "total_recipients": job.total_recipients,
                "successful_enrichments": job.successful_enrichments,
                "failed_enrichments": job.failed_enrichments,
                "results": job.enrichment_results or [],
                "persisted_to_recipients": True,  # Results are now persisted
                "completed_at": job.completed_at.isoformat()
                if job.completed_at
                else None,
            }

        finally:
            sync_db.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [ENRICHMENT RESULTS] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get enrichment results",
        )


@router.post("/get-persisted")
async def get_persisted_enrichment(
    recipient_ids: List[int],
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Retrieve persisted enrichment data for recipients.

    **Purpose**: Load enrichment data from database on page refresh,
    recovering data that would otherwise be lost from browser state.

    **Use Case**:
    - User enriches recipients in Step 2
    - User refreshes page or navigates away
    - On return, frontend calls this endpoint to restore enrichment data
    - Data is recovered from Recipient.custom_fields

    Returns enrichment data keyed by recipient_id.
    """
    try:
        logger.info(
            f"📥 [ENRICHMENT RETRIEVE] Fetching enrichment for {len(recipient_ids)} recipients"
        )

        if len(recipient_ids) > 1000:
            raise HTTPException(
                status_code=400,
                detail="Maximum 1000 recipients per request"
            )

        repo = AsyncRecipientRepository(db)
        enrichment_data = {}

        for recipient_id in recipient_ids:
            try:
                recipient = await repo.get_by_id(recipient_id)
                if not recipient:
                    continue
                if recipient.candidate_id != current_candidate.id:
                    continue

                custom_fields = recipient.custom_fields or {}

                # Check if this recipient has been enriched
                if "enriched_at" not in custom_fields:
                    continue

                # Extract enrichment data
                enrichment_data[str(recipient_id)] = {
                    "recipient_id": recipient_id,
                    "email": recipient.email,
                    "name": recipient.name,
                    "company": recipient.company,
                    "position": recipient.position,
                    # Enrichment fields
                    "email_verified": custom_fields.get("email_verified"),
                    "email_score": custom_fields.get("email_score"),
                    "email_result": custom_fields.get("email_result"),
                    "is_disposable": custom_fields.get("is_disposable"),
                    "is_role_email": custom_fields.get("is_role_email"),
                    "is_free_email": custom_fields.get("is_free_email"),
                    "phone": custom_fields.get("phone"),
                    "linkedin_url": custom_fields.get("linkedin_url"),
                    "twitter_url": custom_fields.get("twitter_url"),
                    "job_title": custom_fields.get("job_title"),
                    "seniority": custom_fields.get("seniority"),
                    "department": custom_fields.get("department"),
                    "company_name": custom_fields.get("company_name"),
                    "company_domain": custom_fields.get("company_domain"),
                    "company_industry": custom_fields.get("company_industry"),
                    "company_size": custom_fields.get("company_size"),
                    "company_linkedin": custom_fields.get("company_linkedin"),
                    "enrichment_sources": custom_fields.get("enrichment_sources", []),
                    "enrichment_quality": custom_fields.get("enrichment_quality", 0),
                    "enriched_at": custom_fields.get("enriched_at"),
                    "enrichment_job_id": custom_fields.get("enrichment_job_id"),
                }

            except Exception as e:
                logger.warning(
                    f"⚠️ [ENRICHMENT RETRIEVE] Failed for recipient {recipient_id}: {str(e)}"
                )
                continue

        logger.info(
            f"✅ [ENRICHMENT RETRIEVE] Retrieved enrichment for {len(enrichment_data)} recipients"
        )

        return {
            "success": True,
            "total_requested": len(recipient_ids),
            "enriched_count": len(enrichment_data),
            "enrichment_data": enrichment_data,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [ENRICHMENT RETRIEVE] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve enrichment data",
        )


@router.post("/persist-results", response_model=BatchEnrichmentUpdateResponse)
async def batch_update_recipient_enrichment(
    request: BatchEnrichmentUpdateRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Batch update recipients with enrichment data - persists to database.

    **Why this matters (2026 Best Practice)**:
    - Enrichment data is valuable and should not be lost on page refresh
    - Persisting to Recipient.custom_fields ensures data survives browser sessions
    - Enables reuse of enrichment data across multiple campaigns
    - Supports data-driven decision making with persistent analytics

    **Fields stored in custom_fields JSON**:
    - email_verified, email_score, email_result
    - is_disposable, is_role_email, is_free_email
    - phone, linkedin_url, twitter_url
    - job_title, seniority, department
    - company_name, company_domain, company_industry, company_size
    - enrichment_sources, enrichment_quality
    - enriched_at (timestamp)
    """
    try:
        logger.info(
            f"💾 [ENRICHMENT PERSIST] Persisting enrichment for {len(request.updates)} recipients"
        )

        updated_count = 0
        failed_count = 0
        failed_ids = []

        repo = AsyncRecipientRepository(db)

        for update in request.updates:
            try:
                # Get recipient and verify ownership
                recipient = await repo.get_by_id(update.recipient_id)
                if not recipient:
                    logger.warning(
                        f"⚠️ [ENRICHMENT PERSIST] Recipient {update.recipient_id} not found"
                    )
                    failed_ids.append(update.recipient_id)
                    failed_count += 1
                    continue

                if recipient.candidate_id != current_candidate.id:
                    logger.warning(
                        f"⚠️ [ENRICHMENT PERSIST] Recipient {update.recipient_id} not owned by user"
                    )
                    failed_ids.append(update.recipient_id)
                    failed_count += 1
                    continue

                # Merge enrichment data into custom_fields
                current_custom_fields = recipient.custom_fields or {}

                enrichment_data = {
                    "email_verified": update.email_verified,
                    "email_score": update.email_score,
                    "email_result": update.email_result,
                    "is_disposable": update.is_disposable,
                    "is_role_email": update.is_role_email,
                    "is_free_email": update.is_free_email,
                    "phone": update.phone,
                    "linkedin_url": update.linkedin_url,
                    "twitter_url": update.twitter_url,
                    "job_title": update.job_title,
                    "seniority": update.seniority,
                    "department": update.department,
                    "company_name": update.company_name,
                    "company_domain": update.company_domain,
                    "company_industry": update.company_industry,
                    "company_size": update.company_size,
                    "company_linkedin": update.company_linkedin,
                    "enrichment_sources": update.enrichment_sources,
                    "enrichment_quality": update.enrichment_quality,
                    "enriched_at": datetime.now(timezone.utc).isoformat(),
                    "enrichment_job_id": request.job_id,
                }

                # Filter out None values to avoid overwriting with nulls
                enrichment_data = {k: v for k, v in enrichment_data.items() if v is not None}

                # Merge into custom_fields
                current_custom_fields.update(enrichment_data)
                recipient.custom_fields = current_custom_fields

                # Also update direct fields if provided
                if update.job_title and not recipient.position:
                    recipient.position = update.job_title
                if update.company_name and not recipient.company:
                    recipient.company = update.company_name

                updated_count += 1
                logger.debug(
                    f"✅ [ENRICHMENT PERSIST] Updated recipient {update.recipient_id} "
                    f"(quality: {update.enrichment_quality})"
                )

            except Exception as e:
                logger.error(
                    f"❌ [ENRICHMENT PERSIST] Failed to update recipient {update.recipient_id}: {str(e)}"
                )
                failed_ids.append(update.recipient_id)
                failed_count += 1

        # Commit all changes
        await db.commit()

        logger.info(
            f"✅ [ENRICHMENT PERSIST] Completed: {updated_count} updated, {failed_count} failed"
        )

        return BatchEnrichmentUpdateResponse(
            success=failed_count == 0,
            updated_count=updated_count,
            failed_count=failed_count,
            failed_ids=failed_ids,
            message=f"Persisted enrichment data for {updated_count} recipients"
            + (f", {failed_count} failed" if failed_count > 0 else ""),
        )

    except Exception as e:
        logger.error(f"❌ [ENRICHMENT PERSIST] Error: {str(e)}", exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to persist enrichment results",
        )


# ==================== HELPER FUNCTIONS ====================


def _run_enrichment_job(
    job_id: str,
    recipients: List[Recipient],
    config: "EnrichmentConfig",
    depth: str,
    candidate_id: int,
) -> None:
    """Sync wrapper for BackgroundTasks — runs the async enrichment in a new event loop."""
    asyncio.run(_execute_enrichment_job(
        job_id=job_id,
        recipients=recipients,
        config=config,
        depth=depth,
        candidate_id=candidate_id,
    ))


async def _execute_enrichment_job(
    job_id: str,
    recipients: List[Recipient],
    config: EnrichmentConfig,
    depth: str,
    candidate_id: int,
) -> Optional[List[Dict[str, Any]]]:
    """Execute enrichment job in background"""
    sync_db = SessionLocal()

    try:
        # Update job status to RUNNING
        job = (
            sync_db.query(EnrichmentJob).filter(EnrichmentJob.job_id == job_id).first()
        )

        if job:
            job.status = EnrichmentJobStatus.RUNNING
            sync_db.commit()

        logger.info(
            f"🔄 [ENRICHMENT JOB] Starting job {job_id} with {len(recipients)} recipients"
        )

        # Initialize orchestrator
        orchestrator = EnrichmentOrchestrator()

        # Determine features based on depth
        if depth == "quick":
            features = ["email_verification"]
        elif depth == "deep":
            features = [
                "email_verification",
                "phone_discovery",
                "linkedin_profile",
                "job_title_validation",
                "company_info",
                "social_profiles",
            ]
        else:  # standard
            features = ["email_verification", "job_title_validation", "company_info"]

        # Override with config
        if not config.email_verification:
            features = [f for f in features if f != "email_verification"]
        if config.phone_discovery and "phone_discovery" not in features:
            features.append("phone_discovery")
        if config.linkedin_profile and "linkedin_profile" not in features:
            features.append("linkedin_profile")

        # Enrich each recipient
        enriched_results = []
        successful = 0
        failed = 0

        for index, recipient in enumerate(recipients, 1):
            try:
                logger.info(
                    f"🔍 [ENRICHMENT JOB] [{index}/{len(recipients)}] "
                    f"Enriching {recipient.email or recipient.first_name}"
                )

                # Prepare record
                record = {
                    "recipient_id": recipient.id,
                    "email": recipient.email,
                    "first_name": recipient.first_name,
                    "last_name": recipient.last_name,
                    "company": recipient.company,
                    "job_title": recipient.job_title,
                }

                # Enrich
                enriched = await orchestrator.enrich_record(
                    record=record, features=features, use_cache=config.use_cache
                )

                enriched_results.append(enriched)
                successful += 1

                # Update progress
                if job:
                    job.processed_recipients = index
                    job.successful_enrichments = successful
                    job.failed_enrichments = failed
                    sync_db.commit()

            except Exception as e:
                logger.error(
                    f"❌ [ENRICHMENT JOB] Failed to enrich recipient {recipient.id}: {str(e)}"
                )
                failed += 1
                enriched_results.append(
                    {
                        "recipient_id": recipient.id,
                        "error": str(e),
                        "enrichment_quality": 0.0,
                    }
                )

                if job:
                    job.processed_recipients = index
                    job.failed_enrichments = failed
                    sync_db.commit()

        # Mark job as completed
        if job:
            job.status = EnrichmentJobStatus.COMPLETED
            job.enrichment_results = enriched_results
            job.completed_at = datetime.now(timezone.utc)
            sync_db.commit()

        logger.info(
            f"✅ [ENRICHMENT JOB] Job {job_id} completed: "
            f"{successful} successful, {failed} failed"
        )

        # =====================================================
        # CRITICAL FIX: Auto-persist enrichment to Recipient records
        # This ensures data survives page refresh/session loss
        # =====================================================
        persisted = await _persist_enrichment_to_recipients(
            job_id=job_id,
            enriched_results=enriched_results,
            sync_db=sync_db,
        )
        logger.info(
            f"💾 [ENRICHMENT JOB] Auto-persisted {persisted} recipients to database"
        )

        return enriched_results

    except Exception as e:
        logger.error(
            f"❌ [ENRICHMENT JOB] Job {job_id} failed: {str(e)}", exc_info=True
        )

        # Mark job as failed
        if job:
            job.status = EnrichmentJobStatus.FAILED
            job.error_message = str(e)
            sync_db.commit()

        return None

    finally:
        sync_db.close()


async def _persist_enrichment_to_recipients(
    job_id: str,
    enriched_results: List[Dict[str, Any]],
    sync_db,
) -> int:
    """
    Auto-persist enrichment results to Recipient.custom_fields.

    **Purpose**: Ensure enrichment data survives page refresh by storing
    it directly in the Recipient database records rather than only in
    browser state or temporary job results.

    **Data Flow**:
    1. Enrichment completes → Results stored in EnrichmentJob.enrichment_results
    2. This function → Also persisted to each Recipient.custom_fields
    3. On page refresh → Data loaded from database, not lost

    Returns the count of successfully persisted recipients.
    """
    persisted_count = 0

    try:
        logger.info(
            f"💾 [ENRICHMENT PERSIST] Auto-persisting {len(enriched_results)} results to recipients"
        )

        for result in enriched_results:
            try:
                recipient_id = result.get("recipient_id")
                if not recipient_id:
                    continue

                # Skip failed enrichments
                if result.get("error"):
                    continue

                # Get recipient
                recipient = sync_db.query(Recipient).filter(
                    Recipient.id == recipient_id
                ).first()

                if not recipient:
                    logger.warning(
                        f"⚠️ [ENRICHMENT PERSIST] Recipient {recipient_id} not found"
                    )
                    continue

                # Build enrichment data to persist
                current_custom_fields = recipient.custom_fields or {}

                # Extract enrichment data from result
                enrichment_data = {
                    "enriched_at": datetime.now(timezone.utc).isoformat(),
                    "enrichment_job_id": job_id,
                    "enrichment_quality": result.get("enrichment_quality", 0.0),
                }

                # Email verification data
                if "email_verified" in result:
                    enrichment_data["email_verified"] = result["email_verified"]
                if "email_score" in result:
                    enrichment_data["email_score"] = result["email_score"]
                if "email_result" in result:
                    enrichment_data["email_result"] = result["email_result"]
                if "is_disposable" in result:
                    enrichment_data["is_disposable"] = result["is_disposable"]
                if "is_role_email" in result:
                    enrichment_data["is_role_email"] = result["is_role_email"]
                if "is_free_email" in result:
                    enrichment_data["is_free_email"] = result["is_free_email"]

                # Contact data
                if result.get("phone"):
                    enrichment_data["phone"] = result["phone"]
                if result.get("linkedin_url"):
                    enrichment_data["linkedin_url"] = result["linkedin_url"]
                if result.get("twitter_url"):
                    enrichment_data["twitter_url"] = result["twitter_url"]

                # Job data
                if result.get("job_title"):
                    enrichment_data["job_title"] = result["job_title"]
                    # Also update direct field if empty
                    if not recipient.position:
                        recipient.position = result["job_title"]
                if result.get("seniority"):
                    enrichment_data["seniority"] = result["seniority"]
                if result.get("department"):
                    enrichment_data["department"] = result["department"]

                # Company data
                company_data = result.get("company_data") or {}
                if company_data:
                    if company_data.get("name"):
                        enrichment_data["company_name"] = company_data["name"]
                        # Also update direct field if empty
                        if not recipient.company:
                            recipient.company = company_data["name"]
                    if company_data.get("domain"):
                        enrichment_data["company_domain"] = company_data["domain"]
                    if company_data.get("industry"):
                        enrichment_data["company_industry"] = company_data["industry"]
                    if company_data.get("employee_count"):
                        enrichment_data["company_size"] = str(company_data["employee_count"])
                    if company_data.get("linkedin_url"):
                        enrichment_data["company_linkedin"] = company_data["linkedin_url"]

                # Data sources
                if result.get("data_sources"):
                    enrichment_data["enrichment_sources"] = result["data_sources"]

                # Merge into custom_fields (preserving existing data)
                current_custom_fields.update(enrichment_data)
                recipient.custom_fields = current_custom_fields

                persisted_count += 1

                logger.debug(
                    f"✅ [ENRICHMENT PERSIST] Persisted recipient {recipient_id} "
                    f"(quality: {result.get('enrichment_quality', 0):.2f})"
                )

            except Exception as e:
                logger.error(
                    f"❌ [ENRICHMENT PERSIST] Failed to persist recipient: {str(e)}"
                )
                continue

        # Commit all changes
        sync_db.commit()

        logger.info(
            f"✅ [ENRICHMENT PERSIST] Successfully persisted {persisted_count} recipients"
        )

        return persisted_count

    except Exception as e:
        logger.error(f"❌ [ENRICHMENT PERSIST] Error: {str(e)}", exc_info=True)
        return persisted_count
