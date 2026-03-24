"""
Extraction API Endpoints
ULTRA PRO MAX EXTRACTION ENGINE - API Interface
"""

import os
import asyncio
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query
from fastapi.responses import StreamingResponse, FileResponse
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.extraction import ExtractionJob, ExtractionResult, ExtractionTemplate
from app.schemas.extraction import (
    CreateExtractionJobRequest,
    ExtractionJobResponse,
    ExtractionResultResponse,
    PaginatedResultsResponse,
    ExportRequest,
    ExportResponse,
    ImportToRecipientsRequest,
    ImportToRecipientsResponse,
    JobControlResponse,
    CreateExtractionTemplateRequest,
    ExtractionTemplateResponse
)
from app.tasks.extraction_tasks import (
    run_extraction_job,
    pause_extraction_job,
    resume_extraction_job,
    cancel_extraction_job
)
from app.services.excel_export_service import ExcelExportService
from app.utils.progress_tracker import stream_progress
from app.models.recipient import Recipient
from app.models.recipient_group import RecipientGroup

import logging

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================================
# JOB MANAGEMENT ENDPOINTS
# ============================================================================

@router.post("/jobs", response_model=ExtractionJobResponse, status_code=status.HTTP_201_CREATED)
async def create_extraction_job(
    request: CreateExtractionJobRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Create new extraction job

    FREE version supports:
    - URLs scraping (static HTML)
    - CSV file import
    - Depth 1-3 (no JS rendering)
    """
    # Create job record
    job = ExtractionJob(
        candidate_id=current_candidate.id,
        sector=request.sector,
        status="pending",
        sources=request.sources.dict(),
        filters=request.filters.dict() if request.filters else {},
        options=request.options.dict() if request.options else {}
    )

    db.add(job)
    db.commit()
    db.refresh(job)

    logger.info(f"Created extraction job {job.id} for candidate {current_candidate.id}")

    # Convert to response model
    return ExtractionJobResponse.from_orm(job)


@router.post("/jobs/{job_id}/start", response_model=ExtractionJobResponse)
async def start_extraction_job(
    job_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Start extraction job in background

    Returns immediately with job info.
    Monitor progress via /jobs/{job_id}/stream endpoint.
    """
    # Get job
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    if job.status != "pending":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job {job_id} is not pending (status: {job.status})"
        )

    # Start background task
    run_extraction_job.delay(job_id)

    logger.info(f"Started extraction job {job_id}")

    # Refresh and return
    db.refresh(job)
    return ExtractionJobResponse.from_orm(job)


@router.get("/jobs/{job_id}", response_model=ExtractionJobResponse)
async def get_extraction_job(
    job_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get extraction job details"""
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    return ExtractionJobResponse.from_orm(job)


@router.get("/jobs", response_model=List[ExtractionJobResponse])
async def list_extraction_jobs(
    sector: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """List extraction jobs for current candidate"""
    query = db.query(ExtractionJob).filter_by(candidate_id=current_candidate.id)

    # Apply filters
    if sector:
        query = query.filter(ExtractionJob.sector == sector)
    if status:
        query = query.filter(ExtractionJob.status == status)

    # Order by created_at descending
    query = query.order_by(ExtractionJob.created_at.desc())

    # Pagination
    jobs = query.offset(offset).limit(limit).all()

    return [ExtractionJobResponse.from_orm(job) for job in jobs]


@router.delete("/jobs/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_extraction_job(
    job_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Delete extraction job and all associated data"""
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    # Delete file if exists
    if job.result_file_path and os.path.exists(job.result_file_path):
        try:
            os.remove(job.result_file_path)
        except Exception as e:
            logger.error(f"Failed to delete file {job.result_file_path}: {e}")

    db.delete(job)
    db.commit()

    logger.info(f"Deleted extraction job {job_id}")


# ============================================================================
# REAL-TIME PROGRESS STREAMING (SSE)
# ============================================================================

@router.get("/jobs/{job_id}/stream")
async def stream_extraction_progress(
    job_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Stream extraction progress in real-time using Server-Sent Events (SSE)

    Usage:
        const eventSource = new EventSource('/api/v1/extraction/jobs/123/stream');
        eventSource.onmessage = (event) => {
            const update = JSON.parse(event.data);
            console.log(update.message, update.progress_percent);
        };
    """
    # Verify job exists and belongs to candidate
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    async def event_generator():
        """Generate SSE events"""
        try:
            async for update in stream_progress(job_id, db):
                # Format as SSE
                import json
                yield f"data: {json.dumps(update)}\n\n"

                # Check if complete
                if update.get("type") == "complete" or update.get("progress_percent", 0) >= 100:
                    logger.info(f"Extraction {job_id} complete, closing SSE stream")
                    break

        except Exception as e:
            logger.error(f"SSE stream error for job {job_id}: {e}")
            # Send error event
            import json
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )


# ============================================================================
# JOB CONTROL ENDPOINTS
# ============================================================================

@router.post("/jobs/{job_id}/pause", response_model=JobControlResponse)
async def pause_job(
    job_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Pause running extraction job"""
    # Verify ownership
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    # Call Celery task
    result = pause_extraction_job.delay(job_id).get()

    if result["status"] == "error":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result["message"]
        )

    # Refresh job
    db.refresh(job)

    return JobControlResponse(
        job_id=job_id,
        status=job.status,
        message=result["message"]
    )


@router.post("/jobs/{job_id}/resume", response_model=JobControlResponse)
async def resume_job(
    job_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Resume paused extraction job"""
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    result = resume_extraction_job.delay(job_id).get()

    if result["status"] == "error":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result["message"]
        )

    db.refresh(job)

    return JobControlResponse(
        job_id=job_id,
        status=job.status,
        message=result["message"]
    )


@router.post("/jobs/{job_id}/cancel", response_model=JobControlResponse)
async def cancel_job(
    job_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Cancel extraction job"""
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    result = cancel_extraction_job.delay(job_id).get()

    if result["status"] == "error":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result["message"]
        )

    db.refresh(job)

    return JobControlResponse(
        job_id=job_id,
        status=job.status,
        message=result["message"]
    )


# ============================================================================
# RESULTS ENDPOINTS
# ============================================================================

@router.get("/jobs/{job_id}/results", response_model=PaginatedResultsResponse)
async def get_job_results(
    job_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    min_quality: Optional[float] = Query(None, ge=0, le=1),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get paginated extraction results

    Query params:
        - page: Page number (1-indexed)
        - page_size: Results per page (max 200)
        - min_quality: Minimum quality score filter (0-1)
    """
    # Verify job ownership
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    # Build query
    query = db.query(ExtractionResult).filter_by(job_id=job_id)

    # Apply quality filter
    if min_quality is not None:
        query = query.filter(ExtractionResult.quality_score >= min_quality)

    # Order by quality (best first)
    query = query.order_by(ExtractionResult.quality_score.desc())

    # Count total
    total = query.count()

    # Paginate
    offset = (page - 1) * page_size
    results = query.offset(offset).limit(page_size).all()

    return PaginatedResultsResponse(
        job_id=job_id,
        results=[ExtractionResultResponse.from_orm(r) for r in results],
        total=total,
        page=page,
        page_size=page_size,
        has_more=offset + len(results) < total
    )


# ============================================================================
# EXPORT ENDPOINTS
# ============================================================================

@router.post("/jobs/{job_id}/export", response_model=ExportResponse)
async def export_job_results(
    job_id: int,
    request: ExportRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Export extraction results to Excel/CSV/JSON

    FREE version supports Excel export with formatting
    """
    # Verify job ownership
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    # Only Excel export implemented for FREE version
    if request.format != "excel":
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=f"Export format '{request.format}' not yet implemented. Use 'excel'."
        )

    # Export to Excel
    export_service = ExcelExportService(db)

    try:
        min_quality = request.filters.get("min_quality") if request.filters else None

        filepath = export_service.export_job_results(
            job_id=job_id,
            include_metadata=request.include_metadata,
            min_quality=min_quality
        )

        # Get file size
        file_size = os.path.getsize(filepath)

        # Count records
        record_count = db.query(ExtractionResult).filter_by(job_id=job_id).count()

        return ExportResponse(
            file_path=filepath,
            format=request.format,
            record_count=record_count,
            file_size_bytes=file_size,
            download_url=f"/api/v1/extraction/downloads/{os.path.basename(filepath)}"
        )

    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Export failed"
        )


@router.get("/downloads/{filename}")
async def download_export(
    filename: str,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Download exported file

    Security: Verifies file belongs to current candidate
    """
    from app.core.config import settings

    # Prevent path traversal: resolve and verify path stays within EXPORTS_DIR
    exports_dir = os.path.realpath(settings.EXPORTS_DIR)
    filepath = os.path.realpath(os.path.join(exports_dir, filename))
    if not filepath.startswith(exports_dir + os.sep):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid filename"
        )

    if not os.path.exists(filepath):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )

    # Verify ownership (extract job_id from filename)
    # Format: extraction_{sector}_{job_id}_{timestamp}.xlsx
    try:
        parts = filename.split("_")
        job_id = int(parts[2])

        job = db.query(ExtractionJob).filter_by(
            id=job_id,
            candidate_id=current_candidate.id
        ).first()

        if not job:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )

    except (IndexError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid filename"
        )

    return FileResponse(
        filepath,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )


# ============================================================================
# INTEGRATION ENDPOINTS
# ============================================================================

@router.post("/jobs/{job_id}/import-recipients", response_model=ImportToRecipientsResponse)
async def import_to_recipients(
    job_id: int,
    request: ImportToRecipientsRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Import extraction results to Recipients table

    THE GAME CHANGER: Seamless integration with existing system
    Uses Recipients.custom_fields JSON for metadata
    """
    # Verify job ownership
    job = db.query(ExtractionJob).filter_by(
        id=job_id,
        candidate_id=current_candidate.id
    ).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    # Get results
    results = db.query(ExtractionResult).filter_by(
        job_id=job_id,
        is_validated=True  # Only import validated records
    ).all()

    imported_count = 0
    skipped_count = 0
    errors = []

    # Create recipient group if requested
    group = None
    if request.create_group:
        group_name = request.group_name or f"Extraction {job.sector.value.capitalize()} - {job.id}"

        group = RecipientGroup(
            candidate_id=current_candidate.id,
            name=group_name,
            description=f"Imported from extraction job {job.id}"
        )
        db.add(group)
        db.commit()
        db.refresh(group)

    # Import results
    for result in results:
        data = result.data

        # Required fields
        email = data.get("email")
        if not email:
            skipped_count += 1
            continue

        # Check for duplicates
        if request.filter_duplicates:
            existing = db.query(Recipient).filter_by(
                candidate_id=current_candidate.id,
                email=email
            ).first()

            if existing:
                skipped_count += 1
                continue

        # Create recipient
        try:
            recipient = Recipient(
                candidate_id=current_candidate.id,
                name=data.get("name", ""),
                email=email,
                phone=data.get("phone"),
                company_id=None,  # Could lookup/create company
                position=data.get("title"),
                custom_fields={
                    "extraction_job_id": job.id,
                    "extraction_sector": job.sector.value,
                    "quality_score": result.quality_score,
                    "confidence_score": result.confidence_score,
                    "completeness_score": result.completeness_score,
                    "source_url": result.source_url,
                    "linkedin_url": data.get("linkedin_url"),
                    "location": data.get("location"),
                    "company_name": data.get("company")
                }
            )

            db.add(recipient)
            db.flush()

            # Add to group
            if group:
                recipient.groups.append(group)

            imported_count += 1

        except Exception as e:
            logger.error(f"Failed to import result {result.id}: {e}")
            errors.append(f"Record {result.id}: {str(e)}")
            skipped_count += 1

    db.commit()

    logger.info(f"Imported {imported_count} recipients from job {job_id}")

    return ImportToRecipientsResponse(
        imported_count=imported_count,
        skipped_count=skipped_count,
        group_id=group.id if group else None,
        group_name=group.name if group else None,
        errors=errors
    )


# ============================================================================
# TEMPLATE ENDPOINTS
# ============================================================================

@router.post("/templates", response_model=ExtractionTemplateResponse, status_code=status.HTTP_201_CREATED)
async def create_template(
    request: CreateExtractionTemplateRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Save extraction configuration as reusable template"""
    template = ExtractionTemplate(
        candidate_id=current_candidate.id,
        name=request.name,
        description=request.description,
        sector=request.sector,
        filters=request.filters,
        options=request.options,
        is_public=request.is_public
    )

    db.add(template)
    db.commit()
    db.refresh(template)

    return ExtractionTemplateResponse.from_orm(template)


@router.get("/templates", response_model=List[ExtractionTemplateResponse])
async def list_templates(
    include_public: bool = True,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """List extraction templates (own + public)"""
    query = db.query(ExtractionTemplate)

    if include_public:
        query = query.filter(
            (ExtractionTemplate.candidate_id == current_candidate.id) |
            (ExtractionTemplate.is_public == True)
        )
    else:
        query = query.filter_by(candidate_id=current_candidate.id)

    templates = query.order_by(ExtractionTemplate.usage_count.desc()).all()

    return [ExtractionTemplateResponse.from_orm(t) for t in templates]
