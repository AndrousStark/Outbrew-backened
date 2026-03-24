"""Application Attachment Management Endpoints"""
import logging
from typing import List, Optional
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    UploadFile,
    File,
    Form,
    Query,
)
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from pathlib import Path

from app.core.database import get_database_session
from app.models.candidate import Candidate
from app.models.application import Application
from app.models.application_history import ApplicationAttachment
from app.api.dependencies import get_current_candidate
from app.services.storage_service import StorageService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/applications/{application_id}/attachments",
    status_code=status.HTTP_201_CREATED,
)
async def upload_attachment(
    application_id: int,
    file: UploadFile = File(..., description="Attachment file"),
    attachment_type: str = Form(
        "other",
        description="Type: interview_notes, offer_letter, correspondence, other",
    ),
    description: Optional[str] = Form(None, description="Attachment description"),
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session),
):
    """
    Upload attachment to an application

    **Attachment Types:**
    - interview_notes: Interview notes and feedback
    - offer_letter: Job offer letters
    - correspondence: Email correspondence
    - other: Other documents

    **File Size:** Maximum 25MB per file
    **Allowed Types:** PDF, DOC, DOCX, PNG, JPG, JPEG, TXT, ZIP
    """
    # Verify application belongs to current candidate
    application = (
        db.query(Application)
        .filter(
            Application.id == application_id,
            Application.candidate_id == current_candidate.id,
        )
        .first()
    )

    if not application:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Application not found",
        )

    # Upload file using storage service
    storage = StorageService(db)

    try:
        file_path, file_size, file_type = storage.upload_attachment(
            candidate_id=current_candidate.id,
            application_id=application_id,
            file=file,
            attachment_type=attachment_type,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading attachment: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upload attachment",
        )

    # Create attachment record
    attachment = ApplicationAttachment(
        application_id=application_id,
        filename=file.filename,
        file_path=file_path,
        file_type=file_type,
        file_size=file_size,
        attachment_type=attachment_type,
        description=description,
    )

    db.add(attachment)
    db.commit()
    db.refresh(attachment)

    return {
        "id": attachment.id,
        "filename": attachment.filename,
        "file_type": attachment.file_type,
        "file_size": attachment.file_size,
        "attachment_type": attachment.attachment_type,
        "description": attachment.description,
        "uploaded_at": attachment.uploaded_at,
    }


@router.get("/applications/{application_id}/attachments")
async def list_attachments(
    application_id: int,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session),
):
    """Get all attachments for an application"""
    # Verify application belongs to current candidate
    application = (
        db.query(Application)
        .filter(
            Application.id == application_id,
            Application.candidate_id == current_candidate.id,
        )
        .first()
    )

    if not application:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Application not found",
        )

    attachments = (
        db.query(ApplicationAttachment)
        .filter(ApplicationAttachment.application_id == application_id)
        .order_by(ApplicationAttachment.uploaded_at.desc())
        .all()
    )

    return {
        "attachments": [
            {
                "id": att.id,
                "filename": att.filename,
                "file_type": att.file_type,
                "file_size": att.file_size,
                "attachment_type": att.attachment_type,
                "description": att.description,
                "uploaded_at": att.uploaded_at,
            }
            for att in attachments
        ],
        "total": len(attachments),
    }


@router.get("/attachments/{attachment_id}/download")
async def download_attachment(
    attachment_id: int,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session),
):
    """Download an attachment file"""
    # Get attachment
    attachment = db.query(ApplicationAttachment).filter(
        ApplicationAttachment.id == attachment_id
    ).first()

    if not attachment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Attachment not found",
        )

    # Verify ownership through application
    application = db.query(Application).filter(
        Application.id == attachment.application_id,
        Application.candidate_id == current_candidate.id,
    ).first()

    if not application:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to access this attachment",
        )

    # Check if file exists
    file_path = Path(attachment.file_path)
    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found on server",
        )

    return FileResponse(
        path=str(file_path),
        filename=attachment.filename,
        media_type="application/octet-stream",
    )


@router.delete("/attachments/{attachment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_attachment(
    attachment_id: int,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session),
):
    """Delete an attachment"""
    # Get attachment
    attachment = db.query(ApplicationAttachment).filter(
        ApplicationAttachment.id == attachment_id
    ).first()

    if not attachment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Attachment not found",
        )

    # Verify ownership
    application = db.query(Application).filter(
        Application.id == attachment.application_id,
        Application.candidate_id == current_candidate.id,
    ).first()

    if not application:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to delete this attachment",
        )

    # Delete physical file
    storage = StorageService(db)
    try:
        storage.delete_file(
            attachment.file_path,
            current_candidate.id,
            "documents",
        )
    except Exception as e:
        logger.warning(f"Failed to delete physical file: {e}")

    # Delete database record
    db.delete(attachment)
    db.commit()

    return None


@router.get("/storage/stats")
async def get_storage_stats(
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session),
):
    """Get detailed storage statistics for current user"""
    storage = StorageService(db)
    stats = storage.get_storage_stats(current_candidate.id)
    return stats


@router.post("/storage/recalculate")
async def recalculate_storage(
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session),
):
    """Recalculate storage quota by scanning filesystem"""
    storage = StorageService(db)
    quota = storage.recalculate_candidate_quota(current_candidate.id)

    return {
        "message": "Storage quota recalculated successfully",
        "quota_limit_mb": quota.quota_limit / (1024 * 1024),
        "used_mb": quota.used_bytes / (1024 * 1024),
        "available_mb": (quota.quota_limit - quota.used_bytes) / (1024 * 1024),
        "total_files": quota.total_files,
    }
