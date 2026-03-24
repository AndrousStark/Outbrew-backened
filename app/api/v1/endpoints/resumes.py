"""Resume Version Management Endpoints"""
import logging
import shutil
import uuid
from pathlib import Path
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form, Query
from sqlalchemy.orm import Session

from app.core.database import get_database_session
from app.models.candidate import Candidate
from app.models.resume import ResumeVersion, ResumeLanguage
from app.schemas.resume import (
    ResumeVersionCreate,
    ResumeVersionUpdate,
    ResumeVersionResponse,
    ResumeVersionListResponse
)
from app.api.dependencies import get_current_candidate
from app.services.resume_parser import IntelligentResumeParser
from datetime import timezone

logger = logging.getLogger(__name__)

router = APIRouter()

# Directory for storing resume files
RESUME_UPLOAD_DIR = Path("uploads/resumes")
RESUME_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Magic bytes for file type validation
_MAGIC_BYTES = {
    ".pdf": [b"%PDF"],
    ".docx": [b"PK\x03\x04"],
    ".doc": [b"\xd0\xcf\x11\xe0"],
}


def _validate_magic_bytes(content: bytes, file_ext: str) -> bool:
    """Check file content starts with expected magic bytes."""
    expected = _MAGIC_BYTES.get(file_ext, [])
    if not expected:
        return True
    return any(content[:len(sig)] == sig for sig in expected)


@router.post("/parse", status_code=status.HTTP_200_OK)
async def parse_resume_file(
    file: UploadFile = File(..., description="Resume file to parse (PDF or DOCX)"),
    save_to_profile: bool = Form(default=True, description="Save parsed data to candidate profile"),
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """
    Parse uploaded resume file using intelligent AI parser.
    Extracts: skills (categorized), experience, education, projects, contact info, etc.

    By default, saves parsed data to candidate profile for use in email generation.
    Set save_to_profile=false to only return parsed data without saving.
    """
    try:
        # Validate file type
        allowed_extensions = ['.pdf', '.docx', '.doc']
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in allowed_extensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file type. Supported formats: PDF, DOCX. Got: {file_ext}"
            )

        # Validate file size (max 10MB)
        max_size = 10 * 1024 * 1024  # 10MB
        file_content = await file.read()

        # Validate magic bytes (prevents extension spoofing)
        if not _validate_magic_bytes(file_content, file_ext):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File content does not match expected format for {file_ext}"
            )

        if len(file_content) > max_size:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File too large. Maximum size: 10MB. Got: {len(file_content) / (1024*1024):.1f}MB"
            )

        # Parse resume using intelligent parser
        logger.info(f"📄 [Parse] Parsing resume for candidate {current_candidate.id}: {file.filename}")
        parser = IntelligentResumeParser(fuzzy_threshold=65)
        parsed_resume = parser.parse_file(file_content, file.filename)

        # Convert to dict for JSON response
        result = parsed_resume.to_dict()

        logger.info(
            f"✅ [Parse] Successfully parsed resume for candidate {current_candidate.id}. "
            f"Confidence: {result['confidence_score']:.1f}%, "
            f"Skills found: {len(result['skills_raw'])}, "
            f"Sections: {len(result['detected_sections'])}"
        )

        # Save to candidate profile if requested
        if save_to_profile:
            logger.info(f"💾 [Parse] Saving parsed resume data to candidate {current_candidate.id} profile")
            current_candidate.skills = result  # Store full parsed resume in skills JSON field
            db.commit()
            logger.info(f"✅ [Parse] Saved parsed resume data to profile")

        return {
            "success": True,
            "filename": file.filename,
            "candidate_id": current_candidate.id,
            "parsed_data": result,
            "saved_to_profile": save_to_profile
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Parse] Error parsing resume: {str(e)}", exc_info=True)
        if db:
            db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to parse resume"
        )


@router.get("/me/parsed", status_code=status.HTTP_200_OK)
def get_my_parsed_resume(
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get the currently logged-in candidate's parsed resume data.

    Returns the parsed resume data previously saved via /parse endpoint.
    Used by email generation to access candidate's skills and experience.
    """
    try:
        logger.info(f"📄 [Get Parsed] Retrieving parsed resume for candidate {current_candidate.id}")

        if not current_candidate.skills:
            logger.warning(f"⚠️  [Get Parsed] No parsed resume found for candidate {current_candidate.id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No parsed resume found. Please upload and parse a resume first."
            )

        logger.info(f"✅ [Get Parsed] Retrieved parsed resume for candidate {current_candidate.id}")

        return {
            "success": True,
            "candidate_id": current_candidate.id,
            "parsed_data": current_candidate.skills
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Get Parsed] Error retrieving parsed resume: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve parsed resume"
        )


@router.patch("/me/parsed", status_code=status.HTTP_200_OK)
def update_my_parsed_resume(
    resume_data: dict,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """
    Update the currently logged-in candidate's parsed resume data.

    Allows editing of parsed resume fields like skills, experience, contact info, etc.
    All changes are saved to the candidate's profile.
    """
    try:
        logger.info(f"✏️  [Update Parsed] Updating parsed resume for candidate {current_candidate.id}")

        if not current_candidate.skills:
            logger.warning(f"⚠️  [Update Parsed] No existing parsed resume for candidate {current_candidate.id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No parsed resume found. Please upload and parse a resume first."
            )

        # Update the skills field with new data
        current_candidate.skills = resume_data
        db.commit()
        db.refresh(current_candidate)

        logger.info(f"✅ [Update Parsed] Successfully updated parsed resume for candidate {current_candidate.id}")

        return {
            "success": True,
            "candidate_id": current_candidate.id,
            "parsed_data": current_candidate.skills,
            "message": "Resume data updated successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Update Parsed] Error updating parsed resume: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update parsed resume"
        )


@router.post("/", response_model=ResumeVersionResponse, status_code=status.HTTP_201_CREATED)
async def create_resume_version(
    file: UploadFile = File(..., description="Resume file (PDF, DOC, DOCX)"),
    name: str = Form(..., description="Resume name"),
    description: Optional[str] = Form(None, description="Resume description"),
    language: ResumeLanguage = Form(default=ResumeLanguage.ENGLISH, description="Resume language"),
    target_position: Optional[str] = Form(None, description="Target position"),
    target_industry: Optional[str] = Form(None, description="Target industry"),
    target_country: Optional[str] = Form(None, description="Target country"),
    is_default: bool = Form(default=False, description="Set as default"),
    is_active: bool = Form(default=True, description="Active status"),
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """Create a new resume version with file upload"""
    try:
        # Validate file type
        allowed_extensions = ['.pdf', '.doc', '.docx']
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in allowed_extensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file type. Allowed: {', '.join(allowed_extensions)}"
            )

        # Generate secure unique filename using UUID (prevents path traversal attacks)
        # Only preserve the file extension, not the original filename
        unique_id = uuid.uuid4().hex
        safe_filename = f"{current_candidate.id}_{unique_id}{file_ext}"
        file_path = RESUME_UPLOAD_DIR / safe_filename
        logger.debug(f"[Resumes] Generated safe filename: {safe_filename} for original: {file.filename}")

        # Save file
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        file_size = file_path.stat().st_size

        # If setting as default, unset other defaults
        if is_default:
            db.query(ResumeVersion).filter(
                ResumeVersion.candidate_id == current_candidate.id,
                ResumeVersion.is_default == True
            ).update({"is_default": False})

        # Create resume version
        resume_version = ResumeVersion(
            candidate_id=current_candidate.id,
            name=name,
            description=description,
            language=language,
            filename=file.filename,
            file_path=str(file_path),
            file_size=file_size,
            target_position=target_position,
            target_industry=target_industry,
            target_country=target_country,
            is_default=is_default,
            is_active=is_active,
            times_used=0
        )

        db.add(resume_version)
        db.commit()
        db.refresh(resume_version)

        logger.info(f"Created resume version {resume_version.id} for candidate {current_candidate.id}")
        return resume_version

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating resume version: {str(e)}")
        db.rollback()
        # Clean up file if database operation failed
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create resume version"
        )


@router.get("/", response_model=ResumeVersionListResponse)
def list_resume_versions(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    language: Optional[ResumeLanguage] = None,
    target_position: Optional[str] = None,
    target_country: Optional[str] = None,
    is_active: Optional[bool] = None,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """List all resume versions for the current candidate"""
    logger.debug(f"[Resumes] Listing resumes for candidate {current_candidate.id}, skip={skip}, limit={limit}")

    query = db.query(ResumeVersion).filter(
        ResumeVersion.candidate_id == current_candidate.id,
        ResumeVersion.deleted_at.is_(None)
    )

    # Apply filters
    if language:
        query = query.filter(ResumeVersion.language == language)
    if target_position:
        query = query.filter(ResumeVersion.target_position.ilike(f"%{target_position}%"))
    if target_country:
        query = query.filter(ResumeVersion.target_country.ilike(f"%{target_country}%"))
    if is_active is not None:
        query = query.filter(ResumeVersion.is_active == is_active)

    total = query.count()
    items = query.order_by(ResumeVersion.created_at.desc()).offset(skip).limit(limit).all()

    logger.debug(f"[Resumes] Found {len(items)} resumes (total: {total})")

    return ResumeVersionListResponse(total=total, items=items)


@router.get("/{resume_id}", response_model=ResumeVersionResponse)
def get_resume_version(
    resume_id: int,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """Get a specific resume version"""
    resume = db.query(ResumeVersion).filter(
        ResumeVersion.id == resume_id,
        ResumeVersion.candidate_id == current_candidate.id,
        ResumeVersion.deleted_at.is_(None)
    ).first()

    if not resume:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resume version not found"
        )

    return resume


@router.patch("/{resume_id}", response_model=ResumeVersionResponse)
def update_resume_version(
    resume_id: int,
    resume_update: ResumeVersionUpdate,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """Update a resume version"""
    resume = db.query(ResumeVersion).filter(
        ResumeVersion.id == resume_id,
        ResumeVersion.candidate_id == current_candidate.id,
        ResumeVersion.deleted_at.is_(None)
    ).first()

    if not resume:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resume version not found"
        )

    # If setting as default, unset other defaults
    if resume_update.is_default and not resume.is_default:
        db.query(ResumeVersion).filter(
            ResumeVersion.candidate_id == current_candidate.id,
            ResumeVersion.is_default == True,
            ResumeVersion.id != resume_id
        ).update({"is_default": False})

    # Update fields
    update_data = resume_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(resume, field, value)

    try:
        db.commit()
        db.refresh(resume)
        logger.info(f"[Resumes] Updated resume version {resume_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Resumes] Failed to update resume {resume_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update resume")

    return resume


@router.delete("/{resume_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_resume_version(
    resume_id: int,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """Soft delete a resume version"""
    resume = db.query(ResumeVersion).filter(
        ResumeVersion.id == resume_id,
        ResumeVersion.candidate_id == current_candidate.id,
        ResumeVersion.deleted_at.is_(None)
    ).first()

    if not resume:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resume version not found"
        )

    # Soft delete
    from datetime import datetime
    resume.deleted_at = datetime.now(timezone.utc)

    try:
        db.commit()
        logger.info(f"[Resumes] Soft deleted resume version {resume_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Resumes] Failed to delete resume {resume_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete resume")

    return None


@router.post("/{resume_id}/set-default", response_model=ResumeVersionResponse)
def set_default_resume(
    resume_id: int,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """Set a resume version as default"""
    resume = db.query(ResumeVersion).filter(
        ResumeVersion.id == resume_id,
        ResumeVersion.candidate_id == current_candidate.id,
        ResumeVersion.deleted_at.is_(None)
    ).first()

    if not resume:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resume version not found"
        )

    # Unset other defaults
    db.query(ResumeVersion).filter(
        ResumeVersion.candidate_id == current_candidate.id,
        ResumeVersion.is_default == True
    ).update({"is_default": False})

    # Set this as default
    resume.is_default = True

    try:
        db.commit()
        db.refresh(resume)
        logger.info(f"[Resumes] Set resume version {resume_id} as default")
    except Exception as e:
        db.rollback()
        logger.error(f"[Resumes] Failed to set resume {resume_id} as default: {e}")
        raise HTTPException(status_code=500, detail="Failed to set default resume")

    return resume
