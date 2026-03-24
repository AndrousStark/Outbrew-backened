"""
Document Management Endpoints

Handles:
- Resume upload, parsing, listing, viewing
- Company/Service Info Doc upload, parsing, listing, viewing
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status, Query
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from typing import List, Optional
import logging
import os
import shutil
from datetime import datetime, timezone
import hashlib
import mimetypes

from app.core.database_async import get_async_db
from app.core.auth import get_current_candidate
from app.core.config import settings
from app.models.candidate import Candidate
from app.models.resume import ResumeVersion
from app.models.documents import ParsedResume, CompanyInfoDoc
from app.services.resume_parser import IntelligentResumeParser
from app.services.info_doc_parser import IntelligentInfoDocParser

logger = logging.getLogger(__name__)

router = APIRouter()

# File upload settings
UPLOAD_DIR = "uploads"
RESUME_DIR = os.path.join(UPLOAD_DIR, "resumes")
INFO_DOC_DIR = os.path.join(UPLOAD_DIR, "info_docs")
MAX_FILE_SIZE = settings.max_file_size_bytes  # From config (default 10MB)
ALLOWED_RESUME_TYPES = [".pdf", ".docx", ".doc"]
ALLOWED_INFO_DOC_TYPES = [".pdf", ".docx", ".doc", ".pptx", ".ppt"]

# Magic bytes for file type validation (prevents extension spoofing)
MAGIC_BYTES = {
    ".pdf": [b"%PDF"],
    ".docx": [b"PK\x03\x04"],  # ZIP-based Office format
    ".doc": [b"\xd0\xcf\x11\xe0"],  # OLE2 compound document
    ".pptx": [b"PK\x03\x04"],  # ZIP-based Office format
    ".ppt": [b"\xd0\xcf\x11\xe0"],  # OLE2 compound document
}


def _validate_magic_bytes(content: bytes, file_ext: str) -> bool:
    """Check file content starts with expected magic bytes for the extension."""
    expected = MAGIC_BYTES.get(file_ext, [])
    if not expected:
        return True  # No magic bytes defined for this extension
    return any(content[:len(sig)] == sig for sig in expected)

# Ensure directories exist
os.makedirs(RESUME_DIR, exist_ok=True)
os.makedirs(INFO_DOC_DIR, exist_ok=True)


# ==================== RESUME ENDPOINTS ====================

@router.post("/resumes/upload")
async def upload_resume(
    file: UploadFile = File(...),
    name: Optional[str] = None,
    description: Optional[str] = None,
    target_position: Optional[str] = None,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Upload and parse a resume

    - Accepts PDF, DOCX, DOC
    - Max size: 10MB
    - Automatically parses skills, experience, education
    - Returns parsed data + confidence score
    """
    try:
        # Validate file type
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in ALLOWED_RESUME_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Allowed: {', '.join(ALLOWED_RESUME_TYPES)}"
            )

        # Read file content
        content = await file.read()
        file_size = len(content)

        # Validate magic bytes (prevents extension spoofing)
        if not _validate_magic_bytes(content, file_ext):
            raise HTTPException(
                status_code=400,
                detail=f"File content does not match expected format for {file_ext}"
            )

        # Validate file size
        if file_size > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"File too large. Max size: {MAX_FILE_SIZE / 1024 / 1024}MB"
            )

        # Generate unique filename
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_hash = hashlib.md5(content).hexdigest()[:8]
        safe_filename = f"{current_candidate.id}_{timestamp}_{file_hash}{file_ext}"
        file_path = os.path.join(RESUME_DIR, safe_filename)

        # Save file
        with open(file_path, "wb") as f:
            f.write(content)

        logger.info(f"📄 [Resume] Saved resume: {file_path} ({file_size} bytes)")

        # Create ResumeVersion record
        resume_version = ResumeVersion(
            candidate_id=current_candidate.id,
            name=name or file.filename,
            description=description,
            filename=file.filename,
            file_path=file_path,
            file_size=file_size,
            target_position=target_position,
            is_active=True
        )

        db.add(resume_version)
        await db.flush()
        await db.refresh(resume_version)

        # Parse resume
        logger.info(f"🔍 [Resume] Parsing resume ID {resume_version.id}")
        parser = IntelligentResumeParser()

        try:
            # parse_file takes bytes content and filename (not file path)
            parsed_result = parser.parse_file(content, file.filename)
            parsed_data = parsed_result.to_dict() if hasattr(parsed_result, 'to_dict') else parsed_result

            # Extract contact info from nested structure
            contact = parsed_data.get("contact", {})
            skills_categorized = parsed_data.get("skills_categorized", {})

            # Create ParsedResume record
            parsed_resume = ParsedResume(
                candidate_id=current_candidate.id,
                resume_version_id=resume_version.id,
                name=parsed_data.get("name"),
                email=contact.get("email") if contact else parsed_data.get("email"),
                phone=contact.get("phone") if contact else parsed_data.get("phone"),
                location=contact.get("location") if contact else parsed_data.get("location"),
                linkedin_url=contact.get("linkedin") if contact else parsed_data.get("linkedin_url"),
                github_url=contact.get("github") if contact else parsed_data.get("github_url"),
                portfolio_url=contact.get("portfolio") if contact else parsed_data.get("portfolio_url"),
                professional_summary=parsed_data.get("summary") or parsed_data.get("professional_summary"),
                years_of_experience=parsed_data.get("years_of_experience"),
                technical_skills=skills_categorized.get("languages", []) + skills_categorized.get("frameworks", []) + skills_categorized.get("tools", []) if skills_categorized else parsed_data.get("skills_raw", []),
                soft_skills=skills_categorized.get("soft_skills", []) if skills_categorized else parsed_data.get("soft_skills", []),
                languages_spoken=parsed_data.get("languages", []),
                certifications=parsed_data.get("certifications", []),
                work_experience=parsed_data.get("experience", []),
                education=parsed_data.get("education", []),
                projects=parsed_data.get("projects", []),
                publications=parsed_data.get("publications", []),
                patents=parsed_data.get("patents", []),
                achievements=parsed_data.get("achievements", []),
                awards=parsed_data.get("awards", []),
                parsing_confidence_score=parsed_data.get("confidence_score", 0),
                total_pages=parsed_data.get("total_pages"),
                word_count=parsed_data.get("word_count")
            )

            db.add(parsed_resume)
            await db.commit()
            await db.refresh(parsed_resume)

            logger.info(
                f"✅ [Resume] Parsed successfully! "
                f"Skills: {len(parsed_resume.technical_skills or [])}, "
                f"Experience: {len(parsed_resume.work_experience or [])}, "
                f"Confidence: {parsed_resume.parsing_confidence_score}%"
            )

            return {
                "success": True,
                "message": "Resume uploaded and parsed successfully",
                "resume_id": resume_version.id,
                "parsed_resume_id": parsed_resume.id,
                "parsed_data": {
                    "name": parsed_resume.name,
                    "email": parsed_resume.email,
                    "phone": parsed_resume.phone,
                    "technical_skills": parsed_resume.technical_skills,
                    "work_experience": parsed_resume.work_experience,
                    "education": parsed_resume.education,
                    "projects": parsed_resume.projects,
                    "confidence_score": parsed_resume.parsing_confidence_score
                }
            }

        except Exception as parse_error:
            logger.error(f"❌ [Resume] Parsing failed: {parse_error}")
            # Still save the resume version even if parsing fails
            await db.commit()

            return {
                "success": True,
                "message": "Resume uploaded but parsing failed",
                "resume_id": resume_version.id,
                "parsed_resume_id": None,
                "parsing_error": str(parse_error)
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Resume] Upload error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upload resume"
        )


@router.get("/resumes")
async def list_resumes(
    include_parsed: bool = Query(True, description="Include parsed data"),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """List all resumes for current user"""
    try:
        # Get resume versions
        query = select(ResumeVersion).where(
            and_(
                ResumeVersion.candidate_id == current_candidate.id,
                ResumeVersion.deleted_at.is_(None)
            )
        ).order_by(ResumeVersion.created_at.desc())

        result = await db.execute(query)
        resume_versions = result.scalars().all()

        resumes = []
        for rv in resume_versions:
            resume_data = {
                "id": rv.id,
                "name": rv.name,
                "description": rv.description,
                "filename": rv.filename,
                "file_size": rv.file_size,
                "target_position": rv.target_position,
                "target_industry": rv.target_industry,
                "is_default": rv.is_default,
                "times_used": rv.times_used,
                "last_used_at": rv.last_used_at.isoformat() if rv.last_used_at else None,
                "created_at": rv.created_at.isoformat(),
                "parsed_data": None
            }

            # Get parsed data if requested
            if include_parsed:
                parsed_query = select(ParsedResume).where(
                    ParsedResume.resume_version_id == rv.id
                ).order_by(ParsedResume.parsed_at.desc()).limit(1)

                parsed_result = await db.execute(parsed_query)
                parsed_resume = parsed_result.scalar_one_or_none()

                if parsed_resume:
                    resume_data["parsed_data"] = {
                        "id": parsed_resume.id,
                        "name": parsed_resume.name,
                        "email": parsed_resume.email,
                        "phone": parsed_resume.phone,
                        "location": parsed_resume.location,
                        "linkedin_url": parsed_resume.linkedin_url,
                        "github_url": parsed_resume.github_url,
                        "technical_skills": parsed_resume.technical_skills,
                        "soft_skills": parsed_resume.soft_skills,
                        "work_experience": parsed_resume.work_experience,
                        "education": parsed_resume.education,
                        "projects": parsed_resume.projects,
                        "certifications": parsed_resume.certifications,
                        "achievements": parsed_resume.achievements,
                        "confidence_score": parsed_resume.parsing_confidence_score,
                        "parsed_at": parsed_resume.parsed_at.isoformat()
                    }

            resumes.append(resume_data)

        return {
            "success": True,
            "count": len(resumes),
            "resumes": resumes
        }

    except Exception as e:
        logger.error(f"❌ [Resume] List error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list resumes"
        )


@router.get("/resumes/{resume_id}")
async def get_resume(
    resume_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get single resume with full parsed data"""
    try:
        # Get resume version
        query = select(ResumeVersion).where(
            and_(
                ResumeVersion.id == resume_id,
                ResumeVersion.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        resume_version = result.scalar_one_or_none()

        if not resume_version:
            raise HTTPException(status_code=404, detail="Resume not found")

        # Get parsed data
        parsed_query = select(ParsedResume).where(
            ParsedResume.resume_version_id == resume_id
        ).order_by(ParsedResume.parsed_at.desc()).limit(1)

        parsed_result = await db.execute(parsed_query)
        parsed_resume = parsed_result.scalar_one_or_none()

        return {
            "success": True,
            "resume": {
                "id": resume_version.id,
                "name": resume_version.name,
                "description": resume_version.description,
                "filename": resume_version.filename,
                "file_size": resume_version.file_size,
                "target_position": resume_version.target_position,
                "created_at": resume_version.created_at.isoformat(),
                "parsed_data": {
                    "name": parsed_resume.name,
                    "email": parsed_resume.email,
                    "phone": parsed_resume.phone,
                    "location": parsed_resume.location,
                    "linkedin_url": parsed_resume.linkedin_url,
                    "github_url": parsed_resume.github_url,
                    "portfolio_url": parsed_resume.portfolio_url,
                    "professional_summary": parsed_resume.professional_summary,
                    "years_of_experience": parsed_resume.years_of_experience,
                    "technical_skills": parsed_resume.technical_skills,
                    "soft_skills": parsed_resume.soft_skills,
                    "languages_spoken": parsed_resume.languages_spoken,
                    "certifications": parsed_resume.certifications,
                    "work_experience": parsed_resume.work_experience,
                    "education": parsed_resume.education,
                    "projects": parsed_resume.projects,
                    "publications": parsed_resume.publications,
                    "patents": parsed_resume.patents,
                    "achievements": parsed_resume.achievements,
                    "awards": parsed_resume.awards,
                    "confidence_score": parsed_resume.parsing_confidence_score,
                    "parsed_at": parsed_resume.parsed_at.isoformat()
                } if parsed_resume else None
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Resume] Get error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get resume"
        )


@router.get("/resumes/{resume_id}/download")
async def download_resume(
    resume_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Download the original resume file"""
    try:
        query = select(ResumeVersion).where(
            and_(
                ResumeVersion.id == resume_id,
                ResumeVersion.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        resume_version = result.scalar_one_or_none()

        if not resume_version:
            raise HTTPException(status_code=404, detail="Resume not found")

        file_path = resume_version.file_path

        # Guard against path traversal: resolved path must be under uploads/storage
        resolved = os.path.realpath(file_path)
        base_dir = os.path.realpath("storage")
        uploads_dir = os.path.realpath("uploads")
        if not (resolved.startswith(base_dir + os.sep) or resolved.startswith(uploads_dir + os.sep)):
            raise HTTPException(status_code=403, detail="Access denied")

        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found on disk")

        # Determine media type
        media_type, _ = mimetypes.guess_type(file_path)
        if not media_type:
            media_type = "application/octet-stream"

        return FileResponse(
            path=file_path,
            filename=resume_version.filename,
            media_type=media_type
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Resume] Download error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to download resume"
        )


@router.delete("/resumes/{resume_id}")
async def delete_resume(
    resume_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Soft delete a resume"""
    try:
        query = select(ResumeVersion).where(
            and_(
                ResumeVersion.id == resume_id,
                ResumeVersion.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        resume_version = result.scalar_one_or_none()

        if not resume_version:
            raise HTTPException(status_code=404, detail="Resume not found")

        # Soft delete
        resume_version.deleted_at = datetime.now(timezone.utc)
        await db.commit()

        return {
            "success": True,
            "message": "Resume deleted successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Resume] Delete error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete resume"
        )


@router.put("/resumes/{resume_id}")
async def update_resume(
    resume_id: int,
    data: dict,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Update resume metadata and parsed data

    Allows editing:
    - name, description
    - All parsed data fields (name, email, phone, skills, experience, etc.)
    """
    try:
        # Get resume version
        query = select(ResumeVersion).where(
            and_(
                ResumeVersion.id == resume_id,
                ResumeVersion.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        resume_version = result.scalar_one_or_none()

        if not resume_version:
            raise HTTPException(status_code=404, detail="Resume not found")

        # Update ResumeVersion fields
        if "name" in data:
            resume_version.name = data["name"]
        if "description" in data:
            resume_version.description = data["description"]

        # Update ParsedResume if parsed_data provided
        if "parsed_data" in data and data["parsed_data"]:
            parsed_query = select(ParsedResume).where(
                ParsedResume.resume_version_id == resume_id
            ).order_by(ParsedResume.parsed_at.desc()).limit(1)

            parsed_result = await db.execute(parsed_query)
            parsed_resume = parsed_result.scalar_one_or_none()

            if parsed_resume:
                pd = data["parsed_data"]

                # Update all editable fields
                if "name" in pd:
                    parsed_resume.name = pd["name"]
                if "email" in pd:
                    parsed_resume.email = pd["email"]
                if "phone" in pd:
                    parsed_resume.phone = pd["phone"]
                if "location" in pd:
                    parsed_resume.location = pd["location"]
                if "linkedin_url" in pd:
                    parsed_resume.linkedin_url = pd["linkedin_url"]
                if "github_url" in pd:
                    parsed_resume.github_url = pd["github_url"]
                if "portfolio_url" in pd:
                    parsed_resume.portfolio_url = pd["portfolio_url"]
                if "professional_summary" in pd:
                    parsed_resume.professional_summary = pd["professional_summary"]
                if "years_of_experience" in pd:
                    parsed_resume.years_of_experience = pd["years_of_experience"]
                if "technical_skills" in pd:
                    parsed_resume.technical_skills = pd["technical_skills"]
                if "soft_skills" in pd:
                    parsed_resume.soft_skills = pd["soft_skills"]
                if "languages_spoken" in pd:
                    parsed_resume.languages_spoken = pd["languages_spoken"]
                if "certifications" in pd:
                    parsed_resume.certifications = pd["certifications"]
                if "work_experience" in pd:
                    parsed_resume.work_experience = pd["work_experience"]
                if "education" in pd:
                    parsed_resume.education = pd["education"]
                if "projects" in pd:
                    parsed_resume.projects = pd["projects"]
                if "publications" in pd:
                    parsed_resume.publications = pd["publications"]
                if "achievements" in pd:
                    parsed_resume.achievements = pd["achievements"]
                if "awards" in pd:
                    parsed_resume.awards = pd["awards"]

                logger.info(f"✅ [Resume] Updated parsed data for resume {resume_id}")

        await db.commit()
        await db.refresh(resume_version)

        # Get updated parsed data
        parsed_query = select(ParsedResume).where(
            ParsedResume.resume_version_id == resume_id
        ).order_by(ParsedResume.parsed_at.desc()).limit(1)

        parsed_result = await db.execute(parsed_query)
        parsed_resume = parsed_result.scalar_one_or_none()

        return {
            "success": True,
            "message": "Resume updated successfully",
            "resume": {
                "id": resume_version.id,
                "name": resume_version.name,
                "description": resume_version.description,
                "parsed_data": {
                    "name": parsed_resume.name,
                    "email": parsed_resume.email,
                    "phone": parsed_resume.phone,
                    "location": parsed_resume.location,
                    "linkedin_url": parsed_resume.linkedin_url,
                    "github_url": parsed_resume.github_url,
                    "portfolio_url": parsed_resume.portfolio_url,
                    "professional_summary": parsed_resume.professional_summary,
                    "technical_skills": parsed_resume.technical_skills,
                    "soft_skills": parsed_resume.soft_skills,
                    "work_experience": parsed_resume.work_experience,
                    "education": parsed_resume.education,
                    "projects": parsed_resume.projects,
                    "certifications": parsed_resume.certifications,
                    "achievements": parsed_resume.achievements,
                } if parsed_resume else None
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Resume] Update error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update resume"
        )


@router.post("/resumes/{resume_id}/reparse")
async def reparse_resume(
    resume_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Re-parse a resume using the AI parser

    - Deletes existing parsed data
    - Re-runs the parser on the original file
    - Returns fresh parsed data with new confidence score
    """
    try:
        # Get resume version
        query = select(ResumeVersion).where(
            and_(
                ResumeVersion.id == resume_id,
                ResumeVersion.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        resume_version = result.scalar_one_or_none()

        if not resume_version:
            raise HTTPException(status_code=404, detail="Resume not found")

        # Check file exists
        if not os.path.exists(resume_version.file_path):
            raise HTTPException(status_code=404, detail="Original file not found")

        logger.info(f"🔄 [Resume] Re-parsing resume ID {resume_id}")

        # Read file content
        with open(resume_version.file_path, "rb") as f:
            file_content = f.read()

        # Parse resume - parse_file takes bytes content and filename
        parser = IntelligentResumeParser()
        parse_result = parser.parse_file(file_content, resume_version.filename)
        parsed_data = parse_result.to_dict() if hasattr(parse_result, 'to_dict') else parse_result

        # Extract contact info from nested structure
        contact = parsed_data.get("contact", {})
        skills_categorized = parsed_data.get("skills_categorized", {})

        # Update or create ParsedResume record
        parsed_query = select(ParsedResume).where(
            ParsedResume.resume_version_id == resume_id
        ).order_by(ParsedResume.parsed_at.desc()).limit(1)

        db_result = await db.execute(parsed_query)
        parsed_resume = db_result.scalar_one_or_none()

        # Helper to get technical skills from categorized
        tech_skills = (skills_categorized.get("languages", []) +
                      skills_categorized.get("frameworks", []) +
                      skills_categorized.get("tools", [])) if skills_categorized else parsed_data.get("skills_raw", [])

        if parsed_resume:
            # Update existing
            parsed_resume.name = parsed_data.get("name")
            parsed_resume.email = contact.get("email") if contact else parsed_data.get("email")
            parsed_resume.phone = contact.get("phone") if contact else parsed_data.get("phone")
            parsed_resume.location = contact.get("location") if contact else parsed_data.get("location")
            parsed_resume.linkedin_url = contact.get("linkedin") if contact else parsed_data.get("linkedin_url")
            parsed_resume.github_url = contact.get("github") if contact else parsed_data.get("github_url")
            parsed_resume.portfolio_url = contact.get("portfolio") if contact else parsed_data.get("portfolio_url")
            parsed_resume.professional_summary = parsed_data.get("summary") or parsed_data.get("professional_summary")
            parsed_resume.years_of_experience = parsed_data.get("years_of_experience")
            parsed_resume.technical_skills = tech_skills
            parsed_resume.soft_skills = skills_categorized.get("soft_skills", []) if skills_categorized else parsed_data.get("soft_skills", [])
            parsed_resume.languages_spoken = parsed_data.get("languages", [])
            parsed_resume.certifications = parsed_data.get("certifications", [])
            parsed_resume.work_experience = parsed_data.get("experience", [])
            parsed_resume.education = parsed_data.get("education", [])
            parsed_resume.projects = parsed_data.get("projects", [])
            parsed_resume.publications = parsed_data.get("publications", [])
            parsed_resume.patents = parsed_data.get("patents", [])
            parsed_resume.achievements = parsed_data.get("achievements", [])
            parsed_resume.awards = parsed_data.get("awards", [])
            parsed_resume.parsing_confidence_score = parsed_data.get("confidence_score", 0)
            parsed_resume.total_pages = parsed_data.get("total_pages")
            parsed_resume.word_count = parsed_data.get("word_count")
            parsed_resume.parsed_at = datetime.now(timezone.utc)
        else:
            # Create new
            parsed_resume = ParsedResume(
                candidate_id=current_candidate.id,
                resume_version_id=resume_version.id,
                name=parsed_data.get("name"),
                email=contact.get("email") if contact else parsed_data.get("email"),
                phone=contact.get("phone") if contact else parsed_data.get("phone"),
                location=contact.get("location") if contact else parsed_data.get("location"),
                linkedin_url=contact.get("linkedin") if contact else parsed_data.get("linkedin_url"),
                github_url=contact.get("github") if contact else parsed_data.get("github_url"),
                portfolio_url=contact.get("portfolio") if contact else parsed_data.get("portfolio_url"),
                professional_summary=parsed_data.get("summary") or parsed_data.get("professional_summary"),
                years_of_experience=parsed_data.get("years_of_experience"),
                technical_skills=tech_skills,
                soft_skills=skills_categorized.get("soft_skills", []) if skills_categorized else parsed_data.get("soft_skills", []),
                languages_spoken=parsed_data.get("languages", []),
                certifications=parsed_data.get("certifications", []),
                work_experience=parsed_data.get("experience", []),
                education=parsed_data.get("education", []),
                projects=parsed_data.get("projects", []),
                publications=parsed_data.get("publications", []),
                patents=parsed_data.get("patents", []),
                achievements=parsed_data.get("achievements", []),
                awards=parsed_data.get("awards", []),
                parsing_confidence_score=parsed_data.get("confidence_score", 0),
                total_pages=parsed_data.get("total_pages"),
                word_count=parsed_data.get("word_count")
            )
            db.add(parsed_resume)

        await db.commit()
        await db.refresh(parsed_resume)

        logger.info(
            f"✅ [Resume] Re-parsed successfully! "
            f"Skills: {len(parsed_resume.technical_skills or [])}, "
            f"Experience: {len(parsed_resume.work_experience or [])}, "
            f"Confidence: {parsed_resume.parsing_confidence_score}%"
        )

        return {
            "success": True,
            "message": "Resume re-parsed successfully",
            "parsed_data": {
                "name": parsed_resume.name,
                "email": parsed_resume.email,
                "phone": parsed_resume.phone,
                "location": parsed_resume.location,
                "linkedin_url": parsed_resume.linkedin_url,
                "github_url": parsed_resume.github_url,
                "technical_skills": parsed_resume.technical_skills,
                "soft_skills": parsed_resume.soft_skills,
                "work_experience": parsed_resume.work_experience,
                "education": parsed_resume.education,
                "projects": parsed_resume.projects,
                "confidence_score": parsed_resume.parsing_confidence_score,
                "parsed_at": parsed_resume.parsed_at.isoformat()
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Resume] Re-parse error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to re-parse resume"
        )


# ==================== COMPANY INFO DOC ENDPOINTS ====================

@router.post("/info-docs/upload")
async def upload_info_doc(
    file: UploadFile = File(...),
    name: str = Query(..., description="Document name"),
    description: Optional[str] = None,
    doc_type: str = Query("company", description="Type: product, service, company, portfolio"),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Upload company/service info document

    - Accepts PDF, DOCX, DOC, PPTX, PPT
    - Max size: 10MB
    - Stores for later use in marketing/sales emails
    """
    try:
        # Validate file type
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in ALLOWED_INFO_DOC_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Allowed: {', '.join(ALLOWED_INFO_DOC_TYPES)}"
            )

        # Read file content
        content = await file.read()
        file_size = len(content)

        # Validate magic bytes (prevents extension spoofing)
        if not _validate_magic_bytes(content, file_ext):
            raise HTTPException(
                status_code=400,
                detail=f"File content does not match expected format for {file_ext}"
            )

        # Validate file size
        if file_size > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"File too large. Max size: {MAX_FILE_SIZE / 1024 / 1024}MB"
            )

        # Generate unique filename
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_hash = hashlib.md5(content).hexdigest()[:8]
        safe_filename = f"{current_candidate.id}_{timestamp}_{file_hash}{file_ext}"
        file_path = os.path.join(INFO_DOC_DIR, safe_filename)

        # Save file
        with open(file_path, "wb") as f:
            f.write(content)

        logger.info(f"📄 [InfoDoc] Saved info doc: {file_path} ({file_size} bytes)")

        # Create CompanyInfoDoc record
        info_doc = CompanyInfoDoc(
            candidate_id=current_candidate.id,
            name=name,
            description=description,
            doc_type=doc_type,
            filename=file.filename,
            file_path=file_path,
            file_size=file_size,
            is_active=True
        )

        db.add(info_doc)
        await db.commit()
        await db.refresh(info_doc)

        logger.info(f"✅ [InfoDoc] Info doc saved! ID: {info_doc.id}")

        # Parse info doc with IntelligentInfoDocParser
        logger.info(f"🧠 [InfoDoc] Starting AI-powered parsing...")
        parser = IntelligentInfoDocParser(fuzzy_threshold=60)
        parsed_data = parser.parse_file(content, file.filename)

        # Update info_doc with parsed data
        info_doc.company_name = parsed_data.company_name
        info_doc.tagline = parsed_data.tagline
        info_doc.industry = parsed_data.industry

        # Store structured data as JSON
        info_doc.products_services = [
            {
                "name": p.name,
                "description": p.description,
                "pricing": p.pricing,
                "features": p.features,
                "category": p.category
            }
            for p in parsed_data.products_services
        ]

        info_doc.key_benefits = parsed_data.key_benefits
        info_doc.unique_selling_points = parsed_data.unique_selling_points
        info_doc.problem_solved = parsed_data.problem_solved

        info_doc.pricing_tiers = [
            {
                "name": t.name,
                "price": t.price,
                "billing_cycle": t.billing_cycle,
                "currency": t.currency,
                "features": t.features,
                "is_popular": t.is_popular,
                "is_enterprise": t.is_enterprise
            }
            for t in parsed_data.pricing_tiers
        ]

        info_doc.contact_info = {
            "emails": parsed_data.contact_info.emails,
            "phones": parsed_data.contact_info.phones,
            "websites": parsed_data.contact_info.websites,
            "social_media": parsed_data.contact_info.social_media,
            "address": parsed_data.contact_info.address
        }

        info_doc.team_members = [
            {
                "name": m.name,
                "role": m.role,
                "title": m.title,
                "email": m.email,
                "linkedin": m.linkedin
            }
            for m in parsed_data.team_members
        ]

        info_doc.parsing_confidence_score = parsed_data.confidence_score
        info_doc.word_count = parsed_data.word_count

        await db.commit()
        await db.refresh(info_doc)

        logger.info(f"✅ [InfoDoc] Parsing complete! Confidence: {parsed_data.confidence_score:.1f}%")

        return {
            "success": True,
            "message": "Info document uploaded and parsed successfully",
            "info_doc_id": info_doc.id,
            "name": info_doc.name,
            "doc_type": info_doc.doc_type,
            "parsed_data": {
                "company_name": parsed_data.company_name,
                "tagline": parsed_data.tagline,
                "industry": parsed_data.industry,
                "products_services_count": len(parsed_data.products_services),
                "benefits_count": len(parsed_data.key_benefits),
                "usp_count": len(parsed_data.unique_selling_points),
                "pricing_tiers_count": len(parsed_data.pricing_tiers),
                "team_members_count": len(parsed_data.team_members),
                "confidence_score": parsed_data.confidence_score,
                "warnings": parsed_data.warnings
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [InfoDoc] Upload error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upload info document"
        )


@router.get("/info-docs")
async def list_info_docs(
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """List all company/service info documents"""
    try:
        query = select(CompanyInfoDoc).where(
            and_(
                CompanyInfoDoc.candidate_id == current_candidate.id,
                CompanyInfoDoc.is_active == True
            )
        ).order_by(CompanyInfoDoc.created_at.desc())

        result = await db.execute(query)
        info_docs = result.scalars().all()

        docs = []
        for doc in info_docs:
            docs.append({
                "id": doc.id,
                "name": doc.name,
                "description": doc.description,
                "doc_type": doc.doc_type,
                "filename": doc.filename,
                "file_size": doc.file_size,
                "company_name": doc.company_name,
                "tagline": doc.tagline,
                "industry": doc.industry,
                "is_default": doc.is_default,
                "times_used": doc.times_used,
                "parsing_confidence_score": doc.parsing_confidence_score,
                "products_services_count": len(doc.products_services) if doc.products_services else 0,
                "last_used_at": doc.last_used_at.isoformat() if doc.last_used_at else None,
                "created_at": doc.created_at.isoformat()
            })

        return {
            "success": True,
            "count": len(docs),
            "info_docs": docs
        }

    except Exception as e:
        logger.error(f"❌ [InfoDoc] List error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list info docs"
        )


@router.get("/info-docs/{doc_id}")
async def get_info_doc(
    doc_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get single info doc with full details"""
    try:
        query = select(CompanyInfoDoc).where(
            and_(
                CompanyInfoDoc.id == doc_id,
                CompanyInfoDoc.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        info_doc = result.scalar_one_or_none()

        if not info_doc:
            raise HTTPException(status_code=404, detail="Info document not found")

        return {
            "success": True,
            "info_doc": {
                "id": info_doc.id,
                "name": info_doc.name,
                "description": info_doc.description,
                "doc_type": info_doc.doc_type,
                "filename": info_doc.filename,
                "file_size": info_doc.file_size,
                "company_name": info_doc.company_name,
                "tagline": info_doc.tagline,
                "industry": info_doc.industry,
                "products_services": info_doc.products_services,
                "key_benefits": info_doc.key_benefits,
                "unique_selling_points": info_doc.unique_selling_points,
                "problem_solved": info_doc.problem_solved,
                "pricing_tiers": info_doc.pricing_tiers,
                "contact_info": info_doc.contact_info,
                "team_members": info_doc.team_members,
                "parsing_confidence_score": info_doc.parsing_confidence_score,
                "word_count": info_doc.word_count,
                "is_default": info_doc.is_default,
                "times_used": info_doc.times_used,
                "created_at": info_doc.created_at.isoformat()
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [InfoDoc] Get error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get info doc"
        )


@router.get("/info-docs/{doc_id}/download")
async def download_info_doc(
    doc_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Download the original info doc file"""
    try:
        query = select(CompanyInfoDoc).where(
            and_(
                CompanyInfoDoc.id == doc_id,
                CompanyInfoDoc.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        info_doc = result.scalar_one_or_none()

        if not info_doc:
            raise HTTPException(status_code=404, detail="Info document not found")

        file_path = info_doc.file_path
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found on disk")

        # Determine media type
        media_type, _ = mimetypes.guess_type(file_path)
        if not media_type:
            media_type = "application/octet-stream"

        return FileResponse(
            path=file_path,
            filename=info_doc.filename,
            media_type=media_type
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [InfoDoc] Download error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to download info doc"
        )


@router.delete("/info-docs/{doc_id}")
async def delete_info_doc(
    doc_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Delete an info document"""
    try:
        query = select(CompanyInfoDoc).where(
            and_(
                CompanyInfoDoc.id == doc_id,
                CompanyInfoDoc.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        info_doc = result.scalar_one_or_none()

        if not info_doc:
            raise HTTPException(status_code=404, detail="Info document not found")

        # Soft delete
        info_doc.is_active = False
        await db.commit()

        return {
            "success": True,
            "message": "Info document deleted successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [InfoDoc] Delete error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete info doc"
        )


@router.put("/info-docs/{doc_id}")
async def update_info_doc(
    doc_id: int,
    data: dict,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Update info document data

    Allows editing all parsed fields:
    - company_name, tagline, industry
    - products_services, key_benefits, unique_selling_points
    - problem_solved, pricing_tiers, contact_info, team_members
    """
    try:
        query = select(CompanyInfoDoc).where(
            and_(
                CompanyInfoDoc.id == doc_id,
                CompanyInfoDoc.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        info_doc = result.scalar_one_or_none()

        if not info_doc:
            raise HTTPException(status_code=404, detail="Info document not found")

        # Update all provided fields
        if "name" in data:
            info_doc.name = data["name"]
        if "description" in data:
            info_doc.description = data["description"]
        if "company_name" in data:
            info_doc.company_name = data["company_name"]
        if "tagline" in data:
            info_doc.tagline = data["tagline"]
        if "industry" in data:
            info_doc.industry = data["industry"]
        if "products_services" in data:
            info_doc.products_services = data["products_services"]
        if "key_benefits" in data:
            info_doc.key_benefits = data["key_benefits"]
        if "unique_selling_points" in data:
            info_doc.unique_selling_points = data["unique_selling_points"]
        if "problem_solved" in data:
            info_doc.problem_solved = data["problem_solved"]
        if "pricing_tiers" in data:
            info_doc.pricing_tiers = data["pricing_tiers"]
        if "contact_info" in data:
            info_doc.contact_info = data["contact_info"]
        if "team_members" in data:
            info_doc.team_members = data["team_members"]

        await db.commit()
        await db.refresh(info_doc)

        logger.info(f"✅ [InfoDoc] Updated info doc {doc_id}")

        return {
            "success": True,
            "message": "Info document updated successfully",
            "info_doc": {
                "id": info_doc.id,
                "name": info_doc.name,
                "description": info_doc.description,
                "doc_type": info_doc.doc_type,
                "company_name": info_doc.company_name,
                "tagline": info_doc.tagline,
                "industry": info_doc.industry,
                "products_services": info_doc.products_services,
                "key_benefits": info_doc.key_benefits,
                "unique_selling_points": info_doc.unique_selling_points,
                "problem_solved": info_doc.problem_solved,
                "pricing_tiers": info_doc.pricing_tiers,
                "contact_info": info_doc.contact_info,
                "team_members": info_doc.team_members,
                "parsing_confidence_score": info_doc.parsing_confidence_score,
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [InfoDoc] Update error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update info doc"
        )


@router.post("/info-docs/{doc_id}/reparse")
async def reparse_info_doc(
    doc_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Re-parse an info document using the AI parser

    - Re-runs the parser on the original file
    - Returns fresh parsed data with new confidence score
    """
    try:
        # Get info doc
        query = select(CompanyInfoDoc).where(
            and_(
                CompanyInfoDoc.id == doc_id,
                CompanyInfoDoc.candidate_id == current_candidate.id
            )
        )

        result = await db.execute(query)
        info_doc = result.scalar_one_or_none()

        if not info_doc:
            raise HTTPException(status_code=404, detail="Info document not found")

        # Check file exists
        if not os.path.exists(info_doc.file_path):
            raise HTTPException(status_code=404, detail="Original file not found")

        logger.info(f"🔄 [InfoDoc] Re-parsing info doc ID {doc_id}")

        # Read file content
        with open(info_doc.file_path, "rb") as f:
            content = f.read()

        # Parse info doc
        parser = IntelligentInfoDocParser(fuzzy_threshold=60)
        parsed_data = parser.parse_file(content, info_doc.filename)

        # Update info_doc with parsed data
        info_doc.company_name = parsed_data.company_name
        info_doc.tagline = parsed_data.tagline
        info_doc.industry = parsed_data.industry

        # Store structured data as JSON
        info_doc.products_services = [
            {
                "name": p.name,
                "description": p.description,
                "pricing": p.pricing,
                "features": p.features,
                "category": p.category
            }
            for p in parsed_data.products_services
        ]

        info_doc.key_benefits = parsed_data.key_benefits
        info_doc.unique_selling_points = parsed_data.unique_selling_points
        info_doc.problem_solved = parsed_data.problem_solved

        info_doc.pricing_tiers = [
            {
                "name": t.name,
                "price": t.price,
                "billing_cycle": t.billing_cycle,
                "currency": t.currency,
                "features": t.features,
                "is_popular": t.is_popular,
                "is_enterprise": t.is_enterprise
            }
            for t in parsed_data.pricing_tiers
        ]

        info_doc.contact_info = {
            "emails": parsed_data.contact_info.emails,
            "phones": parsed_data.contact_info.phones,
            "websites": parsed_data.contact_info.websites,
            "social_media": parsed_data.contact_info.social_media,
            "address": parsed_data.contact_info.address
        }

        info_doc.team_members = [
            {
                "name": m.name,
                "role": m.role,
                "title": m.title,
                "email": m.email,
                "linkedin": m.linkedin
            }
            for m in parsed_data.team_members
        ]

        info_doc.parsing_confidence_score = parsed_data.confidence_score
        info_doc.word_count = parsed_data.word_count

        await db.commit()
        await db.refresh(info_doc)

        logger.info(f"✅ [InfoDoc] Re-parsed successfully! Confidence: {parsed_data.confidence_score:.1f}%")

        return {
            "success": True,
            "message": "Info document re-parsed successfully",
            "parsed_data": {
                "company_name": info_doc.company_name,
                "tagline": info_doc.tagline,
                "industry": info_doc.industry,
                "products_services_count": len(info_doc.products_services or []),
                "benefits_count": len(info_doc.key_benefits or []),
                "usp_count": len(info_doc.unique_selling_points or []),
                "pricing_tiers_count": len(info_doc.pricing_tiers or []),
                "team_members_count": len(info_doc.team_members or []),
                "confidence_score": info_doc.parsing_confidence_score,
                "warnings": parsed_data.warnings
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [InfoDoc] Re-parse error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to re-parse info doc"
        )
