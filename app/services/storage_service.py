"""
Storage Management Service

Handles file uploads, quota enforcement, organized directory structure,
and file lifecycle management for the application.
"""
import os
import uuid
import hashlib
import mimetypes
from pathlib import Path
from typing import Optional, List, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from fastapi import UploadFile, HTTPException

from app.core.config import settings
from app.models.email_inbox import StorageQuota
from app.models.resume import ResumeVersion


class StorageService:
    """Centralized storage management service"""

    def __init__(self, db: Session):
        self.db = db
        self.base_path = Path(settings.STORAGE_BASE_PATH)
        self._ensure_base_directories()

    def _ensure_base_directories(self):
        """Create base storage directories if they don't exist"""
        directories = [
            self.base_path,
            self.base_path / "documents",
            self.base_path / "emails",
            self.base_path / "templates",
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def _get_candidate_paths(self, candidate_id: int) -> dict:
        """Get organized directory paths for a candidate"""
        candidate_base = self.base_path / "documents" / f"candidate_{candidate_id}"
        return {
            "base": candidate_base,
            "resumes": candidate_base / "resumes",
            "attachments": candidate_base / "attachments",
            "other": candidate_base / "other",
        }

    def _ensure_candidate_directories(self, candidate_id: int):
        """Create candidate-specific directories"""
        paths = self._get_candidate_paths(candidate_id)
        for path in paths.values():
            path.mkdir(parents=True, exist_ok=True)

    def _validate_file_extension(
        self, filename: str, allowed_extensions: List[str]
    ) -> bool:
        """Validate file extension"""
        file_ext = Path(filename).suffix.lower()
        return file_ext in [ext.lower() for ext in allowed_extensions]

    def _validate_file_size(self, file_size: int) -> bool:
        """Validate file size against maximum limit"""
        return file_size <= settings.max_file_size_bytes

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file for integrity checking"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _detect_mime_type(self, file_path: Path) -> Optional[str]:
        """Detect MIME type using python-magic or mimetypes"""
        # Try to guess mime type
        mime_type, _ = mimetypes.guess_type(str(file_path))
        return mime_type

    def get_or_create_storage_quota(
        self, candidate_id: int
    ) -> StorageQuota:
        """Get existing quota or create new one with defaults"""
        quota = (
            self.db.query(StorageQuota)
            .filter(StorageQuota.candidate_id == candidate_id)
            .first()
        )

        if not quota:
            quota = StorageQuota(
                candidate_id=candidate_id,
                quota_limit=settings.max_storage_quota_bytes,
                used_bytes=0,
                resumes_bytes=0,
                emails_bytes=0,
                documents_bytes=0,
                templates_bytes=0,
                total_files=0,
                total_emails_archived=0,
                last_calculated_at=datetime.now(timezone.utc),
            )
            self.db.add(quota)
            self.db.commit()
            self.db.refresh(quota)

        return quota

    def check_quota_available(
        self, candidate_id: int, file_size: int
    ) -> Tuple[bool, StorageQuota]:
        """Check if candidate has enough quota for the file"""
        quota = self.get_or_create_storage_quota(candidate_id)
        available = (quota.quota_limit - quota.used_bytes) >= file_size
        return available, quota

    def update_quota_usage(
        self,
        candidate_id: int,
        file_size: int,
        file_category: str = "documents",
        increment_file_count: bool = True,
    ):
        """Update storage quota after file upload/delete"""
        quota = self.get_or_create_storage_quota(candidate_id)

        quota.used_bytes += file_size

        if file_category == "resumes":
            quota.resumes_bytes += file_size
        elif file_category == "emails":
            quota.emails_bytes += file_size
        elif file_category == "documents":
            quota.documents_bytes += file_size
        elif file_category == "templates":
            quota.templates_bytes += file_size

        if increment_file_count:
            quota.total_files += 1

        quota.last_calculated_at = datetime.now(timezone.utc)
        self.db.commit()

    def upload_resume(
        self,
        candidate_id: int,
        file: UploadFile,
        version_name: Optional[str] = None,
    ) -> Tuple[str, int]:
        """
        Upload resume file with validation and quota checking

        Returns:
            Tuple[str, int]: (file_path, file_size)
        """
        # Validate extension
        if not self._validate_file_extension(
            file.filename, settings.ALLOWED_RESUME_EXTENSIONS
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Allowed: {', '.join(settings.ALLOWED_RESUME_EXTENSIONS)}",
            )

        # Read file content
        file_content = file.file.read()
        file_size = len(file_content)

        # Validate size
        if not self._validate_file_size(file_size):
            raise HTTPException(
                status_code=400,
                detail=f"File too large. Maximum size: {settings.MAX_FILE_SIZE_MB}MB",
            )

        # Check quota
        has_quota, quota = self.check_quota_available(candidate_id, file_size)
        if not has_quota:
            raise HTTPException(
                status_code=400,
                detail=f"Storage quota exceeded. Used: {quota.used_bytes / (1024*1024):.2f}MB / {quota.quota_limit / (1024*1024):.2f}MB",
            )

        # Ensure directories
        self._ensure_candidate_directories(candidate_id)
        paths = self._get_candidate_paths(candidate_id)

        # Generate secure filename
        file_ext = Path(file.filename).suffix
        secure_filename = f"{candidate_id}_{uuid.uuid4()}{file_ext}"
        file_path = paths["resumes"] / secure_filename

        # Write file
        with open(file_path, "wb") as f:
            f.write(file_content)

        # Calculate hash for integrity
        file_hash = self._calculate_file_hash(file_path)

        # Update quota
        self.update_quota_usage(candidate_id, file_size, "resumes")

        return str(file_path), file_size

    def upload_attachment(
        self,
        candidate_id: int,
        application_id: int,
        file: UploadFile,
        attachment_type: str = "other",
    ) -> Tuple[str, int, str]:
        """
        Upload application attachment with validation

        Returns:
            Tuple[str, int, str]: (file_path, file_size, file_type)
        """
        # Validate extension
        if not self._validate_file_extension(
            file.filename, settings.ALLOWED_ATTACHMENT_EXTENSIONS
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Allowed: {', '.join(settings.ALLOWED_ATTACHMENT_EXTENSIONS)}",
            )

        # Read file content
        file_content = file.file.read()
        file_size = len(file_content)

        # Validate size
        if not self._validate_file_size(file_size):
            raise HTTPException(
                status_code=400,
                detail=f"File too large. Maximum size: {settings.MAX_FILE_SIZE_MB}MB",
            )

        # Check quota
        has_quota, quota = self.check_quota_available(candidate_id, file_size)
        if not has_quota:
            raise HTTPException(
                status_code=400,
                detail=f"Storage quota exceeded. Used: {quota.used_bytes / (1024*1024):.2f}MB / {quota.quota_limit / (1024*1024):.2f}MB",
            )

        # Ensure directories
        self._ensure_candidate_directories(candidate_id)
        paths = self._get_candidate_paths(candidate_id)

        # Generate secure filename
        file_ext = Path(file.filename).suffix
        secure_filename = f"app_{application_id}_{uuid.uuid4()}{file_ext}"
        file_path = paths["attachments"] / secure_filename

        # Write file
        with open(file_path, "wb") as f:
            f.write(file_content)

        # Update quota
        self.update_quota_usage(candidate_id, file_size, "documents")

        return str(file_path), file_size, file_ext.lstrip(".")

    def delete_file(
        self, file_path: str, candidate_id: int, file_category: str = "documents"
    ) -> bool:
        """
        Delete file and update quota

        Args:
            file_path: Full path to file
            candidate_id: Candidate ID for quota update
            file_category: Category (resumes, emails, documents, templates)

        Returns:
            bool: True if deleted successfully
        """
        path = Path(file_path)

        if not path.exists():
            return False

        # Get file size before deletion
        file_size = path.stat().st_size

        # Delete file
        path.unlink()

        # Update quota (negative value to decrease)
        self.update_quota_usage(
            candidate_id, -file_size, file_category, increment_file_count=False
        )

        # Decrement file count
        quota = self.get_or_create_storage_quota(candidate_id)
        quota.total_files = max(0, quota.total_files - 1)
        self.db.commit()

        return True

    def cleanup_soft_deleted_resumes(self, days_old: int = 30) -> int:
        """
        Clean up physical files for soft-deleted resumes older than X days

        Returns:
            int: Number of files cleaned up
        """
        from datetime import timedelta

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)

        # Find soft-deleted resumes
        deleted_resumes = (
            self.db.query(ResumeVersion)
            .filter(
                ResumeVersion.deleted_at.isnot(None),
                ResumeVersion.deleted_at < cutoff_date,
            )
            .all()
        )

        cleanup_count = 0
        for resume in deleted_resumes:
            if resume.file_path and Path(resume.file_path).exists():
                try:
                    self.delete_file(
                        resume.file_path, resume.candidate_id, "resumes"
                    )
                    cleanup_count += 1
                except Exception as e:
                    print(f"Error cleaning up {resume.file_path}: {e}")

        return cleanup_count

    def recalculate_candidate_quota(self, candidate_id: int) -> StorageQuota:
        """
        Recalculate actual storage usage for a candidate

        Scans filesystem and updates quota to match reality
        """
        quota = self.get_or_create_storage_quota(candidate_id)
        paths = self._get_candidate_paths(candidate_id)

        # Reset counters
        resumes_bytes = 0
        documents_bytes = 0
        total_files = 0

        # Calculate resumes
        if paths["resumes"].exists():
            for file_path in paths["resumes"].rglob("*"):
                if file_path.is_file():
                    resumes_bytes += file_path.stat().st_size
                    total_files += 1

        # Calculate attachments and other
        for category in ["attachments", "other"]:
            if paths[category].exists():
                for file_path in paths[category].rglob("*"):
                    if file_path.is_file():
                        documents_bytes += file_path.stat().st_size
                        total_files += 1

        # Update quota
        quota.resumes_bytes = resumes_bytes
        quota.documents_bytes = documents_bytes
        quota.used_bytes = resumes_bytes + documents_bytes + quota.emails_bytes + quota.templates_bytes
        quota.total_files = total_files
        quota.last_calculated_at = datetime.now(timezone.utc)

        self.db.commit()
        self.db.refresh(quota)

        return quota

    def get_storage_stats(self, candidate_id: int) -> dict:
        """Get detailed storage statistics for a candidate"""
        quota = self.get_or_create_storage_quota(candidate_id)

        return {
            "quota_limit_mb": quota.quota_limit / (1024 * 1024),
            "used_mb": quota.used_bytes / (1024 * 1024),
            "available_mb": (quota.quota_limit - quota.used_bytes) / (1024 * 1024),
            "usage_percentage": (quota.used_bytes / quota.quota_limit * 100) if quota.quota_limit > 0 else 0,
            "breakdown": {
                "resumes_mb": quota.resumes_bytes / (1024 * 1024),
                "emails_mb": quota.emails_bytes / (1024 * 1024),
                "documents_mb": quota.documents_bytes / (1024 * 1024),
                "templates_mb": quota.templates_bytes / (1024 * 1024),
            },
            "total_files": quota.total_files,
            "total_emails_archived": quota.total_emails_archived,
            "last_calculated_at": quota.last_calculated_at,
            "is_over_quota": quota.used_bytes > quota.quota_limit,
        }
