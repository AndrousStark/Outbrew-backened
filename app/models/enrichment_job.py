"""Enrichment Job Model - for tracking batch enrichment runs and retry history."""

from sqlalchemy import Column, Integer, String, JSON, DateTime, Text
from sqlalchemy.sql import func
from datetime import datetime, timezone
import enum

from app.core.database import Base


class EnrichmentJobStatus(str, enum.Enum):
    """Enrichment job status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


class EnrichmentJob(Base):
    """Track enrichment job execution, retries, and results."""

    __tablename__ = "enrichment_jobs"

    id = Column(Integer, primary_key=True, index=True)

    # Job identification
    job_id = Column(String(36), unique=True, index=True, nullable=False)  # UUID
    candidate_id = Column(Integer, nullable=False, index=True)

    # Status tracking
    status = Column(String(20), default=EnrichmentJobStatus.PENDING.value, index=True)
    current_step = Column(String(100), nullable=True)  # e.g., "enriching_recipient_5"

    # Progress
    total_recipients = Column(Integer, default=0)
    completed_recipients = Column(Integer, default=0)
    failed_recipients = Column(Integer, default=0)

    # Retry tracking
    attempt_number = Column(Integer, default=1)
    max_attempts = Column(Integer, default=3)
    last_error = Column(Text, nullable=True)

    # Configuration
    enrichment_config = Column(JSON, nullable=True)  # enable_email_validation, etc.

    # Results
    enrichment_results = Column(JSON, nullable=True)  # Full enrichment results
    email_validation_results = Column(JSON, nullable=True)
    statistics = Column(JSON, nullable=True)  # Cache hits, API calls, etc.

    # Timing
    created_at = Column(DateTime, default=func.now(), index=True)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    next_retry_at = Column(DateTime, nullable=True)

    def mark_running(self):
        """Mark job as running."""
        self.status = EnrichmentJobStatus.RUNNING.value
        self.started_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def mark_completed(self, enrichment_results, email_validation_results, statistics):
        """Mark job as completed with results."""
        self.status = EnrichmentJobStatus.COMPLETED.value
        self.enrichment_results = enrichment_results
        self.email_validation_results = email_validation_results
        self.statistics = statistics
        self.finished_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def mark_failed(self, error_message):
        """Mark job as failed."""
        self.status = EnrichmentJobStatus.FAILED.value
        self.last_error = error_message
        self.finished_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def schedule_retry(self, delay_seconds: int):
        """Schedule a retry attempt with backoff."""
        if self.attempt_number < self.max_attempts:
            self.status = EnrichmentJobStatus.PENDING.value
            self.attempt_number += 1
            self.next_retry_at = datetime.now(timezone.utc) + __import__("datetime").timedelta(
                seconds=delay_seconds
            )
            self.updated_at = datetime.now(timezone.utc)
            return True
        return False
