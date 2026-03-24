"""Merge History Model - Track recipient deduplication and merge operations for audit/rollback."""

from sqlalchemy import Column, Integer, String, JSON, DateTime, Text
from sqlalchemy.sql import func
from datetime import datetime, timezone
import enum

from app.core.database import Base


class MergeStrategyEnum(str, enum.Enum):
    """Merge strategy used."""

    KEEP_FIRST = "keep_first"  # Keep the first recipient
    KEEP_MOST_COMPLETE = "keep_most_complete"  # Keep recipient with most fields filled
    KEEP_MOST_RECENT = "keep_most_recent"  # Keep most recently updated recipient


class MergeHistory(Base):
    """Track recipient merge operations for audit and rollback capability."""

    __tablename__ = "merge_history"

    id = Column(Integer, primary_key=True, index=True)

    # Identification
    merge_id = Column(
        String(36), unique=True, index=True, nullable=False
    )  # UUID for rollback reference
    candidate_id = Column(Integer, nullable=False, index=True)

    # Merge details
    primary_recipient_id = Column(
        Integer, nullable=False
    )  # The recipient that was kept
    secondary_recipient_ids = Column(
        JSON, nullable=False
    )  # List of recipient IDs that were merged into primary
    strategy = Column(
        String(50), nullable=False
    )  # keep_first, keep_most_complete, keep_most_recent
    confidence_score = Column(
        Integer, default=0
    )  # Average confidence of the merge (0-100)

    # Merge results
    merged_fields = Column(JSON, nullable=False)  # Fields that were merged/updated
    conflicts_resolved = Column(JSON, nullable=False)  # How conflicts were resolved
    data_snapshot = Column(
        JSON, nullable=True
    )  # Snapshot of original data before merge

    # Status tracking
    status = Column(String(20), default="completed")  # completed, rolled_back
    rolled_back_at = Column(DateTime, nullable=True)
    rollback_reason = Column(Text, nullable=True)

    # Metadata
    created_at = Column(DateTime, default=func.now(), index=True)
    created_by = Column(String(255), nullable=True)  # User ID or system
    notes = Column(Text, nullable=True)  # Admin notes

    def mark_rolled_back(self, reason: str = None):
        """Mark this merge as rolled back."""
        self.status = "rolled_back"
        self.rolled_back_at = datetime.now(timezone.utc)
        self.rollback_reason = reason
