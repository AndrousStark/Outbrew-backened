"""
Email Inbox Models - Complete email threading and archiving system

Features:
- Bidirectional email sync (sent + received)
- Email threading and conversation grouping
- IMAP/OAuth integration for inbox sync
- Search and filtering
- No attachment storage for received emails (as per user requirement)
"""

from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Boolean,
    Text, Enum as SQLEnum, Index, BigInteger, func
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from enum import Enum
from typing import Optional

from app.core.database import Base


class EmailDirection(str, Enum):
    """Direction of email flow"""
    SENT = "sent"
    RECEIVED = "received"


class EmailSyncStatus(str, Enum):
    """IMAP sync status"""
    PENDING = "pending"
    SYNCING = "syncing"
    SYNCED = "synced"
    FAILED = "failed"


class EmailAccountType(str, Enum):
    """Type of email account"""
    GMAIL = "gmail"
    OUTLOOK = "outlook"
    YAHOO = "yahoo"
    IMAP = "imap"  # Generic IMAP
    OTHER = "other"


class EmailAccount(Base):
    """
    Email account configuration for inbox integration
    """
    __tablename__ = "email_accounts"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Account details
    email_address = Column(String(255), nullable=False, unique=True, index=True)
    account_type = Column(SQLEnum(EmailAccountType), nullable=False)
    display_name = Column(String(255))

    # IMAP Configuration
    imap_host = Column(String(255))
    imap_port = Column(Integer, default=993)
    imap_use_ssl = Column(Boolean, default=True)
    imap_username = Column(String(255))
    imap_password = Column(String(500))  # Encrypted

    # OAuth2 tokens (for Gmail/Outlook)
    oauth_access_token = Column(Text)  # Encrypted
    oauth_refresh_token = Column(Text)  # Encrypted
    oauth_token_expires_at = Column(DateTime(timezone=True))

    # Sync settings
    sync_enabled = Column(Boolean, default=True)
    sync_frequency_minutes = Column(Integer, default=15)  # Check every 15 minutes
    sync_folders = Column(String(500), default="INBOX")  # Comma-separated
    last_sync_at = Column(DateTime(timezone=True))
    sync_status = Column(SQLEnum(EmailSyncStatus), default=EmailSyncStatus.PENDING)
    sync_error = Column(Text)

    # Stats
    total_emails_synced = Column(Integer, default=0)
    total_emails_sent = Column(Integer, default=0)

    # Status
    is_active = Column(Boolean, default=True)
    is_primary = Column(Boolean, default=False)  # Primary account for sending

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    deleted_at = Column(DateTime(timezone=True), index=True)

    # Relationships
    candidate = relationship("Candidate", backref="email_accounts")
    messages = relationship("EmailMessage", back_populates="email_account", cascade="all, delete-orphan")

    __table_args__ = (
        Index('ix_email_account_candidate', 'candidate_id', 'is_active'),
    )


class EmailMessage(Base):
    """
    Complete email storage with threading support
    """
    __tablename__ = "email_messages"

    id = Column(Integer, primary_key=True, index=True)

    # Owner & Account
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)
    email_account_id = Column(Integer, ForeignKey("email_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    application_id = Column(Integer, ForeignKey("applications.id", ondelete="SET NULL"), index=True)

    # Direction
    direction = Column(SQLEnum(EmailDirection), nullable=False, index=True)

    # Email identifiers
    message_id = Column(String(500), unique=True, index=True)  # RFC 5322 Message-ID
    in_reply_to = Column(String(500), index=True)  # Threading
    thread_id = Column(String(500), index=True)  # Group conversations

    # Email details
    from_email = Column(String(255), nullable=False, index=True)
    from_name = Column(String(255))
    to_email = Column(String(255), nullable=False, index=True)
    to_name = Column(String(255))

    subject = Column(String(1000))
    body_text = Column(Text)
    body_html = Column(Text)
    snippet = Column(String(500))  # First 500 chars for preview

    # File storage path
    file_path = Column(String(1000))  # Path to saved .html file

    # Status flags
    is_read = Column(Boolean, default=False, index=True)
    is_starred = Column(Boolean, default=False)
    is_important = Column(Boolean, default=False)
    is_archived = Column(Boolean, default=False)
    is_spam = Column(Boolean, default=False)
    is_trash = Column(Boolean, default=False)

    # Tracking (for sent emails)
    opened_at = Column(DateTime(timezone=True))
    clicked_at = Column(DateTime(timezone=True))
    bounced = Column(Boolean, default=False)
    bounce_reason = Column(Text)

    # Size
    size_bytes = Column(Integer)

    # Timestamps
    sent_at = Column(DateTime(timezone=True), index=True)
    received_at = Column(DateTime(timezone=True), index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    deleted_at = Column(DateTime(timezone=True), index=True)

    # Relationships
    candidate = relationship("Candidate", backref="email_messages")
    email_account = relationship("EmailAccount", back_populates="messages")
    application = relationship("Application", backref="email_messages")

    __table_args__ = (
        Index('ix_email_message_thread', 'thread_id', 'sent_at'),
        Index('ix_email_message_candidate_direction', 'candidate_id', 'direction', 'sent_at'),
        Index('ix_email_message_read_status', 'candidate_id', 'is_read', 'sent_at'),
    )

    @property
    def is_sent(self) -> bool:
        return self.direction == EmailDirection.SENT

    @property
    def is_received(self) -> bool:
        return self.direction == EmailDirection.RECEIVED


class EmailThread(Base):
    """
    Email conversation threads for grouping related emails
    """
    __tablename__ = "email_threads"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Thread info
    thread_id = Column(String(500), unique=True, index=True)
    subject = Column(String(1000))

    # Participants
    participants = Column(Text)  # JSON array of email addresses

    # Stats
    message_count = Column(Integer, default=0)
    unread_count = Column(Integer, default=0)

    # Status
    is_starred = Column(Boolean, default=False)
    is_archived = Column(Boolean, default=False)

    # Latest message
    latest_message_at = Column(DateTime(timezone=True), index=True)
    latest_snippet = Column(String(500))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", backref="email_threads")

    __table_args__ = (
        Index('ix_email_thread_candidate_updated', 'candidate_id', 'updated_at'),
    )


class StorageQuota(Base):
    """
    Track storage usage per candidate
    """
    __tablename__ = "storage_quotas"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), unique=True, nullable=False)

    # Quota limits (in bytes)
    quota_limit = Column(BigInteger, default=524288000)  # 500 MB default
    used_bytes = Column(BigInteger, default=0)

    # Breakdown
    resumes_bytes = Column(BigInteger, default=0)
    emails_bytes = Column(BigInteger, default=0)
    documents_bytes = Column(BigInteger, default=0)
    templates_bytes = Column(BigInteger, default=0)

    # Stats
    total_files = Column(Integer, default=0)
    total_emails_archived = Column(Integer, default=0)

    # Timestamps
    last_calculated_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", backref="storage_quota", uselist=False)

    @property
    def usage_percentage(self) -> float:
        if self.quota_limit == 0:
            return 0.0
        return (self.used_bytes / self.quota_limit) * 100

    @property
    def remaining_bytes(self) -> int:
        return max(0, self.quota_limit - self.used_bytes)

    @property
    def is_over_quota(self) -> bool:
        return self.used_bytes >= self.quota_limit


class EmailLabel(Base):
    """
    Custom labels for email organization (like Gmail labels)
    """
    __tablename__ = "email_labels"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)

    # Label info
    name = Column(String(100), nullable=False)
    color = Column(String(7), default="#808080")  # Hex color code
    description = Column(String(255))

    # Stats
    email_count = Column(Integer, default=0)  # Cached count

    # System labels (predefined, cannot be deleted)
    is_system = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", backref="email_labels")

    __table_args__ = (
        Index('idx_email_labels_candidate', 'candidate_id'),
        Index('idx_email_labels_name', 'candidate_id', 'name', unique=True),
    )


class EmailLabelAssignment(Base):
    """
    Many-to-many relationship between emails and labels
    """
    __tablename__ = "email_label_assignments"

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey("email_messages.id", ondelete="CASCADE"), nullable=False)
    label_id = Column(Integer, ForeignKey("email_labels.id", ondelete="CASCADE"), nullable=False)

    # Timestamps
    assigned_at = Column(DateTime(timezone=True), default=func.now())

    # Relationships
    message = relationship("EmailMessage", backref="label_assignments")
    label = relationship("EmailLabel", backref="assignments")

    __table_args__ = (
        Index('idx_label_assignments_message', 'message_id'),
        Index('idx_label_assignments_label', 'label_id'),
        Index('idx_label_assignments_unique', 'message_id', 'label_id', unique=True),
    )


class FilterConditionType(str, Enum):
    """Type of filter condition"""
    FROM_CONTAINS = "from_contains"
    TO_CONTAINS = "to_contains"
    SUBJECT_CONTAINS = "subject_contains"
    BODY_CONTAINS = "body_contains"
    HAS_ATTACHMENT = "has_attachment"
    IS_STARRED = "is_starred"
    IS_IMPORTANT = "is_important"


class FilterAction(str, Enum):
    """Action to perform when filter matches"""
    APPLY_LABEL = "apply_label"
    MARK_AS_READ = "mark_as_read"
    MARK_AS_STARRED = "mark_as_starred"
    MARK_AS_IMPORTANT = "mark_as_important"
    SKIP_INBOX = "skip_inbox"  # Archive automatically


class EmailFilter(Base):
    """
    Automatic email filtering rules
    """
    __tablename__ = "email_filters"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)

    # Filter info
    name = Column(String(100), nullable=False)
    description = Column(String(255))
    is_enabled = Column(Boolean, default=True)

    # Conditions (JSON array of conditions)
    # Example: [{"type": "from_contains", "value": "boss@company.com"}]
    conditions = Column(Text, nullable=False)  # JSON string

    # Match behavior
    match_all = Column(Boolean, default=True)  # AND vs OR logic

    # Actions (JSON array of actions)
    # Example: [{"type": "apply_label", "label_id": 5}, {"type": "mark_as_read"}]
    actions = Column(Text, nullable=False)  # JSON string

    # Stats
    times_matched = Column(Integer, default=0)
    last_matched_at = Column(DateTime(timezone=True))

    # Priority (lower number = higher priority)
    priority = Column(Integer, default=100)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", backref="email_filters")

    __table_args__ = (
        Index('idx_email_filters_candidate', 'candidate_id'),
        Index('idx_email_filters_enabled', 'candidate_id', 'is_enabled'),
        Index('idx_email_filters_priority', 'candidate_id', 'priority'),
    )
