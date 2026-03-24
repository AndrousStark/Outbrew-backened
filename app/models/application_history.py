from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base


class ApplicationHistory(Base):
    """Track all status changes and updates to applications"""
    __tablename__ = "application_history"

    id = Column(Integer, primary_key=True, index=True)
    application_id = Column(Integer, ForeignKey("applications.id", ondelete="CASCADE"), nullable=False, index=True)
    changed_by = Column(Integer, ForeignKey("candidates.id", ondelete="SET NULL"), index=True)

    # What changed
    field_name = Column(String(100))  # e.g., "status", "recruiter_email", etc.
    old_value = Column(Text)
    new_value = Column(Text)

    # Additional context
    note = Column(Text)
    change_type = Column(String(50), index=True)  # "status_change", "field_update", "note_added"

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)

    # Composite indexes for common queries
    __table_args__ = (
        Index('ix_app_history_app_created', 'application_id', 'created_at'),
    )

    # Relationships
    application = relationship("Application", back_populates="history", viewonly=True)
    changed_by_user = relationship("Candidate", foreign_keys=[changed_by], viewonly=True)

    def __repr__(self):
        return f"<ApplicationHistory {self.id}: {self.field_name} changed>"


class ApplicationNote(Base):
    """Notes and comments on applications"""
    __tablename__ = "application_notes"

    id = Column(Integer, primary_key=True, index=True)
    application_id = Column(Integer, ForeignKey("applications.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    content = Column(Text, nullable=False)
    note_type = Column(String(50), default="general", index=True)  # "general", "interview", "follow_up"

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Composite indexes for common queries
    __table_args__ = (
        Index('ix_app_notes_app_created', 'application_id', 'created_at'),
    )

    # Relationships
    application = relationship("Application", back_populates="application_notes_list", viewonly=True)
    author = relationship("Candidate", viewonly=True)

    def __repr__(self):
        return f"<ApplicationNote {self.id}>"


class ApplicationAttachment(Base):
    """File attachments for applications (interview notes, offer letters, etc.)"""
    __tablename__ = "application_attachments"

    id = Column(Integer, primary_key=True, index=True)
    application_id = Column(Integer, ForeignKey("applications.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_type = Column(String(100), index=True)  # "pdf", "docx", "png", etc.
    file_size = Column(Integer)  # in bytes

    attachment_type = Column(String(50), index=True)  # "interview_notes", "offer_letter", "correspondence", "other"
    description = Column(Text)

    uploaded_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)

    # Composite indexes for common queries
    __table_args__ = (
        Index('ix_app_attach_app_uploaded', 'application_id', 'uploaded_at'),
    )

    # Relationships
    application = relationship("Application", back_populates="attachments", viewonly=True)
    uploaded_by = relationship("Candidate", viewonly=True)

    def __repr__(self):
        return f"<ApplicationAttachment {self.id}: {self.filename}>"
