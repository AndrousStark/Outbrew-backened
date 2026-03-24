"""Resume Version Model"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum

from app.core.database import Base


class ResumeLanguage(str, enum.Enum):
    """Resume languages"""
    ENGLISH = "english"
    HINDI = "hindi"
    SPANISH = "spanish"
    FRENCH = "french"
    GERMAN = "german"
    CHINESE = "chinese"
    JAPANESE = "japanese"
    KOREAN = "korean"


class ResumeVersion(Base):
    """Multiple resume versions for different purposes"""
    __tablename__ = "resume_versions"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Resume details
    name = Column(String(255), nullable=False)  # e.g., "ML Engineer Resume", "Data Scientist Resume"
    description = Column(Text)
    language = Column(Enum(ResumeLanguage), default=ResumeLanguage.ENGLISH, nullable=False, index=True)

    # File info
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(Integer)  # in bytes

    # Categorization
    target_position = Column(String(255))  # e.g., "Machine Learning Engineer"
    target_industry = Column(String(255))  # e.g., "Tech", "Finance"
    target_country = Column(String(100))  # e.g., "USA", "India", "Germany"

    # Status
    is_default = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)

    # Usage tracking
    times_used = Column(Integer, default=0)
    last_used_at = Column(DateTime(timezone=True))

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", back_populates="resume_versions")
    applications = relationship("Application", back_populates="resume_version")
