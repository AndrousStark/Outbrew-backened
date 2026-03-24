"""Recipient Group Model - Static and dynamic recipient groups"""
import enum
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, JSON, Enum as SQLEnum, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class GroupTypeEnum(str, enum.Enum):
    """Group type enumeration"""
    STATIC = "static"  # Manually managed membership
    DYNAMIC = "dynamic"  # Auto-populated based on filter criteria


class RecipientGroup(Base):
    """
    RecipientGroup model represents a collection of recipients.

    Groups can be:
    - STATIC: Manually curated list of recipients
    - DYNAMIC: Auto-populated based on filter criteria (e.g., company, tags, country)

    Dynamic groups can auto-refresh to keep membership up-to-date as recipients
    are added/updated in the system.
    """
    __tablename__ = "recipient_groups"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Ownership (multi-tenant isolation)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Group Details
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    group_type = Column(SQLEnum(GroupTypeEnum), nullable=False, default=GroupTypeEnum.STATIC, index=True)

    # Dynamic Group Filters (JSON)
    # Example: {"company": "Google", "tags": ["senior", "ml"], "country": "USA"}
    filter_criteria = Column(JSON)

    # Auto-refresh settings for dynamic groups
    auto_refresh = Column(Boolean, default=True)
    last_refreshed_at = Column(DateTime(timezone=True))

    # Statistics (cached)
    total_recipients = Column(Integer, default=0)
    active_recipients = Column(Integer, default=0)

    # UI Customization
    color = Column(String(50))  # For visual identification (blue, green, red, purple, etc.)

    # Soft Delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", back_populates="recipient_groups")
    group_recipients = relationship(
        "GroupRecipient",
        back_populates="group",
        cascade="all, delete-orphan"
    )
    campaigns = relationship(
        "GroupCampaign",
        back_populates="group",
        cascade="all, delete-orphan"
    )

    # Composite Indexes
    __table_args__ = (
        # Search optimization
        Index('ix_group_candidate_type', 'candidate_id', 'group_type', 'deleted_at'),
    )

    def __repr__(self):
        return f"<RecipientGroup(id={self.id}, name={self.name}, type={self.group_type}, recipients={self.total_recipients})>"
