"""Group Recipient Model - Many-to-many junction table for groups and recipients"""
from sqlalchemy import Column, Integer, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class GroupRecipient(Base):
    """
    GroupRecipient model represents the many-to-many relationship between
    RecipientGroup and Recipient.

    This allows recipients to be members of multiple groups simultaneously,
    and tracks when membership was established.

    For dynamic groups, this table caches the evaluated membership and tracks
    whether the membership was determined automatically or manually added.
    """
    __tablename__ = "group_recipients"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign Keys
    group_id = Column(Integer, ForeignKey("recipient_groups.id", ondelete="CASCADE"), nullable=False, index=True)
    recipient_id = Column(Integer, ForeignKey("recipients.id", ondelete="CASCADE"), nullable=False, index=True)

    # Membership Metadata
    is_dynamic_membership = Column(Boolean, default=False)  # True if added by dynamic filter evaluation

    # Timestamps
    added_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    group = relationship("RecipientGroup", back_populates="group_recipients")
    recipient = relationship("Recipient", back_populates="group_memberships")

    # Composite Indexes
    __table_args__ = (
        # Ensure one recipient per group only once
        Index('ix_group_recipient_unique', 'group_id', 'recipient_id', unique=True),
        # Query optimization for fetching group members
        Index('ix_group_recipient_group', 'group_id', 'recipient_id'),
    )

    def __repr__(self):
        return f"<GroupRecipient(group_id={self.group_id}, recipient_id={self.recipient_id})>"
