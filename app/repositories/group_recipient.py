"""
Async Group Recipient Repository

Repository for GroupRecipient junction table operations.
Provides methods for querying group membership.
"""
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload
import logging

from app.repositories.base_async import AsyncBaseRepository
from app.models.group_recipient import GroupRecipient
from app.models.recipient import Recipient

logger = logging.getLogger(__name__)


class AsyncGroupRecipientRepository(AsyncBaseRepository[GroupRecipient]):
    """
    Async repository for GroupRecipient junction table.

    Provides methods for querying group-recipient membership,
    used by group campaigns to resolve recipients in a group.
    """

    def __init__(self, db: AsyncSession):
        super().__init__(GroupRecipient, db)

    async def get_by_group(
        self,
        group_id: int,
        active_only: bool = True
    ) -> List[GroupRecipient]:
        """
        Get all GroupRecipient records for a given group.

        Args:
            group_id: The group ID to look up
            active_only: Only return memberships for active, non-unsubscribed recipients

        Returns:
            List of GroupRecipient records with recipient relationship loaded
        """
        stmt = (
            select(GroupRecipient)
            .where(GroupRecipient.group_id == group_id)
            .options(selectinload(GroupRecipient.recipient))
        )

        if active_only:
            stmt = (
                stmt
                .join(Recipient, GroupRecipient.recipient_id == Recipient.id)
                .where(
                    and_(
                        Recipient.is_active == True,
                        Recipient.unsubscribed == False,
                        Recipient.deleted_at.is_(None)
                    )
                )
            )

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_by_group_and_recipient(
        self,
        group_id: int,
        recipient_id: int
    ) -> GroupRecipient:
        """Get a specific group-recipient membership."""
        stmt = (
            select(GroupRecipient)
            .where(
                and_(
                    GroupRecipient.group_id == group_id,
                    GroupRecipient.recipient_id == recipient_id
                )
            )
        )
        result = await self.db.execute(stmt)
        return result.scalars().first()
