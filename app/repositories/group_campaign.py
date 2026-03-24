"""
Async Group Campaign Repository

Specialized async repository for GroupCampaign model with:
- Campaign CRUD operations
- Campaign status management
- Per-recipient tracking
- Progress statistics
- Campaign sending coordination
"""

from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, literal, Float
from sqlalchemy.orm import selectinload, Session
from datetime import datetime, timezone
import logging

from app.repositories.base_async import AsyncBaseRepository
from app.repositories.base import BaseRepository
from app.models.group_campaign import GroupCampaign, CampaignStatusEnum
from app.models.group_campaign_recipient import (
    GroupCampaignRecipient,
    RecipientStatusEnum,
)
from app.models.recipient import Recipient
from app.core.cache_async import async_cache

logger = logging.getLogger(__name__)


class AsyncGroupCampaignRepository(AsyncBaseRepository[GroupCampaign]):
    """
    Async group campaign-specific repository.

    Key Features:
    - Campaign lifecycle management
    - Per-recipient status tracking
    - Progress monitoring
    - Statistics computation
    - Send coordination
    """

    def __init__(self, db: AsyncSession):
        super().__init__(GroupCampaign, db)

    # ==================== CAMPAIGN LOOKUP ====================

    async def get_by_name(
        self, candidate_id: int, name: str
    ) -> Optional[GroupCampaign]:
        """
        Get campaign by name within candidate's scope.

        Args:
            candidate_id: Owner candidate ID
            name: Campaign name

        Returns:
            GroupCampaign or None
        """
        stmt = select(GroupCampaign).where(
            and_(
                GroupCampaign.candidate_id == candidate_id,
                GroupCampaign.campaign_name == name,
                GroupCampaign.deleted_at.is_(None),
            )
        )

        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def get_with_recipients(self, campaign_id: int) -> Optional[GroupCampaign]:
        """
        Get campaign with all recipient relationships loaded.

        Loads:
        - Campaign recipients (junction table)
        - Group
        - Template
        """
        stmt = (
            select(GroupCampaign)
            .where(GroupCampaign.id == campaign_id)
            .options(
                selectinload(GroupCampaign.campaign_recipients).selectinload(
                    GroupCampaignRecipient.recipient
                ),
                selectinload(GroupCampaign.group),
                selectinload(GroupCampaign.email_template),
            )
        )

        stmt = self._apply_soft_delete_filter(stmt)

        result = await self.db.execute(stmt)
        return result.scalars().first()

    # ==================== CAMPAIGN RECIPIENT TRACKING ====================

    async def get_campaign_recipients(
        self,
        campaign_id: int,
        status: Optional[RecipientStatusEnum] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> Tuple[List[GroupCampaignRecipient], int]:
        """
        Get campaign recipients with pagination.

        Args:
            campaign_id: Campaign ID
            status: Filter by recipient status
            skip: Pagination offset
            limit: Max results

        Returns:
            Tuple of (campaign_recipients list, total count)
        """
        # Build base query
        stmt = (
            select(GroupCampaignRecipient)
            .where(GroupCampaignRecipient.campaign_id == campaign_id)
            .options(selectinload(GroupCampaignRecipient.recipient))
        )

        if status:
            stmt = stmt.where(GroupCampaignRecipient.status == status)

        # Get total count
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await self.db.execute(count_stmt)
        total = count_result.scalar() or 0

        # Apply pagination and ordering
        stmt = (
            stmt.order_by(GroupCampaignRecipient.created_at.desc())
            .offset(skip)
            .limit(limit)
        )

        # Execute query
        result = await self.db.execute(stmt)
        campaign_recipients = list(result.scalars().all())

        return campaign_recipients, total

    async def get_recipient_status(
        self, campaign_id: int, recipient_id: int
    ) -> Optional[GroupCampaignRecipient]:
        """Get status for a specific recipient in a campaign"""
        stmt = select(GroupCampaignRecipient).where(
            and_(
                GroupCampaignRecipient.campaign_id == campaign_id,
                GroupCampaignRecipient.recipient_id == recipient_id,
            )
        )

        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def update_recipient_status(
        self, campaign_recipient_id: int, status: RecipientStatusEnum, **kwargs
    ) -> Optional[GroupCampaignRecipient]:
        """
        Update campaign recipient status.

        Args:
            campaign_recipient_id: GroupCampaignRecipient ID
            status: New status
            **kwargs: Additional fields to update (sent_at, opened_at, etc.)

        Returns:
            Updated GroupCampaignRecipient
        """
        # Get campaign recipient
        stmt = select(GroupCampaignRecipient).where(
            GroupCampaignRecipient.id == campaign_recipient_id
        )
        result = await self.db.execute(stmt)
        campaign_recipient = result.scalars().first()

        if not campaign_recipient:
            return None

        # Update status
        campaign_recipient.status = status

        # Update timestamps based on status
        now = datetime.now(timezone.utc)
        if status == RecipientStatusEnum.SENT and not campaign_recipient.sent_at:
            campaign_recipient.sent_at = now
        elif status == RecipientStatusEnum.OPENED and not campaign_recipient.opened_at:
            campaign_recipient.opened_at = now
        elif (
            status == RecipientStatusEnum.REPLIED and not campaign_recipient.replied_at
        ):
            campaign_recipient.replied_at = now
        elif (
            status == RecipientStatusEnum.BOUNCED and not campaign_recipient.bounced_at
        ):
            campaign_recipient.bounced_at = now

        # Apply additional updates
        for key, value in kwargs.items():
            if hasattr(campaign_recipient, key):
                setattr(campaign_recipient, key, value)

        await self.db.commit()
        await self.db.refresh(campaign_recipient)

        # Update campaign statistics
        await self._update_campaign_stats(campaign_recipient.campaign_id)

        return campaign_recipient

    # ==================== CAMPAIGN STATUS MANAGEMENT ====================

    async def update_campaign_status(
        self,
        campaign_id: int,
        status: CampaignStatusEnum,
        error_message: Optional[str] = None,
    ) -> Optional[GroupCampaign]:
        """
        Update campaign status.

        Args:
            campaign_id: Campaign ID
            status: New status
            error_message: Optional error message

        Returns:
            Updated campaign
        """
        campaign = await self.get_by_id(campaign_id, use_cache=False)
        if not campaign:
            return None

        # Update status
        campaign.status = status

        # Update timestamps
        now = datetime.now(timezone.utc)
        if status == CampaignStatusEnum.SENDING and not campaign.started_at:
            campaign.started_at = now
        elif status == CampaignStatusEnum.COMPLETED and not campaign.completed_at:
            campaign.completed_at = now
        elif status == CampaignStatusEnum.PAUSED and not campaign.paused_at:
            campaign.paused_at = now

        # Set error message if provided
        if error_message:
            campaign.error_message = error_message

        await self.db.commit()
        await self.db.refresh(campaign)

        logger.info(f"📊 [CAMPAIGNS] Updated campaign {campaign_id} status to {status}")

        return campaign

    async def pause_campaign(self, campaign_id: int) -> Optional[GroupCampaign]:
        """Pause a running campaign"""
        return await self.update_campaign_status(campaign_id, CampaignStatusEnum.PAUSED)

    async def resume_campaign(self, campaign_id: int) -> Optional[GroupCampaign]:
        """Resume a paused campaign"""
        campaign = await self.update_campaign_status(
            campaign_id, CampaignStatusEnum.SENDING
        )
        if campaign:
            campaign.paused_at = None
            await self.db.commit()
            await self.db.refresh(campaign)
        return campaign

    async def complete_campaign(self, campaign_id: int) -> Optional[GroupCampaign]:
        """Mark campaign as completed"""
        return await self.update_campaign_status(
            campaign_id, CampaignStatusEnum.COMPLETED
        )

    async def fail_campaign(
        self, campaign_id: int, error_message: str
    ) -> Optional[GroupCampaign]:
        """Mark campaign as failed"""
        return await self.update_campaign_status(
            campaign_id, CampaignStatusEnum.FAILED, error_message=error_message
        )

    # ==================== STATISTICS ====================

    async def _update_campaign_stats(self, campaign_id: int):
        """Update campaign statistics based on recipient statuses"""
        # Count by status
        status_counts_stmt = (
            select(
                GroupCampaignRecipient.status,
                func.count(GroupCampaignRecipient.id).label("count"),
            )
            .where(GroupCampaignRecipient.campaign_id == campaign_id)
            .group_by(GroupCampaignRecipient.status)
        )

        result = await self.db.execute(status_counts_stmt)
        status_counts = {row[0]: row[1] for row in result.all()}

        # Update campaign
        campaign = await self.get_by_id(campaign_id, use_cache=False)
        if campaign:
            campaign.sent_count = (
                status_counts.get(RecipientStatusEnum.SENT, 0)
                + status_counts.get(RecipientStatusEnum.OPENED, 0)
                + status_counts.get(RecipientStatusEnum.REPLIED, 0)
            )
            campaign.failed_count = status_counts.get(RecipientStatusEnum.FAILED, 0)
            campaign.skipped_count = status_counts.get(RecipientStatusEnum.SKIPPED, 0)
            campaign.opened_count = status_counts.get(
                RecipientStatusEnum.OPENED, 0
            ) + status_counts.get(RecipientStatusEnum.REPLIED, 0)
            campaign.replied_count = status_counts.get(RecipientStatusEnum.REPLIED, 0)
            campaign.bounced_count = status_counts.get(RecipientStatusEnum.BOUNCED, 0)

            await self.db.commit()

    async def get_campaign_statistics(self, candidate_id: int) -> Dict[str, Any]:
        """
        Get campaign statistics for a candidate.

        Returns:
            Dict with various stats
        """
        # Total campaigns
        total = await self.count({"candidate_id": candidate_id})

        # Campaigns by status
        status_counts_stmt = (
            select(GroupCampaign.status, func.count(GroupCampaign.id).label("count"))
            .where(
                and_(
                    GroupCampaign.candidate_id == candidate_id,
                    GroupCampaign.deleted_at.is_(None),
                )
            )
            .group_by(GroupCampaign.status)
        )
        status_result = await self.db.execute(status_counts_stmt)
        status_counts = {row[0].value: row[1] for row in status_result.all()}

        # Total emails sent across all campaigns
        total_sent_stmt = select(func.sum(GroupCampaign.sent_count)).where(
            and_(
                GroupCampaign.candidate_id == candidate_id,
                GroupCampaign.deleted_at.is_(None),
            )
        )
        total_sent_result = await self.db.execute(total_sent_stmt)
        total_sent = total_sent_result.scalar() or 0

        # Average success rate
        avg_success_stmt = select(
            func.avg(
                func.cast(GroupCampaign.sent_count, Float)
                / func.nullif(GroupCampaign.total_recipients, 0)
                * literal(100)
            )
        ).where(
            and_(
                GroupCampaign.candidate_id == candidate_id,
                GroupCampaign.total_recipients > 0,
                GroupCampaign.deleted_at.is_(None),
            )
        )
        avg_success_result = await self.db.execute(avg_success_stmt)
        avg_success_rate = avg_success_result.scalar() or 0.0

        return {
            "total_campaigns": total,
            "by_status": status_counts,
            "total_emails_sent": total_sent,
            "avg_success_rate": round(avg_success_rate, 2),
        }

    # ==================== PENDING OPERATIONS ====================

    async def get_pending_recipients(
        self, campaign_id: int, limit: int = 100
    ) -> List[GroupCampaignRecipient]:
        """
        Get pending recipients for a campaign (for sending).

        Args:
            campaign_id: Campaign ID
            limit: Max recipients to fetch

        Returns:
            List of pending GroupCampaignRecipient records
        """
        stmt = (
            select(GroupCampaignRecipient)
            .where(
                and_(
                    GroupCampaignRecipient.campaign_id == campaign_id,
                    GroupCampaignRecipient.status == RecipientStatusEnum.PENDING,
                )
            )
            .options(selectinload(GroupCampaignRecipient.recipient))
            .order_by(GroupCampaignRecipient.id.asc())
            .limit(limit)
        )

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_active_campaigns(
        self, candidate_id: Optional[int] = None
    ) -> List[GroupCampaign]:
        """
        Get campaigns that are currently sending or scheduled.

        Args:
            candidate_id: Filter by candidate (optional)

        Returns:
            List of active campaigns
        """
        stmt = select(GroupCampaign).where(
            and_(
                GroupCampaign.status.in_(
                    [CampaignStatusEnum.SENDING, CampaignStatusEnum.SCHEDULED]
                ),
                GroupCampaign.deleted_at.is_(None),
            )
        )

        if candidate_id:
            stmt = stmt.where(GroupCampaign.candidate_id == candidate_id)

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    # ==================== RECENT CAMPAIGNS ====================

    async def get_recent_campaigns(
        self, candidate_id: int, limit: int = 10
    ) -> List[GroupCampaign]:
        """Get recently created campaigns"""
        stmt = (
            select(GroupCampaign)
            .where(
                and_(
                    GroupCampaign.candidate_id == candidate_id,
                    GroupCampaign.deleted_at.is_(None),
                )
            )
            .order_by(GroupCampaign.created_at.desc())
            .limit(limit)
        )

        result = await self.db.execute(stmt)
        return list(result.scalars().all())


class GroupCampaignRepository(BaseRepository[GroupCampaign]):
    """
    Sync repository adapter used by background tasks.

    This mirrors the async repository for code paths that rely on
    synchronous SQLAlchemy sessions (e.g., background tasks).
    """

    def __init__(self, db: Session):
        super().__init__(GroupCampaign, db)
