"""
Async Recipient Group Repository

Specialized async repository for RecipientGroup model with:
- Static and dynamic group management
- Filter evaluation for dynamic groups
- Group membership operations (add/remove)
- Auto-refresh for dynamic groups
- Group statistics
"""
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, delete
from sqlalchemy.orm import selectinload
from datetime import datetime, timedelta, timezone
import logging

from app.repositories.base_async import AsyncBaseRepository
from app.models.recipient_group import RecipientGroup, GroupTypeEnum
from app.models.group_recipient import GroupRecipient
from app.models.recipient import Recipient
from app.core.cache_async import async_cache

logger = logging.getLogger(__name__)


class AsyncRecipientGroupRepository(AsyncBaseRepository[RecipientGroup]):
    """
    Async recipient group-specific repository.

    Key Features:
    - Static and dynamic group management
    - Filter-based recipient selection for dynamic groups
    - Group membership CRUD operations
    - Auto-refresh scheduling for dynamic groups
    - Group statistics and analytics
    """

    def __init__(self, db: AsyncSession):
        super().__init__(RecipientGroup, db)

    # ==================== GROUP LOOKUP ====================

    async def get_by_name(
        self,
        candidate_id: int,
        name: str
    ) -> Optional[RecipientGroup]:
        """
        Get group by name within candidate's scope.

        Args:
            candidate_id: Owner candidate ID
            name: Group name

        Returns:
            RecipientGroup or None
        """
        stmt = (
            select(RecipientGroup)
            .where(
                and_(
                    RecipientGroup.candidate_id == candidate_id,
                    RecipientGroup.name == name,
                    RecipientGroup.deleted_at.is_(None)
                )
            )
        )

        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def exists_by_name(
        self,
        candidate_id: int,
        name: str
    ) -> bool:
        """Check if group name exists for candidate"""
        group = await self.get_by_name(candidate_id, name)
        return group is not None

    async def get_with_recipients(
        self,
        group_id: int
    ) -> Optional[RecipientGroup]:
        """
        Get group with all recipient relationships loaded.

        Loads:
        - Group recipients (junction table)
        - Recipient details
        - Campaigns
        """
        stmt = (
            select(RecipientGroup)
            .where(RecipientGroup.id == group_id)
            .options(
                selectinload(RecipientGroup.group_recipients)
                .selectinload(GroupRecipient.recipient),
                selectinload(RecipientGroup.campaigns)
            )
        )

        stmt = self._apply_soft_delete_filter(stmt)

        result = await self.db.execute(stmt)
        return result.scalars().first()

    # ==================== DYNAMIC GROUP FILTER EVALUATION ====================

    async def evaluate_dynamic_filters(
        self,
        candidate_id: int,
        filter_criteria: Dict[str, Any]
    ) -> List[int]:
        """
        Evaluate filter criteria and return matching recipient IDs.

        Supported filters:
        - companies: List[str] - Match any company
        - tags: List[str] - Match any tag
        - countries: List[str] - Match any country
        - positions: List[str] - Match position (partial match)
        - min_engagement_score: float - Minimum engagement score
        - is_active: bool - Active recipients only
        - exclude_unsubscribed: bool - Exclude unsubscribed

        Args:
            candidate_id: Owner candidate ID
            filter_criteria: Filter configuration

        Returns:
            List of matching recipient IDs

        Example:
            filter_criteria = {
                "companies": ["Google", "Microsoft"],
                "tags": ["tech", "senior"],
                "countries": ["USA", "Canada"],
                "min_engagement_score": 30.0,
                "is_active": True,
                "exclude_unsubscribed": True
            }
        """
        # Build base query
        stmt = (
            select(Recipient.id)
            .where(
                and_(
                    Recipient.candidate_id == candidate_id,
                    Recipient.deleted_at.is_(None)
                )
            )
        )

        # Apply filters from criteria
        if filter_criteria.get("is_active", True):
            stmt = stmt.where(Recipient.is_active == True)

        if filter_criteria.get("exclude_unsubscribed", True):
            stmt = stmt.where(Recipient.unsubscribed == False)

        # Company filter (OR condition)
        if "companies" in filter_criteria and filter_criteria["companies"]:
            company_conditions = [
                Recipient.company.ilike(f"%{company}%")
                for company in filter_criteria["companies"]
            ]
            stmt = stmt.where(or_(*company_conditions))

        # Tags filter (OR condition)
        if "tags" in filter_criteria and filter_criteria["tags"]:
            tag_conditions = [
                Recipient.tags.ilike(f"%{tag}%")
                for tag in filter_criteria["tags"]
            ]
            stmt = stmt.where(or_(*tag_conditions))

        # Countries filter (OR condition)
        if "countries" in filter_criteria and filter_criteria["countries"]:
            stmt = stmt.where(Recipient.country.in_(filter_criteria["countries"]))

        # Position filter (partial match, OR condition)
        if "positions" in filter_criteria and filter_criteria["positions"]:
            position_conditions = [
                Recipient.position.ilike(f"%{position}%")
                for position in filter_criteria["positions"]
            ]
            stmt = stmt.where(or_(*position_conditions))

        # Engagement score filter
        if "min_engagement_score" in filter_criteria:
            min_score = filter_criteria["min_engagement_score"]
            stmt = stmt.where(Recipient.engagement_score >= min_score)

        # Execute query
        result = await self.db.execute(stmt)
        recipient_ids = [row[0] for row in result.all()]

        logger.info(
            f"🔍 [REPO] Dynamic filter evaluation: {len(recipient_ids)} recipients matched"
        )

        return recipient_ids

    async def refresh_dynamic_group(
        self,
        group_id: int,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Refresh a dynamic group by re-evaluating filters and updating membership.

        Args:
            group_id: Group ID to refresh
            force: Force refresh even if recently refreshed

        Returns:
            Dict with refresh statistics

        Raises:
            ValueError: If group is not dynamic or doesn't exist
        """
        # Get group
        group = await self.get_by_id(group_id, use_cache=False)
        if not group:
            raise ValueError(f"Group {group_id} not found")

        if group.group_type != GroupTypeEnum.DYNAMIC:
            raise ValueError(f"Group {group_id} is not a dynamic group")

        # Check if refresh needed
        if not force and group.last_refreshed_at:
            time_since_refresh = datetime.now(timezone.utc) - group.last_refreshed_at
            if time_since_refresh < timedelta(hours=1):
                logger.info(
                    f"⏭️  [REPO] Skipping refresh for group {group_id} "
                    f"(refreshed {time_since_refresh.total_seconds() / 60:.1f} minutes ago)"
                )
                return {
                    "refreshed": False,
                    "reason": "recently_refreshed",
                    "last_refresh": group.last_refreshed_at
                }

        # Evaluate filters
        matching_recipient_ids = await self.evaluate_dynamic_filters(
            group.candidate_id,
            group.filter_criteria or {}
        )

        # Get current membership
        current_members_stmt = (
            select(GroupRecipient.recipient_id)
            .where(GroupRecipient.group_id == group_id)
        )
        current_result = await self.db.execute(current_members_stmt)
        current_member_ids = set(row[0] for row in current_result.all())

        # Calculate changes
        matching_ids_set = set(matching_recipient_ids)
        to_add = matching_ids_set - current_member_ids
        to_remove = current_member_ids - matching_ids_set

        # Remove no-longer-matching recipients
        if to_remove:
            delete_stmt = (
                delete(GroupRecipient)
                .where(
                    and_(
                        GroupRecipient.group_id == group_id,
                        GroupRecipient.recipient_id.in_(to_remove)
                    )
                )
            )
            await self.db.execute(delete_stmt)

        # Add newly-matching recipients
        if to_add:
            new_memberships = [
                GroupRecipient(
                    group_id=group_id,
                    recipient_id=recipient_id,
                    is_dynamic_membership=True
                )
                for recipient_id in to_add
            ]
            self.db.add_all(new_memberships)

        # Update group stats and timestamp
        active_count_stmt = (
            select(func.count())
            .select_from(GroupRecipient)
            .join(Recipient, GroupRecipient.recipient_id == Recipient.id)
            .where(
                and_(
                    GroupRecipient.group_id == group_id,
                    Recipient.is_active == True,
                    Recipient.unsubscribed == False,
                    Recipient.deleted_at.is_(None)
                )
            )
        )
        active_result = await self.db.execute(active_count_stmt)
        active_count = active_result.scalar() or 0

        await self.update(group_id, {
            "total_recipients": len(matching_recipient_ids),
            "active_recipients": active_count,
            "last_refreshed_at": datetime.now(timezone.utc)
        })

        await self.db.commit()

        # Invalidate cache
        await self._invalidate_cache()

        result = {
            "refreshed": True,
            "total_recipients": len(matching_recipient_ids),
            "active_recipients": active_count,
            "added": len(to_add),
            "removed": len(to_remove),
            "last_refresh": datetime.now(timezone.utc)
        }

        logger.info(
            f"♻️  [REPO] Refreshed dynamic group {group_id}: "
            f"+{result['added']} / -{result['removed']} recipients"
        )

        return result

    # ==================== GROUP MEMBERSHIP OPERATIONS ====================

    async def add_recipients(
        self,
        group_id: int,
        recipient_ids: List[int],
        is_dynamic: bool = False
    ) -> int:
        """
        Add recipients to a group.

        Args:
            group_id: Group ID
            recipient_ids: List of recipient IDs to add
            is_dynamic: Mark as dynamic membership

        Returns:
            Number of recipients added (excluding duplicates)

        Raises:
            ValueError: If group is dynamic (use refresh instead)
        """
        # Get group
        group = await self.get_by_id(group_id, use_cache=False)
        if not group:
            raise ValueError(f"Group {group_id} not found")

        if group.group_type == GroupTypeEnum.DYNAMIC and not is_dynamic:
            raise ValueError(
                f"Cannot manually add recipients to dynamic group {group_id}. "
                f"Use refresh_dynamic_group() instead."
            )

        # Get existing memberships
        existing_stmt = (
            select(GroupRecipient.recipient_id)
            .where(
                and_(
                    GroupRecipient.group_id == group_id,
                    GroupRecipient.recipient_id.in_(recipient_ids)
                )
            )
        )
        existing_result = await self.db.execute(existing_stmt)
        existing_ids = set(row[0] for row in existing_result.all())

        # Filter out existing memberships
        new_recipient_ids = [rid for rid in recipient_ids if rid not in existing_ids]

        if not new_recipient_ids:
            logger.info(f"⏭️  [REPO] No new recipients to add to group {group_id}")
            return 0

        # Create new memberships
        new_memberships = [
            GroupRecipient(
                group_id=group_id,
                recipient_id=recipient_id,
                is_dynamic_membership=is_dynamic
            )
            for recipient_id in new_recipient_ids
        ]
        self.db.add_all(new_memberships)

        # Update group stats
        await self._update_group_stats(group_id)

        await self.db.commit()

        # Invalidate cache
        await self._invalidate_cache()

        logger.info(f"✅ [REPO] Added {len(new_recipient_ids)} recipients to group {group_id}")
        return len(new_recipient_ids)

    async def remove_recipients(
        self,
        group_id: int,
        recipient_ids: List[int]
    ) -> int:
        """
        Remove recipients from a group.

        Args:
            group_id: Group ID
            recipient_ids: List of recipient IDs to remove

        Returns:
            Number of recipients removed

        Raises:
            ValueError: If group is dynamic (use refresh instead)
        """
        # Get group
        group = await self.get_by_id(group_id, use_cache=False)
        if not group:
            raise ValueError(f"Group {group_id} not found")

        if group.group_type == GroupTypeEnum.DYNAMIC:
            raise ValueError(
                f"Cannot manually remove recipients from dynamic group {group_id}. "
                f"Use refresh_dynamic_group() instead."
            )

        # Delete memberships
        delete_stmt = (
            delete(GroupRecipient)
            .where(
                and_(
                    GroupRecipient.group_id == group_id,
                    GroupRecipient.recipient_id.in_(recipient_ids)
                )
            )
        )
        result = await self.db.execute(delete_stmt)
        removed_count = result.rowcount

        # Update group stats
        await self._update_group_stats(group_id)

        await self.db.commit()

        # Invalidate cache
        await self._invalidate_cache()

        logger.info(f"🗑️  [REPO] Removed {removed_count} recipients from group {group_id}")
        return removed_count

    async def get_recipient_ids(
        self,
        group_id: int,
        active_only: bool = False
    ) -> List[int]:
        """
        Get all recipient IDs in a group.

        Args:
            group_id: Group ID
            active_only: Only return active, non-unsubscribed recipients

        Returns:
            List of recipient IDs
        """
        stmt = (
            select(GroupRecipient.recipient_id)
            .where(GroupRecipient.group_id == group_id)
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
        return [row[0] for row in result.all()]

    async def get_recipients(
        self,
        group_id: int,
        active_only: bool = False,
        skip: int = 0,
        limit: int = 100
    ) -> Tuple[List[Recipient], int]:
        """
        Get recipient details for a group with pagination.

        Args:
            group_id: Group ID
            active_only: Only return active recipients
            skip: Pagination offset
            limit: Max results

        Returns:
            Tuple of (recipients list, total count)
        """
        # Build base query
        stmt = (
            select(Recipient)
            .join(GroupRecipient, Recipient.id == GroupRecipient.recipient_id)
            .where(
                and_(
                    GroupRecipient.group_id == group_id,
                    Recipient.deleted_at.is_(None)
                )
            )
        )

        if active_only:
            stmt = stmt.where(
                and_(
                    Recipient.is_active == True,
                    Recipient.unsubscribed == False
                )
            )

        # Get total count
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await self.db.execute(count_stmt)
        total = count_result.scalar() or 0

        # Apply pagination and ordering
        stmt = stmt.order_by(Recipient.name.asc()).offset(skip).limit(limit)

        # Execute query
        result = await self.db.execute(stmt)
        recipients = list(result.scalars().all())

        return recipients, total

    # ==================== STATISTICS ====================

    async def get_group_statistics(
        self,
        candidate_id: int
    ) -> Dict[str, Any]:
        """
        Get group statistics for a candidate.

        Returns:
            Dict with various stats
        """
        # Total groups
        total_groups = await self.count({"candidate_id": candidate_id})

        # Groups by type
        static_count = await self.count({
            "candidate_id": candidate_id,
            "group_type": GroupTypeEnum.STATIC
        })

        dynamic_count = await self.count({
            "candidate_id": candidate_id,
            "group_type": GroupTypeEnum.DYNAMIC
        })

        # Total unique recipients across all groups
        unique_recipients_stmt = (
            select(func.count(func.distinct(GroupRecipient.recipient_id)))
            .select_from(GroupRecipient)
            .join(RecipientGroup, GroupRecipient.group_id == RecipientGroup.id)
            .where(
                and_(
                    RecipientGroup.candidate_id == candidate_id,
                    RecipientGroup.deleted_at.is_(None)
                )
            )
        )
        unique_result = await self.db.execute(unique_recipients_stmt)
        unique_recipients = unique_result.scalar() or 0

        # Average group size
        avg_size = await self.get_avg(
            "total_recipients",
            filters={"candidate_id": candidate_id}
        ) or 0.0

        return {
            "total_groups": total_groups,
            "static_groups": static_count,
            "dynamic_groups": dynamic_count,
            "unique_recipients": unique_recipients,
            "avg_group_size": round(avg_size, 1)
        }

    async def get_groups_needing_refresh(
        self,
        candidate_id: int = None,
        stale_threshold_hours: int = 24
    ) -> List[RecipientGroup]:
        """
        Get dynamic groups that need refreshing.

        Args:
            candidate_id: Filter by candidate (optional)
            stale_threshold_hours: Hours since last refresh

        Returns:
            List of groups needing refresh
        """
        stale_time = datetime.now(timezone.utc) - timedelta(hours=stale_threshold_hours)

        stmt = (
            select(RecipientGroup)
            .where(
                and_(
                    RecipientGroup.group_type == GroupTypeEnum.DYNAMIC,
                    RecipientGroup.auto_refresh == True,
                    RecipientGroup.deleted_at.is_(None),
                    or_(
                        RecipientGroup.last_refreshed_at.is_(None),
                        RecipientGroup.last_refreshed_at < stale_time
                    )
                )
            )
        )

        if candidate_id:
            stmt = stmt.where(RecipientGroup.candidate_id == candidate_id)

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    # ==================== HELPER METHODS ====================

    async def _update_group_stats(self, group_id: int):
        """Update group recipient counts"""
        # Total recipients
        total_stmt = (
            select(func.count())
            .select_from(GroupRecipient)
            .where(GroupRecipient.group_id == group_id)
        )
        total_result = await self.db.execute(total_stmt)
        total = total_result.scalar() or 0

        # Active recipients
        active_stmt = (
            select(func.count())
            .select_from(GroupRecipient)
            .join(Recipient, GroupRecipient.recipient_id == Recipient.id)
            .where(
                and_(
                    GroupRecipient.group_id == group_id,
                    Recipient.is_active == True,
                    Recipient.unsubscribed == False,
                    Recipient.deleted_at.is_(None)
                )
            )
        )
        active_result = await self.db.execute(active_stmt)
        active = active_result.scalar() or 0

        # Update group
        await self.update(group_id, {
            "total_recipients": total,
            "active_recipients": active
        })
