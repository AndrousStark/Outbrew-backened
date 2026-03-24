"""
Async Candidate Repository for Phase 2 Optimization

Specialized async repository for Candidate (User) model with:
- Non-blocking I/O
- Authentication queries
- Profile management
- Statistics caching
- Relationship loading
"""
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
import logging

from app.repositories.base_async import AsyncBaseRepository
from app.models.candidate import Candidate
from app.core.cache_async import async_cache

logger = logging.getLogger(__name__)


class AsyncCandidateRepository(AsyncBaseRepository[Candidate]):
    """
    Async candidate-specific repository.

    Key Features:
    - Email-based lookups
    - Profile caching
    - Related data loading
    - Non-blocking operations
    """

    def __init__(self, db: AsyncSession):
        super().__init__(Candidate, db)

    # ==================== AUTHENTICATION QUERIES ====================

    async def get_by_email(
        self,
        email: str,
        use_cache: bool = True
    ) -> Optional[Candidate]:
        """
        Get candidate by email (for authentication, async).

        Args:
            email: Email address
            use_cache: Use cache

        Returns:
            Candidate or None
        """
        cache_key = f"candidate:email:{email}"

        # Try cache
        if use_cache:
            cached = await async_cache.get(cache_key)
            if cached:
                return cached

        # Query database
        candidate = await self.get_by_field("email", email)

        # Cache result
        if use_cache and candidate:
            await async_cache.set(cache_key, candidate, ttl=1800)  # 30 min

        return candidate

    async def get_by_username(
        self,
        username: str,
        use_cache: bool = True
    ) -> Optional[Candidate]:
        """Get candidate by username (async)"""
        return await self.get_by_field("username", username)

    # ==================== PROFILE QUERIES ====================

    async def get_with_relations(self, id: int) -> Optional[Candidate]:
        """
        Get candidate with all relations (async).

        Loads:
        - Applications
        - Resume versions
        - Email templates
        - Email logs
        - Notifications
        """
        stmt = (
            select(Candidate)
            .where(Candidate.id == id)
            .options(
                selectinload(Candidate.applications).limit(10),  # Recent apps
                selectinload(Candidate.resume_versions),
                selectinload(Candidate.email_templates).limit(5),
                selectinload(Candidate.notifications).limit(10)
            )
        )

        stmt = self._apply_soft_delete_filter(stmt)

        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def get_profile_summary(
        self,
        id: int,
        use_cache: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Get candidate profile summary (cached, async).

        Returns:
            {
                "candidate": Candidate,
                "total_applications": int,
                "total_resumes": int,
                "total_templates": int,
                "unread_notifications": int
            }
        """
        cache_key = f"candidate:profile:{id}"

        # Try cache
        if use_cache:
            cached = await async_cache.get(cache_key)
            if cached:
                return cached

        # Get candidate
        candidate = await self.get_by_id(id)
        if not candidate:
            return None

        # Get counts
        from app.models.application import Application
        from app.models.resume import ResumeVersion
        from app.models.email_template import EmailTemplate
        from app.models.notification import Notification

        # Run count queries
        total_applications_stmt = select(func.count()).select_from(Application).where(
            Application.candidate_id == id,
            Application.deleted_at.is_(None)
        )
        result = await self.db.execute(total_applications_stmt)
        total_applications = result.scalar() or 0

        total_resumes_stmt = select(func.count()).select_from(ResumeVersion).where(
            ResumeVersion.candidate_id == id,
            ResumeVersion.deleted_at.is_(None)
        )
        result = await self.db.execute(total_resumes_stmt)
        total_resumes = result.scalar() or 0

        total_templates_stmt = select(func.count()).select_from(EmailTemplate).where(
            EmailTemplate.candidate_id == id,
            EmailTemplate.deleted_at.is_(None)
        )
        result = await self.db.execute(total_templates_stmt)
        total_templates = result.scalar() or 0

        unread_notifications_stmt = select(func.count()).select_from(Notification).where(
            Notification.candidate_id == id,
            Notification.is_read == False
        )
        result = await self.db.execute(unread_notifications_stmt)
        unread_notifications = result.scalar() or 0

        summary = {
            "candidate": candidate,
            "total_applications": total_applications,
            "total_resumes": total_resumes,
            "total_templates": total_templates,
            "unread_notifications": unread_notifications
        }

        # Cache for 5 minutes
        if use_cache:
            await async_cache.set(cache_key, summary, ttl=300)

        return summary

    # ==================== USER MANAGEMENT ====================

    async def get_all_users(
        self,
        skip: int = 0,
        limit: int = 100,
        role: Optional[str] = None
    ) -> List[Candidate]:
        """
        Get all users (admin function, async).

        Args:
            skip: Offset
            limit: Limit
            role: Optional role filter

        Returns:
            List of candidates
        """
        filters = {}
        if role:
            filters["role"] = role

        return await self.get_all(
            skip=skip,
            limit=limit,
            filters=filters,
            order_by="created_at"
        )

    async def search_users(
        self,
        search_term: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Candidate]:
        """
        Search users by email, username, or full name (async).

        Args:
            search_term: Search query
            skip: Offset
            limit: Limit

        Returns:
            Matching candidates
        """
        return await self.search(
            search_fields=["email", "username", "full_name"],
            search_term=search_term,
            skip=skip,
            limit=limit
        )

    # ==================== PASSWORD MANAGEMENT ====================

    async def update_password(
        self,
        id: int,
        hashed_password: str
    ) -> bool:
        """
        Update candidate password (async).

        Args:
            id: Candidate ID
            hashed_password: New hashed password

        Returns:
            True if successful
        """
        result = await self.update(id, {"hashed_password": hashed_password})

        if result:
            # Invalidate email cache (forces re-auth)
            if result.email:
                await async_cache.delete(f"candidate:email:{result.email}")
            logger.info(f"🔒 [CANDIDATE-REPO-ASYNC] Updated password for candidate {id}")
            return True

        return False

    # ==================== STATISTICS ====================

    async def get_activity_stats(
        self,
        id: int,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get candidate activity statistics (async).

        Returns:
            {
                "emails_sent": int,
                "emails_opened": int,
                "responses_received": int,
                "interviews_scheduled": int,
                "offers_received": int
            }
        """
        cache_key = f"candidate:activity:{id}"

        # Try cache
        if use_cache:
            cached = await async_cache.get(cache_key)
            if cached:
                return cached

        # Query database
        from app.models.application import Application, ApplicationStatusEnum

        # Emails sent
        emails_sent_stmt = select(func.count()).select_from(Application).where(
            Application.candidate_id == id,
            Application.status.in_([
                ApplicationStatusEnum.SENT,
                ApplicationStatusEnum.OPENED,
                ApplicationStatusEnum.RESPONDED,
                ApplicationStatusEnum.INTERVIEW,
                ApplicationStatusEnum.OFFER
            ])
        )
        result = await self.db.execute(emails_sent_stmt)
        emails_sent = result.scalar() or 0

        # Emails opened
        emails_opened_stmt = select(func.count()).select_from(Application).where(
            Application.candidate_id == id,
            Application.status.in_([
                ApplicationStatusEnum.OPENED,
                ApplicationStatusEnum.RESPONDED,
                ApplicationStatusEnum.INTERVIEW,
                ApplicationStatusEnum.OFFER
            ])
        )
        result = await self.db.execute(emails_opened_stmt)
        emails_opened = result.scalar() or 0

        # Responses received
        responses_received_stmt = select(func.count()).select_from(Application).where(
            Application.candidate_id == id,
            Application.status.in_([
                ApplicationStatusEnum.RESPONDED,
                ApplicationStatusEnum.INTERVIEW,
                ApplicationStatusEnum.OFFER
            ])
        )
        result = await self.db.execute(responses_received_stmt)
        responses_received = result.scalar() or 0

        # Interviews scheduled
        interviews_scheduled_stmt = select(func.count()).select_from(Application).where(
            Application.candidate_id == id,
            Application.status == ApplicationStatusEnum.INTERVIEW
        )
        result = await self.db.execute(interviews_scheduled_stmt)
        interviews_scheduled = result.scalar() or 0

        # Offers received
        offers_received_stmt = select(func.count()).select_from(Application).where(
            Application.candidate_id == id,
            Application.status == ApplicationStatusEnum.OFFER
        )
        result = await self.db.execute(offers_received_stmt)
        offers_received = result.scalar() or 0

        stats = {
            "emails_sent": emails_sent,
            "emails_opened": emails_opened,
            "responses_received": responses_received,
            "interviews_scheduled": interviews_scheduled,
            "offers_received": offers_received
        }

        # Cache for 5 minutes
        if use_cache:
            await async_cache.set(cache_key, stats, ttl=300)

        return stats
