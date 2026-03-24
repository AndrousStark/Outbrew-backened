"""
Candidate Repository

Specialized repository for Candidate (User) model with:
- Authentication queries
- Profile management
- Statistics caching
- Relationship loading
"""
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session, selectinload
import logging

from app.repositories.base import BaseRepository
from app.models.candidate import Candidate
from app.core.cache import cache

logger = logging.getLogger(__name__)


class CandidateRepository(BaseRepository[Candidate]):
    """
    Candidate-specific repository.

    Key Features:
    - Email-based lookups
    - Profile caching
    - Related data loading
    """

    def __init__(self, db: Session):
        super().__init__(Candidate, db)

    # ==================== AUTHENTICATION QUERIES ====================

    def get_by_email(
        self,
        email: str,
        use_cache: bool = True
    ) -> Optional[Candidate]:
        """
        Get candidate by email (for authentication).

        Args:
            email: Email address
            use_cache: Use cache

        Returns:
            Candidate or None
        """
        cache_key = f"candidate:email:{email}"

        # Try cache
        if use_cache:
            cached = cache.get(cache_key)
            if cached:
                return cached

        # Query database
        candidate = self.get_by_field("email", email)

        # Cache result
        if use_cache and candidate:
            cache.set(cache_key, candidate, ttl=1800)  # 30 min

        return candidate

    def get_by_username(
        self,
        username: str,
        use_cache: bool = True
    ) -> Optional[Candidate]:
        """Get candidate by username"""
        return self.get_by_field("username", username)

    # ==================== PROFILE QUERIES ====================

    def get_with_relations(self, id: int) -> Optional[Candidate]:
        """
        Get candidate with all relations.

        Loads:
        - Applications
        - Resume versions
        - Email templates
        - Email logs
        - Notifications
        """
        query = (
            self.db.query(Candidate)
            .filter(Candidate.id == id)
            .options(
                selectinload(Candidate.applications).limit(10),  # Recent apps
                selectinload(Candidate.resume_versions),
                selectinload(Candidate.email_templates).limit(5),
                selectinload(Candidate.notifications).limit(10)
            )
        )

        query = self._apply_soft_delete_filter(query)

        return query.first()

    def get_profile_summary(
        self,
        id: int,
        use_cache: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Get candidate profile summary (cached).

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
            cached = cache.get(cache_key)
            if cached:
                return cached

        # Get candidate
        candidate = self.get_by_id(id)
        if not candidate:
            return None

        # Get counts
        from app.models.application import Application
        from app.models.resume import ResumeVersion
        from app.models.email_template import EmailTemplate
        from app.models.notification import Notification

        total_applications = self.db.query(Application).filter(
            Application.candidate_id == id,
            Application.deleted_at.is_(None)
        ).count()

        total_resumes = self.db.query(ResumeVersion).filter(
            ResumeVersion.candidate_id == id,
            ResumeVersion.deleted_at.is_(None)
        ).count()

        total_templates = self.db.query(EmailTemplate).filter(
            EmailTemplate.candidate_id == id,
            EmailTemplate.deleted_at.is_(None)
        ).count()

        unread_notifications = self.db.query(Notification).filter(
            Notification.candidate_id == id,
            Notification.is_read == False
        ).count()

        summary = {
            "candidate": candidate,
            "total_applications": total_applications,
            "total_resumes": total_resumes,
            "total_templates": total_templates,
            "unread_notifications": unread_notifications
        }

        # Cache for 5 minutes
        if use_cache:
            cache.set(cache_key, summary, ttl=300)

        return summary

    # ==================== USER MANAGEMENT ====================

    def get_all_users(
        self,
        skip: int = 0,
        limit: int = 100,
        role: Optional[str] = None
    ) -> List[Candidate]:
        """
        Get all users (admin function).

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

        return self.get_all(
            skip=skip,
            limit=limit,
            filters=filters,
            order_by="created_at"
        )

    def search_users(
        self,
        search_term: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Candidate]:
        """
        Search users by email, username, or full name.

        Args:
            search_term: Search query
            skip: Offset
            limit: Limit

        Returns:
            Matching candidates
        """
        return self.search(
            search_fields=["email", "username", "full_name"],
            search_term=search_term,
            skip=skip,
            limit=limit
        )

    # ==================== PASSWORD MANAGEMENT ====================

    def update_password(
        self,
        id: int,
        hashed_password: str
    ) -> bool:
        """
        Update candidate password.

        Args:
            id: Candidate ID
            hashed_password: New hashed password

        Returns:
            True if successful
        """
        result = self.update(id, {"hashed_password": hashed_password})

        if result:
            # Invalidate email cache (forces re-auth)
            if result.email:
                cache.delete(f"candidate:email:{result.email}")
            logger.info(f"🔒 [CANDIDATE-REPO] Updated password for candidate {id}")
            return True

        return False

    # ==================== STATISTICS ====================

    def get_activity_stats(
        self,
        id: int,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get candidate activity statistics.

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
            cached = cache.get(cache_key)
            if cached:
                return cached

        # Query database
        from app.models.application import Application, ApplicationStatusEnum

        stats = {
            "emails_sent": self.db.query(Application).filter(
                Application.candidate_id == id,
                Application.status.in_([
                    ApplicationStatusEnum.SENT,
                    ApplicationStatusEnum.OPENED,
                    ApplicationStatusEnum.RESPONDED,
                    ApplicationStatusEnum.INTERVIEW,
                    ApplicationStatusEnum.OFFER
                ])
            ).count(),

            "emails_opened": self.db.query(Application).filter(
                Application.candidate_id == id,
                Application.status.in_([
                    ApplicationStatusEnum.OPENED,
                    ApplicationStatusEnum.RESPONDED,
                    ApplicationStatusEnum.INTERVIEW,
                    ApplicationStatusEnum.OFFER
                ])
            ).count(),

            "responses_received": self.db.query(Application).filter(
                Application.candidate_id == id,
                Application.status.in_([
                    ApplicationStatusEnum.RESPONDED,
                    ApplicationStatusEnum.INTERVIEW,
                    ApplicationStatusEnum.OFFER
                ])
            ).count(),

            "interviews_scheduled": self.db.query(Application).filter(
                Application.candidate_id == id,
                Application.status == ApplicationStatusEnum.INTERVIEW
            ).count(),

            "offers_received": self.db.query(Application).filter(
                Application.candidate_id == id,
                Application.status == ApplicationStatusEnum.OFFER
            ).count()
        }

        # Cache for 5 minutes
        if use_cache:
            cache.set(cache_key, stats, ttl=300)

        return stats
