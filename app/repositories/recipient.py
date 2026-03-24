"""
Async Recipient Repository

Specialized async repository for Recipient model with:
- Multi-field search (email, name, company, position)
- Advanced filtering (tags, country, company)
- CSV import with duplicate detection
- Engagement tracking queries
- Bulk operations
"""
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_, and_
from sqlalchemy.orm import selectinload
import logging
import csv
import io

from app.repositories.base_async import AsyncBaseRepository
from app.models.recipient import Recipient
from app.core.cache_async import async_cache
from app.services.intelligent_csv_parser import IntelligentCSVParser

logger = logging.getLogger(__name__)


class AsyncRecipientRepository(AsyncBaseRepository[Recipient]):
    """
    Async recipient-specific repository.

    Key Features:
    - Email-based lookups with caching
    - Multi-field search across name, email, company, position
    - CSV import with duplicate detection
    - Tag filtering
    - Engagement score queries
    - Bulk operations
    """

    def __init__(self, db: AsyncSession):
        super().__init__(Recipient, db)

    # ==================== LOOKUP METHODS ====================

    async def get_by_email(
        self,
        candidate_id: int,
        email: str,
        use_cache: bool = True
    ) -> Optional[Recipient]:
        """
        Get recipient by email within candidate's scope.

        Args:
            candidate_id: Owner candidate ID
            email: Email address
            use_cache: Use cache

        Returns:
            Recipient or None
        """
        email = email.strip().lower()
        cache_key = f"recipient:candidate:{candidate_id}:email:{email}"

        # Try cache
        if use_cache:
            cached = await async_cache.get(cache_key)
            if cached:
                logger.debug(f"📦 [REPO] Cache hit for recipient email: {email}")
                return cached

        # Query database
        stmt = (
            select(Recipient)
            .where(
                and_(
                    Recipient.candidate_id == candidate_id,
                    Recipient.email == email,
                    Recipient.deleted_at.is_(None)
                )
            )
        )

        result = await self.db.execute(stmt)
        recipient = result.scalars().first()

        # Cache result
        if use_cache and recipient:
            await async_cache.set(cache_key, recipient, ttl=1800)  # 30 min

        return recipient

    async def exists_by_email(
        self,
        candidate_id: int,
        email: str
    ) -> bool:
        """Check if recipient exists by email"""
        recipient = await self.get_by_email(candidate_id, email, use_cache=False)
        return recipient is not None

    # ==================== SEARCH & FILTER ====================

    async def search_recipients(
        self,
        candidate_id: int,
        search_term: str = None,
        company: str = None,
        tags: List[str] = None,
        country: str = None,
        is_active: bool = True,
        unsubscribed: bool = False,
        skip: int = 0,
        limit: int = 100,
        order_by: str = "created_at",
        order_desc: bool = True
    ) -> Tuple[List[Recipient], int]:
        """
        Advanced search and filter recipients.

        Args:
            candidate_id: Owner candidate ID
            search_term: Search across name, email, position
            company: Filter by company
            tags: Filter by tags (comma-separated in DB)
            country: Filter by country
            is_active: Filter active recipients
            unsubscribed: Include unsubscribed
            skip: Pagination offset
            limit: Max results
            order_by: Sort field
            order_desc: Sort descending

        Returns:
            Tuple of (recipients list, total count)
        """
        # Build base query
        stmt = (
            select(Recipient)
            .where(
                and_(
                    Recipient.candidate_id == candidate_id,
                    Recipient.deleted_at.is_(None)
                )
            )
        )

        # Apply filters
        if is_active is not None:
            stmt = stmt.where(Recipient.is_active == is_active)

        if not unsubscribed:
            stmt = stmt.where(Recipient.unsubscribed == False)

        if company:
            stmt = stmt.where(Recipient.company.ilike(f"%{company}%"))

        if country:
            stmt = stmt.where(Recipient.country == country)

        if tags:
            # Tags are comma-separated in DB
            tag_conditions = []
            for tag in tags:
                tag_conditions.append(Recipient.tags.ilike(f"%{tag}%"))
            stmt = stmt.where(or_(*tag_conditions))

        # Search across multiple fields
        if search_term:
            search_conditions = [
                Recipient.name.ilike(f"%{search_term}%"),
                Recipient.email.ilike(f"%{search_term}%"),
                Recipient.position.ilike(f"%{search_term}%"),
                Recipient.company.ilike(f"%{search_term}%")
            ]
            stmt = stmt.where(or_(*search_conditions))

        # Get total count
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await self.db.execute(count_stmt)
        total = count_result.scalar() or 0

        # Apply ordering (allowlisted columns only)
        ALLOWED_ORDER_COLUMNS = {"created_at", "name", "company", "email", "engagement_score", "position", "country"}
        if order_by and order_by in ALLOWED_ORDER_COLUMNS and hasattr(Recipient, order_by):
            order_column = getattr(Recipient, order_by)
            stmt = stmt.order_by(order_column.desc() if order_desc else order_column.asc())

        # Apply pagination
        stmt = stmt.offset(skip).limit(limit)

        # Execute query
        result = await self.db.execute(stmt)
        recipients = list(result.scalars().all())

        logger.info(f"🔍 [REPO] Searched recipients: {len(recipients)}/{total} results")
        return recipients, total

    async def get_by_tags(
        self,
        candidate_id: int,
        tags: List[str],
        match_all: bool = False
    ) -> List[Recipient]:
        """
        Get recipients by tags.

        Args:
            candidate_id: Owner candidate ID
            tags: List of tags to match
            match_all: If True, match all tags; if False, match any tag

        Returns:
            List of recipients
        """
        stmt = (
            select(Recipient)
            .where(
                and_(
                    Recipient.candidate_id == candidate_id,
                    Recipient.deleted_at.is_(None)
                )
            )
        )

        if match_all:
            # Match all tags (AND)
            for tag in tags:
                stmt = stmt.where(Recipient.tags.ilike(f"%{tag}%"))
        else:
            # Match any tag (OR)
            tag_conditions = [Recipient.tags.ilike(f"%{tag}%") for tag in tags]
            stmt = stmt.where(or_(*tag_conditions))

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_by_company(
        self,
        candidate_id: int,
        company: str
    ) -> List[Recipient]:
        """Get all recipients from a specific company"""
        return await self.get_all(
            filters={"candidate_id": candidate_id, "company": company},
            include_deleted=False
        )

    # ==================== ENGAGEMENT QUERIES ====================

    async def get_high_engagement(
        self,
        candidate_id: int,
        min_score: float = 50.0,
        limit: int = 50
    ) -> List[Recipient]:
        """
        Get recipients with high engagement scores.

        Args:
            candidate_id: Owner candidate ID
            min_score: Minimum engagement score
            limit: Max results

        Returns:
            List of high-engagement recipients
        """
        stmt = (
            select(Recipient)
            .where(
                and_(
                    Recipient.candidate_id == candidate_id,
                    Recipient.engagement_score >= min_score,
                    Recipient.deleted_at.is_(None)
                )
            )
            .order_by(Recipient.engagement_score.desc())
            .limit(limit)
        )

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_never_contacted(
        self,
        candidate_id: int,
        limit: int = 100
    ) -> List[Recipient]:
        """Get recipients who have never been emailed"""
        stmt = (
            select(Recipient)
            .where(
                and_(
                    Recipient.candidate_id == candidate_id,
                    Recipient.total_emails_sent == 0,
                    Recipient.is_active == True,
                    Recipient.unsubscribed == False,
                    Recipient.deleted_at.is_(None)
                )
            )
            .order_by(Recipient.created_at.desc())
            .limit(limit)
        )

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    # ==================== BULK OPERATIONS ====================

    async def bulk_create_from_csv(
        self,
        candidate_id: int,
        csv_content: str,
        source: str = "csv_import",
        skip_duplicates: bool = True
    ) -> Dict[str, Any]:
        """
        Bulk import recipients from CSV content using Intelligent CSV Parser.

        CSV Format (flexible - uses fuzzy column matching):
            Any combination of: email, name, first name, last name, company, position, country, etc.

        Args:
            candidate_id: Owner candidate ID
            csv_content: CSV file content as string
            source: Source identifier
            skip_duplicates: Skip existing emails

        Returns:
            Dict with import statistics
        """
        logger.info("🧠 [REPO] Using Intelligent CSV Parser for bulk import")

        # Use intelligent parser
        parser = IntelligentCSVParser(fuzzy_threshold=70)
        parse_result = parser._parse_csv(csv_content)

        logger.info(
            f"📊 [REPO] Parsed {parse_result.total_rows} rows: "
            f"{parse_result.valid_rows} valid, {parse_result.invalid_rows} invalid"
        )

        created = []
        skipped = []
        errors = []
        seen_emails = set()  # Track emails in this batch

        for parsed_recipient in parse_result.recipients:
            try:
                # Skip if no email
                if not parsed_recipient.email:
                    errors.append(f"Row {parsed_recipient.row_number}: Missing email")
                    continue

                email = parsed_recipient.email.strip().lower()

                # Skip empty emails
                if not email:
                    errors.append(f"Row {parsed_recipient.row_number}: Empty email")
                    continue

                # Check for duplicates within this batch
                if email in seen_emails:
                    skipped.append(email)
                    logger.debug(f"⏭️  [REPO] Skipping batch duplicate: {email}")
                    continue

                # Check for duplicates in database
                if skip_duplicates:
                    exists = await self.exists_by_email(candidate_id, email)
                    if exists:
                        skipped.append(email)
                        logger.debug(f"⏭️  [REPO] Skipping database duplicate: {email}")
                        continue

                # Prepare recipient data using parsed values
                recipient_data = {
                    "candidate_id": candidate_id,
                    "email": email,
                    "name": parsed_recipient.name or None,  # Intelligent parser handles name merging
                    "company": parsed_recipient.company or None,
                    "position": parsed_recipient.position or None,
                    "country": parsed_recipient.country or None,
                    "language": "en",  # Default language
                    "tags": None,
                    "source": source,
                    "is_active": True,
                    "unsubscribed": False,
                }

                # Create recipient
                recipient = await self.create(recipient_data)
                created.append(recipient)
                seen_emails.add(email)  # Track this email
                logger.debug(
                    f"✅ [REPO] Created: {parsed_recipient.name} <{email}> at {parsed_recipient.company}"
                )

            except Exception as e:
                # Rollback the session after error to allow continuing
                await self.db.rollback()
                errors.append(f"Row {parsed_recipient.row_number}: {str(e)}")
                logger.error(f"❌ [REPO] CSV import error on row {parsed_recipient.row_number}: {e}")

        # Invalidate cache
        await self._invalidate_cache()

        result = {
            "created": len(created),
            "skipped": len(skipped),
            "errors": len(errors),
            "total_processed": len(created) + len(skipped) + len(errors),
            "error_details": errors if errors else None,
            "column_mappings": [
                {
                    "detected_column": m.detected_column,
                    "mapped_to": m.mapped_to,
                    "confidence": m.confidence
                }
                for m in parse_result.column_mappings
            ],
            "detected_country": parse_result.detected_country,
            "country_confidence": parse_result.country_confidence
        }

        logger.info(
            f"📥 [REPO] Intelligent CSV import complete: {result['created']} created, "
            f"{result['skipped']} skipped, {result['errors']} errors"
        )
        logger.info(f"🎯 [REPO] Column mappings: {result['column_mappings']}")
        if result['detected_country']:
            logger.info(
                f"🌍 [REPO] Detected country: {result['detected_country']} "
                f"({result['country_confidence']:.1f}% confidence)"
            )

        return result

    async def bulk_add_tag(
        self,
        recipient_ids: List[int],
        tag: str
    ) -> int:
        """
        Add tag to multiple recipients.

        Args:
            recipient_ids: List of recipient IDs
            tag: Tag to add

        Returns:
            Number of recipients updated
        """
        updated_count = 0

        for recipient_id in recipient_ids:
            recipient = await self.get_by_id(recipient_id, use_cache=False)
            if recipient:
                # Add tag if not already present
                existing_tags = recipient.tags.split(",") if recipient.tags else []
                if tag not in existing_tags:
                    existing_tags.append(tag)
                    await self.update(
                        recipient_id,
                        {"tags": ",".join(existing_tags)}
                    )
                    updated_count += 1

        logger.info(f"🏷️  [REPO] Added tag '{tag}' to {updated_count} recipients")
        return updated_count

    async def bulk_update_engagement(
        self,
        recipient_id: int,
        sent: bool = False,
        opened: bool = False,
        replied: bool = False
    ) -> Optional[Recipient]:
        """
        Update engagement stats for a recipient.

        Args:
            recipient_id: Recipient ID
            sent: Email was sent
            opened: Email was opened
            replied: Recipient replied

        Returns:
            Updated recipient
        """
        recipient = await self.get_by_id(recipient_id, use_cache=False)
        if not recipient:
            return None

        updates = {}

        if sent:
            updates["total_emails_sent"] = recipient.total_emails_sent + 1

        if opened:
            updates["total_emails_opened"] = recipient.total_emails_opened + 1

        if replied:
            updates["total_emails_replied"] = recipient.total_emails_replied + 1

        # Calculate engagement score
        if updates:
            # Engagement score = (opens * 10 + replies * 50) / sends
            total_sent = updates.get("total_emails_sent", recipient.total_emails_sent)
            total_opened = updates.get("total_emails_opened", recipient.total_emails_opened)
            total_replied = updates.get("total_emails_replied", recipient.total_emails_replied)

            if total_sent > 0:
                engagement_score = ((total_opened * 10) + (total_replied * 50)) / total_sent
                updates["engagement_score"] = round(engagement_score, 2)

            return await self.update(recipient_id, updates)

        return recipient

    # ==================== STATISTICS ====================

    async def get_statistics(self, candidate_id: int) -> Dict[str, Any]:
        """
        Get recipient statistics for a candidate.

        Returns:
            Dict with various stats
        """
        # Total recipients
        total = await self.count({"candidate_id": candidate_id})

        # Active recipients
        active = await self.count({
            "candidate_id": candidate_id,
            "is_active": True,
            "unsubscribed": False
        })

        # Unsubscribed
        unsubscribed = await self.count({
            "candidate_id": candidate_id,
            "unsubscribed": True
        })

        # Never contacted
        never_contacted_stmt = (
            select(func.count())
            .select_from(Recipient)
            .where(
                and_(
                    Recipient.candidate_id == candidate_id,
                    Recipient.total_emails_sent == 0,
                    Recipient.deleted_at.is_(None)
                )
            )
        )
        never_contacted_result = await self.db.execute(never_contacted_stmt)
        never_contacted = never_contacted_result.scalar() or 0

        # Average engagement score
        avg_engagement = await self.get_avg(
            "engagement_score",
            filters={"candidate_id": candidate_id}
        ) or 0.0

        # Top companies
        top_companies_stmt = (
            select(Recipient.company, func.count(Recipient.id).label("count"))
            .where(
                and_(
                    Recipient.candidate_id == candidate_id,
                    Recipient.company.isnot(None),
                    Recipient.deleted_at.is_(None)
                )
            )
            .group_by(Recipient.company)
            .order_by(func.count(Recipient.id).desc())
            .limit(10)
        )
        top_companies_result = await self.db.execute(top_companies_stmt)
        top_companies = [
            {"company": row[0], "count": row[1]}
            for row in top_companies_result.all()
        ]

        return {
            "total": total,
            "active": active,
            "unsubscribed": unsubscribed,
            "never_contacted": never_contacted,
            "avg_engagement_score": round(avg_engagement, 2),
            "top_companies": top_companies
        }

    # ==================== GROUP MEMBERSHIP ====================

    async def get_with_groups(self, recipient_id: int) -> Optional[Recipient]:
        """
        Get recipient with group memberships.

        Loads:
        - Group memberships
        - Campaign sends
        """
        stmt = (
            select(Recipient)
            .where(Recipient.id == recipient_id)
            .options(
                selectinload(Recipient.group_memberships),
                selectinload(Recipient.campaign_sends).limit(10)
            )
        )

        stmt = self._apply_soft_delete_filter(stmt)

        result = await self.db.execute(stmt)
        return result.scalars().first()
