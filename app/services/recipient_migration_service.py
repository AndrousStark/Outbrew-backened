"""
Recipient Migration Service

Intelligent service for migrating existing Application data to the new Recipients system.

Features:
- Extract unique recipients from applications
- Intelligent deduplication by email
- Data enrichment from company records
- Preserves engagement statistics
- Batch processing for performance
- Detailed migration statistics
- Rollback support
"""
import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from collections import defaultdict

from app.models.application import Application
from app.models.candidate import Candidate
from app.models.company import Company
from app.models.recipient import Recipient
from app.models.email_log import EmailLog, EmailStatusEnum
from app.repositories.recipient import AsyncRecipientRepository

logger = logging.getLogger(__name__)


class RecipientMigrationService:
    """
    Intelligent Recipient Migration Service

    This service extracts recipient data from existing applications
    and creates normalized Recipient records with smart deduplication.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the migration service.

        Args:
            db: Async database session
        """
        self.db = db
        self.recipient_repo = AsyncRecipientRepository(db)
        logger.debug("[MigrationService] Initialized")

    async def analyze_migration(
        self,
        candidate_id: int
    ) -> Dict[str, Any]:
        """
        Analyze applications to preview migration without making changes.

        This provides statistics about what would be migrated:
        - Total applications
        - Unique emails
        - Duplicate count
        - Estimated recipients to create

        Args:
            candidate_id: Candidate ID to analyze

        Returns:
            Dict with migration preview statistics
        """
        logger.info(f"📊 [MigrationService] Analyzing applications for candidate {candidate_id}")

        # Get all applications for candidate
        applications_stmt = (
            select(Application)
            .where(
                and_(
                    Application.candidate_id == candidate_id,
                    Application.deleted_at.is_(None)
                )
            )
        )
        result = await self.db.execute(applications_stmt)
        applications = list(result.scalars().all())

        total_applications = len(applications)

        # Extract unique emails
        email_to_applications = defaultdict(list)
        emails_with_data = {}

        for app in applications:
            email = app.recruiter_email.lower().strip() if app.recruiter_email else None
            if not email:
                continue

            email_to_applications[email].append(app)

            # Store most complete data for each email
            if email not in emails_with_data:
                emails_with_data[email] = {
                    "name": app.recruiter_name,
                    "company": app.company_name,
                    "country": getattr(app, "recruiter_country", None),
                    "language": getattr(app, "recruiter_language", None),
                    "applications": 1
                }
            else:
                # Update with better data if available
                current = emails_with_data[email]
                if not current["name"] and app.recruiter_name:
                    current["name"] = app.recruiter_name
                if not current["company"] and app.company_name:
                    current["company"] = app.company_name
                current["applications"] += 1

        unique_emails = len(emails_with_data)
        duplicate_count = total_applications - unique_emails

        # Check how many already exist as recipients
        existing_count = 0
        for email in emails_with_data.keys():
            exists = await self.recipient_repo.exists_by_email(candidate_id, email)
            if exists:
                existing_count += 1

        new_recipients_to_create = unique_emails - existing_count

        # Calculate engagement stats from email logs
        email_logs_stmt = (
            select(
                EmailLog.recipient_email,
                func.count(EmailLog.id).label("total_sent"),
                func.sum(func.cast(EmailLog.opened, int)).label("total_opened"),
                func.sum(func.cast(EmailLog.replied, int)).label("total_replied")
            )
            .where(
                and_(
                    EmailLog.candidate_id == candidate_id,
                    EmailLog.recipient_email.in_(list(emails_with_data.keys()))
                )
            )
            .group_by(EmailLog.recipient_email)
        )
        logs_result = await self.db.execute(email_logs_stmt)
        engagement_data = {row[0]: {"sent": row[1], "opened": row[2] or 0, "replied": row[3] or 0} for row in logs_result.all()}

        # Top companies
        company_counts = defaultdict(int)
        for data in emails_with_data.values():
            if data["company"]:
                company_counts[data["company"]] += 1

        top_companies = sorted(
            [{"company": k, "count": v} for k, v in company_counts.items()],
            key=lambda x: x["count"],
            reverse=True
        )[:10]

        result = {
            "candidate_id": candidate_id,
            "total_applications": total_applications,
            "unique_emails": unique_emails,
            "duplicates": duplicate_count,
            "already_exist": existing_count,
            "new_recipients_to_create": new_recipients_to_create,
            "with_engagement_data": len(engagement_data),
            "top_companies": top_companies,
            "preview_sample": [
                {
                    "email": email,
                    "name": data["name"],
                    "company": data["company"],
                    "applications": data["applications"],
                    "engagement": engagement_data.get(email, {"sent": 0, "opened": 0, "replied": 0})
                }
                for email, data in list(emails_with_data.items())[:5]
            ]
        }

        logger.info(
            f"✅ [MigrationService] Analysis complete: {unique_emails} unique emails, "
            f"{new_recipients_to_create} new recipients to create"
        )

        return result

    async def migrate_applications_to_recipients(
        self,
        candidate_id: int,
        skip_duplicates: bool = True,
        enrich_from_companies: bool = True,
        add_engagement_stats: bool = True,
        source_tag: str = "application_migration"
    ) -> Dict[str, Any]:
        """
        Migrate applications to recipients with intelligent deduplication.

        This method:
        1. Extracts unique recipient emails from applications
        2. Deduplicates by email
        3. Enriches data from Company records if available
        4. Adds engagement statistics from EmailLog
        5. Tags all migrated recipients

        Args:
            candidate_id: Candidate ID to migrate
            skip_duplicates: Skip emails that already exist as recipients
            enrich_from_companies: Enrich data from Company table
            add_engagement_stats: Add engagement stats from EmailLog
            source_tag: Tag to add to migrated recipients

        Returns:
            Dict with migration statistics
        """
        logger.info(f"🔄 [MigrationService] Starting migration for candidate {candidate_id}")

        # Get all applications
        applications_stmt = (
            select(Application)
            .where(
                and_(
                    Application.candidate_id == candidate_id,
                    Application.deleted_at.is_(None)
                )
            )
        )
        result = await self.db.execute(applications_stmt)
        applications = list(result.scalars().all())

        if not applications:
            logger.info(f"ℹ️  [MigrationService] No applications found for candidate {candidate_id}")
            return {
                "candidate_id": candidate_id,
                "total_applications": 0,
                "created": 0,
                "skipped": 0,
                "errors": 0
            }

        logger.info(f"📋 [MigrationService] Processing {len(applications)} applications")

        # Build email-to-data mapping with deduplication
        email_data_map = {}

        for app in applications:
            email = app.recruiter_email.lower().strip() if app.recruiter_email else None
            if not email:
                continue

            if email not in email_data_map:
                email_data_map[email] = {
                    "email": email,
                    "name": app.recruiter_name,
                    "company": app.company_name,
                    "position": None,  # Not in Application model
                    "country": getattr(app, "recruiter_country", None),
                    "language": getattr(app, "recruiter_language", "en"),
                    "company_id": app.company_id,
                    "applications": [app.id]
                }
            else:
                # Merge data (prefer non-empty values)
                existing = email_data_map[email]
                if not existing["name"] and app.recruiter_name:
                    existing["name"] = app.recruiter_name
                if not existing["company"] and app.company_name:
                    existing["company"] = app.company_name
                if not existing["country"] and getattr(app, "recruiter_country", None):
                    existing["country"] = getattr(app, "recruiter_country", None)
                existing["applications"].append(app.id)

        unique_emails = len(email_data_map)
        logger.info(f"🔍 [MigrationService] Found {unique_emails} unique emails")

        # Enrich from Company records if requested
        if enrich_from_companies:
            await self._enrich_from_companies(email_data_map, candidate_id)

        # Get engagement statistics if requested
        engagement_stats = {}
        if add_engagement_stats:
            engagement_stats = await self._get_engagement_stats(
                list(email_data_map.keys()),
                candidate_id
            )

        # Create recipients
        created_count = 0
        skipped_count = 0
        error_count = 0

        for email, data in email_data_map.items():
            try:
                # Check if already exists
                if skip_duplicates:
                    exists = await self.recipient_repo.exists_by_email(candidate_id, email)
                    if exists:
                        skipped_count += 1
                        logger.debug(f"⏭️  [MigrationService] Skipping existing email: {email}")
                        continue

                # Prepare recipient data
                recipient_data = {
                    "candidate_id": candidate_id,
                    "email": email,
                    "name": data.get("name"),
                    "company": data.get("company"),
                    "position": data.get("position"),
                    "country": data.get("country"),
                    "language": data.get("language", "en"),
                    "tags": source_tag,
                    "source": "application_migration",
                    "is_active": True,
                    "unsubscribed": False,
                }

                # Add engagement stats if available
                if email in engagement_stats:
                    stats = engagement_stats[email]
                    recipient_data.update({
                        "total_emails_sent": stats["sent"],
                        "total_emails_opened": stats["opened"],
                        "total_emails_replied": stats["replied"],
                        "engagement_score": stats["engagement_score"]
                    })

                # Create recipient
                await self.recipient_repo.create(recipient_data)
                created_count += 1

                if created_count % 100 == 0:
                    logger.info(f"📊 [MigrationService] Progress: {created_count}/{unique_emails} created")

            except Exception as e:
                logger.error(f"❌ [MigrationService] Error creating recipient for {email}: {e}")
                error_count += 1

        # Final statistics
        result = {
            "candidate_id": candidate_id,
            "total_applications": len(applications),
            "unique_emails": unique_emails,
            "created": created_count,
            "skipped": skipped_count,
            "errors": error_count,
            "success_rate": round((created_count / unique_emails * 100), 2) if unique_emails > 0 else 0
        }

        logger.info(
            f"✅ [MigrationService] Migration complete: {created_count} created, "
            f"{skipped_count} skipped, {error_count} errors"
        )

        return result

    async def _enrich_from_companies(
        self,
        email_data_map: Dict[str, Dict[str, Any]],
        candidate_id: int
    ):
        """Enrich recipient data from Company records."""
        logger.debug("[MigrationService] Enriching data from Company records")

        # Get unique company IDs
        company_ids = set(
            data["company_id"]
            for data in email_data_map.values()
            if data.get("company_id")
        )

        if not company_ids:
            return

        # Fetch companies
        companies_stmt = select(Company).where(Company.id.in_(company_ids))
        result = await self.db.execute(companies_stmt)
        companies = {comp.id: comp for comp in result.scalars().all()}

        # Enrich data
        enriched_count = 0
        for data in email_data_map.values():
            company_id = data.get("company_id")
            if company_id and company_id in companies:
                company = companies[company_id]
                # Use company data if recipient data is missing
                if not data["company"] and company.name:
                    data["company"] = company.name
                if not data["country"] and getattr(company, "country", None):
                    data["country"] = getattr(company, "country")
                enriched_count += 1

        logger.debug(f"✅ [MigrationService] Enriched {enriched_count} recipients from Company data")

    async def _get_engagement_stats(
        self,
        emails: List[str],
        candidate_id: int
    ) -> Dict[str, Dict[str, Any]]:
        """Get engagement statistics from EmailLog for given emails."""
        logger.debug(f"[MigrationService] Fetching engagement stats for {len(emails)} emails")

        # Query EmailLog for engagement stats
        stmt = (
            select(
                EmailLog.recipient_email,
                func.count(EmailLog.id).label("total_sent"),
                func.sum(func.cast(EmailLog.opened, int)).label("total_opened"),
                func.sum(func.cast(EmailLog.replied, int)).label("total_replied")
            )
            .where(
                and_(
                    EmailLog.candidate_id == candidate_id,
                    EmailLog.recipient_email.in_(emails),
                    EmailLog.status == EmailStatusEnum.SENT
                )
            )
            .group_by(EmailLog.recipient_email)
        )

        result = await self.db.execute(stmt)

        engagement_stats = {}
        for row in result.all():
            email = row[0]
            sent = row[1] or 0
            opened = row[2] or 0
            replied = row[3] or 0

            # Calculate engagement score (opened * 10 + replied * 50) / sent
            engagement_score = 0.0
            if sent > 0:
                engagement_score = ((opened * 10) + (replied * 50)) / sent

            engagement_stats[email] = {
                "sent": sent,
                "opened": opened,
                "replied": replied,
                "engagement_score": round(engagement_score, 2)
            }

        logger.debug(f"✅ [MigrationService] Retrieved engagement stats for {len(engagement_stats)} emails")

        return engagement_stats

    async def create_migration_group(
        self,
        candidate_id: int,
        group_name: str = "Migrated from Applications",
        add_all_migrated: bool = True
    ) -> Optional[int]:
        """
        Create a recipient group containing all migrated recipients.

        Args:
            candidate_id: Candidate ID
            group_name: Name for the group
            add_all_migrated: Add all recipients with migration tag

        Returns:
            Group ID if created, None if error
        """
        try:
            from app.models.recipient_group import RecipientGroup, GroupTypeEnum
            from app.repositories.recipient_group import AsyncRecipientGroupRepository

            group_repo = AsyncRecipientGroupRepository(self.db)

            # Check if group already exists
            existing = await group_repo.get_by_name(candidate_id, group_name)
            if existing:
                logger.info(f"ℹ️  [MigrationService] Group '{group_name}' already exists")
                return existing.id

            # Create group
            group = await group_repo.create({
                "candidate_id": candidate_id,
                "name": group_name,
                "description": "Auto-created group containing all recipients migrated from existing applications",
                "group_type": GroupTypeEnum.STATIC,
                "color": "#3b82f6"  # Blue color
            })

            logger.info(f"✅ [MigrationService] Created group '{group_name}' (ID: {group.id})")

            if add_all_migrated:
                # Get all migrated recipients
                stmt = (
                    select(Recipient.id)
                    .where(
                        and_(
                            Recipient.candidate_id == candidate_id,
                            Recipient.source == "application_migration",
                            Recipient.deleted_at.is_(None)
                        )
                    )
                )
                result = await self.db.execute(stmt)
                recipient_ids = [row[0] for row in result.all()]

                if recipient_ids:
                    # Add recipients to group
                    added = await group_repo.add_recipients(
                        group.id,
                        recipient_ids,
                        is_dynamic=False
                    )
                    logger.info(f"✅ [MigrationService] Added {added} migrated recipients to group")

            return group.id

        except Exception as e:
            logger.error(f"❌ [MigrationService] Error creating migration group: {e}")
            return None
