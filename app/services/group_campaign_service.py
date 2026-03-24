"""
Group Campaign Service

Intelligent service for managing and sending group email campaigns with:
- Per-recipient template personalization
- Rate limiting with configurable delays
- Progress tracking and real-time updates
- Pause/Resume functionality
- Error handling and automatic retries
- Engagement tracking integration
- Email log creation
"""
import asyncio
import logging
import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.models.candidate import Candidate
from app.models.group_campaign import GroupCampaign, CampaignStatusEnum
from app.models.group_campaign_recipient import GroupCampaignRecipient, RecipientStatusEnum
from app.models.recipient import Recipient
from app.models.email_log import EmailLog, EmailStatusEnum
from app.repositories.group_campaign import AsyncGroupCampaignRepository
from app.repositories.recipient import AsyncRecipientRepository
from app.services.template_engine import TemplateEngine, get_template_engine
from app.services.email_service import EmailService
from app.core.database import get_db
from app.core.encryption import decrypt_value

logger = logging.getLogger(__name__)


class GroupCampaignService:
    """
    Intelligent Group Campaign Service

    This service orchestrates the entire campaign sending process with:
    - Smart rate limiting
    - Real-time progress tracking
    - Intelligent retry logic
    - Engagement score updates
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the campaign service.

        Args:
            db: Async database session
        """
        self.db = db
        self.campaign_repo = AsyncGroupCampaignRepository(db)
        self.recipient_repo = AsyncRecipientRepository(db)
        self.template_engine = get_template_engine(strict_mode=False)
        logger.debug("[GroupCampaignService] Initialized")

    async def prepare_campaign(
        self,
        campaign_id: int,
        candidate: Candidate
    ) -> Dict[str, Any]:
        """
        Prepare campaign for sending by rendering all templates.

        This method:
        1. Gets all pending recipients
        2. Renders subject and body for each recipient
        3. Stores rendered content in campaign_recipients table
        4. Generates unique tracking IDs

        Args:
            campaign_id: Campaign ID to prepare
            candidate: Candidate (sender) object

        Returns:
            Dict with preparation statistics

        Raises:
            ValueError: If campaign not found or not in correct status
        """
        logger.info(f"📋 [CampaignService] Preparing campaign {campaign_id}")

        # Get campaign
        campaign = await self.campaign_repo.get_with_recipients(campaign_id)
        if not campaign:
            raise ValueError(f"Campaign {campaign_id} not found")

        # Verify campaign status
        if campaign.status not in [CampaignStatusEnum.DRAFT, CampaignStatusEnum.SCHEDULED]:
            raise ValueError(
                f"Campaign must be in DRAFT or SCHEDULED status to prepare. "
                f"Current status: {campaign.status}"
            )

        # Get pending campaign recipients
        pending_recipients = await self.campaign_repo.get_pending_recipients(
            campaign_id,
            limit=10000  # Process all
        )

        logger.info(
            f"📝 [CampaignService] Rendering templates for {len(pending_recipients)} recipients"
        )

        rendered_count = 0
        skipped_count = 0
        error_count = 0

        for campaign_recipient in pending_recipients:
            try:
                recipient = campaign_recipient.recipient

                if not recipient:
                    logger.warning(
                        f"⚠️  [CampaignService] No recipient for campaign_recipient {campaign_recipient.id}"
                    )
                    skipped_count += 1
                    continue

                # Check if recipient is active and not unsubscribed
                if not recipient.is_active or recipient.unsubscribed:
                    campaign_recipient.status = RecipientStatusEnum.SKIPPED
                    campaign_recipient.error_message = (
                        "Inactive" if not recipient.is_active else "Unsubscribed"
                    )
                    skipped_count += 1
                    continue

                # Build template context
                context = self.template_engine.build_recipient_context(
                    recipient=recipient,
                    candidate=candidate
                )

                # Render templates
                try:
                    rendered_subject = self.template_engine.render(
                        campaign.subject_template,
                        context,
                        validate=False
                    )

                    rendered_body = self.template_engine.render(
                        campaign.body_template_html,
                        context,
                        validate=False
                    )

                    # Generate unique tracking ID
                    tracking_id = self._generate_tracking_id(campaign_id, recipient.id)

                    # Update campaign recipient with rendered content
                    campaign_recipient.rendered_subject = rendered_subject
                    campaign_recipient.rendered_body_html = rendered_body
                    campaign_recipient.tracking_id = tracking_id

                    rendered_count += 1

                except Exception as e:
                    logger.error(
                        f"❌ [CampaignService] Template rendering failed for recipient {recipient.id}: {e}"
                    )
                    campaign_recipient.status = RecipientStatusEnum.FAILED
                    campaign_recipient.error_message = f"Template error: {str(e)}"
                    error_count += 1

            except Exception as e:
                logger.error(
                    f"❌ [CampaignService] Error processing campaign_recipient {campaign_recipient.id}: {e}"
                )
                error_count += 1

        # Commit all updates
        await self.db.commit()

        result = {
            "campaign_id": campaign_id,
            "total_recipients": len(pending_recipients),
            "rendered": rendered_count,
            "skipped": skipped_count,
            "errors": error_count,
            "ready_to_send": rendered_count
        }

        logger.info(
            f"✅ [CampaignService] Campaign {campaign_id} prepared: "
            f"{rendered_count} ready, {skipped_count} skipped, {error_count} errors"
        )

        return result

    async def send_campaign_batch(
        self,
        campaign_id: int,
        candidate: Candidate,
        batch_size: int = 100,
        check_pause: bool = True
    ) -> Dict[str, Any]:
        """
        Send a batch of campaign emails.

        This method:
        1. Gets pending recipients (up to batch_size)
        2. Sends emails with rate limiting
        3. Updates recipient status
        4. Tracks progress
        5. Checks for pause signal

        Args:
            campaign_id: Campaign ID
            candidate: Candidate (sender) object
            batch_size: Number of emails to send in this batch
            check_pause: Whether to check for pause status

        Returns:
            Dict with batch sending statistics
        """
        logger.info(f"📤 [CampaignService] Sending batch for campaign {campaign_id}")

        # Get campaign
        campaign = await self.campaign_repo.get_by_id(campaign_id, use_cache=False)
        if not campaign:
            raise ValueError(f"Campaign {campaign_id} not found")

        # Check if paused
        if check_pause and campaign.status == CampaignStatusEnum.PAUSED:
            logger.info(f"⏸️  [CampaignService] Campaign {campaign_id} is paused, stopping")
            return {
                "campaign_id": campaign_id,
                "status": "paused",
                "sent": 0,
                "failed": 0,
                "remaining": 0
            }

        # Get pending recipients with rendered content
        pending_recipients = await self.campaign_repo.get_pending_recipients(
            campaign_id,
            limit=batch_size
        )

        if not pending_recipients:
            # No more recipients to send
            logger.info(f"✅ [CampaignService] Campaign {campaign_id} complete - no more pending recipients")
            await self.campaign_repo.complete_campaign(campaign_id)
            return {
                "campaign_id": campaign_id,
                "status": "completed",
                "sent": 0,
                "failed": 0,
                "remaining": 0
            }

        logger.info(
            f"📨 [CampaignService] Processing {len(pending_recipients)} recipients "
            f"with {campaign.send_delay_seconds}s delay"
        )

        sent_count = 0
        failed_count = 0

        # Create sync EmailService for actual sending
        # Note: EmailService uses sync SMTP, we wrap it in async
        for idx, campaign_recipient in enumerate(pending_recipients):
            try:
                # Check pause before each email
                if check_pause and idx > 0:
                    # Reload campaign to check status
                    campaign = await self.campaign_repo.get_by_id(campaign_id, use_cache=False)
                    if campaign.status == CampaignStatusEnum.PAUSED:
                        logger.info(f"⏸️  [CampaignService] Campaign {campaign_id} paused mid-batch")
                        break

                recipient = campaign_recipient.recipient

                if not recipient:
                    logger.warning(f"⚠️  [CampaignService] No recipient for campaign_recipient {campaign_recipient.id}")
                    await self._mark_recipient_failed(
                        campaign_recipient.id,
                        "Recipient not found"
                    )
                    failed_count += 1
                    continue

                # Send email
                success, error_msg, email_log_id = await self._send_single_email(
                    campaign_recipient=campaign_recipient,
                    recipient=recipient,
                    candidate=candidate
                )

                if success:
                    # Update recipient status to SENT
                    await self.campaign_repo.update_recipient_status(
                        campaign_recipient.id,
                        RecipientStatusEnum.SENT,
                        sent_at=datetime.now(timezone.utc),
                        email_log_id=email_log_id
                    )

                    # Update recipient engagement stats
                    await self.recipient_repo.bulk_update_engagement(
                        recipient.id,
                        sent=True
                    )

                    sent_count += 1
                    logger.info(
                        f"✅ [CampaignService] Sent to {recipient.email} "
                        f"({sent_count}/{len(pending_recipients)})"
                    )
                else:
                    # Update recipient status to FAILED
                    await self._mark_recipient_failed(
                        campaign_recipient.id,
                        error_msg
                    )
                    failed_count += 1
                    logger.error(
                        f"❌ [CampaignService] Failed to send to {recipient.email}: {error_msg}"
                    )

                # Rate limiting: delay before next email
                if idx < len(pending_recipients) - 1:
                    await asyncio.sleep(campaign.send_delay_seconds)

            except Exception as e:
                logger.error(
                    f"❌ [CampaignService] Unexpected error processing recipient {campaign_recipient.id}: {e}"
                )
                await self._mark_recipient_failed(
                    campaign_recipient.id,
                    f"Unexpected error: {str(e)}"
                )
                failed_count += 1

        # Update campaign stats
        await self.campaign_repo._update_campaign_stats(campaign_id)

        # Check if there are more recipients
        remaining_count = await self._count_pending_recipients(campaign_id)

        result = {
            "campaign_id": campaign_id,
            "status": "sending",
            "sent": sent_count,
            "failed": failed_count,
            "remaining": remaining_count
        }

        logger.info(
            f"📊 [CampaignService] Batch complete: {sent_count} sent, "
            f"{failed_count} failed, {remaining_count} remaining"
        )

        return result

    async def _send_single_email(
        self,
        campaign_recipient: GroupCampaignRecipient,
        recipient: Recipient,
        candidate: Candidate
    ) -> tuple[bool, Optional[str], Optional[int]]:
        """
        Send a single email to a recipient.

        Args:
            campaign_recipient: GroupCampaignRecipient record
            recipient: Recipient object
            candidate: Candidate (sender) object

        Returns:
            Tuple of (success: bool, error_message: str, email_log_id: int)
        """
        try:
            # Use sync database session for EmailService
            # This is a workaround since EmailService is sync
            with next(get_db()) as sync_db:
                email_service = EmailService(sync_db)

                # Prepare email
                from email.mime.multipart import MIMEMultipart
                from email.mime.text import MIMEText

                msg = MIMEMultipart("alternative")
                msg["Subject"] = campaign_recipient.rendered_subject
                msg["From"] = candidate.email_account
                msg["To"] = recipient.email

                # Add HTML body
                html_part = MIMEText(campaign_recipient.rendered_body_html, "html")
                msg.attach(html_part)

                # Send via SMTP
                # Decrypt the email password (stored encrypted in database)
                decrypted_password = decrypt_value(candidate.email_password) if candidate.email_password else ""
                try:
                    email_service._send_via_smtp(
                        smtp_host=candidate.smtp_host,
                        smtp_port=candidate.smtp_port,
                        email_account=candidate.email_account,
                        email_password=decrypted_password,
                        msg=msg
                    )

                    # Create email log
                    email_log = EmailLog(
                        candidate_id=candidate.id,
                        from_email=candidate.email_account,
                        to_email=recipient.email,
                        subject=campaign_recipient.rendered_subject,
                        body_html=campaign_recipient.rendered_body_html,
                        status=EmailStatusEnum.SENT,
                        sent_at=datetime.now(timezone.utc),
                        tracking_id=campaign_recipient.tracking_id,
                        campaign_id=campaign_recipient.campaign_id
                    )
                    sync_db.add(email_log)
                    sync_db.commit()
                    sync_db.refresh(email_log)

                    return True, None, email_log.id

                except Exception as e:
                    logger.error(f"❌ [CampaignService] SMTP error: {e}")

                    # Create failed email log
                    email_log = EmailLog(
                        candidate_id=candidate.id,
                        from_email=candidate.email_account,
                        to_email=recipient.email,
                        subject=campaign_recipient.rendered_subject,
                        body_html=campaign_recipient.rendered_body_html,
                        status=EmailStatusEnum.FAILED,
                        error_message=str(e),
                        tracking_id=campaign_recipient.tracking_id,
                        campaign_id=campaign_recipient.campaign_id
                    )
                    sync_db.add(email_log)
                    sync_db.commit()
                    sync_db.refresh(email_log)

                    return False, str(e), email_log.id

        except Exception as e:
            logger.error(f"❌ [CampaignService] Error creating email: {e}")
            return False, str(e), None

    async def _mark_recipient_failed(
        self,
        campaign_recipient_id: int,
        error_message: str
    ):
        """Mark a campaign recipient as failed."""
        await self.campaign_repo.update_recipient_status(
            campaign_recipient_id,
            RecipientStatusEnum.FAILED,
            error_message=error_message
        )

    async def _count_pending_recipients(self, campaign_id: int) -> int:
        """Count remaining pending recipients for a campaign."""
        from sqlalchemy import select, func
        stmt = (
            select(func.count())
            .select_from(GroupCampaignRecipient)
            .where(
                GroupCampaignRecipient.campaign_id == campaign_id,
                GroupCampaignRecipient.status == RecipientStatusEnum.PENDING
            )
        )
        result = await self.db.execute(stmt)
        return result.scalar() or 0

    def _generate_tracking_id(self, campaign_id: int, recipient_id: int) -> str:
        """Generate a unique tracking ID for email tracking."""
        return f"CAM-{campaign_id}-REC-{recipient_id}-{uuid.uuid4().hex[:8]}"

    async def process_campaign_fully(
        self,
        campaign_id: int,
        candidate: Candidate,
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Process an entire campaign from start to finish.

        This is the main entry point for campaign processing. It:
        1. Prepares the campaign (renders templates)
        2. Sends emails in batches with rate limiting
        3. Tracks progress
        4. Handles pause/resume
        5. Marks campaign as complete

        Args:
            campaign_id: Campaign ID to process
            candidate: Candidate (sender) object
            batch_size: Number of emails per batch

        Returns:
            Dict with final campaign statistics
        """
        logger.info(f"🚀 [CampaignService] Starting full campaign processing for {campaign_id}")

        try:
            # Step 1: Prepare campaign (render templates)
            prep_result = await self.prepare_campaign(campaign_id, candidate)

            if prep_result["ready_to_send"] == 0:
                logger.warning(f"⚠️  [CampaignService] No recipients ready to send for campaign {campaign_id}")
                await self.campaign_repo.complete_campaign(campaign_id)
                return {
                    "campaign_id": campaign_id,
                    "status": "completed",
                    "total_sent": 0,
                    "total_failed": prep_result["errors"] + prep_result["skipped"],
                    "message": "No recipients ready to send"
                }

            # Step 2: Update campaign status to SENDING
            await self.campaign_repo.update_campaign_status(
                campaign_id,
                CampaignStatusEnum.SENDING
            )

            # Step 3: Send emails in batches
            total_sent = 0
            total_failed = 0

            while True:
                # Send one batch
                batch_result = await self.send_campaign_batch(
                    campaign_id,
                    candidate,
                    batch_size=batch_size,
                    check_pause=True
                )

                total_sent += batch_result["sent"]
                total_failed += batch_result["failed"]

                # Check if paused
                if batch_result["status"] == "paused":
                    logger.info(f"⏸️  [CampaignService] Campaign {campaign_id} paused")
                    return {
                        "campaign_id": campaign_id,
                        "status": "paused",
                        "total_sent": total_sent,
                        "total_failed": total_failed,
                        "message": "Campaign paused by user"
                    }

                # Check if completed
                if batch_result["status"] == "completed" or batch_result["remaining"] == 0:
                    logger.info(f"✅ [CampaignService] Campaign {campaign_id} completed")
                    await self.campaign_repo.complete_campaign(campaign_id)
                    return {
                        "campaign_id": campaign_id,
                        "status": "completed",
                        "total_sent": total_sent,
                        "total_failed": total_failed,
                        "message": f"Campaign completed: {total_sent} sent, {total_failed} failed"
                    }

        except Exception as e:
            logger.error(f"❌ [CampaignService] Campaign {campaign_id} failed: {e}")
            await self.campaign_repo.fail_campaign(campaign_id, str(e))
            raise
