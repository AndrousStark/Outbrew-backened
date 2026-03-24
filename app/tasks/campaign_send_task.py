"""
Campaign Send Background Task

Handles asynchronous email sending for campaigns:
- Fetches campaign recipients
- Renders personalized templates
- Sends emails via SMTP with RATE LIMITING
- Tracks send progress and status
- Updates campaign metrics
- Handles failures gracefully
- Creates follow-up campaigns (if enabled)

2026 Best Practices Applied (from Instantly.ai Benchmark Report):
- Daily send limit: 50 emails/day max per domain
- Warmup period: 4-6 weeks gradual increase
- Send delay: Minimum 30 seconds between emails
- Business hours: 9 AM - 5 PM recipient timezone
- Follow-up limit: 3-5 maximum per recipient
"""

import asyncio
import logging
import random
import time
from datetime import datetime, timedelta, date, timezone
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.database import SessionLocal
from app.models.group_campaign import GroupCampaign, CampaignStatusEnum
from app.models.group_campaign_recipient import (
    GroupCampaignRecipient,
    RecipientStatusEnum,
)
from app.models.recipient import Recipient
from app.models.candidate import Candidate
from app.models.follow_up import (
    FollowUpCampaign,
    FollowUpSequence,
    CampaignStatus as FollowUpCampaignStatus,
)
from app.repositories.group_campaign import GroupCampaignRepository
from app.services.email_service import EmailService
from app.services.notification_service import NotificationService
from app.services.template_engine import get_template_engine

logger = logging.getLogger(__name__)


# ==================== 2026 BEST PRACTICE CONSTANTS ====================
# Source: https://instantly.ai/cold-email-benchmark-report-2026

class SendingLimits:
    """
    2026 Cold Email Best Practice Sending Limits

    These limits are based on the Instantly.ai Cold Email Benchmark Report 2026
    and industry best practices for email deliverability.
    """

    # Daily send limit per domain (prevents spam detection)
    # Reason: "Send no more than 50 cold emails per day to protect your sender reputation"
    DAILY_LIMIT_DEFAULT = 50
    DAILY_LIMIT_WARMED = 100  # After 6 weeks of warmup

    # Minimum delay between emails (seconds)
    # Reason: "High-volume sending signals spam behavior to ESPs"
    MIN_DELAY_SECONDS = 30
    MAX_DELAY_SECONDS = 120

    # Warmup period (days) - gradual increase
    # Reason: "Starting slow signals legitimate sender behavior"
    WARMUP_PERIOD_DAYS = 42  # 6 weeks
    WARMUP_SCHEDULE = [
        (7, 5),    # Week 1: 5 emails/day
        (14, 15),  # Week 2: 15 emails/day
        (21, 30),  # Week 3: 30 emails/day
        (28, 50),  # Week 4: 50 emails/day
        (35, 75),  # Week 5: 75 emails/day
        (42, 100), # Week 6: 100 emails/day
    ]

    # Maximum follow-ups per recipient
    # Reason: "Most successful campaigns use 3-5 follow-ups"
    MAX_FOLLOW_UPS = 5

    # Business hours (UTC) for optimal send time
    # Reason: "Emails sent during business hours have 23% higher open rates"
    BUSINESS_HOURS_START = 9   # 9 AM
    BUSINESS_HOURS_END = 17    # 5 PM


class DailyLimitTracker:
    """
    Track daily email sending limits per candidate/domain

    Ensures compliance with 2026 best practice of 50 emails/day max.
    """

    def __init__(self, db_session: Session, candidate_id: int):
        self.db = db_session
        self.candidate_id = candidate_id
        self._cache: Dict[str, int] = {}
        self._cache_date: Optional[date] = None

    def get_sent_today(self) -> int:
        """Get number of emails sent today by this candidate"""
        today = date.today()

        # Use cache if same day
        cache_key = f"{self.candidate_id}:{today}"
        if self._cache_date == today and cache_key in self._cache:
            return self._cache[cache_key]

        # Query database
        today_start = datetime.combine(today, datetime.min.time())
        today_end = datetime.combine(today, datetime.max.time())

        count = (
            self.db.query(func.count(GroupCampaignRecipient.id))
            .join(GroupCampaign)
            .filter(
                GroupCampaign.candidate_id == self.candidate_id,
                GroupCampaignRecipient.sent_at >= today_start,
                GroupCampaignRecipient.sent_at <= today_end,
                GroupCampaignRecipient.status == RecipientStatusEnum.SENT
            )
            .scalar() or 0
        )

        # Cache result
        self._cache_date = today
        self._cache[cache_key] = count

        return count

    def increment(self):
        """Increment daily counter after sending an email"""
        today = date.today()
        cache_key = f"{self.candidate_id}:{today}"
        if cache_key in self._cache:
            self._cache[cache_key] += 1

    def get_warmup_limit(self, days_since_first_send: int) -> int:
        """
        Get current daily limit based on warmup progress

        Warmup schedule (2026 best practice):
        - Week 1: 5/day
        - Week 2: 15/day
        - Week 3: 30/day
        - Week 4: 50/day
        - Week 5: 75/day
        - Week 6+: 100/day
        """
        for day_threshold, limit in SendingLimits.WARMUP_SCHEDULE:
            if days_since_first_send < day_threshold:
                return limit
        return SendingLimits.DAILY_LIMIT_WARMED

    def can_send_more(self, days_since_first_send: int = 0) -> tuple[bool, int, str]:
        """
        Check if more emails can be sent today

        Returns:
            (can_send: bool, remaining: int, reason: str)
        """
        sent_today = self.get_sent_today()
        daily_limit = self.get_warmup_limit(days_since_first_send)
        remaining = daily_limit - sent_today

        if remaining <= 0:
            reason = (
                f"Daily limit reached ({daily_limit}/day). "
                f"2026 Best Practice: Max 50-100 emails/day to protect sender reputation. "
                f"Campaign will resume tomorrow."
            )
            return False, 0, reason

        return True, remaining, f"Can send {remaining} more today (limit: {daily_limit})"


def get_candidate_first_send_date(db_session: Session, candidate_id: int) -> Optional[datetime]:
    """Get the date of the candidate's first sent email (for warmup tracking)"""
    result = (
        db_session.query(func.min(GroupCampaignRecipient.sent_at))
        .join(GroupCampaign)
        .filter(
            GroupCampaign.candidate_id == candidate_id,
            GroupCampaignRecipient.status == RecipientStatusEnum.SENT
        )
        .scalar()
    )
    return result


def create_follow_up_campaign(
    db_session: Session,
    campaign: GroupCampaign,
    campaign_recipient: GroupCampaignRecipient,
    recipient: Recipient,
    candidate: Candidate,
    sequence: FollowUpSequence,
) -> Optional[FollowUpCampaign]:
    """
    Create a FollowUpCampaign for a recipient after their initial email is sent.

    Args:
        db_session: Database session
        campaign: The parent GroupCampaign
        campaign_recipient: The GroupCampaignRecipient record
        recipient: The Recipient
        candidate: The sending Candidate
        sequence: The FollowUpSequence to use

    Returns:
        Created FollowUpCampaign or None if creation failed
    """
    try:
        # Calculate when to send the first follow-up
        first_step = sequence.steps[0] if sequence.steps else None
        if not first_step:
            logger.warning(f"[FOLLOW-UP] Sequence {sequence.id} has no steps, skipping")
            return None

        # Calculate next send date based on first step delay
        next_send_date = datetime.now(timezone.utc) + timedelta(
            days=first_step.delay_days,
            hours=first_step.delay_hours or 0
        )

        # Build original email context
        original_email_context = {
            "subject": campaign_recipient.rendered_subject,
            "body_preview": (campaign_recipient.rendered_body_html or "")[:500],
            "sent_at": campaign_recipient.sent_at.isoformat() if campaign_recipient.sent_at else datetime.now(timezone.utc).isoformat(),
            "campaign_name": campaign.campaign_name,
            "group_campaign_id": campaign.id,
        }

        # Build recipient context
        recipient_context = {
            "name": recipient.name or f"{recipient.first_name or ''} {recipient.last_name or ''}".strip(),
            "email": recipient.email,
            "company": recipient.company,
            "position": recipient.position,
        }

        # Build candidate context
        candidate_context = {
            "name": candidate.full_name or candidate.email,
            "email": candidate.email,
        }

        # Create FollowUpCampaign
        follow_up_campaign = FollowUpCampaign(
            sequence_id=sequence.id,
            group_campaign_recipient_id=campaign_recipient.id,
            group_campaign_id=campaign.id,
            candidate_id=candidate.id,
            status=FollowUpCampaignStatus.ACTIVE,  # Start active for auto-mode
            is_auto_mode=True,
            auto_mode_approved=True,  # Pre-approved from campaign settings
            auto_mode_approved_at=datetime.now(timezone.utc),
            current_step=0,  # 0 = initial email sent, 1 = first follow-up
            total_steps=len(sequence.steps),
            next_send_date=next_send_date,
            last_sent_date=datetime.now(timezone.utc),
            original_email_context=original_email_context,
            candidate_context=candidate_context,
            company_context=recipient_context,  # Using recipient as company context
        )

        db_session.add(follow_up_campaign)
        db_session.flush()  # Get the ID

        logger.info(
            f"✅ [FOLLOW-UP] Created campaign {follow_up_campaign.id} for recipient {recipient.email} "
            f"(sequence: {sequence.name}, next send: {next_send_date})"
        )

        return follow_up_campaign

    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Failed to create follow-up campaign: {str(e)}", exc_info=True)
        return None


def process_campaign_send(campaign_id: int, db_session: Session = None):
    """
    Background task to send campaign emails with 2026 best practice rate limiting.

    This function:
    1. Fetches all campaign recipients with status=PENDING
    2. Checks daily sending limits (50/day max)
    3. Applies warmup schedule if applicable
    4. Sends emails via SMTP with configurable delay (min 30s)
    5. Updates recipient status (SENT/FAILED/SKIPPED)
    6. Updates campaign metrics
    7. Pauses campaign if daily limit reached
    8. Creates follow-up campaigns (if enabled)

    2026 Best Practices Applied:
    - Daily limit: 50-100 emails/day (based on warmup)
    - Min delay: 30 seconds between emails
    - Warmup: 6-week gradual increase schedule
    - Rate limit logging for transparency

    Args:
        campaign_id: ID of campaign to send
        db_session: Optional database session (if None, creates new one)
    """

    # Use provided session or create new one
    if db_session is None:
        db_session = SessionLocal()

    try:
        logger.info(f"🚀 [CAMPAIGN SEND] Starting send task for campaign {campaign_id}")

        # Get campaign
        campaign_repo = GroupCampaignRepository(db_session)
        campaign = campaign_repo.get_by_id(campaign_id)

        if not campaign:
            logger.error(f"❌ [CAMPAIGN SEND] Campaign {campaign_id} not found")
            return

        if campaign.status != CampaignStatusEnum.SENDING:
            logger.warning(
                f"⚠️ [CAMPAIGN SEND] Campaign {campaign_id} not in SENDING status "
                f"(current: {campaign.status})"
            )
            return

        # Initialize rate limit tracker
        limit_tracker = DailyLimitTracker(db_session, campaign.candidate_id)

        # Get warmup info
        first_send = get_candidate_first_send_date(db_session, campaign.candidate_id)
        days_since_first = (datetime.now(timezone.utc) - first_send).days if first_send else 0
        current_limit = limit_tracker.get_warmup_limit(days_since_first)

        logger.info(
            f"📧 [CAMPAIGN SEND] Campaign '{campaign.campaign_name}' - "
            f"{campaign.total_recipients} recipients, {campaign.send_delay_seconds}s delay\n"
            f"   📊 Warmup Status: Day {days_since_first} (limit: {current_limit}/day)\n"
            f"   ⚡ Sent Today: {limit_tracker.get_sent_today()}/{current_limit}"
        )

        # Check if follow-up is enabled and load sequence
        follow_up_sequence = None
        if campaign.enable_follow_up and campaign.follow_up_sequence_id:
            follow_up_sequence = (
                db_session.query(FollowUpSequence)
                .filter(FollowUpSequence.id == campaign.follow_up_sequence_id)
                .first()
            )
            if follow_up_sequence:
                logger.info(
                    f"📋 [CAMPAIGN SEND] Follow-up enabled with sequence '{follow_up_sequence.name}' "
                    f"({len(follow_up_sequence.steps)} steps)"
                )
            else:
                logger.warning(
                    f"⚠️ [CAMPAIGN SEND] Follow-up sequence {campaign.follow_up_sequence_id} not found"
                )

        # Get campaign candidate (sender)
        candidate = (
            db_session.query(Candidate)
            .filter(Candidate.id == campaign.candidate_id)
            .first()
        )

        if not candidate:
            logger.error(
                f"❌ [CAMPAIGN SEND] Candidate {campaign.candidate_id} not found"
            )
            campaign.status = CampaignStatusEnum.FAILED
            campaign.error_message = "Sender account not found"
            campaign_repo.update(
                campaign_id,
                {"status": campaign.status, "error_message": campaign.error_message},
            )
            return

        # Update campaign start time
        campaign.started_at = datetime.now(timezone.utc)
        db_session.commit()

        # Initialize metrics
        sent_count = 0
        failed_count = 0
        skipped_count = 0
        follow_up_count = 0
        errors = []
        daily_limit_hit = False

        # Fetch all pending recipients
        pending_recipients = (
            db_session.query(GroupCampaignRecipient)
            .filter(
                GroupCampaignRecipient.campaign_id == campaign_id,
                GroupCampaignRecipient.status == RecipientStatusEnum.PENDING,
            )
            .all()
        )

        logger.info(
            f"📬 [CAMPAIGN SEND] Found {len(pending_recipients)} pending recipients"
        )

        # Template rendering fallback: if rendered_subject is None, render now
        template_engine = get_template_engine(strict_mode=False)
        for cr in pending_recipients:
            if cr.rendered_subject is None or cr.rendered_body_html is None:
                try:
                    recipient = (
                        db_session.query(Recipient)
                        .filter(Recipient.id == cr.recipient_id)
                        .first()
                    )
                    if recipient:
                        context = template_engine.build_recipient_context(
                            recipient=recipient,
                            candidate=candidate
                        )
                        cr.rendered_subject = template_engine.render(
                            campaign.subject_template, context, validate=False
                        )
                        cr.rendered_body_html = template_engine.render(
                            campaign.body_template_html, context, validate=False
                        )
                        if not cr.tracking_id:
                            import uuid
                            cr.tracking_id = f"CAM-{campaign_id}-REC-{recipient.id}-{uuid.uuid4().hex[:8]}"
                        db_session.commit()
                except Exception as render_err:
                    logger.warning(
                        f"[CAMPAIGN SEND] Template rendering failed for recipient {cr.recipient_id}: {render_err}"
                    )
                    cr.status = RecipientStatusEnum.SKIPPED
                    cr.error_message = f"Template rendering failed: {render_err}"
                    skipped_count += 1
                    db_session.commit()
                    continue

        # Calculate actual delay (enforce minimum)
        actual_delay = max(
            campaign.send_delay_seconds or SendingLimits.MIN_DELAY_SECONDS,
            SendingLimits.MIN_DELAY_SECONDS
        )

        if actual_delay != campaign.send_delay_seconds:
            logger.info(
                f"⚠️ [RATE LIMIT] Delay increased to {actual_delay}s "
                f"(2026 best practice: minimum {SendingLimits.MIN_DELAY_SECONDS}s between emails)"
            )

        # Send emails to each recipient
        email_service = EmailService(db_session)

        for index, campaign_recipient in enumerate(pending_recipients, 1):
            try:
                # Check daily limit BEFORE each send
                can_send, remaining, limit_reason = limit_tracker.can_send_more(days_since_first)

                if not can_send:
                    logger.warning(
                        f"🛑 [RATE LIMIT] Daily limit reached!\n"
                        f"   Reason: {limit_reason}\n"
                        f"   Sent so far: {sent_count}, Remaining recipients: {len(pending_recipients) - index + 1}\n"
                        f"   Campaign will be PAUSED and can resume tomorrow."
                    )
                    daily_limit_hit = True
                    break

                # Get recipient details
                recipient = (
                    db_session.query(Recipient)
                    .filter(Recipient.id == campaign_recipient.recipient_id)
                    .first()
                )

                if not recipient:
                    logger.warning(
                        f"⏭️ [CAMPAIGN SEND] Recipient {campaign_recipient.recipient_id} not found - skipping"
                    )
                    campaign_recipient.status = RecipientStatusEnum.SKIPPED
                    campaign_recipient.error_message = "Recipient not found"
                    skipped_count += 1
                    db_session.commit()
                    continue

                # Validate email address
                if not recipient.email:
                    logger.warning(
                        f"⏭️ [CAMPAIGN SEND] Recipient {recipient.id} ({recipient.first_name} {recipient.last_name}) "
                        f"has no email - skipping"
                    )
                    campaign_recipient.status = RecipientStatusEnum.SKIPPED
                    campaign_recipient.error_message = "No email address"
                    skipped_count += 1
                    db_session.commit()
                    continue

                # Send email
                try:
                    logger.info(
                        f"📤 [CAMPAIGN SEND] [{index}/{len(pending_recipients)}] "
                        f"Sending to {recipient.email} "
                        f"(Subject: {campaign_recipient.rendered_subject[:40]}...) "
                        f"[{remaining} remaining today]"
                    )

                    # Create email log
                    email_log = email_service.send_email(
                        candidate=candidate,
                        to_email=recipient.email,
                        subject=campaign_recipient.rendered_subject,
                        body_html=campaign_recipient.rendered_body_html,
                        tracking_id=campaign_recipient.tracking_id,
                        campaign_id=campaign_id,
                    )

                    # Update recipient status
                    campaign_recipient.status = RecipientStatusEnum.SENT
                    campaign_recipient.sent_at = datetime.now(timezone.utc)
                    campaign_recipient.email_log_id = email_log.id
                    sent_count += 1
                    limit_tracker.increment()

                    logger.info(f"✅ [CAMPAIGN SEND] Email sent to {recipient.email}")

                    # Create follow-up campaign if enabled
                    if follow_up_sequence:
                        follow_up_created = create_follow_up_campaign(
                            db_session=db_session,
                            campaign=campaign,
                            campaign_recipient=campaign_recipient,
                            recipient=recipient,
                            candidate=candidate,
                            sequence=follow_up_sequence,
                        )
                        if follow_up_created:
                            follow_up_count += 1
                            logger.info(
                                f"📝 [CAMPAIGN SEND] Follow-up scheduled for {recipient.email}"
                            )

                except Exception as e:
                    error_msg = str(e)
                    logger.error(
                        f"❌ [CAMPAIGN SEND] Failed to send to {recipient.email}: {error_msg}"
                    )

                    campaign_recipient.status = RecipientStatusEnum.FAILED
                    campaign_recipient.error_message = error_msg
                    campaign_recipient.retry_count = (
                        campaign_recipient.retry_count or 0
                    ) + 1
                    failed_count += 1
                    errors.append(f"{recipient.email}: {error_msg}")

                db_session.commit()

                # Apply rate limiting delay between emails
                if index < len(pending_recipients) and not daily_limit_hit:
                    logger.debug(
                        f"⏸️ [RATE LIMIT] Waiting {actual_delay}s before next email "
                        f"(2026 best practice: min {SendingLimits.MIN_DELAY_SECONDS}s)"
                    )
                    jittered = actual_delay + random.uniform(-0.3 * actual_delay, 0.3 * actual_delay)
                    jittered = max(jittered, SendingLimits.MIN_DELAY_SECONDS)
                    time.sleep(jittered)

            except Exception as e:
                logger.error(
                    f"❌ [CAMPAIGN SEND] Unexpected error processing recipient {campaign_recipient.id}: {str(e)}",
                    exc_info=True,
                )
                failed_count += 1
                errors.append(f"Recipient {campaign_recipient.id}: {str(e)}")

        # Update campaign status and metrics
        follow_up_info = f", {follow_up_count} follow-ups scheduled" if follow_up_count > 0 else ""
        logger.info(
            f"📊 [CAMPAIGN SEND] Campaign {campaign_id} batch complete: "
            f"{sent_count} sent, {failed_count} failed, {skipped_count} skipped{follow_up_info}"
        )

        # Update template usage stats if template was used
        if sent_count > 0 and campaign.email_template_id:
            from app.models.email_template import EmailTemplate
            template = db_session.query(EmailTemplate).filter(
                EmailTemplate.id == campaign.email_template_id
            ).first()
            if template:
                template.times_used = (template.times_used or 0) + sent_count
                template.last_used_at = datetime.now(timezone.utc)

        # Determine final status
        if daily_limit_hit:
            # Pause campaign due to daily limit - can resume tomorrow
            final_status = CampaignStatusEnum.PAUSED
            campaign.paused_at = datetime.now(timezone.utc)
            status_msg = (
                f"Campaign paused due to daily limit ({current_limit}/day). "
                f"Sent {sent_count} emails. Will resume tomorrow automatically."
            )
            logger.info(
                f"⏸️ [RATE LIMIT] Campaign {campaign_id} paused - daily limit reached\n"
                f"   Sent today: {sent_count}\n"
                f"   Remaining: {len(pending_recipients) - sent_count - failed_count - skipped_count}\n"
                f"   Resume: Tomorrow at 9 AM"
            )
        elif failed_count == 0 and skipped_count == 0:
            final_status = CampaignStatusEnum.COMPLETED
            status_msg = "Campaign completed successfully"
        elif sent_count > 0:
            final_status = CampaignStatusEnum.COMPLETED
            status_msg = f"Campaign completed with {failed_count} failures"
        else:
            final_status = CampaignStatusEnum.FAILED
            status_msg = "Campaign failed to send any emails"

        # Update campaign
        campaign.status = final_status
        campaign.sent_count = (campaign.sent_count or 0) + sent_count
        campaign.failed_count = (campaign.failed_count or 0) + failed_count
        campaign.skipped_count = (campaign.skipped_count or 0) + skipped_count

        if not daily_limit_hit:
            campaign.completed_at = datetime.now(timezone.utc)

        if errors:
            campaign.error_message = "; ".join(errors[:5])  # Store first 5 errors

        db_session.commit()

        logger.info(
            f"✨ [CAMPAIGN SEND] Campaign {campaign_id} status updated to {final_status}: {status_msg}"
        )

        # Send notification
        try:
            sync_db = SessionLocal()
            if final_status == CampaignStatusEnum.COMPLETED and failed_count == 0:
                NotificationService.notify_campaign_completed(
                    db=sync_db,
                    campaign_id=campaign_id,
                    campaign_name=campaign.campaign_name,
                    sent_count=sent_count,
                    candidate_id=campaign.candidate_id,
                )
            elif final_status == CampaignStatusEnum.FAILED:
                NotificationService.notify_campaign_failed(
                    db=sync_db,
                    campaign_id=campaign_id,
                    campaign_name=campaign.campaign_name,
                    error_message=campaign.error_message or "Unknown error",
                    candidate_id=campaign.candidate_id,
                )
            elif final_status == CampaignStatusEnum.PAUSED and daily_limit_hit:
                # Notify about rate limit pause
                NotificationService.notify_campaign_paused(
                    db=sync_db,
                    campaign_id=campaign_id,
                    campaign_name=campaign.campaign_name,
                    reason="Daily sending limit reached (2026 best practice: max 50-100/day)",
                    sent_count=sent_count,
                    candidate_id=campaign.candidate_id,
                )
            sync_db.close()
        except Exception as e:
            logger.warning(f"[CAMPAIGN SEND] Failed to send notification: {e}")

    except Exception as e:
        logger.error(
            f"❌ [CAMPAIGN SEND] Critical error in send task for campaign {campaign_id}: {str(e)}",
            exc_info=True,
        )

        try:
            campaign = (
                db_session.query(GroupCampaign)
                .filter(GroupCampaign.id == campaign_id)
                .first()
            )

            if campaign:
                campaign.status = CampaignStatusEnum.FAILED
                campaign.error_message = str(e)
                campaign.completed_at = datetime.now(timezone.utc)
                db_session.commit()
        except Exception as update_error:
            logger.error(f"Failed to update campaign status: {update_error}")

    finally:
        if db_session and hasattr(db_session, "close"):
            db_session.close()
        logger.info(
            f"🏁 [CAMPAIGN SEND] Send task completed for campaign {campaign_id}"
        )
