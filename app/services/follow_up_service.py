"""
Follow-Up Campaign Service

Manages the complete lifecycle of follow-up campaigns:
- Creating and managing sequence templates
- Starting campaigns with auto-mode approval
- Generating and scheduling emails
- Processing due emails
- Handling replies and completion
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, select
import logging
import pytz

# Setup logger for debugging
logger = logging.getLogger(__name__)

from app.models.follow_up import (
    FollowUpSequence, FollowUpStep, FollowUpCampaign, FollowUpEmail,
    FollowUpLog, CandidateProfile, SequenceStatus, CampaignStatus,
    FollowUpEmailStatus, FollowUpTone, FollowUpStrategy,
    DEFAULT_SEQUENCE_PRESETS
)
from app.models.application import Application
from app.models.company import Company
from app.models.candidate import Candidate
from app.models.email_log import EmailLog, EmailStatusEnum
from app.models.company_intelligence import CompanyResearchCache
from app.services.follow_up_email_generator import FollowUpEmailGenerator
from app.services.email_service import EmailService
from app.services.send_time_optimizer import SendTimeOptimizer
from app.services.ml.send_time_ml import SendTimeMLOptimizer
from app.services.ml.reply_predictor import ReplyPredictor
from app.models.follow_up_ml import PredictionConfidence


class FollowUpService:
    """
    Complete follow-up campaign management service.

    Features:
    - Sequence template management
    - Campaign lifecycle management
    - Auto-mode with approval workflow
    - Email generation and scheduling
    - Reply detection handling
    - Analytics and tracking
    """

    def __init__(self, db: Session):
        self.db = db
        self.email_generator = FollowUpEmailGenerator(db)
        self.send_time_optimizer = SendTimeOptimizer()
        # ML Services (Sprint 2)
        self.send_time_ml = SendTimeMLOptimizer(db)
        self.reply_predictor = ReplyPredictor(db)

    # ============= TIMEZONE HELPERS =============

    def _get_timezone(self, timezone_str: str) -> pytz.timezone:
        """
        Get timezone object from string, with fallback to UTC.

        Args:
            timezone_str: Timezone string (e.g., "America/New_York")

        Returns:
            pytz.timezone object
        """
        if not timezone_str:
            return pytz.UTC

        try:
            return pytz.timezone(timezone_str)
        except Exception as e:
            logger.warning(f"Invalid timezone '{timezone_str}': {e}. Using UTC.")
            return pytz.UTC

    def _convert_to_timezone(
        self,
        dt: datetime,
        target_timezone: str
    ) -> datetime:
        """
        Convert a datetime to a target timezone.

        Args:
            dt: Datetime object (UTC naive or aware)
            target_timezone: Target timezone string

        Returns:
            Timezone-aware datetime in target timezone
        """
        # Ensure dt is timezone-aware (assume UTC if naive)
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)

        # Convert to target timezone
        target_tz = self._get_timezone(target_timezone)
        return dt.astimezone(target_tz)

    def _to_utc(self, dt: datetime) -> datetime:
        """
        Convert timezone-aware datetime to UTC naive datetime for storage.

        Args:
            dt: Timezone-aware datetime

        Returns:
            UTC naive datetime
        """
        if dt.tzinfo is None:
            # Already naive, assume it's UTC
            return dt

        # Convert to UTC and remove timezone info
        return dt.astimezone(pytz.UTC).replace(tzinfo=None)

    # ============= SEQUENCE TEMPLATE MANAGEMENT =============

    def create_sequence(
        self,
        candidate_id: int,
        name: str,
        steps: List[Dict],
        description: str = None,
        stop_on_reply: bool = True,
        stop_on_bounce: bool = True,
        use_threading: bool = True,
        include_candidate_links: bool = True,
        preferred_send_hour: int = 10
    ) -> FollowUpSequence:
        """
        Create a new follow-up sequence template.

        Args:
            candidate_id: Owner of this sequence
            name: Sequence name
            steps: List of step dicts with:
                - delay_days: int
                - strategy: str (soft_bump, add_value, etc.)
                - tone: str (professional, friendly, etc.)
                - body_template: str (optional)
                - subject_template: str (optional)
            description: Optional description
            stop_on_reply: Stop sequence if recipient replies
            stop_on_bounce: Stop if email bounces
            use_threading: Send as reply thread
            include_candidate_links: Add social/portfolio links
            preferred_send_hour: Hour of day to send (0-23)
        """
        sequence = FollowUpSequence(
            candidate_id=candidate_id,
            name=name,
            description=description,
            stop_on_reply=stop_on_reply,
            stop_on_bounce=stop_on_bounce,
            use_threading=use_threading,
            include_candidate_links=include_candidate_links,
            preferred_send_hour=preferred_send_hour
        )
        self.db.add(sequence)
        self.db.flush()

        # Add steps
        for i, step_data in enumerate(steps, 1):
            step = FollowUpStep(
                sequence_id=sequence.id,
                step_number=i,
                delay_days=step_data.get("delay_days", 2),
                delay_hours=step_data.get("delay_hours", 0),
                strategy=FollowUpStrategy(step_data.get("strategy", "soft_bump")),
                tone=FollowUpTone(step_data.get("tone", "professional")),
                subject_template=step_data.get("subject_template"),
                body_template=step_data.get("body_template", ""),
                generation_hints=step_data.get("generation_hints", {}),
                include_original_context=step_data.get("include_original_context", True),
                include_value_proposition=step_data.get("include_value_proposition", False),
                include_portfolio_link=step_data.get("include_portfolio_link", False),
                include_call_to_action=step_data.get("include_call_to_action", True)
            )
            self.db.add(step)

        self.db.commit()
        return sequence

    def create_preset_sequences(self, candidate_id: int) -> List[FollowUpSequence]:
        """Create all default preset sequences for a candidate."""
        created = []

        for preset in DEFAULT_SEQUENCE_PRESETS:
            sequence = self.create_sequence(
                candidate_id=candidate_id,
                name=preset["name"],
                description=preset["description"],
                steps=preset["steps"]
            )
            sequence.is_system_preset = True
            created.append(sequence)

        self.db.commit()
        return created

    def get_sequences(
        self,
        candidate_id: int,
        include_presets: bool = True
    ) -> List[FollowUpSequence]:
        """Get all sequences for a candidate."""
        query = self.db.query(FollowUpSequence).filter(
            FollowUpSequence.candidate_id == candidate_id,
            FollowUpSequence.status != SequenceStatus.ARCHIVED
        )

        if not include_presets:
            query = query.filter(FollowUpSequence.is_system_preset == False)

        return query.order_by(FollowUpSequence.created_at.desc()).all()

    def update_sequence(
        self,
        sequence_id: int,
        updates: Dict[str, Any]
    ) -> FollowUpSequence:
        """Update a sequence and its steps."""
        sequence = self.db.query(FollowUpSequence).filter(
            FollowUpSequence.id == sequence_id
        ).first()

        if not sequence:
            raise ValueError("Sequence not found")

        # Update sequence fields
        for key in ["name", "description", "stop_on_reply", "stop_on_bounce",
                    "use_threading", "include_candidate_links", "preferred_send_hour"]:
            if key in updates:
                setattr(sequence, key, updates[key])

        # Update steps if provided
        if "steps" in updates:
            # Delete existing steps
            self.db.query(FollowUpStep).filter(
                FollowUpStep.sequence_id == sequence_id
            ).delete()

            # Add new steps
            for i, step_data in enumerate(updates["steps"], 1):
                step = FollowUpStep(
                    sequence_id=sequence_id,
                    step_number=i,
                    delay_days=step_data.get("delay_days", 2),
                    strategy=FollowUpStrategy(step_data.get("strategy", "soft_bump")),
                    tone=FollowUpTone(step_data.get("tone", "professional")),
                    body_template=step_data.get("body_template", ""),
                    subject_template=step_data.get("subject_template"),
                    include_call_to_action=step_data.get("include_call_to_action", True),
                    include_portfolio_link=step_data.get("include_portfolio_link", False)
                )
                self.db.add(step)

        sequence.updated_at = datetime.now(timezone.utc)
        self.db.commit()
        return sequence

    def delete_sequence(self, sequence_id: int) -> bool:
        """Soft delete a sequence (archive it)."""
        sequence = self.db.query(FollowUpSequence).filter(
            FollowUpSequence.id == sequence_id
        ).first()

        if sequence:
            sequence.status = SequenceStatus.ARCHIVED
            self.db.commit()
            return True
        return False

    # ============= CAMPAIGN MANAGEMENT =============

    async def start_campaign(
        self,
        application_id: int,
        sequence_id: int,
        candidate_id: int,
        auto_mode: bool = False,
        original_email_context: Dict = None
    ) -> Tuple[FollowUpCampaign, List[Dict]]:
        """
        Start a new follow-up campaign.

        Returns:
            Tuple of (campaign, preview_emails) for auto-mode approval
        """
        # Get sequence
        sequence = self.db.query(FollowUpSequence).filter(
            FollowUpSequence.id == sequence_id
        ).first()

        if not sequence:
            raise ValueError("Sequence not found")

        # Get application and company context
        application = self.db.query(Application).filter(
            Application.id == application_id
        ).first()

        if not application:
            raise ValueError("Application not found")

        # Build company context
        company_context = {}
        if application.company:
            company = application.company
            company_context = {
                "name": company.name,
                "industry": company.industry,
                "position": application.position_title,
                "tech_stack": company.tech_stack or [],
                "description": company.description,
                "website": company.website_url
            }

            # Add research data if available
            research = self.db.query(CompanyResearchCache).filter(
                CompanyResearchCache.company_id == company.id
            ).first()
            if research:
                company_context["research_highlights"] = {
                    "about": research.about_summary,
                    "culture": research.company_culture,
                    "tech": research.tech_stack_detailed
                }

        # Build candidate context with null-safe access
        candidate = self.db.query(Candidate).filter(
            Candidate.id == candidate_id
        ).first()

        if not candidate:
            logger.warning(f"[FollowUp] Candidate {candidate_id} not found for campaign")

        candidate_context = {
            "name": candidate.full_name if candidate else "",
            "email": candidate.email if candidate else "",
            "skills": getattr(candidate, 'skills', []) if candidate else []
        }

        # Get extended profile
        profile = self.db.query(CandidateProfile).filter(
            CandidateProfile.candidate_id == candidate_id
        ).first()

        if profile:
            candidate_context.update({
                "phone": profile.phone_number,
                "linkedin": profile.linkedin_url,
                "github": profile.github_url,
                "website": profile.website_url,
                "portfolio": profile.portfolio_url,
                "headline": profile.headline,
                "portfolio_projects": profile.portfolio_projects,
                "achievements": profile.achievements
            })

        # Create campaign
        campaign = FollowUpCampaign(
            sequence_id=sequence_id,
            application_id=application_id,
            candidate_id=candidate_id,
            status=CampaignStatus.PENDING_APPROVAL if auto_mode else CampaignStatus.MANUAL,
            is_auto_mode=auto_mode,
            current_step=0,
            total_steps=len(sequence.steps),
            original_email_context=original_email_context or {},
            company_context=company_context,
            candidate_context=candidate_context
        )

        self.db.add(campaign)
        self.db.flush()

        # Log creation
        self._log_action(campaign.id, "created", {
            "sequence_name": sequence.name,
            "auto_mode": auto_mode,
            "total_steps": len(sequence.steps)
        })

        # Generate preview emails for all steps
        previews = await self.email_generator.generate_all_previews(campaign)

        # Create draft emails for each step
        for preview in previews:
            step = self.db.query(FollowUpStep).filter(
                FollowUpStep.sequence_id == sequence_id,
                FollowUpStep.step_number == preview["step_number"]
            ).first()

            email = FollowUpEmail(
                campaign_id=campaign.id,
                step_id=step.id if step else None,
                step_number=preview["step_number"],
                status=FollowUpEmailStatus.DRAFT,
                subject=preview["subject"],
                body_text=preview["body_text"],
                body_html=preview["body_html"],
                original_subject=preview["subject"],
                original_body=preview["body_text"],
                strategy_used=FollowUpStrategy(preview["strategy"]),
                tone_used=FollowUpTone(preview["tone"]),
                personalization_data=preview["personalization"],
                is_auto_generated=True
            )
            self.db.add(email)

        # Update sequence stats
        sequence.times_used += 1
        sequence.total_campaigns += 1

        self.db.commit()

        return campaign, previews

    async def approve_auto_mode(
        self,
        campaign_id: int,
        approved: bool = True,
        edited_emails: List[Dict] = None
    ) -> FollowUpCampaign:
        """
        Approve or reject auto-mode for a campaign.

        Args:
            campaign_id: Campaign to approve
            approved: Whether to approve
            edited_emails: Optional list of edited emails [{step_number, subject, body}]
        """
        campaign = self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id
        ).first()

        if not campaign:
            raise ValueError("Campaign not found")

        if not approved:
            campaign.status = CampaignStatus.MANUAL
            campaign.is_auto_mode = False
            self._log_action(campaign_id, "auto_mode_rejected")
            self.db.commit()
            return campaign

        # Apply any edits
        if edited_emails:
            for edit in edited_emails:
                email = self.db.query(FollowUpEmail).filter(
                    FollowUpEmail.campaign_id == campaign_id,
                    FollowUpEmail.step_number == edit["step_number"]
                ).first()

                if email:
                    if edit.get("subject"):
                        email.subject = edit["subject"]
                    if edit.get("body"):
                        email.body_text = edit["body"]
                        email.body_html = self.email_generator._convert_to_html(
                            edit["body"], {}
                        )
                    email.is_user_edited = True
                    email.edit_count += 1
                    email.last_edited_at = datetime.now(timezone.utc)

        # Approve and activate
        campaign.status = CampaignStatus.ACTIVE
        campaign.auto_mode_approved = True
        campaign.auto_mode_approved_at = datetime.now(timezone.utc)

        # Schedule first email with timezone awareness
        first_email = self.db.query(FollowUpEmail).filter(
            FollowUpEmail.campaign_id == campaign_id,
            FollowUpEmail.step_number == 1
        ).first()

        if first_email:
            # Safely get first step - check for sequence and steps list
            first_step = None
            if campaign.sequence and campaign.sequence.steps and len(campaign.sequence.steps) > 0:
                first_step = campaign.sequence.steps[0]
            delay_days = first_step.delay_days if first_step else 2
            delay_hours = first_step.delay_hours if first_step else 0
            logger.debug(f"[FollowUp] Scheduling first email with delay_days={delay_days}, delay_hours={delay_hours}")

            # Get timezone from sequence
            recipient_timezone = "UTC"
            if campaign.sequence and campaign.sequence.preferred_timezone:
                recipient_timezone = campaign.sequence.preferred_timezone

            # Calculate send time in recipient timezone
            base_date = datetime.now(timezone.utc)
            base_date_tz = self._convert_to_timezone(base_date, recipient_timezone)
            send_date_tz = base_date_tz + timedelta(days=delay_days, hours=delay_hours)

            # Apply business hours if configured
            if campaign.sequence and campaign.sequence.respect_business_hours:
                send_date_tz = self._adjust_to_business_hours(
                    send_date_tz,
                    campaign.sequence.preferred_send_hour,
                    campaign.sequence.business_hours_start or 9,
                    campaign.sequence.business_hours_end or 18,
                    recipient_timezone
                )

            # Convert back to UTC for storage
            send_date_utc = self._to_utc(send_date_tz)

            first_email.status = FollowUpEmailStatus.SCHEDULED
            first_email.scheduled_for = send_date_utc
            first_email.timezone = recipient_timezone
            campaign.next_send_date = send_date_utc
            logger.info(
                f"[FollowUp] First email scheduled for {send_date_utc} UTC "
                f"({send_date_tz.strftime('%Y-%m-%d %H:%M %Z')} in recipient timezone)"
            )

        self._log_action(campaign_id, "auto_mode_approved", {
            "edits_applied": len(edited_emails) if edited_emails else 0
        })

        self.db.commit()
        return campaign

    def update_email(
        self,
        email_id: int,
        subject: str = None,
        body: str = None,
        is_custom: bool = False
    ) -> FollowUpEmail:
        """
        Update a follow-up email (user editing).

        Args:
            email_id: Email to update
            subject: New subject (optional)
            body: New body text (optional)
            is_custom: Whether this is completely custom written
        """
        email = self.db.query(FollowUpEmail).filter(
            FollowUpEmail.id == email_id
        ).first()

        if not email:
            raise ValueError("Email not found")

        if subject:
            email.subject = subject
        if body:
            email.body_text = body
            email.body_html = self.email_generator._convert_to_html(body, {})

        email.is_user_edited = True
        email.is_custom_written = is_custom
        email.edit_count += 1
        email.last_edited_at = datetime.now(timezone.utc)
        email.status = FollowUpEmailStatus.EDITED

        self._log_action(email.campaign_id, "step_edited", {
            "step_number": email.step_number,
            "is_custom": is_custom
        })

        self.db.commit()
        return email

    def approve_email(self, email_id: int) -> FollowUpEmail:
        """Approve a draft email for sending."""
        email = self.db.query(FollowUpEmail).filter(
            FollowUpEmail.id == email_id
        ).first()

        if not email:
            raise ValueError("Email not found")

        email.status = FollowUpEmailStatus.APPROVED
        email.approved_at = datetime.now(timezone.utc)

        self._log_action(email.campaign_id, "step_approved", {
            "step_number": email.step_number
        })

        self.db.commit()
        return email

    def skip_email(self, email_id: int) -> FollowUpEmail:
        """Skip a follow-up step."""
        email = self.db.query(FollowUpEmail).filter(
            FollowUpEmail.id == email_id
        ).first()

        if not email:
            raise ValueError("Email not found")

        email.status = FollowUpEmailStatus.SKIPPED

        # Update campaign progress
        campaign = email.campaign
        campaign.current_step += 1

        # Schedule next email if any
        self._schedule_next_email(campaign)

        self._log_action(email.campaign_id, "step_skipped", {
            "step_number": email.step_number
        })

        self.db.commit()
        return email

    # ============= SENDING =============

    async def send_email_now(
        self,
        email_id: int,
        email_service: EmailService = None
    ) -> FollowUpEmail:
        """
        Send a follow-up email immediately.

        For manual mode or when user triggers immediate send.
        """
        email = self.db.query(FollowUpEmail).filter(
            FollowUpEmail.id == email_id
        ).first()

        if not email:
            raise ValueError("Email not found")

        # Check if email was already sent to prevent duplicate sends
        if email.status == FollowUpEmailStatus.SENT:
            logger.warning(f"[FollowUp] Email {email_id} has already been sent, skipping")
            return email

        if email.sent_at is not None:
            logger.warning(f"[FollowUp] Email {email_id} has sent_at timestamp, already sent")
            return email

        campaign = email.campaign
        application = campaign.application

        # Mark as sending
        email.status = FollowUpEmailStatus.SENDING
        logger.info(f"[FollowUp] Starting to send email {email_id} for campaign {campaign.id}")

        try:
            # Send via email service
            if email_service:
                # Fetch candidate object (EmailService needs full object, not ID)
                candidate_obj = self.db.query(Candidate).filter(
                    Candidate.id == campaign.candidate_id
                ).first()
                if not candidate_obj:
                    raise Exception(f"Candidate {campaign.candidate_id} not found")

                # EmailService.send_email() is sync, not async
                email_log = email_service.send_email(
                    candidate=candidate_obj,
                    to_email=application.recruiter_email,
                    subject=email.subject,
                    body_html=email.body_html or email.body_text,
                )

                if email_log.status == EmailStatusEnum.SENT:
                    email.status = FollowUpEmailStatus.SENT
                    email.sent_at = datetime.now(timezone.utc)
                    email.email_log_id = email_log.id
                else:
                    raise Exception(email_log.error_message or "Send failed")
            else:
                # Simulate send for testing
                email.status = FollowUpEmailStatus.SENT
                email.sent_at = datetime.now(timezone.utc)

            # Update campaign
            campaign.emails_sent += 1
            campaign.current_step = email.step_number
            campaign.last_sent_date = datetime.now(timezone.utc)

            # Update ML analytics (Sprint 2)
            try:
                self.send_time_ml.update_analytics(campaign.candidate_id, email)
                logger.debug(f"[FollowUp] Updated ML analytics for email {email.id}")
            except Exception as ml_error:
                logger.warning(f"[FollowUp] Failed to update ML analytics: {ml_error}")

            # Schedule next or complete
            if campaign.current_step >= campaign.total_steps:
                campaign.status = CampaignStatus.COMPLETED
                campaign.completed_at = datetime.now(timezone.utc)
                self._log_action(campaign.id, "completed")
            else:
                self._schedule_next_email(campaign)

            self._log_action(campaign.id, "step_sent", {
                "step_number": email.step_number
            })

        except Exception as e:
            error_msg = str(e)
            logger.error(f"[FollowUp] Failed to send email {email_id}: {error_msg}", exc_info=True)
            email.status = FollowUpEmailStatus.FAILED
            email.error_message = error_msg
            email.retry_count += 1

            self._log_action(campaign.id, "step_failed", {
                "step_number": email.step_number,
                "error": error_msg
            })

        # Commit changes with error handling
        try:
            self.db.commit()
            logger.debug(f"[FollowUp] Email send operation completed for email {email_id}, status: {email.status}")
        except Exception as commit_error:
            self.db.rollback()
            logger.error(f"[FollowUp] Failed to commit send operation for email {email_id}: {commit_error}")
            raise ValueError(f"Failed to save email send status: {commit_error}")

        return email

    async def process_due_campaigns(self) -> Dict[str, Any]:
        """
        Process all campaigns with due emails.

        Called by scheduler every few minutes.
        Uses row-level locking to prevent race conditions.
        """
        now = datetime.now(timezone.utc)
        logger.info(f"[FollowUp] Processing due campaigns at {now}")

        # Find campaigns with emails due - use FOR UPDATE to prevent race conditions
        # This locks the rows to prevent multiple workers from processing same campaign
        stmt = select(FollowUpCampaign).filter(
            FollowUpCampaign.status == CampaignStatus.ACTIVE,
            FollowUpCampaign.is_auto_mode == True,
            FollowUpCampaign.next_send_date <= now
        ).with_for_update(skip_locked=True)  # Skip locked rows to allow other workers

        due_campaigns = self.db.execute(stmt).scalars().all()
        logger.info(f"[FollowUp] Found {len(due_campaigns)} due campaigns to process")

        results = {
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "errors": []
        }

        for campaign in due_campaigns:
            results["processed"] += 1
            logger.info(f"[FollowUp] Processing campaign {campaign.id} for application {campaign.application_id}")

            # Find the next email to send
            next_email = self.db.query(FollowUpEmail).filter(
                FollowUpEmail.campaign_id == campaign.id,
                FollowUpEmail.status.in_([
                    FollowUpEmailStatus.SCHEDULED,
                    FollowUpEmailStatus.APPROVED
                ]),
                FollowUpEmail.step_number == campaign.current_step + 1
            ).first()

            if next_email:
                logger.info(f"[FollowUp] Sending email {next_email.id} (step {next_email.step_number}) for campaign {campaign.id}")
                try:
                    await self.send_email_now(next_email.id)
                    results["sent"] += 1
                    logger.info(f"[FollowUp] Successfully sent email {next_email.id}")
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append({
                        "campaign_id": campaign.id,
                        "error": str(e)
                    })
                    logger.error(f"[FollowUp] Failed to send email {next_email.id}: {str(e)}")
            else:
                logger.warning(f"[FollowUp] No scheduled email found for campaign {campaign.id} at step {campaign.current_step + 1}")

        logger.info(f"[FollowUp] Processing complete: {results}")
        return results

    def _schedule_next_email(self, campaign: FollowUpCampaign):
        """
        Schedule the next email in the campaign with timezone-aware scheduling.

        Uses recipient's timezone and optionally integrates with SendTimeOptimizer
        for truly optimal send times.
        """
        next_step_num = campaign.current_step + 1

        if next_step_num > campaign.total_steps:
            campaign.next_send_date = None
            return

        # Get next email
        next_email = self.db.query(FollowUpEmail).filter(
            FollowUpEmail.campaign_id == campaign.id,
            FollowUpEmail.step_number == next_step_num
        ).first()

        if not next_email:
            return

        # Get step for delay info
        step = next_email.step

        if step:
            delay_days = step.delay_days
            delay_hours = step.delay_hours or 0
        else:
            delay_days = 2
            delay_hours = 0

        # Get timezone from sequence (default to UTC)
        recipient_timezone = "UTC"
        if campaign.sequence and campaign.sequence.preferred_timezone:
            recipient_timezone = campaign.sequence.preferred_timezone

        # Calculate base date in UTC
        base_date = campaign.last_sent_date or datetime.now(timezone.utc)

        # Convert to recipient timezone for scheduling
        base_date_tz = self._convert_to_timezone(base_date, recipient_timezone)

        # Add delay
        send_date_tz = base_date_tz + timedelta(days=delay_days, hours=delay_hours)

        # Respect business hours and optimize send time if configured
        if campaign.sequence and campaign.sequence.respect_business_hours:
            send_date_tz = self._adjust_to_business_hours(
                send_date_tz,
                campaign.sequence.preferred_send_hour,
                campaign.sequence.business_hours_start or 9,
                campaign.sequence.business_hours_end or 18,
                recipient_timezone
            )

            # Use ML-enhanced send time optimization (Sprint 2)
            # This learns from actual engagement data and auto-applies when confidence is high
            try:
                # Get recipient domain for domain-specific optimization
                recipient_domain = None
                if campaign.application and campaign.application.contact_email:
                    email_parts = campaign.application.contact_email.split("@")
                    if len(email_parts) == 2:
                        recipient_domain = email_parts[1]

                ml_result = self.send_time_ml.get_optimal_send_time(
                    candidate_id=campaign.candidate_id,
                    recipient_domain=recipient_domain,
                    recipient_industry=campaign.company_context.get("industry") if campaign.company_context else None,
                    recipient_timezone=recipient_timezone
                )

                # Auto-apply when HIGH confidence, suggest for MEDIUM
                if ml_result.confidence == PredictionConfidence.HIGH:
                    # Find the next occurrence of the recommended day/hour
                    target_day = ml_result.recommended_day  # 0=Monday
                    target_hour = ml_result.recommended_hour

                    # Adjust send_date_tz to match ML recommendation
                    current_day = send_date_tz.weekday()
                    days_until_target = (target_day - current_day) % 7

                    # If today is the target day but hour has passed, go to next week
                    if days_until_target == 0 and send_date_tz.hour > target_hour:
                        days_until_target = 7

                    optimal_date_tz = send_date_tz + timedelta(days=days_until_target)
                    optimal_date_tz = optimal_date_tz.replace(
                        hour=target_hour,
                        minute=0,
                        second=0,
                        microsecond=0
                    )

                    logger.info(
                        f"[FollowUp] ML auto-applied send time: {optimal_date_tz.strftime('%A %H:%M')} "
                        f"(confidence: {ml_result.confidence.value}, source: {ml_result.data_source}, "
                        f"samples: {ml_result.sample_size}, expected boost: +{ml_result.expected_open_rate_boost:.1f}%)"
                    )
                    send_date_tz = optimal_date_tz

                elif ml_result.confidence == PredictionConfidence.MEDIUM:
                    # Log suggestion but don't auto-apply
                    logger.info(
                        f"[FollowUp] ML suggests {ml_result.recommended_day}/{ml_result.recommended_hour}:00 "
                        f"(medium confidence, not auto-applied)"
                    )

                # Update campaign with ML prediction
                campaign.reply_probability = None  # Will be set by reply predictor if called
                campaign.priority_score = 50  # Default, will be updated by reply predictor

            except Exception as e:
                logger.warning(f"[FollowUp] ML send time optimization failed, using fallback: {e}")
                # Fall back to industry defaults
                if campaign.company_context and campaign.company_context.get("industry"):
                    try:
                        optimal_result = self.send_time_optimizer.get_optimal_send_time(
                            industry=campaign.company_context.get("industry", "default"),
                            recipient_timezone=recipient_timezone
                        )

                        optimal_time_tz = optimal_result["send_at"].astimezone(
                            self._get_timezone(recipient_timezone)
                        )

                        time_diff = abs((optimal_time_tz - send_date_tz).total_seconds() / 3600)

                        if time_diff <= 24:
                            logger.info(
                                f"[FollowUp] Using fallback optimized send time: {optimal_time_tz}"
                            )
                            send_date_tz = optimal_time_tz
                    except Exception as fallback_e:
                        logger.warning(f"[FollowUp] Fallback optimization also failed: {fallback_e}")

        # Convert back to UTC for storage
        send_date_utc = self._to_utc(send_date_tz)

        # Store timezone info in email record
        next_email.scheduled_for = send_date_utc
        next_email.timezone = recipient_timezone
        next_email.status = FollowUpEmailStatus.SCHEDULED
        campaign.next_send_date = send_date_utc

        logger.info(
            f"[FollowUp] Scheduled email {next_email.id} for {send_date_utc} UTC "
            f"({send_date_tz.strftime('%Y-%m-%d %H:%M %Z')} in recipient timezone)"
        )

    def _adjust_to_business_hours(
        self,
        dt: datetime,
        preferred_hour: int = 10,
        business_hours_start: int = 9,
        business_hours_end: int = 18,
        timezone_str: str = "UTC"
    ) -> datetime:
        """
        Adjust datetime to business hours in the specified timezone.

        Args:
            dt: Datetime to adjust (should be timezone-aware)
            preferred_hour: Preferred hour to send (0-23)
            business_hours_start: Start of business hours (default 9am)
            business_hours_end: End of business hours (default 6pm)
            timezone_str: Timezone for business hours

        Returns:
            Timezone-aware datetime adjusted to business hours
        """
        # Ensure dt is timezone-aware
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)

        # Convert to target timezone
        tz = self._get_timezone(timezone_str)
        dt_local = dt.astimezone(tz)

        # If weekend, move to Monday
        while dt_local.weekday() >= 5:  # 5=Saturday, 6=Sunday
            dt_local += timedelta(days=1)

        # Check if hour is within business hours
        if dt_local.hour < business_hours_start:
            # Too early, move to start of business day
            dt_local = dt_local.replace(
                hour=max(preferred_hour, business_hours_start),
                minute=0,
                second=0,
                microsecond=0
            )
        elif dt_local.hour >= business_hours_end:
            # Too late, move to next business day
            dt_local += timedelta(days=1)
            # Check if next day is weekend
            while dt_local.weekday() >= 5:
                dt_local += timedelta(days=1)
            dt_local = dt_local.replace(
                hour=max(preferred_hour, business_hours_start),
                minute=0,
                second=0,
                microsecond=0
            )
        else:
            # Within business hours, use preferred hour if reasonable
            target_hour = preferred_hour
            if target_hour < business_hours_start:
                target_hour = business_hours_start
            elif target_hour >= business_hours_end:
                target_hour = business_hours_start

            dt_local = dt_local.replace(
                hour=target_hour,
                minute=0,
                second=0,
                microsecond=0
            )

        logger.debug(
            f"[FollowUp] Adjusted to business hours: {dt_local.strftime('%Y-%m-%d %H:%M %Z')} "
            f"(business hours: {business_hours_start}-{business_hours_end} {timezone_str})"
        )

        return dt_local

    # ============= REPLY HANDLING =============

    def mark_reply_received(
        self,
        campaign_id: int,
        source: str = "manual"
    ) -> FollowUpCampaign:
        """
        Mark that a reply was received, stopping the campaign.

        Args:
            campaign_id: Campaign that received reply
            source: How reply was detected (manual, auto, webhook)
        """
        campaign = self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id
        ).first()

        if not campaign:
            raise ValueError("Campaign not found")

        campaign.reply_detected = True
        campaign.reply_detected_at = datetime.now(timezone.utc)
        campaign.reply_source = source

        if campaign.sequence and campaign.sequence.stop_on_reply:
            campaign.status = CampaignStatus.REPLIED

            # Cancel any pending emails
            self.db.query(FollowUpEmail).filter(
                FollowUpEmail.campaign_id == campaign_id,
                FollowUpEmail.status.in_([
                    FollowUpEmailStatus.DRAFT,
                    FollowUpEmailStatus.SCHEDULED,
                    FollowUpEmailStatus.APPROVED
                ])
            ).update({FollowUpEmail.status: FollowUpEmailStatus.SKIPPED})

        # Update sequence stats
        if campaign.sequence:
            campaign.sequence.successful_replies += 1
            total = campaign.sequence.total_campaigns or 1
            campaign.sequence.reply_rate = campaign.sequence.successful_replies / total

        self._log_action(campaign_id, "reply_detected", {"source": source})

        self.db.commit()
        return campaign

    # ============= CAMPAIGN CONTROL =============

    def pause_campaign(self, campaign_id: int) -> Optional[FollowUpCampaign]:
        """Pause an active campaign."""
        logger.info(f"[FollowUp] Pausing campaign {campaign_id}")

        campaign = self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id
        ).first()

        if not campaign:
            logger.warning(f"[FollowUp] Campaign {campaign_id} not found for pausing")
            return None

        campaign.status = CampaignStatus.PAUSED
        self._log_action(campaign_id, "paused")
        self.db.commit()

        logger.info(f"[FollowUp] Campaign {campaign_id} paused successfully")
        return campaign

    def resume_campaign(self, campaign_id: int) -> Optional[FollowUpCampaign]:
        """Resume a paused campaign."""
        logger.info(f"[FollowUp] Resuming campaign {campaign_id}")

        campaign = self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id
        ).first()

        if not campaign:
            logger.warning(f"[FollowUp] Campaign {campaign_id} not found for resuming")
            return None

        if campaign.status != CampaignStatus.PAUSED:
            logger.warning(f"[FollowUp] Campaign {campaign_id} is not paused (status: {campaign.status})")
            return campaign

        campaign.status = CampaignStatus.ACTIVE if campaign.is_auto_mode else CampaignStatus.MANUAL
        self._schedule_next_email(campaign)
        self._log_action(campaign_id, "resumed")
        self.db.commit()

        logger.info(f"[FollowUp] Campaign {campaign_id} resumed successfully, next send: {campaign.next_send_date}")
        return campaign

    def cancel_campaign(
        self,
        campaign_id: int,
        reason: str = None
    ) -> Optional[FollowUpCampaign]:
        """Cancel a campaign."""
        logger.info(f"[FollowUp] Cancelling campaign {campaign_id}, reason: {reason}")

        campaign = self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id
        ).first()

        if not campaign:
            logger.warning(f"[FollowUp] Campaign {campaign_id} not found for cancellation")
            return None

        campaign.status = CampaignStatus.CANCELLED
        campaign.cancellation_reason = reason

        # Skip all pending emails
        skipped_count = self.db.query(FollowUpEmail).filter(
            FollowUpEmail.campaign_id == campaign_id,
            FollowUpEmail.status.in_([
                FollowUpEmailStatus.DRAFT,
                FollowUpEmailStatus.SCHEDULED,
                FollowUpEmailStatus.APPROVED
            ])
        ).update({FollowUpEmail.status: FollowUpEmailStatus.SKIPPED})

        logger.info(f"[FollowUp] Skipped {skipped_count} pending emails for campaign {campaign_id}")

        self._log_action(campaign_id, "cancelled", {"reason": reason, "emails_skipped": skipped_count})
        self.db.commit()

        logger.info(f"[FollowUp] Campaign {campaign_id} cancelled successfully")
        return campaign

    # ============= QUERIES =============

    def get_campaign(self, campaign_id: int) -> Optional[FollowUpCampaign]:
        """Get a campaign by ID."""
        return self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.id == campaign_id
        ).first()

    def get_campaigns_for_application(
        self,
        application_id: int
    ) -> List[FollowUpCampaign]:
        """Get all campaigns for an application."""
        return self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.application_id == application_id
        ).order_by(FollowUpCampaign.created_at.desc()).all()

    def get_active_campaigns(
        self,
        candidate_id: int
    ) -> List[FollowUpCampaign]:
        """Get all active campaigns for a candidate."""
        return self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.candidate_id == candidate_id,
            FollowUpCampaign.status.in_([
                CampaignStatus.ACTIVE,
                CampaignStatus.PENDING_APPROVAL,
                CampaignStatus.MANUAL
            ])
        ).order_by(FollowUpCampaign.next_send_date).all()

    def get_campaign_emails(
        self,
        campaign_id: int
    ) -> List[FollowUpEmail]:
        """Get all emails for a campaign."""
        return self.db.query(FollowUpEmail).filter(
            FollowUpEmail.campaign_id == campaign_id
        ).order_by(FollowUpEmail.step_number).all()

    def get_campaign_stats(
        self,
        candidate_id: int
    ) -> Dict[str, Any]:
        """Get follow-up campaign statistics for a candidate."""
        campaigns = self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.candidate_id == candidate_id
        ).all()

        total = len(campaigns)
        active = sum(1 for c in campaigns if c.status in [CampaignStatus.ACTIVE, CampaignStatus.MANUAL])
        completed = sum(1 for c in campaigns if c.status == CampaignStatus.COMPLETED)
        replied = sum(1 for c in campaigns if c.status == CampaignStatus.REPLIED)

        total_emails_sent = sum(c.emails_sent for c in campaigns)

        return {
            "total_campaigns": total,
            "active_campaigns": active,
            "completed_campaigns": completed,
            "replied_campaigns": replied,
            "reply_rate": replied / total if total > 0 else 0,
            "total_emails_sent": total_emails_sent,
            "campaigns_by_status": {
                status.value: sum(1 for c in campaigns if c.status == status)
                for status in CampaignStatus
            }
        }

    # ============= LOGGING =============

    def _log_action(
        self,
        campaign_id: int,
        action: str,
        details: Dict = None,
        actor: str = "system"
    ):
        """Log an action on a campaign and flush to ensure it's persisted."""
        log = FollowUpLog(
            campaign_id=campaign_id,
            action=action,
            details=details or {},
            actor=actor
        )
        self.db.add(log)
        # Flush to ensure log is persisted even if subsequent operations fail
        self.db.flush()
        logger.debug(f"[FollowUp] Logged action '{action}' for campaign {campaign_id}")

    # ============= CANDIDATE PROFILE =============

    def get_or_create_profile(
        self,
        candidate_id: int
    ) -> CandidateProfile:
        """Get or create a candidate profile."""
        profile = self.db.query(CandidateProfile).filter(
            CandidateProfile.candidate_id == candidate_id
        ).first()

        if not profile:
            profile = CandidateProfile(candidate_id=candidate_id)
            self.db.add(profile)
            self.db.commit()

        return profile

    def update_profile(
        self,
        candidate_id: int,
        updates: Dict[str, Any]
    ) -> CandidateProfile:
        """Update a candidate's follow-up profile."""
        profile = self.get_or_create_profile(candidate_id)

        allowed_fields = [
            "phone_number", "personal_email", "linkedin_url", "github_url",
            "twitter_url", "website_url", "portfolio_url", "behance_url",
            "dribbble_url", "medium_url", "stackoverflow_url", "other_links",
            "headline", "bio", "years_experience", "current_company",
            "current_title", "portfolio_projects", "achievements",
            "value_propositions", "email_signature", "signature_html",
            "preferred_follow_up_tone", "default_sequence_id"
        ]

        for key, value in updates.items():
            if key in allowed_fields:
                if key == "preferred_follow_up_tone" and isinstance(value, str):
                    value = FollowUpTone(value)
                setattr(profile, key, value)

        profile.updated_at = datetime.now(timezone.utc)
        self.db.commit()

        return profile
