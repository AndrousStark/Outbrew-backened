"""
Reply Detection Service

Automatically detects replies to follow-up emails and marks campaigns as replied.
Integrates EmailInbox with FollowUpCampaigns.
"""
import logging
import re
from typing import List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from app.models.email_inbox import EmailMessage
from app.models.follow_up import FollowUpCampaign, FollowUpEmail
from app.services.follow_up_service import FollowUpService

logger = logging.getLogger(__name__)


class ReplyDetectionService:
    """Service for detecting replies and updating follow-up campaigns"""

    def __init__(self, db: Session):
        self.db = db
        self.follow_up_service = FollowUpService(db)

    def _extract_email_from_string(self, email_str: str) -> str:
        """Extract email address from a string like 'Name <email@example.com>'"""
        email_pattern = r'<([^>]+)>|([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        match = re.search(email_pattern, email_str)
        if match:
            return match.group(1) or match.group(2)
        return email_str.strip()

    def _is_likely_reply(self, message: EmailMessage, original_subject: str) -> bool:
        """
        Determine if email message is likely a reply based on subject and headers

        Common reply indicators:
        - Subject starts with "Re:", "RE:", "Fwd:", "FW:"
        - Subject contains original subject text
        - Has in_reply_to header
        """
        if not message.subject:
            return False

        subject_lower = message.subject.lower().strip()

        # Check for reply prefixes
        reply_prefixes = ["re:", "reply:", "re :", "response:"]
        if any(subject_lower.startswith(prefix) for prefix in reply_prefixes):
            return True

        # Check if original subject is contained in this subject
        if original_subject:
            original_lower = original_subject.lower().strip()
            # Remove "Re:" and similar from comparison
            cleaned_subject = re.sub(r'^(re|fwd?|reply):\s*', '', subject_lower)
            cleaned_original = re.sub(r'^(re|fwd?|reply):\s*', '', original_lower)

            if cleaned_original in cleaned_subject:
                return True

        # Check for in_reply_to header (strong indicator)
        if message.in_reply_to:
            return True

        return False

    def _match_reply_to_campaign(
        self, message: EmailMessage
    ) -> Optional[FollowUpCampaign]:
        """
        Try to match a received email to a follow-up campaign

        Matching strategies:
        1. Check if from_email matches any campaign's target_email
        2. Check if subject matches any sent follow-up email
        3. Check thread_id or in_reply_to headers
        4. Check recent campaigns (within last 30 days)
        """
        # Extract clean email address
        from_email = self._extract_email_from_string(message.from_email).lower()

        # Strategy 1: Match by email address and recent activity
        # Find campaigns where target email matches and campaign is active
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)

        campaigns = (
            self.db.query(FollowUpCampaign)
            .join(FollowUpEmail)
            .filter(
                and_(
                    or_(
                        FollowUpCampaign.status == "ACTIVE",
                        FollowUpCampaign.status == "PAUSED"
                    ),
                    FollowUpCampaign.reply_detected == False,
                    FollowUpCampaign.created_at >= cutoff_date,
                )
            )
            .all()
        )

        for campaign in campaigns:
            # Get the application to check target email
            if campaign.application:
                target_email = self._extract_email_from_string(
                    campaign.application.recruiter_email or ""
                ).lower()

                if target_email == from_email:
                    # Check if subject matches
                    if self._is_likely_reply(
                        message,
                        campaign.application.email_subject or ""
                    ):
                        return campaign

            # Strategy 2: Check sent emails from this campaign
            sent_emails = (
                self.db.query(FollowUpEmail)
                .filter(
                    FollowUpEmail.campaign_id == campaign.id,
                    FollowUpEmail.status == "SENT",
                )
                .all()
            )

            for sent_email in sent_emails:
                if sent_email.subject and self._is_likely_reply(message, sent_email.subject):
                    return campaign

        return None

    def detect_reply_for_message(
        self, message: EmailMessage
    ) -> Tuple[bool, Optional[FollowUpCampaign]]:
        """
        Check if a single message is a reply to any follow-up campaign

        Returns:
            Tuple[bool, Optional[FollowUpCampaign]]: (is_reply, campaign)
        """
        # Only process RECEIVED messages
        if message.direction != "RECEIVED":
            return False, None

        # Try to match to a campaign
        campaign = self._match_reply_to_campaign(message)

        if campaign:
            logger.info(
                f"Detected reply to campaign {campaign.id} from {message.from_email}"
            )
            return True, campaign

        return False, None

    def mark_campaign_as_replied(
        self, campaign: FollowUpCampaign, reply_message: EmailMessage
    ) -> bool:
        """
        Mark a campaign as replied and stop pending emails

        Returns:
            bool: True if successfully marked
        """
        try:
            # Use the existing mark_reply_received method from FollowUpService
            self.follow_up_service.mark_reply_received(
                campaign_id=campaign.id,
                source="auto"
            )

            # Link the reply message to the application if not already linked
            if not reply_message.application_id and campaign.application_id:
                reply_message.application_id = campaign.application_id
                self.db.commit()

            logger.info(
                f"Campaign {campaign.id} marked as replied due to message {reply_message.id}"
            )
            return True

        except Exception as e:
            logger.error(f"Error marking campaign {campaign.id} as replied: {e}")
            return False

    def scan_inbox_for_replies(
        self, candidate_id: int, limit: int = 50
    ) -> dict:
        """
        Scan recent inbox messages for replies to active follow-up campaigns

        Args:
            candidate_id: Candidate to check
            limit: Number of recent messages to check

        Returns:
            dict: Statistics about detected replies
        """
        # Get recent RECEIVED messages that haven't been processed
        recent_messages = (
            self.db.query(EmailMessage)
            .filter(
                EmailMessage.candidate_id == candidate_id,
                EmailMessage.direction == "RECEIVED",
            )
            .order_by(EmailMessage.received_at.desc())
            .limit(limit)
            .all()
        )

        detected_count = 0
        campaigns_updated = []

        for message in recent_messages:
            is_reply, campaign = self.detect_reply_for_message(message)

            if is_reply and campaign:
                # Mark campaign as replied
                success = self.mark_campaign_as_replied(campaign, message)
                if success:
                    detected_count += 1
                    campaigns_updated.append(campaign.id)

        return {
            "messages_scanned": len(recent_messages),
            "replies_detected": detected_count,
            "campaigns_updated": campaigns_updated,
        }

    def auto_detect_on_sync(
        self, candidate_id: int, new_message_ids: List[int]
    ) -> dict:
        """
        Called after email sync to check new messages for replies

        Args:
            candidate_id: Candidate whose inbox was synced
            new_message_ids: IDs of newly synced messages

        Returns:
            dict: Detection statistics
        """
        if not new_message_ids:
            return {
                "messages_checked": 0,
                "replies_detected": 0,
                "campaigns_updated": [],
            }

        # Get the new messages
        new_messages = (
            self.db.query(EmailMessage)
            .filter(
                EmailMessage.id.in_(new_message_ids),
                EmailMessage.direction == "RECEIVED",
            )
            .all()
        )

        detected_count = 0
        campaigns_updated = []

        for message in new_messages:
            is_reply, campaign = self.detect_reply_for_message(message)

            if is_reply and campaign:
                success = self.mark_campaign_as_replied(campaign, message)
                if success:
                    detected_count += 1
                    campaigns_updated.append(campaign.id)

        return {
            "messages_checked": len(new_messages),
            "replies_detected": detected_count,
            "campaigns_updated": campaigns_updated,
        }
