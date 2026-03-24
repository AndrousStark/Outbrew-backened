"""
Feature Extractor for Follow-Up ML

ULTRA Follow-Up System V2.0 - Sprint 2

Extracts 19 features from campaign, email, and recipient data
for ML predictions (reply probability, send time optimization).

Features are normalized to 0-1 range for gradient boosting.
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
import hashlib
import logging

from sqlalchemy.orm import Session
from sqlalchemy import func, case

logger = logging.getLogger(__name__)


@dataclass
class FollowUpFeatures:
    """
    Feature vector for ML predictions.

    19 features across 6 categories:
    - Recipient (3): domain_hash, domain_popularity, seniority_score
    - Timing (4): hour_of_day, day_of_week, is_business_hours, days_since_original
    - Content (5): subject_length, body_length, has_personalization, has_cta, step_number
    - Historical (3): domain_open_rate, domain_reply_rate, candidate_reply_rate
    - Engagement (2): previous_opens, previous_clicks
    - Company (2): industry_code, company_size_bucket
    """
    # Recipient features
    recipient_domain_hash: int = 0
    recipient_domain_popularity: float = 0.0
    recipient_seniority_score: float = 0.3

    # Timing features
    hour_of_day: int = 10
    day_of_week: int = 1  # 0=Monday
    is_business_hours: int = 1
    days_since_original_email: int = 0

    # Content features
    subject_length: int = 0
    body_length: int = 0
    has_personalization: int = 0
    has_call_to_action: int = 0
    email_step_number: int = 1

    # Historical features
    domain_historical_open_rate: float = 0.0
    domain_historical_reply_rate: float = 0.0
    candidate_overall_reply_rate: float = 0.0

    # Engagement features
    previous_opens_in_campaign: int = 0
    previous_clicks_in_campaign: int = 0

    # Company features
    industry_code: int = 0
    company_size_bucket: int = 2  # 0=tiny, 1=small, 2=medium, 3=large, 4=enterprise

    def to_vector(self) -> List[float]:
        """
        Convert to normalized feature vector (0-1 range).

        Returns:
            List of 19 floats, all normalized to 0-1
        """
        return [
            # Recipient (3)
            (self.recipient_domain_hash % 1000) / 1000,
            min(self.recipient_domain_popularity, 1.0),
            min(self.recipient_seniority_score, 1.0),

            # Timing (4)
            self.hour_of_day / 24,
            self.day_of_week / 7,
            float(self.is_business_hours),
            min(self.days_since_original_email / 30, 1.0),

            # Content (5)
            min(self.subject_length / 100, 1.0),
            min(self.body_length / 2000, 1.0),
            float(self.has_personalization),
            float(self.has_call_to_action),
            min(self.email_step_number / 10, 1.0),

            # Historical (3)
            min(self.domain_historical_open_rate, 1.0),
            min(self.domain_historical_reply_rate, 1.0),
            min(self.candidate_overall_reply_rate, 1.0),

            # Engagement (2)
            min(self.previous_opens_in_campaign / 5, 1.0),
            min(self.previous_clicks_in_campaign / 5, 1.0),

            # Company (2)
            self.industry_code / 20,
            self.company_size_bucket / 5,
        ]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for debugging/logging"""
        return {
            "recipient_domain_hash": self.recipient_domain_hash,
            "recipient_domain_popularity": round(self.recipient_domain_popularity, 3),
            "recipient_seniority_score": round(self.recipient_seniority_score, 3),
            "hour_of_day": self.hour_of_day,
            "day_of_week": self.day_of_week,
            "is_business_hours": self.is_business_hours,
            "days_since_original_email": self.days_since_original_email,
            "subject_length": self.subject_length,
            "body_length": self.body_length,
            "has_personalization": self.has_personalization,
            "has_call_to_action": self.has_call_to_action,
            "email_step_number": self.email_step_number,
            "domain_historical_open_rate": round(self.domain_historical_open_rate, 3),
            "domain_historical_reply_rate": round(self.domain_historical_reply_rate, 3),
            "candidate_overall_reply_rate": round(self.candidate_overall_reply_rate, 3),
            "previous_opens_in_campaign": self.previous_opens_in_campaign,
            "previous_clicks_in_campaign": self.previous_clicks_in_campaign,
            "industry_code": self.industry_code,
            "company_size_bucket": self.company_size_bucket,
        }


# Feature names for explainability
FEATURE_NAMES = [
    "domain_hash", "domain_popularity", "seniority_score",
    "hour_of_day", "day_of_week", "is_business_hours", "days_since_original",
    "subject_length", "body_length", "has_personalization", "has_cta", "step_number",
    "domain_open_rate", "domain_reply_rate", "candidate_reply_rate",
    "previous_opens", "previous_clicks",
    "industry", "company_size"
]


class FeatureExtractor:
    """
    Extracts ML features from campaign and email data.

    Features are extracted from:
    - FollowUpCampaign: company context, candidate context, timing
    - FollowUpEmail: content, engagement tracking
    - Historical data: domain statistics, candidate statistics
    """

    # Seniority detection keywords with scores (1.0 = highest)
    SENIORITY_KEYWORDS = {
        # C-Suite
        'ceo': 1.0, 'chief executive': 1.0, 'cto': 1.0, 'cfo': 1.0,
        'coo': 1.0, 'cmo': 1.0, 'cio': 1.0, 'founder': 1.0, 'co-founder': 1.0,

        # Executive
        'president': 0.95, 'executive': 0.9,
        'vp': 0.9, 'vice president': 0.9, 'svp': 0.92, 'evp': 0.92,

        # Director
        'director': 0.8, 'head of': 0.8, 'head': 0.75,

        # Manager
        'manager': 0.6, 'lead': 0.55, 'team lead': 0.55, 'principal': 0.65,

        # Senior
        'senior': 0.4, 'sr.': 0.4, 'sr ': 0.4, 'staff': 0.45,

        # Mid-level
        'engineer': 0.3, 'developer': 0.3, 'analyst': 0.25,
        'specialist': 0.25, 'coordinator': 0.2,

        # Entry
        'associate': 0.15, 'assistant': 0.15, 'junior': 0.1, 'intern': 0.05,
    }

    # Industry codes
    INDUSTRY_CODES = {
        'technology': 1, 'tech': 1, 'software': 1, 'it': 1,
        'finance': 2, 'financial': 2, 'banking': 2, 'fintech': 2,
        'healthcare': 3, 'health': 3, 'medical': 3, 'pharma': 3,
        'consulting': 4, 'professional services': 4,
        'retail': 5, 'ecommerce': 5, 'e-commerce': 5,
        'manufacturing': 6, 'industrial': 6,
        'education': 7, 'edtech': 7,
        'nonprofit': 8, 'non-profit': 8,
        'government': 9, 'public sector': 9,
        'media': 10, 'entertainment': 10,
        'legal': 11, 'law': 11,
        'real estate': 12, 'real_estate': 12,
        'telecommunications': 13, 'telecom': 13,
        'energy': 14, 'utilities': 14,
        'transportation': 15, 'logistics': 15,
        'hospitality': 16, 'travel': 16,
        'agriculture': 17, 'food': 17,
        'construction': 18,
        'insurance': 19,
    }

    # Company size buckets
    COMPANY_SIZE_BUCKETS = {
        # employees: bucket
        (0, 10): 0,       # Tiny
        (11, 50): 1,      # Small
        (51, 200): 2,     # Medium
        (201, 1000): 3,   # Large
        (1001, float('inf')): 4,  # Enterprise
    }

    # CTA keywords
    CTA_KEYWORDS = [
        'let me know', 'would you be', 'can we', 'could we', 'shall we',
        'schedule a call', 'book a meeting', 'set up a time', 'connect',
        'reply', 'respond', 'get back to', 'reach out', 'hear from you',
        'interested', 'thoughts?', 'available', 'free to chat',
    ]

    # Personalization indicators
    PERSONALIZATION_KEYWORDS = [
        # Company mentions
        'your company', 'at {company}', 'noticed',

        # Role mentions
        'your role', 'as a', 'your experience',

        # Recent events
        'recent', 'saw that', 'read about', 'congratulations',

        # Specific details
        'your article', 'your post', 'your work on',
    ]

    def __init__(self, db: Session):
        self.db = db
        self._domain_cache: Dict[str, Tuple[float, float, float]] = {}
        self._candidate_cache: Dict[int, float] = {}

    def extract_features(
        self,
        campaign: 'FollowUpCampaign',
        email: Optional['FollowUpEmail'] = None,
        recipient_info: Optional[Dict] = None
    ) -> FollowUpFeatures:
        """
        Extract features for a follow-up email prediction.

        Args:
            campaign: The follow-up campaign
            email: Specific email (uses latest if not provided)
            recipient_info: Additional recipient info (optional)

        Returns:
            FollowUpFeatures object
        """
        features = FollowUpFeatures()

        # Get email if not provided
        if email is None and campaign.emails:
            email = campaign.emails[-1]

        # === Recipient Features ===
        recipient_email = ""
        if campaign.application:
            recipient_email = campaign.application.contact_email or ""

        domain = self._extract_domain(recipient_email)
        features.recipient_domain_hash = self._hash_domain(domain)

        # Domain statistics
        open_rate, reply_rate, popularity = self._get_domain_stats(domain, campaign.candidate_id)
        features.domain_historical_open_rate = open_rate
        features.domain_historical_reply_rate = reply_rate
        features.recipient_domain_popularity = popularity

        # Seniority detection
        recipient_title = ""
        if recipient_info:
            recipient_title = recipient_info.get("title", "")
        elif campaign.company_context:
            recipient_title = campaign.company_context.get("position", "")
        features.recipient_seniority_score = self._detect_seniority(recipient_title)

        # === Timing Features ===
        send_time = datetime.now(timezone.utc)
        if email and email.scheduled_for:
            send_time = email.scheduled_for
        elif email and email.sent_at:
            send_time = email.sent_at

        features.hour_of_day = send_time.hour
        features.day_of_week = send_time.weekday()
        features.is_business_hours = 1 if 9 <= send_time.hour < 18 and send_time.weekday() < 5 else 0

        # Days since original email
        if campaign.created_at:
            delta = send_time - campaign.created_at
            features.days_since_original_email = max(0, delta.days)

        # === Content Features ===
        if email:
            features.subject_length = len(email.subject or "")
            features.body_length = len(email.body_text or "")
            features.email_step_number = email.step_number or 1

            # Check for CTA and personalization
            body_lower = (email.body_text or "").lower()
            features.has_call_to_action = 1 if any(kw in body_lower for kw in self.CTA_KEYWORDS) else 0
            features.has_personalization = 1 if self._check_personalization(body_lower, campaign) else 0

        # === Historical Features ===
        features.candidate_overall_reply_rate = self._get_candidate_reply_rate(campaign.candidate_id)

        # === Engagement Features ===
        if campaign.emails_opened is not None:
            features.previous_opens_in_campaign = campaign.emails_opened
        if campaign.emails_clicked is not None:
            features.previous_clicks_in_campaign = campaign.emails_clicked

        # === Company Features ===
        if campaign.company_context:
            industry = campaign.company_context.get("industry", "")
            features.industry_code = self._get_industry_code(industry)

            company_size = campaign.company_context.get("company_size", "")
            features.company_size_bucket = self._get_company_size_bucket(company_size)

        logger.debug(f"[FeatureExtractor] Extracted features: {features.to_dict()}")

        return features

    def _extract_domain(self, email: str) -> str:
        """Extract domain from email address"""
        if "@" in email:
            return email.split("@")[-1].lower()
        return ""

    def _hash_domain(self, domain: str) -> int:
        """Hash domain to integer for feature encoding"""
        if not domain:
            return 0
        return int(hashlib.md5(domain.encode()).hexdigest()[:8], 16)

    def _get_domain_stats(
        self,
        domain: str,
        candidate_id: int
    ) -> Tuple[float, float, float]:
        """
        Get historical statistics for a domain.

        Returns:
            (open_rate, reply_rate, popularity)
        """
        cache_key = f"{candidate_id}:{domain}"
        if cache_key in self._domain_cache:
            return self._domain_cache[cache_key]

        # Query historical data
        try:
            from app.models.follow_up import FollowUpEmail, FollowUpCampaign
            from app.models.application import Application

            # Get emails sent to this domain
            query = self.db.query(
                func.count(FollowUpEmail.id).label('total'),
                func.sum(case((FollowUpEmail.opened_at.isnot(None), 1), else_=0)).label('opened'),
                func.sum(case((FollowUpEmail.replied_at.isnot(None), 1), else_=0)).label('replied')
            ).join(FollowUpCampaign).join(Application).filter(
                FollowUpCampaign.candidate_id == candidate_id,
                Application.contact_email.like(f'%@{domain}'),
                FollowUpEmail.sent_at.isnot(None)
            )

            result = query.first()

            total = result.total or 0
            opened = result.opened or 0
            replied = result.replied or 0

            open_rate = opened / total if total > 0 else 0.0
            reply_rate = replied / total if total > 0 else 0.0
            popularity = min(total / 100, 1.0)  # Normalize to 0-1

            stats = (open_rate, reply_rate, popularity)
            self._domain_cache[cache_key] = stats

            return stats

        except Exception as e:
            logger.warning(f"[FeatureExtractor] Error getting domain stats: {e}")
            return (0.0, 0.0, 0.0)

    def _get_candidate_reply_rate(self, candidate_id: int) -> float:
        """Get overall reply rate for a candidate"""
        if candidate_id in self._candidate_cache:
            return self._candidate_cache[candidate_id]

        try:
            from app.models.follow_up import FollowUpCampaign, CampaignStatus

            query = self.db.query(
                func.count(FollowUpCampaign.id).label('total'),
                func.sum(case((FollowUpCampaign.reply_detected == True, 1), else_=0)).label('replied')
            ).filter(
                FollowUpCampaign.candidate_id == candidate_id,
                FollowUpCampaign.status.in_([
                    CampaignStatus.COMPLETED,
                    CampaignStatus.REPLIED,
                    CampaignStatus.CANCELLED
                ])
            )

            result = query.first()

            total = result.total or 0
            replied = result.replied or 0

            reply_rate = replied / total if total > 0 else 0.15  # Default 15%
            self._candidate_cache[candidate_id] = reply_rate

            return reply_rate

        except Exception as e:
            logger.warning(f"[FeatureExtractor] Error getting candidate reply rate: {e}")
            return 0.15

    def _detect_seniority(self, title: str) -> float:
        """
        Detect seniority level from job title.

        Returns:
            Score from 0 (entry) to 1 (C-suite)
        """
        if not title:
            return 0.3  # Default to mid-level

        title_lower = title.lower()

        # Check keywords in order of priority
        for keyword, score in sorted(self.SENIORITY_KEYWORDS.items(), key=lambda x: -x[1]):
            if keyword in title_lower:
                return score

        return 0.3  # Default

    def _get_industry_code(self, industry: str) -> int:
        """Map industry string to numeric code"""
        if not industry:
            return 0

        industry_lower = industry.lower()

        for keyword, code in self.INDUSTRY_CODES.items():
            if keyword in industry_lower:
                return code

        return 0

    def _get_company_size_bucket(self, size_str: str) -> int:
        """Map company size to bucket"""
        if not size_str:
            return 2  # Default to medium

        # Try to extract number
        try:
            size_lower = size_str.lower()

            # Check for keywords
            if 'enterprise' in size_lower or 'large' in size_lower or '1000+' in size_lower:
                return 4
            elif 'medium' in size_lower or '100-' in size_lower:
                return 2
            elif 'small' in size_lower or '50' in size_lower:
                return 1
            elif 'startup' in size_lower or 'tiny' in size_lower:
                return 0

            # Try to parse number
            import re
            numbers = re.findall(r'\d+', size_str)
            if numbers:
                employees = int(numbers[0])
                for (low, high), bucket in self.COMPANY_SIZE_BUCKETS.items():
                    if low <= employees <= high:
                        return bucket

        except Exception:
            pass

        return 2  # Default to medium

    def _check_personalization(self, body: str, campaign: 'FollowUpCampaign') -> bool:
        """Check if email body contains personalization"""
        # Check for company name mention
        if campaign.company_context:
            company_name = campaign.company_context.get("name", "")
            if company_name and company_name.lower() in body:
                return True

        # Check for personalization keywords
        for keyword in self.PERSONALIZATION_KEYWORDS:
            if keyword in body:
                return True

        return False

    def extract_send_time_features(
        self,
        recipient_domain: str,
        recipient_timezone: str,
        industry: str,
        candidate_id: int
    ) -> Dict[str, Any]:
        """
        Extract features specifically for send time optimization.

        Returns:
            Dictionary of features for send time prediction
        """
        # Get domain stats
        open_rate, reply_rate, popularity = self._get_domain_stats(recipient_domain, candidate_id)

        return {
            "recipient_domain": recipient_domain,
            "recipient_timezone": recipient_timezone,
            "industry_code": self._get_industry_code(industry),
            "domain_open_rate": open_rate,
            "domain_reply_rate": reply_rate,
            "domain_popularity": popularity,
        }
