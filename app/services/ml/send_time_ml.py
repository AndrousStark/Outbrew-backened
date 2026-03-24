"""
ML-Enhanced Send Time Optimizer

ULTRA Follow-Up System V2.0 - Sprint 2

Learns optimal send times from actual engagement data.
Falls back to industry defaults when insufficient data.

Data hierarchy:
1. ML prediction from SendTimeAnalytics (30+ samples)
2. Historical email data query (10+ samples)
3. Industry defaults from SendTimeOptimizer
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
import logging

from sqlalchemy.orm import Session
from sqlalchemy import func, case

from app.models.follow_up import FollowUpEmail, FollowUpCampaign
from app.models.follow_up_ml import SendTimeAnalytics, PredictionConfidence
from app.models.application import Application
from app.services.send_time_optimizer import SendTimeOptimizer, OPTIMAL_SEND_TIMES

logger = logging.getLogger(__name__)


@dataclass
class MLSendTimeResult:
    """Result from ML-enhanced send time optimization"""
    recommended_hour: int           # 0-23
    recommended_day: int            # 0=Monday, 6=Sunday
    confidence: PredictionConfidence
    expected_open_rate_boost: float # Percentage improvement expected
    data_source: str                # ml_prediction, historical_data, industry_default
    sample_size: int
    heatmap_data: Dict[int, Dict[int, float]]  # day -> hour -> score


class SendTimeMLOptimizer:
    """
    ML-enhanced send time optimizer that learns from engagement data.

    Features:
    - Aggregates engagement data by day/hour/domain
    - Builds recipient-specific send time models
    - Falls back to industry defaults when insufficient data
    - Auto-applies when confidence > 85%
    """

    # Minimum samples required for different prediction sources
    MIN_SAMPLES_FOR_ML = 30
    MIN_SAMPLES_FOR_HISTORICAL = 10

    # Confidence thresholds
    HIGH_CONFIDENCE_SAMPLES = 100
    HIGH_CONFIDENCE_SCORE = 0.3
    MEDIUM_CONFIDENCE_SAMPLES = 30
    MEDIUM_CONFIDENCE_SCORE = 0.15

    # Day names for response
    DAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    def __init__(self, db: Session):
        self.db = db
        self.base_optimizer = SendTimeOptimizer()

    def get_optimal_send_time(
        self,
        candidate_id: int,
        recipient_domain: Optional[str] = None,
        recipient_industry: Optional[str] = None,
        recipient_timezone: str = "UTC"
    ) -> MLSendTimeResult:
        """
        Get optimal send time using ML when possible, falling back to industry defaults.

        Args:
            candidate_id: The candidate's ID
            recipient_domain: Domain for per-domain optimization
            recipient_industry: Industry for industry-based defaults
            recipient_timezone: Recipient's timezone

        Returns:
            MLSendTimeResult with recommendation and confidence
        """
        logger.debug(
            f"[SendTimeML] Getting optimal time for candidate={candidate_id}, "
            f"domain={recipient_domain}, industry={recipient_industry}"
        )

        # Try ML-based prediction first (from SendTimeAnalytics)
        ml_result = self._get_ml_prediction(candidate_id, recipient_domain)

        if ml_result and ml_result.sample_size >= self.MIN_SAMPLES_FOR_ML:
            logger.info(
                f"[SendTimeML] Using ML prediction: Day {ml_result.recommended_day}, "
                f"Hour {ml_result.recommended_hour}, samples={ml_result.sample_size}"
            )
            return ml_result

        # Fall back to historical email data query
        historical_result = self._get_historical_prediction(candidate_id, recipient_domain)

        if historical_result and historical_result.sample_size >= self.MIN_SAMPLES_FOR_HISTORICAL:
            logger.info(
                f"[SendTimeML] Using historical data: Day {historical_result.recommended_day}, "
                f"Hour {historical_result.recommended_hour}, samples={historical_result.sample_size}"
            )
            return historical_result

        # Fall back to industry defaults
        logger.debug(f"[SendTimeML] Using industry default for {recipient_industry}")
        return self._get_industry_default(recipient_industry, recipient_timezone)

    def _get_ml_prediction(
        self,
        candidate_id: int,
        recipient_domain: Optional[str]
    ) -> Optional[MLSendTimeResult]:
        """Get prediction from aggregated SendTimeAnalytics"""
        try:
            query = self.db.query(SendTimeAnalytics).filter(
                SendTimeAnalytics.candidate_id == candidate_id
            )

            if recipient_domain:
                query = query.filter(
                    SendTimeAnalytics.recipient_domain == recipient_domain
                )

            analytics = query.all()

            if not analytics:
                return None

            # Calculate total samples
            total_samples = sum(a.emails_sent or 0 for a in analytics)

            if total_samples < self.MIN_SAMPLES_FOR_ML:
                return None

            # Build heatmap and find best slot
            heatmap: Dict[int, Dict[int, float]] = {}
            best_score = 0.0
            best_day = 1  # Tuesday default
            best_hour = 10  # 10am default

            for analytic in analytics:
                day = analytic.day_of_week
                hour = analytic.hour_of_day

                if day not in heatmap:
                    heatmap[day] = {}

                # Score = weighted combination of open and reply rates
                open_rate = analytic.open_rate or 0
                reply_rate = analytic.reply_rate or 0
                score = open_rate * 0.4 + reply_rate * 0.6

                heatmap[day][hour] = score

                # Update best if this slot has enough samples
                if score > best_score and (analytic.emails_sent or 0) >= 5:
                    best_score = score
                    best_day = day
                    best_hour = hour

            # Calculate confidence
            confidence = self._calculate_confidence(total_samples, best_score)

            # Calculate expected boost
            expected_boost = min(best_score * 100, 30)

            return MLSendTimeResult(
                recommended_hour=best_hour,
                recommended_day=best_day,
                confidence=confidence,
                expected_open_rate_boost=expected_boost,
                data_source="ml_prediction",
                sample_size=total_samples,
                heatmap_data=heatmap
            )

        except Exception as e:
            logger.warning(f"[SendTimeML] Error getting ML prediction: {e}")
            return None

    def _get_historical_prediction(
        self,
        candidate_id: int,
        recipient_domain: Optional[str]
    ) -> Optional[MLSendTimeResult]:
        """Get prediction from historical email engagement data"""
        try:
            # Query actual email engagement grouped by day/hour
            # Using PostgreSQL extract for day of week and hour
            query = self.db.query(
                func.extract('dow', FollowUpEmail.sent_at).label('day_of_week'),
                func.extract('hour', FollowUpEmail.sent_at).label('hour_of_day'),
                func.count(FollowUpEmail.id).label('total_sent'),
                func.sum(case((FollowUpEmail.opened_at.isnot(None), 1), else_=0)).label('total_opened'),
                func.sum(case((FollowUpEmail.replied_at.isnot(None), 1), else_=0)).label('total_replied')
            ).join(
                FollowUpCampaign,
                FollowUpEmail.campaign_id == FollowUpCampaign.id
            ).filter(
                FollowUpCampaign.candidate_id == candidate_id,
                FollowUpEmail.sent_at.isnot(None)
            )

            # Filter by domain if provided
            if recipient_domain:
                query = query.join(
                    Application,
                    FollowUpCampaign.application_id == Application.id
                ).filter(
                    Application.contact_email.like(f'%@{recipient_domain}')
                )

            query = query.group_by('day_of_week', 'hour_of_day')

            results = query.all()

            if not results:
                return None

            # Build heatmap and find best slot
            heatmap: Dict[int, Dict[int, float]] = {}
            best_score = 0.0
            best_day = 1
            best_hour = 10
            total_samples = 0

            for row in results:
                # PostgreSQL DOW: 0=Sunday, 1=Monday, etc.
                # Convert to Python: 0=Monday
                day = int(row.day_of_week)
                if day == 0:
                    day = 6  # Sunday
                else:
                    day = day - 1  # Shift Monday-Saturday

                hour = int(row.hour_of_day)
                sent = row.total_sent or 0
                opened = row.total_opened or 0
                replied = row.total_replied or 0

                total_samples += sent

                if day not in heatmap:
                    heatmap[day] = {}

                if sent > 0:
                    open_rate = opened / sent
                    reply_rate = replied / sent
                    score = open_rate * 0.4 + reply_rate * 0.6
                    heatmap[day][hour] = score

                    if score > best_score and sent >= 3:
                        best_score = score
                        best_day = day
                        best_hour = hour

            if total_samples < self.MIN_SAMPLES_FOR_HISTORICAL:
                return None

            confidence = self._calculate_confidence(total_samples, best_score)

            return MLSendTimeResult(
                recommended_hour=best_hour,
                recommended_day=best_day,
                confidence=confidence,
                expected_open_rate_boost=min(best_score * 100, 25),
                data_source="historical_data",
                sample_size=total_samples,
                heatmap_data=heatmap
            )

        except Exception as e:
            logger.warning(f"[SendTimeML] Error getting historical prediction: {e}")
            return None

    def _get_industry_default(
        self,
        industry: Optional[str],
        timezone: str
    ) -> MLSendTimeResult:
        """Fall back to research-backed industry defaults"""
        try:
            # Use existing SendTimeOptimizer
            result = self.base_optimizer.get_optimal_send_time(
                industry=industry or "default",
                recipient_timezone=timezone
            )

            # Get config for heatmap
            industry_key = (industry or "default").lower()
            config = OPTIMAL_SEND_TIMES.get(industry_key, OPTIMAL_SEND_TIMES.get("default", {}))

            # Build default heatmap
            heatmap: Dict[int, Dict[int, float]] = {}

            if config:
                best_days = config.get("days", [1, 2, 3])
                best_hours = config.get("hours", [10, 11, 14])

                for i, day in enumerate(best_days):
                    heatmap[day] = {}
                    for j, hour in enumerate(best_hours):
                        # Score based on position (first = best)
                        score = 1.0 - (i * 0.1) - (j * 0.1)
                        heatmap[day][hour] = max(0.5, score)

                recommended_day = best_days[0] if best_days else 1
                recommended_hour = best_hours[0] if best_hours else 10
                boost_range = config.get("expected_boost_range", (15, 25))
                expected_boost = boost_range[0]
            else:
                # Fallback defaults
                recommended_day = 1  # Tuesday
                recommended_hour = 10
                expected_boost = 15
                heatmap = {1: {10: 0.8, 11: 0.75}, 2: {10: 0.7, 11: 0.65}}

            return MLSendTimeResult(
                recommended_hour=recommended_hour,
                recommended_day=recommended_day,
                confidence=PredictionConfidence.MEDIUM,
                expected_open_rate_boost=expected_boost,
                data_source="industry_default",
                sample_size=0,
                heatmap_data=heatmap
            )

        except Exception as e:
            logger.warning(f"[SendTimeML] Error getting industry default: {e}")
            # Ultimate fallback
            return MLSendTimeResult(
                recommended_hour=10,
                recommended_day=1,
                confidence=PredictionConfidence.LOW,
                expected_open_rate_boost=10,
                data_source="fallback_default",
                sample_size=0,
                heatmap_data={1: {10: 0.7}}
            )

    def _calculate_confidence(
        self,
        sample_size: int,
        best_score: float
    ) -> PredictionConfidence:
        """Calculate confidence based on sample size and best score"""
        if sample_size >= self.HIGH_CONFIDENCE_SAMPLES and best_score >= self.HIGH_CONFIDENCE_SCORE:
            return PredictionConfidence.HIGH
        elif sample_size >= self.MEDIUM_CONFIDENCE_SAMPLES and best_score >= self.MEDIUM_CONFIDENCE_SCORE:
            return PredictionConfidence.MEDIUM
        else:
            return PredictionConfidence.LOW

    def update_analytics(
        self,
        candidate_id: int,
        email: FollowUpEmail
    ) -> None:
        """
        Update SendTimeAnalytics when email engagement is recorded.

        Called after email is sent or engagement is detected.
        """
        if not email.sent_at:
            return

        try:
            # Extract domain from recipient
            campaign = email.campaign
            if not campaign or not campaign.application:
                return

            recipient_email = campaign.application.contact_email or ""
            domain = recipient_email.split("@")[-1] if "@" in recipient_email else ""

            if not domain:
                return

            # Get day and hour
            day_of_week = email.sent_at.weekday()  # 0=Monday
            hour_of_day = email.sent_at.hour

            # Get or create analytics record
            analytic = self.db.query(SendTimeAnalytics).filter(
                SendTimeAnalytics.candidate_id == candidate_id,
                SendTimeAnalytics.recipient_domain == domain,
                SendTimeAnalytics.day_of_week == day_of_week,
                SendTimeAnalytics.hour_of_day == hour_of_day
            ).first()

            if not analytic:
                analytic = SendTimeAnalytics(
                    candidate_id=candidate_id,
                    recipient_domain=domain,
                    recipient_industry=(
                        campaign.company_context.get("industry")
                        if campaign.company_context else None
                    ),
                    day_of_week=day_of_week,
                    hour_of_day=hour_of_day,
                    emails_sent=0,
                    emails_opened=0,
                    emails_clicked=0,
                    emails_replied=0
                )
                self.db.add(analytic)

            # Update counts
            analytic.emails_sent = (analytic.emails_sent or 0) + 1

            if email.opened_at:
                analytic.emails_opened = (analytic.emails_opened or 0) + 1

            if email.clicked_at:
                analytic.emails_clicked = (analytic.emails_clicked or 0) + 1

            if email.replied_at:
                analytic.emails_replied = (analytic.emails_replied or 0) + 1

            # Recalculate rates
            analytic.calculate_rates()
            analytic.last_email_at = datetime.now(timezone.utc)

            self.db.commit()

            logger.debug(
                f"[SendTimeML] Updated analytics for {domain} at day={day_of_week}, hour={hour_of_day}: "
                f"sent={analytic.emails_sent}, opened={analytic.emails_opened}"
            )

        except Exception as e:
            logger.warning(f"[SendTimeML] Error updating analytics: {e}")
            self.db.rollback()

    def get_heatmap_data(
        self,
        candidate_id: int,
        recipient_domain: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get heatmap data for visualization.

        Returns:
            Dictionary with day/hour matrix and metadata
        """
        result = self.get_optimal_send_time(
            candidate_id=candidate_id,
            recipient_domain=recipient_domain
        )

        # Convert heatmap to string keys for JSON
        day_names = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        serialized_heatmap = {}

        for day, hours in result.heatmap_data.items():
            day_key = day_names[day] if isinstance(day, int) and day < 7 else str(day)
            serialized_heatmap[day_key] = {
                str(hour): round(score, 3)
                for hour, score in hours.items()
            }

        return {
            "heatmap": serialized_heatmap,
            "best_day": result.recommended_day,
            "best_day_name": self.DAY_NAMES[result.recommended_day],
            "best_hour": result.recommended_hour,
            "confidence": result.confidence.value,
            "data_source": result.data_source,
            "sample_size": result.sample_size,
            "expected_boost": round(result.expected_open_rate_boost, 1)
        }
