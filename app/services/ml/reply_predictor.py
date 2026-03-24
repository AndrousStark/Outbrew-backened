"""
Reply Probability Predictor

ULTRA Follow-Up System V2.0 - Sprint 2

Predicts the likelihood of a recipient replying to a follow-up email.
Uses gradient boosting when trained, falls back to heuristics otherwise.

Key features:
- Probability prediction (0-100%)
- Confidence levels for auto-apply decision
- Priority scoring for campaign ordering
- Explainable predictions with top factors
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone
from dataclasses import dataclass
from enum import Enum
import logging
import json
import os

from sqlalchemy.orm import Session

from app.models.follow_up import FollowUpCampaign, FollowUpEmail
from app.models.follow_up_ml import FollowUpPrediction, PredictionConfidence
from app.services.ml.gradient_boosting import GradientBoostingClassifier
from app.services.ml.feature_extractor import FeatureExtractor, FollowUpFeatures, FEATURE_NAMES

logger = logging.getLogger(__name__)


@dataclass
class ReplyPredictionResult:
    """Result of reply probability prediction"""
    probability: float          # 0-1
    confidence: PredictionConfidence
    priority_score: int         # 0-100
    top_factors: Dict[str, float]
    recommended_action: str
    model_version: str
    is_ml_prediction: bool = False


class ReplyPredictor:
    """
    Predicts reply probability for follow-up campaigns.

    Features:
    - Gradient boosting ML model when trained
    - Heuristic fallback when insufficient data
    - Confidence-based auto-apply (85%+ threshold)
    - Priority scoring for campaign ordering
    - Explainable predictions with feature importance
    """

    AUTO_APPLY_CONFIDENCE_THRESHOLD = 0.85
    MIN_TRAINING_SAMPLES = 100
    MODEL_VERSION = "v2.0.0"
    MODEL_STORAGE_DIR = "ml_models"

    # Confidence thresholds
    HIGH_CONFIDENCE_MIN_SAMPLES = 100
    HIGH_CONFIDENCE_MIN_SCORE = 0.3
    MEDIUM_CONFIDENCE_MIN_SAMPLES = 30
    MEDIUM_CONFIDENCE_MIN_SCORE = 0.15

    def __init__(self, db: Session):
        self.db = db
        self.feature_extractor = FeatureExtractor(db)
        self.model: Optional[GradientBoostingClassifier] = None
        self._model_loaded = False
        self._load_model_attempted = False

    def predict(
        self,
        campaign: FollowUpCampaign,
        email: Optional[FollowUpEmail] = None
    ) -> ReplyPredictionResult:
        """
        Predict reply probability for a campaign/email.

        Args:
            campaign: The follow-up campaign
            email: Specific email (optional, uses next scheduled if not provided)

        Returns:
            ReplyPredictionResult with probability, confidence, and recommendations
        """
        # Try to load model if not attempted
        if not self._load_model_attempted:
            self._try_load_model(campaign.candidate_id)

        # Extract features
        features = self.feature_extractor.extract_features(campaign, email)
        feature_vector = features.to_vector()

        # Get prediction
        if self._model_loaded and self.model:
            probability = self.model.predict_proba(feature_vector)
            is_ml_prediction = True
            logger.debug(f"[ReplyPredictor] ML prediction: {probability:.3f}")
        else:
            probability = self._heuristic_predict(features)
            is_ml_prediction = False
            logger.debug(f"[ReplyPredictor] Heuristic prediction: {probability:.3f}")

        # Calculate confidence
        confidence = self._calculate_confidence(probability, features, is_ml_prediction)

        # Calculate priority score (0-100)
        priority_score = self._calculate_priority(probability, confidence, features)

        # Get top contributing factors
        top_factors = self._get_top_factors(features, feature_vector)

        # Determine recommended action
        recommended_action = self._get_recommended_action(probability, confidence)

        return ReplyPredictionResult(
            probability=probability,
            confidence=confidence,
            priority_score=priority_score,
            top_factors=top_factors,
            recommended_action=recommended_action,
            model_version=self.MODEL_VERSION,
            is_ml_prediction=is_ml_prediction
        )

    def predict_batch(
        self,
        campaigns: List[FollowUpCampaign]
    ) -> List[Tuple[int, ReplyPredictionResult]]:
        """
        Predict for multiple campaigns and return sorted by priority.

        Args:
            campaigns: List of campaigns to predict

        Returns:
            List of (campaign_id, result) tuples, sorted by priority descending
        """
        results = []

        for campaign in campaigns:
            try:
                result = self.predict(campaign)
                results.append((campaign.id, result))
            except Exception as e:
                logger.warning(f"[ReplyPredictor] Error predicting for campaign {campaign.id}: {e}")
                continue

        # Sort by priority score descending
        results.sort(key=lambda x: x[1].priority_score, reverse=True)

        return results

    def store_prediction(
        self,
        campaign: FollowUpCampaign,
        result: ReplyPredictionResult,
        email: Optional[FollowUpEmail] = None
    ) -> FollowUpPrediction:
        """Store prediction in database for tracking and accuracy evaluation"""
        prediction = FollowUpPrediction(
            campaign_id=campaign.id,
            email_id=email.id if email else None,
            candidate_id=campaign.candidate_id,
            reply_probability=result.probability,
            reply_probability_confidence=result.confidence,
            priority_score=result.priority_score,
            features_json=result.top_factors,
            model_version=result.model_version,
            model_type="gradient_boosting" if result.is_ml_prediction else "heuristic",
            auto_applied=(result.confidence == PredictionConfidence.HIGH),
            auto_apply_reason=self._get_auto_apply_reason(result),
            predicted_at=datetime.now(timezone.utc)
        )

        self.db.add(prediction)
        self.db.commit()

        logger.info(
            f"[ReplyPredictor] Stored prediction for campaign {campaign.id}: "
            f"prob={result.probability:.2%}, confidence={result.confidence.value}"
        )

        return prediction

    def _heuristic_predict(self, features: FollowUpFeatures) -> float:
        """
        Fallback heuristic prediction when ML model not trained.

        Based on industry research and engagement patterns.
        """
        # Base probability (industry average)
        base_probability = 0.15

        # === Domain historical data (strongest signal) ===
        if features.domain_historical_reply_rate > 0:
            # Blend with historical rate
            base_probability = (
                features.domain_historical_reply_rate * 0.6 +
                base_probability * 0.4
            )

        # === Timing adjustments ===
        # Business hours boost
        if features.is_business_hours:
            base_probability *= 1.15

        # Best days (Tuesday, Wednesday, Thursday)
        if features.day_of_week in [1, 2, 3]:
            base_probability *= 1.1

        # Best hours (9-11am, 2-4pm)
        if features.hour_of_day in [9, 10, 11, 14, 15, 16]:
            base_probability *= 1.1

        # === Recipient adjustments ===
        # Seniority factor (senior people are busier but respond when engaged)
        if features.recipient_seniority_score >= 0.8:
            # C-suite: slightly lower base but higher when engaged
            base_probability *= 0.9
        elif features.recipient_seniority_score >= 0.5:
            # Directors/managers: good response rate
            base_probability *= 1.1

        # === Content adjustments ===
        # Step number decay (later steps have lower response)
        step_decay = max(0.5, 1.0 - (features.email_step_number - 1) * 0.12)
        base_probability *= step_decay

        # CTA boost
        if features.has_call_to_action:
            base_probability *= 1.1

        # Personalization boost
        if features.has_personalization:
            base_probability *= 1.15

        # Optimal subject length (40-60 chars)
        if 40 <= features.subject_length <= 60:
            base_probability *= 1.05

        # === Engagement history (strong signal) ===
        # Previous opens = interest
        if features.previous_opens_in_campaign > 0:
            open_boost = 1 + (features.previous_opens_in_campaign * 0.1)
            base_probability *= min(open_boost, 1.4)

        # Previous clicks = high interest
        if features.previous_clicks_in_campaign > 0:
            click_boost = 1 + (features.previous_clicks_in_campaign * 0.15)
            base_probability *= min(click_boost, 1.6)

        # === Candidate history ===
        if features.candidate_overall_reply_rate > 0.2:
            # Above average performer
            base_probability *= 1.1
        elif features.candidate_overall_reply_rate < 0.1:
            # Below average
            base_probability *= 0.9

        # === Time since original ===
        # Sweet spot is 3-7 days
        if 3 <= features.days_since_original_email <= 7:
            base_probability *= 1.05
        elif features.days_since_original_email > 14:
            # Going stale
            base_probability *= 0.9

        # Clamp to valid range
        return max(0.01, min(0.95, base_probability))

    def _calculate_confidence(
        self,
        probability: float,
        features: FollowUpFeatures,
        is_ml_prediction: bool
    ) -> PredictionConfidence:
        """Calculate confidence level based on data quality and prediction certainty"""
        confidence_score = 0.5

        # ML model increases confidence
        if is_ml_prediction:
            confidence_score += 0.25

        # Historical domain data increases confidence
        if features.domain_historical_reply_rate > 0:
            confidence_score += 0.15

        if features.domain_historical_open_rate > 0:
            confidence_score += 0.1

        # Previous engagement in this campaign
        if features.previous_opens_in_campaign > 0:
            confidence_score += 0.1

        # Extreme probabilities are more confident
        if probability < 0.1 or probability > 0.5:
            confidence_score += 0.1

        # Domain popularity (more data = more confidence)
        confidence_score += features.recipient_domain_popularity * 0.1

        # Determine level
        if confidence_score >= self.AUTO_APPLY_CONFIDENCE_THRESHOLD:
            return PredictionConfidence.HIGH
        elif confidence_score >= 0.60:
            return PredictionConfidence.MEDIUM
        else:
            return PredictionConfidence.LOW

    def _calculate_priority(
        self,
        probability: float,
        confidence: PredictionConfidence,
        features: FollowUpFeatures
    ) -> int:
        """
        Calculate priority score for campaign ordering.

        Higher scores = should send first.
        """
        # Base score from probability (0-50 points)
        score = probability * 50

        # Confidence multiplier
        confidence_multiplier = {
            PredictionConfidence.HIGH: 1.4,
            PredictionConfidence.MEDIUM: 1.2,
            PredictionConfidence.LOW: 1.0
        }
        score *= confidence_multiplier[confidence]

        # Engagement boost (previous interest signals)
        if features.previous_opens_in_campaign > 0:
            score += 10
        if features.previous_clicks_in_campaign > 0:
            score += 15

        # Time sensitivity (slightly older campaigns get priority)
        # But not too old (>14 days penalized)
        if features.days_since_original_email <= 14:
            score += min(features.days_since_original_email * 0.5, 7)
        else:
            score -= (features.days_since_original_email - 14) * 0.5

        # Step number consideration (earlier steps = higher priority)
        if features.email_step_number == 1:
            score += 5
        elif features.email_step_number >= 4:
            score -= 5

        # High seniority targets get slight priority
        if features.recipient_seniority_score >= 0.8:
            score += 5

        return max(0, min(100, int(score)))

    def _get_top_factors(
        self,
        features: FollowUpFeatures,
        feature_vector: List[float]
    ) -> Dict[str, float]:
        """Get top contributing factors for prediction explainability"""
        # If ML model has feature importance, use it
        if self._model_loaded and self.model:
            importance = self.model.get_feature_importance(normalize=True)
            if importance:
                # Map indices to names and sort
                named_importance = {
                    FEATURE_NAMES[idx]: score
                    for idx, score in importance.items()
                    if idx < len(FEATURE_NAMES)
                }
                # Return top 5
                sorted_factors = sorted(
                    named_importance.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5]
                return dict(sorted_factors)

        # Fallback: heuristic importance based on feature values
        factor_importance = {}

        # Domain reply rate (strongest)
        if features.domain_historical_reply_rate > 0:
            factor_importance["domain_reply_rate"] = 0.25

        # Previous engagement
        if features.previous_opens_in_campaign > 0:
            factor_importance["previous_opens"] = 0.20
        if features.previous_clicks_in_campaign > 0:
            factor_importance["previous_clicks"] = 0.22

        # Step number
        factor_importance["step_number"] = 0.15

        # Timing
        if features.is_business_hours:
            factor_importance["is_business_hours"] = 0.10

        # Seniority
        factor_importance["seniority_score"] = 0.08

        # Normalize and return top 5
        total = sum(factor_importance.values())
        if total > 0:
            factor_importance = {k: v / total for k, v in factor_importance.items()}

        return dict(sorted(factor_importance.items(), key=lambda x: -x[1])[:5])

    def _get_recommended_action(
        self,
        probability: float,
        confidence: PredictionConfidence
    ) -> str:
        """Get recommended action based on prediction"""
        if probability >= 0.4 and confidence == PredictionConfidence.HIGH:
            return "prioritize_send"
        elif probability >= 0.25:
            return "send_at_optimal_time"
        elif probability >= 0.1:
            return "consider_different_approach"
        else:
            return "review_campaign_strategy"

    def _get_auto_apply_reason(self, result: ReplyPredictionResult) -> Optional[str]:
        """Get reason for auto-apply decision"""
        if result.confidence == PredictionConfidence.HIGH:
            return f"High confidence ({result.confidence.value}), probability {result.probability:.1%}"
        elif result.confidence == PredictionConfidence.MEDIUM:
            return f"Medium confidence - suggested but not auto-applied"
        else:
            return f"Low confidence - manual review needed"

    def _try_load_model(self, candidate_id: int) -> bool:
        """Attempt to load trained model for candidate"""
        self._load_model_attempted = True

        try:
            model_path = os.path.join(
                self.MODEL_STORAGE_DIR,
                f"reply_model_{candidate_id}.json"
            )

            if os.path.exists(model_path):
                self.model = GradientBoostingClassifier.load_from_json(model_path)
                self._model_loaded = True
                logger.info(f"[ReplyPredictor] Loaded model for candidate {candidate_id}")
                return True
            else:
                logger.debug(f"[ReplyPredictor] No model found for candidate {candidate_id}")
                return False

        except Exception as e:
            logger.warning(f"[ReplyPredictor] Error loading model: {e}")
            return False

    def set_model(self, model: GradientBoostingClassifier) -> None:
        """Set model directly (for testing or after training)"""
        self.model = model
        self._model_loaded = True
        self._load_model_attempted = True
