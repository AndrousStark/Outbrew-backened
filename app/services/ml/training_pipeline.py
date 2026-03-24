"""
ML Training Pipeline

ULTRA Follow-Up System V2.0 - Sprint 2

Background training pipeline for ML models:
- Collects training data from completed campaigns
- Trains gradient boosting models per candidate
- Updates prediction accuracy based on actual outcomes
- Provides accuracy statistics for dashboard

Designed to run daily at 05:00 UTC via scheduler.
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
import logging
import os
import random

from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from app.models.follow_up import (
    FollowUpCampaign, FollowUpEmail, CampaignStatus
)
from app.models.follow_up_ml import (
    FollowUpPrediction, PredictionConfidence, SendTimeAnalytics
)
from app.services.ml.gradient_boosting import GradientBoostingClassifier
from app.services.ml.feature_extractor import FeatureExtractor

logger = logging.getLogger(__name__)


@dataclass
class TrainingResult:
    """Result from model training"""
    candidate_id: int
    success: bool
    model_version: str
    samples_used: int
    positive_samples: int
    negative_samples: int
    validation_accuracy: float
    validation_precision: float
    validation_recall: float
    feature_importance: Dict[int, float]
    training_time_ms: int
    error_message: Optional[str] = None


@dataclass
class AccuracyStats:
    """Prediction accuracy statistics"""
    total_predictions: int
    evaluated_predictions: int
    accurate_predictions: int
    overall_accuracy: float
    accuracy_by_confidence: Dict[str, Dict[str, Any]]
    avg_probability_when_replied: float
    avg_probability_when_not_replied: float


class MLTrainingPipeline:
    """
    ML Training Pipeline for Reply Prediction Models.

    Features:
    - Collects training data from completed campaigns
    - Trains per-candidate gradient boosting models
    - Validates models with 20% holdout
    - Updates prediction accuracy retroactively
    - Tracks accuracy by confidence level
    """

    MIN_TRAINING_SAMPLES = 100
    VALIDATION_SPLIT = 0.2
    MODEL_VERSION = "v2.0.0"
    MODEL_STORAGE_DIR = "ml_models"

    # Lookback period for training data
    DEFAULT_LOOKBACK_DAYS = 90

    def __init__(self, db: Session):
        self.db = db
        self.feature_extractor = FeatureExtractor(db)

    def collect_training_data(
        self,
        candidate_id: int,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS
    ) -> Tuple[List[List[float]], List[int]]:
        """
        Collect training data from completed campaigns.

        Args:
            candidate_id: The candidate's ID
            lookback_days: Number of days to look back for training data

        Returns:
            Tuple of (X feature matrix, y labels)
        """
        logger.info(f"[MLTraining] Collecting training data for candidate {candidate_id}")

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        # Query completed campaigns with outcome known
        campaigns = self.db.query(FollowUpCampaign).filter(
            FollowUpCampaign.candidate_id == candidate_id,
            FollowUpCampaign.created_at >= cutoff_date,
            FollowUpCampaign.status.in_([
                CampaignStatus.COMPLETED,
                CampaignStatus.REPLIED,
                CampaignStatus.CANCELLED,
                CampaignStatus.BOUNCED
            ])
        ).all()

        X = []
        y = []

        for campaign in campaigns:
            try:
                # Get the last sent email for feature extraction
                last_email = self.db.query(FollowUpEmail).filter(
                    FollowUpEmail.campaign_id == campaign.id,
                    FollowUpEmail.sent_at.isnot(None)
                ).order_by(FollowUpEmail.step_number.desc()).first()

                if not last_email:
                    continue

                # Extract features
                features = self.feature_extractor.extract_features(campaign, last_email)
                feature_vector = features.to_vector()

                # Label: 1 if replied, 0 otherwise
                label = 1 if campaign.reply_detected else 0

                X.append(feature_vector)
                y.append(label)

            except Exception as e:
                logger.warning(
                    f"[MLTraining] Error extracting features for campaign {campaign.id}: {e}"
                )
                continue

        logger.info(
            f"[MLTraining] Collected {len(X)} samples for candidate {candidate_id}: "
            f"{sum(y)} positive, {len(y) - sum(y)} negative"
        )

        return X, y

    def train_model(
        self,
        candidate_id: int,
        X: List[List[float]],
        y: List[int]
    ) -> TrainingResult:
        """
        Train a gradient boosting model for the candidate.

        Args:
            candidate_id: The candidate's ID
            X: Feature matrix
            y: Labels

        Returns:
            TrainingResult with training metrics
        """
        import time
        start_time = time.time()

        n_samples = len(X)
        n_positive = sum(y)
        n_negative = n_samples - n_positive

        logger.info(
            f"[MLTraining] Training model for candidate {candidate_id} with {n_samples} samples"
        )

        # Check minimum samples
        if n_samples < self.MIN_TRAINING_SAMPLES:
            return TrainingResult(
                candidate_id=candidate_id,
                success=False,
                model_version=self.MODEL_VERSION,
                samples_used=n_samples,
                positive_samples=n_positive,
                negative_samples=n_negative,
                validation_accuracy=0.0,
                validation_precision=0.0,
                validation_recall=0.0,
                feature_importance={},
                training_time_ms=0,
                error_message=f"Insufficient samples: {n_samples} < {self.MIN_TRAINING_SAMPLES}"
            )

        # Check class balance
        if n_positive < 5 or n_negative < 5:
            return TrainingResult(
                candidate_id=candidate_id,
                success=False,
                model_version=self.MODEL_VERSION,
                samples_used=n_samples,
                positive_samples=n_positive,
                negative_samples=n_negative,
                validation_accuracy=0.0,
                validation_precision=0.0,
                validation_recall=0.0,
                feature_importance={},
                training_time_ms=0,
                error_message=f"Insufficient class balance: {n_positive} positive, {n_negative} negative"
            )

        try:
            # Split data for validation
            X_train, X_val, y_train, y_val = self._train_test_split(X, y)

            # Train model
            model = GradientBoostingClassifier(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=3,
                min_samples_split=10,
                subsample=0.8,
                random_state=42
            )

            model.fit(X_train, y_train)

            # Validate
            y_pred_proba = model.predict_proba_batch(X_val)
            y_pred = [1 if p >= 0.5 else 0 for p in y_pred_proba]

            # Calculate metrics
            accuracy = sum(1 for a, b in zip(y_val, y_pred) if a == b) / len(y_val)

            # Precision and recall
            true_positives = sum(1 for a, b in zip(y_val, y_pred) if a == 1 and b == 1)
            predicted_positives = sum(y_pred)
            actual_positives = sum(y_val)

            precision = true_positives / predicted_positives if predicted_positives > 0 else 0.0
            recall = true_positives / actual_positives if actual_positives > 0 else 0.0

            # Save model
            self._save_model(candidate_id, model)

            training_time_ms = int((time.time() - start_time) * 1000)

            logger.info(
                f"[MLTraining] Model trained for candidate {candidate_id}: "
                f"accuracy={accuracy:.3f}, precision={precision:.3f}, recall={recall:.3f}"
            )

            return TrainingResult(
                candidate_id=candidate_id,
                success=True,
                model_version=self.MODEL_VERSION,
                samples_used=n_samples,
                positive_samples=n_positive,
                negative_samples=n_negative,
                validation_accuracy=accuracy,
                validation_precision=precision,
                validation_recall=recall,
                feature_importance=model.get_feature_importance(normalize=True),
                training_time_ms=training_time_ms
            )

        except Exception as e:
            logger.error(f"[MLTraining] Error training model for candidate {candidate_id}: {e}")
            return TrainingResult(
                candidate_id=candidate_id,
                success=False,
                model_version=self.MODEL_VERSION,
                samples_used=n_samples,
                positive_samples=n_positive,
                negative_samples=n_negative,
                validation_accuracy=0.0,
                validation_precision=0.0,
                validation_recall=0.0,
                feature_importance={},
                training_time_ms=int((time.time() - start_time) * 1000),
                error_message=str(e)
            )

    def _train_test_split(
        self,
        X: List[List[float]],
        y: List[int]
    ) -> Tuple[List[List[float]], List[List[float]], List[int], List[int]]:
        """Split data into training and validation sets"""
        # Create indices
        indices = list(range(len(X)))
        random.seed(42)
        random.shuffle(indices)

        # Calculate split point
        split_idx = int(len(indices) * (1 - self.VALIDATION_SPLIT))

        train_indices = indices[:split_idx]
        val_indices = indices[split_idx:]

        X_train = [X[i] for i in train_indices]
        X_val = [X[i] for i in val_indices]
        y_train = [y[i] for i in train_indices]
        y_val = [y[i] for i in val_indices]

        return X_train, X_val, y_train, y_val

    def _save_model(self, candidate_id: int, model: GradientBoostingClassifier) -> None:
        """Save trained model to disk"""
        os.makedirs(self.MODEL_STORAGE_DIR, exist_ok=True)

        model_path = os.path.join(
            self.MODEL_STORAGE_DIR,
            f"reply_model_{candidate_id}.json"
        )

        model.save_to_json(model_path)
        logger.info(f"[MLTraining] Model saved to {model_path}")

    def update_prediction_accuracy(self, candidate_id: int) -> int:
        """
        Update prediction accuracy based on actual campaign outcomes.

        Called after campaigns complete to backfill accuracy data.

        Args:
            candidate_id: The candidate's ID

        Returns:
            Number of predictions updated
        """
        logger.info(f"[MLTraining] Updating prediction accuracy for candidate {candidate_id}")

        # Find predictions without accuracy data
        predictions = self.db.query(FollowUpPrediction).filter(
            FollowUpPrediction.candidate_id == candidate_id,
            FollowUpPrediction.prediction_accurate.is_(None),
            FollowUpPrediction.campaign_id.isnot(None)
        ).all()

        updated_count = 0

        for prediction in predictions:
            try:
                # Get campaign outcome
                campaign = self.db.query(FollowUpCampaign).filter(
                    FollowUpCampaign.id == prediction.campaign_id
                ).first()

                if not campaign:
                    continue

                # Only update if campaign has a definitive outcome
                if campaign.status not in [
                    CampaignStatus.COMPLETED,
                    CampaignStatus.REPLIED,
                    CampaignStatus.CANCELLED,
                    CampaignStatus.BOUNCED
                ]:
                    continue

                # Record actual outcome
                prediction.actual_replied = campaign.reply_detected

                if campaign.reply_detected and campaign.reply_detected_at:
                    # Calculate hours to reply
                    first_email = self.db.query(FollowUpEmail).filter(
                        FollowUpEmail.campaign_id == campaign.id,
                        FollowUpEmail.sent_at.isnot(None)
                    ).order_by(FollowUpEmail.sent_at.asc()).first()

                    if first_email and first_email.sent_at:
                        hours = (campaign.reply_detected_at - first_email.sent_at).total_seconds() / 3600
                        prediction.actual_reply_hours = hours

                # Determine if prediction was accurate
                # Accurate if: predicted high probability and got reply, OR
                #             predicted low probability and no reply
                predicted_reply = prediction.reply_probability >= 0.25  # Threshold
                prediction.prediction_accurate = (predicted_reply == prediction.actual_replied)

                updated_count += 1

            except Exception as e:
                logger.warning(
                    f"[MLTraining] Error updating prediction {prediction.id}: {e}"
                )
                continue

        self.db.commit()

        logger.info(
            f"[MLTraining] Updated {updated_count} predictions for candidate {candidate_id}"
        )

        return updated_count

    def get_accuracy_stats(self, candidate_id: int) -> AccuracyStats:
        """
        Get prediction accuracy statistics for dashboard.

        Args:
            candidate_id: The candidate's ID

        Returns:
            AccuracyStats with detailed accuracy breakdown
        """
        # Get all predictions with accuracy data
        predictions = self.db.query(FollowUpPrediction).filter(
            FollowUpPrediction.candidate_id == candidate_id
        ).all()

        total_predictions = len(predictions)
        evaluated_predictions = sum(1 for p in predictions if p.prediction_accurate is not None)
        accurate_predictions = sum(1 for p in predictions if p.prediction_accurate is True)

        overall_accuracy = (
            accurate_predictions / evaluated_predictions
            if evaluated_predictions > 0 else 0.0
        )

        # Accuracy by confidence level
        accuracy_by_confidence = {
            "high": {"total": 0, "evaluated": 0, "accurate": 0, "accuracy": 0.0},
            "medium": {"total": 0, "evaluated": 0, "accurate": 0, "accuracy": 0.0},
            "low": {"total": 0, "evaluated": 0, "accurate": 0, "accuracy": 0.0}
        }

        # Average probabilities by outcome
        prob_when_replied = []
        prob_when_not_replied = []

        for prediction in predictions:
            confidence = prediction.reply_probability_confidence
            if confidence:
                conf_key = confidence.value.lower()
                if conf_key in accuracy_by_confidence:
                    accuracy_by_confidence[conf_key]["total"] += 1

                    if prediction.prediction_accurate is not None:
                        accuracy_by_confidence[conf_key]["evaluated"] += 1

                        if prediction.prediction_accurate:
                            accuracy_by_confidence[conf_key]["accurate"] += 1

            # Track probabilities by actual outcome
            if prediction.actual_replied is not None:
                if prediction.actual_replied:
                    prob_when_replied.append(prediction.reply_probability)
                else:
                    prob_when_not_replied.append(prediction.reply_probability)

        # Calculate accuracy per confidence level
        for conf_key in accuracy_by_confidence:
            conf_data = accuracy_by_confidence[conf_key]
            if conf_data["evaluated"] > 0:
                conf_data["accuracy"] = conf_data["accurate"] / conf_data["evaluated"]

        # Calculate average probabilities
        avg_prob_replied = (
            sum(prob_when_replied) / len(prob_when_replied)
            if prob_when_replied else 0.0
        )
        avg_prob_not_replied = (
            sum(prob_when_not_replied) / len(prob_when_not_replied)
            if prob_when_not_replied else 0.0
        )

        return AccuracyStats(
            total_predictions=total_predictions,
            evaluated_predictions=evaluated_predictions,
            accurate_predictions=accurate_predictions,
            overall_accuracy=overall_accuracy,
            accuracy_by_confidence=accuracy_by_confidence,
            avg_probability_when_replied=avg_prob_replied,
            avg_probability_when_not_replied=avg_prob_not_replied
        )

    def run_full_training(self, candidate_id: int) -> TrainingResult:
        """
        Run the complete training pipeline for a candidate.

        1. Update prediction accuracy
        2. Collect training data
        3. Train model

        Args:
            candidate_id: The candidate's ID

        Returns:
            TrainingResult
        """
        logger.info(f"[MLTraining] Running full training for candidate {candidate_id}")

        # Step 1: Update prediction accuracy
        self.update_prediction_accuracy(candidate_id)

        # Step 2: Collect training data
        X, y = self.collect_training_data(candidate_id)

        # Step 3: Train model
        result = self.train_model(candidate_id, X, y)

        return result

    def update_send_time_analytics(self, candidate_id: int) -> int:
        """
        Update SendTimeAnalytics aggregations from recent email data.

        Called to refresh the aggregated send time statistics used
        by SendTimeMLOptimizer.

        Args:
            candidate_id: The candidate's ID

        Returns:
            Number of analytics records updated
        """
        logger.info(f"[MLTraining] Updating send time analytics for candidate {candidate_id}")

        # Get all sent emails for this candidate
        emails_query = self.db.query(
            FollowUpEmail.sent_at,
            FollowUpEmail.opened_at,
            FollowUpEmail.clicked_at,
            FollowUpEmail.replied_at,
            FollowUpCampaign.application_id
        ).join(
            FollowUpCampaign,
            FollowUpEmail.campaign_id == FollowUpCampaign.id
        ).filter(
            FollowUpCampaign.candidate_id == candidate_id,
            FollowUpEmail.sent_at.isnot(None)
        )

        emails = emails_query.all()

        if not emails:
            return 0

        # Group by domain, day, hour
        from app.models.application import Application

        analytics_data: Dict[Tuple[str, int, int], Dict[str, int]] = {}

        for email in emails:
            if not email.sent_at:
                continue

            # Get recipient domain
            application = self.db.query(Application).filter(
                Application.id == email.application_id
            ).first()

            if not application or not application.contact_email:
                continue

            domain = application.contact_email.split("@")[-1] if "@" in application.contact_email else ""
            if not domain:
                continue

            day = email.sent_at.weekday()  # 0=Monday
            hour = email.sent_at.hour

            key = (domain, day, hour)

            if key not in analytics_data:
                analytics_data[key] = {
                    "sent": 0,
                    "opened": 0,
                    "clicked": 0,
                    "replied": 0
                }

            analytics_data[key]["sent"] += 1
            if email.opened_at:
                analytics_data[key]["opened"] += 1
            if email.clicked_at:
                analytics_data[key]["clicked"] += 1
            if email.replied_at:
                analytics_data[key]["replied"] += 1

        # Update or create analytics records
        updated_count = 0

        for (domain, day, hour), data in analytics_data.items():
            try:
                analytic = self.db.query(SendTimeAnalytics).filter(
                    SendTimeAnalytics.candidate_id == candidate_id,
                    SendTimeAnalytics.recipient_domain == domain,
                    SendTimeAnalytics.day_of_week == day,
                    SendTimeAnalytics.hour_of_day == hour
                ).first()

                if not analytic:
                    analytic = SendTimeAnalytics(
                        candidate_id=candidate_id,
                        recipient_domain=domain,
                        day_of_week=day,
                        hour_of_day=hour
                    )
                    self.db.add(analytic)

                analytic.emails_sent = data["sent"]
                analytic.emails_opened = data["opened"]
                analytic.emails_clicked = data["clicked"]
                analytic.emails_replied = data["replied"]
                analytic.calculate_rates()
                analytic.last_email_at = datetime.now(timezone.utc)

                updated_count += 1

            except Exception as e:
                logger.warning(f"[MLTraining] Error updating analytics for {domain}/{day}/{hour}: {e}")
                continue

        self.db.commit()

        logger.info(
            f"[MLTraining] Updated {updated_count} send time analytics for candidate {candidate_id}"
        )

        return updated_count


async def run_daily_training(db: Session) -> Dict[str, Any]:
    """
    Daily training job to run for all active candidates.

    Called by scheduler at 05:00 UTC.

    Args:
        db: Database session

    Returns:
        Summary of training results
    """
    from app.models.candidate import Candidate

    logger.info("[MLTraining] Starting daily training job")

    pipeline = MLTrainingPipeline(db)

    # Get all candidates with completed campaigns
    candidates = db.query(Candidate.id).join(
        FollowUpCampaign,
        FollowUpCampaign.candidate_id == Candidate.id
    ).filter(
        FollowUpCampaign.status.in_([
            CampaignStatus.COMPLETED,
            CampaignStatus.REPLIED
        ])
    ).distinct().all()

    results = {
        "total_candidates": len(candidates),
        "successful_training": 0,
        "failed_training": 0,
        "skipped_insufficient_data": 0,
        "training_results": []
    }

    for (candidate_id,) in candidates:
        try:
            result = pipeline.run_full_training(candidate_id)

            if result.success:
                results["successful_training"] += 1
            elif "Insufficient" in (result.error_message or ""):
                results["skipped_insufficient_data"] += 1
            else:
                results["failed_training"] += 1

            results["training_results"].append({
                "candidate_id": candidate_id,
                "success": result.success,
                "samples": result.samples_used,
                "accuracy": result.validation_accuracy,
                "error": result.error_message
            })

            # Also update send time analytics
            pipeline.update_send_time_analytics(candidate_id)

        except Exception as e:
            logger.error(f"[MLTraining] Error training for candidate {candidate_id}: {e}")
            results["failed_training"] += 1
            results["training_results"].append({
                "candidate_id": candidate_id,
                "success": False,
                "error": str(e)
            })

    logger.info(
        f"[MLTraining] Daily training complete: "
        f"{results['successful_training']} successful, "
        f"{results['failed_training']} failed, "
        f"{results['skipped_insufficient_data']} skipped"
    )

    return results
