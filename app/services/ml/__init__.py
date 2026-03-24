"""
ML Intelligence Services for Follow-Up System

ULTRA Follow-Up System V2.0 - Sprint 2

Provides:
- Pure Python Gradient Boosting for reply prediction
- Feature extraction from campaign/email data
- Send time optimization with ML learning
- Training pipeline for model updates
"""

from app.services.ml.gradient_boosting import (
    GradientBoostingClassifier,
    DecisionNode,
)
from app.services.ml.feature_extractor import (
    FeatureExtractor,
    FollowUpFeatures,
)
from app.services.ml.reply_predictor import (
    ReplyPredictor,
    ReplyPredictionResult,
)
from app.services.ml.send_time_ml import (
    SendTimeMLOptimizer,
    MLSendTimeResult,
)
from app.services.ml.training_pipeline import (
    MLTrainingPipeline,
    TrainingResult,
    AccuracyStats,
    run_daily_training,
)

__all__ = [
    # Gradient Boosting
    "GradientBoostingClassifier",
    "DecisionNode",
    # Feature Extraction
    "FeatureExtractor",
    "FollowUpFeatures",
    # Reply Prediction
    "ReplyPredictor",
    "ReplyPredictionResult",
    # Send Time ML
    "SendTimeMLOptimizer",
    "MLSendTimeResult",
    # Training
    "MLTrainingPipeline",
    "TrainingResult",
    "AccuracyStats",
    "run_daily_training",
]
