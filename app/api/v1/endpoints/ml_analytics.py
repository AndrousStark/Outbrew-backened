"""
ML Analytics API - ULTRA Follow-Up System V2.0 Sprint 2

Endpoints for ML-powered insights:
- Reply probability prediction
- Send time optimization with heatmap
- Prediction accuracy tracking
- ML insights dashboard
"""

import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Body
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from datetime import datetime

from app.api.dependencies import get_db, get_current_candidate
from app.models.candidate import Candidate
from app.models.follow_up import FollowUpCampaign, CampaignStatus
from app.models.follow_up_ml import PredictionConfidence

logger = logging.getLogger(__name__)

router = APIRouter()


# ============= PYDANTIC SCHEMAS =============

class PredictReplyRequest(BaseModel):
    """Request body for reply prediction"""
    campaign_id: int = Field(
        ...,
        description="Campaign ID to predict reply probability for"
    )
    store_prediction: bool = Field(
        default=True,
        description="Whether to store the prediction in database"
    )


class PredictBatchRequest(BaseModel):
    """Request body for batch prediction"""
    campaign_ids: Optional[List[int]] = Field(
        default=None,
        description="Specific campaign IDs to predict (None = all active)"
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=200,
        description="Maximum campaigns to include"
    )


class OptimalSendTimeRequest(BaseModel):
    """Request body for optimal send time"""
    recipient_domain: Optional[str] = Field(
        default=None,
        description="Recipient email domain for domain-specific optimization"
    )
    recipient_industry: Optional[str] = Field(
        default=None,
        description="Recipient industry for industry defaults"
    )
    recipient_timezone: str = Field(
        default="UTC",
        description="Recipient timezone"
    )


class TrainModelRequest(BaseModel):
    """Request body for manual model training"""
    lookback_days: int = Field(
        default=90,
        ge=30,
        le=365,
        description="Days of data to use for training"
    )


class ReplyPredictionResponse(BaseModel):
    """Reply prediction response"""
    campaign_id: int
    probability: float
    probability_percent: str
    confidence: str
    priority_score: int
    recommended_action: str
    top_factors: Dict[str, float]
    is_ml_prediction: bool
    model_version: str


class SendTimeResponse(BaseModel):
    """Send time optimization response"""
    recommended_day: int
    recommended_day_name: str
    recommended_hour: int
    confidence: str
    expected_boost: float
    data_source: str
    sample_size: int


class HeatmapResponse(BaseModel):
    """Send time heatmap response"""
    heatmap: Dict[str, Dict[str, float]]
    best_day: int
    best_day_name: str
    best_hour: int
    confidence: str
    data_source: str
    sample_size: int
    expected_boost: float


class AccuracyStatsResponse(BaseModel):
    """Prediction accuracy statistics response"""
    total_predictions: int
    evaluated_predictions: int
    accurate_predictions: int
    overall_accuracy: float
    overall_accuracy_percent: str
    accuracy_by_confidence: Dict[str, Dict[str, Any]]
    avg_probability_when_replied: float
    avg_probability_when_not_replied: float


class MLInsightsResponse(BaseModel):
    """Unified ML insights dashboard response"""
    accuracy_stats: Dict[str, Any]
    top_campaigns: List[Dict[str, Any]]
    send_time_recommendation: Dict[str, Any]
    model_status: Dict[str, Any]


# ============= ENDPOINTS =============

@router.post("/predict-reply", response_model=ReplyPredictionResponse)
async def predict_reply(
    request: PredictReplyRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Predict reply probability for a single campaign.

    Returns probability (0-100%), confidence level, and recommended action.
    """
    from app.services.ml.reply_predictor import ReplyPredictor

    # Get campaign
    campaign = db.query(FollowUpCampaign).filter(
        FollowUpCampaign.id == request.campaign_id,
        FollowUpCampaign.candidate_id == current_candidate.id
    ).first()

    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    try:
        predictor = ReplyPredictor(db)
        result = predictor.predict(campaign)

        # Store prediction if requested
        if request.store_prediction:
            predictor.store_prediction(campaign, result)

        return ReplyPredictionResponse(
            campaign_id=campaign.id,
            probability=round(result.probability, 4),
            probability_percent=f"{result.probability * 100:.1f}%",
            confidence=result.confidence.value,
            priority_score=result.priority_score,
            recommended_action=result.recommended_action,
            top_factors=result.top_factors,
            is_ml_prediction=result.is_ml_prediction,
            model_version=result.model_version
        )

    except Exception as e:
        logger.error(f"[MLAnalytics] Error predicting reply: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict-batch")
async def predict_batch(
    request: PredictBatchRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Predict reply probability for multiple campaigns.

    Returns campaigns sorted by priority score (highest first).
    """
    from app.services.ml.reply_predictor import ReplyPredictor

    # Get campaigns
    query = db.query(FollowUpCampaign).filter(
        FollowUpCampaign.candidate_id == current_candidate.id
    )

    if request.campaign_ids:
        query = query.filter(FollowUpCampaign.id.in_(request.campaign_ids))
    else:
        # Default to active campaigns
        query = query.filter(
            FollowUpCampaign.status.in_([
                CampaignStatus.ACTIVE,
                CampaignStatus.PENDING_APPROVAL,
                CampaignStatus.PAUSED
            ])
        )

    campaigns = query.limit(request.limit).all()

    if not campaigns:
        return {"campaigns": [], "total": 0}

    try:
        predictor = ReplyPredictor(db)
        results = predictor.predict_batch(campaigns)

        response_data = []
        for campaign_id, result in results:
            campaign = next((c for c in campaigns if c.id == campaign_id), None)
            if campaign:
                response_data.append({
                    "campaign_id": campaign_id,
                    "application_id": campaign.application_id,
                    "probability": round(result.probability, 4),
                    "probability_percent": f"{result.probability * 100:.1f}%",
                    "confidence": result.confidence.value,
                    "priority_score": result.priority_score,
                    "recommended_action": result.recommended_action,
                    "status": campaign.status.value,
                    "current_step": campaign.current_step,
                    "next_send_date": campaign.next_send_date.isoformat() if campaign.next_send_date else None
                })

        return {
            "campaigns": response_data,
            "total": len(response_data)
        }

    except Exception as e:
        logger.error(f"[MLAnalytics] Error in batch prediction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimal-send-time", response_model=SendTimeResponse)
async def get_optimal_send_time(
    request: OptimalSendTimeRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get ML-optimized send time recommendation.

    Uses candidate's historical data when available,
    falls back to industry defaults.
    """
    from app.services.ml.send_time_ml import SendTimeMLOptimizer

    DAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    try:
        optimizer = SendTimeMLOptimizer(db)
        result = optimizer.get_optimal_send_time(
            candidate_id=current_candidate.id,
            recipient_domain=request.recipient_domain,
            recipient_industry=request.recipient_industry,
            recipient_timezone=request.recipient_timezone
        )

        return SendTimeResponse(
            recommended_day=result.recommended_day,
            recommended_day_name=DAY_NAMES[result.recommended_day],
            recommended_hour=result.recommended_hour,
            confidence=result.confidence.value,
            expected_boost=round(result.expected_open_rate_boost, 1),
            data_source=result.data_source,
            sample_size=result.sample_size
        )

    except Exception as e:
        logger.error(f"[MLAnalytics] Error getting optimal send time: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/send-time-heatmap", response_model=HeatmapResponse)
async def get_send_time_heatmap(
    recipient_domain: Optional[str] = Query(None, description="Filter by recipient domain"),
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get day/hour engagement heatmap for visualization.

    Returns a 7x24 grid of engagement scores for send time optimization UI.
    """
    from app.services.ml.send_time_ml import SendTimeMLOptimizer

    try:
        optimizer = SendTimeMLOptimizer(db)
        data = optimizer.get_heatmap_data(
            candidate_id=current_candidate.id,
            recipient_domain=recipient_domain
        )

        return HeatmapResponse(
            heatmap=data["heatmap"],
            best_day=data["best_day"],
            best_day_name=data["best_day_name"],
            best_hour=data["best_hour"],
            confidence=data["confidence"],
            data_source=data["data_source"],
            sample_size=data["sample_size"],
            expected_boost=data["expected_boost"]
        )

    except Exception as e:
        logger.error(f"[MLAnalytics] Error getting heatmap: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/accuracy-stats", response_model=AccuracyStatsResponse)
async def get_accuracy_stats(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get prediction accuracy statistics.

    Shows overall accuracy and breakdown by confidence level.
    """
    from app.services.ml.training_pipeline import MLTrainingPipeline

    try:
        pipeline = MLTrainingPipeline(db)
        stats = pipeline.get_accuracy_stats(current_candidate.id)

        return AccuracyStatsResponse(
            total_predictions=stats.total_predictions,
            evaluated_predictions=stats.evaluated_predictions,
            accurate_predictions=stats.accurate_predictions,
            overall_accuracy=round(stats.overall_accuracy, 4),
            overall_accuracy_percent=f"{stats.overall_accuracy * 100:.1f}%",
            accuracy_by_confidence=stats.accuracy_by_confidence,
            avg_probability_when_replied=round(stats.avg_probability_when_replied, 4),
            avg_probability_when_not_replied=round(stats.avg_probability_when_not_replied, 4)
        )

    except Exception as e:
        logger.error(f"[MLAnalytics] Error getting accuracy stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ml-insights", response_model=MLInsightsResponse)
async def get_ml_insights(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get unified ML insights for dashboard.

    Combines accuracy stats, top campaigns, and send time recommendations
    into a single dashboard response.
    """
    from app.services.ml.reply_predictor import ReplyPredictor
    from app.services.ml.send_time_ml import SendTimeMLOptimizer
    from app.services.ml.training_pipeline import MLTrainingPipeline
    import os

    DAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    try:
        # 1. Get accuracy stats
        pipeline = MLTrainingPipeline(db)
        accuracy_stats = pipeline.get_accuracy_stats(current_candidate.id)

        # 2. Get top campaigns by priority
        predictor = ReplyPredictor(db)
        active_campaigns = db.query(FollowUpCampaign).filter(
            FollowUpCampaign.candidate_id == current_candidate.id,
            FollowUpCampaign.status.in_([
                CampaignStatus.ACTIVE,
                CampaignStatus.PENDING_APPROVAL
            ])
        ).limit(10).all()

        top_campaigns = []
        if active_campaigns:
            results = predictor.predict_batch(active_campaigns)
            for campaign_id, result in results[:5]:
                campaign = next((c for c in active_campaigns if c.id == campaign_id), None)
                if campaign:
                    top_campaigns.append({
                        "campaign_id": campaign_id,
                        "application_id": campaign.application_id,
                        "probability_percent": f"{result.probability * 100:.1f}%",
                        "priority_score": result.priority_score,
                        "confidence": result.confidence.value,
                        "recommended_action": result.recommended_action
                    })

        # 3. Get send time recommendation
        optimizer = SendTimeMLOptimizer(db)
        send_time = optimizer.get_optimal_send_time(
            candidate_id=current_candidate.id
        )

        # 4. Check model status
        model_path = os.path.join(
            pipeline.MODEL_STORAGE_DIR,
            f"reply_model_{current_candidate.id}.json"
        )
        model_exists = os.path.exists(model_path)
        model_modified = None
        if model_exists:
            model_modified = datetime.fromtimestamp(
                os.path.getmtime(model_path)
            ).isoformat()

        return MLInsightsResponse(
            accuracy_stats={
                "total_predictions": accuracy_stats.total_predictions,
                "evaluated": accuracy_stats.evaluated_predictions,
                "accurate": accuracy_stats.accurate_predictions,
                "overall_accuracy_percent": f"{accuracy_stats.overall_accuracy * 100:.1f}%",
                "by_confidence": accuracy_stats.accuracy_by_confidence
            },
            top_campaigns=top_campaigns,
            send_time_recommendation={
                "day": send_time.recommended_day,
                "day_name": DAY_NAMES[send_time.recommended_day],
                "hour": send_time.recommended_hour,
                "time_display": f"{DAY_NAMES[send_time.recommended_day]} {send_time.recommended_hour:02d}:00",
                "confidence": send_time.confidence.value,
                "expected_boost": f"+{send_time.expected_open_rate_boost:.1f}%",
                "data_source": send_time.data_source,
                "sample_size": send_time.sample_size
            },
            model_status={
                "trained": model_exists,
                "last_trained": model_modified,
                "version": predictor.MODEL_VERSION,
                "using_ml": model_exists,
                "fallback_mode": "heuristic" if not model_exists else None
            }
        )

    except Exception as e:
        logger.error(f"[MLAnalytics] Error getting ML insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/train-model")
async def trigger_model_training(
    request: TrainModelRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Manually trigger model training for current candidate.

    This is typically run automatically by the daily scheduler,
    but can be triggered manually.
    """
    from app.services.ml.training_pipeline import MLTrainingPipeline

    try:
        pipeline = MLTrainingPipeline(db)

        # Collect training data
        X, y = pipeline.collect_training_data(
            candidate_id=current_candidate.id,
            lookback_days=request.lookback_days
        )

        if len(X) < pipeline.MIN_TRAINING_SAMPLES:
            return {
                "success": False,
                "message": f"Insufficient training data: {len(X)} samples (need {pipeline.MIN_TRAINING_SAMPLES})",
                "samples_collected": len(X),
                "positive_samples": sum(y) if y else 0,
                "negative_samples": len(y) - sum(y) if y else 0
            }

        # Train model
        result = pipeline.train_model(current_candidate.id, X, y)

        return {
            "success": result.success,
            "message": "Model trained successfully" if result.success else result.error_message,
            "samples_used": result.samples_used,
            "positive_samples": result.positive_samples,
            "negative_samples": result.negative_samples,
            "validation_accuracy": round(result.validation_accuracy, 4),
            "validation_precision": round(result.validation_precision, 4),
            "validation_recall": round(result.validation_recall, 4),
            "training_time_ms": result.training_time_ms,
            "model_version": result.model_version
        }

    except Exception as e:
        logger.error(f"[MLAnalytics] Error training model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update-accuracy")
async def update_prediction_accuracy(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Update prediction accuracy based on campaign outcomes.

    Backfills accuracy data for predictions where the campaign
    has completed and outcome is known.
    """
    from app.services.ml.training_pipeline import MLTrainingPipeline

    try:
        pipeline = MLTrainingPipeline(db)
        updated_count = pipeline.update_prediction_accuracy(current_candidate.id)

        return {
            "success": True,
            "predictions_updated": updated_count
        }

    except Exception as e:
        logger.error(f"[MLAnalytics] Error updating accuracy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update-send-time-analytics")
async def update_send_time_analytics(
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Refresh send time analytics aggregations.

    Updates the day/hour/domain aggregations used for
    send time optimization.
    """
    from app.services.ml.training_pipeline import MLTrainingPipeline

    try:
        pipeline = MLTrainingPipeline(db)
        updated_count = pipeline.update_send_time_analytics(current_candidate.id)

        return {
            "success": True,
            "analytics_records_updated": updated_count
        }

    except Exception as e:
        logger.error(f"[MLAnalytics] Error updating send time analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))
