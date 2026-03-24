"""
Warmup Advanced API Endpoints - PHASE 4 GOD TIER EDITION

Advanced API endpoints for the ML-powered adaptive warmup system.
Features:
- ML model management and predictions
- Real-time adaptive control
- Advanced optimization
- System health and monitoring
- Admin controls

Author: Metaminds AI
Version: 4.0.0 - ULTRA GOD TIER
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.auth import get_current_candidate, get_current_candidate_from_sse_ticket
from app.models.candidate import Candidate
from app.services.warmup_ml_engine import (
    get_warmup_ml_engine,
    State,
    ActionType,
)
from app.services.warmup_optimizer import (
    get_warmup_optimizer,
)
from app.services.warmup_adaptive_engine import (
    get_warmup_adaptive_engine,
    SignalType,
    Severity,
    HealthMetrics,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Warmup Advanced - Phase 4"])


# ============================================================================
# PYDANTIC SCHEMAS
# ============================================================================

class MLPredictionRequest(BaseModel):
    """Request for ML prediction"""
    context: Dict[str, Any] = Field(..., description="Current context/state")
    prediction_type: str = Field(default="action", description="action, engagement, deliverability, anomaly")


class MLPredictionResponse(BaseModel):
    """ML prediction response"""
    prediction_type: str
    result: Dict[str, Any]
    confidence: float
    computation_time_ms: float
    model_version: str


class SendDecisionRequest(BaseModel):
    """Request for send decision"""
    target_email: str
    context: Dict[str, Any] = Field(default_factory=dict)


class SendDecisionResponse(BaseModel):
    """Send decision response"""
    decision: str
    can_send: bool
    ml_action: Optional[str] = None
    ml_confidence: Optional[float] = None
    optimization: Dict[str, Any]
    throttle: Dict[str, Any]
    active_fallbacks: List[str]
    system_state: str
    computation_time_ms: float


class SignalRequest(BaseModel):
    """Request to emit a signal"""
    signal_type: str
    severity: str = "info"
    data: Dict[str, Any] = Field(default_factory=dict)


class HealthMetricsRequest(BaseModel):
    """Health metrics update request"""
    open_rate_7d: float = Field(..., ge=0, le=1)
    reply_rate_7d: float = Field(..., ge=0, le=1)
    bounce_rate_7d: float = Field(..., ge=0, le=1)
    spam_rate_7d: float = Field(..., ge=0, le=1)
    blacklist_count: int = Field(default=0, ge=0)
    active_accounts: int = Field(default=1, ge=0)
    volume_utilization: float = Field(default=0.5, ge=0, le=1)


class ManualOverrideRequest(BaseModel):
    """Manual override request"""
    action: str = Field(..., description="pause_all, resume_all, reset_throttle, clear_fallbacks, throttle_account")
    parameters: Dict[str, Any] = Field(default_factory=dict)


class ContentOptimizationRequest(BaseModel):
    """Request for content optimization"""
    subject_variations: List[str]
    body_template: str
    recipient_context: Dict[str, Any] = Field(default_factory=dict)


class TimingOptimizationRequest(BaseModel):
    """Request for timing optimization"""
    target_email: str
    send_window_hours: int = Field(default=24, ge=1, le=168)
    preferred_hours: List[int] = Field(default_factory=list)


# ============================================================================
# ML PREDICTION ENDPOINTS
# ============================================================================

@router.post("/ml/predict", response_model=MLPredictionResponse)
def get_ml_prediction(
    request: MLPredictionRequest,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get ML model prediction.

    Prediction types:
    - **action**: Optimal action recommendation (RL-based)
    - **engagement**: Predicted engagement metrics (LSTM-based)
    - **deliverability**: Deliverability score prediction (GBM-based)
    - **anomaly**: Anomaly detection (Isolation Forest)
    """
    import time
    start_time = time.time()

    ml_engine = get_warmup_ml_engine()
    prediction_type = request.prediction_type

    result = {}
    confidence = 0.0
    model_version = "4.0.0"

    try:
        if prediction_type == "action":
            # Build state from context
            state = State(
                hour_of_day=request.context.get("hour", datetime.now(timezone.utc).hour),
                day_of_week=request.context.get("day", datetime.now(timezone.utc).weekday()),
                emails_sent_today=request.context.get("emails_sent", 0),
                emails_remaining=request.context.get("emails_remaining", 50),
                last_open_rate=request.context.get("open_rate", 0.5),
                last_reply_rate=request.context.get("reply_rate", 0.3),
                spam_rate_24h=request.context.get("spam_rate", 0.01),
                bounce_rate_24h=request.context.get("bounce_rate", 0.02),
                provider_score=request.context.get("provider_score", 0.8),
                account_age_days=request.context.get("account_age", 30),
                warmup_day=request.context.get("warmup_day", 1),
                consecutive_successes=request.context.get("consecutive_successes", 0),
                consecutive_failures=request.context.get("consecutive_failures", 0),
            )

            action, details = ml_engine.get_optimal_action(state)
            result = {
                "recommended_action": action.value,
                "q_values": details["q_values"],
                "risk_level": details["risk_level"],
                "anomaly_score": details["anomaly_score"],
                "timing_slot": details["timing_slot"],
            }
            confidence = details["confidence"]
            model_version = details["model_version"]

        elif prediction_type == "engagement":
            history = request.context.get("history", [])
            prediction = ml_engine.predict_engagement(history)
            result = {
                "predicted_open_rate": prediction.factors.get("predicted_open_rate", 0),
                "predicted_reply_rate": prediction.factors.get("predicted_reply_rate", 0),
                "predicted_click_rate": prediction.factors.get("predicted_click_rate", 0),
                "predicted_bounce_rate": prediction.factors.get("predicted_bounce_rate", 0),
            }
            confidence = prediction.confidence
            model_version = prediction.model_version

        elif prediction_type == "deliverability":
            features = request.context
            prediction = ml_engine.predict_deliverability(features)
            result = {
                "deliverability_score": prediction.value,
                "factor_breakdown": prediction.factors,
            }
            confidence = prediction.confidence
            model_version = prediction.model_version

        elif prediction_type == "anomaly":
            metrics = request.context
            is_anomaly, score, issues = ml_engine.detect_anomaly(metrics)
            result = {
                "is_anomaly": is_anomaly,
                "anomaly_score": score,
                "detected_issues": issues,
            }
            confidence = 0.85 if not is_anomaly else 0.9

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown prediction type: {prediction_type}"
            )

    except Exception as e:
        logger.error(f"[WarmupAdvanced] ML prediction error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Prediction failed"
        )

    computation_time = (time.time() - start_time) * 1000

    return MLPredictionResponse(
        prediction_type=prediction_type,
        result=result,
        confidence=confidence,
        computation_time_ms=computation_time,
        model_version=model_version,
    )


@router.get("/ml/training-stats")
def get_ml_training_stats(
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get ML model training statistics"""
    ml_engine = get_warmup_ml_engine()
    return {
        "status": "ok",
        "stats": ml_engine.get_training_stats(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/ml/bandit/update")
def update_bandit_feedback(
    variation: int = Query(..., ge=0, le=4),
    engagement_score: float = Query(..., ge=0, le=1),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Update multi-armed bandit with content feedback"""
    ml_engine = get_warmup_ml_engine()
    ml_engine.update_content_feedback(variation, engagement_score)

    return {
        "success": True,
        "variation": variation,
        "engagement_score": engagement_score,
        "updated_stats": ml_engine.content_bandit.get_statistics(),
    }


@router.get("/ml/bandit/recommend")
def get_bandit_recommendation(
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get content variation recommendation from bandit"""
    ml_engine = get_warmup_ml_engine()
    variation, stats = ml_engine.get_content_variation({})

    return {
        "recommended_variation": variation,
        "bandit_stats": stats,
    }


# ============================================================================
# ADAPTIVE ENGINE ENDPOINTS
# ============================================================================

@router.post("/adaptive/send-decision", response_model=SendDecisionResponse)
def get_send_decision(
    request: SendDecisionRequest,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get comprehensive send decision from adaptive engine.

    Combines ML predictions, optimization, and system state to provide
    the optimal decision for sending an email.
    """
    engine = get_warmup_adaptive_engine()

    decision = engine.get_send_decision(
        account_id=str(current_user.id),
        target_email=request.target_email,
        context=request.context,
    )

    return SendDecisionResponse(
        decision=decision["decision"],
        can_send=decision["can_send"],
        ml_action=decision.get("ml_action"),
        ml_confidence=decision.get("ml_confidence"),
        optimization=decision.get("optimization", {}),
        throttle=decision.get("throttle", {}),
        active_fallbacks=decision.get("active_fallbacks", []),
        system_state=decision.get("system_state", "unknown"),
        computation_time_ms=decision.get("computation_time_ms", 0),
    )


@router.post("/adaptive/emit-signal")
def emit_signal(
    request: SignalRequest,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Emit a signal to the adaptive engine.

    Signal types:
    - email_sent, email_opened, email_replied, email_bounced
    - spam_detected, spam_rescued
    - blacklist_detected, rate_limit_hit
    - placement_test, health_check
    """
    engine = get_warmup_adaptive_engine()

    try:
        signal_type = SignalType(request.signal_type)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid signal type: {request.signal_type}"
        )

    try:
        severity = Severity[request.severity.upper()]
    except KeyError:
        severity = Severity.INFO

    engine.emit_signal(
        signal_type=signal_type,
        account_id=str(current_user.id),
        data=request.data,
        severity=severity,
    )

    # Process signals immediately
    processed = engine.process_signals()

    return {
        "success": True,
        "signal_type": signal_type.value,
        "severity": severity.name,
        "signals_processed": processed,
    }


@router.post("/adaptive/health-update")
def update_health_metrics(
    request: HealthMetricsRequest,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Update system health metrics"""
    engine = get_warmup_adaptive_engine()

    metrics = HealthMetrics(
        overall_score=0,  # Will be calculated
        open_rate_7d=request.open_rate_7d,
        reply_rate_7d=request.reply_rate_7d,
        bounce_rate_7d=request.bounce_rate_7d,
        spam_rate_7d=request.spam_rate_7d,
        blacklist_count=request.blacklist_count,
        active_accounts=request.active_accounts,
        volume_utilization=request.volume_utilization,
    )

    engine.update_health_metrics(metrics)

    return {
        "success": True,
        "reputation": engine.reputation_guardian.get_status(),
        "system_state": engine.system_state.value,
        "throttle": engine.throttle_controller.get_status(),
    }


@router.get("/adaptive/dashboard")
def get_adaptive_dashboard(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get comprehensive adaptive engine dashboard data.

    Includes:
    - System state and throttle info
    - Reputation status and alerts
    - Active fallbacks
    - ML engine statistics
    - Optimizer statistics
    - Signal processing stats
    - Recent actions
    """
    engine = get_warmup_adaptive_engine()
    return engine.get_dashboard_data()


@router.post("/adaptive/override")
def manual_override(
    request: ManualOverrideRequest,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Manual admin override.

    Actions:
    - **pause_all**: Pause all sending
    - **resume_all**: Resume normal operation
    - **reset_throttle**: Reset throttle to normal
    - **clear_fallbacks**: Clear all active fallbacks
    - **throttle_account**: Throttle specific account
    """
    engine = get_warmup_adaptive_engine()

    result = engine.manual_override(request.action, request.parameters)

    if not result.get("success"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result.get("message", "Override failed")
        )

    return result


# ============================================================================
# OPTIMIZATION ENDPOINTS
# ============================================================================

@router.get("/optimizer/statistics")
def get_optimizer_statistics(
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get optimizer statistics"""
    optimizer = get_warmup_optimizer()
    return optimizer.get_statistics()


@router.get("/optimizer/patterns")
def analyze_patterns(
    current_user: Candidate = Depends(get_current_candidate)
):
    """Analyze engagement patterns"""
    optimizer = get_warmup_optimizer()
    return optimizer.analyze_patterns()


@router.post("/optimizer/timing", response_model=dict)
def get_timing_optimization(
    request: TimingOptimizationRequest,
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get optimal send timing for target email"""
    optimizer = get_warmup_optimizer()

    provider = optimizer.provider_optimizer.get_provider_from_email(request.target_email)
    optimal_time, confidence = optimizer.provider_optimizer.get_next_optimal_window(provider)
    hourly_dist = optimizer.volume_optimizer.get_hourly_distribution()

    # Get best hours from distribution
    best_hours = sorted(hourly_dist.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "provider": provider,
        "optimal_time": optimal_time.isoformat() if optimal_time else None,
        "confidence": confidence,
        "hourly_distribution": hourly_dist,
        "best_hours": [{"hour": h, "volume": v} for h, v in best_hours],
        "preferred_hours_available": [h for h in request.preferred_hours if hourly_dist.get(h, 0) > 0],
    }


@router.get("/optimizer/volume-simulation")
def simulate_volume_ramp(
    days: int = Query(30, ge=7, le=90),
    profile: str = Query("balanced", regex="^(conservative|balanced|aggressive|enterprise)$"),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Simulate volume ramp progression"""
    from app.services.warmup_optimizer import VolumeOptimizer

    volume_opt = VolumeOptimizer(profile=profile)
    simulation = volume_opt.simulate_ramp(days)

    return {
        "profile": profile,
        "simulation_days": days,
        "initial_volume": volume_opt.profile_config["initial"],
        "max_volume": volume_opt.profile_config["max"],
        "ramp_rate": volume_opt.profile_config["rate"],
        "daily_projections": simulation,
        "final_projected_volume": simulation[-1]["volume"] if simulation else 0,
    }


@router.post("/optimizer/record-engagement")
def record_engagement(
    account_id: str,
    target_email: str,
    opened: bool = False,
    replied: bool = False,
    was_spam: bool = False,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Record engagement for optimizer learning"""
    from app.models.warmup_pool import WarmupPoolMember

    # Verify the account belongs to the current user
    member = db.query(WarmupPoolMember).filter(
        WarmupPoolMember.candidate_id == current_user.id
    ).first()

    if not member:
        raise HTTPException(status_code=403, detail="No warmup pool membership found for your account")

    # Scope account_id to current user to prevent cross-user data corruption
    scoped_account_id = f"user_{current_user.id}_{account_id}"

    optimizer = get_warmup_optimizer()

    optimizer.record_engagement(
        account_id=scoped_account_id,
        target_email=target_email,
        opened=opened,
        replied=replied,
        was_spam=was_spam,
    )

    return {
        "success": True,
        "recorded": {
            "account_id": account_id,
            "target_email": target_email,
            "opened": opened,
            "replied": replied,
            "was_spam": was_spam,
        },
    }


# ============================================================================
# CONTENT OPTIMIZATION ENDPOINTS
# ============================================================================

@router.post("/content/optimize")
def optimize_content(
    request: ContentOptimizationRequest,
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get AI-optimized content recommendations.

    Uses multi-armed bandit to select best-performing variations
    and provides optimization suggestions.
    """
    ml_engine = get_warmup_ml_engine()

    # Get bandit recommendation
    variation_idx, bandit_stats = ml_engine.get_content_variation(request.recipient_context)

    # Ensure we have enough variations
    if variation_idx >= len(request.subject_variations):
        variation_idx = 0

    selected_subject = request.subject_variations[variation_idx]

    # Calculate expected performance
    expected_values = bandit_stats.get("expected_values", [])
    expected_performance = expected_values[variation_idx] if variation_idx < len(expected_values) else 0.5

    # Provide optimization suggestions
    suggestions = []
    if expected_performance < 0.3:
        suggestions.append("Consider testing new subject line variations")
    if len(request.subject_variations) < 3:
        suggestions.append("Add more subject line variations for better A/B testing")
    if "urgent" in selected_subject.lower() or "important" in selected_subject.lower():
        suggestions.append("Avoid urgency words that may trigger spam filters")

    return {
        "recommended_variation": {
            "index": variation_idx,
            "subject": selected_subject,
        },
        "expected_performance": expected_performance,
        "bandit_stats": bandit_stats,
        "all_variations_performance": [
            {"index": i, "subject": s, "expected_rate": expected_values[i] if i < len(expected_values) else 0.5}
            for i, s in enumerate(request.subject_variations)
        ],
        "suggestions": suggestions,
    }


# ============================================================================
# REAL-TIME SSE ENDPOINTS
# ============================================================================

@router.get("/realtime/adaptive-events")
async def adaptive_events_stream(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate_from_sse_ticket)
):
    """
    Server-Sent Events stream for real-time adaptive engine updates.

    Events include:
    - system_state_change
    - throttle_change
    - fallback_activated
    - alert_generated
    - action_taken
    """
    import asyncio
    import json

    engine = get_warmup_adaptive_engine()

    async def event_generator():
        last_state = None
        last_throttle_level = None

        # Initial state
        yield f"data: {json.dumps({'type': 'connected', 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"

        while True:
            await asyncio.sleep(2)

            # Check for state changes
            current_state = engine.system_state.value
            current_throttle = engine.throttle_controller.current_level

            if current_state != last_state:
                yield f"data: {json.dumps({'type': 'system_state_change', 'old': last_state, 'new': current_state, 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"
                last_state = current_state

            if current_throttle != last_throttle_level:
                yield f"data: {json.dumps({'type': 'throttle_change', 'old': last_throttle_level, 'new': current_throttle, 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"
                last_throttle_level = current_throttle

            # Send heartbeat
            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


# ============================================================================
# MODEL MANAGEMENT ENDPOINTS
# ============================================================================

@router.post("/ml/export")
def export_ml_model(
    current_user: Candidate = Depends(get_current_candidate)
):
    """Export ML model weights for backup/transfer"""
    ml_engine = get_warmup_ml_engine()
    model_data = ml_engine.export_model()

    return {
        "success": True,
        "model_data": model_data,
        "export_timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/ml/import")
def import_ml_model(
    model_data: Dict[str, Any],
    current_user: Candidate = Depends(get_current_candidate)
):
    """Import ML model weights"""
    ml_engine = get_warmup_ml_engine()

    try:
        ml_engine.import_model(model_data)
        return {
            "success": True,
            "imported_version": model_data.get("version", "unknown"),
            "import_timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"[WarmupAdvanced] Model import failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Model import failed"
        )


# ============================================================================
# HEALTH & DIAGNOSTICS ENDPOINTS
# ============================================================================

@router.get("/health")
def get_system_health(
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get overall system health status"""
    engine = get_warmup_adaptive_engine()
    ml_engine = get_warmup_ml_engine()
    optimizer = get_warmup_optimizer()

    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {
            "adaptive_engine": {
                "state": engine.system_state.value,
                "accounts": len(engine.account_states),
            },
            "ml_engine": {
                "replay_buffer": len(ml_engine.replay_buffer),
                "epsilon": ml_engine.dqn.epsilon,
            },
            "optimizer": {
                "time_slots": len(optimizer.time_slots),
                "hourly_samples": len(optimizer.hourly_engagement),
            },
            "throttle": engine.throttle_controller.get_status(),
            "reputation": engine.reputation_guardian.get_status(),
        },
        "version": "4.0.0-ultra-god-tier",
    }


@router.get("/diagnostics")
def run_diagnostics(
    current_user: Candidate = Depends(get_current_candidate)
):
    """Run system diagnostics"""
    import time

    diagnostics = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tests": [],
    }

    # Test ML engine
    try:
        start = time.time()
        ml_engine = get_warmup_ml_engine()
        test_state = State(12, 3, 10, 40, 0.5, 0.3, 0.01, 0.02, 0.8, 30, 15, 3, 0)
        action, _ = ml_engine.get_optimal_action(test_state)
        elapsed = (time.time() - start) * 1000
        diagnostics["tests"].append({
            "name": "ML Engine Prediction",
            "status": "pass",
            "latency_ms": elapsed,
        })
    except Exception as e:
        diagnostics["tests"].append({
            "name": "ML Engine Prediction",
            "status": "fail",
            "error": str(e),
        })

    # Test optimizer
    try:
        start = time.time()
        optimizer = get_warmup_optimizer()
        result = optimizer.get_optimization("test", "test@gmail.com", {})
        elapsed = (time.time() - start) * 1000
        diagnostics["tests"].append({
            "name": "Optimizer Decision",
            "status": "pass",
            "latency_ms": elapsed,
        })
    except Exception as e:
        diagnostics["tests"].append({
            "name": "Optimizer Decision",
            "status": "fail",
            "error": str(e),
        })

    # Test adaptive engine
    try:
        start = time.time()
        engine = get_warmup_adaptive_engine()
        dashboard = engine.get_dashboard_data()
        elapsed = (time.time() - start) * 1000
        diagnostics["tests"].append({
            "name": "Adaptive Engine Dashboard",
            "status": "pass",
            "latency_ms": elapsed,
        })
    except Exception as e:
        diagnostics["tests"].append({
            "name": "Adaptive Engine Dashboard",
            "status": "fail",
            "error": str(e),
        })

    # Overall status
    failed = [t for t in diagnostics["tests"] if t["status"] == "fail"]
    diagnostics["overall_status"] = "healthy" if not failed else "degraded"
    diagnostics["failed_count"] = len(failed)

    return diagnostics
