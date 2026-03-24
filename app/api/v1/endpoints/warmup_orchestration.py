"""
Warmup Orchestration API Endpoints - PHASE 5 GOD TIER EDITION

API endpoints for campaign orchestration, A/B testing, and analytics.

Features:
- Campaign management (CRUD, lifecycle)
- A/B testing management
- Analytics and reporting
- Real-time dashboards

Author: Metaminds AI
Version: 5.0.0 - ULTRA GOD TIER ORCHESTRATION
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.services.warmup_campaign_orchestrator import (
    get_campaign_orchestrator,
    CampaignStatus,
    CampaignGoal,
)
from app.services.warmup_ab_testing import (
    get_ab_testing_engine,
    TestStatus,
    TestType,
    MetricType,
    AllocationStrategy,
)
from app.services.warmup_analytics_engine import (
    get_warmup_analytics_engine,
    TimeGranularity,
    AlertSeverity,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Warmup Orchestration - Phase 5"])


# ============================================================================
# PYDANTIC SCHEMAS
# ============================================================================

# Campaign Schemas
class CreateCampaignRequest(BaseModel):
    """Request to create a campaign"""
    name: Optional[str] = None
    template: str = Field(default="new_domain", description="Template: new_domain, recovery, aggressive")
    target_volume: int = Field(default=100, ge=10, le=1000)
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class CampaignActionRequest(BaseModel):
    """Request for campaign action"""
    action: str = Field(..., description="start, pause, resume, complete, cancel")
    reason: Optional[str] = None
    force: bool = False


class UpdateMetricsRequest(BaseModel):
    """Request to update campaign metrics"""
    emails_sent: int = 0
    emails_received: int = 0
    opens: int = 0
    replies: int = 0
    bounces: int = 0
    spam_reports: int = 0
    inbox_placements: int = 0
    spam_placements: int = 0


# A/B Testing Schemas
class VariantConfig(BaseModel):
    """Variant configuration"""
    name: str
    description: str = ""
    config: Dict[str, Any] = Field(default_factory=dict)
    weight: float = Field(default=0.5, ge=0, le=1)


class CreateTestRequest(BaseModel):
    """Request to create an A/B test"""
    name: str
    test_type: str = Field(..., description="subject_line, send_time, content_style, volume_strategy")
    metric: str = Field(default="open_rate", description="open_rate, reply_rate, inbox_rate")
    variants: List[VariantConfig]
    allocation_strategy: str = Field(default="equal", description="equal, weighted, bandit_epsilon, bandit_thompson, bandit_ucb")
    min_sample_size: int = Field(default=100, ge=10)
    max_sample_size: int = Field(default=10000, le=100000)
    confidence_level: float = Field(default=0.95, ge=0.8, le=0.99)
    early_stopping_enabled: bool = True


class RecordResultRequest(BaseModel):
    """Request to record A/B test result"""
    variant_id: str
    converted: bool
    segment_ids: List[str] = Field(default_factory=list)


# Analytics Schemas
class RecordMetricRequest(BaseModel):
    """Request to record a metric"""
    metric_id: str
    value: float
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BulkMetricsRequest(BaseModel):
    """Request to record multiple metrics"""
    metrics: Dict[str, float]
    timestamp: Optional[datetime] = None


class CreateCohortRequest(BaseModel):
    """Request to create a cohort"""
    name: str
    description: str
    filter_criteria: Dict[str, Any]


# ============================================================================
# CAMPAIGN ENDPOINTS
# ============================================================================

@router.post("/campaigns", summary="Create a new campaign")
async def create_campaign(
    request: CreateCampaignRequest,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Create a new warmup campaign from a template"""
    orchestrator = get_campaign_orchestrator()

    try:
        campaign = orchestrator.create_campaign(
            account_id=str(current_user.id),
            template=request.template,
            name=request.name,
            target_volume=request.target_volume,
            description=request.description or "",
            tags=request.tags,
        )

        return {
            "success": True,
            "campaign": campaign.to_dict(),
            "message": f"Campaign '{campaign.name}' created successfully",
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/campaigns", summary="List all campaigns")
async def list_campaigns(
    status_filter: Optional[str] = Query(None, description="Filter by status"),
    current_user: Candidate = Depends(get_current_candidate),
):
    """List all campaigns for the current account"""
    orchestrator = get_campaign_orchestrator()

    campaigns = orchestrator.get_account_campaigns(str(current_user.id))

    if status_filter:
        try:
            filter_status = CampaignStatus(status_filter)
            campaigns = [c for c in campaigns if c.status == filter_status]
        except ValueError:
            pass

    return {
        "campaigns": [c.to_dict() for c in campaigns],
        "total": len(campaigns),
    }


@router.get("/campaigns/{campaign_id}", summary="Get campaign details")
async def get_campaign(
    campaign_id: str,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get detailed campaign information"""
    orchestrator = get_campaign_orchestrator()

    campaign = orchestrator.get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    if campaign.account_id != str(current_user.id):
        raise HTTPException(status_code=403, detail="Access denied")

    return {
        "campaign": campaign.to_dict(),
        "health": orchestrator.get_campaign_health(campaign_id),
    }


@router.post("/campaigns/{campaign_id}/action", summary="Perform campaign action")
async def campaign_action(
    campaign_id: str,
    request: CampaignActionRequest,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Perform an action on a campaign (start, pause, resume, complete, cancel)"""
    orchestrator = get_campaign_orchestrator()

    campaign = orchestrator.get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    if campaign.account_id != str(current_user.id):
        raise HTTPException(status_code=403, detail="Access denied")

    try:
        if request.action == "start":
            campaign = orchestrator.start_campaign(campaign_id)
        elif request.action == "pause":
            campaign = orchestrator.pause_campaign(campaign_id, request.reason or "")
        elif request.action == "resume":
            campaign = orchestrator.resume_campaign(campaign_id)
        elif request.action == "complete":
            campaign = orchestrator.complete_campaign(campaign_id)
        elif request.action == "cancel":
            campaign = orchestrator.cancel_campaign(campaign_id, request.reason or "")
        elif request.action == "advance_stage":
            campaign = orchestrator.advance_stage(campaign_id, force=request.force)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown action: {request.action}")

        return {
            "success": True,
            "campaign": campaign.to_dict(),
            "message": f"Campaign {request.action} successful",
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/campaigns/{campaign_id}/metrics", summary="Update campaign metrics")
async def update_campaign_metrics(
    campaign_id: str,
    request: UpdateMetricsRequest,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Update campaign metrics with new data"""
    orchestrator = get_campaign_orchestrator()

    campaign = orchestrator.get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    if campaign.account_id != str(current_user.id):
        raise HTTPException(status_code=403, detail="Access denied")

    metrics_delta = {
        "emails_sent": request.emails_sent,
        "emails_received": request.emails_received,
        "opens": request.opens,
        "replies": request.replies,
        "bounces": request.bounces,
        "spam_reports": request.spam_reports,
        "inbox_placements": request.inbox_placements,
        "spam_placements": request.spam_placements,
    }

    campaign = orchestrator.update_metrics(campaign_id, metrics_delta)

    # Process any automation actions
    actions_result = orchestrator.process_action_queue()

    return {
        "success": True,
        "campaign": campaign.to_dict(),
        "actions_triggered": actions_result,
    }


@router.delete("/campaigns/{campaign_id}", summary="Delete a campaign")
async def delete_campaign(
    campaign_id: str,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Delete a campaign"""
    orchestrator = get_campaign_orchestrator()

    campaign = orchestrator.get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    if campaign.account_id != str(current_user.id):
        raise HTTPException(status_code=403, detail="Access denied")

    success = orchestrator.delete_campaign(campaign_id)

    return {
        "success": success,
        "message": "Campaign deleted" if success else "Failed to delete campaign",
    }


@router.get("/campaigns/templates/list", summary="List campaign templates")
async def list_campaign_templates(
    current_user: Candidate = Depends(get_current_candidate),
):
    """List available campaign templates"""
    return {
        "templates": [
            {
                "id": "new_domain",
                "name": "New Domain Warmup",
                "description": "Standard warmup for a new email domain",
                "stages": 5,
                "estimated_duration": "42 days",
                "recommended_for": ["New domains", "First-time senders"],
            },
            {
                "id": "recovery",
                "name": "Reputation Recovery",
                "description": "Recovery campaign for damaged reputation",
                "stages": 4,
                "estimated_duration": "38 days",
                "recommended_for": ["Blacklisted domains", "High spam rates"],
            },
            {
                "id": "aggressive",
                "name": "Aggressive Volume Ramp",
                "description": "Fast volume scaling for experienced senders",
                "stages": 3,
                "estimated_duration": "13 days",
                "recommended_for": ["Experienced senders", "Good reputation"],
            },
        ],
    }


# ============================================================================
# A/B TESTING ENDPOINTS
# ============================================================================

@router.post("/ab-tests", summary="Create a new A/B test")
async def create_ab_test(
    request: CreateTestRequest,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Create a new A/B test"""
    engine = get_ab_testing_engine()

    try:
        test_type = TestType(request.test_type)
        metric = MetricType(request.metric)
        allocation = AllocationStrategy(request.allocation_strategy)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid enum value: {e}")

    variants_config = [
        {
            "name": v.name,
            "description": v.description,
            "config": v.config,
            "weight": v.weight,
        }
        for v in request.variants
    ]

    test = engine.create_test(
        account_id=str(current_user.id),
        name=request.name,
        test_type=test_type,
        metric=metric,
        variants_config=variants_config,
        allocation_strategy=allocation,
        min_sample_size=request.min_sample_size,
        max_sample_size=request.max_sample_size,
        confidence_level=request.confidence_level,
        early_stopping_enabled=request.early_stopping_enabled,
    )

    return {
        "success": True,
        "test": test.to_dict(),
        "message": f"A/B test '{test.name}' created successfully",
    }


@router.get("/ab-tests", summary="List all A/B tests")
async def list_ab_tests(
    status_filter: Optional[str] = Query(None),
    current_user: Candidate = Depends(get_current_candidate),
):
    """List all A/B tests for the current account"""
    engine = get_ab_testing_engine()

    tests = engine.get_account_tests(str(current_user.id))

    if status_filter:
        try:
            filter_status = TestStatus(status_filter)
            tests = [t for t in tests if t.status == filter_status]
        except ValueError:
            pass

    return {
        "tests": [t.to_dict() for t in tests],
        "total": len(tests),
    }


@router.get("/ab-tests/{test_id}", summary="Get A/B test details")
async def get_ab_test(
    test_id: str,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get detailed A/B test information with analysis"""
    engine = get_ab_testing_engine()

    test = engine.get_test(test_id)
    if not test:
        raise HTTPException(status_code=404, detail="Test not found")

    analysis = engine.analyze_test(test_id)

    return {
        "test": test.to_dict(),
        "analysis": analysis,
    }


@router.post("/ab-tests/{test_id}/start", summary="Start an A/B test")
async def start_ab_test(
    test_id: str,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Start an A/B test"""
    engine = get_ab_testing_engine()

    try:
        test = engine.start_test(test_id)
        return {
            "success": True,
            "test": test.to_dict(),
            "message": "Test started",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ab-tests/{test_id}/pause", summary="Pause an A/B test")
async def pause_ab_test(
    test_id: str,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Pause an A/B test"""
    engine = get_ab_testing_engine()

    try:
        test = engine.pause_test(test_id)
        return {
            "success": True,
            "test": test.to_dict(),
            "message": "Test paused",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ab-tests/{test_id}/complete", summary="Complete an A/B test")
async def complete_ab_test(
    test_id: str,
    winner_id: Optional[str] = Query(None),
    current_user: Candidate = Depends(get_current_candidate),
):
    """Complete an A/B test and declare winner"""
    engine = get_ab_testing_engine()

    try:
        test = engine.complete_test(test_id, winner_id)
        return {
            "success": True,
            "test": test.to_dict(),
            "message": f"Test completed. Winner: {test.winner_id}",
            "conclusion": test.conclusion,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/ab-tests/{test_id}/assign", summary="Get variant assignment")
async def assign_variant(
    test_id: str,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get a variant assignment for a test"""
    engine = get_ab_testing_engine()

    try:
        variant = engine.assign_variant(test_id)
        return {
            "variant_id": variant.id,
            "variant_name": variant.name,
            "config": variant.config,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ab-tests/{test_id}/record", summary="Record test result")
async def record_test_result(
    test_id: str,
    request: RecordResultRequest,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Record an impression and optional conversion for a variant"""
    engine = get_ab_testing_engine()

    try:
        engine.record_impression(test_id, request.variant_id, request.segment_ids)

        if request.converted:
            engine.record_conversion(test_id, request.variant_id, request.segment_ids)
        else:
            engine.record_non_conversion(test_id, request.variant_id)

        test = engine.get_test(test_id)

        return {
            "success": True,
            "test": test.to_dict() if test else None,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# ANALYTICS ENDPOINTS
# ============================================================================

@router.post("/analytics/metrics", summary="Record a metric")
async def record_metric(
    request: RecordMetricRequest,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Record a single metric value"""
    engine = get_warmup_analytics_engine()

    engine.record_metric(
        account_id=str(current_user.id),
        metric_id=request.metric_id,
        value=request.value,
        timestamp=request.timestamp,
        metadata=request.metadata,
    )

    return {"success": True, "message": "Metric recorded"}


@router.post("/analytics/metrics/bulk", summary="Record multiple metrics")
async def record_bulk_metrics(
    request: BulkMetricsRequest,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Record multiple metrics at once"""
    engine = get_warmup_analytics_engine()

    engine.record_bulk_metrics(
        account_id=str(current_user.id),
        metrics=request.metrics,
        timestamp=request.timestamp,
    )

    return {"success": True, "message": f"Recorded {len(request.metrics)} metrics"}


@router.get("/analytics/kpi", summary="Get KPI snapshot")
async def get_kpi_snapshot(
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get current KPI snapshot"""
    engine = get_warmup_analytics_engine()

    snapshot = engine.get_kpi_snapshot(str(current_user.id))

    return {
        "snapshot": snapshot.to_dict(),
    }


@router.get("/analytics/time-series/{metric_id}", summary="Get time series data")
async def get_time_series(
    metric_id: str,
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    granularity: str = Query("daily"),
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get time series data for a metric"""
    engine = get_warmup_analytics_engine()

    try:
        gran = TimeGranularity(granularity)
    except ValueError:
        gran = TimeGranularity.DAILY

    data = engine.get_time_series(
        account_id=str(current_user.id),
        metric_id=metric_id,
        start_time=start_date,
        end_time=end_date,
        granularity=gran,
    )

    return {
        "metric_id": metric_id,
        "granularity": granularity,
        "data": data,
    }


@router.get("/analytics/benchmarks", summary="Get benchmarks")
async def get_benchmarks(
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get benchmark comparisons"""
    engine = get_warmup_analytics_engine()

    benchmarks = engine.get_benchmarks(str(current_user.id))

    return {
        "benchmarks": [b.to_dict() for b in benchmarks],
    }


@router.get("/analytics/funnel", summary="Get funnel analysis")
async def get_funnel_analysis(
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get warmup funnel analysis"""
    engine = get_warmup_analytics_engine()

    funnel = engine.analyze_warmup_funnel(str(current_user.id))

    return {
        "funnel": funnel.to_dict(),
    }


@router.post("/analytics/cohorts", summary="Create a cohort")
async def create_cohort(
    request: CreateCohortRequest,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Create a new cohort for analysis"""
    engine = get_warmup_analytics_engine()

    cohort = engine.create_cohort(
        name=request.name,
        description=request.description,
        filter_criteria=request.filter_criteria,
    )

    return {
        "success": True,
        "cohort": cohort.to_dict(),
    }


@router.get("/analytics/cohorts/{cohort_id}/analyze", summary="Analyze cohort")
async def analyze_cohort(
    cohort_id: str,
    metrics: str = Query("open_rate,reply_rate,inbox_rate"),
    period_days: int = Query(30, ge=1, le=365),
    current_user: Candidate = Depends(get_current_candidate),
):
    """Analyze a cohort's performance"""
    engine = get_warmup_analytics_engine()

    metric_list = [m.strip() for m in metrics.split(",")]

    try:
        analysis = engine.analyze_cohort(cohort_id, metric_list, period_days)
        return {
            "analysis": analysis.to_dict(),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/analytics/alerts", summary="Get alerts")
async def get_alerts(
    severity: Optional[str] = Query(None),
    acknowledged: Optional[bool] = Query(None),
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get analytics alerts"""
    engine = get_warmup_analytics_engine()

    try:
        sev = AlertSeverity(severity) if severity else None
    except ValueError:
        sev = None

    alerts = engine.get_alerts(str(current_user.id), sev, acknowledged)

    return {
        "alerts": [a.to_dict() for a in alerts],
        "total": len(alerts),
    }


@router.post("/analytics/alerts/{alert_id}/acknowledge", summary="Acknowledge alert")
async def acknowledge_alert(
    alert_id: str,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Acknowledge an alert"""
    engine = get_warmup_analytics_engine()

    success = engine.acknowledge_alert(str(current_user.id), alert_id)

    return {
        "success": success,
        "message": "Alert acknowledged" if success else "Alert not found",
    }


@router.post("/analytics/reports/generate", summary="Generate report")
async def generate_report(
    period_days: int = Query(30, ge=1, le=365),
    name: Optional[str] = Query(None),
    current_user: Candidate = Depends(get_current_candidate),
):
    """Generate a comprehensive analytics report"""
    engine = get_warmup_analytics_engine()

    report = engine.generate_report(
        account_id=str(current_user.id),
        period_days=period_days,
        name=name,
    )

    return {
        "report": report.to_dict(),
    }


@router.get("/analytics/reports/{report_id}", summary="Get report")
async def get_report(
    report_id: str,
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get a generated report"""
    engine = get_warmup_analytics_engine()

    report = engine.reports.get(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    return {
        "report": report.to_dict(),
    }


# ============================================================================
# DASHBOARD ENDPOINTS
# ============================================================================

@router.get("/dashboard/overview", summary="Get orchestration dashboard")
async def get_orchestration_dashboard(
    current_user: Candidate = Depends(get_current_candidate),
):
    """Get comprehensive orchestration dashboard data"""
    orchestrator = get_campaign_orchestrator()
    ab_engine = get_ab_testing_engine()
    analytics_engine = get_warmup_analytics_engine()

    account_id = str(current_user.id)

    # Get campaigns
    campaigns = orchestrator.get_account_campaigns(account_id)
    active_campaigns = [c for c in campaigns if c.status == CampaignStatus.SENDING]

    # Get A/B tests
    tests = ab_engine.get_account_tests(account_id)
    active_tests = [t for t in tests if t.status == TestStatus.RUNNING]

    # Get KPI snapshot
    kpi = analytics_engine.get_kpi_snapshot(account_id)

    # Get recent alerts
    alerts = analytics_engine.get_alerts(account_id, acknowledged=False)[:5]

    # Get benchmarks
    benchmarks = analytics_engine.get_benchmarks(account_id)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_campaigns": len(campaigns),
            "active_campaigns": len(active_campaigns),
            "total_ab_tests": len(tests),
            "active_ab_tests": len(active_tests),
            "health_score": kpi.score,
            "unacknowledged_alerts": len(alerts),
        },
        "campaigns": [c.to_dict() for c in active_campaigns[:5]],
        "ab_tests": [t.to_dict() for t in active_tests[:5]],
        "kpi": kpi.to_dict(),
        "alerts": [a.to_dict() for a in alerts],
        "benchmarks": [b.to_dict() for b in benchmarks[:5]],
        "statistics": {
            "orchestrator": orchestrator.get_statistics(),
            "ab_testing": ab_engine.get_statistics(),
            "analytics": analytics_engine.get_statistics(),
        },
    }


@router.get("/health", summary="Phase 5 health check")
async def health_check():
    """Health check for Phase 5 services"""
    orchestrator = get_campaign_orchestrator()
    ab_engine = get_ab_testing_engine()
    analytics_engine = get_warmup_analytics_engine()

    return {
        "status": "healthy",
        "phase": 5,
        "services": {
            "campaign_orchestrator": "active",
            "ab_testing_engine": "active",
            "analytics_engine": "active",
        },
        "statistics": {
            "campaigns": orchestrator.get_statistics()["total_campaigns"],
            "ab_tests": ab_engine.get_statistics()["total_tests"],
            "data_points": analytics_engine.get_statistics()["total_data_points"],
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
