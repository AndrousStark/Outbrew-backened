"""Warmup Health API Endpoints

Provides comprehensive email health monitoring:
- Health score dashboard
- Alert management
- Milestone tracking
- Domain reputation
- Trend analysis
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, status, Query

logger = logging.getLogger(__name__)
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.warmup_health import (
    WarmupHealthScore,
    WarmupHealthAlert,
    DomainReputation,
    WarmupMilestone,
    HealthStatusEnum,
    AlertSeverityEnum
)
from app.services.warmup_health_tracker import WarmupHealthTracker

router = APIRouter(tags=["Warmup Health"])


# ============== Schemas ==============

class HealthScoreComponent(BaseModel):
    """Component score details"""
    score: float
    rate: Optional[float] = None


class HealthBreakdownItem(BaseModel):
    """Individual breakdown metric"""
    category: str
    score: float
    weight: int
    status: str  # "excellent", "good", "warning", "critical"
    tip: str


class HealthScoreResponse(BaseModel):
    """Health score response"""
    overall_score: float
    health_status: str
    status: str  # "excellent", "good", "fair", "poor", "critical"
    status_color: str
    inbox_placement_score: float = 0
    spam_rate_score: float = 0
    activity_score: float = 0
    breakdown: List[HealthBreakdownItem] = []
    components: dict
    trends: dict
    averages_7day: dict
    recommendations: List[dict]
    updated_at: str


class AlertResponse(BaseModel):
    """Alert response"""
    id: int
    type: str
    severity: str
    severity_color: str
    title: str
    message: str
    context: Optional[dict] = None
    recommended_actions: Optional[List[dict]] = None
    is_read: bool
    triggered_at: str


class MilestoneResponse(BaseModel):
    """Milestone response"""
    id: int
    type: str
    title: str
    description: Optional[str] = None
    badge_icon: Optional[str] = None
    badge_color: Optional[str] = None
    achieved_at: str


class DomainReputationResponse(BaseModel):
    """Domain reputation response"""
    domain: str
    overall_reputation: float
    authentication: dict
    blacklist: dict
    lifetime_stats: dict


class WarmingStatusResponse(BaseModel):
    """Warming status response"""
    status: str
    strategy: str
    current_day: int
    max_day: int
    progress_percent: int
    daily_limit: int
    sent_today: int
    remaining_today: int
    total_sent: int
    start_date: Optional[str] = None


class QuickStatsResponse(BaseModel):
    """Quick stats response"""
    health_emoji: str
    health_label: str
    tip: str
    score: Optional[float] = None
    trend_emoji: Optional[str] = None


class HealthDashboardResponse(BaseModel):
    """Complete health dashboard response"""
    health_score: Optional[HealthScoreResponse] = None
    warming_status: Optional[WarmingStatusResponse] = None
    alerts: List[AlertResponse]
    alert_counts: dict
    milestones: List[MilestoneResponse]
    score_history: List[dict]
    domain_reputation: Optional[DomainReputationResponse] = None
    quick_stats: QuickStatsResponse


class ResolveAlertRequest(BaseModel):
    """Request to resolve an alert"""
    note: Optional[str] = Field(None, max_length=500, description="Resolution note")


# ============== Endpoints ==============

@router.get("/dashboard", response_model=HealthDashboardResponse)
def get_health_dashboard(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get comprehensive health dashboard data.

    Returns:
    - Overall health score and components
    - Warming progress
    - Active alerts
    - Achieved milestones
    - Score history (30 days)
    - Domain reputation
    """
    tracker = WarmupHealthTracker(db)
    dashboard = tracker.get_health_dashboard(current_user.id)
    return dashboard


@router.post("/calculate-score", response_model=HealthScoreResponse)
def calculate_health_score(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Calculate and store current health score.

    This triggers a fresh calculation of:
    - Delivery score
    - Bounce score
    - Open rate score
    - Spam score
    - Consistency score

    Returns the new overall health score with recommendations.
    """
    tracker = WarmupHealthTracker(db)
    score = tracker.calculate_health_score(current_user.id)

    return tracker._format_health_score(score)


@router.get("/score/history")
def get_score_history(
    days: int = Query(default=30, le=90, description="Number of days of history"),
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get historical health scores.

    Returns daily scores for trend analysis and charting.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    scores = db.query(WarmupHealthScore).filter(
        WarmupHealthScore.candidate_id == current_user.id,
        WarmupHealthScore.score_date >= cutoff
    ).order_by(WarmupHealthScore.score_date).all()

    return {
        "history": [
            {
                "date": s.score_date.isoformat(),
                "overall_score": s.overall_score,
                "health_status": s.health_status,
                "delivery_rate": s.delivery_rate,
                "bounce_rate": s.bounce_rate,
                "open_rate": s.open_rate,
                "emails_sent": s.emails_sent,
                "trend": s.score_trend
            }
            for s in scores
        ],
        "summary": {
            "avg_score": sum(s.overall_score for s in scores) / len(scores) if scores else 0,
            "best_score": max(s.overall_score for s in scores) if scores else 0,
            "worst_score": min(s.overall_score for s in scores) if scores else 0,
            "total_days": len(scores)
        }
    }


@router.get("/score/latest")
def get_latest_score(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get the most recent health score.

    Quick endpoint for displaying current health status.
    """
    score = db.query(WarmupHealthScore).filter(
        WarmupHealthScore.candidate_id == current_user.id
    ).order_by(WarmupHealthScore.score_date.desc()).first()

    if not score:
        return {
            "has_score": False,
            "message": "No health score calculated yet. Start warming to build your score."
        }

    tracker = WarmupHealthTracker(db)
    breakdown = tracker._calculate_score_breakdown(score, current_user.id)

    # Derive individual top-level breakdown scores
    inbox_item = next((b for b in breakdown if b["category"] == "Inbox Placement"), None)
    spam_item = next((b for b in breakdown if b["category"] == "Spam Rate"), None)
    activity_item = next((b for b in breakdown if b["category"] == "Activity Level"), None)

    return {
        "has_score": True,
        "score": score.overall_score,
        "status": tracker._get_overall_status(score.overall_score),
        "health_status": score.health_status,
        "trend": score.score_trend,
        "inbox_placement_score": inbox_item["score"] if inbox_item else 0,
        "spam_rate_score": spam_item["score"] if spam_item else 0,
        "activity_score": activity_item["score"] if activity_item else 0,
        "breakdown": breakdown,
        "updated_at": score.score_date.isoformat()
    }


# ============== Alerts ==============

@router.get("/alerts", response_model=List[AlertResponse])
def get_alerts(
    include_resolved: bool = Query(default=False, description="Include resolved alerts"),
    severity: Optional[str] = Query(default=None, description="Filter by severity"),
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get health alerts for the current user.

    Alerts are created automatically when:
    - Bounce rate exceeds threshold
    - Delivery rate drops
    - Spam complaints detected
    - Reputation declining
    """
    tracker = WarmupHealthTracker(db)
    alerts = tracker.get_alerts(
        candidate_id=current_user.id,
        include_resolved=include_resolved,
        severity=severity
    )

    return [tracker._format_alert(a) for a in alerts]


@router.get("/alerts/count")
def get_alert_counts(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get count of active alerts by severity.

    Useful for notification badges.
    """
    alerts = db.query(WarmupHealthAlert).filter(
        WarmupHealthAlert.candidate_id == current_user.id,
        WarmupHealthAlert.is_resolved.is_(False)
    ).all()

    return {
        "total": len(alerts),
        "unread": len([a for a in alerts if not a.is_read]),
        "by_severity": {
            "critical": len([a for a in alerts if a.severity == AlertSeverityEnum.CRITICAL.value]),
            "warning": len([a for a in alerts if a.severity == AlertSeverityEnum.WARNING.value]),
            "info": len([a for a in alerts if a.severity == AlertSeverityEnum.INFO.value])
        }
    }


@router.put("/alerts/{alert_id}/read")
def mark_alert_read(
    alert_id: int,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Mark an alert as read."""
    alert = db.query(WarmupHealthAlert).filter(
        WarmupHealthAlert.id == alert_id,
        WarmupHealthAlert.candidate_id == current_user.id
    ).first()

    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found"
        )

    tracker = WarmupHealthTracker(db)
    tracker.mark_alert_read(alert_id)

    return {"success": True, "message": "Alert marked as read"}


@router.put("/alerts/{alert_id}/resolve")
def resolve_alert(
    alert_id: int,
    request: ResolveAlertRequest,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Manually resolve an alert."""
    alert = db.query(WarmupHealthAlert).filter(
        WarmupHealthAlert.id == alert_id,
        WarmupHealthAlert.candidate_id == current_user.id
    ).first()

    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found"
        )

    tracker = WarmupHealthTracker(db)
    resolved_alert = tracker.resolve_alert(alert_id, request.note)

    return {
        "success": True,
        "message": "Alert resolved",
        "alert": tracker._format_alert(resolved_alert)
    }


@router.put("/alerts/read-all")
def mark_all_alerts_read(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Mark all alerts as read."""
    alerts = db.query(WarmupHealthAlert).filter(
        WarmupHealthAlert.candidate_id == current_user.id,
        WarmupHealthAlert.is_read.is_(False)
    ).all()

    for alert in alerts:
        alert.is_read = True

    try:
        db.commit()
        logger.info(f"[WarmupHealth] Marked {len(alerts)} alerts as read for candidate {current_user.id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[WarmupHealth] Failed to mark alerts as read: {e}")
        raise HTTPException(status_code=500, detail="Failed to mark alerts as read")

    return {"success": True, "marked_read": len(alerts)}


# ============== Milestones ==============

@router.get("/milestones", response_model=List[MilestoneResponse])
def get_milestones(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get all achieved milestones.

    Milestones are awarded for:
    - Days of warming completed
    - Emails sent thresholds
    - Perfect delivery streaks
    - Health score achievements
    """
    milestones = db.query(WarmupMilestone).filter(
        WarmupMilestone.candidate_id == current_user.id
    ).order_by(WarmupMilestone.achieved_at.desc()).all()

    tracker = WarmupHealthTracker(db)
    return [tracker._format_milestone(m) for m in milestones]


@router.get("/milestones/available")
def get_available_milestones(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get all milestone definitions with achievement status.

    Shows which milestones are earned vs. still available.
    """
    from app.models.warmup_health import MILESTONE_DEFINITIONS

    achieved = db.query(WarmupMilestone).filter(
        WarmupMilestone.candidate_id == current_user.id
    ).all()

    achieved_types = {m.milestone_type for m in achieved}

    all_milestones = []
    for milestone_id, definition in MILESTONE_DEFINITIONS.items():
        is_achieved = milestone_id in achieved_types
        achieved_milestone = next(
            (m for m in achieved if m.milestone_type == milestone_id),
            None
        )

        all_milestones.append({
            "id": milestone_id,
            "title": definition["title"],
            "description": definition["description"],
            "badge_icon": definition["badge_icon"],
            "badge_color": definition["badge_color"],
            "is_achieved": is_achieved,
            "achieved_at": achieved_milestone.achieved_at.isoformat() if achieved_milestone else None
        })

    return {
        "milestones": all_milestones,
        "achieved_count": len(achieved_types),
        "total_count": len(MILESTONE_DEFINITIONS)
    }


# ============== Domain Reputation ==============

@router.get("/domain-reputation")
def get_domain_reputation(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get domain reputation details.

    Shows:
    - Overall reputation score
    - Authentication status (SPF, DKIM, DMARC)
    - Blacklist status
    - Lifetime sending stats
    """
    rep = db.query(DomainReputation).filter(
        DomainReputation.candidate_id == current_user.id
    ).first()

    if not rep:
        # Create default reputation record
        email = current_user.email
        domain = email.split('@')[1] if '@' in email else 'unknown'

        rep = DomainReputation(
            candidate_id=current_user.id,
            domain=domain,
            email_address=email,
            overall_reputation=50.0,  # Start neutral
            authentication_score=0.0
        )
        try:
            db.add(rep)
            db.commit()
            db.refresh(rep)
            logger.info(f"[WarmupHealth] Created domain reputation for {domain}")
        except Exception as e:
            db.rollback()
            logger.error(f"[WarmupHealth] Failed to create domain reputation: {e}")
            raise HTTPException(status_code=500, detail="Failed to create domain reputation")

    tracker = WarmupHealthTracker(db)
    return tracker._format_domain_reputation(rep)


@router.post("/domain-reputation/check-authentication")
def check_domain_authentication(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Check and update domain authentication status.

    Verifies SPF, DKIM, and DMARC via real DNS lookups.
    """
    import dns.resolver

    rep = db.query(DomainReputation).filter(
        DomainReputation.candidate_id == current_user.id
    ).first()

    if not rep:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Domain reputation not found"
        )

    domain = rep.domain
    auth_score = 0.0
    recommendations = []

    # Real SPF check
    rep.spf_configured = False
    try:
        txt_records = dns.resolver.resolve(domain, 'TXT')
        for record in txt_records:
            if 'v=spf1' in str(record):
                rep.spf_configured = True
                break
    except Exception:
        pass

    if rep.spf_configured:
        auth_score += 33.3
    else:
        recommendations.append({
            "issue": "SPF not configured",
            "action": "Add an SPF TXT record to your domain's DNS (e.g., v=spf1 include:_spf.google.com ~all)",
            "impact": "Prevents email spoofing and improves deliverability"
        })

    # Real DKIM check (common selectors)
    rep.dkim_configured = False
    for selector in ['default', 'google', 'selector1', 'selector2', 'k1']:
        try:
            dns.resolver.resolve(f'{selector}._domainkey.{domain}', 'TXT')
            rep.dkim_configured = True
            break
        except Exception:
            continue

    if rep.dkim_configured:
        auth_score += 33.3
    else:
        recommendations.append({
            "issue": "DKIM not configured",
            "action": "Set up DKIM signing for your domain with your email provider",
            "impact": "Verifies email authenticity and improves deliverability"
        })

    # Real DMARC check
    rep.dmarc_configured = False
    try:
        dmarc_records = dns.resolver.resolve(f'_dmarc.{domain}', 'TXT')
        for record in dmarc_records:
            if 'v=DMARC1' in str(record):
                rep.dmarc_configured = True
                break
    except Exception:
        pass

    if rep.dmarc_configured:
        auth_score += 33.4
    else:
        recommendations.append({
            "issue": "DMARC not configured",
            "action": "Add a DMARC record to your domain's DNS (e.g., v=DMARC1; p=quarantine; rua=mailto:dmarc@yourdomain.com)",
            "impact": "Improves deliverability and prevents spoofing"
        })

    rep.authentication_score = auth_score
    rep.updated_at = datetime.now(timezone.utc)

    # Update overall reputation based on auth
    base_rep = 50.0
    auth_bonus = (auth_score / 100) * 30  # Auth can add up to 30 points
    rep.overall_reputation = min(100, base_rep + auth_bonus)

    try:
        db.commit()
        logger.info(f"[WarmupHealth] Updated auth status for {rep.domain}: score={auth_score}")
    except Exception as e:
        db.rollback()
        logger.error(f"[WarmupHealth] Failed to update auth status: {e}")
        raise HTTPException(status_code=500, detail="Failed to update authentication status")

    return {
        "domain": rep.domain,
        "authentication": {
            "spf": {"configured": rep.spf_configured, "status": "pass" if rep.spf_configured else "missing"},
            "dkim": {"configured": rep.dkim_configured, "status": "pass" if rep.dkim_configured else "missing"},
            "dmarc": {"configured": rep.dmarc_configured, "status": "pass" if rep.dmarc_configured else "missing"}
        },
        "authentication_score": auth_score,
        "overall_reputation": rep.overall_reputation,
        "recommendations": recommendations
    }


# ============== Quick Actions ==============

@router.get("/quick-check")
def quick_health_check(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Quick health status check.

    Returns a simple summary suitable for headers/badges.
    """
    tracker = WarmupHealthTracker(db)
    dashboard = tracker.get_health_dashboard(current_user.id)

    return {
        "quick_stats": dashboard["quick_stats"],
        "alert_count": dashboard["alert_counts"]["critical"] + dashboard["alert_counts"]["warning"],
        "has_critical": dashboard["alert_counts"]["critical"] > 0,
        "warming_active": dashboard["warming_status"]["status"] == "active" if dashboard["warming_status"] else False
    }


@router.get("/recommendations")
def get_recommendations(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get current recommendations based on health score.

    Returns prioritized actions to improve email health.
    """
    score = db.query(WarmupHealthScore).filter(
        WarmupHealthScore.candidate_id == current_user.id
    ).order_by(WarmupHealthScore.score_date.desc()).first()

    if not score or not score.recommendations:
        return {
            "recommendations": [{
                "priority": 1,
                "action": "Start your warming campaign",
                "reason": "Begin building your sender reputation",
                "impact": "info",
                "icon": "rocket"
            }]
        }

    return {
        "recommendations": score.recommendations,
        "health_score": score.overall_score,
        "health_status": score.health_status
    }


# ============== Analytics ==============

@router.get("/analytics/summary")
def get_health_analytics(
    days: int = Query(default=7, le=30),
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get health analytics summary.

    Provides aggregated metrics for the specified period.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    scores = db.query(WarmupHealthScore).filter(
        WarmupHealthScore.candidate_id == current_user.id,
        WarmupHealthScore.score_date >= cutoff
    ).all()

    if not scores:
        return {
            "period_days": days,
            "has_data": False,
            "message": "No health data available for this period"
        }

    # Calculate analytics
    avg_score = sum(s.overall_score for s in scores) / len(scores)
    avg_delivery = sum(s.delivery_rate for s in scores) / len(scores)
    avg_bounce = sum(s.bounce_rate for s in scores) / len(scores)
    total_sent = sum(s.emails_sent for s in scores)
    total_delivered = sum(s.emails_delivered for s in scores)
    total_bounced = sum(s.emails_bounced for s in scores)

    # Find best and worst days
    best_day = max(scores, key=lambda s: s.overall_score)
    worst_day = min(scores, key=lambda s: s.overall_score)

    # Calculate trend
    if len(scores) >= 2:
        first_half = scores[:len(scores)//2]
        second_half = scores[len(scores)//2:]
        first_avg = sum(s.overall_score for s in first_half) / len(first_half)
        second_avg = sum(s.overall_score for s in second_half) / len(second_half)
        trend = "improving" if second_avg > first_avg else ("declining" if second_avg < first_avg else "stable")
    else:
        trend = "insufficient_data"

    return {
        "period_days": days,
        "has_data": True,
        "summary": {
            "average_score": round(avg_score, 1),
            "average_delivery_rate": round(avg_delivery, 1),
            "average_bounce_rate": round(avg_bounce, 2),
            "total_emails_sent": total_sent,
            "total_delivered": total_delivered,
            "total_bounced": total_bounced,
            "delivery_success_rate": round((total_delivered / total_sent * 100) if total_sent > 0 else 100, 1)
        },
        "best_day": {
            "date": best_day.score_date.isoformat(),
            "score": best_day.overall_score
        },
        "worst_day": {
            "date": worst_day.score_date.isoformat(),
            "score": worst_day.overall_score
        },
        "trend": trend,
        "data_points": len(scores)
    }
