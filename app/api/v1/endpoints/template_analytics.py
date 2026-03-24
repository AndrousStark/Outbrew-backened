"""
Template Analytics API Endpoints

Comprehensive REST API for template performance tracking, trending analysis, and marketplace insights.
"""

from typing import List, Optional
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.api.dependencies import get_current_candidate
from app.models.candidate import Candidate
from app.services.template_analytics import TemplateAnalyticsService
from app.models.template_marketplace import (
    TemplateAnalyticsEvent,
    TemplatePerformanceSnapshot
)


router = APIRouter(
    prefix="/template-analytics",
    tags=["Template Analytics"],
    dependencies=[Depends(get_current_candidate)]
)


# ========================================
# Pydantic Schemas
# ========================================

class EventTrackRequest(BaseModel):
    """Schema for tracking an analytics event"""
    template_id: int
    event_type: str = Field(..., description="view, clone, use, rate, favorite, share, report")
    user_id: Optional[int] = None
    template_version_id: Optional[int] = None
    event_metadata: Optional[dict] = Field(default_factory=dict)
    session_id: Optional[str] = None
    referrer: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "template_id": 42,
                "event_type": "view",
                "user_id": 123,
                "event_metadata": {"source": "search", "category": "cold_outreach"},
                "session_id": "sess_abc123",
                "referrer": "/templates/search"
            }
        }


class RatingTrackRequest(BaseModel):
    """Schema for tracking a template rating"""
    template_id: int
    user_id: int
    rating: int = Field(..., ge=1, le=5, description="Rating from 1 to 5 stars")
    review_text: Optional[str] = Field(None, max_length=1000)

    class Config:
        json_schema_extra = {
            "example": {
                "template_id": 42,
                "user_id": 123,
                "rating": 5,
                "review_text": "Excellent template! Got 3 replies in the first week."
            }
        }


class EventResponse(BaseModel):
    """Response schema for analytics event"""
    id: int
    template_id: int
    event_type: str
    user_id: Optional[int]
    created_at: datetime

    class Config:
        from_attributes = True


class PerformanceSnapshotResponse(BaseModel):
    """Response schema for performance snapshot"""
    id: int
    template_id: int
    snapshot_date: datetime
    period_type: str
    total_views: int
    total_clones: int
    total_uses: int
    total_favorites: int
    view_to_clone_rate: Optional[float]
    clone_to_use_rate: Optional[float]
    views_growth_pct: Optional[float]
    uses_growth_pct: Optional[float]
    rank_in_category: Optional[int]
    rank_overall: Optional[int]

    class Config:
        from_attributes = True


class PerformanceMetrics(BaseModel):
    """Performance metrics over time"""
    date: datetime
    views: int
    clones: int
    uses: int
    favorites: int
    view_to_clone_rate: Optional[float]
    clone_to_use_rate: Optional[float]
    views_growth: Optional[float]
    uses_growth: Optional[float]


class TemplateComparison(BaseModel):
    """Template comparison data"""
    template_id: int
    template_name: str
    total_views: int
    total_clones: int
    total_uses: int
    conversion_rate: float
    average_rating: Optional[float]


class TrendingTemplate(BaseModel):
    """Trending template data"""
    template_id: int
    name: str
    category: str
    total_events_7d: int
    unique_users_7d: int
    average_rating: Optional[float]
    total_uses: int


# ========================================
# Event Tracking Endpoints
# ========================================

@router.post("/events", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def track_event(
    event: EventTrackRequest,
    db: Session = Depends(get_db)
):
    """
    Track a template analytics event

    Event types:
    - **view**: User viewed template details
    - **clone**: User cloned template to their library
    - **use**: User used template in a campaign
    - **rate**: User rated the template
    - **favorite**: User favorited the template
    - **share**: User shared the template
    - **report**: User reported an issue
    """
    created_event = TemplateAnalyticsService.track_event(
        db=db,
        template_id=event.template_id,
        event_type=event.event_type,
        user_id=event.user_id,
        template_version_id=event.template_version_id,
        event_metadata=event.event_metadata,
        session_id=event.session_id,
        referrer=event.referrer
    )

    return created_event


@router.post("/events/view", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def track_view(
    template_id: int,
    user_id: Optional[int] = None,
    session_id: Optional[str] = None,
    source: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Track a template view (simplified endpoint)

    Use this for quick view tracking without full event payload.
    """
    event = TemplateAnalyticsService.track_view(
        db=db,
        template_id=template_id,
        user_id=user_id,
        session_id=session_id,
        source=source
    )

    return event


@router.post("/events/clone", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def track_clone(
    template_id: int,
    user_id: int,
    template_version_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Track a template clone"""
    event = TemplateAnalyticsService.track_clone(
        db=db,
        template_id=template_id,
        user_id=user_id,
        template_version_id=template_version_id
    )

    return event


@router.post("/events/use", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def track_use(
    template_id: int,
    user_id: int,
    template_version_id: Optional[int] = None,
    campaign_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Track template usage in a campaign"""
    event = TemplateAnalyticsService.track_use(
        db=db,
        template_id=template_id,
        user_id=user_id,
        template_version_id=template_version_id,
        campaign_id=campaign_id
    )

    return event


@router.post("/events/rate", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def track_rating(
    rating: RatingTrackRequest,
    db: Session = Depends(get_db)
):
    """
    Track a template rating

    Ratings range from 1 to 5 stars. Optional review text can be included.
    """
    event = TemplateAnalyticsService.track_rating(
        db=db,
        template_id=rating.template_id,
        user_id=rating.user_id,
        rating=rating.rating,
        review_text=rating.review_text
    )

    return event


# ========================================
# Performance Snapshot Endpoints
# ========================================

@router.post("/snapshots/generate", response_model=PerformanceSnapshotResponse, status_code=status.HTTP_201_CREATED)
async def generate_snapshot(
    template_id: int,
    period_type: str = Query("daily", description="daily, weekly, or monthly"),
    snapshot_date: Optional[datetime] = None,
    db: Session = Depends(get_db)
):
    """
    Generate a performance snapshot for a template

    This aggregates all events for the specified period into a single snapshot.
    Snapshots are used for efficient querying of historical performance.

    Period types:
    - **daily**: Snapshot for a single day
    - **weekly**: Snapshot for a week (Monday to Sunday)
    - **monthly**: Snapshot for a calendar month
    """
    snapshot = TemplateAnalyticsService.generate_snapshot(
        db=db,
        template_id=template_id,
        period_type=period_type,
        snapshot_date=snapshot_date
    )

    return snapshot


@router.post("/snapshots/generate-all", status_code=status.HTTP_202_ACCEPTED)
async def generate_all_snapshots(
    period_type: str = Query("daily", description="daily, weekly, or monthly"),
    snapshot_date: Optional[datetime] = None,
    db: Session = Depends(get_db)
):
    """
    Generate snapshots for all published templates

    This is typically called by a scheduled job to generate daily/weekly/monthly snapshots.
    Returns 202 Accepted as this may take time for large numbers of templates.
    """
    snapshots = TemplateAnalyticsService.generate_all_snapshots(
        db=db,
        period_type=period_type,
        snapshot_date=snapshot_date
    )

    return {
        "message": f"Generated {len(snapshots)} snapshots",
        "count": len(snapshots),
        "period_type": period_type
    }


@router.get("/templates/{template_id}/performance", response_model=List[PerformanceMetrics])
async def get_template_performance(
    template_id: int,
    period_type: str = Query("daily", description="daily, weekly, or monthly"),
    limit: int = Query(30, ge=1, le=365, description="Number of periods to return"),
    db: Session = Depends(get_db)
):
    """
    Get recent performance data for a template

    Returns time-series data showing how the template has performed over time.
    Perfect for rendering performance charts.

    Parameters:
    - **template_id**: ID of the template
    - **period_type**: Granularity of data (daily, weekly, monthly)
    - **limit**: Number of periods to return (default: 30)
    """
    performance = TemplateAnalyticsService.get_template_performance(
        db=db,
        template_id=template_id,
        period_type=period_type,
        limit=limit
    )

    return performance


# ========================================
# Comparison & Trending Endpoints
# ========================================

@router.get("/templates/compare", response_model=dict)
async def compare_templates(
    template_ids: str = Query(..., description="Comma-separated template IDs"),
    period_days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Compare performance of multiple templates

    Returns side-by-side comparison of:
    - Total views, clones, uses
    - Conversion rates
    - Average ratings

    Example: `/templates/compare?template_ids=1,2,3&period_days=30`
    """
    # Parse template IDs
    try:
        ids = [int(id.strip()) for id in template_ids.split(",")]
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid template_ids format. Use comma-separated integers."
        )

    if len(ids) < 2:
        raise HTTPException(
            status_code=400,
            detail="Need at least 2 templates to compare"
        )

    if len(ids) > 10:
        raise HTTPException(
            status_code=400,
            detail="Can compare maximum 10 templates at once"
        )

    comparison = TemplateAnalyticsService.compare_templates(
        db=db,
        template_ids=ids,
        period_days=period_days
    )

    return comparison


@router.get("/templates/trending", response_model=List[TrendingTemplate])
async def get_trending_templates(
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(10, ge=1, le=50, description="Number of templates to return"),
    db: Session = Depends(get_db)
):
    """
    Get trending templates based on recent activity

    Analyzes the last 7 days of activity to identify templates with:
    - High view counts
    - Growing usage
    - Strong engagement

    Perfect for "Trending Now" sections in the UI.
    """
    trending = TemplateAnalyticsService.get_trending_templates(
        db=db,
        category=category,
        limit=limit
    )

    return trending


# ========================================
# Rankings Endpoints
# ========================================

@router.post("/rankings/calculate", status_code=status.HTTP_202_ACCEPTED)
async def calculate_rankings(
    db: Session = Depends(get_db)
):
    """
    Calculate and update rankings for all templates

    This computes:
    - Overall marketplace rankings
    - Category-specific rankings

    Rankings are based on a composite score of views, uses, and ratings.
    Typically called by a scheduled job (e.g., daily at midnight).
    """
    TemplateAnalyticsService.calculate_rankings(db)

    return {
        "message": "Rankings calculated successfully",
        "timestamp": datetime.now(timezone.utc)
    }


# ========================================
# Statistics Endpoints
# ========================================

@router.get("/templates/{template_id}/stats", response_model=dict)
async def get_template_stats(
    template_id: int,
    db: Session = Depends(get_db)
):
    """
    Get comprehensive statistics for a template

    Returns:
    - Total lifetime views, clones, uses, favorites
    - Current performance metrics
    - Recent growth trends
    - Latest ranking position
    """
    # Get latest snapshot
    latest_snapshot = db.query(TemplatePerformanceSnapshot).filter(
        TemplatePerformanceSnapshot.template_id == template_id
    ).order_by(TemplatePerformanceSnapshot.snapshot_date.desc()).first()

    if not latest_snapshot:
        raise HTTPException(
            status_code=404,
            detail=f"No performance data found for template {template_id}"
        )

    # Get lifetime totals
    from sqlalchemy import func
    lifetime_views = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
        TemplateAnalyticsEvent.template_id == template_id,
        TemplateAnalyticsEvent.event_type == "view"
    ).scalar() or 0

    lifetime_clones = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
        TemplateAnalyticsEvent.template_id == template_id,
        TemplateAnalyticsEvent.event_type == "clone"
    ).scalar() or 0

    lifetime_uses = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
        TemplateAnalyticsEvent.template_id == template_id,
        TemplateAnalyticsEvent.event_type == "use"
    ).scalar() or 0

    lifetime_favorites = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
        TemplateAnalyticsEvent.template_id == template_id,
        TemplateAnalyticsEvent.event_type == "favorite"
    ).scalar() or 0

    return {
        "template_id": template_id,
        "lifetime": {
            "views": lifetime_views,
            "clones": lifetime_clones,
            "uses": lifetime_uses,
            "favorites": lifetime_favorites,
            "conversion_rate": (lifetime_uses / lifetime_views * 100) if lifetime_views > 0 else 0
        },
        "latest_period": {
            "period_type": latest_snapshot.period_type,
            "date": latest_snapshot.snapshot_date,
            "views": latest_snapshot.total_views,
            "clones": latest_snapshot.total_clones,
            "uses": latest_snapshot.total_uses,
            "favorites": latest_snapshot.total_favorites,
            "view_to_clone_rate": latest_snapshot.view_to_clone_rate,
            "clone_to_use_rate": latest_snapshot.clone_to_use_rate
        },
        "growth": {
            "views_growth_pct": latest_snapshot.views_growth_pct,
            "uses_growth_pct": latest_snapshot.uses_growth_pct
        },
        "rankings": {
            "overall": latest_snapshot.rank_overall,
            "in_category": latest_snapshot.rank_in_category
        }
    }


@router.get("/dashboard/summary", response_model=dict)
async def get_dashboard_summary(
    db: Session = Depends(get_db)
):
    """
    Get marketplace-wide summary statistics

    Returns:
    - Total templates
    - Total events (last 7/30 days)
    - Most popular categories
    - Top performing templates
    """
    from sqlalchemy import func

    # Date ranges
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    month_ago = datetime.now(timezone.utc) - timedelta(days=30)

    # Count events
    events_7d = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
        TemplateAnalyticsEvent.created_at >= week_ago
    ).scalar() or 0

    events_30d = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
        TemplateAnalyticsEvent.created_at >= month_ago
    ).scalar() or 0

    # Get top templates (by recent views)
    top_templates = db.query(
        TemplateAnalyticsEvent.template_id,
        func.count(TemplateAnalyticsEvent.id).label("event_count")
    ).filter(
        TemplateAnalyticsEvent.created_at >= week_ago
    ).group_by(
        TemplateAnalyticsEvent.template_id
    ).order_by(
        func.count(TemplateAnalyticsEvent.id).desc()
    ).limit(5).all()

    return {
        "period": {
            "last_7_days": {
                "total_events": events_7d
            },
            "last_30_days": {
                "total_events": events_30d
            }
        },
        "top_templates_7d": [
            {"template_id": t[0], "events": t[1]}
            for t in top_templates
        ]
    }
