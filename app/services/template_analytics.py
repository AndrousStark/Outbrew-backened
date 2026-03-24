"""
Template Analytics Service - Track and analyze template performance

Provides:
- Event tracking (views, clones, uses, ratings)
- Performance snapshot generation (daily, weekly, monthly)
- Template comparison and A/B test analysis
- Trend analysis and growth metrics
- Ranking calculation
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc

from app.models.template_marketplace import (
    PublicTemplate,
    TemplateVersion,
    TemplateVisibility,
    TemplateAnalyticsEvent,
    TemplatePerformanceSnapshot,
    TemplateABTestResult
)


class TemplateAnalyticsService:
    """Service for tracking and analyzing template performance"""

    @staticmethod
    def track_event(
        db: Session,
        template_id: int,
        event_type: str,
        user_id: Optional[int] = None,
        template_version_id: Optional[int] = None,
        event_metadata: Optional[Dict] = None,
        session_id: Optional[str] = None,
        referrer: Optional[str] = None
    ) -> TemplateAnalyticsEvent:
        """
        Track a template analytics event

        Args:
            db: Database session
            template_id: ID of the template
            event_type: Type of event (view, clone, use, rate, favorite, etc.)
            user_id: ID of user who performed the action
            template_version_id: Specific version if applicable
            event_metadata: Additional event data
            session_id: User session ID
            referrer: Source/referrer

        Returns:
            The created event
        """
        event = TemplateAnalyticsEvent(
            template_id=template_id,
            template_version_id=template_version_id,
            user_id=user_id,
            event_type=event_type,
            event_metadata=event_metadata or {},
            session_id=session_id,
            referrer=referrer
        )
        db.add(event)
        db.commit()
        db.refresh(event)

        return event

    @staticmethod
    def track_view(
        db: Session,
        template_id: int,
        user_id: Optional[int] = None,
        session_id: Optional[str] = None,
        source: Optional[str] = None
    ) -> TemplateAnalyticsEvent:
        """Track a template view"""
        return TemplateAnalyticsService.track_event(
            db=db,
            template_id=template_id,
            event_type="view",
            user_id=user_id,
            session_id=session_id,
            event_metadata={"source": source} if source else {}
        )

    @staticmethod
    def track_clone(
        db: Session,
        template_id: int,
        user_id: int,
        template_version_id: Optional[int] = None
    ) -> TemplateAnalyticsEvent:
        """Track a template clone"""
        return TemplateAnalyticsService.track_event(
            db=db,
            template_id=template_id,
            event_type="clone",
            user_id=user_id,
            template_version_id=template_version_id
        )

    @staticmethod
    def track_use(
        db: Session,
        template_id: int,
        user_id: int,
        template_version_id: Optional[int] = None,
        campaign_id: Optional[int] = None
    ) -> TemplateAnalyticsEvent:
        """Track template usage in a campaign"""
        return TemplateAnalyticsService.track_event(
            db=db,
            template_id=template_id,
            event_type="use",
            user_id=user_id,
            template_version_id=template_version_id,
            event_metadata={"campaign_id": campaign_id} if campaign_id else {}
        )

    @staticmethod
    def track_rating(
        db: Session,
        template_id: int,
        user_id: int,
        rating: int,
        review_text: Optional[str] = None
    ) -> TemplateAnalyticsEvent:
        """Track a template rating"""
        return TemplateAnalyticsService.track_event(
            db=db,
            template_id=template_id,
            event_type="rate",
            user_id=user_id,
            event_metadata={"rating": rating, "review": review_text}
        )

    @staticmethod
    def generate_snapshot(
        db: Session,
        template_id: int,
        period_type: str = "daily",
        snapshot_date: Optional[datetime] = None
    ) -> TemplatePerformanceSnapshot:
        """
        Generate a performance snapshot for a template

        Args:
            db: Database session
            template_id: ID of the template
            period_type: Type of period (daily, weekly, monthly)
            snapshot_date: Date of snapshot (defaults to today)

        Returns:
            The created snapshot
        """
        if not snapshot_date:
            snapshot_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        # Calculate period boundaries
        if period_type == "daily":
            period_start = snapshot_date
            period_end = period_start + timedelta(days=1)
        elif period_type == "weekly":
            # Start of week (Monday)
            period_start = snapshot_date - timedelta(days=snapshot_date.weekday())
            period_end = period_start + timedelta(days=7)
        elif period_type == "monthly":
            period_start = snapshot_date.replace(day=1)
            if snapshot_date.month == 12:
                period_end = period_start.replace(year=period_start.year + 1, month=1)
            else:
                period_end = period_start.replace(month=period_start.month + 1)
        else:
            raise ValueError(f"Invalid period_type: {period_type}")

        # Get events for this period
        events = db.query(TemplateAnalyticsEvent).filter(
            and_(
                TemplateAnalyticsEvent.template_id == template_id,
                TemplateAnalyticsEvent.created_at >= period_start,
                TemplateAnalyticsEvent.created_at < period_end
            )
        ).all()

        # Count events by type
        event_counts = {}
        for event in events:
            event_counts[event.event_type] = event_counts.get(event.event_type, 0) + 1

        # Count unique viewers
        unique_viewers = db.query(func.count(func.distinct(TemplateAnalyticsEvent.user_id))).filter(
            and_(
                TemplateAnalyticsEvent.template_id == template_id,
                TemplateAnalyticsEvent.event_type == "view",
                TemplateAnalyticsEvent.created_at >= period_start,
                TemplateAnalyticsEvent.created_at < period_end
            )
        ).scalar() or 0

        # Get template for cumulative metrics
        template = db.query(PublicTemplate).filter(PublicTemplate.id == template_id).first()

        # Calculate rates
        total_views = event_counts.get("view", 0)
        total_clones = event_counts.get("clone", 0)
        total_uses = event_counts.get("use", 0)
        total_favorites = event_counts.get("favorite", 0)

        view_to_clone_rate = (total_clones / total_views) if total_views > 0 else 0
        clone_to_use_rate = (total_uses / total_clones) if total_clones > 0 else 0
        view_to_favorite_rate = (total_favorites / total_views) if total_views > 0 else 0

        # Get previous period snapshot for growth calculation
        previous_snapshot = db.query(TemplatePerformanceSnapshot).filter(
            and_(
                TemplatePerformanceSnapshot.template_id == template_id,
                TemplatePerformanceSnapshot.period_type == period_type,
                TemplatePerformanceSnapshot.snapshot_date < snapshot_date
            )
        ).order_by(desc(TemplatePerformanceSnapshot.snapshot_date)).first()

        # Calculate growth
        views_growth_pct = None
        uses_growth_pct = None
        rating_growth_pct = None

        if previous_snapshot:
            if previous_snapshot.total_views > 0:
                views_growth_pct = ((total_views - previous_snapshot.total_views) /
                                   previous_snapshot.total_views * 100)
            if previous_snapshot.total_uses > 0:
                uses_growth_pct = ((total_uses - previous_snapshot.total_uses) /
                                  previous_snapshot.total_uses * 100)

        # Create snapshot
        snapshot = TemplatePerformanceSnapshot(
            template_id=template_id,
            snapshot_date=snapshot_date,
            period_type=period_type,
            period_start=period_start,
            period_end=period_end,
            total_views=total_views,
            unique_viewers=unique_viewers,
            total_clones=total_clones,
            total_uses=total_uses,
            total_favorites=total_favorites,
            new_ratings=event_counts.get("rate", 0),
            cumulative_avg_rating=template.average_rating if template else None,
            view_to_clone_rate=view_to_clone_rate,
            clone_to_use_rate=clone_to_use_rate,
            view_to_favorite_rate=view_to_favorite_rate,
            views_growth_pct=views_growth_pct,
            uses_growth_pct=uses_growth_pct,
            rating_growth_pct=rating_growth_pct
        )

        db.add(snapshot)
        db.commit()
        db.refresh(snapshot)

        return snapshot

    @staticmethod
    def generate_all_snapshots(
        db: Session,
        period_type: str = "daily",
        snapshot_date: Optional[datetime] = None
    ) -> List[TemplatePerformanceSnapshot]:
        """
        Generate snapshots for all templates

        Args:
            db: Database session
            period_type: Type of period (daily, weekly, monthly)
            snapshot_date: Date of snapshot (defaults to today)

        Returns:
            List of created snapshots
        """
        templates = db.query(PublicTemplate).filter(
            PublicTemplate.visibility == TemplateVisibility.PUBLIC,
            PublicTemplate.deleted_at.is_(None)
        ).all()

        snapshots = []
        for template in templates:
            snapshot = TemplateAnalyticsService.generate_snapshot(
                db=db,
                template_id=template.id,
                period_type=period_type,
                snapshot_date=snapshot_date
            )
            snapshots.append(snapshot)

        return snapshots

    @staticmethod
    def get_template_performance(
        db: Session,
        template_id: int,
        period_type: str = "daily",
        limit: int = 30
    ) -> List[Dict]:
        """
        Get recent performance snapshots for a template

        Args:
            db: Database session
            template_id: ID of the template
            period_type: Type of period
            limit: Number of periods to return

        Returns:
            List of snapshot data
        """
        snapshots = db.query(TemplatePerformanceSnapshot).filter(
            and_(
                TemplatePerformanceSnapshot.template_id == template_id,
                TemplatePerformanceSnapshot.period_type == period_type
            )
        ).order_by(desc(TemplatePerformanceSnapshot.snapshot_date)).limit(limit).all()

        return [
            {
                "date": s.snapshot_date,
                "views": s.total_views,
                "clones": s.total_clones,
                "uses": s.total_uses,
                "favorites": s.total_favorites,
                "view_to_clone_rate": s.view_to_clone_rate,
                "clone_to_use_rate": s.clone_to_use_rate,
                "views_growth": s.views_growth_pct,
                "uses_growth": s.uses_growth_pct
            }
            for s in reversed(snapshots)  # Chronological order
        ]

    @staticmethod
    def compare_templates(
        db: Session,
        template_ids: List[int],
        period_days: int = 30
    ) -> Dict:
        """
        Compare performance of multiple templates

        Args:
            db: Database session
            template_ids: List of template IDs to compare
            period_days: Number of days to analyze

        Returns:
            Comparison data
        """
        period_start = datetime.now(timezone.utc) - timedelta(days=period_days)

        comparisons = []
        for template_id in template_ids:
            # Get events in period
            total_views = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
                and_(
                    TemplateAnalyticsEvent.template_id == template_id,
                    TemplateAnalyticsEvent.event_type == "view",
                    TemplateAnalyticsEvent.created_at >= period_start
                )
            ).scalar() or 0

            total_clones = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
                and_(
                    TemplateAnalyticsEvent.template_id == template_id,
                    TemplateAnalyticsEvent.event_type == "clone",
                    TemplateAnalyticsEvent.created_at >= period_start
                )
            ).scalar() or 0

            total_uses = db.query(func.count(TemplateAnalyticsEvent.id)).filter(
                and_(
                    TemplateAnalyticsEvent.template_id == template_id,
                    TemplateAnalyticsEvent.event_type == "use",
                    TemplateAnalyticsEvent.created_at >= period_start
                )
            ).scalar() or 0

            # Get template info
            template = db.query(PublicTemplate).filter(PublicTemplate.id == template_id).first()

            comparisons.append({
                "template_id": template_id,
                "template_name": template.name if template else "Unknown",
                "total_views": total_views,
                "total_clones": total_clones,
                "total_uses": total_uses,
                "conversion_rate": (total_uses / total_views * 100) if total_views > 0 else 0,
                "average_rating": template.average_rating if template else None
            })

        # Sort by conversion rate
        comparisons.sort(key=lambda x: x["conversion_rate"], reverse=True)

        return {
            "period_days": period_days,
            "templates": comparisons
        }

    @staticmethod
    def get_trending_templates(
        db: Session,
        category: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict]:
        """
        Get trending templates based on recent activity

        Args:
            db: Database session
            category: Filter by category
            limit: Number of templates to return

        Returns:
            List of trending templates with metrics
        """
        # Get activity from last 7 days
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)

        # Query for most viewed/used templates
        query = db.query(
            TemplateAnalyticsEvent.template_id,
            func.count(TemplateAnalyticsEvent.id).label("total_events"),
            func.count(func.distinct(TemplateAnalyticsEvent.user_id)).label("unique_users")
        ).filter(
            TemplateAnalyticsEvent.created_at >= week_ago
        ).group_by(
            TemplateAnalyticsEvent.template_id
        ).order_by(
            desc("total_events")
        ).limit(limit)

        results = query.all()

        trending = []
        for template_id, total_events, unique_users in results:
            template = db.query(PublicTemplate).filter(PublicTemplate.id == template_id).first()
            if template and (not category or template.category == category):
                trending.append({
                    "template_id": template_id,
                    "name": template.name,
                    "category": template.category,
                    "total_events_7d": total_events,
                    "unique_users_7d": unique_users,
                    "average_rating": template.average_rating,
                    "total_uses": template.use_count
                })

        return trending

    @staticmethod
    def calculate_rankings(db: Session) -> None:
        """
        Calculate and update rankings for all templates

        Updates the rank_in_category and rank_overall fields in performance snapshots
        """
        # Get latest snapshots
        latest_date = db.query(func.max(TemplatePerformanceSnapshot.snapshot_date)).scalar()
        if not latest_date:
            return

        snapshots = db.query(TemplatePerformanceSnapshot).filter(
            TemplatePerformanceSnapshot.snapshot_date == latest_date
        ).all()

        # Sort by composite score (views + uses + rating)
        def calculate_score(s):
            template = db.query(PublicTemplate).filter(PublicTemplate.id == s.template_id).first()
            rating = template.average_rating if template else 0
            return (s.total_views * 0.3 + s.total_uses * 0.5 + rating * 20 * 0.2)

        sorted_snapshots = sorted(snapshots, key=calculate_score, reverse=True)

        # Assign overall ranks
        for rank, snapshot in enumerate(sorted_snapshots, start=1):
            snapshot.rank_overall = rank

        # Assign category ranks
        by_category = {}
        for snapshot in snapshots:
            template = db.query(PublicTemplate).filter(PublicTemplate.id == snapshot.template_id).first()
            if template:
                category = template.category or "Other"
                if category not in by_category:
                    by_category[category] = []
                by_category[category].append(snapshot)

        for category, category_snapshots in by_category.items():
            sorted_category = sorted(category_snapshots, key=calculate_score, reverse=True)
            for rank, snapshot in enumerate(sorted_category, start=1):
                snapshot.rank_in_category = rank

        db.commit()
