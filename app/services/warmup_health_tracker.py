"""Warmup Health Tracker Service

Intelligent health monitoring for email sender reputation.
Provides:
- Real-time health scoring
- Trend analysis
- Predictive alerts
- Smart recommendations
- Milestone tracking

Based on 2025 email deliverability research and best practices.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc

from app.models.warmup_health import (
    WarmupHealthScore,
    WarmupHealthAlert,
    DomainReputation,
    WarmupMilestone,
    HealthStatusEnum,
    AlertSeverityEnum,
    AlertTypeEnum,
    HEALTH_SCORE_WEIGHTS,
    ALERT_THRESHOLDS,
    MILESTONE_DEFINITIONS
)
from app.models.email_warming import (
    EmailWarmingConfig,
    EmailWarmingDailyLog,
    WarmingStatusEnum
)
from app.core.logger import get_logger

logger = get_logger(__name__)


class WarmupHealthTracker:
    """Service for tracking and analyzing email warming health"""

    def __init__(self, db: Session):
        self.db = db
        logger.debug("[WarmupHealthTracker] Initialized with database session")

    # ============== Health Score Calculation ==============

    def calculate_health_score(self, candidate_id: int) -> WarmupHealthScore:
        """
        Calculate comprehensive health score for a candidate.

        Score components:
        - Delivery rate (30%): Higher is better
        - Bounce rate (25%): Lower is better (inverted)
        - Open rate (15%): Higher is better
        - Spam rate (20%): Lower is better (inverted)
        - Consistency (10%): Regular sending patterns
        """
        logger.info(f"Calculating health score for candidate {candidate_id}")

        # Get warming config and logs
        config = self.db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.candidate_id == candidate_id
        ).first()

        if not config:
            logger.warning(f"No warming config found for candidate {candidate_id}")
            return self._create_default_health_score(candidate_id)

        # Get last 7 days of logs
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        recent_logs = self.db.query(EmailWarmingDailyLog).filter(
            EmailWarmingDailyLog.config_id == config.id,
            EmailWarmingDailyLog.date >= seven_days_ago
        ).order_by(desc(EmailWarmingDailyLog.date)).all()

        # Calculate raw metrics
        metrics = self._calculate_metrics(recent_logs, config)

        # Calculate component scores
        delivery_score = self._calculate_delivery_score(metrics["delivery_rate"])
        bounce_score = self._calculate_bounce_score(metrics["bounce_rate"])
        open_score = self._calculate_open_score(metrics["open_rate"])
        spam_score = self._calculate_spam_score(metrics["spam_rate"])
        consistency_score = self._calculate_consistency_score(recent_logs)

        # Calculate weighted overall score
        overall_score = (
            delivery_score * HEALTH_SCORE_WEIGHTS["delivery_rate"] +
            bounce_score * HEALTH_SCORE_WEIGHTS["bounce_rate"] +
            open_score * HEALTH_SCORE_WEIGHTS["open_rate"] +
            spam_score * HEALTH_SCORE_WEIGHTS["spam_rate"] +
            consistency_score * HEALTH_SCORE_WEIGHTS["consistency"]
        )

        # Determine health status
        health_status = self._get_health_status(overall_score)

        # Get previous score for trend analysis
        previous_score = self.db.query(WarmupHealthScore).filter(
            WarmupHealthScore.candidate_id == candidate_id
        ).order_by(desc(WarmupHealthScore.score_date)).first()

        score_trend = self._calculate_trend(
            overall_score,
            previous_score.overall_score if previous_score else 100.0
        )

        # Calculate 7-day averages
        avg_scores = self._calculate_7day_averages(candidate_id)

        # Generate recommendations
        recommendations = self._generate_recommendations(
            metrics, delivery_score, bounce_score, spam_score, consistency_score
        )

        # Create health score record
        health_score = WarmupHealthScore(
            candidate_id=candidate_id,
            score_date=datetime.now(timezone.utc),
            overall_score=round(overall_score, 2),
            health_status=health_status.value,
            delivery_score=round(delivery_score, 2),
            bounce_score=round(bounce_score, 2),
            open_score=round(open_score, 2),
            spam_score=round(spam_score, 2),
            consistency_score=round(consistency_score, 2),
            delivery_rate=round(metrics["delivery_rate"], 2),
            bounce_rate=round(metrics["bounce_rate"], 2),
            open_rate=round(metrics["open_rate"], 2),
            spam_rate=round(metrics["spam_rate"], 4),
            emails_sent=metrics["total_sent"],
            emails_delivered=metrics["total_delivered"],
            emails_bounced=metrics["total_bounced"],
            spam_complaints=metrics["spam_complaints"],
            score_trend=score_trend,
            avg_7day_score=avg_scores.get("score"),
            avg_7day_delivery=avg_scores.get("delivery"),
            avg_7day_bounce=avg_scores.get("bounce"),
            recommendations=recommendations
        )

        try:
            self.db.add(health_score)
            self.db.commit()
            self.db.refresh(health_score)
        except Exception as e:
            self.db.rollback()
            logger.error(f"[WarmupHealthTracker] Failed to save health score for candidate {candidate_id}: {e}")
            raise ValueError(f"Failed to save health score: {e}")

        # Check for alerts
        self._check_and_create_alerts(candidate_id, metrics, health_score)

        # Check for milestones
        self._check_milestones(candidate_id, config, metrics, health_score)

        logger.info(f"Health score calculated: {overall_score:.1f} ({health_status.value})")
        return health_score

    def _calculate_metrics(
        self,
        logs: List[EmailWarmingDailyLog],
        config: EmailWarmingConfig
    ) -> Dict:
        """Calculate raw metrics from logs"""
        logger.debug(f"[WarmupHealthTracker] Calculating metrics from {len(logs)} log entries")

        if not logs:
            logger.debug("[WarmupHealthTracker] No logs found, returning default metrics")
            return {
                "delivery_rate": 100.0,
                "bounce_rate": 0.0,
                "open_rate": 0.0,
                "spam_rate": 0.0,
                "total_sent": 0,
                "total_delivered": 0,
                "total_bounced": 0,
                "spam_complaints": 0,
                "days_active": 0
            }

        total_sent = sum(log.emails_sent for log in logs)
        total_delivered = sum(log.emails_delivered for log in logs)
        total_bounced = sum(log.emails_bounced for log in logs)
        total_opened = sum(getattr(log, 'emails_opened', 0) or 0 for log in logs)
        spam_complaints = 0  # Would come from provider feedback

        # Division by zero protection
        delivery_rate = (total_delivered / total_sent * 100) if total_sent > 0 else 100.0
        bounce_rate = (total_bounced / total_sent * 100) if total_sent > 0 else 0.0
        open_rate = (total_opened / total_delivered * 100) if total_delivered > 0 else 0.0
        spam_rate = (spam_complaints / total_sent * 100) if total_sent > 0 else 0.0

        logger.debug(f"[WarmupHealthTracker] Metrics calculated - Sent: {total_sent}, Delivered: {total_delivered}, Bounced: {total_bounced}")
        logger.debug(f"[WarmupHealthTracker] Rates - Delivery: {delivery_rate:.1f}%, Bounce: {bounce_rate:.1f}%, Open: {open_rate:.1f}%")

        return {
            "delivery_rate": delivery_rate,
            "bounce_rate": bounce_rate,
            "open_rate": open_rate,
            "spam_rate": spam_rate,
            "total_sent": total_sent,
            "total_delivered": total_delivered,
            "total_bounced": total_bounced,
            "spam_complaints": spam_complaints,
            "days_active": len(logs)
        }

    def _calculate_delivery_score(self, delivery_rate: float) -> float:
        """Convert delivery rate to score (0-100)"""
        # 100% delivery = 100 score
        # 95% delivery = 80 score
        # 90% delivery = 50 score
        # Below 85% = linear decrease
        if delivery_rate >= 99:
            return 100.0
        elif delivery_rate >= 97:
            return 95.0
        elif delivery_rate >= 95:
            return 85.0
        elif delivery_rate >= 90:
            return 70.0
        elif delivery_rate >= 85:
            return 50.0
        else:
            return max(0, delivery_rate * 0.5)

    def _calculate_bounce_score(self, bounce_rate: float) -> float:
        """Convert bounce rate to score (0-100, inverted)"""
        # 0% bounce = 100 score
        # 1% bounce = 90 score
        # 2% bounce = 70 score
        # 5% bounce = 30 score
        # Above 10% = 0 score
        if bounce_rate <= 0.5:
            return 100.0
        elif bounce_rate <= 1.0:
            return 90.0
        elif bounce_rate <= 2.0:
            return 70.0
        elif bounce_rate <= 3.0:
            return 50.0
        elif bounce_rate <= 5.0:
            return 30.0
        elif bounce_rate <= 10.0:
            return 10.0
        else:
            return 0.0

    def _calculate_open_score(self, open_rate: float) -> float:
        """Convert open rate to score (0-100)"""
        # Industry average is ~20%, so:
        # 30%+ = excellent
        # 20-30% = good
        # 10-20% = fair
        # Below 10% = needs improvement
        if open_rate >= 35:
            return 100.0
        elif open_rate >= 25:
            return 85.0
        elif open_rate >= 20:
            return 70.0
        elif open_rate >= 15:
            return 55.0
        elif open_rate >= 10:
            return 40.0
        else:
            return 50.0  # Neutral if we can't track opens

    def _calculate_spam_score(self, spam_rate: float) -> float:
        """Convert spam rate to score (0-100, inverted)"""
        # 0% spam = 100 score
        # 0.05% = 80 score
        # 0.1% = 50 score (critical threshold)
        # Above 0.2% = 0 score
        if spam_rate <= 0.01:
            return 100.0
        elif spam_rate <= 0.03:
            return 90.0
        elif spam_rate <= 0.05:
            return 75.0
        elif spam_rate <= 0.1:
            return 50.0
        elif spam_rate <= 0.2:
            return 20.0
        else:
            return 0.0

    def _calculate_consistency_score(self, logs: List[EmailWarmingDailyLog]) -> float:
        """Calculate consistency score based on sending patterns"""
        if len(logs) < 2:
            return 100.0  # New accounts start with perfect consistency

        # Check for gaps in sending (missed days)
        expected_days = 7
        actual_days = len(logs)
        gap_penalty = (expected_days - actual_days) * 10

        # Check for volume consistency (not too spiky)
        volumes = [log.emails_sent for log in logs if log.emails_sent > 0]
        if not volumes:
            return 100.0

        avg_volume = sum(volumes) / len(volumes)
        variance = sum((v - avg_volume) ** 2 for v in volumes) / len(volumes)
        cv = (variance ** 0.5 / avg_volume) if avg_volume > 0 else 0  # Coefficient of variation

        # CV below 0.5 is good, above 1.0 is concerning
        if cv <= 0.3:
            volume_score = 100.0
        elif cv <= 0.5:
            volume_score = 80.0
        elif cv <= 0.75:
            volume_score = 60.0
        elif cv <= 1.0:
            volume_score = 40.0
        else:
            volume_score = 20.0

        return max(0, min(100, volume_score - gap_penalty))

    def _get_health_status(self, score: float) -> HealthStatusEnum:
        """Convert score to health status"""
        if score >= 90:
            return HealthStatusEnum.EXCELLENT
        elif score >= 70:
            return HealthStatusEnum.GOOD
        elif score >= 50:
            return HealthStatusEnum.FAIR
        elif score >= 30:
            return HealthStatusEnum.POOR
        else:
            return HealthStatusEnum.CRITICAL

    def _calculate_trend(self, current: float, previous: float) -> int:
        """Calculate trend direction"""
        diff = current - previous
        if diff >= 5:
            return 1  # Improving
        elif diff <= -5:
            return -1  # Declining
        else:
            return 0  # Stable

    def _calculate_7day_averages(self, candidate_id: int) -> Dict:
        """Calculate 7-day rolling averages using aggregation (optimized - no N+1 query)"""
        logger.debug(f"[WarmupHealthTracker] Calculating 7-day averages for candidate {candidate_id}")
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

        # Use aggregation to avoid N+1 query - single query instead of fetching all records
        averages = self.db.query(
            func.avg(WarmupHealthScore.overall_score).label('avg_score'),
            func.avg(WarmupHealthScore.delivery_rate).label('avg_delivery'),
            func.avg(WarmupHealthScore.bounce_rate).label('avg_bounce'),
            func.count(WarmupHealthScore.id).label('count')
        ).filter(
            WarmupHealthScore.candidate_id == candidate_id,
            WarmupHealthScore.score_date >= seven_days_ago
        ).first()

        if not averages or not averages.count:
            logger.debug(f"[WarmupHealthTracker] No scores found for 7-day average")
            return {}

        logger.debug(f"[WarmupHealthTracker] 7-day averages calculated from {averages.count} records")
        return {
            "score": float(averages.avg_score) if averages.avg_score else 0.0,
            "delivery": float(averages.avg_delivery) if averages.avg_delivery else 0.0,
            "bounce": float(averages.avg_bounce) if averages.avg_bounce else 0.0
        }

    def _generate_recommendations(
        self,
        metrics: Dict,
        delivery_score: float,
        bounce_score: float,
        spam_score: float,
        consistency_score: float
    ) -> List[Dict]:
        """Generate actionable recommendations based on scores"""
        recommendations = []
        priority = 1

        # Critical issues first
        if bounce_score < 30:
            recommendations.append({
                "priority": priority,
                "action": "Clean your email list immediately",
                "reason": f"Bounce rate of {metrics['bounce_rate']:.1f}% is damaging your reputation",
                "impact": "critical",
                "icon": "alert-triangle"
            })
            priority += 1

        if spam_score < 50:
            recommendations.append({
                "priority": priority,
                "action": "Review email content and sending practices",
                "reason": "Spam complaints are too high",
                "impact": "critical",
                "icon": "shield-alert"
            })
            priority += 1

        if delivery_score < 70:
            recommendations.append({
                "priority": priority,
                "action": "Slow down sending volume",
                "reason": f"Delivery rate of {metrics['delivery_rate']:.1f}% needs improvement",
                "impact": "high",
                "icon": "trending-down"
            })
            priority += 1

        # Improvement suggestions
        if consistency_score < 70:
            recommendations.append({
                "priority": priority,
                "action": "Send emails more consistently",
                "reason": "Irregular sending patterns hurt reputation",
                "impact": "medium",
                "icon": "calendar"
            })
            priority += 1

        if metrics["total_sent"] < 10:
            recommendations.append({
                "priority": priority,
                "action": "Continue warming - stay consistent",
                "reason": "Early stage warming requires patience",
                "impact": "info",
                "icon": "clock"
            })
            priority += 1

        # Positive reinforcement
        if delivery_score >= 90 and bounce_score >= 90:
            recommendations.append({
                "priority": priority,
                "action": "Keep up the great work!",
                "reason": "Your email health is excellent",
                "impact": "positive",
                "icon": "check-circle"
            })

        return recommendations

    def _create_default_health_score(self, candidate_id: int) -> WarmupHealthScore:
        """Create a default health score for new accounts"""
        logger.debug(f"[WarmupHealthTracker] Creating default health score for candidate {candidate_id}")
        health_score = WarmupHealthScore(
            candidate_id=candidate_id,
            score_date=datetime.now(timezone.utc),
            overall_score=100.0,
            health_status=HealthStatusEnum.EXCELLENT.value,
            delivery_score=100.0,
            bounce_score=100.0,
            open_score=50.0,
            spam_score=100.0,
            consistency_score=100.0,
            recommendations=[{
                "priority": 1,
                "action": "Start your warming campaign",
                "reason": "Begin sending emails to build reputation",
                "impact": "info",
                "icon": "rocket"
            }]
        )
        try:
            self.db.add(health_score)
            self.db.commit()
            self.db.refresh(health_score)
            logger.info(f"[WarmupHealthTracker] Created default health score for candidate {candidate_id}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[WarmupHealthTracker] Failed to create default health score for candidate {candidate_id}: {e}")
            raise ValueError(f"Failed to create default health score: {e}")
        return health_score

    # ============== Alert Management ==============

    def _check_and_create_alerts(
        self,
        candidate_id: int,
        metrics: Dict,
        health_score: WarmupHealthScore
    ) -> List[WarmupHealthAlert]:
        """Check metrics and create alerts if thresholds exceeded"""
        alerts = []

        # Check bounce rate
        if metrics["bounce_rate"] >= ALERT_THRESHOLDS["bounce_rate_critical"]:
            alert = self._create_alert(
                candidate_id=candidate_id,
                alert_type=AlertTypeEnum.HIGH_BOUNCE_RATE,
                severity=AlertSeverityEnum.CRITICAL,
                title="Critical: High Bounce Rate",
                message=f"Your bounce rate of {metrics['bounce_rate']:.1f}% exceeds the critical threshold of {ALERT_THRESHOLDS['bounce_rate_critical']}%. This is severely damaging your sender reputation.",
                context={
                    "metric": "bounce_rate",
                    "current_value": metrics["bounce_rate"],
                    "threshold": ALERT_THRESHOLDS["bounce_rate_critical"]
                },
                actions=[
                    {"action": "Pause warming campaign immediately", "priority": 1},
                    {"action": "Clean your email list", "priority": 2},
                    {"action": "Verify email addresses before adding", "priority": 3}
                ]
            )
            if alert:
                alerts.append(alert)

        elif metrics["bounce_rate"] >= ALERT_THRESHOLDS["bounce_rate_warning"]:
            alert = self._create_alert(
                candidate_id=candidate_id,
                alert_type=AlertTypeEnum.HIGH_BOUNCE_RATE,
                severity=AlertSeverityEnum.WARNING,
                title="Warning: Elevated Bounce Rate",
                message=f"Your bounce rate of {metrics['bounce_rate']:.1f}% is above the recommended {ALERT_THRESHOLDS['bounce_rate_warning']}%.",
                context={
                    "metric": "bounce_rate",
                    "current_value": metrics["bounce_rate"],
                    "threshold": ALERT_THRESHOLDS["bounce_rate_warning"]
                },
                actions=[
                    {"action": "Review recent bounced emails", "priority": 1},
                    {"action": "Consider reducing sending volume", "priority": 2}
                ]
            )
            if alert:
                alerts.append(alert)

        # Check delivery rate
        if metrics["delivery_rate"] < ALERT_THRESHOLDS["delivery_rate_critical"]:
            alert = self._create_alert(
                candidate_id=candidate_id,
                alert_type=AlertTypeEnum.DELIVERY_FAILURE_SPIKE,
                severity=AlertSeverityEnum.CRITICAL,
                title="Critical: Low Delivery Rate",
                message=f"Only {metrics['delivery_rate']:.1f}% of your emails are being delivered.",
                context={
                    "metric": "delivery_rate",
                    "current_value": metrics["delivery_rate"],
                    "threshold": ALERT_THRESHOLDS["delivery_rate_critical"]
                },
                actions=[
                    {"action": "Check SMTP configuration", "priority": 1},
                    {"action": "Verify domain authentication (SPF/DKIM)", "priority": 2}
                ]
            )
            if alert:
                alerts.append(alert)

        # Check for reputation drop
        if health_score.score_trend == -1 and health_score.overall_score < 70:
            alert = self._create_alert(
                candidate_id=candidate_id,
                alert_type=AlertTypeEnum.REPUTATION_DROP,
                severity=AlertSeverityEnum.WARNING,
                title="Reputation Declining",
                message=f"Your sender reputation score has dropped to {health_score.overall_score:.0f}.",
                context={
                    "metric": "overall_score",
                    "current_value": health_score.overall_score,
                    "trend": "declining"
                },
                actions=[
                    {"action": "Review sending practices", "priority": 1},
                    {"action": "Reduce volume temporarily", "priority": 2}
                ]
            )
            if alert:
                alerts.append(alert)

        # Auto-resolve improved alerts
        self._auto_resolve_alerts(candidate_id, metrics)

        return alerts

    def _create_alert(
        self,
        candidate_id: int,
        alert_type: AlertTypeEnum,
        severity: AlertSeverityEnum,
        title: str,
        message: str,
        context: Dict,
        actions: List[Dict]
    ) -> Optional[WarmupHealthAlert]:
        """Create an alert if not already exists (unresolved)"""
        # Check for existing unresolved alert of same type
        existing = self.db.query(WarmupHealthAlert).filter(
            WarmupHealthAlert.candidate_id == candidate_id,
            WarmupHealthAlert.alert_type == alert_type.value,
            WarmupHealthAlert.is_resolved == False
        ).first()

        if existing:
            # Update context with new values
            existing.context = context
            existing.triggered_at = datetime.now(timezone.utc)
            try:
                self.db.commit()
                logger.debug(f"[WarmupHealthTracker] Updated existing alert {existing.id}")
            except Exception as e:
                self.db.rollback()
                logger.error(f"[WarmupHealthTracker] Failed to update alert: {e}")
            return None

        alert = WarmupHealthAlert(
            candidate_id=candidate_id,
            alert_type=alert_type.value,
            severity=severity.value,
            title=title,
            message=message,
            context=context,
            recommended_actions=actions
        )
        try:
            self.db.add(alert)
            self.db.commit()
            self.db.refresh(alert)
            logger.info(f"[WarmupHealthTracker] Created alert: {title} (ID: {alert.id})")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[WarmupHealthTracker] Failed to create alert '{title}': {e}")
            return None
        return alert

    def _auto_resolve_alerts(self, candidate_id: int, metrics: Dict) -> None:
        """Auto-resolve alerts when metrics improve"""
        # Get unresolved alerts
        alerts = self.db.query(WarmupHealthAlert).filter(
            WarmupHealthAlert.candidate_id == candidate_id,
            WarmupHealthAlert.is_resolved == False,
            WarmupHealthAlert.auto_resolve_on_improvement == True
        ).all()

        for alert in alerts:
            should_resolve = False
            resolution_note = ""

            if alert.alert_type == AlertTypeEnum.HIGH_BOUNCE_RATE.value:
                if metrics["bounce_rate"] < ALERT_THRESHOLDS["bounce_rate_warning"]:
                    should_resolve = True
                    resolution_note = f"Bounce rate improved to {metrics['bounce_rate']:.1f}%"

            elif alert.alert_type == AlertTypeEnum.DELIVERY_FAILURE_SPIKE.value:
                if metrics["delivery_rate"] >= ALERT_THRESHOLDS["delivery_rate_warning"]:
                    should_resolve = True
                    resolution_note = f"Delivery rate improved to {metrics['delivery_rate']:.1f}%"

            if should_resolve:
                alert.is_resolved = True
                alert.resolved_at = datetime.now(timezone.utc)
                alert.resolved_by = "system"
                alert.resolution_note = resolution_note
                alert.severity = AlertSeverityEnum.RESOLVED.value
                logger.info(f"[WarmupHealthTracker] Auto-resolved alert {alert.id}: {resolution_note}")

        try:
            self.db.commit()
            logger.debug(f"[WarmupHealthTracker] Auto-resolve alerts completed for candidate {candidate_id}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[WarmupHealthTracker] Failed to auto-resolve alerts for candidate {candidate_id}: {e}")

    # ============== Milestone Tracking ==============

    def _check_milestones(
        self,
        candidate_id: int,
        config: EmailWarmingConfig,
        metrics: Dict,
        health_score: WarmupHealthScore
    ) -> List[WarmupMilestone]:
        """Check and award milestones"""
        achieved = []

        # Build stats dict for milestone checks
        stats = {
            "total_sent": config.total_emails_sent,
            "current_day": config.current_day,
            "status": config.status,
            "health_score": health_score.overall_score,
            "perfect_days": self._count_perfect_days(config.id),
            "bounce_free_days": self._count_bounce_free_days(config.id)
        }

        # Check each milestone
        for milestone_id, definition in MILESTONE_DEFINITIONS.items():
            # Check if already achieved
            existing = self.db.query(WarmupMilestone).filter(
                WarmupMilestone.candidate_id == candidate_id,
                WarmupMilestone.milestone_type == milestone_id
            ).first()

            if existing:
                continue

            # Check condition
            if definition["condition"](stats):
                milestone = WarmupMilestone(
                    candidate_id=candidate_id,
                    milestone_type=milestone_id,
                    title=definition["title"],
                    description=definition["description"],
                    badge_icon=definition["badge_icon"],
                    badge_color=definition["badge_color"],
                    achievement_data=stats
                )
                self.db.add(milestone)
                achieved.append(milestone)
                logger.info(f"[WarmupHealthTracker] Milestone achieved: {definition['title']}")

        if achieved:
            try:
                self.db.commit()
                logger.info(f"[WarmupHealthTracker] Saved {len(achieved)} new milestone(s) for candidate {candidate_id}")
            except Exception as e:
                self.db.rollback()
                logger.error(f"[WarmupHealthTracker] Failed to save milestones for candidate {candidate_id}: {e}")
                achieved = []

        return achieved

    def _count_perfect_days(self, config_id: int) -> int:
        """Count consecutive days with 100% delivery"""
        logs = self.db.query(EmailWarmingDailyLog).filter(
            EmailWarmingDailyLog.config_id == config_id,
            EmailWarmingDailyLog.delivery_rate >= 100.0
        ).order_by(desc(EmailWarmingDailyLog.date)).all()

        count = 0
        for log in logs:
            if log.delivery_rate >= 100.0:
                count += 1
            else:
                break
        return count

    def _count_bounce_free_days(self, config_id: int) -> int:
        """Count consecutive days with zero bounces"""
        logs = self.db.query(EmailWarmingDailyLog).filter(
            EmailWarmingDailyLog.config_id == config_id
        ).order_by(desc(EmailWarmingDailyLog.date)).all()

        count = 0
        for log in logs:
            if log.emails_bounced == 0:
                count += 1
            else:
                break
        return count

    # ============== Dashboard Data ==============

    def get_health_dashboard(self, candidate_id: int) -> Dict:
        """Get comprehensive health dashboard data"""
        logger.info(f"[WarmupHealthTracker] Getting health dashboard for candidate {candidate_id}")

        # Get latest health score
        latest_score = self.db.query(WarmupHealthScore).filter(
            WarmupHealthScore.candidate_id == candidate_id
        ).order_by(desc(WarmupHealthScore.score_date)).first()
        logger.debug(f"[WarmupHealthTracker] Latest health score: {latest_score.overall_score if latest_score else 'None'}")

        # Get warming config
        config = self.db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.candidate_id == candidate_id
        ).first()

        # Get unresolved alerts
        alerts = self.db.query(WarmupHealthAlert).filter(
            WarmupHealthAlert.candidate_id == candidate_id,
            WarmupHealthAlert.is_resolved == False
        ).order_by(desc(WarmupHealthAlert.triggered_at)).all()

        # Get milestones
        milestones = self.db.query(WarmupMilestone).filter(
            WarmupMilestone.candidate_id == candidate_id
        ).order_by(desc(WarmupMilestone.achieved_at)).all()

        # Get score history (last 30 days)
        thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
        score_history = self.db.query(WarmupHealthScore).filter(
            WarmupHealthScore.candidate_id == candidate_id,
            WarmupHealthScore.score_date >= thirty_days_ago
        ).order_by(WarmupHealthScore.score_date).all()

        # Get domain reputation
        domain_rep = self.db.query(DomainReputation).filter(
            DomainReputation.candidate_id == candidate_id
        ).first()

        return {
            "health_score": self._format_health_score(latest_score) if latest_score else None,
            "warming_status": self._format_warming_status(config) if config else None,
            "alerts": [self._format_alert(a) for a in alerts],
            "alert_counts": {
                "critical": len([a for a in alerts if a.severity == AlertSeverityEnum.CRITICAL.value]),
                "warning": len([a for a in alerts if a.severity == AlertSeverityEnum.WARNING.value]),
                "info": len([a for a in alerts if a.severity == AlertSeverityEnum.INFO.value])
            },
            "milestones": [self._format_milestone(m) for m in milestones],
            "score_history": [
                {
                    "date": s.score_date.isoformat(),
                    "score": s.overall_score,
                    "delivery_rate": s.delivery_rate,
                    "bounce_rate": s.bounce_rate
                }
                for s in score_history
            ],
            "domain_reputation": self._format_domain_reputation(domain_rep) if domain_rep else None,
            "quick_stats": self._get_quick_stats(latest_score, config)
        }

    def _calculate_score_breakdown(self, score: WarmupHealthScore, candidate_id: int) -> List[Dict]:
        """Calculate weighted sub-score breakdown for health metrics.

        Mirrors the logic previously in the frontend HealthScoreCard.
        Returns a list of breakdown items with category, score, weight, status, and tip.
        """
        # Inbox Placement: use delivery_rate (percentage of emails reaching inbox)
        inbox_rate = score.delivery_rate if score.delivery_rate is not None else 100.0
        if inbox_rate >= 90:
            inbox_status = "excellent"
            inbox_tip = "Excellent inbox delivery rate"
        elif inbox_rate >= 75:
            inbox_status = "good"
            inbox_tip = "Good inbox delivery rate with room for improvement"
        elif inbox_rate >= 60:
            inbox_status = "warning"
            inbox_tip = "Increase warmup volume gradually to improve"
        else:
            inbox_status = "critical"
            inbox_tip = "Increase warmup volume gradually to improve"

        # Spam Rate: convert spam_rate to an inverted score (lower spam = higher score)
        spam_rate = score.spam_rate if score.spam_rate is not None else 0.0
        spam_score_val = max(0.0, 100.0 - spam_rate * 10.0)
        if spam_rate <= 2:
            spam_status = "excellent"
            spam_tip = "Very low spam complaints"
        elif spam_rate <= 5:
            spam_status = "good"
            spam_tip = "Spam rate is acceptable"
        elif spam_rate <= 10:
            spam_status = "warning"
            spam_tip = "Consider reviewing email content and sending patterns"
        else:
            spam_status = "critical"
            spam_tip = "Consider reviewing email content and sending patterns"

        # Activity Level: check active warming configs for this candidate
        active_configs = self.db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.candidate_id == candidate_id,
            EmailWarmingConfig.status == WarmingStatusEnum.ACTIVE.value
        ).count()
        total_configs = self.db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.candidate_id == candidate_id
        ).count()
        activity_rate = (active_configs / total_configs * 100.0) if total_configs > 0 else 0.0

        if activity_rate >= 80:
            activity_status = "excellent"
            activity_tip = "Consistent warmup activity"
        elif activity_rate >= 60:
            activity_status = "good"
            activity_tip = "Good activity level"
        elif activity_rate >= 40:
            activity_status = "warning"
            activity_tip = "Increase active accounts for better results"
        else:
            activity_status = "critical"
            activity_tip = "Increase active accounts for better results"

        breakdown = [
            {
                "category": "Inbox Placement",
                "score": round(inbox_rate, 1),
                "weight": 40,
                "status": inbox_status,
                "tip": inbox_tip,
            },
            {
                "category": "Spam Rate",
                "score": round(spam_score_val, 1),
                "weight": 30,
                "status": spam_status,
                "tip": spam_tip,
            },
            {
                "category": "Activity Level",
                "score": round(activity_rate, 1),
                "weight": 30,
                "status": activity_status,
                "tip": activity_tip,
            },
        ]

        # Only include items with meaningful data
        return [item for item in breakdown if item["score"] > 0 or total_configs > 0]

    def _get_overall_status(self, score: float) -> str:
        """Derive a human-readable status label from overall score."""
        if score >= 90:
            return "excellent"
        if score >= 75:
            return "good"
        if score >= 60:
            return "fair"
        if score >= 40:
            return "poor"
        return "critical"

    def _format_health_score(self, score: WarmupHealthScore) -> Dict:
        """Format health score for API response"""
        breakdown = self._calculate_score_breakdown(score, score.candidate_id)

        # Derive individual top-level breakdown scores for convenience
        inbox_item = next((b for b in breakdown if b["category"] == "Inbox Placement"), None)
        spam_item = next((b for b in breakdown if b["category"] == "Spam Rate"), None)
        activity_item = next((b for b in breakdown if b["category"] == "Activity Level"), None)

        return {
            "overall_score": score.overall_score,
            "health_status": score.health_status,
            "status": self._get_overall_status(score.overall_score),
            "status_color": self._get_status_color(score.health_status),
            "inbox_placement_score": inbox_item["score"] if inbox_item else 0,
            "spam_rate_score": spam_item["score"] if spam_item else 0,
            "activity_score": activity_item["score"] if activity_item else 0,
            "breakdown": breakdown,
            "components": {
                "delivery": {"score": score.delivery_score, "rate": score.delivery_rate},
                "bounce": {"score": score.bounce_score, "rate": score.bounce_rate},
                "open": {"score": score.open_score, "rate": score.open_rate},
                "spam": {"score": score.spam_score, "rate": score.spam_rate},
                "consistency": {"score": score.consistency_score}
            },
            "trends": {
                "score": score.score_trend,
                "delivery": score.delivery_trend,
                "bounce": score.bounce_trend
            },
            "averages_7day": {
                "score": score.avg_7day_score,
                "delivery": score.avg_7day_delivery,
                "bounce": score.avg_7day_bounce
            },
            "recommendations": score.recommendations or [],
            "updated_at": score.score_date.isoformat()
        }

    def _format_warming_status(self, config: EmailWarmingConfig) -> Dict:
        """Format warming status for API response"""
        from app.services.email_warming_service import EmailWarmingService
        daily_limit = EmailWarmingService.get_daily_limit(config)
        max_day = EmailWarmingService.get_max_day_for_strategy(config.strategy)

        return {
            "status": config.status,
            "strategy": config.strategy,
            "current_day": config.current_day,
            "max_day": max_day,
            "progress_percent": int((config.current_day / max_day) * 100) if max_day > 0 else 0,
            "daily_limit": daily_limit,
            "sent_today": config.emails_sent_today,
            "remaining_today": max(0, daily_limit - config.emails_sent_today),
            "total_sent": config.total_emails_sent,
            "start_date": config.start_date.isoformat() if config.start_date else None
        }

    def _format_alert(self, alert: WarmupHealthAlert) -> Dict:
        """Format alert for API response"""
        return {
            "id": alert.id,
            "type": alert.alert_type,
            "severity": alert.severity,
            "severity_color": self._get_severity_color(alert.severity),
            "title": alert.title,
            "message": alert.message,
            "context": alert.context,
            "recommended_actions": alert.recommended_actions,
            "is_read": alert.is_read,
            "triggered_at": alert.triggered_at.isoformat()
        }

    def _format_milestone(self, milestone: WarmupMilestone) -> Dict:
        """Format milestone for API response"""
        return {
            "id": milestone.id,
            "type": milestone.milestone_type,
            "title": milestone.title,
            "description": milestone.description,
            "badge_icon": milestone.badge_icon,
            "badge_color": milestone.badge_color,
            "achieved_at": milestone.achieved_at.isoformat()
        }

    def _format_domain_reputation(self, rep: DomainReputation) -> Dict:
        """Format domain reputation for API response"""
        return {
            "domain": rep.domain,
            "overall_reputation": rep.overall_reputation,
            "authentication": {
                "spf": rep.spf_configured,
                "dkim": rep.dkim_configured,
                "dmarc": rep.dmarc_configured,
                "score": rep.authentication_score
            },
            "blacklist": {
                "is_blacklisted": rep.is_blacklisted,
                "sources": rep.blacklist_sources,
                "last_check": rep.last_blacklist_check.isoformat() if rep.last_blacklist_check else None
            },
            "lifetime_stats": {
                "emails_sent": rep.total_emails_sent,
                "delivery_rate": rep.lifetime_delivery_rate,
                "bounce_rate": rep.lifetime_bounce_rate
            }
        }

    def _get_quick_stats(self, score: WarmupHealthScore, config: EmailWarmingConfig) -> Dict:
        """Get quick stats for dashboard header"""
        if not score:
            return {
                "health_emoji": "🆕",
                "health_label": "New Account",
                "tip": "Start warming to build reputation"
            }

        health_emojis = {
            HealthStatusEnum.EXCELLENT.value: "🌟",
            HealthStatusEnum.GOOD.value: "✅",
            HealthStatusEnum.FAIR.value: "⚠️",
            HealthStatusEnum.POOR.value: "🔻",
            HealthStatusEnum.CRITICAL.value: "🚨"
        }

        health_labels = {
            HealthStatusEnum.EXCELLENT.value: "Excellent",
            HealthStatusEnum.GOOD.value: "Good",
            HealthStatusEnum.FAIR.value: "Needs Attention",
            HealthStatusEnum.POOR.value: "Poor",
            HealthStatusEnum.CRITICAL.value: "Critical"
        }

        tips = {
            HealthStatusEnum.EXCELLENT.value: "Keep up the great sending practices!",
            HealthStatusEnum.GOOD.value: "Your reputation is solid.",
            HealthStatusEnum.FAIR.value: "Review recommendations to improve.",
            HealthStatusEnum.POOR.value: "Action needed to protect reputation.",
            HealthStatusEnum.CRITICAL.value: "Immediate action required!"
        }

        return {
            "health_emoji": health_emojis.get(score.health_status, "❓"),
            "health_label": health_labels.get(score.health_status, "Unknown"),
            "tip": tips.get(score.health_status, ""),
            "score": score.overall_score,
            "trend_emoji": "📈" if score.score_trend == 1 else ("📉" if score.score_trend == -1 else "➡️")
        }

    def _get_status_color(self, status: str) -> str:
        """Get color for health status"""
        colors = {
            HealthStatusEnum.EXCELLENT.value: "emerald",
            HealthStatusEnum.GOOD.value: "green",
            HealthStatusEnum.FAIR.value: "yellow",
            HealthStatusEnum.POOR.value: "orange",
            HealthStatusEnum.CRITICAL.value: "red"
        }
        return colors.get(status, "gray")

    def _get_severity_color(self, severity: str) -> str:
        """Get color for alert severity"""
        colors = {
            AlertSeverityEnum.INFO.value: "blue",
            AlertSeverityEnum.WARNING.value: "yellow",
            AlertSeverityEnum.CRITICAL.value: "red",
            AlertSeverityEnum.RESOLVED.value: "green"
        }
        return colors.get(severity, "gray")

    # ============== Alert Actions ==============

    def mark_alert_read(self, alert_id: int) -> WarmupHealthAlert:
        """Mark an alert as read"""
        logger.debug(f"[WarmupHealthTracker] Marking alert {alert_id} as read")
        alert = self.db.query(WarmupHealthAlert).get(alert_id)
        if alert:
            alert.is_read = True
            try:
                self.db.commit()
                self.db.refresh(alert)
                logger.info(f"[WarmupHealthTracker] Alert {alert_id} marked as read")
            except Exception as e:
                self.db.rollback()
                logger.error(f"[WarmupHealthTracker] Failed to mark alert {alert_id} as read: {e}")
        else:
            logger.warning(f"[WarmupHealthTracker] Alert {alert_id} not found")
        return alert

    def resolve_alert(self, alert_id: int, note: str = None) -> WarmupHealthAlert:
        """Manually resolve an alert"""
        logger.info(f"[WarmupHealthTracker] Resolving alert {alert_id}")
        alert = self.db.query(WarmupHealthAlert).get(alert_id)
        if alert:
            alert.is_resolved = True
            alert.resolved_at = datetime.now(timezone.utc)
            alert.resolved_by = "user"
            alert.resolution_note = note
            alert.severity = AlertSeverityEnum.RESOLVED.value
            try:
                self.db.commit()
                self.db.refresh(alert)
                logger.info(f"[WarmupHealthTracker] Alert {alert_id} resolved")
            except Exception as e:
                self.db.rollback()
                logger.error(f"[WarmupHealthTracker] Failed to resolve alert {alert_id}: {e}")
        else:
            logger.warning(f"[WarmupHealthTracker] Alert {alert_id} not found for resolution")
        return alert

    def get_alerts(
        self,
        candidate_id: int,
        include_resolved: bool = False,
        severity: str = None
    ) -> List[WarmupHealthAlert]:
        """Get alerts for a candidate"""
        query = self.db.query(WarmupHealthAlert).filter(
            WarmupHealthAlert.candidate_id == candidate_id
        )

        if not include_resolved:
            query = query.filter(WarmupHealthAlert.is_resolved == False)

        if severity:
            query = query.filter(WarmupHealthAlert.severity == severity)

        return query.order_by(desc(WarmupHealthAlert.triggered_at)).all()
