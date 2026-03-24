"""
Warmup Advanced Analytics Engine - PHASE 5 GOD TIER EDITION

Comprehensive analytics and reporting system for warmup performance.

Features:
- Multi-dimensional KPI tracking
- Cohort analysis for sender reputation
- Funnel analysis for warmup progression
- Comparative benchmarking
- Predictive analytics integration
- Automated report generation
- Real-time anomaly detection

Author: Metaminds AI
Version: 5.0.0 - ULTRA GOD TIER ANALYTICS
"""

import logging
import uuid
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict
import math
import json

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

class TimeGranularity(str, Enum):
    """Time granularity for analytics"""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class MetricCategory(str, Enum):
    """Metric categories"""
    VOLUME = "volume"
    ENGAGEMENT = "engagement"
    DELIVERABILITY = "deliverability"
    REPUTATION = "reputation"
    EFFICIENCY = "efficiency"


class TrendDirection(str, Enum):
    """Trend direction"""
    UP = "up"
    DOWN = "down"
    STABLE = "stable"
    VOLATILE = "volatile"


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class TimeSeriesDataPoint:
    """Single data point in time series"""
    timestamp: datetime
    value: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "metadata": self.metadata,
        }


@dataclass
class MetricDefinition:
    """Definition of a trackable metric"""
    id: str
    name: str
    description: str
    category: MetricCategory
    unit: str
    aggregation: str  # sum, avg, max, min, last
    higher_is_better: bool = True
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "unit": self.unit,
            "aggregation": self.aggregation,
            "higher_is_better": self.higher_is_better,
            "warning_threshold": self.warning_threshold,
            "critical_threshold": self.critical_threshold,
        }


@dataclass
class KPISnapshot:
    """Snapshot of key performance indicators"""
    timestamp: datetime
    metrics: Dict[str, float]
    trends: Dict[str, TrendDirection]
    anomalies: List[str]
    score: float  # Overall health score

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "metrics": self.metrics,
            "trends": {k: v.value for k, v in self.trends.items()},
            "anomalies": self.anomalies,
            "score": round(self.score, 2),
        }


@dataclass
class CohortDefinition:
    """Cohort definition for analysis"""
    id: str
    name: str
    description: str
    filter_criteria: Dict[str, Any]
    created_at: datetime
    member_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "filter_criteria": self.filter_criteria,
            "created_at": self.created_at.isoformat(),
            "member_count": self.member_count,
        }


@dataclass
class CohortAnalysis:
    """Results of cohort analysis"""
    cohort_id: str
    cohort_name: str
    period: str
    metrics: Dict[str, float]
    comparison_to_baseline: Dict[str, float]  # Percentage difference
    retention_curve: List[float]
    insights: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cohort_id": self.cohort_id,
            "cohort_name": self.cohort_name,
            "period": self.period,
            "metrics": self.metrics,
            "comparison_to_baseline": self.comparison_to_baseline,
            "retention_curve": self.retention_curve,
            "insights": self.insights,
        }


@dataclass
class FunnelStage:
    """Stage in a funnel"""
    name: str
    count: int
    conversion_rate: float  # From previous stage

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "count": self.count,
            "conversion_rate": round(self.conversion_rate, 4),
        }


@dataclass
class FunnelAnalysis:
    """Funnel analysis results"""
    funnel_id: str
    name: str
    stages: List[FunnelStage]
    overall_conversion: float
    bottleneck_stage: str
    recommendations: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "funnel_id": self.funnel_id,
            "name": self.name,
            "stages": [s.to_dict() for s in self.stages],
            "overall_conversion": round(self.overall_conversion, 4),
            "bottleneck_stage": self.bottleneck_stage,
            "recommendations": self.recommendations,
        }


@dataclass
class Benchmark:
    """Benchmark comparison data"""
    metric_id: str
    your_value: float
    industry_average: float
    industry_top_10: float
    industry_bottom_10: float
    percentile: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric_id": self.metric_id,
            "your_value": round(self.your_value, 4),
            "industry_average": round(self.industry_average, 4),
            "industry_top_10": round(self.industry_top_10, 4),
            "industry_bottom_10": round(self.industry_bottom_10, 4),
            "percentile": round(self.percentile, 1),
        }


@dataclass
class AnalyticsAlert:
    """Analytics-generated alert"""
    id: str
    severity: AlertSeverity
    metric_id: str
    message: str
    current_value: float
    threshold: float
    created_at: datetime
    acknowledged: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "severity": self.severity.value,
            "metric_id": self.metric_id,
            "message": self.message,
            "current_value": self.current_value,
            "threshold": self.threshold,
            "created_at": self.created_at.isoformat(),
            "acknowledged": self.acknowledged,
        }


@dataclass
class Report:
    """Generated analytics report"""
    id: str
    name: str
    period_start: datetime
    period_end: datetime
    generated_at: datetime
    kpi_snapshot: KPISnapshot
    cohort_analyses: List[CohortAnalysis]
    funnel_analyses: List[FunnelAnalysis]
    benchmarks: List[Benchmark]
    alerts: List[AnalyticsAlert]
    executive_summary: str
    detailed_insights: List[str]
    recommendations: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "kpi_snapshot": self.kpi_snapshot.to_dict(),
            "cohort_analyses": [c.to_dict() for c in self.cohort_analyses],
            "funnel_analyses": [f.to_dict() for f in self.funnel_analyses],
            "benchmarks": [b.to_dict() for b in self.benchmarks],
            "alerts": [a.to_dict() for a in self.alerts],
            "executive_summary": self.executive_summary,
            "detailed_insights": self.detailed_insights,
            "recommendations": self.recommendations,
        }


# ============================================================================
# PREDEFINED METRICS
# ============================================================================

WARMUP_METRICS = {
    # Volume Metrics
    "emails_sent": MetricDefinition(
        id="emails_sent",
        name="Emails Sent",
        description="Total warmup emails sent",
        category=MetricCategory.VOLUME,
        unit="emails",
        aggregation="sum",
    ),
    "emails_received": MetricDefinition(
        id="emails_received",
        name="Emails Received",
        description="Total warmup emails received",
        category=MetricCategory.VOLUME,
        unit="emails",
        aggregation="sum",
    ),
    "daily_volume": MetricDefinition(
        id="daily_volume",
        name="Daily Volume",
        description="Average daily email volume",
        category=MetricCategory.VOLUME,
        unit="emails/day",
        aggregation="avg",
    ),

    # Engagement Metrics
    "open_rate": MetricDefinition(
        id="open_rate",
        name="Open Rate",
        description="Percentage of emails opened",
        category=MetricCategory.ENGAGEMENT,
        unit="%",
        aggregation="avg",
        warning_threshold=0.15,
        critical_threshold=0.10,
    ),
    "reply_rate": MetricDefinition(
        id="reply_rate",
        name="Reply Rate",
        description="Percentage of emails replied to",
        category=MetricCategory.ENGAGEMENT,
        unit="%",
        aggregation="avg",
        warning_threshold=0.05,
        critical_threshold=0.02,
    ),
    "click_rate": MetricDefinition(
        id="click_rate",
        name="Click Rate",
        description="Percentage of emails with link clicks",
        category=MetricCategory.ENGAGEMENT,
        unit="%",
        aggregation="avg",
    ),

    # Deliverability Metrics
    "inbox_rate": MetricDefinition(
        id="inbox_rate",
        name="Inbox Placement Rate",
        description="Percentage landing in inbox",
        category=MetricCategory.DELIVERABILITY,
        unit="%",
        aggregation="avg",
        warning_threshold=0.85,
        critical_threshold=0.70,
    ),
    "spam_rate": MetricDefinition(
        id="spam_rate",
        name="Spam Rate",
        description="Percentage landing in spam",
        category=MetricCategory.DELIVERABILITY,
        unit="%",
        aggregation="avg",
        higher_is_better=False,
        warning_threshold=0.02,
        critical_threshold=0.05,
    ),
    "bounce_rate": MetricDefinition(
        id="bounce_rate",
        name="Bounce Rate",
        description="Percentage of bounced emails",
        category=MetricCategory.DELIVERABILITY,
        unit="%",
        aggregation="avg",
        higher_is_better=False,
        warning_threshold=0.05,
        critical_threshold=0.10,
    ),

    # Reputation Metrics
    "health_score": MetricDefinition(
        id="health_score",
        name="Health Score",
        description="Overall account health score",
        category=MetricCategory.REPUTATION,
        unit="points",
        aggregation="last",
        warning_threshold=60,
        critical_threshold=40,
    ),
    "sender_score": MetricDefinition(
        id="sender_score",
        name="Sender Score",
        description="Estimated sender reputation",
        category=MetricCategory.REPUTATION,
        unit="points",
        aggregation="last",
        warning_threshold=70,
        critical_threshold=50,
    ),
    "blacklist_count": MetricDefinition(
        id="blacklist_count",
        name="Blacklist Count",
        description="Number of blacklist listings",
        category=MetricCategory.REPUTATION,
        unit="listings",
        aggregation="last",
        higher_is_better=False,
        warning_threshold=1,
        critical_threshold=3,
    ),

    # Efficiency Metrics
    "warmup_progress": MetricDefinition(
        id="warmup_progress",
        name="Warmup Progress",
        description="Progress towards warmup completion",
        category=MetricCategory.EFFICIENCY,
        unit="%",
        aggregation="last",
    ),
    "volume_efficiency": MetricDefinition(
        id="volume_efficiency",
        name="Volume Efficiency",
        description="Actual vs planned volume ratio",
        category=MetricCategory.EFFICIENCY,
        unit="%",
        aggregation="avg",
    ),
}

# Industry benchmarks (simulated)
INDUSTRY_BENCHMARKS = {
    "open_rate": {"avg": 0.22, "top_10": 0.35, "bottom_10": 0.10},
    "reply_rate": {"avg": 0.08, "top_10": 0.15, "bottom_10": 0.02},
    "inbox_rate": {"avg": 0.88, "top_10": 0.98, "bottom_10": 0.65},
    "spam_rate": {"avg": 0.03, "top_10": 0.005, "bottom_10": 0.10},
    "bounce_rate": {"avg": 0.04, "top_10": 0.01, "bottom_10": 0.12},
    "health_score": {"avg": 72, "top_10": 95, "bottom_10": 45},
}


# ============================================================================
# ANALYTICS ENGINE
# ============================================================================

class WarmupAnalyticsEngine:
    """
    Advanced analytics engine for warmup performance analysis.

    Provides comprehensive tracking, analysis, and reporting
    capabilities for email warmup operations.
    """

    def __init__(self):
        self.time_series: Dict[str, Dict[str, List[TimeSeriesDataPoint]]] = defaultdict(lambda: defaultdict(list))
        self.cohorts: Dict[str, CohortDefinition] = {}
        self.alerts: Dict[str, List[AnalyticsAlert]] = defaultdict(list)
        self.reports: Dict[str, Report] = {}
        self._initialized = False

        logger.info("[WarmupAnalyticsEngine] Initialized")

    def _ensure_initialized(self):
        if not self._initialized:
            self._initialized = True

    # ========================================
    # Data Collection
    # ========================================

    def record_metric(
        self,
        account_id: str,
        metric_id: str,
        value: float,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Record a metric value"""
        self._ensure_initialized()

        if metric_id not in WARMUP_METRICS:
            logger.warning(f"Unknown metric: {metric_id}")
            return

        ts = timestamp or datetime.now(timezone.utc)
        data_point = TimeSeriesDataPoint(
            timestamp=ts,
            value=value,
            metadata=metadata or {},
        )

        self.time_series[account_id][metric_id].append(data_point)

        # Check for alerts
        self._check_metric_alerts(account_id, metric_id, value)

        # Trim old data (keep last 90 days)
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        self.time_series[account_id][metric_id] = [
            dp for dp in self.time_series[account_id][metric_id]
            if dp.timestamp > cutoff
        ]

    def record_bulk_metrics(
        self,
        account_id: str,
        metrics: Dict[str, float],
        timestamp: Optional[datetime] = None
    ):
        """Record multiple metrics at once"""
        for metric_id, value in metrics.items():
            self.record_metric(account_id, metric_id, value, timestamp)

    # ========================================
    # Time Series Analysis
    # ========================================

    def get_time_series(
        self,
        account_id: str,
        metric_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        granularity: TimeGranularity = TimeGranularity.DAILY
    ) -> List[Dict[str, Any]]:
        """Get time series data with specified granularity"""
        series = self.time_series.get(account_id, {}).get(metric_id, [])

        # Filter by time range
        if start_time:
            series = [dp for dp in series if dp.timestamp >= start_time]
        if end_time:
            series = [dp for dp in series if dp.timestamp <= end_time]

        # Aggregate by granularity
        return self._aggregate_time_series(series, metric_id, granularity)

    def _aggregate_time_series(
        self,
        series: List[TimeSeriesDataPoint],
        metric_id: str,
        granularity: TimeGranularity
    ) -> List[Dict[str, Any]]:
        """Aggregate time series by granularity"""
        if not series:
            return []

        metric_def = WARMUP_METRICS.get(metric_id)
        aggregation = metric_def.aggregation if metric_def else "avg"

        # Group by time bucket
        buckets: Dict[str, List[float]] = defaultdict(list)

        for dp in series:
            if granularity == TimeGranularity.HOURLY:
                key = dp.timestamp.strftime("%Y-%m-%d %H:00")
            elif granularity == TimeGranularity.DAILY:
                key = dp.timestamp.strftime("%Y-%m-%d")
            elif granularity == TimeGranularity.WEEKLY:
                # ISO week
                key = dp.timestamp.strftime("%Y-W%V")
            else:  # Monthly
                key = dp.timestamp.strftime("%Y-%m")

            buckets[key].append(dp.value)

        # Aggregate
        result = []
        for key, values in sorted(buckets.items()):
            if aggregation == "sum":
                agg_value = sum(values)
            elif aggregation == "max":
                agg_value = max(values)
            elif aggregation == "min":
                agg_value = min(values)
            elif aggregation == "last":
                agg_value = values[-1]
            else:  # avg
                agg_value = sum(values) / len(values)

            result.append({
                "period": key,
                "value": round(agg_value, 4),
                "count": len(values),
            })

        return result

    def calculate_trend(
        self,
        account_id: str,
        metric_id: str,
        days: int = 7
    ) -> TrendDirection:
        """Calculate trend direction for a metric"""
        series = self.time_series.get(account_id, {}).get(metric_id, [])

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        recent = [dp for dp in series if dp.timestamp > cutoff]

        if len(recent) < 3:
            return TrendDirection.STABLE

        # Calculate linear regression slope
        values = [dp.value for dp in recent]
        n = len(values)
        x_mean = (n - 1) / 2
        y_mean = sum(values) / n

        numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        slope = numerator / denominator if denominator != 0 else 0

        # Calculate coefficient of variation for volatility
        std = math.sqrt(sum((v - y_mean) ** 2 for v in values) / n)
        cv = std / y_mean if y_mean != 0 else 0

        if cv > 0.3:
            return TrendDirection.VOLATILE

        threshold = y_mean * 0.02  # 2% change threshold

        if slope > threshold:
            return TrendDirection.UP
        elif slope < -threshold:
            return TrendDirection.DOWN
        else:
            return TrendDirection.STABLE

    # ========================================
    # KPI Analysis
    # ========================================

    def get_kpi_snapshot(self, account_id: str) -> KPISnapshot:
        """Get current KPI snapshot"""
        metrics = {}
        trends = {}
        anomalies = []

        for metric_id in WARMUP_METRICS:
            # Get latest value
            series = self.time_series.get(account_id, {}).get(metric_id, [])
            if series:
                metrics[metric_id] = series[-1].value
            else:
                metrics[metric_id] = 0.0

            # Calculate trend
            trends[metric_id] = self.calculate_trend(account_id, metric_id)

            # Check for anomalies
            if self._is_anomaly(series):
                anomalies.append(metric_id)

        # Calculate overall score
        score = self._calculate_health_score(metrics)

        return KPISnapshot(
            timestamp=datetime.now(timezone.utc),
            metrics=metrics,
            trends=trends,
            anomalies=anomalies,
            score=score,
        )

    def _calculate_health_score(self, metrics: Dict[str, float]) -> float:
        """Calculate overall health score from metrics"""
        score = 0
        weights = {
            "inbox_rate": 25,
            "open_rate": 20,
            "reply_rate": 15,
            "bounce_rate": 20,  # Negative weight
            "spam_rate": 20,    # Negative weight
        }

        for metric_id, weight in weights.items():
            value = metrics.get(metric_id, 0)
            metric_def = WARMUP_METRICS.get(metric_id)

            if not metric_def:
                continue

            # Normalize to 0-1 scale
            if metric_id == "inbox_rate":
                normalized = min(value / 0.95, 1.0)
            elif metric_id == "open_rate":
                normalized = min(value / 0.30, 1.0)
            elif metric_id == "reply_rate":
                normalized = min(value / 0.10, 1.0)
            elif metric_id in ["bounce_rate", "spam_rate"]:
                # Inverse - lower is better
                normalized = max(0, 1 - (value / 0.10))
            else:
                normalized = value

            score += normalized * weight

        return min(100, max(0, score))

    def _is_anomaly(self, series: List[TimeSeriesDataPoint], threshold: float = 2.5) -> bool:
        """Detect if latest value is an anomaly using Z-score"""
        if len(series) < 10:
            return False

        values = [dp.value for dp in series[:-1]]  # Exclude latest
        latest = series[-1].value

        mean = sum(values) / len(values)
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))

        if std == 0:
            return False

        z_score = abs(latest - mean) / std
        return z_score > threshold

    # ========================================
    # Cohort Analysis
    # ========================================

    def create_cohort(
        self,
        name: str,
        description: str,
        filter_criteria: Dict[str, Any]
    ) -> CohortDefinition:
        """Create a new cohort for analysis"""
        cohort_id = str(uuid.uuid4())

        cohort = CohortDefinition(
            id=cohort_id,
            name=name,
            description=description,
            filter_criteria=filter_criteria,
            created_at=datetime.now(timezone.utc),
        )

        self.cohorts[cohort_id] = cohort

        logger.info(f"[Analytics] Created cohort: {cohort_id}")
        return cohort

    def analyze_cohort(
        self,
        cohort_id: str,
        metric_ids: List[str],
        period_days: int = 30
    ) -> CohortAnalysis:
        """Analyze a cohort's performance"""
        cohort = self.cohorts.get(cohort_id)
        if not cohort:
            raise ValueError(f"Cohort not found: {cohort_id}")

        # Aggregate metrics for cohort
        metrics = {}
        baseline = {}

        for metric_id in metric_ids:
            # This would normally filter accounts by cohort criteria
            # For now, simulate with averages
            metrics[metric_id] = self._get_cohort_metric(cohort, metric_id)
            baseline[metric_id] = INDUSTRY_BENCHMARKS.get(metric_id, {}).get("avg", 0)

        # Calculate comparison to baseline
        comparison = {}
        for metric_id, value in metrics.items():
            base = baseline.get(metric_id, 0)
            if base > 0:
                comparison[metric_id] = round((value - base) / base * 100, 2)
            else:
                comparison[metric_id] = 0

        # Generate retention curve (simulated)
        retention_curve = self._generate_retention_curve(period_days)

        # Generate insights
        insights = self._generate_cohort_insights(metrics, comparison)

        return CohortAnalysis(
            cohort_id=cohort_id,
            cohort_name=cohort.name,
            period=f"Last {period_days} days",
            metrics=metrics,
            comparison_to_baseline=comparison,
            retention_curve=retention_curve,
            insights=insights,
        )

    def _get_cohort_metric(self, cohort: CohortDefinition, metric_id: str) -> float:
        """Get aggregated metric for cohort"""
        # Simplified - would normally filter by cohort criteria
        total = 0
        count = 0

        for account_id, metrics in self.time_series.items():
            if metric_id in metrics and metrics[metric_id]:
                total += metrics[metric_id][-1].value
                count += 1

        return total / count if count > 0 else 0

    def _generate_retention_curve(self, days: int) -> List[float]:
        """Generate retention curve (simulated)"""
        # Exponential decay with some noise
        curve = []
        for day in range(min(days, 30)):
            retention = 100 * math.exp(-0.05 * day) + (5 * (0.5 - 0.5))
            curve.append(round(max(0, retention), 2))
        return curve

    def _generate_cohort_insights(
        self,
        metrics: Dict[str, float],
        comparison: Dict[str, float]
    ) -> List[str]:
        """Generate insights from cohort analysis"""
        insights = []

        for metric_id, diff in comparison.items():
            metric_def = WARMUP_METRICS.get(metric_id)
            if not metric_def:
                continue

            if diff > 20:
                insights.append(f"{metric_def.name} is {diff:.1f}% above industry average - excellent performance!")
            elif diff < -20:
                insights.append(f"{metric_def.name} is {abs(diff):.1f}% below industry average - needs improvement")

        if metrics.get("bounce_rate", 0) > 0.08:
            insights.append("High bounce rate detected - consider email list hygiene")

        if metrics.get("open_rate", 0) < 0.15:
            insights.append("Low open rate - optimize subject lines and send timing")

        return insights

    # ========================================
    # Funnel Analysis
    # ========================================

    def analyze_warmup_funnel(self, account_id: str) -> FunnelAnalysis:
        """Analyze warmup progression funnel"""
        # Define funnel stages
        stages_data = [
            ("Sent", "emails_sent"),
            ("Delivered", "inbox_rate"),
            ("Opened", "open_rate"),
            ("Replied", "reply_rate"),
        ]

        stages = []
        prev_count = None

        for stage_name, metric_id in stages_data:
            series = self.time_series.get(account_id, {}).get(metric_id, [])
            value = series[-1].value if series else 0

            if stage_name == "Sent":
                count = int(value)
            else:
                # Convert rate to count
                count = int(prev_count * value) if prev_count else 0

            conversion_rate = count / prev_count if prev_count and prev_count > 0 else 1.0
            stages.append(FunnelStage(
                name=stage_name,
                count=count,
                conversion_rate=conversion_rate,
            ))

            prev_count = count if count > 0 else prev_count

        # Calculate overall conversion
        if stages[0].count > 0 and len(stages) > 1:
            overall = stages[-1].count / stages[0].count
        else:
            overall = 0

        # Identify bottleneck
        bottleneck = min(stages[1:], key=lambda s: s.conversion_rate).name if len(stages) > 1 else "N/A"

        # Generate recommendations
        recommendations = self._generate_funnel_recommendations(stages)

        return FunnelAnalysis(
            funnel_id=f"{account_id}-warmup-funnel",
            name="Warmup Email Funnel",
            stages=stages,
            overall_conversion=overall,
            bottleneck_stage=bottleneck,
            recommendations=recommendations,
        )

    def _generate_funnel_recommendations(self, stages: List[FunnelStage]) -> List[str]:
        """Generate recommendations from funnel analysis"""
        recommendations = []

        for i, stage in enumerate(stages[1:], 1):
            if stage.conversion_rate < 0.5:
                prev_stage = stages[i - 1].name
                recommendations.append(
                    f"Significant drop at {stage.name} stage (from {prev_stage}). "
                    f"Only {stage.conversion_rate:.1%} conversion rate."
                )

        if not recommendations:
            recommendations.append("Funnel performance is healthy. Maintain current practices.")

        return recommendations

    # ========================================
    # Benchmarking
    # ========================================

    def get_benchmarks(self, account_id: str) -> List[Benchmark]:
        """Get benchmark comparisons for account"""
        benchmarks = []

        for metric_id, bench_data in INDUSTRY_BENCHMARKS.items():
            series = self.time_series.get(account_id, {}).get(metric_id, [])
            your_value = series[-1].value if series else 0

            # Calculate percentile
            avg = bench_data["avg"]
            top = bench_data["top_10"]
            bottom = bench_data["bottom_10"]

            if your_value >= top:
                percentile = 90 + (your_value - top) / (1 - top) * 10 if top < 1 else 95
            elif your_value >= avg:
                percentile = 50 + (your_value - avg) / (top - avg) * 40 if top > avg else 50
            elif your_value >= bottom:
                percentile = 10 + (your_value - bottom) / (avg - bottom) * 40 if avg > bottom else 10
            else:
                percentile = 10 * (your_value / bottom) if bottom > 0 else 0

            benchmarks.append(Benchmark(
                metric_id=metric_id,
                your_value=your_value,
                industry_average=avg,
                industry_top_10=top,
                industry_bottom_10=bottom,
                percentile=min(99, max(1, percentile)),
            ))

        return benchmarks

    # ========================================
    # Alerting
    # ========================================

    def _check_metric_alerts(self, account_id: str, metric_id: str, value: float):
        """Check if metric value triggers an alert"""
        metric_def = WARMUP_METRICS.get(metric_id)
        if not metric_def:
            return

        alert = None

        # Check critical threshold
        if metric_def.critical_threshold is not None:
            if metric_def.higher_is_better:
                triggered = value < metric_def.critical_threshold
            else:
                triggered = value > metric_def.critical_threshold

            if triggered:
                alert = AnalyticsAlert(
                    id=str(uuid.uuid4()),
                    severity=AlertSeverity.CRITICAL,
                    metric_id=metric_id,
                    message=f"{metric_def.name} has reached critical level: {value:.4f}",
                    current_value=value,
                    threshold=metric_def.critical_threshold,
                    created_at=datetime.now(timezone.utc),
                )

        # Check warning threshold (if no critical alert)
        elif metric_def.warning_threshold is not None:
            if metric_def.higher_is_better:
                triggered = value < metric_def.warning_threshold
            else:
                triggered = value > metric_def.warning_threshold

            if triggered:
                alert = AnalyticsAlert(
                    id=str(uuid.uuid4()),
                    severity=AlertSeverity.WARNING,
                    metric_id=metric_id,
                    message=f"{metric_def.name} has reached warning level: {value:.4f}",
                    current_value=value,
                    threshold=metric_def.warning_threshold,
                    created_at=datetime.now(timezone.utc),
                )

        if alert:
            self.alerts[account_id].append(alert)
            logger.warning(f"[Analytics] Alert created for {account_id}: {alert.message}")

    def get_alerts(
        self,
        account_id: str,
        severity: Optional[AlertSeverity] = None,
        acknowledged: Optional[bool] = None
    ) -> List[AnalyticsAlert]:
        """Get alerts for account"""
        alerts = self.alerts.get(account_id, [])

        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        if acknowledged is not None:
            alerts = [a for a in alerts if a.acknowledged == acknowledged]

        return sorted(alerts, key=lambda a: a.created_at, reverse=True)

    def acknowledge_alert(self, account_id: str, alert_id: str):
        """Acknowledge an alert"""
        for alert in self.alerts.get(account_id, []):
            if alert.id == alert_id:
                alert.acknowledged = True
                return True
        return False

    # ========================================
    # Report Generation
    # ========================================

    def generate_report(
        self,
        account_id: str,
        period_days: int = 30,
        name: Optional[str] = None
    ) -> Report:
        """Generate comprehensive analytics report"""
        report_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        # Get KPI snapshot
        kpi_snapshot = self.get_kpi_snapshot(account_id)

        # Get funnel analysis
        funnel_analyses = [self.analyze_warmup_funnel(account_id)]

        # Get benchmarks
        benchmarks = self.get_benchmarks(account_id)

        # Get recent alerts
        alerts = self.get_alerts(account_id)[:10]

        # Generate executive summary
        executive_summary = self._generate_executive_summary(kpi_snapshot, benchmarks)

        # Generate detailed insights
        detailed_insights = self._generate_detailed_insights(kpi_snapshot, funnel_analyses)

        # Generate recommendations
        recommendations = self._generate_recommendations(kpi_snapshot, benchmarks, funnel_analyses)

        report = Report(
            id=report_id,
            name=name or f"Warmup Report - {now.strftime('%Y-%m-%d')}",
            period_start=now - timedelta(days=period_days),
            period_end=now,
            generated_at=now,
            kpi_snapshot=kpi_snapshot,
            cohort_analyses=[],
            funnel_analyses=funnel_analyses,
            benchmarks=benchmarks,
            alerts=alerts,
            executive_summary=executive_summary,
            detailed_insights=detailed_insights,
            recommendations=recommendations,
        )

        self.reports[report_id] = report

        logger.info(f"[Analytics] Generated report: {report_id}")
        return report

    def _generate_executive_summary(
        self,
        kpi: KPISnapshot,
        benchmarks: List[Benchmark]
    ) -> str:
        """Generate executive summary"""
        score = kpi.score
        anomaly_count = len(kpi.anomalies)

        if score >= 80:
            status = "excellent"
        elif score >= 60:
            status = "good"
        elif score >= 40:
            status = "needs attention"
        else:
            status = "critical"

        above_avg = sum(1 for b in benchmarks if b.percentile > 50)
        total = len(benchmarks)

        summary = f"Overall warmup health is {status} with a score of {score:.0f}/100. "
        summary += f"{above_avg} out of {total} metrics are above industry average. "

        if anomaly_count > 0:
            summary += f"{anomaly_count} metric(s) showing unusual patterns. "

        return summary

    def _generate_detailed_insights(
        self,
        kpi: KPISnapshot,
        funnels: List[FunnelAnalysis]
    ) -> List[str]:
        """Generate detailed insights"""
        insights = []

        # Trend insights
        up_trends = [k for k, v in kpi.trends.items() if v == TrendDirection.UP]
        down_trends = [k for k, v in kpi.trends.items() if v == TrendDirection.DOWN]

        if up_trends:
            insights.append(f"Positive trends detected in: {', '.join(up_trends)}")

        if down_trends:
            insights.append(f"Declining trends in: {', '.join(down_trends)} - monitor closely")

        # Anomaly insights
        if kpi.anomalies:
            insights.append(f"Anomalies detected in: {', '.join(kpi.anomalies)}")

        # Funnel insights
        for funnel in funnels:
            insights.append(f"Funnel bottleneck: {funnel.bottleneck_stage} stage")

        return insights

    def _generate_recommendations(
        self,
        kpi: KPISnapshot,
        benchmarks: List[Benchmark],
        funnels: List[FunnelAnalysis]
    ) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []

        # Score-based recommendations
        if kpi.score < 40:
            recommendations.append("Consider pausing warmup and reviewing email authentication (SPF, DKIM, DMARC)")
        elif kpi.score < 60:
            recommendations.append("Reduce daily volume by 30% until metrics improve")

        # Benchmark-based recommendations
        for bench in benchmarks:
            if bench.percentile < 25:
                metric_def = WARMUP_METRICS.get(bench.metric_id)
                if metric_def:
                    recommendations.append(f"Focus on improving {metric_def.name} - currently in bottom 25%")

        # Funnel-based recommendations
        for funnel in funnels:
            recommendations.extend(funnel.recommendations)

        return recommendations[:10]  # Limit to top 10

    # ========================================
    # Statistics
    # ========================================

    def get_statistics(self) -> Dict[str, Any]:
        """Get analytics engine statistics"""
        total_data_points = sum(
            sum(len(series) for series in account_data.values())
            for account_data in self.time_series.values()
        )

        return {
            "accounts_tracked": len(self.time_series),
            "total_data_points": total_data_points,
            "metrics_defined": len(WARMUP_METRICS),
            "cohorts_created": len(self.cohorts),
            "reports_generated": len(self.reports),
            "active_alerts": sum(
                len([a for a in alerts if not a.acknowledged])
                for alerts in self.alerts.values()
            ),
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_analytics_engine: Optional[WarmupAnalyticsEngine] = None


def get_warmup_analytics_engine() -> WarmupAnalyticsEngine:
    """Get the singleton analytics engine instance"""
    global _analytics_engine
    if _analytics_engine is None:
        _analytics_engine = WarmupAnalyticsEngine()
    return _analytics_engine
