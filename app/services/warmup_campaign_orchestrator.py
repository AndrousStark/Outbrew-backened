"""
Warmup Campaign Orchestrator - PHASE 5 GOD TIER EDITION

Advanced campaign orchestration system for automated warmup management.

Features:
- Goal-based campaign creation with smart targeting
- Multi-stage warmup sequences with branching logic
- Automated milestone tracking and progression
- Dynamic volume adjustment based on performance
- Campaign templates for different use cases
- Real-time campaign health monitoring
- Automated pause/resume based on conditions

Author: Metaminds AI
Version: 5.0.0 - ULTRA GOD TIER ORCHESTRATION
"""

import logging
import uuid
import asyncio
from typing import List, Dict, Any, Optional, Callable, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import json
import math

from app.models.group_campaign import CampaignStatusEnum as CampaignStatus

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

class CampaignGoal(str, Enum):
    """Campaign objective types"""
    INBOX_RATE = "inbox_rate"  # Achieve target inbox placement
    VOLUME_RAMP = "volume_ramp"  # Reach target daily volume
    REPUTATION_BUILD = "reputation_build"  # Build sender reputation
    DOMAIN_WARM = "domain_warm"  # Warm up new domain
    RECOVERY = "recovery"  # Recover from deliverability issues
    MAINTENANCE = "maintenance"  # Maintain current reputation


class StageType(str, Enum):
    """Campaign stage types"""
    WARMUP = "warmup"  # Initial warmup phase
    RAMP = "ramp"  # Volume ramping phase
    STABILIZE = "stabilize"  # Stabilization phase
    OPTIMIZE = "optimize"  # Optimization phase
    MAINTAIN = "maintain"  # Maintenance phase
    RECOVERY = "recovery"  # Recovery phase


class TriggerType(str, Enum):
    """Stage transition triggers"""
    TIME_BASED = "time_based"  # After X days
    METRIC_BASED = "metric_based"  # When metric reaches threshold
    MANUAL = "manual"  # Manual transition
    ML_RECOMMENDED = "ml_recommended"  # ML model recommendation


class ActionType(str, Enum):
    """Automated actions"""
    PAUSE_CAMPAIGN = "pause_campaign"
    RESUME_CAMPAIGN = "resume_campaign"
    ADJUST_VOLUME = "adjust_volume"
    CHANGE_STAGE = "change_stage"
    SEND_ALERT = "send_alert"
    TRIGGER_RECOVERY = "trigger_recovery"
    ESCALATE = "escalate"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class CampaignMetrics:
    """Real-time campaign metrics"""
    emails_sent: int = 0
    emails_received: int = 0
    opens: int = 0
    replies: int = 0
    bounces: int = 0
    spam_reports: int = 0
    inbox_placements: int = 0
    spam_placements: int = 0

    @property
    def open_rate(self) -> float:
        return self.opens / max(self.emails_sent, 1)

    @property
    def reply_rate(self) -> float:
        return self.replies / max(self.emails_sent, 1)

    @property
    def bounce_rate(self) -> float:
        return self.bounces / max(self.emails_sent, 1)

    @property
    def spam_rate(self) -> float:
        return self.spam_reports / max(self.emails_sent, 1)

    @property
    def inbox_rate(self) -> float:
        total_placements = self.inbox_placements + self.spam_placements
        return self.inbox_placements / max(total_placements, 1)

    @property
    def health_score(self) -> float:
        """Calculate overall health score (0-100)"""
        # Weighted scoring
        open_score = min(self.open_rate / 0.3, 1.0) * 25  # Target 30% open rate
        reply_score = min(self.reply_rate / 0.1, 1.0) * 25  # Target 10% reply rate
        bounce_penalty = min(self.bounce_rate / 0.05, 1.0) * 25  # Penalty for >5% bounce
        spam_penalty = min(self.spam_rate / 0.01, 1.0) * 25  # Penalty for >1% spam

        return max(0, open_score + reply_score + (25 - bounce_penalty) + (25 - spam_penalty))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "emails_sent": self.emails_sent,
            "emails_received": self.emails_received,
            "opens": self.opens,
            "replies": self.replies,
            "bounces": self.bounces,
            "spam_reports": self.spam_reports,
            "inbox_placements": self.inbox_placements,
            "spam_placements": self.spam_placements,
            "open_rate": round(self.open_rate, 4),
            "reply_rate": round(self.reply_rate, 4),
            "bounce_rate": round(self.bounce_rate, 4),
            "spam_rate": round(self.spam_rate, 4),
            "inbox_rate": round(self.inbox_rate, 4),
            "health_score": round(self.health_score, 2),
        }


@dataclass
class StageCondition:
    """Condition for stage transitions"""
    metric: str
    operator: str  # gt, lt, gte, lte, eq
    threshold: float

    def evaluate(self, metrics: CampaignMetrics) -> bool:
        """Evaluate condition against metrics"""
        value = getattr(metrics, self.metric, None)
        if value is None:
            return False

        ops = {
            "gt": lambda a, b: a > b,
            "lt": lambda a, b: a < b,
            "gte": lambda a, b: a >= b,
            "lte": lambda a, b: a <= b,
            "eq": lambda a, b: abs(a - b) < 0.001,
        }

        return ops.get(self.operator, lambda a, b: False)(value, self.threshold)


@dataclass
class CampaignStage:
    """Campaign stage definition"""
    id: str
    name: str
    stage_type: StageType
    duration_days: int
    target_volume: int
    volume_increment: float  # Daily increase rate
    conditions_to_advance: List[StageCondition] = field(default_factory=list)
    conditions_to_pause: List[StageCondition] = field(default_factory=list)
    next_stage_id: Optional[str] = None
    fallback_stage_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "stage_type": self.stage_type.value,
            "duration_days": self.duration_days,
            "target_volume": self.target_volume,
            "volume_increment": self.volume_increment,
            "conditions_to_advance": [
                {"metric": c.metric, "operator": c.operator, "threshold": c.threshold}
                for c in self.conditions_to_advance
            ],
            "conditions_to_pause": [
                {"metric": c.metric, "operator": c.operator, "threshold": c.threshold}
                for c in self.conditions_to_pause
            ],
            "next_stage_id": self.next_stage_id,
            "fallback_stage_id": self.fallback_stage_id,
        }


@dataclass
class CampaignMilestone:
    """Campaign milestone for tracking progress"""
    id: str
    name: str
    description: str
    target_metric: str
    target_value: float
    achieved: bool = False
    achieved_at: Optional[datetime] = None
    reward_points: int = 0

    def check_achievement(self, metrics: CampaignMetrics) -> bool:
        """Check if milestone is achieved"""
        if self.achieved:
            return True

        value = getattr(metrics, self.target_metric, None)
        if value is not None and value >= self.target_value:
            self.achieved = True
            self.achieved_at = datetime.now(timezone.utc)
            return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "target_metric": self.target_metric,
            "target_value": self.target_value,
            "achieved": self.achieved,
            "achieved_at": self.achieved_at.isoformat() if self.achieved_at else None,
            "reward_points": self.reward_points,
        }


@dataclass
class AutomationRule:
    """Automation rule for campaign actions"""
    id: str
    name: str
    trigger_type: TriggerType
    condition: StageCondition
    action: ActionType
    action_params: Dict[str, Any] = field(default_factory=dict)
    cooldown_minutes: int = 60
    last_triggered: Optional[datetime] = None
    enabled: bool = True

    def can_trigger(self) -> bool:
        """Check if rule can be triggered (cooldown check)"""
        if not self.enabled:
            return False
        if self.last_triggered is None:
            return True

        cooldown = timedelta(minutes=self.cooldown_minutes)
        return datetime.now(timezone.utc) - self.last_triggered >= cooldown

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "trigger_type": self.trigger_type.value,
            "condition": {
                "metric": self.condition.metric,
                "operator": self.condition.operator,
                "threshold": self.condition.threshold,
            },
            "action": self.action.value,
            "action_params": self.action_params,
            "cooldown_minutes": self.cooldown_minutes,
            "last_triggered": self.last_triggered.isoformat() if self.last_triggered else None,
            "enabled": self.enabled,
        }


@dataclass
class Campaign:
    """Full campaign definition"""
    id: str
    name: str
    description: str
    account_id: str
    goal: CampaignGoal
    status: CampaignStatus
    stages: List[CampaignStage]
    milestones: List[CampaignMilestone]
    automation_rules: List[AutomationRule]
    current_stage_id: Optional[str]
    metrics: CampaignMetrics

    # Timing
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    stage_started_at: Optional[datetime] = None

    # Configuration
    target_inbox_rate: float = 0.9
    target_daily_volume: int = 100
    max_daily_volume: int = 500
    current_daily_volume: int = 10
    timezone: str = "UTC"

    # Metadata
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def current_stage(self) -> Optional[CampaignStage]:
        """Get current stage"""
        if not self.current_stage_id:
            return None
        for stage in self.stages:
            if stage.id == self.current_stage_id:
                return stage
        return None

    @property
    def days_in_current_stage(self) -> int:
        """Days spent in current stage"""
        if not self.stage_started_at:
            return 0
        return (datetime.now(timezone.utc) - self.stage_started_at).days

    @property
    def total_days_running(self) -> int:
        """Total days campaign has been running"""
        if not self.started_at:
            return 0
        end = self.completed_at or datetime.now(timezone.utc)
        return (end - self.started_at).days

    @property
    def progress_percentage(self) -> float:
        """Calculate campaign progress"""
        if self.status == CampaignStatus.COMPLETED:
            return 100.0

        # Based on milestones achieved
        if not self.milestones:
            return 0.0

        achieved = sum(1 for m in self.milestones if m.achieved)
        return (achieved / len(self.milestones)) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "account_id": self.account_id,
            "goal": self.goal.value,
            "status": self.status.value,
            "stages": [s.to_dict() for s in self.stages],
            "milestones": [m.to_dict() for m in self.milestones],
            "automation_rules": [r.to_dict() for r in self.automation_rules],
            "current_stage_id": self.current_stage_id,
            "current_stage": self.current_stage.to_dict() if self.current_stage else None,
            "metrics": self.metrics.to_dict(),
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "stage_started_at": self.stage_started_at.isoformat() if self.stage_started_at else None,
            "days_in_current_stage": self.days_in_current_stage,
            "total_days_running": self.total_days_running,
            "progress_percentage": round(self.progress_percentage, 2),
            "target_inbox_rate": self.target_inbox_rate,
            "target_daily_volume": self.target_daily_volume,
            "max_daily_volume": self.max_daily_volume,
            "current_daily_volume": self.current_daily_volume,
            "timezone": self.timezone,
            "tags": self.tags,
            "metadata": self.metadata,
        }


# ============================================================================
# CAMPAIGN TEMPLATES
# ============================================================================

class CampaignTemplates:
    """Pre-built campaign templates for common use cases"""

    @staticmethod
    def new_domain_warmup(account_id: str, target_volume: int = 100) -> Campaign:
        """Template for warming up a new domain"""
        campaign_id = str(uuid.uuid4())

        stages = [
            CampaignStage(
                id=f"{campaign_id}-stage-1",
                name="Initial Warmup",
                stage_type=StageType.WARMUP,
                duration_days=7,
                target_volume=10,
                volume_increment=0.0,
                conditions_to_advance=[
                    StageCondition("open_rate", "gte", 0.2),
                    StageCondition("bounce_rate", "lte", 0.05),
                ],
                conditions_to_pause=[
                    StageCondition("spam_rate", "gte", 0.02),
                ],
                next_stage_id=f"{campaign_id}-stage-2",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-2",
                name="Gradual Ramp",
                stage_type=StageType.RAMP,
                duration_days=14,
                target_volume=50,
                volume_increment=0.15,  # 15% daily increase
                conditions_to_advance=[
                    StageCondition("open_rate", "gte", 0.25),
                    StageCondition("inbox_rate", "gte", 0.85),
                ],
                conditions_to_pause=[
                    StageCondition("bounce_rate", "gte", 0.08),
                ],
                next_stage_id=f"{campaign_id}-stage-3",
                fallback_stage_id=f"{campaign_id}-stage-1",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-3",
                name="Volume Scaling",
                stage_type=StageType.RAMP,
                duration_days=14,
                target_volume=target_volume,
                volume_increment=0.1,
                conditions_to_advance=[
                    StageCondition("inbox_rate", "gte", 0.9),
                ],
                conditions_to_pause=[
                    StageCondition("spam_rate", "gte", 0.015),
                ],
                next_stage_id=f"{campaign_id}-stage-4",
                fallback_stage_id=f"{campaign_id}-stage-2",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-4",
                name="Stabilization",
                stage_type=StageType.STABILIZE,
                duration_days=7,
                target_volume=target_volume,
                volume_increment=0.0,
                conditions_to_advance=[
                    StageCondition("health_score", "gte", 80),
                ],
                next_stage_id=f"{campaign_id}-stage-5",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-5",
                name="Maintenance",
                stage_type=StageType.MAINTAIN,
                duration_days=365,  # Ongoing
                target_volume=target_volume,
                volume_increment=0.0,
                conditions_to_pause=[
                    StageCondition("inbox_rate", "lt", 0.8),
                ],
            ),
        ]

        milestones = [
            CampaignMilestone(
                id=f"{campaign_id}-milestone-1",
                name="First 100 Emails",
                description="Send your first 100 warmup emails",
                target_metric="emails_sent",
                target_value=100,
                reward_points=10,
            ),
            CampaignMilestone(
                id=f"{campaign_id}-milestone-2",
                name="20% Open Rate",
                description="Achieve 20% open rate",
                target_metric="open_rate",
                target_value=0.2,
                reward_points=20,
            ),
            CampaignMilestone(
                id=f"{campaign_id}-milestone-3",
                name="85% Inbox Rate",
                description="Achieve 85% inbox placement",
                target_metric="inbox_rate",
                target_value=0.85,
                reward_points=30,
            ),
            CampaignMilestone(
                id=f"{campaign_id}-milestone-4",
                name="Target Volume",
                description=f"Reach {target_volume} daily emails",
                target_metric="emails_sent",
                target_value=target_volume * 7,  # Weekly target
                reward_points=50,
            ),
            CampaignMilestone(
                id=f"{campaign_id}-milestone-5",
                name="Health Score 80+",
                description="Achieve health score of 80 or higher",
                target_metric="health_score",
                target_value=80,
                reward_points=40,
            ),
        ]

        automation_rules = [
            AutomationRule(
                id=f"{campaign_id}-rule-1",
                name="Pause on High Spam",
                trigger_type=TriggerType.METRIC_BASED,
                condition=StageCondition("spam_rate", "gte", 0.03),
                action=ActionType.PAUSE_CAMPAIGN,
                cooldown_minutes=1440,  # 24 hours
            ),
            AutomationRule(
                id=f"{campaign_id}-rule-2",
                name="Alert on Bounce Spike",
                trigger_type=TriggerType.METRIC_BASED,
                condition=StageCondition("bounce_rate", "gte", 0.1),
                action=ActionType.SEND_ALERT,
                action_params={"severity": "high", "message": "Bounce rate exceeded 10%"},
                cooldown_minutes=60,
            ),
            AutomationRule(
                id=f"{campaign_id}-rule-3",
                name="Volume Adjustment",
                trigger_type=TriggerType.METRIC_BASED,
                condition=StageCondition("health_score", "lt", 60),
                action=ActionType.ADJUST_VOLUME,
                action_params={"multiplier": 0.5},  # Reduce by 50%
                cooldown_minutes=360,
            ),
        ]

        return Campaign(
            id=campaign_id,
            name="New Domain Warmup",
            description="Standard warmup campaign for a new email domain",
            account_id=account_id,
            goal=CampaignGoal.DOMAIN_WARM,
            status=CampaignStatus.DRAFT,
            stages=stages,
            milestones=milestones,
            automation_rules=automation_rules,
            current_stage_id=None,
            metrics=CampaignMetrics(),
            created_at=datetime.now(timezone.utc),
            target_daily_volume=target_volume,
            max_daily_volume=target_volume * 2,
            current_daily_volume=5,
        )

    @staticmethod
    def reputation_recovery(account_id: str) -> Campaign:
        """Template for recovering from deliverability issues"""
        campaign_id = str(uuid.uuid4())

        stages = [
            CampaignStage(
                id=f"{campaign_id}-stage-1",
                name="Assessment",
                stage_type=StageType.RECOVERY,
                duration_days=3,
                target_volume=5,
                volume_increment=0.0,
                conditions_to_advance=[
                    StageCondition("bounce_rate", "lte", 0.03),
                ],
                next_stage_id=f"{campaign_id}-stage-2",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-2",
                name="Careful Recovery",
                stage_type=StageType.RECOVERY,
                duration_days=14,
                target_volume=20,
                volume_increment=0.05,
                conditions_to_advance=[
                    StageCondition("inbox_rate", "gte", 0.7),
                    StageCondition("open_rate", "gte", 0.15),
                ],
                conditions_to_pause=[
                    StageCondition("spam_rate", "gte", 0.01),
                ],
                next_stage_id=f"{campaign_id}-stage-3",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-3",
                name="Gradual Rebuild",
                stage_type=StageType.RAMP,
                duration_days=21,
                target_volume=50,
                volume_increment=0.08,
                conditions_to_advance=[
                    StageCondition("inbox_rate", "gte", 0.85),
                ],
                next_stage_id=f"{campaign_id}-stage-4",
                fallback_stage_id=f"{campaign_id}-stage-2",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-4",
                name="Maintenance",
                stage_type=StageType.MAINTAIN,
                duration_days=365,
                target_volume=50,
                volume_increment=0.0,
            ),
        ]

        milestones = [
            CampaignMilestone(
                id=f"{campaign_id}-milestone-1",
                name="Clean Start",
                description="Achieve less than 3% bounce rate",
                target_metric="bounce_rate",
                target_value=0.03,  # Note: this is inverted logic in check
                reward_points=20,
            ),
            CampaignMilestone(
                id=f"{campaign_id}-milestone-2",
                name="Inbox Recovery",
                description="Achieve 70% inbox placement",
                target_metric="inbox_rate",
                target_value=0.7,
                reward_points=30,
            ),
            CampaignMilestone(
                id=f"{campaign_id}-milestone-3",
                name="Full Recovery",
                description="Achieve 85% inbox placement",
                target_metric="inbox_rate",
                target_value=0.85,
                reward_points=50,
            ),
        ]

        automation_rules = [
            AutomationRule(
                id=f"{campaign_id}-rule-1",
                name="Emergency Pause",
                trigger_type=TriggerType.METRIC_BASED,
                condition=StageCondition("spam_rate", "gte", 0.02),
                action=ActionType.PAUSE_CAMPAIGN,
                cooldown_minutes=2880,  # 48 hours
            ),
        ]

        return Campaign(
            id=campaign_id,
            name="Reputation Recovery",
            description="Recovery campaign for damaged sender reputation",
            account_id=account_id,
            goal=CampaignGoal.RECOVERY,
            status=CampaignStatus.DRAFT,
            stages=stages,
            milestones=milestones,
            automation_rules=automation_rules,
            current_stage_id=None,
            metrics=CampaignMetrics(),
            created_at=datetime.now(timezone.utc),
            target_daily_volume=50,
            max_daily_volume=100,
            current_daily_volume=5,
        )

    @staticmethod
    def aggressive_ramp(account_id: str, target_volume: int = 200) -> Campaign:
        """Template for aggressive volume ramping (experienced senders)"""
        campaign_id = str(uuid.uuid4())

        stages = [
            CampaignStage(
                id=f"{campaign_id}-stage-1",
                name="Quick Start",
                stage_type=StageType.WARMUP,
                duration_days=3,
                target_volume=25,
                volume_increment=0.0,
                conditions_to_advance=[
                    StageCondition("open_rate", "gte", 0.25),
                ],
                conditions_to_pause=[
                    StageCondition("bounce_rate", "gte", 0.05),
                ],
                next_stage_id=f"{campaign_id}-stage-2",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-2",
                name="Aggressive Ramp",
                stage_type=StageType.RAMP,
                duration_days=10,
                target_volume=target_volume,
                volume_increment=0.25,  # 25% daily increase
                conditions_to_advance=[
                    StageCondition("inbox_rate", "gte", 0.9),
                ],
                conditions_to_pause=[
                    StageCondition("spam_rate", "gte", 0.01),
                ],
                next_stage_id=f"{campaign_id}-stage-3",
            ),
            CampaignStage(
                id=f"{campaign_id}-stage-3",
                name="Maintain",
                stage_type=StageType.MAINTAIN,
                duration_days=365,
                target_volume=target_volume,
                volume_increment=0.0,
            ),
        ]

        return Campaign(
            id=campaign_id,
            name="Aggressive Volume Ramp",
            description="Fast volume scaling for experienced senders",
            account_id=account_id,
            goal=CampaignGoal.VOLUME_RAMP,
            status=CampaignStatus.DRAFT,
            stages=stages,
            milestones=[],
            automation_rules=[],
            current_stage_id=None,
            metrics=CampaignMetrics(),
            created_at=datetime.now(timezone.utc),
            target_daily_volume=target_volume,
            max_daily_volume=target_volume * 2,
            current_daily_volume=25,
        )


# ============================================================================
# CAMPAIGN ORCHESTRATOR ENGINE
# ============================================================================

class CampaignOrchestrator:
    """
    Main campaign orchestration engine.

    Manages campaign lifecycle, stage transitions, automation rules,
    and real-time monitoring.
    """

    def __init__(self):
        self.campaigns: Dict[str, Campaign] = {}
        self.account_campaigns: Dict[str, List[str]] = defaultdict(list)
        self.event_handlers: Dict[str, List[Callable]] = defaultdict(list)
        self.action_queue: List[Tuple[str, ActionType, Dict[str, Any]]] = []
        self._initialized = False

        logger.info("[CampaignOrchestrator] Initialized")

    def _ensure_initialized(self):
        """Lazy initialization"""
        if not self._initialized:
            self._initialized = True
            logger.info("[CampaignOrchestrator] Ready")

    # ========================================
    # Campaign CRUD Operations
    # ========================================

    def create_campaign(
        self,
        account_id: str,
        template: str = "new_domain",
        name: Optional[str] = None,
        target_volume: int = 100,
        **kwargs
    ) -> Campaign:
        """Create a new campaign from template"""
        self._ensure_initialized()

        # Get template
        templates = {
            "new_domain": lambda: CampaignTemplates.new_domain_warmup(account_id, target_volume),
            "recovery": lambda: CampaignTemplates.reputation_recovery(account_id),
            "aggressive": lambda: CampaignTemplates.aggressive_ramp(account_id, target_volume),
        }

        if template not in templates:
            raise ValueError(f"Unknown template: {template}")

        campaign = templates[template]()

        # Override with custom values
        if name:
            campaign.name = name
        for key, value in kwargs.items():
            if hasattr(campaign, key):
                setattr(campaign, key, value)

        # Store campaign
        self.campaigns[campaign.id] = campaign
        self.account_campaigns[account_id].append(campaign.id)

        self._emit_event("campaign_created", campaign)
        logger.info(f"[CampaignOrchestrator] Created campaign: {campaign.id} for account: {account_id}")

        return campaign

    def get_campaign(self, campaign_id: str) -> Optional[Campaign]:
        """Get campaign by ID"""
        return self.campaigns.get(campaign_id)

    def get_account_campaigns(self, account_id: str) -> List[Campaign]:
        """Get all campaigns for an account"""
        campaign_ids = self.account_campaigns.get(account_id, [])
        return [self.campaigns[cid] for cid in campaign_ids if cid in self.campaigns]

    def delete_campaign(self, campaign_id: str) -> bool:
        """Delete a campaign"""
        campaign = self.campaigns.get(campaign_id)
        if not campaign:
            return False

        # Remove from account list
        if campaign.account_id in self.account_campaigns:
            self.account_campaigns[campaign.account_id] = [
                cid for cid in self.account_campaigns[campaign.account_id]
                if cid != campaign_id
            ]

        # Remove campaign
        del self.campaigns[campaign_id]

        self._emit_event("campaign_deleted", {"campaign_id": campaign_id})
        logger.info(f"[CampaignOrchestrator] Deleted campaign: {campaign_id}")

        return True

    # ========================================
    # Campaign Lifecycle Management
    # ========================================

    def start_campaign(self, campaign_id: str) -> Campaign:
        """Start a campaign"""
        campaign = self._get_campaign_or_raise(campaign_id)

        if campaign.status not in [CampaignStatus.DRAFT, CampaignStatus.PAUSED, CampaignStatus.SCHEDULED]:
            raise ValueError(f"Cannot start campaign in status: {campaign.status}")

        campaign.status = CampaignStatus.SENDING
        campaign.started_at = campaign.started_at or datetime.now(timezone.utc)

        # Set first stage if not set
        if not campaign.current_stage_id and campaign.stages:
            campaign.current_stage_id = campaign.stages[0].id
            campaign.stage_started_at = datetime.now(timezone.utc)

        self._emit_event("campaign_started", campaign)
        logger.info(f"[CampaignOrchestrator] Started campaign: {campaign_id}")

        return campaign

    def pause_campaign(self, campaign_id: str, reason: str = "") -> Campaign:
        """Pause a running campaign"""
        campaign = self._get_campaign_or_raise(campaign_id)

        if campaign.status != CampaignStatus.SENDING:
            raise ValueError(f"Cannot pause campaign in status: {campaign.status}")

        campaign.status = CampaignStatus.PAUSED
        campaign.metadata["pause_reason"] = reason
        campaign.metadata["paused_at"] = datetime.now(timezone.utc).isoformat()

        self._emit_event("campaign_paused", {"campaign": campaign, "reason": reason})
        logger.info(f"[CampaignOrchestrator] Paused campaign: {campaign_id}, reason: {reason}")

        return campaign

    def resume_campaign(self, campaign_id: str) -> Campaign:
        """Resume a paused campaign"""
        campaign = self._get_campaign_or_raise(campaign_id)

        if campaign.status != CampaignStatus.PAUSED:
            raise ValueError(f"Cannot resume campaign in status: {campaign.status}")

        campaign.status = CampaignStatus.SENDING
        campaign.metadata.pop("pause_reason", None)
        campaign.metadata["resumed_at"] = datetime.now(timezone.utc).isoformat()

        self._emit_event("campaign_resumed", campaign)
        logger.info(f"[CampaignOrchestrator] Resumed campaign: {campaign_id}")

        return campaign

    def complete_campaign(self, campaign_id: str) -> Campaign:
        """Mark campaign as completed"""
        campaign = self._get_campaign_or_raise(campaign_id)

        campaign.status = CampaignStatus.COMPLETED
        campaign.completed_at = datetime.now(timezone.utc)

        self._emit_event("campaign_completed", campaign)
        logger.info(f"[CampaignOrchestrator] Completed campaign: {campaign_id}")

        return campaign

    def cancel_campaign(self, campaign_id: str, reason: str = "") -> Campaign:
        """Cancel a campaign"""
        campaign = self._get_campaign_or_raise(campaign_id)

        campaign.status = CampaignStatus.CANCELLED
        campaign.completed_at = datetime.now(timezone.utc)
        campaign.metadata["cancel_reason"] = reason

        self._emit_event("campaign_cancelled", {"campaign": campaign, "reason": reason})
        logger.info(f"[CampaignOrchestrator] Cancelled campaign: {campaign_id}")

        return campaign

    # ========================================
    # Stage Management
    # ========================================

    def advance_stage(self, campaign_id: str, force: bool = False) -> Campaign:
        """Advance campaign to next stage"""
        campaign = self._get_campaign_or_raise(campaign_id)
        current_stage = campaign.current_stage

        if not current_stage:
            raise ValueError("Campaign has no current stage")

        if not current_stage.next_stage_id:
            # No next stage - complete campaign
            return self.complete_campaign(campaign_id)

        if not force:
            # Check conditions
            all_conditions_met = all(
                c.evaluate(campaign.metrics)
                for c in current_stage.conditions_to_advance
            )
            if not all_conditions_met:
                raise ValueError("Not all conditions met for stage advancement")

        # Advance to next stage
        campaign.current_stage_id = current_stage.next_stage_id
        campaign.stage_started_at = datetime.now(timezone.utc)

        self._emit_event("stage_advanced", {
            "campaign": campaign,
            "from_stage": current_stage.id,
            "to_stage": campaign.current_stage_id,
        })
        logger.info(f"[CampaignOrchestrator] Advanced campaign {campaign_id} to stage {campaign.current_stage_id}")

        return campaign

    def fallback_stage(self, campaign_id: str) -> Campaign:
        """Move campaign to fallback stage"""
        campaign = self._get_campaign_or_raise(campaign_id)
        current_stage = campaign.current_stage

        if not current_stage or not current_stage.fallback_stage_id:
            raise ValueError("No fallback stage defined")

        old_stage_id = campaign.current_stage_id
        campaign.current_stage_id = current_stage.fallback_stage_id
        campaign.stage_started_at = datetime.now(timezone.utc)

        # Reduce volume as precaution
        campaign.current_daily_volume = max(
            5,
            int(campaign.current_daily_volume * 0.5)
        )

        self._emit_event("stage_fallback", {
            "campaign": campaign,
            "from_stage": old_stage_id,
            "to_stage": campaign.current_stage_id,
        })
        logger.info(f"[CampaignOrchestrator] Fallback campaign {campaign_id} to stage {campaign.current_stage_id}")

        return campaign

    # ========================================
    # Metrics & Monitoring
    # ========================================

    def update_metrics(
        self,
        campaign_id: str,
        metrics_delta: Dict[str, int]
    ) -> Campaign:
        """Update campaign metrics with delta values"""
        campaign = self._get_campaign_or_raise(campaign_id)

        # Apply deltas
        for key, delta in metrics_delta.items():
            if hasattr(campaign.metrics, key):
                current = getattr(campaign.metrics, key)
                setattr(campaign.metrics, key, current + delta)

        # Check milestones
        for milestone in campaign.milestones:
            if not milestone.achieved:
                if milestone.check_achievement(campaign.metrics):
                    self._emit_event("milestone_achieved", {
                        "campaign": campaign,
                        "milestone": milestone,
                    })

        # Evaluate automation rules
        self._evaluate_automation_rules(campaign)

        # Check stage conditions
        self._evaluate_stage_conditions(campaign)

        return campaign

    def get_campaign_health(self, campaign_id: str) -> Dict[str, Any]:
        """Get comprehensive campaign health report"""
        campaign = self._get_campaign_or_raise(campaign_id)

        return {
            "campaign_id": campaign_id,
            "status": campaign.status.value,
            "health_score": campaign.metrics.health_score,
            "progress_percentage": campaign.progress_percentage,
            "current_stage": campaign.current_stage.name if campaign.current_stage else None,
            "days_running": campaign.total_days_running,
            "days_in_stage": campaign.days_in_current_stage,
            "metrics": campaign.metrics.to_dict(),
            "milestones_achieved": sum(1 for m in campaign.milestones if m.achieved),
            "milestones_total": len(campaign.milestones),
            "recommendations": self._generate_recommendations(campaign),
            "risks": self._identify_risks(campaign),
        }

    # ========================================
    # Automation Engine
    # ========================================

    def _evaluate_automation_rules(self, campaign: Campaign):
        """Evaluate and trigger automation rules"""
        for rule in campaign.automation_rules:
            if not rule.can_trigger():
                continue

            if rule.condition.evaluate(campaign.metrics):
                rule.last_triggered = datetime.now(timezone.utc)
                self._queue_action(campaign.id, rule.action, rule.action_params)

                self._emit_event("automation_triggered", {
                    "campaign": campaign,
                    "rule": rule,
                })
                logger.info(f"[CampaignOrchestrator] Automation rule triggered: {rule.name}")

    def _queue_action(self, campaign_id: str, action: ActionType, params: Dict[str, Any]):
        """Queue an action for execution"""
        self.action_queue.append((campaign_id, action, params))

    def process_action_queue(self) -> List[Dict[str, Any]]:
        """Process queued actions"""
        results = []

        while self.action_queue:
            campaign_id, action, params = self.action_queue.pop(0)

            try:
                result = self._execute_action(campaign_id, action, params)
                results.append({
                    "campaign_id": campaign_id,
                    "action": action.value,
                    "success": True,
                    "result": result,
                })
            except Exception as e:
                logger.error(f"[CampaignOrchestrator] Action failed: {e}")
                results.append({
                    "campaign_id": campaign_id,
                    "action": action.value,
                    "success": False,
                    "error": str(e),
                })

        return results

    def _execute_action(self, campaign_id: str, action: ActionType, params: Dict[str, Any]) -> Any:
        """Execute a single action"""
        campaign = self._get_campaign_or_raise(campaign_id)

        if action == ActionType.PAUSE_CAMPAIGN:
            return self.pause_campaign(campaign_id, params.get("reason", "Automation"))

        elif action == ActionType.RESUME_CAMPAIGN:
            return self.resume_campaign(campaign_id)

        elif action == ActionType.ADJUST_VOLUME:
            multiplier = params.get("multiplier", 1.0)
            new_volume = int(campaign.current_daily_volume * multiplier)
            campaign.current_daily_volume = max(5, min(new_volume, campaign.max_daily_volume))
            return {"new_volume": campaign.current_daily_volume}

        elif action == ActionType.CHANGE_STAGE:
            target_stage = params.get("stage_id")
            if target_stage:
                campaign.current_stage_id = target_stage
                campaign.stage_started_at = datetime.now(timezone.utc)
            return {"new_stage": campaign.current_stage_id}

        elif action == ActionType.SEND_ALERT:
            self._emit_event("alert", {
                "campaign": campaign,
                "severity": params.get("severity", "info"),
                "message": params.get("message", ""),
            })
            return {"alert_sent": True}

        elif action == ActionType.TRIGGER_RECOVERY:
            return self.fallback_stage(campaign_id)

        elif action == ActionType.ESCALATE:
            self._emit_event("escalation", {
                "campaign": campaign,
                "reason": params.get("reason", ""),
            })
            return {"escalated": True}

        return None

    # ========================================
    # Stage Evaluation
    # ========================================

    def _evaluate_stage_conditions(self, campaign: Campaign):
        """Evaluate stage conditions and trigger transitions"""
        if campaign.status != CampaignStatus.SENDING:
            return

        current_stage = campaign.current_stage
        if not current_stage:
            return

        # Check pause conditions
        any_pause_condition_met = any(
            c.evaluate(campaign.metrics)
            for c in current_stage.conditions_to_pause
        )

        if any_pause_condition_met:
            if current_stage.fallback_stage_id:
                self._queue_action(campaign.id, ActionType.TRIGGER_RECOVERY, {})
            else:
                self._queue_action(campaign.id, ActionType.PAUSE_CAMPAIGN, {
                    "reason": "Stage pause condition met"
                })
            return

        # Check advancement conditions (time-based)
        if campaign.days_in_current_stage >= current_stage.duration_days:
            # Check metric conditions
            all_conditions_met = all(
                c.evaluate(campaign.metrics)
                for c in current_stage.conditions_to_advance
            ) if current_stage.conditions_to_advance else True

            if all_conditions_met and current_stage.next_stage_id:
                self._queue_action(campaign.id, ActionType.CHANGE_STAGE, {
                    "stage_id": current_stage.next_stage_id
                })

    # ========================================
    # Recommendations & Risk Analysis
    # ========================================

    def _generate_recommendations(self, campaign: Campaign) -> List[str]:
        """Generate recommendations based on campaign state"""
        recommendations = []
        metrics = campaign.metrics

        if metrics.open_rate < 0.15:
            recommendations.append("Open rate is low. Consider improving subject lines or send timing.")

        if metrics.reply_rate < 0.05:
            recommendations.append("Reply rate is low. Warmup emails may need more engaging content.")

        if metrics.bounce_rate > 0.05:
            recommendations.append("High bounce rate detected. Verify email list quality.")

        if metrics.spam_rate > 0.01:
            recommendations.append("Spam rate is elevated. Consider pausing and reviewing content.")

        if campaign.days_in_current_stage > (campaign.current_stage.duration_days * 1.5 if campaign.current_stage else 0):
            recommendations.append("Stage duration exceeded. Review conditions for advancement.")

        if metrics.health_score < 50:
            recommendations.append("Health score is critical. Consider switching to recovery mode.")

        return recommendations

    def _identify_risks(self, campaign: Campaign) -> List[Dict[str, Any]]:
        """Identify potential risks"""
        risks = []
        metrics = campaign.metrics

        if metrics.spam_rate > 0.015:
            risks.append({
                "level": "high",
                "type": "spam_risk",
                "message": "Spam rate approaching dangerous levels",
                "metric": metrics.spam_rate,
            })

        if metrics.bounce_rate > 0.08:
            risks.append({
                "level": "high",
                "type": "bounce_risk",
                "message": "High bounce rate may damage reputation",
                "metric": metrics.bounce_rate,
            })

        if metrics.inbox_rate < 0.7:
            risks.append({
                "level": "medium",
                "type": "deliverability_risk",
                "message": "Inbox placement below acceptable threshold",
                "metric": metrics.inbox_rate,
            })

        return risks

    # ========================================
    # Event System
    # ========================================

    def on_event(self, event_type: str, handler: Callable):
        """Register event handler"""
        self.event_handlers[event_type].append(handler)

    def _emit_event(self, event_type: str, data: Any):
        """Emit event to handlers"""
        for handler in self.event_handlers.get(event_type, []):
            try:
                handler(data)
            except Exception as e:
                logger.error(f"[CampaignOrchestrator] Event handler error: {e}")

    # ========================================
    # Helpers
    # ========================================

    def _get_campaign_or_raise(self, campaign_id: str) -> Campaign:
        """Get campaign or raise exception"""
        campaign = self.campaigns.get(campaign_id)
        if not campaign:
            raise ValueError(f"Campaign not found: {campaign_id}")
        return campaign

    def get_statistics(self) -> Dict[str, Any]:
        """Get orchestrator statistics"""
        return {
            "total_campaigns": len(self.campaigns),
            "campaigns_by_status": {
                status.value: len([c for c in self.campaigns.values() if c.status == status])
                for status in CampaignStatus
            },
            "campaigns_by_goal": {
                goal.value: len([c for c in self.campaigns.values() if c.goal == goal])
                for goal in CampaignGoal
            },
            "pending_actions": len(self.action_queue),
            "active_accounts": len(self.account_campaigns),
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_campaign_orchestrator: Optional[CampaignOrchestrator] = None


def get_campaign_orchestrator() -> CampaignOrchestrator:
    """Get the singleton campaign orchestrator instance"""
    global _campaign_orchestrator
    if _campaign_orchestrator is None:
        _campaign_orchestrator = CampaignOrchestrator()
    return _campaign_orchestrator
