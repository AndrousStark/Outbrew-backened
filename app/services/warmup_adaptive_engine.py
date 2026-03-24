"""
Warmup Adaptive Engine - PHASE 4 GOD TIER EDITION

Real-time adaptive control system combining ML and optimization.
Features:
- Real-time signal processing and response
- Automatic throttling and recovery
- Reputation protection system
- Intelligent fallback strategies
- Self-healing mechanisms

Author: Metaminds AI
Version: 4.0.0 - ULTRA GOD TIER
"""

import logging
import time
from typing import List, Dict, Any, Optional, Tuple, Callable
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import threading
import json

from app.services.warmup_ml_engine import (
    WarmupMLEngine, get_warmup_ml_engine,
    State, ActionType, Experience
)
from app.services.warmup_optimizer import (
    WarmupOptimizer, get_warmup_optimizer,
    OptimizationResult
)

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS AND CONSTANTS
# ============================================================================

class SystemState(Enum):
    """System operational states"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    THROTTLED = "throttled"
    RECOVERY = "recovery"
    EMERGENCY = "emergency"
    PAUSED = "paused"


class SignalType(Enum):
    """Types of signals the system processes"""
    EMAIL_SENT = "email_sent"
    EMAIL_OPENED = "email_opened"
    EMAIL_REPLIED = "email_replied"
    EMAIL_BOUNCED = "email_bounced"
    SPAM_DETECTED = "spam_detected"
    SPAM_RESCUED = "spam_rescued"
    BLACKLIST_DETECTED = "blacklist_detected"
    RATE_LIMIT_HIT = "rate_limit_hit"
    PLACEMENT_TEST = "placement_test"
    HEALTH_CHECK = "health_check"


class Severity(Enum):
    """Signal severity levels"""
    INFO = 0
    WARNING = 1
    ERROR = 2
    CRITICAL = 3


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class Signal:
    """Represents an incoming signal to the adaptive engine"""
    type: SignalType
    severity: Severity
    account_id: str
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    processed: bool = False


@dataclass
class AdaptiveAction:
    """Action taken by the adaptive engine"""
    action_type: str
    target: str
    parameters: Dict[str, Any]
    timestamp: datetime
    reasoning: str
    auto_revert_at: Optional[datetime] = None


@dataclass
class HealthMetrics:
    """Current health metrics"""
    overall_score: float
    open_rate_7d: float
    reply_rate_7d: float
    bounce_rate_7d: float
    spam_rate_7d: float
    blacklist_count: int
    active_accounts: int
    volume_utilization: float
    last_updated: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AccountState:
    """State tracking for individual account"""
    account_id: str
    status: SystemState
    health_score: float
    daily_sent: int
    daily_limit: int
    consecutive_failures: int
    last_activity: datetime
    throttle_until: Optional[datetime] = None
    recovery_started: Optional[datetime] = None


# ============================================================================
# SIGNAL PROCESSOR
# ============================================================================

class SignalProcessor:
    """
    Processes incoming signals and triggers appropriate responses.
    Uses a priority queue for signal handling.
    """

    def __init__(self):
        self.signal_queue: deque = deque(maxlen=10000)
        self.processed_count = 0
        self.signal_handlers: Dict[SignalType, List[Callable]] = {}
        self.aggregated_metrics: Dict[str, deque] = {
            "opens": deque(maxlen=1000),
            "replies": deque(maxlen=1000),
            "bounces": deque(maxlen=1000),
            "spam": deque(maxlen=1000),
        }

    def register_handler(self, signal_type: SignalType, handler: Callable):
        """Register a handler for a signal type"""
        if signal_type not in self.signal_handlers:
            self.signal_handlers[signal_type] = []
        self.signal_handlers[signal_type].append(handler)

    def emit(self, signal: Signal):
        """Emit a signal for processing"""
        self.signal_queue.append(signal)

        # Immediate processing for critical signals
        if signal.severity == Severity.CRITICAL:
            self._process_signal(signal)

    def process_pending(self, max_count: int = 100) -> int:
        """Process pending signals"""
        processed = 0

        while self.signal_queue and processed < max_count:
            signal = self.signal_queue.popleft()
            if not signal.processed:
                self._process_signal(signal)
                processed += 1

        return processed

    def _process_signal(self, signal: Signal):
        """Process a single signal"""
        signal.processed = True
        self.processed_count += 1

        # Update aggregated metrics
        if signal.type == SignalType.EMAIL_OPENED:
            self.aggregated_metrics["opens"].append(signal.timestamp)
        elif signal.type == SignalType.EMAIL_REPLIED:
            self.aggregated_metrics["replies"].append(signal.timestamp)
        elif signal.type == SignalType.EMAIL_BOUNCED:
            self.aggregated_metrics["bounces"].append(signal.timestamp)
        elif signal.type == SignalType.SPAM_DETECTED:
            self.aggregated_metrics["spam"].append(signal.timestamp)

        # Call handlers
        handlers = self.signal_handlers.get(signal.type, [])
        for handler in handlers:
            try:
                handler(signal)
            except Exception as e:
                logger.error(f"[SignalProcessor] Handler error: {e}")

    def get_rates(self, window_hours: int = 24) -> Dict[str, float]:
        """Calculate rates over time window"""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
        rates = {}

        for metric, timestamps in self.aggregated_metrics.items():
            recent = [t for t in timestamps if t > cutoff]
            rates[metric] = len(recent)

        return rates


# ============================================================================
# THROTTLE CONTROLLER
# ============================================================================

class ThrottleController:
    """
    Controls throttling behavior to protect sender reputation.
    Implements progressive throttling and recovery.
    """

    THROTTLE_LEVELS = {
        0: {"factor": 1.0, "description": "Normal operation"},
        1: {"factor": 0.75, "description": "Light throttle - 25% reduction"},
        2: {"factor": 0.50, "description": "Medium throttle - 50% reduction"},
        3: {"factor": 0.25, "description": "Heavy throttle - 75% reduction"},
        4: {"factor": 0.10, "description": "Emergency - 90% reduction"},
        5: {"factor": 0.0, "description": "Full stop - all sending paused"},
    }

    def __init__(self):
        self.current_level = 0
        self.level_history: List[Dict] = []
        self.auto_recovery = True
        self.recovery_delay = 3600  # 1 hour before attempting recovery
        self.last_escalation: Optional[datetime] = None
        self.escalation_count = 0

    def escalate(self, reason: str) -> int:
        """Escalate throttle level"""
        if self.current_level < 5:
            self.current_level += 1
            self.last_escalation = datetime.now(timezone.utc)
            self.escalation_count += 1

            self.level_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "escalate",
                "level": self.current_level,
                "reason": reason,
            })

            logger.warning(f"[ThrottleController] Escalated to level {self.current_level}: {reason}")

        return self.current_level

    def de_escalate(self, reason: str) -> int:
        """De-escalate throttle level"""
        if self.current_level > 0:
            self.current_level -= 1

            self.level_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "de_escalate",
                "level": self.current_level,
                "reason": reason,
            })

            logger.info(f"[ThrottleController] De-escalated to level {self.current_level}: {reason}")

        return self.current_level

    def reset(self, reason: str = "Manual reset"):
        """Reset to normal operation"""
        self.current_level = 0
        self.escalation_count = 0

        self.level_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "reset",
            "level": 0,
            "reason": reason,
        })

        logger.info(f"[ThrottleController] Reset to normal: {reason}")

    def get_throttle_factor(self) -> float:
        """Get current throttle factor"""
        return self.THROTTLE_LEVELS[self.current_level]["factor"]

    def should_attempt_recovery(self) -> bool:
        """Check if recovery should be attempted"""
        if not self.auto_recovery or self.current_level == 0:
            return False

        if self.last_escalation is None:
            return True

        elapsed = (datetime.now(timezone.utc) - self.last_escalation).total_seconds()
        return elapsed >= self.recovery_delay

    def get_status(self) -> Dict[str, Any]:
        """Get throttle status"""
        return {
            "level": self.current_level,
            "factor": self.get_throttle_factor(),
            "description": self.THROTTLE_LEVELS[self.current_level]["description"],
            "escalation_count": self.escalation_count,
            "last_escalation": self.last_escalation.isoformat() if self.last_escalation else None,
            "auto_recovery": self.auto_recovery,
            "history": self.level_history[-10:],
        }


# ============================================================================
# REPUTATION GUARDIAN
# ============================================================================

class ReputationGuardian:
    """
    Monitors and protects sender reputation.
    Implements proactive measures before reputation damage.
    """

    # Thresholds for action
    THRESHOLDS = {
        "bounce_rate_warning": 0.02,
        "bounce_rate_critical": 0.05,
        "spam_rate_warning": 0.01,
        "spam_rate_critical": 0.03,
        "open_rate_low": 0.15,
        "reply_rate_low": 0.05,
        "blacklist_any": 1,
    }

    def __init__(self):
        self.alerts: List[Dict] = []
        self.reputation_score = 100.0
        self.score_history: deque = deque(maxlen=100)
        self.protective_measures: List[str] = []

    def evaluate(self, metrics: HealthMetrics) -> Tuple[float, List[Dict]]:
        """
        Evaluate reputation based on metrics.
        Returns (score, alerts)
        """
        alerts = []
        score = 100.0

        # Bounce rate
        if metrics.bounce_rate_7d >= self.THRESHOLDS["bounce_rate_critical"]:
            score -= 30
            alerts.append({
                "level": "critical",
                "type": "bounce_rate",
                "message": f"Critical bounce rate: {metrics.bounce_rate_7d:.1%}",
                "action": "Immediately pause sending",
            })
        elif metrics.bounce_rate_7d >= self.THRESHOLDS["bounce_rate_warning"]:
            score -= 10
            alerts.append({
                "level": "warning",
                "type": "bounce_rate",
                "message": f"Elevated bounce rate: {metrics.bounce_rate_7d:.1%}",
                "action": "Review email list quality",
            })

        # Spam rate
        if metrics.spam_rate_7d >= self.THRESHOLDS["spam_rate_critical"]:
            score -= 40
            alerts.append({
                "level": "critical",
                "type": "spam_rate",
                "message": f"Critical spam rate: {metrics.spam_rate_7d:.1%}",
                "action": "Stop sending and investigate",
            })
        elif metrics.spam_rate_7d >= self.THRESHOLDS["spam_rate_warning"]:
            score -= 15
            alerts.append({
                "level": "warning",
                "type": "spam_rate",
                "message": f"Elevated spam rate: {metrics.spam_rate_7d:.1%}",
                "action": "Reduce volume and review content",
            })

        # Open rate
        if metrics.open_rate_7d < self.THRESHOLDS["open_rate_low"]:
            score -= 10
            alerts.append({
                "level": "warning",
                "type": "open_rate",
                "message": f"Low open rate: {metrics.open_rate_7d:.1%}",
                "action": "Improve subject lines and sending times",
            })

        # Blacklist
        if metrics.blacklist_count >= self.THRESHOLDS["blacklist_any"]:
            score -= 20 * metrics.blacklist_count
            alerts.append({
                "level": "critical",
                "type": "blacklist",
                "message": f"Listed on {metrics.blacklist_count} blacklist(s)",
                "action": "Initiate delisting process immediately",
            })

        # Volume utilization (sending too fast)
        if metrics.volume_utilization > 0.95:
            score -= 5
            alerts.append({
                "level": "info",
                "type": "volume",
                "message": "Near daily volume limit",
                "action": "Consider reducing send rate",
            })

        # Update score
        self.reputation_score = max(0, min(100, score))
        self.score_history.append({
            "score": self.reputation_score,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        self.alerts = alerts
        return self.reputation_score, alerts

    def get_protective_measures(self) -> List[str]:
        """Get recommended protective measures based on score"""
        measures = []

        if self.reputation_score < 30:
            measures.append("EMERGENCY: Stop all sending immediately")
            measures.append("Review and clean email lists")
            measures.append("Check authentication (SPF, DKIM, DMARC)")
            measures.append("Contact ESP support")

        elif self.reputation_score < 50:
            measures.append("Reduce sending volume by 50%")
            measures.append("Focus on most engaged recipients")
            measures.append("Increase warmup conversation quality")
            measures.append("Run inbox placement tests")

        elif self.reputation_score < 70:
            measures.append("Reduce sending volume by 25%")
            measures.append("Improve content quality")
            measures.append("Optimize send times")

        elif self.reputation_score < 85:
            measures.append("Monitor metrics closely")
            measures.append("Minor optimizations recommended")

        else:
            measures.append("Reputation is healthy")
            measures.append("Continue current strategy")

        self.protective_measures = measures
        return measures

    def get_status(self) -> Dict[str, Any]:
        """Get reputation guardian status"""
        return {
            "score": self.reputation_score,
            "health": (
                "critical" if self.reputation_score < 30 else
                "poor" if self.reputation_score < 50 else
                "fair" if self.reputation_score < 70 else
                "good" if self.reputation_score < 85 else
                "excellent"
            ),
            "alerts": self.alerts,
            "protective_measures": self.protective_measures,
            "score_trend": list(self.score_history)[-10:],
        }


# ============================================================================
# FALLBACK MANAGER
# ============================================================================

class FallbackManager:
    """
    Manages fallback strategies when primary methods fail.
    Implements graceful degradation.
    """

    def __init__(self):
        self.fallback_stack: List[str] = []
        self.active_fallbacks: Dict[str, datetime] = {}
        self.fallback_history: List[Dict] = []

    def activate_fallback(self, fallback_type: str, reason: str, duration_hours: int = 24):
        """Activate a fallback strategy"""
        self.active_fallbacks[fallback_type] = datetime.now(timezone.utc) + timedelta(hours=duration_hours)
        self.fallback_stack.append(fallback_type)

        self.fallback_history.append({
            "type": fallback_type,
            "reason": reason,
            "activated_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": self.active_fallbacks[fallback_type].isoformat(),
        })

        logger.info(f"[FallbackManager] Activated: {fallback_type} - {reason}")

    def deactivate_fallback(self, fallback_type: str):
        """Deactivate a fallback strategy"""
        if fallback_type in self.active_fallbacks:
            del self.active_fallbacks[fallback_type]

        if fallback_type in self.fallback_stack:
            self.fallback_stack.remove(fallback_type)

        logger.info(f"[FallbackManager] Deactivated: {fallback_type}")

    def is_fallback_active(self, fallback_type: str) -> bool:
        """Check if fallback is active"""
        if fallback_type not in self.active_fallbacks:
            return False

        if datetime.now(timezone.utc) > self.active_fallbacks[fallback_type]:
            self.deactivate_fallback(fallback_type)
            return False

        return True

    def get_active_fallbacks(self) -> List[str]:
        """Get list of active fallbacks"""
        # Clean expired
        expired = [k for k, v in self.active_fallbacks.items() if datetime.now(timezone.utc) > v]
        for e in expired:
            self.deactivate_fallback(e)

        return list(self.active_fallbacks.keys())

    def get_fallback_config(self, fallback_type: str) -> Dict[str, Any]:
        """Get configuration for a fallback type"""
        configs = {
            "reduce_volume": {
                "volume_factor": 0.5,
                "priority": "high",
            },
            "conservative_timing": {
                "only_optimal_hours": True,
                "min_delay_minutes": 30,
            },
            "safe_content_only": {
                "use_templates": True,
                "avoid_links": True,
            },
            "provider_specific": {
                "skip_problematic_providers": True,
            },
            "manual_review": {
                "require_approval": True,
            },
        }
        return configs.get(fallback_type, {})


# ============================================================================
# MAIN ADAPTIVE ENGINE
# ============================================================================

class WarmupAdaptiveEngine:
    """
    Central adaptive engine orchestrating real-time control.

    Features:
    - Signal processing and response
    - ML-based decision making
    - Automatic throttling and recovery
    - Reputation protection
    - Self-healing mechanisms
    """

    def __init__(self):
        self.ml_engine = get_warmup_ml_engine()
        self.optimizer = get_warmup_optimizer()

        self.signal_processor = SignalProcessor()
        self.throttle_controller = ThrottleController()
        self.reputation_guardian = ReputationGuardian()
        self.fallback_manager = FallbackManager()

        self.system_state = SystemState.HEALTHY
        self.account_states: Dict[str, AccountState] = {}
        self.action_log: deque = deque(maxlen=1000)

        # Register signal handlers
        self._register_handlers()

        logger.info("[WarmupAdaptiveEngine] Initialized")

    def _register_handlers(self):
        """Register signal handlers"""
        self.signal_processor.register_handler(
            SignalType.SPAM_DETECTED,
            self._handle_spam_detected
        )
        self.signal_processor.register_handler(
            SignalType.EMAIL_BOUNCED,
            self._handle_bounce
        )
        self.signal_processor.register_handler(
            SignalType.BLACKLIST_DETECTED,
            self._handle_blacklist
        )
        self.signal_processor.register_handler(
            SignalType.RATE_LIMIT_HIT,
            self._handle_rate_limit
        )

    def _handle_spam_detected(self, signal: Signal):
        """Handle spam detection signal"""
        account_id = signal.account_id

        # Update account state
        if account_id in self.account_states:
            state = self.account_states[account_id]
            state.consecutive_failures += 1

            if state.consecutive_failures >= 3:
                # Throttle account
                state.throttle_until = datetime.now(timezone.utc) + timedelta(hours=2)
                state.status = SystemState.THROTTLED
                self._log_action("throttle_account", account_id, {
                    "reason": "Multiple spam detections",
                    "consecutive_failures": state.consecutive_failures,
                })

        # Escalate system throttle if widespread
        spam_count = len([s for s in self.account_states.values()
                         if s.consecutive_failures >= 2])
        if spam_count >= 3:
            self.throttle_controller.escalate("Multiple accounts experiencing spam issues")
            self.system_state = SystemState.THROTTLED

    def _handle_bounce(self, signal: Signal):
        """Handle bounce signal"""
        bounce_rate = signal.data.get("bounce_rate", 0)

        if bounce_rate > 0.05:
            self.throttle_controller.escalate(f"High bounce rate: {bounce_rate:.1%}")
            self.fallback_manager.activate_fallback(
                "reduce_volume",
                "High bounce rate detected",
                duration_hours=12
            )

    def _handle_blacklist(self, signal: Signal):
        """Handle blacklist detection"""
        blacklist_name = signal.data.get("blacklist", "unknown")

        self.throttle_controller.escalate(f"Blacklisted on {blacklist_name}")
        self.system_state = SystemState.EMERGENCY

        self.fallback_manager.activate_fallback(
            "manual_review",
            f"Blacklist detected: {blacklist_name}",
            duration_hours=48
        )

        self._log_action("emergency_blacklist", signal.account_id, {
            "blacklist": blacklist_name,
            "action": "Initiated emergency protocol",
        })

    def _handle_rate_limit(self, signal: Signal):
        """Handle rate limit hit"""
        provider = signal.data.get("provider", "unknown")

        self._log_action("rate_limit", signal.account_id, {
            "provider": provider,
            "action": "Implementing backoff",
        })

        # Activate conservative timing
        self.fallback_manager.activate_fallback(
            "conservative_timing",
            f"Rate limit hit on {provider}",
            duration_hours=4
        )

    def _log_action(self, action_type: str, target: str, parameters: Dict):
        """Log an action"""
        action = AdaptiveAction(
            action_type=action_type,
            target=target,
            parameters=parameters,
            timestamp=datetime.now(timezone.utc),
            reasoning=parameters.get("reason", ""),
        )
        self.action_log.append(action)

    def emit_signal(self, signal_type: SignalType, account_id: str,
                   data: Dict[str, Any], severity: Severity = Severity.INFO):
        """Emit a signal to the engine"""
        signal = Signal(
            type=signal_type,
            severity=severity,
            account_id=account_id,
            data=data,
        )
        self.signal_processor.emit(signal)

    def process_signals(self) -> int:
        """Process pending signals"""
        return self.signal_processor.process_pending()

    def get_send_decision(
        self,
        account_id: str,
        target_email: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get comprehensive send decision combining ML and optimization.

        Returns decision with reasoning and recommendations.
        """
        start_time = time.time()

        # 1. Check system state
        if self.system_state == SystemState.EMERGENCY:
            return {
                "decision": "block",
                "reason": "System in emergency state",
                "can_send": False,
                "recommendations": self.reputation_guardian.get_protective_measures(),
            }

        # 2. Check account state
        if account_id in self.account_states:
            account = self.account_states[account_id]
            if account.throttle_until and datetime.now(timezone.utc) < account.throttle_until:
                remaining = (account.throttle_until - datetime.now(timezone.utc)).total_seconds()
                return {
                    "decision": "throttled",
                    "reason": f"Account throttled for {remaining:.0f}s",
                    "can_send": False,
                    "retry_after": account.throttle_until.isoformat(),
                }

        # 3. Check fallbacks
        active_fallbacks = self.fallback_manager.get_active_fallbacks()
        if "manual_review" in active_fallbacks:
            return {
                "decision": "review_required",
                "reason": "Manual review fallback active",
                "can_send": False,
                "fallbacks": active_fallbacks,
            }

        # 4. Get ML recommendation
        state = self._build_state(account_id, context)
        action, ml_details = self.ml_engine.get_optimal_action(state)

        # 5. Get optimizer recommendation
        opt_result = self.optimizer.get_optimization(
            account_id,
            target_email,
            context.get("current_stats", {})
        )

        # 6. Apply throttle factor
        throttle_factor = self.throttle_controller.get_throttle_factor()

        # 7. Combine decisions
        can_send = (
            action in [ActionType.SEND_NOW, ActionType.INCREASE_VOLUME] and
            opt_result.send_now and
            throttle_factor > 0
        )

        # 8. Apply fallback modifications
        if "reduce_volume" in active_fallbacks:
            volume_factor = self.fallback_manager.get_fallback_config("reduce_volume")["volume_factor"]
            throttle_factor *= volume_factor

        if "conservative_timing" in active_fallbacks:
            # Only send during optimal hours
            hour = datetime.now(timezone.utc).hour
            if hour not in [9, 10, 11, 14, 15, 16]:
                can_send = False

        computation_time = (time.time() - start_time) * 1000

        return {
            "decision": "send" if can_send else "wait",
            "can_send": can_send,
            "ml_action": action.value,
            "ml_confidence": ml_details["confidence"],
            "ml_risk_level": ml_details["risk_level"],
            "optimization": {
                "send_now": opt_result.send_now,
                "optimal_time": opt_result.optimal_time.isoformat() if opt_result.optimal_time else None,
                "volume_recommendation": opt_result.volume_recommendation,
                "provider_order": opt_result.provider_order,
                "reasoning": opt_result.reasoning,
            },
            "throttle": {
                "level": self.throttle_controller.current_level,
                "factor": throttle_factor,
            },
            "active_fallbacks": active_fallbacks,
            "system_state": self.system_state.value,
            "computation_time_ms": computation_time,
            "model_version": "adaptive-4.0.0",
        }

    def _build_state(self, account_id: str, context: Dict[str, Any]) -> State:
        """Build RL state from context"""
        now = datetime.now(timezone.utc)

        return State(
            hour_of_day=now.hour,
            day_of_week=now.weekday(),
            emails_sent_today=context.get("emails_sent_today", 0),
            emails_remaining=context.get("emails_remaining", 50),
            last_open_rate=context.get("open_rate", 0.5),
            last_reply_rate=context.get("reply_rate", 0.3),
            spam_rate_24h=context.get("spam_rate", 0.01),
            bounce_rate_24h=context.get("bounce_rate", 0.02),
            provider_score=context.get("provider_score", 0.8),
            account_age_days=context.get("account_age_days", 30),
            warmup_day=context.get("warmup_day", 1),
            consecutive_successes=context.get("consecutive_successes", 0),
            consecutive_failures=context.get("consecutive_failures", 0),
        )

    def record_outcome(
        self,
        account_id: str,
        action_taken: str,
        outcome: Dict[str, Any]
    ):
        """Record outcome for learning"""
        # Update account state
        if account_id in self.account_states:
            state = self.account_states[account_id]
            if outcome.get("success", False):
                state.consecutive_failures = 0
                if state.status == SystemState.RECOVERY:
                    state.status = SystemState.HEALTHY
            else:
                state.consecutive_failures += 1

        # Update optimizer
        self.optimizer.record_engagement(
            account_id=account_id,
            target_email=outcome.get("target_email", ""),
            opened=outcome.get("opened", False),
            replied=outcome.get("replied", False),
            was_spam=outcome.get("was_spam", False),
        )

        # Train ML model
        if "state" in outcome and "next_state" in outcome:
            experience = Experience(
                state=outcome["state"],
                action=ActionType(action_taken),
                reward=self._calculate_reward(outcome),
                next_state=outcome["next_state"],
                done=outcome.get("done", False),
            )
            self.ml_engine.train_on_experience(experience)

    def _calculate_reward(self, outcome: Dict[str, Any]) -> float:
        """Calculate RL reward from outcome"""
        reward = 0.0

        # Positive rewards
        if outcome.get("delivered", False):
            reward += 0.3
        if outcome.get("opened", False):
            reward += 0.4
        if outcome.get("replied", False):
            reward += 0.5

        # Negative rewards
        if outcome.get("bounced", False):
            reward -= 0.5
        if outcome.get("was_spam", False):
            reward -= 1.0
        if outcome.get("unsubscribed", False):
            reward -= 0.3

        return reward

    def update_health_metrics(self, metrics: HealthMetrics):
        """Update system health metrics"""
        # Evaluate reputation
        score, alerts = self.reputation_guardian.evaluate(metrics)

        # Adjust system state based on score
        if score < 30:
            self.system_state = SystemState.EMERGENCY
            self.throttle_controller.escalate("Critical reputation score")
        elif score < 50:
            self.system_state = SystemState.DEGRADED
        elif score < 70:
            self.system_state = SystemState.RECOVERY
        else:
            # Attempt recovery if conditions allow
            if self.throttle_controller.should_attempt_recovery():
                self.throttle_controller.de_escalate("Metrics improved")

            if self.throttle_controller.current_level == 0:
                self.system_state = SystemState.HEALTHY

        logger.info(f"[AdaptiveEngine] Health update - Score: {score}, State: {self.system_state.value}")

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive dashboard data"""
        return {
            "system": {
                "state": self.system_state.value,
                "throttle": self.throttle_controller.get_status(),
            },
            "reputation": self.reputation_guardian.get_status(),
            "fallbacks": {
                "active": self.fallback_manager.get_active_fallbacks(),
                "history": self.fallback_manager.fallback_history[-10:],
            },
            "ml_engine": {
                "training_stats": self.ml_engine.get_training_stats(),
            },
            "optimizer": self.optimizer.get_statistics(),
            "signals": {
                "processed": self.signal_processor.processed_count,
                "pending": len(self.signal_processor.signal_queue),
                "rates": self.signal_processor.get_rates(24),
            },
            "accounts": {
                account_id: {
                    "status": state.status.value,
                    "health_score": state.health_score,
                    "daily_sent": state.daily_sent,
                    "consecutive_failures": state.consecutive_failures,
                }
                for account_id, state in self.account_states.items()
            },
            "recent_actions": [
                {
                    "type": a.action_type,
                    "target": a.target,
                    "timestamp": a.timestamp.isoformat(),
                    "reasoning": a.reasoning,
                }
                for a in list(self.action_log)[-20:]
            ],
        }

    def register_account(self, account_id: str, daily_limit: int, health_score: float):
        """Register an account with the engine"""
        self.account_states[account_id] = AccountState(
            account_id=account_id,
            status=SystemState.HEALTHY,
            health_score=health_score,
            daily_sent=0,
            daily_limit=daily_limit,
            consecutive_failures=0,
            last_activity=datetime.now(timezone.utc),
        )

        self.optimizer.load_balancer.register_account(account_id, daily_limit, health_score)
        logger.info(f"[AdaptiveEngine] Registered account: {account_id}")

    def manual_override(self, action: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Manual override for admin control"""
        result = {"success": False}

        if action == "pause_all":
            self.system_state = SystemState.PAUSED
            self.throttle_controller.escalate("Manual pause")
            result = {"success": True, "message": "All sending paused"}

        elif action == "resume_all":
            self.system_state = SystemState.HEALTHY
            self.throttle_controller.reset("Manual resume")
            result = {"success": True, "message": "Sending resumed"}

        elif action == "reset_throttle":
            self.throttle_controller.reset("Manual reset")
            result = {"success": True, "message": "Throttle reset to normal"}

        elif action == "clear_fallbacks":
            for fb in list(self.fallback_manager.active_fallbacks.keys()):
                self.fallback_manager.deactivate_fallback(fb)
            result = {"success": True, "message": "All fallbacks cleared"}

        elif action == "throttle_account":
            account_id = parameters.get("account_id")
            hours = parameters.get("hours", 2)
            if account_id in self.account_states:
                self.account_states[account_id].throttle_until = datetime.now(timezone.utc) + timedelta(hours=hours)
                result = {"success": True, "message": f"Account throttled for {hours} hours"}

        self._log_action(f"manual_{action}", "admin", parameters)
        return result


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_adaptive_engine_instance: Optional[WarmupAdaptiveEngine] = None


def get_warmup_adaptive_engine() -> WarmupAdaptiveEngine:
    """Get singleton adaptive engine instance"""
    global _adaptive_engine_instance
    if _adaptive_engine_instance is None:
        _adaptive_engine_instance = WarmupAdaptiveEngine()
    return _adaptive_engine_instance
