"""
Warmup Optimizer Service - PHASE 4 GOD TIER EDITION

Advanced optimization algorithms for email warmup:
- Time Series Analysis for optimal send windows
- Provider-specific timing optimization
- Dynamic volume ramping with feedback loops
- Cross-account load balancing
- Intelligent throttling and recovery

Author: Metaminds AI
Version: 4.0.0 - ULTRA GOD TIER
"""

import logging
import math
import random
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import statistics

logger = logging.getLogger(__name__)


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class TimeSlot:
    """Represents a time slot with performance metrics"""
    hour: int
    day_of_week: int
    open_rate: float = 0.0
    reply_rate: float = 0.0
    spam_rate: float = 0.0
    sample_count: int = 0
    last_updated: datetime = field(default_factory=datetime.utcnow)

    @property
    def score(self) -> float:
        """Calculate composite score for this time slot"""
        if self.sample_count == 0:
            return 0.5  # Unknown slots get neutral score

        # Weighted score: open_rate matters most, then reply, penalize spam
        return (
            self.open_rate * 0.4 +
            self.reply_rate * 0.4 -
            self.spam_rate * 0.5 +
            min(self.sample_count / 100, 0.2)  # Confidence bonus
        )


@dataclass
class ProviderProfile:
    """Provider-specific performance profile"""
    provider: str
    optimal_hours: List[int] = field(default_factory=list)
    avoid_hours: List[int] = field(default_factory=list)
    max_hourly_rate: int = 10
    cooldown_after_spam: int = 3600  # seconds
    weight_multiplier: float = 1.0
    authentication_score: float = 1.0

    # Performance tracking
    total_sent: int = 0
    total_opened: int = 0
    total_spam: int = 0
    last_spam_time: Optional[datetime] = None


@dataclass
class VolumeState:
    """Tracks volume ramping state"""
    current_daily_limit: int = 10
    target_daily_limit: int = 50
    ramp_rate: float = 0.15  # 15% daily increase
    consecutive_good_days: int = 0
    consecutive_bad_days: int = 0
    last_adjustment: datetime = field(default_factory=datetime.utcnow)
    adjustment_history: List[Dict] = field(default_factory=list)


@dataclass
class OptimizationResult:
    """Result of optimization calculation"""
    recommended_action: str
    send_now: bool
    optimal_time: Optional[datetime]
    volume_recommendation: int
    provider_order: List[str]
    confidence: float
    reasoning: List[str]
    metrics: Dict[str, float]


# ============================================================================
# TIME SERIES ANALYSIS
# ============================================================================

class TimeSeriesAnalyzer:
    """
    Advanced time series analysis for engagement patterns.
    Uses exponential smoothing and seasonal decomposition.
    """

    def __init__(self, alpha: float = 0.3, beta: float = 0.1, gamma: float = 0.2):
        self.alpha = alpha  # Level smoothing
        self.beta = beta    # Trend smoothing
        self.gamma = gamma  # Seasonal smoothing
        self.seasonal_period = 24 * 7  # Hourly data, weekly seasonality

        # State
        self.level = 0.0
        self.trend = 0.0
        self.seasonal = [0.0] * self.seasonal_period
        self.fitted_values: List[float] = []

    def fit(self, data: List[float]):
        """
        Fit Holt-Winters exponential smoothing model.
        """
        n = len(data)
        if n < self.seasonal_period * 2:
            # Not enough data for seasonal model
            self._fit_simple(data)
            return

        # Initialize seasonal components
        for i in range(self.seasonal_period):
            season_values = [data[i + j * self.seasonal_period]
                           for j in range(n // self.seasonal_period)
                           if i + j * self.seasonal_period < n]
            if season_values:
                self.seasonal[i] = statistics.mean(season_values) - statistics.mean(data[:self.seasonal_period])

        # Initialize level and trend
        self.level = statistics.mean(data[:self.seasonal_period])
        self.trend = (statistics.mean(data[self.seasonal_period:2*self.seasonal_period]) -
                     statistics.mean(data[:self.seasonal_period])) / self.seasonal_period

        # Fit model
        self.fitted_values = []
        for t in range(n):
            seasonal_idx = t % self.seasonal_period

            # Forecast
            forecast = (self.level + self.trend) + self.seasonal[seasonal_idx]
            self.fitted_values.append(forecast)

            # Update
            new_level = self.alpha * (data[t] - self.seasonal[seasonal_idx]) + (1 - self.alpha) * (self.level + self.trend)
            new_trend = self.beta * (new_level - self.level) + (1 - self.beta) * self.trend
            self.seasonal[seasonal_idx] = self.gamma * (data[t] - new_level) + (1 - self.gamma) * self.seasonal[seasonal_idx]

            self.level = new_level
            self.trend = new_trend

    def _fit_simple(self, data: List[float]):
        """Simple exponential smoothing for short series"""
        if not data:
            return

        self.level = data[0]
        self.fitted_values = [self.level]

        for value in data[1:]:
            self.level = self.alpha * value + (1 - self.alpha) * self.level
            self.fitted_values.append(self.level)

    def forecast(self, steps: int) -> List[float]:
        """Generate forecasts for future steps"""
        forecasts = []
        current_level = self.level
        current_trend = self.trend

        for h in range(1, steps + 1):
            seasonal_idx = (len(self.fitted_values) + h - 1) % self.seasonal_period
            forecast = current_level + h * current_trend + self.seasonal[seasonal_idx]
            forecasts.append(forecast)

        return forecasts

    def detect_trend(self, data: List[float], window: int = 7) -> str:
        """Detect trend direction in recent data"""
        if len(data) < window * 2:
            return "insufficient_data"

        recent = statistics.mean(data[-window:])
        earlier = statistics.mean(data[-window*2:-window])

        change = (recent - earlier) / (earlier + 1e-8)

        if change > 0.1:
            return "rising"
        elif change < -0.1:
            return "declining"
        return "stable"

    def find_seasonality(self, data: List[float]) -> Dict[str, Any]:
        """Detect and analyze seasonal patterns"""
        if len(data) < 48:  # Need at least 2 days of hourly data
            return {"detected": False}

        # Calculate autocorrelation at different lags
        n = len(data)
        mean = statistics.mean(data)
        var = sum((x - mean) ** 2 for x in data) / n

        autocorr = {}
        for lag in [24, 168]:  # Daily and weekly
            if lag >= n:
                continue
            cov = sum((data[i] - mean) * (data[i - lag] - mean) for i in range(lag, n)) / (n - lag)
            autocorr[lag] = cov / (var + 1e-8)

        daily_correlation = autocorr.get(24, 0)
        weekly_correlation = autocorr.get(168, 0)

        return {
            "detected": daily_correlation > 0.3 or weekly_correlation > 0.3,
            "daily_strength": daily_correlation,
            "weekly_strength": weekly_correlation,
            "dominant_period": "weekly" if weekly_correlation > daily_correlation else "daily",
        }


# ============================================================================
# PROVIDER OPTIMIZER
# ============================================================================

class ProviderOptimizer:
    """
    Optimizes email delivery across different providers.
    Learns provider-specific preferences and constraints.
    """

    # Default provider profiles
    DEFAULT_PROFILES = {
        "gmail": ProviderProfile(
            provider="gmail",
            optimal_hours=[9, 10, 11, 14, 15, 16],
            avoid_hours=[0, 1, 2, 3, 4, 5, 23],
            max_hourly_rate=8,
            cooldown_after_spam=7200,
            weight_multiplier=1.2,
        ),
        "outlook": ProviderProfile(
            provider="outlook",
            optimal_hours=[8, 9, 10, 11, 13, 14, 15],
            avoid_hours=[0, 1, 2, 3, 4, 22, 23],
            max_hourly_rate=10,
            cooldown_after_spam=3600,
            weight_multiplier=1.1,
        ),
        "yahoo": ProviderProfile(
            provider="yahoo",
            optimal_hours=[10, 11, 12, 14, 15],
            avoid_hours=[0, 1, 2, 3, 4, 5],
            max_hourly_rate=6,
            cooldown_after_spam=5400,
            weight_multiplier=0.9,
        ),
        "other": ProviderProfile(
            provider="other",
            optimal_hours=[9, 10, 11, 14, 15],
            avoid_hours=[0, 1, 2, 3, 4],
            max_hourly_rate=10,
            cooldown_after_spam=3600,
            weight_multiplier=1.0,
        ),
    }

    def __init__(self):
        self.profiles: Dict[str, ProviderProfile] = dict(self.DEFAULT_PROFILES)
        self.hourly_counts: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self.provider_scores: Dict[str, float] = defaultdict(lambda: 0.5)

    def get_provider_from_email(self, email: str) -> str:
        """Extract provider from email address"""
        domain = email.split("@")[-1].lower() if "@" in email else ""

        if "gmail" in domain or "googlemail" in domain:
            return "gmail"
        elif "outlook" in domain or "hotmail" in domain or "live" in domain or "msn" in domain:
            return "outlook"
        elif "yahoo" in domain or "ymail" in domain:
            return "yahoo"
        return "other"

    def can_send_now(self, provider: str) -> Tuple[bool, str]:
        """Check if we can send to provider now"""
        profile = self.profiles.get(provider, self.profiles["other"])
        hour = datetime.now(timezone.utc).hour

        # Check cooldown
        if profile.last_spam_time:
            elapsed = (datetime.now(timezone.utc) - profile.last_spam_time).total_seconds()
            if elapsed < profile.cooldown_after_spam:
                remaining = int(profile.cooldown_after_spam - elapsed)
                return False, f"Cooldown active: {remaining}s remaining"

        # Check hourly rate
        current_hour_count = self.hourly_counts[provider][hour]
        if current_hour_count >= profile.max_hourly_rate:
            return False, f"Hourly rate limit reached: {current_hour_count}/{profile.max_hourly_rate}"

        # Check avoid hours
        if hour in profile.avoid_hours:
            return False, f"Hour {hour} is in avoid list for {provider}"

        return True, "OK"

    def record_send(self, provider: str, success: bool, was_spam: bool = False):
        """Record send result"""
        profile = self.profiles.get(provider, self.profiles["other"])
        hour = datetime.now(timezone.utc).hour

        profile.total_sent += 1
        self.hourly_counts[provider][hour] += 1

        if success:
            profile.total_opened += 1
            # Increase score
            self.provider_scores[provider] = min(1.0, self.provider_scores[provider] + 0.01)
        else:
            # Decrease score
            self.provider_scores[provider] = max(0.0, self.provider_scores[provider] - 0.02)

        if was_spam:
            profile.total_spam += 1
            profile.last_spam_time = datetime.now(timezone.utc)
            self.provider_scores[provider] = max(0.0, self.provider_scores[provider] - 0.1)

    def get_optimal_provider_order(self) -> List[str]:
        """Get providers ordered by current score"""
        scored = [(p, s) for p, s in self.provider_scores.items()]
        scored.sort(key=lambda x: x[1], reverse=True)
        return [p for p, s in scored]

    def get_next_optimal_window(self, provider: str) -> Tuple[datetime, float]:
        """Find next optimal sending window for provider"""
        profile = self.profiles.get(provider, self.profiles["other"])
        now = datetime.now(timezone.utc)

        best_hour = None
        best_score = -1

        # Check next 24 hours
        for offset in range(24):
            check_time = now + timedelta(hours=offset)
            hour = check_time.hour

            if hour in profile.avoid_hours:
                continue

            score = 0.5
            if hour in profile.optimal_hours:
                score = 0.9

            # Adjust for historical performance
            if profile.total_sent > 0:
                historical_rate = profile.total_opened / profile.total_sent
                score = score * 0.5 + historical_rate * 0.5

            if score > best_score:
                best_score = score
                best_hour = check_time.replace(minute=0, second=0, microsecond=0)

        return best_hour or now, best_score

    def reset_hourly_counts(self):
        """Reset hourly counts (call on hour change)"""
        self.hourly_counts.clear()


# ============================================================================
# VOLUME OPTIMIZER
# ============================================================================

class VolumeOptimizer:
    """
    Intelligent volume ramping with feedback loops.
    Adapts send volume based on engagement and deliverability signals.
    """

    # Ramp profiles
    RAMP_PROFILES = {
        "conservative": {"initial": 5, "max": 30, "rate": 0.10, "recovery_rate": 0.5},
        "balanced": {"initial": 10, "max": 50, "rate": 0.15, "recovery_rate": 0.6},
        "aggressive": {"initial": 20, "max": 100, "rate": 0.20, "recovery_rate": 0.7},
        "enterprise": {"initial": 50, "max": 500, "rate": 0.25, "recovery_rate": 0.8},
    }

    def __init__(self, profile: str = "balanced"):
        self.profile_config = self.RAMP_PROFILES.get(profile, self.RAMP_PROFILES["balanced"])
        self.state = VolumeState(
            current_daily_limit=self.profile_config["initial"],
            target_daily_limit=self.profile_config["max"],
        )
        self.daily_metrics: List[Dict] = []

    def record_day_metrics(self, metrics: Dict[str, float]):
        """
        Record daily performance metrics.

        Expected keys:
        - open_rate, reply_rate, bounce_rate, spam_rate, delivered_count
        """
        self.daily_metrics.append({
            **metrics,
            "date": datetime.now(timezone.utc).date().isoformat(),
            "volume": self.state.current_daily_limit,
        })

        # Evaluate performance
        is_good_day = self._evaluate_day(metrics)

        if is_good_day:
            self.state.consecutive_good_days += 1
            self.state.consecutive_bad_days = 0
        else:
            self.state.consecutive_bad_days += 1
            self.state.consecutive_good_days = 0

        # Adjust volume
        self._adjust_volume()

    def _evaluate_day(self, metrics: Dict[str, float]) -> bool:
        """Evaluate if day was successful"""
        bounce_threshold = 0.02
        spam_threshold = 0.02
        min_open_rate = 0.20

        bounce_ok = metrics.get("bounce_rate", 0) <= bounce_threshold
        spam_ok = metrics.get("spam_rate", 0) <= spam_threshold
        open_ok = metrics.get("open_rate", 0) >= min_open_rate

        return bounce_ok and spam_ok and open_ok

    def _adjust_volume(self):
        """Adjust volume based on performance"""
        now = datetime.now(timezone.utc)

        # Minimum time between adjustments
        if (now - self.state.last_adjustment).total_seconds() < 86400:  # 24 hours
            return

        old_limit = self.state.current_daily_limit

        if self.state.consecutive_good_days >= 2:
            # Increase volume
            increase = int(self.state.current_daily_limit * self.profile_config["rate"])
            self.state.current_daily_limit = min(
                self.state.target_daily_limit,
                self.state.current_daily_limit + max(1, increase)
            )
            action = "increase"

        elif self.state.consecutive_bad_days >= 1:
            # Decrease volume
            decrease_rate = 0.3 if self.state.consecutive_bad_days >= 3 else 0.15
            decrease = int(self.state.current_daily_limit * decrease_rate)
            self.state.current_daily_limit = max(
                self.profile_config["initial"],
                self.state.current_daily_limit - max(1, decrease)
            )
            action = "decrease"

        else:
            action = "hold"

        if old_limit != self.state.current_daily_limit:
            self.state.last_adjustment = now
            self.state.adjustment_history.append({
                "date": now.isoformat(),
                "action": action,
                "old_limit": old_limit,
                "new_limit": self.state.current_daily_limit,
                "consecutive_good": self.state.consecutive_good_days,
                "consecutive_bad": self.state.consecutive_bad_days,
            })

            logger.info(f"[VolumeOptimizer] Volume adjusted: {old_limit} -> {self.state.current_daily_limit} ({action})")

    def get_recommended_volume(self) -> int:
        """Get current recommended daily volume"""
        return self.state.current_daily_limit

    def get_hourly_distribution(self) -> Dict[int, int]:
        """
        Get optimal hourly send distribution.
        Concentrates volume in high-engagement hours.
        """
        total = self.state.current_daily_limit

        # Peak hours get more volume
        distribution = {h: 0 for h in range(24)}

        # Business hours distribution (weighted)
        weights = {
            9: 1.2, 10: 1.5, 11: 1.3,
            14: 1.4, 15: 1.3, 16: 1.1,
            # Off-peak but acceptable
            8: 0.8, 12: 0.7, 13: 0.6, 17: 0.9,
        }

        total_weight = sum(weights.values())

        for hour, weight in weights.items():
            distribution[hour] = max(1, int(total * (weight / total_weight)))

        # Ensure we hit total (might be slightly off due to rounding)
        allocated = sum(distribution.values())
        if allocated < total:
            distribution[10] += total - allocated

        return distribution

    def simulate_ramp(self, days: int = 30) -> List[Dict]:
        """Simulate volume ramp over specified days"""
        simulation = []
        current = self.profile_config["initial"]

        for day in range(1, days + 1):
            simulation.append({
                "day": day,
                "volume": current,
                "projected_sends": current,
            })

            # Simulate good performance
            if random.random() > 0.1:  # 90% good days
                increase = int(current * self.profile_config["rate"])
                current = min(self.profile_config["max"], current + max(1, increase))
            else:
                # Bad day - reduce
                current = max(self.profile_config["initial"], int(current * 0.85))

        return simulation


# ============================================================================
# LOAD BALANCER
# ============================================================================

class CrossAccountLoadBalancer:
    """
    Balances warmup load across multiple accounts.
    Prevents any single account from being over-utilized.
    """

    def __init__(self):
        self.account_states: Dict[str, Dict] = {}
        self.global_hourly_limit = 100
        self.current_hour_total = 0

    def register_account(self, account_id: str, daily_limit: int, health_score: float):
        """Register account with load balancer"""
        self.account_states[account_id] = {
            "daily_limit": daily_limit,
            "health_score": health_score,
            "sent_today": 0,
            "sent_this_hour": 0,
            "last_send": None,
            "cooldown_until": None,
            "priority": health_score,  # Higher health = higher priority
        }

    def can_send(self, account_id: str) -> Tuple[bool, str]:
        """Check if account can send"""
        if account_id not in self.account_states:
            return False, "Account not registered"

        state = self.account_states[account_id]

        # Check cooldown
        if state["cooldown_until"] and datetime.now(timezone.utc) < state["cooldown_until"]:
            return False, "Account in cooldown"

        # Check daily limit
        if state["sent_today"] >= state["daily_limit"]:
            return False, "Daily limit reached"

        # Check global hourly limit
        if self.current_hour_total >= self.global_hourly_limit:
            return False, "Global hourly limit reached"

        # Check minimum spacing (2 minutes between sends)
        if state["last_send"]:
            elapsed = (datetime.now(timezone.utc) - state["last_send"]).total_seconds()
            if elapsed < 120:
                return False, f"Minimum spacing: wait {120 - elapsed:.0f}s"

        return True, "OK"

    def record_send(self, account_id: str, success: bool):
        """Record send event"""
        if account_id not in self.account_states:
            return

        state = self.account_states[account_id]
        state["sent_today"] += 1
        state["sent_this_hour"] += 1
        state["last_send"] = datetime.now(timezone.utc)
        self.current_hour_total += 1

        if not success:
            # Apply cooldown on failure
            state["cooldown_until"] = datetime.now(timezone.utc) + timedelta(minutes=30)
            state["priority"] *= 0.9  # Reduce priority

    def get_next_account(self) -> Optional[str]:
        """Get next account to send from based on priority and capacity"""
        eligible = []

        for account_id, state in self.account_states.items():
            can_send, _ = self.can_send(account_id)
            if can_send:
                # Score = priority * remaining_capacity_percentage
                remaining = state["daily_limit"] - state["sent_today"]
                score = state["priority"] * (remaining / state["daily_limit"])
                eligible.append((account_id, score))

        if not eligible:
            return None

        # Sort by score and add some randomness
        eligible.sort(key=lambda x: x[1] + random.uniform(0, 0.1), reverse=True)
        return eligible[0][0]

    def reset_hourly_counts(self):
        """Reset hourly counts (call on hour change)"""
        self.current_hour_total = 0
        for state in self.account_states.values():
            state["sent_this_hour"] = 0

    def reset_daily_counts(self):
        """Reset daily counts (call at midnight)"""
        for state in self.account_states.values():
            state["sent_today"] = 0

    def get_distribution_plan(self) -> Dict[str, int]:
        """Get planned send distribution across accounts"""
        plan = {}
        total_capacity = sum(s["daily_limit"] - s["sent_today"]
                           for s in self.account_states.values())

        if total_capacity == 0:
            return plan

        for account_id, state in self.account_states.items():
            remaining = state["daily_limit"] - state["sent_today"]
            if remaining > 0:
                # Weight by priority
                weighted_share = (remaining * state["priority"]) / total_capacity
                plan[account_id] = max(1, int(weighted_share * 100))  # Normalize to percentage

        return plan


# ============================================================================
# MAIN OPTIMIZER CLASS
# ============================================================================

class WarmupOptimizer:
    """
    Central optimizer orchestrating all optimization components.

    Features:
    - Time series analysis for pattern detection
    - Provider-specific optimization
    - Dynamic volume ramping
    - Cross-account load balancing
    - Real-time adaptation
    """

    def __init__(self):
        self.time_analyzer = TimeSeriesAnalyzer()
        self.provider_optimizer = ProviderOptimizer()
        self.volume_optimizer = VolumeOptimizer()
        self.load_balancer = CrossAccountLoadBalancer()

        # Historical tracking
        self.hourly_engagement: List[float] = []
        self.time_slots: Dict[Tuple[int, int], TimeSlot] = {}

        logger.info("[WarmupOptimizer] Initialized all optimization components")

    def get_optimization(
        self,
        account_id: str,
        target_email: str,
        current_stats: Dict[str, float],
    ) -> OptimizationResult:
        """
        Get comprehensive optimization recommendation.

        Args:
            account_id: Sender account ID
            target_email: Recipient email address
            current_stats: Current performance metrics

        Returns:
            OptimizationResult with all recommendations
        """
        reasoning = []
        metrics = {}

        # 1. Check if we can send now
        can_send, reason = self.load_balancer.can_send(account_id)
        if not can_send:
            reasoning.append(f"Load balancer: {reason}")

        # 2. Get provider info
        provider = self.provider_optimizer.get_provider_from_email(target_email)
        provider_can_send, provider_reason = self.provider_optimizer.can_send_now(provider)
        if not provider_can_send:
            can_send = False
            reasoning.append(f"Provider ({provider}): {provider_reason}")

        # 3. Check time slot quality
        now = datetime.now(timezone.utc)
        slot_key = (now.hour, now.weekday())
        slot = self.time_slots.get(slot_key, TimeSlot(now.hour, now.weekday()))
        slot_score = slot.score

        if slot_score < 0.4:
            reasoning.append(f"Time slot score is low ({slot_score:.2f})")
            can_send = False

        # 4. Get optimal send time if not sending now
        optimal_time = None
        if not can_send:
            optimal_time, confidence = self.provider_optimizer.get_next_optimal_window(provider)
            reasoning.append(f"Next optimal window: {optimal_time.strftime('%H:%M')}")

        # 5. Volume recommendation
        volume_rec = self.volume_optimizer.get_recommended_volume()
        hourly_dist = self.volume_optimizer.get_hourly_distribution()
        current_hour_target = hourly_dist.get(now.hour, 0)

        # 6. Provider order
        provider_order = self.provider_optimizer.get_optimal_provider_order()

        # 7. Calculate confidence
        confidence = 0.7
        if current_stats.get("spam_rate", 0) > 0.05:
            confidence -= 0.2
            reasoning.append("High spam rate detected - reducing confidence")
        if slot.sample_count > 50:
            confidence += 0.1
            reasoning.append("Good sample size for time slot")

        metrics = {
            "slot_score": slot_score,
            "slot_samples": slot.sample_count,
            "volume_recommendation": volume_rec,
            "current_hour_target": current_hour_target,
            "provider_score": self.provider_optimizer.provider_scores.get(provider, 0.5),
        }

        return OptimizationResult(
            recommended_action="send" if can_send else "wait",
            send_now=can_send,
            optimal_time=optimal_time,
            volume_recommendation=volume_rec,
            provider_order=provider_order,
            confidence=confidence,
            reasoning=reasoning,
            metrics=metrics,
        )

    def record_engagement(
        self,
        account_id: str,
        target_email: str,
        opened: bool,
        replied: bool,
        was_spam: bool,
    ):
        """Record engagement for learning"""
        provider = self.provider_optimizer.get_provider_from_email(target_email)
        now = datetime.now(timezone.utc)
        slot_key = (now.hour, now.weekday())

        # Update provider stats
        self.provider_optimizer.record_send(provider, opened, was_spam)

        # Update load balancer
        self.load_balancer.record_send(account_id, not was_spam)

        # Update time slot
        if slot_key not in self.time_slots:
            self.time_slots[slot_key] = TimeSlot(now.hour, now.weekday())

        slot = self.time_slots[slot_key]
        n = slot.sample_count + 1

        # Running average update
        slot.open_rate = (slot.open_rate * slot.sample_count + (1.0 if opened else 0.0)) / n
        slot.reply_rate = (slot.reply_rate * slot.sample_count + (1.0 if replied else 0.0)) / n
        slot.spam_rate = (slot.spam_rate * slot.sample_count + (1.0 if was_spam else 0.0)) / n
        slot.sample_count = n
        slot.last_updated = now

        # Update hourly engagement for time series
        engagement = 1.0 if opened else 0.0
        self.hourly_engagement.append(engagement)

        # Keep last 30 days of hourly data
        if len(self.hourly_engagement) > 24 * 30:
            self.hourly_engagement = self.hourly_engagement[-24*30:]

    def record_daily_stats(self, stats: Dict[str, float]):
        """Record end-of-day stats for volume adjustment"""
        self.volume_optimizer.record_day_metrics(stats)

    def analyze_patterns(self) -> Dict[str, Any]:
        """Analyze engagement patterns"""
        if len(self.hourly_engagement) < 48:
            return {"status": "insufficient_data", "samples": len(self.hourly_engagement)}

        # Fit time series model
        self.time_analyzer.fit(self.hourly_engagement)

        # Detect trend
        trend = self.time_analyzer.detect_trend(self.hourly_engagement)

        # Detect seasonality
        seasonality = self.time_analyzer.find_seasonality(self.hourly_engagement)

        # Forecast next 24 hours
        forecast = self.time_analyzer.forecast(24)

        # Find best hours from time slots
        best_slots = sorted(
            self.time_slots.items(),
            key=lambda x: x[1].score,
            reverse=True
        )[:5]

        return {
            "status": "analyzed",
            "samples": len(self.hourly_engagement),
            "trend": trend,
            "seasonality": seasonality,
            "forecast_24h": forecast,
            "best_time_slots": [
                {"hour": s.hour, "day": s.day_of_week, "score": s.score}
                for _, s in best_slots
            ],
            "provider_scores": dict(self.provider_optimizer.provider_scores),
        }

    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics"""
        return {
            "time_slots_tracked": len(self.time_slots),
            "hourly_samples": len(self.hourly_engagement),
            "volume_state": {
                "current_limit": self.volume_optimizer.state.current_daily_limit,
                "target_limit": self.volume_optimizer.state.target_daily_limit,
                "consecutive_good_days": self.volume_optimizer.state.consecutive_good_days,
                "consecutive_bad_days": self.volume_optimizer.state.consecutive_bad_days,
                "adjustment_history": self.volume_optimizer.state.adjustment_history[-5:],
            },
            "provider_stats": {
                p: {
                    "score": self.provider_optimizer.provider_scores.get(p, 0.5),
                    "total_sent": profile.total_sent,
                    "total_opened": profile.total_opened,
                    "total_spam": profile.total_spam,
                }
                for p, profile in self.provider_optimizer.profiles.items()
            },
            "load_balancer": {
                "accounts": len(self.load_balancer.account_states),
                "current_hour_total": self.load_balancer.current_hour_total,
            },
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_optimizer_instance: Optional[WarmupOptimizer] = None


def get_warmup_optimizer() -> WarmupOptimizer:
    """Get singleton optimizer instance"""
    global _optimizer_instance
    if _optimizer_instance is None:
        _optimizer_instance = WarmupOptimizer()
    return _optimizer_instance
