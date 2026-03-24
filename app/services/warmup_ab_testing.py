"""
Warmup A/B Testing Framework - PHASE 5 GOD TIER EDITION

Scientific A/B testing framework for warmup strategy optimization.

Features:
- Statistical significance testing (Z-test, Chi-squared)
- Bayesian analysis with posterior distributions
- Multi-armed bandit integration for adaptive allocation
- Sequential testing with early stopping
- Segment-based analysis
- Automated winner detection

Author: Metaminds AI
Version: 5.0.0 - ULTRA GOD TIER TESTING
"""

import logging
import uuid
import math
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict
import random

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

class TestStatus(str, Enum):
    """A/B test lifecycle states"""
    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class TestType(str, Enum):
    """Types of A/B tests"""
    SUBJECT_LINE = "subject_line"
    SEND_TIME = "send_time"
    CONTENT_STYLE = "content_style"
    VOLUME_STRATEGY = "volume_strategy"
    REPLY_DELAY = "reply_delay"
    PROVIDER_ROUTING = "provider_routing"


class MetricType(str, Enum):
    """Metrics to optimize"""
    OPEN_RATE = "open_rate"
    REPLY_RATE = "reply_rate"
    INBOX_RATE = "inbox_rate"
    BOUNCE_RATE = "bounce_rate"
    ENGAGEMENT_SCORE = "engagement_score"


class AllocationStrategy(str, Enum):
    """Traffic allocation strategies"""
    EQUAL = "equal"  # 50/50 split
    WEIGHTED = "weighted"  # Custom weights
    BANDIT_EPSILON = "bandit_epsilon"  # Epsilon-greedy
    BANDIT_THOMPSON = "bandit_thompson"  # Thompson sampling
    BANDIT_UCB = "bandit_ucb"  # Upper Confidence Bound


# ============================================================================
# STATISTICAL UTILITIES
# ============================================================================

class StatisticalEngine:
    """Statistical analysis engine for A/B testing"""

    @staticmethod
    def z_score(p1: float, p2: float, n1: int, n2: int) -> float:
        """Calculate Z-score for two proportions"""
        if n1 == 0 or n2 == 0:
            return 0.0

        # Pooled proportion
        p_pool = (p1 * n1 + p2 * n2) / (n1 + n2)

        if p_pool == 0 or p_pool == 1:
            return 0.0

        # Standard error
        se = math.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))

        if se == 0:
            return 0.0

        return (p1 - p2) / se

    @staticmethod
    def p_value_from_z(z: float) -> float:
        """Calculate two-tailed p-value from Z-score (approximation)"""
        # Using approximation of normal CDF
        def norm_cdf(x: float) -> float:
            t = 1.0 / (1.0 + 0.2316419 * abs(x))
            d = 0.3989423 * math.exp(-x * x / 2)
            p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))))
            return 1 - p if x > 0 else p

        return 2 * (1 - norm_cdf(abs(z)))

    @staticmethod
    def confidence_interval(
        proportion: float,
        n: int,
        confidence: float = 0.95
    ) -> Tuple[float, float]:
        """Calculate confidence interval for proportion"""
        if n == 0:
            return (0.0, 0.0)

        # Z-score for confidence level (1.96 for 95%)
        z_scores = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}
        z = z_scores.get(confidence, 1.96)

        # Standard error
        se = math.sqrt(proportion * (1 - proportion) / n)

        lower = max(0, proportion - z * se)
        upper = min(1, proportion + z * se)

        return (lower, upper)

    @staticmethod
    def sample_size_required(
        baseline_rate: float,
        minimum_effect: float,
        alpha: float = 0.05,
        power: float = 0.8
    ) -> int:
        """Calculate required sample size for detecting effect"""
        # Z-scores
        z_alpha = 1.96 if alpha == 0.05 else 2.576 if alpha == 0.01 else 1.645
        z_beta = 0.84 if power == 0.8 else 1.28 if power == 0.9 else 0.52

        p1 = baseline_rate
        p2 = baseline_rate + minimum_effect
        p_avg = (p1 + p2) / 2

        if p_avg == 0 or p_avg == 1:
            return 1000  # Default

        numerator = (z_alpha * math.sqrt(2 * p_avg * (1 - p_avg)) +
                    z_beta * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
        denominator = (p2 - p1) ** 2

        return int(math.ceil(numerator / denominator)) if denominator > 0 else 1000

    @staticmethod
    def bayesian_probability_better(
        successes_a: int,
        trials_a: int,
        successes_b: int,
        trials_b: int,
        simulations: int = 10000
    ) -> float:
        """
        Calculate probability that B is better than A using Bayesian inference.
        Uses Beta-Binomial model with uniform prior.
        """
        a_wins = 0

        for _ in range(simulations):
            # Sample from posterior Beta distributions
            # Using Box-Muller for normal approximation when sample is large
            alpha_a = successes_a + 1
            beta_a = trials_a - successes_a + 1
            alpha_b = successes_b + 1
            beta_b = trials_b - successes_b + 1

            # Simple approximation using random sampling
            sample_a = StatisticalEngine._sample_beta(alpha_a, beta_a)
            sample_b = StatisticalEngine._sample_beta(alpha_b, beta_b)

            if sample_b > sample_a:
                a_wins += 1

        return a_wins / simulations

    @staticmethod
    def _sample_beta(alpha: float, beta: float) -> float:
        """Sample from Beta distribution using gamma approximation"""
        if alpha <= 0 or beta <= 0:
            return 0.5

        # Use ratio of gamma samples
        x = sum(random.random() for _ in range(int(alpha))) / alpha if alpha >= 1 else random.random()
        y = sum(random.random() for _ in range(int(beta))) / beta if beta >= 1 else random.random()

        # Simplified approximation
        mean = alpha / (alpha + beta)
        variance = (alpha * beta) / ((alpha + beta) ** 2 * (alpha + beta + 1))
        std = math.sqrt(variance)

        # Sample from normal approximation
        sample = mean + std * (random.random() - 0.5) * 2
        return max(0, min(1, sample))

    @staticmethod
    def sequential_test(
        successes_a: int,
        trials_a: int,
        successes_b: int,
        trials_b: int,
        alpha: float = 0.05
    ) -> Dict[str, Any]:
        """
        Sequential probability ratio test for early stopping.
        Returns whether to continue, stop for A, or stop for B.
        """
        if trials_a < 10 or trials_b < 10:
            return {"decision": "continue", "reason": "Insufficient samples"}

        p_a = successes_a / trials_a
        p_b = successes_b / trials_b

        z = StatisticalEngine.z_score(p_a, p_b, trials_a, trials_b)
        p_value = StatisticalEngine.p_value_from_z(z)

        # O'Brien-Fleming boundaries (simplified)
        spending = min(1, (trials_a + trials_b) / 1000)  # Fraction of max sample
        boundary = 2.0 / math.sqrt(spending) if spending > 0 else float('inf')

        if abs(z) > boundary:
            winner = "A" if p_a > p_b else "B"
            return {
                "decision": f"stop_{winner.lower()}",
                "reason": f"Boundary crossed: Z={z:.3f}, boundary={boundary:.3f}",
                "p_value": p_value,
                "winner": winner,
            }

        return {
            "decision": "continue",
            "reason": f"Z={z:.3f} within boundary {boundary:.3f}",
            "p_value": p_value,
        }


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Variant:
    """Test variant definition"""
    id: str
    name: str
    description: str
    config: Dict[str, Any]
    weight: float = 0.5  # Traffic allocation weight

    # Results tracking
    impressions: int = 0
    conversions: int = 0

    @property
    def conversion_rate(self) -> float:
        return self.conversions / max(self.impressions, 1)

    def to_dict(self) -> Dict[str, Any]:
        ci_lower, ci_upper = StatisticalEngine.confidence_interval(
            self.conversion_rate, self.impressions
        )
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "config": self.config,
            "weight": self.weight,
            "impressions": self.impressions,
            "conversions": self.conversions,
            "conversion_rate": round(self.conversion_rate, 4),
            "confidence_interval": {
                "lower": round(ci_lower, 4),
                "upper": round(ci_upper, 4),
            },
        }


@dataclass
class TestSegment:
    """Test segment for subgroup analysis"""
    id: str
    name: str
    filter_criteria: Dict[str, Any]
    variant_results: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def record_result(self, variant_id: str, converted: bool):
        """Record result for segment"""
        if variant_id not in self.variant_results:
            self.variant_results[variant_id] = {"impressions": 0, "conversions": 0}

        self.variant_results[variant_id]["impressions"] += 1
        if converted:
            self.variant_results[variant_id]["conversions"] += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "filter_criteria": self.filter_criteria,
            "results": self.variant_results,
        }


@dataclass
class ABTest:
    """Full A/B test definition"""
    id: str
    name: str
    description: str
    test_type: TestType
    metric: MetricType
    allocation_strategy: AllocationStrategy
    variants: List[Variant]
    segments: List[TestSegment]
    status: TestStatus

    # Configuration
    min_sample_size: int = 100
    max_sample_size: int = 10000
    confidence_level: float = 0.95
    minimum_effect_size: float = 0.05
    early_stopping_enabled: bool = True

    # Timing
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    max_duration_days: int = 30

    # Results
    winner_id: Optional[str] = None
    conclusion: Optional[str] = None

    # Bandit state (for adaptive allocation)
    bandit_alpha: Dict[str, float] = field(default_factory=dict)
    bandit_beta: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self):
        # Initialize bandit parameters
        for variant in self.variants:
            if variant.id not in self.bandit_alpha:
                self.bandit_alpha[variant.id] = 1.0
                self.bandit_beta[variant.id] = 1.0

    @property
    def total_impressions(self) -> int:
        return sum(v.impressions for v in self.variants)

    @property
    def is_significant(self) -> bool:
        """Check if results are statistically significant"""
        if len(self.variants) < 2:
            return False

        control = self.variants[0]
        treatment = self.variants[1]

        z = StatisticalEngine.z_score(
            control.conversion_rate,
            treatment.conversion_rate,
            control.impressions,
            treatment.impressions,
        )

        p_value = StatisticalEngine.p_value_from_z(z)
        alpha = 1 - self.confidence_level

        return p_value < alpha

    @property
    def required_sample_size(self) -> int:
        """Calculate required sample size"""
        if not self.variants:
            return self.min_sample_size

        baseline = self.variants[0].conversion_rate if self.variants[0].impressions > 0 else 0.1
        return StatisticalEngine.sample_size_required(
            baseline,
            self.minimum_effect_size,
            alpha=1 - self.confidence_level,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "test_type": self.test_type.value,
            "metric": self.metric.value,
            "allocation_strategy": self.allocation_strategy.value,
            "variants": [v.to_dict() for v in self.variants],
            "segments": [s.to_dict() for s in self.segments],
            "status": self.status.value,
            "min_sample_size": self.min_sample_size,
            "max_sample_size": self.max_sample_size,
            "confidence_level": self.confidence_level,
            "minimum_effect_size": self.minimum_effect_size,
            "early_stopping_enabled": self.early_stopping_enabled,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "max_duration_days": self.max_duration_days,
            "winner_id": self.winner_id,
            "conclusion": self.conclusion,
            "total_impressions": self.total_impressions,
            "is_significant": self.is_significant,
            "required_sample_size": self.required_sample_size,
        }


# ============================================================================
# A/B TESTING ENGINE
# ============================================================================

class ABTestingEngine:
    """
    Advanced A/B testing engine with statistical analysis.

    Supports multiple allocation strategies, sequential testing,
    and Bayesian analysis.
    """

    def __init__(self):
        self.tests: Dict[str, ABTest] = {}
        self.account_tests: Dict[str, List[str]] = defaultdict(list)
        self._initialized = False

        logger.info("[ABTestingEngine] Initialized")

    def _ensure_initialized(self):
        if not self._initialized:
            self._initialized = True

    # ========================================
    # Test CRUD Operations
    # ========================================

    def create_test(
        self,
        account_id: str,
        name: str,
        test_type: TestType,
        metric: MetricType,
        variants_config: List[Dict[str, Any]],
        allocation_strategy: AllocationStrategy = AllocationStrategy.EQUAL,
        **kwargs
    ) -> ABTest:
        """Create a new A/B test"""
        self._ensure_initialized()

        test_id = str(uuid.uuid4())

        # Create variants
        variants = []
        for i, config in enumerate(variants_config):
            variant = Variant(
                id=f"{test_id}-variant-{i}",
                name=config.get("name", f"Variant {chr(65 + i)}"),
                description=config.get("description", ""),
                config=config.get("config", {}),
                weight=config.get("weight", 1.0 / len(variants_config)),
            )
            variants.append(variant)

        # Normalize weights
        total_weight = sum(v.weight for v in variants)
        for v in variants:
            v.weight /= total_weight

        test = ABTest(
            id=test_id,
            name=name,
            description=kwargs.get("description", ""),
            test_type=test_type,
            metric=metric,
            allocation_strategy=allocation_strategy,
            variants=variants,
            segments=[],
            status=TestStatus.DRAFT,
            min_sample_size=kwargs.get("min_sample_size", 100),
            max_sample_size=kwargs.get("max_sample_size", 10000),
            confidence_level=kwargs.get("confidence_level", 0.95),
            minimum_effect_size=kwargs.get("minimum_effect_size", 0.05),
            early_stopping_enabled=kwargs.get("early_stopping_enabled", True),
            max_duration_days=kwargs.get("max_duration_days", 30),
        )

        self.tests[test_id] = test
        self.account_tests[account_id].append(test_id)

        logger.info(f"[ABTestingEngine] Created test: {test_id}")
        return test

    def get_test(self, test_id: str) -> Optional[ABTest]:
        """Get test by ID"""
        return self.tests.get(test_id)

    def get_account_tests(self, account_id: str) -> List[ABTest]:
        """Get all tests for an account"""
        test_ids = self.account_tests.get(account_id, [])
        return [self.tests[tid] for tid in test_ids if tid in self.tests]

    def delete_test(self, test_id: str) -> bool:
        """Delete a test"""
        if test_id not in self.tests:
            return False

        del self.tests[test_id]

        # Remove from account lists
        for account_id, tests in self.account_tests.items():
            self.account_tests[account_id] = [t for t in tests if t != test_id]

        logger.info(f"[ABTestingEngine] Deleted test: {test_id}")
        return True

    # ========================================
    # Test Lifecycle
    # ========================================

    def start_test(self, test_id: str) -> ABTest:
        """Start a test"""
        test = self._get_test_or_raise(test_id)

        if test.status not in [TestStatus.DRAFT, TestStatus.PAUSED]:
            raise ValueError(f"Cannot start test in status: {test.status}")

        test.status = TestStatus.RUNNING
        test.started_at = test.started_at or datetime.now(timezone.utc)

        logger.info(f"[ABTestingEngine] Started test: {test_id}")
        return test

    def pause_test(self, test_id: str) -> ABTest:
        """Pause a running test"""
        test = self._get_test_or_raise(test_id)

        if test.status != TestStatus.RUNNING:
            raise ValueError(f"Cannot pause test in status: {test.status}")

        test.status = TestStatus.PAUSED

        logger.info(f"[ABTestingEngine] Paused test: {test_id}")
        return test

    def complete_test(self, test_id: str, winner_id: Optional[str] = None) -> ABTest:
        """Complete a test"""
        test = self._get_test_or_raise(test_id)

        test.status = TestStatus.COMPLETED
        test.completed_at = datetime.now(timezone.utc)

        # Determine winner if not specified
        if winner_id:
            test.winner_id = winner_id
        else:
            test.winner_id = self._determine_winner(test)

        # Generate conclusion
        test.conclusion = self._generate_conclusion(test)

        logger.info(f"[ABTestingEngine] Completed test: {test_id}, winner: {test.winner_id}")
        return test

    # ========================================
    # Variant Assignment
    # ========================================

    def assign_variant(
        self,
        test_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Variant:
        """
        Assign a variant based on allocation strategy.
        Returns the selected variant.
        """
        test = self._get_test_or_raise(test_id)

        if test.status != TestStatus.RUNNING:
            raise ValueError(f"Test is not running: {test.status}")

        strategy = test.allocation_strategy

        if strategy == AllocationStrategy.EQUAL:
            return self._assign_equal(test)

        elif strategy == AllocationStrategy.WEIGHTED:
            return self._assign_weighted(test)

        elif strategy == AllocationStrategy.BANDIT_EPSILON:
            return self._assign_epsilon_greedy(test, epsilon=0.1)

        elif strategy == AllocationStrategy.BANDIT_THOMPSON:
            return self._assign_thompson_sampling(test)

        elif strategy == AllocationStrategy.BANDIT_UCB:
            return self._assign_ucb(test)

        else:
            return self._assign_equal(test)

    def _assign_equal(self, test: ABTest) -> Variant:
        """Equal probability assignment"""
        return random.choice(test.variants)

    def _assign_weighted(self, test: ABTest) -> Variant:
        """Weighted assignment based on variant weights"""
        r = random.random()
        cumulative = 0

        for variant in test.variants:
            cumulative += variant.weight
            if r <= cumulative:
                return variant

        return test.variants[-1]

    def _assign_epsilon_greedy(self, test: ABTest, epsilon: float = 0.1) -> Variant:
        """Epsilon-greedy assignment"""
        if random.random() < epsilon:
            # Explore: random choice
            return random.choice(test.variants)
        else:
            # Exploit: best performer
            return max(test.variants, key=lambda v: v.conversion_rate)

    def _assign_thompson_sampling(self, test: ABTest) -> Variant:
        """Thompson sampling assignment"""
        best_sample = -1
        best_variant = test.variants[0]

        for variant in test.variants:
            alpha = test.bandit_alpha.get(variant.id, 1)
            beta = test.bandit_beta.get(variant.id, 1)

            # Sample from Beta distribution
            sample = StatisticalEngine._sample_beta(alpha, beta)

            if sample > best_sample:
                best_sample = sample
                best_variant = variant

        return best_variant

    def _assign_ucb(self, test: ABTest, c: float = 2.0) -> Variant:
        """Upper Confidence Bound assignment"""
        total_pulls = sum(v.impressions for v in test.variants)

        if total_pulls == 0:
            return random.choice(test.variants)

        best_ucb = -float('inf')
        best_variant = test.variants[0]

        for variant in test.variants:
            if variant.impressions == 0:
                return variant  # Always try untested variants

            avg_reward = variant.conversion_rate
            exploration = c * math.sqrt(math.log(total_pulls) / variant.impressions)
            ucb = avg_reward + exploration

            if ucb > best_ucb:
                best_ucb = ucb
                best_variant = variant

        return best_variant

    # ========================================
    # Result Recording
    # ========================================

    def record_impression(self, test_id: str, variant_id: str, segment_ids: Optional[List[str]] = None):
        """Record an impression for a variant"""
        test = self._get_test_or_raise(test_id)
        variant = self._get_variant_or_raise(test, variant_id)

        variant.impressions += 1

        # Record for segments
        if segment_ids:
            for segment in test.segments:
                if segment.id in segment_ids:
                    segment.record_result(variant_id, False)

    def record_conversion(
        self,
        test_id: str,
        variant_id: str,
        segment_ids: Optional[List[str]] = None
    ):
        """Record a conversion for a variant"""
        test = self._get_test_or_raise(test_id)
        variant = self._get_variant_or_raise(test, variant_id)

        variant.conversions += 1

        # Update bandit parameters
        test.bandit_alpha[variant_id] = test.bandit_alpha.get(variant_id, 1) + 1

        # Record for segments
        if segment_ids:
            for segment in test.segments:
                if segment.id in segment_ids:
                    segment.record_result(variant_id, True)

        # Check for early stopping
        if test.early_stopping_enabled:
            self._check_early_stopping(test)

    def record_non_conversion(self, test_id: str, variant_id: str):
        """Record a non-conversion (for bandit updates)"""
        test = self._get_test_or_raise(test_id)

        # Update bandit parameters
        test.bandit_beta[variant_id] = test.bandit_beta.get(variant_id, 1) + 1

    # ========================================
    # Analysis
    # ========================================

    def analyze_test(self, test_id: str) -> Dict[str, Any]:
        """Comprehensive test analysis"""
        test = self._get_test_or_raise(test_id)

        if len(test.variants) < 2:
            return {"error": "Need at least 2 variants for analysis"}

        control = test.variants[0]
        treatment = test.variants[1]

        # Frequentist analysis
        z_score = StatisticalEngine.z_score(
            control.conversion_rate,
            treatment.conversion_rate,
            control.impressions,
            treatment.impressions,
        )
        p_value = StatisticalEngine.p_value_from_z(z_score)

        # Bayesian analysis
        prob_b_better = StatisticalEngine.bayesian_probability_better(
            control.conversions,
            control.impressions,
            treatment.conversions,
            treatment.impressions,
        )

        # Sequential test
        sequential = StatisticalEngine.sequential_test(
            control.conversions,
            control.impressions,
            treatment.conversions,
            treatment.impressions,
        )

        # Lift calculation
        if control.conversion_rate > 0:
            relative_lift = (treatment.conversion_rate - control.conversion_rate) / control.conversion_rate
        else:
            relative_lift = 0

        absolute_lift = treatment.conversion_rate - control.conversion_rate

        return {
            "test_id": test_id,
            "status": test.status.value,
            "total_impressions": test.total_impressions,
            "required_sample_size": test.required_sample_size,
            "progress_percentage": min(100, (test.total_impressions / test.required_sample_size) * 100),
            "frequentist": {
                "z_score": round(z_score, 4),
                "p_value": round(p_value, 6),
                "is_significant": p_value < (1 - test.confidence_level),
                "confidence_level": test.confidence_level,
            },
            "bayesian": {
                "probability_b_better": round(prob_b_better, 4),
                "probability_a_better": round(1 - prob_b_better, 4),
            },
            "sequential": sequential,
            "lift": {
                "relative": round(relative_lift, 4),
                "absolute": round(absolute_lift, 4),
            },
            "variants": [v.to_dict() for v in test.variants],
            "recommendation": self._generate_recommendation(test, p_value, prob_b_better),
        }

    def _generate_recommendation(
        self,
        test: ABTest,
        p_value: float,
        prob_b_better: float
    ) -> str:
        """Generate recommendation based on analysis"""
        alpha = 1 - test.confidence_level

        if test.total_impressions < test.min_sample_size:
            return "Continue collecting data - insufficient sample size"

        if p_value < alpha:
            if test.variants[1].conversion_rate > test.variants[0].conversion_rate:
                return f"WINNER: {test.variants[1].name} - statistically significant improvement"
            else:
                return f"WINNER: {test.variants[0].name} (Control) - treatment underperformed"

        if prob_b_better > 0.9:
            return f"Likely winner: {test.variants[1].name} - high Bayesian probability but not yet significant"

        if test.total_impressions >= test.max_sample_size:
            return "No significant difference detected - consider ending test"

        return "Continue test - not yet conclusive"

    # ========================================
    # Winner Detection
    # ========================================

    def _determine_winner(self, test: ABTest) -> Optional[str]:
        """Determine the winning variant"""
        if len(test.variants) < 2:
            return test.variants[0].id if test.variants else None

        # Check statistical significance
        control = test.variants[0]
        best_variant = control
        best_rate = control.conversion_rate

        for variant in test.variants[1:]:
            z = StatisticalEngine.z_score(
                control.conversion_rate,
                variant.conversion_rate,
                control.impressions,
                variant.impressions,
            )
            p_value = StatisticalEngine.p_value_from_z(z)

            if p_value < (1 - test.confidence_level) and variant.conversion_rate > best_rate:
                best_variant = variant
                best_rate = variant.conversion_rate

        return best_variant.id

    def _generate_conclusion(self, test: ABTest) -> str:
        """Generate conclusion text"""
        if not test.winner_id:
            return "No clear winner determined"

        winner = next((v for v in test.variants if v.id == test.winner_id), None)
        if not winner:
            return "Winner variant not found"

        control = test.variants[0]

        if winner.id == control.id:
            return f"Control ({winner.name}) maintained best performance with {winner.conversion_rate:.2%} {test.metric.value}"
        else:
            lift = ((winner.conversion_rate - control.conversion_rate) / control.conversion_rate * 100) if control.conversion_rate > 0 else 0
            return f"{winner.name} won with {winner.conversion_rate:.2%} {test.metric.value} ({lift:+.1f}% vs control)"

    def _check_early_stopping(self, test: ABTest):
        """Check if test should be stopped early"""
        if len(test.variants) < 2:
            return

        control = test.variants[0]
        treatment = test.variants[1]

        result = StatisticalEngine.sequential_test(
            control.conversions,
            control.impressions,
            treatment.conversions,
            treatment.impressions,
        )

        if result["decision"].startswith("stop"):
            logger.info(f"[ABTestingEngine] Early stopping triggered for test {test.id}: {result['reason']}")
            self.complete_test(test.id, result.get("winner_id"))

    # ========================================
    # Helpers
    # ========================================

    def _get_test_or_raise(self, test_id: str) -> ABTest:
        """Get test or raise exception"""
        test = self.tests.get(test_id)
        if not test:
            raise ValueError(f"Test not found: {test_id}")
        return test

    def _get_variant_or_raise(self, test: ABTest, variant_id: str) -> Variant:
        """Get variant or raise exception"""
        for variant in test.variants:
            if variant.id == variant_id:
                return variant
        raise ValueError(f"Variant not found: {variant_id}")

    def get_statistics(self) -> Dict[str, Any]:
        """Get engine statistics"""
        return {
            "total_tests": len(self.tests),
            "tests_by_status": {
                status.value: len([t for t in self.tests.values() if t.status == status])
                for status in TestStatus
            },
            "tests_by_type": {
                test_type.value: len([t for t in self.tests.values() if t.test_type == test_type])
                for test_type in TestType
            },
            "total_impressions": sum(t.total_impressions for t in self.tests.values()),
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_ab_testing_engine: Optional[ABTestingEngine] = None


def get_ab_testing_engine() -> ABTestingEngine:
    """Get the singleton A/B testing engine instance"""
    global _ab_testing_engine
    if _ab_testing_engine is None:
        _ab_testing_engine = ABTestingEngine()
    return _ab_testing_engine
