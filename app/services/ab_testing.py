"""
A/B Testing Service

Provides statistical analysis for A/B tests on email templates.
"""

import logging
import math
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ABTestingService:
    """Service for managing and analyzing A/B tests."""

    @staticmethod
    def calculate_statistical_significance(
        db: Session,
        test_id: int
    ) -> Dict[str, Any]:
        """
        Calculate statistical significance for an A/B test.

        Uses a two-proportion z-test to determine if there is a
        statistically significant difference between variants.

        Args:
            db: Database session
            test_id: ID of the A/B test

        Returns:
            Dict with p_value, z_score, is_significant, and winner info
        """
        from app.models.follow_up import ABTest, ABTestVariant

        test = db.query(ABTest).filter(ABTest.id == test_id).first()
        if not test:
            return {"error": "Test not found", "is_significant": False}

        variants = db.query(ABTestVariant).filter(
            ABTestVariant.test_id == test_id
        ).all()

        if len(variants) < 2:
            return {"error": "Need at least 2 variants", "is_significant": False}

        # Get the two main variants (control and treatment)
        control = variants[0]
        treatment = variants[1]

        n1 = control.total_sent or 0
        n2 = treatment.total_sent or 0

        if n1 == 0 or n2 == 0:
            return {
                "is_significant": False,
                "p_value": 1.0,
                "z_score": 0.0,
                "reason": "Insufficient data"
            }

        # Calculate reply rates
        p1 = (control.total_replies or 0) / n1
        p2 = (treatment.total_replies or 0) / n2

        # Pooled proportion
        p_pool = ((control.total_replies or 0) + (treatment.total_replies or 0)) / (n1 + n2)

        if p_pool == 0 or p_pool == 1:
            return {
                "is_significant": False,
                "p_value": 1.0,
                "z_score": 0.0,
                "reason": "No variance in data"
            }

        # Standard error
        se = math.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))

        if se == 0:
            return {
                "is_significant": False,
                "p_value": 1.0,
                "z_score": 0.0,
                "reason": "Zero standard error"
            }

        # Z-score
        z_score = (p2 - p1) / se

        # Approximate p-value using normal distribution
        p_value = 2 * (1 - _normal_cdf(abs(z_score)))

        is_significant = p_value < 0.05

        winner = None
        if is_significant:
            winner = "treatment" if p2 > p1 else "control"

        return {
            "is_significant": is_significant,
            "p_value": p_value,
            "z_score": z_score,
            "control_rate": p1,
            "treatment_rate": p2,
            "winner": winner,
            "confidence_level": 1 - p_value if is_significant else 0
        }


def _normal_cdf(x: float) -> float:
    """Approximate the cumulative distribution function of the standard normal."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))
