"""
Usage & Plan Endpoints — Current usage stats, plan info, upgrade check.

These are lightweight endpoints the frontend polls to show usage bars.
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_database_session
from app.api.dependencies import get_current_candidate
from app.models.candidate import Candidate
from app.services.usage_service import get_usage_stats, PLAN_LIMITS

router = APIRouter()


@router.get("/")
def get_current_usage(
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """Get current user's usage stats and plan limits."""
    stats = get_usage_stats(current_candidate)

    # Add percentage calculations for UI progress bars
    for key in ["emails", "campaigns"]:
        item = stats[key]
        if item["limit"] > 0 and item["limit"] < 999999:
            item["percent"] = round(item["used"] / item["limit"] * 100, 1)
        else:
            item["percent"] = 0

    return stats


@router.get("/limits")
def get_plan_limits(
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """Get the limits for all plan tiers (for comparison/upgrade UI)."""
    return {
        "current_plan": str(getattr(current_candidate, "plan_tier", "free") or "free").lower(),
        "plans": PLAN_LIMITS,
    }
