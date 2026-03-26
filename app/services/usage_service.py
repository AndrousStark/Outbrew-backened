"""
Usage Service — Centralized usage metering and quota enforcement.

Tracks monthly usage of emails, campaigns, and recipients.
Enforces limits based on plan tier.
Provides usage stats for dashboard display.

Plan limits:
  FREE: 100 emails/mo, 3 campaigns/mo, 100 recipients
  PRO:  unlimited (999999)
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.candidate import Candidate

logger = logging.getLogger(__name__)

# Plan limit defaults
PLAN_LIMITS = {
    "free": {
        "monthly_email_limit": 100,
        "monthly_campaign_limit": 3,
        "monthly_recipient_limit": 100,
    },
    "pro": {
        "monthly_email_limit": 999999,
        "monthly_campaign_limit": 999999,
        "monthly_recipient_limit": 999999,
    },
}


def get_usage_stats(candidate: Candidate) -> Dict[str, Any]:
    """Get current usage stats for a user."""
    plan = str(getattr(candidate, "plan_tier", "free") or "free").lower()
    return {
        "plan_tier": plan,
        "emails": {
            "used": candidate.monthly_email_sent or 0,
            "limit": candidate.monthly_email_limit or PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])["monthly_email_limit"],
            "remaining": max(0, (candidate.monthly_email_limit or 100) - (candidate.monthly_email_sent or 0)),
        },
        "campaigns": {
            "used": candidate.monthly_campaigns_created or 0,
            "limit": candidate.monthly_campaign_limit or PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])["monthly_campaign_limit"],
            "remaining": max(0, (candidate.monthly_campaign_limit or 3) - (candidate.monthly_campaigns_created or 0)),
        },
        "recipients": {
            "limit": candidate.monthly_recipient_limit or PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])["monthly_recipient_limit"],
        },
    }


def check_and_increment_email(db: Session, candidate: Candidate, count: int = 1) -> Tuple[bool, str]:
    """
    Check if user can send `count` emails, and increment the counter.
    Returns (allowed: bool, message: str).
    """
    # Super admin bypasses limits
    if candidate.role and candidate.role.value == "super_admin":
        return True, "ok"

    used = candidate.monthly_email_sent or 0
    limit = candidate.monthly_email_limit or 100

    if used + count > limit:
        remaining = max(0, limit - used)
        return False, f"Monthly email limit reached ({used}/{limit}). {remaining} remaining. Upgrade to Pro for unlimited."

    candidate.monthly_email_sent = used + count
    return True, "ok"


def check_and_increment_campaign(db: Session, candidate: Candidate) -> Tuple[bool, str]:
    """
    Check if user can create a campaign, and increment the counter.
    Returns (allowed: bool, message: str).
    """
    if candidate.role and candidate.role.value == "super_admin":
        return True, "ok"

    used = candidate.monthly_campaigns_created or 0
    limit = candidate.monthly_campaign_limit or 3

    if used >= limit:
        return False, f"Monthly campaign limit reached ({used}/{limit}). Upgrade to Pro for unlimited."

    candidate.monthly_campaigns_created = used + 1
    return True, "ok"


def is_pro_feature(candidate: Candidate) -> bool:
    """Check if user has pro plan."""
    if candidate.role and candidate.role.value == "super_admin":
        return True
    plan = str(getattr(candidate, "plan_tier", "free") or "free").lower()
    return plan == "pro"


def reset_monthly_usage(db: Session) -> Dict[str, int]:
    """
    Reset monthly usage counters for all users whose reset date has passed.
    Called by the scheduler monthly.
    """
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)

    users = db.query(Candidate).filter(
        Candidate.deleted_at.is_(None),
        (Candidate.usage_reset_at.is_(None)) | (Candidate.usage_reset_at <= thirty_days_ago),
    ).all()

    reset_count = 0
    for user in users:
        user.monthly_email_sent = 0
        user.monthly_campaigns_created = 0
        user.usage_reset_at = now
        reset_count += 1

    if reset_count > 0:
        db.commit()
        logger.info(f"[UsageService] Reset monthly usage for {reset_count} users")

    return {"reset_count": reset_count}
