"""Rate Limiting Endpoints"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.api.dependencies import get_db
from app.core.auth import get_current_candidate
from app.models import Candidate, RateLimitConfig, RATE_LIMIT_PRESETS
from app.services.rate_limiting_service import RateLimitingService


router = APIRouter(tags=["Rate Limiting"])


# Pydantic schemas
class RateLimitConfigCreate(BaseModel):
    preset: str = "moderate"
    daily_limit: Optional[int] = None
    hourly_limit: Optional[int] = None


class RateLimitConfigUpdate(BaseModel):
    preset: Optional[str] = None
    daily_limit: Optional[int] = None
    hourly_limit: Optional[int] = None
    weekly_limit: Optional[int] = None
    monthly_limit: Optional[int] = None
    enabled: Optional[bool] = None


class RateLimitConfigResponse(BaseModel):
    id: int
    candidate_id: int
    preset: str
    daily_limit: int
    hourly_limit: int
    weekly_limit: Optional[int]
    monthly_limit: Optional[int]
    emails_sent_today: int
    emails_sent_this_hour: int
    enabled: bool

    class Config:
        from_attributes = True


@router.get("/presets")
def get_rate_limit_presets():
    """Get all available rate limit presets"""
    presets = []

    for preset_id, preset_data in RATE_LIMIT_PRESETS.items():
        presets.append({
            "id": preset_id.value,
            "name": preset_id.value.replace("_", " ").title(),
            "daily_limit": preset_data.get("daily_limit"),
            "hourly_limit": preset_data.get("hourly_limit"),
            "description": preset_data.get("description"),
            "recommended_for": preset_data.get("recommended_for")
        })

    # Add custom preset
    presets.append({
        "id": "custom",
        "name": "Custom Limits",
        "daily_limit": None,
        "hourly_limit": None,
        "description": "Set your own custom limits",
        "recommended_for": "Users with specific requirements"
    })

    return {"presets": presets}


@router.post("/config", response_model=RateLimitConfigResponse)
def create_rate_limit_config(
    data: RateLimitConfigCreate,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Create or get rate limit configuration for current user"""
    config = RateLimitingService.create_config(
        db=db,
        candidate_id=current_user.id,
        preset=data.preset,
        daily_limit=data.daily_limit,
        hourly_limit=data.hourly_limit
    )
    return config


@router.get("/config", response_model=RateLimitConfigResponse)
def get_rate_limit_config(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get current rate limit configuration"""
    config = db.query(RateLimitConfig).filter(
        RateLimitConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No rate limit configuration found. Create one first."
        )

    return config


@router.put("/config", response_model=RateLimitConfigResponse)
def update_rate_limit_config(
    data: RateLimitConfigUpdate,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Update rate limit configuration"""
    config = db.query(RateLimitConfig).filter(
        RateLimitConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No rate limit configuration found. Create one first."
        )

    # Update preset
    if data.preset and data.preset != "custom":
        config = RateLimitingService.update_preset(
            db=db,
            config_id=config.id,
            preset=data.preset
        )

    # Update custom limits
    if data.preset == "custom" or any([
        data.daily_limit,
        data.hourly_limit,
        data.weekly_limit,
        data.monthly_limit
    ]):
        config = RateLimitingService.update_custom_limits(
            db=db,
            config_id=config.id,
            daily_limit=data.daily_limit,
            hourly_limit=data.hourly_limit,
            weekly_limit=data.weekly_limit,
            monthly_limit=data.monthly_limit
        )

    # Update enabled status
    if data.enabled is not None:
        config.enabled = data.enabled
        try:
            db.commit()
            db.refresh(config)
            logger.info(f"[RateLimiting] Updated config for candidate {current_user.id}")
        except Exception as e:
            db.rollback()
            logger.error(f"[RateLimiting] Failed to update config: {e}")
            raise HTTPException(status_code=500, detail="Failed to update rate limit config")

    return config


@router.get("/check")
def check_can_send(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Check if user can send an email based on rate limits"""
    can_send, reason, quota_info = RateLimitingService.can_send_email(
        db, current_user.id
    )

    return {
        "can_send": can_send,
        "reason": reason,
        "quota": quota_info
    }


@router.get("/usage")
def get_usage_stats(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get current usage statistics"""
    stats = RateLimitingService.get_usage_stats(db, current_user.id)

    return {
        "success": True,
        "stats": stats
    }


@router.post("/enable")
def enable_rate_limiting(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Enable rate limiting"""
    config = db.query(RateLimitConfig).filter(
        RateLimitConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No rate limit configuration found. Create one first."
        )

    config.enabled = True

    try:
        db.commit()
        logger.info(f"[RateLimiting] Enabled rate limiting for candidate {current_user.id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[RateLimiting] Failed to enable rate limiting: {e}")
        raise HTTPException(status_code=500, detail="Failed to enable rate limiting")

    return {
        "success": True,
        "message": "Rate limiting enabled",
        "daily_limit": config.daily_limit,
        "hourly_limit": config.hourly_limit
    }


@router.post("/disable")
def disable_rate_limiting(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Disable rate limiting"""
    config = db.query(RateLimitConfig).filter(
        RateLimitConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No rate limit configuration found"
        )

    config.enabled = False

    try:
        db.commit()
        logger.info(f"[RateLimiting] Disabled rate limiting for candidate {current_user.id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[RateLimiting] Failed to disable rate limiting: {e}")
        raise HTTPException(status_code=500, detail="Failed to disable rate limiting")

    return {
        "success": True,
        "message": "Rate limiting disabled"
    }


@router.get("/usage-logs")
def get_usage_logs(
    limit: int = 30,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get usage logs history"""
    config = db.query(RateLimitConfig).filter(
        RateLimitConfig.candidate_id == current_user.id
    ).first()

    if not config:
        return {"logs": []}

    logs = config.usage_logs[:limit]

    return {
        "logs": [
            {
                "period_type": log.period_type,
                "period_start": log.period_start.isoformat() if log.period_start else None,
                "period_end": log.period_end.isoformat() if log.period_end else None,
                "limit_value": log.limit_value,
                "emails_sent": log.emails_sent,
                "usage_percentage": log.usage_percentage,
                "limit_reached": log.limit_reached,
                "limit_exceeded": log.limit_exceeded
            }
            for log in logs
        ]
    }


@router.delete("/config")
def delete_rate_limit_config(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Delete rate limit configuration (WARNING: This will delete all usage data)"""
    config = db.query(RateLimitConfig).filter(
        RateLimitConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No rate limit configuration found"
        )

    try:
        db.delete(config)
        db.commit()
        logger.info(f"[RateLimiting] Deleted config for candidate {current_user.id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[RateLimiting] Failed to delete config: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete rate limit config")

    return {
        "success": True,
        "message": "Rate limit configuration deleted"
    }
