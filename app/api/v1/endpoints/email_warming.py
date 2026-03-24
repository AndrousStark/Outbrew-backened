"""Email Warming Endpoints"""

import logging
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.api.dependencies import get_db
from app.core.auth import get_current_candidate
from app.models import Candidate, EmailWarmingConfig, WarmingStrategyEnum, WARMING_SCHEDULES
from app.services.email_warming_service import EmailWarmingService


router = APIRouter(tags=["Email Warming"])


# Pydantic schemas
class WarmingConfigCreate(BaseModel):
    strategy: str = "moderate"
    custom_schedule: Optional[Dict[int, int]] = None
    auto_progress: bool = True


class WarmingConfigUpdate(BaseModel):
    strategy: Optional[str] = None
    custom_schedule: Optional[Dict[int, int]] = None
    auto_progress: Optional[bool] = None


class WarmingConfigResponse(BaseModel):
    id: int
    candidate_id: int
    strategy: str
    status: str
    current_day: int
    emails_sent_today: int
    total_emails_sent: int
    success_rate: float
    bounce_rate: float
    auto_progress: bool

    class Config:
        from_attributes = True


@router.get("/presets")
def get_warming_presets():
    """Get all available warming strategy presets with schedules"""
    return {
        "presets": [
            {
                "id": "conservative",
                "name": "Conservative (14 days)",
                "description": "Slow and steady - safest for new accounts",
                "duration_days": 14,
                "final_limit": 50,
                "recommended_for": "Brand new email accounts",
                "schedule": WARMING_SCHEDULES[WarmingStrategyEnum.CONSERVATIVE]
            },
            {
                "id": "moderate",
                "name": "Moderate (14 days)",
                "description": "Balanced approach - good for most users",
                "duration_days": 14,
                "final_limit": 100,
                "recommended_for": "Accounts 1-2 weeks old",
                "schedule": WARMING_SCHEDULES[WarmingStrategyEnum.MODERATE]
            },
            {
                "id": "aggressive",
                "name": "Aggressive (12 days)",
                "description": "Fast warm-up - for established domains",
                "duration_days": 12,
                "final_limit": 150,
                "recommended_for": "Established email accounts",
                "schedule": WARMING_SCHEDULES[WarmingStrategyEnum.AGGRESSIVE]
            },
            {
                "id": "custom",
                "name": "Custom Schedule",
                "description": "Define your own warming schedule",
                "duration_days": None,
                "final_limit": None,
                "recommended_for": "Advanced users with specific needs",
                "schedule": {}
            }
        ]
    }


@router.post("/config", response_model=WarmingConfigResponse)
def create_warming_config(
    data: WarmingConfigCreate,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Create or get warming configuration for current user"""
    config = EmailWarmingService.create_config(
        db=db,
        candidate_id=current_user.id,
        strategy=data.strategy,
        custom_schedule=data.custom_schedule,
        auto_progress=data.auto_progress
    )
    return config


@router.get("/config", response_model=WarmingConfigResponse)
def get_warming_config(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get current warming configuration"""
    config = db.query(EmailWarmingConfig).filter(
        EmailWarmingConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No warming configuration found. Create one first."
        )

    return config


@router.put("/config", response_model=WarmingConfigResponse)
def update_warming_config(
    data: WarmingConfigUpdate,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Update warming configuration"""
    config = db.query(EmailWarmingConfig).filter(
        EmailWarmingConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No warming configuration found. Create one first."
        )

    if data.strategy:
        config = EmailWarmingService.update_strategy(
            db=db,
            config_id=config.id,
            strategy=data.strategy,
            custom_schedule=data.custom_schedule
        )

    if data.auto_progress is not None:
        config.auto_progress = data.auto_progress
        try:
            db.commit()
            db.refresh(config)
            logger.info(f"[EmailWarming] Updated config for candidate {current_user.id}")
        except Exception as e:
            db.rollback()
            logger.error(f"[EmailWarming] Failed to update config: {e}")
            raise HTTPException(status_code=500, detail="Failed to update warming config")

    return config


@router.post("/start")
def start_warming(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Start the warming campaign"""
    config = db.query(EmailWarmingConfig).filter(
        EmailWarmingConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No warming configuration found. Create one first."
        )

    config = EmailWarmingService.start_warming(db, config.id)

    return {
        "success": True,
        "message": "Warming campaign started",
        "current_day": config.current_day,
        "daily_limit": EmailWarmingService.get_daily_limit(config)
    }


@router.post("/pause")
def pause_warming(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Pause the warming campaign"""
    config = db.query(EmailWarmingConfig).filter(
        EmailWarmingConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No warming configuration found"
        )

    config = EmailWarmingService.pause_warming(db, config.id)

    return {
        "success": True,
        "message": "Warming campaign paused",
        "status": config.status
    }


@router.post("/resume")
def resume_warming(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Resume the warming campaign"""
    config = db.query(EmailWarmingConfig).filter(
        EmailWarmingConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No warming configuration found"
        )

    config = EmailWarmingService.resume_warming(db, config.id)

    return {
        "success": True,
        "message": "Warming campaign resumed",
        "status": config.status
    }


@router.get("/progress")
def get_warming_progress(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get detailed warming progress"""
    progress = EmailWarmingService.get_warming_progress(db, current_user.id)

    return {
        "success": True,
        "progress": progress
    }


@router.get("/daily-logs")
def get_daily_logs(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get daily warming logs"""
    config = db.query(EmailWarmingConfig).filter(
        EmailWarmingConfig.candidate_id == current_user.id
    ).first()

    if not config:
        return {"logs": []}

    logs = config.daily_logs

    return {
        "logs": [
            {
                "day_number": log.day_number,
                "date": log.date.isoformat() if log.date else None,
                "daily_limit": log.daily_limit,
                "emails_sent": log.emails_sent,
                "emails_delivered": log.emails_delivered,
                "emails_bounced": log.emails_bounced,
                "emails_failed": log.emails_failed,
                "delivery_rate": round(log.delivery_rate, 2),
                "bounce_rate": round(log.bounce_rate, 2),
                "limit_reached": log.limit_reached,
                "notes": log.notes
            }
            for log in logs
        ]
    }


@router.delete("/config")
def delete_warming_config(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Delete warming configuration (WARNING: This will delete all warming data)"""
    config = db.query(EmailWarmingConfig).filter(
        EmailWarmingConfig.candidate_id == current_user.id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No warming configuration found"
        )

    try:
        db.delete(config)
        db.commit()
        logger.info(f"[EmailWarming] Deleted config for candidate {current_user.id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[EmailWarming] Failed to delete config: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete warming config")

    return {
        "success": True,
        "message": "Warming configuration deleted"
    }
