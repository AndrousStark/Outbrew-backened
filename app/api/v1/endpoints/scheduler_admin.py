"""
Scheduler Admin API Endpoints

Administrative endpoints for managing scheduled background jobs
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import List

logger = logging.getLogger(__name__)

from app.tasks.scheduler import get_scheduler_status as get_status, scheduler
from app.api.dependencies import require_super_admin
from app.models.candidate import Candidate


router = APIRouter(tags=["Admin - Scheduler"])


# ========================================
# Pydantic Schemas
# ========================================

class JobStatus(BaseModel):
    """Schema for job status"""
    id: str
    name: str
    next_run: str
    trigger: str


class SchedulerStatus(BaseModel):
    """Schema for scheduler status"""
    scheduler_running: bool
    jobs: List[JobStatus]


# ========================================
# Admin Endpoints
# ========================================

@router.get("/status", response_model=SchedulerStatus)
async def get_scheduler_status_endpoint(
    admin: Candidate = Depends(require_super_admin)
):
    """
    Get status of the background scheduler

    Returns:
    - Scheduler running state
    - List of all scheduled jobs with their next run times
    """
    status_data = get_status()

    return {
        "scheduler_running": status_data.get("running", False),
        "jobs": [
            {
                "id": job["id"],
                "name": job["name"],
                "next_run": job["next_run"] if job["next_run"] else "",
                "trigger": job["trigger"]
            }
            for job in status_data.get("jobs", [])
        ]
    }


@router.post("/jobs/{job_id}/trigger", status_code=status.HTTP_202_ACCEPTED)
async def trigger_job_manually(
    job_id: str,
    admin: Candidate = Depends(require_super_admin)
):
    """
    Manually trigger a scheduled job

    Available job IDs:
    - **daily_snapshots**: Generate daily analytics snapshots
    - **weekly_snapshots**: Generate weekly analytics snapshots
    - **monthly_snapshots**: Generate monthly analytics snapshots
    - **calculate_rankings**: Calculate template rankings
    - **ab_test_checks**: Check A/B test statistical significance
    - **auto_complete_tests**: Auto-complete eligible A/B tests

    Returns 202 Accepted as the job runs asynchronously.
    """
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not running")

    try:
        job = scheduler.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        from datetime import datetime, timezone
        job.modify(next_run_time=datetime.now(timezone.utc))  # Run immediately
        return {
            "message": f"Job {job_id} triggered successfully",
            "job_id": job_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Error triggering job")


@router.get("/jobs", response_model=dict)
async def list_available_jobs(
    admin: Candidate = Depends(require_super_admin)
):
    """
    List all available scheduled jobs with descriptions

    Helpful for understanding what each job does before manually triggering.
    """
    return {
        "jobs": [
            {
                "id": "daily_snapshots",
                "name": "Generate Daily Analytics Snapshots",
                "description": "Aggregates template analytics events into daily performance snapshots",
                "schedule": "Every day at 1:00 AM"
            },
            {
                "id": "weekly_snapshots",
                "name": "Generate Weekly Analytics Snapshots",
                "description": "Aggregates template analytics events into weekly performance snapshots",
                "schedule": "Every Monday at 2:00 AM"
            },
            {
                "id": "monthly_snapshots",
                "name": "Generate Monthly Analytics Snapshots",
                "description": "Aggregates template analytics events into monthly performance snapshots",
                "schedule": "1st of each month at 3:00 AM"
            },
            {
                "id": "calculate_rankings",
                "name": "Calculate Template Rankings",
                "description": "Computes overall and category rankings for all templates",
                "schedule": "Every day at 4:00 AM"
            },
            {
                "id": "ab_test_checks",
                "name": "Check A/B Test Statistical Significance",
                "description": "Checks all running A/B tests for statistical significance",
                "schedule": "Every 6 hours"
            },
            {
                "id": "auto_complete_tests",
                "name": "Auto-Complete Eligible A/B Tests",
                "description": "Automatically completes tests that have significant results",
                "schedule": "Every 12 hours"
            }
        ]
    }
