"""
Celery Tasks for Extraction Engine
Background processing for long-running extraction jobs
"""

import asyncio
import logging
from typing import Dict, Any

from app.core.celery_app import celery_app
from app.core.database import SessionLocal
from app.services.scraper_manager import ScraperManager
from app.utils.progress_tracker import ProgressPublisher
from app.models.extraction import ExtractionJob
from datetime import timezone

logger = logging.getLogger(__name__)


@celery_app.task(name="extraction.run_extraction_job", bind=True)
def run_extraction_job(self, job_id: int) -> Dict[str, Any]:
    """
    Run extraction job in background

    Args:
        job_id: ID of extraction job to process

    Returns:
        Result dictionary with statistics
    """
    logger.info(f"Starting extraction job {job_id}")

    db = SessionLocal()
    publisher = None

    try:
        # Get job
        job = db.query(ExtractionJob).filter_by(id=job_id).first()
        if not job:
            raise ValueError(f"Job {job_id} not found")

        # Create scraper manager
        manager = ScraperManager(db=db, job_id=job_id)

        # Run extraction (async)
        # If we're already inside an event loop (FastAPI + mock celery), use a background thread
        try:
            asyncio.get_running_loop()
            loop_running = True
        except RuntimeError:
            loop_running = False

        if loop_running:
            import concurrent.futures

            def _run_in_thread():
                thread_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(thread_loop)
                try:
                    return thread_loop.run_until_complete(manager.start_extraction())
                finally:
                    thread_loop.close()

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                result = executor.submit(_run_in_thread).result()
        else:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(manager.start_extraction())
            finally:
                loop.close()

        logger.info(f"Extraction job {job_id} completed: {result}")
        return result

    except Exception as e:
        logger.error(f"Extraction job {job_id} failed: {e}", exc_info=True)

        # Update job status
        try:
            job = db.query(ExtractionJob).filter_by(id=job_id).first()
            if job:
                job.status = "failed"
                db.commit()
        except Exception as e:
            logger.warning(f"[ExtractionTasks] Failed to update job status: {e}")

        # Publish error
        try:
            publisher = ProgressPublisher(job_id)
            err_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(err_loop)
            try:
                err_loop.run_until_complete(publisher.connect())
                err_loop.run_until_complete(publisher.publish_error(str(e)))
                err_loop.run_until_complete(publisher.close())
            finally:
                err_loop.close()
        except Exception as e:
            logger.warning(f"[ExtractionTasks] Failed to publish error: {e}")

        raise

    finally:
        db.close()


@celery_app.task(name="extraction.pause_extraction_job")
def pause_extraction_job(job_id: int) -> Dict[str, str]:
    """
    Pause extraction job

    Note: Pausing is best-effort - current operation will complete
    """
    db = SessionLocal()

    try:
        job = db.query(ExtractionJob).filter_by(id=job_id).first()
        if not job:
            return {"status": "error", "message": f"Job {job_id} not found"}

        if job.status != "running":
            return {"status": "error", "message": f"Job {job_id} is not running"}

        job.status = "paused"
        db.commit()

        logger.info(f"Paused extraction job {job_id}")
        return {"status": "success", "message": f"Job {job_id} paused"}

    finally:
        db.close()


@celery_app.task(name="extraction.resume_extraction_job")
def resume_extraction_job(job_id: int) -> Dict[str, str]:
    """
    Resume paused extraction job
    """
    db = SessionLocal()

    try:
        job = db.query(ExtractionJob).filter_by(id=job_id).first()
        if not job:
            return {"status": "error", "message": f"Job {job_id} not found"}

        if job.status != "paused":
            return {"status": "error", "message": f"Job {job_id} is not paused"}

        # Resume by starting new task
        job.status = "running"
        db.commit()

        # Note: In production, would need to implement actual resume logic
        # For now, this is a placeholder

        logger.info(f"Resumed extraction job {job_id}")
        return {"status": "success", "message": f"Job {job_id} resumed"}

    finally:
        db.close()


@celery_app.task(name="extraction.cancel_extraction_job")
def cancel_extraction_job(job_id: int) -> Dict[str, str]:
    """
    Cancel extraction job
    """
    db = SessionLocal()

    try:
        job = db.query(ExtractionJob).filter_by(id=job_id).first()
        if not job:
            return {"status": "error", "message": f"Job {job_id} not found"}

        if job.status in ["completed", "failed", "cancelled"]:
            return {"status": "error", "message": f"Job {job_id} already {job.status}"}

        job.status = "cancelled"
        db.commit()

        logger.info(f"Cancelled extraction job {job_id}")
        return {"status": "success", "message": f"Job {job_id} cancelled"}

    finally:
        db.close()


@celery_app.task(name="extraction.cleanup_old_jobs")
def cleanup_old_jobs(days: int = 30) -> Dict[str, Any]:
    """
    Cleanup old extraction jobs and results

    Args:
        days: Delete jobs older than this many days

    Returns:
        Cleanup statistics
    """
    from datetime import datetime, timedelta

    db = SessionLocal()

    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        # Find old jobs
        old_jobs = (
            db.query(ExtractionJob)
            .filter(
                ExtractionJob.created_at < cutoff_date,
                ExtractionJob.status.in_(["completed", "failed", "cancelled"]),
            )
            .all()
        )

        deleted_count = len(old_jobs)

        # Delete jobs (cascade will delete results and progress)
        for job in old_jobs:
            db.delete(job)

        db.commit()

        logger.info(f"Cleaned up {deleted_count} old extraction jobs")

        return {
            "status": "success",
            "deleted_jobs": deleted_count,
            "cutoff_date": cutoff_date.isoformat(),
        }

    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}

    finally:
        db.close()
