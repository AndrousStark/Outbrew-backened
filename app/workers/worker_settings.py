"""
ARQ Worker Settings and Configuration (Phase 3)

This file configures the ARQ worker for background job processing.

RUNNING THE WORKER:
    arq app.workers.worker_settings.WorkerSettings

PERFORMANCE:
- Async task execution
- Redis-based queue
- Automatic retries
- Job scheduling
- Result storage
"""
import logging
from arq import create_pool
from arq.connections import RedisSettings
from typing import Optional

from app.workers.email_worker import (
    send_email_task,
    send_bulk_emails_task,
    send_scheduled_email_task,
    send_follow_up_email_task
)
from app.workers.data_worker import (
    refresh_materialized_views_task,
    refresh_all_materialized_views_task,
    cleanup_old_records_task,
    generate_report_task,
    update_company_intelligence_task,
    vacuum_database_task
)
from app.workers.notification_worker import (
    send_notification_task,
    send_bulk_notifications_task,
    send_application_status_notification_task,
    send_email_opened_notification_task,
    send_response_received_notification_task
)

logger = logging.getLogger(__name__)


# ============================================================================
# WORKER CONFIGURATION
# ============================================================================

class WorkerSettings:
    """
    ARQ Worker Settings

    Configures the worker process for background task execution.
    """

    # Redis connection settings
    redis_settings = RedisSettings(
        host="localhost",
        port=6379,
        database=0
    )

    # Task functions available to the worker
    functions = [
        # Email tasks
        send_email_task,
        send_bulk_emails_task,
        send_scheduled_email_task,
        send_follow_up_email_task,

        # Data tasks
        refresh_materialized_views_task,
        refresh_all_materialized_views_task,
        cleanup_old_records_task,
        generate_report_task,
        update_company_intelligence_task,
        vacuum_database_task,

        # Notification tasks
        send_notification_task,
        send_bulk_notifications_task,
        send_application_status_notification_task,
        send_email_opened_notification_task,
        send_response_received_notification_task,
    ]

    # Cron jobs (scheduled tasks)
    cron_jobs = [
        # Refresh stats views every 5 minutes
        {
            "function": refresh_materialized_views_task,
            "cron": "*/5 * * * *",  # Every 5 minutes
            "run_at_startup": True
        },

        # Refresh all views every hour
        {
            "function": refresh_all_materialized_views_task,
            "cron": "0 * * * *",  # Every hour at :00
            "run_at_startup": False
        },

        # Cleanup old records weekly
        {
            "function": cleanup_old_records_task,
            "cron": "0 2 * * 0",  # Sunday at 2:00 AM
            "run_at_startup": False
        },

        # Vacuum database weekly
        {
            "function": vacuum_database_task,
            "cron": "0 3 * * 0",  # Sunday at 3:00 AM
            "run_at_startup": False
        },
    ]

    # Worker settings
    max_jobs = 10  # Maximum concurrent jobs
    job_timeout = 300  # Job timeout in seconds (5 minutes)
    keep_result = 3600  # Keep job results for 1 hour
    queue_name = "arq:queue"  # Queue name in Redis

    # Retry settings
    max_tries = 3  # Maximum retry attempts
    retry_delay = 60  # Delay between retries (seconds)

    # Health check
    health_check_interval = 60  # Check health every 60 seconds

    # Logging
    log_results = True
    log_errors = True


# ============================================================================
# HELPER FUNCTIONS FOR ENQUEUEING JOBS
# ============================================================================

async def enqueue_email(
    to_email: str,
    subject: str,
    body_html: str,
    from_email: str,
    from_name: str = "Outbrew",
    smtp_host: str = "smtp.gmail.com",
    smtp_port: int = 587,
    application_id: Optional[int] = None,
    candidate_id: Optional[int] = None
):
    """
    Enqueue email sending job.

    SMTP password is looked up from the database inside the worker task
    using candidate_id, NOT passed through Redis args.

    Usage in endpoints:
        from app.workers.worker_settings import enqueue_email

        await enqueue_email(
            to_email="recruiter@company.com",
            subject="Application for Senior Developer",
            body_html="<html>...</html>",
            from_email="candidate@email.com",
            candidate_id=123
        )
    """
    redis = await create_pool(WorkerSettings.redis_settings)

    job = await redis.enqueue_job(
        "send_email_task",
        to_email=to_email,
        subject=subject,
        body_html=body_html,
        from_email=from_email,
        from_name=from_name,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        application_id=application_id,
        candidate_id=candidate_id
    )

    logger.info(f"📧 [WORKER] Enqueued email job: {job.job_id}")
    return job.job_id


async def enqueue_notification(
    candidate_id: int,
    notification_type: str,
    title: str,
    message: str,
    data: dict = None
):
    """
    Enqueue notification job.

    Usage:
        await enqueue_notification(
            candidate_id=1,
            notification_type="application_status",
            title="Application Updated",
            message="Your application status changed to 'opened'",
            data={"application_id": 123}
        )
    """
    redis = await create_pool(WorkerSettings.redis_settings)

    job = await redis.enqueue_job(
        "send_notification_task",
        candidate_id=candidate_id,
        notification_type=notification_type,
        title=title,
        message=message,
        data=data
    )

    logger.info(f"🔔 [WORKER] Enqueued notification job: {job.job_id}")
    return job.job_id


async def enqueue_materialized_view_refresh():
    """
    Enqueue materialized view refresh job.

    Can be called manually or via endpoint:
        @router.post("/admin/refresh-views")
        async def refresh_views():
            job_id = await enqueue_materialized_view_refresh()
            return {"job_id": job_id, "status": "queued"}
    """
    redis = await create_pool(WorkerSettings.redis_settings)

    job = await redis.enqueue_job("refresh_materialized_views_task")

    logger.info(f"🔄 [WORKER] Enqueued materialized view refresh: {job.job_id}")
    return job.job_id


async def get_job_result(job_id: str):
    """
    Get job result by ID.

    Usage:
        job_id = await enqueue_email(...)
        # Wait for job to complete
        result = await get_job_result(job_id)
        print(result)  # {"status": "success", ...}
    """
    redis = await create_pool(WorkerSettings.redis_settings)

    job = await redis.get_job(job_id)
    if not job:
        return {"error": "Job not found"}

    result = await job.result()
    return result


async def get_job_status(job_id: str):
    """
    Get job status by ID.

    Returns one of:
    - "queued": Job is waiting to be processed
    - "in_progress": Job is currently running
    - "complete": Job finished successfully
    - "not_found": Job doesn't exist
    """
    redis = await create_pool(WorkerSettings.redis_settings)

    job = await redis.get_job(job_id)
    if not job:
        return "not_found"

    return await job.status()


# ============================================================================
# STARTUP FUNCTION
# ============================================================================

async def startup(ctx):
    """
    Called when worker starts up.

    Can be used to initialize resources, connections, etc.
    """
    logger.info("🚀 [WORKER] ARQ worker starting up...")
    logger.info(f"📋 [WORKER] Registered {len(WorkerSettings.functions)} task functions")
    logger.info(f"⏰ [WORKER] Scheduled {len(WorkerSettings.cron_jobs)} cron jobs")


async def shutdown(ctx):
    """
    Called when worker shuts down.

    Clean up resources here.
    """
    logger.info("🛑 [WORKER] ARQ worker shutting down...")


# Add startup/shutdown to settings
WorkerSettings.on_startup = startup
WorkerSettings.on_shutdown = shutdown


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

"""
1. Start the worker:
    arq app.workers.worker_settings.WorkerSettings

2. In your FastAPI endpoint:
    from app.workers.worker_settings import enqueue_email

    @router.post("/applications/{id}/send")
    async def send_application_email(id: int):
        app = await get_application(id)

        job_id = await enqueue_email(
            to_email=app.recruiter_email,
            subject=f"Application for {app.position_title}",
            body_html=app.email_body_html,
            from_email=app.candidate.email_account,
            from_password=app.candidate.email_password,
            application_id=id,
            candidate_id=app.candidate_id
        )

        return {
            "status": "queued",
            "job_id": job_id,
            "message": "Email is being sent in background"
        }

3. Check job status:
    @router.get("/jobs/{job_id}/status")
    async def check_job_status(job_id: str):
        status = await get_job_status(job_id)
        return {"job_id": job_id, "status": status}

4. Get job result:
    @router.get("/jobs/{job_id}/result")
    async def get_job_result_endpoint(job_id: str):
        result = await get_job_result(job_id)
        return result
"""
