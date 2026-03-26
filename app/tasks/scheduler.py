"""
Advanced Background Task Scheduler

Implements intelligent email warming progression, rate limit resets,
and scheduled maintenance tasks using APScheduler.

Architecture:
- AsyncIOScheduler for non-blocking execution
- CronTrigger for time-based scheduling
- IntervalTrigger for periodic tasks
- JobStore persistence for reliability
- Distributed locking for multi-worker deployments

Distributed Lock Support:
- Uses database-based advisory locks to prevent duplicate job execution
- Falls back gracefully if lock cannot be acquired
- Supports Redis-based locks when REDIS_URL is configured
"""

import os
import logging
import hashlib
from functools import wraps
from contextlib import contextmanager
from datetime import datetime, date, timedelta, timezone
from typing import Optional, List, Dict, Any, Generator, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from sqlalchemy.orm import Session
from sqlalchemy import func, text

from app.core.database import SessionLocal

# Redis URL for distributed locking (optional)
REDIS_URL = os.getenv("REDIS_URL", "")


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions in scheduler jobs.
    Ensures proper cleanup and error handling.
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"[Scheduler] Database error: {e}")
        raise
    finally:
        db.close()


class DistributedLock:
    """
    Distributed lock implementation for preventing duplicate job execution
    across multiple workers.

    Supports:
    - Redis-based locks (when REDIS_URL is set) - preferred for production
    - Database-based advisory locks (PostgreSQL) - fallback
    - In-memory locks (SQLite/single worker) - development only

    Usage:
        with DistributedLock("job_name", timeout=300) as acquired:
            if acquired:
                # Execute job
            else:
                # Skip - another worker is running this job
    """

    def __init__(self, lock_name: str, timeout: int = 300):
        """
        Initialize distributed lock.

        Args:
            lock_name: Unique identifier for the lock
            timeout: Lock timeout in seconds (default: 5 minutes)
        """
        self.lock_name = lock_name
        self.timeout = timeout
        self.lock_id = self._generate_lock_id(lock_name)
        self._redis_client = None
        self._db_session = None
        self._lock_acquired = False

    @staticmethod
    def _generate_lock_id(name: str) -> int:
        """Generate a numeric lock ID from lock name (for PostgreSQL advisory locks)."""
        # Use sha256 for more secure, consistent numeric ID (not for cryptographic security)
        return int(hashlib.sha256(name.encode()).hexdigest()[:8], 16)

    def _try_redis_lock(self) -> bool:
        """Try to acquire lock using Redis."""
        if not REDIS_URL:
            return False

        try:
            import redis
            self._redis_client = redis.from_url(REDIS_URL)
            # SET NX with expiry for atomic lock acquisition
            result = self._redis_client.set(
                f"scheduler_lock:{self.lock_name}",
                "locked",
                nx=True,
                ex=self.timeout
            )
            if result:
                logger.debug(f"[Lock] Acquired Redis lock for {self.lock_name}")
                return True
            else:
                logger.debug(f"[Lock] Redis lock already held for {self.lock_name}")
                return False
        except ImportError:
            logger.debug("[Lock] Redis not installed, falling back to DB lock")
            return False
        except Exception as e:
            logger.warning(f"[Lock] Redis lock failed: {e}, falling back to DB lock")
            return False

    def _try_db_lock(self) -> bool:
        """Try to acquire lock using database advisory lock (PostgreSQL)."""
        try:
            self._db_session = SessionLocal()

            # Check if PostgreSQL (supports advisory locks)
            dialect = self._db_session.bind.dialect.name

            if dialect == "postgresql":
                # PostgreSQL advisory lock - non-blocking
                result = self._db_session.execute(
                    text("SELECT pg_try_advisory_lock(:lock_id)"),
                    {"lock_id": self.lock_id}
                ).scalar()

                if result:
                    logger.debug(f"[Lock] Acquired PostgreSQL advisory lock for {self.lock_name}")
                    return True
                else:
                    logger.debug(f"[Lock] PostgreSQL lock already held for {self.lock_name}")
                    self._db_session.close()
                    self._db_session = None
                    return False
            else:
                # SQLite or other - use simple in-memory tracking
                # This only works for single-worker deployments
                logger.debug(f"[Lock] Using in-memory lock for {self.lock_name} (single worker mode)")
                return True

        except Exception as e:
            logger.warning(f"[Lock] DB lock failed for {self.lock_name}: {e}")
            if self._db_session:
                self._db_session.close()
                self._db_session = None
            return False

    def _release_redis_lock(self):
        """Release Redis lock."""
        if self._redis_client:
            try:
                self._redis_client.delete(f"scheduler_lock:{self.lock_name}")
                logger.debug(f"[Lock] Released Redis lock for {self.lock_name}")
            except Exception as e:
                logger.warning(f"[Lock] Failed to release Redis lock: {e}")
            finally:
                self._redis_client = None

    def _release_db_lock(self):
        """Release database advisory lock."""
        if self._db_session:
            try:
                dialect = self._db_session.bind.dialect.name
                if dialect == "postgresql":
                    self._db_session.execute(
                        text("SELECT pg_advisory_unlock(:lock_id)"),
                        {"lock_id": self.lock_id}
                    )
                    logger.debug(f"[Lock] Released PostgreSQL advisory lock for {self.lock_name}")
            except Exception as e:
                logger.warning(f"[Lock] Failed to release DB lock: {e}")
            finally:
                self._db_session.close()
                self._db_session = None

    def __enter__(self) -> bool:
        """Acquire lock. Returns True if acquired, False otherwise."""
        # Try Redis first (preferred for distributed)
        if self._try_redis_lock():
            self._lock_acquired = True
            return True

        # Fall back to database lock
        if self._try_db_lock():
            self._lock_acquired = True
            return True

        return False

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release lock."""
        if self._lock_acquired:
            self._release_redis_lock()
            self._release_db_lock()
            self._lock_acquired = False
        return False  # Don't suppress exceptions


def with_distributed_lock(lock_name: str, timeout: int = 300):
    """
    Decorator to wrap async job functions with distributed locking.

    Usage:
        @with_distributed_lock("my_job", timeout=300)
        async def my_scheduled_job():
            # Job code here
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            with DistributedLock(lock_name, timeout) as acquired:
                if acquired:
                    logger.info(f"[Scheduler] Acquired lock for {lock_name}, executing job")
                    return await func(*args, **kwargs)
                else:
                    logger.info(f"[Scheduler] Could not acquire lock for {lock_name}, skipping (another worker is running)")
                    return {"skipped": True, "reason": "lock_not_acquired"}
        return wrapper
    return decorator


from app.models.email_warming import EmailWarmingConfig, EmailWarmingDailyLog, WarmingStatusEnum
from app.models.rate_limiting import RateLimitConfig, RateLimitUsageLog
from app.models.application import Application
from app.models.candidate import Candidate
from app.models.scheduled_email import ScheduledEmail, ScheduledEmailStatus
from app.models.follow_up import FollowUpCampaign, FollowUpEmail, CampaignStatus, FollowUpEmailStatus

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler: Optional[AsyncIOScheduler] = None


class WarmingProgressionEngine:
    """
    Intelligent Email Warming Progression Engine

    Features:
    - Automatic daily limit progression based on schedule
    - Success rate monitoring and adaptive adjustments
    - Bounce detection and automatic pause
    - Smart recovery after issues
    """

    BOUNCE_THRESHOLD = 10  # Pause if bounce rate exceeds 10%
    MIN_SUCCESS_RATE = 85  # Minimum success rate to continue progression

    @classmethod
    @with_distributed_lock("email_warming_progression", timeout=600)
    async def process_all_warming_configs(cls) -> Dict[str, Any]:
        """Process all active warming configurations."""
        logger.info("[WARMING] Starting email warming progression job")

        results = {
            "processed": 0,
            "progressed": 0,
            "paused": 0,
            "completed": 0,
            "errors": []
        }

        with get_db_session() as db:
            # Get all active warming configs
            configs = db.query(EmailWarmingConfig).filter(
                EmailWarmingConfig.status == WarmingStatusEnum.ACTIVE,
                EmailWarmingConfig.deleted_at.is_(None)
            ).all()

            for config in configs:
                try:
                    result = cls._process_single_config(db, config)
                    results["processed"] += 1

                    if result == "progressed":
                        results["progressed"] += 1
                    elif result == "paused":
                        results["paused"] += 1
                    elif result == "completed":
                        results["completed"] += 1

                except Exception as e:
                    logger.error(f"Failed to process warming for candidate {config.candidate_id}: {e}", exc_info=True)
                    results["errors"].append({
                        "candidate_id": config.candidate_id,
                        "error": str(e)
                    })

        logger.info(f"[OK] Warming progression complete: {results}")
        return results

    @classmethod
    def _process_single_config(cls, db: Session, config: EmailWarmingConfig) -> str:
        """Process a single warming configuration."""

        # Check if warming period is complete
        if config.current_day >= config.total_days:
            config.status = WarmingStatusEnum.COMPLETED
            logger.info(f"[COMPLETE] Warming completed for candidate {config.candidate_id}")
            return "completed"

        # Get yesterday's statistics
        yesterday = date.today() - timedelta(days=1)
        yesterday_log = db.query(EmailWarmingDailyLog).filter(
            EmailWarmingDailyLog.config_id == config.id,
            EmailWarmingDailyLog.date == yesterday
        ).first()

        # Check for bounce issues
        if yesterday_log and yesterday_log.emails_sent > 0:
            bounce_rate = (yesterday_log.bounces / yesterday_log.emails_sent) * 100
            if bounce_rate > cls.BOUNCE_THRESHOLD:
                config.status = WarmingStatusEnum.PAUSED
                logger.warning(
                    f"[WARN] Paused warming for candidate {config.candidate_id} "
                    f"due to high bounce rate: {bounce_rate:.1f}%"
                )
                return "paused"

            # Check success rate
            if yesterday_log.success_rate < cls.MIN_SUCCESS_RATE:
                logger.warning(
                    f"[WARN] Low success rate for candidate {config.candidate_id}: "
                    f"{yesterday_log.success_rate:.1f}%"
                )
                # Don't pause, but don't progress either - stay at current limit
                return "held"

        # Progress to next day
        config.current_day += 1

        # Calculate new daily limit from schedule
        schedule = config.custom_schedule or cls._get_default_schedule(config.strategy)
        new_limit = cls._get_limit_for_day(schedule, config.current_day)
        config.current_daily_limit = new_limit

        # Create today's log entry
        today_log = EmailWarmingDailyLog(
            config_id=config.id,
            day_number=config.current_day,
            date=date.today(),
            emails_sent=0,
            target_emails=new_limit,
            bounces=0,
            success_rate=100.0
        )
        db.add(today_log)

        logger.info(
            f"[PROGRESS] Progressed warming for candidate {config.candidate_id}: "
            f"Day {config.current_day}, Limit {new_limit}"
        )
        return "progressed"

    @staticmethod
    def _get_default_schedule(strategy: str) -> Dict[int, int]:
        """Get default warming schedule for strategy."""
        schedules = {
            "conservative": {
                1: 5, 7: 10, 14: 20, 21: 35, 30: 50, 45: 75, 60: 100
            },
            "moderate": {
                1: 10, 5: 20, 10: 40, 15: 60, 20: 80, 25: 100, 30: 150
            },
            "aggressive": {
                1: 20, 3: 40, 5: 60, 7: 80, 10: 100, 14: 150, 21: 200
            }
        }
        return schedules.get(strategy, schedules["moderate"])

    @staticmethod
    def _get_limit_for_day(schedule: Dict[int, int], day: int) -> int:
        """Get the email limit for a specific day using linear interpolation."""
        sorted_days = sorted(schedule.keys())

        # If exact day exists
        if day in schedule:
            return schedule[day]

        # Find surrounding days
        lower_day = max([d for d in sorted_days if d <= day], default=sorted_days[0])
        upper_day = min([d for d in sorted_days if d >= day], default=sorted_days[-1])

        if lower_day == upper_day:
            return schedule[lower_day]

        # Linear interpolation
        lower_limit = schedule[lower_day]
        upper_limit = schedule[upper_day]
        progress = (day - lower_day) / (upper_day - lower_day)

        return int(lower_limit + (upper_limit - lower_limit) * progress)


class ScheduledEmailEngine:
    """
    Scheduled Email Processing Engine

    Processes emails scheduled for optimal send times.
    Runs every 5 minutes to check for pending emails that are due.

    Features:
    - Processes pending emails when scheduled_for <= now
    - Handles retries for failed sends (up to 3 attempts)
    - Updates application status after successful send
    - Logs all activity for monitoring
    """

    MAX_BATCH_SIZE = 50  # Process max 50 emails per run to avoid overload

    @classmethod
    @with_distributed_lock("scheduled_email_processing", timeout=300)
    async def process_scheduled_emails(cls) -> Dict[str, Any]:
        """
        Find and send emails that are due.
        Called by APScheduler every 5 minutes.
        """
        logger.info("[SCHEDULED] Starting scheduled email processing")

        results = {
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "errors": []
        }

        with get_db_session() as db:
            now = datetime.now(timezone.utc)

            # Get pending emails that are due
            pending_emails = db.query(ScheduledEmail).filter(
                ScheduledEmail.status == ScheduledEmailStatus.PENDING,
                ScheduledEmail.scheduled_for <= now,
                ScheduledEmail.deleted_at.is_(None)
            ).order_by(
                ScheduledEmail.scheduled_for.asc()
            ).limit(cls.MAX_BATCH_SIZE).all()

            if not pending_emails:
                logger.debug("[SCHEDULED] No pending emails to process")
                return results

            logger.info(f"[SCHEDULED] Found {len(pending_emails)} emails to process")

            for scheduled in pending_emails:
                results["processed"] += 1

                try:
                    # Mark as processing
                    scheduled.status = ScheduledEmailStatus.PROCESSING
                    db.flush()  # Flush but don't commit yet

                    # Get the application and candidate
                    application = scheduled.application
                    candidate = scheduled.candidate

                    if not application or not candidate:
                        logger.error(f"Missing application or candidate for scheduled email {scheduled.id}")
                        scheduled.mark_failed("Missing application or candidate")
                        results["skipped"] += 1
                        continue

                    # Check if application is still valid (not deleted, still draft)
                    if application.deleted_at:
                        scheduled.status = ScheduledEmailStatus.CANCELLED
                        results["skipped"] += 1
                        logger.info(f"[SKIP] Application {application.id} was deleted")
                        continue

                    # Send the email using existing EmailService
                    from app.services.email_service import EmailService
                    email_service = EmailService(db)

                    # Get resume version if available
                    resume_version = application.resume_version

                    # Get email template if available
                    email_template = application.email_template

                    email_log = email_service.send_application_email(
                        application=application,
                        candidate=candidate,
                        resume_version=resume_version,
                        email_template=email_template
                    )

                    # Update scheduled email status
                    scheduled.mark_sent()
                    results["sent"] += 1

                    logger.info(
                        f"[SENT] Scheduled email {scheduled.id} sent successfully "
                        f"(App: {application.id}, To: {application.recruiter_email})"
                    )

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Failed to send scheduled email {scheduled.id}: {error_msg}", exc_info=True)

                    scheduled.mark_failed(error_msg)
                    results["failed"] += 1
                    results["errors"].append({
                        "scheduled_id": scheduled.id,
                        "application_id": scheduled.application_id,
                        "error": error_msg
                    })

        logger.info(f"[OK] Scheduled email processing complete: {results}")
        return results

    @classmethod
    async def cleanup_old_scheduled_emails(cls) -> int:
        """
        Clean up old completed/cancelled scheduled emails.
        Keep records for 30 days after completion.
        """
        logger.info("[CLEANUP] Cleaning up old scheduled emails")

        with get_db_session() as db:
            cutoff = datetime.now(timezone.utc) - timedelta(days=30)

            # Delete old sent/cancelled/failed records
            deleted = db.query(ScheduledEmail).filter(
                ScheduledEmail.status.in_([
                    ScheduledEmailStatus.SENT,
                    ScheduledEmailStatus.CANCELLED,
                    ScheduledEmailStatus.FAILED
                ]),
                ScheduledEmail.updated_at < cutoff
            ).delete(synchronize_session='fetch')

            logger.info(f"[OK] Deleted {deleted} old scheduled email records")
            return deleted

    @classmethod
    def get_pending_count(cls) -> int:
        """Get count of pending scheduled emails."""
        with get_db_session() as db:
            return db.query(ScheduledEmail).filter(
                ScheduledEmail.status == ScheduledEmailStatus.PENDING,
                ScheduledEmail.deleted_at.is_(None)
            ).count()

    @classmethod
    def get_stats(cls) -> Dict[str, Any]:
        """Get statistics about scheduled emails."""
        with get_db_session() as db:
            now = datetime.now(timezone.utc)
            today_start = datetime.combine(now.date(), datetime.min.time())

            return {
                "pending": db.query(ScheduledEmail).filter(
                    ScheduledEmail.status == ScheduledEmailStatus.PENDING
                ).count(),
                "due_now": db.query(ScheduledEmail).filter(
                    ScheduledEmail.status == ScheduledEmailStatus.PENDING,
                    ScheduledEmail.scheduled_for <= now
                ).count(),
                "sent_today": db.query(ScheduledEmail).filter(
                    ScheduledEmail.status == ScheduledEmailStatus.SENT,
                    ScheduledEmail.sent_at >= today_start
                ).count(),
                "failed_today": db.query(ScheduledEmail).filter(
                    ScheduledEmail.status == ScheduledEmailStatus.FAILED,
                    ScheduledEmail.updated_at >= today_start
                ).count()
            }


class RateLimitResetEngine:
    """
    Rate Limit Reset Engine

    Handles automatic reset of hourly, daily, weekly, and monthly rate limits.
    """

    @classmethod
    async def reset_hourly_limits(cls) -> int:
        """Reset hourly rate limit counters."""
        logger.info("[RESET] Resetting hourly rate limits")

        with get_db_session() as db:
            # Archive old hourly logs (keep last 24 hours)
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            deleted = db.query(RateLimitUsageLog).filter(
                RateLimitUsageLog.period_type == "hourly",
                RateLimitUsageLog.period_start < cutoff
            ).delete()

            logger.info(f"[OK] Archived {deleted} old hourly rate limit logs")
            return deleted

    @classmethod
    async def reset_daily_limits(cls) -> int:
        """Reset daily rate limit counters at midnight."""
        logger.info("[RESET] Resetting daily rate limits")

        with get_db_session() as db:
            # Archive old daily logs (keep last 30 days)
            cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            deleted = db.query(RateLimitUsageLog).filter(
                RateLimitUsageLog.period_type == "daily",
                RateLimitUsageLog.period_start < cutoff
            ).delete()

            logger.info(f"[OK] Archived {deleted} old daily rate limit logs")
            return deleted


class MaintenanceEngine:
    """
    Database Maintenance Engine

    Handles periodic cleanup, statistics generation, and health checks.
    """

    @classmethod
    async def cleanup_old_data(cls) -> Dict[str, int]:
        """Clean up old logs and temporary data."""
        logger.info("[CLEANUP] Starting database cleanup")

        results = {}

        with get_db_session() as db:
            # Clean old email warming logs (keep 90 days)
            cutoff_90_days = date.today() - timedelta(days=90)
            results["warming_logs"] = db.query(EmailWarmingDailyLog).filter(
                EmailWarmingDailyLog.date < cutoff_90_days
            ).delete()

            # Clean old rate limit logs (keep 60 days)
            cutoff_60_days = datetime.now(timezone.utc) - timedelta(days=60)
            results["rate_limit_logs"] = db.query(RateLimitUsageLog).filter(
                RateLimitUsageLog.period_start < cutoff_60_days
            ).delete()

            logger.info(f"[OK] Cleanup complete: {results}")
            return results

    @classmethod
    async def generate_daily_statistics(cls) -> Dict[str, Any]:
        """Generate daily application statistics."""
        logger.info("[STATS] Generating daily statistics")

        with get_db_session() as db:
            stats = {
                "date": date.today().isoformat(),
                "total_applications": db.query(Application).count(),
                "sent_today": db.query(Application).filter(
                    func.date(Application.sent_at) == date.today()
                ).count(),
                "active_candidates": db.query(Candidate).filter(
                    Candidate.is_active == True
                ).count(),
            }

            logger.info(f"[OK] Statistics generated: {stats}")
            return stats


class FollowUpEngine:
    """
    Follow-Up Email Processing Engine

    Processes scheduled follow-up emails for active campaigns.
    Features:
    - Processes approved emails when scheduled_at <= now
    - Respects business hours settings from campaign sequence
    - Handles auto-mode campaigns automatically
    - Updates campaign status on completion or reply
    - Generates next step emails when current step is sent
    """

    MAX_BATCH_SIZE = 50  # Process max 50 follow-ups per run

    @classmethod
    @with_distributed_lock("follow_up_email_processing", timeout=900)
    async def process_follow_up_emails(cls) -> Dict[str, Any]:
        """
        Process follow-up emails that are due to be sent.
        Called by APScheduler every 15 minutes.
        """
        logger.info("[FOLLOW-UP] Starting follow-up email processing")

        results = {
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "campaigns_completed": 0,
            "errors": []
        }

        with get_db_session() as db:
            now = datetime.now(timezone.utc)
            current_hour = now.hour

            # Get emails that are scheduled and approved
            pending_emails = db.query(FollowUpEmail).join(FollowUpCampaign).filter(
                FollowUpEmail.status == FollowUpEmailStatus.SCHEDULED,
                FollowUpEmail.scheduled_for <= now,
                FollowUpCampaign.status == CampaignStatus.ACTIVE
            ).order_by(
                FollowUpEmail.scheduled_for.asc()
            ).limit(cls.MAX_BATCH_SIZE).all()

            if not pending_emails:
                logger.debug("[FOLLOW-UP] No pending follow-up emails to process")
                return results

            logger.info(f"[FOLLOW-UP] Found {len(pending_emails)} follow-up emails to process")

            for email in pending_emails:
                results["processed"] += 1

                try:
                    campaign = email.campaign
                    sequence = campaign.sequence

                    # Check business hours if enabled
                    if sequence and sequence.respect_business_hours:
                        if current_hour < sequence.business_hours_start or current_hour >= sequence.business_hours_end:
                            logger.debug(
                                f"[FOLLOW-UP] Skipping email {email.id} - outside business hours "
                                f"({sequence.business_hours_start}:00 - {sequence.business_hours_end}:00)"
                            )
                            results["skipped"] += 1
                            continue

                    # Check if campaign is still active (might have received reply)
                    if campaign.status != CampaignStatus.ACTIVE:
                        email.status = FollowUpEmailStatus.CANCELLED
                        results["skipped"] += 1
                        continue

                    # Send the email
                    from app.services.follow_up_service import FollowUpService
                    from app.services.email_service import EmailService

                    service = FollowUpService(db)
                    email_service = EmailService(db)

                    await service.send_email_now(email.id, email_service)
                    results["sent"] += 1

                    logger.info(
                        f"[FOLLOW-UP] Sent email {email.id} for campaign {campaign.id} "
                        f"(Step {email.step_number})"
                    )

                    # Check if campaign is complete
                    total_steps = len(sequence.steps) if sequence else 0
                    if email.step_number >= total_steps:
                        campaign.status = CampaignStatus.COMPLETED
                        campaign.completed_at = datetime.now(timezone.utc)
                        results["campaigns_completed"] += 1
                        logger.info(f"[FOLLOW-UP] Campaign {campaign.id} completed")
                    else:
                        # Generate next email in sequence (if auto-mode and approved)
                        if campaign.is_auto_mode and campaign.auto_mode_approved:
                            from app.services.follow_up_email_generator import FollowUpEmailGenerator
                            generator = FollowUpEmailGenerator(db)

                            next_step_num = email.step_number + 1
                            next_step = next(
                                (s for s in sequence.steps if s.step_number == next_step_num),
                                None
                            )

                            if next_step:
                                next_email = await generator.generate_follow_up_email(
                                    campaign, next_step, next_step_num
                                )
                                # Auto-approve for auto-mode campaigns
                                next_email.status = FollowUpEmailStatus.SCHEDULED
                                campaign.current_step = next_step_num
                                campaign.next_email_at = next_email.scheduled_at

                                logger.info(
                                    f"[FOLLOW-UP] Generated next email for campaign {campaign.id} "
                                    f"(Step {next_step_num})"
                                )

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Failed to send follow-up email {email.id}: {error_msg}", exc_info=True)

                    email.status = FollowUpEmailStatus.FAILED
                    email.error_message = error_msg
                    results["failed"] += 1
                    results["errors"].append({
                        "email_id": email.id,
                        "campaign_id": email.campaign_id,
                        "error": error_msg
                    })

        logger.info(f"[OK] Follow-up email processing complete: {results}")
        return results

    @classmethod
    async def check_for_replies(cls) -> Dict[str, Any]:
        """
        Check active campaigns for replies.
        This is a placeholder - actual reply detection would integrate
        with email provider APIs (IMAP, Gmail API, etc.)
        """
        logger.info("[FOLLOW-UP] Checking for campaign replies")

        results = {
            "campaigns_checked": 0,
            "replies_detected": 0
        }

        with get_db_session() as db:
            # Get active campaigns
            active_campaigns = db.query(FollowUpCampaign).filter(
                FollowUpCampaign.status == CampaignStatus.ACTIVE
            ).all()

            results["campaigns_checked"] = len(active_campaigns)

            # TODO: Integrate with email provider API to check for replies
            # For now, this is a placeholder that would be implemented
            # based on the specific email provider integration

            # Example integration points:
            # - IMAP polling for new messages
            # - Gmail API watch/push notifications
            # - Webhook from email tracking service

        return results

    @classmethod
    async def cleanup_old_campaigns(cls) -> int:
        """
        Archive old completed/cancelled campaigns.
        Keep detailed records for 90 days.
        """
        logger.info("[FOLLOW-UP] Cleaning up old campaigns")

        with get_db_session() as db:
            cutoff = datetime.now(timezone.utc) - timedelta(days=90)

            # Delete old campaign logs
            from app.models.follow_up import FollowUpLog
            deleted_logs = db.query(FollowUpLog).filter(
                FollowUpLog.created_at < cutoff
            ).delete(synchronize_session='fetch')

            # Archive old emails from completed campaigns
            deleted_emails = db.query(FollowUpEmail).join(FollowUpCampaign).filter(
                FollowUpCampaign.status.in_([
                    CampaignStatus.COMPLETED,
                    CampaignStatus.CANCELLED,
                    CampaignStatus.REPLIED
                ]),
                FollowUpCampaign.completed_at < cutoff
            ).delete(synchronize_session='fetch')

            total_deleted = deleted_logs + deleted_emails
            logger.info(f"[OK] Cleaned up {total_deleted} old follow-up records")
            return total_deleted

    @classmethod
    def get_stats(cls) -> Dict[str, Any]:
        """Get follow-up campaign statistics."""
        with get_db_session() as db:
            return {
                "active_campaigns": db.query(FollowUpCampaign).filter(
                    FollowUpCampaign.status == CampaignStatus.ACTIVE
                ).count(),
                "pending_approval": db.query(FollowUpCampaign).filter(
                    FollowUpCampaign.status == CampaignStatus.PENDING_APPROVAL
                ).count(),
                "emails_pending": db.query(FollowUpEmail).filter(
                    FollowUpEmail.status == FollowUpEmailStatus.SCHEDULED
                ).count(),
                "emails_sent_today": db.query(FollowUpEmail).filter(
                    FollowUpEmail.status == FollowUpEmailStatus.SENT,
                    func.date(FollowUpEmail.sent_at) == date.today()
                ).count()
            }


def job_listener(event):
    """Listen for job events for monitoring."""
    if event.exception:
        logger.error(f"Job {event.job_id} failed with exception: {event.exception}")
    else:
        logger.debug(f"Job {event.job_id} executed successfully")


def start_scheduler() -> AsyncIOScheduler:
    """Initialize and start the background scheduler."""
    global scheduler

    if scheduler is not None:
        logger.warning("Scheduler already running")
        return scheduler

    logger.info("[STARTUP] Starting background scheduler")

    scheduler = AsyncIOScheduler(
        timezone="UTC",
        job_defaults={
            "coalesce": True,  # Combine missed runs into one
            "max_instances": 1,  # Only one instance of each job
            "misfire_grace_time": 60 * 60  # 1 hour grace for missed jobs
        }
    )

    # Add event listener for monitoring
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

    # === Email Warming Jobs ===
    scheduler.add_job(
        WarmingProgressionEngine.process_all_warming_configs,
        CronTrigger(hour=0, minute=5),  # 00:05 UTC daily
        id="email_warming_progression",
        name="Email Warming Daily Progression",
        replace_existing=True
    )

    # Warmup auto-send: send warmup emails every 30 minutes for active configs
    @with_distributed_lock("warmup_auto_send", timeout=600)
    async def warmup_auto_send():
        """Send warmup emails for all active warming configs."""
        from app.services.email_warming_service import EmailWarmingService
        return EmailWarmingService.send_warmup_emails()

    scheduler.add_job(
        warmup_auto_send,
        IntervalTrigger(minutes=30),  # Every 30 minutes
        id="warmup_auto_send",
        name="Warmup Auto-Send Emails",
        replace_existing=True
    )

    # Warmup pool conversation scheduling: schedule peer-to-peer conversations
    @with_distributed_lock("warmup_pool_conversations", timeout=600)
    async def warmup_schedule_conversations():
        """Schedule peer-to-peer warmup conversations for all active pool members."""
        from app.core.database import SessionLocal
        from app.models.warmup_pool import WarmupPoolMember
        from app.services.warmup_pool_service import get_warmup_pool_service

        db = SessionLocal()
        try:
            members = db.query(WarmupPoolMember).filter(
                WarmupPoolMember.is_active == True,
                WarmupPoolMember.status == "active"
            ).all()

            service = get_warmup_pool_service(db)
            total = 0
            for member in members:
                if member.remaining_sends_today > 0:
                    convos = service.schedule_batch_conversations(member.id, count=3)
                    total += len(convos)

            logger.info(f"[Scheduler] Warmup conversations scheduled: {total} total for {len(members)} members")
        except Exception as e:
            logger.error(f"[Scheduler] Warmup conversation scheduling failed: {e}")
        finally:
            db.close()

    scheduler.add_job(
        warmup_schedule_conversations,
        IntervalTrigger(minutes=15),  # Every 15 minutes
        id="warmup_pool_conversations",
        name="Warmup Pool Conversation Scheduling",
        replace_existing=True
    )

    # === Scheduled Email Jobs (Send Time Optimization) ===
    scheduler.add_job(
        ScheduledEmailEngine.process_scheduled_emails,
        IntervalTrigger(minutes=5),  # Every 5 minutes
        id="scheduled_email_processing",
        name="Scheduled Email Processing",
        replace_existing=True
    )

    scheduler.add_job(
        ScheduledEmailEngine.cleanup_old_scheduled_emails,
        CronTrigger(hour=4, minute=0, day_of_week="sun"),  # Every Sunday at 04:00 UTC
        id="scheduled_email_cleanup",
        name="Scheduled Email Cleanup",
        replace_existing=True
    )

    # === Rate Limit Reset Jobs ===
    scheduler.add_job(
        RateLimitResetEngine.reset_hourly_limits,
        CronTrigger(minute=0),  # Every hour at :00
        id="hourly_rate_limit_reset",
        name="Hourly Rate Limit Reset",
        replace_existing=True
    )

    scheduler.add_job(
        RateLimitResetEngine.reset_daily_limits,
        CronTrigger(hour=0, minute=1),  # 00:01 UTC daily
        id="daily_rate_limit_reset",
        name="Daily Rate Limit Reset",
        replace_existing=True
    )

    # === Maintenance Jobs ===
    scheduler.add_job(
        MaintenanceEngine.cleanup_old_data,
        CronTrigger(hour=3, minute=0, day_of_week="sun"),  # Every Sunday at 03:00 UTC
        id="weekly_cleanup",
        name="Weekly Database Cleanup",
        replace_existing=True
    )

    scheduler.add_job(
        MaintenanceEngine.generate_daily_statistics,
        CronTrigger(hour=23, minute=55),  # 23:55 UTC daily
        id="daily_statistics",
        name="Daily Statistics Generation",
        replace_existing=True
    )

    # === Follow-Up Email Jobs ===
    scheduler.add_job(
        FollowUpEngine.process_follow_up_emails,
        IntervalTrigger(minutes=15),  # Every 15 minutes
        id="follow_up_email_processing",
        name="Follow-Up Email Processing",
        replace_existing=True
    )

    scheduler.add_job(
        FollowUpEngine.check_for_replies,
        IntervalTrigger(minutes=30),  # Every 30 minutes
        id="follow_up_reply_check",
        name="Follow-Up Reply Detection",
        replace_existing=True
    )

    scheduler.add_job(
        FollowUpEngine.cleanup_old_campaigns,
        CronTrigger(hour=2, minute=0, day_of_week="sun"),  # Every Sunday at 02:00 UTC
        id="follow_up_cleanup",
        name="Follow-Up Campaign Cleanup",
        replace_existing=True
    )

    # === A/B Testing & Analytics Jobs ===
    # Import analytics service jobs
    from app.services.template_analytics import TemplateAnalyticsService
    from app.services.ab_testing import ABTestingService
    from app.models.follow_up import ABTest, ABTestStatus

    @with_distributed_lock("daily_analytics_snapshots", timeout=600)
    async def generate_daily_snapshots():
        """Generate daily analytics snapshots for all templates"""
        logger.info("[ANALYTICS] Generating daily snapshots")
        with get_db_session() as db:
            from datetime import datetime, timedelta
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            snapshots = TemplateAnalyticsService.generate_all_snapshots(
                db=db,
                period_type="daily",
                snapshot_date=yesterday
            )
            logger.info(f"[ANALYTICS] Generated {len(snapshots)} daily snapshots")
            return {"count": len(snapshots)}

    @with_distributed_lock("weekly_analytics_snapshots", timeout=900)
    async def generate_weekly_snapshots():
        """Generate weekly analytics snapshots"""
        logger.info("[ANALYTICS] Generating weekly snapshots")
        with get_db_session() as db:
            from datetime import datetime, timedelta
            last_monday = datetime.now(timezone.utc) - timedelta(days=datetime.now(timezone.utc).weekday() + 7)
            snapshots = TemplateAnalyticsService.generate_all_snapshots(
                db=db,
                period_type="weekly",
                snapshot_date=last_monday
            )
            logger.info(f"[ANALYTICS] Generated {len(snapshots)} weekly snapshots")
            return {"count": len(snapshots)}

    @with_distributed_lock("monthly_analytics_snapshots", timeout=1200)
    async def generate_monthly_snapshots():
        """Generate monthly analytics snapshots"""
        logger.info("[ANALYTICS] Generating monthly snapshots")
        with get_db_session() as db:
            from datetime import datetime, timedelta
            today = datetime.now(timezone.utc)
            first_of_prev_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
            snapshots = TemplateAnalyticsService.generate_all_snapshots(
                db=db,
                period_type="monthly",
                snapshot_date=first_of_prev_month
            )
            logger.info(f"[ANALYTICS] Generated {len(snapshots)} monthly snapshots")
            return {"count": len(snapshots)}

    @with_distributed_lock("template_rankings", timeout=600)
    async def calculate_template_rankings():
        """Calculate marketplace rankings for all templates"""
        logger.info("[ANALYTICS] Calculating template rankings")
        with get_db_session() as db:
            TemplateAnalyticsService.calculate_rankings(db)
            logger.info("[ANALYTICS] Rankings calculated successfully")
            return {"status": "success"}

    @with_distributed_lock("ab_test_significance_check", timeout=600)
    async def check_ab_test_significance():
        """Check statistical significance for all running A/B tests"""
        logger.info("[AB-TEST] Checking test significance")
        results = {"checked": 0, "significant": 0}

        with get_db_session() as db:
            running_tests = db.query(ABTest).filter(
                ABTest.status == ABTestStatus.RUNNING
            ).all()

            for test in running_tests:
                try:
                    analysis = ABTestingService.calculate_statistical_significance(
                        db=db,
                        test_id=test.id
                    )
                    results["checked"] += 1

                    if analysis.get("is_significant"):
                        results["significant"] += 1
                        logger.info(
                            f"[AB-TEST] Test {test.id} '{test.name}' is significant "
                            f"(p={analysis.get('p_value'):.4f})"
                        )
                except Exception as e:
                    logger.error(f"[AB-TEST] Error checking test {test.id}: {e}")

        logger.info(f"[AB-TEST] Checked {results['checked']} tests, {results['significant']} significant")
        return results

    @with_distributed_lock("ab_test_auto_complete", timeout=900)
    async def auto_complete_ab_tests():
        """Auto-complete A/B tests with sufficient data and significance"""
        logger.info("[AB-TEST] Auto-completing eligible tests")
        results = {"checked": 0, "completed": 0}

        with get_db_session() as db:
            running_tests = db.query(ABTest).filter(
                ABTest.status == ABTestStatus.RUNNING
            ).all()

            for test in running_tests:
                try:
                    # Check if test has enough data
                    if test.total_campaigns < test.minimum_sample_size * 2:
                        continue

                    # Check significance
                    analysis = ABTestingService.calculate_statistical_significance(
                        db=db,
                        test_id=test.id
                    )

                    results["checked"] += 1

                    if analysis.get("is_significant"):
                        test.status = ABTestStatus.COMPLETED
                        test.completed_at = datetime.now(timezone.utc)
                        db.commit()

                        results["completed"] += 1
                        logger.info(
                            f"[AB-TEST] Auto-completed test {test.id} '{test.name}' "
                            f"(p={analysis.get('p_value'):.4f})"
                        )
                except Exception as e:
                    logger.error(f"[AB-TEST] Error auto-completing test {test.id}: {e}")

        logger.info(f"[AB-TEST] Completed {results['completed']}/{results['checked']} tests")
        return results

    # Schedule analytics jobs
    scheduler.add_job(
        generate_daily_snapshots,
        CronTrigger(hour=1, minute=0),  # 01:00 UTC daily
        id="daily_analytics_snapshots",
        name="Daily Analytics Snapshots",
        replace_existing=True
    )

    scheduler.add_job(
        generate_weekly_snapshots,
        CronTrigger(day_of_week="mon", hour=2, minute=0),  # Monday 02:00 UTC
        id="weekly_analytics_snapshots",
        name="Weekly Analytics Snapshots",
        replace_existing=True
    )

    scheduler.add_job(
        generate_monthly_snapshots,
        CronTrigger(day=1, hour=3, minute=0),  # 1st of month 03:00 UTC
        id="monthly_analytics_snapshots",
        name="Monthly Analytics Snapshots",
        replace_existing=True
    )

    scheduler.add_job(
        calculate_template_rankings,
        CronTrigger(hour=4, minute=0),  # 04:00 UTC daily
        id="template_rankings",
        name="Template Rankings Calculation",
        replace_existing=True
    )

    # Schedule A/B testing jobs
    scheduler.add_job(
        check_ab_test_significance,
        IntervalTrigger(hours=6),  # Every 6 hours
        id="ab_test_significance_check",
        name="A/B Test Significance Check",
        replace_existing=True
    )

    scheduler.add_job(
        auto_complete_ab_tests,
        IntervalTrigger(hours=12),  # Every 12 hours
        id="ab_test_auto_complete",
        name="A/B Test Auto-Complete",
        replace_existing=True
    )

    # === ML Intelligence Jobs (Sprint 2) ===
    @with_distributed_lock("ml_daily_training", timeout=1800)
    async def run_ml_daily_training():
        """Daily ML model training for reply prediction and send time optimization"""
        logger.info("[ML] Starting daily ML training")
        with get_db_session() as db:
            from app.services.ml.training_pipeline import run_daily_training
            results = await run_daily_training(db)
            logger.info(f"[ML] Training complete: {results['successful_training']} successful, {results['failed_training']} failed")
            return results

    scheduler.add_job(
        run_ml_daily_training,
        CronTrigger(hour=5, minute=0),  # 05:00 UTC daily
        id="ml_daily_training",
        name="ML Daily Training",
        replace_existing=True
    )

    # === Monthly Usage Reset ===
    @with_distributed_lock("monthly_usage_reset", timeout=120)
    async def reset_usage_counters():
        """Reset monthly email/campaign counters for users past their reset date."""
        from app.core.database import SessionLocal
        from app.services.usage_service import reset_monthly_usage
        db = SessionLocal()
        try:
            result = reset_monthly_usage(db)
            logger.info(f"[UsageReset] Reset {result['reset_count']} users")
        except Exception as e:
            logger.error(f"[UsageReset] Failed: {e}")
            db.rollback()
        finally:
            db.close()

    scheduler.add_job(
        reset_usage_counters,
        CronTrigger(hour=0, minute=10),  # 00:10 UTC daily (checks 30-day window)
        id="monthly_usage_reset",
        name="Monthly Usage Counter Reset",
        replace_existing=True
    )

    scheduler.start()

    # Log scheduled jobs
    jobs = scheduler.get_jobs()
    logger.info(f"[OK] Scheduler started with {len(jobs)} jobs:")
    for job in jobs:
        logger.info(f"   - {job.name}: {job.trigger}")

    return scheduler


def shutdown_scheduler():
    """Gracefully shutdown the scheduler."""
    global scheduler

    if scheduler is None:
        return

    logger.info("[SHUTDOWN] Shutting down scheduler")
    scheduler.shutdown(wait=True)
    scheduler = None
    logger.info("[OK] Scheduler shutdown complete")


def get_scheduler_status() -> Dict[str, Any]:
    """Get current scheduler status and job information."""
    global scheduler

    if scheduler is None:
        return {"running": False, "jobs": []}

    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        })

    return {
        "running": scheduler.running,
        "jobs": jobs
    }
