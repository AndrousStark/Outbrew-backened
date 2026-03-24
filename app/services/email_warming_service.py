"""Email Warming Service

Handles email warming logic, daily limits, and progress tracking.
"""

import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session

from app.models.email_warming import (
    EmailWarmingConfig,
    EmailWarmingDailyLog,
    WarmingStrategyEnum,
    WarmingStatusEnum,
    WARMING_SCHEDULES
)
from app.core.logger import warming_logger as logger


def _ensure_bounce_threshold_column():
    """Add bounce_threshold column if missing (migration for existing DBs)"""
    from app.core.database import engine
    from sqlalchemy import inspect, text
    inspector = inspect(engine)
    columns = [c['name'] for c in inspector.get_columns('email_warming_configs')]
    if 'bounce_threshold' not in columns:
        with engine.connect() as conn:
            conn.execute(text('ALTER TABLE email_warming_configs ADD COLUMN bounce_threshold FLOAT DEFAULT 2.0'))
            conn.commit()


try:
    _ensure_bounce_threshold_column()
except Exception as _migration_err:
    logger.warning(f"Could not run bounce_threshold migration (table may not exist yet): {_migration_err}")


class EmailWarmingService:
    """Service for managing email warming campaigns"""

    @staticmethod
    def create_config(
        db: Session,
        candidate_id: int,
        strategy: str = WarmingStrategyEnum.MODERATE.value,
        custom_schedule: Optional[Dict[int, int]] = None,
        auto_progress: bool = True
    ) -> EmailWarmingConfig:
        """Create a new email warming configuration"""
        logger.info(f"📝 Creating warming config for candidate {candidate_id}")
        logger.debug(f"   Strategy: {strategy}, Auto-progress: {auto_progress}")

        # Check if config already exists
        existing = db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.candidate_id == candidate_id
        ).first()

        if existing:
            logger.info(f"✓ Config already exists for candidate {candidate_id} (ID: {existing.id})")
            return existing

        config = EmailWarmingConfig(
            candidate_id=candidate_id,
            strategy=strategy,
            custom_schedule=custom_schedule,
            auto_progress=auto_progress,
            status=WarmingStatusEnum.NOT_STARTED.value
        )

        try:
            db.add(config)
            db.commit()
            db.refresh(config)
            logger.info(f"✓ Created warming config ID {config.id} for candidate {candidate_id}")
        except Exception as e:
            db.rollback()
            logger.error(f"[ERROR] Failed to create warming config for candidate {candidate_id}: {e}")
            raise ValueError(f"Failed to create warming config: {e}")

        return config

    @staticmethod
    def start_warming(db: Session, config_id: int) -> EmailWarmingConfig:
        """Start the warming campaign"""
        logger.info(f"🚀 Starting warming campaign for config ID {config_id}")

        config = db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.id == config_id
        ).first()

        if not config:
            logger.error(f"[ERROR] Warming config {config_id} not found")
            raise ValueError("Warming config not found")

        config.status = WarmingStatusEnum.ACTIVE.value
        config.start_date = datetime.now(timezone.utc)
        config.current_day = 1
        config.emails_sent_today = 0
        config.last_reset_date = datetime.now(timezone.utc)

        try:
            db.commit()
            db.refresh(config)
            daily_limit = EmailWarmingService.get_daily_limit(config)
            logger.info(f"✓ Warming campaign started! Day 1, Limit: {daily_limit} emails")
        except Exception as e:
            db.rollback()
            logger.error(f"[ERROR] Failed to start warming campaign for config {config_id}: {e}")
            raise ValueError(f"Failed to start warming campaign: {e}")

        return config

    @staticmethod
    def get_daily_limit(config: EmailWarmingConfig) -> int:
        """Get the daily email limit based on current day and strategy"""

        if config.strategy == WarmingStrategyEnum.CUSTOM.value:
            if config.custom_schedule:
                return config.custom_schedule.get(str(config.current_day), 0)
            return 0

        # Get schedule for the strategy
        schedule = WARMING_SCHEDULES.get(WarmingStrategyEnum(config.strategy), {})

        # Return limit for current day, or max limit if beyond schedule
        if config.current_day in schedule:
            return schedule[config.current_day]
        else:
            # Beyond the schedule, return the max value
            return max(schedule.values()) if schedule else 100

    @staticmethod
    def can_send_email(db: Session, candidate_id: int) -> Tuple[bool, str, int]:
        """
        Check if an email can be sent based on warming limits

        Returns:
            (can_send, reason, remaining_quota)
        """
        logger.debug(f"[CHECK] Checking warming limits for candidate {candidate_id}")

        # Use with_for_update() to prevent race conditions on concurrent sends
        # Note: SQLite serializes inherently; FOR UPDATE is for future PostgreSQL
        config = db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.candidate_id == candidate_id
        ).with_for_update().first()

        if not config:
            logger.debug(f"   No warming config found - allowing send")
            return True, "No warming config", 999999

        logger.debug(f"   Config ID: {config.id}, Status: {config.status}, Day: {config.current_day}")

        if config.status == WarmingStatusEnum.NOT_STARTED.value:
            logger.info(f"🚀 Auto-starting warming campaign for candidate {candidate_id}")
            EmailWarmingService.start_warming(db, config.id)
            # Refresh config with null check
            refreshed_config = db.query(EmailWarmingConfig).get(config.id)
            if refreshed_config:
                config = refreshed_config
            else:
                logger.error(f"[ERROR] Failed to refresh warming config {config.id} after start")
                return False, "Failed to start warming config", 0

        if config.status == WarmingStatusEnum.PAUSED.value:
            logger.warning(f"⏸️  Warming campaign is PAUSED for candidate {candidate_id}")
            return False, "Warming campaign is paused", 0

        if config.status == WarmingStatusEnum.FAILED.value:
            logger.error(f"[ERROR] Warming campaign FAILED for candidate {candidate_id}")
            return False, "Warming campaign failed", 0

        if config.status == WarmingStatusEnum.COMPLETED.value:
            logger.info(f"✓ Warming COMPLETED for candidate {candidate_id} - no limits")
            return True, "Warming completed", 999999

        # Check if we need to reset daily counter
        EmailWarmingService.check_and_reset_daily_counter(db, config)

        # Get daily limit
        daily_limit = EmailWarmingService.get_daily_limit(config)
        remaining = daily_limit - config.emails_sent_today

        logger.debug(f"   Daily limit: {daily_limit}, Sent today: {config.emails_sent_today}, Remaining: {remaining}")

        if config.emails_sent_today >= daily_limit:
            logger.warning(f"🚫 Daily warming limit REACHED for candidate {candidate_id}: {daily_limit} emails")
            return False, f"Daily warming limit reached ({daily_limit} emails)", 0

        logger.info(f"✓ Can send email - {remaining} remaining of {daily_limit} daily limit")
        return True, "OK", remaining

    @staticmethod
    def record_email_sent(
        db: Session,
        candidate_id: int,
        success: bool = True,
        bounced: bool = False
    ) -> None:
        """Record that an email was sent"""
        logger.info(f"[WARMUP] Recording email sent for candidate {candidate_id} (success={success}, bounced={bounced})")

        # Use with_for_update() to prevent race conditions on concurrent sends
        # Note: SQLite serializes inherently; FOR UPDATE is for future PostgreSQL
        config = db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.candidate_id == candidate_id
        ).with_for_update().first()

        if not config:
            logger.debug(f"   No warming config found - skipping record")
            return

        # Increment counters (guard against None from DB)
        config.emails_sent_today = (config.emails_sent_today or 0) + 1
        config.total_emails_sent = (config.total_emails_sent or 0) + 1

        logger.debug(f"   Updated counters: Today={config.emails_sent_today}, Total={config.total_emails_sent}")

        # Update or create daily log
        today_log = db.query(EmailWarmingDailyLog).filter(
            EmailWarmingDailyLog.config_id == config.id,
            EmailWarmingDailyLog.day_number == config.current_day
        ).first()

        if not today_log:
            today_log = EmailWarmingDailyLog(
                config_id=config.id,
                day_number=config.current_day,
                date=datetime.now(timezone.utc),
                daily_limit=EmailWarmingService.get_daily_limit(config)
            )
            db.add(today_log)

        today_log.emails_sent = (today_log.emails_sent or 0) + 1

        if success:
            today_log.emails_delivered = (today_log.emails_delivered or 0) + 1
        else:
            today_log.emails_failed = (today_log.emails_failed or 0) + 1

        if bounced:
            today_log.emails_bounced = (today_log.emails_bounced or 0) + 1

        # Calculate metrics with division by zero protection
        if today_log.emails_sent and today_log.emails_sent > 0:
            today_log.delivery_rate = ((today_log.emails_delivered or 0) / today_log.emails_sent) * 100
            today_log.bounce_rate = ((today_log.emails_bounced or 0) / today_log.emails_sent) * 100
            logger.debug(f"   Metrics: delivery_rate={today_log.delivery_rate:.1f}%, bounce_rate={today_log.bounce_rate:.1f}%")
        else:
            today_log.delivery_rate = 0.0
            today_log.bounce_rate = 0.0
            logger.debug("   No emails sent today, metrics set to 0")

        # Check if limit reached
        if config.emails_sent_today >= EmailWarmingService.get_daily_limit(config):
            today_log.limit_reached = True

        # Update config metrics with N+1 query optimization and division by zero protection
        # Use aggregation instead of iterating over daily_logs to avoid N+1
        from sqlalchemy import func
        totals = db.query(
            func.sum(EmailWarmingDailyLog.emails_delivered).label('delivered'),
            func.sum(EmailWarmingDailyLog.emails_bounced).label('bounced')
        ).filter(EmailWarmingDailyLog.config_id == config.id).first()

        total_delivered = totals.delivered or 0
        total_bounced = totals.bounced or 0
        logger.debug(f"   Aggregated totals: delivered={total_delivered}, bounced={total_bounced}")

        if config.total_emails_sent and config.total_emails_sent > 0:
            config.success_rate = (total_delivered / config.total_emails_sent) * 100
            config.bounce_rate = (total_bounced / config.total_emails_sent) * 100
            logger.debug(f"   Config metrics: success_rate={config.success_rate:.1f}%, bounce_rate={config.bounce_rate:.1f}%")
        else:
            config.success_rate = 0.0
            config.bounce_rate = 0.0
            logger.debug("   No total emails sent, config metrics set to 0")

        # Pause if bounce rate too high (2026 best practice: 2% threshold)
        bounce_threshold = getattr(config, 'bounce_threshold', None) or 2.0
        if config.pause_on_high_bounce and config.bounce_rate > bounce_threshold:
            config.status = WarmingStatusEnum.PAUSED.value
            today_log.notes = f"Auto-paused: Bounce rate {config.bounce_rate:.1f}% exceeds {bounce_threshold}%"
            logger.warning(f"⚠️  Auto-pausing warming for config {config.id} due to high bounce rate")

            # Create a notification for the auto-pause event
            try:
                from app.models.notification import Notification, NotificationType
                notification = Notification(
                    title="Email Warmup Auto-Paused",
                    message=(
                        f"Your email warmup campaign has been automatically paused because the "
                        f"bounce rate ({config.bounce_rate:.1f}%) exceeded the threshold "
                        f"({bounce_threshold}%). Please review your email configuration and "
                        f"resume warming when ready."
                    ),
                    notification_type=NotificationType.WARMING_ALERT.value,
                    candidate_id=candidate_id,
                    icon="warning",
                    priority=2,
                )
                db.add(notification)
                logger.info(f"   Created auto-pause notification for candidate {candidate_id}")
            except Exception as notif_err:
                logger.warning(f"   Failed to create auto-pause notification: {notif_err}")

        try:
            db.commit()
            logger.debug(f"   Email record saved for candidate {candidate_id}")
        except Exception as e:
            db.rollback()
            logger.error(f"[ERROR] Failed to record email sent for candidate {candidate_id}: {e}")

    @staticmethod
    def check_and_reset_daily_counter(db: Session, config: EmailWarmingConfig) -> bool:
        """Check if we need to reset daily counter and advance to next day.
        Uses fixed-hour boundary check based on daily_reset_hour config."""
        now = datetime.now(timezone.utc)
        last_reset = config.last_reset_date or config.start_date or now

        # Use fixed-hour boundary instead of rolling 24h window
        reset_hour = getattr(config, 'daily_reset_hour', 0) or 0
        today_reset = now.replace(hour=reset_hour, minute=0, second=0, microsecond=0)
        # If we haven't reached today's reset hour yet, use yesterday's
        if now < today_reset:
            today_reset = today_reset - timedelta(days=1)

        should_reset = last_reset < today_reset

        if should_reset:
            # Reset daily counter
            config.emails_sent_today = 0
            config.last_reset_date = now

            # Auto-progress to next day if enabled
            if config.auto_progress:
                config.current_day = (config.current_day or 0) + 1
                logger.info(f"[DAY] Advanced warming to day {config.current_day} for config {config.id}")

                # Check if warming is complete
                max_day = EmailWarmingService.get_max_day_for_strategy(config.strategy, config.custom_schedule)
                if config.current_day > max_day:
                    config.status = WarmingStatusEnum.COMPLETED.value
                    config.completion_date = now
                    logger.info(f"[COMPLETE] Warming campaign COMPLETED for config {config.id}")

            try:
                db.commit()
                logger.debug(f"   Daily counter reset for config {config.id}")
            except Exception as e:
                db.rollback()
                logger.error(f"[ERROR] Failed to reset daily counter for config {config.id}: {e}")
                return False
            return True

        return False

    @staticmethod
    def get_max_day_for_strategy(strategy: str, custom_schedule: Optional[Dict] = None) -> int:
        """Get the maximum day number for a strategy"""
        if strategy == WarmingStrategyEnum.CUSTOM.value:
            if custom_schedule:
                return max(int(k) for k in custom_schedule.keys())
            return 30  # Default for custom

        schedule = WARMING_SCHEDULES.get(WarmingStrategyEnum(strategy), {})
        return max(schedule.keys()) if schedule else 14

    @staticmethod
    def update_strategy(
        db: Session,
        config_id: int,
        strategy: str,
        custom_schedule: Optional[Dict[int, int]] = None
    ) -> EmailWarmingConfig:
        """Update warming strategy"""
        logger.info(f"📝 Updating strategy for config {config_id} to {strategy}")
        config = db.query(EmailWarmingConfig).get(config_id)

        if not config:
            logger.error(f"[ERROR] Config {config_id} not found for strategy update")
            raise ValueError("Config not found")

        config.strategy = strategy
        if strategy == WarmingStrategyEnum.CUSTOM.value:
            config.custom_schedule = custom_schedule

        try:
            db.commit()
            db.refresh(config)
            logger.info(f"✓ Strategy updated for config {config_id}")
        except Exception as e:
            db.rollback()
            logger.error(f"[ERROR] Failed to update strategy for config {config_id}: {e}")
            raise ValueError(f"Failed to update strategy: {e}")

        return config

    @staticmethod
    def pause_warming(db: Session, config_id: int) -> EmailWarmingConfig:
        """Pause the warming campaign"""
        logger.info(f"⏸️  Pausing warming for config {config_id}")
        config = db.query(EmailWarmingConfig).get(config_id)

        if config:
            config.status = WarmingStatusEnum.PAUSED.value
            try:
                db.commit()
                db.refresh(config)
                logger.info(f"✓ Warming paused for config {config_id}")
            except Exception as e:
                db.rollback()
                logger.error(f"[ERROR] Failed to pause warming for config {config_id}: {e}")
        else:
            logger.warning(f"⚠️  Config {config_id} not found for pausing")

        return config

    @staticmethod
    def resume_warming(db: Session, config_id: int) -> EmailWarmingConfig:
        """Resume the warming campaign"""
        logger.info(f"▶️  Resuming warming for config {config_id}")
        config = db.query(EmailWarmingConfig).get(config_id)

        if config:
            config.status = WarmingStatusEnum.ACTIVE.value
            try:
                db.commit()
                db.refresh(config)
                logger.info(f"✓ Warming resumed for config {config_id}")
            except Exception as e:
                db.rollback()
                logger.error(f"[ERROR] Failed to resume warming for config {config_id}: {e}")
        else:
            logger.warning(f"⚠️  Config {config_id} not found for resuming")

        return config

    @staticmethod
    def get_warming_progress(db: Session, candidate_id: int) -> Dict:
        """Get detailed warming progress"""
        config = db.query(EmailWarmingConfig).filter(
            EmailWarmingConfig.candidate_id == candidate_id
        ).first()

        if not config:
            return {
                "enabled": False,
                "status": "not_configured"
            }

        daily_limit = EmailWarmingService.get_daily_limit(config)
        max_day = EmailWarmingService.get_max_day_for_strategy(config.strategy, config.custom_schedule)

        return {
            "enabled": True,
            "status": config.status,
            "strategy": config.strategy,
            "current_day": config.current_day,
            "max_day": max_day,
            "progress_percentage": int((config.current_day / max_day) * 100) if max_day > 0 else 0,
            "daily_limit": daily_limit,
            "emails_sent_today": config.emails_sent_today or 0,
            "remaining_today": max(0, daily_limit - (config.emails_sent_today or 0)),
            "total_emails_sent": config.total_emails_sent or 0,
            "success_rate": round(config.success_rate or 0, 2),
            "bounce_rate": round(config.bounce_rate or 0, 2),
            "start_date": config.start_date.isoformat() if config.start_date else None,
            "completion_date": config.completion_date.isoformat() if config.completion_date else None
        }

    # --- Warmup subject/body pools for natural-sounding self-warmup emails ---
    _WARMUP_SUBJECTS = [
        "Quick question",
        "Following up",
        "Checking in",
        "Just a heads up",
        "Hey, quick note",
        "Touching base",
        "Brief update",
        "One small thing",
        "Hope you're well",
        "Friendly hello",
    ]

    _WARMUP_BODIES = [
        "Hi there,\n\nJust wanted to check in and see how things are going. Let me know if you need anything.\n\nBest regards",
        "Hey,\n\nHope your week is going well! Wanted to touch base quickly. Talk soon.\n\nCheers",
        "Hi,\n\nSending a quick note to say hello. Hope everything is great on your end.\n\nAll the best",
        "Hello,\n\nJust a friendly follow-up. Let me know if there is anything I can help with.\n\nThanks",
        "Hi,\n\nHope you are having a great day. Just checking in briefly. No rush on a reply!\n\nWarm regards",
        "Hey there,\n\nQuick note to keep in touch. Looking forward to connecting soon.\n\nBest",
        "Hi,\n\nJust dropping a line to say hello. Hope all is well with you.\n\nTake care",
        "Hello,\n\nWanted to reach out and see how things are going. Always happy to chat.\n\nBest wishes",
    ]

    @classmethod
    def send_warmup_emails(cls) -> Dict:
        """
        Background task: send warmup emails for all active warming configs.

        For each active config that still has remaining daily quota, sends a
        simple conversational self-warmup email (to the candidate's own address)
        and records the result.

        Returns a summary dict of what was processed.
        """
        from app.core.database import SessionLocal
        from app.models.candidate import Candidate
        from app.services.email_service import EmailService

        results = {"processed": 0, "sent": 0, "failed": 0, "skipped": 0, "errors": []}

        db = SessionLocal()
        try:
            # Get all active warming configs
            configs: List[EmailWarmingConfig] = db.query(EmailWarmingConfig).filter(
                EmailWarmingConfig.status == WarmingStatusEnum.ACTIVE.value,
                EmailWarmingConfig.deleted_at.is_(None)
            ).all()

            for config in configs:
                results["processed"] += 1

                try:
                    # Reset daily counter if needed
                    cls.check_and_reset_daily_counter(db, config)

                    # Check remaining quota
                    daily_limit = cls.get_daily_limit(config)
                    if (config.emails_sent_today or 0) >= daily_limit:
                        logger.debug(f"Warmup skip config {config.id}: daily limit reached ({daily_limit})")
                        results["skipped"] += 1
                        continue

                    # Get the candidate for SMTP credentials
                    candidate = db.query(Candidate).filter(
                        Candidate.id == config.candidate_id
                    ).first()
                    if not candidate:
                        logger.warning(f"Warmup skip config {config.id}: candidate not found")
                        results["skipped"] += 1
                        continue

                    # Build a natural-sounding warmup email
                    subject = random.choice(cls._WARMUP_SUBJECTS)
                    body_text = random.choice(cls._WARMUP_BODIES)
                    body_html = body_text.replace("\n", "<br>")

                    # Self-warmup: send to the candidate's own email address
                    email_service = EmailService(db)
                    try:
                        email_service.send_email(
                            candidate=candidate,
                            to_email=candidate.email_account,
                            subject=subject,
                            body_html=body_html,
                            body_text=body_text,
                        )
                        # Record successful send
                        cls.record_email_sent(db, config.candidate_id, success=True, bounced=False)
                        results["sent"] += 1
                        logger.info(f"Warmup email sent for config {config.id} (candidate {config.candidate_id})")
                    except Exception as send_err:
                        # Record failed send
                        cls.record_email_sent(db, config.candidate_id, success=False, bounced=False)
                        results["failed"] += 1
                        results["errors"].append({
                            "config_id": config.id,
                            "error": str(send_err)
                        })
                        logger.error(f"Warmup email failed for config {config.id}: {send_err}")

                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append({"config_id": config.id, "error": str(e)})
                    logger.error(f"Error processing warmup config {config.id}: {e}")

        except Exception as e:
            logger.error(f"send_warmup_emails fatal error: {e}")
            results["errors"].append({"error": str(e)})
        finally:
            db.close()

        logger.info(f"Warmup email run complete: {results}")
        return results
