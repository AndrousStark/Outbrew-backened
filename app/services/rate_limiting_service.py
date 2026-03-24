"""Rate Limiting Service

Handles rate limiting logic, quota tracking, and usage monitoring.
"""

from datetime import datetime, timedelta, timezone
from typing import Tuple, Dict, Optional
from sqlalchemy.orm import Session

from app.models.rate_limiting import (
    RateLimitConfig,
    RateLimitUsageLog,
    RateLimitPresetEnum,
    RateLimitPeriodEnum,
    RATE_LIMIT_PRESETS
)
from app.core.logger import rate_limit_logger as logger


class RateLimitingService:
    """Service for managing rate limiting"""

    @staticmethod
    def create_config(
        db: Session,
        candidate_id: int,
        preset: str = RateLimitPresetEnum.MODERATE.value,
        daily_limit: Optional[int] = None,
        hourly_limit: Optional[int] = None
    ) -> RateLimitConfig:
        """Create a new rate limit configuration"""
        logger.info(f"[RateLimiting] Creating config for candidate {candidate_id} with preset: {preset}")

        # Check if config already exists
        existing = db.query(RateLimitConfig).filter(
            RateLimitConfig.candidate_id == candidate_id
        ).first()

        if existing:
            logger.debug(f"[RateLimiting] Config already exists for candidate {candidate_id}")
            return existing

        # Get preset values
        preset_values = RATE_LIMIT_PRESETS.get(RateLimitPresetEnum(preset), {})

        config = RateLimitConfig(
            candidate_id=candidate_id,
            preset=preset,
            daily_limit=daily_limit or preset_values.get("daily_limit", 100),
            hourly_limit=hourly_limit or preset_values.get("hourly_limit", 25),
            last_hourly_reset=datetime.now(timezone.utc),
            last_daily_reset=datetime.now(timezone.utc)
        )

        try:
            db.add(config)
            db.commit()
            db.refresh(config)
            logger.info(f"[RateLimiting] Created config ID {config.id}: Daily={config.daily_limit}, Hourly={config.hourly_limit}")
        except Exception as e:
            db.rollback()
            logger.error(f"[RateLimiting] Failed to create config for candidate {candidate_id}: {e}")
            raise ValueError(f"Failed to create rate limit config: {e}")

        return config

    @staticmethod
    def can_send_email(db: Session, candidate_id: int) -> Tuple[bool, str, Dict]:
        """
        Check if an email can be sent based on rate limits

        Returns:
            (can_send, reason, quota_info)
        """
        logger.debug(f"🔍 Checking rate limits for candidate {candidate_id}")

        # Use FOR UPDATE to lock the row and prevent race conditions
        config = db.query(RateLimitConfig).filter(
            RateLimitConfig.candidate_id == candidate_id
        ).with_for_update(nowait=False).first()

        if not config:
            logger.debug(f"   No rate limit config found - allowing send")
            return True, "No rate limit configured", {
                "daily_remaining": 999999,
                "hourly_remaining": 999999
            }

        if not config.enabled:
            logger.debug(f"   Rate limiting DISABLED - allowing send")
            return True, "Rate limiting disabled", {
                "daily_remaining": 999999,
                "hourly_remaining": 999999
            }

        logger.debug(f"   Config: {config.preset}, Daily: {config.daily_limit}, Hourly: {config.hourly_limit}")

        # Check and reset counters if needed
        RateLimitingService.check_and_reset_counters(db, config)

        # Check hourly limit
        hourly_remaining = config.hourly_limit - config.emails_sent_this_hour
        logger.debug(f"   Hourly: {config.emails_sent_this_hour}/{config.hourly_limit}, Remaining: {hourly_remaining}")

        if config.emails_sent_this_hour >= config.hourly_limit:
            logger.warning(f"🚫 HOURLY limit REACHED for candidate {candidate_id}: {config.hourly_limit} emails/hour")
            return False, f"Hourly rate limit reached ({config.hourly_limit} emails/hour)", {
                "daily_remaining": config.daily_limit - config.emails_sent_today,
                "hourly_remaining": 0,
                "next_hourly_reset": config.last_hourly_reset + timedelta(hours=1)
            }

        # Check daily limit
        daily_remaining = config.daily_limit - config.emails_sent_today
        logger.debug(f"   Daily: {config.emails_sent_today}/{config.daily_limit}, Remaining: {daily_remaining}")

        if config.emails_sent_today >= config.daily_limit:
            logger.warning(f"🚫 DAILY limit REACHED for candidate {candidate_id}: {config.daily_limit} emails/day")
            return False, f"Daily rate limit reached ({config.daily_limit} emails/day)", {
                "daily_remaining": 0,
                "hourly_remaining": hourly_remaining,
                "next_daily_reset": config.last_daily_reset + timedelta(days=1)
            }

        # Check weekly limit if set
        if config.weekly_limit and config.emails_sent_this_week >= config.weekly_limit:
            return False, f"Weekly rate limit reached ({config.weekly_limit} emails/week)", {
                "daily_remaining": daily_remaining,
                "hourly_remaining": hourly_remaining,
                "weekly_remaining": 0
            }

        # Check monthly limit if set
        if config.monthly_limit and config.emails_sent_this_month >= config.monthly_limit:
            return False, f"Monthly rate limit reached ({config.monthly_limit} emails/month)", {
                "daily_remaining": daily_remaining,
                "hourly_remaining": hourly_remaining,
                "monthly_remaining": 0
            }

        # Check if approaching limit (warning threshold)
        warnings = []

        daily_percentage = (config.emails_sent_today / config.daily_limit) * 100
        if daily_percentage >= config.warning_threshold_daily:
            warnings.append(f"Daily limit {daily_percentage:.0f}% used")

        hourly_percentage = (config.emails_sent_this_hour / config.hourly_limit) * 100
        if hourly_percentage >= config.warning_threshold_hourly:
            warnings.append(f"Hourly limit {hourly_percentage:.0f}% used")

        return True, "OK", {
            "daily_remaining": daily_remaining,
            "hourly_remaining": hourly_remaining,
            "warnings": warnings
        }

    @staticmethod
    def record_email_sent(db: Session, candidate_id: int) -> None:
        """Record that an email was sent"""
        logger.info(f"📧 Recording email sent for rate limiting (candidate {candidate_id})")

        # Use FOR UPDATE to lock the row and prevent race conditions
        config = db.query(RateLimitConfig).filter(
            RateLimitConfig.candidate_id == candidate_id
        ).with_for_update(nowait=False).first()

        if not config:
            logger.debug(f"   No rate limit config found - skipping record")
            return

        # Increment all counters
        config.emails_sent_today += 1
        config.emails_sent_this_hour += 1
        config.emails_sent_this_week += 1
        config.emails_sent_this_month += 1

        logger.debug(f"   Updated: Hour={config.emails_sent_this_hour}, Day={config.emails_sent_today}, Week={config.emails_sent_this_week}, Month={config.emails_sent_this_month}")

        try:
            db.commit()
            logger.debug(f"   Rate limit counters saved for candidate {candidate_id}")
        except Exception as e:
            db.rollback()
            logger.error(f"[RateLimiting] Failed to record email sent for candidate {candidate_id}: {e}")

    @staticmethod
    def check_and_reset_counters(db: Session, config: RateLimitConfig) -> None:
        """Check and reset counters if time periods have elapsed"""
        now = datetime.now(timezone.utc)

        # Hourly reset
        if config.last_hourly_reset:
            hours_elapsed = (now - config.last_hourly_reset).total_seconds() / 3600
            if hours_elapsed >= 1:
                # Log usage before reset
                RateLimitingService.log_usage(
                    db, config, RateLimitPeriodEnum.HOURLY,
                    config.last_hourly_reset, now,
                    config.hourly_limit, config.emails_sent_this_hour
                )

                config.emails_sent_this_hour = 0
                config.last_hourly_reset = now

        # Daily reset
        if config.last_daily_reset:
            days_elapsed = (now - config.last_daily_reset).days
            if days_elapsed >= 1:
                # Log usage before reset
                RateLimitingService.log_usage(
                    db, config, RateLimitPeriodEnum.DAILY,
                    config.last_daily_reset, now,
                    config.daily_limit, config.emails_sent_today
                )

                config.emails_sent_today = 0
                config.last_daily_reset = now

        # Weekly reset - handle None case
        if config.last_weekly_reset is not None:
            days_elapsed = (now - config.last_weekly_reset).days
            if days_elapsed >= 7:
                if config.weekly_limit and config.weekly_limit > 0:
                    RateLimitingService.log_usage(
                        db, config, RateLimitPeriodEnum.WEEKLY,
                        config.last_weekly_reset, now,
                        config.weekly_limit, config.emails_sent_this_week
                    )

                config.emails_sent_this_week = 0
                config.last_weekly_reset = now
        else:
            # Initialize weekly reset if not set
            config.last_weekly_reset = now

        # Monthly reset (approximate - 30 days) - handle None case
        if config.last_monthly_reset is not None:
            days_elapsed = (now - config.last_monthly_reset).days
            if days_elapsed >= 30:
                if config.monthly_limit and config.monthly_limit > 0:
                    RateLimitingService.log_usage(
                        db, config, RateLimitPeriodEnum.MONTHLY,
                        config.last_monthly_reset, now,
                        config.monthly_limit, config.emails_sent_this_month
                    )

                config.emails_sent_this_month = 0
                config.last_monthly_reset = now
        else:
            # Initialize monthly reset if not set
            config.last_monthly_reset = now

        try:
            db.commit()
            logger.debug(f"[RateLimiting] Counter reset completed for config {config.id}")
        except Exception as e:
            db.rollback()
            logger.error(f"[RateLimiting] Failed to reset counters for config {config.id}: {e}")

    @staticmethod
    def log_usage(
        db: Session,
        config: RateLimitConfig,
        period_type: str,
        period_start: datetime,
        period_end: datetime,
        limit: int,
        emails_sent: int
    ) -> None:
        """Log usage statistics"""
        logger.debug(f"[RateLimiting] Logging usage for config {config.id}, period: {period_type}")
        usage_percentage = int((emails_sent / limit) * 100) if limit > 0 else 0

        log = RateLimitUsageLog(
            config_id=config.id,
            period_type=period_type,
            period_start=period_start,
            period_end=period_end,
            limit_value=limit,
            emails_sent=emails_sent,
            limit_reached=(emails_sent >= limit),
            limit_exceeded=(emails_sent > limit),
            usage_percentage=usage_percentage
        )

        try:
            db.add(log)
            db.commit()
            logger.debug(f"[RateLimiting] Usage logged: {emails_sent}/{limit} ({usage_percentage}%) for {period_type}")
        except Exception as e:
            db.rollback()
            logger.error(f"[RateLimiting] Failed to log usage for config {config.id}: {e}")

    @staticmethod
    def update_preset(
        db: Session,
        config_id: int,
        preset: str
    ) -> RateLimitConfig:
        """Update rate limit preset"""
        logger.info(f"[RateLimiting] Updating config {config_id} to preset: {preset}")
        config = db.query(RateLimitConfig).get(config_id)

        if not config:
            logger.error(f"[RateLimiting] Config {config_id} not found")
            raise ValueError("Config not found")

        preset_values = RATE_LIMIT_PRESETS.get(RateLimitPresetEnum(preset), {})

        config.preset = preset
        config.daily_limit = preset_values.get("daily_limit", 100)
        config.hourly_limit = preset_values.get("hourly_limit", 25)

        try:
            db.commit()
            db.refresh(config)
            logger.info(f"[RateLimiting] Updated config {config_id}: Daily={config.daily_limit}, Hourly={config.hourly_limit}")
        except Exception as e:
            db.rollback()
            logger.error(f"[RateLimiting] Failed to update preset for config {config_id}: {e}")
            raise ValueError(f"Failed to update rate limit preset: {e}")

        return config

    @staticmethod
    def update_custom_limits(
        db: Session,
        config_id: int,
        daily_limit: Optional[int] = None,
        hourly_limit: Optional[int] = None,
        weekly_limit: Optional[int] = None,
        monthly_limit: Optional[int] = None
    ) -> RateLimitConfig:
        """Update custom rate limits"""
        logger.info(f"[RateLimiting] Updating custom limits for config {config_id}")
        config = db.query(RateLimitConfig).get(config_id)

        if not config:
            logger.error(f"[RateLimiting] Config {config_id} not found")
            raise ValueError("Config not found")

        config.preset = RateLimitPresetEnum.CUSTOM.value

        if daily_limit is not None:
            config.daily_limit = daily_limit
        if hourly_limit is not None:
            config.hourly_limit = hourly_limit
        if weekly_limit is not None:
            config.weekly_limit = weekly_limit
        if monthly_limit is not None:
            config.monthly_limit = monthly_limit

        try:
            db.commit()
            db.refresh(config)
            logger.info(f"[RateLimiting] Custom limits updated: Daily={config.daily_limit}, Hourly={config.hourly_limit}, Weekly={config.weekly_limit}, Monthly={config.monthly_limit}")
        except Exception as e:
            db.rollback()
            logger.error(f"[RateLimiting] Failed to update custom limits for config {config_id}: {e}")
            raise ValueError(f"Failed to update custom rate limits: {e}")

        return config

    @staticmethod
    def get_usage_stats(db: Session, candidate_id: int) -> Dict:
        """Get current usage statistics"""
        logger.debug(f"[RateLimiting] Getting usage stats for candidate {candidate_id}")
        config = db.query(RateLimitConfig).filter(
            RateLimitConfig.candidate_id == candidate_id
        ).first()

        if not config:
            logger.debug(f"[RateLimiting] No config found for candidate {candidate_id}")
            return {
                "enabled": False,
                "status": "not_configured"
            }

        # Calculate percentages
        daily_percentage = int((config.emails_sent_today / config.daily_limit) * 100) if config.daily_limit > 0 else 0
        hourly_percentage = int((config.emails_sent_this_hour / config.hourly_limit) * 100) if config.hourly_limit > 0 else 0

        return {
            "enabled": config.enabled,
            "preset": config.preset,
            "limits": {
                "daily": config.daily_limit,
                "hourly": config.hourly_limit,
                "weekly": config.weekly_limit,
                "monthly": config.monthly_limit
            },
            "usage": {
                "today": config.emails_sent_today,
                "this_hour": config.emails_sent_this_hour,
                "this_week": config.emails_sent_this_week,
                "this_month": config.emails_sent_this_month
            },
            "remaining": {
                "daily": max(0, config.daily_limit - config.emails_sent_today),
                "hourly": max(0, config.hourly_limit - config.emails_sent_this_hour),
                "weekly": max(0, config.weekly_limit - config.emails_sent_this_week) if config.weekly_limit else None,
                "monthly": max(0, config.monthly_limit - config.emails_sent_this_month) if config.monthly_limit else None
            },
            "percentage_used": {
                "daily": daily_percentage,
                "hourly": hourly_percentage
            },
            "next_reset": {
                "hourly": (config.last_hourly_reset + timedelta(hours=1)).isoformat() if config.last_hourly_reset else None,
                "daily": (config.last_daily_reset + timedelta(days=1)).isoformat() if config.last_daily_reset else None
            }
        }
