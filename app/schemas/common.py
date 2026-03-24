"""Common Schemas and Validators

This module contains reusable schemas and validators used across the application.
"""
from pydantic import BaseModel, field_validator, model_validator
from typing import Generic, TypeVar, List, Optional, Dict, Any
import json


DataT = TypeVar("DataT")


class PaginatedResponse(BaseModel, Generic[DataT]):
    """Paginated response wrapper for list endpoints."""
    items: List[DataT]
    total: int
    page: int
    page_size: int


class StatusResponse(BaseModel):
    """Simple status response for operations."""
    success: bool
    message: str


# ============================================
# JSON Field Validators
# ============================================

class SkillsValidator:
    """Validator for skills JSON field."""

    @classmethod
    def validate_skills(cls, v: Any) -> List[str]:
        """
        Validate and normalize skills field.

        Accepts:
        - None -> []
        - List of strings -> validated list
        - JSON string -> parsed and validated
        - Single string -> wrapped in list
        """
        if v is None:
            return []

        # Handle JSON string
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                # Treat as single skill
                return [v.strip()] if v.strip() else []

        # Must be a list at this point
        if not isinstance(v, list):
            raise ValueError("Skills must be a list of strings")

        # Validate and clean each skill
        cleaned_skills = []
        for skill in v:
            if skill is None:
                continue
            skill_str = str(skill).strip()
            if skill_str:
                # Validate skill length
                if len(skill_str) > 100:
                    raise ValueError(f"Skill '{skill_str[:20]}...' exceeds 100 characters")
                cleaned_skills.append(skill_str)

        # Check total count
        if len(cleaned_skills) > 50:
            raise ValueError("Maximum 50 skills allowed")

        return cleaned_skills


class WarmingScheduleValidator:
    """Validator for email warming custom_schedule field."""

    @classmethod
    def validate_schedule(cls, v: Any) -> Optional[Dict[int, int]]:
        """
        Validate warming schedule.

        Format: {day_number: email_limit}
        Example: {1: 5, 2: 10, 3: 15, ...}

        Rules:
        - Days must be 1-60
        - Limits must be 1-500
        - Should be generally increasing (warning only)
        """
        if v is None:
            return None

        # Handle JSON string
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON for custom_schedule")

        if not isinstance(v, dict):
            raise ValueError("custom_schedule must be a dictionary")

        if not v:
            return None

        validated = {}
        for day, limit in v.items():
            # Convert day to int
            try:
                day_int = int(day)
            except (ValueError, TypeError):
                raise ValueError(f"Day '{day}' must be an integer")

            # Validate day range
            if day_int < 1 or day_int > 60:
                raise ValueError(f"Day {day_int} must be between 1 and 60")

            # Convert limit to int
            try:
                limit_int = int(limit)
            except (ValueError, TypeError):
                raise ValueError(f"Limit for day {day_int} must be an integer")

            # Validate limit range
            if limit_int < 1 or limit_int > 500:
                raise ValueError(f"Limit {limit_int} for day {day_int} must be between 1 and 500")

            validated[day_int] = limit_int

        return validated


class RateLimitValidator:
    """Validator for rate limit values."""

    @classmethod
    def validate_limit(cls, value: int, limit_type: str, min_val: int, max_val: int) -> int:
        """Validate a rate limit value."""
        if value < min_val or value > max_val:
            raise ValueError(f"{limit_type} must be between {min_val} and {max_val}")
        return value

    @classmethod
    def validate_daily_limit(cls, v: int) -> int:
        """Validate daily limit (1-5000)."""
        return cls.validate_limit(v, "Daily limit", 1, 5000)

    @classmethod
    def validate_hourly_limit(cls, v: int) -> int:
        """Validate hourly limit (1-1000)."""
        return cls.validate_limit(v, "Hourly limit", 1, 1000)

    @classmethod
    def validate_weekly_limit(cls, v: Optional[int]) -> Optional[int]:
        """Validate weekly limit (1-35000 or None)."""
        if v is None:
            return None
        return cls.validate_limit(v, "Weekly limit", 1, 35000)

    @classmethod
    def validate_monthly_limit(cls, v: Optional[int]) -> Optional[int]:
        """Validate monthly limit (1-150000 or None)."""
        if v is None:
            return None
        return cls.validate_limit(v, "Monthly limit", 1, 150000)


# ============================================
# Email Warming Schemas
# ============================================

class WarmingConfigCreate(BaseModel):
    """Schema for creating email warming configuration."""
    strategy: str = "moderate"
    custom_schedule: Optional[Dict[int, int]] = None
    auto_progress: bool = True

    @field_validator("strategy")
    @classmethod
    def validate_strategy(cls, v: str) -> str:
        valid_strategies = ["conservative", "moderate", "aggressive", "custom"]
        if v.lower() not in valid_strategies:
            raise ValueError(f"Strategy must be one of: {', '.join(valid_strategies)}")
        return v.lower()

    @field_validator("custom_schedule")
    @classmethod
    def validate_custom_schedule(cls, v: Any) -> Optional[Dict[int, int]]:
        return WarmingScheduleValidator.validate_schedule(v)


class WarmingConfigUpdate(BaseModel):
    """Schema for updating email warming configuration."""
    strategy: Optional[str] = None
    custom_schedule: Optional[Dict[int, int]] = None
    auto_progress: Optional[bool] = None

    @field_validator("strategy")
    @classmethod
    def validate_strategy(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        valid_strategies = ["conservative", "moderate", "aggressive", "custom"]
        if v.lower() not in valid_strategies:
            raise ValueError(f"Strategy must be one of: {', '.join(valid_strategies)}")
        return v.lower()

    @field_validator("custom_schedule")
    @classmethod
    def validate_custom_schedule(cls, v: Any) -> Optional[Dict[int, int]]:
        return WarmingScheduleValidator.validate_schedule(v)


# ============================================
# Rate Limiting Schemas
# ============================================

class RateLimitConfigCreate(BaseModel):
    """Schema for creating rate limit configuration."""
    preset: str = "moderate"
    daily_limit: Optional[int] = None
    hourly_limit: Optional[int] = None
    weekly_limit: Optional[int] = None
    monthly_limit: Optional[int] = None

    @field_validator("preset")
    @classmethod
    def validate_preset(cls, v: str) -> str:
        valid_presets = [
            "conservative", "moderate", "aggressive",
            "gmail_free", "gmail_workspace", "outlook", "custom"
        ]
        if v.lower() not in valid_presets:
            raise ValueError(f"Preset must be one of: {', '.join(valid_presets)}")
        return v.lower()

    @field_validator("daily_limit")
    @classmethod
    def validate_daily(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return None
        return RateLimitValidator.validate_daily_limit(v)

    @field_validator("hourly_limit")
    @classmethod
    def validate_hourly(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return None
        return RateLimitValidator.validate_hourly_limit(v)

    @field_validator("weekly_limit")
    @classmethod
    def validate_weekly(cls, v: Optional[int]) -> Optional[int]:
        return RateLimitValidator.validate_weekly_limit(v)

    @field_validator("monthly_limit")
    @classmethod
    def validate_monthly(cls, v: Optional[int]) -> Optional[int]:
        return RateLimitValidator.validate_monthly_limit(v)

    @model_validator(mode="after")
    def validate_limits_consistency(self) -> "RateLimitConfigCreate":
        """Ensure hourly <= daily <= weekly <= monthly."""
        daily = self.daily_limit
        hourly = self.hourly_limit

        if daily is not None and hourly is not None:
            if hourly > daily:
                raise ValueError("Hourly limit cannot exceed daily limit")

        return self


class RateLimitConfigUpdate(BaseModel):
    """Schema for updating rate limit configuration."""
    preset: Optional[str] = None
    daily_limit: Optional[int] = None
    hourly_limit: Optional[int] = None
    weekly_limit: Optional[int] = None
    monthly_limit: Optional[int] = None
    enabled: Optional[bool] = None

    @field_validator("preset")
    @classmethod
    def validate_preset(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        valid_presets = [
            "conservative", "moderate", "aggressive",
            "gmail_free", "gmail_workspace", "outlook", "custom"
        ]
        if v.lower() not in valid_presets:
            raise ValueError(f"Preset must be one of: {', '.join(valid_presets)}")
        return v.lower()

    @field_validator("daily_limit")
    @classmethod
    def validate_daily(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return None
        return RateLimitValidator.validate_daily_limit(v)

    @field_validator("hourly_limit")
    @classmethod
    def validate_hourly(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return None
        return RateLimitValidator.validate_hourly_limit(v)
