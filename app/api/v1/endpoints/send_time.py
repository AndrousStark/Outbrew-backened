"""
Send Time Optimization API Endpoints

Provides endpoints for:
- Getting optimal send times for emails
- Scheduling emails for optimal times
- Managing scheduled emails
- User preferences for send time optimization
"""

from typing import List, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.api.dependencies import get_db
from app.core.auth import get_current_candidate
from app.models import Candidate, Application
from app.models.scheduled_email import (
    ScheduledEmail,
    SendTimePreference,
    ScheduledEmailStatus
)
from app.services.send_time_optimizer import (
    SendTimeOptimizer,
    Industry,
    SendTimeStrategy,
    OPTIMAL_SEND_TIMES,
    TIMEZONE_MAPPINGS,
    COUNTRY_SEND_TIMES
)
from app.tasks.scheduler import ScheduledEmailEngine


router = APIRouter(tags=["Send Time Optimization"])


# ============== Pydantic Schemas ==============

class OptimalTimeRequest(BaseModel):
    """Request for optimal send time calculation."""
    industry: str = Field(default="default", description="Target industry")
    recipient_country: Optional[str] = Field(default=None, description="Recipient's country")
    recipient_timezone: Optional[str] = Field(default=None, description="Explicit timezone")


class OptimalTimeResponse(BaseModel):
    """Response with optimal send time."""
    send_at: datetime
    send_at_local: str
    day_name: str
    hour: int
    minute: int
    timezone: str
    reason: str
    expected_boost: str
    is_now_optimal: bool
    wait_hours: float
    industry: str
    strategy: str


class ScheduleEmailRequest(BaseModel):
    """Request to schedule an email for optimal time."""
    application_id: int
    industry: Optional[str] = "default"
    recipient_country: Optional[str] = None
    use_optimal_time: bool = True
    custom_schedule_time: Optional[datetime] = None
    send_immediately_if_optimal: bool = True


class ScheduledEmailResponse(BaseModel):
    """Response with scheduled email details."""
    id: int
    application_id: int
    scheduled_for: datetime
    timezone: str
    industry: Optional[str]
    expected_boost: Optional[str]
    reason: Optional[str]
    status: str
    created_at: datetime

    class Config:
        from_attributes = True


class PreferenceUpdate(BaseModel):
    """User preferences for send time optimization."""
    default_industry: Optional[str] = None
    default_timezone: Optional[str] = None
    auto_schedule_enabled: Optional[bool] = None
    tolerance_hours: Optional[int] = None
    prefer_morning: Optional[bool] = None
    prefer_afternoon: Optional[bool] = None
    avoid_mondays: Optional[bool] = None
    avoid_fridays: Optional[bool] = None
    use_custom_schedule: Optional[bool] = None
    custom_days: Optional[List[int]] = None
    custom_hours: Optional[List[int]] = None


class IndustryInfo(BaseModel):
    """Industry information for send time optimization."""
    value: str
    label: str
    best_days: List[str]
    best_hours: List[int]
    reason: str


class WeeklySlot(BaseModel):
    """A time slot in the weekly schedule."""
    datetime: datetime
    datetime_local: str
    day: str
    date: str
    time: str
    hour: int
    is_primary: bool
    expected_boost: str
    timezone: str


class CountryInfo(BaseModel):
    """Country information for send time optimization."""
    name: str
    flag: str
    timezone: str
    best_days: List[str]
    primary_hours: List[int]
    secondary_hours: List[int]
    avoid_hours: List[int]
    lunch_time: str
    work_hours: str
    work_culture: str
    email_culture: str
    best_days_note: str
    expected_boost: str
    response_time: str


class CountryOptimalTimeRequest(BaseModel):
    """Request for country-specific optimal send time."""
    country: str = Field(..., description="Target country name")
    industry: str = Field(default="default", description="Target industry (optional fallback)")


class CountryOptimalTimeResponse(BaseModel):
    """Response with country-specific optimal send time."""
    send_at: datetime
    send_at_local: str
    day_name: str
    hour: int
    minute: int
    timezone: str
    country: str
    flag: str
    is_now_optimal: bool
    wait_hours: float
    expected_boost: str
    is_primary_hour: Optional[bool] = None
    work_culture: str
    email_culture: str
    lunch_time: str
    work_hours: str
    best_days_note: str
    response_time: str
    primary_hours: Optional[List[int]] = None
    secondary_hours: Optional[List[int]] = None
    avoid_hours: Optional[List[int]] = None
    best_days: Optional[List[str]] = None


class CountryWeeklySlot(BaseModel):
    """A time slot in the country-specific weekly schedule."""
    datetime: datetime
    datetime_local: str
    day: str
    date: str
    time: str
    hour: int
    is_primary: bool
    expected_boost: str
    timezone: str
    slot_type: Optional[str] = None


# ============== Endpoints ==============

@router.get("/industries", response_model=List[IndustryInfo])
def get_industries():
    """
    Get all supported industries with their optimal send times.

    Returns information about each industry including:
    - Best days to send
    - Best hours to send
    - Reason for the recommendation
    """
    return SendTimeOptimizer.get_all_industries()


@router.get("/timezones")
def get_timezones():
    """
    Get all supported country to timezone mappings.

    Returns a dictionary mapping country names to their timezone strings.
    """
    return {
        "timezones": TIMEZONE_MAPPINGS,
        "total": len(TIMEZONE_MAPPINGS)
    }


# ============== Country-Specific Endpoints ==============

@router.get("/countries", response_model=List[CountryInfo])
def get_countries():
    """
    Get all supported countries with detailed send time information.

    Returns comprehensive data for each country including:
    - Flag emoji and timezone
    - Best days and hours for sending
    - Work culture and email culture insights
    - Expected open rate boost
    - Response time expectations

    Countries supported: Germany, France, UK, USA, UAE, Dubai,
    Singapore, India, Australia, Switzerland, Ireland, Denmark,
    Poland, Luxembourg
    """
    return SendTimeOptimizer.get_all_countries()


@router.get("/countries/list")
def get_country_list():
    """
    Get a simplified list of supported countries.

    Returns just the country names and flags for dropdown/selection UI.
    """
    countries = SendTimeOptimizer.get_all_countries()
    return {
        "countries": [
            {
                "name": c["name"],
                "flag": c["flag"],
                "timezone": c["timezone"],
                "expected_boost": c["expected_boost"]
            }
            for c in countries
        ],
        "total": len(countries)
    }


@router.post("/countries/optimal", response_model=CountryOptimalTimeResponse)
def get_country_optimal_time(
    request: CountryOptimalTimeRequest,
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get optimal send time for a specific country.

    Uses research-backed country-specific data to calculate the best time
    to send emails based on:
    - Local work hours and culture
    - Typical email engagement patterns
    - Lunch breaks and avoid times
    - Weekend considerations (e.g., UAE has different weekend)

    **Country-specific insights:**
    - Germany/Switzerland: Sunday campaigns work well (DACH region)
    - France: Highest open rate in Europe (38.33%), long lunches
    - USA: 8PM evening peak has 59% open rate
    - UAE/Dubai: Weekend is Fri-Sat, business days Sun-Thu
    """
    optimizer = SendTimeOptimizer()
    result = optimizer.get_country_optimal_time(
        country=request.country,
        industry=request.industry
    )
    return CountryOptimalTimeResponse(**result)


@router.get("/countries/{country}/info")
def get_country_details(country: str):
    """
    Get detailed information about a specific country.

    Returns:
    - Work hours and lunch times
    - Work culture insights
    - Email engagement patterns
    - Best days and hours
    - Expected boost and response times
    """
    info = SendTimeOptimizer.get_country_info(country)
    if not info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Country '{country}' not found. Use /countries to see available countries."
        )
    return info


@router.get("/countries/{country}/schedule", response_model=List[CountryWeeklySlot])
def get_country_weekly_schedule(
    country: str,
    max_slots: int = Query(default=10, le=20, description="Maximum slots to return")
):
    """
    Get optimal send slots for the next 7 days for a specific country.

    Returns up to 20 time slots sorted by date, marked as primary or secondary.
    Primary hours typically have higher engagement rates.
    """
    optimizer = SendTimeOptimizer()

    # Check if country exists
    if country not in COUNTRY_SEND_TIMES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Country '{country}' not found. Use /countries to see available countries."
        )

    slots = optimizer.get_country_weekly_schedule(
        country=country,
        max_slots=max_slots
    )
    return [CountryWeeklySlot(**slot) for slot in slots]


@router.get("/countries/{country}/quick-check")
def country_quick_check(
    country: str,
    tolerance_hours: int = Query(default=2, description="Acceptable hours from optimal")
):
    """
    Quick check: should I send an email to this country now?

    Returns a simple recommendation with explanation.
    """
    if country not in COUNTRY_SEND_TIMES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Country '{country}' not found"
        )

    optimizer = SendTimeOptimizer()
    result = optimizer.get_country_optimal_time(country=country)

    if result["is_now_optimal"]:
        return {
            "should_send_now": True,
            "recommendation": "Send now!",
            "reason": f"Current time is optimal for {country}. {result['email_culture']}",
            "expected_boost": result["expected_boost"],
            "country": country,
            "flag": result["flag"]
        }

    if result["wait_hours"] <= tolerance_hours:
        return {
            "should_send_now": True,
            "recommendation": "Good to send",
            "reason": f"Within {tolerance_hours}h of optimal. Expected boost: {result['expected_boost']}",
            "expected_boost": result["expected_boost"],
            "country": country,
            "flag": result["flag"]
        }

    return {
        "should_send_now": False,
        "recommendation": "Wait for optimal time",
        "reason": f"Wait {result['wait_hours']}h for optimal time ({result['send_at_local']})",
        "expected_boost": result["expected_boost"],
        "optimal_time": result["send_at_local"],
        "wait_hours": result["wait_hours"],
        "country": country,
        "flag": result["flag"]
    }


@router.post("/optimal", response_model=OptimalTimeResponse)
def get_optimal_send_time(
    request: OptimalTimeRequest,
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Calculate the optimal time to send an email.

    Based on 2025 research data, this endpoint calculates when you
    should send your email for maximum open rates.

    **Key findings:**
    - Tuesday 10am is universally the best time
    - Tech companies: 10-11am, 2pm (after standup, before meetings)
    - Finance: 8-9am, 4pm (before market, after trading)
    - 8PM has 59% open rate (people checking at home)
    """
    optimizer = SendTimeOptimizer()

    result = optimizer.get_optimal_send_time(
        industry=request.industry,
        recipient_country=request.recipient_country,
        recipient_timezone=request.recipient_timezone
    )

    return OptimalTimeResponse(**result)


@router.get("/optimal/quick")
def quick_check_send_time(
    industry: str = Query(default="default", description="Target industry"),
    recipient_country: Optional[str] = Query(default=None, description="Recipient's country"),
    tolerance_hours: int = Query(default=2, description="Acceptable hours from optimal")
):
    """
    Quick check: should I send this email now?

    Returns a simple yes/no with reason.
    """
    optimizer = SendTimeOptimizer()
    should_send, reason = optimizer.should_send_now(
        industry=industry,
        recipient_country=recipient_country,
        tolerance_hours=tolerance_hours
    )

    return {
        "should_send_now": should_send,
        "reason": reason
    }


@router.get("/industry/{industry}", response_model=dict)
def get_industry_info(
    industry: str
):
    """
    Get detailed information about optimal times for a specific industry.

    Returns:
    - Best days to send
    - Best hours to send
    - Hours to avoid
    - Reason for recommendations
    - Expected boost range
    """
    optimizer = SendTimeOptimizer()
    return optimizer.get_industry_info(industry)


@router.get("/schedule/week", response_model=List[WeeklySlot])
def get_weekly_schedule(
    industry: str = Query(default="default", description="Target industry"),
    recipient_country: Optional[str] = Query(default=None, description="Recipient's country"),
    max_slots: int = Query(default=10, le=20, description="Maximum slots to return")
):
    """
    Get all optimal send slots for the next 7 days.

    Useful for planning batch sends or seeing the full schedule.
    Returns up to 20 time slots sorted by date.
    """
    optimizer = SendTimeOptimizer()
    slots = optimizer.get_schedule_for_week(
        industry=industry,
        recipient_country=recipient_country,
        max_slots=max_slots
    )

    return [WeeklySlot(**slot) for slot in slots]


@router.post("/schedule", response_model=ScheduledEmailResponse)
def schedule_email(
    request: ScheduleEmailRequest,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Schedule an email to be sent at the optimal time.

    If use_optimal_time is True, calculates the best time based on industry.
    If False, uses custom_schedule_time.

    The email will be automatically sent by the scheduler when the time arrives.
    """
    # Verify application exists and belongs to user
    application = db.query(Application).filter(
        Application.id == request.application_id,
        Application.candidate_id == current_user.id,
        Application.deleted_at.is_(None)
    ).first()

    if not application:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Application not found"
        )

    # Check if application is already scheduled or sent
    existing = db.query(ScheduledEmail).filter(
        ScheduledEmail.application_id == request.application_id,
        ScheduledEmail.status.in_([
            ScheduledEmailStatus.PENDING,
            ScheduledEmailStatus.PROCESSING
        ]),
        ScheduledEmail.deleted_at.is_(None)
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Application already has a pending scheduled send"
        )

    # Calculate optimal time or use custom time
    optimizer = SendTimeOptimizer()

    if request.use_optimal_time:
        # Get recipient country from application if not provided
        recipient_country = request.recipient_country or application.recruiter_country

        result = optimizer.get_optimal_send_time(
            industry=request.industry,
            recipient_country=recipient_country
        )

        scheduled_for = result["send_at"]
        timezone = result["timezone"]
        expected_boost = result["expected_boost"]
        reason = result["reason"]
        is_optimal = True

        # If now is optimal and user wants immediate send
        if result["is_now_optimal"] and request.send_immediately_if_optimal:
            # Instead of scheduling, just return info that now is optimal
            return ScheduledEmailResponse(
                id=0,
                application_id=request.application_id,
                scheduled_for=scheduled_for,
                timezone=timezone,
                industry=request.industry,
                expected_boost=expected_boost,
                reason="Current time is optimal - send now!",
                status="optimal_now",
                created_at=datetime.now(timezone.utc)
            )
    else:
        if not request.custom_schedule_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="custom_schedule_time is required when use_optimal_time is False"
            )

        scheduled_for = request.custom_schedule_time
        timezone = "UTC"
        expected_boost = None
        reason = "Custom scheduled time"
        is_optimal = False

    # Create scheduled email record
    scheduled_email = ScheduledEmail(
        application_id=request.application_id,
        candidate_id=current_user.id,
        scheduled_for=scheduled_for,
        timezone=timezone,
        industry=request.industry,
        recipient_country=request.recipient_country or application.recruiter_country,
        expected_boost=expected_boost,
        optimization_reason=reason,
        is_optimal_time=is_optimal,
        original_request_time=datetime.now(timezone.utc),
        send_immediately_if_optimal=request.send_immediately_if_optimal
    )

    try:
        db.add(scheduled_email)
        db.commit()
        db.refresh(scheduled_email)
        logger.info(f"[SendTime] Scheduled email {scheduled_email.id} for {scheduled_email.scheduled_for}")
    except Exception as e:
        db.rollback()
        logger.error(f"[SendTime] Failed to schedule email: {e}")
        raise HTTPException(status_code=500, detail="Failed to schedule email")

    return ScheduledEmailResponse(
        id=scheduled_email.id,
        application_id=scheduled_email.application_id,
        scheduled_for=scheduled_email.scheduled_for,
        timezone=scheduled_email.timezone,
        industry=scheduled_email.industry,
        expected_boost=scheduled_email.expected_boost,
        reason=scheduled_email.optimization_reason,
        status=scheduled_email.status.value,
        created_at=scheduled_email.created_at
    )


@router.get("/scheduled", response_model=List[ScheduledEmailResponse])
def get_scheduled_emails(
    status_filter: Optional[str] = Query(default=None, description="Filter by status"),
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get all scheduled emails for the current user.

    Optionally filter by status: pending, sent, cancelled, failed
    """
    query = db.query(ScheduledEmail).filter(
        ScheduledEmail.candidate_id == current_user.id,
        ScheduledEmail.deleted_at.is_(None)
    )

    if status_filter:
        try:
            status_enum = ScheduledEmailStatus(status_filter)
            query = query.filter(ScheduledEmail.status == status_enum)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status. Valid values: {[s.value for s in ScheduledEmailStatus]}"
            )

    scheduled_emails = query.order_by(ScheduledEmail.scheduled_for.asc()).all()

    return [
        ScheduledEmailResponse(
            id=se.id,
            application_id=se.application_id,
            scheduled_for=se.scheduled_for,
            timezone=se.timezone,
            industry=se.industry,
            expected_boost=se.expected_boost,
            reason=se.optimization_reason,
            status=se.status.value,
            created_at=se.created_at
        )
        for se in scheduled_emails
    ]


@router.get("/scheduled/{scheduled_id}", response_model=ScheduledEmailResponse)
def get_scheduled_email(
    scheduled_id: int,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get details of a specific scheduled email."""
    scheduled = db.query(ScheduledEmail).filter(
        ScheduledEmail.id == scheduled_id,
        ScheduledEmail.candidate_id == current_user.id,
        ScheduledEmail.deleted_at.is_(None)
    ).first()

    if not scheduled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Scheduled email not found"
        )

    return ScheduledEmailResponse(
        id=scheduled.id,
        application_id=scheduled.application_id,
        scheduled_for=scheduled.scheduled_for,
        timezone=scheduled.timezone,
        industry=scheduled.industry,
        expected_boost=scheduled.expected_boost,
        reason=scheduled.optimization_reason,
        status=scheduled.status.value,
        created_at=scheduled.created_at
    )


@router.delete("/scheduled/{scheduled_id}")
def cancel_scheduled_email(
    scheduled_id: int,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Cancel a scheduled email.

    Only pending emails can be cancelled.
    """
    scheduled = db.query(ScheduledEmail).filter(
        ScheduledEmail.id == scheduled_id,
        ScheduledEmail.candidate_id == current_user.id,
        ScheduledEmail.deleted_at.is_(None)
    ).first()

    if not scheduled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Scheduled email not found"
        )

    if not scheduled.can_cancel:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel email with status: {scheduled.status.value}"
        )

    scheduled.cancel()

    try:
        db.commit()
        logger.info(f"[SendTime] Cancelled scheduled email {scheduled_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[SendTime] Failed to cancel scheduled email {scheduled_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel scheduled email")

    return {
        "success": True,
        "message": "Scheduled email cancelled",
        "id": scheduled_id
    }


@router.put("/scheduled/{scheduled_id}/reschedule", response_model=ScheduledEmailResponse)
def reschedule_email(
    scheduled_id: int,
    new_time: Optional[datetime] = None,
    use_optimal: bool = True,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Reschedule a pending email to a new time.

    If use_optimal is True, recalculates optimal time.
    Otherwise, uses new_time.
    """
    scheduled = db.query(ScheduledEmail).filter(
        ScheduledEmail.id == scheduled_id,
        ScheduledEmail.candidate_id == current_user.id,
        ScheduledEmail.deleted_at.is_(None)
    ).first()

    if not scheduled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Scheduled email not found"
        )

    if scheduled.status != ScheduledEmailStatus.PENDING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Can only reschedule pending emails, current status: {scheduled.status.value}"
        )

    if use_optimal:
        optimizer = SendTimeOptimizer()
        result = optimizer.get_optimal_send_time(
            industry=scheduled.industry or "default",
            recipient_country=scheduled.recipient_country
        )
        scheduled.scheduled_for = result["send_at"]
        scheduled.expected_boost = result["expected_boost"]
        scheduled.optimization_reason = result["reason"]
        scheduled.is_optimal_time = True
    else:
        if not new_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="new_time is required when use_optimal is False"
            )
        scheduled.scheduled_for = new_time
        scheduled.is_optimal_time = False
        scheduled.optimization_reason = "Manually rescheduled"

    try:
        db.commit()
        db.refresh(scheduled)
        logger.info(f"[SendTime] Rescheduled email {scheduled.id} to {scheduled.scheduled_for}")
    except Exception as e:
        db.rollback()
        logger.error(f"[SendTime] Failed to reschedule email {scheduled.id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to reschedule email")

    return ScheduledEmailResponse(
        id=scheduled.id,
        application_id=scheduled.application_id,
        scheduled_for=scheduled.scheduled_for,
        timezone=scheduled.timezone,
        industry=scheduled.industry,
        expected_boost=scheduled.expected_boost,
        reason=scheduled.optimization_reason,
        status=scheduled.status.value,
        created_at=scheduled.created_at
    )


@router.get("/stats")
def get_send_time_stats(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """
    Get statistics about scheduled emails for the current user.

    Returns counts by status and other metrics.
    """
    # User-specific stats
    user_stats = {
        "pending": db.query(ScheduledEmail).filter(
            ScheduledEmail.candidate_id == current_user.id,
            ScheduledEmail.status == ScheduledEmailStatus.PENDING
        ).count(),
        "sent": db.query(ScheduledEmail).filter(
            ScheduledEmail.candidate_id == current_user.id,
            ScheduledEmail.status == ScheduledEmailStatus.SENT
        ).count(),
        "cancelled": db.query(ScheduledEmail).filter(
            ScheduledEmail.candidate_id == current_user.id,
            ScheduledEmail.status == ScheduledEmailStatus.CANCELLED
        ).count(),
        "failed": db.query(ScheduledEmail).filter(
            ScheduledEmail.candidate_id == current_user.id,
            ScheduledEmail.status == ScheduledEmailStatus.FAILED
        ).count()
    }

    # Global stats from scheduler
    global_stats = ScheduledEmailEngine.get_stats()

    return {
        "user_stats": user_stats,
        "system_stats": global_stats
    }


# ============== User Preferences ==============

@router.get("/preferences")
def get_preferences(
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Get user's send time optimization preferences."""
    pref = db.query(SendTimePreference).filter(
        SendTimePreference.candidate_id == current_user.id
    ).first()

    if not pref:
        # Return defaults
        return {
            "default_industry": "default",
            "default_timezone": "Asia/Kolkata",
            "auto_schedule_enabled": True,
            "tolerance_hours": 2,
            "prefer_morning": True,
            "prefer_afternoon": False,
            "avoid_mondays": True,
            "avoid_fridays": True,
            "use_custom_schedule": False,
            "custom_days": [1, 2, 3],
            "custom_hours": [10, 11, 14],
            "total_scheduled": 0,
            "total_sent_optimal": 0
        }

    return {
        "default_industry": pref.default_industry,
        "default_timezone": pref.default_timezone,
        "auto_schedule_enabled": pref.auto_schedule_enabled,
        "tolerance_hours": pref.tolerance_hours,
        "prefer_morning": pref.prefer_morning,
        "prefer_afternoon": pref.prefer_afternoon,
        "avoid_mondays": pref.avoid_mondays,
        "avoid_fridays": pref.avoid_fridays,
        "use_custom_schedule": pref.use_custom_schedule,
        "custom_days": pref.get_custom_days(),
        "custom_hours": pref.get_custom_hours(),
        "total_scheduled": pref.total_scheduled,
        "total_sent_optimal": pref.total_sent_optimal,
        "average_boost_achieved": pref.average_boost_achieved
    }


@router.put("/preferences")
def update_preferences(
    data: PreferenceUpdate,
    db: Session = Depends(get_db),
    current_user: Candidate = Depends(get_current_candidate)
):
    """Update user's send time optimization preferences."""
    pref = db.query(SendTimePreference).filter(
        SendTimePreference.candidate_id == current_user.id
    ).first()

    if not pref:
        # Create new preferences
        pref = SendTimePreference(candidate_id=current_user.id)
        db.add(pref)

    # Update fields if provided
    if data.default_industry is not None:
        pref.default_industry = data.default_industry
    if data.default_timezone is not None:
        pref.default_timezone = data.default_timezone
    if data.auto_schedule_enabled is not None:
        pref.auto_schedule_enabled = data.auto_schedule_enabled
    if data.tolerance_hours is not None:
        pref.tolerance_hours = data.tolerance_hours
    if data.prefer_morning is not None:
        pref.prefer_morning = data.prefer_morning
    if data.prefer_afternoon is not None:
        pref.prefer_afternoon = data.prefer_afternoon
    if data.avoid_mondays is not None:
        pref.avoid_mondays = data.avoid_mondays
    if data.avoid_fridays is not None:
        pref.avoid_fridays = data.avoid_fridays
    if data.use_custom_schedule is not None:
        pref.use_custom_schedule = data.use_custom_schedule
    if data.custom_days is not None:
        pref.set_custom_days(data.custom_days)
    if data.custom_hours is not None:
        pref.set_custom_hours(data.custom_hours)

    try:
        db.commit()
        db.refresh(pref)
        logger.info(f"[SendTime] Updated preferences for candidate {current_user.id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[SendTime] Failed to update preferences: {e}")
        raise HTTPException(status_code=500, detail="Failed to update preferences")

    return {
        "success": True,
        "message": "Preferences updated",
        "preferences": {
            "default_industry": pref.default_industry,
            "default_timezone": pref.default_timezone,
            "auto_schedule_enabled": pref.auto_schedule_enabled,
            "tolerance_hours": pref.tolerance_hours,
            "prefer_morning": pref.prefer_morning,
            "prefer_afternoon": pref.prefer_afternoon,
            "avoid_mondays": pref.avoid_mondays,
            "avoid_fridays": pref.avoid_fridays,
            "use_custom_schedule": pref.use_custom_schedule,
            "custom_days": pref.get_custom_days(),
            "custom_hours": pref.get_custom_hours()
        }
    }
