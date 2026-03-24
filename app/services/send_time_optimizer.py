"""
Send Time Optimization Service

Uses research-backed data to determine the best time to send emails.
No external APIs needed - pure Python logic.

Based on 2025 research from:
- Omnisend: https://www.omnisend.com/blog/best-time-to-send-email/
- Moosend: https://moosend.com/blog/best-time-to-send-an-email/
- MailerLite: https://www.mailerlite.com/blog/best-time-to-send-email

Key Findings:
- Tuesday 10am is universally the best time
- Tech companies: After standup (10-11am), before afternoon meetings
- Finance: Before market opens (8-9am) or after close (4pm)
- 70% of responses come from follow-up emails
- 8PM has 59% open rate (people checking email at home)
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from enum import Enum
import logging

import pytz

logger = logging.getLogger(__name__)


class Industry(str, Enum):
    """Supported industries for send time optimization."""
    TECH = "tech"
    FINANCE = "finance"
    HEALTHCARE = "healthcare"
    CONSULTING = "consulting"
    STARTUP = "startup"
    ECOMMERCE = "ecommerce"
    EDUCATION = "education"
    NONPROFIT = "nonprofit"
    GOVERNMENT = "government"
    MANUFACTURING = "manufacturing"
    RETAIL = "retail"
    MEDIA = "media"
    LEGAL = "legal"
    REAL_ESTATE = "real_estate"
    DEFAULT = "default"


class SendTimeStrategy(str, Enum):
    """Send time optimization strategies."""
    OPTIMAL = "optimal"                 # Use research-backed optimal times
    BUSINESS_HOURS = "business_hours"   # Standard 9-5
    RECIPIENT_TIMEZONE = "recipient_timezone"  # Adjust for recipient
    CUSTOM = "custom"                   # User-defined schedule


# Research-backed optimal send times by industry
# Format: {industry: {days: [0=Mon, 1=Tue...], hours: [best hours], avoid_hours: [...], reason: str}}
OPTIMAL_SEND_TIMES: Dict[str, Dict] = {
    "tech": {
        "days": [1, 2, 3],              # Tuesday, Wednesday, Thursday
        "hours": [10, 11, 14],          # 10am, 11am, 2pm
        "avoid_hours": [8, 9, 17, 18],  # Morning rush, end of day
        "reason": "After morning standup, before afternoon meetings",
        "expected_boost_range": (20, 30)
    },
    "finance": {
        "days": [1, 2],                 # Tuesday, Wednesday
        "hours": [8, 9, 16],            # 8am, 9am, 4pm
        "avoid_hours": [12, 13],        # Lunch meetings
        "reason": "Before market opens or after trading hours",
        "expected_boost_range": (25, 35)
    },
    "healthcare": {
        "days": [2, 3],                 # Wednesday, Thursday
        "hours": [11, 12, 15],          # 11am, 12pm, 3pm
        "avoid_hours": [7, 8, 19, 20],  # Shift changes
        "reason": "Between shift changes, when staff available",
        "expected_boost_range": (15, 25)
    },
    "consulting": {
        "days": [1, 2],                 # Tuesday, Wednesday
        "hours": [8, 9, 10],            # Early morning
        "avoid_hours": [16, 17, 18],    # Often traveling
        "reason": "Before client meetings start",
        "expected_boost_range": (20, 30)
    },
    "startup": {
        "days": [0, 1, 2],              # Monday, Tuesday, Wednesday
        "hours": [9, 10, 14, 15],       # Morning and early afternoon
        "avoid_hours": [18, 19, 20],    # After hours (though they work late)
        "reason": "Startups work non-stop, early week is best",
        "expected_boost_range": (15, 25)
    },
    "ecommerce": {
        "days": [2, 3],                 # Wednesday, Thursday
        "hours": [10, 14, 20],          # Morning, afternoon, evening
        "avoid_hours": [6, 7, 23],      # Too early/late
        "reason": "Shopping research times, including evening browsing",
        "expected_boost_range": (20, 30)
    },
    "education": {
        "days": [1, 2, 3],              # Tuesday, Wednesday, Thursday
        "hours": [9, 10, 14],           # School hours
        "avoid_hours": [8, 12, 15],     # Class times
        "reason": "Between classes, during office hours",
        "expected_boost_range": (15, 20)
    },
    "nonprofit": {
        "days": [1, 2],                 # Tuesday, Wednesday
        "hours": [10, 11, 14],          # Standard business
        "avoid_hours": [8, 17],         # Rush hours
        "reason": "Standard nonprofit office hours",
        "expected_boost_range": (15, 25)
    },
    "government": {
        "days": [1, 2, 3],              # Tuesday, Wednesday, Thursday
        "hours": [9, 10, 14],           # Government office hours
        "avoid_hours": [8, 12, 17],     # Rush, lunch, end of day
        "reason": "Government offices are busiest mid-week",
        "expected_boost_range": (10, 20)
    },
    "manufacturing": {
        "days": [1, 2],                 # Tuesday, Wednesday
        "hours": [9, 10, 14],           # Standard business
        "avoid_hours": [6, 7, 18, 19],  # Shift times
        "reason": "Office staff available mid-morning",
        "expected_boost_range": (15, 25)
    },
    "retail": {
        "days": [1, 2, 3],              # Tuesday, Wednesday, Thursday
        "hours": [9, 10, 15],           # Before busy hours
        "avoid_hours": [11, 12, 17, 18], # Peak shopping
        "reason": "Before retail rush hours",
        "expected_boost_range": (15, 20)
    },
    "media": {
        "days": [0, 1, 2],              # Monday, Tuesday, Wednesday
        "hours": [10, 11, 15],          # Mid-morning, afternoon
        "avoid_hours": [8, 9, 17, 18],  # Deadlines
        "reason": "After morning news cycles",
        "expected_boost_range": (20, 30)
    },
    "legal": {
        "days": [1, 2, 3],              # Tuesday, Wednesday, Thursday
        "hours": [9, 10, 14],           # Business hours
        "avoid_hours": [8, 12, 17],     # Court times, lunch
        "reason": "Between court sessions",
        "expected_boost_range": (15, 25)
    },
    "real_estate": {
        "days": [1, 2, 3],              # Tuesday, Wednesday, Thursday
        "hours": [9, 10, 16],           # Morning, late afternoon
        "avoid_hours": [12, 13, 14],    # Showings
        "reason": "Before and after property showings",
        "expected_boost_range": (20, 30)
    },
    "default": {
        "days": [1, 2, 3],              # Tuesday, Wednesday, Thursday
        "hours": [10, 11, 14],          # Universal best times
        "avoid_hours": [8, 17, 18],     # Avoid rush hours
        "reason": "Research-backed general optimal times (Tuesday 10am is universally best)",
        "expected_boost_range": (20, 30)
    }
}

# Common timezone mappings by country/region
TIMEZONE_MAPPINGS = {
    # North America
    "US": "America/New_York",
    "USA": "America/New_York",
    "United States": "America/New_York",
    "Canada": "America/Toronto",
    "Mexico": "America/Mexico_City",

    # Europe
    "UK": "Europe/London",
    "United Kingdom": "Europe/London",
    "Germany": "Europe/Berlin",
    "France": "Europe/Paris",
    "Netherlands": "Europe/Amsterdam",
    "Spain": "Europe/Madrid",
    "Italy": "Europe/Rome",
    "Switzerland": "Europe/Zurich",
    "Sweden": "Europe/Stockholm",
    "Norway": "Europe/Oslo",
    "Denmark": "Europe/Copenhagen",
    "Poland": "Europe/Warsaw",
    "Ireland": "Europe/Dublin",
    "Belgium": "Europe/Brussels",
    "Austria": "Europe/Vienna",
    "Portugal": "Europe/Lisbon",
    "Luxembourg": "Europe/Luxembourg",

    # Asia Pacific
    "India": "Asia/Kolkata",
    "Japan": "Asia/Tokyo",
    "China": "Asia/Shanghai",
    "Singapore": "Asia/Singapore",
    "Australia": "Australia/Sydney",
    "New Zealand": "Pacific/Auckland",
    "South Korea": "Asia/Seoul",
    "Hong Kong": "Asia/Hong_Kong",
    "Taiwan": "Asia/Taipei",
    "Thailand": "Asia/Bangkok",
    "Malaysia": "Asia/Kuala_Lumpur",
    "Philippines": "Asia/Manila",
    "Indonesia": "Asia/Jakarta",
    "Vietnam": "Asia/Ho_Chi_Minh",

    # Middle East
    "UAE": "Asia/Dubai",
    "Dubai": "Asia/Dubai",
    "Israel": "Asia/Jerusalem",
    "Saudi Arabia": "Asia/Riyadh",
    "Qatar": "Asia/Qatar",
    "Kuwait": "Asia/Kuwait",
    "Turkey": "Europe/Istanbul",

    # South America
    "Brazil": "America/Sao_Paulo",
    "Argentina": "America/Argentina/Buenos_Aires",
    "Chile": "America/Santiago",
    "Colombia": "America/Bogota",

    # Africa
    "South Africa": "Africa/Johannesburg",
    "Egypt": "Africa/Cairo",
    "Nigeria": "Africa/Lagos",
    "Kenya": "Africa/Nairobi",

    # Default
    "default": "America/New_York"
}

# Country-specific optimal send times based on 2025 research
# Format: {country: {days, hours, avoid_hours, lunch_time, work_culture, email_culture, flag, best_boost}}
COUNTRY_SEND_TIMES: Dict[str, Dict] = {
    "Germany": {
        "flag": "🇩🇪",
        "timezone": "Europe/Berlin",
        "days": [1, 2, 3, 4, 6],  # Tue, Wed, Thu, Fri, Sun (DACH region: Sunday works!)
        "primary_hours": [9, 10],  # Peak 9-10am
        "secondary_hours": [16, 17],  # Second peak 4-5pm
        "avoid_hours": [12, 13],  # Lunch dip
        "lunch_time": "12:00-13:00",
        "work_hours": "8:00-17:00",
        "work_culture": "Punctual and efficient. Germans value direct communication.",
        "email_culture": "57.8% open emails in early evening. Weekend campaigns work well in DACH region.",
        "best_days_note": "Friday highest open rate. Sunday surprisingly effective.",
        "expected_boost": "+28%",
        "response_time": "Same day to 24 hours"
    },
    "France": {
        "flag": "🇫🇷",
        "timezone": "Europe/Paris",
        "days": [1, 2, 3],  # Tue, Wed, Thu
        "primary_hours": [9, 10, 11],
        "secondary_hours": [14, 15],
        "avoid_hours": [12, 13, 14],  # Long lunch breaks!
        "lunch_time": "12:00-14:00",
        "work_hours": "9:00-18:00",
        "work_culture": "Work-life balance focused. Long lunches are cultural norm.",
        "email_culture": "38.33% open rate - highest in Europe! Avoid lunch hours strictly.",
        "best_days_note": "Mid-week performs best. Avoid Monday morning.",
        "expected_boost": "+32%",
        "response_time": "24-48 hours"
    },
    "UK": {
        "flag": "🇬🇧",
        "timezone": "Europe/London",
        "days": [1, 2, 3, 4],  # Tue, Wed, Thu, Fri
        "primary_hours": [9, 10, 11],
        "secondary_hours": [14, 15, 16],
        "avoid_hours": [12, 13],  # Lunch dip
        "lunch_time": "12:00-13:00",
        "work_hours": "9:00-17:30",
        "work_culture": "Professional and polite. Tea breaks are sacred.",
        "email_culture": "Consistent opens during working hours. Friday strong performer.",
        "best_days_note": "Friday generates highest open rates (49.72%).",
        "expected_boost": "+25%",
        "response_time": "Same day"
    },
    "USA": {
        "flag": "🇺🇸",
        "timezone": "America/New_York",
        "days": [1, 2, 3],  # Tue, Wed, Thu
        "primary_hours": [10, 11],
        "secondary_hours": [14, 15, 20],  # 8PM has 59% open rate!
        "avoid_hours": [8, 12, 17],
        "lunch_time": "12:00-13:00",
        "work_hours": "9:00-17:00",
        "work_culture": "Fast-paced, results-oriented. Quick responses expected.",
        "email_culture": "10am most popular send time globally. 8PM evening peak (59% opens).",
        "best_days_note": "Tuesday universally best. Thursday close second.",
        "expected_boost": "+30%",
        "response_time": "Same day"
    },
    "UAE": {
        "flag": "🇦🇪",
        "timezone": "Asia/Dubai",
        "days": [1, 2, 4, 5],  # Tue, Wed, Fri, Sat (Different weekend!)
        "primary_hours": [8, 9, 10, 11],
        "secondary_hours": [17, 18, 19],  # 5-7pm: 26% opens
        "avoid_hours": [12, 13, 14, 15],  # Heat of day + prayers
        "lunch_time": "12:00-14:00",
        "work_hours": "8:00-17:00 (Sun-Thu)",
        "work_culture": "Weekend is Friday-Saturday. Business formal, relationship-focused.",
        "email_culture": "26% open between 5-7pm. Saturday is a work day!",
        "best_days_note": "Tuesday, Wednesday, Friday, Saturday work best.",
        "expected_boost": "+27%",
        "response_time": "24 hours"
    },
    "Dubai": {
        "flag": "🇦🇪",
        "timezone": "Asia/Dubai",
        "days": [1, 2, 4, 5],  # Same as UAE
        "primary_hours": [8, 9, 10, 11],
        "secondary_hours": [17, 18, 19],
        "avoid_hours": [12, 13, 14, 15],
        "lunch_time": "12:00-14:00",
        "work_hours": "8:00-17:00 (Sun-Thu)",
        "work_culture": "International business hub. Multicultural workforce.",
        "email_culture": "Morning emails preferred. Evening catch-up common.",
        "best_days_note": "Business days are Sunday to Thursday.",
        "expected_boost": "+27%",
        "response_time": "24 hours"
    },
    "Singapore": {
        "flag": "🇸🇬",
        "timezone": "Asia/Singapore",
        "days": [1, 2, 3],  # Tue, Wed, Thu
        "primary_hours": [9, 10, 11],
        "secondary_hours": [14, 15, 16],
        "avoid_hours": [12, 13],
        "lunch_time": "12:00-13:00",
        "work_hours": "9:00-18:00",
        "work_culture": "Efficient, hardworking. Long hours common. Very professional.",
        "email_culture": "High email engagement. Quick response culture.",
        "best_days_note": "Mid-week optimal. Avoid Monday morning.",
        "expected_boost": "+26%",
        "response_time": "Same day"
    },
    "India": {
        "flag": "🇮🇳",
        "timezone": "Asia/Kolkata",
        "days": [1, 2, 3, 4],  # Tue, Wed, Thu, Fri
        "primary_hours": [10, 11],
        "secondary_hours": [15, 16, 17],
        "avoid_hours": [13, 14],  # Post-lunch lull
        "lunch_time": "13:00-14:00",
        "work_hours": "9:30-18:30",
        "work_culture": "Hierarchical, relationship-driven. Festivals impact availability.",
        "email_culture": "Morning emails get priority. Late afternoon also effective.",
        "best_days_note": "Tuesday-Thursday optimal. Check for festival holidays.",
        "expected_boost": "+24%",
        "response_time": "24-48 hours"
    },
    "Australia": {
        "flag": "🇦🇺",
        "timezone": "Australia/Sydney",
        "days": [1, 2, 3],  # Tue, Wed, Thu
        "primary_hours": [9, 10, 11],
        "secondary_hours": [14, 15],
        "avoid_hours": [12, 13],
        "lunch_time": "12:00-13:00",
        "work_hours": "9:00-17:00",
        "work_culture": "Relaxed but professional. Work-life balance valued.",
        "email_culture": "Similar patterns to UK. Morning engagement highest.",
        "best_days_note": "Mid-week performs best. Friday afternoon drops off.",
        "expected_boost": "+25%",
        "response_time": "Same day to 24 hours"
    },
    "Switzerland": {
        "flag": "🇨🇭",
        "timezone": "Europe/Zurich",
        "days": [1, 2, 3, 6],  # Tue, Wed, Thu, Sun (DACH region)
        "primary_hours": [9, 10],
        "secondary_hours": [14, 15, 16],
        "avoid_hours": [12, 13],
        "lunch_time": "12:00-13:30",
        "work_hours": "8:00-17:00",
        "work_culture": "Precise, punctual, quality-focused. Multilingual (DE/FR/IT).",
        "email_culture": "DACH region: Sunday campaigns work well. High precision expected.",
        "best_days_note": "Weekend emails surprisingly effective in DACH.",
        "expected_boost": "+26%",
        "response_time": "Same day"
    },
    "Ireland": {
        "flag": "🇮🇪",
        "timezone": "Europe/Dublin",
        "days": [1, 2, 3, 4],  # Tue, Wed, Thu, Fri
        "primary_hours": [9, 10, 11],
        "secondary_hours": [14, 15],
        "avoid_hours": [12, 13],
        "lunch_time": "12:30-13:30",
        "work_hours": "9:00-17:30",
        "work_culture": "Friendly, relationship-focused. Tech hub culture.",
        "email_culture": "Strong tech sector engagement. Similar to UK patterns.",
        "best_days_note": "Tuesday-Thursday optimal. Friday mornings good.",
        "expected_boost": "+25%",
        "response_time": "Same day"
    },
    "Denmark": {
        "flag": "🇩🇰",
        "timezone": "Europe/Copenhagen",
        "days": [1, 2, 3],  # Tue, Wed, Thu
        "primary_hours": [9, 10],
        "secondary_hours": [13, 14, 15],
        "avoid_hours": [12],  # Short lunch
        "lunch_time": "12:00-12:30",
        "work_hours": "8:00-16:00",
        "work_culture": "Flat hierarchy, work-life balance priority. Leave early culture.",
        "email_culture": "Early starts, early finishes. Afternoon emails may wait until next day.",
        "best_days_note": "Mid-week best. Avoid Friday afternoon entirely.",
        "expected_boost": "+23%",
        "response_time": "Same day (before 15:00)"
    },
    "Poland": {
        "flag": "🇵🇱",
        "timezone": "Europe/Warsaw",
        "days": [1, 2, 3, 4],  # Tue, Wed, Thu, Fri
        "primary_hours": [9, 10, 11],
        "secondary_hours": [14, 15, 16],
        "avoid_hours": [12, 13],
        "lunch_time": "12:00-13:00",
        "work_hours": "8:00-16:00 or 9:00-17:00",
        "work_culture": "Growing tech hub. Professional, hardworking.",
        "email_culture": "Standard European patterns. Strong IT sector engagement.",
        "best_days_note": "Tuesday-Thursday optimal. Friday acceptable.",
        "expected_boost": "+24%",
        "response_time": "Same day to 24 hours"
    },
    "Luxembourg": {
        "flag": "🇱🇺",
        "timezone": "Europe/Luxembourg",
        "days": [1, 2, 3],  # Tue, Wed, Thu
        "primary_hours": [9, 10],
        "secondary_hours": [14, 15],
        "avoid_hours": [12, 13],
        "lunch_time": "12:00-14:00",
        "work_hours": "8:30-17:30",
        "work_culture": "International finance hub. Multilingual (FR/DE/LU). Very professional.",
        "email_culture": "High-value contacts. Quality over quantity approach.",
        "best_days_note": "Mid-week optimal. Avoid Monday and Friday afternoon.",
        "expected_boost": "+25%",
        "response_time": "24 hours"
    },
}

# Day name mappings for display
DAY_NAMES = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday"
}


class SendTimeOptimizer:
    """
    Calculates optimal email send times based on industry research.

    Usage:
        optimizer = SendTimeOptimizer()

        # Get next optimal time for a tech company
        result = optimizer.get_optimal_send_time(
            industry="tech",
            recipient_country="US"
        )

        print(f"Send at: {result['send_at']}")
        print(f"Reason: {result['reason']}")
        print(f"Expected open rate boost: {result['expected_boost']}")

        # Quick check if now is a good time
        should_send, reason = optimizer.should_send_now(
            industry="tech",
            recipient_country="US"
        )

        # Get all optimal slots for the week
        slots = optimizer.get_schedule_for_week(
            industry="tech",
            recipient_country="US"
        )
    """

    def __init__(self, sender_timezone: str = "Asia/Kolkata"):
        """
        Initialize the optimizer.

        Args:
            sender_timezone: Your timezone (default: India)
        """
        logger.debug(f"[SendTimeOptimizer] Initializing with sender_timezone: {sender_timezone}")
        try:
            self.sender_tz = pytz.timezone(sender_timezone)
            logger.debug(f"[SendTimeOptimizer] Successfully set sender timezone: {sender_timezone}")
        except Exception as e:
            logger.warning(f"[SendTimeOptimizer] Invalid sender timezone '{sender_timezone}': {e}. Falling back to Asia/Kolkata")
            self.sender_tz = pytz.timezone("Asia/Kolkata")

    def get_optimal_send_time(
        self,
        industry: str = "default",
        recipient_country: str = None,
        recipient_timezone: str = None,
        strategy: SendTimeStrategy = SendTimeStrategy.OPTIMAL,
        custom_hours: List[int] = None,
        custom_days: List[int] = None
    ) -> Dict:
        """
        Calculate the next optimal time to send an email.

        Args:
            industry: Target industry (tech, finance, healthcare, etc.)
            recipient_country: Country of recipient (for timezone)
            recipient_timezone: Explicit timezone (overrides country)
            strategy: Which strategy to use
            custom_hours: Custom preferred hours (for CUSTOM strategy)
            custom_days: Custom preferred days (for CUSTOM strategy)

        Returns:
            {
                "send_at": datetime (UTC),
                "send_at_local": str (human readable),
                "day_name": "Tuesday",
                "hour": 10,
                "minute": 0,
                "timezone": "America/New_York",
                "recipient_timezone": "America/New_York",
                "reason": "After morning standup...",
                "expected_boost": "+25% expected",
                "is_now_optimal": True/False,
                "wait_hours": 0 if optimal now, else hours to wait,
                "industry": "tech",
                "strategy": "optimal"
            }
        """
        logger.info(f"[SendTimeOptimizer] Calculating optimal send time for industry: {industry}, country: {recipient_country}")

        # Determine recipient timezone
        recipient_tz = self._get_timezone(recipient_country, recipient_timezone)
        logger.debug(f"[SendTimeOptimizer] Resolved recipient timezone: {recipient_tz}")

        # Get optimal times based on strategy
        if strategy == SendTimeStrategy.CUSTOM and custom_hours and custom_days:
            optimal_config = {
                "days": custom_days,
                "hours": custom_hours,
                "avoid_hours": [],
                "reason": "Custom schedule",
                "expected_boost_range": (10, 20)
            }
        else:
            # Validate and normalize industry with fallback to default
            industry_normalized = industry.lower() if industry else "default"
            if industry_normalized in OPTIMAL_SEND_TIMES:
                industry_key = industry_normalized
            else:
                industry_key = "default"
                logger.warning(f"[SendTimeOptimizer] Unknown industry '{industry}', using default settings")

            optimal_config = OPTIMAL_SEND_TIMES[industry_key]
            logger.debug(f"[SendTimeOptimizer] Using industry config: {industry_key}")

        # Get current time in recipient's timezone
        now_utc = datetime.now(pytz.UTC)
        now_recipient = now_utc.astimezone(recipient_tz)

        # Find next optimal slot
        optimal_time, is_now = self._find_next_optimal_slot(
            now_recipient,
            optimal_config["days"],
            optimal_config["hours"],
            optimal_config.get("avoid_hours", [])
        )

        # Calculate wait time
        wait_delta = optimal_time - now_recipient
        wait_hours = wait_delta.total_seconds() / 3600

        # Calculate expected boost based on timing
        expected_boost = self._calculate_expected_boost(
            optimal_time.weekday(),
            optimal_time.hour,
            optimal_config
        )

        logger.info(f"[SendTimeOptimizer] Optimal time calculated: {optimal_time.strftime('%A %I:%M %p %Z')}, is_now_optimal: {is_now}, wait_hours: {round(wait_hours, 1)}")

        return {
            "send_at": optimal_time.astimezone(pytz.UTC),  # UTC for storage
            "send_at_local": optimal_time.strftime("%A, %B %d at %I:%M %p %Z"),
            "day_name": optimal_time.strftime("%A"),
            "hour": optimal_time.hour,
            "minute": optimal_time.minute,
            "timezone": str(recipient_tz),
            "recipient_timezone": str(recipient_tz),
            "reason": optimal_config["reason"],
            "expected_boost": expected_boost,
            "is_now_optimal": is_now,
            "wait_hours": round(wait_hours, 1) if not is_now else 0,
            "industry": industry,
            "strategy": strategy.value if isinstance(strategy, SendTimeStrategy) else strategy
        }

    def _get_timezone(
        self,
        country: str = None,
        explicit_tz: str = None
    ) -> pytz.timezone:
        """Get timezone from country or explicit value."""
        logger.debug(f"[SendTimeOptimizer] Getting timezone - country: {country}, explicit_tz: {explicit_tz}")

        if explicit_tz:
            try:
                tz = pytz.timezone(explicit_tz)
                logger.debug(f"[SendTimeOptimizer] Using explicit timezone: {explicit_tz}")
                return tz
            except Exception as e:
                logger.warning(f"[SendTimeOptimizer] Invalid explicit timezone '{explicit_tz}': {e}. Will try country fallback.")

        if country:
            tz_str = TIMEZONE_MAPPINGS.get(country)
            if tz_str:
                logger.debug(f"[SendTimeOptimizer] Found timezone mapping for country '{country}': {tz_str}")
                return pytz.timezone(tz_str)
            else:
                logger.warning(f"[SendTimeOptimizer] No timezone mapping for country '{country}'. Using default: {TIMEZONE_MAPPINGS['default']}")
                return pytz.timezone(TIMEZONE_MAPPINGS["default"])

        logger.debug(f"[SendTimeOptimizer] No country or explicit timezone provided. Using default: {TIMEZONE_MAPPINGS['default']}")
        return pytz.timezone(TIMEZONE_MAPPINGS["default"])

    def _find_next_optimal_slot(
        self,
        now: datetime,
        optimal_days: List[int],
        optimal_hours: List[int],
        avoid_hours: List[int]
    ) -> Tuple[datetime, bool]:
        """
        Find the next optimal send time.

        Returns:
            (optimal_datetime, is_current_time_optimal)
        """
        current_day = now.weekday()
        current_hour = now.hour

        # Check if NOW is optimal (within the current hour)
        if (current_day in optimal_days and
            current_hour in optimal_hours and
            current_hour not in avoid_hours):
            # Current time is optimal! Send now.
            return now, True

        # Find next optimal slot
        for days_ahead in range(8):  # Check up to 8 days
            check_date = now + timedelta(days=days_ahead)
            check_day = check_date.weekday()

            if check_day not in optimal_days:
                continue

            for hour in sorted(optimal_hours):
                if hour in avoid_hours:
                    continue

                candidate = check_date.replace(
                    hour=hour,
                    minute=0,
                    second=0,
                    microsecond=0
                )

                if candidate > now:
                    return candidate, False

        # Fallback: next Tuesday at 10am (universal best time)
        days_until_tuesday = (1 - now.weekday()) % 7 or 7
        fallback = now + timedelta(days=days_until_tuesday)
        fallback = fallback.replace(hour=10, minute=0, second=0, microsecond=0)
        return fallback, False

    def _calculate_expected_boost(
        self,
        day: int,
        hour: int,
        config: Dict
    ) -> str:
        """Calculate expected open rate boost."""
        boost = 0

        # Day boost
        if day in config.get("days", []):
            boost += 15

        # Hour boost
        if hour in config.get("hours", []):
            boost += 10

        # Primary hour (first in list) gets extra boost
        if config.get("hours") and hour == config["hours"][0]:
            boost += 5

        # Penalty for avoid hours
        if hour in config.get("avoid_hours", []):
            boost -= 20

        # Use the configured boost range if available
        boost_range = config.get("expected_boost_range", (15, 30))

        if boost > 20:
            return f"+{min(boost, boost_range[1])}% expected"
        elif boost > 10:
            return f"+{boost}% likely"
        elif boost > 0:
            return f"+{boost}% possible"
        else:
            return "Sub-optimal time"

    def should_send_now(
        self,
        industry: str = "default",
        recipient_country: str = None,
        tolerance_hours: int = 2
    ) -> Tuple[bool, str]:
        """
        Quick check: should I send this email now?

        Args:
            industry: Target industry
            recipient_country: Recipient's country
            tolerance_hours: How many hours off from optimal is acceptable

        Returns:
            (should_send: bool, reason: str)
        """
        result = self.get_optimal_send_time(
            industry=industry,
            recipient_country=recipient_country
        )

        if result["is_now_optimal"]:
            return True, f"Current time is optimal for {industry} industry. {result['reason']}"

        if result["wait_hours"] <= tolerance_hours:
            return True, f"Within {tolerance_hours}h of optimal time. Expected boost: {result['expected_boost']}"

        return False, f"Wait {result['wait_hours']}h for optimal time ({result['send_at_local']})"

    def get_schedule_for_week(
        self,
        industry: str = "default",
        recipient_country: str = None,
        max_slots: int = 10
    ) -> List[Dict]:
        """
        Get all optimal send slots for the next 7 days.

        Returns list of optimal times for planning batch sends.
        """
        logger.debug(f"[SendTimeOptimizer] Getting schedule for industry: {industry}, country: {recipient_country}")

        recipient_tz = self._get_timezone(recipient_country)
        now = datetime.now(recipient_tz)

        # Validate and normalize industry
        industry_normalized = industry.lower() if industry else "default"
        industry_key = industry_normalized if industry_normalized in OPTIMAL_SEND_TIMES else "default"
        config = OPTIMAL_SEND_TIMES[industry_key]

        slots = []

        for days_ahead in range(7):
            check_date = now + timedelta(days=days_ahead)

            if check_date.weekday() not in config["days"]:
                continue

            for hour in config["hours"]:
                if hour in config.get("avoid_hours", []):
                    continue

                slot_time = check_date.replace(
                    hour=hour,
                    minute=0,
                    second=0,
                    microsecond=0
                )

                if slot_time > now:
                    expected_boost = self._calculate_expected_boost(
                        slot_time.weekday(),
                        hour,
                        config
                    )

                    slots.append({
                        "datetime": slot_time.astimezone(pytz.UTC),
                        "datetime_local": slot_time.strftime("%A, %B %d at %I:%M %p"),
                        "day": slot_time.strftime("%A"),
                        "date": slot_time.strftime("%Y-%m-%d"),
                        "time": slot_time.strftime("%I:%M %p"),
                        "hour": hour,
                        "is_primary": hour == config["hours"][0],
                        "expected_boost": expected_boost,
                        "timezone": str(recipient_tz)
                    })

        return slots[:max_slots]

    def get_industry_info(self, industry: str) -> Dict:
        """
        Get information about optimal times for a specific industry.

        Returns:
            {
                "industry": "tech",
                "best_days": ["Tuesday", "Wednesday", "Thursday"],
                "best_hours": [10, 11, 14],
                "avoid_hours": [8, 9, 17, 18],
                "reason": "After morning standup...",
                "expected_boost_range": (20, 30)
            }
        """
        industry_key = industry.lower() if industry.lower() in OPTIMAL_SEND_TIMES else "default"
        config = OPTIMAL_SEND_TIMES[industry_key]

        return {
            "industry": industry_key,
            "best_days": [DAY_NAMES[d] for d in config["days"]],
            "best_hours": config["hours"],
            "best_hours_formatted": [f"{h}:00 {'AM' if h < 12 else 'PM'}" for h in config["hours"]],
            "avoid_hours": config.get("avoid_hours", []),
            "reason": config["reason"],
            "expected_boost_range": config.get("expected_boost_range", (15, 30))
        }

    @staticmethod
    def get_all_industries() -> List[Dict]:
        """Get list of all supported industries with their info."""
        industries = []
        for key in OPTIMAL_SEND_TIMES.keys():
            config = OPTIMAL_SEND_TIMES[key]
            industries.append({
                "value": key,
                "label": key.replace("_", " ").title(),
                "best_days": [DAY_NAMES[d] for d in config["days"]],
                "best_hours": config["hours"],
                "reason": config["reason"]
            })
        return industries

    @staticmethod
    def get_all_timezones() -> Dict[str, str]:
        """Get all supported country to timezone mappings."""
        return TIMEZONE_MAPPINGS.copy()

    @staticmethod
    def get_all_countries() -> List[Dict]:
        """Get list of all supported countries with detailed info."""
        countries = []
        for country, data in COUNTRY_SEND_TIMES.items():
            countries.append({
                "name": country,
                "flag": data["flag"],
                "timezone": data["timezone"],
                "best_days": [DAY_NAMES[d] for d in data["days"]],
                "primary_hours": data["primary_hours"],
                "secondary_hours": data["secondary_hours"],
                "avoid_hours": data["avoid_hours"],
                "lunch_time": data["lunch_time"],
                "work_hours": data["work_hours"],
                "work_culture": data["work_culture"],
                "email_culture": data["email_culture"],
                "best_days_note": data["best_days_note"],
                "expected_boost": data["expected_boost"],
                "response_time": data["response_time"]
            })
        return countries

    def get_country_optimal_time(
        self,
        country: str,
        industry: str = "default"
    ) -> Dict:
        """
        Get optimal send time for a specific country.

        Uses country-specific data if available, falls back to industry defaults.
        """
        # Check if we have country-specific data
        if country in COUNTRY_SEND_TIMES:
            country_data = COUNTRY_SEND_TIMES[country]
            recipient_tz = pytz.timezone(country_data["timezone"])

            # Get current time in country's timezone
            now_utc = datetime.now(pytz.UTC)
            now_local = now_utc.astimezone(recipient_tz)

            # Combine primary and secondary hours
            all_hours = country_data["primary_hours"] + country_data["secondary_hours"]

            # Find next optimal slot
            optimal_time, is_now = self._find_next_optimal_slot(
                now_local,
                country_data["days"],
                all_hours,
                country_data["avoid_hours"]
            )

            # Calculate wait time
            wait_delta = optimal_time - now_local
            wait_hours = max(0, wait_delta.total_seconds() / 3600)

            # Determine if it's primary or secondary hour
            is_primary_hour = optimal_time.hour in country_data["primary_hours"]

            return {
                "send_at": optimal_time.astimezone(pytz.UTC),
                "send_at_local": optimal_time.strftime("%A, %B %d at %I:%M %p"),
                "day_name": optimal_time.strftime("%A"),
                "hour": optimal_time.hour,
                "minute": optimal_time.minute,
                "timezone": str(recipient_tz),
                "country": country,
                "flag": country_data["flag"],
                "is_now_optimal": is_now,
                "wait_hours": round(wait_hours, 1) if not is_now else 0,
                "expected_boost": country_data["expected_boost"],
                "is_primary_hour": is_primary_hour,
                "work_culture": country_data["work_culture"],
                "email_culture": country_data["email_culture"],
                "lunch_time": country_data["lunch_time"],
                "work_hours": country_data["work_hours"],
                "best_days_note": country_data["best_days_note"],
                "response_time": country_data["response_time"],
                "primary_hours": country_data["primary_hours"],
                "secondary_hours": country_data["secondary_hours"],
                "avoid_hours": country_data["avoid_hours"],
                "best_days": [DAY_NAMES[d] for d in country_data["days"]]
            }
        else:
            # Fall back to industry-based optimization
            result = self.get_optimal_send_time(
                industry=industry,
                recipient_country=country
            )
            result["country"] = country
            result["flag"] = "🌍"
            result["work_culture"] = "Standard business culture"
            result["email_culture"] = "Standard email patterns"
            result["lunch_time"] = "12:00-13:00"
            result["work_hours"] = "9:00-17:00"
            result["best_days_note"] = "Tuesday-Thursday optimal"
            result["response_time"] = "24 hours"
            return result

    def get_country_weekly_schedule(
        self,
        country: str,
        max_slots: int = 10
    ) -> List[Dict]:
        """
        Get optimal send slots for the next 7 days for a specific country.
        """
        if country not in COUNTRY_SEND_TIMES:
            return self.get_schedule_for_week(recipient_country=country, max_slots=max_slots)

        country_data = COUNTRY_SEND_TIMES[country]
        recipient_tz = pytz.timezone(country_data["timezone"])
        now = datetime.now(recipient_tz)

        slots = []
        all_hours = country_data["primary_hours"] + country_data["secondary_hours"]

        for days_ahead in range(7):
            check_date = now + timedelta(days=days_ahead)

            if check_date.weekday() not in country_data["days"]:
                continue

            for hour in sorted(set(all_hours)):
                if hour in country_data["avoid_hours"]:
                    continue

                slot_time = check_date.replace(
                    hour=hour,
                    minute=0,
                    second=0,
                    microsecond=0
                )

                if slot_time > now:
                    is_primary = hour in country_data["primary_hours"]
                    boost = country_data["expected_boost"] if is_primary else f"+{int(country_data['expected_boost'].replace('+', '').replace('%', '')) - 5}%"

                    slots.append({
                        "datetime": slot_time.astimezone(pytz.UTC),
                        "datetime_local": slot_time.strftime("%A, %B %d at %I:%M %p"),
                        "day": slot_time.strftime("%A"),
                        "date": slot_time.strftime("%Y-%m-%d"),
                        "time": slot_time.strftime("%I:%M %p"),
                        "hour": hour,
                        "is_primary": is_primary,
                        "expected_boost": boost,
                        "timezone": str(recipient_tz),
                        "slot_type": "Primary" if is_primary else "Secondary"
                    })

        return slots[:max_slots]

    @staticmethod
    def get_country_info(country: str) -> Dict:
        """Get detailed information about a specific country."""
        if country in COUNTRY_SEND_TIMES:
            data = COUNTRY_SEND_TIMES[country]
            return {
                "name": country,
                "flag": data["flag"],
                "timezone": data["timezone"],
                "best_days": [DAY_NAMES[d] for d in data["days"]],
                "primary_hours": data["primary_hours"],
                "primary_hours_formatted": [f"{h}:00" for h in data["primary_hours"]],
                "secondary_hours": data["secondary_hours"],
                "secondary_hours_formatted": [f"{h}:00" for h in data["secondary_hours"]],
                "avoid_hours": data["avoid_hours"],
                "avoid_hours_formatted": [f"{h}:00" for h in data["avoid_hours"]],
                "lunch_time": data["lunch_time"],
                "work_hours": data["work_hours"],
                "work_culture": data["work_culture"],
                "email_culture": data["email_culture"],
                "best_days_note": data["best_days_note"],
                "expected_boost": data["expected_boost"],
                "response_time": data["response_time"]
            }
        return None


# Convenience functions for direct import

def get_optimal_send_time(
    industry: str = "default",
    recipient_country: str = None,
    sender_timezone: str = "Asia/Kolkata"
) -> Dict:
    """Quick function to get optimal send time."""
    optimizer = SendTimeOptimizer(sender_timezone=sender_timezone)
    return optimizer.get_optimal_send_time(
        industry=industry,
        recipient_country=recipient_country
    )


def should_send_now(
    industry: str = "default",
    recipient_country: str = None,
    tolerance_hours: int = 2
) -> Tuple[bool, str]:
    """Quick function to check if now is a good time."""
    optimizer = SendTimeOptimizer()
    return optimizer.should_send_now(
        industry=industry,
        recipient_country=recipient_country,
        tolerance_hours=tolerance_hours
    )


def get_schedule_for_week(
    industry: str = "default",
    recipient_country: str = None
) -> List[Dict]:
    """Quick function to get weekly schedule."""
    optimizer = SendTimeOptimizer()
    return optimizer.get_schedule_for_week(
        industry=industry,
        recipient_country=recipient_country
    )
