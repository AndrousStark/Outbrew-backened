"""
Validation Service
FREE - No API costs required
Validates and scores extracted data
"""

import re
from typing import Dict, Any
from email_validator import validate_email, EmailNotValidError
import logging

logger = logging.getLogger(__name__)


class ValidationService:
    """
    Free data validation and quality scoring
    No API costs - uses regex and pattern matching
    """

    def validate_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and score a single extraction record

        Returns:
            {
                "is_valid": bool,
                "quality_score": float (0-1),
                "confidence_score": float (0-1),
                "completeness_score": float (0-1),
                "validation_details": {
                    "email_valid": bool,
                    "email_format_ok": bool,
                    "phone_valid": bool,
                    "has_name": bool,
                    "has_company": bool,
                    "has_title": bool
                }
            }
        """
        details = {
            "email_valid": False,
            "email_format_ok": False,
            "phone_valid": False,
            "has_name": False,
            "has_company": False,
            "has_title": False,
            "has_location": False
        }

        # Validate email (CRITICAL)
        email = record.get("email", "").strip()
        if email:
            details["email_format_ok"] = self._is_valid_email_format(email)
            details["email_valid"] = details["email_format_ok"]  # Basic validation without API

        # Validate phone
        phone = record.get("phone", "").strip()
        if phone:
            details["phone_valid"] = self._is_valid_phone_format(phone)

        # Check required fields
        details["has_name"] = bool(record.get("name", "").strip())
        details["has_company"] = bool(record.get("company", "").strip())
        details["has_title"] = bool(record.get("title", "").strip())
        details["has_location"] = bool(record.get("location", "").strip())

        # Calculate scores
        quality_score = self._calculate_quality_score(record, details)
        confidence_score = self._calculate_confidence_score(record, details)
        completeness_score = self._calculate_completeness_score(record, details)

        # Record is valid if it has at least email and either name or company
        is_valid = (
            details["email_valid"] and
            (details["has_name"] or details["has_company"])
        )

        return {
            "is_valid": is_valid,
            "quality_score": quality_score,
            "confidence_score": confidence_score,
            "completeness_score": completeness_score,
            "validation_details": details
        }

    def _is_valid_email_format(self, email: str) -> bool:
        """
        Validate email format
        FREE - no API call required
        """
        try:
            # Use email-validator library for robust validation
            validate_email(email, check_deliverability=False)
            return True
        except EmailNotValidError:
            # Fallback to regex
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return bool(re.match(pattern, email))

    def _is_valid_phone_format(self, phone: str) -> bool:
        """
        Validate phone number format
        FREE - basic format checking
        """
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', phone)

        # Phone must have 7-15 digits
        if len(digits) < 7 or len(digits) > 15:
            return False

        # Common invalid patterns
        if digits == '0' * len(digits):  # All zeros
            return False

        if digits == '1' * len(digits):  # All ones
            return False

        return True

    def _calculate_quality_score(
        self,
        record: Dict[str, Any],
        details: Dict[str, bool]
    ) -> float:
        """
        Calculate overall quality score (0-1)

        Factors:
        - Email validity: 40%
        - Name presence: 20%
        - Company presence: 15%
        - Title presence: 15%
        - Phone presence: 10%
        """
        score = 0.0

        # Email (40 points)
        if details["email_valid"]:
            score += 0.40
        elif details["email_format_ok"]:
            score += 0.20

        # Name (20 points)
        if details["has_name"]:
            name = record.get("name", "")
            if len(name.split()) >= 2:  # Full name (first + last)
                score += 0.20
            else:  # Partial name
                score += 0.10

        # Company (15 points)
        if details["has_company"]:
            score += 0.15

        # Title (15 points)
        if details["has_title"]:
            score += 0.15

        # Phone (10 points)
        if details["phone_valid"]:
            score += 0.10

        return round(score, 2)

    def _calculate_confidence_score(
        self,
        record: Dict[str, Any],
        details: Dict[str, bool]
    ) -> float:
        """
        Calculate confidence score (0-1)
        How confident are we this data is accurate?

        Factors:
        - Source URL known: +0.2
        - Multiple data points: +0.2
        - Consistent patterns: +0.3
        - No duplicates: +0.3
        """
        score = 0.0

        # Source URL known
        if record.get("source_url"):
            score += 0.2

        # Multiple data points
        data_points = sum([
            details["email_valid"],
            details["has_name"],
            details["has_company"],
            details["has_title"],
            details["phone_valid"]
        ])

        if data_points >= 4:
            score += 0.3
        elif data_points >= 3:
            score += 0.2
        elif data_points >= 2:
            score += 0.1

        # Email domain matches company
        if details["email_valid"] and details["has_company"]:
            email = record.get("email", "")
            company = record.get("company", "").lower()
            email_domain = email.split('@')[1] if '@' in email else ""

            # Check if company name is in email domain
            company_clean = re.sub(r'[^a-z0-9]', '', company.lower())
            domain_clean = re.sub(r'[^a-z0-9]', '', email_domain.split('.')[0])

            if company_clean and domain_clean and company_clean in domain_clean:
                score += 0.3
            else:
                score += 0.1

        return round(min(score, 1.0), 2)

    def _calculate_completeness_score(
        self,
        record: Dict[str, Any],
        details: Dict[str, bool]
    ) -> float:
        """
        Calculate completeness score (0-1)
        What percentage of fields are populated?
        """
        # Define expected fields and their weights
        field_weights = {
            "email": 0.25,
            "name": 0.20,
            "company": 0.15,
            "title": 0.15,
            "phone": 0.10,
            "location": 0.10,
            "linkedin_url": 0.05
        }

        score = 0.0

        for field, weight in field_weights.items():
            value = record.get(field, "")
            if value and str(value).strip():
                score += weight

        return round(score, 2)

    def deduplicate_records(
        self,
        records: list[Dict[str, Any]]
    ) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
        """
        Remove duplicate records based on email

        Returns:
            (unique_records, duplicates)
        """
        seen_emails = set()
        unique = []
        duplicates = []

        for record in records:
            email = record.get("email", "").lower().strip()

            if not email:
                # No email - keep but mark as low quality
                unique.append(record)
                continue

            if email in seen_emails:
                duplicates.append(record)
            else:
                seen_emails.add(email)
                unique.append(record)

        return unique, duplicates

    def filter_by_quality(
        self,
        records: list[Dict[str, Any]],
        min_quality: float = 0.7
    ) -> list[Dict[str, Any]]:
        """
        Filter records by minimum quality score
        """
        return [
            record for record in records
            if record.get("quality_score", 0) >= min_quality
        ]
