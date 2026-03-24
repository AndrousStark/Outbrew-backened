"""
Hunter.io API Client (Layer 5: Email Verification & Finding)

Hunter.io provides:
- Email verification (deliverability, disposable, role, free provider)
- Domain search (find all emails for a domain)
- Email finder (from name + domain)
- Email count (how many emails in domain)
- Author finder (find email of article/blog author)

Cost: $49/month for 1,000 searches + 5,000 verifications (Starter plan)
API Docs: https://hunter.io/api-documentation/v2
"""

import httpx
import logging
from typing import List, Dict, Any, Optional
from app.core.config import settings
from app.utils.advanced_cache import TimeBoundCache

logger = logging.getLogger(__name__)


class HunterClient:
    """
    Hunter.io API Client

    Features:
    - Email verification (check if email is deliverable)
    - Email finding from name + company domain
    - Domain search (find all emails at company)
    - Email scoring (0-100 confidence)

    Rate Limits:
    - 10 requests per second
    - 1,000 searches per month (Starter)
    - 5,000 verifications per month (Starter)
    """

    BASE_URL = "https://api.hunter.io/v2"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.HUNTER_API_KEY
        self.client = httpx.AsyncClient(timeout=30.0)

        # Cache API responses (30 day TTL for verifications, 7 days for searches)
        self.verification_cache = TimeBoundCache(capacity=10000, default_ttl_seconds=30*24*3600)
        self.search_cache = TimeBoundCache(capacity=5000, default_ttl_seconds=7*24*3600)

        # Statistics
        self.verifications_used = 0
        self.searches_used = 0
        self.requests_made = 0

    async def verify_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Verify email address deliverability

        Returns:
            {
                "email": "john.doe@example.com",
                "result": "deliverable",  # deliverable, undeliverable, risky, unknown
                "score": 95,  # 0-100 confidence score
                "is_deliverable": True,
                "is_disposable": False,  # Temp email service
                "is_role": False,  # Role-based (e.g., info@, support@)
                "is_free": False,  # Free provider (Gmail, Yahoo, etc.)
                "mx_records": True,
                "smtp_server": True,
                "smtp_check": True,
                "accept_all": False,  # Domain accepts all emails
                "block": False  # Email should be blocked
            }
        """
        if not self.api_key:
            logger.warning("Hunter.io API key not configured, skipping verification")
            return None

        # Check cache (30 day TTL)
        cache_key = f"hunter:verify:{email}"
        cached = self.verification_cache.get(cache_key)
        if cached:
            return cached

        try:
            response = await self.client.get(
                f"{self.BASE_URL}/email-verifier",
                params={
                    "email": email,
                    "api_key": self.api_key
                }
            )
            response.raise_for_status()

            data = response.json()
            self.requests_made += 1
            self.verifications_used += 1

            verification = data.get("data", {})

            result = {
                "email": verification.get("email"),
                "result": verification.get("result"),  # deliverable, undeliverable, risky, unknown
                "score": verification.get("score", 0),
                "is_deliverable": verification.get("result") == "deliverable",
                "is_disposable": verification.get("disposable", False),
                "is_role": verification.get("webmail", False),
                "is_free": verification.get("free", False),
                "mx_records": verification.get("mx_records", False),
                "smtp_server": verification.get("smtp_server", False),
                "smtp_check": verification.get("smtp_check", False),
                "accept_all": verification.get("accept_all", False),
                "block": verification.get("block", False),
                "sources": verification.get("sources", [])
            }

            # Cache result (30 days)
            self.verification_cache.put(cache_key, result)

            logger.info(f"Hunter verify: {email} -> {result['result']} (score: {result['score']})")

            return result

        except httpx.HTTPStatusError as e:
            logger.error(f"Hunter API error: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Hunter API exception: {e}")
            return None

    async def find_email(
        self,
        first_name: str,
        last_name: str,
        domain: str,
        full_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Find email address from name and company domain

        Uses Hunter's proprietary algorithm to predict email format

        Args:
            first_name: Person's first name
            last_name: Person's last name
            domain: Company domain (e.g., "example.com")
            full_name: Full name (optional, improves accuracy)

        Returns:
            {
                "email": "john.doe@example.com",
                "score": 85,  # 0-100 confidence
                "first_name": "John",
                "last_name": "Doe",
                "position": "CEO",
                "company": "Example Inc",
                "sources": [...]  # Where email was found
            }
        """
        if not self.api_key:
            return None

        # Check cache (7 days)
        cache_key = f"hunter:find:{first_name}:{last_name}:{domain}"
        cached = self.search_cache.get(cache_key)
        if cached:
            return cached

        try:
            params = {
                "first_name": first_name,
                "last_name": last_name,
                "domain": domain,
                "api_key": self.api_key
            }

            if full_name:
                params["full_name"] = full_name

            response = await self.client.get(
                f"{self.BASE_URL}/email-finder",
                params=params
            )
            response.raise_for_status()

            data = response.json()
            self.requests_made += 1
            self.searches_used += 1

            email_data = data.get("data", {})

            if not email_data.get("email"):
                return None

            result = {
                "email": email_data.get("email"),
                "score": email_data.get("score", 0),
                "first_name": email_data.get("first_name"),
                "last_name": email_data.get("last_name"),
                "position": email_data.get("position"),
                "company": email_data.get("company"),
                "linkedin_url": email_data.get("linkedin"),
                "twitter": email_data.get("twitter"),
                "phone_number": email_data.get("phone_number"),
                "sources": email_data.get("sources", []),
                "verification": email_data.get("verification")
            }

            # Cache result
            self.search_cache.put(cache_key, result)

            logger.info(f"Hunter find: {first_name} {last_name} @ {domain} -> {result['email']} (score: {result['score']})")

            return result

        except httpx.HTTPStatusError as e:
            logger.error(f"Hunter find_email error: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Hunter find_email exception: {e}")
            return None

    async def domain_search(
        self,
        domain: str,
        limit: int = 100,
        offset: int = 0
    ) -> Optional[Dict[str, Any]]:
        """
        Search all emails for a domain

        Returns all public emails found for the domain

        Args:
            domain: Company domain (e.g., "example.com")
            limit: Number of results (max 100)
            offset: Pagination offset

        Returns:
            {
                "domain": "example.com",
                "emails": [
                    {
                        "email": "john.doe@example.com",
                        "first_name": "John",
                        "last_name": "Doe",
                        "position": "CEO",
                        "linkedin": "...",
                        "phone": "...",
                        "confidence": 95,
                        "sources": [...]
                    },
                    ...
                ],
                "total": 42,
                "pattern": "{first}.{last}@{domain}",  # Email pattern used
                "organization": "Example Inc"
            }
        """
        if not self.api_key:
            return None

        # Check cache
        cache_key = f"hunter:domain:{domain}:{offset}"
        cached = self.search_cache.get(cache_key)
        if cached:
            return cached

        try:
            response = await self.client.get(
                f"{self.BASE_URL}/domain-search",
                params={
                    "domain": domain,
                    "limit": limit,
                    "offset": offset,
                    "api_key": self.api_key
                }
            )
            response.raise_for_status()

            data = response.json()
            self.requests_made += 1
            self.searches_used += 1

            result = data.get("data", {})

            # Transform emails
            emails = []
            for email_data in result.get("emails", []):
                emails.append({
                    "email": email_data.get("value"),
                    "first_name": email_data.get("first_name"),
                    "last_name": email_data.get("last_name"),
                    "position": email_data.get("position"),
                    "department": email_data.get("department"),
                    "seniority": email_data.get("seniority"),
                    "linkedin": email_data.get("linkedin"),
                    "twitter": email_data.get("twitter"),
                    "phone": email_data.get("phone_number"),
                    "confidence": email_data.get("confidence", 0),
                    "sources": email_data.get("sources", []),
                    "verification": email_data.get("verification")
                })

            output = {
                "domain": result.get("domain"),
                "emails": emails,
                "total": result.get("total", len(emails)),
                "pattern": result.get("pattern"),
                "organization": result.get("organization"),
                "country": result.get("country")
            }

            # Cache result
            self.search_cache.put(cache_key, output)

            logger.info(f"Hunter domain_search: {domain} -> {len(emails)} emails")

            return output

        except httpx.HTTPStatusError as e:
            logger.error(f"Hunter domain_search error: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Hunter domain_search exception: {e}")
            return None

    async def email_count(self, domain: str) -> int:
        """
        Get count of emails found for domain (doesn't use quota)

        Returns:
            Number of emails available for the domain
        """
        if not self.api_key:
            return 0

        try:
            response = await self.client.get(
                f"{self.BASE_URL}/email-count",
                params={
                    "domain": domain,
                    "api_key": self.api_key
                }
            )
            response.raise_for_status()

            data = response.json()
            return data.get("data", {}).get("total", 0)

        except Exception as e:
            logger.error(f"Hunter email_count exception: {e}")
            return 0

    def stats(self) -> Dict[str, Any]:
        """Get usage statistics"""
        return {
            "requests_made": self.requests_made,
            "verifications_used": self.verifications_used,
            "searches_used": self.searches_used,
            "verification_cache_stats": self.verification_cache.stats(),
            "search_cache_stats": self.search_cache.stats()
        }

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()


# Usage Example:
"""
hunter = HunterClient(api_key="your_api_key")

# Verify email deliverability
verification = await hunter.verify_email("john.doe@example.com")
if verification:
    if verification['is_deliverable']:
        print(f"✓ Valid email (score: {verification['score']})")
    else:
        print(f"✗ Invalid: {verification['result']}")

# Find email from name + domain
email_result = await hunter.find_email(
    first_name="John",
    last_name="Doe",
    domain="example.com"
)
if email_result:
    print(f"Found: {email_result['email']} (confidence: {email_result['score']}%)")

# Search all emails for domain
domain_results = await hunter.domain_search("example.com", limit=50)
if domain_results:
    print(f"Found {domain_results['total']} emails at {domain_results['domain']}")
    print(f"Email pattern: {domain_results['pattern']}")

    for email in domain_results['emails'][:10]:  # Show first 10
        print(f"- {email['email']} ({email['position']})")

# Check how many emails available (free, doesn't use quota)
count = await hunter.email_count("example.com")
print(f"Available emails for example.com: {count}")

# Get usage stats
print(hunter.stats())
# {
#     'requests_made': 42,
#     'verifications_used': 15,
#     'searches_used': 27,
#     'verification_cache_stats': {...},
#     'search_cache_stats': {...}
# }

await hunter.close()
"""
