"""
Enrichment Orchestrator (Layer 5: API Enrichment)

Intelligently coordinates multiple enrichment APIs:
- Apollo.io (people search, email finding, company data)
- Hunter.io (email verification, domain search)
- Priority-based enrichment strategy
- Cost optimization (cache-first, fallback chain)
- Quality scoring

Strategy:
1. Check cache first (avoid API costs)
2. Try FREE enrichment methods
3. Use paid APIs only when necessary
4. Verify all emails with Hunter
5. Combine data from multiple sources
6. Calculate confidence scores
"""

import logging
from typing import Dict, Any, Optional, List
from app.services.apollo_client import ApolloClient
from app.services.hunter_client import HunterClient
from app.utils.advanced_cache import LRUCache

logger = logging.getLogger(__name__)


def calculate_backoff_delay(
    attempt: int, base_delay: int = 2, max_delay: int = 30
) -> int:
    """Calculate exponential backoff delay with jitter.

    Attempt 1: 2s
    Attempt 2: 5s
    Attempt 3: 10s
    """
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    # Add random jitter ±20%
    import random

    jitter = delay * 0.2 * (2 * random.random() - 1)
    return max(1, int(delay + jitter))


class EnrichmentOrchestrator:
    """
    Intelligent API enrichment orchestration

    Features:
    - Multi-source data aggregation
    - Cost-optimized API usage
    - Automatic fallback chains
    - Quality scoring and validation
    - Request batching and rate limiting

    Usage Priority (lowest cost → highest accuracy):
    1. Cache lookup (FREE)
    2. Basic extraction (FREE - already have name/company)
    3. Hunter domain search (if have domain, find emails)
    4. Apollo search (find missing data)
    5. Hunter verification (verify emails)
    6. Apollo enrichment (get full profile)
    """

    def __init__(
        self, apollo_api_key: Optional[str] = None, hunter_api_key: Optional[str] = None
    ):
        # Initialize API clients
        self.apollo = ApolloClient(api_key=apollo_api_key) if apollo_api_key else None
        self.hunter = HunterClient(api_key=hunter_api_key) if hunter_api_key else None

        # Master cache (combines all enrichment data)
        # Use a 30-day TTL to avoid repeated paid lookups while keeping data reasonably fresh
        self.master_cache = LRUCache(capacity=20000, ttl_seconds=30 * 24 * 3600)

        # Statistics
        self.stats = {
            "total_enrichments": 0,
            "cache_hits": 0,
            "apollo_calls": 0,
            "hunter_calls": 0,
            "fully_enriched": 0,
            "partially_enriched": 0,
            "failed": 0,
        }

    async def enrich_record(
        self, record: Dict[str, Any], strategies: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Enrich extraction record with maximum data

        Args:
            record: Basic extracted data (name, email, company, etc.)
            strategies: List of enrichment strategies to try
                       Default: ["cache", "hunter_verify", "apollo_enrich"]

        Returns:
            Enriched record with:
            - Verified email
            - Phone number
            - LinkedIn URL
            - Title/position
            - Company details
            - Technologies
            - Confidence scores
        """
        strategies = strategies or ["cache", "hunter_verify", "apollo_enrich"]

        self.stats["total_enrichments"] += 1

        # Extract key fields
        email = record.get("email", "").strip()
        name = record.get("name", "").strip()
        company = record.get("company", "").strip()

        # Create cache key
        cache_key = self._make_cache_key(email, name, company)

        enriched = record.copy()
        enriched["enrichment_sources"] = []
        enriched["enrichment_quality"] = 0.0

        # Strategy 1: Check cache (FREE!)
        if "cache" in strategies:
            cached = self.master_cache.get(cache_key)
            if cached:
                self.stats["cache_hits"] += 1
                logger.debug(f"Cache hit for: {email or name}")
                return cached

        # Strategy 2: Hunter email verification (if have email)
        if "hunter_verify" in strategies and email and self.hunter:
            verification = await self.hunter.verify_email(email)
            if verification:
                enriched["email_verified"] = verification["is_deliverable"]
                enriched["email_score"] = verification["score"]
                enriched["email_result"] = verification["result"]
                enriched["is_disposable"] = verification["is_disposable"]
                enriched["is_role_email"] = verification["is_role"]
                enriched["is_free_email"] = verification["is_free"]
                enriched["enrichment_sources"].append("hunter_verify")
                self.stats["hunter_calls"] += 1

                # If email is not deliverable, try to find correct one
                if not verification["is_deliverable"] and name and company:
                    await self._find_correct_email(enriched, name, company)

        # Strategy 3: Apollo enrichment (if have email)
        if "apollo_enrich" in strategies and email and self.apollo:
            apollo_data = await self.apollo.enrich_person(email)
            if apollo_data:
                # Merge Apollo data
                enriched["phone"] = apollo_data.get("phone") or enriched.get("phone")
                enriched["linkedin_url"] = apollo_data.get(
                    "linkedin_url"
                ) or enriched.get("linkedin_url")
                enriched["title"] = apollo_data.get("title") or enriched.get("title")
                enriched["seniority"] = apollo_data.get("seniority")
                enriched["departments"] = apollo_data.get("departments", [])

                # Company data
                if apollo_data.get("company"):
                    enriched["company_data"] = apollo_data["company"]

                enriched["enrichment_sources"].append("apollo")
                self.stats["apollo_calls"] += 1

        # Strategy 4: Find missing email (if have name + company but no email)
        if not email and name and company:
            if "hunter_find" in strategies and self.hunter:
                await self._find_email_hunter(enriched, name, company)
            elif "apollo_find" in strategies and self.apollo:
                await self._find_email_apollo(enriched, name, company)

        # Calculate enrichment quality score
        enriched["enrichment_quality"] = self._calculate_enrichment_quality(enriched)

        # Update statistics
        if enriched["enrichment_quality"] >= 0.8:
            self.stats["fully_enriched"] += 1
        elif enriched["enrichment_quality"] >= 0.4:
            self.stats["partially_enriched"] += 1
        else:
            self.stats["failed"] += 1

        # Cache enriched result
        self.master_cache.put(cache_key, enriched)

        return enriched

    async def _find_correct_email(
        self, enriched: Dict, name: str, company: str
    ) -> None:
        """Try to find correct email if verification failed"""
        # Extract name parts
        name_parts = name.split()
        if len(name_parts) < 2:
            return

        first_name, last_name = name_parts[0], name_parts[-1]

        # Extract domain from company
        domain = self._extract_domain(company)
        if not domain:
            return

        # Try Hunter first (faster)
        if self.hunter:
            result = await self.hunter.find_email(first_name, last_name, domain)
            if result and result["score"] >= 70:
                enriched["email"] = result["email"]
                enriched["email_score"] = result["score"]
                enriched["email_source"] = "hunter_find"
                enriched["enrichment_sources"].append("hunter_find")
                self.stats["hunter_calls"] += 1
                return

        # Try Apollo as fallback
        if self.apollo:
            result = await self.apollo.find_email(first_name, last_name, domain)
            if result and result["score"] >= 70:
                enriched["email"] = result["email"]
                enriched["email_score"] = result["score"]
                enriched["email_source"] = "apollo_find"
                enriched["enrichment_sources"].append("apollo_find")
                self.stats["apollo_calls"] += 1

    async def _find_email_hunter(self, enriched: Dict, name: str, company: str) -> None:
        """Find email using Hunter"""
        name_parts = name.split()
        if len(name_parts) < 2:
            return

        first_name, last_name = name_parts[0], name_parts[-1]
        domain = self._extract_domain(company)

        if not domain:
            return

        result = await self.hunter.find_email(first_name, last_name, domain)
        if result:
            enriched["email"] = result["email"]
            enriched["email_score"] = result["score"]
            enriched["position"] = result.get("position")
            enriched["phone"] = result.get("phone_number")
            enriched["linkedin_url"] = result.get("linkedin_url")
            enriched["enrichment_sources"].append("hunter_find")
            self.stats["hunter_calls"] += 1

    async def _find_email_apollo(self, enriched: Dict, name: str, company: str) -> None:
        """Find email using Apollo"""
        name_parts = name.split()
        if len(name_parts) < 2:
            return

        first_name, last_name = name_parts[0], name_parts[-1]
        domain = self._extract_domain(company)

        if not domain:
            return

        result = await self.apollo.find_email(first_name, last_name, domain)
        if result:
            enriched["email"] = result["email"]
            enriched["email_score"] = result["score"]
            enriched["enrichment_sources"].append("apollo_find")
            self.stats["apollo_calls"] += 1

    def _extract_domain(self, company: str) -> Optional[str]:
        """Extract domain from company name"""
        import re
        from urllib.parse import urlparse

        if not company:
            return None

        # If already a URL/domain
        if "." in company and " " not in company:
            parsed = urlparse(
                f"https://{company}" if not company.startswith("http") else company
            )
            return parsed.netloc

        # Try to guess domain (company name → domain.com)
        clean_name = re.sub(r"[^a-zA-Z0-9]", "", company.lower())
        return f"{clean_name}.com"  # Simple heuristic

    def _make_cache_key(self, email: str, name: str, company: str) -> str:
        """Create cache key from identifiers"""
        parts = [
            email.lower() if email else "",
            name.lower() if name else "",
            company.lower() if company else "",
        ]
        return ":".join(p for p in parts if p)

    def _calculate_enrichment_quality(self, record: Dict) -> float:
        """
        Calculate enrichment quality score (0.0 - 1.0)

        Factors:
        - Email verified: +0.30
        - Phone found: +0.15
        - LinkedIn found: +0.15
        - Title found: +0.15
        - Company data found: +0.15
        - Multiple sources: +0.10
        """
        score = 0.0

        # Email verification
        if record.get("email_verified"):
            score += 0.30
        elif record.get("email"):
            score += 0.15

        # Contact info
        if record.get("phone"):
            score += 0.15

        if record.get("linkedin_url"):
            score += 0.15

        # Professional info
        if record.get("title"):
            score += 0.15

        # Company data
        if record.get("company_data"):
            score += 0.15

        # Multiple sources bonus
        sources = len(record.get("enrichment_sources", []))
        if sources >= 2:
            score += 0.10

        return min(round(score, 2), 1.0)

    def get_stats(self) -> Dict[str, Any]:
        """Get enrichment statistics"""
        total = self.stats["total_enrichments"]

        stats = self.stats.copy()
        if total > 0:
            stats["cache_hit_rate"] = round((self.stats["cache_hits"] / total) * 100, 2)
            stats["success_rate"] = round(
                (
                    (self.stats["fully_enriched"] + self.stats["partially_enriched"])
                    / total
                )
                * 100,
                2,
            )

        if self.apollo:
            stats["apollo_stats"] = self.apollo.stats()

        if self.hunter:
            stats["hunter_stats"] = self.hunter.stats()

        return stats

    async def close(self):
        """Close API clients"""
        if self.apollo:
            await self.apollo.close()
        if self.hunter:
            await self.hunter.close()


# Usage Example:
"""
orchestrator = EnrichmentOrchestrator(
    apollo_api_key="your_apollo_key",
    hunter_api_key="your_hunter_key"
)

# Basic extracted record
record = {
    "name": "John Doe",
    "email": "john.doe@example.com",
    "company": "Example Inc",
    "title": "Software Engineer"
}

# Enrich with all available sources
enriched = await orchestrator.enrich_record(record)

print(f"Enrichment quality: {enriched['enrichment_quality']}")
print(f"Sources used: {enriched['enrichment_sources']}")
print(f"Email verified: {enriched.get('email_verified', False)}")
print(f"Phone: {enriched.get('phone', 'Not found')}")
print(f"LinkedIn: {enriched.get('linkedin_url', 'Not found')}")

# Get statistics
stats = orchestrator.get_stats()
print(f"Total enrichments: {stats['total_enrichments']}")
print(f"Cache hit rate: {stats['cache_hit_rate']}%")
print(f"Success rate: {stats['success_rate']}%")
print(f"Apollo calls: {stats['apollo_calls']}")
print(f"Hunter calls: {stats['hunter_calls']}")

await orchestrator.close()
"""
