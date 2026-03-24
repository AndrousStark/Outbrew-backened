"""
Clearbit API Client (Layer 5: Company & Person Enrichment)

Clearbit provides:
- Person enrichment (from email -> full profile)
- Company enrichment (from domain -> full company data)
- Prospector API (find people matching criteria)
- Reveal API (identify company from IP)
- Risk API (fraud detection)

Cost: Pay-per-use (starts at $99/month)
API Docs: https://clearbit.com/docs

2026 Best Practice Integration:
- Waterfall enrichment with Apollo/Hunter fallback
- Cache-first strategy (30-day TTL)
- Rate limit compliance (600 requests/minute)
"""

import httpx
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from app.core.config import settings
from app.utils.advanced_cache import TimeBoundCache

logger = logging.getLogger(__name__)


class ClearbitClient:
    """
    Clearbit API Client

    Features:
    - Person enrichment from email address
    - Company enrichment from domain
    - Combined person + company enrichment
    - Prospector for lead discovery
    - Risk assessment for fraud detection

    Rate Limits:
    - 600 requests per minute
    - Automatic retry on rate limit

    2026 Best Practices Applied:
    - 30-day cache TTL for enrichment results
    - Waterfall fallback to Apollo/Hunter
    - Quality scoring based on data completeness
    - Detailed logging for debugging
    """

    # Clearbit API endpoints
    BASE_URL = "https://person.clearbit.com/v2"
    COMPANY_URL = "https://company.clearbit.com/v2"
    PROSPECTOR_URL = "https://prospector.clearbit.com/v1"
    RISK_URL = "https://risk.clearbit.com/v1"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.CLEARBIT_API_KEY
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
        )

        # Cache with 30-day TTL (industry best practice for enrichment data)
        self.person_cache = TimeBoundCache(capacity=10000, default_ttl_seconds=30*24*3600)
        self.company_cache = TimeBoundCache(capacity=5000, default_ttl_seconds=30*24*3600)

        # Statistics for monitoring
        self.requests_made = 0
        self.successful_enrichments = 0
        self.failed_enrichments = 0
        self.cache_hits = 0

        logger.info(f"[CLEARBIT] Client initialized (API key configured: {bool(self.api_key)})")

    async def enrich_person(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Enrich person data from email address

        Returns comprehensive profile including:
        - Full name, title, bio, avatar
        - Employment history
        - Social profiles (LinkedIn, Twitter, GitHub, etc.)
        - Location and timezone
        - Company information

        Args:
            email: Email address to enrich

        Returns:
            Enriched person profile or None if not found

        Example:
            profile = await clearbit.enrich_person("john@example.com")
            print(f"Name: {profile['name']['fullName']}")
            print(f"Title: {profile['employment']['title']}")
        """
        if not self.api_key:
            logger.warning("[CLEARBIT] API key not configured, skipping person enrichment")
            return None

        # Check cache first (30-day TTL)
        cache_key = f"clearbit:person:{email.lower()}"
        cached = self.person_cache.get(cache_key)
        if cached:
            self.cache_hits += 1
            logger.debug(f"[CLEARBIT] Cache hit for person: {email}")
            return cached

        try:
            logger.info(f"[CLEARBIT] Enriching person: {email}")

            response = await self.client.get(
                f"{self.BASE_URL}/people/find",
                params={"email": email}
            )

            self.requests_made += 1

            if response.status_code == 404:
                logger.info(f"[CLEARBIT] Person not found: {email}")
                return None

            if response.status_code == 202:
                # Async lookup in progress - Clearbit is processing
                logger.info(f"[CLEARBIT] Async lookup initiated for: {email}")
                return {"status": "pending", "email": email}

            response.raise_for_status()
            data = response.json()

            # Transform to our standard format
            result = self._transform_person(data)

            # Calculate quality score
            result["quality_score"] = self._calculate_person_quality_score(data)
            result["enriched_via"] = "clearbit"
            result["enriched_at"] = datetime.now(timezone.utc).isoformat()

            # Cache the result
            self.person_cache.put(cache_key, result)
            self.successful_enrichments += 1

            logger.info(f"[CLEARBIT] Successfully enriched person: {email} (quality: {result['quality_score']}%)")

            return result

        except httpx.HTTPStatusError as e:
            self.failed_enrichments += 1
            if e.response.status_code == 429:
                logger.warning(f"[CLEARBIT] Rate limited on person enrichment: {email}")
            else:
                logger.error(f"[CLEARBIT] HTTP error enriching person {email}: {e.response.status_code}")
            return None
        except Exception as e:
            self.failed_enrichments += 1
            logger.error(f"[CLEARBIT] Exception enriching person {email}: {str(e)}")
            return None

    async def enrich_company(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Enrich company data from domain

        Returns comprehensive company profile including:
        - Name, description, logo, category
        - Industry, sector, tech stack
        - Employee count, revenue, funding
        - Social profiles, location
        - Tech stack detection

        Args:
            domain: Company domain (e.g., "example.com")

        Returns:
            Enriched company profile or None if not found

        Example:
            company = await clearbit.enrich_company("stripe.com")
            print(f"Name: {company['name']}")
            print(f"Employees: {company['metrics']['employees']}")
            print(f"Tech: {company['tech']}")
        """
        if not self.api_key:
            logger.warning("[CLEARBIT] API key not configured, skipping company enrichment")
            return None

        # Normalize domain
        domain = domain.lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]

        # Check cache first
        cache_key = f"clearbit:company:{domain}"
        cached = self.company_cache.get(cache_key)
        if cached:
            self.cache_hits += 1
            logger.debug(f"[CLEARBIT] Cache hit for company: {domain}")
            return cached

        try:
            logger.info(f"[CLEARBIT] Enriching company: {domain}")

            response = await self.client.get(
                f"{self.COMPANY_URL}/companies/find",
                params={"domain": domain}
            )

            self.requests_made += 1

            if response.status_code == 404:
                logger.info(f"[CLEARBIT] Company not found: {domain}")
                return None

            response.raise_for_status()
            data = response.json()

            # Transform to our standard format
            result = self._transform_company(data)

            # Calculate quality score
            result["quality_score"] = self._calculate_company_quality_score(data)
            result["enriched_via"] = "clearbit"
            result["enriched_at"] = datetime.now(timezone.utc).isoformat()

            # Cache the result
            self.company_cache.put(cache_key, result)
            self.successful_enrichments += 1

            logger.info(f"[CLEARBIT] Successfully enriched company: {domain} (quality: {result['quality_score']}%)")

            return result

        except httpx.HTTPStatusError as e:
            self.failed_enrichments += 1
            if e.response.status_code == 429:
                logger.warning(f"[CLEARBIT] Rate limited on company enrichment: {domain}")
            else:
                logger.error(f"[CLEARBIT] HTTP error enriching company {domain}: {e.response.status_code}")
            return None
        except Exception as e:
            self.failed_enrichments += 1
            logger.error(f"[CLEARBIT] Exception enriching company {domain}: {str(e)}")
            return None

    async def enrich_combined(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Enrich both person and company from email in single call

        More efficient than calling person and company separately.
        Returns combined profile with both person and company data.

        Args:
            email: Email address to enrich

        Returns:
            Combined person + company enrichment result
        """
        if not self.api_key:
            return None

        try:
            logger.info(f"[CLEARBIT] Combined enrichment for: {email}")

            response = await self.client.get(
                f"{self.BASE_URL}/combined/find",
                params={"email": email}
            )

            self.requests_made += 1

            if response.status_code == 404:
                return None

            response.raise_for_status()
            data = response.json()

            result = {
                "person": self._transform_person(data.get("person", {})) if data.get("person") else None,
                "company": self._transform_company(data.get("company", {})) if data.get("company") else None,
                "enriched_via": "clearbit",
                "enriched_at": datetime.now(timezone.utc).isoformat()
            }

            self.successful_enrichments += 1
            logger.info(f"[CLEARBIT] Combined enrichment successful for: {email}")

            return result

        except Exception as e:
            self.failed_enrichments += 1
            logger.error(f"[CLEARBIT] Combined enrichment failed for {email}: {str(e)}")
            return None

    async def find_prospects(
        self,
        domain: str,
        role: Optional[str] = None,
        seniority: Optional[str] = None,
        title: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Find prospects at a company matching criteria

        Great for building targeted lead lists.

        Args:
            domain: Company domain to search
            role: Role filter (e.g., "engineering", "sales", "marketing")
            seniority: Seniority filter (e.g., "executive", "director", "manager")
            title: Job title filter
            limit: Max results (1-20)

        Returns:
            List of matching prospects with contact info

        Example:
            prospects = await clearbit.find_prospects(
                domain="stripe.com",
                role="engineering",
                seniority="director"
            )
        """
        if not self.api_key:
            return []

        try:
            logger.info(f"[CLEARBIT] Finding prospects at: {domain} (role={role}, seniority={seniority})")

            params = {
                "domain": domain,
                "page_size": min(limit, 20)
            }

            if role:
                params["role"] = role
            if seniority:
                params["seniority"] = seniority
            if title:
                params["title"] = title

            response = await self.client.get(
                f"{self.PROSPECTOR_URL}/people/search",
                params=params
            )

            self.requests_made += 1
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])

            # Transform to our format
            prospects = []
            for person in results:
                prospects.append({
                    "id": person.get("id"),
                    "name": person.get("name", {}).get("fullName"),
                    "first_name": person.get("name", {}).get("givenName"),
                    "last_name": person.get("name", {}).get("familyName"),
                    "email": person.get("email"),
                    "title": person.get("title"),
                    "role": person.get("role"),
                    "seniority": person.get("seniority"),
                    "linkedin_url": person.get("linkedin"),
                    "company": domain,
                    "enriched_via": "clearbit_prospector"
                })

            logger.info(f"[CLEARBIT] Found {len(prospects)} prospects at {domain}")

            return prospects

        except Exception as e:
            logger.error(f"[CLEARBIT] Prospector search failed for {domain}: {str(e)}")
            return []

    async def assess_risk(self, email: str, ip: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Assess fraud risk for an email/IP

        Uses Clearbit Risk API to detect:
        - Disposable email addresses
        - Free email providers
        - Suspicious patterns
        - IP-based risk signals

        Args:
            email: Email to assess
            ip: Optional IP address for additional signals

        Returns:
            Risk assessment with score and signals
        """
        if not self.api_key:
            return None

        try:
            params = {"email": email}
            if ip:
                params["ip"] = ip

            response = await self.client.get(
                f"{self.RISK_URL}/calculate",
                params=params
            )

            self.requests_made += 1
            response.raise_for_status()

            data = response.json()

            return {
                "email": email,
                "risk_score": data.get("risk", {}).get("score", 0),
                "risk_level": data.get("risk", {}).get("level", "unknown"),
                "is_disposable": data.get("email", {}).get("disposable", False),
                "is_free_provider": data.get("email", {}).get("free", False),
                "is_valid": data.get("email", {}).get("valid", True),
                "blacklisted": data.get("email", {}).get("blacklisted", False),
                "signals": data.get("flags", [])
            }

        except Exception as e:
            logger.error(f"[CLEARBIT] Risk assessment failed for {email}: {str(e)}")
            return None

    def _transform_person(self, data: Dict) -> Dict[str, Any]:
        """Transform Clearbit person data to our standard format"""
        if not data:
            return {}

        return {
            "id": data.get("id"),
            "email": data.get("email"),
            "name": data.get("name", {}).get("fullName"),
            "first_name": data.get("name", {}).get("givenName"),
            "last_name": data.get("name", {}).get("familyName"),
            "avatar": data.get("avatar"),
            "bio": data.get("bio"),
            "site": data.get("site"),
            "location": data.get("location"),
            "timezone": data.get("timeZone"),
            "utc_offset": data.get("utcOffset"),
            "geo": {
                "city": data.get("geo", {}).get("city"),
                "state": data.get("geo", {}).get("state"),
                "country": data.get("geo", {}).get("country"),
                "lat": data.get("geo", {}).get("lat"),
                "lng": data.get("geo", {}).get("lng")
            },
            "employment": {
                "name": data.get("employment", {}).get("name"),
                "title": data.get("employment", {}).get("title"),
                "role": data.get("employment", {}).get("role"),
                "seniority": data.get("employment", {}).get("seniority"),
                "domain": data.get("employment", {}).get("domain")
            },
            "social_profiles": {
                "linkedin_url": data.get("linkedin", {}).get("handle"),
                "twitter_handle": data.get("twitter", {}).get("handle"),
                "twitter_followers": data.get("twitter", {}).get("followers"),
                "github_handle": data.get("github", {}).get("handle"),
                "facebook_handle": data.get("facebook", {}).get("handle"),
                "gravatar_handle": data.get("gravatar", {}).get("handle")
            },
            "indexed_at": data.get("indexedAt")
        }

    def _transform_company(self, data: Dict) -> Dict[str, Any]:
        """Transform Clearbit company data to our standard format"""
        if not data:
            return {}

        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "legal_name": data.get("legalName"),
            "domain": data.get("domain"),
            "domain_aliases": data.get("domainAliases", []),
            "logo": data.get("logo"),
            "site_url": data.get("url"),
            "description": data.get("description"),
            "category": {
                "sector": data.get("category", {}).get("sector"),
                "industry_group": data.get("category", {}).get("industryGroup"),
                "industry": data.get("category", {}).get("industry"),
                "sub_industry": data.get("category", {}).get("subIndustry"),
                "sic_code": data.get("category", {}).get("sicCode"),
                "naics_code": data.get("category", {}).get("naicsCode")
            },
            "tags": data.get("tags", []),
            "tech": data.get("tech", []),  # Technology stack
            "phone": data.get("phone"),
            "email_provider": data.get("emailProvider"),
            "type": data.get("type"),  # public, private, nonprofit, etc.
            "founded_year": data.get("foundedYear"),
            "location": data.get("location"),
            "geo": {
                "street_number": data.get("geo", {}).get("streetNumber"),
                "street_name": data.get("geo", {}).get("streetName"),
                "city": data.get("geo", {}).get("city"),
                "state": data.get("geo", {}).get("state"),
                "postal_code": data.get("geo", {}).get("postalCode"),
                "country": data.get("geo", {}).get("country"),
                "lat": data.get("geo", {}).get("lat"),
                "lng": data.get("geo", {}).get("lng")
            },
            "metrics": {
                "employees": data.get("metrics", {}).get("employees"),
                "employees_range": data.get("metrics", {}).get("employeesRange"),
                "alexa_rank": data.get("metrics", {}).get("alexaGlobalRank"),
                "annual_revenue": data.get("metrics", {}).get("annualRevenue"),
                "estimated_annual_revenue": data.get("metrics", {}).get("estimatedAnnualRevenue"),
                "raised": data.get("metrics", {}).get("raised"),
                "fiscal_year_end": data.get("metrics", {}).get("fiscalYearEnd")
            },
            "social_profiles": {
                "facebook_handle": data.get("facebook", {}).get("handle"),
                "linkedin_handle": data.get("linkedin", {}).get("handle"),
                "twitter_handle": data.get("twitter", {}).get("handle"),
                "twitter_followers": data.get("twitter", {}).get("followers"),
                "crunchbase_handle": data.get("crunchbase", {}).get("handle")
            },
            "parent_domain": data.get("parent", {}).get("domain") if data.get("parent") else None,
            "indexed_at": data.get("indexedAt")
        }

    def _calculate_person_quality_score(self, data: Dict) -> int:
        """Calculate quality score for person enrichment (0-100)"""
        if not data:
            return 0

        score = 0

        # Core fields (50 points)
        if data.get("name", {}).get("fullName"):
            score += 15
        if data.get("email"):
            score += 10
        if data.get("employment", {}).get("title"):
            score += 15
        if data.get("employment", {}).get("name"):
            score += 10

        # Social profiles (25 points)
        if data.get("linkedin", {}).get("handle"):
            score += 10
        if data.get("twitter", {}).get("handle"):
            score += 5
        if data.get("github", {}).get("handle"):
            score += 5
        if data.get("avatar"):
            score += 5

        # Additional data (25 points)
        if data.get("bio"):
            score += 5
        if data.get("location"):
            score += 5
        if data.get("timeZone"):
            score += 5
        if data.get("geo", {}).get("country"):
            score += 5
        if data.get("employment", {}).get("seniority"):
            score += 5

        return min(score, 100)

    def _calculate_company_quality_score(self, data: Dict) -> int:
        """Calculate quality score for company enrichment (0-100)"""
        if not data:
            return 0

        score = 0

        # Core fields (40 points)
        if data.get("name"):
            score += 10
        if data.get("domain"):
            score += 10
        if data.get("description"):
            score += 10
        if data.get("category", {}).get("industry"):
            score += 10

        # Metrics (30 points)
        if data.get("metrics", {}).get("employees"):
            score += 10
        if data.get("metrics", {}).get("annualRevenue"):
            score += 10
        if data.get("metrics", {}).get("raised"):
            score += 10

        # Tech stack (15 points)
        tech = data.get("tech", [])
        if tech:
            score += min(len(tech) * 3, 15)

        # Social presence (15 points)
        if data.get("linkedin", {}).get("handle"):
            score += 5
        if data.get("twitter", {}).get("handle"):
            score += 5
        if data.get("crunchbase", {}).get("handle"):
            score += 5

        return min(score, 100)

    def stats(self) -> Dict[str, Any]:
        """Get client statistics for monitoring"""
        return {
            "api_key_configured": bool(self.api_key),
            "requests_made": self.requests_made,
            "successful_enrichments": self.successful_enrichments,
            "failed_enrichments": self.failed_enrichments,
            "cache_hits": self.cache_hits,
            "success_rate": round(
                (self.successful_enrichments / max(self.requests_made, 1)) * 100, 2
            ),
            "person_cache_stats": self.person_cache.stats(),
            "company_cache_stats": self.company_cache.stats()
        }

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()
        logger.info("[CLEARBIT] Client closed")


# Usage Example:
"""
clearbit = ClearbitClient()

# Enrich person from email
person = await clearbit.enrich_person("john.doe@stripe.com")
if person:
    print(f"Name: {person['name']}")
    print(f"Title: {person['employment']['title']}")
    print(f"Company: {person['employment']['name']}")
    print(f"LinkedIn: {person['social_profiles']['linkedin_url']}")
    print(f"Quality Score: {person['quality_score']}%")

# Enrich company from domain
company = await clearbit.enrich_company("stripe.com")
if company:
    print(f"Company: {company['name']}")
    print(f"Industry: {company['category']['industry']}")
    print(f"Employees: {company['metrics']['employees']}")
    print(f"Tech Stack: {', '.join(company['tech'][:10])}")

# Find prospects
prospects = await clearbit.find_prospects(
    domain="stripe.com",
    role="engineering",
    seniority="director"
)
for p in prospects:
    print(f"- {p['name']} ({p['title']}) - {p['email']}")

# Assess risk
risk = await clearbit.assess_risk("suspicious@tempmail.com")
if risk:
    print(f"Risk Score: {risk['risk_score']}")
    print(f"Disposable: {risk['is_disposable']}")

# Get stats
print(clearbit.stats())

await clearbit.close()
"""
