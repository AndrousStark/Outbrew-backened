"""
Apollo.io API Client (Layer 5: API Enrichment)

Apollo.io provides:
- People search with advanced filters
- Email finding (70-95% accuracy)
- Email verification
- Company data enrichment
- Tech stack detection
- Job changes tracking

Cost: $49/month for 10,000 credits (Basic plan)
API Docs: https://apolloio.github.io/apollo-api-docs/
"""

import httpx
import logging
from typing import List, Dict, Any, Optional
from app.core.config import settings
from app.utils.advanced_cache import TimeBoundCache

logger = logging.getLogger(__name__)


class ApolloClient:
    """
    Apollo.io API Client

    Features:
    - People search with filters (job title, location, company size, etc.)
    - Email finding from name + company
    - Email verification and scoring
    - Company enrichment
    - Contact data export

    Rate Limits:
    - 60 requests per minute
    - 10,000 credits per month (Basic plan)
    """

    BASE_URL = "https://api.apollo.io/v1"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.APOLLO_API_KEY
        self.client = httpx.AsyncClient(timeout=30.0)

        # Cache API responses (10 minute TTL)
        self.cache = TimeBoundCache(capacity=5000, default_ttl_seconds=600)

        # Statistics
        self.credits_used = 0
        self.requests_made = 0

    async def search_people(
        self,
        job_titles: Optional[List[str]] = None,
        locations: Optional[List[str]] = None,
        industries: Optional[List[str]] = None,
        company_sizes: Optional[List[str]] = None,
        page: int = 1,
        per_page: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search for people matching criteria

        Args:
            job_titles: List of job titles to search (e.g., ["HR Manager", "Recruiter"])
            locations: List of locations (e.g., ["Luxembourg", "Germany"])
            industries: List of industries (e.g., ["Software", "Finance"])
            company_sizes: List of company sizes (e.g., ["1-10", "11-50", "51-200"])
            page: Page number (1-indexed)
            per_page: Results per page (max 100)

        Returns:
            List of person records with contact info
        """
        if not self.api_key:
            logger.warning("Apollo.io API key not configured, skipping search")
            return []

        # Build cache key
        cache_key = f"apollo:people:{job_titles}:{locations}:{industries}:{page}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        # Build request body
        body = {
            "api_key": self.api_key,
            "page": page,
            "per_page": per_page,
        }

        # Add filters
        if job_titles:
            body["person_titles"] = job_titles

        if locations:
            body["person_locations"] = locations

        if industries:
            body["organization_industries"] = industries

        if company_sizes:
            body["organization_num_employees_ranges"] = company_sizes

        try:
            response = await self.client.post(
                f"{self.BASE_URL}/mixed_people/search",
                json=body
            )
            response.raise_for_status()

            data = response.json()
            self.requests_made += 1

            # Extract people from response
            people = data.get("people", [])

            # Transform to our format
            results = []
            for person in people:
                result = self._transform_person(person)
                results.append(result)

            # Update credits used
            self.credits_used += data.get("credits_used", 1)

            # Cache results
            self.cache.put(cache_key, results)

            logger.info(f"Apollo search: Found {len(results)} people, used {data.get('credits_used', 1)} credits")

            return results

        except httpx.HTTPStatusError as e:
            logger.error(f"Apollo API error: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Apollo API exception: {e}")
            return []

    async def enrich_person(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Enrich person data from email address

        Returns:
            Full profile with 50+ data points including:
            - Name, title, seniority, department
            - Email, phone, LinkedIn
            - Company details
            - Employment history
            - Technologies used
        """
        if not self.api_key:
            return None

        # Check cache
        cache_key = f"apollo:person:{email}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        try:
            response = await self.client.post(
                f"{self.BASE_URL}/people/match",
                json={
                    "api_key": self.api_key,
                    "email": email,
                    "reveal_personal_emails": True,
                    "reveal_phone_number": True
                }
            )
            response.raise_for_status()

            data = response.json()
            self.requests_made += 1
            self.credits_used += 1

            person = data.get("person")
            if not person:
                return None

            result = self._transform_person(person)

            # Cache result
            self.cache.put(cache_key, result)

            return result

        except httpx.HTTPStatusError as e:
            logger.error(f"Apollo enrich error: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Apollo enrich exception: {e}")
            return None

    async def find_email(
        self,
        first_name: str,
        last_name: str,
        domain: str
    ) -> Optional[Dict[str, Any]]:
        """
        Find email address from name and company domain

        Args:
            first_name: Person's first name
            last_name: Person's last name
            domain: Company domain (e.g., "example.com")

        Returns:
            {
                "email": "john.doe@example.com",
                "confidence": "high",  # high, medium, low
                "score": 95  # 0-100
            }
        """
        if not self.api_key:
            return None

        # Check cache
        cache_key = f"apollo:email:{first_name}:{last_name}:{domain}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        try:
            response = await self.client.post(
                f"{self.BASE_URL}/people/match",
                json={
                    "api_key": self.api_key,
                    "first_name": first_name,
                    "last_name": last_name,
                    "organization_domain": domain,
                    "reveal_personal_emails": True
                }
            )
            response.raise_for_status()

            data = response.json()
            self.requests_made += 1
            self.credits_used += 1

            person = data.get("person")
            if not person or not person.get("email"):
                return None

            result = {
                "email": person["email"],
                "confidence": person.get("email_confidence", "unknown"),
                "score": person.get("email_status", {}).get("score", 0),
                "is_verified": person.get("email_status", {}).get("verified", False)
            }

            # Cache result
            self.cache.put(cache_key, result)

            return result

        except Exception as e:
            logger.error(f"Apollo find_email exception: {e}")
            return None

    async def enrich_company(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Enrich company data from domain

        Returns:
            Company details including:
            - Name, website, phone
            - Industry, keywords, description
            - Employee count, revenue range
            - Technologies used
            - Social profiles
            - Founded year, funding info
        """
        if not self.api_key:
            return None

        # Check cache
        cache_key = f"apollo:company:{domain}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        try:
            response = await self.client.post(
                f"{self.BASE_URL}/organizations/enrich",
                json={
                    "api_key": self.api_key,
                    "domain": domain
                }
            )
            response.raise_for_status()

            data = response.json()
            self.requests_made += 1
            self.credits_used += 1

            organization = data.get("organization")
            if not organization:
                return None

            result = {
                "name": organization.get("name"),
                "domain": organization.get("primary_domain"),
                "phone": organization.get("phone"),
                "website": organization.get("website_url"),
                "industry": organization.get("industry"),
                "keywords": organization.get("keywords", []),
                "description": organization.get("short_description"),
                "employee_count": organization.get("estimated_num_employees"),
                "revenue_range": organization.get("annual_revenue"),
                "technologies": organization.get("technologies", []),
                "founded_year": organization.get("founded_year"),
                "linkedin_url": organization.get("linkedin_url"),
                "twitter_url": organization.get("twitter_url"),
                "facebook_url": organization.get("facebook_url")
            }

            # Cache result
            self.cache.put(cache_key, result)

            return result

        except Exception as e:
            logger.error(f"Apollo enrich_company exception: {e}")
            return None

    def _transform_person(self, person: Dict) -> Dict[str, Any]:
        """Transform Apollo person data to our format"""
        return {
            "name": person.get("name"),
            "first_name": person.get("first_name"),
            "last_name": person.get("last_name"),
            "email": person.get("email"),
            "phone": person.get("phone_numbers", [{}])[0].get("raw_number") if person.get("phone_numbers") else None,
            "title": person.get("title"),
            "seniority": person.get("seniority"),
            "departments": person.get("departments", []),
            "linkedin_url": person.get("linkedin_url"),
            "twitter_url": person.get("twitter_url"),
            "company": {
                "name": person.get("organization", {}).get("name"),
                "domain": person.get("organization", {}).get("primary_domain"),
                "industry": person.get("organization", {}).get("industry"),
                "employee_count": person.get("organization", {}).get("estimated_num_employees"),
                "technologies": person.get("organization", {}).get("technologies", [])
            },
            "apollo_id": person.get("id"),
            "enriched_via": "apollo"
        }

    def stats(self) -> Dict[str, Any]:
        """Get usage statistics"""
        return {
            "requests_made": self.requests_made,
            "credits_used": self.credits_used,
            "cache_stats": self.cache.stats()
        }

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()


# Usage Example:
"""
apollo = ApolloClient(api_key="your_api_key")

# Search for HR managers in Luxembourg
results = await apollo.search_people(
    job_titles=["HR Manager", "Talent Acquisition Manager"],
    locations=["Luxembourg"],
    industries=["Technology", "Finance"],
    company_sizes=["51-200", "201-500"]
)

for person in results:
    print(f"{person['name']} - {person['title']} at {person['company']['name']}")
    print(f"Email: {person['email']}")

# Find email from name + company
email_result = await apollo.find_email(
    first_name="John",
    last_name="Doe",
    domain="example.com"
)
if email_result:
    print(f"Found: {email_result['email']} (confidence: {email_result['confidence']})")

# Enrich person from email
enriched = await apollo.enrich_person("john.doe@example.com")
if enriched:
    print(f"Title: {enriched['title']}")
    print(f"Technologies: {enriched['company']['technologies']}")

# Get usage stats
print(apollo.stats())
# {
#     'requests_made': 15,
#     'credits_used': 23,
#     'cache_stats': {...}
# }

await apollo.close()
"""
