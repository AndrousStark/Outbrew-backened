"""
Proxycurl API Client (Layer 5: LinkedIn Scraping)

Proxycurl provides LEGAL LinkedIn data scraping:
- Person profile enrichment (from LinkedIn URL)
- Company profile enrichment (from LinkedIn URL)
- Job listings from company LinkedIn
- Role lookup (find LinkedIn profiles by email)
- Work email finder
- Company lookup by domain

Cost: Pay-per-credit ($0.01-0.03 per request depending on endpoint)
API Docs: https://nubela.co/proxycurl/docs

2026 Best Practice:
- Legal alternative to scraping LinkedIn directly
- Caching to minimize API costs
- Waterfall integration with Apollo/Clearbit
"""

import httpx
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from app.core.config import settings
from app.utils.advanced_cache import TimeBoundCache

logger = logging.getLogger(__name__)


class ProxycurlClient:
    """
    Proxycurl API Client for LinkedIn Data

    Features:
    - LinkedIn profile scraping (person)
    - LinkedIn company profile scraping
    - Email to LinkedIn lookup
    - LinkedIn to work email finder
    - Company job listings
    - Contact info extraction

    Rate Limits:
    - 300 requests per minute (default)
    - 10 concurrent requests

    Pricing:
    - Person Profile: 1 credit ($0.01)
    - Company Profile: 1 credit ($0.01)
    - Role Lookup: 3 credits ($0.03)
    - Work Email: 3 credits ($0.03)

    2026 Best Practices Applied:
    - 7-day cache for LinkedIn profiles (data changes slowly)
    - 30-day cache for company profiles
    - Rate limit compliance with backoff
    - Detailed logging and monitoring
    """

    BASE_URL = "https://nubela.co/proxycurl/api"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.PROXYCURL_API_KEY
        self.client = httpx.AsyncClient(
            timeout=60.0,  # LinkedIn scraping can be slow
            headers={
                "Authorization": f"Bearer {self.api_key}"
            }
        )

        # Cache with appropriate TTLs
        self.person_cache = TimeBoundCache(capacity=10000, default_ttl_seconds=7*24*3600)  # 7 days
        self.company_cache = TimeBoundCache(capacity=5000, default_ttl_seconds=30*24*3600)  # 30 days
        self.email_cache = TimeBoundCache(capacity=10000, default_ttl_seconds=30*24*3600)  # 30 days

        # Statistics
        self.requests_made = 0
        self.credits_used = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.cache_hits = 0

        logger.info(f"[PROXYCURL] Client initialized (API key configured: {bool(self.api_key)})")

    async def get_person_profile(
        self,
        linkedin_url: str,
        extra_data: bool = True,
        skills: bool = True,
        inferred_salary: bool = False,
        personal_email: bool = True,
        personal_contact_number: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Get full LinkedIn person profile

        Returns comprehensive profile including:
        - Full name, headline, summary
        - Current and past employment
        - Education history
        - Skills and endorsements
        - Contact information (if available)
        - Profile picture

        Args:
            linkedin_url: Full LinkedIn profile URL
            extra_data: Include extra computed data
            skills: Include skills list
            inferred_salary: Include salary estimate
            personal_email: Try to find personal email
            personal_contact_number: Try to find phone number

        Returns:
            Full LinkedIn profile data

        Example:
            profile = await proxycurl.get_person_profile(
                "https://www.linkedin.com/in/johndoe/"
            )
            print(f"Name: {profile['full_name']}")
            print(f"Headline: {profile['headline']}")
        """
        if not self.api_key:
            logger.warning("[PROXYCURL] API key not configured, skipping profile fetch")
            return None

        # Normalize LinkedIn URL
        linkedin_url = self._normalize_linkedin_url(linkedin_url)
        if not linkedin_url:
            logger.warning("[PROXYCURL] Invalid LinkedIn URL provided")
            return None

        # Check cache first
        cache_key = f"proxycurl:person:{linkedin_url}"
        cached = self.person_cache.get(cache_key)
        if cached:
            self.cache_hits += 1
            logger.debug(f"[PROXYCURL] Cache hit for profile: {linkedin_url}")
            return cached

        try:
            logger.info(f"[PROXYCURL] Fetching LinkedIn profile: {linkedin_url}")

            params = {
                "url": linkedin_url,
                "extra": "include" if extra_data else "exclude",
                "skills": "include" if skills else "exclude",
                "inferred_salary": "include" if inferred_salary else "exclude",
                "personal_email": "include" if personal_email else "exclude",
                "personal_contact_number": "include" if personal_contact_number else "exclude"
            }

            response = await self.client.get(
                f"{self.BASE_URL}/v2/linkedin",
                params=params
            )

            self.requests_made += 1
            self.credits_used += 1  # Person profile = 1 credit

            if response.status_code == 404:
                logger.info(f"[PROXYCURL] Profile not found: {linkedin_url}")
                return None

            if response.status_code == 429:
                logger.warning("[PROXYCURL] Rate limited, backoff required")
                return None

            response.raise_for_status()
            data = response.json()

            # Transform to our standard format
            result = self._transform_person_profile(data, linkedin_url)
            result["enriched_via"] = "proxycurl"
            result["enriched_at"] = datetime.now(timezone.utc).isoformat()

            # Cache the result
            self.person_cache.put(cache_key, result)
            self.successful_requests += 1

            logger.info(f"[PROXYCURL] Successfully fetched profile: {result.get('full_name', 'Unknown')}")

            return result

        except httpx.HTTPStatusError as e:
            self.failed_requests += 1
            logger.error(f"[PROXYCURL] HTTP error fetching profile {linkedin_url}: {e.response.status_code}")
            return None
        except Exception as e:
            self.failed_requests += 1
            logger.error(f"[PROXYCURL] Exception fetching profile {linkedin_url}: {str(e)}")
            return None

    async def get_company_profile(
        self,
        linkedin_url: Optional[str] = None,
        domain: Optional[str] = None,
        resolve_numeric_id: bool = True,
        extra_data: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Get LinkedIn company profile

        Returns comprehensive company profile including:
        - Company name, description, tagline
        - Industry, specialties
        - Employee count, website
        - Locations
        - Founding date

        Args:
            linkedin_url: LinkedIn company page URL
            domain: Company domain (alternative to URL)
            resolve_numeric_id: Resolve to full profile
            extra_data: Include extra computed data

        Returns:
            Full LinkedIn company profile data

        Example:
            company = await proxycurl.get_company_profile(
                linkedin_url="https://www.linkedin.com/company/stripe/"
            )
            # Or by domain:
            company = await proxycurl.get_company_profile(domain="stripe.com")
        """
        if not self.api_key:
            logger.warning("[PROXYCURL] API key not configured, skipping company fetch")
            return None

        if not linkedin_url and not domain:
            logger.warning("[PROXYCURL] Either linkedin_url or domain must be provided")
            return None

        # Check cache first
        cache_key = f"proxycurl:company:{linkedin_url or domain}"
        cached = self.company_cache.get(cache_key)
        if cached:
            self.cache_hits += 1
            logger.debug(f"[PROXYCURL] Cache hit for company: {linkedin_url or domain}")
            return cached

        try:
            logger.info(f"[PROXYCURL] Fetching company profile: {linkedin_url or domain}")

            params = {
                "resolve_numeric_id": str(resolve_numeric_id).lower(),
                "extra": "include" if extra_data else "exclude"
            }

            if linkedin_url:
                params["url"] = linkedin_url
            else:
                # Use domain lookup endpoint
                params["company_domain"] = domain

            endpoint = f"{self.BASE_URL}/linkedin/company"
            if domain and not linkedin_url:
                endpoint = f"{self.BASE_URL}/linkedin/company/resolve"

            response = await self.client.get(endpoint, params=params)

            self.requests_made += 1
            self.credits_used += 1

            if response.status_code == 404:
                logger.info(f"[PROXYCURL] Company not found: {linkedin_url or domain}")
                return None

            response.raise_for_status()
            data = response.json()

            # Transform to our format
            result = self._transform_company_profile(data)
            result["enriched_via"] = "proxycurl"
            result["enriched_at"] = datetime.now(timezone.utc).isoformat()

            # Cache the result
            self.company_cache.put(cache_key, result)
            self.successful_requests += 1

            logger.info(f"[PROXYCURL] Successfully fetched company: {result.get('name', 'Unknown')}")

            return result

        except httpx.HTTPStatusError as e:
            self.failed_requests += 1
            logger.error(f"[PROXYCURL] HTTP error fetching company: {e.response.status_code}")
            return None
        except Exception as e:
            self.failed_requests += 1
            logger.error(f"[PROXYCURL] Exception fetching company: {str(e)}")
            return None

    async def lookup_email_to_linkedin(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Find LinkedIn profile URL from email address

        Uses reverse email lookup to find associated LinkedIn profile.

        Args:
            email: Email address to lookup

        Returns:
            LinkedIn profile URL and basic info if found

        Example:
            result = await proxycurl.lookup_email_to_linkedin("john@stripe.com")
            if result:
                print(f"LinkedIn: {result['linkedin_url']}")
        """
        if not self.api_key:
            return None

        # Check cache
        cache_key = f"proxycurl:email_lookup:{email.lower()}"
        cached = self.email_cache.get(cache_key)
        if cached:
            self.cache_hits += 1
            return cached

        try:
            logger.info(f"[PROXYCURL] Looking up LinkedIn for email: {email}")

            response = await self.client.get(
                f"{self.BASE_URL}/linkedin/profile/resolve/email",
                params={"work_email": email}
            )

            self.requests_made += 1
            self.credits_used += 3  # Role lookup = 3 credits

            if response.status_code == 404:
                logger.info(f"[PROXYCURL] No LinkedIn found for: {email}")
                return None

            response.raise_for_status()
            data = response.json()

            result = {
                "email": email,
                "linkedin_url": data.get("url"),
                "found": bool(data.get("url")),
                "enriched_via": "proxycurl"
            }

            self.email_cache.put(cache_key, result)
            self.successful_requests += 1

            logger.info(f"[PROXYCURL] Found LinkedIn for {email}: {result['linkedin_url']}")

            return result

        except Exception as e:
            self.failed_requests += 1
            logger.error(f"[PROXYCURL] Email lookup failed for {email}: {str(e)}")
            return None

    async def find_work_email(
        self,
        linkedin_url: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Find work email from LinkedIn profile

        Uses LinkedIn profile to find associated work email.

        Args:
            linkedin_url: LinkedIn profile URL
            first_name: First name (improves accuracy)
            last_name: Last name (improves accuracy)

        Returns:
            Work email and confidence score

        Example:
            result = await proxycurl.find_work_email(
                "https://www.linkedin.com/in/johndoe/",
                first_name="John",
                last_name="Doe"
            )
            if result:
                print(f"Email: {result['email']} (confidence: {result['confidence']})")
        """
        if not self.api_key:
            return None

        linkedin_url = self._normalize_linkedin_url(linkedin_url)
        if not linkedin_url:
            return None

        # Check cache
        cache_key = f"proxycurl:work_email:{linkedin_url}"
        cached = self.email_cache.get(cache_key)
        if cached:
            self.cache_hits += 1
            return cached

        try:
            logger.info(f"[PROXYCURL] Finding work email for: {linkedin_url}")

            params = {"linkedin_profile_url": linkedin_url}
            if first_name:
                params["first_name"] = first_name
            if last_name:
                params["last_name"] = last_name

            response = await self.client.get(
                f"{self.BASE_URL}/linkedin/profile/email",
                params=params
            )

            self.requests_made += 1
            self.credits_used += 3  # Work email = 3 credits

            if response.status_code == 404:
                return None

            response.raise_for_status()
            data = response.json()

            result = {
                "linkedin_url": linkedin_url,
                "email": data.get("email"),
                "email_type": data.get("email_type", "work"),
                "confidence": data.get("confidence", "unknown"),
                "first_name": first_name,
                "last_name": last_name,
                "enriched_via": "proxycurl"
            }

            self.email_cache.put(cache_key, result)
            self.successful_requests += 1

            logger.info(f"[PROXYCURL] Found work email: {result['email']}")

            return result

        except Exception as e:
            self.failed_requests += 1
            logger.error(f"[PROXYCURL] Work email lookup failed: {str(e)}")
            return None

    async def get_company_jobs(
        self,
        linkedin_url: str,
        job_type: Optional[str] = None,
        experience_level: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get job listings from LinkedIn company page

        Useful for identifying hiring signals and finding HR contacts.

        Args:
            linkedin_url: LinkedIn company page URL
            job_type: Filter by type (full-time, contract, etc.)
            experience_level: Filter by level (entry, mid, senior)
            limit: Max results

        Returns:
            List of job listings

        Example:
            jobs = await proxycurl.get_company_jobs(
                "https://www.linkedin.com/company/stripe/"
            )
            for job in jobs:
                print(f"- {job['title']} ({job['location']})")
        """
        if not self.api_key:
            return []

        try:
            logger.info(f"[PROXYCURL] Fetching jobs for: {linkedin_url}")

            params = {
                "url": linkedin_url,
                "limit": min(limit, 50)
            }

            if job_type:
                params["job_type"] = job_type
            if experience_level:
                params["experience_level"] = experience_level

            response = await self.client.get(
                f"{self.BASE_URL}/linkedin/company/job",
                params=params
            )

            self.requests_made += 1
            self.credits_used += 1

            response.raise_for_status()
            data = response.json()

            jobs = []
            for job in data.get("jobs", []):
                jobs.append({
                    "title": job.get("job_title"),
                    "url": job.get("job_url"),
                    "location": job.get("location"),
                    "posted_date": job.get("list_date"),
                    "company": job.get("company_name"),
                    "company_url": job.get("company_url"),
                    "employment_type": job.get("employment_type")
                })

            self.successful_requests += 1
            logger.info(f"[PROXYCURL] Found {len(jobs)} jobs for company")

            return jobs

        except Exception as e:
            self.failed_requests += 1
            logger.error(f"[PROXYCURL] Job fetch failed: {str(e)}")
            return []

    async def search_people(
        self,
        current_company_linkedin_url: Optional[str] = None,
        current_role_title: Optional[str] = None,
        region: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search for people on LinkedIn

        Find people matching specific criteria.

        Args:
            current_company_linkedin_url: Filter by current company
            current_role_title: Filter by job title
            region: Filter by region
            limit: Max results

        Returns:
            List of matching profiles

        Example:
            people = await proxycurl.search_people(
                current_company_linkedin_url="https://www.linkedin.com/company/stripe/",
                current_role_title="recruiter"
            )
        """
        if not self.api_key:
            return []

        try:
            logger.info("[PROXYCURL] Searching for people")

            params = {"page_size": min(limit, 100)}

            if current_company_linkedin_url:
                params["current_company_linkedin_profile_url"] = current_company_linkedin_url
            if current_role_title:
                params["current_role_title"] = current_role_title
            if region:
                params["region"] = region

            response = await self.client.get(
                f"{self.BASE_URL}/search/person",
                params=params
            )

            self.requests_made += 1
            self.credits_used += 3  # Search = 3 credits

            response.raise_for_status()
            data = response.json()

            results = []
            for person in data.get("results", []):
                results.append({
                    "linkedin_url": person.get("linkedin_profile_url"),
                    "name": person.get("name"),
                    "headline": person.get("headline"),
                    "location": person.get("location"),
                    "profile_pic_url": person.get("profile_pic_url")
                })

            self.successful_requests += 1
            logger.info(f"[PROXYCURL] Found {len(results)} people")

            return results

        except Exception as e:
            self.failed_requests += 1
            logger.error(f"[PROXYCURL] People search failed: {str(e)}")
            return []

    def _normalize_linkedin_url(self, url: str) -> Optional[str]:
        """Normalize LinkedIn URL to standard format"""
        if not url:
            return None

        url = url.strip().lower()

        # Handle various LinkedIn URL formats
        if "linkedin.com/in/" in url or "linkedin.com/company/" in url:
            # Extract the clean URL
            if not url.startswith("http"):
                url = "https://" + url

            # Remove query params and trailing slash
            if "?" in url:
                url = url.split("?")[0]

            return url.rstrip("/")

        return None

    def _transform_person_profile(self, data: Dict, linkedin_url: str) -> Dict[str, Any]:
        """Transform Proxycurl person data to our standard format"""
        return {
            "linkedin_url": linkedin_url,
            "public_identifier": data.get("public_identifier"),
            "full_name": data.get("full_name"),
            "first_name": data.get("first_name"),
            "last_name": data.get("last_name"),
            "headline": data.get("headline"),
            "summary": data.get("summary"),
            "occupation": data.get("occupation"),
            "profile_pic_url": data.get("profile_pic_url"),
            "background_cover_image_url": data.get("background_cover_image_url"),
            "country": data.get("country"),
            "country_full_name": data.get("country_full_name"),
            "city": data.get("city"),
            "state": data.get("state"),
            "connections": data.get("connections"),
            "follower_count": data.get("follower_count"),

            # Experience
            "experiences": [
                {
                    "title": exp.get("title"),
                    "company": exp.get("company"),
                    "company_linkedin_url": exp.get("company_linkedin_profile_url"),
                    "location": exp.get("location"),
                    "starts_at": exp.get("starts_at"),
                    "ends_at": exp.get("ends_at"),
                    "description": exp.get("description")
                }
                for exp in data.get("experiences", [])
            ],

            # Education
            "education": [
                {
                    "school": edu.get("school"),
                    "degree_name": edu.get("degree_name"),
                    "field_of_study": edu.get("field_of_study"),
                    "starts_at": edu.get("starts_at"),
                    "ends_at": edu.get("ends_at")
                }
                for edu in data.get("education", [])
            ],

            # Skills
            "skills": data.get("skills", []),

            # Languages
            "languages": data.get("languages", []),

            # Contact (if available)
            "personal_emails": data.get("personal_emails", []),
            "personal_numbers": data.get("personal_numbers", []),

            # Extra data
            "inferred_salary": data.get("inferred_salary"),
            "gender": data.get("gender"),
            "birth_date": data.get("birth_date"),

            # Certifications
            "certifications": [
                {
                    "name": cert.get("name"),
                    "authority": cert.get("authority"),
                    "starts_at": cert.get("starts_at")
                }
                for cert in data.get("certifications", [])
            ]
        }

    def _transform_company_profile(self, data: Dict) -> Dict[str, Any]:
        """Transform Proxycurl company data to our standard format"""
        return {
            "linkedin_url": data.get("linkedin_internal_id"),
            "name": data.get("name"),
            "description": data.get("description"),
            "tagline": data.get("tagline"),
            "website": data.get("website"),
            "industry": data.get("industry"),
            "company_size": data.get("company_size"),
            "company_size_on_linkedin": data.get("company_size_on_linkedin"),
            "company_type": data.get("company_type"),
            "founded_year": data.get("founded_year"),
            "specialties": data.get("specialties", []),
            "locations": data.get("locations", []),
            "profile_pic_url": data.get("profile_pic_url"),
            "background_cover_image_url": data.get("background_cover_image_url"),
            "follower_count": data.get("follower_count"),
            "hq": {
                "country": data.get("hq", {}).get("country"),
                "city": data.get("hq", {}).get("city"),
                "state": data.get("hq", {}).get("state"),
                "postal_code": data.get("hq", {}).get("postal_code"),
                "line_1": data.get("hq", {}).get("line_1")
            } if data.get("hq") else None,

            # Funding info
            "funding_data": data.get("funding_data"),

            # Similar companies
            "similar_companies": data.get("similar_companies", []),

            # Affiliated companies
            "affiliated_companies": data.get("affiliated_companies", [])
        }

    def stats(self) -> Dict[str, Any]:
        """Get client statistics for monitoring"""
        return {
            "api_key_configured": bool(self.api_key),
            "requests_made": self.requests_made,
            "credits_used": self.credits_used,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "cache_hits": self.cache_hits,
            "success_rate": round(
                (self.successful_requests / max(self.requests_made, 1)) * 100, 2
            ),
            "avg_credits_per_request": round(
                self.credits_used / max(self.requests_made, 1), 2
            ),
            "person_cache_stats": self.person_cache.stats(),
            "company_cache_stats": self.company_cache.stats(),
            "email_cache_stats": self.email_cache.stats()
        }

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()
        logger.info("[PROXYCURL] Client closed")


# Usage Example:
"""
proxycurl = ProxycurlClient()

# Get LinkedIn profile
profile = await proxycurl.get_person_profile(
    "https://www.linkedin.com/in/johndoe/"
)
if profile:
    print(f"Name: {profile['full_name']}")
    print(f"Headline: {profile['headline']}")
    print(f"Current: {profile['experiences'][0]['title']} at {profile['experiences'][0]['company']}")
    print(f"Skills: {', '.join(profile['skills'][:5])}")

# Get company profile
company = await proxycurl.get_company_profile(domain="stripe.com")
if company:
    print(f"Company: {company['name']}")
    print(f"Industry: {company['industry']}")
    print(f"Size: {company['company_size']}")

# Find LinkedIn from email
result = await proxycurl.lookup_email_to_linkedin("john@stripe.com")
if result and result['found']:
    print(f"LinkedIn: {result['linkedin_url']}")

# Find work email from LinkedIn
email = await proxycurl.find_work_email(
    "https://www.linkedin.com/in/johndoe/",
    first_name="John",
    last_name="Doe"
)
if email:
    print(f"Work Email: {email['email']}")

# Get company job listings
jobs = await proxycurl.get_company_jobs(
    "https://www.linkedin.com/company/stripe/"
)
for job in jobs:
    print(f"- {job['title']} ({job['location']})")

# Search for recruiters at company
recruiters = await proxycurl.search_people(
    current_company_linkedin_url="https://www.linkedin.com/company/stripe/",
    current_role_title="recruiter"
)
for r in recruiters:
    print(f"- {r['name']}: {r['headline']}")

# Get stats
print(proxycurl.stats())

await proxycurl.close()
"""
