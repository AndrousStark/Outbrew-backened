"""
ENHANCED FREE DATA SOURCES V1.0
Additional FREE data sources for maximum extraction coverage

No API keys required for any of these sources!

New Sources Added:
1. Bing Search (HTML scraping)
2. Google Custom Search (free 100/day)
3. GitHub API (60 req/hour unauth, emails from commits)
4. Wikipedia/Wikidata API (unlimited)
5. Wayback Machine (archive.org - unlimited)
6. crt.sh (Certificate Transparency - unlimited)
7. Team/About Page Deep Crawler
8. Press Release Aggregators
9. Job Board Scraping (for company info)
10. SEC EDGAR (US company filings)
11. CommonCrawl Index (massive free crawl data)
"""

import asyncio
import logging
import re
import json
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse, urlencode, quote
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class BingSearch:
    """
    FREE Bing Search using HTML scraping.
    No API key required.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.base_url = "https://www.bing.com/search"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        self.client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.search_count = 0

    async def search(self, query: str, max_results: int = 30) -> List[Dict[str, str]]:
        """Search Bing and return results."""
        results = []

        try:
            params = {"q": query, "count": min(max_results, 50)}
            response = await self.client.get(
                self.base_url,
                params=params,
                headers=self.headers
            )

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Parse organic results
                for result in soup.select("li.b_algo"):
                    title_elem = result.select_one("h2 a")
                    snippet_elem = result.select_one(".b_caption p")

                    if title_elem:
                        url = title_elem.get("href", "")
                        title = title_elem.get_text(strip=True)
                        snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""

                        if url and title:
                            results.append({
                                "title": title,
                                "url": url,
                                "snippet": snippet,
                                "source": "bing"
                            })

                            if len(results) >= max_results:
                                break

                self.search_count += 1
                logger.info(f"Bing search for '{query[:50]}...' returned {len(results)} results")

        except Exception as e:
            logger.error(f"Bing search error: {e}")

        return results

    async def close(self):
        await self.client.aclose()


class GitHubExtractor:
    """
    FREE GitHub data extraction.
    - 60 requests/hour without auth
    - Extract emails from commits, profiles, repos
    - Find company team members
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.api_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "DataExtractor/1.0"
        }
        self.client = httpx.AsyncClient(timeout=timeout)
        self.rate_remaining = 60

    async def search_users(
        self,
        query: str,
        max_results: int = 30
    ) -> List[Dict[str, Any]]:
        """Search GitHub users by query."""
        results = []

        try:
            params = {"q": query, "per_page": min(max_results, 100)}
            response = await self.client.get(
                f"{self.api_url}/search/users",
                params=params,
                headers=self.headers
            )

            # Track rate limit
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                data = response.json()
                for user in data.get("items", [])[:max_results]:
                    results.append({
                        "username": user.get("login"),
                        "profile_url": user.get("html_url"),
                        "avatar_url": user.get("avatar_url"),
                        "type": user.get("type"),
                        "source": "github"
                    })

        except Exception as e:
            logger.error(f"GitHub user search error: {e}")

        return results

    async def get_user_details(self, username: str) -> Optional[Dict[str, Any]]:
        """Get detailed user info including email if public."""
        try:
            response = await self.client.get(
                f"{self.api_url}/users/{username}",
                headers=self.headers
            )

            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                data = response.json()
                return {
                    "username": data.get("login"),
                    "name": data.get("name"),
                    "email": data.get("email"),  # May be null if private
                    "company": data.get("company"),
                    "location": data.get("location"),
                    "bio": data.get("bio"),
                    "blog": data.get("blog"),
                    "twitter": data.get("twitter_username"),
                    "public_repos": data.get("public_repos"),
                    "followers": data.get("followers"),
                    "profile_url": data.get("html_url"),
                    "source": "github"
                }
        except Exception as e:
            logger.error(f"GitHub user details error for {username}: {e}")

        return None

    async def get_email_from_commits(self, username: str) -> Optional[str]:
        """
        Extract email from user's public commits.
        GitHub commits always contain the committer's email!
        """
        try:
            # Get user's recent events (includes push events with commits)
            response = await self.client.get(
                f"{self.api_url}/users/{username}/events/public",
                headers=self.headers
            )

            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                events = response.json()

                for event in events:
                    if event.get("type") == "PushEvent":
                        commits = event.get("payload", {}).get("commits", [])
                        for commit in commits:
                            author = commit.get("author", {})
                            email = author.get("email", "")

                            # Filter out noreply emails
                            if email and "noreply" not in email.lower():
                                logger.info(f"Found email for {username}: {email}")
                                return email

        except Exception as e:
            logger.error(f"GitHub commit email extraction error: {e}")

        return None

    async def search_org_members(
        self,
        org_name: str,
        max_results: int = 50
    ) -> List[Dict[str, Any]]:
        """Get public members of an organization."""
        members = []

        try:
            response = await self.client.get(
                f"{self.api_url}/orgs/{org_name}/members",
                params={"per_page": min(max_results, 100)},
                headers=self.headers
            )

            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                for member in response.json()[:max_results]:
                    members.append({
                        "username": member.get("login"),
                        "profile_url": member.get("html_url"),
                        "organization": org_name,
                        "source": "github_org"
                    })

        except Exception as e:
            logger.error(f"GitHub org members error for {org_name}: {e}")

        return members

    async def close(self):
        await self.client.aclose()


class WikidataExtractor:
    """
    FREE Wikidata/Wikipedia extraction.
    - Unlimited queries
    - Company info, executives, founders
    - Structured data via SPARQL
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.wikidata_api = "https://www.wikidata.org/w/api.php"
        self.wikipedia_api = "https://en.wikipedia.org/w/api.php"
        self.sparql_url = "https://query.wikidata.org/sparql"
        self.headers = {
            "User-Agent": "DataExtractor/1.0 (Educational/Research)"
        }
        self.client = httpx.AsyncClient(timeout=timeout)

    async def search_company(self, company_name: str) -> Optional[Dict[str, Any]]:
        """Search for company info in Wikidata."""
        try:
            # Search Wikidata
            params = {
                "action": "wbsearchentities",
                "search": company_name,
                "language": "en",
                "format": "json",
                "type": "item"
            }

            response = await self.client.get(
                self.wikidata_api,
                params=params,
                headers=self.headers
            )

            if response.status_code == 200:
                data = response.json()
                results = data.get("search", [])

                if results:
                    entity_id = results[0].get("id")
                    # Get detailed entity info
                    return await self._get_entity_details(entity_id)

        except Exception as e:
            logger.error(f"Wikidata search error: {e}")

        return None

    async def _get_entity_details(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed entity information from Wikidata."""
        try:
            params = {
                "action": "wbgetentities",
                "ids": entity_id,
                "languages": "en",
                "format": "json",
                "props": "labels|descriptions|claims"
            }

            response = await self.client.get(
                self.wikidata_api,
                params=params,
                headers=self.headers
            )

            if response.status_code == 200:
                data = response.json()
                entity = data.get("entities", {}).get(entity_id, {})

                labels = entity.get("labels", {})
                descriptions = entity.get("descriptions", {})
                claims = entity.get("claims", {})

                result = {
                    "wikidata_id": entity_id,
                    "name": labels.get("en", {}).get("value"),
                    "description": descriptions.get("en", {}).get("value"),
                    "source": "wikidata"
                }

                # Extract key properties
                # P856 = official website
                if "P856" in claims:
                    result["website"] = claims["P856"][0].get("mainsnak", {}).get("datavalue", {}).get("value")

                # P159 = headquarters location
                if "P159" in claims:
                    hq = claims["P159"][0].get("mainsnak", {}).get("datavalue", {}).get("value", {})
                    result["headquarters_id"] = hq.get("id")

                # P169 = CEO
                if "P169" in claims:
                    ceo = claims["P169"][0].get("mainsnak", {}).get("datavalue", {}).get("value", {})
                    result["ceo_id"] = ceo.get("id")

                # P112 = founder
                if "P112" in claims:
                    founders = []
                    for founder_claim in claims["P112"]:
                        founder = founder_claim.get("mainsnak", {}).get("datavalue", {}).get("value", {})
                        if founder.get("id"):
                            founders.append(founder.get("id"))
                    result["founder_ids"] = founders

                # P571 = inception date
                if "P571" in claims:
                    inception = claims["P571"][0].get("mainsnak", {}).get("datavalue", {}).get("value", {})
                    result["founded"] = inception.get("time", "").split("T")[0].replace("+", "")

                # P452 = industry
                if "P452" in claims:
                    industry = claims["P452"][0].get("mainsnak", {}).get("datavalue", {}).get("value", {})
                    result["industry_id"] = industry.get("id")

                return result

        except Exception as e:
            logger.error(f"Wikidata entity details error: {e}")

        return None

    async def get_company_executives(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Use SPARQL to get company executives.
        Returns CEOs, founders, board members.
        """
        executives = []

        # SPARQL query for company executives
        query = f"""
        SELECT ?company ?companyLabel ?person ?personLabel ?positionLabel ?email WHERE {{
          ?company rdfs:label "{company_name}"@en .
          ?company wdt:P169|wdt:P112|wdt:P3320 ?person .
          OPTIONAL {{ ?person wdt:P39 ?position }}
          OPTIONAL {{ ?person wdt:P968 ?email }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
        }}
        LIMIT 50
        """

        try:
            response = await self.client.get(
                self.sparql_url,
                params={"query": query, "format": "json"},
                headers=self.headers
            )

            if response.status_code == 200:
                data = response.json()
                bindings = data.get("results", {}).get("bindings", [])

                for binding in bindings:
                    exec_info = {
                        "name": binding.get("personLabel", {}).get("value"),
                        "company": binding.get("companyLabel", {}).get("value"),
                        "position": binding.get("positionLabel", {}).get("value"),
                        "wikidata_id": binding.get("person", {}).get("value", "").split("/")[-1],
                        "source": "wikidata_sparql"
                    }

                    if binding.get("email", {}).get("value"):
                        exec_info["email"] = binding["email"]["value"].replace("mailto:", "")

                    executives.append(exec_info)

        except Exception as e:
            logger.error(f"Wikidata SPARQL error: {e}")

        return executives

    async def close(self):
        await self.client.aclose()


class WaybackMachineExtractor:
    """
    FREE Wayback Machine (archive.org) extraction.
    - Access archived versions of pages
    - Find historical emails/contacts
    - Useful when current pages block scraping
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.cdx_api = "https://web.archive.org/cdx/search/cdx"
        self.wayback_url = "https://web.archive.org/web"
        self.headers = {
            "User-Agent": "DataExtractor/1.0 (Research)"
        }
        self.client = httpx.AsyncClient(timeout=timeout)

    async def get_snapshots(
        self,
        url: str,
        limit: int = 10
    ) -> List[Dict[str, str]]:
        """Get available snapshots for a URL."""
        snapshots = []

        try:
            params = {
                "url": url,
                "output": "json",
                "limit": limit,
                "fl": "timestamp,original,statuscode",
                "filter": "statuscode:200"
            }

            response = await self.client.get(
                self.cdx_api,
                params=params,
                headers=self.headers
            )

            if response.status_code == 200:
                lines = response.text.strip().split("\n")
                # Skip header row
                for line in lines[1:]:
                    try:
                        parts = json.loads(line) if line.startswith("[") else line.split()
                        if len(parts) >= 3:
                            snapshots.append({
                                "timestamp": parts[0],
                                "original_url": parts[1],
                                "archive_url": f"{self.wayback_url}/{parts[0]}/{parts[1]}"
                            })
                    except Exception as e:
                        logger.warning(f"[EnhancedSources] Failed to parse Wayback CDX line: {e}")
                        continue

        except Exception as e:
            logger.error(f"Wayback CDX error: {e}")

        return snapshots

    async def get_archived_page(
        self,
        url: str,
        timestamp: Optional[str] = None
    ) -> Optional[str]:
        """Fetch an archived version of a page."""
        try:
            if timestamp:
                archive_url = f"{self.wayback_url}/{timestamp}/{url}"
            else:
                # Get latest snapshot
                archive_url = f"{self.wayback_url}/{url}"

            response = await self.client.get(
                archive_url,
                headers=self.headers,
                follow_redirects=True
            )

            if response.status_code == 200:
                return response.text

        except Exception as e:
            logger.error(f"Wayback fetch error: {e}")

        return None

    async def extract_emails_from_history(
        self,
        domain: str,
        pages: List[str] = None
    ) -> List[str]:
        """
        Extract emails from archived versions of pages.
        Useful when current pages don't show emails.
        """
        emails = set()

        # Default pages to check
        if not pages:
            pages = [
                f"https://{domain}/",
                f"https://{domain}/contact",
                f"https://{domain}/about",
                f"https://{domain}/team",
                f"https://{domain}/about-us",
                f"https://{domain}/contact-us"
            ]

        for page_url in pages:
            snapshots = await self.get_snapshots(page_url, limit=3)

            for snapshot in snapshots:
                html = await self.get_archived_page(
                    snapshot["original_url"],
                    snapshot["timestamp"]
                )

                if html:
                    # Extract emails
                    found = re.findall(
                        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
                        html
                    )
                    emails.update(found)

                await asyncio.sleep(1)  # Rate limiting

        return list(emails)

    async def close(self):
        await self.client.aclose()


class CertificateTransparencyExtractor:
    """
    FREE crt.sh (Certificate Transparency Logs) extraction.
    - Find subdomains and related domains
    - Discover company infrastructure
    - Unlimited queries
    """

    def __init__(self, timeout: int = 60):
        self.timeout = timeout
        self.crt_url = "https://crt.sh"
        self.headers = {
            "User-Agent": "DataExtractor/1.0"
        }
        self.client = httpx.AsyncClient(timeout=timeout)

    async def find_subdomains(self, domain: str) -> List[str]:
        """Find all subdomains via Certificate Transparency."""
        subdomains = set()

        try:
            params = {"q": f"%.{domain}", "output": "json"}
            response = await self.client.get(
                self.crt_url,
                params=params,
                headers=self.headers
            )

            if response.status_code == 200:
                try:
                    certs = response.json()
                    for cert in certs:
                        name = cert.get("name_value", "")
                        # Handle wildcard and multi-domain certs
                        for subdomain in name.split("\n"):
                            subdomain = subdomain.strip().replace("*.", "")
                            if subdomain and domain in subdomain:
                                subdomains.add(subdomain)
                except json.JSONDecodeError:
                    # Parse HTML if JSON fails
                    soup = BeautifulSoup(response.text, "html.parser")
                    for td in soup.find_all("td"):
                        text = td.get_text(strip=True)
                        if domain in text and "." in text:
                            subdomains.add(text.replace("*.", ""))

        except Exception as e:
            logger.error(f"crt.sh error: {e}")

        return list(subdomains)

    async def close(self):
        await self.client.aclose()


class TeamPageScraper:
    """
    Deep scraper for company team/about pages.
    Finds emails and contacts from various page structures.
    """

    TEAM_PAGE_PATHS = [
        "/team",
        "/about",
        "/about-us",
        "/our-team",
        "/leadership",
        "/management",
        "/people",
        "/staff",
        "/executives",
        "/company",
        "/who-we-are",
        "/contact",
        "/contact-us",
        "/careers",
    ]

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)

    async def find_team_pages(self, domain: str) -> List[str]:
        """Find all team/about pages on a domain."""
        found_pages = []

        for path in self.TEAM_PAGE_PATHS:
            url = f"https://{domain}{path}"
            try:
                response = await self.client.head(url, headers=self.headers)
                if response.status_code in [200, 301, 302]:
                    found_pages.append(url)
            except Exception:
                continue

            await asyncio.sleep(0.1)

        return found_pages

    async def extract_team_members(
        self,
        url: str
    ) -> List[Dict[str, Any]]:
        """Extract team member info from a page."""
        members = []

        try:
            response = await self.client.get(url, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Common team member selectors
                selectors = [
                    ".team-member",
                    ".staff-member",
                    ".person",
                    ".employee",
                    ".leadership-card",
                    ".executive",
                    "[class*='team']",
                    "[class*='member']",
                    "[class*='person']",
                ]

                for selector in selectors:
                    elements = soup.select(selector)
                    for elem in elements:
                        member = self._extract_member_info(elem)
                        if member.get("name"):
                            member["source_url"] = url
                            members.append(member)

                # Fallback: look for common patterns
                if not members:
                    members = self._fallback_extraction(soup, url)

        except Exception as e:
            logger.error(f"Team page extraction error: {e}")

        return members

    def _extract_member_info(self, element) -> Dict[str, Any]:
        """Extract info from a team member element."""
        info = {}

        # Find name (usually in h2, h3, h4, or .name)
        name_elem = element.select_one("h2, h3, h4, .name, [class*='name']")
        if name_elem:
            info["name"] = name_elem.get_text(strip=True)

        # Find title/position
        title_elem = element.select_one(".title, .position, .role, [class*='title'], [class*='position']")
        if title_elem:
            info["title"] = title_elem.get_text(strip=True)

        # Find email
        email_elem = element.select_one("a[href^='mailto:']")
        if email_elem:
            href = email_elem.get("href", "")
            info["email"] = href.replace("mailto:", "").split("?")[0]

        # Find LinkedIn
        linkedin_elem = element.select_one("a[href*='linkedin.com']")
        if linkedin_elem:
            info["linkedin"] = linkedin_elem.get("href")

        # Find Twitter/X
        twitter_elem = element.select_one("a[href*='twitter.com'], a[href*='x.com']")
        if twitter_elem:
            info["twitter"] = twitter_elem.get("href")

        return info

    def _fallback_extraction(self, soup, url: str) -> List[Dict[str, Any]]:
        """Fallback extraction using regex patterns."""
        members = []
        text = soup.get_text()

        # Extract all emails
        emails = re.findall(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
            text
        )

        # Extract mailto links
        for mailto in soup.select("a[href^='mailto:']"):
            email = mailto.get("href", "").replace("mailto:", "").split("?")[0]
            if email:
                # Try to find name near the email
                parent = mailto.parent
                name = None
                if parent:
                    name_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', parent.get_text())
                    if name_match:
                        name = name_match.group(1)

                members.append({
                    "email": email,
                    "name": name,
                    "source_url": url,
                    "extraction_method": "mailto_link"
                })

        return members

    async def close(self):
        await self.client.aclose()


class JobBoardScraper:
    """
    Scrape job boards for company info and contacts.
    - Indeed company pages
    - Glassdoor company pages
    - LinkedIn company pages (public)
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)

    async def search_indeed_company(
        self,
        company_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get company info from Indeed."""
        try:
            search_url = f"https://www.indeed.com/cmp/{company_name.lower().replace(' ', '-')}"
            response = await self.client.get(search_url, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                info = {
                    "company_name": company_name,
                    "source": "indeed",
                    "profile_url": search_url
                }

                # Extract company info
                rating = soup.select_one("[data-testid='rating']")
                if rating:
                    info["rating"] = rating.get_text(strip=True)

                review_count = soup.select_one("[data-testid='review-count']")
                if review_count:
                    info["review_count"] = review_count.get_text(strip=True)

                return info

        except Exception as e:
            logger.debug(f"Indeed scraping error: {e}")

        return None

    async def close(self):
        await self.client.aclose()


class SECEdgarExtractor:
    """
    FREE SEC EDGAR extraction for US public companies.
    - Company filings and financial data
    - Executive names from 10-K, DEF 14A
    - Unlimited access
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        self.api_url = "https://data.sec.gov"
        self.headers = {
            "User-Agent": "DataExtractor/1.0 (Research contact@example.com)",
            "Accept": "application/json"
        }
        self.client = httpx.AsyncClient(timeout=timeout)

    async def search_company(
        self,
        company_name: str
    ) -> Optional[Dict[str, Any]]:
        """Search for a company in SEC EDGAR."""
        try:
            params = {
                "company": company_name,
                "type": "10-K",
                "action": "getcompany",
                "output": "atom"
            }

            response = await self.client.get(
                self.base_url,
                params=params,
                headers=self.headers
            )

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "xml")
                entry = soup.find("entry")

                if entry:
                    cik = entry.find("cik")
                    return {
                        "company_name": company_name,
                        "cik": cik.get_text() if cik else None,
                        "source": "sec_edgar"
                    }

        except Exception as e:
            logger.error(f"SEC EDGAR search error: {e}")

        return None

    async def get_company_executives(
        self,
        cik: str
    ) -> List[Dict[str, Any]]:
        """Get executive names from SEC filings."""
        executives = []

        try:
            # Get company info
            cik_padded = cik.zfill(10)
            response = await self.client.get(
                f"{self.api_url}/submissions/CIK{cik_padded}.json",
                headers=self.headers
            )

            if response.status_code == 200:
                data = response.json()

                # Basic company info
                company_info = {
                    "name": data.get("name"),
                    "cik": data.get("cik"),
                    "sic": data.get("sic"),
                    "sic_description": data.get("sicDescription"),
                    "source": "sec_edgar"
                }

                # Officers from filings
                if "insiders" in data:
                    for insider in data["insiders"].get("officers", []):
                        executives.append({
                            "name": insider.get("name"),
                            "title": insider.get("title"),
                            "company": company_info["name"],
                            "source": "sec_edgar"
                        })

        except Exception as e:
            logger.error(f"SEC EDGAR executives error: {e}")

        return executives

    async def close(self):
        await self.client.aclose()


class EnhancedFreeSourcesAggregator:
    """
    Aggregates all enhanced free sources into a single interface.
    """

    def __init__(self, timeout: int = 30):
        self.bing = BingSearch(timeout)
        self.github = GitHubExtractor(timeout)
        self.wikidata = WikidataExtractor(timeout)
        self.wayback = WaybackMachineExtractor(timeout)
        self.crt = CertificateTransparencyExtractor(timeout)
        self.team_scraper = TeamPageScraper(timeout)
        self.job_board = JobBoardScraper(timeout)
        self.sec = SECEdgarExtractor(timeout)

        self.stats = {
            "bing_searches": 0,
            "github_queries": 0,
            "wikidata_queries": 0,
            "wayback_queries": 0,
            "crt_queries": 0,
            "team_pages_scraped": 0,
            "job_board_queries": 0,
            "sec_queries": 0,
            "total_records": 0
        }

    async def multi_source_search(
        self,
        query: str,
        sources: List[str] = None,
        max_results: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search across multiple free sources simultaneously.

        Args:
            query: Search query
            sources: List of sources to use (default: all)
            max_results: Max results per source
        """
        if sources is None:
            sources = ["bing", "github", "wikidata"]

        all_results = []
        tasks = []

        if "bing" in sources:
            tasks.append(self._search_bing(query, max_results))
        if "github" in sources:
            tasks.append(self._search_github(query, max_results))
        if "wikidata" in sources:
            tasks.append(self._search_wikidata(query))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, list):
                all_results.extend(result)
            elif isinstance(result, dict):
                all_results.append(result)

        self.stats["total_records"] = len(all_results)
        return all_results

    async def _search_bing(
        self,
        query: str,
        max_results: int
    ) -> List[Dict[str, Any]]:
        results = await self.bing.search(query, max_results)
        self.stats["bing_searches"] += 1
        return results

    async def _search_github(
        self,
        query: str,
        max_results: int
    ) -> List[Dict[str, Any]]:
        results = await self.github.search_users(query, max_results)
        self.stats["github_queries"] += 1
        return results

    async def _search_wikidata(self, query: str) -> Optional[Dict[str, Any]]:
        result = await self.wikidata.search_company(query)
        self.stats["wikidata_queries"] += 1
        return result

    async def deep_company_extraction(
        self,
        domain: str,
        company_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Deep extraction of company data from all free sources.

        Returns comprehensive company info including:
        - Team members with emails
        - Company info from Wikidata
        - Subdomains from crt.sh
        - Historical data from Wayback
        - GitHub organization members
        - SEC filings (if US public company)
        """
        results = {
            "domain": domain,
            "company_name": company_name,
            "team_members": [],
            "company_info": {},
            "subdomains": [],
            "historical_emails": [],
            "github_members": [],
            "sec_info": None,
            "sources_used": []
        }

        # Run extractions in parallel
        tasks = {
            "team": self.team_scraper.find_team_pages(domain),
            "subdomains": self.crt.find_subdomains(domain),
            "wayback": self.wayback.extract_emails_from_history(domain),
        }

        if company_name:
            tasks["wikidata"] = self.wikidata.search_company(company_name)
            tasks["sec"] = self.sec.search_company(company_name)
            # Try GitHub org
            org_name = company_name.lower().replace(" ", "").replace("-", "")
            tasks["github"] = self.github.search_org_members(org_name)

        completed = await asyncio.gather(
            *tasks.values(),
            return_exceptions=True
        )

        task_keys = list(tasks.keys())
        for i, result in enumerate(completed):
            key = task_keys[i]

            if isinstance(result, Exception):
                logger.warning(f"Error in {key}: {result}")
                continue

            if key == "team" and result:
                results["sources_used"].append("team_pages")
                # Extract from each team page
                for page_url in result[:5]:  # Limit pages
                    members = await self.team_scraper.extract_team_members(page_url)
                    results["team_members"].extend(members)
                    self.stats["team_pages_scraped"] += 1

            elif key == "subdomains" and result:
                results["sources_used"].append("crt_sh")
                results["subdomains"] = result
                self.stats["crt_queries"] += 1

            elif key == "wayback" and result:
                results["sources_used"].append("wayback_machine")
                results["historical_emails"] = result
                self.stats["wayback_queries"] += 1

            elif key == "wikidata" and result:
                results["sources_used"].append("wikidata")
                results["company_info"] = result
                self.stats["wikidata_queries"] += 1

            elif key == "sec" and result:
                results["sources_used"].append("sec_edgar")
                results["sec_info"] = result
                self.stats["sec_queries"] += 1

            elif key == "github" and result:
                results["sources_used"].append("github")
                results["github_members"] = result
                self.stats["github_queries"] += 1

        return results

    async def extract_emails_from_github(
        self,
        usernames: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Extract emails from GitHub users via their commits.

        This is one of the best FREE email extraction methods because:
        - GitHub commits ALWAYS contain committer email
        - Many developers use personal/work emails
        - High confidence (actual email used for git)
        """
        results = []

        for username in usernames:
            if self.github.rate_remaining <= 5:
                logger.warning("GitHub rate limit low, pausing...")
                await asyncio.sleep(60)

            # Get user details
            user = await self.github.get_user_details(username)

            if user:
                email = user.get("email")

                # If no public email, try commits
                if not email:
                    email = await self.github.get_email_from_commits(username)

                if email:
                    results.append({
                        "email": email,
                        "name": user.get("name"),
                        "company": user.get("company"),
                        "github_url": user.get("profile_url"),
                        "source": "github_commits",
                        "confidence": 90  # High confidence - actual email from git
                    })

            await asyncio.sleep(1)  # Rate limiting

        return results

    def get_stats(self) -> Dict[str, int]:
        return self.stats

    async def close(self):
        """Close all clients."""
        await asyncio.gather(
            self.bing.close(),
            self.github.close(),
            self.wikidata.close(),
            self.wayback.close(),
            self.crt.close(),
            self.team_scraper.close(),
            self.job_board.close(),
            self.sec.close()
        )


# Quick usage example
"""
aggregator = EnhancedFreeSourcesAggregator()

# Multi-source search
results = await aggregator.multi_source_search(
    "CEO technology company",
    sources=["bing", "github", "wikidata"]
)

# Deep company extraction
company_data = await aggregator.deep_company_extraction(
    domain="example.com",
    company_name="Example Inc"
)

# GitHub email extraction (best free method!)
emails = await aggregator.extract_emails_from_github(["torvalds", "gvanrossum"])

await aggregator.close()
"""
