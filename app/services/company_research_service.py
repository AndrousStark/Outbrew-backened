"""Company Research Service with Multiple Web Scraping Libraries"""
import logging
import re
import time
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, List
from urllib.parse import urlparse, urljoin
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from app.models.company import Company

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_BASE = 1  # Base delay in seconds (exponential backoff)
REQUEST_TIMEOUT = 10  # Default timeout for HTTP requests


def retry_with_backoff(func):
    """Decorator for retrying HTTP requests with exponential backoff"""
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except requests.RequestException as e:
                last_exception = e
                delay = RETRY_DELAY_BASE * (2 ** attempt)
                logger.warning(
                    f"[CompanyResearch] Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}. "
                    f"Retrying in {delay}s..."
                )
                if attempt < MAX_RETRIES - 1:
                    time.sleep(delay)
        logger.error(f"[CompanyResearch] All {MAX_RETRIES} retry attempts failed")
        raise last_exception
    return wrapper


class CompanyResearchError(Exception):
    """Custom exception for company research errors"""
    pass


class CompanyResearchService:
    """Service for researching companies using web scraping"""

    def __init__(self, db: Session):
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        logger.debug("[CompanyResearch] Service initialized with new session")

    def __enter__(self):
        """Context manager entry - return self for use in with statement"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure session is closed"""
        self.close()
        logger.debug("[CompanyResearch] Session closed via context manager")
        return False  # Don't suppress exceptions

    def __del__(self):
        """Destructor - ensure session is closed if not already"""
        try:
            if hasattr(self, 'session') and self.session:
                self.session.close()
                logger.debug("[CompanyResearch] Session closed via destructor")
        except Exception:
            pass  # Ignore errors during cleanup

    def research_company(
        self,
        company: Company,
        scrape_website: bool = True,
        scrape_linkedin: bool = True,
        scrape_careers: bool = True
    ) -> Company:
        """
        Research a company by scraping various sources

        Args:
            company: The company to research
            scrape_website: Whether to scrape company website
            scrape_linkedin: Whether to scrape LinkedIn
            scrape_careers: Whether to scrape careers page

        Returns:
            Company: Updated company with research data
        """
        logger.info(f"Researching company: {company.name}")

        try:
            # Scrape website
            if scrape_website and company.website_url:
                self._scrape_website(company)

            # Scrape LinkedIn
            if scrape_linkedin and company.linkedin_url:
                self._scrape_linkedin(company)

            # Scrape careers page
            if scrape_careers and company.careers_url:
                self._scrape_careers_page(company)

            # Update research metadata
            company.last_researched_at = datetime.now(timezone.utc)
            company.research_source = "web_scraping"

            self.db.commit()
            self.db.refresh(company)

            logger.info(f"Successfully researched company: {company.name}")
            return company

        except Exception as e:
            logger.error(f"Error researching company {company.name}: {str(e)}")
            self.db.rollback()
            raise CompanyResearchError(f"Failed to research company: {str(e)}")

    def _fetch_url_with_retry(self, url: str) -> Optional[requests.Response]:
        """Fetch URL with retry logic and exponential backoff"""
        last_exception = None
        for attempt in range(MAX_RETRIES):
            try:
                logger.debug(f"[CompanyResearch] Fetching URL (attempt {attempt + 1}): {url}")
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                logger.debug(f"[CompanyResearch] Successfully fetched URL: {url}")
                return response
            except requests.RequestException as e:
                last_exception = e
                delay = RETRY_DELAY_BASE * (2 ** attempt)
                logger.warning(
                    f"[CompanyResearch] Request to {url} failed (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}"
                )
                if attempt < MAX_RETRIES - 1:
                    logger.debug(f"[CompanyResearch] Retrying in {delay}s...")
                    time.sleep(delay)

        logger.error(f"[CompanyResearch] All {MAX_RETRIES} attempts failed for URL: {url}")
        return None

    def _scrape_website(self, company: Company):
        """Scrape company website for information"""
        try:
            logger.info(f"[CompanyResearch] Scraping website: {company.website_url}")
            response = self._fetch_url_with_retry(company.website_url)
            if not response:
                logger.warning(f"[CompanyResearch] Could not fetch website: {company.website_url}")
                return

            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract description from meta tags
            if not company.description:
                meta_desc = soup.find('meta', {'name': 'description'}) or \
                           soup.find('meta', {'property': 'og:description'})
                if meta_desc and meta_desc.get('content'):
                    company.description = meta_desc['content'].strip()

            # Try to extract tech stack from website
            if not company.tech_stack:
                tech_stack = self._extract_tech_stack(soup)
                if tech_stack:
                    company.tech_stack = tech_stack

            # Try to find careers page if not set
            if not company.careers_url:
                careers_url = self._find_careers_url(company.website_url, soup)
                if careers_url:
                    company.careers_url = careers_url
                    logger.debug(f"[CompanyResearch] Found careers URL: {careers_url}")

            logger.info(f"[CompanyResearch] Successfully scraped website: {company.website_url}")

        except Exception as e:
            logger.error(f"[CompanyResearch] Unexpected error scraping website {company.website_url}: {str(e)}")

    def _scrape_linkedin(self, company: Company):
        """Scrape LinkedIn company page for information"""
        try:
            logger.info(f"[CompanyResearch] Scraping LinkedIn: {company.linkedin_url}")

            # Note: LinkedIn actively blocks scrapers, so this might not work reliably
            # Consider using LinkedIn API or Proxycurl API for production
            response = self._fetch_url_with_retry(company.linkedin_url)
            if not response:
                logger.warning(f"[CompanyResearch] Could not fetch LinkedIn: {company.linkedin_url}")
                logger.info("[CompanyResearch] Note: LinkedIn actively blocks scrapers. Consider using LinkedIn API or Proxycurl for production.")
                return

            soup = BeautifulSoup(response.content, 'html.parser')

            # Try to extract company size
            if not company.company_size:
                # LinkedIn often shows this in specific elements
                size_elem = soup.find(text=re.compile(r'\d+[-–]\d+ employees', re.I))
                if size_elem:
                    company.company_size = size_elem.strip()
                    logger.debug(f"[CompanyResearch] Extracted company size: {company.company_size}")

            # Try to extract industry
            if not company.industry:
                industry_elem = soup.find('div', class_=re.compile(r'industry', re.I))
                if industry_elem:
                    company.industry = industry_elem.get_text(strip=True)
                    logger.debug(f"[CompanyResearch] Extracted industry: {company.industry}")

            logger.info(f"[CompanyResearch] Successfully scraped LinkedIn: {company.linkedin_url}")

        except Exception as e:
            logger.error(f"[CompanyResearch] Unexpected error scraping LinkedIn {company.linkedin_url}: {str(e)}")

    def _scrape_careers_page(self, company: Company):
        """Scrape careers page for job postings"""
        try:
            logger.info(f"[CompanyResearch] Scraping careers page: {company.careers_url}")
            response = self._fetch_url_with_retry(company.careers_url)
            if not response:
                logger.warning(f"[CompanyResearch] Could not fetch careers page: {company.careers_url}")
                return

            soup = BeautifulSoup(response.content, 'html.parser')

            # Try to extract job postings
            # This is highly site-specific, so we'll use common patterns
            job_postings = self._extract_job_postings(soup, company.careers_url)

            # Store job postings (would need to determine which candidate to assign to)
            # For now, we'll just log the count
            if job_postings:
                logger.info(f"[CompanyResearch] Found {len(job_postings)} job postings on careers page")
            else:
                logger.debug(f"[CompanyResearch] No job postings found on careers page")

        except Exception as e:
            logger.error(f"[CompanyResearch] Unexpected error scraping careers page {company.careers_url}: {str(e)}")

    def _extract_tech_stack(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract technology stack from website HTML"""
        tech_keywords = {
            'languages': ['Python', 'Java', 'JavaScript', 'TypeScript', 'Go', 'Rust', 'C++', 'C#', 'Ruby', 'PHP'],
            'frameworks': ['React', 'Angular', 'Vue', 'Django', 'Flask', 'FastAPI', 'Spring', 'Express', 'Rails', 'Laravel'],
            'databases': ['PostgreSQL', 'MySQL', 'MongoDB', 'Redis', 'Elasticsearch', 'Cassandra', 'DynamoDB'],
            'cloud': ['AWS', 'Azure', 'GCP', 'Google Cloud', 'Heroku', 'DigitalOcean'],
            'tools': ['Docker', 'Kubernetes', 'Jenkins', 'GitLab', 'GitHub Actions', 'Terraform']
        }

        tech_stack = {}
        page_text = soup.get_text().lower()

        for category, keywords in tech_keywords.items():
            found_tech = [tech for tech in keywords if tech.lower() in page_text]
            if found_tech:
                tech_stack[category] = found_tech

        return tech_stack if tech_stack else None

    def _find_careers_url(self, base_url: str, soup: BeautifulSoup) -> Optional[str]:
        """Find careers/jobs page URL from website"""
        careers_keywords = ['careers', 'jobs', 'join', 'hiring', 'opportunities', 'work-with-us']

        # Look for links containing careers keywords
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if any(keyword in href for keyword in careers_keywords):
                # Convert relative URLs to absolute
                return urljoin(base_url, link['href'])

        return None

    def _extract_job_postings(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract job postings from careers page"""
        job_postings = []

        # Common patterns for job posting elements
        job_elements = soup.find_all(['div', 'li', 'article'], class_=re.compile(r'job|position|role|opening', re.I))

        for job_elem in job_elements[:20]:  # Limit to 20 to avoid performance issues
            try:
                title_elem = job_elem.find(['h2', 'h3', 'h4', 'a', 'span'], class_=re.compile(r'title|name|position', re.I))
                if not title_elem:
                    title_elem = job_elem.find('a')

                if title_elem:
                    title = title_elem.get_text(strip=True)
                    link_elem = job_elem.find('a', href=True)
                    link = urljoin(base_url, link_elem['href']) if link_elem else None

                    # Try to extract location
                    location_elem = job_elem.find(['span', 'div'], class_=re.compile(r'location|city|country', re.I))
                    location = location_elem.get_text(strip=True) if location_elem else None

                    job_postings.append({
                        'title': title,
                        'url': link,
                        'location': location
                    })

            except Exception as e:
                logger.debug(f"Error parsing job element: {str(e)}")
                continue

        return job_postings

    def enrich_company_from_domain(self, domain: str) -> Optional[Company]:
        """
        Create and enrich a company using just the domain

        Args:
            domain: Company domain (e.g., "google.com")

        Returns:
            Company: Enriched company object or None
        """
        try:
            # Ensure domain has protocol
            if not domain.startswith(('http://', 'https://')):
                domain = f"https://{domain}"

            # Parse domain
            parsed = urlparse(domain)
            company_name = parsed.netloc.replace('www.', '').split('.')[0].title()

            # Check if company already exists
            existing_company = self.db.query(Company).filter(
                Company.domain == parsed.netloc
            ).first()

            if existing_company:
                logger.info(f"Company {company_name} already exists")
                return existing_company

            # Create new company
            company = Company(
                name=company_name,
                domain=parsed.netloc,
                website_url=domain
            )

            self.db.add(company)
            self.db.commit()
            self.db.refresh(company)

            # Research the company
            return self.research_company(company)

        except Exception as e:
            logger.error(f"Error enriching company from domain {domain}: {str(e)}")
            self.db.rollback()
            return None

    def search_company_info(self, company_name: str) -> Dict:
        """
        Search for company information using search engines (simplified approach)

        Args:
            company_name: Name of the company to search

        Returns:
            Dict: Basic company information found
        """
        logger.info(f"[CompanyResearch] Searching for company info: {company_name}")
        # Note: This is a basic implementation
        # For production, consider using:
        # - Clearbit API
        # - Hunter.io
        # - Proxycurl (for LinkedIn data)
        # - Google Custom Search API
        # - SerpAPI

        try:
            # Simple DuckDuckGo search (doesn't require API key)
            search_url = f"https://lite.duckduckgo.com/lite/?q={company_name}+company"
            response = self._fetch_url_with_retry(search_url)
            if not response:
                logger.warning(f"[CompanyResearch] Could not search for company: {company_name}")
                return {'error': 'Failed to fetch search results after retries'}

            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract search results
            results = []
            for result in soup.find_all('a', class_='result-link')[:5]:
                results.append({
                    'title': result.get_text(strip=True),
                    'url': result.get('href', '')
                })

            logger.info(f"[CompanyResearch] Found {len(results)} search results for {company_name}")
            return {
                'company_name': company_name,
                'search_results': results,
                'source': 'duckduckgo'
            }

        except Exception as e:
            logger.error(f"[CompanyResearch] Error searching for company {company_name}: {str(e)}")
            return {'error': str(e)}

    def close(self):
        """Close the requests session"""
        self.session.close()
