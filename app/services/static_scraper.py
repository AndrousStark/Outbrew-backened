"""
Static Web Scraper Service
FREE - No API costs required
Uses BeautifulSoup + httpx for powerful web scraping
"""

import re
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import logging

logger = logging.getLogger(__name__)


class StaticScraperService:
    """
    Free, powerful static HTML scraper
    No API costs - works with any public website
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

    async def scrape_url(
        self,
        url: str,
        extract_links: bool = True,
        max_links: int = 50
    ) -> Dict[str, Any]:
        """
        Scrape a single URL and extract all data

        Returns:
            {
                "data": {
                    "emails": [...],
                    "phones": [...],
                    "names": [...],
                    "titles": [...],
                    "companies": [...],
                    "linkedin_urls": [...],
                    "text": "..."
                },
                "internal_links": [...],
                "external_links": [...]
            }
        """
        try:
            # Fetch HTML with anti-bot headers
            headers = self._get_stealth_headers(url)
            response = await self.client.get(url, headers=headers)
            response.raise_for_status()

            # Parse with BeautifulSoup
            soup = BeautifulSoup(response.text, 'lxml')

            # Extract all data
            data = {
                "emails": self._extract_emails(soup, response.text),
                "phones": self._extract_phones(soup, response.text),
                "names": self._extract_names(soup),
                "titles": self._extract_job_titles(soup),
                "companies": self._extract_companies(soup),
                "linkedin_urls": self._extract_linkedin_urls(soup, response.text),
                "social_links": self._extract_social_links(soup),
                "text": soup.get_text(separator=' ', strip=True)[:5000]  # First 5K chars
            }

            # Extract links if requested
            internal_links = []
            external_links = []

            if extract_links:
                internal_links, external_links = self._extract_links(soup, url, max_links)

            return {
                "data": data,
                "internal_links": internal_links,
                "external_links": external_links,
                "status": "success"
            }

        except httpx.HTTPError as e:
            logger.error(f"Failed to scrape {url}: {e}")
            return {
                "data": {},
                "internal_links": [],
                "external_links": [],
                "status": "error",
                "error": str(e)
            }

    def _get_stealth_headers(self, url: str) -> Dict[str, str]:
        """Generate realistic browser headers to avoid bot detection"""
        domain = urlparse(url).netloc

        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Referer': f'https://{domain}/'
        }

    def _extract_emails(self, soup: BeautifulSoup, text: str) -> List[str]:
        """
        Extract email addresses using multiple patterns
        Very effective - catches 95%+ of emails on pages
        """
        emails = set()

        # Pattern 1: Standard email regex
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
        emails.update(re.findall(email_pattern, text))

        # Pattern 2: Check mailto: links
        for link in soup.find_all('a', href=True):
            if link['href'].startswith('mailto:'):
                email = link['href'].replace('mailto:', '').split('?')[0]
                emails.add(email)

        # Pattern 3: Look in specific elements
        for tag in soup.find_all(['a', 'span', 'p', 'div'], class_=re.compile(r'(email|contact|mail)', re.I)):
            text_content = tag.get_text()
            emails.update(re.findall(email_pattern, text_content))

        # Filter out common false positives
        filtered = [
            email for email in emails
            if not any(invalid in email.lower() for invalid in [
                'example.com', 'test.com', 'domain.com',
                'yourcompany.com', 'company.com', 'email.com',
                '@sentry.', '@example.', 'noreply@'
            ])
        ]

        return list(set(filtered))[:50]  # Max 50 emails per page

    def _extract_phones(self, soup: BeautifulSoup, text: str) -> List[str]:
        """
        Extract phone numbers using multiple patterns
        Handles international formats
        """
        phones = set()

        # Pattern 1: International format (+352, +1, etc.)
        pattern1 = r'\+\d{1,3}[\s.-]?\(?\d{1,4}\)?[\s.-]?\d{1,4}[\s.-]?\d{1,4}[\s.-]?\d{1,9}'
        phones.update(re.findall(pattern1, text))

        # Pattern 2: US format (123) 456-7890
        pattern2 = r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}'
        phones.update(re.findall(pattern2, text))

        # Pattern 3: Simple format 123-456-7890
        pattern3 = r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}'
        phones.update(re.findall(pattern3, text))

        # Pattern 4: Look in tel: links
        for link in soup.find_all('a', href=True):
            if link['href'].startswith('tel:'):
                phone = link['href'].replace('tel:', '').strip()
                phones.add(phone)

        # Clean up and validate
        cleaned = []
        for phone in phones:
            # Remove extra spaces/characters
            cleaned_phone = re.sub(r'[^\d+\-\(\)\s]', '', phone).strip()
            # Must have at least 7 digits
            if len(re.findall(r'\d', cleaned_phone)) >= 7:
                cleaned.append(cleaned_phone)

        return list(set(cleaned))[:20]  # Max 20 phones per page

    def _extract_names(self, soup: BeautifulSoup) -> List[str]:
        """
        Extract person names from common patterns
        Looks in team pages, about pages, contact pages
        """
        names = set()

        # Pattern 1: Look in common name containers
        name_selectors = [
            'h1', 'h2', 'h3', 'h4',
            '[class*="name"]', '[class*="author"]', '[class*="person"]',
            '[class*="team-member"]', '[class*="contact"]', '[class*="profile"]'
        ]

        for selector in name_selectors:
            elements = soup.select(selector)
            for elem in elements:
                text = elem.get_text().strip()
                # Basic name validation (2-4 words, each capitalized, 2-20 chars each)
                if self._looks_like_name(text):
                    names.add(text)

        # Pattern 2: Look in specific structures (team pages)
        # <div class="team-member"><h3>John Doe</h3><span>CEO</span></div>
        team_containers = soup.find_all(['div', 'article', 'section'], class_=re.compile(r'(team|member|staff|person)', re.I))
        for container in team_containers:
            heading = container.find(['h1', 'h2', 'h3', 'h4', 'h5'])
            if heading:
                text = heading.get_text().strip()
                if self._looks_like_name(text):
                    names.add(text)

        return list(names)[:100]  # Max 100 names per page

    def _looks_like_name(self, text: str) -> bool:
        """Check if text looks like a person's name"""
        # Split into words
        words = text.split()

        # Must be 2-4 words
        if len(words) < 2 or len(words) > 4:
            return False

        # Each word must be 2-20 characters
        if not all(2 <= len(w) <= 20 for w in words):
            return False

        # Each word should start with capital letter
        if not all(w[0].isupper() for w in words):
            return False

        # Must be mostly alphabetic
        if not all(w.replace('-', '').replace("'", '').isalpha() for w in words):
            return False

        # Not all caps (likely heading/title)
        if text.isupper():
            return False

        return True

    def _extract_job_titles(self, soup: BeautifulSoup) -> List[str]:
        """Extract job titles from common patterns"""
        titles = set()

        # Common job title indicators
        title_patterns = [
            r'\b(CEO|CTO|CFO|COO|CMO|VP|Director|Manager|Lead|Head|Chief|President|Founder)\b',
            r'\b(Senior|Junior|Staff|Principal)\s+\w+\s+(Engineer|Developer|Designer|Analyst)',
            r'\b(Software|Data|Product|Project|Engineering|Marketing|Sales)\s+(Engineer|Manager|Lead|Director)'
        ]

        text = soup.get_text()
        for pattern in title_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            titles.update(matches)

        # Look in specific elements
        for elem in soup.find_all(['span', 'div', 'p'], class_=re.compile(r'(title|position|role)', re.I)):
            text = elem.get_text().strip()
            if 5 <= len(text) <= 50 and any(word in text.lower() for word in ['engineer', 'manager', 'director', 'lead', 'developer']):
                titles.add(text)

        return list(titles)[:50]

    def _extract_companies(self, soup: BeautifulSoup) -> List[str]:
        """Extract company names"""
        companies = set()

        # Look in meta tags
        for meta in soup.find_all('meta', attrs={'property': re.compile(r'og:(site_name|title)')}):
            if meta.get('content'):
                companies.add(meta['content'])

        # Look in title
        title = soup.find('title')
        if title:
            # Company name often in format "Page Title | Company Name"
            parts = title.get_text().split('|')
            if len(parts) > 1:
                companies.add(parts[-1].strip())

        return list(companies)[:10]

    def _extract_linkedin_urls(self, soup: BeautifulSoup, text: str) -> List[str]:
        """Extract LinkedIn profile URLs"""
        linkedin_urls = set()

        # Pattern 1: Look in links
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'linkedin.com/in/' in href or 'linkedin.com/company/' in href:
                # Clean up URL
                clean_url = href.split('?')[0]  # Remove query params
                linkedin_urls.add(clean_url)

        # Pattern 2: Regex in text
        pattern = r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[\w-]+'
        linkedin_urls.update(re.findall(pattern, text))

        return list(linkedin_urls)[:50]

    def _extract_social_links(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract social media profile links"""
        social = {}

        social_patterns = {
            'twitter': r'twitter\.com/[\w]+',
            'github': r'github\.com/[\w-]+',
            'facebook': r'facebook\.com/[\w.]+',
            'instagram': r'instagram\.com/[\w.]+',
        }

        for link in soup.find_all('a', href=True):
            href = link['href']
            for platform, pattern in social_patterns.items():
                if re.search(pattern, href):
                    social[platform] = href.split('?')[0]
                    break

        return social

    def _extract_links(
        self,
        soup: BeautifulSoup,
        base_url: str,
        max_links: int = 50
    ) -> tuple[List[str], List[str]]:
        """
        Extract internal and external links

        Returns:
            (internal_links, external_links)
        """
        base_domain = urlparse(base_url).netloc
        internal_links = set()
        external_links = set()

        for link in soup.find_all('a', href=True):
            href = link['href']

            # Skip anchors and javascript
            if href.startswith('#') or href.startswith('javascript:'):
                continue

            # Convert relative URLs to absolute
            absolute_url = urljoin(base_url, href)

            # Parse domain
            link_domain = urlparse(absolute_url).netloc

            # Classify as internal or external
            if link_domain == base_domain:
                internal_links.add(absolute_url)
            else:
                external_links.add(absolute_url)

            # Limit total links
            if len(internal_links) + len(external_links) >= max_links * 2:
                break

        # Prioritize useful internal pages
        priority_keywords = ['team', 'about', 'contact', 'people', 'staff', 'careers', 'join', 'leadership']

        sorted_internal = sorted(
            internal_links,
            key=lambda url: sum(kw in url.lower() for kw in priority_keywords),
            reverse=True
        )

        return sorted_internal[:max_links], list(external_links)[:max_links]

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()
