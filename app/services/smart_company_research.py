"""Smart Company Research Service - Intelligent Company Analysis & Skill Matching"""
import re
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
import httpx
from bs4 import BeautifulSoup

# Setup logger for debugging
logger = logging.getLogger(__name__)

# HTTP retry configuration
MAX_RETRIES = 3
RETRY_DELAY_BASE = 1.0  # Base delay in seconds (exponential backoff)

from app.models.company import Company
from app.models.candidate import Candidate
from app.models.company_intelligence import (
    CompanyProject, CompanyResearchCache, SkillMatch, PersonalizedEmailDraft,
    CandidateSkillProfile, MatchStrengthEnum, ResearchDepthEnum, EmailToneEnum,
    ProjectTypeEnum, SKILL_CATEGORIES, EMAIL_TEMPLATES
)


class SmartCompanyResearchService:
    """
    Advanced company research service that:
    1. Deeply researches companies from multiple sources
    2. Extracts projects, technologies, and skill requirements
    3. Matches candidate skills with company needs
    4. Generates personalized email drafts
    """

    def __init__(self, db: Session):
        self.db = db
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        logger.debug("[SmartCompanyResearch] Service initialized")

    async def _fetch_with_retry(
        self,
        url: str,
        timeout: float = 30.0,
        headers: Optional[Dict] = None,
        max_retries: int = MAX_RETRIES
    ) -> Optional[httpx.Response]:
        """Fetch URL with exponential backoff retry logic"""
        logger.debug(f"[SmartCompanyResearch] Fetching URL: {url}")
        request_headers = headers or self.headers

        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.get(url, headers=request_headers, follow_redirects=True)
                    logger.debug(f"[SmartCompanyResearch] Successfully fetched {url} (status: {response.status_code})")
                    return response
            except httpx.HTTPError as e:
                delay = RETRY_DELAY_BASE * (2 ** attempt)
                logger.warning(
                    f"[SmartCompanyResearch] HTTP error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt < max_retries - 1:
                    logger.debug(f"[SmartCompanyResearch] Retrying in {delay}s...")
                    await asyncio.sleep(delay)
            except Exception as e:
                logger.error(f"[SmartCompanyResearch] Unexpected error fetching {url}: {e}")
                break

        logger.error(f"[SmartCompanyResearch] Failed to fetch {url} after {max_retries} attempts")
        return None

    # ============= COMPANY RESEARCH =============

    async def research_company(
        self,
        company_id: int,
        depth: ResearchDepthEnum = ResearchDepthEnum.STANDARD,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Comprehensive company research with caching
        """
        company = self.db.query(Company).filter(Company.id == company_id).first()
        if not company:
            raise ValueError(f"Company with ID {company_id} not found")

        # Check cache
        cache = self.db.query(CompanyResearchCache).filter(
            CompanyResearchCache.company_id == company_id
        ).first()

        if cache and not force_refresh:
            if cache.expires_at and cache.expires_at > datetime.now(timezone.utc):
                if cache.research_depth == depth or self._depth_value(cache.research_depth) >= self._depth_value(depth):
                    return self._format_cache_result(cache)

        # Perform research
        research_data = await self._perform_research(company, depth)

        # Update or create cache
        if cache:
            for key, value in research_data.items():
                if hasattr(cache, key):
                    setattr(cache, key, value)
            cache.research_depth = depth
            cache.last_refreshed = datetime.now(timezone.utc)
            cache.expires_at = datetime.now(timezone.utc) + timedelta(days=7)
        else:
            cache = CompanyResearchCache(
                company_id=company_id,
                research_depth=depth,
                expires_at=datetime.now(timezone.utc) + timedelta(days=7),
                **research_data
            )
            self.db.add(cache)

        # Extract and save projects
        await self._extract_projects(company, research_data)

        try:
            self.db.commit()
            logger.info(f"[SmartCompanyResearch] Research saved for company {company.id}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[SmartCompanyResearch] Failed to save research for company {company.id}: {e}")
            raise ValueError(f"Failed to save company research: {e}")

        return self._format_cache_result(cache)

    async def _perform_research(self, company: Company, depth: ResearchDepthEnum) -> Dict[str, Any]:
        """Perform actual research from multiple sources"""
        result = {
            "about_summary": "",
            "mission_statement": "",
            "company_culture": {},
            "recent_news": [],
            "job_openings": [],
            "key_people": [],
            "funding_info": {},
            "competitors": [],
            "tech_stack_detailed": {},
            "github_repos": [],
            "blog_posts": [],
            "patents": [],
            "social_links": {},
            "employee_count_estimate": None,
            "growth_signals": [],
            "completeness_score": 0.0,
            "data_sources": []
        }

        tasks = []

        # Website scraping
        if company.website_url:
            tasks.append(self._scrape_website(company.website_url, depth))

        # LinkedIn research (if URL available)
        if company.linkedin_url:
            tasks.append(self._research_linkedin(company.linkedin_url))

        # GitHub research (extract from domain)
        if company.website_url:
            tasks.append(self._research_github(company.name))

        # Careers page
        if company.careers_url:
            tasks.append(self._scrape_careers(company.careers_url))

        # Execute all research tasks
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, dict):
                    for key, value in res.items():
                        if key in result:
                            if isinstance(result[key], list):
                                result[key].extend(value if isinstance(value, list) else [value])
                            elif isinstance(result[key], dict):
                                result[key].update(value if isinstance(value, dict) else {})
                            elif not result[key]:
                                result[key] = value

        # Calculate completeness
        result["completeness_score"] = self._calculate_completeness(result)

        return result

    async def _scrape_website(self, url: str, depth: ResearchDepthEnum) -> Dict[str, Any]:
        """Scrape company website for information"""
        logger.info(f"[SmartCompanyResearch] Scraping website: {url}")
        result = {
            "about_summary": "",
            "tech_stack_detailed": {},
            "company_culture": {},
            "data_sources": ["website"]
        }

        try:
            response = await self._fetch_with_retry(url, timeout=30.0)
            if response and response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract about/description
                about_sections = soup.find_all(['section', 'div'], class_=re.compile(r'about|mission|story', re.I))
                for section in about_sections[:2]:
                    text = section.get_text(strip=True, separator=' ')[:1000]
                    if len(text) > 50:
                        result["about_summary"] = text
                        logger.debug(f"[SmartCompanyResearch] Extracted about summary ({len(text)} chars)")
                        break

                # Extract tech stack from meta tags, scripts, etc.
                result["tech_stack_detailed"] = self._extract_tech_from_html(soup)

                # Culture keywords
                result["company_culture"] = self._extract_culture(soup)

                # If deep research, crawl more pages
                if depth in [ResearchDepthEnum.DEEP, ResearchDepthEnum.EXHAUSTIVE]:
                    await self._deep_crawl(url, soup, result)

                logger.info(f"[SmartCompanyResearch] Successfully scraped website: {url}")
            else:
                logger.warning(f"[SmartCompanyResearch] Could not fetch website: {url}")

        except Exception as e:
            logger.error(f"[SmartCompanyResearch] Unexpected error scraping website {url}: {e}")

        return result

    async def _research_linkedin(self, url: str) -> Dict[str, Any]:
        """Research company LinkedIn presence"""
        result = {
            "employee_count_estimate": None,
            "key_people": [],
            "social_links": {"linkedin": url},
            "data_sources": ["linkedin"]
        }
        # LinkedIn scraping would require authentication
        # For now, extract from URL pattern
        return result

    async def _research_github(self, company_name: str) -> Dict[str, Any]:
        """Research company's GitHub presence"""
        logger.info(f"[SmartCompanyResearch] Researching GitHub for company: {company_name}")
        result = {
            "github_repos": [],
            "tech_stack_detailed": {},
            "data_sources": []
        }

        try:
            # Search GitHub for company repos
            search_name = company_name.lower().replace(" ", "-")
            github_url = f"https://api.github.com/orgs/{search_name}/repos"
            github_headers = {"Accept": "application/vnd.github.v3+json"}

            response = await self._fetch_with_retry(
                github_url,
                timeout=15.0,
                headers=github_headers
            )

            if response and response.status_code == 200:
                try:
                    repos = response.json()
                except (ValueError, TypeError) as json_error:
                    logger.warning(f"[SmartCompanyResearch] Invalid JSON from GitHub API: {json_error}")
                    repos = []

                if repos:
                    result["data_sources"].append("github")
                    logger.debug(f"[SmartCompanyResearch] Found {len(repos)} GitHub repos")

                tech_languages = {}
                for repo in repos[:20]:  # Limit to 20 repos
                    result["github_repos"].append({
                        "name": repo.get("name"),
                        "description": repo.get("description"),
                        "language": repo.get("language"),
                        "stars": repo.get("stargazers_count"),
                        "url": repo.get("html_url"),
                        "topics": repo.get("topics", [])
                    })

                    lang = repo.get("language")
                    if lang:
                        tech_languages[lang] = tech_languages.get(lang, 0) + 1

                if tech_languages:
                    result["tech_stack_detailed"]["languages"] = tech_languages
                    logger.debug(f"[SmartCompanyResearch] Detected languages: {list(tech_languages.keys())}")
            else:
                logger.warning(f"[SmartCompanyResearch] No GitHub organization found for {company_name}")

        except Exception as e:
            logger.error(f"[SmartCompanyResearch] Error researching GitHub for {company_name}: {e}")

        return result

    async def _scrape_careers(self, url: str) -> Dict[str, Any]:
        """Scrape careers page for job openings and required skills"""
        logger.info(f"[SmartCompanyResearch] Scraping careers page: {url}")
        result = {
            "job_openings": [],
            "growth_signals": [],
            "data_sources": ["careers"]
        }

        try:
            response = await self._fetch_with_retry(url, timeout=20.0)
            if response and response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job listings
                job_cards = soup.find_all(['article', 'div', 'li'], class_=re.compile(r'job|opening|position', re.I))
                logger.debug(f"[SmartCompanyResearch] Found {len(job_cards)} potential job cards")

                for card in job_cards[:30]:  # Limit to 30 jobs
                    title_elem = card.find(['h2', 'h3', 'h4', 'a', 'span'], class_=re.compile(r'title|name', re.I))
                    if title_elem:
                        job = {
                            "title": title_elem.get_text(strip=True),
                            "description": card.get_text(strip=True, separator=' ')[:500],
                            "skills_mentioned": self._extract_skills_from_text(card.get_text())
                        }
                        result["job_openings"].append(job)

                if len(result["job_openings"]) > 10:
                    result["growth_signals"].append("High hiring activity")

                logger.info(f"[SmartCompanyResearch] Extracted {len(result['job_openings'])} job openings from careers page")
            else:
                logger.warning(f"[SmartCompanyResearch] Could not fetch careers page: {url}")

        except Exception as e:
            logger.error(f"[SmartCompanyResearch] Error scraping careers page {url}: {e}")

        return result

    def _extract_tech_from_html(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract technology stack from HTML source"""
        tech = {"detected": [], "scripts": [], "meta": []}

        # Check script sources
        for script in soup.find_all('script', src=True):
            src = script['src'].lower()
            if 'react' in src:
                tech["detected"].append("React")
            elif 'vue' in src:
                tech["detected"].append("Vue.js")
            elif 'angular' in src:
                tech["detected"].append("Angular")
            elif 'jquery' in src:
                tech["detected"].append("jQuery")

        # Check meta tags
        generator = soup.find('meta', attrs={'name': 'generator'})
        if generator and generator.get('content'):
            tech["meta"].append(generator['content'])

        return tech

    def _extract_culture(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract company culture keywords"""
        culture_keywords = [
            "innovation", "teamwork", "diversity", "inclusion", "remote",
            "flexible", "growth", "learning", "collaboration", "impact",
            "mission", "values", "transparency", "autonomy"
        ]

        text = soup.get_text().lower()
        found = [kw for kw in culture_keywords if kw in text]

        return {
            "keywords": found,
            "remote_friendly": "remote" in text or "work from home" in text,
            "diversity_focus": "diversity" in text or "inclusion" in text
        }

    async def _deep_crawl(self, base_url: str, soup: BeautifulSoup, result: Dict):
        """Deep crawl additional pages"""
        # Find links to about, team, blog, etc.
        interesting_pages = ['about', 'team', 'blog', 'products', 'services', 'technology']

        links_to_crawl = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            for page in interesting_pages:
                if page in href and href.startswith(('/', 'http')):
                    if href.startswith('/'):
                        href = base_url.rstrip('/') + href
                    links_to_crawl.append(href)
                    break

        # Crawl first 3 interesting pages
        logger.debug(f"[CompanyResearch] Deep crawling {len(links_to_crawl[:3])} pages from {base_url}")
        async with httpx.AsyncClient(timeout=15.0) as client:
            for url in links_to_crawl[:3]:
                try:
                    logger.debug(f"[CompanyResearch] Crawling page: {url}")
                    response = await client.get(url, headers=self.headers, follow_redirects=True)
                    if response.status_code == 200:
                        page_soup = BeautifulSoup(response.text, 'html.parser')
                        # Extract additional info
                        if 'blog' in url:
                            posts = self._extract_blog_posts(page_soup)
                            result.get("blog_posts", []).extend(posts)
                            logger.debug(f"[CompanyResearch] Extracted {len(posts)} blog posts from {url}")
                        elif 'team' in url:
                            people = self._extract_team_members(page_soup)
                            result.get("key_people", []).extend(people)
                            logger.debug(f"[CompanyResearch] Extracted {len(people)} team members from {url}")
                except httpx.HTTPError as e:
                    logger.warning(f"[CompanyResearch] HTTP error crawling {url}: {e}")
                except Exception as e:
                    logger.warning(f"[CompanyResearch] Error crawling {url}: {e}")

    def _extract_blog_posts(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract blog post titles and topics"""
        posts = []
        articles = soup.find_all(['article', 'div'], class_=re.compile(r'post|article|blog', re.I))

        for article in articles[:10]:
            title_elem = article.find(['h1', 'h2', 'h3', 'a'])
            if title_elem:
                posts.append({
                    "title": title_elem.get_text(strip=True),
                    "tech_mentioned": self._extract_skills_from_text(article.get_text())
                })

        return posts

    def _extract_team_members(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract team member information"""
        people = []
        cards = soup.find_all(['div', 'article'], class_=re.compile(r'team|member|person', re.I))

        for card in cards[:20]:
            name_elem = card.find(['h2', 'h3', 'h4', 'span'], class_=re.compile(r'name', re.I))
            title_elem = card.find(['p', 'span'], class_=re.compile(r'title|role|position', re.I))

            if name_elem:
                people.append({
                    "name": name_elem.get_text(strip=True),
                    "title": title_elem.get_text(strip=True) if title_elem else None
                })

        return people

    def _extract_skills_from_text(self, text: str) -> List[str]:
        """Extract skill keywords from text"""
        text_lower = text.lower()
        found_skills = []

        for category, data in SKILL_CATEGORIES.items():
            for keyword in data["keywords"]:
                if keyword in text_lower:
                    found_skills.append(keyword)

        return list(set(found_skills))

    def _calculate_completeness(self, data: Dict) -> float:
        """Calculate data completeness score"""
        weights = {
            "about_summary": 0.15,
            "tech_stack_detailed": 0.20,
            "job_openings": 0.15,
            "github_repos": 0.15,
            "key_people": 0.10,
            "company_culture": 0.10,
            "blog_posts": 0.05,
            "social_links": 0.10
        }

        score = 0.0
        for key, weight in weights.items():
            value = data.get(key)
            if value:
                if isinstance(value, (list, dict)):
                    score += weight if len(value) > 0 else 0
                else:
                    score += weight

        return min(score, 1.0)

    def _depth_value(self, depth: ResearchDepthEnum) -> int:
        """Get numeric value for depth comparison"""
        mapping = {
            ResearchDepthEnum.QUICK: 1,
            ResearchDepthEnum.STANDARD: 2,
            ResearchDepthEnum.DEEP: 3,
            ResearchDepthEnum.EXHAUSTIVE: 4
        }
        return mapping.get(depth, 2)

    def _format_cache_result(self, cache: CompanyResearchCache) -> Dict[str, Any]:
        """Format cache data for API response"""
        return {
            "id": cache.id,
            "company_id": cache.company_id,
            "research_depth": cache.research_depth.value,
            "about_summary": cache.about_summary,
            "mission_statement": cache.mission_statement,
            "company_culture": cache.company_culture,
            "recent_news": cache.recent_news,
            "job_openings": cache.job_openings,
            "key_people": cache.key_people,
            "funding_info": cache.funding_info,
            "competitors": cache.competitors,
            "tech_stack_detailed": cache.tech_stack_detailed,
            "github_repos": cache.github_repos,
            "blog_posts": cache.blog_posts,
            "social_links": cache.social_links,
            "employee_count_estimate": cache.employee_count_estimate,
            "growth_signals": cache.growth_signals,
            "completeness_score": cache.completeness_score,
            "data_sources": cache.data_sources,
            "last_refreshed": cache.last_refreshed.isoformat() if cache.last_refreshed else None,
            "expires_at": cache.expires_at.isoformat() if cache.expires_at else None
        }

    async def _extract_projects(self, company: Company, research_data: Dict):
        """Extract and save company projects from research data"""
        existing_projects = {p.name.lower() for p in company.projects}

        # From GitHub repos
        for repo in research_data.get("github_repos", []):
            name = repo.get("name", "")
            if name.lower() not in existing_projects:
                project = CompanyProject(
                    company_id=company.id,
                    name=name,
                    description=repo.get("description"),
                    project_type=ProjectTypeEnum.OPEN_SOURCE,
                    url=repo.get("url"),
                    technologies=repo.get("topics", []) + ([repo.get("language")] if repo.get("language") else []),
                    source_url=repo.get("url"),
                    confidence_score=0.9
                )
                self.db.add(project)
                existing_projects.add(name.lower())

        # From blog posts (infer projects)
        for post in research_data.get("blog_posts", [])[:5]:
            tech = post.get("tech_mentioned", [])
            if len(tech) >= 2:
                title = post.get("title", "")
                if title.lower() not in existing_projects and len(title) < 100:
                    project = CompanyProject(
                        company_id=company.id,
                        name=f"Project: {title[:50]}",
                        description=f"Inferred from blog: {title}",
                        project_type=ProjectTypeEnum.PRODUCT,
                        technologies=tech,
                        confidence_score=0.5
                    )
                    self.db.add(project)

    # ============= SKILL EXTRACTION & PROFILING =============

    async def extract_candidate_skills(
        self,
        candidate_id: int,
        resume_text: Optional[str] = None
    ) -> CandidateSkillProfile:
        """Extract and categorize candidate skills"""
        candidate = self.db.query(Candidate).filter(Candidate.id == candidate_id).first()
        if not candidate:
            raise ValueError(f"Candidate {candidate_id} not found")

        # Get existing profile or create new
        profile = self.db.query(CandidateSkillProfile).filter(
            CandidateSkillProfile.candidate_id == candidate_id
        ).first()

        if not profile:
            profile = CandidateSkillProfile(candidate_id=candidate_id)
            self.db.add(profile)

        # Source skills from multiple places
        all_skills = []

        # From candidate's skills field
        if candidate.skills:
            if isinstance(candidate.skills, list):
                all_skills.extend(candidate.skills)
            elif isinstance(candidate.skills, str):
                all_skills.extend([s.strip() for s in candidate.skills.split(',')])

        # From resume text if provided
        if resume_text:
            all_skills.extend(self._extract_skills_from_text(resume_text))
            profile.work_experience = self._extract_work_experience(resume_text)
            profile.projects = self._extract_projects_from_resume(resume_text)
            profile.education = self._extract_education(resume_text)
            profile.achievements = self._extract_achievements(resume_text)

        # Categorize skills
        categorized = self._categorize_skills(all_skills)

        profile.programming_languages = categorized.get("programming_languages", [])
        profile.frameworks = categorized.get("frameworks", [])
        profile.databases = categorized.get("databases", [])
        profile.cloud_devops = categorized.get("cloud_devops", [])
        profile.tools = categorized.get("tools", [])
        profile.soft_skills = categorized.get("soft_skills", [])
        profile.domain_knowledge = categorized.get("domain_knowledge", [])

        # Set primary expertise (top 5 skills based on frequency/importance)
        profile.primary_expertise = self._determine_primary_skills(categorized)
        profile.secondary_skills = self._determine_secondary_skills(categorized, profile.primary_expertise)

        # Calculate completeness
        profile.completeness_score = self._calculate_profile_completeness(profile)
        profile.last_analyzed = datetime.now(timezone.utc)

        try:
            self.db.commit()
            logger.info(f"[SmartCompanyResearch] Candidate profile extracted for candidate {candidate_id}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[SmartCompanyResearch] Failed to save candidate profile: {e}")
            raise ValueError(f"Failed to save candidate profile: {e}")

        return profile

    def _categorize_skills(self, skills: List[str]) -> Dict[str, List[str]]:
        """Categorize skills into predefined categories"""
        categorized = {cat: [] for cat in SKILL_CATEGORIES.keys()}

        for skill in skills:
            skill_lower = skill.lower().strip()
            for category, data in SKILL_CATEGORIES.items():
                for keyword in data["keywords"]:
                    if keyword in skill_lower or skill_lower in keyword:
                        if skill not in categorized[category]:
                            categorized[category].append(skill)
                        break

        return categorized

    def _determine_primary_skills(self, categorized: Dict[str, List[str]]) -> List[str]:
        """Determine top 5 primary skills"""
        weighted_skills = []

        for category, skills in categorized.items():
            weight = SKILL_CATEGORIES[category]["weight"]
            for skill in skills:
                weighted_skills.append((skill, weight))

        # Sort by weight and take top 5
        weighted_skills.sort(key=lambda x: x[1], reverse=True)
        return [s[0] for s in weighted_skills[:5]]

    def _determine_secondary_skills(self, categorized: Dict[str, List[str]], primary: List[str]) -> List[str]:
        """Determine secondary skills (not in primary)"""
        secondary = []
        for skills in categorized.values():
            for skill in skills:
                if skill not in primary and skill not in secondary:
                    secondary.append(skill)
                    if len(secondary) >= 10:
                        return secondary
        return secondary

    def _extract_work_experience(self, text: str) -> List[Dict]:
        """Extract work experience from resume text"""
        experiences = []
        # Pattern matching for work experience sections
        exp_patterns = [
            r'(?:worked at|experience at|employed by)\s+([A-Z][a-zA-Z\s]+)',
            r'([A-Z][a-zA-Z\s]+)\s*[-–]\s*(?:Software|Engineer|Developer|Manager|Lead)',
        ]

        for pattern in exp_patterns:
            matches = re.findall(pattern, text)
            for match in matches[:5]:
                experiences.append({
                    "company": match.strip(),
                    "technologies": self._extract_skills_from_text(text)[:10]
                })

        return experiences

    def _extract_projects_from_resume(self, text: str) -> List[Dict]:
        """Extract projects from resume text"""
        projects = []
        # Look for project sections
        project_section = re.search(r'(?:projects?|portfolio)[\s\S]{0,2000}', text, re.I)

        if project_section:
            section_text = project_section.group()
            # Extract project names (typically title case phrases)
            project_names = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b', section_text)

            for name in project_names[:5]:
                projects.append({
                    "name": name,
                    "technologies": self._extract_skills_from_text(section_text)
                })

        return projects

    def _extract_education(self, text: str) -> List[Dict]:
        """Extract education from resume text"""
        education = []
        # Common degree patterns
        degree_patterns = [
            r"(Bachelor|Master|PhD|B\.S\.|M\.S\.|B\.Tech|M\.Tech)\s+(?:of|in)?\s*([A-Za-z\s]+)",
        ]

        for pattern in degree_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                education.append({
                    "degree": match[0],
                    "field": match[1].strip() if len(match) > 1 else None
                })

        return education

    def _extract_achievements(self, text: str) -> List[str]:
        """Extract achievements from resume text"""
        achievements = []
        # Look for achievement indicators
        achievement_patterns = [
            r'(?:achieved|accomplished|delivered|led|increased|reduced|improved)\s+([^.]+\.)',
            r'(?:awarded|recognized|certified)\s+([^.]+\.)',
        ]

        for pattern in achievement_patterns:
            matches = re.findall(pattern, text, re.I)
            achievements.extend(matches[:5])

        return achievements

    def _calculate_profile_completeness(self, profile: CandidateSkillProfile) -> float:
        """Calculate profile completeness score"""
        score = 0.0
        checks = [
            (profile.programming_languages, 0.2),
            (profile.frameworks, 0.15),
            (profile.databases, 0.1),
            (profile.cloud_devops, 0.1),
            (profile.tools, 0.1),
            (profile.projects, 0.15),
            (profile.work_experience, 0.1),
            (profile.education, 0.05),
            (profile.achievements, 0.05),
        ]

        for value, weight in checks:
            if value and len(value) > 0:
                score += weight

        return min(score, 1.0)

    # ============= SKILL MATCHING ENGINE =============

    async def match_candidate_to_company(
        self,
        candidate_id: int,
        company_id: int,
        force_refresh: bool = False
    ) -> SkillMatch:
        """
        Intelligent skill matching between candidate and company
        """
        # Get or create candidate profile
        profile = self.db.query(CandidateSkillProfile).filter(
            CandidateSkillProfile.candidate_id == candidate_id
        ).first()

        if not profile:
            profile = await self.extract_candidate_skills(candidate_id)

        # Get company research
        company = self.db.query(Company).filter(Company.id == company_id).first()
        if not company:
            raise ValueError(f"Company {company_id} not found")

        cache = self.db.query(CompanyResearchCache).filter(
            CompanyResearchCache.company_id == company_id
        ).first()

        if not cache or force_refresh:
            await self.research_company(company_id, ResearchDepthEnum.STANDARD)
            cache = self.db.query(CompanyResearchCache).filter(
                CompanyResearchCache.company_id == company_id
            ).first()

        # Check for existing match
        existing_match = self.db.query(SkillMatch).filter(
            and_(
                SkillMatch.candidate_id == candidate_id,
                SkillMatch.company_id == company_id
            )
        ).first()

        if existing_match and not force_refresh:
            if existing_match.expires_at and existing_match.expires_at > datetime.now(timezone.utc):
                return existing_match

        # Calculate match
        match_result = self._calculate_skill_match(profile, company, cache)

        try:
            if existing_match:
                for key, value in match_result.items():
                    setattr(existing_match, key, value)
                existing_match.calculated_at = datetime.now(timezone.utc)
                existing_match.expires_at = datetime.now(timezone.utc) + timedelta(days=3)
                self.db.commit()
                logger.info(f"[SmartCompanyResearch] Updated skill match for candidate {candidate_id}, company {company_id}")
                return existing_match
            else:
                skill_match = SkillMatch(
                    candidate_id=candidate_id,
                    company_id=company_id,
                    expires_at=datetime.now(timezone.utc) + timedelta(days=3),
                    **match_result
                )
                self.db.add(skill_match)
                self.db.commit()
                logger.info(f"[SmartCompanyResearch] Created skill match for candidate {candidate_id}, company {company_id}")
                return skill_match
        except Exception as e:
            self.db.rollback()
            logger.error(f"[SmartCompanyResearch] Failed to save skill match: {e}")
            raise ValueError(f"Failed to save skill match: {e}")

    def _calculate_skill_match(
        self,
        profile: CandidateSkillProfile,
        company: Company,
        cache: Optional[CompanyResearchCache]
    ) -> Dict[str, Any]:
        """Calculate detailed skill match"""
        # Gather company required skills
        company_skills = set()

        # From tech stack
        if company.tech_stack:
            if isinstance(company.tech_stack, list):
                company_skills.update([s.lower() for s in company.tech_stack])
            elif isinstance(company.tech_stack, dict):
                for skills in company.tech_stack.values():
                    if isinstance(skills, list):
                        company_skills.update([s.lower() for s in skills])

        # From cache
        if cache:
            if cache.tech_stack_detailed:
                for skills in cache.tech_stack_detailed.values():
                    if isinstance(skills, list):
                        company_skills.update([s.lower() for s in skills])
                    elif isinstance(skills, dict):
                        company_skills.update([s.lower() for s in skills.keys()])

            # From job openings
            for job in (cache.job_openings or []):
                if job.get("skills_mentioned"):
                    company_skills.update([s.lower() for s in job["skills_mentioned"]])

            # From GitHub repos
            for repo in (cache.github_repos or []):
                if repo.get("language"):
                    company_skills.add(repo["language"].lower())
                if repo.get("topics"):
                    company_skills.update([t.lower() for t in repo["topics"]])

        # Gather candidate skills
        candidate_skills = set()
        for skills in [
            profile.programming_languages,
            profile.frameworks,
            profile.databases,
            profile.cloud_devops,
            profile.tools,
            profile.soft_skills,
            profile.domain_knowledge
        ]:
            if skills:
                candidate_skills.update([s.lower() for s in skills])

        # Calculate matches
        matched_skills = list(company_skills & candidate_skills)
        candidate_skills_used = list(candidate_skills)
        company_needs = list(company_skills)

        # Calculate category scores
        category_scores = {}
        for category, data in SKILL_CATEGORIES.items():
            category_skills = getattr(profile, category, []) or []
            category_matches = sum(1 for s in category_skills if s.lower() in company_skills)
            if category_skills:
                category_scores[category] = category_matches / len(category_skills)
            else:
                category_scores[category] = 0.0

        # Calculate overall score (weighted)
        overall_score = 0.0
        for category, score in category_scores.items():
            weight = SKILL_CATEGORIES[category]["weight"]
            overall_score += score * weight * 100

        # Normalize to 0-100
        overall_score = min(overall_score * 2, 100)  # Multiply by 2 since partial matches are expected

        # Determine match strength
        if overall_score >= 80:
            match_strength = MatchStrengthEnum.PERFECT
        elif overall_score >= 60:
            match_strength = MatchStrengthEnum.STRONG
        elif overall_score >= 40:
            match_strength = MatchStrengthEnum.MODERATE
        elif overall_score >= 20:
            match_strength = MatchStrengthEnum.WEAK
        else:
            match_strength = MatchStrengthEnum.MINIMAL

        # Generate context and talking points
        match_context = self._generate_match_context(matched_skills, company, cache)
        talking_points = self._generate_talking_points(matched_skills, profile, company, cache)

        return {
            "match_strength": match_strength,
            "overall_score": round(overall_score, 2),
            "matched_skills": matched_skills,
            "candidate_skills_used": candidate_skills_used,
            "company_needs": company_needs,
            "category_scores": category_scores,
            "match_context": match_context,
            "talking_points": talking_points
        }

    def _generate_match_context(
        self,
        matched_skills: List[str],
        company: Company,
        cache: Optional[CompanyResearchCache]
    ) -> str:
        """Generate context explanation for the match"""
        if not matched_skills:
            return f"Limited direct skill overlap with {company.name}'s known requirements."

        context_parts = [f"Strong alignment with {company.name}"]

        if len(matched_skills) >= 5:
            context_parts.append(f"with {len(matched_skills)} matching skills")

        # Highlight key matches
        key_matches = matched_skills[:3]
        if key_matches:
            context_parts.append(f"including {', '.join(key_matches)}")

        # Add company-specific context
        if cache:
            if cache.job_openings:
                context_parts.append("matching active job requirements")
            if cache.github_repos:
                context_parts.append("with proven open-source contributions alignment")

        return ". ".join(context_parts) + "."

    def _generate_talking_points(
        self,
        matched_skills: List[str],
        profile: CandidateSkillProfile,
        company: Company,
        cache: Optional[CompanyResearchCache]
    ) -> List[str]:
        """Generate talking points for email drafting"""
        points = []

        # Primary skill match
        if profile.primary_expertise:
            primary_match = [s for s in profile.primary_expertise if s.lower() in [m.lower() for m in matched_skills]]
            if primary_match:
                points.append(f"Core expertise in {', '.join(primary_match[:3])} directly applies")

        # Project alignment
        if profile.projects and cache and cache.github_repos:
            points.append("Project experience aligns with company's technical initiatives")

        # Tech stack alignment
        tech_matches = [s for s in matched_skills if s.lower() in [k.lower() for k in SKILL_CATEGORIES["programming_languages"]["keywords"] + SKILL_CATEGORIES["frameworks"]["keywords"]]]
        if tech_matches:
            points.append(f"Technical stack match: {', '.join(tech_matches[:4])}")

        # Culture/domain fit
        if cache and cache.company_culture:
            if cache.company_culture.get("remote_friendly"):
                points.append("Compatible with remote work culture")

        # Growth opportunity
        if cache and cache.growth_signals:
            points.append("Company growth phase offers advancement opportunities")

        return points[:5]

    # ============= GET BEST MATCHES =============

    async def get_best_company_matches(
        self,
        candidate_id: int,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get best matching companies for a candidate"""
        # Get all existing matches
        matches = self.db.query(SkillMatch).filter(
            SkillMatch.candidate_id == candidate_id
        ).order_by(SkillMatch.overall_score.desc()).limit(limit).all()

        results = []
        for match in matches:
            company = self.db.query(Company).filter(Company.id == match.company_id).first()
            if company:
                results.append({
                    "company_id": company.id,
                    "company_name": company.name,
                    "industry": company.industry,
                    "match_strength": match.match_strength.value,
                    "overall_score": match.overall_score,
                    "matched_skills": match.matched_skills,
                    "talking_points": match.talking_points,
                    "match_context": match.match_context
                })

        return results

    async def batch_match_companies(
        self,
        candidate_id: int,
        company_ids: List[int]
    ) -> List[SkillMatch]:
        """Match candidate against multiple companies"""
        logger.info(f"[CompanyResearch] Batch matching candidate {candidate_id} against {len(company_ids)} companies")
        results = []
        for company_id in company_ids:
            try:
                match = await self.match_candidate_to_company(candidate_id, company_id)
                results.append(match)
                logger.debug(f"[CompanyResearch] Matched company {company_id}: score={match.overall_score}")
            except ValueError as e:
                logger.warning(f"[CompanyResearch] Company {company_id} not found: {e}")
            except Exception as e:
                logger.error(f"[CompanyResearch] Error matching company {company_id}: {e}")

        logger.info(f"[CompanyResearch] Batch matching complete: {len(results)} successful matches")
        return results
