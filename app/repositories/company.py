"""
Company Repository

Specialized repository for Company model with:
- Caching for frequently accessed companies
- Search by name/domain
- Intelligence data caching
- Relationship loading
"""
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session, selectinload, joinedload
import logging

from app.repositories.base import BaseRepository
from app.models.company import Company
from app.core.cache import cache

logger = logging.getLogger(__name__)


class CompanyRepository(BaseRepository[Company]):
    """
    Company-specific repository with caching and search.

    Key Features:
    - Aggressive caching (companies rarely change)
    - Domain-based lookups
    - Fuzzy name search
    - Intelligence data integration
    """

    def __init__(self, db: Session):
        super().__init__(Company, db)

    # ==================== CACHED OPERATIONS ====================

    def get_by_id_cached(self, id: int, ttl: int = 3600) -> Optional[Company]:
        """
        Get company by ID with caching.

        Companies change rarely, so we cache for 1 hour by default.
        """
        return self.get_by_id(id, use_cache=True)

    def get_by_domain(
        self,
        domain: str,
        use_cache: bool = True
    ) -> Optional[Company]:
        """
        Get company by domain with caching.

        Args:
            domain: Company domain (e.g., "google.com")
            use_cache: Use cache

        Returns:
            Company or None
        """
        cache_key = f"company:domain:{domain}"

        # Try cache
        if use_cache:
            cached = cache.get(cache_key)
            if cached:
                logger.debug(f"📦 [COMPANY-REPO] Cache hit for domain: {domain}")
                return cached

        # Query database
        company = self.get_by_field("domain", domain)

        # Cache result (even if None to prevent repeated queries)
        if use_cache:
            cache.set(cache_key, company, ttl=3600)

        return company

    def get_with_relations(self, id: int) -> Optional[Company]:
        """
        Get company with all relations.

        Loads:
        - Applications
        - Projects
        - Research cache
        - Skill matches
        """
        query = (
            self.db.query(Company)
            .filter(Company.id == id)
            .options(
                selectinload(Company.applications),
                selectinload(Company.projects),
                selectinload(Company.research_cache),
                selectinload(Company.skill_matches)
            )
        )

        query = self._apply_soft_delete_filter(query)

        return query.first()

    # ==================== SEARCH OPERATIONS ====================

    def search_by_name(
        self,
        name_query: str,
        skip: int = 0,
        limit: int = 20
    ) -> List[Company]:
        """
        Search companies by name (case-insensitive).

        Args:
            name_query: Search term
            skip: Pagination offset
            limit: Page size

        Returns:
            Matching companies
        """
        return self.search(
            search_fields=["name"],
            search_term=name_query,
            skip=skip,
            limit=limit
        )

    def search_by_industry(
        self,
        industry: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Company]:
        """Get companies in specific industry"""
        return self.get_all(
            skip=skip,
            limit=limit,
            filters={"industry": industry}
        )

    def get_with_tech_stack(
        self,
        tech_stack: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Company]:
        """
        Find companies using specific technology.

        Args:
            tech_stack: Technology to search for (e.g., "Python", "React")
            skip: Offset
            limit: Limit

        Returns:
            Companies using that tech
        """
        query = (
            self.db.query(Company)
            .filter(Company.tech_stack.ilike(f"%{tech_stack}%"))
        )

        query = self._apply_soft_delete_filter(query)

        return query.offset(skip).limit(limit).all()

    # ==================== INTELLIGENCE DATA ====================

    def get_or_create_by_domain(
        self,
        domain: str,
        company_data: Dict[str, Any] = None
    ) -> Company:
        """
        Get existing company or create new one.

        Useful for automatic company creation during application import.

        Args:
            domain: Company domain
            company_data: Optional data for new company

        Returns:
            Company instance
        """
        # Try to find existing
        company = self.get_by_domain(domain, use_cache=True)

        if company:
            logger.debug(f"✅ [COMPANY-REPO] Found existing company: {domain}")
            return company

        # Create new
        data = company_data or {}
        data["domain"] = domain

        # Extract name from domain if not provided
        if "name" not in data:
            data["name"] = domain.split(".")[0].title()

        company = self.create(data)

        logger.info(f"✅ [COMPANY-REPO] Created new company: {domain}")

        return company

    def update_intelligence_data(
        self,
        id: int,
        intelligence_data: Dict[str, Any]
    ) -> Optional[Company]:
        """
        Update company with AI research data.

        Args:
            id: Company ID
            intelligence_data: Dict with keys like:
                - tech_stack
                - industry
                - employee_count
                - description
                - etc.

        Returns:
            Updated company
        """
        return self.update(id, intelligence_data)

    # ==================== STATISTICS ====================

    def get_top_companies_by_applications(
        self,
        candidate_id: Optional[int] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get companies with most applications.

        Args:
            candidate_id: Optional filter by candidate
            limit: Number of results

        Returns:
            List of dicts with company info and application count
        """
        from sqlalchemy import func
        from app.models.application import Application

        query = (
            self.db.query(
                Company,
                func.count(Application.id).label("application_count")
            )
            .join(Application)
            .group_by(Company.id)
        )

        if candidate_id:
            query = query.filter(Application.candidate_id == candidate_id)

        query = self._apply_soft_delete_filter(query)

        results = query.order_by(func.count(Application.id).desc()).limit(limit).all()

        return [
            {
                "company": company,
                "application_count": count
            }
            for company, count in results
        ]
