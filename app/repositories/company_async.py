"""
Async Company Repository for Phase 2 Optimization

Specialized async repository for Company model with:
- Non-blocking I/O
- Aggressive caching (companies rarely change)
- Domain-based lookups
- Intelligence data integration
"""
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload, joinedload
import logging

from app.repositories.base_async import AsyncBaseRepository
from app.models.company import Company
from app.core.cache_async import async_cache

logger = logging.getLogger(__name__)


class AsyncCompanyRepository(AsyncBaseRepository[Company]):
    """
    Async company-specific repository with caching and search.

    Key Features:
    - Aggressive caching (companies rarely change)
    - Domain-based lookups
    - Fuzzy name search
    - Intelligence data integration
    - Non-blocking operations
    """

    def __init__(self, db: AsyncSession):
        super().__init__(Company, db)

    # ==================== CACHED OPERATIONS ====================

    async def get_by_id_cached(self, id: int, ttl: int = 3600) -> Optional[Company]:
        """
        Get company by ID with caching (async).

        Companies change rarely, so we cache for 1 hour by default.
        """
        return await self.get_by_id(id, use_cache=True)

    async def get_by_domain(
        self,
        domain: str,
        use_cache: bool = True
    ) -> Optional[Company]:
        """
        Get company by domain with caching (async).

        Args:
            domain: Company domain (e.g., "google.com")
            use_cache: Use cache

        Returns:
            Company or None
        """
        cache_key = f"company:domain:{domain}"

        # Try cache
        if use_cache:
            cached = await async_cache.get(cache_key)
            if cached:
                logger.debug(f"📦 [COMPANY-REPO-ASYNC] Cache hit for domain: {domain}")
                return cached

        # Query database
        company = await self.get_by_field("domain", domain)

        # Cache result (even if None to prevent repeated queries)
        if use_cache:
            await async_cache.set(cache_key, company, ttl=3600)

        return company

    async def get_with_relations(self, id: int) -> Optional[Company]:
        """
        Get company with all relations (async).

        Loads:
        - Applications
        - Projects
        - Research cache
        - Skill matches
        """
        stmt = (
            select(Company)
            .where(Company.id == id)
            .options(
                selectinload(Company.applications),
                selectinload(Company.projects),
                selectinload(Company.research_cache),
                selectinload(Company.skill_matches)
            )
        )

        stmt = self._apply_soft_delete_filter(stmt)

        result = await self.db.execute(stmt)
        return result.scalars().first()

    # ==================== SEARCH OPERATIONS ====================

    async def search_by_name(
        self,
        name_query: str,
        skip: int = 0,
        limit: int = 20
    ) -> List[Company]:
        """
        Search companies by name (case-insensitive, async).

        Args:
            name_query: Search term
            skip: Pagination offset
            limit: Page size

        Returns:
            Matching companies
        """
        return await self.search(
            search_fields=["name"],
            search_term=name_query,
            skip=skip,
            limit=limit
        )

    async def search_by_industry(
        self,
        industry: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Company]:
        """Get companies in specific industry (async)"""
        return await self.get_all(
            skip=skip,
            limit=limit,
            filters={"industry": industry}
        )

    async def get_with_tech_stack(
        self,
        tech_stack: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Company]:
        """
        Find companies using specific technology (async).

        Args:
            tech_stack: Technology to search for (e.g., "Python", "React")
            skip: Offset
            limit: Limit

        Returns:
            Companies using that tech
        """
        stmt = (
            select(Company)
            .where(Company.tech_stack.ilike(f"%{tech_stack}%"))
        )

        stmt = self._apply_soft_delete_filter(stmt)
        stmt = stmt.offset(skip).limit(limit)

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    # ==================== INTELLIGENCE DATA ====================

    async def get_or_create_by_domain(
        self,
        domain: str,
        company_data: Dict[str, Any] = None
    ) -> Company:
        """
        Get existing company or create new one (async).

        Useful for automatic company creation during application import.

        Args:
            domain: Company domain
            company_data: Optional data for new company

        Returns:
            Company instance
        """
        # Try to find existing
        company = await self.get_by_domain(domain, use_cache=True)

        if company:
            logger.debug(f"✅ [COMPANY-REPO-ASYNC] Found existing company: {domain}")
            return company

        # Create new
        data = company_data or {}
        data["domain"] = domain

        # Extract name from domain if not provided
        if "name" not in data:
            data["name"] = domain.split(".")[0].title()

        company = await self.create(data)

        logger.info(f"✅ [COMPANY-REPO-ASYNC] Created new company: {domain}")

        return company

    async def update_intelligence_data(
        self,
        id: int,
        intelligence_data: Dict[str, Any]
    ) -> Optional[Company]:
        """
        Update company with AI research data (async).

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
        return await self.update(id, intelligence_data)

    # ==================== STATISTICS ====================

    async def get_top_companies_by_applications(
        self,
        candidate_id: Optional[int] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get companies with most applications (async).

        Args:
            candidate_id: Optional filter by candidate
            limit: Number of results

        Returns:
            List of dicts with company info and application count
        """
        from app.models.application import Application

        stmt = (
            select(
                Company,
                func.count(Application.id).label("application_count")
            )
            .join(Application)
            .group_by(Company.id)
        )

        if candidate_id:
            stmt = stmt.where(Application.candidate_id == candidate_id)

        stmt = self._apply_soft_delete_filter(stmt)
        stmt = stmt.order_by(func.count(Application.id).desc()).limit(limit)

        result = await self.db.execute(stmt)
        rows = result.all()

        return [
            {
                "company": company,
                "application_count": count
            }
            for company, count in rows
        ]
