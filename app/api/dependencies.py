"""API Dependencies - Sync and Async Support"""
from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from typing import AsyncGenerator

# Sync database (for backward compatibility)
from app.core.database import get_database_session

# Async database (Phase 2 optimization)
from app.core.database_async import get_async_db as get_async_session

from app.core.auth import (
    get_current_candidate,
    get_current_active_candidate,
    get_current_candidate_from_sse_ticket,
    require_super_admin,
    require_admin_or_owner
)

# ============================================================================
# DATABASE DEPENDENCIES
# ============================================================================

# Sync database dependency (backward compatibility)
get_db = get_database_session

# Async database dependency (Phase 2 - preferred for new endpoints)
async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Async database session dependency.

    Use this for new async endpoints:
        async def my_endpoint(db: AsyncSession = Depends(get_async_db)):
            ...
    """
    async for session in get_async_session():
        yield session

# ============================================================================
# AUTH DEPENDENCIES
# ============================================================================

# Auth dependencies (re-exported for convenience)
# Note: Auth functions currently return sync Candidate objects
# For fully async auth, these would need to be rewritten to use AsyncSession

__all__ = [
    # Sync dependencies (backward compatibility)
    "get_db",

    # Async dependencies (Phase 2 - preferred)
    "get_async_db",

    # Auth dependencies
    "get_current_candidate",
    "get_current_active_candidate",
    "get_current_candidate_from_sse_ticket",
    "require_super_admin",
    "require_admin_or_owner"
]
