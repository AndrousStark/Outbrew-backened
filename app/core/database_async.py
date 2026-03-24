"""
Async Database Configuration for Phase 2 Optimization

This module provides asynchronous database connectivity using:
- SQLAlchemy 2.0 async engine
- asyncpg driver (high-performance C implementation)
- AsyncSession for non-blocking I/O
- Connection pooling optimized for async workloads

PERFORMANCE GAINS:
- 5-10x throughput increase under concurrent load
- Non-blocking I/O operations
- Better resource utilization
- Handle 100+ concurrent requests easily

Based on 2025 best practices from:
- https://leapcell.io/blog/building-high-performance-async-apis-with-fastapi-sqlalchemy-2-0-and-asyncpg
- https://orchestrator.dev/blog/2025-1-30-fastapi-production-patterns/
"""
import logging
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
    AsyncEngine
)
from sqlalchemy.pool import NullPool, AsyncAdaptedQueuePool
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings

logger = logging.getLogger(__name__)

# Database connection timeout
DB_CONNECT_TIMEOUT = 30

# ==================== ASYNC ENGINE CONFIGURATION ====================

def get_async_database_url() -> str:
    """
    Get async database URL.

    For PostgreSQL with asyncpg:
    postgresql+asyncpg://user:password@host:port/database
    """
    if settings.DATABASE_URL:
        # Convert standard postgresql:// to async postgresql+asyncpg://
        url = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
        # asyncpg doesn't understand sslmode param — strip it (ssl is set in connect_args)
        url = url.replace("?sslmode=require", "").replace("&sslmode=require", "")
        logger.info("[DATABASE-ASYNC] Using DATABASE_URL with asyncpg driver")
        return url
    elif settings.POSTGRES_SERVER and settings.POSTGRES_SERVER != "localhost":
        # PostgreSQL with asyncpg driver
        url = (
            f"postgresql+asyncpg://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
            f"@{settings.POSTGRES_SERVER}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        )
        logger.info("[DATABASE-ASYNC] Using PostgreSQL with asyncpg driver")
        return url
    else:
        # SQLite with aiosqlite (for development)
        url = "sqlite+aiosqlite:///./outbrew.db"
        logger.info(f"[DATABASE-ASYNC] Using SQLite with aiosqlite: {url}")
        return url


# Create async engine with optimized settings
database_url = get_async_database_url()

if "sqlite" in database_url:
    # SQLite configuration (development)
    async_engine: AsyncEngine = create_async_engine(
        database_url,
        echo=settings.DEBUG,
        future=True,
        # SQLite specific settings
        connect_args={
            "check_same_thread": False,
            "timeout": DB_CONNECT_TIMEOUT
        },
        poolclass=NullPool  # SQLite doesn't support connection pooling well
    )
    logger.info("[DATABASE-ASYNC] Created SQLite async engine (no pooling)")

else:
    # PostgreSQL configuration
    is_neon = settings.DATABASE_URL and ("neon.tech" in settings.DATABASE_URL or "sslmode=require" in settings.DATABASE_URL)
    pool_size = 5 if is_neon else 20
    max_overflow = 5 if is_neon else 40

    async_connect_args = {
        "server_settings": {
            "jit": "off",  # Disable JIT for faster simple queries
            "application_name": "outbrew-api-async"
        },
        "command_timeout": 60,  # Query timeout
        "timeout": DB_CONNECT_TIMEOUT,  # Connection timeout
    }
    if is_neon:
        async_connect_args["ssl"] = True
        logger.info("[DATABASE-ASYNC] Neon PostgreSQL detected — using SSL + smaller pool")

    async_engine: AsyncEngine = create_async_engine(
        database_url,
        echo=settings.DEBUG,
        future=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        pool_use_lifo=True,
        connect_args=async_connect_args
    )
    logger.info(
        f"[DATABASE-ASYNC] Created PostgreSQL async engine: "
        f"pool_size={pool_size}, max_overflow={max_overflow}, timeout={DB_CONNECT_TIMEOUT}s"
    )


# Create async session maker
AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,  # Don't expire objects after commit (better for async)
    autoflush=False,  # Manual flush control
    autocommit=False  # Manual commit control
)

logger.info("[DATABASE-ASYNC] Async session maker configured")


# ==================== SESSION DEPENDENCY ====================

async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Async database session dependency.

    Usage in FastAPI endpoints:
        @router.get("/endpoint")
        async def endpoint(db: AsyncSession = Depends(get_async_db)):
            result = await db.execute(select(Model))
            return result.scalars().all()

    Features:
    - Non-blocking I/O
    - Automatic session cleanup
    - Error handling with rollback
    - Connection pool management
    """
    async with AsyncSessionLocal() as session:
        try:
            logger.debug("[DATABASE-ASYNC] Session created")
            yield session
            await session.commit()  # Auto-commit on success
            logger.debug("[DATABASE-ASYNC] Session committed")
        except SQLAlchemyError as e:
            await session.rollback()  # Rollback on error
            logger.error(f"[DATABASE-ASYNC] Session error, rolled back: {e}")
            raise
        except Exception as e:
            await session.rollback()
            logger.error(f"[DATABASE-ASYNC] Unexpected error, rolled back: {e}")
            raise
        finally:
            await session.close()
            logger.debug("[DATABASE-ASYNC] Session closed")


# Alternative: simpler version without auto-commit
async def get_async_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Simple async session without auto-commit.

    Use when you want manual transaction control.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


# ==================== HEALTH CHECK ====================

async def check_async_database_health() -> dict:
    """
    Check async database connectivity and health.

    Returns:
        dict with 'healthy' bool and 'message' string
    """
    try:
        async with async_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

        logger.debug("[DATABASE-ASYNC] Health check passed")
        return {
            "healthy": True,
            "message": "Async database connection successful",
            "driver": "asyncpg" if "postgresql" in database_url else "aiosqlite"
        }

    except SQLAlchemyError as e:
        logger.error(f"[DATABASE-ASYNC] Health check failed: {e}")
        return {
            "healthy": False,
            "message": f"Async database connection failed: {str(e)}",
            "driver": "asyncpg" if "postgresql" in database_url else "aiosqlite"
        }

    except Exception as e:
        logger.error(f"[DATABASE-ASYNC] Health check error: {e}")
        return {
            "healthy": False,
            "message": f"Async database error: {str(e)}"
        }


async def get_async_pool_stats() -> dict:
    """
    Get async connection pool statistics.

    Returns:
        Pool metrics for monitoring
    """
    try:
        pool = async_engine.pool

        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "total_connections": pool.size() + pool.overflow(),
            "utilization_percent": round(
                (pool.checkedout() / (pool.size() + pool.overflow()) * 100), 2
            ) if (pool.size() + pool.overflow()) > 0 else 0
        }

    except Exception as e:
        logger.error(f"[DATABASE-ASYNC] Failed to get pool stats: {e}")
        return {
            "error": str(e)
        }


# ==================== INITIALIZATION ====================

async def init_async_db():
    """
    Initialize async database (create tables).

    Note: For production, use Alembic migrations instead.
    """
    from app.core.database import import_models, Base

    logger.info("[DATABASE-ASYNC] Importing models...")
    import_models()

    logger.info("[DATABASE-ASYNC] Creating database tables...")
    try:
        async with async_engine.begin() as conn:
            # Run sync create_all in async context
            await conn.run_sync(Base.metadata.create_all)

        logger.info("[DATABASE-ASYNC] Database tables created successfully!")

    except SQLAlchemyError as e:
        logger.error(f"[DATABASE-ASYNC] Failed to create tables: {e}")
        raise


async def dispose_async_engine():
    """
    Dispose async engine (cleanup on shutdown).

    Call this in FastAPI shutdown event:
        @app.on_event("shutdown")
        async def shutdown():
            await dispose_async_engine()
    """
    logger.info("[DATABASE-ASYNC] Disposing async engine...")
    await async_engine.dispose()
    logger.info("[DATABASE-ASYNC] Async engine disposed")


# ==================== USAGE EXAMPLES ====================

"""
Example 1: Simple endpoint
    from app.core.database_async import get_async_db

    @router.get("/users")
    async def list_users(db: AsyncSession = Depends(get_async_db)):
        result = await db.execute(select(User))
        return result.scalars().all()


Example 2: With repository pattern
    from app.repositories.async_repositories import AsyncApplicationRepository

    @router.get("/applications")
    async def list_apps(db: AsyncSession = Depends(get_async_db)):
        repo = AsyncApplicationRepository(db)
        apps = await repo.get_list_with_relations()
        return apps


Example 3: Manual transaction control
    @router.post("/complex-operation")
    async def complex_operation(db: AsyncSession = Depends(get_async_db_session)):
        try:
            # Do multiple operations
            result1 = await db.execute(query1)
            result2 = await db.execute(query2)

            # Manual commit
            await db.commit()
            return {"success": True}

        except Exception as e:
            await db.rollback()
            raise HTTPException(500, str(e))


Example 4: Startup/shutdown events
    from fastapi import FastAPI
    from app.core.database_async import check_async_database_health, dispose_async_engine

    app = FastAPI()

    @app.on_event("startup")
    async def startup():
        health = await check_async_database_health()
        if not health["healthy"]:
            raise RuntimeError(f"Database unhealthy: {health['message']}")
        print("✅ Async database connected")

    @app.on_event("shutdown")
    async def shutdown():
        await dispose_async_engine()
        print("✅ Async engine disposed")
"""
