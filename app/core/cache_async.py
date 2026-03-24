"""
Async Redis Cache Manager for Phase 2 Optimization

True asynchronous caching with non-blocking I/O.

PERFORMANCE BENEFITS:
- Non-blocking Redis operations
- Better concurrency under load
- Async/await pattern throughout
- Integrates seamlessly with async endpoints

Based on redis.asyncio (built into redis>=5.0)
"""
import json
import hashlib
import logging
from typing import Any, Optional, Callable
from functools import wraps

from redis import asyncio as aioredis
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError

from app.core.config import settings

logger = logging.getLogger(__name__)


class AsyncCacheManager:
    """
    Async Redis cache manager with non-blocking I/O.

    Features:
    - Full async/await support
    - Connection pooling
    - TTL management
    - Pattern-based deletion
    - Graceful degradation
    """

    def __init__(self, redis_url: str = None, prefix: str = "hrweb"):
        """
        Initialize async cache manager.

        Args:
            redis_url: Redis connection URL
            prefix: Global prefix for cache keys
        """
        self.prefix = prefix
        self.redis_url = redis_url or getattr(settings, 'REDIS_URL', 'redis://localhost:6379/0')
        self._redis_client = None
        self._connection_error_logged = False

    async def _connect(self):
        """Establish async Redis connection"""
        if self._redis_client:
            return

        # Check if Redis is enabled and URL is valid
        if not getattr(settings, 'REDIS_ENABLED', True):
            if not self._connection_error_logged:
                logger.info("⚠️  [CACHE-ASYNC] Redis is disabled in settings. Caching disabled.")
                self._connection_error_logged = True
            self._redis_client = None
            return

        if not self.redis_url or not self.redis_url.strip():
            if not self._connection_error_logged:
                logger.warning("⚠️  [CACHE-ASYNC] Redis URL is empty. Caching disabled.")
                self._connection_error_logged = True
            self._redis_client = None
            return

        try:
            # Create async Redis client with connection pool
            self._redis_client = await aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
                max_connections=50
            )

            # Test connection
            await self._redis_client.ping()
            logger.info(f"✅ [CACHE-ASYNC] Connected to Redis at {self.redis_url}")
            self._connection_error_logged = False

        except (RedisError, RedisConnectionError, ValueError) as e:
            if not self._connection_error_logged:
                logger.warning(f"⚠️  [CACHE-ASYNC] Redis connection failed: {e}. Caching disabled.")
                self._connection_error_logged = True
            self._redis_client = None

    def _make_key(self, key: str) -> str:
        """Generate prefixed cache key"""
        return f"{self.prefix}:{key}"

    def _serialize(self, value: Any) -> str:
        """Serialize to JSON"""
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError) as e:
            logger.error(f"❌ [CACHE-ASYNC] Serialization error: {e}")
            return json.dumps(str(value))

    def _deserialize(self, value: str) -> Any:
        """Deserialize from JSON"""
        try:
            return json.loads(value)
        except (TypeError, ValueError) as e:
            logger.error(f"❌ [CACHE-ASYNC] Deserialization error: {e}")
            return value

    async def is_connected(self) -> bool:
        """Check if Redis is connected"""
        if not self._redis_client:
            await self._connect()

        if not self._redis_client:
            return False

        try:
            await self._redis_client.ping()
            return True
        except (RedisError, RedisConnectionError):
            return False

    # ==================== ASYNC OPERATIONS ====================

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache (async).

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        if not await self.is_connected():
            return None

        try:
            full_key = self._make_key(key)
            value = await self._redis_client.get(full_key)

            if value is None:
                logger.debug(f"🔍 [CACHE-ASYNC] Miss: {key}")
                return None

            logger.debug(f"✅ [CACHE-ASYNC] Hit: {key}")
            return self._deserialize(value)

        except (RedisError, RedisConnectionError) as e:
            logger.error(f"❌ [CACHE-ASYNC] Get error for '{key}': {e}")
            return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl: int = 3600,
        nx: bool = False,
        xx: bool = False
    ) -> bool:
        """
        Set value in cache (async).

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
            nx: Only set if doesn't exist
            xx: Only set if exists

        Returns:
            True if successful
        """
        if not await self.is_connected():
            return False

        try:
            full_key = self._make_key(key)
            serialized = self._serialize(value)

            result = await self._redis_client.set(
                full_key,
                serialized,
                ex=ttl,
                nx=nx,
                xx=xx
            )

            if result:
                logger.debug(f"💾 [CACHE-ASYNC] Set: {key} (TTL: {ttl}s)")
            return bool(result)

        except (RedisError, RedisConnectionError) as e:
            logger.error(f"❌ [CACHE-ASYNC] Set error for '{key}': {e}")
            return False

    async def delete(self, key: str) -> bool:
        """
        Delete key from cache (async).

        Args:
            key: Cache key

        Returns:
            True if deleted
        """
        if not await self.is_connected():
            return False

        try:
            full_key = self._make_key(key)
            result = await self._redis_client.delete(full_key)
            logger.debug(f"🗑️  [CACHE-ASYNC] Delete: {key}")
            return bool(result)

        except (RedisError, RedisConnectionError) as e:
            logger.error(f"❌ [CACHE-ASYNC] Delete error for '{key}': {e}")
            return False

    async def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching pattern (async).

        Args:
            pattern: Pattern to match

        Returns:
            Number of keys deleted
        """
        if not await self.is_connected():
            return 0

        try:
            full_pattern = self._make_key(pattern)
            keys = []

            # Scan for keys (cursor-based for large datasets)
            async for key in self._redis_client.scan_iter(match=full_pattern):
                keys.append(key)

            if not keys:
                return 0

            deleted = await self._redis_client.delete(*keys)
            logger.info(f"🗑️  [CACHE-ASYNC] Deleted {deleted} keys matching '{pattern}'")
            return deleted

        except (RedisError, RedisConnectionError) as e:
            logger.error(f"❌ [CACHE-ASYNC] Delete pattern error for '{pattern}': {e}")
            return 0

    async def exists(self, key: str) -> bool:
        """Check if key exists (async)"""
        if not await self.is_connected():
            return False

        try:
            full_key = self._make_key(key)
            return bool(await self._redis_client.exists(full_key))
        except (RedisError, RedisConnectionError):
            return False

    async def ttl(self, key: str) -> int:
        """Get remaining TTL (async)"""
        if not await self.is_connected():
            return -2

        try:
            full_key = self._make_key(key)
            return await self._redis_client.ttl(full_key)
        except (RedisError, RedisConnectionError):
            return -2

    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """Increment integer value (async)"""
        if not await self.is_connected():
            return None

        try:
            full_key = self._make_key(key)
            return await self._redis_client.incrby(full_key, amount)
        except (RedisError, RedisConnectionError) as e:
            logger.error(f"❌ [CACHE-ASYNC] Increment error for '{key}': {e}")
            return None

    async def get_stats(self) -> dict:
        """Get cache statistics (async)"""
        if not await self.is_connected():
            return {"connected": False, "error": "Redis not connected"}

        try:
            info = await self._redis_client.info()
            return {
                "connected": True,
                "total_keys": await self._redis_client.dbsize(),
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": self._calculate_hit_rate(info)
            }
        except (RedisError, RedisConnectionError) as e:
            return {"connected": False, "error": str(e)}

    def _calculate_hit_rate(self, info: dict) -> float:
        """Calculate cache hit rate"""
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        total = hits + misses

        if total == 0:
            return 0.0

        return round((hits / total) * 100, 2)

    async def flush_all(self) -> bool:
        """Flush all keys (DANGER!)"""
        if not await self.is_connected():
            return False

        try:
            await self._redis_client.flushdb()
            logger.warning("⚠️  [CACHE-ASYNC] Flushed all cache keys!")
            return True
        except (RedisError, RedisConnectionError) as e:
            logger.error(f"❌ [CACHE-ASYNC] Flush error: {e}")
            return False

    # ==================== DECORATOR ====================

    def cached(
        self,
        ttl: int = 3600,
        key_prefix: str = "",
        key_builder: Optional[Callable] = None
    ):
        """
        Async decorator for caching function results.

        Usage:
            @async_cache.cached(ttl=600, key_prefix="user")
            async def get_user(user_id: int):
                return await db.query(User).get(user_id)
        """
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Build cache key
                if key_builder:
                    cache_key = key_builder(*args, **kwargs)
                else:
                    args_str = f"{args}:{kwargs}"
                    args_hash = hashlib.md5(args_str.encode()).hexdigest()[:16]
                    cache_key = f"{key_prefix}:{func.__name__}:{args_hash}"

                # Try cache
                cached_result = await self.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"📦 [CACHE-ASYNC] Using cached result for {func.__name__}")
                    return cached_result

                # Execute function
                logger.debug(f"🔄 [CACHE-ASYNC] Computing result for {func.__name__}")
                result = await func(*args, **kwargs)

                # Cache result
                await self.set(cache_key, result, ttl=ttl)

                return result

            return wrapper
        return decorator

    # ==================== CLEANUP ====================

    async def close(self):
        """Close async Redis connection"""
        if self._redis_client:
            await self._redis_client.close()
            logger.info("[CACHE-ASYNC] Connection closed")


# ==================== GLOBAL INSTANCE ====================

async_cache = AsyncCacheManager()


# ==================== HELPER FUNCTIONS ====================

def build_cache_key(*parts: str) -> str:
    """
    Build a cache key from multiple parts.

    Example:
        build_cache_key("user", str(user_id), "profile")
        # Returns: "user:123:profile"
    """
    return ":".join(str(part) for part in parts)


def build_list_cache_key(resource: str, filters: dict) -> str:
    """
    Build cache key for list endpoints with filters.

    Example:
        build_list_cache_key("applications", {"status": "sent", "candidate_id": 1})
        # Returns: "applications:list:status=sent:candidate_id=1"
    """
    filter_str = ":".join(f"{k}={v}" for k, v in sorted(filters.items()))
    return f"{resource}:list:{filter_str}" if filter_str else f"{resource}:list:all"


# ==================== USAGE EXAMPLES ====================

"""
Example 1: In async endpoint
    from app.core.cache_async import async_cache

    @router.get("/user/{user_id}")
    async def get_user(user_id: int):
        # Try cache first
        cached = await async_cache.get(f"user:{user_id}")
        if cached:
            return cached

        # Query database
        user = await db.get(User, user_id)

        # Cache result
        await async_cache.set(f"user:{user_id}", user, ttl=3600)

        return user


Example 2: With decorator
    @async_cache.cached(ttl=600, key_prefix="company")
    async def get_company(company_id: int):
        return await db.get(Company, company_id)


Example 3: Pattern deletion
    # Invalidate all user caches
    await async_cache.delete_pattern("user:*")


Example 4: In startup/shutdown
    from fastapi import FastAPI

    app = FastAPI()

    @app.on_event("startup")
    async def startup():
        if await async_cache.is_connected():
            print("✅ Async cache connected")

    @app.on_event("shutdown")
    async def shutdown():
        await async_cache.close()
"""
