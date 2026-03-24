"""
Redis Cache Manager for High-Performance Caching

This module provides a comprehensive caching layer using Redis for:
- Function result caching with decorators
- Manual cache operations
- Cache invalidation patterns
- Performance monitoring
"""
import json
import hashlib
import logging
from typing import Any, Optional, Callable, Union
from functools import wraps
from datetime import datetime, timedelta

import redis
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError

from app.core.config import settings

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Centralized cache management with Redis.

    Features:
    - Automatic serialization/deserialization
    - TTL (Time To Live) support
    - Namespace/prefix support
    - Pattern-based deletion
    - Connection pooling
    - Fallback to no-cache on Redis failure
    """

    def __init__(self, redis_url: str = None, prefix: str = "hrweb"):
        """
        Initialize cache manager.

        Args:
            redis_url: Redis connection URL (default from settings)
            prefix: Global prefix for all cache keys
        """
        self.prefix = prefix
        self.redis_url = redis_url or getattr(settings, 'REDIS_URL', 'redis://localhost:6379/0')
        self._redis_client = None
        self._connection_error_logged = False

        # Initialize connection only if Redis is enabled
        if getattr(settings, 'REDIS_ENABLED', True):
            self._connect()
        else:
            logger.info("[CACHE] Redis is disabled via REDIS_ENABLED=False. Caching disabled.")

    def _connect(self):
        """Establish Redis connection with connection pooling"""
        try:
            self._redis_client = redis.Redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
                max_connections=50  # Connection pool size
            )
            # Test connection
            self._redis_client.ping()
            logger.info(f" [CACHE] Connected to Redis at {self.redis_url}")
            self._connection_error_logged = False
        except (RedisError, RedisConnectionError) as e:
            if not self._connection_error_logged:
                logger.warning(f"  [CACHE] Redis connection failed: {e}. Caching disabled.")
                self._connection_error_logged = True
            self._redis_client = None

    def _make_key(self, key: str) -> str:
        """Generate prefixed cache key"""
        return f"{self.prefix}:{key}"

    def _serialize(self, value: Any) -> str:
        """Serialize Python object to JSON string"""
        try:
            return json.dumps(value, default=str)  # default=str handles datetime, etc.
        except (TypeError, ValueError) as e:
            logger.error(f" [CACHE] Serialization error: {e}")
            return json.dumps(str(value))

    def _deserialize(self, value: str) -> Any:
        """Deserialize JSON string to Python object"""
        try:
            return json.loads(value)
        except (TypeError, ValueError) as e:
            logger.error(f" [CACHE] Deserialization error: {e}")
            return value

    def is_connected(self) -> bool:
        """Check if Redis is connected and healthy"""
        if not self._redis_client:
            return False
        try:
            self._redis_client.ping()
            return True
        except (RedisError, RedisConnectionError):
            return False

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/error
        """
        if not self.is_connected():
            return None

        try:
            full_key = self._make_key(key)
            value = self._redis_client.get(full_key)

            if value is None:
                logger.debug(f"🔍 [CACHE] Miss: {key}")
                return None

            logger.debug(f" [CACHE] Hit: {key}")
            return self._deserialize(value)

        except (RedisError, RedisConnectionError) as e:
            logger.error(f" [CACHE] Get error for key '{key}': {e}")
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: int = 3600,
        nx: bool = False,
        xx: bool = False
    ) -> bool:
        """
        Set value in cache with TTL.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time to live in seconds (default 1 hour)
            nx: Only set if key doesn't exist
            xx: Only set if key exists

        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            return False

        try:
            full_key = self._make_key(key)
            serialized = self._serialize(value)

            result = self._redis_client.set(
                full_key,
                serialized,
                ex=ttl,
                nx=nx,
                xx=xx
            )

            if result:
                logger.debug(f" [CACHE] Set: {key} (TTL: {ttl}s)")
            return bool(result)

        except (RedisError, RedisConnectionError) as e:
            logger.error(f" [CACHE] Set error for key '{key}': {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Delete key from cache.

        Args:
            key: Cache key to delete

        Returns:
            True if deleted, False otherwise
        """
        if not self.is_connected():
            return False

        try:
            full_key = self._make_key(key)
            result = self._redis_client.delete(full_key)
            logger.debug(f"🗑️  [CACHE] Delete: {key}")
            return bool(result)

        except (RedisError, RedisConnectionError) as e:
            logger.error(f" [CACHE] Delete error for key '{key}': {e}")
            return False

    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching pattern.

        Args:
            pattern: Pattern to match (e.g., "user:*", "stats:*")

        Returns:
            Number of keys deleted
        """
        if not self.is_connected():
            return 0

        try:
            full_pattern = self._make_key(pattern)
            keys = self._redis_client.keys(full_pattern)

            if not keys:
                return 0

            deleted = self._redis_client.delete(*keys)
            logger.info(f"🗑️  [CACHE] Deleted {deleted} keys matching '{pattern}'")
            return deleted

        except (RedisError, RedisConnectionError) as e:
            logger.error(f" [CACHE] Delete pattern error for '{pattern}': {e}")
            return 0

    def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        if not self.is_connected():
            return False

        try:
            full_key = self._make_key(key)
            return bool(self._redis_client.exists(full_key))
        except (RedisError, RedisConnectionError):
            return False

    def ttl(self, key: str) -> int:
        """
        Get remaining TTL for key.

        Returns:
            Seconds remaining, -1 if no TTL, -2 if key doesn't exist
        """
        if not self.is_connected():
            return -2

        try:
            full_key = self._make_key(key)
            return self._redis_client.ttl(full_key)
        except (RedisError, RedisConnectionError):
            return -2

    def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Increment integer value.

        Args:
            key: Cache key
            amount: Amount to increment by

        Returns:
            New value after increment
        """
        if not self.is_connected():
            return None

        try:
            full_key = self._make_key(key)
            return self._redis_client.incrby(full_key, amount)
        except (RedisError, RedisConnectionError) as e:
            logger.error(f" [CACHE] Increment error for key '{key}': {e}")
            return None

    def get_stats(self) -> dict:
        """Get cache statistics"""
        if not self.is_connected():
            return {"connected": False, "error": "Redis not connected"}

        try:
            info = self._redis_client.info()
            return {
                "connected": True,
                "total_keys": self._redis_client.dbsize(),
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
        """Calculate cache hit rate percentage"""
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        total = hits + misses

        if total == 0:
            return 0.0

        return round((hits / total) * 100, 2)

    def flush_all(self) -> bool:
        """
        DANGER: Delete all keys in current database.
        Use with caution!
        """
        if not self.is_connected():
            return False

        try:
            self._redis_client.flushdb()
            logger.warning("  [CACHE] Flushed all cache keys!")
            return True
        except (RedisError, RedisConnectionError) as e:
            logger.error(f" [CACHE] Flush error: {e}")
            return False

    # ==================== DECORATORS ====================

    def cached(
        self,
        ttl: int = 3600,
        key_prefix: str = "",
        key_builder: Optional[Callable] = None
    ):
        """
        Decorator to cache function results.

        Args:
            ttl: Cache time to live in seconds
            key_prefix: Prefix for cache key
            key_builder: Custom function to build cache key from args

        Usage:
            @cache.cached(ttl=600, key_prefix="user")
            def get_user(user_id: int):
                return db.query(User).get(user_id)
        """
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Build cache key
                if key_builder:
                    cache_key = key_builder(*args, **kwargs)
                else:
                    # Default: hash function name and arguments
                    args_str = f"{args}:{kwargs}"
                    args_hash = hashlib.md5(args_str.encode()).hexdigest()[:16]
                    cache_key = f"{key_prefix}:{func.__name__}:{args_hash}"

                # Try to get from cache
                cached_result = self.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"📦 [CACHE] Using cached result for {func.__name__}")
                    return cached_result

                # Execute function
                logger.debug(f" [CACHE] Computing result for {func.__name__}")
                result = func(*args, **kwargs)

                # Cache result
                self.set(cache_key, result, ttl=ttl)

                return result

            return wrapper
        return decorator

    def invalidate_on_change(self, pattern: str):
        """
        Decorator to invalidate cache pattern when function executes.

        Usage:
            @cache.invalidate_on_change("user:*")
            def update_user(user_id: int, data: dict):
                # Update user...
                pass
        """
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                self.delete_pattern(pattern)
                logger.info(f"♻️  [CACHE] Invalidated cache pattern: {pattern}")
                return result

            return wrapper
        return decorator


# ==================== GLOBAL INSTANCE ====================

# Create global cache instance
cache = CacheManager()


# ==================== CACHE KEY BUILDERS ====================

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
Example 1: Simple caching
    from app.core.cache import cache

    # Set
    cache.set("user:123", {"name": "John", "email": "john@example.com"}, ttl=3600)

    # Get
    user = cache.get("user:123")

    # Delete
    cache.delete("user:123")

Example 2: Pattern deletion
    # Delete all user caches
    cache.delete_pattern("user:*")

Example 3: Decorator
    from app.core.cache import cache

    @cache.cached(ttl=600, key_prefix="company")
    def get_company_research(company_id: int):
        # Expensive operation
        return research_company(company_id)

Example 4: Cache invalidation
    @cache.invalidate_on_change("applications:*")
    def create_application(data: dict):
        # Create application
        return new_application

Example 5: Manual cache management
    # Check if cached
    if cache.exists("expensive_query"):
        result = cache.get("expensive_query")
    else:
        result = expensive_computation()
        cache.set("expensive_query", result, ttl=1800)

Example 6: Statistics
    stats = cache.get_stats()
    print(f"Hit rate: {stats['hit_rate']}%")
"""
