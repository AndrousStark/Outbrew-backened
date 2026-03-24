"""
Multi-Level Cache for Phase 3 Optimization

Implements L1 (in-memory) + L2 (Redis) caching strategy:
- L1: Fast LRU in-memory cache (microseconds)
- L2: Redis distributed cache (milliseconds)

PERFORMANCE IMPACT:
- L1 hit: ~0.1ms (100x faster than Redis)
- L2 hit: ~1ms (10x faster than database)
- Database: ~10-100ms

BEST FOR:
- Frequently accessed, rarely changing data
- Company lookups
- User profiles
- Configuration data
"""
import logging
from typing import Any, Optional, Dict
from functools import lru_cache
from datetime import datetime, timedelta, timezone
import threading

from app.core.cache_async import async_cache

logger = logging.getLogger(__name__)


class MultiLevelCache:
    """
    Multi-level cache with L1 (memory) and L2 (Redis).

    L1 (In-Memory):
    - Ultra-fast access (~0.1ms)
    - Limited size (LRU eviction)
    - Process-local (not shared)
    - Perfect for hot data

    L2 (Redis):
    - Fast access (~1ms)
    - Shared across processes
    - Larger capacity
    - Persistent across restarts

    Usage:
        cache = MultiLevelCache(l1_size=1000, l1_ttl=300)

        # Set value
        await cache.set("user:123", user_data, ttl=3600)

        # Get value (checks L1 first, then L2)
        user = await cache.get("user:123")
    """

    def __init__(
        self,
        l1_size: int = 1000,
        l1_ttl: int = 300,  # L1 TTL in seconds
        l2_default_ttl: int = 3600  # L2 default TTL
    ):
        """
        Initialize multi-level cache.

        Args:
            l1_size: Max items in L1 cache
            l1_ttl: L1 TTL in seconds (shorter than L2)
            l2_default_ttl: Default L2 TTL in seconds
        """
        self.l1_size = l1_size
        self.l1_ttl = l1_ttl
        self.l2_default_ttl = l2_default_ttl

        # L1: In-memory cache with expiration
        self._l1_cache: Dict[str, tuple[Any, datetime]] = {}
        self._l1_lock = threading.Lock()

        # Stats
        self._stats = {
            "l1_hits": 0,
            "l1_misses": 0,
            "l2_hits": 0,
            "l2_misses": 0,
            "sets": 0
        }

        logger.info(
            f"[MULTI-CACHE] Initialized: L1 size={l1_size}, "
            f"L1 TTL={l1_ttl}s, L2 TTL={l2_default_ttl}s"
        )

    def _is_l1_expired(self, expiry: datetime) -> bool:
        """Check if L1 entry is expired"""
        return datetime.now(timezone.utc) > expiry

    def _evict_l1_if_needed(self):
        """Evict oldest entries if L1 is full (LRU)"""
        if len(self._l1_cache) >= self.l1_size:
            # Remove 10% of oldest entries
            num_to_remove = max(1, self.l1_size // 10)

            # Sort by expiry time (oldest first)
            sorted_keys = sorted(
                self._l1_cache.keys(),
                key=lambda k: self._l1_cache[k][1]
            )

            for key in sorted_keys[:num_to_remove]:
                del self._l1_cache[key]

            logger.debug(f"[MULTI-CACHE] Evicted {num_to_remove} L1 entries")

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache (L1 first, then L2).

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        # Check L1 (in-memory)
        with self._l1_lock:
            if key in self._l1_cache:
                value, expiry = self._l1_cache[key]

                if not self._is_l1_expired(expiry):
                    self._stats["l1_hits"] += 1
                    logger.debug(f"[MULTI-CACHE] L1 HIT: {key}")
                    return value
                else:
                    # Expired, remove from L1
                    del self._l1_cache[key]
                    logger.debug(f"[MULTI-CACHE] L1 expired: {key}")

            self._stats["l1_misses"] += 1

        # Check L2 (Redis)
        value = await async_cache.get(key)

        if value is not None:
            self._stats["l2_hits"] += 1
            logger.debug(f"[MULTI-CACHE] L2 HIT: {key}")

            # Store in L1 for next time
            with self._l1_lock:
                expiry = datetime.now(timezone.utc) + timedelta(seconds=self.l1_ttl)
                self._l1_cache[key] = (value, expiry)
                self._evict_l1_if_needed()

            return value

        self._stats["l2_misses"] += 1
        logger.debug(f"[MULTI-CACHE] MISS: {key}")
        return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set value in both L1 and L2 caches.

        Args:
            key: Cache key
            value: Value to cache
            ttl: TTL in seconds (for L2, L1 uses l1_ttl)

        Returns:
            True if successful
        """
        if ttl is None:
            ttl = self.l2_default_ttl

        self._stats["sets"] += 1

        # Set in L2 (Redis)
        l2_success = await async_cache.set(key, value, ttl=ttl)

        # Set in L1 (memory)
        with self._l1_lock:
            expiry = datetime.now(timezone.utc) + timedelta(seconds=self.l1_ttl)
            self._l1_cache[key] = (value, expiry)
            self._evict_l1_if_needed()

        logger.debug(f"[MULTI-CACHE] SET: {key} (L1 TTL={self.l1_ttl}s, L2 TTL={ttl}s)")

        return l2_success

    async def delete(self, key: str) -> bool:
        """
        Delete key from both caches.

        Args:
            key: Cache key

        Returns:
            True if deleted
        """
        # Delete from L1
        with self._l1_lock:
            if key in self._l1_cache:
                del self._l1_cache[key]

        # Delete from L2
        result = await async_cache.delete(key)

        logger.debug(f"[MULTI-CACHE] DELETE: {key}")
        return result

    async def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching pattern from both caches.

        Args:
            pattern: Pattern to match (e.g., "user:*")

        Returns:
            Number of keys deleted
        """
        # Delete from L1 (simple prefix matching)
        with self._l1_lock:
            prefix = pattern.replace("*", "")
            keys_to_delete = [k for k in self._l1_cache.keys() if k.startswith(prefix)]

            for key in keys_to_delete:
                del self._l1_cache[key]

            logger.debug(f"[MULTI-CACHE] Deleted {len(keys_to_delete)} L1 keys matching {pattern}")

        # Delete from L2
        count = await async_cache.delete_pattern(pattern)

        logger.debug(f"[MULTI-CACHE] DELETE PATTERN: {pattern} ({count} L2 keys)")
        return count

    def clear_l1(self):
        """Clear entire L1 cache"""
        with self._l1_lock:
            size = len(self._l1_cache)
            self._l1_cache.clear()

        logger.info(f"[MULTI-CACHE] Cleared {size} L1 entries")

    async def clear_all(self):
        """Clear both L1 and L2 caches"""
        self.clear_l1()
        await async_cache.flush_all()
        logger.warning("[MULTI-CACHE] Cleared ALL caches (L1 + L2)")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dict with hit rates and counts
        """
        total_gets = self._stats["l1_hits"] + self._stats["l1_misses"]

        l1_hit_rate = (
            (self._stats["l1_hits"] / total_gets * 100)
            if total_gets > 0
            else 0.0
        )

        l2_hit_rate = (
            (self._stats["l2_hits"] / self._stats["l1_misses"] * 100)
            if self._stats["l1_misses"] > 0
            else 0.0
        )

        combined_hit_rate = (
            ((self._stats["l1_hits"] + self._stats["l2_hits"]) / total_gets * 100)
            if total_gets > 0
            else 0.0
        )

        with self._l1_lock:
            l1_size = len(self._l1_cache)

        return {
            "l1_hits": self._stats["l1_hits"],
            "l1_misses": self._stats["l1_misses"],
            "l1_hit_rate": round(l1_hit_rate, 2),
            "l1_size": l1_size,
            "l1_max_size": self.l1_size,
            "l2_hits": self._stats["l2_hits"],
            "l2_misses": self._stats["l2_misses"],
            "l2_hit_rate": round(l2_hit_rate, 2),
            "combined_hit_rate": round(combined_hit_rate, 2),
            "total_sets": self._stats["sets"],
            "total_gets": total_gets
        }


# ============================================================================
# GLOBAL INSTANCE
# ============================================================================

# Create global multi-level cache instance
multi_cache = MultiLevelCache(
    l1_size=1000,  # Keep 1000 items in memory
    l1_ttl=300,  # L1 expires after 5 minutes
    l2_default_ttl=3600  # L2 expires after 1 hour
)


# ============================================================================
# DECORATOR FOR MULTI-LEVEL CACHING
# ============================================================================

def multi_cached(
    ttl: int = 3600,
    key_prefix: str = "",
    use_multi_cache: bool = True
):
    """
    Decorator for multi-level caching.

    Usage:
        @multi_cached(ttl=600, key_prefix="company")
        async def get_company(company_id: int):
            return await db.query(Company).get(company_id)

    Args:
        ttl: L2 TTL in seconds
        key_prefix: Cache key prefix
        use_multi_cache: If False, use only L2
    """
    from functools import wraps
    import hashlib

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Build cache key
            args_str = f"{args}:{kwargs}"
            args_hash = hashlib.md5(args_str.encode()).hexdigest()[:16]
            cache_key = f"{key_prefix}:{func.__name__}:{args_hash}"

            # Try cache
            if use_multi_cache:
                cached_result = await multi_cache.get(cache_key)
            else:
                cached_result = await async_cache.get(cache_key)

            if cached_result is not None:
                logger.debug(f"[MULTI-CACHE] Cached result for {func.__name__}")
                return cached_result

            # Execute function
            logger.debug(f"[MULTI-CACHE] Computing result for {func.__name__}")
            result = await func(*args, **kwargs)

            # Cache result
            if use_multi_cache:
                await multi_cache.set(cache_key, result, ttl=ttl)
            else:
                await async_cache.set(cache_key, result, ttl=ttl)

            return result

        return wrapper
    return decorator


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

"""
Example 1: Manual caching

    from app.core.multi_cache import multi_cache

    @router.get("/companies/{id}")
    async def get_company(id: int):
        # Check cache
        cache_key = f"company:{id}"
        cached = await multi_cache.get(cache_key)
        if cached:
            return cached  # L1 hit: ~0.1ms, L2 hit: ~1ms

        # Query database
        company = await db.get(Company, id)

        # Cache result
        await multi_cache.set(cache_key, company, ttl=3600)

        return company


Example 2: With decorator

    from app.core.multi_cache import multi_cached

    @multi_cached(ttl=3600, key_prefix="company")
    async def get_company_by_id(company_id: int):
        return await db.query(Company).get(company_id)

    # First call: Database query + cache
    company = await get_company_by_id(1)  # ~50ms

    # Second call: L1 hit
    company = await get_company_by_id(1)  # ~0.1ms (500x faster!)


Example 3: Get statistics

    @router.get("/admin/cache/stats")
    async def get_cache_stats():
        stats = multi_cache.get_stats()
        return stats
        # {
        #     "l1_hits": 1000,
        #     "l1_hit_rate": 85.5,
        #     "l2_hit_rate": 95.2,
        #     "combined_hit_rate": 98.3
        # }


Example 4: Invalidation

    # Invalidate single key
    await multi_cache.delete("company:123")

    # Invalidate pattern
    await multi_cache.delete_pattern("company:*")

    # Clear L1 only (keep Redis)
    multi_cache.clear_l1()

    # Clear everything
    await multi_cache.clear_all()
"""
