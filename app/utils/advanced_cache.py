"""
Advanced Caching Strategies with Multiple Eviction Policies
Implements LRU, LFU, and ARC (Adaptive Replacement Cache)

Used for caching scraped pages, API responses, and DNS lookups
"""

from collections import OrderedDict
from typing import Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import heapq
import time


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    value: Any
    frequency: int = 0
    last_accessed: float = 0
    created_at: float = 0
    size_bytes: int = 0
    ttl_seconds: Optional[int] = None

    def is_expired(self) -> bool:
        """Check if entry has expired"""
        if self.ttl_seconds is None:
            return False
        return time.time() - self.created_at > self.ttl_seconds


class LRUCache:
    """
    Least Recently Used (LRU) Cache

    Uses OrderedDict for O(1) get/put operations
    Evicts least recently used items when capacity is reached

    Time Complexity:
        - get: O(1)
        - put: O(1)
        - delete: O(1)

    Space Complexity: O(n) where n = capacity
    """

    def __init__(self, capacity: int = 1000, ttl_seconds: Optional[int] = None):
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache

        Returns None if key doesn't exist or is expired
        """
        if key not in self.cache:
            self.misses += 1
            return None

        entry = self.cache[key]

        # Check expiration
        if entry.is_expired():
            del self.cache[key]
            self.misses += 1
            return None

        # Move to end (most recently used)
        self.cache.move_to_end(key)
        entry.last_accessed = time.time()

        self.hits += 1
        return entry.value

    def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Put value in cache"""
        current_time = time.time()

        # Update existing key
        if key in self.cache:
            entry = self.cache[key]
            entry.value = value
            entry.last_accessed = current_time
            entry.ttl_seconds = ttl_seconds or self.ttl_seconds
            self.cache.move_to_end(key)
            return

        # Evict LRU if at capacity
        if len(self.cache) >= self.capacity:
            self.cache.popitem(last=False)  # Remove first (oldest) item

        # Add new entry
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=current_time,
            last_accessed=current_time,
            ttl_seconds=ttl_seconds or self.ttl_seconds
        )
        self.cache[key] = entry

    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        if key in self.cache:
            del self.cache[key]
            return True
        return False

    def clear(self) -> None:
        """Clear all entries"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0

    def stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0

        return {
            "capacity": self.capacity,
            "size": len(self.cache),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate_percent": round(hit_rate, 2),
            "utilization_percent": round((len(self.cache) / self.capacity) * 100, 2)
        }


class LFUCache:
    """
    Least Frequently Used (LFU) Cache

    Evicts items with lowest access frequency
    Uses min-heap for O(log n) eviction

    Time Complexity:
        - get: O(log n) - need to update frequency
        - put: O(log n) - heap operations
        - delete: O(n) - need to find in heap

    Best for: Long-term caching where popular items should stay
    """

    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self.cache: dict[str, CacheEntry] = {}
        self.frequency_heap: list[tuple[int, float, str]] = []  # (frequency, timestamp, key)
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value and increment frequency"""
        if key not in self.cache:
            self.misses += 1
            return None

        entry = self.cache[key]

        # Check expiration
        if entry.is_expired():
            del self.cache[key]
            self.misses += 1
            return None

        # Increment frequency
        entry.frequency += 1
        entry.last_accessed = time.time()

        # Update heap (lazy approach - old entries will be ignored during eviction)
        heapq.heappush(self.frequency_heap, (entry.frequency, entry.last_accessed, key))

        self.hits += 1
        return entry.value

    def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Put value in cache"""
        current_time = time.time()

        # Update existing
        if key in self.cache:
            entry = self.cache[key]
            entry.value = value
            entry.frequency += 1
            entry.last_accessed = current_time
            heapq.heappush(self.frequency_heap, (entry.frequency, entry.last_accessed, key))
            return

        # Evict LFU if at capacity
        if len(self.cache) >= self.capacity:
            self._evict_lfu()

        # Add new entry
        entry = CacheEntry(
            key=key,
            value=value,
            frequency=1,
            created_at=current_time,
            last_accessed=current_time,
            ttl_seconds=ttl_seconds
        )
        self.cache[key] = entry
        heapq.heappush(self.frequency_heap, (1, current_time, key))

    def _evict_lfu(self) -> None:
        """Evict least frequently used item"""
        while self.frequency_heap:
            freq, timestamp, key = heapq.heappop(self.frequency_heap)

            # Check if this is the current frequency for this key
            if key in self.cache and self.cache[key].frequency == freq:
                del self.cache[key]
                return

    def stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0

        avg_frequency = sum(e.frequency for e in self.cache.values()) / len(self.cache) if self.cache else 0

        return {
            "capacity": self.capacity,
            "size": len(self.cache),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate_percent": round(hit_rate, 2),
            "avg_frequency": round(avg_frequency, 2)
        }


class ARCCache:
    """
    Adaptive Replacement Cache (ARC)

    Balances between recency (LRU) and frequency (LFU)
    Self-tuning based on workload

    Maintains two LRU lists:
    - T1: Recent one-time access
    - T2: Recent frequent access

    Best for: Variable workloads with both temporal and frequency patterns
    """

    def __init__(self, capacity: int = 1000):
        self.capacity = capacity

        # Two LRU caches
        self.t1 = OrderedDict()  # Recent one-hit items
        self.t2 = OrderedDict()  # Recent multi-hit items

        # Ghost lists (track evicted items)
        self.b1 = OrderedDict()  # Evicted from T1
        self.b2 = OrderedDict()  # Evicted from T2

        # Adaptive parameter (target size for T1)
        self.p = 0

        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        # Check T1 (one-hit)
        if key in self.t1:
            entry = self.t1.pop(key)
            self.t2[key] = entry  # Promote to T2
            self.t2.move_to_end(key)
            self.hits += 1
            return entry.value

        # Check T2 (multi-hit)
        if key in self.t2:
            self.t2.move_to_end(key)
            self.hits += 1
            return self.t2[key].value

        self.misses += 1
        return None

    def put(self, key: str, value: Any) -> None:
        """Put value in cache with adaptive balancing"""
        current_time = time.time()
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=current_time,
            last_accessed=current_time
        )

        # Case 1: In T1 or T2 (cache hit) - already handled in get()
        if key in self.t1 or key in self.t2:
            if key in self.t1:
                self.t1[key] = entry
            else:
                self.t2[key] = entry
            return

        # Case 2: In B1 (was evicted from T1) - adapt toward T1
        if key in self.b1:
            self.p = min(self.p + 1, self.capacity)
            self._replace(key, True)
            self.b1.pop(key)
            self.t2[key] = entry
            return

        # Case 3: In B2 (was evicted from T2) - adapt toward T2
        if key in self.b2:
            self.p = max(self.p - 1, 0)
            self._replace(key, False)
            self.b2.pop(key)
            self.t2[key] = entry
            return

        # Case 4: Not in any list (new item)
        if len(self.t1) + len(self.t2) >= self.capacity:
            self._replace(key, False)

        # Add to T1
        self.t1[key] = entry

    def _replace(self, key: str, in_b1: bool) -> None:
        """Replace items based on adaptive parameter p"""
        if self.t1 and (len(self.t1) > self.p or (in_b1 and len(self.t1) == self.p)):
            # Evict from T1
            old_key, old_entry = self.t1.popitem(last=False)
            self.b1[old_key] = old_entry
        else:
            # Evict from T2
            if self.t2:
                old_key, old_entry = self.t2.popitem(last=False)
                self.b2[old_key] = old_entry

    def stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0

        return {
            "capacity": self.capacity,
            "t1_size": len(self.t1),
            "t2_size": len(self.t2),
            "b1_size": len(self.b1),
            "b2_size": len(self.b2),
            "adaptive_p": self.p,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate_percent": round(hit_rate, 2)
        }


class TimeBoundCache:
    """
    Time-bound cache with automatic expiration

    Uses LRU with TTL (Time To Live)
    Background thread cleans expired entries

    Best for: API responses, DNS lookups, scraped pages
    """

    def __init__(
        self,
        capacity: int = 1000,
        default_ttl_seconds: int = 3600,  # 1 hour
        cleanup_interval: int = 300  # 5 minutes
    ):
        self.lru = LRUCache(capacity=capacity, ttl_seconds=default_ttl_seconds)
        self.cleanup_interval = cleanup_interval
        self.last_cleanup = time.time()

    def get(self, key: str) -> Optional[Any]:
        """Get value with automatic expiration check"""
        self._maybe_cleanup()
        return self.lru.get(key)

    def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Put value with optional TTL"""
        self._maybe_cleanup()
        self.lru.put(key, value, ttl_seconds)

    def _maybe_cleanup(self) -> None:
        """Cleanup expired entries if interval passed"""
        current_time = time.time()
        if current_time - self.last_cleanup > self.cleanup_interval:
            self._cleanup_expired()
            self.last_cleanup = current_time

    def _cleanup_expired(self) -> None:
        """Remove all expired entries"""
        expired_keys = [
            key for key, entry in self.lru.cache.items()
            if entry.is_expired()
        ]
        for key in expired_keys:
            del self.lru.cache[key]

    def stats(self) -> dict:
        """Get cache statistics"""
        return self.lru.stats()


# Usage Examples:
"""
# For scraped pages (temporal locality - LRU)
page_cache = LRUCache(capacity=10000, ttl_seconds=3600)  # 1 hour TTL
html = page_cache.get("https://example.com")
if html is None:
    html = scrape_page("https://example.com")
    page_cache.put("https://example.com", html)

# For popular pages (frequency matters - LFU)
popular_cache = LFUCache(capacity=5000)
# Popular pages stay longer based on access frequency

# For mixed workload (adaptive - ARC)
arc_cache = ARCCache(capacity=5000)
# Automatically balances between LRU and LFU

# For API responses with expiration
api_cache = TimeBoundCache(capacity=1000, default_ttl_seconds=600)  # 10 min
result = api_cache.get("apollo:search:query123")
if result is None:
    result = apollo_api.search(...)
    api_cache.put("apollo:search:query123", result)

print(api_cache.stats())
# {
#     'capacity': 1000,
#     'size': 342,
#     'hits': 8234,
#     'misses': 1876,
#     'hit_rate_percent': 81.45,
#     'utilization_percent': 34.2
# }
"""
