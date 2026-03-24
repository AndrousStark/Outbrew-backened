"""
Advanced Rate Limiting Algorithms

Implements multiple rate limiting strategies:
1. Token Bucket - Smooth rate limiting with bursts
2. Leaky Bucket - Fixed rate, no bursts
3. Sliding Window Counter - Per-domain rate limiting
4. Adaptive Rate Limiter - Auto-adjusts based on server responses

Used for respecting robots.txt, avoiding bans, and efficient scraping
"""

import time
import asyncio
from typing import Dict, Optional
from dataclasses import dataclass
from collections import deque
from datetime import datetime


@dataclass
class TokenBucket:
    """
    Token Bucket Algorithm

    Allows bursts up to bucket capacity
    Refills at constant rate

    Best for: General API rate limiting with burst allowance

    Time Complexity: O(1) for all operations
    """

    capacity: int  # Max tokens
    refill_rate: float  # Tokens per second
    tokens: float = 0  # Current tokens
    last_refill: float = 0  # Last refill timestamp

    def __post_init__(self):
        self.tokens = self.capacity
        self.last_refill = time.time()

    def consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens

        Returns True if successful, False if not enough tokens
        """
        self._refill()

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    async def wait_and_consume(self, tokens: int = 1) -> None:
        """
        Wait until tokens available, then consume

        Blocks until enough tokens accumulated
        """
        while not self.consume(tokens):
            # Calculate wait time
            needed = tokens - self.tokens
            wait_time = needed / self.refill_rate
            await asyncio.sleep(wait_time)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_refill

        # Add tokens based on refill rate
        tokens_to_add = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now

    @property
    def available_tokens(self) -> int:
        """Get current available tokens"""
        self._refill()
        return int(self.tokens)

    @property
    def time_until_full(self) -> float:
        """Time in seconds until bucket is full"""
        self._refill()
        if self.tokens >= self.capacity:
            return 0.0
        return (self.capacity - self.tokens) / self.refill_rate


class LeakyBucket:
    """
    Leaky Bucket Algorithm

    Fixed output rate, no bursts
    Drops requests if bucket overflows

    Best for: Strict rate limiting (e.g., robots.txt compliance)

    Time Complexity: O(1)
    """

    def __init__(self, capacity: int, leak_rate: float):
        self.capacity = capacity
        self.leak_rate = leak_rate  # Items per second
        self.queue: deque = deque()
        self.last_leak = time.time()

    def add_request(self, request_id: str) -> bool:
        """
        Add request to bucket

        Returns True if added, False if bucket full
        """
        self._leak()

        if len(self.queue) < self.capacity:
            self.queue.append((request_id, time.time()))
            return True
        return False  # Bucket overflow - drop request

    def _leak(self) -> None:
        """Leak items from bucket at constant rate"""
        now = time.time()
        elapsed = now - self.last_leak

        # Number of items to leak
        items_to_leak = int(elapsed * self.leak_rate)

        for _ in range(min(items_to_leak, len(self.queue))):
            self.queue.popleft()

        self.last_leak = now

    @property
    def is_full(self) -> bool:
        """Check if bucket is full"""
        self._leak()
        return len(self.queue) >= self.capacity

    @property
    def size(self) -> int:
        """Current bucket size"""
        self._leak()
        return len(self.queue)


class SlidingWindowCounter:
    """
    Sliding Window Counter Algorithm

    Tracks requests in time windows with precision
    Prevents burst attacks at window boundaries

    Best for: Per-domain rate limiting in web scraping

    Time Complexity: O(log n) for cleanup, O(1) for check
    """

    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: deque = deque()  # (timestamp, request_id)

    def can_make_request(self) -> bool:
        """Check if request allowed within window"""
        self._cleanup_old_requests()
        return len(self.requests) < self.max_requests

    def add_request(self, request_id: str = "") -> bool:
        """
        Try to add request

        Returns True if allowed, False if rate limited
        """
        if not self.can_make_request():
            return False

        self.requests.append((time.time(), request_id))
        return True

    async def wait_for_slot(self) -> None:
        """Wait until a request slot is available"""
        while not self.can_make_request():
            # Wait until oldest request falls out of window
            if self.requests:
                oldest_time = self.requests[0][0]
                wait_time = (oldest_time + self.window_seconds) - time.time()
                if wait_time > 0:
                    await asyncio.sleep(wait_time + 0.01)  # Small buffer
            else:
                break

    def _cleanup_old_requests(self) -> None:
        """Remove requests outside the time window"""
        cutoff = time.time() - self.window_seconds

        while self.requests and self.requests[0][0] < cutoff:
            self.requests.popleft()

    @property
    def current_rate(self) -> float:
        """Current requests per second"""
        self._cleanup_old_requests()
        if not self.requests:
            return 0.0
        return len(self.requests) / self.window_seconds

    @property
    def remaining_requests(self) -> int:
        """Remaining requests in current window"""
        self._cleanup_old_requests()
        return max(0, self.max_requests - len(self.requests))

    def stats(self) -> dict:
        """Get rate limiter statistics"""
        self._cleanup_old_requests()
        return {
            "max_requests": self.max_requests,
            "window_seconds": self.window_seconds,
            "current_requests": len(self.requests),
            "remaining_requests": self.remaining_requests,
            "current_rate_per_sec": round(self.current_rate, 2),
            "utilization_percent": round((len(self.requests) / self.max_requests) * 100, 2)
        }


class AdaptiveRateLimiter:
    """
    Adaptive Rate Limiter

    Automatically adjusts rate based on server responses:
    - Decreases rate on 429 (Too Many Requests)
    - Increases rate if server handles requests well
    - Maintains optimal throughput without bans

    Best for: Production web scraping with unknown limits

    Uses AIMD (Additive Increase, Multiplicative Decrease)
    """

    def __init__(
        self,
        initial_rate: float = 10.0,  # Requests per second
        min_rate: float = 1.0,
        max_rate: float = 50.0,
        increase_step: float = 0.5,  # Additive increase
        decrease_factor: float = 0.5  # Multiplicative decrease
    ):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_step = increase_step
        self.decrease_factor = decrease_factor

        # Token bucket with current rate
        self.bucket = TokenBucket(
            capacity=int(initial_rate * 2),  # Allow 2 seconds of burst
            refill_rate=initial_rate
        )

        # Statistics
        self.success_count = 0
        self.rate_limit_count = 0
        self.last_adjustment = time.time()
        self.adjustment_interval = 10.0  # Adjust every 10 seconds

    async def acquire(self) -> None:
        """Wait for permission to make request"""
        await self.bucket.wait_and_consume(1)

    def report_success(self) -> None:
        """Report successful request"""
        self.success_count += 1
        self._maybe_increase_rate()

    def report_rate_limited(self) -> None:
        """Report 429 rate limit response"""
        self.rate_limit_count += 1
        self._decrease_rate()

    def report_error(self, status_code: int) -> None:
        """Report error response"""
        if status_code == 429:
            self.report_rate_limited()
        elif status_code >= 500:
            # Server error - be more conservative
            self._decrease_rate(factor=0.75)

    def _maybe_increase_rate(self) -> None:
        """Gradually increase rate if stable (AIMD)"""
        now = time.time()
        elapsed = now - self.last_adjustment

        if elapsed < self.adjustment_interval:
            return

        # If no rate limits recently, increase rate
        if self.rate_limit_count == 0 and self.success_count > 20:
            new_rate = min(self.max_rate, self.current_rate + self.increase_step)
            self._update_rate(new_rate)

        # Reset counters
        self.success_count = 0
        self.rate_limit_count = 0
        self.last_adjustment = now

    def _decrease_rate(self, factor: Optional[float] = None) -> None:
        """Immediately decrease rate (AIMD)"""
        factor = factor or self.decrease_factor
        new_rate = max(self.min_rate, self.current_rate * factor)
        self._update_rate(new_rate)

    def _update_rate(self, new_rate: float) -> None:
        """Update current rate and bucket"""
        self.current_rate = new_rate

        # Recreate bucket with new rate
        self.bucket = TokenBucket(
            capacity=int(new_rate * 2),
            refill_rate=new_rate
        )

    def stats(self) -> dict:
        """Get rate limiter statistics"""
        return {
            "current_rate": round(self.current_rate, 2),
            "min_rate": self.min_rate,
            "max_rate": self.max_rate,
            "success_count": self.success_count,
            "rate_limit_count": self.rate_limit_count,
            "available_tokens": self.bucket.available_tokens
        }


class DomainRateLimiter:
    """
    Per-Domain Rate Limiter

    Maintains separate rate limiters for each domain
    Respects robots.txt crawl-delay directives

    Best for: Multi-domain web scraping
    """

    def __init__(
        self,
        default_rate: int = 10,  # Requests per second
        window_seconds: int = 1
    ):
        self.default_rate = default_rate
        self.window_seconds = window_seconds
        self.limiters: Dict[str, SlidingWindowCounter] = {}
        self.crawl_delays: Dict[str, float] = {}  # robots.txt delays

    def set_crawl_delay(self, domain: str, delay_seconds: float) -> None:
        """Set crawl delay from robots.txt"""
        self.crawl_delays[domain] = delay_seconds

        # Create rate limiter based on delay
        rate = int(1.0 / delay_seconds) if delay_seconds > 0 else self.default_rate
        self.limiters[domain] = SlidingWindowCounter(
            max_requests=rate * self.window_seconds,
            window_seconds=self.window_seconds
        )

    def get_limiter(self, domain: str) -> SlidingWindowCounter:
        """Get or create rate limiter for domain"""
        if domain not in self.limiters:
            self.limiters[domain] = SlidingWindowCounter(
                max_requests=self.default_rate * self.window_seconds,
                window_seconds=self.window_seconds
            )
        return self.limiters[domain]

    async def acquire(self, domain: str) -> None:
        """Wait for permission to request from domain"""
        limiter = self.get_limiter(domain)
        await limiter.wait_for_slot()
        limiter.add_request()

    def stats(self, domain: str) -> dict:
        """Get statistics for domain"""
        if domain not in self.limiters:
            return {"message": "No requests yet"}
        return self.limiters[domain].stats()

    def global_stats(self) -> dict:
        """Get statistics for all domains"""
        return {
            "total_domains": len(self.limiters),
            "domains": {
                domain: limiter.stats()
                for domain, limiter in list(self.limiters.items())[:20]  # Top 20
            }
        }


# Usage Examples:
"""
# 1. Token Bucket - General rate limiting
bucket = TokenBucket(capacity=100, refill_rate=10)  # 10 req/sec, burst of 100

async def make_request():
    await bucket.wait_and_consume(1)
    # Make request here
    response = await httpx.get(url)

# 2. Adaptive Rate Limiter - Auto-adjust based on responses
limiter = AdaptiveRateLimiter(initial_rate=10.0)

async def scrape_with_adaptive_limit(url):
    await limiter.acquire()
    try:
        response = await httpx.get(url)
        if response.status_code == 200:
            limiter.report_success()
        elif response.status_code == 429:
            limiter.report_rate_limited()
        return response
    except Exception as e:
        limiter.report_error(500)
        raise

print(limiter.stats())
# Rate automatically adjusts between min_rate and max_rate

# 3. Per-Domain Rate Limiting
domain_limiter = DomainRateLimiter(default_rate=10)

# Set crawl-delay from robots.txt
domain_limiter.set_crawl_delay("example.com", delay_seconds=0.5)  # 2 req/sec

async def crawl_multiple_domains(urls):
    for url in urls:
        domain = extract_domain(url)
        await domain_limiter.acquire(domain)
        # Make request
        response = await httpx.get(url)

# 4. Sliding Window for precise control
window_limiter = SlidingWindowCounter(max_requests=100, window_seconds=60)

# Check before request
if window_limiter.can_make_request():
    window_limiter.add_request()
    response = await httpx.get(url)

print(window_limiter.stats())
# {
#     'max_requests': 100,
#     'window_seconds': 60,
#     'current_requests': 42,
#     'remaining_requests': 58,
#     'current_rate_per_sec': 0.7,
#     'utilization_percent': 42.0
# }
"""
