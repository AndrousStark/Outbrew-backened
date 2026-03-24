"""
Rate Limiter Configuration

Provides brute-force protection for authentication endpoints
using slowapi with Redis (production) or in-memory storage (development).

IMPORTANT: Multi-Worker/Distributed Deployments
=============================================
Without Redis, each worker maintains its own rate limit state, meaning:
- A user could make 5 requests/minute per worker (not 5 total)
- Rate limits are reset when workers restart

For production with multiple workers, you MUST set REDIS_URL:
- export REDIS_URL=redis://localhost:6379/0

Alternatively, use a reverse proxy (nginx, Cloudflare) for rate limiting.
"""

import os
import logging
import ipaddress
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from fastapi import Request
from fastapi.responses import JSONResponse

from app.core.config import settings

logger = logging.getLogger(__name__)

# Get Redis URL from settings or environment for distributed rate limiting
# Priority: settings.REDIS_URL > REDIS_URL env > RATE_LIMIT_REDIS_URL env
REDIS_URL = (
    settings.REDIS_URL or
    os.getenv("REDIS_URL") or
    os.getenv("RATE_LIMIT_REDIS_URL") or
    os.getenv("REDIS_URI") or
    ""
)

# Track if we're using distributed storage
_using_distributed_storage = False


def validate_ip_address(ip: str) -> bool:
    """Validate IP address format (IPv4 or IPv6) using Python's ipaddress module."""
    if not ip or ip == "unknown":
        return False
    try:
        ipaddress.ip_address(ip.strip())
        return True
    except ValueError:
        return False


def get_client_ip(request: Request) -> str:
    """
    Get client IP address, considering proxy headers.

    Priority:
    1. X-Forwarded-For (first IP if multiple)
    2. X-Real-IP
    3. Direct client IP
    """
    client_ip = "unknown"

    # Check for forwarded headers (behind proxy/load balancer)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Take the first IP (original client) and validate
        first_ip = forwarded_for.split(",")[0].strip()
        if validate_ip_address(first_ip):
            client_ip = first_ip
            logger.debug(f"[RateLimiter] Client IP from X-Forwarded-For: {client_ip}")
        else:
            logger.warning(f"[RateLimiter] Invalid IP in X-Forwarded-For header: {first_ip}")

    if client_ip == "unknown":
        real_ip = request.headers.get("X-Real-IP")
        if real_ip and validate_ip_address(real_ip):
            client_ip = real_ip
            logger.debug(f"[RateLimiter] Client IP from X-Real-IP: {client_ip}")

    # Fallback to direct connection
    if client_ip == "unknown" and request.client:
        client_ip = request.client.host
        logger.debug(f"[RateLimiter] Client IP from direct connection: {client_ip}")

    return client_ip


# Determine storage backend for rate limiting
def get_storage_uri() -> str:
    """
    Get storage URI for rate limiter.

    Priority:
    1. Redis URL from environment (for distributed/production)
    2. In-memory storage (for single-worker/development)

    Returns:
        Storage URI string for slowapi
    """
    global _using_distributed_storage

    if REDIS_URL:
        # Validate Redis URL format
        if REDIS_URL.startswith(("redis://", "rediss://")):
            logger.info(f"[RateLimiter] Using Redis for distributed rate limiting: {REDIS_URL.split('@')[-1]}")
            _using_distributed_storage = True
            return REDIS_URL
        else:
            logger.error(f"[RateLimiter] Invalid REDIS_URL format: {REDIS_URL[:20]}...")

    # Check environment to determine warning level
    environment = os.getenv("ENVIRONMENT", "development").lower()
    worker_count = os.getenv("WEB_CONCURRENCY", os.getenv("WORKERS", "1"))

    if environment == "production" or int(worker_count) > 1:
        logger.error(
            "[RateLimiter] SECURITY WARNING: Using in-memory storage in production/multi-worker mode! "
            "Rate limits will NOT be enforced correctly across workers. "
            "Set REDIS_URL environment variable for proper rate limiting."
        )
    else:
        logger.warning(
            "[RateLimiter] Using in-memory storage (single-worker development mode). "
            "Set REDIS_URL for distributed rate limiting in production."
        )

    _using_distributed_storage = False
    return "memory://"


def is_distributed() -> bool:
    """Check if rate limiter is using distributed storage (Redis)"""
    return _using_distributed_storage


# Create limiter instance with custom key function
# Uses Redis for distributed deployments, memory for single-worker
storage_uri = get_storage_uri()
limiter = Limiter(
    key_func=get_client_ip,
    default_limits=["1000/hour"],  # Default: 1000 requests per hour
    storage_uri=storage_uri,
    strategy="fixed-window",  # Fixed window rate limiting
)

# Log final configuration
logger.info(
    f"[RateLimiter] Initialized with storage: {'Redis (distributed)' if _using_distributed_storage else 'Memory (local)'}"
)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """
    Custom handler for rate limit exceeded errors.

    Returns a JSON response with helpful information.
    """
    client_ip = get_client_ip(request)
    endpoint = request.url.path
    method = request.method

    # Log rate limit event for monitoring and security
    logger.warning(
        f"[RateLimiter] RATE LIMIT EXCEEDED - IP: {client_ip}, "
        f"Endpoint: {method} {endpoint}, Limit: {exc.detail}"
    )

    # Parse retry time from exception detail
    try:
        retry_after = exc.detail.split("per")[1].strip() if "per" in str(exc.detail) else "1 minute"
    except (IndexError, AttributeError):
        retry_after = "1 minute"
        logger.debug(f"[RateLimiter] Could not parse retry time from: {exc.detail}")

    return JSONResponse(
        status_code=429,
        content={
            "error": "Rate limit exceeded",
            "detail": f"Too many requests. Please try again later.",
            "retry_after": retry_after,
            "endpoint": endpoint
        },
        headers={
            "Retry-After": "60",  # Suggest retry after 60 seconds
            "X-RateLimit-Limit": str(exc.detail),
        }
    )


# Rate limit decorators for common use cases
# These can be used as: @limiter.limit("5/minute")

# Strict limits for authentication
AUTH_LOGIN_LIMIT = "5/minute"          # 5 login attempts per minute
AUTH_REGISTER_LIMIT = "3/minute"       # 3 registrations per minute
AUTH_PASSWORD_CHANGE_LIMIT = "3/hour"  # 3 password changes per hour

# Standard API limits
API_READ_LIMIT = "100/minute"          # 100 reads per minute
API_WRITE_LIMIT = "30/minute"          # 30 writes per minute
API_BULK_LIMIT = "5/minute"            # 5 bulk operations per minute


def get_rate_limiter_status() -> dict:
    """
    Get rate limiter health/status information.

    Returns:
        Dict with rate limiter configuration and health status
    """
    return {
        "storage_type": "redis" if _using_distributed_storage else "memory",
        "distributed": _using_distributed_storage,
        "storage_uri": "redis://***" if _using_distributed_storage else "memory://",
        "default_limits": ["1000/hour"],
        "strategy": "fixed-window",
        "auth_limits": {
            "login": AUTH_LOGIN_LIMIT,
            "register": AUTH_REGISTER_LIMIT,
            "password_change": AUTH_PASSWORD_CHANGE_LIMIT
        },
        "api_limits": {
            "read": API_READ_LIMIT,
            "write": API_WRITE_LIMIT,
            "bulk": API_BULK_LIMIT
        },
        "warning": None if _using_distributed_storage else (
            "Using in-memory storage. Rate limits are NOT shared across workers. "
            "Set REDIS_URL for production deployments."
        )
    }
