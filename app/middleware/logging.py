"""
Request Logging Middleware with Correlation ID Support

Features:
- Unique correlation ID for each request (for distributed tracing)
- Request/response logging with timing
- Client IP detection (supports proxies)
- Automatic sensitive header redaction
- User agent and referer tracking
"""
import time
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.core.logger import (
    get_logger,
    set_correlation_id,
    get_correlation_id,
    clear_correlation_id,
    log_performance_metric,
)

logger = get_logger("api.requests")

# Headers that should never be logged
REDACTED_HEADERS = {
    "authorization",
    "cookie",
    "x-api-key",
    "x-auth-token",
    "x-csrf-token",
}


def get_client_ip(request: Request) -> str:
    """Extract client IP, handling proxy headers."""
    # Check forwarded headers (reverse proxy)
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        # Take the first IP (original client)
        return forwarded.split(",")[0].strip()

    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()

    # Direct connection
    if request.client:
        return request.client.host

    return "unknown"


def get_safe_headers(request: Request) -> dict:
    """Extract headers, redacting sensitive ones."""
    safe_headers = {}
    for key, value in request.headers.items():
        if key.lower() in REDACTED_HEADERS:
            safe_headers[key] = "***REDACTED***"
        else:
            safe_headers[key] = value
    return safe_headers


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log all API requests and responses with correlation IDs.

    Each request gets a unique correlation_id that is:
    - Added to the response headers (X-Correlation-ID)
    - Included in all log messages during the request
    - Useful for tracing requests across services/logs
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        # Generate and set correlation ID
        # Check if one was provided in the request header
        incoming_correlation_id = request.headers.get("x-correlation-id")
        correlation_id = set_correlation_id(incoming_correlation_id)

        # Start timer
        start_time = time.time()

        # Get request details
        method = request.method
        path = request.url.path
        query = str(request.query_params) if request.query_params else ""
        client_ip = get_client_ip(request)
        user_agent = request.headers.get("user-agent", "unknown")
        referer = request.headers.get("referer", "")

        # Log incoming request
        logger.info(
            f"[REQ] {method} {path}",
            extra={
                "event": "request_start",
                "method": method,
                "path": path,
                "query": query,
                "client_ip": client_ip,
                "user_agent": user_agent[:100] if user_agent else "",  # Truncate long UAs
                "referer": referer,
                "correlation_id": correlation_id,
            }
        )

        # Process request
        try:
            response = await call_next(request)
        except Exception as e:
            # Log unhandled exception
            duration_ms = (time.time() - start_time) * 1000
            logger.error(
                f"[ERROR] {method} {path} - Exception: {type(e).__name__}",
                extra={
                    "event": "request_error",
                    "method": method,
                    "path": path,
                    "client_ip": client_ip,
                    "duration_ms": round(duration_ms, 2),
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "correlation_id": correlation_id,
                },
                exc_info=True
            )
            # Clear correlation ID and re-raise
            clear_correlation_id()
            raise

        # Calculate duration
        duration_ms = (time.time() - start_time) * 1000
        status_code = response.status_code

        # Add correlation ID to response headers
        response.headers["X-Correlation-ID"] = correlation_id

        # Determine log level based on status code
        if status_code >= 500:
            log_level = logging.ERROR
            status_emoji = "[ERROR]"
        elif status_code >= 400:
            log_level = logging.WARNING
            status_emoji = "[WARN]"
        else:
            log_level = logging.INFO
            status_emoji = "[OK]"

        # Log completed request
        logger.log(
            log_level,
            f"{status_emoji} {method} {path} - {status_code} - {duration_ms:.1f}ms",
            extra={
                "event": "request_complete",
                "method": method,
                "path": path,
                "status_code": status_code,
                "duration_ms": round(duration_ms, 2),
                "client_ip": client_ip,
                "correlation_id": correlation_id,
            }
        )

        # Log performance metric for slow requests
        if duration_ms > 500:  # Warn for requests > 500ms
            log_performance_metric(
                operation=f"http_{method.lower()}",
                duration_ms=duration_ms,
                details={
                    "path": path,
                    "status_code": status_code,
                    "slow": duration_ms > 1000,
                }
            )

        # Clear correlation ID after request completes
        clear_correlation_id()

        return response
