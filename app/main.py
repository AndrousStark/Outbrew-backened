"""
Outbrew - Main FastAPI Application

Features:
- RESTful API for outreach campaign management
- Email warming and rate limiting
- Background task scheduling
- Real-time statistics
- Brute-force protection with rate limiting
- Security headers middleware
"""

import logging
import traceback
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.core.config import settings
from app.core.database import Base, engine, init_db, check_database_health
from app.core.rate_limiter import limiter, rate_limit_exceeded_handler
from app.api.v1.router import api_router
from app.middleware import RequestLoggingMiddleware, TimingMiddleware


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses"""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Prevent MIME type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # Prevent clickjacking - allow same origin only
        response.headers["X-Frame-Options"] = "SAMEORIGIN"

        # Enable XSS filtering in browsers
        response.headers["X-XSS-Protection"] = "1; mode=block"

        # Referrer policy - don't leak full URLs
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # Permissions policy - restrict browser features
        response.headers["Permissions-Policy"] = (
            "geolocation=(), microphone=(), camera=()"
        )

        # Content Security Policy (localhost only in development)
        connect_src = "'self'"
        if settings.ENVIRONMENT == "development":
            connect_src = "'self' http://localhost:* https://localhost:*"
        # Allow Swagger UI CDN resources on docs pages
        swagger_cdn = ""
        if request.url.path in ("/api/docs", "/api/redoc", "/api/openapi.json"):
            swagger_cdn = " https://cdn.jsdelivr.net https://unpkg.com"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            f"script-src 'self' 'unsafe-inline'{swagger_cdn}; "
            f"style-src 'self' 'unsafe-inline'{swagger_cdn}; "
            f"img-src 'self' data: https:{swagger_cdn}; "
            f"font-src 'self' data:{swagger_cdn}; "
            f"connect-src {connect_src}; "
            "frame-ancestors 'self'"
        )

        # Cache control for API responses
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
            response.headers["Pragma"] = "no-cache"

        return response


# Configure logging
logging.basicConfig(
    level=logging.INFO if settings.DEBUG else logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown events"""
    logger.info("=" * 80)
    logger.info(f"[STARTUP] Starting {settings.PROJECT_NAME}")
    logger.info(f"[STARTUP] Environment: {settings.ENVIRONMENT}")
    logger.info("=" * 80)

    # Validate SECRET_KEY is set and not a weak default
    if not settings.SECRET_KEY or settings.SECRET_KEY in ("secret", "change-me"):
        raise RuntimeError(
            "SECRET_KEY is empty or insecure. Set a strong SECRET_KEY in .env "
            "(minimum 32 characters, randomly generated)."
        )

    # Initialize database
    logger.info("[STARTUP] Creating database tables...")
    try:
        init_db()
        logger.info("[STARTUP] Database ready!")
    except Exception as e:
        logger.error(f"[STARTUP] Database initialization failed: {e}")
        raise

    # Verify database connectivity
    db_health = check_database_health()
    if db_health["healthy"]:
        logger.info("[STARTUP] Database health check passed")
    else:
        logger.warning(
            f"[STARTUP] Database health check failed: {db_health['message']}"
        )

    # Start background scheduler
    try:
        from app.tasks.scheduler import start_scheduler, get_scheduler_status

        scheduler = start_scheduler()
        status = get_scheduler_status()
        logger.info(f"[STARTUP] Scheduler started with {len(status['jobs'])} jobs")
    except Exception as e:
        logger.warning(f"[STARTUP] Scheduler failed to start: {e}")

    yield

    # Shutdown scheduler
    logger.info("[SHUTDOWN] Shutting down...")
    try:
        from app.tasks.scheduler import shutdown_scheduler

        shutdown_scheduler()
        logger.info("[SHUTDOWN] Scheduler stopped")
    except Exception as e:
        logger.warning(f"[SHUTDOWN] Scheduler shutdown error: {e}")


# OpenAPI Tags for documentation organization
openapi_tags = [
    {
        "name": "Authentication",
        "description": "User authentication, registration, and session management",
    },
    {
        "name": "Applications",
        "description": "Job application CRUD operations and email sending",
    },
    {
        "name": "Resumes",
        "description": "Resume version management and uploads",
    },
    {
        "name": "Email Templates",
        "description": "Email template management and preview",
    },
    {
        "name": "Email Warming",
        "description": "Domain reputation warming configuration and progress",
    },
    {
        "name": "Rate Limiting",
        "description": "Email sending rate limit configuration and usage",
    },
    {
        "name": "Notifications",
        "description": "In-app notification management",
    },
    {
        "name": "Users",
        "description": "User management (admin only)",
    },
    {
        "name": "Health",
        "description": "System health and status endpoints",
    },
]

# Create FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version=settings.VERSION,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    openapi_tags=openapi_tags,
    lifespan=lifespan,
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    contact={
        "name": "Outbrew Support",
        "url": "https://github.com/AndrousStark/Outbrew-backened",
    },
)

# Rate Limiter Setup
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)


# Global Exception Handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle all unhandled exceptions with proper logging"""
    logger.error(
        f"[ERROR] Unhandled exception on {request.method} {request.url.path}: {str(exc)}"
    )
    logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")

    return JSONResponse(
        status_code=500,
        content={
            "detail": "An internal server error occurred",
            "path": request.url.path,
            "method": request.method,
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors with detailed feedback"""
    logger.warning(
        f"[VALIDATION] Validation error on {request.method} {request.url.path}: {exc.errors()}"
    )

    return JSONResponse(
        status_code=422,
        content={
            "detail": "Request validation failed",
            "errors": exc.errors(),
            "path": request.url.path,
        },
    )


# CORS Middleware - MUST be added first to process responses last
# This ensures CORS headers are applied after all other middleware
if settings.DEBUG:
    # In local dev, use explicit localhost origins (wildcard doesn't work with credentials)
    cors_origins = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://localhost:3003",
        "http://localhost:3004",
        "http://localhost:3005",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
        "http://127.0.0.1:3002",
        "http://127.0.0.1:3003",
        "http://127.0.0.1:3004",
        "http://127.0.0.1:3005",
    ]
else:
    cors_origins = settings.get_cors_origins()

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_origin_regex=None,
    allow_credentials=True,
    allow_methods=[
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
    ],  # Explicit methods instead of wildcard
    allow_headers=[
        "Authorization",
        "Content-Type",
        "Accept",
        "Origin",
        "X-Requested-With",
    ],  # Explicit headers
    expose_headers=[
        "X-Correlation-ID",
        "X-Process-Time",
    ],  # Allow frontend to read these headers
    max_age=3600,  # Cache preflight responses for 1 hour
)

# Security Headers Middleware - adds security headers to all responses
app.add_middleware(SecurityHeadersMiddleware)

# Custom Middlewares
app.add_middleware(TimingMiddleware)
app.add_middleware(RequestLoggingMiddleware)

# Include API router
app.include_router(api_router, prefix=settings.API_V1_PREFIX)


@app.get("/", tags=["Health"])
def root():
    """
    Root endpoint - returns application information.

    Use this to verify the API is running.
    """
    return {
        "name": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "status": "operational",
        "docs": "/api/docs",
        "redoc": "/api/redoc",
    }


@app.get("/health", tags=["Health"])
def health_check():
    """
    Health check endpoint - returns system health status.

    Use this for monitoring and load balancer health checks.
    Includes database connectivity status.
    """
    logger.debug("[Health] Running health check")

    # Check database health
    db_health = check_database_health()

    health_status = {
        "status": "healthy" if db_health["healthy"] else "degraded",
        "environment": settings.ENVIRONMENT,
        "version": settings.VERSION,
        "database": {
            "status": "connected" if db_health["healthy"] else "disconnected",
            "message": db_health["message"],
        },
    }

    if not db_health["healthy"]:
        logger.warning(f"[Health] Health check degraded: {db_health['message']}")

    return health_status


@app.get("/ready", tags=["Health"])
def readiness_check():
    """
    Readiness check endpoint - returns whether the application is ready to accept traffic.

    Use this for Kubernetes/container orchestration readiness probes.
    Returns 200 if ready, 503 if not ready.
    """
    logger.debug("[Ready] Running readiness check")

    # Check database health
    db_health = check_database_health()

    if not db_health["healthy"]:
        logger.warning(f"[Ready] Application not ready: {db_health['message']}")
        return JSONResponse(
            status_code=503,
            content={
                "ready": False,
                "reason": "Database not available",
                "message": db_health["message"],
            },
        )

    return {
        "ready": True,
        "environment": settings.ENVIRONMENT,
        "version": settings.VERSION,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
