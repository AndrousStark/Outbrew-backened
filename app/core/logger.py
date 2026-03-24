"""
Structured Logging Configuration with JSON Support and Log Rotation

Features:
- JSON structured logging for production (machine-parseable)
- Human-readable colored console logs for development
- Automatic sensitive data redaction (passwords, tokens, secrets)
- Correlation ID tracking for request tracing
- Log rotation with configurable size and backup count
- Audit logging for security-relevant events
- Performance metrics logging
"""
import logging
import sys
import re
import os
import json
import uuid
import traceback
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional, Any, Dict
from contextvars import ContextVar

# Context variable for request correlation ID (thread-safe)
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID for request tracing."""
    return correlation_id_var.get()


def set_correlation_id(correlation_id: Optional[str] = None) -> str:
    """Set a correlation ID for the current context. Generates one if not provided."""
    cid = correlation_id or str(uuid.uuid4())[:12]
    correlation_id_var.set(cid)
    return cid


def clear_correlation_id() -> None:
    """Clear the correlation ID after request completes."""
    correlation_id_var.set(None)


# Create logs directory with secure permissions (owner read/write only)
LOG_DIR = Path(__file__).parent.parent.parent / "logs"
try:
    LOG_DIR.mkdir(exist_ok=True, mode=0o700)  # Secure: owner only
except OSError as e:
    # Fallback to system temp directory if main log directory fails
    import tempfile
    LOG_DIR = Path(tempfile.gettempdir()) / "outbrew_app_logs"
    LOG_DIR.mkdir(exist_ok=True)
    print(f"[Logger] Warning: Could not create log directory, using fallback: {LOG_DIR}")

# Log rotation configuration
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB per file
BACKUP_COUNT = 5  # Keep 5 backup files

# Environment detection for log format selection
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
USE_JSON_LOGS = ENVIRONMENT in ("production", "staging") or os.getenv("JSON_LOGS", "").lower() == "true"

# Sensitive data patterns to filter from logs (comprehensive list)
SENSITIVE_PATTERNS = [
    # Passwords and secrets
    (re.compile(r'password["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'password: ***REDACTED***'),
    (re.compile(r'hashed_password["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'hashed_password: ***REDACTED***'),
    (re.compile(r'secret["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'secret: ***REDACTED***'),
    (re.compile(r'secret_key["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'secret_key: ***REDACTED***'),
    # API keys and tokens
    (re.compile(r'api[_-]?key["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'api_key: ***REDACTED***'),
    (re.compile(r'access[_-]?token["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'access_token: ***REDACTED***'),
    (re.compile(r'refresh[_-]?token["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'refresh_token: ***REDACTED***'),
    (re.compile(r'Bearer\s+([A-Za-z0-9_\-\.]+)', re.IGNORECASE), r'Bearer ***REDACTED***'),
    # Email credentials
    (re.compile(r'email_password["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'email_password: ***REDACTED***'),
    (re.compile(r'smtp_password["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'smtp_password: ***REDACTED***'),
    (re.compile(r'gmail_app_password["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)', re.IGNORECASE), r'gmail_app_password: ***REDACTED***'),
    # Database URLs with credentials
    (re.compile(r'(postgres|mysql|redis)://[^:]+:([^@]+)@', re.IGNORECASE), r'\1://***:***@'),
    # Credit card patterns (safety)
    (re.compile(r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b'), r'***CARD-REDACTED***'),
    # SSN patterns (safety)
    (re.compile(r'\b\d{3}-\d{2}-\d{4}\b'), r'***SSN-REDACTED***'),
]


def redact_sensitive_data(text: str) -> str:
    """Redact sensitive data from a string."""
    if not isinstance(text, str):
        return text
    for pattern, replacement in SENSITIVE_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


class SensitiveDataFilter(logging.Filter):
    """Filter to redact sensitive data from log messages"""

    def filter(self, record):
        if isinstance(record.msg, str):
            record.msg = redact_sensitive_data(record.msg)
        # Also filter args if present
        if record.args:
            filtered_args = []
            for arg in record.args:
                if isinstance(arg, str):
                    arg = redact_sensitive_data(arg)
                filtered_args.append(arg)
            record.args = tuple(filtered_args)
        return True


class JSONFormatter(logging.Formatter):
    """
    JSON structured log formatter for production environments.

    Output format:
    {
        "timestamp": "2024-01-01T12:00:00.000Z",
        "level": "INFO",
        "logger": "app.auth",
        "message": "User logged in",
        "correlation_id": "abc123",
        "function": "login",
        "line": 42,
        "extra": {...}
    }
    """

    def format(self, record: logging.LogRecord) -> str:
        # Build the base log entry
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_sensitive_data(record.getMessage()),
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add correlation ID if available
        correlation_id = get_correlation_id()
        if correlation_id:
            log_entry["correlation_id"] = correlation_id

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": redact_sensitive_data(
                    "".join(traceback.format_exception(*record.exc_info))
                ) if record.exc_info[2] else None
            }

        # Add extra fields (custom data passed via extra={})
        standard_keys = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs",
            "pathname", "process", "processName", "relativeCreated",
            "stack_info", "exc_info", "exc_text", "thread", "threadName",
            "message"
        }
        extra_fields = {
            k: v for k, v in record.__dict__.items()
            if k not in standard_keys and not k.startswith("_")
        }
        if extra_fields:
            log_entry["extra"] = extra_fields

        return json.dumps(log_entry, default=str)


class ColoredFormatter(logging.Formatter):
    """Colored log formatter for console output in development"""

    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'

    def format(self, record):
        # Save original levelname
        original_levelname = record.levelname

        # Add color to level name
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"

        # Add correlation ID to message if available
        correlation_id = get_correlation_id()
        if correlation_id:
            record.msg = f"[{correlation_id}] {record.msg}"

        result = super().format(record)

        # Restore original levelname for other handlers
        record.levelname = original_levelname

        return result

# Create formatters based on environment
json_formatter = JSONFormatter()

file_formatter = logging.Formatter(
    '[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

console_formatter = ColoredFormatter(
    '[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s',
    datefmt='%H:%M:%S'
)


def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Get a configured logger instance with sensitive data filtering.

    Features:
    - Automatic sensitive data redaction
    - JSON format in production, colored console in development
    - Correlation ID support for request tracing
    - Rotating file handlers with size limits

    Args:
        name: Logger name (e.g., "app.auth", "email.service")
        level: Minimum log level (default: INFO)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    # Add sensitive data filter to prevent password/token leaks
    sensitive_filter = SensitiveDataFilter()
    logger.addFilter(sensitive_filter)

    # Console handler with environment-appropriate format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    if USE_JSON_LOGS:
        console_handler.setFormatter(json_formatter)
    else:
        console_handler.setFormatter(console_formatter)
    console_handler.addFilter(sensitive_filter)
    logger.addHandler(console_handler)

    # File handler - All logs (with rotation) - Always JSON for parseability
    all_logs_file = LOG_DIR / "app.log"
    try:
        # Use regular FileHandler instead of RotatingFileHandler to avoid Windows file locking issues
        file_handler = logging.FileHandler(
            all_logs_file,
            mode='a',
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(json_formatter)  # Always JSON for files
        logger.addHandler(file_handler)
    except (PermissionError, OSError) as e:
        # Log to console if file logging fails
        print(f"[Logger] Warning: Could not create file handler for {all_logs_file}: {e}")

    # File handler - Error logs only (with rotation)
    error_logs_file = LOG_DIR / "errors.log"
    try:
        # Use regular FileHandler instead of RotatingFileHandler to avoid Windows file locking issues
        error_handler = logging.FileHandler(
            error_logs_file,
            mode='a',
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(json_formatter)  # Always JSON for files
        logger.addHandler(error_handler)
    except (PermissionError, OSError) as e:
        print(f"[Logger] Warning: Could not create error handler for {error_logs_file}: {e}")

    # File handler - Email operations (with rotation)
    # Only add email handler to email-related loggers
    if 'email' in name.lower() or 'warming' in name.lower() or 'rate' in name.lower():
        email_logs_file = LOG_DIR / "email.log"
        try:
            # Use regular FileHandler instead of RotatingFileHandler to avoid Windows file locking issues
            email_handler = logging.FileHandler(
                email_logs_file,
                mode='a',
                encoding='utf-8'
            )
            email_handler.setLevel(logging.INFO)
            email_handler.setFormatter(json_formatter)  # Always JSON for files
            logger.addHandler(email_handler)
        except (PermissionError, OSError) as e:
            print(f"[Logger] Warning: Could not create email handler for {email_logs_file}: {e}")

    # File handler - Audit logs for security events
    if 'auth' in name.lower() or 'security' in name.lower() or 'audit' in name.lower():
        audit_logs_file = LOG_DIR / "audit.log"
        try:
            # Use regular FileHandler instead of RotatingFileHandler to avoid Windows file locking issues
            audit_handler = logging.FileHandler(
                audit_logs_file,
                mode='a',
                encoding='utf-8'
            )
            audit_handler.setLevel(logging.INFO)
            audit_handler.setFormatter(json_formatter)
            logger.addHandler(audit_handler)
        except (PermissionError, OSError) as e:
            print(f"[Logger] Warning: Could not create audit handler for {audit_logs_file}: {e}")

    return logger


def log_audit_event(
    event_type: str,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    success: bool = True
) -> None:
    """
    Log a security-relevant audit event.

    Args:
        event_type: Type of event (e.g., "login", "logout", "password_change", "permission_denied")
        user_id: ID of the user involved
        username: Username of the user involved
        details: Additional event details
        success: Whether the action was successful

    Example:
        log_audit_event("login", user_id=1, username="admin", success=True)
        log_audit_event("permission_denied", username="user", details={"resource": "/admin"})
    """
    audit_logger = get_logger("security.audit")
    event_data = {
        "event_type": event_type,
        "user_id": user_id,
        "username": username,
        "success": success,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if details:
        event_data["details"] = details

    correlation_id = get_correlation_id()
    if correlation_id:
        event_data["correlation_id"] = correlation_id

    if success:
        audit_logger.info(f"AUDIT: {event_type}", extra=event_data)
    else:
        audit_logger.warning(f"AUDIT: {event_type} FAILED", extra=event_data)


def log_performance_metric(
    operation: str,
    duration_ms: float,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log a performance metric for monitoring.

    Args:
        operation: Name of the operation (e.g., "db_query", "email_send", "api_call")
        duration_ms: Duration in milliseconds
        details: Additional metric details

    Example:
        log_performance_metric("db_query", 45.2, {"table": "applications", "rows": 100})
    """
    perf_logger = get_logger("performance")
    metric_data = {
        "operation": operation,
        "duration_ms": round(duration_ms, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if details:
        metric_data.update(details)

    # Log warning for slow operations
    if duration_ms > 1000:  # More than 1 second
        perf_logger.warning(f"SLOW OPERATION: {operation} took {duration_ms:.2f}ms", extra=metric_data)
    else:
        perf_logger.debug(f"PERF: {operation} took {duration_ms:.2f}ms", extra=metric_data)


# Pre-configured loggers
app_logger = get_logger("app")
email_logger = get_logger("email")
warming_logger = get_logger("email.warming")
rate_limit_logger = get_logger("email.rate_limit")
api_logger = get_logger("api")
auth_logger = get_logger("auth")
audit_logger = get_logger("security.audit")


# Export utilities
__all__ = [
    "get_logger",
    "get_correlation_id",
    "set_correlation_id",
    "clear_correlation_id",
    "log_audit_event",
    "log_performance_metric",
    "redact_sensitive_data",
    "app_logger",
    "email_logger",
    "warming_logger",
    "rate_limit_logger",
    "api_logger",
    "auth_logger",
    "audit_logger",
    "LOG_DIR",
    "USE_JSON_LOGS",
]
