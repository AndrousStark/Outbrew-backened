"""Authentication and Authorization

Security Features:
- Short-lived access tokens (configurable, default 30 minutes) for API requests
- Long-lived refresh tokens (configurable, default 7 days) for session renewal
- Secure password hashing with bcrypt
- JWT-based stateless authentication
- Token blacklisting for secure logout and revocation
"""
import json
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, Set


def utc_now() -> datetime:
    """Get current UTC time as timezone-aware datetime.

    Replaces deprecated datetime.now(timezone.utc) with timezone-aware alternative.
    """
    return datetime.now(timezone.utc)
from fastapi import Depends, HTTPException, Query, status
from fastapi.security import OAuth2PasswordBearer
import jwt
from jwt.exceptions import PyJWTError as JWTError
import bcrypt
from sqlalchemy.orm import Session
import threading

from app.core.config import settings
from app.core.database import get_database_session
from app.models.candidate import Candidate, UserRole, PlanTier

logger = logging.getLogger(__name__)

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

# ============= TOKEN BLACKLIST =============
# Thread-safe in-memory token blacklist with automatic cleanup
# For production with multiple workers, use Redis or database storage
class TokenBlacklist:
    """
    Token blacklist for secure logout and token revocation.

    Stores blacklisted JTIs (JWT IDs) with their expiry times.
    Automatically cleans up expired entries to prevent memory growth.

    Note: For multi-worker deployments, extend this to use Redis:
    - Set REDIS_URL environment variable
    - Blacklisted tokens will be shared across all workers
    """

    _PERSIST_PATH = Path("data/token_blacklist.json")

    def __init__(self):
        self._blacklist: dict[str, datetime] = {}  # jti -> expiry_time
        self._lock = threading.Lock()
        self._last_cleanup = utc_now()
        self._cleanup_interval = timedelta(minutes=15)
        self._dirty = False
        self._load_from_disk()

    def add(self, jti: str, expiry: datetime) -> None:
        """Add a token JTI to the blacklist"""
        with self._lock:
            self._blacklist[jti] = expiry
            self._dirty = True
            self._maybe_cleanup()
            logger.info(f"[Auth] Token blacklisted: {jti[:8]}... (expires: {expiry})")

    def is_blacklisted(self, jti: str) -> bool:
        """Check if a token JTI is blacklisted"""
        with self._lock:
            if jti not in self._blacklist:
                return False
            # Check if the blacklist entry has expired (token would be invalid anyway)
            if utc_now() > self._blacklist[jti]:
                del self._blacklist[jti]
                return False
            return True

    def _maybe_cleanup(self) -> None:
        """Remove expired entries and persist to disk periodically (debounced)"""
        now = utc_now()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        self._last_cleanup = now
        expired_keys = [
            jti for jti, expiry in self._blacklist.items()
            if now > expiry
        ]
        for jti in expired_keys:
            del self._blacklist[jti]
            self._dirty = True

        if expired_keys:
            logger.debug(f"[Auth] Cleaned up {len(expired_keys)} expired blacklist entries")

        # Persist to disk only when dirty (debounced — not on every add)
        if self._dirty:
            self._persist_to_disk()
            self._dirty = False

    def _persist_to_disk(self) -> None:
        """Save blacklist to disk for persistence across restarts."""
        try:
            self._PERSIST_PATH.parent.mkdir(parents=True, exist_ok=True)
            data = {jti: exp.isoformat() for jti, exp in self._blacklist.items()}
            self._PERSIST_PATH.write_text(json.dumps(data), encoding="utf-8")
        except Exception as e:
            logger.warning(f"[Auth] Failed to persist token blacklist: {e}")

    def _load_from_disk(self) -> None:
        """Load blacklist from disk on startup."""
        try:
            if self._PERSIST_PATH.exists():
                data = json.loads(self._PERSIST_PATH.read_text(encoding="utf-8"))
                now = utc_now()
                for jti, exp_str in data.items():
                    expiry = datetime.fromisoformat(exp_str)
                    if expiry.tzinfo is None:
                        expiry = expiry.replace(tzinfo=timezone.utc)
                    if expiry > now:
                        self._blacklist[jti] = expiry
                logger.info(f"[Auth] Loaded {len(self._blacklist)} blacklisted tokens from disk")
        except Exception as e:
            logger.warning(f"[Auth] Failed to load token blacklist from disk: {e}")

    def revoke_all_for_user(self, username: str, db: Session) -> int:
        """
        Revoke all tokens for a user (for password change, account compromise, etc.)

        Sets tokens_invalid_before to now — all tokens with iat < this are rejected.
        """
        candidate = db.query(Candidate).filter(
            Candidate.username == username,
            Candidate.deleted_at.is_(None)
        ).first()
        if not candidate:
            logger.warning(f"[Auth] Cannot revoke tokens — user not found: {username}")
            return 0

        candidate.tokens_invalid_before = utc_now()
        db.commit()
        logger.warning(f"[Auth] All tokens revoked for user: {username} (tokens_invalid_before={candidate.tokens_invalid_before})")
        return 1


# Global token blacklist instance
# WARNING: This is per-process — tokens blacklisted in one worker are not
# visible to others. For multi-worker deployments, replace with Redis-backed
# storage (set REDIS_URL in environment). File-based persistence provides
# cross-restart continuity but NOT cross-worker consistency.
token_blacklist = TokenBlacklist()
logger.warning(
    "[Auth] Token blacklist is in-memory + file-backed (single-worker mode). "
    "For multi-worker deployments, implement Redis-backed blacklist."
)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash using bcrypt directly"""
    try:
        return bcrypt.checkpw(
            plain_password.encode('utf-8'),
            hashed_password.encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Password verification error: {e}")
        return False


def get_password_hash(password: str) -> str:
    """Hash a password using bcrypt directly"""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a short-lived JWT access token.

    Args:
        data: Token payload (typically includes 'sub' for username)
        expires_delta: Custom expiry time (default from settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    Returns:
        Encoded JWT string
    """
    to_encode = data.copy()
    now = utc_now()
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    # Add unique identifier for blacklisting support
    jti = secrets.token_hex(16)
    to_encode.update({
        "exp": expire,
        "type": "access",
        "iat": now,
        "jti": jti  # Unique token ID for blacklisting
    })
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    logger.debug(f"[Auth] Created access token for {data.get('sub')}, jti: {jti[:8]}..., expires: {expire}")
    return encoded_jwt


def create_refresh_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a long-lived JWT refresh token.

    Args:
        data: Token payload (typically includes 'sub' for username)
        expires_delta: Custom expiry time (default from settings.REFRESH_TOKEN_EXPIRE_DAYS)

    Returns:
        Encoded JWT string
    """
    to_encode = data.copy()
    now = utc_now()
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)

    # Add unique identifier for blacklisting and preventing token reuse attacks
    jti = secrets.token_hex(16)
    to_encode.update({
        "exp": expire,
        "type": "refresh",
        "iat": now,
        "jti": jti  # Unique token ID for blacklisting
    })
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    logger.debug(f"[Auth] Created refresh token for {data.get('sub')}, jti: {jti[:8]}..., expires: {expire}")
    return encoded_jwt


def create_token_pair(username: str) -> Tuple[str, str]:
    """
    Create both access and refresh tokens for a user.

    Args:
        username: The username to encode in tokens

    Returns:
        Tuple of (access_token, refresh_token)
    """
    token_data = {"sub": username}
    access_token = create_access_token(token_data)
    refresh_token = create_refresh_token(token_data)
    return access_token, refresh_token


def verify_refresh_token(token: str) -> Optional[str]:
    """
    Verify a refresh token and return the username.

    Args:
        token: The refresh token to verify

    Returns:
        Username if valid, None otherwise
    """
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])

        # Verify it's a refresh token
        if payload.get("type") != "refresh":
            logger.warning("[Auth] Token is not a refresh token")
            return None

        # Check if token is blacklisted
        jti = payload.get("jti")
        if jti and token_blacklist.is_blacklisted(jti):
            logger.warning(f"[Auth] Refresh token is blacklisted: {jti[:8]}...")
            return None

        username: str = payload.get("sub")
        if username is None:
            logger.warning("[Auth] Refresh token missing 'sub' claim")
            return None

        logger.debug(f"[Auth] Verified refresh token for {username}")
        return username

    except JWTError as e:
        logger.warning(f"[Auth] Refresh token validation failed: {e}")
        return None


def blacklist_token(token: str) -> bool:
    """
    Add a token to the blacklist (for logout/revocation).

    Args:
        token: The JWT token to blacklist

    Returns:
        True if blacklisted successfully, False otherwise
    """
    try:
        # Decode without verification to get claims (token may already be expired)
        payload = jwt.decode(
            token, settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
            options={"verify_exp": False}
        )

        jti = payload.get("jti")
        exp = payload.get("exp")

        if not jti:
            logger.warning("[Auth] Cannot blacklist token without JTI")
            return False

        # Convert exp timestamp to datetime
        if exp:
            expiry = datetime.fromtimestamp(exp, tz=timezone.utc)
        else:
            # If no expiry, blacklist for 24 hours
            expiry = utc_now() + timedelta(hours=24)

        token_blacklist.add(jti, expiry)
        return True

    except JWTError as e:
        logger.error(f"[Auth] Failed to blacklist token: {e}")
        return False


def authenticate_candidate(db: Session, username: str, password: str) -> Optional[Candidate]:
    """Authenticate a candidate by username and password"""
    candidate = db.query(Candidate).filter(
        Candidate.username == username,
        Candidate.deleted_at.is_(None)
    ).first()

    if not candidate:
        return None

    if not verify_password(password, candidate.hashed_password):
        return None

    return candidate


async def get_current_candidate(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_database_session)
) -> Candidate:
    """Get the current authenticated candidate from JWT token"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])

        # Check if token is blacklisted
        jti = payload.get("jti")
        if jti and token_blacklist.is_blacklisted(jti):
            logger.warning(f"[Auth] Access token is blacklisted: {jti[:8]}...")
            raise credentials_exception

        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    candidate = db.query(Candidate).filter(
        Candidate.username == username,
        Candidate.deleted_at.is_(None)
    ).first()

    if candidate is None:
        raise credentials_exception

    if not candidate.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user account"
        )

    # Check if token was issued before a revoke-all operation
    if candidate.tokens_invalid_before:
        token_iat = payload.get("iat")
        if token_iat:
            issued_at = datetime.fromtimestamp(token_iat, tz=timezone.utc)
            if issued_at < candidate.tokens_invalid_before:
                logger.warning(f"[Auth] Token rejected — issued before revocation for {username}")
                raise credentials_exception

    return candidate


async def get_current_active_candidate(
    current_candidate: Candidate = Depends(get_current_candidate)
) -> Candidate:
    """Get current active candidate"""
    if not current_candidate.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user account"
        )
    return current_candidate


async def require_super_admin(
    current_candidate: Candidate = Depends(get_current_candidate)
) -> Candidate:
    """Require Super Admin role"""
    if current_candidate.role != UserRole.SUPER_ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Super Admin access required"
        )
    return current_candidate


async def require_admin_or_owner(
    candidate_id: int,
    current_candidate: Candidate = Depends(get_current_candidate)
) -> Candidate:
    """
    Require either Super Admin role OR ownership of the resource

    Args:
        candidate_id: The candidate ID of the resource owner
        current_candidate: The current authenticated candidate

    Returns:
        The current candidate if authorized

    Raises:
        HTTPException: If not authorized
    """
    # Super admins can access everything
    if current_candidate.role == UserRole.SUPER_ADMIN:
        return current_candidate

    # Regular users can only access their own data
    if current_candidate.id != candidate_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to access this resource"
        )

    return current_candidate


def has_permission(
    current_candidate: Candidate,
    required_role: UserRole = None,
    resource_candidate_id: int = None
) -> bool:
    """
    Check if candidate has permission

    Args:
        current_candidate: The current authenticated candidate
        required_role: Required role (e.g., SUPER_ADMIN)
        resource_candidate_id: Candidate ID of the resource owner

    Returns:
        True if has permission, False otherwise
    """
    # Super admin has all permissions
    if current_candidate.role == UserRole.SUPER_ADMIN:
        return True

    # Check role requirement
    if required_role and current_candidate.role != required_role:
        return False

    # Check ownership
    if resource_candidate_id and current_candidate.id != resource_candidate_id:
        return False

    return True


# ============= PLAN ENFORCEMENT =============


def require_plan(required_tier: str = "pro"):
    """
    FastAPI dependency that requires a specific plan tier.

    Usage:
        @router.get("/pro-feature")
        def pro_feature(candidate: Candidate = Depends(require_plan("pro"))):
            ...
    """
    async def _check_plan(
        current_candidate: Candidate = Depends(get_current_candidate)
    ) -> Candidate:
        # Super admins always have access
        if current_candidate.role == UserRole.SUPER_ADMIN:
            return current_candidate

        raw_plan = getattr(current_candidate, "plan_tier", None) or "free"
        plan = raw_plan.value if hasattr(raw_plan, "value") else str(raw_plan).lower()

        if required_tier == "pro" and plan != "pro":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "upgrade_required",
                    "message": "This feature requires a Pro plan",
                    "current_plan": plan,
                    "required_plan": required_tier,
                }
            )
        return current_candidate
    return _check_plan


def check_usage_limit(limit_type: str):
    """
    FastAPI dependency that checks usage limits.

    limit_type: "email" | "campaign" | "recipient"
    """
    async def _check(
        current_candidate: Candidate = Depends(get_current_candidate)
    ) -> Candidate:
        # Super admins bypass limits
        if current_candidate.role == UserRole.SUPER_ADMIN:
            return current_candidate

        if limit_type == "email":
            used = current_candidate.monthly_email_sent or 0
            limit = current_candidate.monthly_email_limit or 100
            label = "emails"
        elif limit_type == "campaign":
            used = current_candidate.monthly_campaigns_created or 0
            limit = current_candidate.monthly_campaign_limit or 3
            label = "campaigns"
        elif limit_type == "recipient":
            # Count from DB would be better, but use limit field for now
            used = 0  # Checked at endpoint level
            limit = current_candidate.monthly_recipient_limit or 100
            label = "recipients"
            return current_candidate
        else:
            return current_candidate

        if used >= limit:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "limit_reached",
                    "message": f"Monthly {label} limit reached ({used}/{limit})",
                    "limit_type": limit_type,
                    "used": used,
                    "limit": limit,
                    "upgrade_url": "/settings/plan",
                }
            )
        return current_candidate
    return _check


# ============= SSE TICKET SYSTEM =============
# Short-lived, single-use tickets for EventSource connections
# (EventSource API doesn't support Authorization headers)

_sse_tickets: dict[str, tuple[str, datetime]] = {}  # ticket -> (username, expiry)
_sse_lock = threading.Lock()

SSE_TICKET_TTL_SECONDS = 30


def create_sse_ticket(username: str) -> str:
    """Create a short-lived, single-use ticket for SSE connections."""
    ticket = secrets.token_urlsafe(32)
    expiry = utc_now() + timedelta(seconds=SSE_TICKET_TTL_SECONDS)
    with _sse_lock:
        _sse_tickets[ticket] = (username, expiry)
        # Cleanup expired tickets opportunistically
        now = utc_now()
        expired = [t for t, (_, exp) in _sse_tickets.items() if now > exp]
        for t in expired:
            del _sse_tickets[t]
    return ticket


def validate_sse_ticket(ticket: str) -> Optional[str]:
    """Validate and consume an SSE ticket. Returns username if valid."""
    with _sse_lock:
        entry = _sse_tickets.pop(ticket, None)
    if not entry:
        return None
    username, expiry = entry
    if utc_now() > expiry:
        return None
    return username


async def get_current_candidate_from_sse_ticket(
    ticket: str = Query(None, alias="ticket"),
    db: Session = Depends(get_database_session)
) -> Candidate:
    """Authenticate SSE connections using a short-lived ticket instead of JWT in URL."""
    if not ticket:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="SSE ticket required",
        )

    username = validate_sse_ticket(ticket)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired SSE ticket",
        )

    candidate = db.query(Candidate).filter(
        Candidate.username == username,
        Candidate.deleted_at.is_(None),
    ).first()

    if not candidate or not candidate.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
        )

    return candidate
