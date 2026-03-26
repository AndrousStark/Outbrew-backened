"""Authentication Endpoints with Rate Limiting Protection

Endpoints:
- POST /register — Create new account (email verification sent)
- POST /login — OAuth2 form login
- POST /login/json — JSON login
- POST /logout — Blacklist current tokens
- POST /refresh — Refresh access token
- GET  /me — Get current user with plan + usage
- POST /change-password — Change password
- POST /forgot-password — Request password reset
- POST /reset-password — Reset password with token
- GET  /verify-email — Verify email with token
- POST /resend-verification — Resend verification email
- POST /sse-ticket — Get short-lived SSE ticket
"""
import secrets
import time
from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from app.core.database import get_database_session
from app.core.auth import (
    authenticate_candidate,
    create_access_token,
    create_refresh_token,
    create_token_pair,
    create_sse_ticket,
    verify_refresh_token,
    get_password_hash,
    verify_password,
    blacklist_token,
)
from app.core.config import settings

ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_DAYS = settings.REFRESH_TOKEN_EXPIRE_DAYS

from app.core.rate_limiter import (
    limiter,
    AUTH_LOGIN_LIMIT,
    AUTH_PASSWORD_CHANGE_LIMIT,
    AUTH_REGISTER_LIMIT
)
from app.core.encryption import encrypt_value
from app.core.logger import auth_logger as logger
from app.services.audit_service import log_audit, get_client_ip
from app.models.candidate import Candidate, UserRole, PlanTier
from app.schemas.auth import (
    Token,
    LoginRequest,
    RegisterRequest,
    UserResponse,
    UsageStats,
    ChangePasswordRequest,
    RefreshTokenRequest,
    ForgotPasswordRequest,
    ResetPasswordRequest,
    LogoutRequest,
)
from app.api.dependencies import get_current_candidate

router = APIRouter()


# ===================== HELPERS =====================

def _build_user_response(candidate: Candidate) -> dict:
    """Build UserResponse dict with plan + usage data."""
    raw_plan = getattr(candidate, "plan_tier", None) or "free"
    plan_value = raw_plan.value if hasattr(raw_plan, "value") else str(raw_plan).lower()

    return {
        "id": candidate.id,
        "username": candidate.username,
        "email": candidate.email,
        "full_name": candidate.full_name,
        "role": candidate.role,
        "email_account": candidate.email_account,
        "title": candidate.title,
        "is_active": candidate.is_active,
        "email_verified": getattr(candidate, "email_verified", False) or False,
        "plan_tier": plan_value,
        "usage": {
            "monthly_email_sent": getattr(candidate, "monthly_email_sent", 0) or 0,
            "monthly_email_limit": getattr(candidate, "monthly_email_limit", 100) or 100,
            "monthly_campaigns_created": getattr(candidate, "monthly_campaigns_created", 0) or 0,
            "monthly_campaign_limit": getattr(candidate, "monthly_campaign_limit", 3) or 3,
            "monthly_recipient_limit": getattr(candidate, "monthly_recipient_limit", 100) or 100,
        },
        "total_applications_sent": candidate.total_applications_sent or 0,
        "total_responses_received": candidate.total_responses_received or 0,
        "response_rate": candidate.response_rate or 0.0,
    }


# ===================== REGISTER =====================

@router.post("/register", status_code=status.HTTP_201_CREATED)
@limiter.limit(AUTH_REGISTER_LIMIT)
def register(
    request: Request,
    register_data: RegisterRequest,
    db: Session = Depends(get_database_session)
):
    """Register a new user. Email verification token is generated."""
    # Check if username already exists
    if db.query(Candidate).filter(Candidate.username == register_data.username).first():
        log_audit("registration_failed", username=register_data.username,
                       details={"reason": "username_exists"}, success=False)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already registered")

    # Check if email already exists
    if db.query(Candidate).filter(Candidate.email == register_data.email).first():
        log_audit("registration_failed", username=register_data.username,
                       details={"reason": "email_exists"}, success=False)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")

    # Encrypt email password for secure storage
    encrypted_email_password = encrypt_value(register_data.email_password)

    # Generate email verification token
    verification_token = secrets.token_urlsafe(32)

    # Create new candidate
    candidate = Candidate(
        username=register_data.username,
        email=register_data.email,
        hashed_password=get_password_hash(register_data.password),
        full_name=register_data.full_name,
        role=UserRole.PRAGYA,
        email_account=register_data.email_account,
        email_password=encrypted_email_password,
        smtp_host=register_data.smtp_host,
        smtp_port=register_data.smtp_port,
        title=register_data.title,
        is_active=True,
        email_verified=False,
        email_verification_token=verification_token,
        plan_tier=PlanTier.FREE,
    )

    try:
        db.add(candidate)
        db.commit()
        db.refresh(candidate)
        log_audit("user_registered", user_id=candidate.id, username=candidate.username,
                       details={"email": candidate.email, "role": candidate.role.value})
        logger.info(f"New user registered: {candidate.username} (ID: {candidate.id})")
    except Exception as e:
        db.rollback()
        log_audit("registration_failed", username=register_data.username,
                       details={"reason": "database_error", "error": str(e)}, success=False)
        logger.error(f"Failed to register user {register_data.username}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create user account")

    # TODO: Send verification email with link: /verify-email?token={verification_token}
    logger.info(f"Email verification token for {candidate.email}: {verification_token}")

    return _build_user_response(candidate)


# ===================== LOGIN =====================

def _do_login(candidate: Candidate, method: str, request: Request) -> dict:
    """Shared login logic for OAuth2 and JSON login."""
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": candidate.username, "role": candidate.role.value},
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(data={"sub": candidate.username})

    log_audit("login_success", user_id=candidate.id, username=candidate.username,
              ip_address=get_client_ip(request),
              user_agent=request.headers.get("User-Agent", "")[:512],
              details={"role": candidate.role.value, "method": method,
                       "email_verified": getattr(candidate, "email_verified", False)})
    logger.info(f"User logged in ({method}): {candidate.username}")

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }


def _validate_login(candidate, username: str, method: str, request: Request):
    """Shared validation for login endpoints."""
    if not candidate:
        log_audit("login_failed", username=username,
                  ip_address=get_client_ip(request),
                  details={"reason": "invalid_credentials", "method": method}, success=False)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    if not candidate.is_active:
        log_audit("login_failed", user_id=candidate.id, username=candidate.username,
                  ip_address=get_client_ip(request),
                  details={"reason": "inactive_account", "method": method}, success=False)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Inactive user account")


@router.post("/login", response_model=Token)
@limiter.limit(AUTH_LOGIN_LIMIT)
def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_database_session)
):
    """Login with username and password (OAuth2 form). Rate limited: 5/minute."""
    candidate = authenticate_candidate(db, form_data.username, form_data.password)
    _validate_login(candidate, form_data.username, "oauth2", request)
    return _do_login(candidate, "oauth2", request)


@router.post("/login/json", response_model=Token)
@limiter.limit(AUTH_LOGIN_LIMIT)
def login_json(
    request: Request,
    login_data: LoginRequest,
    db: Session = Depends(get_database_session)
):
    """Login with JSON payload. Rate limited: 5/minute."""
    candidate = authenticate_candidate(db, login_data.username, login_data.password)
    _validate_login(candidate, login_data.username, "json", request)
    return _do_login(candidate, "json", request)


# ===================== LOGOUT =====================

@router.post("/logout")
def logout(
    request: Request,
    body: LogoutRequest = None,
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Logout — blacklist current access token (and optionally refresh token).

    After logout, the access token cannot be used again even before expiry.
    """
    # Get the raw token from the Authorization header
    auth_header = request.headers.get("Authorization", "")
    access_token = auth_header.replace("Bearer ", "") if auth_header.startswith("Bearer ") else None

    blacklisted_access = False
    blacklisted_refresh = False

    if access_token:
        blacklisted_access = blacklist_token(access_token)

    if body and body.refresh_token:
        blacklisted_refresh = blacklist_token(body.refresh_token)

    log_audit("logout", user_id=current_candidate.id, username=current_candidate.username,
                   details={"access_blacklisted": blacklisted_access, "refresh_blacklisted": blacklisted_refresh})
    logger.info(f"User logged out: {current_candidate.username}")

    return {
        "success": True,
        "message": "Logged out successfully",
        "access_token_invalidated": blacklisted_access,
        "refresh_token_invalidated": blacklisted_refresh,
    }


# ===================== REFRESH =====================

@router.post("/refresh", response_model=Token)
def refresh_access_token(
    request: Request,
    refresh_data: RefreshTokenRequest,
    db: Session = Depends(get_database_session)
):
    """Get a new access token using a refresh token."""
    username = verify_refresh_token(refresh_data.refresh_token)
    if not username:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")

    candidate = db.query(Candidate).filter(
        Candidate.username == username, Candidate.deleted_at.is_(None)
    ).first()
    if not candidate:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    if not candidate.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Inactive user account")

    access_token = create_access_token(data={"sub": candidate.username, "role": candidate.role.value})
    logger.info(f"Token refreshed for: {candidate.username}")

    return {
        "access_token": access_token,
        "refresh_token": refresh_data.refresh_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }


# ===================== GET ME (with plan + usage) =====================

@router.get("/me")
def get_current_user(
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get current logged-in user information including plan tier and usage stats."""
    return _build_user_response(current_candidate)


# ===================== EMAIL VERIFICATION =====================

@router.get("/verify-email")
def verify_email(
    token: str = Query(..., description="Email verification token"),
    db: Session = Depends(get_database_session)
):
    """Verify email address using the token sent during registration."""
    candidate = db.query(Candidate).filter(
        Candidate.email_verification_token == token,
        Candidate.deleted_at.is_(None)
    ).first()

    if not candidate:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired verification token")

    if candidate.email_verified:
        return {"success": True, "message": "Email already verified. You can log in."}

    candidate.email_verified = True
    candidate.email_verification_token = None

    try:
        db.commit()
        log_audit("email_verified", user_id=candidate.id, username=candidate.username)
        logger.info(f"Email verified for: {candidate.username}")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to verify email: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to verify email")

    return {"success": True, "message": "Email verified successfully! You can now log in."}


@router.post("/resend-verification")
@limiter.limit("3/minute")
def resend_verification(
    request: Request,
    data: ForgotPasswordRequest,  # Reuse — just needs email field
    db: Session = Depends(get_database_session)
):
    """Resend email verification. Rate limited: 3/minute."""
    candidate = db.query(Candidate).filter(
        Candidate.email == data.email,
        Candidate.deleted_at.is_(None)
    ).first()

    if candidate and not candidate.email_verified:
        new_token = secrets.token_urlsafe(32)
        candidate.email_verification_token = new_token
        try:
            db.commit()
            # TODO: Send verification email
            logger.info(f"Resent verification token for {candidate.email}: {new_token}")
        except Exception:
            db.rollback()

    # Always return success (anti-enumeration)
    return {"success": True, "message": "If an unverified account exists, a new verification email has been sent."}


# ===================== CHANGE PASSWORD =====================

@router.post("/change-password")
@limiter.limit(AUTH_PASSWORD_CHANGE_LIMIT)
def change_password(
    request: Request,
    password_data: ChangePasswordRequest,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """Change current user's password. Rate limited: 3/hour."""
    if not verify_password(password_data.current_password, current_candidate.hashed_password):
        log_audit("password_change_failed", user_id=current_candidate.id,
                       username=current_candidate.username,
                       details={"reason": "invalid_current_password"}, success=False)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Incorrect current password")

    current_candidate.hashed_password = get_password_hash(password_data.new_password)

    try:
        db.commit()
        db.refresh(current_candidate)
        log_audit("password_changed", user_id=current_candidate.id, username=current_candidate.username)
        logger.info(f"Password changed for user: {current_candidate.username}")
    except Exception as e:
        db.rollback()
        log_audit("password_change_failed", user_id=current_candidate.id,
                       username=current_candidate.username,
                       details={"reason": "database_error", "error": str(e)}, success=False)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update password")

    return _build_user_response(current_candidate)


# ===================== SSE TICKET =====================

@router.post("/sse-ticket")
def get_sse_ticket(
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get a short-lived, single-use ticket for SSE connections (30s expiry)."""
    ticket = create_sse_ticket(current_candidate.username)
    return {"ticket": ticket}


# ===================== FORGOT / RESET PASSWORD =====================

_reset_tokens: dict = {}


@router.post("/forgot-password")
@limiter.limit("5/minute")
def forgot_password(
    request: Request,
    data: ForgotPasswordRequest,
    db: Session = Depends(get_database_session)
):
    """Request password reset. Always returns success (anti-enumeration)."""
    candidate = db.query(Candidate).filter(
        Candidate.email == data.email, Candidate.deleted_at.is_(None)
    ).first()

    if candidate:
        token = secrets.token_urlsafe(32)
        _reset_tokens[token] = {"email": candidate.email, "expires": time.time() + 3600}
        log_audit("password_reset_requested", user_id=candidate.id, username=candidate.username)
        # TODO: Send reset email with link containing the token
        logger.info(f"Password reset token generated for: {candidate.email}")

    return {"success": True, "message": "If an account with that email exists, a password reset link has been sent."}


@router.post("/reset-password")
@limiter.limit("5/minute")
def reset_password(
    request: Request,
    data: ResetPasswordRequest,
    db: Session = Depends(get_database_session)
):
    """Reset password using a reset token."""
    token_data = _reset_tokens.get(data.token)
    if not token_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired reset token")

    if time.time() > token_data["expires"]:
        _reset_tokens.pop(data.token, None)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Reset token has expired")

    candidate = db.query(Candidate).filter(
        Candidate.email == token_data["email"], Candidate.deleted_at.is_(None)
    ).first()
    if not candidate:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Account not found")

    candidate.hashed_password = get_password_hash(data.new_password)

    try:
        db.commit()
        _reset_tokens.pop(data.token, None)
        log_audit("password_reset_completed", user_id=candidate.id, username=candidate.username)
        logger.info(f"Password reset completed for: {candidate.username}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to reset password")

    return {"success": True, "message": "Password has been reset successfully. You can now log in."}
