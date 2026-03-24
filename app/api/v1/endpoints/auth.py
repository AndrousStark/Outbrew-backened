"""Authentication Endpoints with Rate Limiting Protection"""
from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Request
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
)
from app.core.config import settings

# Token expiry from settings
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_DAYS = settings.REFRESH_TOKEN_EXPIRE_DAYS
from app.core.rate_limiter import (
    limiter,
    AUTH_LOGIN_LIMIT,
    AUTH_PASSWORD_CHANGE_LIMIT,
    AUTH_REGISTER_LIMIT
)
from app.core.encryption import encrypt_value
from app.core.logger import auth_logger as logger, log_audit_event
from app.models.candidate import Candidate, UserRole
from app.schemas.auth import (
    Token,
    LoginRequest,
    RegisterRequest,
    UserResponse,
    ChangePasswordRequest,
    RefreshTokenRequest
)
from app.api.dependencies import get_current_candidate

router = APIRouter()


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
@limiter.limit(AUTH_REGISTER_LIMIT)
def register(
    request: Request,
    register_data: RegisterRequest,
    db: Session = Depends(get_database_session)
):
    """
    Register a new user (candidate)

    Note: By default, new users get 'pragya' role. Only Super Admin can create other Super Admins.
    """
    # Check if username already exists
    if db.query(Candidate).filter(Candidate.username == register_data.username).first():
        log_audit_event("registration_failed", username=register_data.username,
                       details={"reason": "username_exists"}, success=False)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )

    # Check if email already exists
    if db.query(Candidate).filter(Candidate.email == register_data.email).first():
        log_audit_event("registration_failed", username=register_data.username,
                       details={"reason": "email_exists"}, success=False)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Encrypt email password for secure storage
    encrypted_email_password = encrypt_value(register_data.email_password)

    # Create new candidate with default role
    candidate = Candidate(
        username=register_data.username,
        email=register_data.email,
        hashed_password=get_password_hash(register_data.password),
        full_name=register_data.full_name,
        role=UserRole.PRAGYA,  # Default role
        email_account=register_data.email_account,
        email_password=encrypted_email_password,  # Store encrypted
        smtp_host=register_data.smtp_host,
        smtp_port=register_data.smtp_port,
        title=register_data.title,
        is_active=True
    )

    try:
        db.add(candidate)
        db.commit()
        db.refresh(candidate)
        log_audit_event("user_registered", user_id=candidate.id, username=candidate.username,
                       details={"email": candidate.email, "role": candidate.role.value})
        logger.info(f"New user registered: {candidate.username} (ID: {candidate.id})")
    except Exception as e:
        db.rollback()
        log_audit_event("registration_failed", username=register_data.username,
                       details={"reason": "database_error", "error": str(e)}, success=False)
        logger.error(f"Failed to register user {register_data.username}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user account"
        )

    return candidate


@router.post("/login", response_model=Token)
@limiter.limit(AUTH_LOGIN_LIMIT)
def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_database_session)
):
    """
    Login with username and password to get JWT token

    Rate Limited: 5 login attempts per minute per IP (brute-force protection).
    OAuth2 compatible token endpoint.
    """
    candidate = authenticate_candidate(db, form_data.username, form_data.password)

    if not candidate:
        log_audit_event("login_failed", username=form_data.username,
                       details={"reason": "invalid_credentials", "method": "oauth2"}, success=False)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not candidate.is_active:
        log_audit_event("login_failed", user_id=candidate.id, username=candidate.username,
                       details={"reason": "inactive_account", "method": "oauth2"}, success=False)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user account"
        )

    # Create access and refresh tokens
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": candidate.username, "role": candidate.role.value},
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(
        data={"sub": candidate.username}
    )

    log_audit_event("login_success", user_id=candidate.id, username=candidate.username,
                   details={"role": candidate.role.value, "method": "oauth2"})
    logger.info(f"User logged in: {candidate.username} (role: {candidate.role.value})")
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }


@router.post("/login/json", response_model=Token)
@limiter.limit(AUTH_LOGIN_LIMIT)
def login_json(
    request: Request,
    login_data: LoginRequest,
    db: Session = Depends(get_database_session)
):
    """
    Login with JSON payload (alternative to OAuth2 form)

    Rate Limited: 5 login attempts per minute per IP (brute-force protection).
    Returns JWT access token and refresh token.
    """
    candidate = authenticate_candidate(db, login_data.username, login_data.password)

    if not candidate:
        log_audit_event("login_failed", username=login_data.username,
                       details={"reason": "invalid_credentials", "method": "json"}, success=False)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )

    if not candidate.is_active:
        log_audit_event("login_failed", user_id=candidate.id, username=candidate.username,
                       details={"reason": "inactive_account", "method": "json"}, success=False)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user account"
        )

    # Create access and refresh tokens
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": candidate.username, "role": candidate.role.value},
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(
        data={"sub": candidate.username}
    )

    log_audit_event("login_success", user_id=candidate.id, username=candidate.username,
                   details={"role": candidate.role.value, "method": "json"})
    logger.info(f"User logged in (JSON): {candidate.username} (role: {candidate.role.value})")
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }


@router.post("/refresh", response_model=Token)
def refresh_access_token(
    request: Request,
    refresh_data: RefreshTokenRequest,
    db: Session = Depends(get_database_session)
):
    """
    Get a new access token using a refresh token.

    Use this endpoint when the access token expires.
    The refresh token remains valid for 7 days.
    """
    # Verify refresh token
    username = verify_refresh_token(refresh_data.refresh_token)

    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Get the candidate
    candidate = db.query(Candidate).filter(
        Candidate.username == username,
        Candidate.deleted_at.is_(None)
    ).first()

    if not candidate:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    if not candidate.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user account"
        )

    # Create new access token
    access_token = create_access_token(
        data={"sub": candidate.username, "role": candidate.role.value}
    )

    logger.info(f"[Auth] Token refreshed for: {candidate.username}")
    return {
        "access_token": access_token,
        "refresh_token": refresh_data.refresh_token,  # Return same refresh token
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }


@router.get("/me", response_model=UserResponse)
def get_current_user(
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get current logged-in user information"""
    return current_candidate


@router.post("/change-password", response_model=UserResponse)
@limiter.limit(AUTH_PASSWORD_CHANGE_LIMIT)
def change_password(
    request: Request,
    password_data: ChangePasswordRequest,
    current_candidate: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_database_session)
):
    """
    Change current user's password

    Rate Limited: 3 password changes per hour per IP (security protection).
    """
    # Verify current password
    if not verify_password(password_data.current_password, current_candidate.hashed_password):
        log_audit_event("password_change_failed", user_id=current_candidate.id,
                       username=current_candidate.username,
                       details={"reason": "invalid_current_password"}, success=False)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect current password"
        )

    # Update password
    current_candidate.hashed_password = get_password_hash(password_data.new_password)

    try:
        db.commit()
        db.refresh(current_candidate)
        log_audit_event("password_changed", user_id=current_candidate.id,
                       username=current_candidate.username)
        logger.info(f"Password changed for user: {current_candidate.username}")
    except Exception as e:
        db.rollback()
        log_audit_event("password_change_failed", user_id=current_candidate.id,
                       username=current_candidate.username,
                       details={"reason": "database_error", "error": str(e)}, success=False)
        logger.error(f"Failed to change password for user {current_candidate.username}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update password"
        )

    return current_candidate


@router.post("/sse-ticket")
def get_sse_ticket(
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get a short-lived, single-use ticket for SSE connections.

    EventSource API doesn't support Authorization headers,
    so this ticket replaces putting JWT tokens in URL query params.
    The ticket expires in 30 seconds and can only be used once.
    """
    ticket = create_sse_ticket(current_candidate.username)
    return {"ticket": ticket}
