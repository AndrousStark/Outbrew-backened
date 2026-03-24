"""User Management Endpoints (Super Admin Only)"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.database import get_database_session
from app.core.auth import get_password_hash
from app.core.encryption import encrypt_value
from app.models.candidate import Candidate, UserRole
from app.schemas.user_management import (
    UserCreateAdmin,
    UserUpdateAdmin,
    UserDetailResponse,
    UserListResponse
)
from app.api.dependencies import require_super_admin
from datetime import timezone

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/", response_model=UserDetailResponse, status_code=status.HTTP_201_CREATED)
def create_user(
    user_data: UserCreateAdmin,
    db: Session = Depends(get_database_session),
    _: Candidate = Depends(require_super_admin)  # Super Admin only
):
    """
    Create a new user (Super Admin only)

    Super Admin can create users with any role, including other Super Admins
    """
    # Check if username already exists
    if db.query(Candidate).filter(Candidate.username == user_data.username).first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )

    # Check if email already exists
    if db.query(Candidate).filter(Candidate.email == user_data.email).first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Encrypt email password for secure storage
    encrypted_email_password = encrypt_value(user_data.email_password)

    # Create new user
    candidate = Candidate(
        username=user_data.username,
        email=user_data.email,
        hashed_password=get_password_hash(user_data.password),
        full_name=user_data.full_name,
        role=user_data.role,
        email_account=user_data.email_account,
        email_password=encrypted_email_password,  # Store encrypted
        smtp_host=user_data.smtp_host,
        smtp_port=user_data.smtp_port,
        title=user_data.title,
        is_active=user_data.is_active
    )

    try:
        db.add(candidate)
        db.commit()
        db.refresh(candidate)
        logger.info(f"[Users] Super Admin created new user: {candidate.username} with role {candidate.role} (ID: {candidate.id})")
    except Exception as e:
        db.rollback()
        logger.error(f"[Users] Failed to create user {user_data.username}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user"
        )

    return candidate


@router.get("/", response_model=UserListResponse)
def list_users(
    skip: int = 0,
    limit: int = 100,
    role: Optional[UserRole] = None,
    is_active: Optional[bool] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_database_session),
    _: Candidate = Depends(require_super_admin)  # Super Admin only
):
    """
    List all users (Super Admin only)

    Supports filtering by role, active status, and search by name/email/username
    """
    query = db.query(Candidate).filter(Candidate.deleted_at.is_(None))

    # Apply filters
    if role:
        query = query.filter(Candidate.role == role)

    if is_active is not None:
        query = query.filter(Candidate.is_active == is_active)

    if search:
        search_term = f"%{search}%"
        query = query.filter(
            (Candidate.username.ilike(search_term)) |
            (Candidate.email.ilike(search_term)) |
            (Candidate.full_name.ilike(search_term))
        )

    total = query.count()
    items = query.order_by(Candidate.created_at.desc()).offset(skip).limit(limit).all()

    return UserListResponse(total=total, items=items)


@router.get("/{user_id}", response_model=UserDetailResponse)
def get_user(
    user_id: int,
    db: Session = Depends(get_database_session),
    _: Candidate = Depends(require_super_admin)  # Super Admin only
):
    """Get detailed user information (Super Admin only)"""
    user = db.query(Candidate).filter(
        Candidate.id == user_id,
        Candidate.deleted_at.is_(None)
    ).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    return user


@router.patch("/{user_id}", response_model=UserDetailResponse)
def update_user(
    user_id: int,
    user_update: UserUpdateAdmin,
    db: Session = Depends(get_database_session),
    current_admin: Candidate = Depends(require_super_admin)  # Super Admin only
):
    """
    Update user information (Super Admin only)

    Can update role, active status, and all user fields
    """
    user = db.query(Candidate).filter(
        Candidate.id == user_id,
        Candidate.deleted_at.is_(None)
    ).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Prevent Super Admin from deactivating themselves
    if user.id == current_admin.id and user_update.is_active is False:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot deactivate your own account"
        )

    # Prevent Super Admin from changing their own role
    if user.id == current_admin.id and user_update.role and user_update.role != UserRole.SUPER_ADMIN:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot change your own role"
        )

    # Check email uniqueness if being updated
    if user_update.email and user_update.email != user.email:
        if db.query(Candidate).filter(Candidate.email == user_update.email).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use"
            )

    # Update fields
    update_data = user_update.model_dump(exclude_unset=True)

    # Hash password if provided
    if "password" in update_data and update_data["password"]:
        update_data["hashed_password"] = get_password_hash(update_data.pop("password"))

    # Encrypt email password if provided
    if "email_password" in update_data and update_data["email_password"]:
        update_data["email_password"] = encrypt_value(update_data["email_password"])

    for field, value in update_data.items():
        setattr(user, field, value)

    try:
        db.commit()
        db.refresh(user)
        logger.info(f"[Users] Super Admin updated user {user_id}: {user.username}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Users] Failed to update user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user"
        )

    return user


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    user_id: int,
    db: Session = Depends(get_database_session),
    current_admin: Candidate = Depends(require_super_admin)  # Super Admin only
):
    """
    Soft delete a user (Super Admin only)

    Users are soft-deleted (deleted_at timestamp) to preserve data integrity
    """
    user = db.query(Candidate).filter(
        Candidate.id == user_id,
        Candidate.deleted_at.is_(None)
    ).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Prevent Super Admin from deleting themselves
    if user.id == current_admin.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )

    # Soft delete
    from datetime import datetime
    user.deleted_at = datetime.now(timezone.utc)

    try:
        db.commit()
        logger.info(f"[Users] Super Admin soft-deleted user {user_id}: {user.username}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Users] Failed to delete user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete user"
        )

    return None


@router.post("/{user_id}/activate", response_model=UserDetailResponse)
def activate_user(
    user_id: int,
    db: Session = Depends(get_database_session),
    _: Candidate = Depends(require_super_admin)  # Super Admin only
):
    """Activate a user account (Super Admin only)"""
    user = db.query(Candidate).filter(
        Candidate.id == user_id,
        Candidate.deleted_at.is_(None)
    ).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    user.is_active = True

    try:
        db.commit()
        db.refresh(user)
        logger.info(f"[Users] Super Admin activated user {user_id}: {user.username}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Users] Failed to activate user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to activate user"
        )

    return user


@router.post("/{user_id}/deactivate", response_model=UserDetailResponse)
def deactivate_user(
    user_id: int,
    db: Session = Depends(get_database_session),
    current_admin: Candidate = Depends(require_super_admin)  # Super Admin only
):
    """Deactivate a user account (Super Admin only)"""
    user = db.query(Candidate).filter(
        Candidate.id == user_id,
        Candidate.deleted_at.is_(None)
    ).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Prevent Super Admin from deactivating themselves
    if user.id == current_admin.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot deactivate your own account"
        )

    user.is_active = False

    try:
        db.commit()
        db.refresh(user)
        logger.info(f"[Users] Super Admin deactivated user {user_id}: {user.username}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Users] Failed to deactivate user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to deactivate user"
        )

    return user
