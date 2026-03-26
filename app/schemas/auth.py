"""Authentication Schemas"""
import re
from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import Optional
from app.models.candidate import UserRole


def validate_password_strength(password: str) -> str:
    """Validate password meets strength requirements:
    - At least 8 characters
    - At least 1 letter
    - At least 1 number
    - At least 1 special character
    """
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters long")
    if not re.search(r"[A-Za-z]", password):
        raise ValueError("Password must contain at least one letter")
    if not re.search(r"[0-9]", password):
        raise ValueError("Password must contain at least one number")
    if not re.search(r"[^A-Za-z0-9]", password):
        raise ValueError("Password must contain at least one special character (!@#$%^&* etc.)")
    return password


class Token(BaseModel):
    """JWT token response with access and refresh tokens"""
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int = 1800  # 30 minutes in seconds


class TokenData(BaseModel):
    """Token payload data"""
    username: Optional[str] = None


class RefreshTokenRequest(BaseModel):
    """Refresh token request"""
    refresh_token: str = Field(..., description="The refresh token")


class LoginRequest(BaseModel):
    """Login request"""
    username: str = Field(..., min_length=3, max_length=100)
    password: str = Field(..., min_length=1)


class RegisterRequest(BaseModel):
    """User registration request"""
    username: str = Field(..., min_length=3, max_length=100, description="Unique username")
    email: EmailStr = Field(..., description="Email address")
    password: str = Field(..., description="Password (min 8 chars, 1 letter, 1 number, 1 special)")
    full_name: str = Field(..., min_length=1, max_length=255, description="Full name")
    email_account: EmailStr = Field(..., description="Email account for sending applications")
    email_password: str = Field(..., description="Email password or app password")
    smtp_host: str = Field(default="smtp.gmail.com", description="SMTP server host")
    smtp_port: int = Field(default=587, description="SMTP server port")
    title: Optional[str] = Field(None, max_length=255, description="Professional title")

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        return validate_password_strength(v)


class UsageStats(BaseModel):
    """Current usage statistics"""
    monthly_email_sent: int = 0
    monthly_email_limit: int = 100
    monthly_campaigns_created: int = 0
    monthly_campaign_limit: int = 3
    monthly_recipient_limit: int = 100


class UserResponse(BaseModel):
    """User information response — includes plan and usage"""
    id: int
    username: str
    email: str
    full_name: str
    role: UserRole
    email_account: str
    title: Optional[str] = None
    is_active: bool
    email_verified: bool = False
    plan_tier: str = "free"
    usage: Optional[UsageStats] = None
    total_applications_sent: int = 0
    total_responses_received: int = 0
    response_rate: float = 0.0

    class Config:
        from_attributes = True


class ChangePasswordRequest(BaseModel):
    """Change password request"""
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., description="New password (min 8 chars, 1 letter, 1 number, 1 special)")

    @field_validator("new_password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        return validate_password_strength(v)


class ForgotPasswordRequest(BaseModel):
    """Request password reset"""
    email: EmailStr = Field(..., description="Email address associated with the account")


class ResetPasswordRequest(BaseModel):
    """Reset password with token"""
    token: str = Field(..., description="Password reset token from email")
    new_password: str = Field(..., description="New password (min 8 chars, 1 letter, 1 number, 1 special)")

    @field_validator("new_password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        return validate_password_strength(v)


class LogoutRequest(BaseModel):
    """Logout request — optionally include refresh token to blacklist it too"""
    refresh_token: Optional[str] = Field(None, description="Refresh token to also invalidate")
