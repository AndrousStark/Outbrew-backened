"""Authentication Schemas"""
from pydantic import BaseModel, Field, EmailStr
from typing import Optional
from app.models.candidate import UserRole


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
    password: str = Field(..., min_length=6)


class RegisterRequest(BaseModel):
    """User registration request"""
    username: str = Field(..., min_length=3, max_length=100, description="Unique username")
    email: EmailStr = Field(..., description="Email address")
    password: str = Field(..., min_length=6, description="Password (min 6 characters)")
    full_name: str = Field(..., min_length=1, max_length=255, description="Full name")
    email_account: EmailStr = Field(..., description="Email account for sending applications")
    email_password: str = Field(..., description="Email password or app password")
    smtp_host: str = Field(default="smtp.gmail.com", description="SMTP server host")
    smtp_port: int = Field(default=587, description="SMTP server port")
    title: Optional[str] = Field(None, max_length=255, description="Professional title")


class UserResponse(BaseModel):
    """User information response"""
    id: int
    username: str
    email: str
    full_name: str
    role: UserRole
    email_account: str
    title: Optional[str] = None
    is_active: bool
    total_applications_sent: int = 0
    total_responses_received: int = 0
    response_rate: float = 0.0

    class Config:
        from_attributes = True


class ChangePasswordRequest(BaseModel):
    """Change password request"""
    current_password: str = Field(..., min_length=6)
    new_password: str = Field(..., min_length=6)
