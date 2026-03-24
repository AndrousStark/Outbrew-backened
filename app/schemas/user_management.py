"""User Management Schemas (Super Admin)"""
from pydantic import BaseModel, Field, EmailStr
from typing import Optional
from datetime import datetime
from app.models.candidate import UserRole


class UserCreateAdmin(BaseModel):
    """Create user (Super Admin only)"""
    username: str = Field(..., min_length=3, max_length=100)
    email: EmailStr
    password: str = Field(..., min_length=6)
    full_name: str = Field(..., min_length=1, max_length=255)
    role: UserRole = Field(default=UserRole.PRAGYA, description="User role")
    email_account: EmailStr
    email_password: str
    smtp_host: str = Field(default="smtp.gmail.com")
    smtp_port: int = Field(default=587)
    title: Optional[str] = Field(None, max_length=255)
    is_active: bool = Field(default=True)


class UserUpdateAdmin(BaseModel):
    """Update user (Super Admin only)"""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = Field(None, min_length=1, max_length=255)
    role: Optional[UserRole] = None
    email_account: Optional[EmailStr] = None
    email_password: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    title: Optional[str] = Field(None, max_length=255)
    is_active: Optional[bool] = None
    password: Optional[str] = Field(None, min_length=6, description="New password (optional)")


class UserDetailResponse(BaseModel):
    """Detailed user information (Super Admin view)"""
    id: int
    username: str
    email: str
    full_name: str
    role: UserRole
    email_account: str
    smtp_host: str
    smtp_port: int
    title: Optional[str] = None
    is_active: bool
    total_applications_sent: int = 0
    total_responses_received: int = 0
    response_rate: float = 0.0
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class UserListResponse(BaseModel):
    """List of users"""
    total: int
    items: list[UserDetailResponse]
