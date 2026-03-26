"""User Session Model — Tracks active login sessions for device management and security."""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Index
from sqlalchemy.sql import func

from app.core.database import Base


class UserSession(Base):
    __tablename__ = "user_sessions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)
    token_jti = Column(String(64), nullable=False, unique=True)
    refresh_jti = Column(String(64), nullable=True)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(512), nullable=True)
    device_info = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_active_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    is_current = Column(Boolean, default=False, server_default="false")

    __table_args__ = (
        Index("idx_sessions_user_id", "user_id"),
        Index("idx_sessions_jti", "token_jti"),
        Index("idx_sessions_active", "user_id", "revoked_at"),
    )

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None

    def __repr__(self) -> str:
        return f"<UserSession user={self.user_id} ip={self.ip_address} active={self.is_active}>"
