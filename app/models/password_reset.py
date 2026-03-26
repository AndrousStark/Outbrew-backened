"""Password Reset Token Model — DB-backed reset tokens (replaces in-memory dict)."""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index
from sqlalchemy.sql import func

from app.core.database import Base


class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)
    token_hash = Column(String(128), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_reset_user_id", "user_id"),
        Index("idx_reset_token_hash", "token_hash"),
    )

    def __repr__(self) -> str:
        return f"<PasswordResetToken user={self.user_id} used={self.used_at is not None}>"
