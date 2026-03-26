"""Audit Log Model — Persistent, queryable audit trail for all security-relevant events."""
from sqlalchemy import Column, Integer, String, DateTime, Boolean, JSON, ForeignKey, Index
from sqlalchemy.sql import func

from app.core.database import Base


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    event_type = Column(String(50), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("candidates.id", ondelete="SET NULL"), nullable=True)
    username = Column(String(100), nullable=True)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(512), nullable=True)
    details = Column(JSON, nullable=True)
    success = Column(Boolean, default=True, nullable=False)

    __table_args__ = (
        Index("idx_audit_logs_user_id", "user_id"),
        Index("idx_audit_logs_event_type", "event_type"),
        Index("idx_audit_logs_timestamp", "timestamp"),
        Index("idx_audit_logs_user_event", "user_id", "event_type"),
    )

    def __repr__(self) -> str:
        return f"<AuditLog {self.event_type} user={self.username} at={self.timestamp}>"
