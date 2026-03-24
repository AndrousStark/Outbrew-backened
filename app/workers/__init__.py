"""
Background Workers for Phase 3 Optimization

ARQ (Async Redis Queue) workers for:
- Email sending
- Data processing
- Scheduled tasks
- Report generation
"""

from app.workers.email_worker import send_email_task, send_bulk_emails_task
from app.workers.data_worker import refresh_materialized_views_task, cleanup_old_records_task
from app.workers.notification_worker import send_notification_task

__all__ = [
    "send_email_task",
    "send_bulk_emails_task",
    "refresh_materialized_views_task",
    "cleanup_old_records_task",
    "send_notification_task",
]
