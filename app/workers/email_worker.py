"""
Email Worker for Background Email Sending (Phase 3)

This worker handles email sending asynchronously, preventing
API endpoints from blocking while emails are sent.

PERFORMANCE IMPACT:
- API endpoints return instantly
- Emails sent in background
- Retry logic for failures
- Better user experience
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_TIMEOUT = 30  # seconds

logger = logging.getLogger(__name__)


async def send_email_task(
    ctx: Dict,
    to_email: str,
    subject: str,
    body_html: str,
    from_email: str,
    from_name: str = "Outbrew",
    smtp_host: str = "smtp.gmail.com",
    smtp_port: int = 587,
    application_id: Optional[int] = None,
    candidate_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Send email asynchronously in background.

    SMTP password is looked up from the database using candidate_id,
    NOT passed through Redis job args (security improvement).

    Args:
        ctx: ARQ context
        to_email: Recipient email
        subject: Email subject
        body_html: HTML email body
        from_email: Sender email
        from_name: Sender display name
        smtp_host: SMTP server
        smtp_port: SMTP port
        application_id: Optional application ID (for tracking)
        candidate_id: Optional candidate ID (for tracking)

    Returns:
        Dict with status and details
    """
    logger.info(f"[EMAIL-WORKER] Sending email to {to_email}")

    try:
        # Look up SMTP password from database using candidate_id
        from_password = ""
        if candidate_id:
            try:
                from app.core.database import SessionLocal
                from app.models.candidate import Candidate
                from app.core.encryption import decrypt_value
                db = SessionLocal()
                try:
                    candidate = db.query(Candidate).filter(Candidate.id == candidate_id).first()
                    if candidate and candidate.email_password:
                        from_password = decrypt_value(candidate.email_password)
                finally:
                    db.close()
            except Exception as cred_err:
                logger.error(f"[EMAIL-WORKER] Failed to load credentials for candidate {candidate_id}: {cred_err}")
                return {"status": "failed", "error": "Could not load SMTP credentials", "to_email": to_email}

        if not from_password:
            logger.error(f"[EMAIL-WORKER] No SMTP password available for {from_email}")
            return {"status": "failed", "error": "No SMTP credentials configured", "to_email": to_email}

        # Create message
        message = MIMEMultipart("alternative")
        message["From"] = f"{from_name} <{from_email}>"
        message["To"] = to_email
        message["Subject"] = subject

        # Attach HTML body
        html_part = MIMEText(body_html, "html")
        message.attach(html_part)

        # Connect to SMTP server with proper SSL context and timeout
        ssl_context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port, timeout=SMTP_TIMEOUT) as server:
            server.starttls(context=ssl_context)
            server.login(from_email, from_password)
            server.send_message(message)

        logger.info(f"✅ [EMAIL-WORKER] Email sent successfully to {to_email}")

        # TODO: Log to email_logs table
        # async with get_async_db() as db:
        #     from app.models.email_log import EmailLog, EmailStatusEnum
        #     log = EmailLog(
        #         candidate_id=candidate_id,
        #         application_id=application_id,
        #         from_email=from_email,
        #         to_email=to_email,
        #         subject=subject,
        #         status=EmailStatusEnum.SENT,
        #         sent_at=datetime.now(timezone.utc)
        #     )
        #     db.add(log)
        #     await db.commit()

        return {
            "status": "success",
            "to_email": to_email,
            "subject": subject,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "application_id": application_id,
            "candidate_id": candidate_id
        }

    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"❌ [EMAIL-WORKER] Authentication failed: {e}")
        return {
            "status": "failed",
            "error": "Authentication failed",
            "to_email": to_email
        }

    except smtplib.SMTPException as e:
        logger.error(f"❌ [EMAIL-WORKER] SMTP error: {e}")
        return {
            "status": "failed",
            "error": str(e),
            "to_email": to_email
        }

    except Exception as e:
        logger.error(f"❌ [EMAIL-WORKER] Unexpected error: {e}")
        return {
            "status": "failed",
            "error": str(e),
            "to_email": to_email
        }


async def send_bulk_emails_task(
    ctx: Dict,
    emails: List[Dict[str, Any]],
    from_email: str,
    from_name: str = "Outbrew",
    smtp_host: str = "smtp.gmail.com",
    smtp_port: int = 587,
    candidate_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Send multiple emails in background (bulk operation).

    Args:
        ctx: ARQ context
        emails: List of email dicts with 'to_email', 'subject', 'body_html'
        from_email: Sender email
        from_name: Sender display name
        smtp_host: SMTP server
        smtp_port: SMTP port
        candidate_id: Candidate ID for credential lookup

    Returns:
        Dict with success/failure counts
    """
    logger.info(f"📧 [EMAIL-WORKER] Sending {len(emails)} emails in bulk")

    results = {
        "total": len(emails),
        "success": 0,
        "failed": 0,
        "errors": []
    }

    for email_data in emails:
        try:
            result = await send_email_task(
                ctx,
                to_email=email_data["to_email"],
                subject=email_data["subject"],
                body_html=email_data["body_html"],
                from_email=from_email,
                from_name=from_name,
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                application_id=email_data.get("application_id"),
                candidate_id=candidate_id or email_data.get("candidate_id")
            )

            if result["status"] == "success":
                results["success"] += 1
            else:
                results["failed"] += 1
                results["errors"].append({
                    "to_email": email_data["to_email"],
                    "error": result.get("error")
                })

        except Exception as e:
            results["failed"] += 1
            results["errors"].append({
                "to_email": email_data.get("to_email", "unknown"),
                "error": str(e)
            })

    logger.info(
        f"✅ [EMAIL-WORKER] Bulk email complete: "
        f"{results['success']} sent, {results['failed']} failed"
    )

    return results


async def send_scheduled_email_task(
    ctx: Dict,
    scheduled_email_id: int
) -> Dict[str, Any]:
    """
    Send a scheduled email (for scheduled email feature).

    Args:
        ctx: ARQ context
        scheduled_email_id: ID of scheduled email

    Returns:
        Dict with status
    """
    logger.info(f"📧 [EMAIL-WORKER] Sending scheduled email {scheduled_email_id}")

    logger.warning(f"[EMAIL-WORKER] send_scheduled_email_task not implemented for ID {scheduled_email_id}")
    return {"status": "not_implemented", "scheduled_email_id": scheduled_email_id, "error": "Scheduled email sending is not yet implemented"}


async def send_follow_up_email_task(
    ctx: Dict,
    follow_up_id: int
) -> Dict[str, Any]:
    """
    Send a follow-up email (for follow-up feature).

    Args:
        ctx: ARQ context
        follow_up_id: ID of follow-up

    Returns:
        Dict with status
    """
    logger.info(f"📧 [EMAIL-WORKER] Sending follow-up email {follow_up_id}")

    logger.warning(f"[EMAIL-WORKER] send_follow_up_email_task not implemented for ID {follow_up_id}")
    return {"status": "not_implemented", "follow_up_id": follow_up_id, "error": "Follow-up email sending is not yet implemented"}
