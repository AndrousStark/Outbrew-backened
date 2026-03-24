"""Email Tracking Endpoints

Handles open tracking, click tracking, bounce webhooks, and unsubscribe (RFC 8058).
"""
import base64
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response
from sqlalchemy.orm import Session

from urllib.parse import urlparse

from app.core.config import settings
from app.core.database import get_database_session
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.email_log import EmailLog, EmailStatusEnum
from app.models.group_campaign_recipient import GroupCampaignRecipient, RecipientStatusEnum
from app.models.recipient import Recipient

logger = logging.getLogger(__name__)

router = APIRouter()

# 1x1 transparent GIF pixel (43 bytes)
TRACKING_PIXEL = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)


def _get_email_log_by_tracking_id(db: Session, tracking_id: str) -> Optional[EmailLog]:
    """Fetch email log by tracking ID."""
    return db.query(EmailLog).filter(
        EmailLog.tracking_id == tracking_id
    ).first()


@router.get("/open/{tracking_id}")
def track_open(
    tracking_id: str,
    db: Session = Depends(get_database_session)
):
    """
    Track email open via 1x1 transparent pixel.

    Returns a 1x1 transparent GIF and records the open event.
    """
    try:
        email_log = _get_email_log_by_tracking_id(db, tracking_id)
        if email_log and not email_log.opened:
            email_log.opened = True
            email_log.opened_at = datetime.now(timezone.utc)

            # Also update campaign recipient if linked
            if email_log.campaign_id:
                campaign_recipient = db.query(GroupCampaignRecipient).filter(
                    GroupCampaignRecipient.tracking_id == tracking_id
                ).first()
                if campaign_recipient:
                    campaign_recipient.opened_at = datetime.now(timezone.utc)

            db.commit()
            logger.info(f"[Tracking] Open tracked for {tracking_id}")
    except Exception as e:
        logger.error(f"[Tracking] Failed to track open for {tracking_id}: {e}")
        # Don't fail the response - always return the pixel

    return Response(
        content=TRACKING_PIXEL,
        media_type="image/gif",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        }
    )


@router.get("/click/{tracking_id}")
def track_click(
    tracking_id: str,
    url: str = Query(..., description="Original destination URL"),
    db: Session = Depends(get_database_session)
):
    """
    Track email link click and redirect to original URL.

    Logs the click event and performs a 302 redirect to the destination.
    """
    # Validate URL to prevent open redirect attacks
    if not url.startswith(("http://", "https://", "mailto:")):
        raise HTTPException(status_code=400, detail="Invalid redirect URL")

    # Check domain allowlist (REQUIRED for non-mailto URLs)
    if not url.startswith("mailto:"):
        allowed_raw = settings.ALLOWED_REDIRECT_DOMAINS or ""
        allowed = {d.strip().lower() for d in allowed_raw.split(",") if d.strip()}
        if not allowed:
            raise HTTPException(status_code=400, detail="Redirect not configured - no allowed domains set")
        parsed_domain = urlparse(url).hostname or ""
        if not any(parsed_domain == d or parsed_domain.endswith(f".{d}") for d in allowed):
            raise HTTPException(status_code=400, detail="Redirect domain not allowed")

    try:
        email_log = _get_email_log_by_tracking_id(db, tracking_id)
        if email_log:
            email_log.clicked = True
            db.commit()
            logger.info(f"[Tracking] Click tracked for {tracking_id} -> {url[:80]}")
    except Exception as e:
        logger.error(f"[Tracking] Failed to track click for {tracking_id}: {e}")

    return RedirectResponse(url=url, status_code=302)


@router.post("/bounce")
async def handle_bounce(
    request: Request,
    db: Session = Depends(get_database_session)
):
    """
    Webhook endpoint for SMTP bounce events.

    Expected payload: { "tracking_id": str, "bounce_type": str, "reason": str }
    """
    # Verify webhook secret (REQUIRED — reject if not configured)
    if not settings.WEBHOOK_SECRET:
        logger.error("[Tracking] WEBHOOK_SECRET not configured — bounce webhook disabled")
        raise HTTPException(status_code=503, detail="Webhook not configured")
    import hmac
    provided_secret = request.headers.get("X-Webhook-Secret", "")
    if not hmac.compare_digest(provided_secret, settings.WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    try:
        # Parse body - handle both JSON and form data
        body = {}
        content_type = request.headers.get("content-type", "")
        if "json" in content_type:
            body = await request.json()
        else:
            logger.warning("[Tracking] Bounce webhook received with non-JSON content type")
            return {"status": "ignored", "reason": "unsupported content type"}

        tracking_id = body.get("tracking_id")
        if not tracking_id:
            return {"status": "ignored", "reason": "no tracking_id"}

        email_log = _get_email_log_by_tracking_id(db, tracking_id)
        if email_log:
            email_log.status = EmailStatusEnum.BOUNCED
            email_log.error_message = body.get("reason", "Bounced")

            # Update campaign recipient if linked
            if email_log.campaign_id:
                campaign_recipient = db.query(GroupCampaignRecipient).filter(
                    GroupCampaignRecipient.tracking_id == tracking_id
                ).first()
                if campaign_recipient:
                    campaign_recipient.status = RecipientStatusEnum.FAILED
                    campaign_recipient.error_message = f"Bounced: {body.get('reason', 'Unknown')}"

            db.commit()
            logger.info(f"[Tracking] Bounce recorded for {tracking_id}")

            # Record bounce in warming service if applicable
            try:
                from app.services.email_warming_service import EmailWarmingService
                EmailWarmingService.record_email_sent(
                    db, email_log.candidate_id, success=False, bounced=True
                )
            except Exception as warming_err:
                logger.warning(f"[Tracking] Failed to update warming stats: {warming_err}")

            return {"status": "processed", "tracking_id": tracking_id}

        return {"status": "not_found", "tracking_id": tracking_id}

    except Exception as e:
        logger.error(f"[Tracking] Bounce webhook error: {e}")
        return {"status": "error", "message": "Internal error processing bounce"}


@router.get("/unsubscribe/{tracking_id}")
def unsubscribe_get(
    tracking_id: str,
    db: Session = Depends(get_database_session)
):
    """
    Show unsubscribe confirmation page (GET request).
    """
    email_log = _get_email_log_by_tracking_id(db, tracking_id)
    if not email_log:
        raise HTTPException(status_code=404, detail="Invalid unsubscribe link")

    return Response(
        content=f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; text-align: center;">
            <h2>Unsubscribe</h2>
            <p>Click the button below to unsubscribe from future emails.</p>
            <form method="POST" action="/api/v1/tracking/unsubscribe/{tracking_id}">
                <button type="submit" style="padding: 12px 24px; background: #dc3545; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 16px;">
                    Unsubscribe
                </button>
            </form>
        </body>
        </html>
        """,
        media_type="text/html"
    )


@router.post("/unsubscribe/{tracking_id}")
def unsubscribe_post(
    tracking_id: str,
    db: Session = Depends(get_database_session)
):
    """
    Process unsubscribe request (RFC 8058 one-click unsubscribe).

    Marks the recipient as unsubscribed so they won't receive future campaign emails.
    """
    email_log = _get_email_log_by_tracking_id(db, tracking_id)
    if not email_log:
        raise HTTPException(status_code=404, detail="Invalid unsubscribe link")

    try:
        # Find and mark recipient as unsubscribed
        if email_log.campaign_id:
            campaign_recipient = db.query(GroupCampaignRecipient).filter(
                GroupCampaignRecipient.tracking_id == tracking_id
            ).first()
            if campaign_recipient and campaign_recipient.recipient_id:
                recipient = db.query(Recipient).filter(
                    Recipient.id == campaign_recipient.recipient_id
                ).first()
                if recipient:
                    recipient.unsubscribed = True
                    logger.info(f"[Tracking] Recipient {recipient.email} unsubscribed via {tracking_id}")

        db.commit()

        return Response(
            content="""
            <html>
            <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; text-align: center;">
                <h2>Unsubscribed</h2>
                <p>You have been successfully unsubscribed. You will no longer receive emails from this sender.</p>
            </body>
            </html>
            """,
            media_type="text/html"
        )

    except Exception as e:
        logger.error(f"[Tracking] Unsubscribe error for {tracking_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to process unsubscribe request")


@router.get("/email-auth/verify/{domain}")
def verify_email_auth(
    domain: str,
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Check SPF, DKIM, and DMARC DNS records for a domain.

    Returns auth status badges for campaign Step 4.
    Requires authentication to prevent abuse as DNS oracle.
    """
    import dns.resolver

    results = {"domain": domain, "spf": False, "dkim": False, "dmarc": False}

    try:
        # Check SPF (TXT record on domain)
        try:
            txt_records = dns.resolver.resolve(domain, 'TXT')
            for record in txt_records:
                if 'v=spf1' in str(record):
                    results["spf"] = True
                    break
        except Exception:
            pass

        # Check DKIM (common selectors)
        for selector in ['default', 'google', 'selector1', 'selector2', 'k1']:
            try:
                dns.resolver.resolve(f'{selector}._domainkey.{domain}', 'TXT')
                results["dkim"] = True
                break
            except Exception:
                continue

        # Check DMARC
        try:
            dmarc_records = dns.resolver.resolve(f'_dmarc.{domain}', 'TXT')
            for record in dmarc_records:
                if 'v=DMARC1' in str(record):
                    results["dmarc"] = True
                    break
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[Tracking] DNS lookup error for {domain}: {e}")

    return results
