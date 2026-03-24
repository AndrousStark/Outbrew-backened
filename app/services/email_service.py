"""Email Sending Service with Attachment Support and Retry Logic"""
import smtplib
import ssl
import re
import logging
import socket
import time
import functools
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import Optional, List, Callable, TypeVar, Any
from datetime import datetime, timezone
from jinja2.sandbox import SandboxedEnvironment
from sqlalchemy.orm import Session

from app.models.candidate import Candidate
from app.models.email_log import EmailLog, EmailStatusEnum
from app.models.application import Application
from app.models.email_template import EmailTemplate
from app.models.resume import ResumeVersion
from app.core.encryption import decrypt_value

logger = logging.getLogger(__name__)

# Configurable SMTP timeout (in seconds)
SMTP_TIMEOUT = 30

# Retry configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1  # seconds
MAX_RETRY_DELAY = 30  # seconds
EXPONENTIAL_BACKOFF_MULTIPLIER = 2

# Email validation regex pattern
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

# Retryable exceptions - transient errors that may succeed on retry
RETRYABLE_EXCEPTIONS = (
    socket.timeout,
    socket.gaierror,  # DNS resolution errors
    ConnectionResetError,
    ConnectionRefusedError,
    ConnectionAbortedError,
    BrokenPipeError,
    smtplib.SMTPServerDisconnected,
    smtplib.SMTPConnectError,
    smtplib.SMTPHeloError,
    smtplib.SMTPDataError,  # 4xx temporary errors
    ssl.SSLError,
    OSError,  # Network unreachable, etc.
)

# Non-retryable exceptions - permanent failures
NON_RETRYABLE_EXCEPTIONS = (
    smtplib.SMTPAuthenticationError,  # Wrong credentials
    smtplib.SMTPRecipientsRefused,  # Invalid recipient
    smtplib.SMTPSenderRefused,  # Invalid sender
    smtplib.SMTPNotSupportedError,  # Server doesn't support feature
)

T = TypeVar('T')


def with_retry(
    max_retries: int = MAX_RETRIES,
    initial_delay: float = INITIAL_RETRY_DELAY,
    max_delay: float = MAX_RETRY_DELAY,
    backoff_multiplier: float = EXPONENTIAL_BACKOFF_MULTIPLIER,
    retryable_exceptions: tuple = RETRYABLE_EXCEPTIONS,
    non_retryable_exceptions: tuple = NON_RETRYABLE_EXCEPTIONS
) -> Callable:
    """
    Decorator for retry logic with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
        backoff_multiplier: Multiplier for exponential backoff
        retryable_exceptions: Tuple of exceptions that should trigger retry
        non_retryable_exceptions: Tuple of exceptions that should NOT be retried

    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            delay = initial_delay

            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        logger.info(f"[Retry] Attempt {attempt + 1}/{max_retries + 1} for {func.__name__}")
                    return func(*args, **kwargs)

                except non_retryable_exceptions as e:
                    # Don't retry permanent failures
                    logger.error(f"[Retry] Non-retryable error in {func.__name__}: {type(e).__name__}: {e}")
                    raise

                except retryable_exceptions as e:
                    last_exception = e

                    if attempt < max_retries:
                        logger.warning(
                            f"[Retry] Transient error in {func.__name__} (attempt {attempt + 1}/{max_retries + 1}): "
                            f"{type(e).__name__}: {e}. Retrying in {delay:.1f}s..."
                        )
                        time.sleep(delay)
                        delay = min(delay * backoff_multiplier, max_delay)
                    else:
                        logger.error(
                            f"[Retry] All {max_retries + 1} attempts failed for {func.__name__}. "
                            f"Last error: {type(e).__name__}: {e}"
                        )
                        raise

                except Exception as e:
                    # Unknown exceptions - don't retry by default
                    logger.error(f"[Retry] Unexpected error in {func.__name__}: {type(e).__name__}: {e}")
                    raise

            # Should not reach here, but just in case
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


def validate_email(email: str) -> bool:
    """Validate email address format"""
    if not email:
        return False
    return bool(EMAIL_REGEX.match(email))


class EmailServiceError(Exception):
    """Custom exception for email service errors"""
    pass


class EmailService:
    """Service for sending emails with attachments and template support"""

    def __init__(self, db: Session):
        self.db = db
        logger.debug("[EmailService] Initialized with database session")

    @staticmethod
    @with_retry(max_retries=MAX_RETRIES)
    def _send_via_smtp(
        smtp_host: str,
        smtp_port: int,
        email_account: str,
        email_password: str,
        msg: MIMEMultipart
    ) -> None:
        """
        Send email via SMTP with retry logic for transient failures.

        This method is decorated with retry logic that will automatically
        retry on transient network/connection errors with exponential backoff.

        Args:
            smtp_host: SMTP server hostname
            smtp_port: SMTP server port
            email_account: Email account for authentication
            email_password: Email password for authentication
            msg: The email message to send

        Raises:
            EmailServiceError: If sending fails after all retries
        """
        logger.info(f"[EmailService] Connecting to SMTP server: {smtp_host}:{smtp_port}")
        logger.debug(f"[EmailService] SMTP timeout set to {SMTP_TIMEOUT} seconds")

        # Create SSL context with certificate verification
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        logger.debug(f"[EmailService] SSL context created with certificate verification enabled")

        with smtplib.SMTP(smtp_host, smtp_port, timeout=SMTP_TIMEOUT) as server:
            logger.debug("[EmailService] SMTP connection established")
            server.starttls(context=ssl_context)
            logger.debug("[EmailService] STARTTLS established with verified certificate")
            server.login(email_account, email_password)
            logger.debug("[EmailService] SMTP login successful")
            server.send_message(msg)
            logger.info("[EmailService] Email sent via SMTP successfully")

    def render_template(
        self,
        template_text: str,
        variables: dict
    ) -> str:
        """Render a Jinja2 template with variables"""
        try:
            template = SandboxedEnvironment().from_string(template_text)
            return template.render(**variables)
        except Exception as e:
            logger.error(f"Template rendering error: {str(e)}")
            raise EmailServiceError(f"Failed to render template: {str(e)}")

    def send_email(
        self,
        candidate: Candidate,
        to_email: str,
        subject: str,
        body_html: str,
        body_text: Optional[str] = None,
        attachments: Optional[List[str]] = None,
        application_id: Optional[int] = None,
        tracking_id: Optional[str] = None,
        campaign_id: Optional[int] = None
    ) -> EmailLog:
        """
        Send an email with optional attachments

        Args:
            candidate: The candidate sending the email
            to_email: Recipient email address
            subject: Email subject
            body_html: HTML version of email body
            body_text: Plain text version of email body (optional)
            attachments: List of file paths to attach (optional)
            application_id: Associated application ID (optional)

        Returns:
            EmailLog: The email log entry

        Raises:
            EmailServiceError: If email sending fails
        """
        logger.info(f"[EmailService] ========== SEND EMAIL START ==========")
        logger.info(f"[EmailService] From: {candidate.email_account}")
        logger.info(f"[EmailService] To: {to_email}")
        logger.info(f"[EmailService] Subject: {subject[:50]}...")
        logger.info(f"[EmailService] Application ID: {application_id}")

        # Validate email addresses before proceeding
        if not validate_email(to_email):
            error_msg = f"Invalid recipient email address format: {to_email}"
            logger.error(f"[EmailService] {error_msg}")
            raise EmailServiceError(error_msg)

        if not validate_email(candidate.email_account):
            error_msg = f"Invalid sender email address format: {candidate.email_account}"
            logger.error(f"[EmailService] {error_msg}")
            raise EmailServiceError(error_msg)

        logger.debug(f"[EmailService] Email addresses validated successfully")

        email_log = EmailLog(
            candidate_id=candidate.id,
            application_id=application_id,
            from_email=candidate.email_account,
            to_email=to_email,
            subject=subject,
            body_html=body_html,
            tracking_id=tracking_id,
            campaign_id=campaign_id,
            status=EmailStatusEnum.PENDING
        )

        try:
            logger.info(f"[EmailService] Preparing email from {candidate.email_account} to {to_email}")
            logger.debug(f"[EmailService] Subject: {subject}")
            logger.debug(f"[EmailService] Application ID: {application_id}")
            logger.debug(f"[EmailService] Attachments: {attachments}")

            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = candidate.email_account
            msg['To'] = to_email
            msg['Subject'] = subject

            # RFC 8058 one-click unsubscribe headers (required by Google/Microsoft for bulk email)
            if campaign_id and tracking_id:
                from app.core.config import settings
                unsub_url = f"{settings.BASE_URL.rstrip('/')}/api/v1/tracking/unsubscribe/{tracking_id}"
                msg['List-Unsubscribe'] = f"<{unsub_url}>"
                msg['List-Unsubscribe-Post'] = "List-Unsubscribe=One-Click"

            # Auto-generate plain text from HTML if not provided
            # Plain text cold emails have significantly lower bounce rates
            if not body_text:
                body_text = self._html_to_plain_text(body_html)

            # Attach plain text version
            part1 = MIMEText(body_text, 'plain')
            msg.attach(part1)

            # Attach HTML version
            part2 = MIMEText(body_html, 'html')
            msg.attach(part2)

            # Attach files if provided
            if attachments:
                for file_path in attachments:
                    self._attach_file(msg, file_path)

            # Send email via SMTP with retry logic for transient failures
            # The _send_via_smtp method has built-in retry with exponential backoff
            # Decrypt the email password (stored encrypted in database)
            decrypted_password = decrypt_value(candidate.email_password) if candidate.email_password else ""
            self._send_via_smtp(
                smtp_host=candidate.smtp_host,
                smtp_port=candidate.smtp_port,
                email_account=candidate.email_account,
                email_password=decrypted_password,
                msg=msg
            )

            # Update log on success
            email_log.status = EmailStatusEnum.SENT
            email_log.sent_at = datetime.now(timezone.utc)
            logger.info(f"Email sent successfully to {to_email} (with retry support)")

        except smtplib.SMTPAuthenticationError as e:
            error_msg = f"SMTP authentication failed: {str(e)}"
            logger.error(error_msg)
            email_log.status = EmailStatusEnum.FAILED
            email_log.error_message = error_msg
            raise EmailServiceError(error_msg)

        except smtplib.SMTPException as e:
            error_msg = f"SMTP error: {str(e)}"
            logger.error(error_msg)
            email_log.status = EmailStatusEnum.FAILED
            email_log.error_message = error_msg
            raise EmailServiceError(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error sending email: {str(e)}"
            logger.error(error_msg)
            email_log.status = EmailStatusEnum.FAILED
            email_log.error_message = error_msg
            raise EmailServiceError(error_msg)

        finally:
            # Save email log to database with error handling
            try:
                logger.debug(f"[EmailService] Saving email log to database (status: {email_log.status})")
                self.db.add(email_log)
                self.db.commit()
                self.db.refresh(email_log)
                logger.info(f"[EmailService] Email log saved successfully (ID: {email_log.id})")
            except Exception as db_error:
                logger.error(f"[EmailService] Failed to save email log to database: {db_error}")
                try:
                    self.db.rollback()
                    logger.debug("[EmailService] Database rollback completed")
                except Exception as rollback_error:
                    logger.error(f"[EmailService] Rollback also failed: {rollback_error}")

        logger.info(f"[EmailService] ========== SEND EMAIL END (status: {email_log.status}) ==========")
        return email_log

    def send_application_email(
        self,
        application: Application,
        candidate: Candidate,
        resume_version: Optional[ResumeVersion] = None,
        email_template: Optional[EmailTemplate] = None
    ) -> EmailLog:
        """
        Send an application email with resume attachment and template rendering

        Args:
            application: The application being sent
            candidate: The candidate sending the application
            resume_version: Optional resume version to attach
            email_template: Optional email template to use

        Returns:
            EmailLog: The email log entry
        """
        # Prepare template variables
        template_vars = {
            'candidate_name': candidate.full_name,
            'candidate_email': candidate.email,
            'recruiter_name': application.recruiter_name or 'Hiring Manager',
            'company_name': application.company.name if application.company else 'your company',
            'position_title': application.position_title or 'the position',
            'position_level': application.position_level or '',
            'skills': candidate.skills or [],
            'candidate_title': candidate.title or '',
        }

        # Determine subject and body
        if email_template:
            subject = self.render_template(email_template.subject_template, template_vars)
            body_html = self.render_template(email_template.body_template_html, template_vars)
            body_text = self.render_template(
                email_template.body_template_text, template_vars
            ) if email_template.body_template_text else None
        else:
            subject = application.email_subject or f"Application for {template_vars['position_title']}"
            body_html = application.email_body_html or self._generate_default_email_body(template_vars)
            body_text = None

        # Prepare attachments
        attachments = []
        if resume_version and resume_version.file_path:
            if Path(resume_version.file_path).exists():
                attachments.append(resume_version.file_path)
            else:
                logger.warning(f"Resume file not found: {resume_version.file_path}")
        elif candidate.resume_path:
            if Path(candidate.resume_path).exists():
                attachments.append(candidate.resume_path)
            else:
                logger.warning(f"Resume file not found: {candidate.resume_path}")

        # Send email
        email_log = self.send_email(
            candidate=candidate,
            to_email=application.recruiter_email,
            subject=subject,
            body_html=body_html,
            body_text=body_text,
            attachments=attachments if attachments else None,
            application_id=application.id
        )

        # Update application status
        if email_log.status == EmailStatusEnum.SENT:
            application.status = "sent"
            application.sent_at = datetime.now(timezone.utc)

            # Update usage tracking for resume and template
            if resume_version:
                resume_version.times_used = (resume_version.times_used or 0) + 1
                resume_version.last_used_at = datetime.now(timezone.utc)

            if email_template:
                email_template.times_used = (email_template.times_used or 0) + 1
                email_template.last_used_at = datetime.now(timezone.utc)

            # Update candidate stats
            candidate.total_applications_sent = (candidate.total_applications_sent or 0) + 1

            try:
                self.db.commit()
                logger.info(f"[EmailService] Application {application.id} status updated to 'sent', usage stats updated")
            except Exception as e:
                self.db.rollback()
                logger.error(f"[EmailService] Failed to update application status after sending email: {e}")
                # Don't raise - email was sent successfully, just log the DB error

        return email_log

    def _attach_file(self, msg: MIMEMultipart, file_path: str):
        """Attach a file to the email message"""
        try:
            file_path_obj = Path(file_path)

            if not file_path_obj.exists():
                raise EmailServiceError(f"Attachment file not found: {file_path}")

            with open(file_path, 'rb') as file:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(file.read())

            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {file_path_obj.name}'
            )

            msg.attach(part)
            logger.debug(f"Attached file: {file_path_obj.name}")

        except Exception as e:
            logger.error(f"Failed to attach file {file_path}: {str(e)}")
            raise EmailServiceError(f"Failed to attach file: {str(e)}")

    def _generate_default_email_body(self, variables: dict) -> str:
        """Generate a default email body if no template is provided"""
        return f"""
        <html>
            <body>
                <p>Dear {variables['recruiter_name']},</p>

                <p>I am writing to express my interest in the {variables['position_title']} position at {variables['company_name']}.</p>

                <p>I am {variables['candidate_name']}, {variables['candidate_title']}, and I believe my skills and experience make me a strong candidate for this role.</p>

                <p>Please find my resume attached for your review.</p>

                <p>I look forward to discussing this opportunity with you.</p>

                <p>Best regards,<br>
                {variables['candidate_name']}<br>
                {variables['candidate_email']}</p>
            </body>
        </html>
        """

    def send_follow_up_email(
        self,
        parent_application: Application,
        follow_up_application: Application,
        candidate: Candidate,
        email_template: Optional[EmailTemplate] = None
    ) -> EmailLog:
        """
        Send a follow-up email for an existing application

        Args:
            parent_application: The original application
            follow_up_application: The follow-up application
            candidate: The candidate sending the follow-up
            email_template: Optional follow-up email template

        Returns:
            EmailLog: The email log entry
        """
        # Prepare template variables
        template_vars = {
            'candidate_name': candidate.full_name,
            'candidate_email': candidate.email,
            'recruiter_name': follow_up_application.recruiter_name or 'Hiring Manager',
            'company_name': follow_up_application.company.name if follow_up_application.company else 'your company',
            'position_title': follow_up_application.position_title or 'the position',
            'original_sent_date': parent_application.sent_at.strftime('%B %d, %Y') if parent_application.sent_at else 'recently',
        }

        # Determine subject and body
        if email_template and email_template.category.value == 'FOLLOW_UP':
            subject = self.render_template(email_template.subject_template, template_vars)
            body_html = self.render_template(email_template.body_template_html, template_vars)
            body_text = self.render_template(
                email_template.body_template_text, template_vars
            ) if email_template.body_template_text else None
        else:
            subject = f"Following up on {template_vars['position_title']} Application"
            body_html = self._generate_default_follow_up_body(template_vars)
            body_text = None

        # Send email (no attachment for follow-ups by default)
        return self.send_email(
            candidate=candidate,
            to_email=follow_up_application.recruiter_email,
            subject=subject,
            body_html=body_html,
            body_text=body_text,
            application_id=follow_up_application.id
        )

    def _generate_default_follow_up_body(self, variables: dict) -> str:
        """Generate a default follow-up email body"""
        return f"""
        <html>
            <body>
                <p>Dear {variables['recruiter_name']},</p>

                <p>I hope this email finds you well.</p>

                <p>I wanted to follow up on my application for the {variables['position_title']} position at {variables['company_name']}, which I submitted on {variables['original_sent_date']}.</p>

                <p>I remain very interested in this opportunity and would appreciate any update on the status of my application.</p>

                <p>Thank you for your time and consideration.</p>

                <p>Best regards,<br>
                {variables['candidate_name']}<br>
                {variables['candidate_email']}</p>
            </body>
        </html>
        """

    @staticmethod
    def _html_to_plain_text(html_content: str) -> str:
        """Strip HTML tags to create plain text version of email body."""
        if not html_content:
            return ""
        # Remove HTML tags
        text = re.sub(r'<br\s*/?>', '\n', html_content)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'</div>', '\n', text)
        text = re.sub(r'</li>', '\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    @staticmethod
    def _inject_tracking(body_html: str, tracking_id: str, base_url: str = "", enable_open_tracking: bool = False) -> str:
        """
        Inject tracking into HTML body.

        Note: Open tracking HURTS cold email deliverability per 2026 research.
        Only inject open pixel if explicitly enabled.
        """
        if not tracking_id or not body_html:
            return body_html

        # Click tracking: wrap <a href> links with tracking redirect
        from urllib.parse import urlencode

        def replace_link(match):
            original_url = match.group(1)
            # Don't track unsubscribe links or mailto
            if 'unsubscribe' in original_url.lower() or original_url.startswith('mailto:'):
                return match.group(0)
            tracked_url = f"{base_url}/api/v1/tracking/click/{tracking_id}?{urlencode({'url': original_url})}"
            return f'href="{tracked_url}"'

        body_html = re.sub(r'href="([^"]+)"', replace_link, body_html)

        # Open tracking pixel (disabled by default for deliverability)
        if enable_open_tracking:
            pixel = f'<img src="{base_url}/api/v1/tracking/open/{tracking_id}" width="1" height="1" style="display:none" alt="" />'
            if '</body>' in body_html:
                body_html = body_html.replace('</body>', f'{pixel}</body>')
            else:
                body_html += pixel

        return body_html
