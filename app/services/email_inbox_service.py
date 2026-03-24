"""
Email Inbox Service - IMAP/OAuth email synchronization

Features:
- IMAP connection with SSL/TLS
- OAuth2 authentication for Gmail/Outlook
- Email fetching and parsing
- Thread detection and grouping
- Storage management
- Automatic sync scheduling
"""

import imaplib
import email
import logging
import hashlib
import os
from email.message import EmailMessage as EmailMessageLib
from email.utils import parsedate_to_datetime, parseaddr
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.models.email_inbox import (
    EmailAccount, EmailMessage, EmailThread, StorageQuota,
    EmailDirection, EmailSyncStatus, EmailAccountType
)
from app.models.candidate import Candidate
from app.core.config import settings

logger = logging.getLogger(__name__)


class EmailInboxService:
    """Service for managing email inbox integration"""

    def __init__(self, db: Session):
        self.db = db
        logger.debug("[EmailInboxService] Initialized")

    # ==================== EMAIL ACCOUNT MANAGEMENT ====================

    def create_email_account(
        self,
        candidate_id: int,
        email_address: str,
        account_type: EmailAccountType,
        imap_host: Optional[str] = None,
        imap_port: int = 993,
        imap_username: Optional[str] = None,
        imap_password: Optional[str] = None,
        display_name: Optional[str] = None
    ) -> EmailAccount:
        """
        Create a new email account for inbox integration
        """
        logger.info(f"[EmailInbox] Creating email account for {email_address}")

        # Check if account already exists
        existing = self.db.query(EmailAccount).filter(
            EmailAccount.email_address == email_address,
            EmailAccount.deleted_at.is_(None)
        ).first()

        if existing:
            raise ValueError(f"Email account {email_address} already exists")

        # Auto-configure IMAP settings for known providers
        if not imap_host:
            imap_host, imap_port = self._get_imap_config(account_type, email_address)

        account = EmailAccount(
            candidate_id=candidate_id,
            email_address=email_address,
            account_type=account_type,
            display_name=display_name or email_address,
            imap_host=imap_host,
            imap_port=imap_port,
            imap_username=imap_username or email_address,
            imap_password=imap_password,  # TODO: Encrypt in production
            sync_enabled=True,
            is_primary=False
        )

        self.db.add(account)
        self.db.commit()
        self.db.refresh(account)

        # Initialize storage quota
        self._initialize_storage_quota(candidate_id)

        logger.info(f"[EmailInbox] Created email account {account.id} for {email_address}")
        return account

    def _get_imap_config(self, account_type: EmailAccountType, email_address: str) -> Tuple[str, int]:
        """Auto-configure IMAP settings for known providers"""
        if account_type == EmailAccountType.GMAIL or "@gmail.com" in email_address:
            return "imap.gmail.com", 993
        elif account_type == EmailAccountType.OUTLOOK or "@outlook.com" in email_address or "@hotmail.com" in email_address:
            return "outlook.office365.com", 993
        elif account_type == EmailAccountType.YAHOO or "@yahoo.com" in email_address:
            return "imap.mail.yahoo.com", 993
        else:
            return "imap.gmail.com", 993  # Default

    def _initialize_storage_quota(self, candidate_id: int):
        """Initialize storage quota for candidate if not exists"""
        existing_quota = self.db.query(StorageQuota).filter(
            StorageQuota.candidate_id == candidate_id
        ).first()

        if not existing_quota:
            quota = StorageQuota(
                candidate_id=candidate_id,
                quota_limit=524288000,  # 500 MB
                used_bytes=0
            )
            self.db.add(quota)
            self.db.commit()
            logger.info(f"[EmailInbox] Initialized storage quota for candidate {candidate_id}")

    def get_email_accounts(self, candidate_id: int) -> List[EmailAccount]:
        """Get all email accounts for a candidate"""
        return self.db.query(EmailAccount).filter(
            EmailAccount.candidate_id == candidate_id,
            EmailAccount.deleted_at.is_(None)
        ).all()

    def delete_email_account(self, account_id: int, candidate_id: int) -> bool:
        """Soft delete an email account"""
        account = self.db.query(EmailAccount).filter(
            EmailAccount.id == account_id,
            EmailAccount.candidate_id == candidate_id
        ).first()

        if not account:
            return False

        account.deleted_at = datetime.now(timezone.utc)
        account.sync_enabled = False
        self.db.commit()

        logger.info(f"[EmailInbox] Deleted email account {account_id}")
        return True

    # ==================== EMAIL SYNCING ====================

    def sync_inbox(self, account_id: int, limit: int = 50) -> Dict[str, Any]:
        """
        Sync emails from IMAP inbox
        """
        logger.info(f"[EmailInbox] Starting inbox sync for account {account_id}")

        account = self.db.query(EmailAccount).filter(
            EmailAccount.id == account_id
        ).first()

        if not account:
            raise ValueError(f"Email account {account_id} not found")

        if not account.sync_enabled:
            raise ValueError(f"Sync disabled for account {account_id}")

        account.sync_status = EmailSyncStatus.SYNCING
        self.db.commit()

        results = {
            "account_id": account_id,
            "emails_fetched": 0,
            "emails_saved": 0,
            "threads_created": 0,
            "errors": []
        }

        try:
            # Connect to IMAP
            mail = self._connect_imap(account)

            # Select inbox
            mail.select("INBOX")

            # Search for recent emails
            _, message_ids = mail.search(None, "ALL")
            message_id_list = message_ids[0].split()

            # Get last N emails
            recent_ids = message_id_list[-limit:] if len(message_id_list) > limit else message_id_list

            logger.info(f"[EmailInbox] Found {len(recent_ids)} emails to process")

            for msg_id in reversed(recent_ids):  # Most recent first
                try:
                    # Fetch email
                    _, msg_data = mail.fetch(msg_id, "(RFC822)")
                    raw_email = msg_data[0][1]

                    # Parse email
                    email_message = email.message_from_bytes(raw_email)
                    results["emails_fetched"] += 1

                    # Save to database
                    saved = self._save_email_message(account, email_message)
                    if saved:
                        results["emails_saved"] += 1

                except Exception as e:
                    logger.error(f"[EmailInbox] Error processing email {msg_id}: {e}")
                    results["errors"].append(str(e))
                    continue

            # Close connection
            mail.close()
            mail.logout()

            # Update account sync status
            account.sync_status = EmailSyncStatus.SYNCED
            account.last_sync_at = datetime.now(timezone.utc)
            account.total_emails_synced = account.total_emails_synced + results["emails_saved"]
            self.db.commit()

            logger.info(f"[EmailInbox] Sync complete: {results}")
            return results

        except Exception as e:
            logger.error(f"[EmailInbox] Sync failed for account {account_id}: {e}")
            account.sync_status = EmailSyncStatus.FAILED
            account.sync_error = str(e)
            self.db.commit()
            raise

    def _connect_imap(self, account: EmailAccount) -> imaplib.IMAP4_SSL:
        """Connect to IMAP server with SSL"""
        logger.debug(f"[EmailInbox] Connecting to {account.imap_host}:{account.imap_port}")

        try:
            mail = imaplib.IMAP4_SSL(account.imap_host, account.imap_port)
            mail.login(account.imap_username, account.imap_password)
            logger.info(f"[EmailInbox] Connected to IMAP server for {account.email_address}")
            return mail
        except Exception as e:
            logger.error(f"[EmailInbox] IMAP connection failed: {e}")
            raise

    def _save_email_message(self, account: EmailAccount, email_message: EmailMessageLib) -> bool:
        """
        Save email message to database
        """
        try:
            # Extract email details
            message_id = email_message.get("Message-ID", "")
            subject = email_message.get("Subject", "")
            from_header = email_message.get("From", "")
            to_header = email_message.get("To", "")
            date_header = email_message.get("Date", "")
            in_reply_to = email_message.get("In-Reply-To", "")

            # Parse from/to
            from_name, from_email = parseaddr(from_header)
            to_name, to_email = parseaddr(to_header)

            # Determine direction
            is_sent = from_email.lower() == account.email_address.lower()
            direction = EmailDirection.SENT if is_sent else EmailDirection.RECEIVED

            # Check if already exists
            existing = self.db.query(EmailMessage).filter(
                EmailMessage.message_id == message_id
            ).first()

            if existing:
                logger.debug(f"[EmailInbox] Email {message_id} already exists, skipping")
                return False

            # Parse date
            try:
                sent_at = parsedate_to_datetime(date_header) if date_header else datetime.now(timezone.utc)
            except Exception as e:
                logger.warning(f"[EmailInbox] Failed to parse date header: {e}")
                sent_at = datetime.now(timezone.utc)

            # Extract body
            body_text, body_html = self._extract_email_body(email_message)

            # Generate snippet
            snippet = (body_text[:500] if body_text else body_html[:500]) if body_text or body_html else ""

            # Generate thread ID
            thread_id = self._generate_thread_id(message_id, in_reply_to, subject)

            # Calculate size
            size_bytes = len(str(email_message))

            # Save email to file
            file_path = self._save_email_to_file(account.candidate_id, direction, message_id, body_html or body_text)

            # Create email message record
            email_msg = EmailMessage(
                candidate_id=account.candidate_id,
                email_account_id=account.id,
                direction=direction,
                message_id=message_id,
                in_reply_to=in_reply_to,
                thread_id=thread_id,
                from_email=from_email,
                from_name=from_name,
                to_email=to_email,
                to_name=to_name,
                subject=subject,
                body_text=body_text,
                body_html=body_html,
                snippet=snippet,
                file_path=file_path,
                is_read=is_sent,  # Sent emails are read by default
                size_bytes=size_bytes,
                sent_at=sent_at if is_sent else None,
                received_at=sent_at if not is_sent else None
            )

            self.db.add(email_msg)

            # Update or create thread
            self._update_thread(account.candidate_id, thread_id, subject, sent_at, snippet)

            # Update storage quota
            self._update_storage_usage(account.candidate_id, size_bytes, "emails")

            self.db.commit()

            logger.info(f"[EmailInbox] Saved email {message_id[:20]}... ({direction.value})")
            return True

        except Exception as e:
            logger.error(f"[EmailInbox] Failed to save email: {e}")
            self.db.rollback()
            return False

    def _extract_email_body(self, email_message: EmailMessageLib) -> Tuple[Optional[str], Optional[str]]:
        """Extract text and HTML body from email"""
        body_text = None
        body_html = None

        if email_message.is_multipart():
            for part in email_message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                if "attachment" not in content_disposition:
                    if content_type == "text/plain" and not body_text:
                        body_text = part.get_payload(decode=True).decode(errors="ignore")
                    elif content_type == "text/html" and not body_html:
                        body_html = part.get_payload(decode=True).decode(errors="ignore")
        else:
            content_type = email_message.get_content_type()
            payload = email_message.get_payload(decode=True)
            if payload:
                decoded = payload.decode(errors="ignore")
                if content_type == "text/plain":
                    body_text = decoded
                elif content_type == "text/html":
                    body_html = decoded

        return body_text, body_html

    def _generate_thread_id(self, message_id: str, in_reply_to: str, subject: str) -> str:
        """Generate thread ID for grouping conversations"""
        if in_reply_to:
            # Use in_reply_to as thread base
            return hashlib.sha256(in_reply_to.encode()).hexdigest()[:32]
        else:
            # Use subject (cleaned) as thread base
            clean_subject = subject.lower().replace("re:", "").replace("fwd:", "").strip()
            return hashlib.sha256(clean_subject.encode()).hexdigest()[:32]

    def _save_email_to_file(
        self,
        candidate_id: int,
        direction: EmailDirection,
        message_id: str,
        content: str
    ) -> str:
        """Save email content to file"""
        try:
            # Create directory structure
            now = datetime.now(timezone.utc)
            base_path = Path(f"backend/user_data/candidate_{candidate_id}/emails")
            year_month_path = base_path / str(now.year) / f"{now.month:02d}"
            year_month_path.mkdir(parents=True, exist_ok=True)

            # Generate filename
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            safe_msg_id = hashlib.md5(message_id.encode()).hexdigest()[:8]
            filename = f"{direction.value}_email_{safe_msg_id}_{timestamp}.html"

            # Full file path
            file_path = year_month_path / filename

            # Write content
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)

            return str(file_path)

        except Exception as e:
            logger.error(f"[EmailInbox] Failed to save email to file: {e}")
            return ""

    def _update_thread(self, candidate_id: int, thread_id: str, subject: str, latest_at: datetime, snippet: str):
        """Update or create email thread"""
        thread = self.db.query(EmailThread).filter(
            EmailThread.thread_id == thread_id
        ).first()

        if thread:
            # Update existing thread
            thread.message_count += 1
            thread.latest_message_at = latest_at
            thread.latest_snippet = snippet
        else:
            # Create new thread
            thread = EmailThread(
                candidate_id=candidate_id,
                thread_id=thread_id,
                subject=subject,
                message_count=1,
                unread_count=1,
                latest_message_at=latest_at,
                latest_snippet=snippet
            )
            self.db.add(thread)

    def _update_storage_usage(self, candidate_id: int, size_bytes: int, category: str):
        """Update storage quota usage"""
        quota = self.db.query(StorageQuota).filter(
            StorageQuota.candidate_id == candidate_id
        ).first()

        if quota:
            quota.used_bytes += size_bytes
            if category == "emails":
                quota.emails_bytes += size_bytes
                quota.total_emails_archived += 1
            quota.last_calculated_at = datetime.now(timezone.utc)

    # ==================== EMAIL RETRIEVAL ====================

    def get_inbox_threads(
        self,
        candidate_id: int,
        skip: int = 0,
        limit: int = 50,
        unread_only: bool = False
    ) -> List[EmailThread]:
        """Get email threads for inbox view"""
        query = self.db.query(EmailThread).filter(
            EmailThread.candidate_id == candidate_id
        )

        if unread_only:
            query = query.filter(EmailThread.unread_count > 0)

        return query.order_by(desc(EmailThread.latest_message_at)).offset(skip).limit(limit).all()

    def get_thread_messages(self, thread_id: str, candidate_id: int) -> List[EmailMessage]:
        """Get all messages in a thread"""
        return self.db.query(EmailMessage).filter(
            EmailMessage.thread_id == thread_id,
            EmailMessage.candidate_id == candidate_id,
            EmailMessage.deleted_at.is_(None)
        ).order_by(EmailMessage.sent_at).all()

    def get_email_message(self, message_id: int, candidate_id: int) -> Optional[EmailMessage]:
        """Get a single email message"""
        return self.db.query(EmailMessage).filter(
            EmailMessage.id == message_id,
            EmailMessage.candidate_id == candidate_id
        ).first()

    def mark_as_read(self, message_id: int, candidate_id: int) -> bool:
        """Mark email as read"""
        message = self.get_email_message(message_id, candidate_id)
        if message and not message.is_read:
            message.is_read = True

            # Update thread unread count
            thread = self.db.query(EmailThread).filter(
                EmailThread.thread_id == message.thread_id
            ).first()
            if thread and thread.unread_count > 0:
                thread.unread_count -= 1

            self.db.commit()
            return True
        return False

    def toggle_starred(self, message_id: int, candidate_id: int) -> bool:
        """Toggle starred status"""
        message = self.get_email_message(message_id, candidate_id)
        if message:
            message.is_starred = not message.is_starred
            self.db.commit()
            return message.is_starred
        return False

    # ==================== EMAIL SEARCH ====================

    def search_emails(
        self,
        candidate_id: int,
        query: Optional[str] = None,
        from_email: Optional[str] = None,
        to_email: Optional[str] = None,
        subject: Optional[str] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        is_read: Optional[bool] = None,
        is_starred: Optional[bool] = None,
        is_important: Optional[bool] = None,
        direction: Optional[EmailDirection] = None,
        has_attachments: Optional[bool] = None,
        skip: int = 0,
        limit: int = 50
    ) -> Tuple[List[EmailMessage], int]:
        """
        Search emails with advanced filters

        Args:
            candidate_id: Candidate who owns the emails
            query: Full-text search query (searches subject, from, to, body)
            from_email: Filter by sender email (partial match)
            to_email: Filter by recipient email (partial match)
            subject: Filter by subject (partial match)
            date_from: Filter emails from this date onwards
            date_to: Filter emails up to this date
            is_read: Filter by read status
            is_starred: Filter by starred status
            is_important: Filter by important status
            direction: Filter by direction (SENT/RECEIVED)
            has_attachments: Filter emails with attachments
            skip: Pagination offset
            limit: Results per page

        Returns:
            Tuple of (messages list, total count)
        """
        logger.info(f"[EmailInbox] Searching emails for candidate {candidate_id} with query: {query}")

        # Build base query
        query_builder = self.db.query(EmailMessage).filter(
            EmailMessage.candidate_id == candidate_id
        )

        # Apply full-text search if query provided
        if query and query.strip():
            search_term = f"%{query.strip()}%"
            query_builder = query_builder.filter(
                (EmailMessage.subject.ilike(search_term)) |
                (EmailMessage.from_email.ilike(search_term)) |
                (EmailMessage.from_name.ilike(search_term)) |
                (EmailMessage.to_email.ilike(search_term)) |
                (EmailMessage.to_name.ilike(search_term)) |
                (EmailMessage.body_text.ilike(search_term)) |
                (EmailMessage.snippet.ilike(search_term))
            )

        # Apply specific field filters
        if from_email:
            query_builder = query_builder.filter(
                (EmailMessage.from_email.ilike(f"%{from_email}%")) |
                (EmailMessage.from_name.ilike(f"%{from_email}%"))
            )

        if to_email:
            query_builder = query_builder.filter(
                (EmailMessage.to_email.ilike(f"%{to_email}%")) |
                (EmailMessage.to_name.ilike(f"%{to_email}%"))
            )

        if subject:
            query_builder = query_builder.filter(
                EmailMessage.subject.ilike(f"%{subject}%")
            )

        # Date range filters
        if date_from:
            query_builder = query_builder.filter(
                (EmailMessage.received_at >= date_from) |
                (EmailMessage.sent_at >= date_from)
            )

        if date_to:
            query_builder = query_builder.filter(
                (EmailMessage.received_at <= date_to) |
                (EmailMessage.sent_at <= date_to)
            )

        # Boolean filters
        if is_read is not None:
            query_builder = query_builder.filter(EmailMessage.is_read == is_read)

        if is_starred is not None:
            query_builder = query_builder.filter(EmailMessage.is_starred == is_starred)

        if is_important is not None:
            query_builder = query_builder.filter(EmailMessage.is_important == is_important)

        if direction is not None:
            query_builder = query_builder.filter(EmailMessage.direction == direction)

        if has_attachments is not None:
            if has_attachments:
                query_builder = query_builder.filter(EmailMessage.has_attachments == True)
            else:
                query_builder = query_builder.filter(
                    (EmailMessage.has_attachments == False) |
                    (EmailMessage.has_attachments.is_(None))
                )

        # Get total count before pagination
        total = query_builder.count()

        # Apply pagination and ordering
        messages = (
            query_builder
            .order_by(desc(EmailMessage.received_at), desc(EmailMessage.sent_at))
            .offset(skip)
            .limit(limit)
            .all()
        )

        logger.info(f"[EmailInbox] Found {total} matching emails, returning {len(messages)} results")
        return messages, total

    # ==================== STORAGE MANAGEMENT ====================

    def get_storage_quota(self, candidate_id: int) -> Optional[StorageQuota]:
        """Get storage quota for candidate"""
        return self.db.query(StorageQuota).filter(
            StorageQuota.candidate_id == candidate_id
        ).first()
