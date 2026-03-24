"""
Email Inbox API Endpoints

Provides email inbox integration functionality including:
- Email account management (connect/disconnect)
- Inbox syncing (IMAP)
- Thread and message viewing
- Read/unread/starred status management
- Storage quota tracking
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel, EmailStr, Field
from datetime import datetime

logger = logging.getLogger(__name__)

from app.api.dependencies import get_db, get_current_candidate
from app.models.candidate import Candidate
from app.models.email_inbox import (
    EmailAccountType, EmailDirection, EmailSyncStatus
)
from app.services.email_inbox_service import EmailInboxService

router = APIRouter()


# ==================== SCHEMAS ====================

class EmailAccountCreate(BaseModel):
    """Schema for creating email account"""
    email_address: EmailStr
    account_type: EmailAccountType
    imap_host: Optional[str] = None
    imap_port: int = 993
    imap_username: Optional[str] = None
    imap_password: str
    display_name: Optional[str] = None


class EmailAccountResponse(BaseModel):
    """Schema for email account response"""
    id: int
    email_address: str
    account_type: EmailAccountType
    display_name: str
    sync_enabled: bool
    sync_status: EmailSyncStatus
    last_sync_at: Optional[datetime]
    total_emails_synced: int
    is_primary: bool
    created_at: datetime

    class Config:
        from_attributes = True


class EmailMessageResponse(BaseModel):
    """Schema for email message response"""
    id: int
    direction: EmailDirection
    from_email: str
    from_name: Optional[str]
    to_email: str
    to_name: Optional[str]
    subject: Optional[str]
    snippet: Optional[str]
    is_read: bool
    is_starred: bool
    is_important: bool
    sent_at: Optional[datetime]
    received_at: Optional[datetime]
    thread_id: str
    size_bytes: Optional[int]

    class Config:
        from_attributes = True


class EmailMessageDetailResponse(EmailMessageResponse):
    """Schema for detailed email message"""
    body_text: Optional[str]
    body_html: Optional[str]
    file_path: Optional[str]


class EmailThreadResponse(BaseModel):
    """Schema for email thread"""
    id: int
    thread_id: str
    subject: Optional[str]
    message_count: int
    unread_count: int
    is_starred: bool
    latest_message_at: datetime
    latest_snippet: Optional[str]

    class Config:
        from_attributes = True


class StorageQuotaResponse(BaseModel):
    """Schema for storage quota"""
    candidate_id: int
    quota_limit: int
    used_bytes: int
    resumes_bytes: int
    emails_bytes: int
    documents_bytes: int
    templates_bytes: int
    total_files: int
    usage_percentage: float
    remaining_bytes: int
    is_over_quota: bool

    class Config:
        from_attributes = True


class SyncResponse(BaseModel):
    """Schema for sync operation response"""
    account_id: int
    emails_fetched: int
    emails_saved: int
    threads_created: int
    errors: List[str]


# ==================== EMAIL ACCOUNT ENDPOINTS ====================

@router.post("/accounts", response_model=EmailAccountResponse, status_code=status.HTTP_201_CREATED)
def connect_email_account(
    account_data: EmailAccountCreate,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Connect a new email account for inbox integration

    Supports:
    - Gmail (requires app password)
    - Outlook
    - Yahoo
    - Generic IMAP
    """
    service = EmailInboxService(db)

    try:
        account = service.create_email_account(
            candidate_id=current_user.id,
            email_address=account_data.email_address,
            account_type=account_data.account_type,
            imap_host=account_data.imap_host,
            imap_port=account_data.imap_port,
            imap_username=account_data.imap_username,
            imap_password=account_data.imap_password,
            display_name=account_data.display_name
        )
        return account
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to connect email account: {e}")
        raise HTTPException(status_code=500, detail="Failed to connect email account")


@router.get("/accounts", response_model=List[EmailAccountResponse])
def list_email_accounts(
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Get all connected email accounts for the current user
    """
    service = EmailInboxService(db)
    accounts = service.get_email_accounts(current_user.id)
    return accounts


@router.delete("/accounts/{account_id}", status_code=status.HTTP_204_NO_CONTENT)
def disconnect_email_account(
    account_id: int,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Disconnect an email account
    """
    service = EmailInboxService(db)
    success = service.delete_email_account(account_id, current_user.id)

    if not success:
        raise HTTPException(status_code=404, detail="Email account not found")

    return None


# ==================== INBOX SYNC ENDPOINTS ====================

@router.post("/accounts/{account_id}/sync", response_model=SyncResponse)
def sync_inbox(
    account_id: int,
    limit: int = Query(50, ge=1, le=200, description="Max emails to fetch"),
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Sync inbox from IMAP server

    Fetches recent emails and saves them to the database.
    Does NOT save attachments from received emails.
    """
    service = EmailInboxService(db)

    try:
        result = service.sync_inbox(account_id, limit=limit)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise HTTPException(status_code=500, detail="Sync failed")


# ==================== THREAD & MESSAGE ENDPOINTS ====================

@router.get("/threads", response_model=List[EmailThreadResponse])
def list_threads(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    unread_only: bool = Query(False, description="Show only unread threads"),
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Get email threads (inbox view)

    Returns threads sorted by latest message first.
    """
    service = EmailInboxService(db)
    threads = service.get_inbox_threads(
        candidate_id=current_user.id,
        skip=skip,
        limit=limit,
        unread_only=unread_only
    )
    return threads


@router.get("/threads/{thread_id}/messages", response_model=List[EmailMessageResponse])
def get_thread_messages(
    thread_id: str,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Get all messages in a thread (conversation view)

    Returns messages in chronological order.
    """
    service = EmailInboxService(db)
    messages = service.get_thread_messages(thread_id, current_user.id)
    return messages


@router.get("/messages/{message_id}", response_model=EmailMessageDetailResponse)
def get_message_detail(
    message_id: int,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Get detailed email message including full body
    """
    service = EmailInboxService(db)
    message = service.get_email_message(message_id, current_user.id)

    if not message:
        raise HTTPException(status_code=404, detail="Message not found")

    return message


# ==================== MESSAGE ACTIONS ====================

@router.patch("/messages/{message_id}/read", status_code=status.HTTP_204_NO_CONTENT)
def mark_message_read(
    message_id: int,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Mark a message as read
    """
    service = EmailInboxService(db)
    service.mark_as_read(message_id, current_user.id)
    return None


@router.patch("/messages/{message_id}/star", response_model=dict)
def toggle_message_starred(
    message_id: int,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Toggle starred status of a message
    """
    service = EmailInboxService(db)
    is_starred = service.toggle_starred(message_id, current_user.id)
    return {"is_starred": is_starred}


# ==================== STORAGE QUOTA ENDPOINTS ====================

@router.get("/search", response_model=dict)
def search_emails(
    query: Optional[str] = Query(None, description="Full-text search query"),
    from_email: Optional[str] = Query(None, description="Filter by sender email"),
    to_email: Optional[str] = Query(None, description="Filter by recipient email"),
    subject: Optional[str] = Query(None, description="Filter by subject"),
    date_from: Optional[datetime] = Query(None, description="Filter from date (ISO format)"),
    date_to: Optional[datetime] = Query(None, description="Filter to date (ISO format)"),
    is_read: Optional[bool] = Query(None, description="Filter by read status"),
    is_starred: Optional[bool] = Query(None, description="Filter by starred status"),
    is_important: Optional[bool] = Query(None, description="Filter by important status"),
    direction: Optional[EmailDirection] = Query(None, description="Filter by direction (SENT/RECEIVED)"),
    has_attachments: Optional[bool] = Query(None, description="Filter emails with attachments"),
    skip: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=100, description="Results per page"),
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Search emails with advanced filters

    **Search Capabilities:**
    - Full-text search across subject, sender, recipient, and body
    - Filter by specific fields (sender, recipient, subject)
    - Date range filtering
    - Status filters (read/unread, starred, important)
    - Direction filter (sent/received)
    - Attachment filter

    **Examples:**
    - Search for all emails from john@example.com: `?from_email=john@example.com`
    - Search unread emails: `?is_read=false`
    - Search emails with "invoice" in subject: `?subject=invoice`
    - Full-text search: `?query=project deadline`
    - Combined search: `?query=meeting&from_email=boss&date_from=2026-01-01`

    **Returns:**
    - `emails`: List of matching email messages
    - `total`: Total count of matching emails
    - `page`: Current page number (skip / limit + 1)
    - `pages`: Total number of pages
    """
    service = EmailInboxService(db)

    messages, total = service.search_emails(
        candidate_id=current_user.id,
        query=query,
        from_email=from_email,
        to_email=to_email,
        subject=subject,
        date_from=date_from,
        date_to=date_to,
        is_read=is_read,
        is_starred=is_starred,
        is_important=is_important,
        direction=direction,
        has_attachments=has_attachments,
        skip=skip,
        limit=limit
    )

    # Calculate pagination info
    pages = (total + limit - 1) // limit if total > 0 else 0
    current_page = (skip // limit) + 1 if limit > 0 else 1

    return {
        "emails": [
            {
                "id": msg.id,
                "direction": msg.direction,
                "from_email": msg.from_email,
                "from_name": msg.from_name,
                "to_email": msg.to_email,
                "to_name": msg.to_name,
                "subject": msg.subject,
                "snippet": msg.snippet,
                "is_read": msg.is_read,
                "is_starred": msg.is_starred,
                "is_important": msg.is_important,
                "sent_at": msg.sent_at,
                "received_at": msg.received_at,
                "thread_id": msg.thread_id,
                "has_attachments": msg.has_attachments,
                "size_bytes": msg.size_bytes
            }
            for msg in messages
        ],
        "total": total,
        "page": current_page,
        "pages": pages,
        "limit": limit,
        "showing": len(messages)
    }


@router.get("/storage/quota", response_model=StorageQuotaResponse)
def get_storage_quota(
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db)
):
    """
    Get storage quota information

    Shows:
    - Total quota limit
    - Used space by category
    - Remaining space
    - Usage percentage
    """
    service = EmailInboxService(db)
    quota = service.get_storage_quota(current_user.id)

    if not quota:
        raise HTTPException(status_code=404, detail="Storage quota not found")

    return quota
