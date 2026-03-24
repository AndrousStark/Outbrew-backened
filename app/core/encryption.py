"""
Encryption utilities for sensitive data storage.

Provides secure encryption for sensitive data like email passwords
using Fernet symmetric encryption.
"""

import os
import base64
import hashlib
import logging
from cryptography.fernet import Fernet, InvalidToken
from typing import Optional

logger = logging.getLogger(__name__)


def _get_encryption_key() -> bytes:
    """
    Get or generate encryption key from environment.

    The key is derived from SECRET_KEY to maintain single secret management.
    """
    from app.core.config import settings

    # Require dedicated ENCRYPTION_KEY — never fall back to SECRET_KEY
    if not settings.ENCRYPTION_KEY:
        raise RuntimeError(
            "ENCRYPTION_KEY not set. Set it in .env or environment variables. "
            "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
    key_source = settings.ENCRYPTION_KEY
    secret = key_source.encode()
    # Use SHA256 to get 32 bytes, then base64 encode for Fernet
    key = base64.urlsafe_b64encode(hashlib.sha256(secret).digest())
    return key


def get_cipher() -> Fernet:
    """Get Fernet cipher instance."""
    return Fernet(_get_encryption_key())


def encrypt_value(value: str) -> str:
    """
    Encrypt a string value.

    Args:
        value: Plain text string to encrypt

    Returns:
        Encrypted string (base64 encoded)
    """
    if not value:
        return value

    try:
        cipher = get_cipher()
        encrypted = cipher.encrypt(value.encode())
        return encrypted.decode()
    except Exception as e:
        logger.error(f"[Encryption] Failed to encrypt value: {e}")
        raise ValueError("Failed to encrypt value")


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt an encrypted string value.

    Args:
        encrypted_value: Encrypted string (base64 encoded)

    Returns:
        Decrypted plain text string
    """
    if not encrypted_value:
        return encrypted_value

    try:
        cipher = get_cipher()
        decrypted = cipher.decrypt(encrypted_value.encode())
        return decrypted.decode()
    except InvalidToken:
        # Only return raw value if it clearly isn't encrypted (legacy plaintext data)
        if not is_encrypted(encrypted_value):
            logger.warning("[Encryption] Value is plaintext (legacy data) — not encrypted")
            return encrypted_value
        # Value looks encrypted but failed decryption — key mismatch or corruption
        logger.error("[Encryption] Decryption failed for encrypted value — possible key mismatch")
        raise ValueError("Failed to decrypt value — encryption key may have changed")
    except Exception as e:
        logger.error(f"[Encryption] Failed to decrypt value: {e}")
        raise ValueError("Failed to decrypt value")


def is_encrypted(value: str) -> bool:
    """
    Check if a value appears to be encrypted.

    Encrypted values are Fernet tokens which have a specific format:
    - Base64 encoded
    - Start with 'gAAAAA' (Fernet token prefix)

    Args:
        value: String to check

    Returns:
        True if the value appears to be encrypted
    """
    if not value:
        return False

    # Fernet tokens are base64 encoded and start with a specific prefix
    try:
        # Fernet tokens are at least 85 characters
        if len(value) < 85:
            return False
        # Fernet tokens can be decoded as base64
        base64.urlsafe_b64decode(value)
        # Fernet tokens start with 'gAAAAA'
        return value.startswith('gAAAAA')
    except Exception:
        return False
