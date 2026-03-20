"""API key encryption using machine-tied Fernet.

Provides host-based encryption for sensitive config values like API keys.
Uses PBKDF2-SHA256 with machine-specific key material to derive a Fernet key.

Graceful degradation: if the `cryptography` package is not installed,
values are stored and returned as plaintext with a warning on first use.
"""

import getpass
import hashlib
import logging
import platform
import socket
from pathlib import Path

logger = logging.getLogger(__name__)

SENSITIVE_FIELDS = frozenset({"sync_access_key", "sync_secret_key"})

# Static salt for PBKDF2 derivation (not secret, just prevents rainbow tables)
_PBKDF2_SALT = b"hyperkb-config-encryption-v1"
_PBKDF2_ITERATIONS = 480_000
_ENC_PREFIX = "enc:"

_warned_no_cryptography = False


def is_sensitive_field(name: str) -> bool:
    """Check if a config field name contains sensitive data."""
    return name in SENSITIVE_FIELDS


def _get_machine_key_material() -> str:
    """Get machine-specific key material for encryption.

    Tries (in order):
      1. /etc/machine-id (Linux)
      2. IOPlatformUUID (macOS)
      3. hostname + username fallback
    """
    # Linux: /etc/machine-id
    machine_id = Path("/etc/machine-id")
    if machine_id.exists():
        try:
            return machine_id.read_text().strip()
        except OSError:
            pass

    # macOS: IOPlatformUUID
    if platform.system() == "Darwin":
        try:
            import subprocess
            result = subprocess.run(
                ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.splitlines():
                if "IOPlatformUUID" in line:
                    return line.split('"')[-2]
        except (OSError, subprocess.TimeoutExpired, IndexError):
            pass

    # Fallback: hostname + username
    return f"{socket.gethostname()}:{getpass.getuser()}"


def _derive_fernet_key(key_material: str) -> bytes:
    """Derive a Fernet key from machine key material using PBKDF2-SHA256."""
    import base64
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        key_material.encode("utf-8"),
        _PBKDF2_SALT,
        _PBKDF2_ITERATIONS,
    )
    # Fernet requires 32 bytes, URL-safe base64 encoded
    return base64.urlsafe_b64encode(dk)


def encrypt_value(value: str) -> str:
    """Encrypt a value for storage. Returns 'enc:...' format.

    If the value is empty or already encrypted, returns it unchanged.
    If cryptography is not installed, returns the value as-is with a warning.
    """
    if not value or value.startswith(_ENC_PREFIX):
        return value

    try:
        from cryptography.fernet import Fernet
    except ImportError:
        global _warned_no_cryptography
        if not _warned_no_cryptography:
            logger.warning(
                "cryptography package not installed. "
                "API keys will be stored in plaintext. "
                "Install with: pip install 'hyperkb[crypto]'"
            )
            _warned_no_cryptography = True
        return value

    key_material = _get_machine_key_material()
    fernet_key = _derive_fernet_key(key_material)
    f = Fernet(fernet_key)
    encrypted = f.encrypt(value.encode("utf-8"))
    return f"{_ENC_PREFIX}{encrypted.decode('ascii')}"


def decrypt_value(value: str) -> str:
    """Decrypt an 'enc:...' value. Passes through plaintext unchanged.

    If cryptography is not installed and value is encrypted, raises RuntimeError.
    """
    if not value or not value.startswith(_ENC_PREFIX):
        return value

    try:
        from cryptography.fernet import Fernet
    except ImportError:
        raise RuntimeError(
            "Cannot decrypt API key: cryptography package not installed. "
            "Install with: pip install 'hyperkb[crypto]'"
        )

    encrypted_data = value[len(_ENC_PREFIX):]
    key_material = _get_machine_key_material()
    fernet_key = _derive_fernet_key(key_material)
    f = Fernet(fernet_key)
    return f.decrypt(encrypted_data.encode("ascii")).decode("utf-8")


def mask_value(value: str) -> str:
    """Mask a sensitive value for display. Shows '****last4'.

    Returns '(not set)' for empty values.
    """
    if not value:
        return "(not set)"

    # Decrypt if needed for masking
    try:
        plain = decrypt_value(value)
    except (RuntimeError, Exception) as e:
        logger.warning("Decryption failed during masking, using raw value: %s", e)
        plain = value

    if len(plain) <= 4:
        return "****"
    return f"****{plain[-4:]}"
