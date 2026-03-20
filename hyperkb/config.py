"""Configuration management for hyperkb.

KB lives at ~/.hkb/ (global). Use --path to override for testing or
non-standard setups.
"""

import json
import os
from dataclasses import dataclass, asdict, fields
from pathlib import Path
from typing import Optional

from .crypto import encrypt_value, decrypt_value, is_sensitive_field

HKB_DIR = ".hkb"
STORAGE_DIR = "storage"
CONFIG_FILENAME = "config.json"
DB_FILENAME = "index.db"
MAX_FILENAME_SEGMENTS = 4
MIN_FILENAME_SEGMENTS = 2

def _global_hkb_dir() -> Path:
    """Compute global HKB dir at runtime (not import time) for testability."""
    return Path.home() / HKB_DIR

# Environment variable overrides for sensitive fields
_ENV_OVERRIDES = {
    "sync_access_key": "HKB_SYNC_ACCESS_KEY",
    "sync_secret_key": "HKB_SYNC_SECRET_KEY",
    "default_source": "HKB_SOURCE",
}


@dataclass
class KBConfig:
    """Knowledge base configuration."""
    root: str = ""
    # Search tuning
    rg_weight: float = 0.5
    bm25_weight: float = 0.5
    # Confidence threshold for auto-routing content to files
    route_confidence_threshold: float = 0.6
    # Timeouts (seconds)
    rg_timeout: float = 10.0
    # Max entry size in bytes (1 MiB default)
    max_entry_size: int = 1_048_576
    # Recency half-life for search scoring (days)
    recency_half_life_days: int = 180
    # Sync settings
    sync_enabled: bool = False
    sync_bucket: str = ""
    sync_prefix: str = "hkb/"
    sync_region: str = ""
    sync_endpoint_url: str = ""  # For MinIO/custom S3-compatible storage
    sync_access_key: str = ""
    sync_secret_key: str = ""
    sync_interval: int = 60  # seconds between background sync checks
    sync_squash_threshold: int = 20  # squash git history after this many commits
    # Default source/author for entries (overridden by HKB_SOURCE env var)
    default_source: str = ""
    # Path to the hyperkb git repo for `hkb update`. Auto-detected if empty.
    update_repo: str = ""

    @property
    def hkb_dir(self) -> Path:
        return Path(self.root) / HKB_DIR

    @property
    def storage_dir(self) -> Path:
        return self.hkb_dir / STORAGE_DIR

    @property
    def db_path(self) -> Path:
        return self.hkb_dir / DB_FILENAME

    @property
    def config_path(self) -> Path:
        return self.hkb_dir / CONFIG_FILENAME

    def save(self):
        """Save config to JSON, encrypting sensitive fields."""
        self.hkb_dir.mkdir(parents=True, exist_ok=True)
        data = asdict(self)
        # Encrypt sensitive fields before writing
        for field_name in data:
            if is_sensitive_field(field_name) and data[field_name]:
                data[field_name] = encrypt_value(data[field_name])
        self.config_path.write_text(json.dumps(data, indent=2))

    @classmethod
    def _from_dict(cls, data: dict) -> "KBConfig":
        """Create KBConfig from dict, decrypting sensitive fields and applying env overrides."""
        # Filter to only known fields
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}

        # Decrypt sensitive fields
        for field_name in filtered:
            if is_sensitive_field(field_name) and filtered[field_name]:
                filtered[field_name] = decrypt_value(filtered[field_name])

        cfg = cls(**filtered)

        # Apply environment variable overrides
        for field_name, env_var in _ENV_OVERRIDES.items():
            env_val = os.environ.get(env_var)
            if env_val is not None:
                setattr(cfg, field_name, env_val)

        return cfg

    @classmethod
    def load(cls, root: Optional[str] = None) -> "KBConfig":
        """Load config from the given root, or from the global KB at ~/.hkb/.

        If root is given explicitly, loads from that path.
        Otherwise loads from the global KB directory.
        """
        if root:
            p = Path(root) / HKB_DIR / CONFIG_FILENAME
            if p.exists():
                data = json.loads(p.read_text())
                return cls._from_dict(data)
            raise FileNotFoundError(
                f"No hyperkb found at {root}. Run 'hkb init --path {root}' to create one."
            )
        # Default to global KB
        p = _global_hkb_dir() / CONFIG_FILENAME
        if p.exists():
            data = json.loads(p.read_text())
            return cls._from_dict(data)
        raise FileNotFoundError(
            "No hyperkb found. Run 'hkb init' to create one at ~/.hkb/"
        )
