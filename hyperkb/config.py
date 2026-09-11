"""Configuration management for hyperkb.

KB lives at ~/.hkb/ (global). Use --path to override for testing or
non-standard setups.
"""

import json
import os
import math
import tempfile
from dataclasses import dataclass, asdict, fields, replace
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

    def validate(self):
        """Reject invalid persisted settings before changing running state."""
        for field in fields(self):
            value = getattr(self, field.name)
            expected = type(field.default)
            if expected is float:
                valid = type(value) in (float, int) and math.isfinite(value)
            else:
                valid = type(value) is expected
            if not valid:
                raise ValueError(f"{field.name} must be {expected.__name__}")
        for key in ("rg_weight", "bm25_weight", "route_confidence_threshold"):
            if not 0 <= getattr(self, key) <= 1:
                raise ValueError(f"{key} must be between 0 and 1")
        if self.rg_weight + self.bm25_weight <= 0:
            raise ValueError("At least one search weight must be positive")
        for key in ("rg_timeout", "max_entry_size", "recency_half_life_days",
                    "sync_interval", "sync_squash_threshold"):
            if getattr(self, key) <= 0:
                raise ValueError(f"{key} must be positive")

    def set_value(self, key: str, value: str):
        """Parse, validate and persist a setting without corrupting this instance."""
        known = {f.name for f in fields(self)}
        if key not in known or key == "root":
            raise ValueError(f"Unknown or read-only config key: {key}")
        current = getattr(self, key)
        if isinstance(current, bool):
            text = value.lower()
            if text not in ("true", "false", "1", "0", "yes", "no"):
                raise ValueError(f"{key} must be true or false")
            parsed = text in ("true", "1", "yes")
        else:
            try:
                parsed = type(current)(value)
            except (TypeError, ValueError):
                raise ValueError(f"Invalid value for {key}") from None
        from .locking import storage_lock
        with storage_lock(self.storage_dir):
            # Read the latest settings under the shared mutation lock so two
            # clients changing different keys cannot overwrite one another.
            # Environment overrides belong to a client, never to shared JSON.
            exists = self.config_path.exists()
            raw = json.loads(self.config_path.read_text(encoding="utf-8")) if exists else asdict(self)
            if isinstance(raw, dict):
                raw["root"] = self.root
            # Untouched encrypted credentials may belong to another machine.
            # Validate their serialized types without decrypting or replacing them.
            base = type(self)._from_dict(raw, apply_env=False, decrypt_sensitive=False)
            candidate = replace(base, **{key: parsed})
            candidate.validate()
            persisted = asdict(candidate)
            for field_name, field_value in persisted.items():
                if is_sensitive_field(field_name) and field_value and (field_name == key or not exists):
                    persisted[field_name] = encrypt_value(field_value)
            effective = type(self)._from_dict(persisted)
            self._write_data(persisted)
            for field in fields(self):
                setattr(self, field.name, getattr(effective, field.name))
        return parsed

    def save(self):
        """Validate and atomically save JSON with encrypted sensitive fields."""
        self.validate()
        data = asdict(self)
        for field_name in data:
            if is_sensitive_field(field_name) and data[field_name]:
                data[field_name] = encrypt_value(data[field_name])
        self._write_data(data)

    def _write_data(self, data: dict):
        """Publish already validated/encoded data without transforming credentials."""
        self.hkb_dir.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".config-", suffix=".tmp", dir=self.hkb_dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(data, stream, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.config_path)
            if os.name != "nt":
                directory_fd = os.open(self.hkb_dir, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    @classmethod
    def _from_dict(cls, data: dict, *, apply_env: bool = True,
                   decrypt_sensitive: bool = True) -> "KBConfig":
        """Validate stored values, then select environment overrides before decryption."""
        if not isinstance(data, dict):
            raise ValueError("Config must be a JSON object")
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        # An environment override cannot conceal malformed persisted data.
        cls(**filtered).validate()
        for field_name in known:
            env_var = _ENV_OVERRIDES.get(field_name)
            if apply_env and env_var and env_var in os.environ:
                filtered[field_name] = os.environ[env_var]
            elif decrypt_sensitive and is_sensitive_field(field_name) and filtered.get(field_name):
                filtered[field_name] = decrypt_value(filtered[field_name])
        cfg = cls(**filtered)
        cfg.validate()
        return cfg

    @classmethod
    def load(cls, root: Optional[str] = None, *, apply_env: bool = True) -> "KBConfig":
        """Load from the actual location, allowing an entire KB to be moved."""
        base = Path(root).expanduser().resolve() if root else _global_hkb_dir().parent
        path = base / HKB_DIR / CONFIG_FILENAME
        if not path.exists():
            raise FileNotFoundError(
                f"No hyperkb found at {base}. Run 'hkb init --path {base}' to create one."
            )
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data["root"] = str(base.resolve())
        return cls._from_dict(data, apply_env=apply_env)
