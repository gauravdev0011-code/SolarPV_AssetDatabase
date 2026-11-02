"""Explicit local API and worker configuration."""

from dataclasses import dataclass
import os
from pathlib import Path

@dataclass(frozen=True, slots=True)
class Settings:
    database_path: Path
    storage_path: Path
    max_file_bytes: int = 100 * 1024 * 1024
    max_json_bytes: int = 5 * 1024 * 1024
    lease_seconds: int = 90
    retry_delay_seconds: int = 5
    worker_poll_seconds: float = 1.0

    @classmethod
    def from_environment(cls) -> "Settings":
        app_data = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "SolarPV"
        default_database = app_data / "solarpv.db"
        default_storage = app_data / "private-storage"
        values = cls(
            Path(os.environ.get("SOLARPV_DB_PATH", str(default_database))).expanduser().resolve(),
            Path(os.environ.get("SOLARPV_STORAGE_PATH", str(default_storage))).expanduser().resolve(),
            int(os.environ.get("SOLARPV_MAX_FILE_BYTES", str(100 * 1024 * 1024))),
            int(os.environ.get("SOLARPV_MAX_JSON_BYTES", str(5 * 1024 * 1024))),
            int(os.environ.get("SOLARPV_LEASE_SECONDS", "90")),
            int(os.environ.get("SOLARPV_RETRY_DELAY_SECONDS", "5")),
            float(os.environ.get("SOLARPV_WORKER_POLL_SECONDS", "1")),
        )
        if min(values.max_file_bytes, values.max_json_bytes, values.lease_seconds) < 1:
            raise ValueError("Configured file, JSON, and lease limits must be positive.")
        if values.retry_delay_seconds < 0 or values.worker_poll_seconds <= 0:
            raise ValueError("Retry delay must be nonnegative and worker poll interval positive.")
        return values
