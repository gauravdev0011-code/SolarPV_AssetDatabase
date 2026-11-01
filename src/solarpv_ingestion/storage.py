"""Private local artifact storage contract and filesystem implementation."""

import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import Protocol


class ArtifactStorage(Protocol):
    """Storage operations used by ingestion artifacts, addressed by safe key."""

    def put_bytes(self, key: str, content: bytes) -> None: ...

    def read_bytes(self, key: str) -> bytes: ...


class LocalArtifactStorage:
    """Store artifacts beneath an explicitly configured private directory."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()

    def _resolve_key(self, key: str) -> Path:
        candidate_key = PurePosixPath(key)
        if (
            not key
            or candidate_key.is_absolute()
            or any(part in {"", ".", ".."} for part in candidate_key.parts)
            or "\\" in key
        ):
            raise ValueError("Artifact key must be a safe relative POSIX path.")

        candidate = (self._root / Path(*candidate_key.parts)).resolve()
        if candidate != self._root and self._root not in candidate.parents:
            raise ValueError("Artifact key escapes the configured storage root.")
        return candidate

    def put_bytes(self, key: str, content: bytes) -> None:
        if not isinstance(content, bytes):
            raise TypeError("Artifact content must be bytes.")
        target = self._resolve_key(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=target.parent, prefix=f".{target.name}.",
                suffix=".partial", delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
            # A hard link publishes atomically and fails if an immutable key exists.
            os.link(temporary_path, target)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def read_bytes(self, key: str) -> bytes:
        return self._resolve_key(key).read_bytes()
