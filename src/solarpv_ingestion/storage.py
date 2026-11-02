"""Private local storage with cross-process upload leases and atomic publish."""

import hashlib
import os
import re
import threading
import time
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Callable, Protocol
from uuid import uuid4

from solarpv_ingestion.models import StagedFile


_PROCESS_LOCK_GUARD = threading.Lock()
_PROCESS_LOCKED_PATHS: set[Path] = set()


class ArtifactStorage(Protocol):
    """Storage operations addressed by safe relative keys."""

    def put_bytes(self, key: str, content: bytes) -> None: ...

    def read_bytes(self, key: str) -> bytes: ...


class StagingLock:
    """Advisory OS file lock held for the lifetime of one active upload."""

    def __init__(self, path: Path, *, blocking: bool) -> None:
        self.path = path.resolve()
        with _PROCESS_LOCK_GUARD:
            if self.path in _PROCESS_LOCKED_PATHS:
                raise BlockingIOError("Staging file is already active in this process.")
            _PROCESS_LOCKED_PATHS.add(self.path)
        self._stream = None
        self._locked = False
        try:
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            self._stream = self.path.open("a+b")
            if self._stream.seek(0, os.SEEK_END) == 0:
                self._stream.write(b"\0")
                self._stream.flush()
            self._stream.seek(0)
            if os.name == "nt":
                import msvcrt

                mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
                msvcrt.locking(self._stream.fileno(), mode, 1)
            else:
                import fcntl

                flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
                fcntl.flock(self._stream.fileno(), flags)
            self._locked = True
        except OSError:
            if self._stream is not None:
                self._stream.close()
            with _PROCESS_LOCK_GUARD:
                _PROCESS_LOCKED_PATHS.discard(self.path)
            raise

    def release(self) -> None:
        if not self._locked:
            return
        try:
            self._stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
        finally:
            self._locked = False
            self._stream.close()
            with _PROCESS_LOCK_GUARD:
                _PROCESS_LOCKED_PATHS.discard(self.path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


class LocalArtifactStorage:
    """Store immutable files beneath an explicitly configured private root."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()
        self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(self._root, 0o700)
        except OSError:
            pass
        self._staging = self._root / ".staging"
        self._staging.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(self._staging, 0o700)
        except OSError:
            pass
        self._active_locks: dict[Path, StagingLock] = {}

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

    def _upload_paths(self, upload_id: str) -> tuple[Path, Path]:
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", upload_id):
            raise ValueError("Upload ID contains unsafe characters.")
        return (
            self._resolve_key(f".staging/{upload_id}.partial"),
            self._resolve_key(f".staging/{upload_id}.lock"),
        )

    def staging_key(self, upload_id: str) -> str:
        self._upload_paths(upload_id)
        return f".staging/{upload_id}.partial"

    def acquire_upload_lock(self, upload_id: str) -> StagingLock | None:
        temporary_path, _ = self._upload_paths(upload_id)
        lock = self.try_lock_upload(upload_id)
        if lock is not None:
            self._active_locks[temporary_path] = lock
        return lock

    def release_upload_lock(self, upload_id: str) -> None:
        temporary_path, _ = self._upload_paths(upload_id)
        lock = self._active_locks.pop(temporary_path, None)
        if lock is not None:
            lock.release()

    def try_lock_upload(self, upload_id: str) -> StagingLock | None:
        _, lock_path = self._upload_paths(upload_id)
        try:
            return StagingLock(lock_path, blocking=False)
        except OSError:
            return None

    def stage_stream(
        self,
        stream: BinaryIO,
        *,
        max_bytes: int,
        chunk_size: int = 1024 * 1024,
        upload_id: str | None = None,
        on_progress: Callable[[int], None] | None = None,
    ) -> StagedFile:
        """Write bounded chunks under an OS lock and return a durable stage key."""
        if max_bytes <= 0 or chunk_size <= 0:
            raise ValueError("Upload limits must be positive.")
        identity = upload_id or uuid4().hex
        temporary_path, lock_path = self._upload_paths(identity)
        lock = StagingLock(lock_path, blocking=False)
        self._active_locks[temporary_path] = lock
        size = 0
        digest = hashlib.sha256()
        last_progress = time.monotonic()
        try:
            descriptor = os.open(
                temporary_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
            )
            with os.fdopen(descriptor, "wb") as temporary:
                while True:
                    chunk = stream.read(chunk_size)
                    if chunk == b"":
                        break
                    if not isinstance(chunk, bytes):
                        raise TypeError("Upload stream must return bytes.")
                    size += len(chunk)
                    if size > max_bytes:
                        raise UploadTooLargeError(max_bytes)
                    digest.update(chunk)
                    temporary.write(chunk)
                    if on_progress is not None and time.monotonic() - last_progress >= 10:
                        on_progress(size)
                        last_progress = time.monotonic()
                temporary.flush()
                os.fsync(temporary.fileno())
            return StagedFile(temporary_path, size, digest.hexdigest(), identity)
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            self._active_locks.pop(temporary_path, None)
            lock.release()
            raise

    def publish(self, staged: StagedFile, key: str) -> None:
        """Atomically publish without replacing an existing immutable key."""
        if staged.upload_id and staged.path not in self._active_locks:
            raise StoragePublicationError("Upload staging lock is not held by this process.")
        target = self._resolve_key(key)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(target.parent, 0o700)
        except OSError:
            pass
        try:
            os.link(staged.path, target)
        except FileExistsError:
            raise FileExistsError(f"Storage key already exists: {key}") from None
        except OSError as error:
            raise StoragePublicationError("Could not atomically publish upload.") from error
        try:
            staged.path.unlink(missing_ok=True)
        except OSError:
            # The published file is durable; recovery can remove the staged link.
            pass

    def discard(self, staged: StagedFile) -> None:
        try:
            staged.path.unlink(missing_ok=True)
        finally:
            self.release_staged(staged)

    def release_staged(self, staged: StagedFile) -> None:
        lock = self._active_locks.pop(staged.path, None)
        if lock is not None:
            lock.release()

    def cleanup_stale_staging(
        self,
        *,
        authorize_delete: Callable[[str], bool],
        older_than_seconds: int = 24 * 60 * 60,
        now: float | None = None,
    ) -> int:
        """Delete old stages only after acquiring their cross-process lock and
        rechecking the owning intake's persisted state through the callback.
        """
        if older_than_seconds < 0:
            raise ValueError("older_than_seconds cannot be negative.")
        cutoff = (time.time() if now is None else now) - older_than_seconds
        removed = 0
        for candidate in self._staging.glob("*.partial"):
            upload_id = candidate.stem
            if candidate in self._active_locks:
                continue
            lock = self.try_lock_upload(upload_id)
            if lock is None:
                continue
            self._active_locks[candidate] = lock
            try:
                try:
                    if candidate.stat().st_mtime > cutoff:
                        continue
                except FileNotFoundError:
                    continue
                if authorize_delete(upload_id):
                    try:
                        candidate.unlink()
                        removed += 1
                    except FileNotFoundError:
                        pass
            finally:
                self._active_locks.pop(candidate, None)
                lock.release()
        return removed

    def exists(self, key: str) -> bool:
        return self._resolve_key(key).is_file()

    def matches(self, key: str, *, sha256: str, size_bytes: int) -> bool:
        digest = hashlib.sha256()
        size = 0
        try:
            with self._resolve_key(key).open("rb") as content:
                while chunk := content.read(1024 * 1024):
                    size += len(chunk)
                    digest.update(chunk)
        except OSError:
            return False
        return size == size_bytes and digest.hexdigest() == sha256

    def delete_uncommitted(self, key: str) -> None:
        self._resolve_key(key).unlink(missing_ok=True)

    def read_bytes(self, key: str) -> bytes:
        return self._resolve_key(key).read_bytes()

    def open_read(self, key: str) -> BinaryIO:
        """Open a stored object for bounded, read-only streaming."""
        return self._resolve_key(key).open("rb")

    def put_bytes(self, key: str, content: bytes) -> None:
        if not isinstance(content, bytes):
            raise TypeError("Artifact content must be bytes.")
        import io

        staged = self.stage_stream(io.BytesIO(content), max_bytes=max(len(content), 1))
        try:
            self.publish(staged, key)
        finally:
            self.discard(staged)


class UploadTooLargeError(ValueError):
    """Raised when a stream exceeds the configured maximum size."""

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        super().__init__(f"Upload exceeds the maximum size of {max_bytes} bytes.")


class StoragePublicationError(OSError):
    """Raised when an upload could not be atomically published."""
