from __future__ import annotations

import fcntl
import json
import os
import tempfile
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path


DEFAULT_LOCK_TIMEOUT_SECONDS = 60.0


def read_json_object(path: Path) -> dict[str, object]:
    """Read a JSON object, returning an empty projection for absent or invalid state."""

    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        directory_fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        pass
    finally:
        os.close(directory_fd)


def atomic_write_json_secure(path: Path, payload: Mapping[str, object]) -> None:
    """Atomically replace a JSON state file and keep it owner-readable only."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temp_path = Path(temp_name)
    try:
        os.fchmod(file_descriptor, 0o600)
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            file_descriptor = -1
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
        temp_path.unlink(missing_ok=True)


def _flock_with_timeout(file_descriptor: int, lock_path: Path, timeout_seconds: float) -> None:
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    delay_seconds = 0.05
    while True:
        try:
            fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"timed out after {timeout_seconds}s waiting for state lock {lock_path}"
                ) from None
            time.sleep(min(delay_seconds, remaining))
            delay_seconds = min(delay_seconds * 2, 1.0)


@contextmanager
def exclusive_state_lock(
    path: Path,
    timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    """Hold an advisory lock across a complete state read-modify-write cycle.

    Waiting is bounded: a wedged lock holder raises TimeoutError after
    *timeout_seconds* instead of blocking the caller forever.
    """

    path = Path(path)
    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    os.fchmod(file_descriptor, 0o600)
    with os.fdopen(file_descriptor, "a+", encoding="utf-8") as handle:
        _flock_with_timeout(handle.fileno(), lock_path, timeout_seconds)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
