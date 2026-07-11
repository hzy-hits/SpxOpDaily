from __future__ import annotations

import fcntl
import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class ExclusiveLockUnavailable(RuntimeError):
    pass


def token_owner_lock_path(token_path: str | os.PathLike[str]) -> Path:
    path = Path(token_path).expanduser()
    return path.with_name(f"{path.name}.owner.lock")


class ExclusiveFileLock:
    """A process-lifetime flock used to enforce one Schwab refresh owner."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path).expanduser()
        self._descriptor: int | None = None

    @contextmanager
    def held(self) -> Iterator["ExclusiveFileLock"]:
        self.acquire()
        try:
            yield self
        finally:
            self.release()

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise RuntimeError(f"Lock is already held by this object: {self.path}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.path, flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise ExclusiveLockUnavailable(f"Lock is held: {self.path}") from exc
        except Exception:
            os.close(descriptor)
            raise
        self._descriptor = descriptor

    def release(self) -> None:
        if self._descriptor is None:
            return
        try:
            fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        finally:
            os.close(self._descriptor)
            self._descriptor = None


class AtomicJsonFile:
    """A small locked JSON store for OAuth state and token metadata."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path).expanduser()
        self.lock_path = self.path.with_name(f"{self.path.name}.lock")

    @property
    def exists(self) -> bool:
        return self.path.is_file()

    @contextmanager
    def locked(self) -> Iterator["AtomicJsonFile"]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.lock_path, flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "r+") as handle:
                descriptor = -1
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                yield self
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def read(self) -> dict[str, Any]:
        with self.locked():
            return self.read_unlocked()

    def read_unlocked(self) -> dict[str, Any]:
        if self.path.is_symlink():
            raise ValueError(f"Refusing to read OAuth data through symlink: {self.path}")
        with self.path.open(encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            raise ValueError(f"OAuth JSON must be an object: {self.path}")
        return value

    def write(self, value: dict[str, Any], *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        with self.locked():
            self.write_unlocked(value)

    def write_unlocked(self, value: dict[str, Any], *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        if not isinstance(value, dict):
            raise TypeError("OAuth JSON writer requires an object")
        if self.path.is_symlink():
            raise ValueError(f"Refusing to replace OAuth data through symlink: {self.path}")

        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(
            dir=self.path.parent,
            prefix=f".{self.path.name}.",
            suffix=".tmp",
        )
        temp_path = Path(temp_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                descriptor = -1
                json.dump(value, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
            fsync_directory(self.path.parent)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temp_path.unlink(missing_ok=True)

    def delete(self) -> None:
        with self.locked():
            self.delete_unlocked()

    def delete_unlocked(self) -> None:
        if self.path.is_symlink():
            raise ValueError(f"Refusing to delete OAuth data through symlink: {self.path}")
        existed = self.path.exists()
        self.path.unlink(missing_ok=True)
        if existed:
            fsync_directory(self.path.parent)


def fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
