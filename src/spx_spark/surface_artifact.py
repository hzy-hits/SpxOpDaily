"""Canonical JSON-value hashing shared by surface artifacts."""

from __future__ import annotations

import hashlib
import math
import struct


def canonical_bytes(value: object) -> bytes:
    """Encode the supported JSON value subset without text-format ambiguity."""

    if value is None:
        return b"n;"
    if isinstance(value, bool):
        return b"b1;" if value else b"b0;"
    if isinstance(value, int | float):
        if isinstance(value, int) and abs(value) > 2**53 - 1:
            raise ValueError("canonical integer exceeds IEEE-754 safe range")
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("canonical number must be finite")
        return b"f" + struct.pack(">d", number).hex().encode("ascii") + b";"
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return b"s" + str(len(encoded)).encode("ascii") + b":" + encoded
    if isinstance(value, list | tuple):
        parts = [canonical_bytes(item) for item in value]
        return b"a" + str(len(parts)).encode("ascii") + b"[" + b"".join(parts) + b"]"
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("canonical object keys must be strings")
        keys = sorted(value)
        parts = [canonical_bytes(key) + canonical_bytes(value[key]) for key in keys]
        return b"o" + str(len(parts)).encode("ascii") + b"{" + b"".join(parts) + b"}"
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


__all__ = ("canonical_bytes", "canonical_sha256")
