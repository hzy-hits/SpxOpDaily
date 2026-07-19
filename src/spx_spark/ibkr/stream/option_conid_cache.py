"""Durable, expiry-scoped IBKR conId cache for same-session SPXW options."""

from __future__ import annotations

import json
import math
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock


CACHE_FILE_NAME = "ibkr_spxw_conid_cache.json"
CACHE_KIND = "ibkr_spxw_conids"
CACHE_PROVIDER = "ibkr"
CACHE_SCHEMA_VERSION = 2
MAX_CACHE_ENTRIES = 512
MAX_ACTIVE_EXPIRIES = 2
LOCK_TIMEOUT_SECONDS = 1.0

_ROOT_KEYS = frozenset(
    {
        "schema_version",
        "cache_kind",
        "provider",
        "active_expiries",
        "updated_at",
        "contracts",
    }
)
_CONTRACT_KEYS = frozenset(
    {
        "label",
        "con_id",
        "sec_type",
        "symbol",
        "expiry",
        "strike",
        "right",
        "exchange",
        "currency",
        "multiplier",
        "trading_class",
    }
)


@dataclass(frozen=True)
class CachedSpxwContract:
    label: str
    con_id: int
    expiry: str
    strike: int
    right: str

    def to_dict(self) -> dict[str, object]:
        return {
            "label": self.label,
            "con_id": self.con_id,
            "sec_type": "OPT",
            "symbol": "SPX",
            "expiry": self.expiry,
            "strike": self.strike,
            "right": self.right,
            "exchange": "SMART",
            "currency": "USD",
            "multiplier": "100",
            "trading_class": "SPXW",
        }


@dataclass(frozen=True)
class _CacheState:
    active_expiries: frozenset[str]
    entries: dict[str, CachedSpxwContract]


class SpxwConIdCache:
    """Persist exact SPXW identities for the active current/next collection expiries."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._active_expiries: frozenset[str] = frozenset()
        self._entries: dict[str, CachedSpxwContract] = {}
        self._mutex = threading.RLock()

    @property
    def active_expiries(self) -> frozenset[str]:
        return self._active_expiries

    def prepare(self, expiries: str | Iterable[str]) -> int:
        """Load allowed expiry buckets and atomically prune stale/corrupt state."""

        allowed = _normalize_expiries(expiries)
        if allowed is None:
            return 0
        with self._mutex:
            if self._active_expiries == allowed:
                return len(self._entries)
            state = _load_state(self.path)
            if state is None:
                retained: dict[str, CachedSpxwContract] = {}
                rewrite = self.path.exists()
            else:
                retained = _retain_expiries(state.entries, allowed)
                rewrite = state.active_expiries != allowed or len(retained) != len(state.entries)
            self._active_expiries = allowed
            self._entries = retained
            if rewrite:
                replacement = self._replace_state(allowed)
                if replacement is not None:
                    self._entries = replacement
            return len(self._entries)

    def cached_con_id(self, label: str, contract: Any, *, expiry: str) -> int | None:
        """Return a conId only when both durable and requested identities match exactly."""

        with self._mutex:
            if expiry not in self._active_expiries:
                return None
            record = self._entries.get(label)
            if (
                record is None
                or record.expiry != expiry
                or not _contract_matches_record(contract, record)
            ):
                return None
            return record.con_id

    def remember(
        self,
        definitions: Iterable[tuple[str, str, Any]],
        *,
        expiry: str,
    ) -> int:
        """Atomically merge strictly valid contracts into an active expiry bucket."""

        if not _valid_expiry(expiry):
            return 0
        additions: dict[str, CachedSpxwContract] = {}
        for label, kind, contract in definitions:
            record = _record_from_contract(label, kind, contract, expected_expiry=expiry)
            if record is not None:
                additions[label] = record
        if not additions:
            return 0
        if len({record.con_id for record in additions.values()}) != len(additions):
            return 0

        with self._mutex:
            if not self._active_expiries:
                self.prepare((expiry,))
            if expiry not in self._active_expiries:
                return 0
            try:
                with exclusive_state_lock(self.path, timeout_seconds=LOCK_TIMEOUT_SECONDS):
                    state = _load_state(self.path)
                    durable = (
                        _retain_expiries(state.entries, self._active_expiries)
                        if state is not None
                        else dict(self._entries)
                    )
                    merged = dict(durable)
                    changed = sum(merged.get(label) != record for label, record in additions.items())
                    if changed == 0:
                        self._entries = merged
                        return 0
                    merged.update(additions)
                    if len(merged) > MAX_CACHE_ENTRIES:
                        return 0
                    if len({record.con_id for record in merged.values()}) != len(merged):
                        return 0
                    atomic_write_json_secure(
                        self.path,
                        _payload(self._active_expiries, merged),
                    )
            except (OSError, TimeoutError):
                return 0
            self._entries = merged
            return changed

    def evict(self, label: str, *, expiry: str) -> bool:
        """Remove a conId rejected by IBKR so a later resolve must qualify again."""

        if not _valid_expiry(expiry):
            return False
        with self._mutex:
            if not self._active_expiries:
                self.prepare((expiry,))
            if expiry not in self._active_expiries:
                return False
            # Runtime reuse must stop even if the best-effort disk write fails.
            self._entries.pop(label, None)
            try:
                with exclusive_state_lock(self.path, timeout_seconds=LOCK_TIMEOUT_SECONDS):
                    state = _load_state(self.path)
                    durable = (
                        _retain_expiries(state.entries, self._active_expiries)
                        if state is not None
                        else dict(self._entries)
                    )
                    existed = durable.pop(label, None) is not None
                    if existed or self.path.exists():
                        atomic_write_json_secure(
                            self.path,
                            _payload(self._active_expiries, durable),
                        )
            except (OSError, TimeoutError):
                return False
            self._entries = durable
            return existed

    def _replace_state(
        self,
        allowed: frozenset[str],
    ) -> dict[str, CachedSpxwContract] | None:
        try:
            with exclusive_state_lock(self.path, timeout_seconds=LOCK_TIMEOUT_SECONDS):
                state = _load_state(self.path)
                retained = (
                    _retain_expiries(state.entries, allowed) if state is not None else {}
                )
                atomic_write_json_secure(self.path, _payload(allowed, retained))
                return retained
        except (OSError, TimeoutError):
            return None


def option_conid_cache_path(data_root: str | Path) -> Path:
    return Path(data_root) / "state" / CACHE_FILE_NAME


def _payload(
    active_expiries: frozenset[str],
    entries: Mapping[str, CachedSpxwContract],
) -> dict[str, object]:
    return {
        "schema_version": CACHE_SCHEMA_VERSION,
        "cache_kind": CACHE_KIND,
        "provider": CACHE_PROVIDER,
        "active_expiries": sorted(active_expiries),
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        "contracts": [entries[label].to_dict() for label in sorted(entries)],
    }


def _load_state(path: Path) -> _CacheState | None:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None
    return _parse_payload(payload)


def _normalize_expiries(expiries: str | Iterable[str]) -> frozenset[str] | None:
    values = (expiries,) if isinstance(expiries, str) else tuple(expiries)
    if (
        not values
        or len(values) > MAX_ACTIVE_EXPIRIES
        or any(not _valid_expiry(value) for value in values)
        or len(set(values)) != len(values)
    ):
        return None
    return frozenset(values)


def _retain_expiries(
    entries: Mapping[str, CachedSpxwContract],
    allowed: frozenset[str],
) -> dict[str, CachedSpxwContract]:
    return {
        label: record
        for label, record in entries.items()
        if record.expiry in allowed
    }


def _parse_payload(
    payload: object,
) -> _CacheState | None:
    if not isinstance(payload, dict) or frozenset(payload) != _ROOT_KEYS:
        return None
    schema_version = payload.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != CACHE_SCHEMA_VERSION
        or payload.get("cache_kind") != CACHE_KIND
        or payload.get("provider") != CACHE_PROVIDER
        or not _valid_timestamp(payload.get("updated_at"))
    ):
        return None
    active_raw = payload.get("active_expiries")
    if not isinstance(active_raw, list):
        return None
    active_expiries = _normalize_expiries(active_raw)
    if active_expiries is None or active_raw != sorted(active_expiries):
        return None
    contracts = payload.get("contracts")
    if not isinstance(contracts, list) or len(contracts) > MAX_CACHE_ENTRIES:
        return None

    entries: dict[str, CachedSpxwContract] = {}
    con_ids: set[int] = set()
    for raw in contracts:
        record = _record_from_payload(raw, active_expiries=active_expiries)
        if record is None or record.label in entries or record.con_id in con_ids:
            return None
        entries[record.label] = record
        con_ids.add(record.con_id)
    return _CacheState(active_expiries=active_expiries, entries=entries)


def _record_from_payload(
    raw: object,
    *,
    active_expiries: frozenset[str],
) -> CachedSpxwContract | None:
    if not isinstance(raw, dict) or frozenset(raw) != _CONTRACT_KEYS:
        return None
    con_id = raw.get("con_id")
    strike = raw.get("strike")
    right = raw.get("right")
    label = raw.get("label")
    expiry = raw.get("expiry")
    if (
        isinstance(con_id, bool)
        or not isinstance(con_id, int)
        or con_id <= 0
        or isinstance(strike, bool)
        or not isinstance(strike, int)
        or strike <= 0
        or right not in {"C", "P"}
        or not isinstance(label, str)
        or not isinstance(expiry, str)
        or expiry not in active_expiries
    ):
        return None
    expected_label = f"option:SPXW:{expiry}:{strike}:{right}"
    if label != expected_label:
        return None
    if any(
        raw.get(field) != expected
        for field, expected in (
            ("sec_type", "OPT"),
            ("symbol", "SPX"),
            ("expiry", expiry),
            ("exchange", "SMART"),
            ("currency", "USD"),
            ("multiplier", "100"),
            ("trading_class", "SPXW"),
        )
    ):
        return None
    return CachedSpxwContract(
        label=label,
        con_id=con_id,
        expiry=expiry,
        strike=strike,
        right=right,
    )


def _record_from_contract(
    label: str,
    kind: str,
    contract: Any,
    *,
    expected_expiry: str,
) -> CachedSpxwContract | None:
    parsed = _parse_label(label)
    con_id = getattr(contract, "conId", 0)
    strike = _integer_strike(getattr(contract, "strike", None))
    if (
        kind != "option"
        or parsed is None
        or parsed != (expected_expiry, strike, _text(contract, "right"))
        or isinstance(con_id, bool)
        or not isinstance(con_id, int)
        or con_id <= 0
        or not _is_strict_spxw_contract(contract, expected_expiry=expected_expiry)
    ):
        return None
    return CachedSpxwContract(
        label=label,
        con_id=con_id,
        expiry=expected_expiry,
        strike=strike,
        right=parsed[2],
    )


def _contract_matches_record(contract: Any, record: CachedSpxwContract) -> bool:
    return bool(
        _parse_label(record.label) == (record.expiry, record.strike, record.right)
        and _integer_strike(getattr(contract, "strike", None)) == record.strike
        and _text(contract, "right") == record.right
        and _is_strict_spxw_contract(contract, expected_expiry=record.expiry)
    )


def _is_strict_spxw_contract(contract: Any, *, expected_expiry: str) -> bool:
    return all(
        (
            _text(contract, "secType") == "OPT",
            _text(contract, "symbol") == "SPX",
            _text(contract, "lastTradeDateOrContractMonth") == expected_expiry,
            _text(contract, "exchange") == "SMART",
            _text(contract, "currency") == "USD",
            str(getattr(contract, "multiplier", "") or "").strip() == "100",
            _text(contract, "tradingClass") == "SPXW",
        )
    )


def _parse_label(label: str) -> tuple[str, int, str] | None:
    parts = label.split(":")
    if len(parts) != 5 or parts[:2] != ["option", "SPXW"]:
        return None
    expiry, strike_text, right = parts[2:]
    if not _valid_expiry(expiry) or not strike_text.isdigit() or right not in {"C", "P"}:
        return None
    strike = int(strike_text)
    return (expiry, strike, right) if strike > 0 and str(strike) == strike_text else None


def _integer_strike(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    numeric = float(value)
    return int(numeric) if math.isfinite(numeric) and numeric > 0 and numeric.is_integer() else None


def _text(contract: Any, field: str) -> str:
    return str(getattr(contract, field, "") or "").strip().upper()


def _valid_expiry(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 8 or not value.isdigit():
        return False
    try:
        return datetime.strptime(value, "%Y%m%d").strftime("%Y%m%d") == value
    except ValueError:
        return False


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None
