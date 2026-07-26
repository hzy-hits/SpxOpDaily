"""Session-cohort selection for the 0DTE level replay."""

from __future__ import annotations

from datetime import date

from .odte_level_signals import SET_TRADE_READY, Signal
from .odte_level_timing import in_rth_1300_entry_window


def readiness_session_cohorts(
    readiness: object,
    *,
    last_complete_date: date,
) -> tuple[set[date], set[date]]:
    """Return all-session and Put-specific contract-consistent cohorts."""

    payload = readiness if isinstance(readiness, dict) else {}
    sessions = payload.get("sessions")
    details = sessions.get("details") if isinstance(sessions, dict) else None
    legacy_complete_dates: set[date] = set()
    contract_dates_from_details: set[date] = set()
    has_global_detail_contract = False
    put_dates_from_details: set[date] = set()
    has_put_detail_contract = False
    if isinstance(details, list):
        for detail in details:
            if not isinstance(detail, dict):
                continue
            try:
                session_date = date.fromisoformat(str(detail.get("session_date") or ""))
            except ValueError:
                continue
            if session_date > last_complete_date:
                continue
            if detail.get("complete") is True:
                legacy_complete_dates.add(session_date)
            if "contract_consistent" in detail:
                has_global_detail_contract = True
                if detail.get("contract_consistent") is True:
                    contract_dates_from_details.add(session_date)
            if "put_contract_consistent" in detail:
                has_put_detail_contract = True
                if detail.get("put_contract_consistent") is True:
                    put_dates_from_details.add(session_date)

    global_dates = sessions.get("dates") if isinstance(sessions, dict) else None
    if isinstance(global_dates, list):
        complete_dates = _bounded_dates(global_dates, last_complete_date=last_complete_date)
    elif has_global_detail_contract:
        complete_dates = contract_dates_from_details
    else:
        # Backward compatibility for readiness artifacts written before the
        # global contract-consistent session list existed.
        complete_dates = legacy_complete_dates

    cohort_sessions = payload.get("cohort_sessions")
    put_cohort = (
        cohort_sessions.get("put_exact_entry") if isinstance(cohort_sessions, dict) else None
    )
    if isinstance(put_cohort, dict) and isinstance(put_cohort.get("dates"), list):
        put_dates = _bounded_dates(
            put_cohort["dates"],
            last_complete_date=last_complete_date,
        )
    elif has_put_detail_contract:
        put_dates = put_dates_from_details
    else:
        # Backward compatibility for readiness artifacts written before
        # cohort-specific RTH evidence existed.
        put_dates = set(complete_dates)
    return complete_dates, put_dates


def uses_put_session_cohort(signal: Signal) -> bool:
    """Return whether this production replay row uses the Put RTH cohort."""

    return bool(
        signal.set_name == SET_TRADE_READY
        and signal.direction == "down"
        and in_rth_1300_entry_window(signal)
    )


def _bounded_dates(values: list[object], *, last_complete_date: date) -> set[date]:
    dates: set[date] = set()
    for value in values:
        try:
            session_date = date.fromisoformat(str(value))
        except ValueError:
            continue
        if session_date <= last_complete_date:
            dates.add(session_date)
    return dates
