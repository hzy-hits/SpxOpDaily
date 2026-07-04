from __future__ import annotations

import argparse
import json

from spx_spark.config import StorageSettings
from spx_spark.storage import LatestState, LatestStateStore


def quote_row(quote, *, as_of) -> list[str]:
    age_ms = quote.quote_age_ms(as_of)
    return [
        quote.instrument.canonical_id,
        quote.provider.value,
        quote.quality.value,
        format_number(quote.bid),
        format_number(quote.ask),
        format_number(quote.mid),
        format_number(quote.last),
        format_number(quote.effective_price),
        "-" if age_ms is None else f"{age_ms / 1000:.1f}s",
    ]


def format_number(value: float | None) -> str:
    if value is None:
        return "-"
    if abs(value) >= 100:
        return f"{value:.2f}"
    return f"{value:.4f}".rstrip("0").rstrip(".")


def print_table(state: LatestState, *, all_providers: bool, instrument: str | None) -> None:
    quotes = state.quotes if all_providers else state.best_quotes
    if instrument:
        quotes = tuple(quote for quote in quotes if quote.instrument.canonical_id == instrument)

    print(f"Latest state: {state.as_of.isoformat()}")
    print(f"Rows: {len(quotes)}")
    if not quotes:
        print_provider_states(state)
        return

    headers = ["instrument", "provider", "quality", "bid", "ask", "mid", "last", "price", "age"]
    rows = [quote_row(quote, as_of=state.as_of) for quote in quotes]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows)) for index in range(len(headers))
    ]
    print(" | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))
    print_provider_states(state)


def print_provider_states(state: LatestState) -> None:
    if not state.provider_states:
        return

    print("\nProvider state:")
    headers = ["provider", "status", "connected", "latency_ms", "checked_at", "reason"]
    rows: list[list[str]] = []
    for provider_state in state.provider_states:
        rows.append(
            [
                provider_state.provider.value,
                provider_state.status.value,
                "-" if provider_state.connected is None else str(provider_state.connected).lower(),
                format_number(provider_state.latency_ms),
                provider_state.checked_at.isoformat(),
                provider_state.reason or "",
            ]
        )
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows)) for index in range(len(headers))
    ]
    print(" | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect the normalized latest market-data state.")
    parser.add_argument("--json", action="store_true", help="Print full latest state JSON.")
    parser.add_argument(
        "--all-providers",
        action="store_true",
        help="Show provider-level latest quotes instead of selected best quotes.",
    )
    parser.add_argument("--instrument", help="Filter by canonical instrument id.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StorageSettings.from_env()
    state = LatestStateStore(settings).load()
    if args.json:
        print(json.dumps(state.to_dict(), indent=2, sort_keys=True))
    else:
        print_table(state, all_providers=args.all_providers, instrument=args.instrument)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
