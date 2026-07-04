from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from spx_spark.config import SchwabSettings


@dataclass(frozen=True)
class SchwabCheckResult:
    label: str
    kind: str
    ok: bool
    status: int | None = None
    summary: dict[str, Any] | None = None
    error: str | None = None


def load_access_token(settings: SchwabSettings) -> str:
    if settings.access_token:
        return settings.access_token

    token_path = Path(settings.token_file)
    if not token_path.exists():
        return ""

    raw = json.loads(token_path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        for key in ("access_token", "accessToken"):
            value = raw.get(key)
            if isinstance(value, str) and value:
                return value
        nested = raw.get("token")
        if isinstance(nested, dict):
            for key in ("access_token", "accessToken"):
                value = nested.get(key)
                if isinstance(value, str) and value:
                    return value
    return ""


class SchwabClient:
    def __init__(self, settings: SchwabSettings, access_token: str) -> None:
        self.settings = settings
        self.access_token = access_token

    def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, Any]:
        base = self.settings.api_base_url.rstrip("/") + "/"
        url = urljoin(base, path.lstrip("/"))
        if params:
            url = f"{url}?{urlencode(params)}"
        request = Request(
            url,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
            },
            method="GET",
        )
        with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else None


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def summarize_quote_payload(payload: Any, symbols: list[str]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"symbols_requested": symbols, "payload_type": type(payload).__name__}

    summaries: dict[str, Any] = {}
    for symbol in symbols:
        quote = payload.get(symbol)
        if quote is None:
            summaries[symbol] = {"present": False}
            continue
        if isinstance(quote, dict):
            quote_section = quote.get("quote") if isinstance(quote.get("quote"), dict) else {}
            reference = quote.get("reference") if isinstance(quote.get("reference"), dict) else {}
            summaries[symbol] = {
                "present": True,
                "assetMainType": quote.get("assetMainType"),
                "assetSubType": quote.get("assetSubType"),
                "quoteType": quote.get("quoteType"),
                "description": reference.get("description"),
                "bid": quote_section.get("bidPrice"),
                "ask": quote_section.get("askPrice"),
                "last": quote_section.get("lastPrice"),
                "mark": quote_section.get("mark"),
                "quoteTime": quote_section.get("quoteTime"),
                "tradeTime": quote_section.get("tradeTime"),
            }
        else:
            summaries[symbol] = {"present": True, "payload_type": type(quote).__name__}
    return {"symbols": summaries}


def summarize_chain_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"payload_type": type(payload).__name__}

    call_map = payload.get("callExpDateMap") if isinstance(payload.get("callExpDateMap"), dict) else {}
    put_map = payload.get("putExpDateMap") if isinstance(payload.get("putExpDateMap"), dict) else {}
    call_contracts = count_chain_contracts(call_map)
    put_contracts = count_chain_contracts(put_map)
    return {
        "symbol": payload.get("symbol"),
        "status": payload.get("status"),
        "underlyingPrice": payload.get("underlyingPrice"),
        "strategy": payload.get("strategy"),
        "interval": payload.get("interval"),
        "isDelayed": payload.get("isDelayed"),
        "callExpirations": len(call_map),
        "putExpirations": len(put_map),
        "callContracts": call_contracts,
        "putContracts": put_contracts,
    }


def count_chain_contracts(expiration_map: dict[str, Any]) -> int:
    count = 0
    for strikes in expiration_map.values():
        if not isinstance(strikes, dict):
            continue
        for contracts in strikes.values():
            if isinstance(contracts, list):
                count += len(contracts)
    return count


def verify_quotes(client: SchwabClient, settings: SchwabSettings) -> list[SchwabCheckResult]:
    symbols = settings.verify_indexes + settings.verify_equities + settings.verify_futures
    results: list[SchwabCheckResult] = []
    for batch in chunked(symbols, 40):
        label = ",".join(batch)
        try:
            status, payload = client.get_json(
                "/marketdata/v1/quotes",
                {"symbols": ",".join(batch), "fields": settings.quote_fields},
            )
            results.append(
                SchwabCheckResult(
                    label=label,
                    kind="quotes",
                    ok=True,
                    status=status,
                    summary=summarize_quote_payload(payload, batch),
                )
            )
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            results.append(
                SchwabCheckResult(label=label, kind="quotes", ok=False, error=format_error(exc))
            )
    return results


def verify_option_chains(client: SchwabClient, settings: SchwabSettings) -> list[SchwabCheckResult]:
    results: list[SchwabCheckResult] = []
    for symbol in settings.verify_option_chains:
        try:
            status, payload = client.get_json(
                "/marketdata/v1/chains",
                {
                    "symbol": symbol,
                    "contractType": "ALL",
                    "strategy": "SINGLE",
                    "strikeCount": settings.option_chain_strike_count,
                    "includeUnderlyingQuote": "true",
                },
            )
            summary = summarize_chain_payload(payload)
            results.append(
                SchwabCheckResult(
                    label=symbol,
                    kind="option_chain",
                    ok=True,
                    status=status,
                    summary=summary,
                )
            )
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            results.append(
                SchwabCheckResult(
                    label=symbol,
                    kind="option_chain",
                    ok=False,
                    error=format_error(exc),
                )
            )
    return results


def format_error(exc: BaseException) -> str:
    if isinstance(exc, HTTPError):
        try:
            body = exc.read().decode("utf-8")[:500]
        except Exception:  # noqa: BLE001
            body = ""
        return f"HTTP {exc.code}: {exc.reason}; {body}".strip()
    return str(exc)


def print_results(results: list[SchwabCheckResult]) -> None:
    headers = ["kind", "label", "ok", "status", "summary/error"]
    rows: list[list[str]] = []
    for result in results:
        detail = json.dumps(result.summary, sort_keys=True) if result.summary else result.error or ""
        rows.append(
            [
                result.kind,
                result.label,
                str(result.ok).lower(),
                "-" if result.status is None else str(result.status),
                detail[:240],
            ]
        )
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows)) for index in range(len(headers))
    ]
    print(" | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def write_snapshot(settings: SchwabSettings, results: list[SchwabCheckResult]) -> Path:
    output_dir = Path("logs")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / f"schwab-verifier-{timestamp}.json"
    safe_settings = asdict(settings) | {"access_token": "***" if settings.access_token else ""}
    payload = {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "settings": safe_settings,
        "results": [asdict(result) for result in results],
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def print_offline_plan(settings: SchwabSettings) -> None:
    plan = {
        "quotes": settings.verify_indexes + settings.verify_equities + settings.verify_futures,
        "option_chains": settings.verify_option_chains,
        "option_chain_strike_count": settings.option_chain_strike_count,
        "quote_fields": settings.quote_fields,
        "token_file": settings.token_file,
    }
    print(json.dumps(plan, indent=2, sort_keys=True))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Schwab market data availability.")
    parser.add_argument("--print-config", action="store_true")
    parser.add_argument("--offline", action="store_true", help="Print verification universe only.")
    parser.add_argument("--skip-quotes", action="store_true")
    parser.add_argument("--skip-chains", action="store_true")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = SchwabSettings.from_env()
    safe_settings = asdict(settings) | {"access_token": "***" if settings.access_token else ""}

    if args.print_config:
        print(json.dumps(safe_settings, indent=2, sort_keys=True))
        return 0
    if args.offline:
        print_offline_plan(settings)
        return 0

    token = load_access_token(settings)
    if not token:
        print(
            "Missing Schwab access token. Set SCHWAB_ACCESS_TOKEN or SCHWAB_TOKEN_FILE, "
            "or run with --offline to inspect the verification universe.",
            file=sys.stderr,
        )
        return 2

    client = SchwabClient(settings, token)
    results: list[SchwabCheckResult] = []
    if not args.skip_quotes:
        results.extend(verify_quotes(client, settings))
    if not args.skip_chains:
        results.extend(verify_option_chains(client, settings))

    print_results(results)
    output_path = write_snapshot(settings, results)
    print(f"\nWrote JSON snapshot: {output_path}")
    return 0 if all(result.ok for result in results) else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
