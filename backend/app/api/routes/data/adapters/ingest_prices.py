from collections import defaultdict
from datetime import date
from typing import List

from app.data_ingestion.models import RetryReason
from app.schemas import (
    IngestPricesResponse,
    IngestSymbolResult,
)


RETRY_SEVERITY = {
    RetryReason.EXCEPTION: 3,
    RetryReason.EMPTY: 2,
    RetryReason.PARTIAL: 1,
    RetryReason.NONE: 0,
}


def adapt_orchestration_result(
    *,
    symbols: list[str],
    start: date,
    end: date,
    interval: str,
    dry_run: bool,
    fetch_results: list[list[dict]],
    rows_inserted: dict,
) -> IngestPricesResponse:
    per_symbol = defaultdict(list)

    # Flatten into symbol buckets
    for symbol_results in fetch_results:
        for r in symbol_results:
            per_symbol[r["symbol"]].append(r)

    results: List[IngestSymbolResult] = []
    succeeded = 0

    for symbol in symbols:
        symbol_runs = per_symbol.get(symbol, [])

        if not symbol_runs:
            results.append(
                IngestSymbolResult(
                    symbol=symbol,
                    success=False,
                    attempts=0,
                    retry_reason=RetryReason.EXCEPTION,
                    coverage=None,
                    missing_dates=[],
                    rows_fetched=0,
                    rows_inserted=0,
                    elapsed_ms=0,
                    error="No fetch attempts executed",
                )
            )
            continue

        attempts = sum(r["attempts"] for r in symbol_runs)
        elapsed_ms = sum(r["elapsed_ms"] for r in symbol_runs)

        retry_reason = max(
            (r["retry_reason"] for r in symbol_runs),
            key=lambda rr: RETRY_SEVERITY[rr],
        )

        coverage_values = [r.get("coverage") for r in symbol_runs if r.get("coverage") is not None]
        coverage = min(coverage_values) if coverage_values else None

        missing_dates = sorted(
            {d for r in symbol_runs for d in r.get("missing_dates", [])}
        )

        rows_fetched = sum(
            0 if r["result"].data is None else len(r["result"].data)
            for r in symbol_runs
        )

        error = next(
            (
                str(r["result"].exception)
                for r in symbol_runs
                if r["result"].exception is not None
            ),
            None,
        )

        success = error == None
        if success:
            succeeded += 1

        results.append(
            IngestSymbolResult(
                symbol=symbol,
                success=success,
                attempts=attempts,
                retry_reason=retry_reason,
                coverage=coverage,
                missing_dates=missing_dates,
                rows_fetched=rows_fetched,
                rows_inserted=rows_inserted[symbol],
                elapsed_ms=elapsed_ms,
                error=error,
            )
        )

    return IngestPricesResponse(
        start=start,
        end=end,
        interval=interval,
        dry_run=dry_run,
        total_symbols=len(symbols),
        succeeded=succeeded,
        failed=len(symbols) - succeeded,
        results=results,
    )
