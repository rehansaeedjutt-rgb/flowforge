"""CLI entry point for flowforge."""

from __future__ import annotations

import argparse
import sys

from flowforge import __version__
from flowforge.config import get_settings
from flowforge.kafka.consumer import consume_to_warehouse
from flowforge.sink.duckdb_sink import DuckDBWarehouse


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flowforge",
        description="Kafka + DuckDB streaming data platform.",
    )
    parser.add_argument("--version", action="version", version=f"flowforge {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    consume = sub.add_parser("consume", help="Read from Kafka into DuckDB.")
    consume.add_argument("--max-messages", type=int, default=None)
    consume.add_argument("--timeout-ms", type=int, default=None)

    stats = sub.add_parser("stats", help="Show warehouse summary.")
    stats.add_argument("--db", type=str, default=None)

    vwap = sub.add_parser("vwap", help="VWAP per symbol from the warehouse.")
    vwap.add_argument("--limit", type=int, default=10)

    return parser


def _run_consume(args: argparse.Namespace) -> int:
    settings = get_settings()
    n = args.max_messages if args.max_messages is not None else settings.consumer_max_messages
    t = args.timeout_ms if args.timeout_ms is not None else settings.consumer_timeout_ms

    print(f"Reading up to {n} messages from {settings.kafka_topic_trades}")
    print(f"Writing to DuckDB: {settings.duckdb_path}")
    print()

    result = consume_to_warehouse(max_messages=n, timeout_ms=t)

    print(f"Messages read:     {result.messages_read}")
    print(f"Messages written:  {result.messages_written}")
    print(f"Bad messages:      {result.bad_messages}")

    # Show warehouse state after ingest.
    warehouse = DuckDBWarehouse(settings.duckdb_path)
    warehouse.initialize()
    try:
        s = warehouse.stats()
        print()
        print("=== Warehouse ===")
        print(f"Total trades:     {s.total_trades}")
        print(f"Distinct symbols: {s.distinct_symbols}")
        if s.earliest_trade and s.latest_trade:
            print(
                f"Time range:       {s.earliest_trade.isoformat()} -> {s.latest_trade.isoformat()}"
            )
    finally:
        warehouse.close()
    return 0


def _run_stats(args: argparse.Namespace) -> int:
    settings = get_settings()
    db = args.db if args.db else settings.duckdb_path
    warehouse = DuckDBWarehouse(db)
    warehouse.initialize()
    try:
        s = warehouse.stats()
        print(f"Database:          {db}")
        print(f"Total trades:      {s.total_trades}")
        print(f"Distinct symbols:  {s.distinct_symbols}")
        if s.earliest_trade and s.latest_trade:
            print(f"Earliest:          {s.earliest_trade.isoformat()}")
            print(f"Latest:            {s.latest_trade.isoformat()}")
    finally:
        warehouse.close()
    return 0


def _run_vwap(args: argparse.Namespace) -> int:
    settings = get_settings()
    warehouse = DuckDBWarehouse(settings.duckdb_path)
    warehouse.initialize()
    try:
        rows = warehouse.vwap_by_symbol(limit=args.limit)
        if not rows:
            print("No trades in warehouse. Run 'flowforge consume' first.")
            return 0
        print(f"{'symbol':<12} {'trades':>8} {'vwap':>15}")
        print("-" * 40)
        for r in rows:
            print(f"{r['symbol']:<12} {r['trade_count']:>8} {r['vwap']:>15.4f}")
    finally:
        warehouse.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "consume":
        return _run_consume(args)
    if args.command == "stats":
        return _run_stats(args)
    if args.command == "vwap":
        return _run_vwap(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
