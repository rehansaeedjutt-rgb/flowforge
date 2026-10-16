"""Tests for the DuckDB warehouse. Uses in-memory DB."""

from datetime import UTC, datetime, timedelta

import pytest

from flowforge.schemas import Side, TradeEvent
from flowforge.sink.duckdb_sink import DuckDBWarehouse


def _trade(tid: int, symbol: str = "BTCUSDT", offset_s: int = 0) -> TradeEvent:
    return TradeEvent(
        symbol=symbol,
        trade_id=tid,
        price="50000.00",
        quantity="0.001",
        side=Side.BUY,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=offset_s),
    )


def _warehouse() -> DuckDBWarehouse:
    w = DuckDBWarehouse(":memory:")
    w.initialize()
    return w


def test_initialize_creates_table() -> None:
    w = _warehouse()
    try:
        s = w.stats()
        assert s.total_trades == 0
    finally:
        w.close()


def test_insert_returns_count() -> None:
    w = _warehouse()
    try:
        inserted = w.insert_trades([_trade(1), _trade(2), _trade(3)])
        assert inserted == 3
        assert w.stats().total_trades == 3
    finally:
        w.close()


def test_insert_is_idempotent() -> None:
    w = _warehouse()
    try:
        first = w.insert_trades([_trade(1), _trade(2)])
        second = w.insert_trades([_trade(1), _trade(2)])
        assert first == 2
        assert second == 0
        assert w.stats().total_trades == 2
    finally:
        w.close()


def test_insert_empty_returns_zero() -> None:
    w = _warehouse()
    try:
        assert w.insert_trades([]) == 0
    finally:
        w.close()


def test_stats_counts_distinct_symbols() -> None:
    w = _warehouse()
    try:
        w.insert_trades([_trade(1, "BTCUSDT"), _trade(2, "ETHUSDT"), _trade(3, "BTCUSDT")])
        s = w.stats()
        assert s.total_trades == 3
        assert s.distinct_symbols == 2
    finally:
        w.close()


def test_vwap_by_symbol() -> None:
    w = _warehouse()
    try:
        w.insert_trades([_trade(1), _trade(2), _trade(3, "ETHUSDT")])
        rows = w.vwap_by_symbol(limit=5)
        symbols = {r["symbol"] for r in rows}
        assert symbols == {"BTCUSDT", "ETHUSDT"}
    finally:
        w.close()


def test_insert_before_initialize_raises() -> None:
    w = DuckDBWarehouse(":memory:")
    with pytest.raises(RuntimeError, match="initialize"):
        w.insert_trades([_trade(1)])


def test_stats_before_initialize_raises() -> None:
    w = DuckDBWarehouse(":memory:")
    with pytest.raises(RuntimeError, match="initialize"):
        w.stats()
