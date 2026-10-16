"""Tests for flowforge.schemas."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from flowforge.schemas import Side, TradeEvent, WarehouseStats

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def test_trade_from_wire_roundtrip() -> None:
    wire = {
        "symbol": "BTCUSDT",
        "trade_id": 12345,
        "price": "50000.00",
        "quantity": "0.001",
        "side": "BUY",
        "timestamp": "2026-01-01T00:00:00+00:00",
    }
    trade = TradeEvent.from_wire(wire)
    assert trade.symbol == "BTCUSDT"
    assert trade.side is Side.BUY
    assert trade.timestamp == _TS


def test_trade_rejects_bad_side() -> None:
    # Side is a StrEnum, so an invalid value fails in the enum constructor
    # with a ValueError before Pydantic ever sees it. Catching ValueError
    # is the correct expectation here.
    with pytest.raises(ValueError):
        TradeEvent.from_wire(
            {
                "symbol": "X",
                "trade_id": 1,
                "price": "1",
                "quantity": "1",
                "side": "MAYBE",
                "timestamp": "2026-01-01T00:00:00+00:00",
            }
        )


def test_trade_immutable() -> None:
    t = TradeEvent(
        symbol="BTCUSDT",
        trade_id=1,
        price="1",
        quantity="1",
        side=Side.BUY,
        timestamp=_TS,
    )
    with pytest.raises(ValidationError):
        t.symbol = "ETHUSDT"  # type: ignore[misc]


def test_stats_defaults() -> None:
    s = WarehouseStats(total_trades=0, distinct_symbols=0)
    assert s.earliest_trade is None
    assert s.latest_trade is None
