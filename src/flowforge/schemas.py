"""Pydantic models for pipeline messages."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class TradeEvent(BaseModel):
    """Wire format for a trade on the Kafka topic.

    Same shape as streamforge's TradeEvent, so the two services can share
    a topic. Kept in this project rather than imported because I do not
    want a hard dependency between the two repos.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    trade_id: int
    price: str
    quantity: str
    side: Side
    timestamp: datetime

    @classmethod
    def from_wire(cls, raw: dict[str, object]) -> TradeEvent:
        return cls(
            symbol=str(raw["symbol"]),
            trade_id=int(str(raw["trade_id"])),
            price=str(raw["price"]),
            quantity=str(raw["quantity"]),
            side=Side(str(raw["side"])),
            timestamp=datetime.fromisoformat(str(raw["timestamp"])),
        )


class WarehouseStats(BaseModel):
    """Stats returned from the DuckDB warehouse."""

    model_config = ConfigDict(frozen=True)

    total_trades: int = Field(..., ge=0)
    distinct_symbols: int = Field(..., ge=0)
    earliest_trade: datetime | None = None
    latest_trade: datetime | None = None
