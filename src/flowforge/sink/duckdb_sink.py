"""DuckDB warehouse sink.

DuckDB is an embedded columnar warehouse — think SQLite, but for analytics.
It reads Parquet and CSV natively, does vectorized query execution, and
fits in a single file. It is the right tool for a streaming pipeline that
needs fast reads on a modest volume of data.

Trade-off vs ClickHouse:
- DuckDB: single file, zero setup, excellent for reads up to tens of GB.
- ClickHouse: distributed, great for billions of rows, needs a server.

For this project DuckDB wins on the "runs out of the box" requirement.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import structlog

from flowforge.schemas import TradeEvent, WarehouseStats

logger = structlog.get_logger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    symbol      VARCHAR NOT NULL,
    trade_id    BIGINT  NOT NULL,
    price       VARCHAR NOT NULL,
    quantity    VARCHAR NOT NULL,
    side        VARCHAR NOT NULL,
    ts          TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_id)
);
"""


class DuckDBWarehouse:
    """Idempotent trade warehouse backed by DuckDB."""

    def __init__(self, db_path: str | Path = "flowforge.duckdb") -> None:
        self._db_path = str(db_path)
        self._conn: duckdb.DuckDBPyConnection | None = None

    def initialize(self) -> None:
        if self._conn is not None:
            return
        self._conn = duckdb.connect(self._db_path)
        self._conn.execute(_SCHEMA)
        logger.info("duckdb.initialized", path=self._db_path)

    def insert_trades(self, trades: list[TradeEvent]) -> int:
        """Insert trades. Returns count of newly inserted rows.

        Uses INSERT OR IGNORE (DuckDB supports it) to make replays safe.
        Composite primary key on (symbol, trade_id) prevents duplicates.
        """
        if self._conn is None:
            msg = "initialize() must be called first"
            raise RuntimeError(msg)
        if not trades:
            return 0

        before = self._row_count()
        self._conn.executemany(
            "INSERT OR IGNORE INTO trades (symbol, trade_id, price, quantity, side, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    t.symbol,
                    t.trade_id,
                    t.price,
                    t.quantity,
                    t.side.value,
                    t.timestamp,
                )
                for t in trades
            ],
        )
        after = self._row_count()
        return after - before

    def stats(self) -> WarehouseStats:
        if self._conn is None:
            msg = "initialize() must be called first"
            raise RuntimeError(msg)
        row = self._conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(ts), MAX(ts) FROM trades"
        ).fetchone()
        if row is None:
            return WarehouseStats(total_trades=0, distinct_symbols=0)
        return WarehouseStats(
            total_trades=int(row[0]),
            distinct_symbols=int(row[1]),
            earliest_trade=row[2],
            latest_trade=row[3],
        )

    def vwap_by_symbol(self, limit: int = 10) -> list[dict[str, object]]:
        """Compute VWAP per symbol in SQL. DuckDB does this in one query."""
        if self._conn is None:
            msg = "initialize() must be called first"
            raise RuntimeError(msg)
        rows = self._conn.execute(
            """
            SELECT
                symbol,
                COUNT(*) AS trade_count,
                SUM(CAST(price AS DOUBLE) * CAST(quantity AS DOUBLE)) /
                    NULLIF(SUM(CAST(quantity AS DOUBLE)), 0) AS vwap
            FROM trades
            GROUP BY symbol
            ORDER BY trade_count DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [{"symbol": r[0], "trade_count": int(r[1]), "vwap": float(r[2] or 0)} for r in rows]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _row_count(self) -> int:
        assert self._conn is not None  # guarded by caller
        row = self._conn.execute("SELECT COUNT(*) FROM trades").fetchone()
        return int(row[0]) if row else 0
