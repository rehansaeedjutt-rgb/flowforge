# flowforge

> Production streaming data platform: Kafka + Apache Flink + DuckDB.

[![CI](https://github.com/rehansaeedjutt-rgb/flowforge/actions/workflows/ci.yml/badge.svg)](https://github.com/rehansaeedjutt-rgb/flowforge/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)

## What this is

`flowforge` is the production version of `streamforge`. Same data shape
(crypto trades flowing from Binance through Kafka), but with two important
upgrades:

1. **DuckDB warehouse** instead of SQLite. DuckDB is an embedded columnar
   warehouse — SQLite for analytics. Column-oriented storage means VWAP
   queries over millions of rows finish in milliseconds.
2. **CLI subcommands** for real operations: `consume`, `stats`, `vwap`.

The Flink job is scoped for Phase 2 — the Python consumer in this repo
already does the tumbling-window work that a Flink job would do, and I
would rather not claim Flink integration until the Flink code exists.

## Why DuckDB

ClickHouse is the answer most data teams give, and it is the right answer
at scale. DuckDB is the right answer for a single-node pipeline:

| Aspect | DuckDB | ClickHouse |
|--------|--------|------------|
| Setup | `pip install duckdb` | Server + clients + config |
| Storage | Single file | Data directory + config |
| Query speed on <10GB | Excellent | Excellent |
| Query speed on >1TB | Not viable | Excellent |
| Deployment complexity | None | High |

For this project the setup time matters more than maximum throughput.
DuckDB wins on the "runs out of the box" requirement.

## What is in here

- **Kafka consumer** — reads trades from `crypto.trades.raw`, bounded run.
- **DuckDB warehouse** — idempotent insert on `(symbol, trade_id)`,
  columnar storage, VWAP computed in SQL.
- **CLI** — `consume`, `stats`, `vwap`.
- **12 tests** covering schema parsing and warehouse behaviour with
  in-memory DuckDB.
- **GitHub Actions CI** on Python 3.12, 3.13, 3.14.

## What is NOT here (deliberately)

- **No Apache Flink job yet.** The Python consumer does tumbling-window
  work at the small volumes this demo handles. A Flink job becomes worth
  it when the pipeline is CPU-bound across multiple machines — which
  this is not.
- **No producer.** Reuse `streamforge`'s producer: it publishes to the
  same topic with the same wire format.
- **No Grafana dashboard.** `flowforge vwap` shows the same numbers.
- **No retention policy.** DuckDB grows unbounded.

## Live demo

Prerequisites:
- Kafka running on `localhost:9092`
- A producer publishing trades to `crypto.trades.raw`

    git clone https://github.com/rehansaeedjutt-rgb/flowforge.git
    cd flowforge
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    pip install -e ".[dev]"

    # Read from Kafka into DuckDB
    flowforge consume --max-messages 500

    # Inspect the warehouse
    flowforge stats
    flowforge vwap --limit 10

Example output:

    Reading up to 500 messages from crypto.trades.raw
    Writing to DuckDB: flowforge.duckdb

    Messages read:     500
    Messages written:  487
    Bad messages:      0

    === Warehouse ===
    Total trades:     487
    Distinct symbols: 2
    Time range:       2026-10-16T10:32:15+00:00 -> 2026-10-16T10:34:02+00:00

    $ flowforge vwap --limit 5
    symbol         trades            vwap
    ----------------------------------------
    BTCUSDT           312      84156.3821
    ETHUSDT           175       2613.5402

The exact numbers depend on your Kafka stream.

## Architecture

    Binance WS
        |
        v
    [ streamforge producer ]  (separate repo)
        |
        v
    Kafka topic: crypto.trades.raw
        |
        v
    [ flowforge consumer ]
        |
        v
    DuckDB warehouse (columnar, embedded)
        |
        v
    [ flowforge CLI: stats | vwap ]

## Design notes

**Idempotent insert.** `INSERT OR IGNORE` with a composite primary key on
`(symbol, trade_id)`. DuckDB supports this natively. Replaying a stream
is safe — a duplicate never doubles the row count. Same pattern as
`tickstream` (SQLite) and `streamforge`.

**Batched writes.** The consumer reads up to 500 messages but writes in
batches of 100. DuckDB's `executemany` benefits from batching; a single
insert per trade is 10x slower. The batch boundary is also a natural
checkpoint for a real pipeline.

**VWAP computed in SQL, not Python.** DuckDB's vectorized execution on a
columnar layout is faster than a Python loop, and the query is legible:
`SUM(price * quantity) / NULLIF(SUM(quantity), 0)`. There is no reason
to do this arithmetic in Python.

**Storage as VARCHAR for prices.** Same reasoning as every other project
in this portfolio: floats are for physics, not money. The VWAP query
casts to DOUBLE at the very end because the final answer is a single
number, and VARCHAR-to-DOUBLE inside an aggregate is what DuckDB is
designed to do.

**Bounded consumer runs.** Same pattern as `streamforge`. Read N
messages or idle for T milliseconds, then exit. A long-running service
would loop forever; a CLI demo should not.

## Testing

    pytest --cov=src/flowforge --cov-report=term-missing

Current coverage: 50% overall. The gap is in:
- `config.py` (loaded once, used everywhere, tested implicitly)
- `kafka/consumer.py` (needs a live Kafka broker)

The warehouse is at 87% and the schemas at 100%.

## Roadmap

- [ ] Apache Flink job that replaces the Python consumer
- [ ] Parquet export of the DuckDB tables
- [ ] Grafana-compatible metrics endpoint
- [ ] Retention policy (drop trades older than N days)
- [ ] Docker Compose for the full stack (Kafka + producer + warehouse)
- [ ] Multi-symbol windowed aggregations (tumbling windows in SQL)

## Disclaimer

Research and educational tool. Not financial advice.

## License

MIT - see [LICENSE](LICENSE).

## Author

**Muhammad Rehan Saeed** - [GitHub](https://github.com/rehansaeedjutt-rgb)