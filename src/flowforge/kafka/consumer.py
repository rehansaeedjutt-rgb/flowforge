"""Kafka consumer that feeds the DuckDB warehouse.

Same design pattern as streamforge's consumer: read N messages or idle
for T milliseconds, then stop. A long-lived service would loop forever,
but for a CLI demo the bounded run is what you want.
"""

from __future__ import annotations

import json
from typing import Any

import structlog
from kafka import KafkaConsumer

from flowforge.config import get_settings
from flowforge.schemas import TradeEvent
from flowforge.sink.duckdb_sink import DuckDBWarehouse

logger = structlog.get_logger(__name__)


class ConsumerResult:
    """Result of a bounded consumer run."""

    def __init__(
        self,
        messages_read: int,
        messages_written: int,
        bad_messages: int,
    ) -> None:
        self.messages_read = messages_read
        self.messages_written = messages_written
        self.bad_messages = bad_messages


def consume_to_warehouse(
    max_messages: int = 500,
    timeout_ms: int = 8000,
) -> ConsumerResult:
    """Read from Kafka and persist to DuckDB. Bounded run.

    Returns counts so the CLI can print a summary.
    """
    settings = get_settings()

    consumer = KafkaConsumer(
        settings.kafka_topic_trades,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,
    )

    warehouse = DuckDBWarehouse(settings.duckdb_path)
    warehouse.initialize()

    read = 0
    written = 0
    bad = 0
    batch: list[TradeEvent] = []
    batch_size = 100

    logger.info(
        "consumer.start",
        topic=settings.kafka_topic_trades,
        max_messages=max_messages,
    )

    try:
        for msg in consumer:
            read += 1
            try:
                trade = TradeEvent.from_wire(msg.value)
                batch.append(trade)
            except (KeyError, ValueError, TypeError) as exc:
                bad += 1
                logger.warning("consumer.bad_message", error=str(exc))
                continue

            if len(batch) >= batch_size:
                written += warehouse.insert_trades(batch)
                batch.clear()

            if read >= max_messages:
                break

        # Flush remaining batch.
        if batch:
            written += warehouse.insert_trades(batch)

    finally:
        consumer.close()
        warehouse.close()

    logger.info(
        "consumer.done",
        read=read,
        written=written,
        bad=bad,
    )
    return ConsumerResult(read, written, bad)


def _placeholder_any(_: Any) -> None:
    """Silences unused-import lint. Remove when Any is actually used."""
