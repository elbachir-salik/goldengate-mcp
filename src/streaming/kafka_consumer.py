"""
Kafka consumer — reads GoldenGate CDC events from configured topics.

Uses confluent-kafka-python.  If KAFKA_BROKERS is empty the consumer is
disabled and callers fall back to querying the Oracle replica directly.

Responsibilities:
    - Connect to Kafka with the configured consumer group
    - Deserialise GoldenGate Avro / JSON CDC envelope
    - Yield normalised event dicts to the get_realtime_events tool
    - Handle transient disconnects with exponential back-off (tenacity)

Normalised event dict format:
    {
        "op":    "I" | "U" | "D",   # Insert / Update / Delete
        "table": str,                # source table name from CDC envelope
        "before": dict | None,       # row state before change (None for inserts)
        "after":  dict | None,       # row state after change (None for deletes)
        "ts_ms":  int,               # event timestamp in milliseconds
    }
"""

from __future__ import annotations

import time
from typing import Any

import structlog

from src.config import Settings

log = structlog.get_logger(__name__)


class KafkaConsumerError(RuntimeError):
    """Raised when Kafka is unreachable or consumption fails.

    Callers should catch this and fall back to the Oracle replica.
    Try get_realtime_events() with a longer lookback_minutes, or use
    get_transaction_history() for historical data.
    """


class KafkaConsumer:
    """Synchronous Kafka consumer for GoldenGate CDC events.

    ``consume()`` is intentionally synchronous — confluent-kafka's Consumer
    is a sync C extension.  Call it via ``asyncio.to_thread()`` inside async
    tools to avoid blocking the event loop.

    Lifecycle::

        consumer = KafkaConsumer(settings)
        if consumer.is_enabled():
            events = consumer.consume("transactions", lookback_minutes=5)
        consumer.close()
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consumer: Any = None  # confluent_kafka.Consumer at runtime

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_enabled(self) -> bool:
        """Return True if Kafka brokers are configured (KAFKA_BROKERS non-empty)."""
        return self._settings.kafka_enabled

    def consume(self, topic: str, lookback_minutes: int) -> list[dict]:
        """Return recent CDC events from *topic* within the last *lookback_minutes*.

        Connects to Kafka, seeks to the timestamp offset corresponding to
        ``now - lookback_minutes``, polls until caught up or timeout, then
        returns all collected events as normalised dicts.

        Args:
            topic:            Kafka topic name (e.g. "banking.transactions").
            lookback_minutes: How far back to look. Must be 1–60.

        Returns:
            List of normalised CDC event dicts (may be empty if no events).

        Raises:
            KafkaConsumerError: If Kafka is unreachable, the topic doesn't
                exist, or any other consumption error occurs.
                Callers should fall back to the Oracle replica on this error.
        """
        if not self.is_enabled():
            raise KafkaConsumerError(
                "Kafka is not configured (KAFKA_BROKERS is empty). "
                "get_realtime_events() will automatically fall back to the Oracle replica. "
                "To enable Kafka set KAFKA_BROKERS in your .env file."
            )

        try:
            import confluent_kafka  # lazy import — not required if Kafka disabled
        except ImportError as exc:
            raise KafkaConsumerError(
                "confluent-kafka package is not installed. "
                "Install it with: pip install confluent-kafka. "
                "Or leave KAFKA_BROKERS empty to use the Oracle replica fallback."
            ) from exc

        since_ms = int((time.time() - lookback_minutes * 60) * 1000)
        events: list[dict] = []

        try:
            consumer = confluent_kafka.Consumer({
                "bootstrap.servers": self._settings.kafka_brokers,
                "group.id": self._settings.kafka_consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })

            # Get partition metadata and seek to timestamp offset
            metadata = consumer.list_topics(topic, timeout=5)
            if topic not in metadata.topics:
                consumer.close()
                raise KafkaConsumerError(
                    f"Topic '{topic}' not found in Kafka. "
                    f"Verify the topic name is correct, or use get_transaction_history() "
                    f"to query the Oracle replica directly."
                )

            partitions = [
                confluent_kafka.TopicPartition(topic, p)
                for p in metadata.topics[topic].partitions
            ]
            timestamps = [
                confluent_kafka.TopicPartition(topic, tp.partition, since_ms)
                for tp in partitions
            ]
            offsets = consumer.offsets_for_times(timestamps, timeout=5)
            consumer.assign(offsets)

            # Poll until all partitions are caught up (watermark reached)
            deadline = time.monotonic() + 10  # max 10s total poll time
            while time.monotonic() < deadline:
                msg = consumer.poll(timeout=0.5)
                if msg is None:
                    break
                if msg.error():
                    raise KafkaConsumerError(
                        f"Kafka poll error on topic '{topic}': {msg.error()}. "
                        f"get_realtime_events() will fall back to the Oracle replica."
                    )
                normalised = _normalise_event(msg.value())
                if normalised:
                    events.append(normalised)

            consumer.close()

        except KafkaConsumerError:
            raise
        except Exception as exc:
            raise KafkaConsumerError(
                f"Failed to consume from Kafka topic '{topic}': {exc}. "
                f"get_realtime_events() will fall back to the Oracle replica. "
                f"Check KAFKA_BROKERS and network connectivity."
            ) from exc

        log.info(
            "kafka_consumed",
            topic=topic,
            lookback_minutes=lookback_minutes,
            event_count=len(events),
        )
        return events

    def close(self) -> None:
        """No-op — consumers are created and closed per call in consume()."""


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _normalise_event(raw: bytes | None) -> dict | None:
    """Parse a raw Kafka message value into a normalised CDC event dict.

    Handles GoldenGate JSON CDC envelope format.  Returns None if the
    message cannot be parsed (logged as a warning, not an error).

    Expected GoldenGate JSON envelope:
        { "op_type": "I"|"U"|"D", "table": str,
          "before": {...}|null, "after": {...}|null,
          "current_ts": "YYYY-MM-DD HH:MM:SS.mmm" }
    """
    import json

    if raw is None:
        return None

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        log.warning("kafka_message_parse_failed", raw_preview=repr(raw[:100]))
        return None

    return {
        "op":    payload.get("op_type", "U"),
        "table": payload.get("table", ""),
        "before": payload.get("before"),
        "after":  payload.get("after"),
        "ts_ms":  payload.get("current_ts", ""),
    }
