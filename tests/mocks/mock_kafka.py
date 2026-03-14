"""
MockKafkaConsumer — in-memory drop-in for streaming/kafka_consumer.KafkaConsumer.

Implements the same synchronous interface.  Tests register fixture events
via set_fixture() and can simulate Kafka errors via set_error().
"""

from __future__ import annotations

from typing import Any

from src.streaming.kafka_consumer import KafkaConsumerError


class MockKafkaConsumer:
    """In-memory mock that stores fixture events per topic.

    Usage in tests::

        mock = MockKafkaConsumer(enabled=True)
        mock.set_fixture("banking.transactions", [
            {"op": "I", "table": "TRANSACTION_LOG", "before": None,
             "after": {"TXN_REFERENCE": "T001", "TXN_AMOUNT": 100.0}, "ts_ms": 0},
        ])
        events = mock.consume("banking.transactions", lookback_minutes=5)
        assert len(events) == 1
    """

    def __init__(self, enabled: bool = True) -> None:
        self._enabled = enabled
        self._fixtures: dict[str, list[dict]] = {}
        self._errors: dict[str, Exception] = {}
        self._calls: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Test setup helpers
    # ------------------------------------------------------------------

    def set_fixture(self, topic: str, events: list[dict]) -> None:
        """Register *events* to return when *topic* is consumed."""
        self._fixtures[topic] = list(events)

    def set_error(self, topic: str, error: Exception) -> None:
        """Make consume() raise *error* for *topic* (simulates Kafka failure)."""
        self._errors[topic] = error

    def call_count(self, topic: str | None = None) -> int:
        """Return number of consume() calls, optionally filtered by topic."""
        if topic is None:
            return len(self._calls)
        return sum(1 for c in self._calls if c["topic"] == topic)

    def reset(self) -> None:
        """Clear all fixtures, errors, and call history."""
        self._fixtures.clear()
        self._errors.clear()
        self._calls.clear()

    # ------------------------------------------------------------------
    # KafkaConsumer interface
    # ------------------------------------------------------------------

    def is_enabled(self) -> bool:
        """Return the enabled flag set at construction."""
        return self._enabled

    def consume(self, topic: str, lookback_minutes: int) -> list[dict]:
        """Return pre-registered fixture events for *topic*.

        Records every call for later assertion.
        Raises pre-registered error if set via set_error().
        Returns [] if no fixture registered (valid — no recent events).
        """
        self._calls.append({"topic": topic, "lookback_minutes": lookback_minutes})

        if topic in self._errors:
            raise self._errors[topic]

        return list(self._fixtures.get(topic, []))

    def close(self) -> None:
        """No-op."""
