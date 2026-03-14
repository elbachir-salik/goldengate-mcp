"""
Tests for tests/mocks/mock_kafka.py — MockKafkaConsumer.

We test the mock itself (not the real KafkaConsumer which needs a live broker).

Covers:
    - set_fixture + consume returns registered events
    - consume on unknown topic returns []
    - is_enabled reflects constructor flag
    - set_error causes consume to raise
    - call_count tracks invocations
    - reset clears all state
"""

from __future__ import annotations

import pytest

from src.streaming.kafka_consumer import KafkaConsumerError
from tests.mocks.mock_kafka import MockKafkaConsumer

SAMPLE_EVENTS = [
    {"op": "I", "table": "TRANSACTION_LOG",
     "before": None, "after": {"TXN_REFERENCE": "T001"}, "ts_ms": 1000},
    {"op": "U", "table": "TRANSACTION_LOG",
     "before": {"TXN_STATUS": "PENDING"}, "after": {"TXN_STATUS": "SETTLED"}, "ts_ms": 2000},
]


def test_mock_kafka_returns_fixture_events() -> None:
    mock = MockKafkaConsumer(enabled=True)
    mock.set_fixture("banking.transactions", SAMPLE_EVENTS)
    result = mock.consume("banking.transactions", lookback_minutes=5)
    assert len(result) == 2
    assert result[0]["op"] == "I"
    assert result[1]["op"] == "U"


def test_mock_kafka_empty_when_no_fixture() -> None:
    mock = MockKafkaConsumer(enabled=True)
    result = mock.consume("unknown.topic", lookback_minutes=5)
    assert result == []


def test_mock_kafka_is_enabled_true() -> None:
    mock = MockKafkaConsumer(enabled=True)
    assert mock.is_enabled() is True


def test_mock_kafka_is_enabled_false() -> None:
    mock = MockKafkaConsumer(enabled=False)
    assert mock.is_enabled() is False


def test_mock_kafka_set_error_raises_on_consume() -> None:
    mock = MockKafkaConsumer(enabled=True)
    mock.set_error("bad.topic", KafkaConsumerError("broker unreachable"))
    with pytest.raises(KafkaConsumerError, match="broker unreachable"):
        mock.consume("bad.topic", lookback_minutes=5)


def test_mock_kafka_call_count_tracks_invocations() -> None:
    mock = MockKafkaConsumer(enabled=True)
    mock.consume("topic.a", lookback_minutes=1)
    mock.consume("topic.a", lookback_minutes=2)
    mock.consume("topic.b", lookback_minutes=1)
    assert mock.call_count() == 3
    assert mock.call_count("topic.a") == 2
    assert mock.call_count("topic.b") == 1


def test_mock_kafka_reset_clears_state() -> None:
    mock = MockKafkaConsumer(enabled=True)
    mock.set_fixture("t", SAMPLE_EVENTS)
    mock.consume("t", lookback_minutes=1)
    mock.reset()
    assert mock.consume("t", lookback_minutes=1) == []
    assert mock.call_count() == 1  # only the call after reset is counted... wait
    # reset clears calls too
    mock.reset()
    assert mock.call_count() == 0


def test_mock_kafka_returns_copy_of_fixture() -> None:
    """Mutating the returned list must not affect the fixture."""
    mock = MockKafkaConsumer(enabled=True)
    mock.set_fixture("t", SAMPLE_EVENTS)
    result = mock.consume("t", lookback_minutes=5)
    result.clear()
    assert len(mock.consume("t", lookback_minutes=5)) == 2
