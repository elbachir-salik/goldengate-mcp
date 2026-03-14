"""
Tests for get_realtime_events tool in src/tools/read_tools.py.

Uses MockKafkaConsumer and MockOracleClient — no real Kafka or Oracle needed.

Covers:
    - Kafka path: events returned from Kafka, decision="kafka" in audit
    - Oracle fallback: Kafka disabled, falls back to Oracle
    - Kafka error fallback: KafkaConsumerError triggers Oracle fallback
    - Empty result from both sources: returns [], no error
    - lookback_minutes > 60: ValidationError
    - lookback_minutes = 0: ValidationError
    - Audit records source in decision field
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from src.schema.mapper import SchemaMapper
from src.streaming.kafka_consumer import KafkaConsumerError
from src.tools.read_tools import get_realtime_events
from tests.mocks.mock_kafka import MockKafkaConsumer
from tests.mocks.mock_oracle import MockOracleClient

SCHEMA_YAML = "src/schema/schema_map.yaml"

KAFKA_EVENTS = [
    {"op": "I", "table": "TRANSACTION_LOG",
     "before": None, "after": {"TXN_REFERENCE": "T001"}, "ts_ms": 1000},
]

ORACLE_TXN_ROWS = [
    {
        "TXN_REFERENCE": "T002",
        "ACCOUNT_NUMBER": "ACC100",
        "TXN_AMOUNT": 250.0,
        "CURRENCY_CODE": "USD",
        "TXN_TYPE": "DEBIT",
        "CHANNEL_CODE": "ATM",
        "COUNTERPARTY_REF": None,
        "VALUE_DATE": date(2024, 1, 15),
        "BOOKING_DATE": date(2024, 1, 15),
        "TXN_STATUS": "SETTLED",
        "NARRATIVE_TEXT": "ATM",
    }
]


@pytest.fixture()
def mapper() -> SchemaMapper:
    return SchemaMapper(SCHEMA_YAML)


@pytest.fixture()
def mock_oracle() -> MockOracleClient:
    return MockOracleClient()


@pytest.fixture()
def mock_audit() -> AsyncMock:
    audit = MagicMock()
    audit.record = AsyncMock(return_value=None)
    return audit


def _patch_all(mapper, oracle, audit, kafka):
    return (
        patch("src.tools.read_tools._get_mapper", return_value=mapper),
        patch("src.tools.read_tools._get_oracle_client", return_value=oracle),
        patch("src.tools.read_tools._get_audit_log", return_value=audit),
        patch("src.tools.read_tools._get_kafka_consumer", return_value=kafka),
    )


# ------------------------------------------------------------------
# Kafka path
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_realtime_events_kafka_path(
    mapper, mock_oracle, mock_audit
) -> None:
    kafka = MockKafkaConsumer(enabled=True)
    kafka.set_fixture("banking.transactions", KAFKA_EVENTS)

    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle, mock_audit, kafka)
    with p1, p2, p3, p4:
        result = await get_realtime_events(topic="banking.transactions", lookback_minutes=5)

    assert len(result) == 1
    assert result[0]["op"] == "I"
    # Audit decision should record source
    call_kwargs = mock_audit.record.call_args.kwargs
    assert call_kwargs["decision"] == "kafka"


@pytest.mark.asyncio
async def test_get_realtime_events_kafka_path_empty(
    mapper, mock_oracle, mock_audit
) -> None:
    """No events in window — empty list returned, no error."""
    kafka = MockKafkaConsumer(enabled=True)
    # no fixture → returns []

    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle, mock_audit, kafka)
    with p1, p2, p3, p4:
        result = await get_realtime_events(topic="banking.transactions")

    assert result == []
    mock_audit.record.assert_called_once()


# ------------------------------------------------------------------
# Oracle fallback — Kafka disabled
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_realtime_events_oracle_fallback_kafka_disabled(
    mapper, mock_oracle, mock_audit
) -> None:
    kafka = MockKafkaConsumer(enabled=False)
    # Register Oracle rows for any call to GET_REALTIME_EVENTS_FALLBACK
    # MockOracleClient returns [] for unregistered keys, so we patch query directly
    mock_oracle_with_rows = MockOracleClient()
    # We can't predict since_timestamp exactly, so patch query to return rows
    patched_query = AsyncMock(return_value=ORACLE_TXN_ROWS)

    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle_with_rows, mock_audit, kafka)
    with p1, p2, p3, p4:
        with patch.object(mock_oracle_with_rows, "query", patched_query):
            result = await get_realtime_events(topic="any.topic", lookback_minutes=5)

    assert len(result) == 1
    assert result[0]["id"] == "T002"  # logical column name
    call_kwargs = mock_audit.record.call_args.kwargs
    assert call_kwargs["decision"] == "oracle_fallback"


# ------------------------------------------------------------------
# Oracle fallback — Kafka error
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_realtime_events_kafka_error_triggers_fallback(
    mapper, mock_oracle, mock_audit
) -> None:
    kafka = MockKafkaConsumer(enabled=True)
    kafka.set_error("banking.transactions", KafkaConsumerError("broker down"))

    patched_query = AsyncMock(return_value=ORACLE_TXN_ROWS)

    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle, mock_audit, kafka)
    with p1, p2, p3, p4:
        with patch.object(mock_oracle, "query", patched_query):
            result = await get_realtime_events(topic="banking.transactions")

    assert len(result) == 1
    call_kwargs = mock_audit.record.call_args.kwargs
    assert call_kwargs["decision"] == "oracle_fallback"


@pytest.mark.asyncio
async def test_get_realtime_events_both_empty(
    mapper, mock_oracle, mock_audit
) -> None:
    kafka = MockKafkaConsumer(enabled=False)
    # Oracle also returns nothing

    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle, mock_audit, kafka)
    with p1, p2, p3, p4:
        result = await get_realtime_events(topic="any.topic")

    assert result == []
    mock_audit.record.assert_called_once()


# ------------------------------------------------------------------
# Validation errors
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_realtime_events_lookback_too_large(
    mapper, mock_oracle, mock_audit
) -> None:
    kafka = MockKafkaConsumer(enabled=False)
    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle, mock_audit, kafka)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError):
            await get_realtime_events(topic="t", lookback_minutes=61)


@pytest.mark.asyncio
async def test_get_realtime_events_lookback_zero(
    mapper, mock_oracle, mock_audit
) -> None:
    kafka = MockKafkaConsumer(enabled=False)
    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle, mock_audit, kafka)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError):
            await get_realtime_events(topic="t", lookback_minutes=0)


@pytest.mark.asyncio
async def test_get_realtime_events_empty_topic_rejected(
    mapper, mock_oracle, mock_audit
) -> None:
    kafka = MockKafkaConsumer(enabled=False)
    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle, mock_audit, kafka)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError):
            await get_realtime_events(topic="")


# ------------------------------------------------------------------
# Audit
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_realtime_events_audit_called_once(
    mapper, mock_oracle, mock_audit
) -> None:
    kafka = MockKafkaConsumer(enabled=True)
    kafka.set_fixture("t", KAFKA_EVENTS)

    p1, p2, p3, p4 = _patch_all(mapper, mock_oracle, mock_audit, kafka)
    with p1, p2, p3, p4:
        await get_realtime_events(topic="t")

    mock_audit.record.assert_called_once()
    call_kwargs = mock_audit.record.call_args.kwargs
    assert call_kwargs["tool_name"] == "get_realtime_events"
