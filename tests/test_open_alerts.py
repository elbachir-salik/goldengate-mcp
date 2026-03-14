"""
Tests for get_open_alerts tool in src/tools/read_tools.py.

Uses MockOracleClient — no real Oracle needed.

Covers:
    - No filters: returns all alerts
    - Filter by alert_type only
    - Filter by status only
    - Both filters combined
    - Empty result: returns [], no error
    - Invalid alert_type: ValidationError with valid options listed
    - Invalid status: ValidationError with valid options listed
    - limit too large (>200): ValidationError
    - limit zero: ValidationError
    - Audit called once per invocation
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from src.schema.mapper import SchemaMapper
from src.tools.read_tools import get_open_alerts
from tests.mocks.mock_oracle import MockOracleClient

SCHEMA_YAML = "src/schema/schema_map.yaml"


def _alert_row(alert_id: str, alert_type: str, status: str) -> dict[str, Any]:
    return {
        "ALERT_ID": alert_id,
        "ALERT_TYPE": alert_type,
        "SUBJECT_ID": "C001",
        "SUBJECT_TYPE": "customer",
        "SEVERITY_LEVEL": "HIGH",
        "ALERT_STATUS": status,
        "CREATED_TIMESTAMP": date(2024, 1, 10),
        "LAST_UPDATE_TS": date(2024, 1, 11),
        "ALERT_DESCRIPTION": "Test alert",
    }


FRAUD_OPEN = _alert_row("A001", "fraud", "open")
AML_OPEN = _alert_row("A002", "aml", "open")
FRAUD_CLOSED = _alert_row("A003", "fraud", "closed")


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


def _patch_deps(mapper, oracle, audit):
    return (
        patch("src.tools.read_tools._get_mapper", return_value=mapper),
        patch("src.tools.read_tools._get_oracle_client", return_value=oracle),
        patch("src.tools.read_tools._get_audit_log", return_value=audit),
    )


def _bind(alert_type=None, status=None, limit=50):
    return {"alert_type": alert_type, "status": status, "limit": limit}


# ------------------------------------------------------------------
# Happy paths
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_open_alerts_no_filters(mapper, mock_oracle, mock_audit) -> None:
    mock_oracle.set_fixture("get_open_alerts", _bind(), [FRAUD_OPEN, AML_OPEN, FRAUD_CLOSED])

    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        result = await get_open_alerts()

    assert len(result) == 3
    assert result[0]["id"] == "A001"
    assert result[0]["type"] == "fraud"


@pytest.mark.asyncio
async def test_get_open_alerts_filter_by_type(mapper, mock_oracle, mock_audit) -> None:
    mock_oracle.set_fixture(
        "get_open_alerts", _bind(alert_type="fraud"), [FRAUD_OPEN, FRAUD_CLOSED]
    )

    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        result = await get_open_alerts(alert_type="fraud")

    assert len(result) == 2
    assert all(r["type"] == "fraud" for r in result)


@pytest.mark.asyncio
async def test_get_open_alerts_filter_by_status(mapper, mock_oracle, mock_audit) -> None:
    mock_oracle.set_fixture(
        "get_open_alerts", _bind(status="open"), [FRAUD_OPEN, AML_OPEN]
    )

    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        result = await get_open_alerts(status="open")

    assert len(result) == 2
    assert all(r["status"] == "open" for r in result)


@pytest.mark.asyncio
async def test_get_open_alerts_both_filters(mapper, mock_oracle, mock_audit) -> None:
    mock_oracle.set_fixture(
        "get_open_alerts", _bind(alert_type="fraud", status="open"), [FRAUD_OPEN]
    )

    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        result = await get_open_alerts(alert_type="fraud", status="open")

    assert len(result) == 1
    assert result[0]["id"] == "A001"


@pytest.mark.asyncio
async def test_get_open_alerts_empty_result(mapper, mock_oracle, mock_audit) -> None:
    # No fixture registered → []
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        result = await get_open_alerts(alert_type="aml", status="escalated")

    assert result == []
    mock_audit.record.assert_called_once()


# ------------------------------------------------------------------
# Validation errors — alert_type
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_open_alerts_invalid_type(mapper, mock_oracle, mock_audit) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Invalid alert_type 'kyc'"):
            await get_open_alerts(alert_type="kyc")


@pytest.mark.asyncio
async def test_get_open_alerts_invalid_type_message_lists_valid_values(
    mapper, mock_oracle, mock_audit
) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Valid values:"):
            await get_open_alerts(alert_type="bad_type")


# ------------------------------------------------------------------
# Validation errors — status
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_open_alerts_invalid_status(mapper, mock_oracle, mock_audit) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Invalid status 'archived'"):
            await get_open_alerts(status="archived")


@pytest.mark.asyncio
async def test_get_open_alerts_invalid_status_message_lists_valid_values(
    mapper, mock_oracle, mock_audit
) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Pass None to retrieve all"):
            await get_open_alerts(status="unknown_status")


# ------------------------------------------------------------------
# Validation errors — limit
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_open_alerts_limit_too_large(mapper, mock_oracle, mock_audit) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_open_alerts(limit=201)


@pytest.mark.asyncio
async def test_get_open_alerts_limit_zero(mapper, mock_oracle, mock_audit) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_open_alerts(limit=0)


# ------------------------------------------------------------------
# Audit
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_open_alerts_audit_called(mapper, mock_oracle, mock_audit) -> None:
    mock_oracle.set_fixture("get_open_alerts", _bind(), [FRAUD_OPEN])

    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        await get_open_alerts()

    mock_audit.record.assert_called_once()
    call_kwargs = mock_audit.record.call_args.kwargs
    assert call_kwargs["tool_name"] == "get_open_alerts"
