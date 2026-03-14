"""
Tests for src/tools/read_tools.py — get_entity and get_transaction_history.

Uses MockOracleClient — no real Oracle connection needed.
The AuditLog is also mocked out to a no-op so tests stay self-contained.

Covers get_entity:
    - happy path: returns row dict with logical column names
    - entity not found: raises EntityNotFoundError
    - unknown entity_type: raises SchemaConfigError
    - invalid entity_id chars (injection attempt): raises ValidationError
    - entity_id too long: raises ValidationError

Covers get_transaction_history:
    - happy path: returns list of transaction dicts
    - date range > 365 days: raises ValidationError
    - to_date before from_date: raises ValidationError
    - limit > 500 is rejected by Pydantic (le=500 constraint)
    - empty result set: returns [], no error
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from src.schema.mapper import SchemaMapper
from src.tools.read_tools import (
    EntityNotFoundError,
    _map_row_to_logical,
    get_entity,
    get_transaction_history,
)
from tests.mocks.mock_oracle import MockOracleClient

# Path to the real schema_map.yaml
SCHEMA_YAML = "src/schema/schema_map.yaml"


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture()
def mapper() -> SchemaMapper:
    return SchemaMapper(SCHEMA_YAML)


@pytest.fixture()
def mock_oracle() -> MockOracleClient:
    return MockOracleClient()


@pytest.fixture()
def mock_audit() -> AsyncMock:
    """A no-op AuditLog mock so tests don't write to disk."""
    audit = MagicMock()
    audit.record = AsyncMock(return_value=None)
    return audit


@pytest.fixture()
def customer_row() -> dict[str, Any]:
    """A sample physical-column row as Oracle would return it."""
    return {
        "CUSTOMER_ID": "C001",
        "FULL_NAME": "Alice Example",
        "CUST_STATUS": "ACTIVE",
        "KYC_FLAG": "Y",
        "RISK_CATEGORY": "LOW",
        "CREATED_DATE": date(2022, 1, 1),
        "LAST_MODIFIED": date(2024, 6, 1),
    }


@pytest.fixture()
def transaction_rows() -> list[dict[str, Any]]:
    """Two sample transaction rows in physical column format."""
    return [
        {
            "TXN_REFERENCE": "TXN001",
            "ACCOUNT_NUMBER": "ACC100",
            "TXN_AMOUNT": 500.00,
            "CURRENCY_CODE": "USD",
            "TXN_TYPE": "DEBIT",
            "CHANNEL_CODE": "ATM",
            "COUNTERPARTY_REF": None,
            "VALUE_DATE": date(2024, 1, 15),
            "BOOKING_DATE": date(2024, 1, 15),
            "TXN_STATUS": "SETTLED",
            "NARRATIVE_TEXT": "ATM withdrawal",
        },
        {
            "TXN_REFERENCE": "TXN002",
            "ACCOUNT_NUMBER": "ACC100",
            "TXN_AMOUNT": 1200.00,
            "CURRENCY_CODE": "USD",
            "TXN_TYPE": "CREDIT",
            "CHANNEL_CODE": "ONLINE",
            "COUNTERPARTY_REF": "EXT999",
            "VALUE_DATE": date(2024, 1, 10),
            "BOOKING_DATE": date(2024, 1, 10),
            "TXN_STATUS": "SETTLED",
            "NARRATIVE_TEXT": "Salary payment",
        },
    ]


# ------------------------------------------------------------------
# Helper: patch dependencies for a tool call
# ------------------------------------------------------------------

def _patch_deps(mock_oracle: MockOracleClient, mock_audit: AsyncMock, mapper: SchemaMapper):
    """Context manager that patches oracle client, audit log, and mapper."""
    return (
        patch("src.tools.read_tools._get_oracle_client", return_value=mock_oracle),
        patch("src.tools.read_tools._get_audit_log", return_value=mock_audit),
        patch("src.tools.read_tools._get_mapper", return_value=mapper),
    )


# ------------------------------------------------------------------
# _map_row_to_logical — unit tests
# ------------------------------------------------------------------

def test_map_row_to_logical_customer(mapper: SchemaMapper) -> None:
    row = {"CUSTOMER_ID": "C001", "FULL_NAME": "Alice", "CUST_STATUS": "ACTIVE"}
    result = _map_row_to_logical(row, mapper, "customer")
    assert result["id"] == "C001"
    assert result["name"] == "Alice"
    assert result["status"] == "ACTIVE"


def test_map_row_to_logical_unknown_col_falls_back_to_lower(
    mapper: SchemaMapper,
) -> None:
    """Physical columns not in the schema map are lowercased and passed through."""
    row = {"MYSTERY_COLUMN": "value"}
    result = _map_row_to_logical(row, mapper, "customer")
    assert result.get("mystery_column") == "value"


# ------------------------------------------------------------------
# get_entity — happy path
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_entity_happy_path(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
    customer_row: dict,
) -> None:
    mock_oracle.set_fixture("get_entity", {"entity_id": "C001"}, [customer_row])

    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        result = await get_entity(entity_type="customer", entity_id="C001")

    assert result["id"] == "C001"
    assert result["name"] == "Alice Example"
    assert result["status"] == "ACTIVE"
    mock_oracle.assert_called_with("get_entity", {"entity_id": "C001"})
    mock_audit.record.assert_called_once()


# ------------------------------------------------------------------
# get_entity — EntityNotFoundError
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_entity_not_found(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
) -> None:
    # No fixture registered → returns []
    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(EntityNotFoundError, match="No customer found"):
            await get_entity(entity_type="customer", entity_id="MISSING")


# ------------------------------------------------------------------
# get_entity — invalid entity_type
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_entity_invalid_entity_type(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
) -> None:
    from src.schema.mapper import SchemaConfigError

    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(SchemaConfigError, match="Unknown entity type"):
            await get_entity(entity_type="mortgage", entity_id="M001")


# ------------------------------------------------------------------
# get_entity — Pydantic validation on entity_id
# ------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_id", [
    "'; DROP TABLE customers; --",   # SQL injection attempt
    "id with spaces",
    "id/with/slashes",
    "id<script>",
])
async def test_get_entity_invalid_id_chars(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
    bad_id: str,
) -> None:
    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_entity(entity_type="customer", entity_id=bad_id)


@pytest.mark.asyncio
async def test_get_entity_id_too_long(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
) -> None:
    long_id = "A" * 101
    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_entity(entity_type="customer", entity_id=long_id)


# ------------------------------------------------------------------
# get_transaction_history — happy path
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_transaction_history_happy_path(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
    transaction_rows: list[dict],
) -> None:
    bind = {
        "account_id": "ACC100",
        "from_date": date(2024, 1, 1),
        "to_date": date(2024, 1, 31),
        "limit": 100,
    }
    mock_oracle.set_fixture("get_transaction_history", bind, transaction_rows)

    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        results = await get_transaction_history(
            account_id="ACC100",
            from_date="2024-01-01",
            to_date="2024-01-31",
            limit=100,
        )

    assert len(results) == 2
    assert results[0]["id"] == "TXN001"
    assert results[0]["amount"] == 500.00
    assert results[1]["type"] == "CREDIT"
    mock_audit.record.assert_called_once()


# ------------------------------------------------------------------
# get_transaction_history — empty result
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_transaction_history_empty_result(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
) -> None:
    # No fixture → returns []
    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        results = await get_transaction_history(
            account_id="ACC999",
            from_date="2024-01-01",
            to_date="2024-01-31",
        )

    assert results == []
    mock_audit.record.assert_called_once()


# ------------------------------------------------------------------
# get_transaction_history — date validation
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_transaction_history_to_before_from(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
) -> None:
    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="to_date must be >= from_date"):
            await get_transaction_history(
                account_id="ACC100",
                from_date="2024-06-01",
                to_date="2024-01-01",
            )


@pytest.mark.asyncio
async def test_get_transaction_history_date_range_too_large(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
) -> None:
    from_d = date(2023, 1, 1)
    to_d = from_d + timedelta(days=366)

    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="cannot exceed 365 days"):
            await get_transaction_history(
                account_id="ACC100",
                from_date=str(from_d),
                to_date=str(to_d),
            )


# ------------------------------------------------------------------
# get_transaction_history — limit validation
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_transaction_history_limit_above_500_rejected(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
) -> None:
    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_transaction_history(
                account_id="ACC100",
                from_date="2024-01-01",
                to_date="2024-01-31",
                limit=501,
            )


@pytest.mark.asyncio
async def test_get_transaction_history_limit_zero_rejected(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
) -> None:
    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_transaction_history(
                account_id="ACC100",
                from_date="2024-01-01",
                to_date="2024-01-31",
                limit=0,
            )


# ------------------------------------------------------------------
# get_transaction_history — invalid account_id
# ------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_id", [
    "ACC 100",           # space
    "ACC;DROP--",        # SQL injection attempt
    "A" * 51,            # too long
])
async def test_get_transaction_history_invalid_account_id(
    mapper: SchemaMapper,
    mock_oracle: MockOracleClient,
    mock_audit: AsyncMock,
    bad_id: str,
) -> None:
    p1, p2, p3 = _patch_deps(mock_oracle, mock_audit, mapper)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_transaction_history(
                account_id=bad_id,
                from_date="2024-01-01",
                to_date="2024-01-31",
            )
