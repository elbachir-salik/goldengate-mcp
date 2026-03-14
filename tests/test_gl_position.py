"""
Tests for get_gl_position tool in src/tools/read_tools.py.

Uses MockOracleClient — no real Oracle needed.

Covers:
    - Happy path: returns dict with logical column names
    - Not found: raises EntityNotFoundError with next-step message
    - Invalid currency (not 3 uppercase chars): ValidationError
    - Invalid account_code chars: ValidationError
    - Invalid date: ValidationError
    - Audit called once per invocation
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from src.schema.mapper import SchemaMapper
from src.tools.read_tools import EntityNotFoundError, get_gl_position
from tests.mocks.mock_oracle import MockOracleClient

SCHEMA_YAML = "src/schema/schema_map.yaml"

GL_ROW = {
    "GL_ACCOUNT_CODE": "1001-USD",
    "CURRENCY_CODE": "USD",
    "VALUE_DATE": date(2024, 1, 15),
    "DR_BALANCE": 0.0,
    "CR_BALANCE": 5000.0,
    "NET_BALANCE": 5000.0,
}


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


# ------------------------------------------------------------------
# Happy path
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_gl_position_happy_path(mapper, mock_oracle, mock_audit) -> None:
    bind = {
        "account_code": "1001-USD",
        "currency": "USD",
        "value_date": date(2024, 1, 15),
    }
    mock_oracle.set_fixture("get_gl_position", bind, [GL_ROW])

    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        result = await get_gl_position(
            account_code="1001-USD",
            currency="USD",
            value_date="2024-01-15",
        )

    assert result["account_code"] == "1001-USD"
    assert result["currency"] == "USD"
    assert result["net_balance"] == 5000.0
    mock_audit.record.assert_called_once()


# ------------------------------------------------------------------
# Not found
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_gl_position_not_found(mapper, mock_oracle, mock_audit) -> None:
    # No fixture registered → returns []
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(EntityNotFoundError, match="No GL position found"):
            await get_gl_position(
                account_code="9999-EUR",
                currency="EUR",
                value_date="2024-01-15",
            )


@pytest.mark.asyncio
async def test_get_gl_position_not_found_message_suggests_next_step(
    mapper, mock_oracle, mock_audit
) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(EntityNotFoundError, match="get_entity"):
            await get_gl_position(
                account_code="9999-EUR",
                currency="EUR",
                value_date="2024-01-15",
            )


# ------------------------------------------------------------------
# Currency validation
# ------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_currency", [
    "us",       # too short
    "USDD",     # too long
    "usd",      # lowercase
    "U1D",      # digit
    "",         # empty
])
async def test_get_gl_position_invalid_currency(
    mapper, mock_oracle, mock_audit, bad_currency
) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_gl_position(
                account_code="1001-USD",
                currency=bad_currency,
                value_date="2024-01-15",
            )


# ------------------------------------------------------------------
# Account code validation
# ------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_code", [
    "'; DROP TABLE GL; --",
    "code with spaces",
    "A" * 51,
])
async def test_get_gl_position_invalid_account_code(
    mapper, mock_oracle, mock_audit, bad_code
) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_gl_position(
                account_code=bad_code,
                currency="USD",
                value_date="2024-01-15",
            )


# ------------------------------------------------------------------
# Date validation
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_gl_position_invalid_date(mapper, mock_oracle, mock_audit) -> None:
    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await get_gl_position(
                account_code="1001-USD",
                currency="USD",
                value_date="not-a-date",
            )


# ------------------------------------------------------------------
# Audit
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_gl_position_audit_called(mapper, mock_oracle, mock_audit) -> None:
    bind = {
        "account_code": "1001-USD",
        "currency": "USD",
        "value_date": date(2024, 1, 15),
    }
    mock_oracle.set_fixture("get_gl_position", bind, [GL_ROW])

    p1, p2, p3 = _patch_deps(mapper, mock_oracle, mock_audit)
    with p1, p2, p3:
        await get_gl_position(
            account_code="1001-USD",
            currency="USD",
            value_date="2024-01-15",
        )

    mock_audit.record.assert_called_once()
    call_kwargs = mock_audit.record.call_args.kwargs
    assert call_kwargs["tool_name"] == "get_gl_position"
