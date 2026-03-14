"""
Tests for src/tools/write_tools.py — flag_entity, post_adjustment.
Also covers src/writeback/circuit_breaker.py — CircuitBreaker.

Uses MockWritebackClient — no real HTTP needed.

Covers:
    CircuitBreaker:
        - Allows writes below the limit
        - Trips (raises) when limit is reached
        - call_count() reflects active window
        - reset() clears state
        - CircuitBreakerOpenError carries count, limit, reset_in_seconds

    flag_entity:
        - Happy path: POSTs to /flags, returns status + reference
        - Circuit breaker tripped: CircuitBreakerOpenError before HTTP call
        - Writeback error: WritebackError propagated
        - Invalid entity_type: ValidationError with valid options listed
        - Invalid action: ValidationError with valid options listed
        - Invalid entity_id chars: ValidationError
        - Audit called once with tool_name="flag_entity" and decision=action

    post_adjustment:
        - Happy path: POSTs to /adjustments, returns status + confirmation_id
        - Circuit breaker tripped: CircuitBreakerOpenError
        - Invalid adjustment_type: ValidationError with valid options listed
        - Invalid reference chars: ValidationError
        - Payload keys forwarded to endpoint
        - Audit called once with tool_name="post_adjustment"
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from src.tools.write_tools import flag_entity, post_adjustment
from src.writeback.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from src.writeback.rest_client import WritebackHTTPError
from tests.mocks.mock_writeback import MockWritebackClient


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture()
def mock_wb() -> MockWritebackClient:
    return MockWritebackClient()


@pytest.fixture()
def circuit_breaker() -> CircuitBreaker:
    """Fresh circuit breaker with low limit for testing."""
    return CircuitBreaker(write_limit=3, reset_seconds=60)


@pytest.fixture()
def mock_audit() -> AsyncMock:
    audit = MagicMock()
    audit.record = AsyncMock(return_value=None)
    return audit


def _patch_write_deps(wb_client, cb, audit):
    return (
        patch("src.tools.write_tools._get_writeback_client", return_value=wb_client),
        patch("src.tools.write_tools._get_circuit_breaker", return_value=cb),
        patch("src.tools.write_tools._get_audit_log", return_value=audit),
    )


# ==================================================================
# CircuitBreaker unit tests
# ==================================================================

def test_circuit_breaker_allows_writes_below_limit() -> None:
    cb = CircuitBreaker(write_limit=3, reset_seconds=60)
    cb.check_and_record()
    cb.check_and_record()
    cb.check_and_record()
    assert cb.call_count() == 3


def test_circuit_breaker_trips_at_limit() -> None:
    cb = CircuitBreaker(write_limit=2, reset_seconds=60)
    cb.check_and_record()
    cb.check_and_record()
    with pytest.raises(CircuitBreakerOpenError):
        cb.check_and_record()


def test_circuit_breaker_error_contains_count_and_limit() -> None:
    cb = CircuitBreaker(write_limit=1, reset_seconds=60)
    cb.check_and_record()
    with pytest.raises(CircuitBreakerOpenError) as exc_info:
        cb.check_and_record()
    err = exc_info.value
    assert err.current_count == 1
    assert err.limit == 1


def test_circuit_breaker_error_message_has_retry_guidance() -> None:
    cb = CircuitBreaker(write_limit=1, reset_seconds=60)
    cb.check_and_record()
    with pytest.raises(CircuitBreakerOpenError, match="Retry in approximately"):
        cb.check_and_record()


def test_circuit_breaker_reset_clears_count() -> None:
    cb = CircuitBreaker(write_limit=2, reset_seconds=60)
    cb.check_and_record()
    cb.check_and_record()
    cb.reset()
    assert cb.call_count() == 0
    # Should allow writes again after reset
    cb.check_and_record()


def test_circuit_breaker_reset_in_seconds_present() -> None:
    cb = CircuitBreaker(write_limit=1, reset_seconds=60)
    cb.check_and_record()
    with pytest.raises(CircuitBreakerOpenError) as exc_info:
        cb.check_and_record()
    assert exc_info.value.reset_in_seconds >= 0.0


def test_circuit_breaker_from_settings() -> None:
    settings = MagicMock()
    settings.circuit_breaker_write_limit = 5
    settings.circuit_breaker_reset_seconds = 30
    cb = CircuitBreaker(settings)
    assert cb._limit == 5
    assert cb._reset_seconds == 30


# ==================================================================
# flag_entity — happy path
# ==================================================================

@pytest.mark.asyncio
async def test_flag_entity_happy_path(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    mock_wb.set_response(200, {"status": "flagged", "reference": "REF-001"})

    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        result = await flag_entity(
            entity_type="customer",
            entity_id="C001",
            action="flag",
            reason="Matches watchlist pattern",
        )

    assert result["entity_type"] == "customer"
    assert result["entity_id"] == "C001"
    assert result["action"] == "flag"
    assert result["status"] == "flagged"
    assert result["reference"] == "REF-001"
    assert mock_wb.call_count() == 1


@pytest.mark.asyncio
async def test_flag_entity_posts_to_flags_path(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    mock_wb.set_response(200, {"status": "blocked"})

    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        await flag_entity(
            entity_type="account",
            entity_id="ACC100",
            action="block",
            reason="Suspected mule account",
        )

    path, payload = mock_wb.last_call()
    assert path == "/flags"
    assert payload["entity_type"] == "account"
    assert payload["entity_id"] == "ACC100"
    assert payload["action"] == "block"


@pytest.mark.asyncio
async def test_flag_entity_all_valid_actions(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    for action in ("flag", "block", "unblock", "review", "clear"):
        mock_wb.set_response(200, {"status": "ok"})
        mock_wb._calls.clear()
        circuit_breaker.reset()

        p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
        with p1, p2, p3:
            result = await flag_entity(
                entity_type="customer",
                entity_id="C001",
                action=action,
                reason="Test",
            )

        assert result["action"] == action


# ==================================================================
# flag_entity — circuit breaker
# ==================================================================

@pytest.mark.asyncio
async def test_flag_entity_circuit_breaker_tripped(
    mock_wb, mock_audit
) -> None:
    cb = CircuitBreaker(write_limit=0, reset_seconds=60)  # always tripped

    p1, p2, p3 = _patch_write_deps(mock_wb, cb, mock_audit)
    with p1, p2, p3:
        with pytest.raises(CircuitBreakerOpenError):
            await flag_entity(
                entity_type="customer",
                entity_id="C001",
                action="flag",
                reason="Test",
            )

    # HTTP call must NOT have been made
    assert mock_wb.call_count() == 0


@pytest.mark.asyncio
async def test_flag_entity_circuit_breaker_message_has_retry_guidance(
    mock_wb, mock_audit
) -> None:
    cb = CircuitBreaker(write_limit=0, reset_seconds=60)

    p1, p2, p3 = _patch_write_deps(mock_wb, cb, mock_audit)
    with p1, p2, p3:
        with pytest.raises(CircuitBreakerOpenError, match="Retry in approximately"):
            await flag_entity(
                entity_type="customer",
                entity_id="C001",
                action="flag",
                reason="Test",
            )


# ==================================================================
# flag_entity — writeback error
# ==================================================================

@pytest.mark.asyncio
async def test_flag_entity_writeback_http_error_propagated(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    mock_wb.set_error(
        WritebackHTTPError("Upstream 500", status_code=500, body={"error": "server error"})
    )

    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        with pytest.raises(WritebackHTTPError):
            await flag_entity(
                entity_type="customer",
                entity_id="C001",
                action="flag",
                reason="Test",
            )


# ==================================================================
# flag_entity — validation errors
# ==================================================================

@pytest.mark.asyncio
async def test_flag_entity_invalid_entity_type(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Invalid entity_type"):
            await flag_entity(
                entity_type="branch",
                entity_id="C001",
                action="flag",
                reason="Test",
            )


@pytest.mark.asyncio
async def test_flag_entity_invalid_entity_type_lists_valid(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Valid values:"):
            await flag_entity(
                entity_type="unknown",
                entity_id="C001",
                action="flag",
                reason="Test",
            )


@pytest.mark.asyncio
async def test_flag_entity_invalid_action(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Invalid action"):
            await flag_entity(
                entity_type="customer",
                entity_id="C001",
                action="suspend",
                reason="Test",
            )


@pytest.mark.asyncio
async def test_flag_entity_invalid_entity_id_chars(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await flag_entity(
                entity_type="customer",
                entity_id="'; DROP TABLE --",
                action="flag",
                reason="Test",
            )


# ==================================================================
# flag_entity — audit
# ==================================================================

@pytest.mark.asyncio
async def test_flag_entity_audit_called(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    mock_wb.set_response(200, {"status": "flagged"})

    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        await flag_entity(
            entity_type="customer",
            entity_id="C001",
            action="block",
            reason="Test",
        )

    mock_audit.record.assert_called_once()
    kwargs = mock_audit.record.call_args.kwargs
    assert kwargs["tool_name"] == "flag_entity"
    assert kwargs["decision"] == "block"


# ==================================================================
# post_adjustment — happy path
# ==================================================================

@pytest.mark.asyncio
async def test_post_adjustment_happy_path(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    mock_wb.set_response(200, {"status": "posted", "confirmation_id": "CONF-42"})

    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        result = await post_adjustment(
            adjustment_type="gl_correction",
            payload={"debit_account": "1001-USD", "credit_account": "9999-USD", "amount": 500.0},
            reference="RECON-2024-001",
        )

    assert result["adjustment_type"] == "gl_correction"
    assert result["reference"] == "RECON-2024-001"
    assert result["status"] == "posted"
    assert result["confirmation_id"] == "CONF-42"
    assert mock_wb.call_count() == 1


@pytest.mark.asyncio
async def test_post_adjustment_posts_to_adjustments_path(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    mock_wb.set_response(200, {"status": "ok"})

    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        await post_adjustment(
            adjustment_type="hold_release",
            payload={"hold_id": "H001"},
            reference="REF-001",
        )

    path, payload = mock_wb.last_call()
    assert path == "/adjustments"
    assert payload["adjustment_type"] == "hold_release"
    assert payload["reference"] == "REF-001"
    assert payload["hold_id"] == "H001"  # payload keys forwarded


@pytest.mark.asyncio
async def test_post_adjustment_all_valid_types(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    for adj_type in ("gl_correction", "hold_release", "workflow_approval", "workflow_rejection"):
        mock_wb.set_response(200, {"status": "ok"})
        mock_wb._calls.clear()
        circuit_breaker.reset()

        p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
        with p1, p2, p3:
            result = await post_adjustment(
                adjustment_type=adj_type,
                payload={"detail": "test"},
                reference="REF-001",
            )

        assert result["adjustment_type"] == adj_type


# ==================================================================
# post_adjustment — circuit breaker
# ==================================================================

@pytest.mark.asyncio
async def test_post_adjustment_circuit_breaker_tripped(
    mock_wb, mock_audit
) -> None:
    cb = CircuitBreaker(write_limit=0, reset_seconds=60)

    p1, p2, p3 = _patch_write_deps(mock_wb, cb, mock_audit)
    with p1, p2, p3:
        with pytest.raises(CircuitBreakerOpenError):
            await post_adjustment(
                adjustment_type="gl_correction",
                payload={},
                reference="REF-001",
            )

    assert mock_wb.call_count() == 0


# ==================================================================
# post_adjustment — validation errors
# ==================================================================

@pytest.mark.asyncio
async def test_post_adjustment_invalid_adjustment_type(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Invalid adjustment_type"):
            await post_adjustment(
                adjustment_type="wire_transfer",
                payload={},
                reference="REF-001",
            )


@pytest.mark.asyncio
async def test_post_adjustment_invalid_type_lists_valid(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError, match="Valid values:"):
            await post_adjustment(
                adjustment_type="bad_type",
                payload={},
                reference="REF-001",
            )


@pytest.mark.asyncio
async def test_post_adjustment_invalid_reference_chars(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        with pytest.raises(ValidationError):
            await post_adjustment(
                adjustment_type="gl_correction",
                payload={},
                reference="ref with spaces",
            )


# ==================================================================
# post_adjustment — audit
# ==================================================================

@pytest.mark.asyncio
async def test_post_adjustment_audit_called(
    mock_wb, circuit_breaker, mock_audit
) -> None:
    mock_wb.set_response(200, {"status": "posted"})

    p1, p2, p3 = _patch_write_deps(mock_wb, circuit_breaker, mock_audit)
    with p1, p2, p3:
        await post_adjustment(
            adjustment_type="workflow_approval",
            payload={"case_id": "CASE-001"},
            reference="REF-999",
        )

    mock_audit.record.assert_called_once()
    kwargs = mock_audit.record.call_args.kwargs
    assert kwargs["tool_name"] == "post_adjustment"
    assert kwargs["decision"] == "workflow_approval"
