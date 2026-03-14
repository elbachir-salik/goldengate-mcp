"""
Write tools — governed calls to the configured external write-back endpoint.

Tools registered here (Phase 4):
    flag_entity     — flag/block/unblock an entity via the bank's REST API
    post_adjustment — post a GL correcting entry or workflow approval

All tools require the "write" RBAC tier (elevated).
Each call is subject to:
    - Circuit breaker (auto-suspend if write rate exceeds threshold)
    - Full audit log entry (tool_name, caller, input_hash, output_hash, decision)
    - Pydantic input validation before any HTTP call

The server does NOT know what system is behind the write-back endpoint —
it is a generic REST client configured via WRITEBACK_BASE_URL + WRITEBACK_API_KEY.
"""

from __future__ import annotations

import time
from typing import Any

from fastmcp import Context, FastMCP
from pydantic import BaseModel, Field, model_validator

from src.audit.audit_log import AuditLog, hash_payload
from src.auth.rbac import require_role
from src.writeback.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from src.writeback.rest_client import WritebackError

mcp = FastMCP("goldengate-write-tools")


# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

VALID_FLAG_ACTIONS = frozenset({"flag", "block", "unblock", "review", "clear"})
VALID_ENTITY_TYPES_WRITE = frozenset({"customer", "account", "transaction", "alert"})

VALID_ADJUSTMENT_TYPES = frozenset({
    "gl_correction",
    "hold_release",
    "workflow_approval",
    "workflow_rejection",
})


# ------------------------------------------------------------------
# Pydantic input models
# ------------------------------------------------------------------

class _FlagEntityInput(BaseModel):
    entity_type: str
    entity_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_\-]+$")
    action: str
    reason: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_enums(self) -> _FlagEntityInput:
        if self.entity_type not in VALID_ENTITY_TYPES_WRITE:
            raise ValueError(
                f"Invalid entity_type '{self.entity_type}'. "
                f"Valid values: {sorted(VALID_ENTITY_TYPES_WRITE)}. "
                f"Use get_entity() to verify the entity exists before flagging."
            )
        if self.action not in VALID_FLAG_ACTIONS:
            raise ValueError(
                f"Invalid action '{self.action}'. "
                f"Valid values: {sorted(VALID_FLAG_ACTIONS)}. "
                f"Use 'flag' to mark for review, 'block' to restrict activity, "
                f"'unblock' to restore, or 'clear' to remove flags."
            )
        return self


class _PostAdjustmentInput(BaseModel):
    adjustment_type: str
    payload: dict[str, Any]
    reference: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_\-]+$")

    @model_validator(mode="after")
    def validate_adjustment_type(self) -> _PostAdjustmentInput:
        if self.adjustment_type not in VALID_ADJUSTMENT_TYPES:
            raise ValueError(
                f"Invalid adjustment_type '{self.adjustment_type}'. "
                f"Valid values: {sorted(VALID_ADJUSTMENT_TYPES)}. "
                f"Use 'gl_correction' for balance corrections, 'hold_release' to "
                f"release holds, or 'workflow_approval/rejection' for case decisions."
            )
        return self


# ------------------------------------------------------------------
# Dependency helpers (lazy, mockable in tests)
# ------------------------------------------------------------------

def _get_writeback_client() -> Any:
    from src.server import get_writeback_client  # type: ignore[import]
    return get_writeback_client()


def _get_circuit_breaker() -> CircuitBreaker:
    from src.server import get_circuit_breaker  # type: ignore[import]
    return get_circuit_breaker()


def _get_audit_log() -> AuditLog:
    from src.server import get_audit_log  # type: ignore[import]
    return get_audit_log()


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _caller_id(ctx: Context | None) -> str:
    if ctx is None:
        return "unknown"
    meta = getattr(ctx, "meta", None)
    if not isinstance(meta, dict):
        return "unknown"
    return str(meta.get("caller_id") or meta.get("role") or "unknown")


# ------------------------------------------------------------------
# Tool — flag_entity
# ------------------------------------------------------------------

@mcp.tool()
@require_role("write")
async def flag_entity(
    entity_type: str,
    entity_id: str,
    action: str,
    reason: str,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Flag, block, or unblock an entity via the configured write-back endpoint.

    Subject to:
    - **Circuit breaker**: raises immediately if write rate exceeds
      ``CIRCUIT_BREAKER_WRITE_LIMIT`` per ``CIRCUIT_BREAKER_RESET_SECONDS``.
    - **Full audit log**: every call is recorded regardless of outcome.
    - **RBAC**: requires the ``"write"`` tier (elevated above ``"score"``).

    Args:
        entity_type: One of ``"customer"``, ``"account"``,
                     ``"transaction"``, ``"alert"``.
        entity_id:   Primary key of the entity to flag. Alphanumeric,
                     dash, underscore only (max 100 chars).
        action:      One of ``"flag"``, ``"block"``, ``"unblock"``,
                     ``"review"``, ``"clear"``.
        reason:      Free-text justification for audit trail (max 500 chars).
        ctx:         FastMCP context (injected by the framework).

    Returns:
        Dict with keys:

        - ``entity_type``  (str): As supplied.
        - ``entity_id``    (str): As supplied.
        - ``action``       (str): As supplied.
        - ``status``       (str): Outcome from the write-back endpoint.
        - ``reference``    (str): Write-back reference ID (if returned).

    Raises:
        ValidationError:          If any input fails validation.
        CircuitBreakerOpenError:  If the write rate limit is exceeded.
                                  Includes retry guidance in the message.
        WritebackError:           If the write-back endpoint call fails.
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    # 1. Validate inputs
    validated = _FlagEntityInput(
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        reason=reason,
    )

    # 2. Circuit breaker — raises CircuitBreakerOpenError if tripped
    _get_circuit_breaker().check_and_record()

    # 3. POST to write-back endpoint
    wb_client = _get_writeback_client()
    response = await wb_client.post(
        path="/flags",
        payload={
            "entity_type": validated.entity_type,
            "entity_id": validated.entity_id,
            "action": validated.action,
            "reason": validated.reason,
            "caller_id": caller_id,
        },
    )

    latency_ms = (time.perf_counter() - start) * 1000

    result: dict[str, Any] = {
        "entity_type": validated.entity_type,
        "entity_id": validated.entity_id,
        "action": validated.action,
        "status": response.body.get("status", "submitted"),
        "reference": response.body.get("reference", ""),
    }

    # 4. Audit — always recorded for writes
    await _get_audit_log().record(
        tool_name="flag_entity",
        caller_id=caller_id,
        input_hash=hash_payload({
            "entity_type": validated.entity_type,
            "entity_id": validated.entity_id,
            "action": validated.action,
        }),
        output_hash=hash_payload(result),
        latency_ms=latency_ms,
        decision=validated.action,
    )

    return result
