"""
Read tools — query the Oracle GoldenGate replica (read-only).

Tools registered here:
    get_entity              — generic entity fetch by type + ID
    get_transaction_history — paginated transaction history for an account

All tools require the "read" RBAC tier.
All inputs are validated with Pydantic before any DB access.
All SQL is sourced from db/queries.py — no inline SQL here.
Every call produces an immutable audit log entry.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

from fastmcp import Context, FastMCP
from pydantic import BaseModel, Field, model_validator

from src.audit.audit_log import AuditLog, hash_payload
from src.auth.rbac import require_role
from src.db import queries
from src.schema.mapper import SchemaConfigError, SchemaMapper

mcp = FastMCP("goldengate-read-tools")


# ------------------------------------------------------------------
# Custom exceptions
# ------------------------------------------------------------------

class EntityNotFoundError(LookupError):
    """Raised when get_entity finds no matching row."""


# ------------------------------------------------------------------
# Pydantic input validators
# ------------------------------------------------------------------

class _GetEntityInput(BaseModel):
    entity_type: str = Field(min_length=1, max_length=50)
    entity_id: str = Field(
        min_length=1,
        max_length=100,
        pattern=r"^[A-Za-z0-9_\-]+$",
    )


class _GetTransactionHistoryInput(BaseModel):
    account_id: str = Field(
        min_length=1,
        max_length=50,
        pattern=r"^[A-Za-z0-9_\-]+$",
    )
    from_date: date
    to_date: date
    limit: int = Field(default=100, ge=1, le=500)

    @model_validator(mode="after")
    def check_date_range(self) -> _GetTransactionHistoryInput:
        if self.to_date < self.from_date:
            raise ValueError("to_date must be >= from_date")
        if (self.to_date - self.from_date).days > 365:
            raise ValueError("Date range cannot exceed 365 days")
        return self


# ------------------------------------------------------------------
# Dependency helpers
# ------------------------------------------------------------------

def _get_oracle_client() -> Any:
    """Return the server-level OracleClient singleton.

    Imported lazily so tests can swap the dependency before importing tools.
    """
    from src.server import get_oracle_client  # type: ignore[import]
    return get_oracle_client()


def _get_audit_log() -> AuditLog:
    """Return the server-level AuditLog singleton."""
    from src.server import get_audit_log  # type: ignore[import]
    return get_audit_log()


def _get_mapper() -> SchemaMapper:
    from src.schema.mapper import get_mapper
    return get_mapper()


def _map_row_to_logical(
    row: dict[str, Any],
    mapper: SchemaMapper,
    entity_type: str,
) -> dict[str, Any]:
    """Translate a row dict from physical column names back to logical names.

    Args:
        row:         Dict keyed by physical Oracle column names (upper-cased).
        mapper:      SchemaMapper instance.
        entity_type: Logical entity type name.

    Returns:
        Dict keyed by logical column names.
    """
    # Build reverse map: physical_name (upper) -> logical_name
    physical_to_logical = {
        v.upper(): k for k, v in mapper.all_columns(entity_type).items()
    }
    return {
        physical_to_logical.get(k.upper(), k.lower()): v
        for k, v in row.items()
    }


# ------------------------------------------------------------------
# Tools
# ------------------------------------------------------------------

@mcp.tool()
@require_role("read")
async def get_entity(
    entity_type: str,
    entity_id: str,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Fetch a single entity from the Oracle replica by type and ID.

    Args:
        entity_type: Logical entity name configured in schema_map.yaml.
                     Examples: "customer", "account", "alert".
        entity_id:   Primary key value. Alphanumeric, dash, and underscore
                     only (max 100 chars).
        ctx:         FastMCP context (injected by the framework).

    Returns:
        Dict of entity fields keyed by logical column names.

    Raises:
        ValidationError:     If entity_id contains invalid characters or is too long.
        SchemaConfigError:   If entity_type is not in schema_map.yaml.
        EntityNotFoundError: If no row matches the given entity_id.
        OracleClientError:   If the DB query fails.
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    # 1. Validate inputs
    validated = _GetEntityInput(entity_type=entity_type, entity_id=entity_id)

    # 2. Resolve schema (raises SchemaConfigError for unknown entity_type)
    mapper = _get_mapper()
    mapper.resolve_table(validated.entity_type)  # early check — surfaces error clearly

    # 3. Build SQL and execute
    sql = queries.build_get_entity_query(mapper, validated.entity_type)
    client = _get_oracle_client()
    rows = await client.query(
        query_key=queries.GET_ENTITY,
        sql=sql,
        bind_params={"entity_id": validated.entity_id},
        max_rows=1,
    )

    latency_ms = (time.perf_counter() - start) * 1000

    # 4. Audit
    inputs = {"entity_type": validated.entity_type, "entity_id": validated.entity_id}
    output = rows[0] if rows else {}
    await _get_audit_log().record(
        tool_name="get_entity",
        caller_id=caller_id,
        input_hash=hash_payload(inputs),
        output_hash=hash_payload(output),
        latency_ms=latency_ms,
        decision=None,
    )

    # 5. Return
    if not rows:
        raise EntityNotFoundError(
            f"No {validated.entity_type} found with id '{validated.entity_id}'"
        )

    return _map_row_to_logical(rows[0], mapper, validated.entity_type)


@mcp.tool()
@require_role("read")
async def get_transaction_history(
    account_id: str,
    from_date: str,
    to_date: str,
    limit: int = 100,
    ctx: Context | None = None,
) -> list[dict[str, Any]]:
    """Fetch paginated transaction history for an account from the Oracle replica.

    Args:
        account_id: Account identifier. Alphanumeric, dash, and underscore
                    only (max 50 chars).
        from_date:  Start of date range, ISO 8601 format (YYYY-MM-DD).
        to_date:    End of date range, ISO 8601 format (YYYY-MM-DD).
                    Must be >= from_date. Range cannot exceed 365 days.
        limit:      Maximum number of rows to return (1–500, default 100).
        ctx:        FastMCP context (injected by the framework).

    Returns:
        List of transaction dicts keyed by logical column names,
        ordered by value_date descending. Empty list if no transactions found.

    Raises:
        ValidationError:   If any input fails validation.
        OracleClientError: If the DB query fails.
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    # 1. Validate inputs (date parsing + range check happen inside the model)
    validated = _GetTransactionHistoryInput(
        account_id=account_id,
        from_date=from_date,  # type: ignore[arg-type]  pydantic coerces str→date
        to_date=to_date,       # type: ignore[arg-type]
        limit=limit,
    )

    # 2. Build SQL and execute
    mapper = _get_mapper()
    sql = queries.build_get_transaction_history_query(mapper)
    client = _get_oracle_client()
    rows = await client.query(
        query_key=queries.GET_TRANSACTION_HISTORY,
        sql=sql,
        bind_params={
            "account_id": validated.account_id,
            "from_date": validated.from_date,
            "to_date": validated.to_date,
            "limit": validated.limit,
        },
        max_rows=validated.limit,
    )

    latency_ms = (time.perf_counter() - start) * 1000

    # 3. Audit
    inputs = {
        "account_id": validated.account_id,
        "from_date": str(validated.from_date),
        "to_date": str(validated.to_date),
        "limit": validated.limit,
    }
    await _get_audit_log().record(
        tool_name="get_transaction_history",
        caller_id=caller_id,
        input_hash=hash_payload(inputs),
        output_hash=hash_payload(rows),
        latency_ms=latency_ms,
        decision=None,
    )

    # 4. Map physical column names → logical names and return
    return [_map_row_to_logical(row, mapper, "transaction") for row in rows]


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _caller_id(ctx: Context | None) -> str:
    """Extract a caller identifier string from FastMCP context."""
    if ctx is None:
        return "unknown"
    meta = getattr(ctx, "meta", None)
    if not isinstance(meta, dict):
        return "unknown"
    return str(meta.get("caller_id") or meta.get("role") or "unknown")
