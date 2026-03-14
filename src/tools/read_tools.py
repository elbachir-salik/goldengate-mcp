"""
Read tools — query the Oracle GoldenGate replica (read-only).

Tools registered here:
    get_entity              — generic entity fetch by type + ID
    get_transaction_history — paginated transaction history for an account
    get_realtime_events     — recent CDC events from Kafka (Oracle fallback)
    get_gl_position         — GL balance for reconciliation
    get_open_alerts         — query open alerts by type / status

All tools require the "read" RBAC tier.
All inputs are validated with Pydantic before any DB access.
All SQL is sourced from db/queries.py — no inline SQL here.
Every call produces an immutable audit log entry.
"""

from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastmcp import Context, FastMCP
from pydantic import BaseModel, Field, model_validator

from src.audit.audit_log import AuditLog, hash_payload
from src.auth.rbac import require_role
from src.db import queries
from src.schema.mapper import SchemaConfigError, SchemaMapper
from src.tools.common import caller_id as _caller_id, map_row_to_logical as _map_row_to_logical

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
            f"No {validated.entity_type} found with id '{validated.entity_id}'. "
            f"Verify the id is correct, or try get_open_alerts(subject_id='{validated.entity_id}') "
            f"to check for related alerts."
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
# Input models — new tools
# ------------------------------------------------------------------

class _GetRealtimeEventsInput(BaseModel):
    topic: str = Field(min_length=1, max_length=200)
    lookback_minutes: int = Field(default=5, ge=1, le=60)


class _GetGLPositionInput(BaseModel):
    account_code: str = Field(
        min_length=1,
        max_length=50,
        pattern=r"^[A-Za-z0-9_\-]+$",
    )
    currency: str = Field(
        min_length=3,
        max_length=3,
        pattern=r"^[A-Z]{3}$",
    )
    value_date: date


VALID_ALERT_TYPES = frozenset({"fraud", "aml", "recon_break", "custom"})
VALID_ALERT_STATUSES = frozenset({"open", "closed", "escalated", "pending"})


class _GetOpenAlertsInput(BaseModel):
    alert_type: str | None = None
    status: str | None = None
    limit: int = Field(default=50, ge=1, le=200)

    @model_validator(mode="after")
    def validate_enums(self) -> _GetOpenAlertsInput:
        if self.alert_type is not None and self.alert_type not in VALID_ALERT_TYPES:
            raise ValueError(
                f"Invalid alert_type '{self.alert_type}'. "
                f"Valid values: {sorted(VALID_ALERT_TYPES)}. "
                f"Pass None to retrieve all alert types."
            )
        if self.status is not None and self.status not in VALID_ALERT_STATUSES:
            raise ValueError(
                f"Invalid status '{self.status}'. "
                f"Valid values: {sorted(VALID_ALERT_STATUSES)}. "
                f"Pass None to retrieve all statuses."
            )
        return self


# ------------------------------------------------------------------
# Dependency helpers — new tools
# ------------------------------------------------------------------

def _get_kafka_consumer() -> Any:
    """Return the server-level KafkaConsumer singleton (lazy, mockable)."""
    from src.server import get_kafka_consumer  # type: ignore[import]
    return get_kafka_consumer()


# ------------------------------------------------------------------
# Tools — get_realtime_events, get_gl_position, get_open_alerts
# ------------------------------------------------------------------

@mcp.tool()
@require_role("read")
async def get_realtime_events(
    topic: str,
    lookback_minutes: int = 5,
    ctx: Context | None = None,
) -> list[dict[str, Any]]:
    """Return recent CDC change events from a Kafka topic (or Oracle fallback).

    Tries Kafka first; automatically falls back to querying the Oracle replica
    if Kafka is disabled or unreachable.  An empty result is valid — it means
    no events occurred in the lookback window.

    Args:
        topic:            Kafka topic name (e.g. "banking.transactions").
                          When using Oracle fallback this is ignored — all
                          recent transactions are returned.
        lookback_minutes: How far back to look (1–60 minutes, default 5).
                          For windows longer than 60 minutes use
                          get_transaction_history() instead.
        ctx:              FastMCP context (injected by the framework).

    Returns:
        List of CDC event dicts. Kafka events are normalised to:
            { op, table, before, after, ts_ms }
        Oracle fallback events are transaction rows with logical column names.

    Raises:
        ValidationError: If lookback_minutes is outside 1–60.
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    validated = _GetRealtimeEventsInput(topic=topic, lookback_minutes=lookback_minutes)

    source = "kafka"
    events: list[dict] = []

    kafka = _get_kafka_consumer()
    if kafka.is_enabled():
        try:
            events = await asyncio.to_thread(
                kafka.consume, validated.topic, validated.lookback_minutes
            )
        except Exception:
            source = "oracle_fallback"
    else:
        source = "oracle_fallback"

    if source == "oracle_fallback":
        mapper = _get_mapper()
        since_ts = datetime.now(timezone.utc).replace(tzinfo=None) - \
                   timedelta(minutes=validated.lookback_minutes)
        sql = queries.build_get_realtime_events_fallback_query(mapper)
        client = _get_oracle_client()
        rows = await client.query(
            query_key=queries.GET_REALTIME_EVENTS_FALLBACK,
            sql=sql,
            bind_params={"since_timestamp": since_ts, "limit": 500},
            max_rows=500,
        )
        events = [_map_row_to_logical(r, mapper, "transaction") for r in rows]

    latency_ms = (time.perf_counter() - start) * 1000

    await _get_audit_log().record(
        tool_name="get_realtime_events",
        caller_id=caller_id,
        input_hash=hash_payload({"topic": validated.topic,
                                  "lookback_minutes": validated.lookback_minutes}),
        output_hash=hash_payload(events),
        latency_ms=latency_ms,
        decision=source,
    )
    return events


@mcp.tool()
@require_role("read")
async def get_gl_position(
    account_code: str,
    currency: str,
    value_date: str,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Fetch the GL balance for an account, currency, and value date.

    Args:
        account_code: GL account code (alphanumeric, dash, underscore; max 50 chars).
                      Example: "1001-USD".
        currency:     ISO 4217 3-letter currency code in uppercase (e.g. "USD", "EUR").
        value_date:   Balance date in ISO 8601 format YYYY-MM-DD.
        ctx:          FastMCP context (injected by the framework).

    Returns:
        Dict of GL fields keyed by logical column names:
        account_code, currency, value_date, debit_balance,
        credit_balance, net_balance.

    Raises:
        ValidationError:     If any input fails validation.
        EntityNotFoundError: If no GL position exists for the given key.
                             Try a different value_date (must be a business day)
                             or verify the account_code with get_entity("account").
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    validated = _GetGLPositionInput(
        account_code=account_code,
        currency=currency,
        value_date=value_date,  # type: ignore[arg-type]
    )

    mapper = _get_mapper()
    sql = queries.build_get_gl_position_query(mapper)
    client = _get_oracle_client()
    rows = await client.query(
        query_key=queries.GET_GL_POSITION,
        sql=sql,
        bind_params={
            "account_code": validated.account_code,
            "currency": validated.currency,
            "value_date": validated.value_date,
        },
        max_rows=1,
    )

    latency_ms = (time.perf_counter() - start) * 1000
    output = rows[0] if rows else {}

    await _get_audit_log().record(
        tool_name="get_gl_position",
        caller_id=caller_id,
        input_hash=hash_payload({
            "account_code": validated.account_code,
            "currency": validated.currency,
            "value_date": str(validated.value_date),
        }),
        output_hash=hash_payload(output),
        latency_ms=latency_ms,
        decision=None,
    )

    if not rows:
        raise EntityNotFoundError(
            f"No GL position found for account '{validated.account_code}', "
            f"currency '{validated.currency}', date '{validated.value_date}'. "
            f"Confirm the value_date is a business day and the account_code format "
            f"is correct (e.g. '1001-USD'). "
            f"Try get_entity('account', '{validated.account_code}') to verify the account exists."
        )

    return _map_row_to_logical(rows[0], mapper, "gl_entry")


@mcp.tool()
@require_role("read")
async def get_open_alerts(
    alert_type: str | None = None,
    status: str | None = None,
    limit: int = 50,
    ctx: Context | None = None,
) -> list[dict[str, Any]]:
    """Return open alerts from the alert queue, with optional filters.

    Args:
        alert_type: Filter by alert type. One of: "fraud", "aml",
                    "recon_break", "custom". Pass None (default) for all types.
        status:     Filter by status. One of: "open", "closed", "escalated",
                    "pending". Pass None (default) for all statuses.
        limit:      Maximum number of alerts to return (1–200, default 50).
                    Results are ordered by creation time descending.
        ctx:        FastMCP context (injected by the framework).

    Returns:
        List of alert dicts keyed by logical column names.
        Empty list means no alerts match the filters — not an error.

    Raises:
        ValidationError: If alert_type or status is not a recognised value,
                         or limit is outside 1–200.
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    validated = _GetOpenAlertsInput(alert_type=alert_type, status=status, limit=limit)

    mapper = _get_mapper()
    sql = queries.build_get_open_alerts_query(mapper)
    client = _get_oracle_client()
    rows = await client.query(
        query_key=queries.GET_OPEN_ALERTS,
        sql=sql,
        bind_params={
            "alert_type": validated.alert_type,
            "status": validated.status,
            "limit": validated.limit,
        },
        max_rows=validated.limit,
    )

    latency_ms = (time.perf_counter() - start) * 1000

    await _get_audit_log().record(
        tool_name="get_open_alerts",
        caller_id=caller_id,
        input_hash=hash_payload({
            "alert_type": validated.alert_type,
            "status": validated.status,
            "limit": validated.limit,
        }),
        output_hash=hash_payload(rows),
        latency_ms=latency_ms,
        decision=None,
    )

    return [_map_row_to_logical(row, mapper, "alert") for row in rows]


