"""
SQL query builders — the ONLY place SQL strings are constructed.

All functions accept a SchemaMapper instance and return fully-formed,
parameterized SQL strings using Oracle :bind syntax.

Column and table names come exclusively from the SchemaMapper (schema_map.yaml).
No hardcoded column/table names appear anywhere in this file or in callers.
No f-string interpolation of user-supplied values — only schema-resolved names.

Query keys (used for audit logging and oracle_client.query() calls):
    GET_ENTITY                    — fetch a single entity row by its ID column
    GET_TRANSACTION_HISTORY       — paginated transactions for an account + date range
    GET_GL_POSITION               — GL balance for account + currency + value_date
    GET_OPEN_ALERTS               — alerts filtered by type and/or status
    GET_REALTIME_EVENTS_FALLBACK  — recent transactions when Kafka is unavailable
"""

from __future__ import annotations

from src.schema.mapper import SchemaMapper

# ------------------------------------------------------------------
# Query key constants — used as the query_key argument to OracleClient.query()
# ------------------------------------------------------------------
GET_ENTITY = "get_entity"
GET_TRANSACTION_HISTORY = "get_transaction_history"
GET_GL_POSITION = "get_gl_position"
GET_OPEN_ALERTS = "get_open_alerts"
GET_REALTIME_EVENTS_FALLBACK = "get_realtime_events_fallback"


def build_get_entity_query(mapper: SchemaMapper, entity_type: str) -> str:
    """Build a SELECT query that fetches one entity row by its logical 'id' column.

    The query uses an explicit column list (all configured columns for the entity)
    rather than SELECT * so that column ordering is stable and predictable.

    Bind variable:
        :entity_id — the value of the entity's primary key column.

    Args:
        mapper:      SchemaMapper instance.
        entity_type: Logical entity name (e.g. "customer", "account").

    Returns:
        Parameterized SQL string ready for python-oracledb execution.

    Example output (customer):
        SELECT CUSTOMER_ID, FULL_NAME, CUST_STATUS, KYC_FLAG,
               RISK_CATEGORY, CREATED_DATE, LAST_MODIFIED
        FROM   BANKING.CUSTOMER_MASTER
        WHERE  CUSTOMER_ID = :entity_id
        FETCH FIRST 1 ROWS ONLY
    """
    table = mapper.resolve_table(entity_type)
    id_col = mapper.resolve_column(entity_type, "id")
    columns = mapper.all_columns(entity_type)

    # Build explicit column list in stable order (logical name order from YAML)
    physical_cols = ", ".join(columns.values())

    return (
        f"SELECT {physical_cols} "
        f"FROM {table} "
        f"WHERE {id_col} = :entity_id "
        f"FETCH FIRST 1 ROWS ONLY"
    )


def build_get_transaction_history_query(mapper: SchemaMapper) -> str:
    """Build a SELECT query for paginated transaction history on an account.

    Bind variables:
        :account_id  — the account's physical ID column value
        :from_date   — lower bound of VALUE_DATE (inclusive)
        :to_date     — upper bound of VALUE_DATE (inclusive)
        :limit       — max rows to return (enforced server-side via FETCH FIRST)

    Args:
        mapper: SchemaMapper instance.

    Returns:
        Parameterized SQL string ready for python-oracledb execution.

    Example output:
        SELECT TXN_REFERENCE, ACCOUNT_NUMBER, TXN_AMOUNT, ...
        FROM   BANKING.TRANSACTION_LOG
        WHERE  ACCOUNT_NUMBER = :account_id
          AND  VALUE_DATE     >= :from_date
          AND  VALUE_DATE     <= :to_date
        ORDER BY VALUE_DATE DESC, TXN_REFERENCE DESC
        FETCH FIRST :limit ROWS ONLY
    """
    table = mapper.resolve_table("transaction")
    account_id_col = mapper.resolve_column("transaction", "account_id")
    value_date_col = mapper.resolve_column("transaction", "value_date")
    columns = mapper.all_columns("transaction")

    physical_cols = ", ".join(columns.values())

    return (
        f"SELECT {physical_cols} "
        f"FROM {table} "
        f"WHERE {account_id_col} = :account_id "
        f"AND {value_date_col} >= :from_date "
        f"AND {value_date_col} <= :to_date "
        f"ORDER BY {value_date_col} DESC, {mapper.resolve_column('transaction', 'id')} DESC "
        f"FETCH FIRST :limit ROWS ONLY"
    )


def build_get_gl_position_query(mapper: SchemaMapper) -> str:
    """Build a SELECT query that fetches a single GL balance row.

    Bind variables:
        :account_code — the GL account code
        :currency     — 3-char ISO currency code
        :value_date   — the balance date

    Args:
        mapper: SchemaMapper instance.

    Returns:
        Parameterized SQL string ready for python-oracledb execution.

    Example output:
        SELECT GL_ACCOUNT_CODE, CURRENCY_CODE, VALUE_DATE,
               DR_BALANCE, CR_BALANCE, NET_BALANCE
        FROM   BANKING.GL_BALANCE
        WHERE  GL_ACCOUNT_CODE = :account_code
          AND  CURRENCY_CODE   = :currency
          AND  VALUE_DATE      = :value_date
        FETCH FIRST 1 ROWS ONLY
    """
    table = mapper.resolve_table("gl_entry")
    account_code_col = mapper.resolve_column("gl_entry", "account_code")
    currency_col = mapper.resolve_column("gl_entry", "currency")
    value_date_col = mapper.resolve_column("gl_entry", "value_date")
    columns = mapper.all_columns("gl_entry")

    physical_cols = ", ".join(columns.values())

    return (
        f"SELECT {physical_cols} "
        f"FROM {table} "
        f"WHERE {account_code_col} = :account_code "
        f"AND {currency_col} = :currency "
        f"AND {value_date_col} = :value_date "
        f"FETCH FIRST 1 ROWS ONLY"
    )


def build_get_open_alerts_query(mapper: SchemaMapper) -> str:
    """Build a SELECT query for open alerts with optional type and status filters.

    Both filter parameters are optional — passing None matches all values.
    The OR :param IS NULL pattern is standard Oracle for optional bind filters.

    Bind variables:
        :alert_type — alert type string or None (matches all if None)
        :status     — status string or None (matches all if None)
        :limit      — max rows to return

    Args:
        mapper: SchemaMapper instance.

    Returns:
        Parameterized SQL string ready for python-oracledb execution.

    Example output:
        SELECT ALERT_ID, ALERT_TYPE, ...
        FROM   BANKING.ALERT_QUEUE
        WHERE  (ALERT_TYPE   = :alert_type OR :alert_type IS NULL)
          AND  (ALERT_STATUS = :status     OR :status     IS NULL)
        ORDER BY CREATED_TIMESTAMP DESC
        FETCH FIRST :limit ROWS ONLY
    """
    table = mapper.resolve_table("alert")
    type_col = mapper.resolve_column("alert", "type")
    status_col = mapper.resolve_column("alert", "status")
    created_at_col = mapper.resolve_column("alert", "created_at")
    columns = mapper.all_columns("alert")

    physical_cols = ", ".join(columns.values())

    return (
        f"SELECT {physical_cols} "
        f"FROM {table} "
        f"WHERE ({type_col} = :alert_type OR :alert_type IS NULL) "
        f"AND ({status_col} = :status OR :status IS NULL) "
        f"ORDER BY {created_at_col} DESC "
        f"FETCH FIRST :limit ROWS ONLY"
    )


def build_get_realtime_events_fallback_query(mapper: SchemaMapper) -> str:
    """Build a SELECT query for recent transactions as an Oracle fallback.

    Used by get_realtime_events when Kafka is unavailable or disabled.
    Returns the most recent transactions booked on or after :since_timestamp.

    Bind variables:
        :since_timestamp — datetime lower bound (booking_date >= this value)
        :limit           — max rows to return

    Args:
        mapper: SchemaMapper instance.

    Returns:
        Parameterized SQL string ready for python-oracledb execution.

    Example output:
        SELECT TXN_REFERENCE, ACCOUNT_NUMBER, TXN_AMOUNT, ...
        FROM   BANKING.TRANSACTION_LOG
        WHERE  BOOKING_DATE >= :since_timestamp
        ORDER BY BOOKING_DATE DESC
        FETCH FIRST :limit ROWS ONLY
    """
    table = mapper.resolve_table("transaction")
    booking_date_col = mapper.resolve_column("transaction", "booking_date")
    columns = mapper.all_columns("transaction")

    physical_cols = ", ".join(columns.values())

    return (
        f"SELECT {physical_cols} "
        f"FROM {table} "
        f"WHERE {booking_date_col} >= :since_timestamp "
        f"ORDER BY {booking_date_col} DESC "
        f"FETCH FIRST :limit ROWS ONLY"
    )
