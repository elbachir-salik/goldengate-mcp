"""
SQL query builders — the ONLY place SQL strings are constructed.

All functions accept a SchemaMapper instance and return fully-formed,
parameterized SQL strings using Oracle :bind syntax.

Column and table names come exclusively from the SchemaMapper (schema_map.yaml).
No hardcoded column/table names appear anywhere in this file or in callers.
No f-string interpolation of user-supplied values — only schema-resolved names.

Query keys (used for audit logging and oracle_client.query() calls):
    GET_ENTITY               — fetch a single entity row by its ID column
    GET_TRANSACTION_HISTORY  — paginated transactions for an account + date range
"""

from __future__ import annotations

from src.schema.mapper import SchemaMapper

# ------------------------------------------------------------------
# Query key constants — used as the query_key argument to OracleClient.query()
# ------------------------------------------------------------------
GET_ENTITY = "get_entity"
GET_TRANSACTION_HISTORY = "get_transaction_history"


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
