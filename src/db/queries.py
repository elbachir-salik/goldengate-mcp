"""
SQL query builders — the ONLY place SQL strings are constructed.

All functions accept a SchemaMapper instance and return fully-formed,
parameterized SQL strings using Oracle :bind syntax.

Column and table names come exclusively from the SchemaMapper (schema_map.yaml).
No hardcoded column/table names appear anywhere in this file or in callers.
No f-string interpolation of user-supplied values — only schema-resolved names.

Query keys (used for audit logging and oracle_client.query() calls):
    "get_entity"               — fetch a single entity row by its ID column
    "get_transaction_history"  — paginated transactions for an account + date range
"""
