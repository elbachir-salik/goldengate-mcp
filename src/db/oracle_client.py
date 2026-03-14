"""
Oracle DB client — python-oracledb async connection pool.

Exposes a single safe query() helper that:
    - Accepts only pre-built SQL strings from queries.py (never raw caller SQL)
    - Executes with parameterized :bind variables (no f-string SQL)
    - Caps result sets at max_rows
    - Logs every execution (query_key, row_count, latency_ms) via structlog
    - Always returns the connection to the pool (finally block)

Callers must pass sql built by a function in db/queries.py, identified by
query_key (a plain string used for audit/logging).
"""
