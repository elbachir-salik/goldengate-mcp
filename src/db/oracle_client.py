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

from __future__ import annotations

import time
from typing import Any

import oracledb
import structlog

from src.config import Settings

log = structlog.get_logger(__name__)


class OracleClientError(RuntimeError):
    """Raised when a query fails after all retries."""


class OracleClient:
    """Async Oracle DB client backed by a python-oracledb connection pool.

    Lifecycle::

        client = OracleClient(settings)
        await client.initialize()        # creates the pool
        rows = await client.query(...)
        await client.close()             # drains the pool

    In practice the server manages one singleton instance and calls
    initialize() on startup / close() on shutdown.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: oracledb.AsyncConnectionPool | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Create the async connection pool.

        Raises:
            OracleClientError: If the pool cannot be created (bad DSN, wrong
                credentials, Oracle not reachable).
        """
        try:
            self._pool = oracledb.create_pool_async(
                user=self._settings.oracle_user,
                password=self._settings.oracle_password.get_secret_value(),
                dsn=self._settings.oracle_dsn,
                min=self._settings.oracle_pool_min,
                max=self._settings.oracle_pool_max,
                increment=1,
            )
            log.info(
                "oracle_pool_created",
                dsn=self._settings.oracle_dsn,
                pool_min=self._settings.oracle_pool_min,
                pool_max=self._settings.oracle_pool_max,
            )
        except Exception as exc:
            raise OracleClientError(f"Failed to create Oracle connection pool: {exc}") from exc

    async def close(self) -> None:
        """Drain and close the connection pool gracefully."""
        if self._pool is not None:
            await self._pool.close(force=False)
            self._pool = None
            log.info("oracle_pool_closed")

    # ------------------------------------------------------------------
    # Query helper
    # ------------------------------------------------------------------

    async def query(
        self,
        query_key: str,
        sql: str,
        bind_params: dict[str, Any],
        max_rows: int = 1000,
    ) -> list[dict[str, Any]]:
        """Execute a parameterized SELECT and return rows as a list of dicts.

        This is the ONLY method callers should use to run SQL.  The ``sql``
        argument must be a string produced by a builder function in
        ``db/queries.py`` — never a raw string constructed by the caller.

        Args:
            query_key:    Identifier string for logging/audit (e.g. "get_entity").
                          Must match one of the constants in db/queries.py.
            sql:          Parameterized SQL string from db/queries.py.
            bind_params:  Dict of bind variable values (Oracle :name syntax).
            max_rows:     Hard cap on returned rows. Defaults to 1 000.

        Returns:
            List of row dicts keyed by physical column name (upper-cased).
            Empty list if no rows match.

        Raises:
            OracleClientError: If the pool is not initialised or the query fails.
        """
        if self._pool is None:
            raise OracleClientError(
                "OracleClient not initialised — call initialize() first."
            )

        start = time.perf_counter()
        connection: oracledb.AsyncConnection | None = None

        try:
            connection = await self._pool.acquire()
            async with connection.cursor() as cursor:
                await cursor.execute(sql, bind_params)
                # Fetch column names from cursor description
                col_names: list[str] = [col[0] for col in cursor.description]
                raw_rows = await cursor.fetchmany(max_rows)

            rows = [dict(zip(col_names, row)) for row in raw_rows]
            latency_ms = (time.perf_counter() - start) * 1000

            log.info(
                "oracle_query_ok",
                query_key=query_key,
                row_count=len(rows),
                latency_ms=round(latency_ms, 2),
                # Bind params logged with values truncated to avoid leaking PII
                bind_keys=list(bind_params.keys()),
            )
            return rows

        except OracleClientError:
            raise
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            log.error(
                "oracle_query_error",
                query_key=query_key,
                latency_ms=round(latency_ms, 2),
                error=str(exc),
            )
            raise OracleClientError(
                f"Query '{query_key}' failed: {exc}"
            ) from exc
        finally:
            if connection is not None:
                await self._pool.release(connection)
