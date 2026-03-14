"""
MockOracleClient — in-memory drop-in replacement for db/oracle_client.OracleClient.

Implements the same async interface.  Tests register fixture rows via
set_fixture() and optionally assert call history via assert_called_with().

No real Oracle connection is used.
"""

from __future__ import annotations

import asyncio
from typing import Any


class MockOracleClient:
    """In-memory mock that stores fixtures keyed by (query_key, frozenset of bind items).

    Usage in tests::

        mock = MockOracleClient()
        mock.set_fixture(
            query_key="get_entity",
            bind_params={"entity_id": "C001"},
            rows=[{"CUSTOMER_ID": "C001", "FULL_NAME": "Alice"}],
        )
        rows = await mock.query("get_entity", sql="...", bind_params={"entity_id": "C001"})
        mock.assert_called_with("get_entity", {"entity_id": "C001"})
    """

    def __init__(self) -> None:
        self._fixtures: dict[tuple, list[dict]] = {}
        self._calls: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Test setup helpers
    # ------------------------------------------------------------------

    def set_fixture(
        self,
        query_key: str,
        bind_params: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> None:
        """Register *rows* to return when *query_key* + *bind_params* are matched.

        Args:
            query_key:   Must match the query_key passed to query().
            bind_params: Must match the bind_params passed to query().
            rows:        List of row dicts to return (keyed by physical column name).
        """
        key = self._make_key(query_key, bind_params)
        self._fixtures[key] = list(rows)

    def assert_called_with(
        self,
        query_key: str,
        bind_params: dict[str, Any],
    ) -> None:
        """Assert that query() was called with the exact query_key and bind_params.

        Raises:
            AssertionError: If no matching call was recorded.
        """
        for call in self._calls:
            if (
                call["query_key"] == query_key
                and call["bind_params"] == bind_params
            ):
                return
        raise AssertionError(
            f"Expected call query_key={query_key!r} bind_params={bind_params!r} "
            f"not found in recorded calls: {self._calls}"
        )

    def call_count(self, query_key: str) -> int:
        """Return how many times query() was called with the given query_key."""
        return sum(1 for c in self._calls if c["query_key"] == query_key)

    def reset(self) -> None:
        """Clear all fixtures and recorded calls."""
        self._fixtures.clear()
        self._calls.clear()

    # ------------------------------------------------------------------
    # OracleClient interface
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """No-op — no pool to create."""
        await asyncio.sleep(0)

    async def close(self) -> None:
        """No-op — no pool to close."""
        await asyncio.sleep(0)

    async def query(
        self,
        query_key: str,
        sql: str,
        bind_params: dict[str, Any],
        max_rows: int = 1000,
    ) -> list[dict[str, Any]]:
        """Return pre-registered fixture rows for the given key + params.

        Records every call for later assertion via assert_called_with().

        Returns an empty list if no fixture was registered for the combination.
        """
        self._calls.append(
            {"query_key": query_key, "sql": sql, "bind_params": bind_params}
        )
        key = self._make_key(query_key, bind_params)
        rows = self._fixtures.get(key, [])
        return rows[:max_rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_key(query_key: str, bind_params: dict[str, Any]) -> tuple:
        """Build a hashable dict key from query_key + bind_params."""
        return (query_key, tuple(sorted(bind_params.items())))
