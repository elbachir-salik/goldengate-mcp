"""
MockWritebackClient — test double for src/writeback/rest_client.py.

Mirrors the interface used by write_tools.py:
    client.post(path, payload) -> WritebackResponse
    client.close()             -> None

Supports:
    set_response(status_code, body)  — register a canned response
    set_error(exc)                   — make post() raise an exception
    call_count()                     — total number of post() calls
    last_call()                      — (path, payload) from the most recent call
    all_calls()                      — list of all (path, payload) tuples
    reset()                          — clear all state
"""

from __future__ import annotations

import asyncio
from typing import Any

from src.writeback.rest_client import WritebackResponse


class MockWritebackClient:
    """Drop-in replacement for ``WritebackClient``.

    Usage in tests::

        mock = MockWritebackClient()
        mock.set_response(200, {"result": "flagged"})

        with patch("src.tools.write_tools._get_writeback_client", return_value=mock):
            result = await flag_entity(entity_type="customer", entity_id="C001", ...)

        assert mock.call_count() == 1
        path, payload = mock.last_call()
        assert payload["entity_id"] == "C001"
    """

    def __init__(self) -> None:
        self._status_code: int = 200
        self._body: dict[str, Any] = {"result": "ok"}
        self._error: Exception | None = None
        self._calls: list[tuple[str, dict[str, Any]]] = []

    # ------------------------------------------------------------------
    # Test setup API
    # ------------------------------------------------------------------

    def set_response(self, status_code: int, body: dict[str, Any]) -> None:
        """Register the canned HTTP response the mock will return."""
        self._status_code = status_code
        self._body = body

    def set_error(self, exc: Exception) -> None:
        """Make the next post() raise *exc*."""
        self._error = exc

    def call_count(self) -> int:
        """Return total number of post() calls since last reset."""
        return len(self._calls)

    def last_call(self) -> tuple[str, dict[str, Any]]:
        """Return ``(path, payload)`` from the most recent post() call.

        Raises IndexError if no calls have been made.
        """
        return self._calls[-1]

    def all_calls(self) -> list[tuple[str, dict[str, Any]]]:
        """Return all ``(path, payload)`` tuples in call order."""
        return list(self._calls)

    def reset(self) -> None:
        """Clear all registered state and call history."""
        self._status_code = 200
        self._body = {"result": "ok"}
        self._error = None
        self._calls.clear()

    # ------------------------------------------------------------------
    # WritebackClient interface
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """No-op — mock is always ready."""
        await asyncio.sleep(0)

    async def post(self, path: str, payload: dict[str, Any]) -> WritebackResponse:
        """Record the call and return the registered response (or raise)."""
        self._calls.append((path, dict(payload)))

        if self._error is not None:
            exc = self._error
            self._error = None  # consume after first use
            raise exc

        return WritebackResponse(
            status_code=self._status_code,
            body=dict(self._body),
            latency_ms=0.5,
        )

    async def close(self) -> None:
        """No-op."""
        await asyncio.sleep(0)
