"""
MockAnthropicClient — test double for the Anthropic messages API.

Mirrors the interface used by score_tools.py:
    client.messages.create(model, max_tokens, system, messages)

Supports:
    set_response(response_dict)  — register a canned JSON response
    set_timeout()                — make next create() simulate timeout
    set_error(exc)               — make next create() raise an exception
    call_count()                 — number of times create() was called
    last_call_kwargs()           — kwargs from the most recent create() call
    reset()                      — clear all state
"""

from __future__ import annotations

import asyncio
import json
from typing import Any


class _TextBlock:
    """Fake Anthropic TextBlock."""

    def __init__(self, text: str) -> None:
        self.type = "text"
        self.text = text


class _Message:
    """Fake Anthropic Message with a single text content block."""

    def __init__(self, text: str) -> None:
        self.content = [_TextBlock(text)]
        self.model = "claude-sonnet-4-6"
        self.stop_reason = "end_turn"


class _FakeMessages:
    """Fake ``client.messages`` namespace."""

    def __init__(self, owner: MockAnthropicClient) -> None:
        self._owner = owner

    async def create(
        self,
        *,
        model: str,
        max_tokens: int,
        system: str,
        messages: list[dict],
        **kwargs: Any,
    ) -> _Message:
        return await self._owner._handle_create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=messages,
            **kwargs,
        )


class MockAnthropicClient:
    """Drop-in replacement for ``anthropic.AsyncAnthropic``.

    Usage in tests::

        mock = MockAnthropicClient()
        mock.set_response({"score": 80, "decision": "review", ...})

        with patch("src.tools.score_tools._get_anthropic_client", return_value=mock):
            result = await score_event(event={...})

        assert result["decision"] == "review"
        assert mock.call_count() == 1
    """

    def __init__(self) -> None:
        self._response: dict[str, Any] = {}
        self._should_timeout: bool = False
        self._error: Exception | None = None
        self._calls: int = 0
        self._last_kwargs: dict[str, Any] = {}
        self.messages = _FakeMessages(self)

    # ------------------------------------------------------------------
    # Test setup API
    # ------------------------------------------------------------------

    def set_response(self, response: dict[str, Any]) -> None:
        """Register the JSON dict the mock will return as the LLM response."""
        self._response = response

    def set_timeout(self) -> None:
        """Make the next create() raise asyncio.TimeoutError immediately.

        This simulates the 180 ms hard timeout firing without the test having
        to wait 180 ms.
        """
        self._should_timeout = True

    def set_error(self, exc: Exception) -> None:
        """Make the next create() raise *exc*."""
        self._error = exc

    def call_count(self) -> int:
        """Return total number of create() invocations since last reset."""
        return self._calls

    def last_call_kwargs(self) -> dict[str, Any]:
        """Return the kwargs from the most recent create() call."""
        return self._last_kwargs

    def reset(self) -> None:
        """Clear all registered state and counters."""
        self._response = {}
        self._should_timeout = False
        self._error = None
        self._calls = 0
        self._last_kwargs = {}

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _handle_create(self, **kwargs: Any) -> _Message:
        self._calls += 1
        self._last_kwargs = kwargs

        if self._error is not None:
            exc = self._error
            self._error = None  # consume after first use
            raise exc

        if self._should_timeout:
            self._should_timeout = False
            raise asyncio.TimeoutError()

        return _Message(json.dumps(self._response, default=str))
