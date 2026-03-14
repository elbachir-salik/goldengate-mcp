"""
Generic REST client for write-back operations.

Uses httpx async client.  Handles:
    - Bearer token auth (WRITEBACK_API_KEY)
    - Configurable timeout (WRITEBACK_TIMEOUT_SECONDS)
    - Retry with exponential back-off (tenacity) for transient 5xx errors
    - Structured error logging via structlog

The client does not interpret the response body — it returns the raw
status code and response dict to the calling write tool.

Public API:
    WritebackClient.post(path, payload) -> WritebackResponse
    WritebackClient.close()             -> None

Errors:
    WritebackError          — base class for all writeback failures
    WritebackUnavailableError — raised when WRITEBACK_BASE_URL is not configured
    WritebackHTTPError      — raised on 4xx/5xx responses that exhaust retries
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import structlog

log = structlog.get_logger(__name__)


# ------------------------------------------------------------------
# Exceptions
# ------------------------------------------------------------------

class WritebackError(RuntimeError):
    """Base class for all writeback client errors."""


class WritebackUnavailableError(WritebackError):
    """Raised when the writeback endpoint is not configured.

    Configure WRITEBACK_BASE_URL in .env to enable write-back operations.
    """


class WritebackHTTPError(WritebackError):
    """Raised when the upstream REST endpoint returns a non-2xx status.

    Attributes:
        status_code: HTTP status code from the upstream response.
        body:        Response body dict (may be empty).
    """

    def __init__(self, message: str, status_code: int, body: dict[str, Any]) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


# ------------------------------------------------------------------
# Response dataclass
# ------------------------------------------------------------------

@dataclass
class WritebackResponse:
    """Normalised response from a write-back POST call."""

    status_code: int
    body: dict[str, Any]
    latency_ms: float


# ------------------------------------------------------------------
# Client
# ------------------------------------------------------------------

class WritebackClient:
    """Async REST client for write-back operations.

    Wraps httpx.AsyncClient with:
    - Bearer token auth from settings
    - Configurable timeout
    - Tenacity retry on transient 5xx (up to 3 attempts, exponential back-off)
    - Structured logging of every request (no PII in log values)

    Usage::

        client = WritebackClient(settings)
        await client.initialize()
        try:
            response = await client.post("/flags", {"entity_id": "C001", ...})
        finally:
            await client.close()

    In tests, use ``MockWritebackClient`` instead — it requires no network.
    """

    def __init__(self, settings: Any) -> None:
        self._settings = settings
        self._client: Any = None  # httpx.AsyncClient — lazy import

    async def initialize(self) -> None:
        """Create the underlying httpx.AsyncClient.

        Raises:
            WritebackUnavailableError: If WRITEBACK_BASE_URL is not set.
        """
        if not self._settings.writeback_enabled:
            raise WritebackUnavailableError(
                "Write-back endpoint is not configured. "
                "Set WRITEBACK_BASE_URL in your .env file to enable write tools. "
                "Contact your infrastructure team if you need write access."
            )

        import httpx  # lazy — not available in all environments

        self._client = httpx.AsyncClient(
            base_url=self._settings.writeback_base_url,
            headers={
                "Authorization": f"Bearer {self._settings.writeback_api_key.get_secret_value()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=self._settings.writeback_timeout_seconds,
        )
        log.info("writeback_client_initialized", base_url=self._settings.writeback_base_url)

    async def post(self, path: str, payload: dict[str, Any]) -> WritebackResponse:
        """POST *payload* to *path* on the configured write-back endpoint.

        Retries up to 3 times on transient 5xx errors with exponential back-off
        (1s, 2s, 4s).  Raises ``WritebackHTTPError`` if all retries are exhausted
        or on a non-retryable 4xx response.

        Args:
            path:    URL path relative to WRITEBACK_BASE_URL (e.g. ``"/flags"``).
            payload: Dict to send as JSON body.  Must be JSON-serialisable.

        Returns:
            WritebackResponse with status_code, body, and latency_ms.

        Raises:
            WritebackUnavailableError: If client was not initialised.
            WritebackHTTPError:        On non-2xx response after retries.
        """
        if self._client is None:
            raise WritebackUnavailableError(
                "WritebackClient.initialize() must be called before post(). "
                "The server lifecycle should call initialize() on startup."
            )

        from tenacity import (  # lazy — not available in all environments
            retry,
            retry_if_exception,
            stop_after_attempt,
            wait_exponential,
        )

        import httpx

        def _is_retryable(exc: BaseException) -> bool:
            return (
                isinstance(exc, WritebackHTTPError) and exc.status_code >= 500
            ) or isinstance(exc, (httpx.ConnectError, httpx.TimeoutException))

        @retry(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=4),
            reraise=True,
        )
        async def _do_post() -> WritebackResponse:
            start = time.perf_counter()
            response = await self._client.post(path, json=payload)
            latency_ms = (time.perf_counter() - start) * 1000

            try:
                body = response.json()
            except Exception:
                body = {"raw": response.text}

            log.info(
                "writeback_post",
                path=path,
                status_code=response.status_code,
                latency_ms=round(latency_ms, 1),
                payload_keys=sorted(payload.keys()),
            )

            if not response.is_success:
                raise WritebackHTTPError(
                    f"Write-back POST {path} returned {response.status_code}. "
                    f"Check the endpoint configuration or retry. "
                    f"If the error persists, contact your infrastructure team.",
                    status_code=response.status_code,
                    body=body,
                )

            return WritebackResponse(
                status_code=response.status_code,
                body=body,
                latency_ms=latency_ms,
            )

        return await _do_post()

    async def close(self) -> None:
        """Close the underlying httpx client and release connections."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            log.info("writeback_client_closed")
