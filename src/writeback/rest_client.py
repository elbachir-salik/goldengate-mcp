"""
Generic REST client for write-back operations (Phase 3).

Uses httpx async client.  Handles:
    - Bearer token auth (WRITEBACK_API_KEY)
    - Configurable timeout (WRITEBACK_TIMEOUT_SECONDS)
    - Retry with exponential back-off (tenacity) for transient 5xx errors
    - Circuit breaker integration (checks write rate before every call)
    - Structured error logging via structlog

The client does not interpret the response body — it returns the raw
status code and response dict to the calling write tool.
"""
