"""
Write tools — governed calls to the configured external write-back endpoint.

Tools registered here (Phase 3):
    flag_entity     — flag/block/unblock an entity via the bank's REST API
    post_adjustment — post a GL correcting entry or workflow approval

All tools require the "write" RBAC tier (elevated).
Each call is subject to:
    - Circuit breaker (auto-suspend if write rate exceeds threshold)
    - Full audit log entry (tool_name, caller, input_hash, output_hash, decision)
    - Pydantic input validation

The server does NOT know what system is behind the write-back endpoint —
it is a generic REST client configured via WRITEBACK_BASE_URL + WRITEBACK_API_KEY.
"""
