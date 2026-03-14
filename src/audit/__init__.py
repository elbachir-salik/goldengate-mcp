"""
Audit log layer — immutable, append-only record of every tool call.

Each entry stores:
    tool_name, caller_id, timestamp_utc, input_hash (SHA-256),
    output_hash (SHA-256), latency_ms, decision

Raw inputs/outputs are never written — only their hashes.
Two backends: "file" (JSON lines) and "oracle" (INSERT into AUDIT_LOG table).
Audit writes are best-effort and never block or fail the tool response.
"""
