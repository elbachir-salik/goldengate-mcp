"""
AuditLog — append-only audit writer.

Public API:
    AuditLog.record(tool_name, caller_id, input_hash, output_hash,
                    latency_ms, decision) -> None

File mode:  appends a JSON line to AUDIT_LOG_FILE_PATH.
Oracle mode: INSERTs a row into the AUDIT_LOG table (non-transactional).

Hashing helper:
    hash_payload(obj: Any) -> str   — SHA-256 of JSON-serialised object
"""
