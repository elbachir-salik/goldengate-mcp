"""
AuditLog — append-only audit writer.

Public API:
    AuditLog.record(tool_name, caller_id, input_hash, output_hash,
                    latency_ms, decision) -> None

File mode:  appends a JSON line to AUDIT_LOG_FILE_PATH.
Oracle mode: INSERTs a row into the AUDIT_LOG table (non-transactional).

Hashing helper:
    hash_payload(obj: Any) -> str   — SHA-256 of JSON-serialised object

Audit writes are best-effort — any exception is logged and swallowed so
that a failing audit write never blocks or fails the tool response.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)


def hash_payload(obj: Any) -> str:
    """Return the SHA-256 hex digest of the JSON-serialised *obj*.

    Non-serialisable values are coerced to their string representation so
    the function never raises.  The serialisation is sorted by key for
    determinism.

    Args:
        obj: Any JSON-serialisable (or str-coercible) Python object.

    Returns:
        64-character lowercase hex string.
    """
    try:
        serialised = json.dumps(obj, sort_keys=True, default=str)
    except Exception:
        serialised = str(obj)
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


class AuditLog:
    """Append-only audit log writer.

    Two storage backends are supported, selected via ``settings.audit_log_mode``:

    ``"file"``
        Appends one JSON line per record to ``settings.audit_log_file_path``.
        Simple, portable, works with no Oracle connection.

    ``"oracle"``
        INSERTs one row into the ``AUDIT_LOG`` table via the supplied
        ``OracleClient``.  The INSERT is non-transactional (auto-commit) and
        best-effort — failures are logged but never propagated.

    Usage::

        audit = AuditLog(settings, oracle_client=client)  # oracle_client optional
        await audit.record(
            tool_name="get_entity",
            caller_id="analyst@bank.com",
            input_hash=hash_payload(inputs),
            output_hash=hash_payload(output),
            latency_ms=42.3,
            decision=None,
        )
    """

    def __init__(self, settings: Any, oracle_client: Any = None) -> None:
        """
        Args:
            settings:      Application Settings instance.
            oracle_client: OracleClient instance (required when mode="oracle").
        """
        self._settings = settings
        self._oracle_client = oracle_client

    async def record(
        self,
        tool_name: str,
        caller_id: str,
        input_hash: str,
        output_hash: str,
        latency_ms: float,
        decision: str | None = None,
    ) -> None:
        """Append one immutable audit record.

        Never raises — any internal error is caught, logged, and swallowed so
        that audit failures never surface to the tool caller.

        Args:
            tool_name:   Name of the MCP tool that was called.
            caller_id:   Identity of the caller (role or user id from RBAC context).
            input_hash:  SHA-256 of the tool's input payload.
            output_hash: SHA-256 of the tool's output payload.
            latency_ms:  Wall-clock time for the tool call in milliseconds.
            decision:    Optional decision string (e.g. "review", "auto_close").
        """
        entry = {
            "tool_name": tool_name,
            "caller_id": caller_id,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "input_hash": input_hash,
            "output_hash": output_hash,
            "latency_ms": round(latency_ms, 3),
            "decision": decision,
        }

        try:
            if self._settings.audit_log_mode == "file":
                await self._write_file(entry)
            else:
                await self._write_oracle(entry)
        except Exception as exc:
            # Audit must never block or fail the tool response
            log.error(
                "audit_write_failed",
                tool_name=tool_name,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Backend implementations
    # ------------------------------------------------------------------

    async def _write_file(self, entry: dict) -> None:
        """Append *entry* as a JSON line to the configured audit log file.

        The write runs in a thread executor so slow filesystem I/O never
        blocks the event loop.
        """
        line = json.dumps(entry, default=str) + "\n"
        path = self._settings.audit_log_file_path
        await asyncio.to_thread(self._sync_write_file, path, line)

    @staticmethod
    def _sync_write_file(path: str, line: str) -> None:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line)

    async def _write_oracle(self, entry: dict) -> None:
        """INSERT *entry* into the AUDIT_LOG Oracle table.

        The table is expected to have columns matching the entry keys:
            TOOL_NAME, CALLER_ID, TIMESTAMP_UTC, INPUT_HASH,
            OUTPUT_HASH, LATENCY_MS, DECISION
        """
        if self._oracle_client is None:
            raise RuntimeError(
                "audit_log_mode='oracle' requires an OracleClient instance."
            )

        # SQL is defined inline here because AUDIT_LOG is an infrastructure
        # table, not a business entity — it is not in schema_map.yaml.
        sql = (
            "INSERT INTO AUDIT_LOG "
            "(TOOL_NAME, CALLER_ID, TIMESTAMP_UTC, INPUT_HASH, "
            " OUTPUT_HASH, LATENCY_MS, DECISION) "
            "VALUES "
            "(:tool_name, :caller_id, :timestamp_utc, :input_hash, "
            " :output_hash, :latency_ms, :decision)"
        )
        await self._oracle_client.query(
            query_key="audit_insert",
            sql=sql,
            bind_params=entry,
        )
