"""
GoldenGate MCP Server — entry point.

Initialises the FastMCP application, wires up all tool modules, and manages
the full dependency lifecycle:

    Startup:
        1. Load Settings (validates all env vars)
        2. Initialise SchemaMapper (parses schema_map.yaml)
        3. Initialise OracleClient if ORACLE_PASSWORD is set (otherwise skipped for local testing)
        4. Initialise KafkaConsumer  (if KAFKA_BROKERS is set)
        5. Initialise WritebackClient (if WRITEBACK_BASE_URL is set)
        6. Initialise CircuitBreaker  (always — in-memory, no I/O)
        7. Initialise AuditLog        (file or Oracle backend)
        8. Initialise Anthropic client (if ANTHROPIC_API_KEY is set)

    Shutdown (reverse order):
        WritebackClient.close()
        OracleClient.close()

Singleton accessors (imported lazily by tool modules):
    get_oracle_client()
    get_kafka_consumer()
    get_writeback_client()
    get_circuit_breaker()
    get_audit_log()
    get_anthropic_client()

Run:
    python -m src.server             (Streamable HTTP on 127.0.0.1:8000 — MCP Inspector)
    fastmcp run src/server.py        (stdio — Claude Desktop; requires fastmcp on PATH)
    fastmcp run src/server.py --transport streamable-http --port 8000
"""

from __future__ import annotations

import structlog
from contextlib import asynccontextmanager
from typing import Any

from fastmcp import FastMCP

from src.audit.audit_log import AuditLog
from src.config import get_settings
from src.db.oracle_client import OracleClient, OracleClientError
from src.schema.mapper import SchemaMapper, get_mapper
from src.writeback.circuit_breaker import CircuitBreaker

log = structlog.get_logger(__name__)

# ------------------------------------------------------------------
# Module-level singletons (populated during lifespan startup)
# ------------------------------------------------------------------

_oracle_client: OracleClient | None = None
_kafka_consumer: Any = None
_writeback_client: Any = None
_circuit_breaker: CircuitBreaker | None = None
_audit_log: AuditLog | None = None
_anthropic_client: Any = None


# ------------------------------------------------------------------
# Singleton accessors — imported by tool modules
# ------------------------------------------------------------------

def get_oracle_client() -> OracleClient:
    if _oracle_client is None:
        raise RuntimeError(
            "OracleClient is not available. "
            "Set ORACLE_PASSWORD in your .env file (non-empty) and ensure Oracle is reachable, "
            "or the pool failed to start — check server logs. "
            "Read and score tools that query the replica require a working Oracle connection."
        )
    return _oracle_client


def get_kafka_consumer() -> Any:
    if _kafka_consumer is None:
        raise RuntimeError(
            "KafkaConsumer is not initialised. "
            "Set KAFKA_BROKERS in your .env file to enable Kafka, "
            "or leave it empty to use the Oracle fallback."
        )
    return _kafka_consumer


def get_writeback_client() -> Any:
    if _writeback_client is None:
        raise RuntimeError(
            "WritebackClient is not initialised. "
            "Set WRITEBACK_BASE_URL and WRITEBACK_API_KEY in your .env file "
            "to enable write tools (flag_entity, post_adjustment)."
        )
    return _writeback_client


def get_circuit_breaker() -> CircuitBreaker:
    if _circuit_breaker is None:
        raise RuntimeError(
            "CircuitBreaker is not initialised. "
            "This should never happen — please report this as a bug."
        )
    return _circuit_breaker


def get_audit_log() -> AuditLog:
    if _audit_log is None:
        raise RuntimeError(
            "AuditLog is not initialised. "
            "This should never happen — please report this as a bug."
        )
    return _audit_log


def get_anthropic_client() -> Any:
    if _anthropic_client is None:
        raise RuntimeError(
            "Anthropic client is not initialised. "
            "Set ANTHROPIC_API_KEY in your .env file to enable score tools "
            "(score_event, classify_alert, generate_report_draft)."
        )
    return _anthropic_client


# ------------------------------------------------------------------
# Lifespan — startup and shutdown
# ------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(app: FastMCP):  # type: ignore[type-arg]
    """Manage the full dependency lifecycle for the MCP server."""
    global _oracle_client, _kafka_consumer, _writeback_client
    global _circuit_breaker, _audit_log, _anthropic_client

    settings = get_settings()
    log.info("goldengate_mcp_starting", version="0.1.0")

    # 1. Schema mapper (synchronous — just parses YAML)
    get_mapper()  # warms the singleton; raises SchemaConfigError on bad YAML
    log.info("schema_mapper_ready", path=settings.schema_map_path)

    # 2. Oracle client (optional — empty ORACLE_PASSWORD skips; failed init logs and continues)
    _oracle_client = None
    if settings.oracle_enabled:
        _oracle_client = OracleClient(settings)
        try:
            await _oracle_client.initialize()
            log.info("oracle_client_ready", dsn=settings.oracle_dsn)
        except OracleClientError as exc:
            log.warning(
                "oracle_client_init_failed",
                dsn=settings.oracle_dsn,
                error=str(exc),
                hint="Server runs without Oracle; set valid ORACLE_* and restart to enable DB tools.",
            )
            _oracle_client = None
    else:
        log.info(
            "oracle_client_skipped",
            reason="ORACLE_PASSWORD empty — Oracle disabled (local testing / MCP Inspector).",
        )

    if settings.audit_log_mode == "oracle" and _oracle_client is None:
        log.warning(
            "audit_oracle_without_client",
            hint="AUDIT_LOG_MODE=oracle requires a working Oracle pool. "
            "Use AUDIT_LOG_MODE=file for testing without Oracle, or fix ORACLE_* and restart.",
        )

    # 3. Kafka consumer (optional)
    if settings.kafka_enabled:
        from src.streaming.kafka_consumer import KafkaConsumer
        _kafka_consumer = KafkaConsumer(settings)
        log.info("kafka_consumer_ready", brokers=settings.kafka_brokers)
    else:
        # Provide a disabled stub so tools can call is_enabled() safely
        from src.streaming.kafka_consumer import KafkaConsumer
        _kafka_consumer = KafkaConsumer(settings)
        log.info("kafka_consumer_disabled")

    # 4. Writeback client (optional)
    if settings.writeback_enabled:
        from src.writeback.rest_client import WritebackClient
        _writeback_client = WritebackClient(settings)
        await _writeback_client.initialize()
        log.info("writeback_client_ready", base_url=settings.writeback_base_url)
    else:
        log.info("writeback_client_disabled")

    # 5. Circuit breaker (always)
    _circuit_breaker = CircuitBreaker(settings)
    log.info(
        "circuit_breaker_ready",
        write_limit=settings.circuit_breaker_write_limit,
        reset_seconds=settings.circuit_breaker_reset_seconds,
    )

    # 6. Audit log (always)
    _audit_log = AuditLog(settings, oracle_client=_oracle_client)
    log.info("audit_log_ready", mode=settings.audit_log_mode)

    # 7. Anthropic client (optional — needed for score tools)
    if settings.anthropic_api_key.get_secret_value():
        import anthropic  # lazy — not available in all environments
        _anthropic_client = anthropic.AsyncAnthropic(
            api_key=settings.anthropic_api_key.get_secret_value()
        )
        log.info("anthropic_client_ready")
    else:
        log.info("anthropic_client_disabled")

    log.info("goldengate_mcp_ready")

    yield  # server is running

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    log.info("goldengate_mcp_shutting_down")

    if _writeback_client is not None and settings.writeback_enabled:
        await _writeback_client.close()
        log.info("writeback_client_closed")

    if _oracle_client is not None:
        await _oracle_client.close()
        log.info("oracle_client_closed")

    log.info("goldengate_mcp_stopped")


# ------------------------------------------------------------------
# FastMCP application
# ------------------------------------------------------------------

mcp = FastMCP(
    "GoldenGate MCP Server",
    instructions=(
        "Real-time banking data access layer over Oracle GoldenGate CDC pipelines. "
        "All tools require an appropriate RBAC role set in the context metadata. "
        "Read tools: analyst, auditor, agent-read. "
        "Score tools: analyst, agent-score. "
        "Write tools: compliance-officer, agent-write."
    ),
    lifespan=_lifespan,
)

# Mount tool modules — mount() with no namespace merges tools flat into this server
from src.tools.read_tools import mcp as _read_mcp      # noqa: E402
from src.tools.score_tools import mcp as _score_mcp    # noqa: E402
from src.tools.write_tools import mcp as _write_mcp    # noqa: E402

mcp.mount(_read_mcp)
mcp.mount(_score_mcp)
mcp.mount(_write_mcp)


# ------------------------------------------------------------------
# Direct execution (python -m src.server or python src/server.py)
# ------------------------------------------------------------------
# Default when run as __main__: Streamable HTTP for MCP Inspector and HTTP clients.
# Claude Desktop / stdio: use `fastmcp run src/server.py` (default CLI transport is stdio).
_DEFAULT_MCP_HTTP_HOST = "127.0.0.1"
_DEFAULT_MCP_HTTP_PORT = 8000

if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host=_DEFAULT_MCP_HTTP_HOST,
        port=_DEFAULT_MCP_HTTP_PORT,
    )
