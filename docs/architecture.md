# GoldenGate MCP Server — Architecture

## High-level diagram

```
Core Banking (black box)
        │
        ▼  GoldenGate CDC
  ┌─────────────┐     ┌───────────┐
  │ Oracle DB   │     │  Kafka    │
  │  Replica    │     │  Topics   │
  └──────┬──────┘     └─────┬─────┘
         │                  │
         └──────────┬────────┘
                    ▼
           goldengate-mcp
           (MCP Server)
                    │
         ┌──────────┼──────────┐
         ▼          ▼          ▼
    Read Tools  Score Tools  Write Tools
                    │
                    ▼
         AI Agents / MCP Clients
```

The server is **read-only against the core banking system**. It connects only
to the downstream Oracle replica (never the source transactional DB) and to
optional Kafka topics. All write-back goes through a separately configured
REST endpoint (`WRITEBACK_BASE_URL`).

---

## Component map

| Component | File | Role |
|-----------|------|------|
| FastMCP app | `src/server.py` | Entry point; lifespan, singleton registry, tool mounting |
| Read tools | `src/tools/read_tools.py` | `get_entity`, `get_transaction_history`, `get_realtime_events`, `get_gl_position`, `get_open_alerts` |
| Score tools | `src/tools/score_tools.py` | `score_event`, `classify_alert`, `generate_report_draft` |
| Write tools | `src/tools/write_tools.py` | `flag_entity`, `post_adjustment` |
| Schema mapper | `src/schema/mapper.py` | Translates logical names ↔ physical Oracle names via `schema_map.yaml` |
| Oracle client | `src/db/oracle_client.py` | Async connection pool; parameterized queries only; tenacity retry |
| SQL builders | `src/db/queries.py` | All SQL strings; zero inline SQL in tool logic |
| Kafka consumer | `src/streaming/kafka_consumer.py` | Sync confluent-kafka consumer; called via `asyncio.to_thread()` |
| Writeback client | `src/writeback/rest_client.py` | httpx async client; tenacity retry on 5xx/connection errors |
| Circuit breaker | `src/writeback/circuit_breaker.py` | Sliding-window write rate limiter |
| RBAC | `src/auth/rbac.py` | `@require_role` decorator; three tiers: read / score / write |
| Audit log | `src/audit/audit_log.py` | SHA-256-hashed append-only records; file or Oracle backend |

---

## Request lifecycle

### Read tool call (e.g. `get_entity`)

```
MCP client
  │  tool call (stdio or HTTP)
  ▼
FastMCP dispatcher
  │  @require_role("read")  →  checks caller role from ctx.meta
  ▼
Tool function
  │  Pydantic validation  →  ValidationError on bad input
  │  SchemaMapper.resolve_table()  →  SchemaConfigError on unknown entity
  │  queries.build_*_query()  →  parameterized SQL string
  │  OracleClient.query()  →  tenacity retry (transient errors)
  │  _map_row_to_logical()  →  physical → logical column names
  │  AuditLog.record()  →  asyncio.to_thread (non-blocking file I/O)
  ▼
Dict / list[dict] returned to MCP client
```

### Score tool call (e.g. `score_event`)

```
  …same RBAC + validation as above…
  │
  │  asyncio.wait_for(anthropic.messages.create(), timeout=0.18s)
  │    └─ on TimeoutError  →  fallback decision="review", timed_out=True
  │    └─ on success       →  parse JSON from LLM response
  │  AuditLog.record()
  ▼
Score result dict
```

User-controlled fields are always passed in a **separate structured content
block** — never interpolated into the system prompt. This prevents prompt
injection regardless of what an adversarial agent sends.

### Write tool call (e.g. `flag_entity`)

```
  …RBAC (write tier) + validation…
  │
  │  CircuitBreaker.check_and_record()
  │    └─ raises CircuitBreakerOpenError if rate > CIRCUIT_BREAKER_WRITE_LIMIT/min
  │
  │  WritebackClient.post("/flags", payload)
  │    └─ tenacity retry on 5xx / connection error (3 attempts, exp backoff)
  │    └─ raises WritebackHTTPError on 4xx
  │
  │  AuditLog.record()  ← always, even if writeback fails
  ▼
Status + reference dict
```

---

## Failure modes and fallbacks

| Failure | Behaviour |
|---------|-----------|
| Kafka unreachable | `get_realtime_events` falls back to querying the Oracle replica |
| Oracle transient error | Retries up to `ORACLE_QUERY_RETRY_ATTEMPTS` times (default 2) with exponential back-off |
| Oracle hard failure | `OracleClientError` raised; tool returns error with suggested next step |
| Anthropic timeout (>180 ms) | `score_event` returns `decision="review"`, `timed_out=True` — never blocks |
| Anthropic API error | Score tools return fallback result; error logged |
| Write-back 5xx | `WritebackClient` retries 3 times; raises `WritebackHTTPError` after exhaustion |
| Write-back 4xx | `WritebackHTTPError` raised immediately (not retried) |
| Circuit breaker open | `CircuitBreakerOpenError` with retry-in seconds; HTTP call never made |
| Audit write failure | Logged and swallowed — never surfaces to the MCP client |
| `WRITEBACK_BASE_URL` not set | Write tools raise `RuntimeError` at call time with configuration guidance |
| `ANTHROPIC_API_KEY` not set | Score tools raise `RuntimeError` at call time with configuration guidance |

---

## Schema mapping

Physical Oracle table and column names live exclusively in
`src/schema/schema_map.yaml`. No table or column name appears in Python code.

```
schema_map.yaml
    │
    ▼
SchemaMapper (singleton, parsed at startup)
    │
    ├── resolve_table(entity_type)   →  "BANKING.CUSTOMER_MASTER"
    ├── resolve_column(entity_type, logical_name)  →  "CUST_ID"
    ├── resolve_column(entity_type, "id")          →  physical PK column name
    └── all_columns(entity_type)     →  {logical: physical, ...}
```

Adding a new queryable entity requires only a YAML edit — no Python changes.

---

## RBAC model

Three tiers, enforced via `@require_role(tier)` on every tool:

| Tier | Default roles | Tools |
|------|--------------|-------|
| `read` | `analyst`, `auditor`, `agent-read` | All read tools |
| `score` | `analyst`, `agent-score` | All score tools |
| `write` | `compliance-officer`, `agent-write` | All write tools |

The caller's role is read from `ctx.meta["role"]` injected by the MCP client.
With `RBAC_STRICT=false` (default) a missing role logs a warning and
continues; with `RBAC_STRICT=true` it raises `PermissionDeniedError`.

---

## Deployment topology

```
┌──────────────────────── Bank network ───────────────────────────┐
│                                                                   │
│  Core banking  ──GoldenGate──▶  Oracle replica  (read-only)     │
│                                                                   │
│  Kafka topics  (optional CDC stream)                             │
│                                                                   │
│  Write-back REST API  ◀──────── MCP server ──────────────────── │
│                                     ▲                            │
└─────────────────────────────────────│────────────────────────────┘
                                      │ stdio or HTTP
                              AI agents / MCP clients
```

**Key constraints:**
- MCP server has **no write access** to the Oracle replica.
- Write-back calls go to a separate API that owns its own auth and audit trail.
- The core banking system is never contacted directly.
- `ORACLE_PASSWORD` has no default — must be explicitly set in production.
