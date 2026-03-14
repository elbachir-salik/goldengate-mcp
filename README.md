# GoldenGate MCP Server

A production-grade [Model Context Protocol](https://modelcontextprotocol.io) server that exposes real-time banking data — replicated via **Oracle GoldenGate CDC** — as structured tools for AI agents and LLM clients.

The core banking system is treated as a **black box**. The server connects only to the downstream Oracle replica and optionally to Kafka topics, never to the source transactional system.

---

## Tool Catalog

### Read tools (`read` tier)

| Tool | Description |
|------|-------------|
| `get_entity` | Fetch any entity (customer, account, transaction, alert) by type and ID |
| `get_transaction_history` | Paginated transaction history for an account (up to 365-day range) |
| `get_realtime_events` | Recent CDC change events from Kafka, with automatic Oracle fallback |
| `get_gl_position` | GL balance for a given account, currency, and value date |
| `get_open_alerts` | Query the alert queue by type and/or status |

### Score tools (`score` tier)

| Tool | Description |
|------|-------------|
| `score_event` | LLM risk score (0–100) for any event — 180 ms hard timeout, never blocks |
| `classify_alert` | Fetch an alert and classify it as genuine or false positive |
| `generate_report_draft` | Draft a SAR / CTR / compliance summary — always includes human-review gate |

### Write tools (`write` tier)

| Tool | Description |
|------|-------------|
| `flag_entity` | Flag, block, or unblock an entity via the configured write-back endpoint |
| `post_adjustment` | Post a GL correction, hold release, or workflow approval/rejection |

> **Note:** Write tools (`flag_entity`, `post_adjustment`) require `WRITEBACK_BASE_URL` to be configured. They are always registered but return a configuration error if called without it.

---

## Architecture

```
AI Agent / LLM Client
        │  MCP protocol (stdio or HTTP)
        ▼
┌─────────────────────────────────────────┐
│         GoldenGate MCP Server           │
│                                         │
│  read_tools.py  score_tools.py          │
│  write_tools.py                         │
│       │               │                 │
│  OracleClient    AnthropicClient        │
│  KafkaConsumer   WritebackClient        │
│  SchemaMapper    CircuitBreaker         │
│  AuditLog                               │
└─────────────────────────────────────────┘
        │                    │
        ▼                    ▼
 Oracle GoldenGate     Kafka Topics
 replica (read-only)   (CDC events)
```

**Key invariants:**
- Schema mapping via `schema_map.yaml` — zero hardcoded column/table names in code
- All SQL in `db/queries.py` — zero inline SQL in tool logic
- Pydantic validation on every input before any DB or HTTP call
- Every tool call produces an immutable SHA-256-hashed audit log entry
- Every error message includes a suggested next step for the agent
- Prompt injection protection: user-controlled fields are always in separate structured content blocks, never interpolated into system prompts

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/your-org/goldengate-mcp.git
cd goldengate-mcp
pip install -e ".[dev]"
```

> **Note:** `python-oracledb` may not be on your corporate PyPI mirror. Install it separately:
> ```bash
> pip install python-oracledb
> ```

### 2. Configure

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

Minimum required for read tools:

```env
ORACLE_DSN=your-host:1521/ORCL
ORACLE_USER=mcp_reader
ORACLE_PASSWORD=your-password
```

### 3. Run

```bash
# stdio transport (Claude Desktop compatible)
fastmcp run src/server.py

# HTTP transport (for testing or multi-client use)
fastmcp run src/server.py --transport streamable-http --port 8000
```

### 4. Docker

```bash
docker build -t goldengate-mcp .
docker run --env-file .env goldengate-mcp
```

---

## Configuration Reference

All settings are loaded from environment variables (or `.env` file).

| Variable | Default | Description |
|----------|---------|-------------|
| `ORACLE_DSN` | `localhost:1521/ORCL` | Oracle connection string |
| `ORACLE_USER` | `mcp_reader` | Oracle username |
| `ORACLE_PASSWORD` | *(required)* | Oracle password |
| `ORACLE_POOL_MIN` | `2` | Minimum pool connections |
| `ORACLE_POOL_MAX` | `10` | Maximum pool connections |
| `ANTHROPIC_API_KEY` | *(optional)* | Required for score tools |
| `KAFKA_BROKERS` | *(optional)* | Comma-separated brokers; leave empty to disable Kafka |
| `KAFKA_CONSUMER_GROUP` | `goldengate-mcp` | Kafka consumer group ID |
| `WRITEBACK_BASE_URL` | *(optional)* | REST endpoint for write tools; leave empty to disable |
| `WRITEBACK_API_KEY` | *(optional)* | Bearer token for write-back endpoint |
| `WRITEBACK_TIMEOUT_SECONDS` | `10.0` | HTTP timeout for write-back calls |
| `CIRCUIT_BREAKER_WRITE_LIMIT` | `100` | Max writes per minute before circuit trips |
| `CIRCUIT_BREAKER_RESET_SECONDS` | `60` | Circuit breaker window duration |
| `RBAC_READ_ROLES` | `analyst,auditor,agent-read` | Comma-separated roles for read tier |
| `RBAC_SCORE_ROLES` | `analyst,agent-score` | Comma-separated roles for score tier |
| `RBAC_WRITE_ROLES` | `compliance-officer,agent-write` | Comma-separated roles for write tier |
| `RBAC_STRICT` | `false` | Reject calls with no auth context |
| `AUDIT_LOG_MODE` | `file` | `file` or `oracle` |
| `AUDIT_LOG_FILE_PATH` | `audit.log` | Path for file-mode audit log |
| `SCHEMA_MAP_PATH` | `src/schema/schema_map.yaml` | Path to schema mapping YAML |

---

## Schema Mapping

Physical Oracle table and column names live **only** in `src/schema/schema_map.yaml`. The server exposes logical names to agents.

Example — adding a new entity requires only a YAML edit:

```yaml
entities:
  my_entity:
    table: BANKING.MY_TABLE
    id_column: MY_PK_COL
    columns:
      id: MY_PK_COL
      status: STATUS_CODE
      amount: TXN_AMOUNT
```

`get_entity("my_entity", "123")` works immediately — no code changes needed.

---

## RBAC

Tools enforce role-based access via the MCP context metadata. Set the caller's role when configuring your MCP client:

```json
{
  "mcpServers": {
    "goldengate": {
      "command": "fastmcp",
      "args": ["run", "/path/to/src/server.py"],
      "env": {
        "ORACLE_DSN": "host:1521/ORCL",
        "ORACLE_PASSWORD": "secret"
      }
    }
  }
}
```

| Tier | Roles (default) | Tools |
|------|----------------|-------|
| `read` | `analyst`, `auditor`, `agent-read` | All read tools |
| `score` | `analyst`, `agent-score` | All score tools |
| `write` | `compliance-officer`, `agent-write` | All write tools |

Roles are configured via `RBAC_*_ROLES` env vars — no code changes needed to add roles.

---

## Worked Example

**Fraud triage loop** — five tool calls to go from raw event to SAR draft:

```
1. get_realtime_events(topic="banking.transactions", lookback_minutes=5)
   → list of recent CDC events from Kafka (or Oracle fallback)

2. score_event(event=<event>, scoring_context={"account_type": "retail"})
   → { score: 87, decision: "review", reasoning: "...", confidence: 0.91 }

3. classify_alert(alert_id="A001", alert_type="fraud")
   → { is_false_positive: false, recommended_action: "escalate_to_compliance" }

4. flag_entity(entity_type="transaction", entity_id="T001",
               action="flag", reason="score 87/100, AML pattern match")
   → { status: "flagged", reference: "REF-2024-001" }

5. generate_report_draft(report_type="SAR", subject_id="C001",
                         evidence_ids=["A001", "T001"])
   → { draft_narrative: "...", HUMAN_REVIEW_REQUIRED: true }
```

---

## Development

```bash
# Run tests (no Oracle, Kafka, or Anthropic account needed)
pytest

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Lint
ruff check src tests
```

All 140 tests run against in-memory mocks — zero external dependencies required for development.

---

## Supported Entities

| Logical name | Oracle table | Used by |
|---|---|---|
| `customer` | `BANKING.CUSTOMER_MASTER` | `get_entity`, `flag_entity` |
| `account` | `BANKING.ACCOUNT_MASTER` | `get_entity`, `get_transaction_history` |
| `transaction` | `BANKING.TRANSACTION_LOG` | `get_transaction_history`, `get_realtime_events` |
| `gl_entry` | `BANKING.GL_BALANCE` | `get_gl_position` |
| `alert` | `BANKING.ALERT_QUEUE` | `get_open_alerts`, `classify_alert` |

---

## License

MIT
