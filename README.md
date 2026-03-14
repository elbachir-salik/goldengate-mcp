# GoldenGate MCP Server

A production-grade MCP (Model Context Protocol) server that sits on top of Oracle GoldenGate
CDC pipelines and exposes replicated, real-time banking data as structured tools for AI agents.

## Architecture

```
Core Banking System (black box)
        │
        ▼ (GoldenGate CDC)
Oracle DB Replica  ──►  goldengate-mcp  ──►  AI Agents
Kafka Topics       ──►  (MCP Server)
```

## Setup

```bash
pip install -e ".[dev]"
cp .env.example .env
# Edit .env with your Oracle DSN, credentials, etc.
```

## Running

```bash
python -m src.server
```

## Testing

```bash
pytest tests/ -v
```

## Configuration

All configuration is via environment variables (see `.env.example`).
Table and column mappings are in `src/schema/schema_map.yaml`.
