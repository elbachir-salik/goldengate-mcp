# GoldenGate MCP Server — Architecture

Placeholder. Full architecture documentation to be written in Phase 1 completion.

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
