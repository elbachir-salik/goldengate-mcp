"""
Reference Agent: Fraud Detection.

Consumes Kafka topic (or polls get_realtime_events as fallback).
Calls score_event for each incoming transaction.
Calls flag_entity for events that exceed the risk threshold.

Full loop target latency: < 200 ms end-to-end.

NOT part of the MCP server core — this is a client-side reference implementation
showing how an agent loop would drive the MCP tools.
"""
