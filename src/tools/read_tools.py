"""
Read tools — query the Oracle GoldenGate replica (read-only).

Tools registered here:
    get_entity              — generic entity fetch by type + ID
    get_transaction_history — paginated transaction history for an account
    get_realtime_events     — recent CDC events from Kafka (or Oracle fallback)
    get_gl_position         — GL balance fetch for reconciliation
    get_open_alerts         — query open alerts by type / status

All tools require the "read" RBAC tier.
All inputs are validated with Pydantic before any DB access.
All SQL is sourced from db/queries.py — no inline SQL here.
Every call produces an immutable audit log entry.
"""
