"""
Tests for src/tools/read_tools.py — get_entity and get_transaction_history.

Uses MockOracleClient (tests/mocks/mock_oracle.py) — no real Oracle connection needed.

Covers get_entity:
    - happy path: returns row dict with logical column names
    - entity not found: raises EntityNotFoundError
    - unknown entity_type: raises SchemaConfigError
    - invalid entity_id chars (injection attempt): raises ValidationError
    - entity_id too long: raises ValidationError

Covers get_transaction_history:
    - happy path: returns list of transaction dicts
    - date range > 365 days: raises ValidationError
    - to_date before from_date: raises ValidationError
    - limit > 500 clamped to 500
    - empty result set: returns [], no error
"""
