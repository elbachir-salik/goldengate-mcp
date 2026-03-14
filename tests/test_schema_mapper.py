"""
Tests for src/schema/mapper.py — SchemaMapper and get_mapper().

Covers:
    - resolve_table: happy path for all 5 entity types
    - resolve_column: happy path column resolution
    - resolve_table: SchemaConfigError on unknown entity type
    - resolve_column: SchemaConfigError on unknown column name
    - all_columns: returns complete {logical: physical} dict
    - entity_types: returns all 5 configured entities
    - constructor: raises on missing YAML file path
"""
