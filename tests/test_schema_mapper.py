"""
Tests for src/schema/mapper.py — SchemaMapper and get_mapper().

Covers:
    - resolve_table: happy path for all 5 entity types
    - resolve_column: happy path column resolution
    - resolve_table: SchemaConfigError on unknown entity type
    - resolve_column: SchemaConfigError on unknown column name
    - all_columns: returns complete {logical: physical} dict
    - entity_types: returns all 5 configured entities
    - constructor: raises FileNotFoundError on missing YAML file path
    - constructor: raises SchemaConfigError on malformed YAML
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from src.schema.mapper import SchemaConfigError, SchemaMapper

# Path to the real schema_map.yaml used by the server
SCHEMA_YAML = Path("src/schema/schema_map.yaml")


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture()
def mapper() -> SchemaMapper:
    """Return a SchemaMapper loaded from the real schema_map.yaml."""
    return SchemaMapper(str(SCHEMA_YAML))


@pytest.fixture()
def minimal_yaml(tmp_path: Path) -> Path:
    """Write a minimal valid schema_map.yaml to a temp file and return its path."""
    content = textwrap.dedent("""\
        version: "1.0"
        entities:
          widget:
            table: APP.WIDGETS
            columns:
              id: WIDGET_ID
              name: WIDGET_NAME
    """)
    p = tmp_path / "schema_map.yaml"
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture()
def missing_table_yaml(tmp_path: Path) -> Path:
    """YAML where an entity is missing the required 'table' key."""
    content = textwrap.dedent("""\
        version: "1.0"
        entities:
          broken:
            columns:
              id: SOME_ID
    """)
    p = tmp_path / "bad_schema.yaml"
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture()
def missing_columns_yaml(tmp_path: Path) -> Path:
    """YAML where an entity is missing the required 'columns' key."""
    content = textwrap.dedent("""\
        version: "1.0"
        entities:
          broken:
            table: APP.BROKEN
    """)
    p = tmp_path / "bad_schema.yaml"
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture()
def no_entities_yaml(tmp_path: Path) -> Path:
    """YAML that is missing the top-level 'entities' key entirely."""
    content = 'version: "1.0"\n'
    p = tmp_path / "no_entities.yaml"
    p.write_text(content, encoding="utf-8")
    return p


# ------------------------------------------------------------------
# resolve_table — happy path
# ------------------------------------------------------------------

@pytest.mark.parametrize("entity_type, expected_table", [
    ("customer",    "BANKING.CUSTOMER_MASTER"),
    ("account",     "BANKING.ACCOUNT_MASTER"),
    ("transaction", "BANKING.TRANSACTION_LOG"),
    ("gl_entry",    "BANKING.GL_BALANCE"),
    ("alert",       "BANKING.ALERT_QUEUE"),
])
def test_resolve_table_known_entities(
    mapper: SchemaMapper,
    entity_type: str,
    expected_table: str,
) -> None:
    assert mapper.resolve_table(entity_type) == expected_table


# ------------------------------------------------------------------
# resolve_column — happy path
# ------------------------------------------------------------------

@pytest.mark.parametrize("entity_type, logical, expected_physical", [
    ("customer",    "id",         "CUSTOMER_ID"),
    ("customer",    "name",       "FULL_NAME"),
    ("account",     "balance",    "CURRENT_BALANCE"),
    ("transaction", "amount",     "TXN_AMOUNT"),
    ("transaction", "value_date", "VALUE_DATE"),
    ("gl_entry",    "net_balance","NET_BALANCE"),
    ("alert",       "status",     "ALERT_STATUS"),
])
def test_resolve_column_known(
    mapper: SchemaMapper,
    entity_type: str,
    logical: str,
    expected_physical: str,
) -> None:
    assert mapper.resolve_column(entity_type, logical) == expected_physical


# ------------------------------------------------------------------
# resolve_table — error cases
# ------------------------------------------------------------------

def test_resolve_table_unknown_raises(mapper: SchemaMapper) -> None:
    with pytest.raises(SchemaConfigError, match="Unknown entity type 'mortgage'"):
        mapper.resolve_table("mortgage")


def test_resolve_table_empty_string_raises(mapper: SchemaMapper) -> None:
    with pytest.raises(SchemaConfigError):
        mapper.resolve_table("")


# ------------------------------------------------------------------
# resolve_column — error cases
# ------------------------------------------------------------------

def test_resolve_column_unknown_column_raises(mapper: SchemaMapper) -> None:
    with pytest.raises(SchemaConfigError, match="Column 'shoe_size' not found"):
        mapper.resolve_column("customer", "shoe_size")


def test_resolve_column_unknown_entity_raises(mapper: SchemaMapper) -> None:
    with pytest.raises(SchemaConfigError, match="Unknown entity type"):
        mapper.resolve_column("mortgage", "id")


# ------------------------------------------------------------------
# all_columns
# ------------------------------------------------------------------

def test_all_columns_returns_full_mapping(mapper: SchemaMapper) -> None:
    cols = mapper.all_columns("transaction")
    assert "id" in cols
    assert "amount" in cols
    assert "value_date" in cols
    assert cols["amount"] == "TXN_AMOUNT"


def test_all_columns_returns_copy(mapper: SchemaMapper) -> None:
    """Mutating the returned dict must not affect the mapper's internal state."""
    cols = mapper.all_columns("customer")
    cols["injected"] = "EVIL_COLUMN"
    assert "injected" not in mapper.all_columns("customer")


def test_all_columns_unknown_entity_raises(mapper: SchemaMapper) -> None:
    with pytest.raises(SchemaConfigError):
        mapper.all_columns("unknown_entity")


# ------------------------------------------------------------------
# entity_types
# ------------------------------------------------------------------

def test_entity_types_returns_all_five(mapper: SchemaMapper) -> None:
    types = mapper.entity_types()
    assert set(types) == {"customer", "account", "transaction", "gl_entry", "alert"}


def test_entity_types_is_sorted(mapper: SchemaMapper) -> None:
    types = mapper.entity_types()
    assert types == sorted(types)


# ------------------------------------------------------------------
# Constructor error cases
# ------------------------------------------------------------------

def test_missing_yaml_raises_file_not_found() -> None:
    with pytest.raises(FileNotFoundError, match="Schema map not found"):
        SchemaMapper("/nonexistent/path/schema_map.yaml")


def test_missing_entities_key_raises(no_entities_yaml: Path) -> None:
    with pytest.raises(SchemaConfigError, match="missing top-level 'entities' key"):
        SchemaMapper(str(no_entities_yaml))


def test_missing_table_key_raises(missing_table_yaml: Path) -> None:
    with pytest.raises(SchemaConfigError, match="missing required 'table' key"):
        SchemaMapper(str(missing_table_yaml))


def test_missing_columns_key_raises(missing_columns_yaml: Path) -> None:
    with pytest.raises(SchemaConfigError, match="missing required 'columns' key"):
        SchemaMapper(str(missing_columns_yaml))


# ------------------------------------------------------------------
# Minimal custom YAML
# ------------------------------------------------------------------

def test_custom_yaml_resolves_correctly(minimal_yaml: Path) -> None:
    m = SchemaMapper(str(minimal_yaml))
    assert m.resolve_table("widget") == "APP.WIDGETS"
    assert m.resolve_column("widget", "id") == "WIDGET_ID"
    assert m.entity_types() == ["widget"]
