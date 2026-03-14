"""
Schema mapper — resolves logical names to physical Oracle names.

Loads schema_map.yaml once at construction.  All tools and query builders
call this module to get table/column names; physical names never appear
in any other source file.

Public API:
    SchemaMapper.resolve_table(entity_type)              -> str
    SchemaMapper.resolve_column(entity_type, logical)    -> str
    SchemaMapper.all_columns(entity_type)                -> dict[str, str]
    SchemaMapper.entity_types()                          -> list[str]

    get_mapper()   — module-level cached singleton (lru_cache)

Raises SchemaConfigError for any unknown entity type or column name.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml


class SchemaConfigError(KeyError):
    """Raised when an entity type or column name is not found in schema_map.yaml."""


class SchemaMapper:
    """Maps logical entity/column names to physical Oracle table/column names.

    Loads schema_map.yaml once at construction time.  Thread-safe for reads
    (the internal dict is never mutated after __init__).
    """

    def __init__(self, yaml_path: str) -> None:
        """Load and validate the schema map from *yaml_path*.

        Args:
            yaml_path: Path to schema_map.yaml (absolute or relative to cwd).

        Raises:
            FileNotFoundError: If the YAML file does not exist.
            SchemaConfigError: If the YAML structure is missing required keys.
        """
        path = Path(yaml_path)
        if not path.exists():
            raise FileNotFoundError(f"Schema map not found: {yaml_path}")

        with path.open("r", encoding="utf-8") as fh:
            raw: dict = yaml.safe_load(fh)

        if "entities" not in raw:
            raise SchemaConfigError(
                f"schema_map.yaml at '{yaml_path}' is missing top-level 'entities' key"
            )

        # Validate each entity has 'table' and 'columns'
        entities: dict = raw["entities"]
        for entity_type, definition in entities.items():
            if "table" not in definition:
                raise SchemaConfigError(
                    f"Entity '{entity_type}' is missing required 'table' key"
                )
            if "columns" not in definition:
                raise SchemaConfigError(
                    f"Entity '{entity_type}' is missing required 'columns' key"
                )

        self._entities: dict[str, dict] = entities

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve_table(self, entity_type: str) -> str:
        """Return the physical Oracle table name for a logical entity type.

        Args:
            entity_type: Logical entity name, e.g. "customer", "account".

        Returns:
            Physical Oracle table name, e.g. "BANKING.CUSTOMER_MASTER".

        Raises:
            SchemaConfigError: If *entity_type* is not in the schema map.
        """
        self._require_entity(entity_type)
        return self._entities[entity_type]["table"]

    def resolve_column(self, entity_type: str, logical_name: str) -> str:
        """Return the physical column name for a logical field on an entity.

        Args:
            entity_type:  Logical entity name, e.g. "transaction".
            logical_name: Logical column name, e.g. "amount".

        Returns:
            Physical Oracle column name, e.g. "TXN_AMOUNT".

        Raises:
            SchemaConfigError: If *entity_type* or *logical_name* is unknown.
        """
        self._require_entity(entity_type)
        columns: dict[str, str] = self._entities[entity_type]["columns"]
        if logical_name not in columns:
            raise SchemaConfigError(
                f"Column '{logical_name}' not found on entity '{entity_type}'. "
                f"Available columns: {sorted(columns.keys())}"
            )
        return columns[logical_name]

    def all_columns(self, entity_type: str) -> dict[str, str]:
        """Return the full ``{logical_name: physical_name}`` mapping for an entity.

        Args:
            entity_type: Logical entity name.

        Returns:
            Dict mapping every logical column name to its physical Oracle name.
            The returned dict is a shallow copy — mutating it has no effect.

        Raises:
            SchemaConfigError: If *entity_type* is not in the schema map.
        """
        self._require_entity(entity_type)
        return dict(self._entities[entity_type]["columns"])

    def entity_types(self) -> list[str]:
        """Return a sorted list of all configured logical entity type names."""
        return sorted(self._entities.keys())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_entity(self, entity_type: str) -> None:
        """Raise SchemaConfigError if *entity_type* is not in the schema map."""
        if entity_type not in self._entities:
            raise SchemaConfigError(
                f"Unknown entity type '{entity_type}'. "
                f"Configured types: {sorted(self._entities.keys())}"
            )


@lru_cache(maxsize=1)
def get_mapper() -> SchemaMapper:
    """Return the cached SchemaMapper singleton.

    Reads SCHEMA_MAP_PATH from settings on first call.
    Call get_mapper.cache_clear() in tests to reset between test cases.
    """
    from src.config import get_settings  # local import to avoid circular deps

    return SchemaMapper(get_settings().schema_map_path)
