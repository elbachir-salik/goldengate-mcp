"""
Shared helpers used by read_tools, score_tools, and write_tools.

Kept in one place so a fix to caller_id or map_row_to_logical propagates to
all tool modules without risk of drift.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastmcp import Context
    from src.schema.mapper import SchemaMapper


def caller_id(ctx: "Context | None") -> str:
    """Extract a caller identifier string from FastMCP context."""
    if ctx is None:
        return "unknown"
    meta = getattr(ctx, "meta", None)
    if not isinstance(meta, dict):
        return "unknown"
    return str(meta.get("caller_id") or meta.get("role") or "unknown")


def map_row_to_logical(
    row: dict[str, Any],
    mapper: "SchemaMapper",
    entity_type: str,
) -> dict[str, Any]:
    """Translate a row dict from physical column names to logical names.

    Args:
        row:         Dict keyed by physical Oracle column names (upper-cased).
        mapper:      SchemaMapper instance.
        entity_type: Logical entity type name.

    Returns:
        Dict keyed by logical column names.
    """
    physical_to_logical = {
        v.upper(): k for k, v in mapper.all_columns(entity_type).items()
    }
    return {
        physical_to_logical.get(k.upper(), k.lower()): v
        for k, v in row.items()
    }
