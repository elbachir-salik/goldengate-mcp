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
