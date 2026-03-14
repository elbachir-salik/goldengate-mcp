"""
RBAC enforcement — require_role() decorator factory.

Usage:
    @mcp.tool()
    @require_role("read")
    async def my_tool(ctx: Context, ...): ...

The decorator checks the caller's role from FastMCP's Context object against
the allowed roles for the given tier (sourced from settings.rbac_*_roles).

If RBAC_STRICT=true and no auth context is present the call is rejected.
If RBAC_STRICT=false (default) a missing context is allowed with a warning log.

Roles are comma-separated strings in config; comparison is case-insensitive.
"""
