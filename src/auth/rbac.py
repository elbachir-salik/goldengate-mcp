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

from __future__ import annotations

import functools
from typing import Any, Callable, Literal

import structlog

log = structlog.get_logger(__name__)

ToolTier = Literal["read", "score", "write"]


class PermissionDeniedError(PermissionError):
    """Raised when the caller does not have the required role for a tool tier."""


def _get_allowed_roles(tier: ToolTier) -> set[str]:
    """Return the set of allowed role names for *tier* from settings."""
    from src.config import get_settings  # local import to avoid circular deps

    settings = get_settings()
    if tier == "read":
        return settings.rbac_read_roles_set
    if tier == "score":
        return settings.rbac_score_roles_set
    if tier == "write":
        return settings.rbac_write_roles_set
    raise ValueError(f"Unknown tool tier: {tier!r}")  # pragma: no cover


def _extract_caller_role(ctx: Any) -> str | None:
    """Extract the caller role string from a FastMCP Context object.

    FastMCP passes a Context to every tool.  Caller identity is carried in
    ``ctx.meta`` as a dict that may contain a ``"role"`` key.  Returns None
    if the context or role is absent.
    """
    if ctx is None:
        return None
    meta = getattr(ctx, "meta", None)
    if not isinstance(meta, dict):
        return None
    role = meta.get("role")
    return str(role).lower() if role is not None else None


def require_role(tier: ToolTier) -> Callable:
    """Decorator factory that enforces RBAC for the given tool tier.

    Args:
        tier: One of "read", "score", or "write".

    Returns:
        A decorator that wraps an async tool function with role checking.

    Raises:
        PermissionDeniedError: If the caller's role is not allowed for *tier*
            (or if no role is present and RBAC_STRICT=true).

    Example::

        @mcp.tool()
        @require_role("read")
        async def get_entity(entity_type: str, entity_id: str, ctx: Context) -> dict:
            ...
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            from src.config import get_settings  # local import

            settings = get_settings()

            # FastMCP passes Context as the last positional arg or as kwarg "ctx"
            ctx = kwargs.get("ctx") or (args[-1] if args else None)
            caller_role = _extract_caller_role(ctx)

            if caller_role is None:
                if settings.rbac_strict:
                    raise PermissionDeniedError(
                        f"Tool requires '{tier}' role but no auth context was provided. "
                        "Set RBAC_STRICT=false to allow unauthenticated access in dev."
                    )
                log.warning(
                    "rbac_no_context",
                    tool=fn.__name__,
                    tier=tier,
                    strict=False,
                )
            else:
                allowed = _get_allowed_roles(tier)
                if caller_role not in allowed:
                    raise PermissionDeniedError(
                        f"Role '{caller_role}' is not authorised for the '{tier}' tier. "
                        f"Required one of: {sorted(allowed)}"
                    )
                log.debug(
                    "rbac_ok",
                    tool=fn.__name__,
                    tier=tier,
                    caller_role=caller_role,
                )

            return await fn(*args, **kwargs)

        return wrapper
    return decorator
