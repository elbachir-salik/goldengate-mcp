"""
Reference Agent: GL Reconciliation.

Calls get_gl_position on both sides of a configured account pair.
Uses score_event to classify breaks as timing vs genuine discrepancies.
Calls post_adjustment for auto-resolvable breaks below the configured threshold.
Escalates large breaks to a human reviewer.

NOT part of the MCP server core — reference client implementation.
"""
