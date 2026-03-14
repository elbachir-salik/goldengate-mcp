"""
Auth layer — RBAC (Role-Based Access Control) for MCP tool tiers.

Three tiers:
    read  — get_entity, get_transaction_history, get_realtime_events,
             get_gl_position, get_open_alerts
    score — score_event, classify_alert, generate_report_draft
    write — flag_entity, post_adjustment  (elevated; circuit-breaker protected)
"""
