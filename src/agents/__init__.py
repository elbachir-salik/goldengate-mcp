"""
Reference agent implementations.

These agents demonstrate how to USE the MCP server tools.
They are example implementations, not core product.

    fraud_agent — real-time fraud detection loop via Kafka / get_realtime_events
    aml_agent   — scheduled AML alert triage (get_open_alerts → classify_alert)
    recon_agent — GL reconciliation loop (get_gl_position → score_event → post_adjustment)
"""
