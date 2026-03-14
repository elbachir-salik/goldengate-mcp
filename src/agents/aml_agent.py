"""
Reference Agent: AML Triage.

Polls get_open_alerts(alert_type="aml") on a configurable schedule.
Calls classify_alert for each open alert.
Auto-closes confirmed false positives (audit log reasoning preserved).
Calls generate_report_draft for escalated alerts (SAR draft).

Human MUST review all generated report drafts before submission.

NOT part of the MCP server core — reference client implementation.
"""
