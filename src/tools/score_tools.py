"""
Scoring tools — LLM-powered reasoning over banking events and alerts.

Tools registered here (Phase 2):
    score_event           — general-purpose event scorer (180 ms hard timeout)
    classify_alert        — fetch alert + classify via LLM
    generate_report_draft — produce SAR / compliance report draft for human review

All tools require the "score" RBAC tier.
User-controlled fields are NEVER interpolated into prompts directly;
they are passed as structured data in a separate content block to prevent
prompt injection.
"""
