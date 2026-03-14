"""
Score tools — LLM-powered reasoning over banking events and alerts.

Tools registered here (Phase 3):
    score_event           — general-purpose event scorer (180 ms hard timeout)
    classify_alert        — fetch alert + classify via LLM
    generate_report_draft — produce SAR / compliance report draft for human review

All tools require the "score" RBAC tier.
User-controlled fields are NEVER interpolated into prompts directly;
they are passed as structured data in a separate content block to prevent
prompt injection.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any

from fastmcp import Context, FastMCP
from pydantic import BaseModel, Field, model_validator

from src.audit.audit_log import AuditLog, hash_payload
from src.auth.rbac import require_role
from src.db import queries
from src.schema.mapper import SchemaMapper

mcp = FastMCP("goldengate-score-tools")


# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

SCORE_TIMEOUT_SECONDS = 0.18  # 180 ms hard limit per spec

_TIMEOUT_FALLBACK: dict[str, Any] = {
    "score": 50,
    "decision": "review",
    "reasoning": (
        "Scoring timed out (180 ms limit exceeded). "
        "Human review required. "
        "Retry score_event() with a shorter event payload, "
        "or escalate directly to a compliance officer."
    ),
    "confidence": 0.0,
    "timed_out": True,
}

VALID_SCORE_DECISIONS = frozenset({"approve", "review", "block"})
VALID_REPORT_TYPES = frozenset({"SAR", "CTR", "compliance_summary"})
VALID_ALERT_TYPES_CLASSIFY = frozenset({"fraud", "aml", "recon_break", "custom"})

_HUMAN_REVIEW_DISCLAIMER = (
    "DRAFT ONLY. This report has NOT been submitted. "
    "A licensed compliance officer must review, verify, and approve "
    "before submission to any regulatory authority."
)

_SCORE_SYSTEM_PROMPT = (
    "You are a banking fraud and compliance risk scoring engine. "
    "Analyse the provided banking event data for risk indicators. "
    "Respond with a single JSON object and no other text:\n"
    '{"score": <integer 0-100>, "decision": "<approve|review|block>", '
    '"reasoning": "<brief explanation>", "confidence": <float 0.0-1.0>}'
)

_CLASSIFY_SYSTEM_PROMPT = (
    "You are a banking compliance classification engine. "
    "Analyse the provided alert and determine whether it is a false positive. "
    "Respond with a single JSON object and no other text:\n"
    '{"is_false_positive": <true|false>, "confidence": <float 0.0-1.0>, '
    '"reasoning": "<explanation>", "recommended_action": "<next step>"}'
)

_REPORT_SYSTEM_PROMPT = (
    "You are a banking compliance report writer. "
    "Draft a factual, professional narrative for the specified report type "
    "based on the subject and evidence provided. "
    "Do NOT include any personally identifying information beyond what is given. "
    "Respond with a single JSON object and no other text:\n"
    '{"draft_narrative": "<full report text>"}'
)


# ------------------------------------------------------------------
# Pydantic input models
# ------------------------------------------------------------------

class _ScoreEventInput(BaseModel):
    event: dict[str, Any]
    scoring_context: dict[str, Any] = Field(default_factory=dict)


class _ClassifyAlertInput(BaseModel):
    alert_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_\-]+$")
    alert_type: str | None = None

    @model_validator(mode="after")
    def validate_alert_type(self) -> _ClassifyAlertInput:
        if self.alert_type is not None and self.alert_type not in VALID_ALERT_TYPES_CLASSIFY:
            raise ValueError(
                f"Invalid alert_type '{self.alert_type}'. "
                f"Valid values: {sorted(VALID_ALERT_TYPES_CLASSIFY)}. "
                f"Pass None to skip type filtering."
            )
        return self


class _GenerateReportDraftInput(BaseModel):
    report_type: str
    subject_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_\-]+$")
    evidence_ids: list[str] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def validate_inputs(self) -> _GenerateReportDraftInput:
        if self.report_type not in VALID_REPORT_TYPES:
            raise ValueError(
                f"Invalid report_type '{self.report_type}'. "
                f"Valid values: {sorted(VALID_REPORT_TYPES)}. "
                f"Use 'SAR' for Suspicious Activity Report, 'CTR' for Currency "
                f"Transaction Report, or 'compliance_summary' for an internal summary."
            )
        bad_ids = [e for e in self.evidence_ids if not re.match(r"^[A-Za-z0-9_\-]+$", e)]
        if bad_ids:
            raise ValueError(
                f"Invalid evidence_id(s): {bad_ids}. "
                f"IDs must be alphanumeric with dashes and underscores only."
            )
        return self


# ------------------------------------------------------------------
# Dependency helpers (lazy, mockable in tests)
# ------------------------------------------------------------------

def _get_anthropic_client() -> Any:
    from src.server import get_anthropic_client  # type: ignore[import]
    return get_anthropic_client()


def _get_oracle_client() -> Any:
    from src.server import get_oracle_client  # type: ignore[import]
    return get_oracle_client()


def _get_audit_log() -> AuditLog:
    from src.server import get_audit_log  # type: ignore[import]
    return get_audit_log()


def _get_mapper() -> SchemaMapper:
    from src.schema.mapper import get_mapper
    return get_mapper()


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _caller_id(ctx: Context | None) -> str:
    if ctx is None:
        return "unknown"
    meta = getattr(ctx, "meta", None)
    if not isinstance(meta, dict):
        return "unknown"
    return str(meta.get("caller_id") or meta.get("role") or "unknown")


def _map_row_to_logical(
    row: dict[str, Any],
    mapper: SchemaMapper,
    entity_type: str,
) -> dict[str, Any]:
    physical_to_logical = {
        v.upper(): k for k, v in mapper.all_columns(entity_type).items()
    }
    return {
        physical_to_logical.get(k.upper(), k.lower()): v
        for k, v in row.items()
    }


def _parse_llm_json(text: str) -> dict[str, Any]:
    """Extract a JSON dict from LLM response text.

    Strips markdown code fences if present. Returns {} on any parse error.
    """
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.split("\n")
        stripped = "\n".join(lines[1:-1]).strip()
    try:
        result = json.loads(stripped)
        return result if isinstance(result, dict) else {}
    except (json.JSONDecodeError, ValueError):
        return {}


# ------------------------------------------------------------------
# Tool — score_event
# ------------------------------------------------------------------

@mcp.tool()
@require_role("score")
async def score_event(
    event: dict[str, Any],
    scoring_context: dict[str, Any] | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Score a banking event for fraud or compliance risk using LLM analysis.

    Sends the event to ``claude-sonnet-4-6`` for risk analysis.
    Hard timeout: **180 ms** — returns a ``"review"`` fallback if exceeded
    so the pipeline is never blocked waiting on the LLM.

    User-controlled fields are passed in a separate structured content block
    to prevent prompt injection.  They are never interpolated into the system
    prompt.

    Args:
        event:           CDC event dict or transaction row from
                         ``get_realtime_events()``.  Passed to the LLM as
                         structured data, not as prompt text.
        scoring_context: Optional dict with additional context (e.g. account
                         history summary, known risk flags).  Defaults to {}.
        ctx:             FastMCP context (injected by the framework).

    Returns:
        Dict with keys:

        - ``score``      (int 0–100):     Risk score. 0 = low risk, 100 = high.
        - ``decision``   (str):           ``"approve"`` | ``"review"`` | ``"block"``
        - ``reasoning``  (str):           LLM explanation of the score.
        - ``confidence`` (float 0.0–1.0): Model confidence in the decision.
        - ``timed_out``  (bool):          ``True`` if 180 ms limit was exceeded.

    Raises:
        ValidationError: If *event* is not a dict.
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    validated = _ScoreEventInput(
        event=event,
        scoring_context=scoring_context or {},
    )

    client = _get_anthropic_client()
    result = await _call_score_llm(client, validated.event, validated.scoring_context)

    latency_ms = (time.perf_counter() - start) * 1000
    await _get_audit_log().record(
        tool_name="score_event",
        caller_id=caller_id,
        input_hash=hash_payload({
            "event_keys": sorted(validated.event.keys()),
            "has_context": bool(validated.scoring_context),
        }),
        output_hash=hash_payload(result),
        latency_ms=latency_ms,
        decision=result.get("decision"),
    )
    return result


async def _call_score_llm(
    client: Any,
    event: dict[str, Any],
    scoring_context: dict[str, Any],
) -> dict[str, Any]:
    """Call the LLM to score an event. Returns timeout fallback on failure."""
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "Score the following banking event for fraud and compliance risk. "
                        "Return only the JSON object specified in your instructions."
                    ),
                },
                # User-controlled data in a separate block — never in the system prompt
                {
                    "type": "text",
                    "text": json.dumps(
                        {"event": event, "scoring_context": scoring_context},
                        default=str,
                    ),
                },
            ],
        }
    ]

    try:
        msg = await asyncio.wait_for(
            client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=256,
                system=_SCORE_SYSTEM_PROMPT,
                messages=messages,
            ),
            timeout=SCORE_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        return _TIMEOUT_FALLBACK.copy()

    raw = msg.content[0].text if msg.content else ""
    parsed = _parse_llm_json(raw)

    score = parsed.get("score", 50)
    decision = parsed.get("decision", "review")
    reasoning = parsed.get("reasoning", "No reasoning provided.")
    confidence = parsed.get("confidence", 0.5)

    if not isinstance(score, (int, float)) or not (0 <= score <= 100):
        score = 50
    if decision not in VALID_SCORE_DECISIONS:
        decision = "review"
    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))

    return {
        "score": int(score),
        "decision": decision,
        "reasoning": reasoning,
        "confidence": confidence,
        "timed_out": False,
    }


# ------------------------------------------------------------------
# Tool — classify_alert
# ------------------------------------------------------------------

class _AlertNotFoundError(LookupError):
    """Raised when classify_alert cannot find the alert entity."""


@mcp.tool()
@require_role("score")
async def classify_alert(
    alert_id: str,
    alert_type: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Fetch an alert from the Oracle replica and classify it via LLM.

    Determines whether the alert is a false positive and recommends a next
    action.  User-controlled fields (the alert payload) are passed in a
    separate structured content block — never interpolated into the system
    prompt.

    Args:
        alert_id:   Alert primary key.  Alphanumeric, dash, underscore only
                    (max 100 chars).
        alert_type: Optional type hint for context (``"fraud"``, ``"aml"``,
                    ``"recon_break"``, ``"custom"``).  Pass ``None`` to skip.
        ctx:        FastMCP context (injected by the framework).

    Returns:
        Dict with keys:

        - ``alert_id``           (str):   The classified alert ID.
        - ``is_false_positive``  (bool):  LLM verdict.
        - ``confidence``         (float): Model confidence 0.0–1.0.
        - ``reasoning``          (str):   LLM explanation.
        - ``recommended_action`` (str):   Suggested next step.

    Raises:
        ValidationError:    If alert_id or alert_type fails validation.
        _AlertNotFoundError: If no alert with the given ID exists in Oracle.
                             Try ``get_open_alerts()`` to browse available alerts.
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    validated = _ClassifyAlertInput(alert_id=alert_id, alert_type=alert_type)

    # Fetch the alert entity from Oracle
    mapper = _get_mapper()
    sql = queries.build_get_entity_query(mapper, "alert")
    client_db = _get_oracle_client()
    rows = await client_db.query(
        query_key=queries.GET_ENTITY,
        sql=sql,
        bind_params={"entity_id": validated.alert_id},
        max_rows=1,
    )

    if not rows:
        raise _AlertNotFoundError(
            f"No alert found with id '{validated.alert_id}'. "
            f"Use get_open_alerts() to browse available alerts, "
            f"or verify the alert_id is correct."
        )

    alert_row = _map_row_to_logical(rows[0], mapper, "alert")

    # Call LLM — user data in structured block, not in system prompt
    llm_client = _get_anthropic_client()
    result = await _call_classify_llm(llm_client, validated.alert_id, alert_row, alert_type)

    latency_ms = (time.perf_counter() - start) * 1000
    await _get_audit_log().record(
        tool_name="classify_alert",
        caller_id=caller_id,
        input_hash=hash_payload({
            "alert_id": validated.alert_id,
            "alert_type": validated.alert_type,
        }),
        output_hash=hash_payload(result),
        latency_ms=latency_ms,
        decision="false_positive" if result.get("is_false_positive") else "genuine",
    )
    return result


async def _call_classify_llm(
    client: Any,
    alert_id: str,
    alert_row: dict[str, Any],
    alert_type: str | None,
) -> dict[str, Any]:
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "Classify the following banking alert. "
                        "Determine if it is a false positive. "
                        "Return only the JSON object specified in your instructions."
                    ),
                },
                # User-controlled alert data in a separate block
                {
                    "type": "text",
                    "text": json.dumps(
                        {"alert_id": alert_id, "alert_type": alert_type, "alert": alert_row},
                        default=str,
                    ),
                },
            ],
        }
    ]

    try:
        msg = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            system=_CLASSIFY_SYSTEM_PROMPT,
            messages=messages,
        )
    except Exception:
        return {
            "alert_id": alert_id,
            "is_false_positive": False,
            "confidence": 0.0,
            "reasoning": (
                "LLM classification failed. "
                "Manual review required. "
                "Retry classify_alert() or escalate to a compliance officer."
            ),
            "recommended_action": "escalate_to_compliance",
        }

    raw = msg.content[0].text if msg.content else ""
    parsed = _parse_llm_json(raw)

    is_fp = bool(parsed.get("is_false_positive", False))
    confidence = parsed.get("confidence", 0.5)
    reasoning = parsed.get("reasoning", "No reasoning provided.")
    action = parsed.get("recommended_action", "manual_review")

    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))

    return {
        "alert_id": alert_id,
        "is_false_positive": is_fp,
        "confidence": confidence,
        "reasoning": reasoning,
        "recommended_action": action,
    }


# ------------------------------------------------------------------
# Tool — generate_report_draft
# ------------------------------------------------------------------

@mcp.tool()
@require_role("score")
async def generate_report_draft(
    report_type: str,
    subject_id: str,
    evidence_ids: list[str],
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Generate a draft SAR / compliance report narrative for human review.

    Calls ``claude-sonnet-4-6`` to produce a professional report narrative.
    **This tool NEVER submits the report.**  The output always carries a
    mandatory human-review gate — a licensed compliance officer must review
    and approve before any submission to a regulatory authority.

    User-controlled fields (subject_id, evidence_ids) are passed in a
    separate structured content block to prevent prompt injection.

    Args:
        report_type:  One of ``"SAR"`` (Suspicious Activity Report),
                      ``"CTR"`` (Currency Transaction Report), or
                      ``"compliance_summary"`` (internal summary).
        subject_id:   Entity ID of the report subject (customer or account).
                      Alphanumeric, dash, underscore only (max 100 chars).
        evidence_ids: List of alert or transaction IDs that support the
                      report.  1–20 IDs, alphanumeric, dash, underscore.
        ctx:          FastMCP context (injected by the framework).

    Returns:
        Dict with keys:

        - ``report_type``           (str):  As supplied.
        - ``subject_id``            (str):  As supplied.
        - ``evidence_ids``          (list): As supplied.
        - ``draft_narrative``       (str):  LLM-generated report text.
        - ``HUMAN_REVIEW_REQUIRED`` (bool): Always ``True``.
        - ``disclaimer``            (str):  Mandatory submission warning.

    Raises:
        ValidationError: If report_type is not recognised, subject_id contains
                         invalid characters, or evidence_ids is empty / too long.
    """
    start = time.perf_counter()
    caller_id = _caller_id(ctx)

    validated = _GenerateReportDraftInput(
        report_type=report_type,
        subject_id=subject_id,
        evidence_ids=evidence_ids,
    )

    client = _get_anthropic_client()
    draft_narrative = await _call_report_llm(
        client,
        validated.report_type,
        validated.subject_id,
        validated.evidence_ids,
    )

    result: dict[str, Any] = {
        "report_type": validated.report_type,
        "subject_id": validated.subject_id,
        "evidence_ids": validated.evidence_ids,
        "draft_narrative": draft_narrative,
        "HUMAN_REVIEW_REQUIRED": True,
        "disclaimer": _HUMAN_REVIEW_DISCLAIMER,
    }

    latency_ms = (time.perf_counter() - start) * 1000
    await _get_audit_log().record(
        tool_name="generate_report_draft",
        caller_id=caller_id,
        input_hash=hash_payload({
            "report_type": validated.report_type,
            "subject_id": validated.subject_id,
            "evidence_count": len(validated.evidence_ids),
        }),
        output_hash=hash_payload(result),
        latency_ms=latency_ms,
        decision="draft_generated",
    )
    return result


async def _call_report_llm(
    client: Any,
    report_type: str,
    subject_id: str,
    evidence_ids: list[str],
) -> str:
    """Call the LLM to draft a report narrative. Returns an error string on failure."""
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        f"Draft a {report_type} report narrative. "
                        "Return only the JSON object specified in your instructions."
                    ),
                },
                # User-controlled identifiers in a separate block — not in system prompt
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "report_type": report_type,
                            "subject_id": subject_id,
                            "evidence_ids": evidence_ids,
                        },
                        default=str,
                    ),
                },
            ],
        }
    ]

    try:
        msg = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=_REPORT_SYSTEM_PROMPT,
            messages=messages,
        )
    except Exception:
        return (
            "Report generation failed due to an LLM error. "
            "Retry generate_report_draft() or draft the narrative manually. "
            "Do NOT submit until a compliance officer has reviewed the content."
        )

    raw = msg.content[0].text if msg.content else ""
    parsed = _parse_llm_json(raw)
    return parsed.get(
        "draft_narrative",
        "Draft narrative unavailable. Please draft manually and have a compliance officer review.",
    )
