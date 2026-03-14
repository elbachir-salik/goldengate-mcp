"""
Tests for score tools in src/tools/score_tools.py.

Uses MockAnthropicClient and MockOracleClient — no real LLM or Oracle needed.

Covers:
    score_event:
        - Happy path: LLM returns valid score dict
        - Timeout: asyncio.TimeoutError → fallback decision="review", timed_out=True
        - LLM returns bad JSON: falls back to safe defaults (decision="review")
        - LLM returns out-of-range score: clamped / replaced
        - LLM returns unknown decision: normalised to "review"
        - Audit called once per invocation with tool_name="score_event"

    classify_alert:
        - Happy path: alert fetched from Oracle, LLM classifies it
        - Alert not found: _AlertNotFoundError with next-step message
        - LLM error: returns safe fallback (is_false_positive=False, low confidence)
        - Audit decision reflects LLM verdict (false_positive / genuine)

    generate_report_draft:
        - Happy path: returns draft with HUMAN_REVIEW_REQUIRED=True
        - Disclaimer always present
        - Invalid report_type: ValidationError with valid options listed
        - Invalid subject_id chars: ValidationError
        - Empty evidence_ids: ValidationError
        - Invalid evidence_id chars: ValidationError
        - Audit called with decision="draft_generated"
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from src.schema.mapper import SchemaMapper
from src.tools.score_tools import (
    _AlertNotFoundError,
    classify_alert,
    generate_report_draft,
    score_event,
)
from tests.mocks.mock_anthropic import MockAnthropicClient
from tests.mocks.mock_oracle import MockOracleClient

SCHEMA_YAML = "src/schema/schema_map.yaml"

SAMPLE_EVENT = {
    "TXN_REFERENCE": "T001",
    "ACCOUNT_NUMBER": "ACC100",
    "TXN_AMOUNT": 9900.0,
    "CURRENCY_CODE": "USD",
    "TXN_TYPE": "DEBIT",
    "CHANNEL_CODE": "ONLINE",
}

ALERT_ROW = {
    "ALERT_ID": "A001",
    "ALERT_TYPE": "fraud",
    "SUBJECT_ID": "C001",
    "SUBJECT_TYPE": "customer",
    "SEVERITY_LEVEL": "HIGH",
    "ALERT_STATUS": "open",
    "CREATED_TIMESTAMP": date(2024, 1, 10),
    "LAST_UPDATE_TS": date(2024, 1, 11),
    "ALERT_DESCRIPTION": "Suspicious transaction pattern",
}


@pytest.fixture()
def mapper() -> SchemaMapper:
    return SchemaMapper(SCHEMA_YAML)


@pytest.fixture()
def mock_oracle() -> MockOracleClient:
    return MockOracleClient()


@pytest.fixture()
def mock_anthropic() -> MockAnthropicClient:
    return MockAnthropicClient()


@pytest.fixture()
def mock_audit() -> AsyncMock:
    audit = MagicMock()
    audit.record = AsyncMock(return_value=None)
    return audit


def _patch_score_deps(mapper, oracle, audit, anthropic):
    return (
        patch("src.tools.score_tools._get_mapper", return_value=mapper),
        patch("src.tools.score_tools._get_oracle_client", return_value=oracle),
        patch("src.tools.score_tools._get_audit_log", return_value=audit),
        patch("src.tools.score_tools._get_anthropic_client", return_value=anthropic),
    )


# ==================================================================
# score_event — happy path
# ==================================================================

@pytest.mark.asyncio
async def test_score_event_happy_path(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({
        "score": 82,
        "decision": "review",
        "reasoning": "High-value ONLINE transaction outside normal hours.",
        "confidence": 0.88,
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await score_event(event=SAMPLE_EVENT)

    assert result["score"] == 82
    assert result["decision"] == "review"
    assert result["confidence"] == pytest.approx(0.88)
    assert result["timed_out"] is False
    assert mock_anthropic.call_count() == 1


@pytest.mark.asyncio
async def test_score_event_decision_approve(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({
        "score": 10,
        "decision": "approve",
        "reasoning": "Routine low-value transaction.",
        "confidence": 0.95,
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await score_event(event=SAMPLE_EVENT)

    assert result["decision"] == "approve"
    assert result["timed_out"] is False


@pytest.mark.asyncio
async def test_score_event_decision_block(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({
        "score": 98,
        "decision": "block",
        "reasoning": "Matches known fraud pattern.",
        "confidence": 0.99,
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await score_event(event=SAMPLE_EVENT)

    assert result["decision"] == "block"
    assert result["score"] == 98


# ==================================================================
# score_event — timeout
# ==================================================================

@pytest.mark.asyncio
async def test_score_event_timeout_returns_fallback(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_timeout()

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await score_event(event=SAMPLE_EVENT)

    assert result["timed_out"] is True
    assert result["decision"] == "review"
    assert result["confidence"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_score_event_timeout_audit_still_called(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_timeout()

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        await score_event(event=SAMPLE_EVENT)

    mock_audit.record.assert_called_once()


# ==================================================================
# score_event — LLM response normalisation
# ==================================================================

@pytest.mark.asyncio
async def test_score_event_bad_json_falls_back_to_review(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    """LLM returns empty dict → safe defaults applied."""
    mock_anthropic.set_response({})

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await score_event(event=SAMPLE_EVENT)

    assert result["decision"] == "review"
    assert result["score"] == 50
    assert result["timed_out"] is False


@pytest.mark.asyncio
async def test_score_event_unknown_decision_normalised(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({
        "score": 60,
        "decision": "escalate",   # not a valid decision
        "reasoning": "...",
        "confidence": 0.7,
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await score_event(event=SAMPLE_EVENT)

    assert result["decision"] == "review"


@pytest.mark.asyncio
async def test_score_event_out_of_range_score_replaced(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({
        "score": 999,
        "decision": "block",
        "reasoning": "...",
        "confidence": 0.9,
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await score_event(event=SAMPLE_EVENT)

    assert result["score"] == 50  # invalid → replaced with default


@pytest.mark.asyncio
async def test_score_event_confidence_clamped(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({
        "score": 50,
        "decision": "review",
        "reasoning": "...",
        "confidence": 5.0,   # > 1.0
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await score_event(event=SAMPLE_EVENT)

    assert result["confidence"] == pytest.approx(1.0)


# ==================================================================
# score_event — audit
# ==================================================================

@pytest.mark.asyncio
async def test_score_event_audit_called(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({"score": 30, "decision": "approve", "confidence": 0.9})

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        await score_event(event=SAMPLE_EVENT)

    mock_audit.record.assert_called_once()
    kwargs = mock_audit.record.call_args.kwargs
    assert kwargs["tool_name"] == "score_event"
    assert kwargs["decision"] == "approve"


# ==================================================================
# classify_alert — happy path
# ==================================================================

@pytest.mark.asyncio
async def test_classify_alert_happy_path(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_oracle.set_fixture(
        "get_entity",
        {"entity_id": "A001"},
        [ALERT_ROW],
    )
    mock_anthropic.set_response({
        "is_false_positive": False,
        "confidence": 0.91,
        "reasoning": "Matches known structuring pattern.",
        "recommended_action": "escalate_to_compliance",
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await classify_alert(alert_id="A001", alert_type="fraud")

    assert result["alert_id"] == "A001"
    assert result["is_false_positive"] is False
    assert result["confidence"] == pytest.approx(0.91)
    assert "recommended_action" in result


@pytest.mark.asyncio
async def test_classify_alert_false_positive(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_oracle.set_fixture("get_entity", {"entity_id": "A001"}, [ALERT_ROW])
    mock_anthropic.set_response({
        "is_false_positive": True,
        "confidence": 0.85,
        "reasoning": "Transaction within expected customer behaviour.",
        "recommended_action": "close_alert",
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await classify_alert(alert_id="A001")

    assert result["is_false_positive"] is True


# ==================================================================
# classify_alert — not found
# ==================================================================

@pytest.mark.asyncio
async def test_classify_alert_not_found(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    # No fixture registered → oracle returns []
    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        with pytest.raises(_AlertNotFoundError, match="No alert found with id"):
            await classify_alert(alert_id="MISSING")


@pytest.mark.asyncio
async def test_classify_alert_not_found_suggests_get_open_alerts(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        with pytest.raises(_AlertNotFoundError, match="get_open_alerts"):
            await classify_alert(alert_id="MISSING")


# ==================================================================
# classify_alert — LLM error fallback
# ==================================================================

@pytest.mark.asyncio
async def test_classify_alert_llm_error_returns_safe_fallback(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_oracle.set_fixture("get_entity", {"entity_id": "A001"}, [ALERT_ROW])
    mock_anthropic.set_error(RuntimeError("API unreachable"))

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await classify_alert(alert_id="A001")

    assert result["is_false_positive"] is False
    assert result["confidence"] == pytest.approx(0.0)
    assert "escalate" in result["recommended_action"]


# ==================================================================
# classify_alert — invalid alert_type
# ==================================================================

@pytest.mark.asyncio
async def test_classify_alert_invalid_type(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError, match="Invalid alert_type"):
            await classify_alert(alert_id="A001", alert_type="unknown_type")


# ==================================================================
# classify_alert — audit
# ==================================================================

@pytest.mark.asyncio
async def test_classify_alert_audit_called(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_oracle.set_fixture("get_entity", {"entity_id": "A001"}, [ALERT_ROW])
    mock_anthropic.set_response({
        "is_false_positive": True,
        "confidence": 0.8,
        "reasoning": "...",
        "recommended_action": "close_alert",
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        await classify_alert(alert_id="A001")

    mock_audit.record.assert_called_once()
    kwargs = mock_audit.record.call_args.kwargs
    assert kwargs["tool_name"] == "classify_alert"
    assert kwargs["decision"] == "false_positive"


# ==================================================================
# generate_report_draft — happy path
# ==================================================================

@pytest.mark.asyncio
async def test_generate_report_draft_happy_path(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({
        "draft_narrative": "On 2024-01-10, subject C001 conducted transactions...",
    })

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await generate_report_draft(
            report_type="SAR",
            subject_id="C001",
            evidence_ids=["A001", "T001"],
        )

    assert result["report_type"] == "SAR"
    assert result["subject_id"] == "C001"
    assert "draft_narrative" in result
    assert len(result["draft_narrative"]) > 0


@pytest.mark.asyncio
async def test_generate_report_draft_human_review_always_true(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({"draft_narrative": "Draft text."})

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await generate_report_draft(
            report_type="CTR",
            subject_id="C001",
            evidence_ids=["T001"],
        )

    assert result["HUMAN_REVIEW_REQUIRED"] is True
    assert "disclaimer" in result
    assert "NOT been submitted" in result["disclaimer"]


@pytest.mark.asyncio
async def test_generate_report_draft_all_report_types(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    for rtype in ("SAR", "CTR", "compliance_summary"):
        mock_anthropic.set_response({"draft_narrative": f"Draft for {rtype}."})

        p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
        with p1, p2, p3, p4:
            result = await generate_report_draft(
                report_type=rtype,
                subject_id="C001",
                evidence_ids=["E001"],
            )

        assert result["HUMAN_REVIEW_REQUIRED"] is True


# ==================================================================
# generate_report_draft — validation errors
# ==================================================================

@pytest.mark.asyncio
async def test_generate_report_draft_invalid_report_type(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError, match="Invalid report_type"):
            await generate_report_draft(
                report_type="FATF_REPORT",
                subject_id="C001",
                evidence_ids=["E001"],
            )


@pytest.mark.asyncio
async def test_generate_report_draft_invalid_report_type_lists_valid(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError, match="Valid values:"):
            await generate_report_draft(
                report_type="bad",
                subject_id="C001",
                evidence_ids=["E001"],
            )


@pytest.mark.asyncio
async def test_generate_report_draft_invalid_subject_id(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError):
            await generate_report_draft(
                report_type="SAR",
                subject_id="'; DROP TABLE --",
                evidence_ids=["E001"],
            )


@pytest.mark.asyncio
async def test_generate_report_draft_empty_evidence_ids(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError):
            await generate_report_draft(
                report_type="SAR",
                subject_id="C001",
                evidence_ids=[],
            )


@pytest.mark.asyncio
async def test_generate_report_draft_invalid_evidence_id_chars(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        with pytest.raises(ValidationError, match="Invalid evidence_id"):
            await generate_report_draft(
                report_type="SAR",
                subject_id="C001",
                evidence_ids=["valid-id", "bad id with spaces"],
            )


# ==================================================================
# generate_report_draft — LLM error fallback
# ==================================================================

@pytest.mark.asyncio
async def test_generate_report_draft_llm_error_returns_fallback(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_error(RuntimeError("LLM unavailable"))

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        result = await generate_report_draft(
            report_type="SAR",
            subject_id="C001",
            evidence_ids=["E001"],
        )

    # Even on LLM error, HUMAN_REVIEW_REQUIRED must be True
    assert result["HUMAN_REVIEW_REQUIRED"] is True
    assert "draft_narrative" in result
    assert len(result["draft_narrative"]) > 0


# ==================================================================
# generate_report_draft — audit
# ==================================================================

@pytest.mark.asyncio
async def test_generate_report_draft_audit_called(
    mapper, mock_oracle, mock_audit, mock_anthropic
) -> None:
    mock_anthropic.set_response({"draft_narrative": "Draft."})

    p1, p2, p3, p4 = _patch_score_deps(mapper, mock_oracle, mock_audit, mock_anthropic)
    with p1, p2, p3, p4:
        await generate_report_draft(
            report_type="SAR",
            subject_id="C001",
            evidence_ids=["E001"],
        )

    mock_audit.record.assert_called_once()
    kwargs = mock_audit.record.call_args.kwargs
    assert kwargs["tool_name"] == "generate_report_draft"
    assert kwargs["decision"] == "draft_generated"
