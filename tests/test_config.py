"""
Tests for src/config.py — Settings validators.

Covers:
    pool_max_gte_pool_min model_validator:
        - oracle_pool_max < oracle_pool_min → ValidationError
        - oracle_pool_max == oracle_pool_min → accepted (valid edge case)
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError


def test_pool_max_lt_pool_min_raises() -> None:
    from src.config import Settings

    with pytest.raises(ValidationError, match="oracle_pool_max"):
        Settings(
            oracle_password="x",
            oracle_pool_min=10,
            oracle_pool_max=5,
        )


def test_pool_max_equals_pool_min_ok() -> None:
    from src.config import Settings

    settings = Settings(
        oracle_password="x",
        oracle_pool_min=5,
        oracle_pool_max=5,
    )
    assert settings.oracle_pool_min == 5
    assert settings.oracle_pool_max == 5
