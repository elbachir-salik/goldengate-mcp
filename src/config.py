"""
Application configuration.

Loads all settings from environment variables / .env file using pydantic-settings.
Exposes a cached singleton via get_settings().

Usage:
    from src.config import get_settings
    settings = get_settings()
    print(settings.oracle_dsn)
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration for the GoldenGate MCP server.

    Values are loaded from environment variables (case-insensitive) and
    optionally from a .env file in the working directory.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ------------------------------------------------------------------
    # Oracle DB replica
    # ------------------------------------------------------------------
    oracle_dsn: str = "localhost:1521/ORCL"
    oracle_user: str = "mcp_reader"
    oracle_password: SecretStr = SecretStr("changeme")
    oracle_pool_min: int = 2
    oracle_pool_max: int = 10

    # ------------------------------------------------------------------
    # Anthropic (used by score/classify tools — Phase 2)
    # ------------------------------------------------------------------
    anthropic_api_key: SecretStr = SecretStr("")

    # ------------------------------------------------------------------
    # Kafka (optional — leave empty string to disable)
    # ------------------------------------------------------------------
    kafka_brokers: str = ""
    kafka_consumer_group: str = "goldengate-mcp"

    # ------------------------------------------------------------------
    # Write-back REST endpoint (optional — leave empty string to disable)
    # ------------------------------------------------------------------
    writeback_base_url: str = ""
    writeback_api_key: SecretStr = SecretStr("")
    writeback_timeout_seconds: float = 10.0

    # ------------------------------------------------------------------
    # Circuit breaker (for write tools)
    # ------------------------------------------------------------------
    circuit_breaker_write_limit: int = 100   # max writes per minute
    circuit_breaker_reset_seconds: int = 60

    # ------------------------------------------------------------------
    # RBAC — comma-separated role names per tier
    # ------------------------------------------------------------------
    rbac_read_roles: str = "analyst,auditor,agent-read"
    rbac_score_roles: str = "analyst,agent-score"
    rbac_write_roles: str = "compliance-officer,agent-write"
    # When True, reject calls with no auth context instead of warning
    rbac_strict: bool = False

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------
    audit_log_mode: Literal["oracle", "file"] = "file"
    audit_log_file_path: str = "audit.log"

    # ------------------------------------------------------------------
    # Schema map
    # ------------------------------------------------------------------
    schema_map_path: str = "src/schema/schema_map.yaml"

    # ------------------------------------------------------------------
    # Derived helpers (not env vars)
    # ------------------------------------------------------------------
    @property
    def kafka_enabled(self) -> bool:
        """True if Kafka brokers are configured."""
        return bool(self.kafka_brokers.strip())

    @property
    def writeback_enabled(self) -> bool:
        """True if a write-back endpoint is configured."""
        return bool(self.writeback_base_url.strip())

    @property
    def rbac_read_roles_set(self) -> set[str]:
        """Parsed set of allowed read-tier role names (lower-cased)."""
        return {r.strip().lower() for r in self.rbac_read_roles.split(",") if r.strip()}

    @property
    def rbac_score_roles_set(self) -> set[str]:
        """Parsed set of allowed score-tier role names (lower-cased)."""
        return {r.strip().lower() for r in self.rbac_score_roles.split(",") if r.strip()}

    @property
    def rbac_write_roles_set(self) -> set[str]:
        """Parsed set of allowed write-tier role names (lower-cased)."""
        return {r.strip().lower() for r in self.rbac_write_roles.split(",") if r.strip()}

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("oracle_pool_min")
    @classmethod
    def pool_min_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("oracle_pool_min must be >= 1")
        return v

    @field_validator("oracle_pool_max")
    @classmethod
    def pool_max_gte_min(cls, v: int) -> int:
        if v < 1:
            raise ValueError("oracle_pool_max must be >= 1")
        return v

    @field_validator("writeback_timeout_seconds")
    @classmethod
    def timeout_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("writeback_timeout_seconds must be > 0")
        return v

    @field_validator("circuit_breaker_write_limit")
    @classmethod
    def write_limit_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("circuit_breaker_write_limit must be >= 1")
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached Settings singleton.

    Call get_settings.cache_clear() in tests to reset between test cases.
    """
    return Settings()
