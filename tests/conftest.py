"""
Shared pytest configuration.

Sets the minimum environment variables required for Settings() to instantiate
in tests.  All external I/O (Oracle, Kafka, writeback) is handled by mocks —
these values are never used to make real connections.
"""

from __future__ import annotations

import os

# ORACLE_PASSWORD has no default (required in production).
# Provide a placeholder here so get_settings() succeeds in the test suite.
os.environ.setdefault("ORACLE_PASSWORD", "test-placeholder")
