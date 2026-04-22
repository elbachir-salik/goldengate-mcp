"""
Shared pytest configuration.

Sets the minimum environment variables required for Settings() to instantiate
in tests.  All external I/O (Oracle, Kafka, writeback) is handled by mocks —
these values are never used to make real connections.
"""

from __future__ import annotations

import os

# Optional: non-empty password for tests that read env before Settings() defaults apply.
os.environ.setdefault("ORACLE_PASSWORD", "test-placeholder")
