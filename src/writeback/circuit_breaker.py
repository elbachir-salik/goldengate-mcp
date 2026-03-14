"""
CircuitBreaker — in-memory write-rate limiter for write tools.

Counts write operations in a rolling time window and trips (opens) when
the count exceeds CIRCUIT_BREAKER_WRITE_LIMIT per CIRCUIT_BREAKER_RESET_SECONDS.

Public API:
    CircuitBreaker.check_and_record() -> None
        Call once per write attempt.  Records the attempt if within limits.
        Raises CircuitBreakerOpenError if the rate limit is exceeded.

    CircuitBreaker.reset() -> None
        Clear all recorded calls (used in tests and on scheduled resets).

    CircuitBreaker.call_count() -> int
        Current number of recorded calls within the active window.

Errors:
    CircuitBreakerOpenError — raised when the write rate limit is exceeded.
"""

from __future__ import annotations

import time
from threading import Lock
from typing import Any


class CircuitBreakerOpenError(RuntimeError):
    """Raised when the write circuit breaker has tripped.

    Attributes:
        current_count: Number of writes recorded in the current window.
        limit:         Configured write limit per window.
        reset_in_seconds: Approximate seconds until the window resets.
    """

    def __init__(
        self,
        message: str,
        current_count: int,
        limit: int,
        reset_in_seconds: float,
    ) -> None:
        super().__init__(message)
        self.current_count = current_count
        self.limit = limit
        self.reset_in_seconds = reset_in_seconds


class CircuitBreaker:
    """Thread-safe in-memory write-rate limiter.

    Uses a sliding window: only calls within the last
    ``reset_seconds`` are counted.  When the count reaches ``write_limit``,
    ``check_and_record()`` raises ``CircuitBreakerOpenError`` until the window
    naturally resets.

    Usage::

        cb = CircuitBreaker(settings)
        cb.check_and_record()  # raises if tripped
        # ... perform the write ...
    """

    def __init__(self, settings: Any = None, *, write_limit: int = 100, reset_seconds: int = 60) -> None:
        """
        Args:
            settings:      Application Settings instance.  If supplied,
                           ``circuit_breaker_write_limit`` and
                           ``circuit_breaker_reset_seconds`` override the
                           keyword defaults.
            write_limit:   Max writes per window (used when settings=None).
            reset_seconds: Window duration in seconds (used when settings=None).
        """
        if settings is not None:
            self._limit = settings.circuit_breaker_write_limit
            self._reset_seconds = settings.circuit_breaker_reset_seconds
        else:
            self._limit = write_limit
            self._reset_seconds = reset_seconds

        self._timestamps: list[float] = []
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_and_record(self) -> None:
        """Check the rate limit and record a write attempt.

        Must be called once per write attempt **before** the write is
        performed.  If the call is allowed, it is recorded for future
        rate-limit checks.

        Raises:
            CircuitBreakerOpenError: If the write rate limit is exceeded.
                                     Includes the current count, limit, and
                                     an estimate of when the window resets.
        """
        now = time.monotonic()
        with self._lock:
            self._evict_expired(now)
            current = len(self._timestamps)

            if current >= self._limit:
                oldest = self._timestamps[0] if self._timestamps else now
                reset_in = max(0.0, (oldest + self._reset_seconds) - now)
                raise CircuitBreakerOpenError(
                    f"Write circuit breaker open: {current}/{self._limit} writes "
                    f"in the last {self._reset_seconds}s window. "
                    f"Retry in approximately {reset_in:.0f}s, or contact your "
                    f"administrator to review the write rate limit.",
                    current_count=current,
                    limit=self._limit,
                    reset_in_seconds=reset_in,
                )

            self._timestamps.append(now)

    def reset(self) -> None:
        """Clear all recorded timestamps.

        Used in tests and for manual administrative resets.
        """
        with self._lock:
            self._timestamps.clear()

    def call_count(self) -> int:
        """Return the number of write calls recorded in the current window."""
        now = time.monotonic()
        with self._lock:
            self._evict_expired(now)
            return len(self._timestamps)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _evict_expired(self, now: float) -> None:
        """Remove timestamps older than the reset window. Must be called under lock."""
        cutoff = now - self._reset_seconds
        # timestamps are appended in order, so we can binary-search or just iterate
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.pop(0)


