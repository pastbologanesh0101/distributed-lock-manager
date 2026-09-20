"""Injectable clock abstraction.

Lease expiry needs to be deterministically testable. Rather than sleeping in
tests to observe real-time expiry, the LockManager depends on a small `Clock`
interface. Production code uses `SystemClock` (wall-clock time); tests use
`FakeClock`, whose time only advances when the test tells it to.
"""

from __future__ import annotations

import threading
import time


class Clock:
    """Abstract clock interface. `now()` returns seconds as a float."""

    def now(self) -> float:
        raise NotImplementedError


class SystemClock(Clock):
    """Real wall-clock time, backed by `time.monotonic()`."""

    def now(self) -> float:
        return time.monotonic()


class FakeClock(Clock):
    """A manually-advanced clock for deterministic tests.

    Thread-safe so that concurrency tests can advance it (or read it) from
    multiple threads without races.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._time = float(start)
        self._guard = threading.Lock()

    def now(self) -> float:
        with self._guard:
            return self._time

    def advance(self, seconds: float) -> float:
        """Move the clock forward by `seconds` and return the new time."""
        if seconds < 0:
            raise ValueError("cannot move a clock backwards")
        with self._guard:
            self._time += seconds
            return self._time

    def set(self, new_time: float) -> None:
        with self._guard:
            self._time = float(new_time)
