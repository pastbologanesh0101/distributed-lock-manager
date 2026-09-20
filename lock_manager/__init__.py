"""Distributed Lock Manager.

A lease-based distributed mutual-exclusion lock service, implemented as an
in-process, thread-safe "lock server" simulation, together with a fencing
token mechanism that protects downstream resources from stale writes made
by clients whose leases have already expired.
"""

from .clock import Clock, SystemClock, FakeClock
from .lock_manager import LockManager, AcquireResult, RenewResult, LeaseInfo
from .protected_resource import FencedResource, StaleFencingTokenError

__all__ = [
    "Clock",
    "SystemClock",
    "FakeClock",
    "LockManager",
    "AcquireResult",
    "RenewResult",
    "LeaseInfo",
    "FencedResource",
    "StaleFencingTokenError",
]
