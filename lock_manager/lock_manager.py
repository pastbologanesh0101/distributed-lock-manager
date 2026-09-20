"""Core lease-based distributed lock manager.

This module simulates a single, authoritative lock-server (the same role
that something like a Zookeeper/etcd/Chubby lock service plays in a real
distributed system, or a lock built on top of a Raft-replicated log). It is
intentionally *not* built on top of a consensus protocol here -- the focus
of this project is the lock/lease/fencing-token semantics themselves, which
is the primitive those systems provide to application code.

Safety properties implemented:

* At most one client holds a valid (non-expired) lease on a given resource
  at any instant.
* Every successful acquisition (whether of a free resource or of one whose
  previous lease expired) is issued a fencing token that is strictly greater
  than every fencing token previously issued for that resource.
* Only the current, non-expired lease holder may renew or release the lease.
  A holder whose lease has already expired -- e.g. because it was paused by
  a GC pause, a scheduler delay, or a network partition -- can no longer
  renew or release; a new client is free to acquire the resource and receive
  a higher fencing token instead.

All shared state is guarded by a single lock, so the manager is safe to call
concurrently from many real threads.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, Optional

from .clock import Clock, SystemClock


@dataclass(frozen=True)
class LeaseInfo:
    """A snapshot of a currently-held lease."""

    resource: str
    holder_id: str
    token: int
    expires_at: float


@dataclass(frozen=True)
class AcquireResult:
    """Outcome of an `acquire()` call."""

    success: bool
    resource: str
    client_id: str
    token: Optional[int] = None
    expires_at: Optional[float] = None
    reason: str = ""

    def __bool__(self) -> bool:
        return self.success


@dataclass(frozen=True)
class RenewResult:
    """Outcome of a `renew()` call."""

    success: bool
    resource: str
    client_id: str
    token: Optional[int] = None
    expires_at: Optional[float] = None
    reason: str = ""

    def __bool__(self) -> bool:
        return self.success


class _LeaseState:
    """Mutable internal record for a single resource's current lease."""

    __slots__ = ("holder_id", "token", "expires_at")

    def __init__(self, holder_id: str, token: int, expires_at: float) -> None:
        self.holder_id = holder_id
        self.token = token
        self.expires_at = expires_at


class LockManager:
    """A thread-safe, lease-based distributed lock manager (single node).

    Usage:
        manager = LockManager()
        result = manager.acquire("resource-a", "client-1", ttl=10)
        if result.success:
            token = result.token
            ...
    """

    def __init__(self, clock: Optional[Clock] = None) -> None:
        self._clock: Clock = clock or SystemClock()
        self._guard = threading.Lock()
        self._leases: Dict[str, _LeaseState] = {}
        # Tracks the highest fencing token ever issued per resource, even
        # after the lease that held it has expired/released, so tokens
        # never repeat and never go backwards.
        self._last_token: Dict[str, int] = {}

    # -- internal helpers -------------------------------------------------

    def _is_expired(self, lease: _LeaseState, now: float) -> bool:
        return lease.expires_at <= now

    # -- public API ---------------------------------------------------------

    def acquire(self, resource: str, client_id: str, ttl: float) -> AcquireResult:
        """Attempt to acquire a lease on `resource` for `client_id`.

        Succeeds if the resource is unheld, or if the existing lease has
        expired. Fails if another client currently holds a valid lease.
        A successful acquisition always issues a fencing token strictly
        greater than any token previously issued for this resource.
        """
        if ttl <= 0:
            raise ValueError("ttl must be positive")

        with self._guard:
            now = self._clock.now()
            existing = self._leases.get(resource)

            if existing is not None and not self._is_expired(existing, now):
                # Someone holds a valid lease. Only that same client could
                # "re-acquire" it -- but that's what renew() is for, so we
                # reject here regardless of identity to keep acquire/renew
                # semantics distinct and unambiguous.
                return AcquireResult(
                    success=False,
                    resource=resource,
                    client_id=client_id,
                    reason=(
                        "resource held by another client"
                        if existing.holder_id != client_id
                        else "resource already held by this client; use renew()"
                    ),
                )

            # Free (never acquired), or the previous lease expired: grant it.
            next_token = self._last_token.get(resource, 0) + 1
            self._last_token[resource] = next_token
            expires_at = now + ttl
            self._leases[resource] = _LeaseState(client_id, next_token, expires_at)

            return AcquireResult(
                success=True,
                resource=resource,
                client_id=client_id,
                token=next_token,
                expires_at=expires_at,
            )

    def renew(self, resource: str, client_id: str, ttl: float) -> RenewResult:
        """Extend the TTL of a lease already held by `client_id`.

        Rejected if there is no lease, the lease belongs to a different
        client, or the lease has already expired (even if it still nominally
        belongs to `client_id` -- an expired holder is no longer valid).
        The fencing token is unchanged by renewal.
        """
        if ttl <= 0:
            raise ValueError("ttl must be positive")

        with self._guard:
            now = self._clock.now()
            existing = self._leases.get(resource)

            if existing is None:
                return RenewResult(
                    success=False, resource=resource, client_id=client_id,
                    reason="no lease exists for this resource",
                )
            if self._is_expired(existing, now):
                return RenewResult(
                    success=False, resource=resource, client_id=client_id,
                    reason="lease has expired",
                )
            if existing.holder_id != client_id:
                return RenewResult(
                    success=False, resource=resource, client_id=client_id,
                    reason="caller is not the current lease holder",
                )

            existing.expires_at = now + ttl
            return RenewResult(
                success=True,
                resource=resource,
                client_id=client_id,
                token=existing.token,
                expires_at=existing.expires_at,
            )

    def release(self, resource: str, client_id: str) -> bool:
        """Release the lease on `resource`, if `client_id` is the current,
        valid (non-expired) holder. Returns True iff the lock was freed.
        """
        with self._guard:
            now = self._clock.now()
            existing = self._leases.get(resource)

            if existing is None:
                return False
            if self._is_expired(existing, now):
                return False
            if existing.holder_id != client_id:
                return False

            del self._leases[resource]
            return True

    def lease_info(self, resource: str) -> Optional[LeaseInfo]:
        """Return a snapshot of the current lease, if one is held and not
        expired; otherwise None."""
        with self._guard:
            now = self._clock.now()
            existing = self._leases.get(resource)
            if existing is None or self._is_expired(existing, now):
                return None
            return LeaseInfo(
                resource=resource,
                holder_id=existing.holder_id,
                token=existing.token,
                expires_at=existing.expires_at,
            )

    def current_token(self, resource: str) -> int:
        """The highest fencing token ever issued for `resource` (0 if none)."""
        with self._guard:
            return self._last_token.get(resource, 0)
