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


class LockAcquisitionError(RuntimeError):
    """Raised by the `LockManager.lease()` context manager when the lock
    could not be acquired on entry."""

    def __init__(self, result: AcquireResult) -> None:
        self.result = result
        super().__init__(
            f"could not acquire lease on {result.resource!r} for "
            f"{result.client_id!r}: {result.reason}"
        )


class _LeaseContext:
    """Context manager returned by `LockManager.lease()`.

    Acquires the lease on `__enter__` (raising `LockAcquisitionError`
    immediately if that fails -- this does not block or retry) and
    releases it on `__exit__`, including on the exception path. That's
    the case callers most often get wrong by hand: forgetting to release
    a lock because an exception skipped past their `release()` call.
    """

    __slots__ = ("_manager", "_resource", "_client_id", "_ttl", "_acquired")

    def __init__(self, manager: "LockManager", resource: str, client_id: str, ttl: float) -> None:
        self._manager = manager
        self._resource = resource
        self._client_id = client_id
        self._ttl = ttl
        self._acquired = False

    def __enter__(self) -> AcquireResult:
        result = self._manager.acquire(self._resource, self._client_id, self._ttl)
        if not result.success:
            raise LockAcquisitionError(result)
        self._acquired = True
        return result

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._acquired:
            self._manager.release(self._resource, self._client_id)
        return False


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

    @staticmethod
    def _validate_identifiers(resource: str, client_id: str) -> None:
        """Reject empty/blank resource or client identifiers.

        An empty string is a perfectly valid dict key, so without this
        check a caller with a bug that produces `resource=""` (e.g. a
        template that failed to interpolate, or a missing config value)
        would silently succeed and start contending with *every other*
        empty-string caller on a single shared "" lock -- a confusing bug
        to track down, since nothing raises anywhere near the real mistake.
        """
        if not resource or not resource.strip():
            raise ValueError("resource must be a non-empty string")
        if not client_id or not client_id.strip():
            raise ValueError("client_id must be a non-empty string")

    # -- public API ---------------------------------------------------------

    def acquire(self, resource: str, client_id: str, ttl: float) -> AcquireResult:
        """Attempt to acquire a lease on `resource` for `client_id`.

        Succeeds if the resource is unheld, or if the existing lease has
        expired. Fails if another client currently holds a valid lease.
        A successful acquisition always issues a fencing token strictly
        greater than any token previously issued for this resource.
        """
        self._validate_identifiers(resource, client_id)
        if ttl <= 0:
            raise ValueError(f"ttl must be positive, got {ttl!r}")

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
        self._validate_identifiers(resource, client_id)
        if ttl <= 0:
            raise ValueError(f"ttl must be positive, got {ttl!r}")

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
        self._validate_identifiers(resource, client_id)
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

    def lease(self, resource: str, client_id: str, ttl: float) -> _LeaseContext:
        """Convenience context manager: acquire on entry, release on exit.

        Usage:
            with manager.lease("resource-a", "client-1", ttl=10) as result:
                token = result.token
                ...  # do work; the lease is released on the way out, even
                     # if this block raises.

        Raises `LockAcquisitionError` immediately if the lease cannot be
        acquired -- this does not block or retry. For that reason it's
        best suited to callers that already have their own retry/backoff
        loop around a `try: ... with manager.lease(...): ...` block, since
        the context manager itself won't wait for the resource to free up.
        """
        return _LeaseContext(self, resource, client_id, ttl)
