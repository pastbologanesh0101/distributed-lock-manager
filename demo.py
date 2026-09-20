#!/usr/bin/env python3
"""Demo: distributed lock manager with lease expiry and fencing tokens.

Runs three scenarios end to end:

  1. Two real client threads race to acquire the same lock. Exactly one
     wins; the other is correctly rejected.
  2. A client's lease expires (simulated via a FakeClock, no real sleeping)
     and a second client successfully reacquires the resource, receiving a
     strictly higher fencing token.
  3. The classic "GC pause" bug: a client's lease expires while it is
     "paused" mid-operation. It wakes up, unaware its lease is gone, and
     tries to write to the protected resource using its now-stale fencing
     token. The write is correctly rejected, because a second client has
     since acquired the lock and already written with a higher token.

Run with:  python3 demo.py
"""

from __future__ import annotations

import threading

from lock_manager import FakeClock, LockManager, StaleFencingTokenError
from lock_manager.protected_resource import FencedResource


def banner(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def scenario_race() -> None:
    banner("Scenario 1: two real threads race for the same lock")

    manager = LockManager()
    resource = "checkout-cart-42"
    results = {}
    start_barrier = threading.Barrier(2)

    def client(client_id: str) -> None:
        start_barrier.wait()  # line both threads up so they race together
        results[client_id] = manager.acquire(resource, client_id, ttl=30)

    threads = [
        threading.Thread(target=client, args=("client-A",)),
        threading.Thread(target=client, args=("client-B",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [cid for cid, res in results.items() if res.success]
    losers = [cid for cid, res in results.items() if not res.success]
    assert len(winners) == 1, "expected exactly one winner"

    print(f"Winner: {winners[0]} got fencing token {results[winners[0]].token}")
    print(f"Loser:  {losers[0]} rejected -> {results[losers[0]].reason!r}")


def scenario_expiry_and_reacquire() -> None:
    banner("Scenario 2: lease expires, a new client reacquires with a higher token")

    clock = FakeClock(start=1000.0)
    manager = LockManager(clock=clock)
    resource = "batch-job-lock"

    first = manager.acquire(resource, "worker-1", ttl=10)
    print(f"worker-1 acquires lock, ttl=10s -> token={first.token}")

    print("... 15 seconds pass without renewal (lease expires) ...")
    clock.advance(15)

    still_valid = manager.lease_info(resource)
    print(f"lease_info() after expiry: {still_valid} (None means expired/free)")

    second = manager.acquire(resource, "worker-2", ttl=10)
    print(f"worker-2 acquires lock -> token={second.token}")

    assert second.success
    assert second.token > first.token, "reacquired token must be strictly higher"
    print(f"Confirmed: token strictly increased ({first.token} -> {second.token})")


def scenario_stale_write_rejected() -> None:
    banner("Scenario 3: classic GC-pause stale write, rejected by fencing token")

    clock = FakeClock(start=0.0)
    manager = LockManager(clock=clock)
    counter = FencedResource()
    resource = "inventory-counter"

    # Client A acquires the lock and gets fencing token 1.
    a = manager.acquire(resource, "client-A", ttl=5)
    print(f"client-A acquires lock -> token={a.token}")
    print("client-A now enters a long GC pause before it gets to write...")

    # While client A is "paused", its lease expires.
    clock.advance(10)
    print("... 10 seconds pass; client-A's lease has now expired ...")

    # Client B notices the lock is free (expired) and acquires it.
    b = manager.acquire(resource, "client-B", ttl=5)
    print(f"client-B acquires lock -> token={b.token}")

    # Client B does its work and writes, correctly, with its fresh token.
    counter.write(resource, value=100, fencing_token=b.token)
    print(f"client-B writes value=100 with token={b.token} -> accepted")

    # Client A now wakes up from its GC pause, blissfully unaware its lease
    # expired, and tries to write using its stale token.
    print("client-A wakes up from its GC pause and tries to write with its "
          f"stale token={a.token} ...")
    try:
        counter.write(resource, value=999, fencing_token=a.token)
        print("ERROR: stale write was NOT rejected (this should not happen!)")
    except StaleFencingTokenError as exc:
        print(f"Correctly rejected: {exc}")

    final_value = counter.read(resource)
    print(f"Final stored value: {final_value} (client-B's write survived, "
          f"client-A's stale write was blocked)")
    assert final_value == 100


def main() -> None:
    scenario_race()
    scenario_expiry_and_reacquire()
    scenario_stale_write_rejected()
    print()
    print("All scenarios completed successfully.")


if __name__ == "__main__":
    main()
