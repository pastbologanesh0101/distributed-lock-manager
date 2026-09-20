# Distributed Lock Manager

A lease-based distributed mutual-exclusion lock service, implemented in pure
Python (standard library only). It simulates a single, authoritative
lock-server — the same role a real system like etcd, Zookeeper, or Chubby
plays for application code — and focuses on getting the lock/lease/fencing
semantics right, rather than on the consensus protocol underneath.

This is a companion project to [raft-consensus](https://github.com/pastbologanesh0101/raft-consensus)
(leader election + log replication). Where that project builds a replicated
log, this project builds the primitive that typically sits *on top of* one
in production: a distributed lock, with leases and fencing tokens.

## Why leases, not just locks

A traditional mutex has no notion of time: whoever holds it holds it until
they explicitly release it. In a distributed system that's dangerous —
if the holder crashes, gets network-partitioned, or is just slow, the lock
can be held forever and the whole system stalls.

A **lease** fixes this by giving every lock a time-to-live (TTL). If the
holder doesn't renew it before the TTL expires, the lease is automatically
considered free and another client can acquire it. This project's
`LockManager` implements exactly that:

- `acquire(resource, client_id, ttl)` — grants the lease if the resource is
  free or its previous lease has expired.
- `renew(resource, client_id, ttl)` — extends the TTL, but **only** if
  `client_id` is the current, still-valid holder.
- `release(resource, client_id)` — frees the lease early, again only for the
  current, still-valid holder.
- Expiry is driven by an injectable `Clock`. Production code uses
  `SystemClock` (real wall-clock time); tests use `FakeClock`, which only
  advances when told to — so lease expiry is deterministic and testable
  without ever calling `time.sleep()`.

## Why fencing tokens matter (the "GC pause" problem)

Leases solve the availability problem, but they introduce a subtler
correctness problem, best explained with the classic example:

1. Client A acquires the lock on resource `X` with a 5-second lease.
2. Client A is about to write to `X`, but the process pauses — a long GC
   pause, a slow disk, being descheduled by the OS, a network delay,
   anything. This pause lasts 10 seconds.
3. While A is frozen, its lease **expires**. The lock server now considers
   `X` free.
4. Client B notices `X` is free, acquires the lease, does its work, and
   writes to `X`.
5. Client A finally wakes up. It has no idea any of this happened — as far
   as its own memory is concerned, it still holds the lock — and it goes
   ahead and writes to `X` too.

Without any additional protection, A's stale write can land *after* B's
correct write and silently clobber it, even though the lock manager did
everything right. The lock manager can't stop this by itself: it has no way
to intercept a message A sends directly to the resource.

**Fencing tokens** solve this. Every successful `acquire()` returns a
fencing token — a strictly monotonically increasing integer, per resource.
Client A got token `1`; when B acquires the lock after A's lease expires, B
gets token `2` (never `1` again, and never lower than any token issued
before). Critically, every write to the *protected resource itself* must
carry this token, and the resource enforces:

> Reject any write whose fencing token is lower than the highest fencing
> token it has already accepted for that key.

So in the scenario above: B writes with token `2` — accepted, and the
resource now remembers `2` as the highest token seen. When A wakes up and
writes with its stale token `1`, the resource rejects it outright, because
`1 < 2`. The lock manager doesn't need to know A is stale; the resource
figures it out itself, from the token alone. This is exactly the mechanism
used in real systems (e.g. Google's Chubby, and the pattern popularized by
Martin Kleppmann's writing on distributed locks).

This project implements that resource as `FencedResource`
(`lock_manager/protected_resource.py`) — a simple key/value store that
raises `StaleFencingTokenError` on a stale write.

## Project layout

```
distributed-lock-manager/
├── lock_manager/
│   ├── __init__.py
│   ├── clock.py               # Clock / SystemClock / FakeClock
│   ├── lock_manager.py         # LockManager: acquire / renew / release
│   └── protected_resource.py   # FencedResource: fencing-token enforcement
├── tests/
│   └── test_lock_manager.py    # 23 unit tests
├── demo.py                      # runnable end-to-end demo (see below)
├── .github/workflows/tests.yml  # CI: pytest/unittest on Python 3.11 & 3.12
├── LICENSE
└── README.md
```

## Running the demo

```bash
python3 demo.py
```

The demo runs three scenarios back to back:

1. **Real race for a lock** — two actual `threading.Thread` objects,
   synchronized with a `threading.Barrier` so they hit `acquire()` at the
   same instant. Exactly one wins.
2. **Lease expiry and reacquisition** — a lease expires (via `FakeClock`,
   no real sleeping), and a second client acquires the resource with a
   strictly higher fencing token.
3. **The classic stale-write rejection** — a client's lease expires while
   it's conceptually "paused"; a second client acquires the lock and writes
   with a fresh token; the first client wakes up and tries to write with
   its now-stale token, and is correctly rejected.

### Example output

```
========================================================================
Scenario 1: two real threads race for the same lock
========================================================================
Winner: client-B got fencing token 1
Loser:  client-A rejected -> 'resource held by another client'

========================================================================
Scenario 2: lease expires, a new client reacquires with a higher token
========================================================================
worker-1 acquires lock, ttl=10s -> token=1
... 15 seconds pass without renewal (lease expires) ...
lease_info() after expiry: None (None means expired/free)
worker-2 acquires lock -> token=2
Confirmed: token strictly increased (1 -> 2)

========================================================================
Scenario 3: classic GC-pause stale write, rejected by fencing token
========================================================================
client-A acquires lock -> token=1
client-A now enters a long GC pause before it gets to write...
... 10 seconds pass; client-A's lease has now expired ...
client-B acquires lock -> token=2
client-B writes value=100 with token=2 -> accepted
client-A wakes up from its GC pause and tries to write with its stale token=1 ...
Correctly rejected: stale fencing token for 'inventory-counter': attempted=1, highest_seen=2
Final stored value: 100 (client-B's write survived, client-A's stale write was blocked)

All scenarios completed successfully.
```

(Winner names in Scenario 1 are nondeterministic between runs, since it's a
genuine thread race — only the "exactly one winner" property is guaranteed.)

## Running the tests

```bash
python3 -m unittest discover -v -s tests
```

23 tests cover, among other things:

- Acquiring an unheld lock succeeds and issues a fencing token.
- Acquiring an already-held (non-expired) lock by a different client is
  rejected.
- Acquiring an **expired** lock by a new client succeeds with a strictly
  higher fencing token than the previous holder's.
- Renewal by the correct current holder extends the lease without changing
  the fencing token.
- Renewal by a non-holder, or after expiry, is rejected.
- Fencing tokens strictly increase across repeated acquire/expire/reacquire
  cycles on the same resource — never decreasing, never repeating.
- **The key safety test**: `FencedResource` accepts writes with a
  current-or-higher fencing token, and rejects writes with a stale
  (lower-than-already-seen) token.
- **Real concurrency**: many actual `threading.Thread` objects, synchronized
  with a `threading.Barrier`, race for the same lock — exactly one succeeds,
  every time (verified over 25+ repeated race runs in a single test, plus a
  dedicated 16-thread race test, to rule out flakiness).
- `release()` only frees a lock when called by the current, valid holder.

CI (`.github/workflows/tests.yml`) runs the full suite plus the demo script
on every push and pull request, on Python 3.11 and 3.12.

## Design notes

- All shared state in `LockManager` is protected by a single
  `threading.Lock`, so `acquire`/`renew`/`release`/`lease_info` are all
  safe to call concurrently from many threads.
- Fencing tokens are tracked per-resource independently of whether a lease
  is currently held — even after a lease is released or expires, the next
  `acquire()` for that resource still issues a token higher than any ever
  issued for it, so tokens are never reused.
- `acquire()` on a resource the *same* client already validly holds is
  rejected (not silently renewed) — this keeps "get a new lease" and
  "extend my current lease" as two distinct, unambiguous operations.
