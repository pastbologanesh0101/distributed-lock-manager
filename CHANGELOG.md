# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] — Initial release

The initial commit established the core lease-based lock manager and its
fencing-token mechanism:

- `lock_manager.clock` — `Clock` abstraction with `SystemClock` (real
  wall-clock time via `time.monotonic()`) and `FakeClock` (manually
  advanced, thread-safe; used throughout the test suite so lease expiry is
  deterministic and doesn't require `time.sleep()`).
- `lock_manager.lock_manager` — `LockManager`, a thread-safe, single-node,
  in-memory lock server with:
  - `acquire(resource, client_id, ttl)` — grants a lease if the resource is
    free or the previous lease expired; issues a fencing token strictly
    greater than any previously issued for that resource.
  - `renew(resource, client_id, ttl)` — extends the TTL for the current,
    still-valid holder only.
  - `release(resource, client_id)` — frees a lease early, for the current,
    still-valid holder only.
  - `lease_info(resource)` / `current_token(resource)` — read-only
    inspection helpers.
  - All shared state guarded by a single `threading.Lock`.
- `lock_manager.protected_resource` — `FencedResource`, an in-memory
  key/value store that enforces fencing tokens on every write, rejecting
  any write whose token is lower than the highest token already accepted
  for that key (`StaleFencingTokenError`), plus a non-raising `try_write`
  variant.
- `demo.py` — a runnable, three-scenario demonstration: a genuine
  multi-threaded race for one lock, lease expiry followed by reacquisition
  with a higher fencing token, and the classic "GC pause" stale-write
  scenario rejected by `FencedResource`.
- `tests/test_lock_manager.py` — 23 unit tests covering acquire/renew/
  release semantics, fencing-token monotonicity across acquire/expire/
  reacquire cycles, `FencedResource` accept/reject behavior, and real
  multi-threaded concurrency (a 16-thread race, repeated 25x to rule out
  flakiness).
- `.github/workflows/tests.yml` — CI running the test suite and the demo
  script on Python 3.11 and 3.12, on every push and pull request.
- MIT `LICENSE`.

[0.1.0]: https://github.com/pastbologanesh0101/distributed-lock-manager/commit/432db1e192b02236042dbb38ee97d60a42c4a282
