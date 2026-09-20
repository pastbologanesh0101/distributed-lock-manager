"""Unit tests for the distributed lock manager.

Uses a FakeClock throughout so lease expiry is deterministic (no real
sleeping), except for the real-concurrency test, which uses actual
threading.Thread objects and a Barrier to force a genuine race.
"""

from __future__ import annotations

import threading
import unittest

from lock_manager import FakeClock, LockAcquisitionError, LockManager
from lock_manager.protected_resource import FencedResource, StaleFencingTokenError


class AcquireTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock(start=0.0)
        self.manager = LockManager(clock=self.clock)

    def test_acquire_unheld_lock_succeeds_and_issues_fencing_token(self):
        result = self.manager.acquire("res-1", "client-A", ttl=10)
        self.assertTrue(result.success)
        self.assertIsNotNone(result.token)
        self.assertGreater(result.token, 0)

    def test_acquire_already_held_lock_by_different_client_is_rejected(self):
        first = self.manager.acquire("res-1", "client-A", ttl=10)
        self.assertTrue(first.success)

        second = self.manager.acquire("res-1", "client-B", ttl=10)
        self.assertFalse(second.success)
        self.assertIn("held", second.reason)

    def test_acquire_expired_lock_by_new_client_succeeds_with_higher_token(self):
        first = self.manager.acquire("res-1", "client-A", ttl=5)
        self.clock.advance(6)  # lease expires

        second = self.manager.acquire("res-1", "client-B", ttl=5)
        self.assertTrue(second.success)
        self.assertGreater(second.token, first.token)

    def test_acquire_rejects_non_positive_ttl(self):
        with self.assertRaises(ValueError):
            self.manager.acquire("res-1", "client-A", ttl=0)

    def test_lease_considered_expired_exactly_at_expiry_boundary(self):
        # expires_at is computed as now + ttl, and _is_expired treats
        # `expires_at <= now` as expired. So a lease acquired at t=0 with
        # ttl=5 must be considered expired the instant the clock reaches
        # exactly t=5, not just strictly after it. An off-by-one here would
        # let a lease linger one tick too long (a safety bug) or expire one
        # tick too early (a false rejection of a still-valid holder).
        first = self.manager.acquire("res-1", "client-A", ttl=5)
        self.clock.advance(5)  # now == expires_at exactly

        self.assertIsNone(self.manager.lease_info("res-1"))

        second = self.manager.acquire("res-1", "client-B", ttl=5)
        self.assertTrue(second.success, "lease must be treated as expired at the exact boundary")
        self.assertGreater(second.token, first.token)

    def test_acquire_rejects_empty_or_blank_resource_and_client_id(self):
        # An empty string is a valid dict key, so without explicit
        # validation a caller bug that produces resource="" would silently
        # "succeed" and start contending with every other empty-key caller
        # on one shared lock, instead of failing loudly at the call site.
        with self.assertRaises(ValueError):
            self.manager.acquire("", "client-A", ttl=10)
        with self.assertRaises(ValueError):
            self.manager.acquire("   ", "client-A", ttl=10)
        with self.assertRaises(ValueError):
            self.manager.acquire("res-1", "", ttl=10)

    def test_acquire_on_two_distinct_resources_does_not_conflict(self):
        a = self.manager.acquire("res-1", "client-A", ttl=10)
        b = self.manager.acquire("res-2", "client-B", ttl=10)
        self.assertTrue(a.success)
        self.assertTrue(b.success)

    def test_acquire_same_client_while_it_already_holds_valid_lease_is_rejected(self):
        first = self.manager.acquire("res-1", "client-A", ttl=10)
        self.assertTrue(first.success)

        second = self.manager.acquire("res-1", "client-A", ttl=10)
        self.assertFalse(second.success, "re-acquiring should not silently succeed; use renew()")


class RenewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock(start=0.0)
        self.manager = LockManager(clock=self.clock)

    def test_renew_by_current_holder_extends_lease_and_keeps_token(self):
        acquired = self.manager.acquire("res-1", "client-A", ttl=5)
        self.clock.advance(3)

        renewed = self.manager.renew("res-1", "client-A", ttl=10)
        self.assertTrue(renewed.success)
        self.assertEqual(renewed.token, acquired.token)

        # Would have expired at t=5 without renewal; confirm it's still valid
        # at t=8 (3 + renewal ttl of 10 puts new expiry at t=13).
        self.clock.advance(5)  # now t=8
        info = self.manager.lease_info("res-1")
        self.assertIsNotNone(info)
        self.assertEqual(info.token, acquired.token)

    def test_renew_by_non_holder_is_rejected(self):
        self.manager.acquire("res-1", "client-A", ttl=10)
        renewed = self.manager.renew("res-1", "client-B", ttl=10)
        self.assertFalse(renewed.success)

    def test_renew_after_expiry_is_rejected_even_for_original_holder(self):
        self.manager.acquire("res-1", "client-A", ttl=5)
        self.clock.advance(6)  # lease expired

        renewed = self.manager.renew("res-1", "client-A", ttl=10)
        self.assertFalse(renewed.success)
        self.assertIn("expired", renewed.reason)

    def test_renew_nonexistent_lease_is_rejected(self):
        renewed = self.manager.renew("never-acquired", "client-A", ttl=10)
        self.assertFalse(renewed.success)

    def test_renew_rejects_non_positive_ttl(self):
        # renew() validates ttl the same way acquire() does, but that check
        # had no test of its own -- a regression here would only be caught
        # accidentally, e.g. by a caller passing ttl=0 in production.
        self.manager.acquire("res-1", "client-A", ttl=10)

        with self.assertRaises(ValueError):
            self.manager.renew("res-1", "client-A", ttl=0)
        with self.assertRaises(ValueError):
            self.manager.renew("res-1", "client-A", ttl=-5)

        # The existing valid lease must be unaffected by the rejected calls.
        info = self.manager.lease_info("res-1")
        self.assertIsNotNone(info)
        self.assertEqual(info.holder_id, "client-A")


class FencingTokenSequenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock(start=0.0)
        self.manager = LockManager(clock=self.clock)

    def test_fencing_tokens_strictly_increase_across_acquire_expire_reacquire_cycles(self):
        tokens = []
        for _ in range(5):
            result = self.manager.acquire("res-1", "client-rotating", ttl=2)
            self.assertTrue(result.success)
            tokens.append(result.token)
            self.clock.advance(3)  # force expiry before next cycle

        for earlier, later in zip(tokens, tokens[1:]):
            self.assertLess(earlier, later)

        self.assertEqual(len(tokens), len(set(tokens)), "tokens must never repeat")

    def test_fencing_tokens_never_decrease_after_release_and_reacquire(self):
        first = self.manager.acquire("res-1", "client-A", ttl=10)
        self.manager.release("res-1", "client-A")
        second = self.manager.acquire("res-1", "client-B", ttl=10)
        self.assertGreater(second.token, first.token)


class ReleaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock(start=0.0)
        self.manager = LockManager(clock=self.clock)

    def test_release_by_current_valid_holder_frees_the_lock(self):
        self.manager.acquire("res-1", "client-A", ttl=10)
        freed = self.manager.release("res-1", "client-A")
        self.assertTrue(freed)

        # A different client can now acquire it immediately.
        result = self.manager.acquire("res-1", "client-B", ttl=10)
        self.assertTrue(result.success)

    def test_release_by_non_holder_fails_and_lock_remains_held(self):
        self.manager.acquire("res-1", "client-A", ttl=10)
        freed = self.manager.release("res-1", "client-B")
        self.assertFalse(freed)

        # Original holder's lock should still be considered held.
        still_blocked = self.manager.acquire("res-1", "client-C", ttl=10)
        self.assertFalse(still_blocked.success)

    def test_release_after_expiry_fails(self):
        self.manager.acquire("res-1", "client-A", ttl=5)
        self.clock.advance(6)
        freed = self.manager.release("res-1", "client-A")
        self.assertFalse(freed, "an already-expired holder has nothing valid to release")

    def test_release_nonexistent_lease_fails(self):
        freed = self.manager.release("never-acquired", "client-A")
        self.assertFalse(freed)


class FencedResourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.resource = FencedResource()

    def test_write_with_valid_current_token_is_accepted(self):
        value = self.resource.write("k", "v1", fencing_token=1)
        self.assertEqual(value, "v1")
        self.assertEqual(self.resource.read("k"), "v1")

    def test_write_with_higher_token_than_previously_seen_is_accepted(self):
        self.resource.write("k", "v1", fencing_token=1)
        self.resource.write("k", "v2", fencing_token=5)
        self.assertEqual(self.resource.read("k"), "v2")
        self.assertEqual(self.resource.highest_token_seen("k"), 5)

    def test_write_with_stale_token_is_rejected(self):
        self.resource.write("k", "v1", fencing_token=5)
        with self.assertRaises(StaleFencingTokenError):
            self.resource.write("k", "stale-write", fencing_token=2)
        # The stale write must not have taken effect.
        self.assertEqual(self.resource.read("k"), "v1")

    def test_try_write_returns_false_on_stale_token_without_raising(self):
        self.resource.write("k", "v1", fencing_token=5)
        ok = self.resource.try_write("k", "stale-write", fencing_token=2)
        self.assertFalse(ok)
        self.assertEqual(self.resource.read("k"), "v1")

    def test_end_to_end_stale_client_write_after_lease_expiry_is_rejected(self):
        """The full classic scenario: client A's lease expires (e.g. it was
        frozen by a GC pause); client B acquires the lock and writes with a
        higher token; client A wakes up and tries to write with its now-stale
        token, and is correctly rejected."""
        clock = FakeClock(start=0.0)
        manager = LockManager(clock=clock)
        counter = FencedResource()

        a = manager.acquire("shared-counter", "client-A", ttl=5)
        clock.advance(10)  # client A's lease silently expires
        b = manager.acquire("shared-counter", "client-B", ttl=5)

        counter.write("shared-counter", 42, fencing_token=b.token)

        with self.assertRaises(StaleFencingTokenError):
            counter.write("shared-counter", 999, fencing_token=a.token)

        self.assertEqual(counter.read("shared-counter"), 42)


class LeaseContextManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock(start=0.0)
        self.manager = LockManager(clock=self.clock)

    def test_lease_context_manager_acquires_and_auto_releases_on_normal_exit(self):
        with self.manager.lease("res-1", "client-A", ttl=10) as result:
            self.assertTrue(result.success)
            self.assertIsNotNone(self.manager.lease_info("res-1"))

        # Released on the way out -- a different client can acquire it now.
        self.assertIsNone(self.manager.lease_info("res-1"))
        second = self.manager.acquire("res-1", "client-B", ttl=10)
        self.assertTrue(second.success)

    def test_lease_context_manager_releases_even_when_block_raises(self):
        with self.assertRaises(ValueError):
            with self.manager.lease("res-1", "client-A", ttl=10):
                raise ValueError("boom")

        # The lease must still have been released despite the exception --
        # this is the exact case a hand-written try/finally is easy to get
        # wrong by forgetting.
        self.assertIsNone(self.manager.lease_info("res-1"))

    def test_lease_context_manager_raises_lock_acquisition_error_when_contended(self):
        self.manager.acquire("res-1", "client-A", ttl=10)

        with self.assertRaises(LockAcquisitionError):
            with self.manager.lease("res-1", "client-B", ttl=10):
                self.fail("block body must not run if acquisition failed")

        # client-A's lease must be untouched by the failed attempt.
        info = self.manager.lease_info("res-1")
        self.assertIsNotNone(info)
        self.assertEqual(info.holder_id, "client-A")


class ConcurrencyTests(unittest.TestCase):
    def test_many_real_threads_race_for_one_lock_exactly_one_succeeds(self):
        manager = LockManager()  # real SystemClock; genuine race, no sleeping
        num_clients = 16
        barrier = threading.Barrier(num_clients)
        results = [None] * num_clients

        def worker(index: int) -> None:
            barrier.wait()  # release all threads at (approximately) once
            results[index] = manager.acquire("contended-resource", f"client-{index}", ttl=30)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_clients)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        self.assertEqual(len(successes), 1, "exactly one client must win the race")
        self.assertEqual(len(failures), num_clients - 1)

    def test_repeated_concurrent_races_are_never_flaky(self):
        # Run the race many times over to guard against rare interleavings
        # that a single run might not expose.
        for _ in range(25):
            manager = LockManager()
            barrier = threading.Barrier(8)
            results = [None] * 8

            def worker(index: int) -> None:
                barrier.wait()
                results[index] = manager.acquire("res", f"client-{index}", ttl=10)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            successes = [r for r in results if r.success]
            self.assertEqual(len(successes), 1)


if __name__ == "__main__":
    unittest.main()
