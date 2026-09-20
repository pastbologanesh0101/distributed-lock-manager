"""A protected resource that enforces fencing tokens on every write.

This is the piece that actually prevents the classic distributed-locking
bug: a client whose lease has expired (say, because it was frozen by a long
GC pause) wakes up believing it still holds the lock, and issues a write.
Without fencing tokens, that write can silently corrupt state that a second
client -- which correctly acquired the lock in the meantime -- is also
writing to.

With fencing tokens, every write must be tagged with the fencing token the
lock manager issued at acquisition time. The resource remembers the highest
token it has ever accepted for a given key, and rejects any write carrying a
token lower than that -- even if the write "arrives" after a more recent
one, or arrives from a client that has no idea its lease already expired.
"""

from __future__ import annotations

import threading
from typing import Any, Dict


class StaleFencingTokenError(Exception):
    """Raised when a write is attempted with a fencing token that is lower
    than the highest token already accepted for that key -- i.e. the write
    is coming from a stale client whose lease has since been superseded."""

    def __init__(self, key: str, attempted_token: int, highest_seen: int) -> None:
        self.key = key
        self.attempted_token = attempted_token
        self.highest_seen = highest_seen
        super().__init__(
            f"stale fencing token for {key!r}: attempted={attempted_token}, "
            f"highest_seen={highest_seen}"
        )


class FencedResource:
    """An in-memory key/value store that only accepts fenced writes.

    A write is accepted iff its fencing token is >= the highest fencing
    token already seen for that key (i.e. current-or-higher). Anything
    lower is rejected as stale.
    """

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._values: Dict[str, Any] = {}
        self._highest_token: Dict[str, int] = {}

    def write(self, key: str, value: Any, fencing_token: int) -> Any:
        """Write `value` under `key`, guarded by `fencing_token`.

        Returns the newly stored value on success.
        Raises StaleFencingTokenError if `fencing_token` is lower than the
        highest token already accepted for this key.
        """
        with self._guard:
            highest = self._highest_token.get(key, 0)
            if fencing_token < highest:
                raise StaleFencingTokenError(key, fencing_token, highest)

            self._highest_token[key] = fencing_token
            self._values[key] = value
            return value

    def try_write(self, key: str, value: Any, fencing_token: int) -> bool:
        """Non-raising variant of `write`. Returns True on success, False
        if the token was stale (the write is silently rejected)."""
        try:
            self.write(key, value, fencing_token)
            return True
        except StaleFencingTokenError:
            return False

    def read(self, key: str) -> Any:
        with self._guard:
            return self._values.get(key)

    def highest_token_seen(self, key: str) -> int:
        with self._guard:
            return self._highest_token.get(key, 0)
