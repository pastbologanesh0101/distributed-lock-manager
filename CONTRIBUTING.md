# Contributing

Thanks for looking at `distributed-lock-manager`. This is a small, focused
library (pure Python standard library, no runtime dependencies), so the bar
for contributions is: correct, tested, and consistent with the existing
style.

## Setup

No `requirements.txt` is needed to *use* the library — it has zero runtime
dependencies. For development (running the test suite with `pytest`, or
linting), create a virtualenv and install the dev tools you need:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pytest flake8
```

## Running the tests

The suite is written with `unittest` but is fully `pytest`-compatible:

```bash
python3 -m pytest tests/ -v
```

or, with no extra dependencies at all:

```bash
python3 -m unittest discover -v -s tests
```

Also run the demo script to sanity-check end-to-end behavior after a change
to `lock_manager/`:

```bash
python3 demo.py
```

All three commands are what CI (`.github/workflows/tests.yml`) runs on every
push and pull request, across the supported Python versions.

## Code style

- Standard library only. Do not add third-party runtime dependencies to
  `lock_manager/` — the whole point of this project is a dependency-free
  reference implementation.
- Type hints on all public functions/methods, and `from __future__ import
  annotations` at the top of modules that use them (matches the existing
  files).
- Docstrings that explain *why*, not just *what* — this codebase treats the
  module and method docstrings as the primary design documentation (see the
  existing modules for the expected level of detail on tricky semantics like
  lease expiry and fencing tokens).
- Keep `LockManager`'s public API thread-safe: any change touching shared
  state (`_leases`, `_last_token`) must stay inside the existing
  `self._guard` lock.
- Prefer raising `ValueError` for invalid input (e.g. non-positive TTL) over
  silently coercing or ignoring it — callers should find bugs at the call
  site, not downstream.

## Submitting changes

1. Add or update tests in `tests/test_lock_manager.py` for any behavior
   change. Tests use `FakeClock` for anything involving lease expiry —
   avoid `time.sleep()`-based tests, since they're slow and flaky.
2. Run `python3 -m pytest tests/ -v` and `python3 demo.py` locally; both
   must pass before opening a pull request.
3. Keep commits focused: one logical change per commit, with a clear
   one-line summary of *why* the change was made.
4. Open a pull request against `main`. CI must be green before it's merged.
