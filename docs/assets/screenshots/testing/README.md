# Test Evidence

## Real evidence (captured 2026-09-28)

**760 tests collected** (fresh `pytest --collect-only` run, just now):
```
760 tests collected in 13.05s
```

**Client Data Delivery — full, isolated suite, run just now:**
```
25 passed in 3.59s
```

**Most recently observed complete full-suite run**: 758 passed, 2 transient timeout
failures, 3 skipped. The two timeout failures were traced to a multi-hour host-suspend
event during that specific background run (not a code defect); both were individually
re-run afterward and passed cleanly.

**This is reported as the verified evidence available — it is not a claim that "100% of
tests pass" in one single, uninterrupted final run of the entire 760-test suite.** The
full suite's real runtime (comparable to a fresh environment) is long enough that a single
uninterrupted run has not yet been completed without an external interruption; the
component-level evidence above is accurate and independently reproducible.

**Caption**: *"Testing — 25/25 Client Data Delivery tests passing against isolated
fixture data; 760 tests collected project-wide."*

## Manual screenshot checklist

- [ ] A terminal running `python -m pytest -q` showing the delivery suite result above
- [ ] A terminal running `python -m pytest --collect-only -q` showing the 760-test count

No credentials appear in any test output — the delivery tests use controlled fixture data,
never production credentials or real API keys.
