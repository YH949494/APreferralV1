#!/usr/bin/env python3
"""Run the full APReferral suite and compare its failure set to the baseline.

This repository's legacy suite has long-standing failures — test-harness gaps
(``'FakeCollection' object has no attribute 'aggregate'``; a mock returning
``None`` for ``update_one``), not product defects. Gating a PR on "the full
suite is green" is therefore impossible, and gating it on an ignored exit code
is worse than nothing: it reports green while a real regression walks past.

So this compares the EXACT SET of failing test identifiers against
``ci/known_test_failures.txt`` and fails on any difference in either
direction:

  * a new identifier  -> this change broke something
  * a missing one     -> a known failure was fixed; update the baseline in the
                         same PR so the file keeps meaning what it says

Two modules are excluded, both broken before this branch existed:

  test_ugc_growth_referral.py   imports ``scheduler._eligible_referrer_tiers``,
                                which does not exist. A collection error aborts
                                the entire pytest run, so it cannot simply be
                                left to fail.
  test_initdata_logging.py      replaces the ``database`` module with a stub at
                                import time, which leaks into unrelated modules
                                collected after it.

Both exclusions are asserted below, so an exclusion cannot quietly grow into a
way of hiding a newly broken module.
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASELINE = ROOT / "ci" / "known_test_failures.txt"

EXCLUDED = {
    "test_ugc_growth_referral.py":
        "ImportError: cannot import name '_eligible_referrer_tiers' from 'scheduler'",
    "test_initdata_logging.py":
        None,  # import-time monkeypatching of `database`; runs clean alone
}

LINE_RE = re.compile(r"^(FAILED|ERROR) ([A-Za-z0-9_]+\.py[^\s]*)")


def _run_pytest(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:randomly", *args],
        cwd=ROOT, capture_output=True, text=True,
    )


def _failure_ids(output: str) -> set[str]:
    ids = set()
    for line in output.splitlines():
        m = LINE_RE.match(line)
        if m:
            ids.add(f"{m.group(1)} {m.group(2)}")
    return ids


def _check_exclusions() -> int:
    """An excluded module must still be broken for the reason we recorded."""
    problems = []
    for module, expected in EXCLUDED.items():
        if not (ROOT / module).exists():
            problems.append(f"{module}: excluded but no longer present — drop it from EXCLUDED")
            continue
        if expected is None:
            continue
        proc = _run_pytest([module])
        combined = proc.stdout + proc.stderr
        if proc.returncode == 0:
            problems.append(
                f"{module}: excluded as broken, but it now passes — remove the exclusion"
            )
        elif expected not in combined:
            problems.append(
                f"{module}: excluded for {expected!r}, but that is no longer the failure:\n"
                + "\n".join(combined.splitlines()[-8:])
            )
    for p in problems:
        print(f"::error::{p}")
    return 1 if problems else 0


def main() -> int:
    if not BASELINE.exists():
        print(f"::error::missing baseline file {BASELINE}")
        return 1
    expected = {l.strip() for l in BASELINE.read_text().splitlines() if l.strip()}

    rc = _check_exclusions()

    proc = _run_pytest([f"--ignore={m}" for m in EXCLUDED])
    combined = proc.stdout + proc.stderr
    actual = _failure_ids(combined)

    summary = [l for l in combined.splitlines()
               if re.search(r"\d+ (passed|failed)", l)]

    # A run that never got past collection has no failure set to compare, and
    # comparing anyway is actively misleading: the baseline's 89 entries all
    # look "newly passing" when in truth nothing ran. Diagnose it as what it
    # is — usually a missing test dependency (see requirements-dev.txt).
    interrupted = re.search(r"Interrupted: (\d+) errors? during collection", combined)
    if interrupted or not summary:
        print("::error::the test run did not complete — this is NOT a failure-set "
              "difference, nothing ran to compare")
        if interrupted:
            print(f"::error::pytest aborted during collection with "
                  f"{interrupted.group(1)} module error(s)")
        for line in combined.splitlines():
            if re.match(r"^(ERROR|E\s+\w*(Import|ModuleNotFound)Error)", line):
                print(f"  {line.strip()}")
        print("\nMost likely cause: a module a test imports is not installed. "
              "requirements-dev.txt pins the test-only dependencies; check it "
              "covers everything the suite imports.")
        return 1

    print("\n".join(summary[-3:]))

    new = sorted(actual - expected)
    fixed = sorted(expected - actual)
    print(f"\nbaseline: {len(expected)}   observed: {len(actual)}   "
          f"new: {len(new)}   newly passing: {len(fixed)}")

    if new:
        print("\n::error::NEW failures — this change broke something:")
        for i in new:
            print(f"  + {i}")
        rc = 1
    if fixed:
        print("\n::error::known failures that now PASS — update ci/known_test_failures.txt:")
        for i in fixed:
            print(f"  - {i}")
        rc = 1
    if not new and not fixed:
        print("\nfailure set is byte-identical to the baseline: zero regressions.")
    return rc


if __name__ == "__main__":
    sys.exit(main())
