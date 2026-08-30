"""The PR gate must name suites that exist, collect, and are green.

A CI gate is only worth the trust placed in it. Two ways this one could rot
into a rubber stamp:

  * it names a file that no longer exists, or one that errors at collection —
    pytest is then gating on less than the list claims;
  * it names a suite with known failures, which forces someone to ignore the
    exit code, and from then on the check reports green regardless.

Both are asserted here, against the workflow file itself rather than a copy of
the list, so the two cannot drift apart.
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent
WORKFLOW = ROOT / ".github" / "workflows" / "affiliate-migration-ci.yml"
BASELINE = ROOT / "ci" / "known_test_failures.txt"


def _workflow_text() -> str:
    assert WORKFLOW.exists(), f"the PR workflow is missing: {WORKFLOW}"
    return WORKFLOW.read_text()


def _gating_suites() -> list[str]:
    """The TARGETED_SUITES block, read from the workflow itself."""
    text = _workflow_text()
    m = re.search(r"^  TARGETED_SUITES: >-\n((?:    .*\n)+)", text, re.M)
    assert m, "TARGETED_SUITES not found in the workflow"
    return m.group(1).split()


GATE = _gating_suites()


def test_the_gate_names_at_least_the_migration_suites():
    required = {
        "test_affiliate_reward_plan_migration.py",
        "test_affiliate_pool_catalogue.py",
        "test_affiliate_threshold_drift.py",
        "test_affiliate_inventory_preflight.py",
        "test_affiliate_index_catalogue.py",
        "test_affiliate_surplus_sweep_bounded.py",
        "test_affiliate_lease_liveness.py",
        "test_affiliate_retry_sweep_fairness.py",
        "test_affiliate_verifier_readonly.py",
        "test_affiliate_voucher_batches.py",
    }
    missing = sorted(required - set(GATE))
    assert not missing, f"suites this migration added are not gated: {missing}"


def test_the_gate_has_no_duplicates():
    dupes = sorted({s for s in GATE if GATE.count(s) > 1})
    assert not dupes, f"duplicated entries in the gate: {dupes}"


@pytest.mark.parametrize("suite", GATE)
def test_each_gated_suite_exists(suite):
    assert (ROOT / suite).exists(), (
        f"the CI gate names {suite}, which does not exist — pytest would gate on "
        f"less than the list claims"
    )


@pytest.mark.parametrize("suite", GATE)
def test_each_gated_suite_collects_tests(suite):
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", suite],
        cwd=ROOT, capture_output=True, text=True,
    )
    assert proc.returncode == 0, (
        f"{suite} does not collect cleanly:\n{proc.stdout[-1500:]}\n{proc.stderr[-800:]}"
    )
    m = re.search(r"(\d+) tests? collected", proc.stdout)
    assert m and int(m.group(1)) > 0, (
        f"{suite} collected no tests; gating on it is meaningless\n{proc.stdout[-800:]}"
    )


@pytest.mark.parametrize("suite", GATE)
def test_no_gated_suite_has_known_failures(suite):
    """A gate containing a known-failing suite forces someone to ignore the
    exit code, and a check that ignores its exit code is worse than no check."""
    known = BASELINE.read_text().splitlines() if BASELINE.exists() else []
    offending = sorted({l for l in known if l.split(" ", 1)[-1].startswith(suite + "::")})
    assert not offending, (
        f"{suite} is in the PR gate but has failures recorded in "
        f"ci/known_test_failures.txt:\n  " + "\n  ".join(offending)
    )


def _workflow_directives() -> str:
    """The workflow with comment-only lines stripped: the prose explains what
    the file must NOT do, and matching that prose is a false positive."""
    return "\n".join(
        l for l in _workflow_text().splitlines() if not l.lstrip().startswith("#")
    )


def test_the_gate_does_not_hide_failures():
    text = _workflow_directives()
    for forbidden, why in (
        ("continue-on-error", "a step that cannot fail is not a gate"),
        ("|| true", "swallowing the exit code reports green regardless"),
        ("|| exit 0", "swallowing the exit code reports green regardless"),
    ):
        assert forbidden not in text, f"{forbidden!r} in the workflow: {why}"


def test_the_workflow_deploys_nothing_and_needs_no_secret():
    text = _workflow_directives()
    assert "secrets." not in text, "a PR test workflow must not need production secrets"
    for word in ("flyctl", "fly deploy", "FLY_API_TOKEN"):
        assert word not in text, f"the PR workflow must not deploy ({word!r} found)"


def test_the_baseline_file_is_present_and_well_formed():
    assert BASELINE.exists(), "ci/known_test_failures.txt is missing"
    lines = [l for l in BASELINE.read_text().splitlines() if l.strip()]
    assert lines, "the baseline is empty; an empty baseline compares against nothing"
    bad = [l for l in lines if not re.match(r"^(FAILED|ERROR) [A-Za-z0-9_]+\.py", l)]
    assert not bad, f"malformed baseline entries: {bad[:5]}"


# ---------------------------------------------------------------------------
# CI must install everything the suite imports
# ---------------------------------------------------------------------------

#: Modules whose import name differs from their distribution name.
_DISTRIBUTION_NAME = {
    "yaml": "PyYAML",
    "dateutil": "python-dateutil",
    "bson": "pymongo",
    "gridfs": "pymongo",
    "telegram": "python-telegram-bot",
    "flask_cors": "flask-cors",
    "google": "google-auth",
    "apscheduler": "APScheduler",
    "pytest_asyncio": "pytest-asyncio",
}


def _declared_requirements() -> set[str]:
    names = set()
    for fname in ("requirements.txt", "requirements-dev.txt"):
        path = ROOT / fname
        assert path.exists(), f"{fname} is missing"
        for line in path.read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            names.add(re.split(r"[=<>!~\[]", line, 1)[0].strip().lower())
    return names


def _third_party_test_imports() -> set[str]:
    """Top-level third-party modules imported by the test suite."""
    import ast
    import sys

    local = {p.stem for p in ROOT.glob("*.py")}
    local |= {p.name for p in ROOT.iterdir() if p.is_dir() and not p.name.startswith(".")}

    found = set()
    for path in sorted(ROOT.glob("test_*.py")) + [ROOT / "fake_mongo.py"]:
        if not path.exists():
            continue
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # pragma: no cover - a broken test file fails elsewhere
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    found.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                found.add(node.module.split(".")[0])
    return {
        n for n in found
        if n not in local and n not in sys.stdlib_module_names and n != "__future__"
    }


def test_every_module_the_suite_imports_is_declared_for_ci():
    """CI installs only what the requirements files name. A test-only import
    that is declared nowhere is not a missing test — ten of this suite's
    modules import mongomock, and an ImportError at COLLECTION aborts the
    entire pytest run rather than failing ten tests."""
    declared = _declared_requirements()
    undeclared = sorted(
        mod for mod in _third_party_test_imports()
        if _DISTRIBUTION_NAME.get(mod, mod).lower() not in declared
    )
    assert not undeclared, (
        "these third-party modules are imported by the test suite but appear in "
        "neither requirements.txt nor requirements-dev.txt, so CI will not have "
        f"them: {undeclared}"
    )


def test_the_test_toolchain_is_pinned():
    """An unpinned toolchain means CI silently runs a different pytest from the
    one the suite was verified against."""
    text = (ROOT / "requirements-dev.txt").read_text()
    for pkg in ("pytest", "pytest-asyncio", "mongomock"):
        assert re.search(rf"^{re.escape(pkg)}==", text, re.M), (
            f"{pkg} is not pinned in requirements-dev.txt"
        )


def test_both_ci_jobs_install_the_dev_requirements():
    text = _workflow_directives()
    assert text.count("pip install -r requirements-dev.txt") == 2, (
        "both jobs must install the test-only dependencies; a job that installs "
        "only requirements.txt cannot even collect the suite"
    )
