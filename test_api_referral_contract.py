"""
Contract tests for the /api/referral route: it must never pair a usable
displayed link with success:false, and must label how the link was produced
via an explicit `mode` (invite_link on success, absent/null on failure —
there is no functional fallback deep-link mode, see investigation notes in
main.py's get_or_create_referral_invite_link_sync / api_referral).
"""
import ast
from pathlib import Path


class _FakeArgs:
    def __init__(self, values):
        self._values = values

    def get(self, key):
        return self._values.get(key)


class _FakeRequest:
    def __init__(self, args):
        self.args = _FakeArgs(args)


class _FakeApp:
    def route(self, *args, **kwargs):  # noqa: ARG002
        def decorator(fn):
            return fn

        return decorator


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


def _identity_jsonify(payload):
    return payload


def _load_api_referral_func():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "api_referral"
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {
        "app": _FakeApp(),
        "jsonify": _identity_jsonify,
        "logger": _Logger(),
    }
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["api_referral"]


def _run(fn, *, user_id=42, username="tester", link_result=None, link_error=None, snapshot=None):
    def fake_get_or_create(uid, uname):  # noqa: ARG001
        if link_error is not None:
            raise link_error
        return link_result

    def fake_snapshot(uid):  # noqa: ARG001
        if snapshot is None:
            return None, None, None
        return snapshot, "2026-07-21T00:00:00Z", 1

    fn.__globals__.update(
        {
            "request": _FakeRequest({"user_id": str(user_id), "username": username}),
            "get_or_create_referral_invite_link_sync": fake_get_or_create,
            "_get_user_snapshot": fake_snapshot,
        }
    )
    body, status = fn()
    return body, status


def test_successful_invite_link_generation_reports_mode_invite_link():
    fn = _load_api_referral_func()
    body, status = _run(fn, link_result="https://t.me/+realInviteLink")

    assert status == 200
    assert body["success"] is True
    assert body["mode"] == "invite_link"
    assert body["referral_link"] == "https://t.me/+realInviteLink"
    assert "error" not in body


def test_genuine_failure_returns_no_usable_link():
    fn = _load_api_referral_func()
    body, status = _run(fn, link_error=RuntimeError("createChatInviteLink failed: bot not admin"))

    assert status == 200
    assert body["success"] is False
    assert body["referral_link"] is None
    assert body.get("mode") is None
    assert "error" in body


def test_failure_response_never_pairs_success_false_with_a_usable_link():
    """Regression guard: a prior version of this endpoint returned a
    t.me/<bot>?start=ref<uid> deep-link as `referral_link` alongside
    success:false. That deep-link is never parsed by /start and never
    reaches referral attribution, so it must not be surfaced as a link."""
    fn = _load_api_referral_func()
    body, _ = _run(fn, link_error=RuntimeError("boom"))

    assert body["success"] is False
    assert not body["referral_link"]


def test_missing_user_id_returns_400_without_touching_link_generation():
    fn = _load_api_referral_func()
    fn.__globals__.update({"request": _FakeRequest({"user_id": None})})

    body, status = fn()

    assert status == 400
    assert body["success"] is False


def test_stats_snapshot_included_on_both_success_and_failure():
    fn = _load_api_referral_func()
    snapshot = {"total_referrals": 3, "weekly_referrals": 1, "monthly_referrals": 2}

    ok_body, _ = _run(fn, link_result="https://t.me/+abc", snapshot=snapshot)
    assert ok_body["total_referrals"] == 3

    fail_body, _ = _run(fn, link_error=RuntimeError("boom"), snapshot=snapshot)
    assert fail_body["total_referrals"] == 3
