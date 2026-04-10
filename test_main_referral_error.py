import ast
from datetime import datetime, timezone
from pathlib import Path


class _Logger:
    def __init__(self):
        self.exceptions = []

    def info(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        self.exceptions.append((args, kwargs))


class _InviteMapCollection:
    def find_one(self, filt, proj=None):  # noqa: ARG002
        return {"inviter_id": 100}


class _PendingReferralsCollection:
    def update_one(self, *args, **kwargs):  # noqa: ARG002
        raise RuntimeError("forced_create_pending_error")


def _load_confirm_referral_func():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn_node = next(
        node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "_confirm_referral_on_main_join"
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["_confirm_referral_on_main_join"]


def test_confirm_referral_exception_path_does_not_reference_undefined_step():
    confirm_fn = _load_confirm_referral_func()
    logger = _Logger()
    audits = []

    confirm_fn.__globals__.update(
        {
            "logger": logger,
            "GROUP_ID": -1001,
            "_truncate_invite_link": lambda link: link,
            "_write_referral_audit": lambda **kwargs: audits.append(kwargs),
            "invite_link_map_collection": _InviteMapCollection(),
            "consume_referral_rate_limits": lambda *args, **kwargs: (True, None, {}),
            "referral_rate_limits_collection": object(),
            "REFERRAL_HOURLY_LIMIT": 999,
            "REFERRAL_DAILY_LIMIT": 999,
            "now_utc": lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
            "KL_TZ": timezone.utc,
            "pending_referrals_collection": _PendingReferralsCollection(),
            "db": object(),
            "datetime": datetime,
            "timezone": timezone,
        }
    )

    confirm_fn(
        200,
        invitee_username="u200",
        invite_link="https://t.me/+abc",
        chat_id=-1001,
    )

    assert audits
    assert audits[-1]["status"] == "failed"
    assert audits[-1]["reason"] == "error"
    assert logger.exceptions
    assert "create_pending" in logger.exceptions[-1][0][0]
