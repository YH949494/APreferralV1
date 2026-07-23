import ast
from datetime import datetime, timezone
from pathlib import Path


class _Logger:
    def __init__(self):
        self.exceptions = []
        self.errors = []

    def info(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        self.errors.append((args, kwargs))

    def exception(self, *args, **kwargs):
        self.exceptions.append((args, kwargs))


class _InviteMapCollection:
    def find_one(self, filt, proj=None):  # noqa: ARG002
        return {"inviter_id": 100}


class _PendingReferralsCollection:
    def update_one(self, *args, **kwargs):  # noqa: ARG002
        raise RuntimeError("forced_create_pending_error")


class _EmptyLookupCollection:
    """Always reports no historical row, never raises."""

    def find_one(self, *args, **kwargs):  # noqa: ARG002
        return None


class _FakeLockCollection:
    """Minimal stand-in for the real ``referral_invitee_locks`` collection,
    just enough to let referral_invitee_lock.claim()/release() succeed so
    the test can reach the pending-referral-creation failure path."""

    def __init__(self):
        self.docs = {}

    def find_one_and_update(self, filt, update, upsert=False, return_document=None):  # noqa: ARG002
        from pymongo.errors import DuplicateKeyError

        invitee = filt["invitee_user_id"]
        existing = self.docs.get(invitee)
        if existing is not None:
            raise DuplicateKeyError("duplicate")
        doc = dict(update.get("$set", {}))
        doc.update(update.get("$setOnInsert", {}))
        self.docs[invitee] = doc
        return doc

    def update_one(self, filt, update):
        invitee = filt.get("invitee_user_id")
        doc = self.docs.get(invitee)
        if doc is None:
            return
        if "inviter_user_id" in filt and doc.get("inviter_user_id") != filt["inviter_user_id"]:
            return
        doc.update(update.get("$set", {}))


class _FakeDb:
    """Fake ``db`` exposing both attribute-style access (for the
    historical-success lookups) and dict-style access (for
    referral_invitee_lock, which indexes collections by name)."""

    def __init__(self):
        self.qualified_events = _EmptyLookupCollection()
        self.referral_events = _EmptyLookupCollection()
        self.referral_award_events = _EmptyLookupCollection()
        self._collections = {"referral_invitee_locks": _FakeLockCollection()}

    def __getitem__(self, name):
        return self._collections[name]


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
            "db": _FakeDb(),
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
