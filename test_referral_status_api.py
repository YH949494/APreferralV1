import ast
from datetime import datetime, timezone, timedelta
from pathlib import Path


def _load_symbols():
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    wanted = {
        "REFERRAL_HOLD_HOURS",
        "_coerce_utc_datetime",
        "_map_referral_status",
        "_build_referral_status_payload",
        "api_referral_status",
    }
    body = []
    for node in module.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id in wanted:
                    body.append(node)
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            node.decorator_list = []
            body.append(node)
    isolated = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    env["datetime"] = datetime
    env["timezone"] = timezone
    env["json"] = __import__("json")
    return env


class _FindResult:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, key, direction):
        reverse = direction == -1
        self.docs = sorted(self.docs, key=lambda d: d.get(key) or datetime.min.replace(tzinfo=timezone.utc), reverse=reverse)
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def __iter__(self):
        return iter(self.docs)


class _Collection:
    def __init__(self, docs):
        self.docs = docs

    def find(self, query, projection=None):
        def _match(doc):
            for k, v in query.items():
                if isinstance(v, dict) and "$in" in v:
                    if doc.get(k) not in v["$in"]:
                        return False
                elif doc.get(k) != v:
                    return False
            return True

        matched = [d for d in self.docs if _match(d)]
        return _FindResult(matched)


class _Req:
    args = {}
    headers = {}


def test_auth_failure_returns_401():
    env = _load_symbols()
    fn = env["api_referral_status"]
    env.update(
        {
            "request": _Req(),
            "extract_raw_init_data_from_query": lambda req: None,
            "jsonify": lambda payload: payload,
        }
    )
    body, code = fn()
    assert code == 401
    assert body["ok"] is False


def test_pending_referral_shape_and_time_fields():
    env = _load_symbols()
    now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
    created = now - timedelta(hours=3, minutes=10)
    env.update(
        {
            "pending_referrals_collection": _Collection([
                {"inviter_user_id": 1, "invitee_user_id": 10, "invitee_username": "abc", "status": "pending", "created_at_utc": created}
            ]),
            "qualified_events_collection": _Collection([]),
            "referral_events_collection": _Collection([]),
        }
    )
    out = env["_build_referral_status_payload"](1, now)
    item = out["referrals"][0]
    assert item["status"] == "pending"
    assert item["age_hours"] == 3
    assert item["remaining_hold_hours"] == 9
    assert item["qualified_at"] is None


def test_awarded_pending_maps_qualified():
    env = _load_symbols()
    now = datetime.now(timezone.utc)
    env.update(
        {
            "pending_referrals_collection": _Collection([
                {"inviter_user_id": 1, "invitee_user_id": 10, "status": "awarded", "created_at_utc": now}
            ]),
            "qualified_events_collection": _Collection([]),
            "referral_events_collection": _Collection([]),
        }
    )
    out = env["_build_referral_status_payload"](1, now)
    assert out["referrals"][0]["status"] == "qualified"


def test_qualified_event_overrides_stale_pending():
    env = _load_symbols()
    now = datetime.now(timezone.utc)
    env.update(
        {
            "pending_referrals_collection": _Collection([
                {"inviter_user_id": 1, "invitee_user_id": 10, "status": "pending", "created_at_utc": now}
            ]),
            "qualified_events_collection": _Collection([
                {"referrer_id": 1, "invitee_id": 10}
            ]),
            "referral_events_collection": _Collection([]),
        }
    )
    out = env["_build_referral_status_payload"](1, now)
    assert out["referrals"][0]["status"] == "qualified"


def test_referral_revoked_maps_failed_without_qualified():
    env = _load_symbols()
    now = datetime.now(timezone.utc)
    env.update(
        {
            "pending_referrals_collection": _Collection([
                {"inviter_user_id": 1, "invitee_user_id": 10, "status": "pending", "created_at_utc": now}
            ]),
            "qualified_events_collection": _Collection([]),
            "referral_events_collection": _Collection([
                {"inviter_id": 1, "invitee_id": 10, "event": "referral_revoked"}
            ]),
        }
    )
    out = env["_build_referral_status_payload"](1, now)
    assert out["referrals"][0]["status"] == "failed"


def test_cap_latest_50_and_excludes_internal_fields():
    env = _load_symbols()
    now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
    docs = []
    for i in range(60):
        docs.append({
            "inviter_user_id": 1,
            "invitee_user_id": i,
            "status": "pending",
            "created_at_utc": now - timedelta(minutes=i),
            "invite_link": "secret",
            "award_key": "k",
            "_id": i,
        })
    env.update(
        {
            "pending_referrals_collection": _Collection(docs),
            "qualified_events_collection": _Collection([]),
            "referral_events_collection": _Collection([]),
        }
    )
    out = env["_build_referral_status_payload"](1, now)
    assert len(out["referrals"]) == 50
    sample = out["referrals"][0]
    assert "invite_link" not in sample
    assert "award_key" not in sample
    assert "_id" not in sample


def test_malformed_date_does_not_500():
    env = _load_symbols()
    now = datetime.now(timezone.utc)
    env.update(
        {
            "pending_referrals_collection": _Collection([
                {"inviter_user_id": 1, "invitee_user_id": 10, "status": "pending", "created_at_utc": "not-a-date"}
            ]),
            "qualified_events_collection": _Collection([]),
            "referral_events_collection": _Collection([]),
        }
    )
    out = env["_build_referral_status_payload"](1, now)
    assert out["referrals"][0]["created_at"] is None
    assert out["referrals"][0]["age_hours"] == 0
