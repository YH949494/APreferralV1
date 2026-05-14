import ast
from datetime import datetime, timezone, timedelta
from pathlib import Path
from referral_rules import build_public_referral_status


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


def _inject_unified_stubs(env):
    now = datetime.now(timezone.utc)
    env.update(
        {
            "users_collection": type("Users", (), {"find_one": lambda self, q, p: {"user_id": q.get("user_id")}})(),
            "resolve_referral_counts_with_snapshot_fallback": lambda uid, doc, now_ref: {
                "stats": {"total_referrals": 0, "weekly_referrals": 0, "monthly_referrals": 0},
                "source": "snapshot",
                "snapshot_updated_at": now,
                "snapshot_age_sec": 0,
            },
            "calc_referral_progress": lambda total_referrals, milestone_size=3: {
                "progress": 0,
                "remaining": 3,
                "progress_pct": 0,
                "near_miss": False,
            },
            "REFERRAL_BONUS_INTERVAL": 3,
            "REFERRAL_BONUS_XP": 400,
            "REFERRAL_XP_PER_SUCCESS": 60,
            "build_public_referral_status": build_public_referral_status,
            "logger": type("L", (), {"info": lambda *args, **kwargs: None})(),
        }
    )


def test_auth_failure_returns_401():
    env = _load_symbols()
    _inject_unified_stubs(env)
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
    _inject_unified_stubs(env)
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
    _inject_unified_stubs(env)
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
    assert out["referrals"][0]["status_label"] == "Qualified"


def test_qualified_event_overrides_stale_pending():
    env = _load_symbols()
    _inject_unified_stubs(env)
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
    _inject_unified_stubs(env)
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
    assert out["referrals"][0]["status_label"] == "Not eligible"


def test_specific_public_reason_mappings():
    fn = build_public_referral_status
    assert fn({"status": "failed", "revoked_reason": "not_subscribed_channel"})["label"] == "Not subscribed"
    assert fn({"status": "failed", "revoked_reason": "left_channel"})["label"] == "Left channel"
    assert fn({"status": "failed", "revoked_reason": "left_group"})["label"] == "Left group"
    assert fn({"status": "failed", "revoked_reason": "abuse"})["label"] == "Not eligible"
    assert fn({"status": "failed", "revoked_reason": "something_new"})["label"] == "Not eligible"
    assert fn({"status": "qualified"})["label"] == "Qualified"


def test_vouchers_and_main_mapping_consistency_matrix():
    env = _load_symbols()
    _inject_unified_stubs(env)
    now = datetime.now(timezone.utc)
    cases = [
        {"status": "pending", "revoked_reason": None, "q": False, "r": False, "label": "Checking"},
        {"status": "pending", "revoked_reason": None, "q": True, "r": False, "label": "Qualified"},
        {"status": "pending", "revoked_reason": None, "q": False, "r": True, "label": "Not eligible"},
        {"status": "failed", "revoked_reason": "not_subscribed_channel", "q": False, "r": False, "label": "Not subscribed"},
        {"status": "failed", "revoked_reason": "left_channel", "q": False, "r": False, "label": "Left channel"},
        {"status": "failed", "revoked_reason": "left_group", "q": False, "r": False, "label": "Left group"},
        {"status": "failed", "revoked_reason": "abuse", "q": False, "r": False, "label": "Not eligible"},
        {"status": "failed", "revoked_reason": "new_reason", "q": False, "r": False, "label": "Not eligible"},
    ]
    for idx, case in enumerate(cases, start=1):
        invitee_id = idx
        env.update(
            {
                "pending_referrals_collection": _Collection([
                    {"inviter_user_id": 1, "invitee_user_id": invitee_id, "status": case["status"], "revoked_reason": case["revoked_reason"], "created_at_utc": now}
                ]),
                "qualified_events_collection": _Collection(
                    [{"referrer_id": 1, "invitee_id": invitee_id}] if case["q"] else []
                ),
                "referral_events_collection": _Collection(
                    [{"inviter_id": 1, "invitee_id": invitee_id, "event": "referral_revoked"}] if case["r"] else []
                ),
            }
        )
        out = env["_build_referral_status_payload"](1, now)
        row = out["referrals"][0]
        assert row["status_label"] == case["label"]


def test_cap_latest_50_and_excludes_internal_fields():
    env = _load_symbols()
    _inject_unified_stubs(env)
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
    _inject_unified_stubs(env)
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


def test_api_ignores_user_id_override_uses_init_data_user():
    env = _load_symbols()
    called = {}
    env.update(
        {
            "request": type("Req", (), {"args": {"user_id": "999"}, "headers": {}})(),
            "extract_raw_init_data_from_query": lambda req: "ok",
            "verify_telegram_init_data": lambda _: (True, {"user": {"id": 123}}, None),
            "_build_referral_status_payload": lambda uid, now: (called.setdefault("uid", uid), {"ok": True, "referrals": [], "hold_hours": 12})[1],
            "jsonify": lambda payload: payload,
        }
    )
    out = env["api_referral_status"]()
    assert out["ok"] is True
    assert called["uid"] == 123


def test_build_payload_includes_unified_progress_fields():
    env = _load_symbols()
    now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
    env.update(
        {
            "pending_referrals_collection": _Collection([]),
            "qualified_events_collection": _Collection([]),
            "referral_events_collection": _Collection([]),
            "users_collection": type("Users", (), {"find_one": lambda self, q, p: {"user_id": 1}})(),
            "resolve_referral_counts_with_snapshot_fallback": lambda uid, doc, now_ref: {
                "stats": {"total_referrals": 2, "weekly_referrals": 1, "monthly_referrals": 2},
                "source": "snapshot",
                "snapshot_updated_at": now,
                "snapshot_age_sec": 60,
            },
            "calc_referral_progress": lambda total_referrals, milestone_size=3: {
                "progress": 2,
                "remaining": 1,
                "progress_pct": (2 / milestone_size) * 100,
                "near_miss": True,
            },
            "REFERRAL_BONUS_INTERVAL": 3,
            "REFERRAL_BONUS_XP": 400,
            "REFERRAL_XP_PER_SUCCESS": 60,
            "logger": type("L", (), {"info": lambda *args, **kwargs: None})(),
        }
    )
    out = env["_build_referral_status_payload"](1, now)
    assert out["total_referrals"] == 2
    assert out["weekly_referrals"] == 1
    assert out["monthly_referrals"] == 2
    assert out["progress"] == 2
    assert out["remaining"] == 1
    assert out["near_miss"] is True
    assert out["bonus_interval"] == 3
    assert out["base_referral_xp"] == 60
