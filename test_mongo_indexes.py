from pymongo.errors import OperationFailure, PyMongoError

from database import safe_create_index


class _FakeCollection:
    def __init__(self, fail=None):
        self.fail = fail
        self.calls = []
        self._index_info = {}
        self.name = "fake"

    def index_information(self):
        return self._index_info

    def create_index(self, keys, **kwargs):
        self.calls.append((keys, kwargs))
        if self.fail:
            raise self.fail
        self._index_info[kwargs["name"]] = {"key": list(keys)}
        return kwargs["name"]

    def list_indexes(self):
        for idx_name, spec in self._index_info.items():
            yield {"name": idx_name, "key": spec.get("key", [])}


def plan_index_specs(query_shapes: dict[str, bool]):
    specs = []
    if query_shapes.get("users_weekly_xp_sort"):
        specs.append(("users", [("weekly_xp", -1)], "users_weekly_xp_desc_idx"))
    if query_shapes.get("users_weekly_referrals_sort"):
        specs.append(("users", [("weekly_referrals", -1)], "users_weekly_referrals_desc_idx"))
    for field in ("pm1_due_at_utc", "pm2_due_at_utc", "pm3_due_at_utc", "pm4_due_at_utc", "mywin7_due_at_utc", "mywin14_due_at_utc"):
        if query_shapes.get(field):
            specs.append(("users", [(field, 1)], f"users_{field}_asc_idx"))
    if query_shapes.get("invite_lookup_chat_inviter_active_created"):
        specs.append(("invite_link_map", [("chat_id", 1), ("inviter_id", 1), ("is_active", 1), ("created_at", -1)], "invite_link_map_chat_inviter_active_created_desc_idx"))
    if query_shapes.get("invite_lookup_inviter_created"):
        specs.append(("invite_link_map", [("inviter_id", 1), ("created_at", -1)], "invite_link_map_inviter_created_desc_idx"))
    if query_shapes.get("miniapp_date"):
        specs.append(("miniapp_sessions_daily", [("date", 1)], "miniapp_sessions_daily_date_asc_idx"))
    elif query_shapes.get("miniapp_day"):
        specs.append(("miniapp_sessions_daily", [("day", 1)], "miniapp_sessions_daily_day_asc_idx"))
    elif query_shapes.get("miniapp_created_at"):
        specs.append(("miniapp_sessions_daily", [("created_at", 1)], "miniapp_sessions_daily_created_at_asc_idx"))
    return specs


def test_safe_create_index_operation_failure_returns_false():
    col = _FakeCollection(fail=OperationFailure("boom"))
    assert safe_create_index(col, [("weekly_xp", -1)], name="x") is False


def test_safe_create_index_pymongo_error_returns_false():
    col = _FakeCollection(fail=PyMongoError("boom"))
    assert safe_create_index(col, [("weekly_xp", -1)], name="x") is False


def test_safe_create_index_uses_explicit_name_and_idempotent():
    col = _FakeCollection()
    assert safe_create_index(col, [("weekly_xp", -1)], name="users_weekly_xp_desc_idx") == "users_weekly_xp_desc_idx"
    assert col.calls[0][1]["name"] == "users_weekly_xp_desc_idx"
    assert safe_create_index(col, [("weekly_xp", -1)], name="users_weekly_xp_desc_idx") == "users_weekly_xp_desc_idx"
    assert len(col.calls) == 1


def test_plan_indexes_only_from_matching_query_shapes():
    specs = plan_index_specs({"users_weekly_xp_sort": True, "invite_lookup_chat_inviter_active_created": True, "miniapp_day": True})
    names = {s[2] for s in specs}
    assert "users_weekly_xp_desc_idx" in names
    assert "users_weekly_referrals_desc_idx" not in names
    assert "invite_link_map_chat_inviter_active_created_desc_idx" in names
    assert "invite_link_map_inviter_created_desc_idx" not in names
    assert "miniapp_sessions_daily_day_asc_idx" in names
    assert "miniapp_sessions_daily_date_asc_idx" not in names
    assert "miniapp_sessions_daily_created_at_asc_idx" not in names


def test_due_time_indexes_only_for_used_fields():
    specs = plan_index_specs({"pm1_due_at_utc": True, "mywin14_due_at_utc": True})
    names = {s[2] for s in specs}
    assert "users_pm1_due_at_utc_asc_idx" in names
    assert "users_mywin14_due_at_utc_asc_idx" in names
    assert "users_pm2_due_at_utc_asc_idx" not in names


def test_safe_create_index_equivalent_name_conflict_returns_existing_name():
    col = _FakeCollection(fail=OperationFailure("exists", code=85))
    col._index_info["date_utc_1_user_id_1"] = {"key": [("date_utc", 1), ("user_id", 1)]}
    got = safe_create_index(col, [("date_utc", 1), ("user_id", 1)], name="miniapp_sessions_daily_date_utc_user_id_uidx")
    assert got == "date_utc_1_user_id_1"


def test_safe_create_index_conflict_without_equivalent_returns_false():
    col = _FakeCollection(fail=OperationFailure("exists", code=85))
    col._index_info["other"] = {"key": [("other_field", 1)]}
    got = safe_create_index(col, [("date_utc", 1), ("user_id", 1)], name="miniapp_sessions_daily_date_utc_user_id_uidx")
    assert got is False
