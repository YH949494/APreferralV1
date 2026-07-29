import ast
import math
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SOURCE = Path("vouchers.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def load_rejoin_function():
    node = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == "check_rejoin_buffer_for_pooled_claim")
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {
        "datetime": datetime,
        "timezone": timezone,
        "math": math,
        "_safe_log": lambda *args, **kwargs: None,
        "_as_aware_utc": lambda value: value,
        "get_rejoin_buffer_settings": lambda: {"mode": "enabled", "hours": 12, "test_user_ids": []},
        "REJOIN_BUFFER_MODE_DISABLED": "disabled",
        "REJOIN_BUFFER_MODE_TEST_USERS_ONLY": "test_users_only",
        "REJOIN_BUFFER_MODE_ENABLED": "enabled",
    }
    exec(compile(module, "vouchers.py", "exec"), namespace)
    return namespace


class Users:
    def __init__(self, until):
        self.until = until

    def find_one(self, *_args, **_kwargs):
        return {"rejoin_buffer_until": self.until}


class RejoinResponseContractTests(unittest.TestCase):
    def test_active_buffer_has_stable_machine_readable_fields(self):
        now = datetime(2026, 7, 29, tzinfo=timezone.utc)
        until = now + timedelta(seconds=3661)
        namespace = load_rejoin_function()
        namespace["users_collection"] = Users(until)
        result = namespace["check_rejoin_buffer_for_pooled_claim"](8413241236, now)
        self.assertEqual(result["code"], "rejoin_buffer_active")
        self.assertEqual(result["reason"], "rejoin_buffer_active")
        self.assertEqual(result["retry_after_sec"], 3661)
        self.assertEqual(result["buffer_until"], until.isoformat())

    def test_preflight_and_claim_share_block_before_check_only_success(self):
        rejoin_branch = SOURCE.index("if is_public_pool(voucher):", SOURCE.index("def api_claim"))
        check_only_success = SOURCE.index("if check_only:", rejoin_branch)
        self.assertLess(rejoin_branch, check_only_success)
        endpoint = SOURCE[rejoin_branch:check_only_success]
        for fragment in (
            '"status": "blocked"',
            '"code": "rejoin_buffer_active"',
            '"reason": "rejoin_buffer_active"',
            '"retry_after_sec": retry_after_sec',
            '"buffer_until": buffer_until',
            "[CLAIM_UI][REJOIN_BUFFER]",
        ):
            self.assertIn(fragment, endpoint)


if __name__ == "__main__":
    unittest.main()
