"""Two narrow regression checks that don't fit test_subscription_audit_cache_policy.py:

1. The subscription-audit scheduler registration in main.py is a recurring
   small-batch interval trigger, not the old once-weekly CronTrigger, and
   there is exactly one job registration for it (no leftover second
   schedule left active by accident).
2. The cache read used on the Mini App's normal voucher-visibility path
   (vouchers.get_cached_subscription, called from user_visible_drops for
   the per-drop channelSubscribed hint) never calls out to Telegram —
   it's a DB read only, even when Telegram itself is unreachable/erroring.
"""

import os
import re
import unittest.mock as mock

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("BOT_TOKEN", "123:ABC")
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret")

import mongomock

import database


def test_subscription_audit_scheduler_registration_is_recurring_interval():
    with open("main.py", "r", encoding="utf-8") as fh:
        source = fh.read()

    # Exactly one add_job(...) block referencing the subscription-audit job.
    job_blocks = re.findall(r'scheduler\.add_job\(\s*_guarded_job\("subscription_audit".*?\)\s*,.*?\n\s*\)', source, re.DOTALL)
    assert len(job_blocks) == 1, f"expected exactly one subscription_audit job registration, found {len(job_blocks)}"

    block = job_blocks[0]
    assert 'trigger="interval"' in block, "subscription_audit job must use a recurring interval trigger, not CronTrigger"
    assert "CronTrigger" not in block, "subscription_audit job must not be a once-a-week CronTrigger any more"
    assert "SUB_AUDIT_CADENCE_MINUTES" in block, "interval minutes must come from SUB_AUDIT_CADENCE_MINUTES, not a hardcoded weekly value"

    # The old weekly job's id must not still be registered anywhere (a
    # mention in an explanatory comment, like this test file's own docstring
    # references, doesn't count — only an actual id= assignment does).
    assert 'id="subscription_audit_weekly"' not in source
    assert '"subscription_audit_batch"' in source


def test_vouchers_visible_cache_read_never_calls_telegram():
    database._client = mongomock.MongoClient()
    database._db = database._client["referral_bot"]
    import vouchers

    uid = 424242

    with mock.patch.object(vouchers.requests, "get", side_effect=AssertionError("must not call Telegram")) as mocked_get:
        # Cache miss (no doc at all) — the exact state on a fresh Mini App load.
        assert vouchers.get_cached_subscription(uid) is None

        # Cache present but expired — still must not fall back to a live call;
        # that's exactly what a claim-time (not visibility-time) check is for.
        vouchers.subscription_cache_col.insert_one({
            "_id": f"sub:{uid}",
            "user_id": uid,
            "subscribed": True,
            "expireAt": vouchers.now_utc() - vouchers.timedelta(seconds=1),
        })
        assert vouchers.get_cached_subscription(uid) is None

        mocked_get.assert_not_called()
