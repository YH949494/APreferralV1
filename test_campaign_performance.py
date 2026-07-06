"""Unit tests for the P4 Campaign Performance Intelligence layer
(campaign_performance.py).

Exercises volume/claim-rate aggregation for both single-drop (P2) and
batch (P3) campaigns, segment/quality breakdown, voucher-hunter share,
unknown-segment handling, campaign score calculation, and the compare
endpoint's underlying aggregation — all against an in-memory fake Mongo.
Also asserts the performance layer performs zero writes: only
find()/count_documents() calls are ever issued against the fake
collections it touches.
"""

import unittest
from datetime import datetime, timedelta, timezone

from bson.objectid import ObjectId

import database
import campaign_performance as cp


def _matches(doc: dict, filt: dict) -> bool:
    for key, cond in (filt or {}).items():
        if key == "$or":
            if not any(_matches(doc, sub) for sub in cond):
                return False
            continue
        val = doc.get(key)
        if isinstance(cond, dict) and any(k.startswith("$") for k in cond):
            for op, opval in cond.items():
                if op == "$ne":
                    if val == opval:
                        return False
                elif op == "$in":
                    if val not in opval:
                        return False
                elif op == "$nin":
                    if val in opval:
                        return False
                elif op == "$lte":
                    if val is None or val > opval:
                        return False
                elif op == "$lt":
                    if val is None or val >= opval:
                        return False
                elif op == "$gt":
                    if val is None or val <= opval:
                        return False
                elif op == "$gte":
                    if val is None or val < opval:
                        return False
                elif op == "$exists":
                    if bool(opval) != (key in doc):
                        return False
                else:
                    return False
        else:
            if val != cond:
                return False
    return True


class FakeInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class FakeCollection:
    def __init__(self, name="col"):
        self.name = name
        self.docs: list[dict] = []
        self.write_calls: list[str] = []

    def create_index(self, *a, **k):
        return "ix"

    def insert_one(self, doc):
        self.write_calls.append("insert_one")
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = ObjectId()
        self.docs.append(doc)
        return FakeInsertResult(doc["_id"])

    def find_one(self, filt=None, sort=None, projection=None):
        results = [d for d in self.docs if _matches(d, filt or {})]
        if sort:
            for key, direction in reversed(sort):
                results.sort(key=lambda d: (d.get(key) is None, d.get(key)), reverse=(direction == -1))
        return dict(results[0]) if results else None

    def find(self, filt=None, sort=None, projection=None, limit=None):
        results = [dict(d) for d in self.docs if _matches(d, filt or {})]
        if sort:
            for key, direction in reversed(sort):
                results.sort(key=lambda d: (d.get(key) is None, d.get(key)), reverse=(direction == -1))
        if limit:
            results = results[:limit]
        return results

    def count_documents(self, filt=None):
        return len([d for d in self.docs if _matches(d, filt or {})])

    def update_one(self, *a, **k):
        self.write_calls.append("update_one")
        raise AssertionError(f"unexpected write to {self.name}: update_one")

    def update_many(self, *a, **k):
        self.write_calls.append("update_many")
        raise AssertionError(f"unexpected write to {self.name}: update_many")

    def delete_one(self, *a, **k):
        self.write_calls.append("delete_one")
        raise AssertionError(f"unexpected write to {self.name}: delete_one")

    def find_one_and_update(self, *a, **k):
        self.write_calls.append("find_one_and_update")
        raise AssertionError(f"unexpected write to {self.name}: find_one_and_update")


class FakeDB:
    def __init__(self):
        self._cols: dict[str, FakeCollection] = {}

    def __getitem__(self, name):
        if name not in self._cols:
            self._cols[name] = FakeCollection(name)
        return self._cols[name]

    def __getattr__(self, name):
        return self[name]


class CampaignPerformanceTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = FakeDB()
        self._orig_db = database._db
        database._db = self.fake_db
        self.now = datetime.now(timezone.utc)

    def tearDown(self):
        database._db = self._orig_db

    # -- fixture helpers -----------------------------------------------

    def _make_campaign(self, **overrides):
        doc = {
            "_id": ObjectId(),
            "campaign_name": "Weekend Reload",
            "campaign_type": "public",
            "status": "active",
            "created_at": self.now - timedelta(days=1),
        }
        doc.update(overrides)
        self.fake_db["campaign_builder_campaigns"].insert_one(doc)
        return doc

    def _make_drop(self, campaign_id, *, drop_type="pooled", status="active", **overrides):
        doc = {
            "_id": ObjectId(),
            "campaign_id": str(campaign_id),
            "name": "drop",
            "type": drop_type,
            "status": status,
            "startsAt": self.now - timedelta(hours=2),
        }
        doc.update(overrides)
        self.fake_db["drops"].insert_one(doc)
        return doc

    def _make_vouchers(self, drop_id, count, claimed_count, drop_type="pooled"):
        for i in range(count):
            self.fake_db["vouchers"].insert_one({
                "dropId": str(drop_id),
                "type": drop_type,
                "code": f"CODE{i:04d}",
                "status": "claimed" if i < claimed_count else "free",
            })

    def _make_user(self, user_id, segment):
        self.fake_db["users"].insert_one({"user_id": user_id, "for_bot_segment": segment})

    def _make_claim(self, drop_id, user_id, *, minutes_after_release=10, subnet="s1", suspicious=False):
        self.fake_db["voucher_claims"].insert_one({
            "drop_id": drop_id,
            "user_id": user_id,
            "status": "claimed",
            "claimed_at": self.now - timedelta(hours=2) + timedelta(minutes=minutes_after_release),
            "claim_subnet": subnet,
            "public_pool_subnet_pressure": suspicious,
        })

    # -- 1. single-drop campaign aggregates correctly --------------------

    def test_single_drop_campaign_aggregates_correctly(self):
        campaign = self._make_campaign()
        drop = self._make_drop(campaign["_id"])
        self._make_vouchers(drop["_id"], count=10, claimed_count=4)
        self._make_user(1, "high_value")
        self._make_claim(drop["_id"], 1)

        result = cp.compute_campaign_performance(campaign)
        self.assertEqual(result["volume"]["total_vouchers"], 10)
        self.assertEqual(result["volume"]["total_released"], 10)
        self.assertEqual(result["volume"]["total_claimed"], 4)
        self.assertEqual(result["volume"]["total_remaining"], 6)

    # -- 2. batch campaign aggregates child drops correctly --------------

    def test_batch_campaign_aggregates_child_drops(self):
        campaign = self._make_campaign(release_type="hourly", batch_status="active")
        d1 = self._make_drop(campaign["_id"], batch_index=1, batch_status="released", batch_actual_release_at=self.now - timedelta(hours=3))
        d2 = self._make_drop(campaign["_id"], batch_index=2, batch_status="scheduled", status="paused")
        self._make_vouchers(d1["_id"], count=50, claimed_count=20)
        self._make_vouchers(d2["_id"], count=50, claimed_count=0)

        result = cp.compute_campaign_performance(campaign)
        # Only batch #1 has actually been released; batch #2 is still scheduled.
        self.assertEqual(result["volume"]["total_vouchers"], 100)
        self.assertEqual(result["volume"]["total_released"], 50)
        self.assertEqual(result["volume"]["total_claimed"], 20)
        self.assertEqual(len(result["child_drops"]), 2)
        batch1_row = next(r for r in result["child_drops"] if r["batch_index"] == 1)
        self.assertEqual(batch1_row["total_codes"], 50)
        self.assertEqual(batch1_row["claimed"], 20)

    # -- 3. claim rate calculation ---------------------------------------

    def test_claim_rate_calculation(self):
        campaign = self._make_campaign()
        drop = self._make_drop(campaign["_id"])
        self._make_vouchers(drop["_id"], count=200, claimed_count=50)

        result = cp.compute_campaign_performance(campaign)
        self.assertEqual(result["volume"]["claim_rate"], 25.0)
        self.assertEqual(result["volume"]["release_completion_pct"], 100.0)

    # -- 4. segment breakdown ---------------------------------------------

    def test_segment_breakdown(self):
        campaign = self._make_campaign()
        drop = self._make_drop(campaign["_id"])
        self._make_vouchers(drop["_id"], count=10, claimed_count=4)
        self._make_user(1, "high_value")
        self._make_user(2, "normal_actual")
        self._make_user(3, "low_value")
        self._make_user(4, "voucher_hunter")
        self._make_claim(drop["_id"], 1)
        self._make_claim(drop["_id"], 2)
        self._make_claim(drop["_id"], 3)
        self._make_claim(drop["_id"], 4)

        result = cp.compute_campaign_performance(campaign)
        q = result["quality"]
        self.assertEqual(q["high_value"], 1)
        self.assertEqual(q["normal_actual"], 1)
        self.assertEqual(q["low_value"], 1)
        self.assertEqual(q["voucher_hunter"], 1)
        self.assertEqual(q["ghost"], 0)
        self.assertEqual(q["unknown"], 0)

    # -- 5. voucher hunter percentage --------------------------------------

    def test_voucher_hunter_percentage(self):
        campaign = self._make_campaign()
        drop = self._make_drop(campaign["_id"])
        self._make_vouchers(drop["_id"], count=10, claimed_count=4)
        self._make_user(1, "voucher_hunter")
        self._make_user(2, "voucher_hunter")
        self._make_user(3, "high_value")
        self._make_user(4, "normal_actual")
        for uid in (1, 2, 3, 4):
            self._make_claim(drop["_id"], uid)

        result = cp.compute_campaign_performance(campaign)
        self.assertEqual(result["abuse_risk"]["voucher_hunter_claim_share_pct"], 50.0)

    # -- 6. unknown segment handling ----------------------------------------

    def test_unknown_segment_handling(self):
        campaign = self._make_campaign()
        drop = self._make_drop(campaign["_id"])
        self._make_vouchers(drop["_id"], count=10, claimed_count=1)
        # No user doc at all for user_id=99 -> must resolve to "unknown"
        # with an explicit reason, never invented data.
        self._make_claim(drop["_id"], 99)

        result = cp.compute_campaign_performance(campaign)
        self.assertEqual(result["quality"]["unknown"], 1)
        self.assertEqual(result["quality"]["unknown_reason"], "missing_user_segment")

    # -- 7. campaign score calculation ---------------------------------------

    def test_campaign_score_calculation(self):
        quality = {"high_value": 2, "normal_actual": 1, "low_value": 1, "voucher_hunter": 1, "ghost": 1, "unknown": 0}
        abuse = {"suspicious_claims": 1}
        conversion = {"qualified_after_claim": 1, "referral_after_claim": 1, "checkin_after_claim": 1}
        score = cp.compute_campaign_score(quality, abuse, conversion)
        # quality_score = 2*5 + 1*3 + 1*1 - 1*3 - 1*1 = 10+3+1-3-1 = 10
        # abuse_penalty = 1*5 = 5
        # conversion_bonus = 1*4 + 1*2 + 1*1 = 7
        # campaign_score = 10 - 5 + 7 = 12
        self.assertEqual(score["score_breakdown"]["quality_score"], 10)
        self.assertEqual(score["score_breakdown"]["abuse_penalty"], 5)
        self.assertEqual(score["score_breakdown"]["conversion_bonus"], 7)
        self.assertEqual(score["campaign_score"], 12)
        self.assertEqual(score["badge"], "Neutral")

    # -- 8. compare endpoint aggregation --------------------------------------

    def test_compare_endpoint_aggregation(self):
        c1 = self._make_campaign(campaign_name="Campaign A")
        d1 = self._make_drop(c1["_id"])
        self._make_vouchers(d1["_id"], count=10, claimed_count=5)

        c2 = self._make_campaign(campaign_name="Campaign B")
        d2 = self._make_drop(c2["_id"])
        self._make_vouchers(d2["_id"], count=20, claimed_count=2)

        with self._app_request_context(f"campaign_ids={c1['_id']},{c2['_id']}"):
            resp = cp.performance_compare()
        body = resp[0].get_json() if isinstance(resp, tuple) else resp.get_json()
        self.assertEqual(len(body["campaigns"]), 2)
        names = {row["campaign_name"] for row in body["campaigns"]}
        self.assertEqual(names, {"Campaign A", "Campaign B"})

    def _app_request_context(self, query_string):
        import flask
        app = flask.Flask(__name__)
        ctx = app.test_request_context(f"/api/admin/campaign-builder/performance/compare?{query_string}")

        class _Ctx:
            def __enter__(self_inner):
                ctx.push()
                import vouchers
                self._orig_require_admin = vouchers.require_admin
                vouchers.require_admin = lambda: ({"usernameLower": "admin"}, None)
                return None

            def __exit__(self_inner, *exc):
                import vouchers
                vouchers.require_admin = self._orig_require_admin
                ctx.pop()

        return _Ctx()

    # -- 9. no writes performed by performance endpoints ------------------

    def test_no_writes_performed(self):
        campaign = self._make_campaign(release_type="daily", batch_status="active")
        drop = self._make_drop(campaign["_id"], batch_index=1, batch_status="released", batch_actual_release_at=self.now - timedelta(hours=1))
        self._make_vouchers(drop["_id"], count=10, claimed_count=3)
        self._make_user(1, "high_value")
        self._make_claim(drop["_id"], 1)

        for col in self.fake_db._cols.values():
            col.write_calls = []

        cp.compute_campaign_performance(campaign)

        for name, col in self.fake_db._cols.items():
            self.assertEqual(col.write_calls, [], f"unexpected write calls on {name}: {col.write_calls}")


if __name__ == "__main__":
    unittest.main()
