"""Unit tests for the P5 Campaign Intelligence & Automation layer
(campaign_intelligence.py).

Covers: campaign ranking, recommendation engine, segment ranking/matrix,
template ranking, release-strategy ranking, best-time calculation,
playbook generation, insight generation, the read-only guarantee, and a
P2/P3/P4 regression smoke check (nothing in this module can touch claim
logic, so these tests only ever call find()/count_documents() against a
fake Mongo).
"""

import unittest
from datetime import datetime, timedelta, timezone

from bson.objectid import ObjectId

import database
import campaign_intelligence as ci
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


class CampaignIntelligenceTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = FakeDB()
        self._orig_db = database._db
        database._db = self.fake_db
        self.now = datetime.now(timezone.utc)
        CampaignIntelligenceTests._next_user_id = 1

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

    _next_user_id = 1

    def _make_full_campaign(self, *, campaign_name="Weekend Reload", campaign_type="public",
                             release_type=None, high_value=1, normal_actual=1, low_value=0,
                             voucher_hunter=0, ghost=0, vouchers=10, claimed=None):
        overrides = {"campaign_name": campaign_name, "campaign_type": campaign_type}
        if release_type:
            overrides["release_type"] = release_type
            overrides["batch_status"] = "active"
        campaign = self._make_campaign(**overrides)
        drop = self._make_drop(campaign["_id"], batch_index=1 if release_type else None,
                                batch_status="released" if release_type else None,
                                batch_actual_release_at=(self.now - timedelta(hours=2)) if release_type else None)
        segments = (["high_value"] * high_value + ["normal_actual"] * normal_actual +
                    ["low_value"] * low_value + ["voucher_hunter"] * voucher_hunter + ["ghost"] * ghost)
        claimed = claimed if claimed is not None else len(segments)
        self._make_vouchers(drop["_id"], count=vouchers, claimed_count=claimed)
        # user_ids must be unique across campaigns within a test — sharing an
        # id range would make one campaign's claimants resolve to another
        # campaign's segment assignment.
        start_id = CampaignIntelligenceTests._next_user_id
        CampaignIntelligenceTests._next_user_id += len(segments)
        for offset, seg in enumerate(segments):
            uid = start_id + offset
            self._make_user(uid, seg)
            self._make_claim(drop["_id"], uid, minutes_after_release=5 * (offset + 1))
        return campaign

    # -- 1. campaign ranking ------------------------------------------------

    def test_campaign_ranking_orders_by_ranking_score(self):
        self._make_full_campaign(campaign_name="Strong", high_value=8, normal_actual=2, vouchers=10, claimed=10)
        self._make_full_campaign(campaign_name="Weak", voucher_hunter=8, ghost=2, vouchers=10, claimed=10)

        rankings = ci.build_rankings()
        self.assertEqual(len(rankings), 2)
        self.assertEqual(rankings[0]["rank"], 1)
        self.assertEqual(rankings[0]["campaign_name"], "Strong")
        self.assertGreater(rankings[0]["ranking_score"], rankings[1]["ranking_score"])

    # -- 2. recommendation engine --------------------------------------------

    def test_recommendation_engine_flags_voucher_hunter_heavy_campaign(self):
        campaign = self._make_full_campaign(voucher_hunter=8, normal_actual=1, high_value=1, vouchers=10, claimed=10)
        full = ci.enrich_performance(cp.compute_campaign_performance(campaign))
        recs = ci.generate_recommendations(full)
        self.assertIn("reduce voucher count -20%", recs)

    def test_recommendation_engine_is_deterministic(self):
        campaign = self._make_full_campaign(high_value=5, normal_actual=5, vouchers=10, claimed=10)
        full = ci.enrich_performance(cp.compute_campaign_performance(campaign))
        recs1 = ci.generate_recommendations(full)
        recs2 = ci.generate_recommendations(full)
        self.assertEqual(recs1, recs2)

    # -- 3. segment ranking (per-campaign matrix) -----------------------------

    def test_segment_matrix_grades_by_conversion(self):
        campaign = self._make_full_campaign(high_value=1, voucher_hunter=1, vouchers=10, claimed=10)
        matrix = ci.segment_matrix(campaign)
        rows = {r["segment"]: r for r in matrix}
        self.assertEqual(rows["high_value"]["claimed"], 1)
        self.assertEqual(rows["voucher_hunter"]["claimed"], 1)
        self.assertEqual(rows["low_value"]["claimed"], 0)
        self.assertEqual(rows["low_value"]["score"], "N/A")

    def test_segment_recommendation_engine_always_avoids_voucher_hunter_and_ghost(self):
        self._make_full_campaign(high_value=5, normal_actual=5, voucher_hunter=2, ghost=1, vouchers=20, claimed=13)
        result = ci.segment_recommendations()
        self.assertIn("voucher_hunter", result["avoid_segments"])
        self.assertIn("ghost", result["avoid_segments"])
        self.assertNotIn("voucher_hunter", result["recommended_segments"])
        self.assertNotIn("ghost", result["recommended_segments"])

    # -- 4. template ranking --------------------------------------------------

    def test_template_ranking_aggregates_by_campaign_type(self):
        self._make_full_campaign(campaign_type="public", high_value=5, vouchers=10, claimed=10)
        self._make_full_campaign(campaign_type="welcome", low_value=5, vouchers=10, claimed=10)

        templates = ci.template_ranking()
        types = {t["template"] for t in templates}
        self.assertEqual(types, {"public", "welcome"})
        for t in templates:
            self.assertEqual(t["campaign_count"], 1)

    # -- 5. release strategy ranking -------------------------------------------

    def test_release_ranking_buckets_immediate_vs_batch(self):
        self._make_full_campaign(campaign_name="Single Drop", vouchers=10, claimed=5)
        self._make_full_campaign(campaign_name="Hourly Batch", release_type="hourly", vouchers=10, claimed=5)

        releases = ci.release_ranking()
        strategies = {r["release_strategy"] for r in releases}
        self.assertIn("immediate", strategies)
        self.assertIn("hourly", strategies)

    # -- 6. best time calculation -----------------------------------------------

    def test_best_time_calculation_returns_recommendation(self):
        campaign = self._make_full_campaign(high_value=5, vouchers=10, claimed=5)
        result = ci.best_time_to_launch()
        self.assertIn("recommendation", result)
        self.assertTrue(result["recommendation"])
        self.assertIsInstance(result["hours"], list)

    def test_best_time_calculation_handles_no_data(self):
        result = ci.best_time_to_launch()
        self.assertEqual(result["hours"], [])
        self.assertIn("Insufficient", result["recommendation"])

    # -- 7. playbook generation --------------------------------------------------

    def test_playbook_generation_produces_expected_fields(self):
        campaign = self._make_full_campaign(high_value=6, normal_actual=4, vouchers=10, claimed=10)
        full = ci.enrich_performance(cp.compute_campaign_performance(campaign))
        matrix = ci.segment_matrix(campaign)
        playbook = ci.generate_playbook(full, matrix)

        for field in ("template", "audience", "release", "voucher_count",
                      "expected_claim_rate_pct", "expected_abuse_pct", "confidence", "recommendations"):
            self.assertIn(field, playbook)
        self.assertEqual(playbook["template"], "public")
        self.assertIn(playbook["confidence"], ("High", "Medium", "Low"))

    def test_playbook_confidence_scales_with_volume(self):
        small = self._make_full_campaign(campaign_name="Small", high_value=2, vouchers=10, claimed=2)
        full_small = ci.enrich_performance(cp.compute_campaign_performance(small))
        matrix_small = ci.segment_matrix(small)
        playbook_small = ci.generate_playbook(full_small, matrix_small)
        self.assertEqual(playbook_small["confidence"], "Low")

    # -- 8. insight generation --------------------------------------------------

    def test_insight_generation_flags_high_voucher_hunter_participation(self):
        campaign = self._make_full_campaign(voucher_hunter=8, high_value=1, normal_actual=1, vouchers=10, claimed=10)
        full = ci.enrich_performance(cp.compute_campaign_performance(campaign))
        insights = ci.generate_insights(full)
        texts = {i["text"] for i in insights}
        self.assertIn("High voucher hunter participation", texts)

    def test_insight_generation_flags_good_actual_player_conversion(self):
        campaign = self._make_full_campaign(high_value=8, normal_actual=2, vouchers=10, claimed=10)
        full = ci.enrich_performance(cp.compute_campaign_performance(campaign))
        insights = ci.generate_insights(full)
        texts = {i["text"] for i in insights}
        self.assertIn("High actual-player conversion", texts)

    def test_insights_are_not_persisted_anywhere(self):
        campaign = self._make_full_campaign(high_value=5, normal_actual=5, vouchers=10, claimed=10)
        for col in self.fake_db._cols.values():
            col.write_calls = []
        full = ci.enrich_performance(cp.compute_campaign_performance(campaign))
        ci.generate_insights(full)
        for name, col in self.fake_db._cols.items():
            self.assertEqual(col.write_calls, [], f"unexpected write on {name}")

    # -- 9. read-only guarantee ----------------------------------------------

    def test_all_intelligence_functions_are_read_only(self):
        campaign = self._make_full_campaign(high_value=3, normal_actual=3, voucher_hunter=2, vouchers=15, claimed=8)
        for col in self.fake_db._cols.values():
            col.write_calls = []

        ci.build_rankings()
        ci.template_ranking()
        ci.release_ranking()
        ci.segment_recommendations()
        ci.best_time_to_launch()
        full = ci.enrich_performance(cp.compute_campaign_performance(campaign))
        matrix = ci.segment_matrix(campaign)
        ci.generate_insights(full)
        ci.generate_recommendations(full)
        ci.generate_playbook(full, matrix)

        for name, col in self.fake_db._cols.items():
            self.assertEqual(col.write_calls, [], f"unexpected write calls on {name}: {col.write_calls}")

    def test_no_mutating_http_methods_registered(self):
        for rule in ci.campaign_intelligence_bp.deferred_functions:
            pass
        import flask
        app = flask.Flask(__name__)
        app.register_blueprint(ci.campaign_intelligence_bp)
        for rule in app.url_map.iter_rules():
            if rule.endpoint == "static":
                continue
            methods = rule.methods - {"HEAD", "OPTIONS"}
            self.assertEqual(methods, {"GET"}, f"{rule} exposes non-GET methods: {methods}")

    # -- 10. P2/P3/P4 regression smoke check -------------------------------------

    def test_p4_performance_layer_unaffected_by_p5_import(self):
        campaign = self._make_full_campaign(high_value=2, normal_actual=1, vouchers=10, claimed=3)
        result = cp.compute_campaign_performance(campaign)
        self.assertIn("campaign_score", result)
        self.assertIn("volume", result)
        self.assertEqual(result["volume"]["total_vouchers"], 10)

    def test_campaign_builder_module_still_importable(self):
        import campaign_builder
        self.assertTrue(hasattr(campaign_builder, "campaign_builder_bp"))
        self.assertTrue(hasattr(campaign_builder, "RELEASE_TYPES"))

    def test_vouchers_claim_engine_untouched_by_p5_import(self):
        import vouchers
        self.assertTrue(hasattr(vouchers, "require_admin"))
        # P5 module must not import or reference claim-mutation entrypoints.
        import inspect
        source = inspect.getsource(ci)
        for forbidden in ("create_drop_from_spec", "reconcile_drop_statuses", "claim_voucher"):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
