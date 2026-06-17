"""Tests for Phase 2B — Marketing Data Validation & Raw Data Explorer.

Tests cover:
  - Summary card calculations (rows, distinct counts, totals, new players)
  - Campaign aggregation
  - Platform aggregation
  - Currency aggregation
  - Snapshot filtering (week, month, auto-latest)
  - Missing-data quality checks
  - Duplicate-data quality checks
  - Overall quality status rules (green/yellow/red)
  - Empty-collection edge cases

All tests use in-memory fake collections — no external dependencies.
"""

import sys
import types
import unittest
from datetime import datetime, timezone

# Stub out database (and its pymongo dependency) before importing the module
_db_stub = types.ModuleType("database")
_db_stub.init_db = lambda *a, **kw: None
_db_stub.marketing_raw_data_col = None
_db_stub.marketing_upload_batches_col = None
sys.modules.setdefault("database", _db_stub)

# Stub pymongo to allow marketing_explorer's optional `import database`
for _mod in ("pymongo", "pymongo.errors"):
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)

import marketing_explorer as ex  # noqa: E402


# ---------------------------------------------------------------------------
# Fake collection infrastructure
# ---------------------------------------------------------------------------

def _safe_float(v):
    try:
        return float(v) if v not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


class FakeMarketingCollection:
    """In-memory fake for marketing_raw_data that handles the aggregation
    pipeline shapes produced by marketing_explorer.py."""

    def __init__(self, docs=None):
        self.docs = [dict(d) for d in (docs or [])]

    def find_one(self, filt=None, sort=None, projection=None):
        matched = self._match_all(filt or {})
        if sort:
            field, direction = sort[0]
            matched = sorted(matched, key=lambda d: (d.get(field) is None, d.get(field)), reverse=(direction < 0))
        return matched[0] if matched else None

    def _match_val(self, doc_val, cond):
        if isinstance(cond, dict):
            for op, v in cond.items():
                if op == "$exists":
                    present = doc_val is not None
                    if bool(v) != present:
                        return False
                elif op == "$gt" and not (doc_val is not None and doc_val > v):
                    return False
            return True
        return doc_val == cond

    def _match(self, doc, filt):
        for key, cond in filt.items():
            if key == "$or":
                if not any(self._match(doc, sub) for sub in cond):
                    return False
            else:
                if not self._match_val(doc.get(key), cond):
                    return False
        return True

    def _match_all(self, filt):
        return [d for d in self.docs if self._match(d, filt)]

    def aggregate(self, pipeline):
        """Simulate the specific pipeline shapes used by marketing_explorer.py."""
        docs = list(self.docs)
        for stage in pipeline:
            if "$match" in stage:
                docs = self._match_all_with(docs, stage["$match"])
            elif "$facet" in stage:
                return iter([self._run_facet(docs, stage["$facet"])])
            elif "$group" in stage:
                docs = self._run_group(docs, stage["$group"])
            elif "$sort" in stage:
                for field, direction in reversed(list(stage["$sort"].items())):
                    docs = sorted(docs, key=lambda d: (d.get(field) is None, d.get(field)), reverse=(direction < 0))
            elif "$count" in stage:
                docs = [{stage["$count"]: len(docs)}]
        return iter(docs)

    def _match_all_with(self, docs, filt):
        return [d for d in docs if self._match(d, filt)]

    def _run_facet(self, docs, facet_spec):
        result = {}
        for name, sub_pipeline in facet_spec.items():
            sub_docs = list(docs)
            for stage in sub_pipeline:
                if "$match" in stage:
                    sub_docs = self._match_all_with(sub_docs, stage["$match"])
                elif "$group" in stage:
                    sub_docs = self._run_group(sub_docs, stage["$group"])
                elif "$count" in stage:
                    sub_docs = [{stage["$count"]: len(sub_docs)}]
                elif "$sort" in stage:
                    for field, direction in reversed(list(stage["$sort"].items())):
                        sub_docs = sorted(sub_docs, key=lambda d: (d.get(field) is None, d.get(field)), reverse=(direction < 0))
            result[name] = sub_docs
        return result

    def _eval_expr(self, doc, expr):
        """Evaluate a simple aggregation expression."""
        if isinstance(expr, dict):
            if "$convert" in expr:
                inp_spec = expr["$convert"].get("input")
                val = self._eval_expr(doc, inp_spec)
                try:
                    return float(val) if val not in (None, "") else 0.0
                except (TypeError, ValueError):
                    return expr["$convert"].get("onError", 0)
            if "$sum" in expr:
                inner = expr["$sum"]
                if isinstance(inner, (int, float)):
                    return inner
                if "$cond" in inner:
                    cond_list = inner["$cond"]
                    test, if_true, if_false = cond_list
                    ok = self._eval_cond(doc, test)
                    return if_true if ok else if_false
                return _safe_float(self._eval_expr(doc, inner))
            if "$subtract" in expr:
                a, b = expr["$subtract"]
                return self._eval_expr(doc, a) - self._eval_expr(doc, b)
            if "$in" in expr:
                val_expr, arr = expr["$in"]
                val = self._eval_expr(doc, val_expr)
                return val in arr
        if isinstance(expr, str) and expr.startswith("$"):
            field = expr[1:]
            # Handle nested _id references like "$_id.campaign_id"
            parts = field.split(".")
            v = doc
            for p in parts:
                v = v.get(p) if isinstance(v, dict) else None
            return v
        return expr

    def _eval_cond(self, doc, test):
        if isinstance(test, dict):
            if "$in" in test:
                val_expr, arr = test["$in"]
                val = self._eval_expr(doc, val_expr)
                return val in arr
            if "$or" in test:
                return any(self._eval_cond(doc, sub) for sub in test["$or"])
            if "$gt" in test:
                a, b = test["$gt"]
                return self._eval_expr(doc, a) > b
        return bool(test)

    def _build_group_key(self, doc, id_spec):
        if id_spec is None:
            return None
        if isinstance(id_spec, str) and id_spec.startswith("$"):
            return doc.get(id_spec[1:])
        if isinstance(id_spec, dict):
            return tuple(
                (k, self._eval_expr(doc, v)) for k, v in sorted(id_spec.items())
            )
        return id_spec

    def _run_group(self, docs, spec):
        id_spec = spec["_id"]
        buckets = {}
        bucket_keys = {}

        for doc in docs:
            key = self._build_group_key(doc, id_spec)
            key_hash = key if not isinstance(key, (list, dict)) else str(key)
            if key_hash not in buckets:
                if isinstance(id_spec, dict):
                    bucket_id = {k: self._eval_expr(doc, v) for k, v in id_spec.items()}
                else:
                    bucket_id = key
                buckets[key_hash] = {"_id": bucket_id}
                bucket_keys[key_hash] = key

            b = buckets[key_hash]
            for fld, agg_expr in spec.items():
                if fld == "_id":
                    continue
                if "$sum" in agg_expr:
                    inner = agg_expr["$sum"]
                    if isinstance(inner, (int, float)):
                        b[fld] = b.get(fld, 0) + inner
                    elif isinstance(inner, dict) and "$cond" in inner:
                        cond_list = inner["$cond"]
                        ok = self._eval_cond(doc, cond_list[0])
                        b[fld] = b.get(fld, 0) + (cond_list[1] if ok else cond_list[2])
                    elif isinstance(inner, dict) and "$subtract" in inner:
                        val = self._eval_expr(doc, inner)
                        b[fld] = b.get(fld, 0) + val
                    elif isinstance(inner, dict) and "$convert" in inner:
                        val = self._eval_expr(doc, inner)
                        b[fld] = b.get(fld, 0) + (val or 0)
                    else:
                        val = self._eval_expr(doc, inner)
                        b[fld] = b.get(fld, 0) + _safe_float(val)

        return list(buckets.values())


class FakeBatchesCollection:
    def __init__(self, docs=None):
        self.docs = [dict(d) for d in (docs or [])]

    def find(self, filt=None):
        return _FakeCursor(self.docs)

    def insert_one(self, doc):
        self.docs.append(dict(doc))


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, field, direction=1):
        self._docs = sorted(
            self._docs,
            key=lambda d: (d.get(field) is None, d.get(field)),
            reverse=(direction < 0),
        )
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

NOW = datetime(2024, 5, 15, tzinfo=timezone.utc)  # ISO week 2024-W20


def _doc(**kwargs):
    defaults = {
        "account": "acc1",
        "campaign_id": "c1",
        "campaign_name": "Camp One",
        "platform_code": "web",
        "currency_code": "MYR",
        "withdraw_amount": "100",
        "after_total_bet_amount": "200",
        "is_new_player": 0,
        "snapshot_week": "2024-W20",
        "snapshot_month": "2024-05",
    }
    defaults.update(kwargs)
    return defaults


# ---------------------------------------------------------------------------
# Summary card tests
# ---------------------------------------------------------------------------

class SummaryTests(unittest.TestCase):
    def _explorer(self, docs, **kwargs):
        col = FakeMarketingCollection(docs)
        batches = FakeBatchesCollection()
        return ex.get_raw_explorer(marketing_col=col, batches_col=batches, **kwargs)

    def test_rows_total(self):
        docs = [_doc(account="a"), _doc(account="b")]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["rows_total"], 2)

    def test_distinct_accounts(self):
        docs = [_doc(account="a"), _doc(account="b"), _doc(account="a", campaign_id="c2")]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["distinct_accounts"], 2)

    def test_campaign_count(self):
        docs = [_doc(campaign_id="c1"), _doc(campaign_id="c2"), _doc(campaign_id="c1")]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["campaign_count"], 2)

    def test_platform_count(self):
        docs = [_doc(platform_code="web"), _doc(platform_code="mobile"), _doc(platform_code="web")]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["platform_count"], 2)

    def test_currency_count(self):
        docs = [_doc(currency_code="MYR"), _doc(currency_code="USD"), _doc(currency_code="MYR")]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["currency_count"], 2)

    def test_new_players_integer_1(self):
        docs = [_doc(is_new_player=1), _doc(is_new_player=0), _doc(is_new_player=1)]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["new_players"], 2)

    def test_new_players_string_1(self):
        docs = [_doc(is_new_player="1"), _doc(is_new_player="0")]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["new_players"], 1)

    def test_new_players_bool_true(self):
        docs = [_doc(is_new_player=True), _doc(is_new_player=False)]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["new_players"], 1)

    def test_total_withdraw_amount(self):
        docs = [_doc(withdraw_amount="50"), _doc(withdraw_amount="150")]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertAlmostEqual(r["summary"]["total_withdraw_amount"], 200.0)

    def test_total_after_bet_amount(self):
        docs = [_doc(after_total_bet_amount="300"), _doc(after_total_bet_amount="700")]
        r = self._explorer(docs, snapshot_week="2024-W20")
        self.assertAlmostEqual(r["summary"]["total_after_bet_amount"], 1000.0)

    def test_empty_collection_returns_zero_summary(self):
        r = self._explorer([], snapshot_week="2024-W20")
        s = r["summary"]
        self.assertEqual(s["rows_total"], 0)
        self.assertEqual(s["distinct_accounts"], 0)
        self.assertEqual(s["total_withdraw_amount"], 0.0)


# ---------------------------------------------------------------------------
# Campaign aggregation tests
# ---------------------------------------------------------------------------

class CampaignBreakdownTests(unittest.TestCase):
    def _explorer(self, docs, **kwargs):
        col = FakeMarketingCollection(docs)
        batches = FakeBatchesCollection()
        return ex.get_raw_explorer(marketing_col=col, batches_col=batches, **kwargs)

    def test_campaign_rows_and_accounts(self):
        docs = [
            _doc(account="a", campaign_id="c1", campaign_name="Alpha", after_total_bet_amount="400"),
            _doc(account="b", campaign_id="c1", campaign_name="Alpha", after_total_bet_amount="600"),
            _doc(account="a", campaign_id="c2", campaign_name="Beta", after_total_bet_amount="100"),
        ]
        r = self._explorer(docs, snapshot_week="2024-W20")
        by_id = {c["campaign_id"]: c for c in r["campaign_breakdown"]}
        self.assertEqual(by_id["c1"]["rows"], 2)
        self.assertEqual(by_id["c1"]["accounts"], 2)
        self.assertAlmostEqual(by_id["c1"]["after_total_bet_amount"], 1000.0)
        self.assertEqual(by_id["c2"]["rows"], 1)
        self.assertEqual(by_id["c2"]["accounts"], 1)

    def test_campaign_breakdown_sorted_by_after_bet_desc(self):
        docs = [
            _doc(campaign_id="c1", campaign_name="Low", after_total_bet_amount="100"),
            _doc(campaign_id="c2", campaign_name="High", after_total_bet_amount="900"),
        ]
        r = self._explorer(docs, snapshot_week="2024-W20")
        breakdown = r["campaign_breakdown"]
        self.assertGreaterEqual(
            breakdown[0]["after_total_bet_amount"],
            breakdown[-1]["after_total_bet_amount"],
        )

    def test_campaign_accounts_counts_distinct(self):
        docs = [
            _doc(account="a", campaign_id="c1", after_total_bet_amount="50"),
            _doc(account="a", campaign_id="c1", after_total_bet_amount="50"),  # same account, same campaign
        ]
        r = self._explorer(docs, snapshot_week="2024-W20")
        by_id = {c["campaign_id"]: c for c in r["campaign_breakdown"]}
        self.assertEqual(by_id["c1"]["accounts"], 1)
        self.assertEqual(by_id["c1"]["rows"], 2)

    def test_empty_collection_returns_empty_campaign_list(self):
        r = self._explorer([], snapshot_week="2024-W20")
        self.assertEqual(r["campaign_breakdown"], [])


# ---------------------------------------------------------------------------
# Platform aggregation tests
# ---------------------------------------------------------------------------

class PlatformBreakdownTests(unittest.TestCase):
    def _explorer(self, docs, **kwargs):
        col = FakeMarketingCollection(docs)
        batches = FakeBatchesCollection()
        return ex.get_raw_explorer(marketing_col=col, batches_col=batches, **kwargs)

    def test_platform_rows_and_accounts(self):
        docs = [
            _doc(account="a", platform_code="web", after_total_bet_amount="200"),
            _doc(account="b", platform_code="web", after_total_bet_amount="300"),
            _doc(account="c", platform_code="mobile", after_total_bet_amount="50"),
        ]
        r = self._explorer(docs, snapshot_week="2024-W20")
        by_plat = {p["platform_code"]: p for p in r["platform_breakdown"]}
        self.assertEqual(by_plat["web"]["rows"], 2)
        self.assertEqual(by_plat["web"]["accounts"], 2)
        self.assertEqual(by_plat["mobile"]["rows"], 1)

    def test_empty_collection_returns_empty_platform_list(self):
        r = self._explorer([], snapshot_week="2024-W20")
        self.assertEqual(r["platform_breakdown"], [])


# ---------------------------------------------------------------------------
# Currency aggregation tests
# ---------------------------------------------------------------------------

class CurrencyBreakdownTests(unittest.TestCase):
    def _explorer(self, docs, **kwargs):
        col = FakeMarketingCollection(docs)
        batches = FakeBatchesCollection()
        return ex.get_raw_explorer(marketing_col=col, batches_col=batches, **kwargs)

    def test_currency_rows_and_totals(self):
        docs = [
            _doc(account="a", currency_code="MYR", withdraw_amount="100", after_total_bet_amount="200"),
            _doc(account="b", currency_code="MYR", withdraw_amount="50", after_total_bet_amount="100"),
            _doc(account="c", currency_code="USD", withdraw_amount="10", after_total_bet_amount="20"),
        ]
        r = self._explorer(docs, snapshot_week="2024-W20")
        by_curr = {c["currency_code"]: c for c in r["currency_breakdown"]}
        self.assertEqual(by_curr["MYR"]["rows"], 2)
        self.assertAlmostEqual(by_curr["MYR"]["withdraw_amount"], 150.0)
        self.assertAlmostEqual(by_curr["MYR"]["after_total_bet_amount"], 300.0)

    def test_empty_collection_returns_empty_currency_list(self):
        r = self._explorer([], snapshot_week="2024-W20")
        self.assertEqual(r["currency_breakdown"], [])


# ---------------------------------------------------------------------------
# Snapshot filter tests
# ---------------------------------------------------------------------------

class SnapshotFilterTests(unittest.TestCase):
    def _col_batches(self, docs):
        return FakeMarketingCollection(docs), FakeBatchesCollection()

    def test_filter_by_snapshot_week(self):
        docs = [
            _doc(account="a", snapshot_week="2024-W20", snapshot_month="2024-05"),
            _doc(account="b", snapshot_week="2024-W21", snapshot_month="2024-05"),
        ]
        col, batches = self._col_batches(docs)
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches, snapshot_week="2024-W20")
        self.assertEqual(r["summary"]["rows_total"], 1)
        self.assertEqual(r["snapshot_filter"]["snapshot_week"], "2024-W20")

    def test_filter_by_snapshot_month(self):
        docs = [
            _doc(account="a", snapshot_week="2024-W20", snapshot_month="2024-05"),
            _doc(account="b", snapshot_week="2024-W20", snapshot_month="2024-05"),
            _doc(account="c", snapshot_week="2024-W14", snapshot_month="2024-04"),
        ]
        col, batches = self._col_batches(docs)
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches, snapshot_month="2024-05")
        self.assertEqual(r["summary"]["rows_total"], 2)
        self.assertEqual(r["snapshot_filter"]["snapshot_month"], "2024-05")

    def test_auto_detects_latest_snapshot_week(self):
        docs = [
            _doc(account="a", snapshot_week="2024-W19", snapshot_month="2024-05"),
            _doc(account="b", snapshot_week="2024-W20", snapshot_month="2024-05"),
        ]
        col, batches = self._col_batches(docs)
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches)
        # Should pick W20 (latest), returning only 1 row
        self.assertEqual(r["summary"]["rows_total"], 1)
        self.assertEqual(r["snapshot_filter"]["snapshot_week"], "2024-W20")

    def test_snapshot_week_takes_precedence_over_month(self):
        docs = [
            _doc(account="a", snapshot_week="2024-W20", snapshot_month="2024-05"),
            _doc(account="b", snapshot_week="2024-W19", snapshot_month="2024-05"),
        ]
        col, batches = self._col_batches(docs)
        r = ex.get_raw_explorer(
            marketing_col=col, batches_col=batches,
            snapshot_week="2024-W20", snapshot_month="2024-05",
        )
        self.assertEqual(r["summary"]["rows_total"], 1)
        self.assertEqual(r["snapshot_filter"]["snapshot_week"], "2024-W20")

    def test_empty_collection_returns_no_filter(self):
        col, batches = self._col_batches([])
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches)
        self.assertIsNone(r["snapshot_filter"]["snapshot_week"])
        self.assertIsNone(r["snapshot_filter"]["snapshot_month"])


# ---------------------------------------------------------------------------
# Missing-data quality check tests
# ---------------------------------------------------------------------------

class MissingDataQualityTests(unittest.TestCase):
    def _dq(self, docs, **kwargs):
        col = FakeMarketingCollection(docs)
        batches = FakeBatchesCollection()
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches, **kwargs)
        return r["data_quality"]

    def test_all_fields_present_all_green(self):
        # Use unique accounts and campaign IDs to avoid triggering duplicate checks
        docs = [_doc(account="acc%d" % i, campaign_id="c%d" % i) for i in range(5)]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["overall_status"], "green")
        for check in dq["checks"].values():
            self.assertEqual(check["count"], 0)
            self.assertEqual(check["status"], "green")

    def test_missing_account_detected(self):
        docs = [_doc(account=""), _doc(account="acc1")] * 5  # 10 docs, 5 missing
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_account"]["count"], 5)
        self.assertGreater(dq["checks"]["missing_account"]["pct"], 0)

    def test_missing_campaign_id_detected(self):
        docs = [_doc(campaign_id=""), _doc(campaign_id="c1")] * 6  # 12 docs, 6 missing
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_campaign_id"]["count"], 6)

    def test_missing_platform_code_detected(self):
        docs = [_doc(platform_code=None), _doc(platform_code="web")]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_platform_code"]["count"], 1)

    def test_missing_currency_code_detected(self):
        docs = [_doc(currency_code=""), _doc(currency_code="MYR")]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_currency_code"]["count"], 1)

    def test_missing_withdraw_amount_detected(self):
        docs = [_doc(withdraw_amount=None), _doc(withdraw_amount="50")]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_withdraw_amount"]["count"], 1)

    def test_missing_after_bet_amount_detected(self):
        docs = [_doc(after_total_bet_amount=""), _doc(after_total_bet_amount="100")]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_after_bet_amount"]["count"], 1)

    def test_green_status_when_zero_issues(self):
        docs = [_doc(account="acc%d" % i) for i in range(20)]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_account"]["status"], "green")

    def test_yellow_status_when_less_than_5_pct(self):
        # 1 out of 100 rows = 1% → yellow
        docs = [_doc(account="acc%d" % i) for i in range(99)]
        docs.append(_doc(account=""))
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_account"]["status"], "yellow")

    def test_red_status_when_5_pct_or_more(self):
        # 5 out of 100 rows = 5% → red
        docs = [_doc(account="acc%d" % i) for i in range(95)]
        docs += [_doc(account="") for _ in range(5)]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_account"]["status"], "red")

    def test_red_status_when_exactly_5_pct(self):
        docs = [_doc(account="a%d" % i) for i in range(95)]
        docs += [_doc(account="") for _ in range(5)]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["missing_account"]["status"], "red")

    def test_empty_collection_all_green(self):
        dq = self._dq([], snapshot_week="2024-W20")
        self.assertEqual(dq["overall_status"], "green")
        self.assertEqual(dq["total_rows"], 0)


# ---------------------------------------------------------------------------
# Duplicate-data quality check tests
# ---------------------------------------------------------------------------

class DuplicateDataQualityTests(unittest.TestCase):
    def _dq(self, docs, **kwargs):
        col = FakeMarketingCollection(docs)
        batches = FakeBatchesCollection()
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches, **kwargs)
        return r["data_quality"]

    def test_no_duplicate_accounts_when_all_unique(self):
        docs = [_doc(account="a%d" % i) for i in range(10)]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["duplicate_accounts"]["count"], 0)
        self.assertEqual(dq["checks"]["duplicate_accounts"]["status"], "green")

    def test_duplicate_accounts_detected(self):
        # account "dup" appears 3 times → 2 extra rows
        docs = [_doc(account="dup")] * 3 + [_doc(account="unique")]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["duplicate_accounts"]["count"], 2)

    def test_duplicate_campaign_entries_detected(self):
        # same (account, campaign_id) pair appears 3 times → 2 extra rows
        docs = [
            _doc(account="a", campaign_id="c1")] * 3 + [
            _doc(account="a", campaign_id="c2"),  # different campaign → not dup
        ]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["duplicate_campaign_entries"]["count"], 2)

    def test_no_duplicate_campaign_entries_when_all_unique(self):
        docs = [_doc(account="a%d" % i, campaign_id="c%d" % i) for i in range(5)]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["checks"]["duplicate_campaign_entries"]["count"], 0)

    def test_overall_status_is_worst_of_all_checks(self):
        # 95% of rows have a duplicate account → red
        docs = [_doc(account="dup")] * 95 + [_doc(account="u%d" % i) for i in range(5)]
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["overall_status"], "red")

    def test_overall_status_yellow_when_only_yellow_checks(self):
        # 1% missing campaign → yellow (use unique accounts to avoid duplicate account check)
        docs = [_doc(account="a%d" % i, campaign_id="c1") for i in range(99)]
        docs.append(_doc(account="a99", campaign_id=""))
        dq = self._dq(docs, snapshot_week="2024-W20")
        self.assertEqual(dq["overall_status"], "yellow")


# ---------------------------------------------------------------------------
# Quality status helper unit tests
# ---------------------------------------------------------------------------

class QualityStatusHelperTests(unittest.TestCase):
    def test_green_for_zero_count(self):
        self.assertEqual(ex._quality_status(0, 100), "green")

    def test_green_for_zero_total(self):
        self.assertEqual(ex._quality_status(0, 0), "green")

    def test_yellow_for_less_than_5_pct(self):
        self.assertEqual(ex._quality_status(4, 100), "yellow")

    def test_red_for_exactly_5_pct(self):
        self.assertEqual(ex._quality_status(5, 100), "red")

    def test_red_for_more_than_5_pct(self):
        self.assertEqual(ex._quality_status(10, 100), "red")

    def test_yellow_boundary_at_4_99_pct(self):
        self.assertEqual(ex._quality_status(4, 100), "yellow")


# ---------------------------------------------------------------------------
# Response structure tests
# ---------------------------------------------------------------------------

class ResponseStructureTests(unittest.TestCase):
    def test_response_has_all_required_keys(self):
        col = FakeMarketingCollection([_doc()])
        batches = FakeBatchesCollection()
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches, snapshot_week="2024-W20")
        for key in ("summary", "campaign_breakdown", "platform_breakdown",
                    "currency_breakdown", "snapshot_summary", "data_quality",
                    "snapshot_filter", "generated_at", "data_source"):
            self.assertIn(key, r, f"Missing key: {key}")
        self.assertEqual(r["data_source"], "marketing_raw_data")

    def test_summary_has_all_required_fields(self):
        col = FakeMarketingCollection([_doc()])
        batches = FakeBatchesCollection()
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches, snapshot_week="2024-W20")
        for field in ("rows_total", "distinct_accounts", "campaign_count",
                      "platform_count", "currency_count", "new_players",
                      "total_withdraw_amount", "total_after_bet_amount"):
            self.assertIn(field, r["summary"], f"Missing summary field: {field}")

    def test_data_quality_has_all_check_keys(self):
        col = FakeMarketingCollection([_doc()])
        batches = FakeBatchesCollection()
        r = ex.get_raw_explorer(marketing_col=col, batches_col=batches, snapshot_week="2024-W20")
        dq = r["data_quality"]
        for key in ("total_rows", "checks", "overall_status"):
            self.assertIn(key, dq, f"Missing data_quality key: {key}")
        for check_name in ("missing_account", "missing_campaign_id", "missing_platform_code",
                           "missing_currency_code", "missing_withdraw_amount",
                           "missing_after_bet_amount", "duplicate_accounts",
                           "duplicate_campaign_entries"):
            self.assertIn(check_name, dq["checks"], f"Missing check: {check_name}")


if __name__ == "__main__":
    unittest.main()
