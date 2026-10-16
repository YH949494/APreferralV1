"""Hypothetical retrospective V2 audit of bundle_denomination_short requests.

Synthetic mongomock data only. The audit must be a pure projection: every
test that runs it does so through a handle that fails on any write method,
and the database content is compared before/after.
"""
import io
import json
from datetime import datetime, timezone

import mongomock
import pytest

import affiliate_qualification as aq
import affiliate_reward_v2_audit as audit
from scripts import affiliate_reward_v2_audit as cli

NOW = datetime(2026, 10, 20, 4, 0, tzinfo=timezone.utc)
OCT5 = datetime(2026, 10, 5, 6, 0, tzinfo=timezone.utc)
MONTH = "202610"
AFFILIATE = 1

_WRITE_METHODS = {
    "insert_one", "insert_many", "update_one", "update_many", "replace_one", "delete_one", "delete_many",
    "find_one_and_update", "find_one_and_replace", "find_one_and_delete", "bulk_write", "create_index",
    "create_indexes", "drop", "drop_index", "drop_indexes", "rename", "aggregate_write",
}


class _RoCol:
    def __init__(self, col):
        self._col = col

    def __getattr__(self, name):
        if name in _WRITE_METHODS:
            raise AssertionError(f"audit attempted a write: {self._col.name}.{name}")
        return getattr(self._col, name)


class RoDb:
    def __init__(self, db):
        self._db = db

    def __getitem__(self, name):
        return _RoCol(self._db[name])

    def __getattr__(self, name):
        return _RoCol(getattr(self._db, name))


def snapshot(db):
    return {n: sorted(map(str, db[n].find())) for n in db.list_collection_names()}


@pytest.fixture(autouse=True)
def _thresholds(monkeypatch):
    # T1=3 keeps fixtures small; T2.. stay far above anything built here.
    for tier, value in (("T1", 3), ("T2", 5), ("T3", 50), ("T4", 150), ("T5", 250)):
        monkeypatch.setenv(f"AFF_{tier}_THRESHOLD", str(value))


class W:
    def __init__(self):
        self.db = mongomock.MongoClient().db
        self.next_eid = 0

    def ledger(self, user=AFFILIATE, tier="T1", month=MONTH, flags=("bundle_denomination_short",), status="PENDING_MANUAL"):
        self.db.affiliate_ledger.insert_one({
            "ledger_type": "AFFILIATE_MONTHLY", "user_id": user, "tier": tier, "year_month": month,
            "entitlement_month": month, "status": status, "risk_flags": list(flags), "voucher_code": None,
            "dedup_key": f"AFF:{user}:{month}:{tier}", "qualified_count": 0, "created_at": NOW,
        })

    def invitee(self, uid, *, referrer=AFFILIATE, new_player=1, bet="80.00", account=None, evidence=True,
                v2=True, at=OCT5, fields_extra=None, complete=True):
        """A qualified event, plus Databot-shaped evidence unless ``evidence`` is False.

        ``v2`` False builds a LEGACY qualification: no rule_version / evidence_id;
        the evidence is then only reachable through the invitee's WELCOME code hash."""
        account = account or f"acct{uid}"
        code = f"CODE{uid}"
        q = {"invitee_id": uid, "referrer_id": referrer, "qualified_at": at, "redeemed_at": at}
        if evidence:
            fields = {"transport_version": 1, "seven_day_complete": complete,
                      "coupon_new_player": new_player, "after_total_bet_amount": bet, **(fields_extra or {})}
            doc = aq.build_evidence_doc(
                source=aq.SOURCE_DATABOT_UIM, code=code, recipient={"status": "ok", "user_id": uid},
                account_raw=account, namespace="uim_global", redeemed_at=at, redemption_successful=True,
                now_utc=NOW, qualification_fields=fields)
            eid = self.db[aq.EVIDENCE_COLLECTION].insert_one(doc).inserted_id
            if v2:
                q.update(rule_version=aq.RULE_VERSION, evidence_id=eid, account_key=doc["account_key"])
        if not v2:
            self.db.affiliate_ledger.insert_one({"dedup_key": f"WELCOME:{uid}", "ledger_type": "WELCOME",
                                                 "user_id": uid, "voucher_code": code, "status": "ISSUED"})
        self.db.qualified_events.insert_one(q)

    def run(self, **kw):
        return audit.build_audit(RoDb(self.db), month=MONTH, now_utc=NOW, **kw)


def only(report):
    assert len(report["requests"]) == 1
    return report["requests"][0]


def counts(req):
    return req["classification_counts"]


def test_pass_reaches_claimed_tier():
    w = W()
    w.ledger()
    for uid in (11, 12, 13):
        w.invitee(uid)
    before = snapshot(w.db)
    r = only(w.run())
    assert counts(r)[audit.PASS] == 3
    assert r["current_qualified_count"] == 3 and r["current_eligible_tier"] == "T1"
    assert r["projected_v2_qualified_count"] == 3 and r["projected_v2_tier"] == "T1"
    assert r["requested_reward_v2_verdict"] == audit.ELIGIBLE
    assert snapshot(w.db) == before


@pytest.mark.parametrize("bet,cls", [("79.99", audit.FAIL_TURNOVER), ("80.00", audit.PASS), ("80", audit.PASS),
                                     ("0", audit.FAIL_TURNOVER)])
def test_turnover_boundary(bet, cls):
    w = W()
    w.ledger()
    w.invitee(11, bet=bet)
    assert counts(only(w.run()))[cls] == 1


def test_new_player_failure_and_verdict_ineligible():
    w = W()
    w.ledger()
    w.invitee(11, new_player=0)
    w.invitee(12, new_player=0)
    w.invitee(13, bet="79.99")
    r = only(w.run())
    assert counts(r)[audit.FAIL_NEW_PLAYER] == 2 and counts(r)[audit.FAIL_TURNOVER] == 1
    assert r["projected_v2_qualified_count"] == 0 and r["projected_v2_tier"] is None
    assert r["requested_reward_v2_verdict"] == audit.INELIGIBLE
    # A gate failure outranks nothing else here: reasons are reported verbatim.
    assert r["reason_counts"] == {"not_a_new_coupon_player": 2, "seven_day_turnover_below_80": 1}


def test_duplicate_account_within_affiliate_keeps_earliest():
    w = W()
    w.ledger()
    w.invitee(11, account="same", at=OCT5)
    w.invitee(12, account="same", at=OCT5.replace(hour=9))
    w.invitee(13)
    r = only(w.run())
    assert counts(r)[audit.PASS] == 2 and counts(r)[audit.FAIL_DUPLICATE] == 1
    assert r["requested_reward_v2_verdict"] == audit.INELIGIBLE  # 2 < T1 threshold of 3
    sample = [s for s in r["masked_samples"] if s["class"] == audit.FAIL_DUPLICATE]
    assert sample and sample[0]["invitee"] == aq.mask_value(12)


def test_duplicate_account_credited_to_someone_else_in_registry():
    w = W()
    w.ledger()
    for uid in (11, 12, 13):
        w.invitee(uid)
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "uim_global:acct11", "state": aq.REG_COMMITTED,
                                             "invitee_id": 999, "inviter_id": 2})
    r = only(w.run())
    assert counts(r)[audit.FAIL_DUPLICATE] == 1 and counts(r)[audit.PASS] == 2


def test_registry_reservation_in_flight_is_unknown_not_duplicate():
    w = W()
    w.ledger()
    w.invitee(11)
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "uim_global:acct11", "state": aq.REG_RESERVED,
                                             "invitee_id": 999})
    r = only(w.run())
    assert counts(r)[audit.UNKNOWN] == 1 and counts(r)[audit.FAIL_DUPLICATE] == 0


def test_own_registry_entry_is_not_a_duplicate():
    w = W()
    w.ledger()
    w.invitee(11)
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "uim_global:acct11", "state": aq.REG_COMMITTED,
                                             "invitee_id": 11})
    assert counts(only(w.run()))[audit.PASS] == 1


def test_missing_stale_partial_conflicting_evidence_is_unknown_never_failure():
    w = W()
    w.ledger()
    w.invitee(11, evidence=False)                     # no evidence at all
    w.invitee(12, complete=False)                     # seven-day window incomplete (partial)
    w.invitee(13, fields_extra={"transport_version": 2})  # unsupported contract
    w.invitee(14, bet="n/a")                          # unparseable amount
    w.invitee(15)
    w.db[aq.EVIDENCE_COLLECTION].update_one({"recipient_resolution.user_id": 15}, {"$set": {"source_conflict_at": NOW}})
    r = only(w.run())
    assert counts(r)[audit.UNKNOWN] == 5
    assert counts(r)[audit.FAIL_TURNOVER] == counts(r)[audit.FAIL_NEW_PLAYER] == counts(r)[audit.FAIL_DUPLICATE] == 0
    assert r["requested_reward_v2_verdict"] == audit.INDETERMINATE  # 0 passes, 5 unknown >= 3
    assert r["projected_v2_upper_bound_count"] == 5


def test_voided_or_wrong_owner_evidence_is_unknown():
    w = W()
    w.ledger()
    w.invitee(11)
    w.invitee(12)
    w.db[aq.EVIDENCE_COLLECTION].update_one({"recipient_resolution.user_id": 11}, {"$set": {"voided_at": NOW}})
    w.db[aq.EVIDENCE_COLLECTION].update_one({"recipient_resolution.user_id": 12}, {"$set": {"recipient_resolution.user_id": 77}})
    assert counts(only(w.run()))[audit.UNKNOWN] == 2


def test_legacy_qualification_is_evaluated_through_welcome_code_evidence():
    w = W()
    w.ledger()
    w.invitee(11, v2=False)                           # legacy, evidence reachable by code hash -> pass
    w.invitee(12, v2=False, bet="79.99")              # legacy, evidence says turnover short
    w.invitee(13, v2=False, evidence=False)           # legacy, nothing to judge it by
    r = only(w.run())
    assert counts(r)[audit.PASS] == 1 and counts(r)[audit.FAIL_TURNOVER] == 1 and counts(r)[audit.UNKNOWN] == 1
    assert r["legacy_qualification_events"] == 3 and r["legacy_events_without_evidence"] == 1
    assert r["reason_counts"]["no_committed_evidence"] == 1
    assert {s["evidence_link"] for s in r["masked_samples"] if s["legacy_qualification"]} == {"welcome_code_hash"}


def test_legacy_conflicting_evidence_rows_are_unknown():
    w = W()
    w.ledger()
    w.invitee(11, v2=False)
    dup = dict(w.db[aq.EVIDENCE_COLLECTION].find_one({"recipient_resolution.user_id": 11}))
    dup.pop("_id")
    dup["source_ref"] = "other"
    dup["qualification_fields"] = {**dup["qualification_fields"], "after_total_bet_amount": "10"}
    w.db[aq.EVIDENCE_COLLECTION].insert_one(dup)
    r = only(w.run())
    assert counts(r)[audit.UNKNOWN] == 1 and r["reason_counts"] == {"conflicting_evidence_rows": 1}


def test_event_cap_leaves_remainder_unknown():
    w = W()
    w.ledger()
    for uid in range(11, 16):
        w.invitee(uid)
    r = only(w.run(max_events_per_affiliate=2))
    assert r["events_scan_truncated"] and counts(r)[audit.PASS] == 2 and counts(r)[audit.UNKNOWN] == 3
    assert r["current_qualified_count"] == 5 and r["reason_counts"] == {"event_scan_cap_exceeded": 3}


def test_scope_filters_only_pending_shortage_requests_for_the_month():
    w = W()
    w.ledger(user=1)
    w.ledger(user=2, flags=("ip_cluster",))
    w.ledger(user=3, status="APPROVED")
    w.ledger(user=4, month="202609")
    w.db.affiliate_ledger.insert_one({"ledger_type": "AFFILIATE_MONTHLY", "user_id": 5, "tier": "T1", "year_month": MONTH,
                                      "status": "PENDING_MANUAL", "risk_flags": ["bundle_denomination_short"],
                                      "voucher_code": "ISSUED-CODE", "dedup_key": "x"})
    w.db.affiliate_ledger.insert_one({"ledger_type": "AFFILIATE_MONTHLY", "user_id": 6, "tier": "T1", "year_month": MONTH,
                                      "status": "PENDING_MANUAL", "risk_flags": ["bundle_denomination_short"],
                                      "voucher_code": None, "simulate": True, "dedup_key": "y"})
    report = w.run()
    assert [r["affiliate_user_id"] for r in report["requests"]] == [1]
    assert report["bounds"]["simulated_ledgers_skipped"] == 1


def test_request_cap_is_reported():
    w = W()
    for user in (1, 2, 3):
        w.ledger(user=user)
    report = w.run(max_requests=2)
    assert len(report["requests"]) == 2 and report["bounds"]["requests_scan_truncated"] is True


def test_exposure_by_denomination_and_verdict_buckets():
    w = W()
    w.ledger(user=1, tier="T2")           # denomination plan T2: $5 x1 + $10 x2 -> needs 5
    w.ledger(user=2, tier="T1")           # $10 x1 -> needs 3
    for uid in (11, 12, 13, 14, 15):
        w.invitee(uid, referrer=1)
    for uid in (21, 22, 23):
        w.invitee(uid, referrer=2, new_player=0)
    report = w.run()
    agg = report["aggregate"]
    assert agg["request_verdicts"] == {audit.ELIGIBLE: 1, audit.INELIGIBLE: 1, audit.INDETERMINATE: 0}
    exp = agg["voucher_exposure_by_denomination"]
    assert exp["all_pending"] == {"5": 1, "10": 3, "50": 0, "other": 0, "usd": 25 + 10}
    assert exp[audit.ELIGIBLE]["usd"] == 25 and exp[audit.INELIGIBLE] == {"5": 0, "10": 1, "50": 0, "other": 0, "usd": 10}
    assert agg["invitee_classification_counts"][audit.PASS] == 5
    assert report["hypothetical"] is True and report["writes_performed"] == 0 and "HYPOTHETICAL" in report["label"]


def test_samples_are_masked_and_bounded():
    w = W()
    w.ledger()
    for uid in range(100001, 100007):
        w.invitee(uid, new_player=0, account=f"secretaccount{uid}")
    r = only(w.run(sample_size=2))
    assert len(r["masked_samples"]) == 2
    blob = json.dumps(r["masked_samples"])
    assert "100001" not in blob and "secretaccount" not in blob


def test_database_is_unchanged_and_no_collection_created():
    w = W()
    w.ledger()
    for uid in (11, 12):
        w.invitee(uid)
    w.invitee(13, v2=False)
    w.invitee(14, new_player=0, account="dup")
    w.invitee(15, account="dup")
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "uim_global:acct12", "state": aq.REG_COMMITTED, "invitee_id": 9})
    before_snapshot, before_names = snapshot(w.db), sorted(w.db.list_collection_names())
    w.run()
    w.run(user_id=AFFILIATE)
    assert snapshot(w.db) == before_snapshot
    assert sorted(w.db.list_collection_names()) == before_names


def test_cli_json_output_is_read_only():
    w = W()
    w.ledger()
    for uid in (11, 12, 13):
        w.invitee(uid)
    before = snapshot(w.db)
    buf = io.StringIO()
    rc = cli.main(["--month", MONTH, "--format", "json"], db_factory=lambda: RoDb(w.db), now_fn=lambda: NOW, out=buf)
    data = json.loads(buf.getvalue())
    assert rc == 0 and data["requests"][0]["requested_reward_v2_verdict"] == audit.ELIGIBLE
    assert snapshot(w.db) == before
    text = io.StringIO()
    cli.main(["--month", MONTH], db_factory=lambda: RoDb(w.db), now_fn=lambda: NOW, out=text)
    assert "HYPOTHETICAL" in text.getvalue()


def test_cli_rejects_bad_month_and_has_no_write_flags():
    with pytest.raises(SystemExit):
        cli.main(["--month", "2026-10"], db_factory=lambda: RoDb(mongomock.MongoClient().db))
    for flag in ("--commit", "--apply"):
        with pytest.raises(SystemExit):
            cli.main(["--month", MONTH, flag], db_factory=lambda: RoDb(mongomock.MongoClient().db))


def test_module_issues_no_write_calls():
    import ast
    import inspect
    banned = _WRITE_METHODS | {"init_db", "ensure_indexes", "ensure_affiliate_indexes"}
    for mod in (audit, cli):
        for node in ast.walk(ast.parse(inspect.getsource(mod))):
            if isinstance(node, ast.Attribute):
                assert node.attr not in banned, (mod.__name__, node.attr)
            if isinstance(node, ast.Name):
                assert node.id not in banned, (mod.__name__, node.id)
