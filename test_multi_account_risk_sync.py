import unittest
from unittest.mock import patch

import multi_account_risk_sync as sync


class FakeBulkResult:
    def __init__(self, modified_count=0):
        self.modified_count = modified_count


class FakeUsersCollection:
    def __init__(self, docs):
        self.docs = {int(doc["user_id"]): dict(doc) for doc in docs}
        self.bulk_calls = 0

    def find(self, filt, projection=None):  # noqa: ARG002
        ids = filt.get("user_id", {}).get("$in", [])
        out = []
        for uid in ids:
            if uid in self.docs:
                doc = dict(self.docs[uid])
                doc["user_id"] = uid
                out.append(doc)
        return out

    def bulk_write(self, ops, ordered=False):  # noqa: ARG002
        self.bulk_calls += 1
        modified = 0
        for op in ops:
            filt = getattr(op, "_filter", {})
            update = getattr(op, "_doc", {})
            uid = int(filt["user_id"])
            if uid in self.docs:
                self.docs[uid].update(update.get("$set", {}))
                modified += 1
        return FakeBulkResult(modified_count=modified)


GAMING_ACCOUNT_ID = "4YCLx8PImw4YvLB8"
CLUSTER_TG_IDS = [201, 202, 203, 204, 205, 206, 207, 208]


class MultiAccountRiskSyncTests(unittest.TestCase):
    def _headers(self):
        return [
            "user_id",
            "linked_gaming_accounts",
            "linked_tg_count",
            "multi_account_cluster_member",
            "multi_account_voucher_hunter",
            "voucher_hunter_reasons",
        ]

    def _cluster_rows(self):
        """8 Telegram identities all linked to gaming account 4YCLx8PImw4YvLB8,
        matching the known production evidence in the task: linked_tg_count=8,
        voucher_hunter_reasons includes 'multiple_account'."""
        rows = [self._headers()]
        for uid in CLUSTER_TG_IDS:
            rows.append(
                [str(uid), GAMING_ACCOUNT_ID, "8", "true", "true", "multiple_account, high_claim_velocity"]
            )
        return rows

    def _rows(self):
        rows = self._cluster_rows()
        # A non-cluster user (single account, no risk) and an invalid id.
        rows.append(["300", "", "1", "false", "false", ""])
        rows.append(["not-an-id", GAMING_ACCOUNT_ID, "8", "true", "false", ""])
        return rows

    def _users(self, extra=None):
        docs = [{"user_id": uid, "for_bot_segment": "normal_actual"} for uid in CLUSTER_TG_IDS]
        docs.append({"user_id": 300, "for_bot_segment": "high_value"})
        if extra:
            docs.extend(extra)
        return FakeUsersCollection(docs)

    def test_dry_run_makes_no_writes_and_reports_cluster(self):
        users = self._users()
        with patch.object(sync.database, "init_db") as init_db:
            summary = sync.sync_multi_account_risk_from_sheet(dry_run=True, users_col=users, rows=self._rows())

        init_db.assert_not_called()
        self.assertTrue(summary["ok"])
        self.assertTrue(summary["dry_run"])
        self.assertFalse(summary["writes_performed"])
        self.assertEqual(summary["users_write_attempted"], 0)
        self.assertEqual(summary["users_modified"], 0)
        self.assertEqual(users.bulk_calls, 0)

        # Item 4: production validation evidence for gaming account 4YCLx8PImw4YvLB8.
        self.assertIn(GAMING_ACCOUNT_ID, summary["clusters"])
        cluster = summary["clusters"][GAMING_ACCOUNT_ID]
        self.assertEqual(sorted(cluster["member_user_ids"]), sorted(CLUSTER_TG_IDS))
        self.assertEqual(len(cluster["member_user_ids"]), 8)
        self.assertEqual(cluster["reported_linked_tg_count"], 8)

        # Risk-member users matched (8 cluster members + 1 invalid-id row skipped).
        self.assertEqual(summary["risk_members_matched"], 8)
        self.assertEqual(summary["users_to_set_risk_true"], 8)
        self.assertEqual(summary["users_to_clear_stale_risk"], 0)
        self.assertEqual(summary["linked_accounts_to_add"], 8)
        self.assertEqual(summary["canonical_segment_updates"], 0)
        self.assertGreaterEqual(summary["canonical_segment_users_matched"], 8)

        # For at least one linked Telegram identity: canonical segment unchanged,
        # multi_account_risk would become True, linked_tg_count=8, and the
        # gaming account id is present in linked_gaming_accounts.
        entry = next(p for p in summary["preview"] if p["user_id"] == CLUSTER_TG_IDS[0])
        self.assertEqual(entry["for_bot_segment"], "normal_actual")
        self.assertFalse(entry["multi_account_risk_current"])
        self.assertTrue(entry["multi_account_risk_new"])
        self.assertEqual(entry["linked_tg_count"], 8)
        self.assertIn(GAMING_ACCOUNT_ID, entry["linked_gaming_accounts"])
        self.assertIn("multiple_account", entry["voucher_hunter_reasons"])

        # Canonical segment field on the underlying user doc is untouched by dry run.
        self.assertEqual(users.docs[CLUSTER_TG_IDS[0]]["for_bot_segment"], "normal_actual")

    def test_commit_writes_risk_fields_without_touching_segment_fields(self):
        users = self._users()
        with patch.object(sync.database, "init_db"):
            summary = sync.sync_multi_account_risk_from_sheet(dry_run=False, users_col=users, rows=self._rows())

        self.assertTrue(summary["ok"])
        self.assertTrue(summary["writes_performed"])
        self.assertEqual(summary["users_modified"], 9)

        self.assertFalse(users.docs[300]["multi_account_risk"])
        self.assertEqual(users.docs[300]["for_bot_segment"], "high_value")

        for uid in CLUSTER_TG_IDS:
            doc = users.docs[uid]
            self.assertTrue(doc["multi_account_risk"])
            self.assertTrue(doc["multi_account_cluster_member"])
            self.assertEqual(doc["linked_tg_count"], 8)
            self.assertIn(GAMING_ACCOUNT_ID, doc["linked_gaming_accounts"])
            self.assertIn("multiple_account", doc["voucher_hunter_reasons"])
            # Canonical segment fields must survive untouched.
            self.assertEqual(doc["for_bot_segment"], "normal_actual")
            self.assertNotIn("for_bot_segment_normalized", doc)
            self.assertNotIn("bot_segment_source", doc)
            self.assertNotIn("bot_segment_synced_at", doc)

    def test_stale_risk_would_be_cleared_when_no_longer_a_cluster_member(self):
        users = self._users(extra=[{"user_id": 999, "for_bot_segment": "voucher_hunter", "multi_account_risk": True}])
        rows = self._rows()
        rows.append(["999", "", "1", "false", "false", ""])
        with patch.object(sync.database, "init_db"):
            summary = sync.sync_multi_account_risk_from_sheet(dry_run=True, users_col=users, rows=rows)

        self.assertTrue(summary["ok"])
        self.assertEqual(summary["users_to_clear_stale_risk"], 1)
        self.assertEqual(users.docs[999]["multi_account_risk"], True)  # unchanged: dry run.

    def test_missing_cluster_member_column_never_clears_existing_risk(self):
        """Codex review finding on PR #414: a sheet snapshot that has
        linked_gaming_accounts but omits multi_account_cluster_member must
        never be treated as "everyone is False" -- that would silently erase
        a previously-synced multi_account_risk=True on --commit."""
        rows = [
            ["user_id", "linked_gaming_accounts", "linked_tg_count"],
            ["999", GAMING_ACCOUNT_ID, "8"],
        ]
        users = FakeUsersCollection(
            [{"user_id": 999, "for_bot_segment": "voucher_hunter", "multi_account_risk": True}]
        )
        with patch.object(sync.database, "init_db"):
            dry_run_summary = sync.sync_multi_account_risk_from_sheet(dry_run=True, users_col=users, rows=rows)
            commit_summary = sync.sync_multi_account_risk_from_sheet(dry_run=False, users_col=users, rows=rows)

        self.assertFalse(dry_run_summary["source_columns_present"]["multi_account_cluster_member"])
        self.assertEqual(dry_run_summary["users_to_clear_stale_risk"], 0)
        self.assertEqual(dry_run_summary["users_to_set_risk_true"], 0)

        self.assertTrue(commit_summary["ok"])
        self.assertTrue(commit_summary["writes_performed"])
        # multi_account_risk/multi_account_cluster_member must be untouched --
        # still True -- since this run had no data for that column.
        self.assertTrue(users.docs[999]["multi_account_risk"])
        # linked_gaming_accounts/linked_tg_count are still propagated, since
        # those columns WERE present.
        self.assertIn(GAMING_ACCOUNT_ID, users.docs[999]["linked_gaming_accounts"])
        self.assertEqual(users.docs[999]["linked_tg_count"], 8)

    def test_missing_required_column_skips_without_crash(self):
        rows = [["user_id", "some_other_column"], ["100", "x"]]
        users = FakeUsersCollection([{"user_id": 100}])
        with patch.object(sync.database, "init_db") as init_db:
            summary = sync.sync_multi_account_risk_from_sheet(dry_run=False, users_col=users, rows=rows)
        init_db.assert_not_called()
        self.assertTrue(summary["ok"])
        self.assertIsNotNone(summary["skipped_reason"])
        self.assertFalse(summary["source_columns_present"]["linked_gaming_accounts"])
        self.assertEqual(users.bulk_calls, 0)

    def test_init_db_called_before_row_parsing_when_users_col_is_none(self):
        users = self._users()
        call_order: list[str] = []

        def fake_init_db(*a, **kw):  # noqa: ARG001
            call_order.append("init_db")

        with patch.object(sync.database, "init_db", side_effect=fake_init_db) as init_db, patch.object(
            sync.database, "users_collection", users
        ):
            summary = sync.sync_multi_account_risk_from_sheet(dry_run=True, users_col=None, rows=self._rows())

        init_db.assert_called_once()
        self.assertEqual(call_order, ["init_db"])
        self.assertTrue(summary["ok"])


class ParseListFieldTests(unittest.TestCase):
    """Unit tests for the strict list-field parser (_parse_list_field), plus
    its shape in the JSON-array cell format production UIM actually returns
    for linked_gaming_accounts/voucher_hunter_reasons."""

    def test_native_list_input(self):
        self.assertEqual(sync._parse_list_field(["A", "B"]), (["A", "B"], True))

    def test_json_array_string_input(self):
        self.assertEqual(sync._parse_list_field('["A","B"]'), (["A", "B"], True))

    def test_json_array_string_with_spacing_matches_bug_report(self):
        # Exact malformed-in-old-code example from the production dry-run.
        self.assertEqual(
            sync._parse_list_field('["0IUNRUPdnWQ5Fzg2", "2wOmVcPgQbGddUPr"]'),
            (["0IUNRUPdnWQ5Fzg2", "2wOmVcPgQbGddUPr"], True),
        )

    def test_voucher_hunter_reasons_json_array(self):
        self.assertEqual(
            sync._parse_list_field('["behavioral","multiple_account"]'),
            (["behavioral", "multiple_account"], True),
        )

    def test_empty_string(self):
        self.assertEqual(sync._parse_list_field(""), ([], True))

    def test_none(self):
        self.assertEqual(sync._parse_list_field(None), ([], True))

    def test_empty_native_list(self):
        self.assertEqual(sync._parse_list_field([]), ([], True))

    def test_empty_json_array_string(self):
        self.assertEqual(sync._parse_list_field("[]"), ([], True))

    def test_legacy_csv_scalar_string(self):
        self.assertEqual(sync._parse_list_field("A, B"), (["A", "B"], True))

    def test_legacy_semicolon_scalar_string(self):
        self.assertEqual(sync._parse_list_field("A; B"), (["A", "B"], True))

    def test_single_legacy_scalar(self):
        self.assertEqual(sync._parse_list_field("A"), (["A"], True))

    def test_dedupes_and_trims_and_drops_empty(self):
        self.assertEqual(sync._parse_list_field(' ["A", " A ", "", "B"] '), (["A", "B"], True))

    def test_malformed_json_fails_closed(self):
        items, ok = sync._parse_list_field('["A", "B"')
        self.assertFalse(ok)
        self.assertEqual(items, [])

    def test_malformed_mismatched_brackets_fails_closed(self):
        items, ok = sync._parse_list_field('["A", "B"}')
        self.assertFalse(ok)
        self.assertEqual(items, [])


class MalformedListRowTests(unittest.TestCase):
    def _headers(self):
        return [
            "user_id",
            "linked_gaming_accounts",
            "linked_tg_count",
            "multi_account_cluster_member",
            "multi_account_voucher_hunter",
            "voucher_hunter_reasons",
        ]

    def test_malformed_row_is_skipped_and_counted_and_cannot_prune(self):
        """A malformed linked_gaming_accounts cell must not be parsed at
        all -- the row is dropped entirely, so it can never appear in
        `matched`/`clusters` and can never drive linked_accounts_to_prune,
        even when the existing user doc already has linked_gaming_accounts
        that a broken parse could otherwise appear to "remove"."""
        rows = [
            self._headers(),
            # truncated JSON array -- looks like list data, fails to parse.
            ["555", '["A", "B"', "2", "true", "false", ""],
        ]
        users = FakeUsersCollection(
            [{"user_id": 555, "for_bot_segment": "normal_actual", "linked_gaming_accounts": ["OLD1", "OLD2"]}]
        )
        with patch.object(sync.database, "init_db"):
            summary = sync.sync_multi_account_risk_from_sheet(dry_run=True, users_col=users, rows=rows)

        self.assertTrue(summary["ok"])
        self.assertEqual(summary["parsing_errors"], 1)
        self.assertEqual(summary["users_matched"], 0)
        self.assertEqual(summary["risk_members_matched"], 0)
        self.assertEqual(summary["linked_accounts_to_add"], 0)
        self.assertEqual(summary["linked_accounts_to_prune"], 0)
        self.assertEqual(summary["clusters"], {})

    def test_known_cluster_stays_one_cluster_not_bracket_quote_variants(self):
        """The known production cluster (gaming account 4YCLx8PImw4YvLB8,
        8 Telegram members) must resolve as exactly one cluster key even
        when the sheet stores linked_gaming_accounts as a JSON array
        string -- not split into malformed variants like
        '["4YCLx8PImw4YvLB8"]' / '"4YCLx8PImw4YvLB8"'."""
        rows = [self._headers()]
        for uid in CLUSTER_TG_IDS:
            rows.append([str(uid), f'["{GAMING_ACCOUNT_ID}"]', "8", "true", "true", '["multiple_account"]'])
        users = self._users_for(CLUSTER_TG_IDS)
        with patch.object(sync.database, "init_db"):
            summary = sync.sync_multi_account_risk_from_sheet(dry_run=True, users_col=users, rows=rows)

        self.assertEqual(summary["parsing_errors"], 0)
        self.assertEqual(list(summary["clusters"].keys()), [GAMING_ACCOUNT_ID])
        cluster = summary["clusters"][GAMING_ACCOUNT_ID]
        self.assertEqual(sorted(cluster["member_user_ids"]), sorted(CLUSTER_TG_IDS))
        self.assertEqual(len(cluster["member_user_ids"]), 8)

        entry = next(p for p in summary["preview"] if p["user_id"] == CLUSTER_TG_IDS[0])
        self.assertEqual(entry["linked_gaming_accounts"], [GAMING_ACCOUNT_ID])
        for account_id in entry["linked_gaming_accounts"]:
            self.assertNotIn("[", account_id)
            self.assertNotIn("]", account_id)
            self.assertNotIn('"', account_id)
        self.assertEqual(entry["voucher_hunter_reasons"], ["multiple_account"])

    def test_multiple_gaming_accounts_json_array(self):
        rows = [
            self._headers(),
            ["700", '["0IUNRUPdnWQ5Fzg2", "2wOmVcPgQbGddUPr"]', "2", "true", "true", '["behavioral", "multiple_account"]'],
        ]
        users = self._users_for([700])
        with patch.object(sync.database, "init_db"):
            summary = sync.sync_multi_account_risk_from_sheet(dry_run=True, users_col=users, rows=rows)

        entry = summary["preview"][0]
        self.assertEqual(entry["linked_gaming_accounts"], ["0IUNRUPdnWQ5Fzg2", "2wOmVcPgQbGddUPr"])
        self.assertEqual(entry["voucher_hunter_reasons"], ["behavioral", "multiple_account"])

    def _users_for(self, uids):
        return FakeUsersCollection([{"user_id": uid, "for_bot_segment": "normal_actual"} for uid in uids])


if __name__ == "__main__":
    unittest.main()
