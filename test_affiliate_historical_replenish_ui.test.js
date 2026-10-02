/**
 * "Replenish Historical Batch" button on the Pending Affiliate Rewards view.
 *
 * Visibility is a convenience only (the server enforces every rule), but the
 * button must never be offered for rows the endpoint will always refuse:
 * current-month stock, non-pinned ledgers, WELCOME, ISSUED/REJECTED, legacy.
 *
 * Same approach as test_admin_dashboard_p0_2_filters.test.js: extract the
 * functions as text and run them in a vm sandbox.
 *
 * Run with: node --test test_affiliate_historical_replenish_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

// 2026-10-02 12:00 UTC -> KL month 202610.
const NOW = Date.UTC(2026, 9, 2, 12, 0, 0);

function load(nowMs) {
  const code = slice(JS, "var AFFP_HIST_STATUSES", "function affpOpenHistoricalReplenishModal");
  const FakeDate = function (ms) { return new Date(ms); };
  FakeDate.now = () => nowMs;
  const sandbox = { Date: FakeDate, Array, String };
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

const base = {
  ledger_type: "AFFILIATE_MONTHLY",
  ledger_status: "PENDING_MANUAL",
  entitlement_month: "202609",
  reward_plan: "denomination_2026_09",
  voucher_code: null,
  pinned_pools: ["AFFILIATE_10"],
};

test("visible for a pinned September PENDING_MANUAL / OUT_OF_STOCK denomination ledger", () => {
  const s = load(NOW);
  assert.equal(s.affpHistoricalReplenishVisible(base), true);
  assert.equal(s.affpHistoricalReplenishVisible(Object.assign({}, base, { ledger_status: "OUT_OF_STOCK" })), true);
});

test("hidden for current-month, non-pinned, WELCOME, ISSUED, REJECTED and legacy rows", () => {
  const s = load(NOW);
  const cases = [
    { entitlement_month: "202610" },
    { pinned_pools: [] },
    { pinned_pools: undefined },
    { ledger_type: "WELCOME" },
    { ledger_type: "AFFILIATE_WEEKLY" },
    { ledger_status: "ISSUED" },
    { ledger_status: "REJECTED" },
    { ledger_status: "PENDING_REVIEW" },
    { voucher_code: "X" },
    { entitlement_month: "202608", reward_plan: "legacy_2026_08" },
    { entitlement_month: "202608", reward_plan: undefined },
  ];
  for (const patch of cases) {
    assert.equal(
      s.affpHistoricalReplenishVisible(Object.assign({}, base, patch)), false, JSON.stringify(patch)
    );
  }
});

test("current KL month rolls over at KL midnight, not UTC midnight", () => {
  // 2026-09-30 16:30 UTC is already 2026-10-01 00:30 in KL.
  const s = load(Date.UTC(2026, 8, 30, 16, 30, 0));
  assert.equal(s.affpCurrentKlMonth(), "202610");
  assert.equal(s.affpHistoricalReplenishVisible(base), true);
  const before = load(Date.UTC(2026, 8, 30, 15, 30, 0));
  assert.equal(before.affpCurrentKlMonth(), "202609");
  assert.equal(before.affpHistoricalReplenishVisible(base), false);
});

test("Approve and Replenish stay separate actions", () => {
  assert.match(JS, /data-affp-op="hist-replenish"/);
  const handler = slice(JS, 'if (op === "hist-replenish")', '} else if (op === "reject")');
  assert.doesNotMatch(handler, /\/approve/);
  const modal = slice(JS, "function affpRenderHistoricalReplenish", "// ---------- Pending Affiliate Rewards");
  assert.doesNotMatch(modal, /\/approve/);
  assert.match(modal, /historical-batch\/replenish/);
  assert.match(modal, /confirm\(/);
});
