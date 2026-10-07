/**
 * "Voucher Stock Required — Pending Manual" panel on the Pending Affiliate
 * Rewards view. All numbers are server-computed; this only checks rendering,
 * which buttons are enabled, and that the per-row Replenish button is gone
 * from the Pending Manual tab.
 *
 * Run with: node --test test_affiliate_pending_manual_shortage_ui.test.js
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

function load() {
  const code = slice(JS, "var AFFP_SHORTAGE_DENOMS", "function affpLoadShortage");
  const sandbox = {
    Array, String, Object, parseInt,
    esc: (v) => String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])),
    fmt: (v) => String(v == null ? 0 : v),
  };
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

const summary = {
  pending_count: 26, total_reward_value: 3740, issuable_after_replenishment: 24, issuable_now: 3, still_blocked: 2,
  blocked_breakdown: { no_batch_for_entitlement_period: 2 },
  partially_reserved_ledgers: 1, scan_truncated: false,
  denominations: {
    "5": { pool_id: "AFFILIATE_5", required: 18, available: 5, shortage: 13 },
    "10": { pool_id: "AFFILIATE_10", required: 47, available: 12, shortage: 35 },
    "50": { pool_id: "AFFILIATE_50", required: 63, available: 63, shortage: 0 },
  },
  by_month: {
    "202609": { "5": { shortage: 13, historical: true }, "10": { shortage: 20, historical: true } },
    "202610": { "10": { shortage: 15, historical: false } },
  },
};

test("renders required / available / need-to-upload per denomination and the headline counts", () => {
  const html = load().affpShortageHtml(summary);
  assert.match(html, /Voucher Stock Required — Pending Manual/);
  assert.match(html, /<b>26<\/b> pending reward\(s\) affected/);
  assert.match(html, /\$3740/);
  assert.match(html, /<b>24<\/b> will become issuable/);
  assert.match(html, /<b>2<\/b> blocked by another reason \(no_batch_for_entitlement_period × 2\)/);
  assert.match(html, /<td class="num">18<\/td><td class="num">5<\/td>/);
  assert.match(html, /<td class="num">47<\/td><td class="num">12<\/td>/);
  assert.match(html, /Upload \$5 Codes/);
  assert.match(html, /Upload \$10 Codes/);
  assert.match(html, /Upload \$50 Codes/);
});

test("upload is disabled where nothing is needed; retry is enabled only with pending rows", () => {
  const html = load().affpShortageHtml(summary);
  assert.match(html, /data-denom="50" disabled>Upload \$50 Codes/);
  assert.doesNotMatch(html, /data-denom="10" disabled/);
  assert.doesNotMatch(html, /data-affp-shortage-op="retry-all" disabled/);
  const empty = load().affpShortageHtml(Object.assign({}, summary, { pending_count: 0 }));
  assert.match(empty, /data-affp-shortage-op="retry-all" disabled/);
});

test("per-month breakdown flags ended batches", () => {
  const html = load().affpShortageHtml(summary);
  assert.match(html, /Sep 2026: 20 \(ended batch\)/);
  assert.match(html, /Oct 2026: 15/);
  const s = load();
  assert.deepEqual(
    JSON.parse(JSON.stringify(s.affpShortageMonthsFor(summary, "10"))),
    [{ month: "202609", shortage: 20, historical: true }, { month: "202610", shortage: 15, historical: false }]
  );
  assert.deepEqual(JSON.parse(JSON.stringify(s.affpShortageMonthsFor(summary, "50"))), []);
});

test("server strings are escaped and truncation is surfaced", () => {
  const html = load().affpShortageHtml(Object.assign({}, summary, {
    blocked_breakdown: { "<img src=x onerror=alert(1)>": 1 }, scan_truncated: true,
  }));
  assert.doesNotMatch(html, /<img src=x/);
  assert.match(html, /&lt;img src=x/);
  assert.match(html, /scan truncated/);
});

test("per-row Replenish Historical Batch is not offered on the Pending Manual tab", () => {
  assert.match(JS, /status !== "PENDING_MANUAL" && affpHistoricalReplenishVisible\(it\)/);
});
