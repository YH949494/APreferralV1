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
    "5": { pool_id: "AFFILIATE_5", required: 18, available: 5, available_compatible: 5, shortage: 13, uploadable_shortage: 13 },
    "10": { pool_id: "AFFILIATE_10", required: 47, available: 12, available_compatible: 12, shortage: 35, uploadable_shortage: 35 },
    "50": { pool_id: "AFFILIATE_50", required: 63, available: 63, available_compatible: 63, shortage: 0, uploadable_shortage: 0 },
  },
  by_month: {
    "202609": { "5": { shortage: 13, uploadable_shortage: 13, historical: true }, "10": { shortage: 20, uploadable_shortage: 20, historical: true } },
    "202610": { "10": { shortage: 15, uploadable_shortage: 15, historical: false } },
  },
};

test("renders required / available / need-to-upload per denomination and the headline counts", () => {
  const html = load().affpShortageHtml(summary);
  assert.match(html, /Voucher Stock Required — Pending Manual/);
  assert.match(html, /<b>26<\/b> pending reward\(s\) affected/);
  assert.match(html, /\$3740/);
  assert.match(html, /<b>24<\/b> will become issuable/);
  assert.match(html, /Issuance blockers/);
  assert.match(html, /⚠ No batch exists for the entitlement month: <b>2<\/b>/);
  assert.match(html, /Usable Available/);
  assert.match(html, /<th class="num">Expired<\/th>/);
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
  // Rows that already hold a full bundle are finalized by retry, so it must stay enabled for them.
  const reservedOnly = load().affpShortageHtml(Object.assign({}, summary, { pending_count: 0, excluded: { reserved_complete: 2 } }));
  assert.doesNotMatch(reservedOnly, /data-affp-shortage-op="retry-all" disabled/);
  assert.match(reservedOnly, /2<\/b>? ?already hold a full bundle|2 already hold a full bundle/);
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
    issuance_blockers: { "<img src=x onerror=alert(1)>": 1 }, scan_truncated: true,
  }));
  assert.doesNotMatch(html, /<img src=x/);
  assert.match(html, /&lt;img src=x/);
  assert.match(html, /scan truncated/);
});

// Production shape: 40 September rewards, every September batch disabled.
const disabledBatch = {
  pending_count: 40, total_reward_value: 1885, issuable_after_stock_replenishment: 0, issuable_now: 0, still_blocked: 40,
  issuance_blockers: { target_batch_disabled: 40 },
  denominations: {
    "5": { pool_id: "AFFILIATE_5", required: 6, available: 0, available_compatible: 0, shortage: 6, uploadable_shortage: 0 },
    "10": { pool_id: "AFFILIATE_10", required: 61, available: 0, available_compatible: 0, shortage: 61, uploadable_shortage: 0 },
    "50": { pool_id: "AFFILIATE_50", required: 22, available: 0, available_compatible: 0, shortage: 22, uploadable_shortage: 0 },
  },
  by_month: { "202609": { "10": { shortage: 61, uploadable_shortage: 0, historical: true, blockers: { target_batch_disabled: 61 } } } },
};

test("a disabled batch shows the demand, names the blocker, and offers no upload that cannot work", () => {
  const sb = load();
  const html = sb.affpShortageHtml(disabledBatch);
  assert.match(html, /<b>40<\/b> pending reward\(s\) affected/);
  assert.match(html, /\$1885/);
  assert.match(html, /<b>0<\/b> will become issuable after full replenishment \(0 issuable with current stock\)/);
  assert.match(html, /⚠ Historical target batch disabled: <b>40<\/b>/);
  // Required / Usable Available / Need To Upload are the real demand, not zeros.
  assert.match(html, /<td class="num">61<\/td><td class="num">0<\/td>/);
  assert.match(html, /<td class="num">22<\/td><td class="num">0<\/td>/);
  assert.match(html, /61 of this cannot be uploaded until its blocker is cleared/);
  assert.match(html, /data-denom="10" disabled>Upload \$10 Codes/);
  assert.deepEqual(JSON.parse(JSON.stringify(sb.affpShortageMonthsFor(disabledBatch, "10"))), []);
  // Retry stays available: it is a safe no-op for gated rows and finalizes the rest.
  assert.doesNotMatch(html, /data-affp-shortage-op="retry-all" disabled/);
});

test("per-row Replenish Historical Batch is not offered on the Pending Manual tab", () => {
  assert.match(JS, /status !== "PENDING_MANUAL" && affpHistoricalReplenishVisible\(it\)/);
});

// September $5/$10 stock exists in the DB but every code is expired: it must show as
// Expired (not Usable) and must not reduce Need To Upload.
const expiredStock = {
  pending_count: 92, total_reward_value: 1000, issuable_after_stock_replenishment: 92, issuable_now: 0, still_blocked: 0,
  denominations: {
    "5": { pool_id: "AFFILIATE_5", required: 9, raw_available: 9, usable_available: 0, expired_excluded: 9, available: 0, available_compatible: 0, shortage: 9, uploadable_shortage: 9 },
    "10": { pool_id: "AFFILIATE_10", required: 59, raw_available: 59, usable_available: 0, expired_excluded: 59, available: 0, available_compatible: 0, shortage: 59, uploadable_shortage: 59 },
    "50": { pool_id: "AFFILIATE_50", required: 24, raw_available: 0, usable_available: 0, expired_excluded: 0, available: 0, available_compatible: 0, shortage: 24, uploadable_shortage: 24 },
  },
  by_month: { "202609": { "5": { shortage: 9, uploadable_shortage: 9, historical: true } } },
};

test("expired stock gets its own column and never offsets Need To Upload", () => {
  const html = load().affpShortageHtml(expiredStock);
  const row = (v) => html.slice(html.indexOf("<b>$" + v + "</b>"), html.indexOf("Upload $" + v + " Codes"));
  const cells = (r) => [...r.matchAll(/<td class="num"[^>]*>(.*?)<\/td>/g)].map((m) => m[1].replace(/<div.*?<\/div>/g, "").replace(/<[^>]+>/g, ""));
  assert.deepEqual(cells(row("5")), ["9", "0", "9", "9"]);
  assert.deepEqual(cells(row("10")), ["59", "0", "59", "59"]);
  assert.deepEqual(cells(row("50")), ["24", "0", "0", "24"]);
  assert.match(html, /data-denom="5"(?! disabled)/);
});
