/**
 * "My Stats (This Month)" compact block + affiliate reward retention status
 * in static/index.html (loadAffiliateLeaderboard).
 *
 * Tier thresholds / reward values are owned by the backend
 * (affiliate_rewards.affiliate_next_tier_progress); the frontend must only
 * render next_tier / qualified_left / next_reward_value / max_tier_reached.
 *
 * Run with: node --test test_affiliate_my_stats_frontend.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");

function slice(startMarker, endMarker) {
  const start = html.indexOf(startMarker);
  const end = html.indexOf(endMarker, start);
  assert.ok(start !== -1, `${startMarker} not found`);
  assert.ok(end !== -1, `${endMarker} not found`);
  return html.slice(start, end);
}

function loadRenderers() {
  const src = slice("function formatAffiliateUnlockRemaining(seconds) {", "async function loadAffiliateSnapshot(weekKey) {");
  const ctx = {
    escapeHtml: (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"),
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx);
  return ctx;
}

// Build the myCard markup exactly as loadAffiliateLeaderboard does, from the
// real source between the progress comment and the campaign banner.
function renderMyCard(my) {
  const src = slice("// Tier progress comes entirely from the backend", "const campaignBanner = campaignActive");
  const ctx = loadRenderers();
  ctx.my = my;
  ctx.monthLabel = "2026-09";
  ctx.t = (k) => k;
  vm.runInContext(`${src}\nthis.__card = myCard;`, ctx);
  return ctx.__card;
}

function rowLabels(markup) {
  return [...markup.matchAll(/<div class="summary-row"><span>([^<]+)/g)].map((m) => m[1]);
}

function rowValue(markup, label) {
  const re = new RegExp(`<span>${label}</span><strong>([^<]*)</strong>`);
  const m = markup.match(re);
  return m ? m[1] : null;
}

test("compact block renders the target rows in order", () => {
  const card = renderMyCard({
    qualified_month: 18, joins_month: 32, next_tier: "T2", qualified_left: 7,
    next_reward_value: 25, max_tier_reached: false, conversion_month: 0.56, quality_flag: "ok",
    reward_entitlements: [],
  });
  assert.match(card, /My Stats \(This Month\)/);
  assert.deepEqual(rowLabels(card), ["Month", "Qualified", "Left to Next Tier", "Joins", "Next Tier", "Reward"]);
  assert.equal(rowValue(card, "Month"), "2026-09");
  assert.equal(rowValue(card, "Qualified"), "18");
  assert.equal(rowValue(card, "Left to Next Tier"), "7");
  assert.equal(rowValue(card, "Joins"), "32");
  assert.equal(rowValue(card, "Next Tier"), "T2");
  assert.equal(rowValue(card, "Reward"), "$25");
  assert.doesNotMatch(card, /Conversion|Flag|Resets/);
});

test("max tier shows MAX, a dash for remaining, and the max-tier reward", () => {
  const card = renderMyCard({
    qualified_month: 260, joins_month: 300, next_tier: "MAX", qualified_left: null,
    next_reward_value: 350, max_tier_reached: true, reward_entitlements: [],
  });
  assert.equal(rowValue(card, "Next Tier"), "MAX");
  assert.equal(rowValue(card, "Left to Next Tier"), "—");
  assert.equal(rowValue(card, "Reward"), "$350");
});

test("a negative remaining value from any source is never shown", () => {
  const card = renderMyCard({ qualified_month: 30, joins_month: 1, next_tier: "T3", qualified_left: -4, next_reward_value: 60 });
  assert.equal(rowValue(card, "Left to Next Tier"), "0");
});

test("frontend does not restate tier thresholds", () => {
  const src = slice("// Tier progress comes entirely from the backend", "const campaignBanner = campaignActive");
  for (const needle of ["THRESHOLD", ">= 25", ">= 50", ">= 150", ">= 250", "- my.qualified_month"]) {
    assert.ok(!src.includes(needle), `threshold logic leaked into frontend: ${needle}`);
  }
});

test("entitlement status copy for every retention state", () => {
  const { renderAffiliateRewardEntitlements: render } = loadRenderers();
  const base = { tier: "T2", reward_value: 25, retention_days: 7 };
  assert.match(
    render([{ ...base, retention_state: "pending_retention", remaining_seconds: 7 * 86400 }]),
    /T2 Reward<br>\$25<\/span><strong>Unlocks after 7 continuous days subscribed/,
  );
  assert.match(
    render([{ ...base, retention_state: "pending_retention", remaining_seconds: 4 * 86400 + 12 * 3600 }]),
    /Unlocks in 4d 12h/,
  );
  assert.match(
    render([{ ...base, retention_state: "pending_retention", remaining_seconds: 6 * 86400 + 23 * 3600 - 60 }]),
    /Unlocks in 6d 22h/,
  );
  assert.match(
    render([{ ...base, retention_state: "pending_retention", remaining_seconds: 6 * 86400 + 23 * 3600 + 1800, window_restarted: true }]),
    /Unlocks in 6d 23h/,
  );
  assert.match(
    render([{ ...base, retention_state: "retention_broken", remaining_seconds: null }]),
    /Retention reset<br>Rejoin the Official Channel to restart the 7-day unlock/,
  );
  assert.match(render([{ ...base, retention_state: "issued" }]), /<strong>Issued<\/strong>/);
  assert.match(render([{ ...base, retention_state: "pending_retention", remaining_seconds: 0 }]), /Unlocking soon/);
  assert.equal(render(undefined), "");
  assert.equal(render([]), "");
});
