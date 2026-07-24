/**
 * Tests for the "Your Affiliate Rewards" card renderer (renderAffiliateRewards)
 * in static/index.html. Backend filtering (welcome_reward_visibility /
 * /api/affiliate_bonus_vouchers) is authoritative, but the frontend must also
 * defensively refuse to render a card once its expires_at has passed, so a
 * stale/cached API response never re-displays an expired WELCOME voucher.
 *
 * Run with: node --test test_affiliate_welcome_rewards_frontend.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "function renderAffiliateRewards(rewards) {";
const END_MARKER = "function copyCampaignVoucherCode() {";

function loadFunctionSource() {
  const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "renderAffiliateRewards not found in static/index.html");
  assert.ok(end !== -1, "copyCampaignVoucherCode end marker not found in static/index.html");
  return html.slice(start, end);
}

function makeListEl() {
  const listEl = {
    appendChild(child) { listEl._children.push(child); },
  };
  listEl._children = [];
  Object.defineProperty(listEl, "innerHTML", {
    set() { listEl._children = []; },
    get() { return ""; },
  });
  Object.defineProperty(listEl, "children", {
    get() { return listEl._children; },
  });
  return listEl;
}

function makeSandbox() {
  const bonusSection = { style: {} };
  const listEl = makeListEl();
  const document = {
    getElementById(id) {
      if (id === "bonus-voucher") return bonusSection;
      if (id === "affiliate-voucher-list") return listEl;
      return null;
    },
    createElement(tag) {
      return { tagName: tag, className: "", textContent: "", dataset: {}, style: {}, onclick: null, appendChild() {} };
    },
  };
  const sandbox = {
    document,
    t: (key) => key,
    fmtKL: (v) => String(v),
    console,
  };
  vm.createContext(sandbox);
  const src = loadFunctionSource() + "\nthis.__renderAffiliateRewards = renderAffiliateRewards;";
  vm.runInContext(src, sandbox);
  return { sandbox, bonusSection, listEl };
}

test("hides a card whose expires_at has already passed (stale cached response)", () => {
  const { sandbox, listEl, bonusSection } = makeSandbox();
  const past = new Date(Date.now() - 60_000).toISOString();
  sandbox.__renderAffiliateRewards([{ tier: "WELCOME", code: "OLD-WELCOME", expires_at: past }]);
  assert.equal(listEl.children.length, 0);
  assert.equal(bonusSection.style.display, "none");
});

test("renders a card whose expires_at is still in the future", () => {
  const { sandbox, listEl } = makeSandbox();
  const future = new Date(Date.now() + 60_000).toISOString();
  sandbox.__renderAffiliateRewards([{ tier: "WELCOME", code: "FRESH-WELCOME", expires_at: future }]);
  assert.equal(listEl.children.length, 1);
});

test("renders a mixed payload, filtering only the expired card", () => {
  const { sandbox, listEl } = makeSandbox();
  const past = new Date(Date.now() - 60_000).toISOString();
  const future = new Date(Date.now() + 60_000).toISOString();
  sandbox.__renderAffiliateRewards([
    { tier: "WELCOME", code: "OLD-WELCOME", expires_at: past },
    { tier: "WELCOME", code: "FRESH-WELCOME", expires_at: future },
  ]);
  assert.equal(listEl.children.length, 1);
});

test("T1-T4 rewards without expires_at are unaffected by the defensive filter", () => {
  const { sandbox, listEl } = makeSandbox();
  sandbox.__renderAffiliateRewards([{ tier: "T1", code: "TIER-CODE" }]);
  assert.equal(listEl.children.length, 1);
});
