"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

function between(source, start, end) {
  const from = source.indexOf(start), to = source.indexOf(end, from);
  assert.ok(from >= 0 && to > from, "production renderer markers exist");
  return source.slice(from, to);
}
const admin = fs.readFileSync("static/admin-dashboard.js", "utf8");
const affiliate = fs.readFileSync("static/index.html", "utf8");
const money = fs.readFileSync("static/creator-share.html", "utf8");

function adminHelpers() {
  const context = vm.createContext({});
  vm.runInContext(between(admin, "function cdParseCount(raw)", "function cdApiError("), context);
  return context;
}

test("admin count parser rejects decimals, invalid strings and unsafe numbers", () => {
  const context = adminHelpers();
  for (const raw of ["", "20.0", "2.5", "-1", "true", true, false, "NaN", "1e2",
                     "9007199254740992", "9".repeat(400)]) {
    assert.equal(context.cdParseCount(raw), null, String(raw));
  }
  assert.equal(context.cdParseCount("30"), 30);
  assert.equal(context.cdParseCount("0"), 0);
  assert.equal(context.cdFmtJoins(null), "—");
  assert.equal(context.cdFmtConversion(null), "—");
});

const rows = [
  { rank: 1, display_name: "Manual", joins_count: 30, qualified_count: 20, conversion_rate: 0.6667 },
  { rank: 2, display_name: "Genuine", joins_count: 10, qualified_count: 4, conversion_rate: 0.4 },
  { rank: 3, display_name: "Legacy", joins_count: null, qualified_count: 3, conversion_rate: null },
  { rank: 4, display_name: "Zero", joins_count: 0, qualified_count: 0, conversion_rate: null },
];

test("Affiliate campaign renderer preserves genuine, manual, legacy and zero counts", () => {
  const context = vm.createContext({ escapeHtml: String, t: String });
  vm.runInContext(between(affiliate, "function renderAffiliateRows(", "async function loadAffiliateSnapshot("), context);
  const rendered = context.renderAffiliateRows(rows, { simpleColumns: true });
  for (const values of [
    ["Manual", 20, 30, "66.7%"], ["Genuine", 4, 10, "40.0%"],
    ["Legacy", 3, "—", "—"], ["Zero", 0, 0, "—"],
  ]) {
    const pattern = values.map(value => `<td>${value}</td>`).join("\\s*");
    assert.match(rendered, new RegExp(pattern));
  }
});

test("Money Room limits combined rows and restores genuine-only rows", () => {
  const genuine = Array.from({ length: 10 }, (_, index) => ({
    rank: index + 2, display_name: "Dup", joins_count: 10,
    qualified_count: 4, conversion_rate: 0.4,
  }));
  const combined = [rows[0], ...genuine];
  const before = JSON.stringify(combined);
  const active = moneyRenderer();
  active.context.renderCampaignActivityRows(combined);
  assert.equal(active.list.children.length, 10);
  assert.match(active.list.text(), /20 qualified · 30 joins/);
  assert.equal(JSON.stringify(combined), before);
  const restored = moneyRenderer();
  restored.context.renderCampaignActivityRows(genuine);
  assert.equal(restored.list.children.length, 10);
  assert.doesNotMatch(restored.list.text(), /20 qualified · 30 joins/);
});

class Element {
  constructor() { this.children = []; this.textContent = ""; }
  appendChild(child) { this.children.push(child); }
  text() { return this.textContent + this.children.map(child => child.text()).join(""); }
}
function moneyRenderer() {
  const list = new Element();
  const context = vm.createContext({
    els: { campaignActivityList: list }, CAMPAIGN_ACTIVITY_TOP_N: 10,
    document: {
      createElement: () => new Element(),
      createTextNode: text => Object.assign(new Element(), { textContent: text }),
    },
  });
  vm.runInContext(between(money, "function renderCampaignActivityRows(", "function loadCampaignActivity("), context);
  return { context, list };
}

test("Money Room renders exact entered values and unknowns using production renderer", () => {
  const { context, list } = moneyRenderer();
  context.renderCampaignActivityRows(rows);
  assert.equal(list.children.length, 4);
  assert.match(list.text(), /Manual20 qualified · 30 joins · 66\.7%/);
  assert.match(list.text(), /Genuine4 qualified · 10 joins · 40\.0%/);
  assert.match(list.text(), /Legacy3 qualified · — joins · —/);
  assert.match(list.text(), /Zero0 qualified · 0 joins · —/);
});

test("duplicate display names remain separate public rows", () => {
  const { context, list } = moneyRenderer();
  context.renderCampaignActivityRows(rows.slice(0, 2).map(row => ({ ...row, display_name: "Duplicate" })));
  assert.equal(list.children.length, 2);
  assert.match(list.children[0].text(), /20 qualified · 30 joins/);
  assert.match(list.children[1].text(), /4 qualified · 10 joins/);
});

test("both admin forms and public campaign surfaces identify their display counts", () => {
  const html = fs.readFileSync("static/admin-dashboard.html", "utf8");
  assert.match(html, /id="cd-p-joins"/);
  assert.match(admin, /id="cd-pe-joins"/);
  assert.match(affiliate, /Display only; reward progress uses genuine referrals/);
  assert.match(money, /Campaign display only; reward progress uses genuine referrals/);
});
