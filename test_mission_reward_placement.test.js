/**
 * Mission Reward Placement + 48h Expiry follow-up — frontend placement
 * (§4, §5, §8, §23 of the follow-up spec).
 *
 * Mission winner rewards must render directly under the existing Live Drop
 * area (#cc-mission-rewards-root, added to index.html right after
 * #voucher-section) rather than inside the generic "Campaign Rewards"
 * section, and with no standalone "Mission Rewards" heading of their own.
 * Every other reward category is untouched — it keeps rendering in the one
 * Campaign Rewards section exactly as before (asserted in
 * test_mission_winner_popup.test.js already; not repeated here).
 *
 * Run with: node --test test_mission_reward_placement.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const SOURCE = fs.readFileSync(path.join(__dirname, "static", "campaign-centre-widget.js"), "utf8");
const INDEX_HTML = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");

function makeNode(tag) {
  const node = {
    tagName: String(tag || "div").toUpperCase(),
    className: "",
    style: {},
    attributes: {},
    _children: [],
    _listeners: {},
    textContent: "",
    parentNode: null,
    appendChild(child) { child.parentNode = node; node._children.push(child); return child; },
    removeChild(child) {
      node._children = node._children.filter((c) => c !== child);
      child.parentNode = null;
    },
    setAttribute(k, v) { node.attributes[k] = String(v); if (k === "id") node.id = String(v); },
    getAttribute(k) { return node.attributes[k]; },
    addEventListener(type, fn) { (node._listeners[type] = node._listeners[type] || []).push(fn); },
    click() { (node._listeners.click || []).forEach((fn) => fn({})); },
    scrollIntoView() { node._scrolled = true; },
    classList: {
      add(c) { node.className += (node.className ? " " : "") + c; },
      remove(c) { node.className = node.className.split(" ").filter((x) => x !== c).join(" "); },
      contains(c) { return node.className.split(" ").indexOf(c) !== -1; },
    },
  };
  Object.defineProperty(node, "children", { get: () => node._children });
  Object.defineProperty(node, "innerHTML", { get: () => "", set(v) { if (!v) node._children = []; } });
  return node;
}

function allText(node, out) {
  out = out || [];
  if (node.textContent) out.push(node.textContent);
  (node._children || []).forEach((c) => allText(c, out));
  return out;
}

function findByText(node, needle) {
  if (String(node.textContent || "").indexOf(needle) !== -1) return node;
  for (const child of node._children || []) {
    const hit = findByText(child, needle);
    if (hit) return hit;
  }
  return null;
}

function loadWidget(opts) {
  opts = opts || {};
  const root = makeNode("div");
  root.id = "campaign-centre-root";
  const voucherSection = makeNode("div");
  voucherSection.id = "voucher-section";
  voucherSection.textContent = "LIVE DROP";
  const hasMissionRoot = opts.withMissionRoot !== false;
  const missionRoot = hasMissionRoot ? makeNode("div") : null;
  if (missionRoot) missionRoot.id = "cc-mission-rewards-root";
  const head = makeNode("head");
  const bodyEl = makeNode("body");
  const byId = {
    "campaign-centre-root": root,
    "voucher-section": voucherSection,
  };
  if (missionRoot) byId["cc-mission-rewards-root"] = missionRoot;
  const calls = [];
  const posts = [];

  const document = {
    readyState: "complete",
    head,
    body: bodyEl,
    getElementById(id) {
      if (byId[id]) return byId[id];
      return findWithId(root, id) || findWithId(bodyEl, id) || findWithId(missionRoot, id) || null;
    },
    createElement(tag) { return makeNode(tag); },
    addEventListener() {},
    dispatchEvent() { return true; },
  };

  function findWithId(node, id) {
    if (!node) return null;
    if (node.id === id) return node;
    for (const child of node._children || []) {
      const hit = findWithId(child, id);
      if (hit) return hit;
    }
    return null;
  }

  const sandbox = {
    document,
    console,
    setTimeout,
    clearTimeout,
    URLSearchParams,
    CustomEvent: function (type, init) { return { type, detail: (init || {}).detail }; },
    fetch(url, init) {
      const method = (init && init.method) || "GET";
      const clean = url.split("?")[0];
      calls.push(method + " " + clean);
      if (method === "POST") posts.push(clean);
      const handler = (opts.routes || {})[method + " " + clean];
      const payload = handler ? handler() : { status: "ok" };
      if (payload === "FAIL") return Promise.reject(new Error("network"));
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(payload) });
    },
    window: { Telegram: { WebApp: { initData: "signed", initDataUnsafe: {} } }, MissionPoolEvents: [] },
    location: { search: "" },
    navigator: { clipboard: { writeText() {} } },
    Date, Math,
  };
  sandbox.window.document = document;
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(SOURCE, sandbox);

  return {
    sandbox, root, bodyEl, missionRoot, voucherSection, calls, posts,
    api: sandbox.window.CampaignCentreWidget,
    events: sandbox.window.MissionPoolEvents,
    findWithId: (id) => findWithId(root, id) || findWithId(bodyEl, id) || findWithId(missionRoot, id),
  };
}

function tick(times) {
  let p = Promise.resolve();
  for (let i = 0; i < (times || 8); i++) p = p.then(() => {});
  return p;
}

const CAMPAIGNS_ROUTE = "GET /api/campaigns/active";
const REWARDS_ROUTE = "GET /api/campaign-rewards/me";

function tournamentReward(overrides) {
  return Object.assign({
    reward_id: "rw_t_1",
    category: "tournament",
    campaign_id: "tourney-1",
    campaign_name: "July Tournament",
    rank: 2,
    reward_label: "RM50 Voucher",
    voucher_code: "TOUR-AAA",
    assigned_at: "2026-08-01T00:00:00+00:00",
    expires_at: null,
    status: "assigned",
  }, overrides || {});
}

function missionReward(overrides) {
  return Object.assign({
    reward_id: "rw_mp_abc",
    category: "mission_pool",
    campaign_id: "m1",
    campaign_name: "Summer Quiz",
    reward_label: "RM10 Voucher",
    voucher_code: "MISS-XYZ",
    assigned_at: new Date().toISOString(),
    expires_at: new Date(Date.now() + 47 * 3600 * 1000 + 20 * 60 * 1000).toISOString(),
    status: "assigned",
    mechanic: "mission_pool",
    is_winner: true,
    winner_popup_pending: false,
    notification_status: "sent",
  }, overrides || {});
}

function mountWith(rewards, opts) {
  return loadWidget(Object.assign({
    routes: {
      [CAMPAIGNS_ROUTE]: () => ({ status: "ok", campaigns: [] }),
      [REWARDS_ROUTE]: () => ({ status: "ok", rewards }),
    },
  }, opts || {}));
}

// ---------------------------------------------------------------------------
// Placement (§4, §5)
// ---------------------------------------------------------------------------

test("a mission reward renders inside the mount point placed under Live Drop", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  const card = w.findWithId("cc-reward-rw_mp_abc");
  assert.ok(card, "mission reward card must exist");
  assert.ok(w.missionRoot._children.indexOf(card) !== -1, "mission card must be a child of cc-mission-rewards-root");
});

test("no standalone Mission Rewards / Winner Rewards heading exists", async () => {
  const w = mountWith([missionReward(), tournamentReward()]);
  await tick();
  const titles = allText(w.root).concat(allText(w.missionRoot));
  assert.equal(titles.indexOf("Mission Rewards"), -1);
  assert.equal(titles.indexOf("Winner Rewards"), -1);
  assert.equal(titles.indexOf("Mission Wallet"), -1);
  // The one generic title that does exist covers non-mission rewards only.
  const sectionTitles = titles.filter((t) => /Rewards$/.test(t));
  assert.deepEqual(sectionTitles, ["Campaign Rewards"]);
});

test("non-mission rewards still render in the existing Campaign Rewards section, not under Live Drop", async () => {
  const w = mountWith([tournamentReward()]);
  await tick();
  const card = w.findWithId("cc-reward-rw_t_1");
  assert.ok(card);
  assert.equal(w.missionRoot._children.indexOf(card), -1, "tournament reward must not move under Live Drop");
});

test("mission and non-mission rewards can both be present without interfering", async () => {
  const w = mountWith([missionReward(), tournamentReward()]);
  await tick();
  assert.ok(w.findWithId("cc-reward-rw_mp_abc"));
  assert.ok(w.findWithId("cc-reward-rw_t_1"));
  assert.ok(w.missionRoot._children.indexOf(w.findWithId("cc-reward-rw_mp_abc")) !== -1);
  assert.ok(w.missionRoot._children.indexOf(w.findWithId("cc-reward-rw_t_1")) === -1);
});

test("existing Live Drop content is untouched by the mission mount point", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  assert.equal(w.voucherSection.textContent, "LIVE DROP");
});

test("falls back to the Campaign Rewards section when the dedicated mount point is absent (older cached page)", async () => {
  const w = mountWith([missionReward()], { withMissionRoot: false });
  await tick();
  assert.ok(findByText(w.root, "🎯 Mission Winner"), "mission reward must still render somewhere, never dropped");
});

// ---------------------------------------------------------------------------
// Copy button + expiry text (§4, §8)
// ---------------------------------------------------------------------------

test("existing Mission reward copy button still works under the new placement", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  findByText(w.missionRoot, "Copy Code").click();
  await tick();
  assert.ok(w.posts.indexOf("/api/campaign-rewards/rw_mp_abc/copy") !== -1);
});

test("expiry text renders for a reward under 48h from now", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  const card = w.findWithId("cc-reward-rw_mp_abc");
  const text = allText(card).join(" ");
  assert.match(text, /Expires in 4[67]h/);
});

test("no expiry text for a legacy reward without expires_at", async () => {
  const w = mountWith([missionReward({ expires_at: null })]);
  await tick();
  const card = w.findWithId("cc-reward-rw_mp_abc");
  const text = allText(card).join(" ");
  assert.equal(text.indexOf("Expires in"), -1);
});

test("an already-expired reward is simply absent (server-side filtered)", async () => {
  // The server never returns an expired reward at all — this just confirms
  // the widget does not need special-case handling when the list is empty.
  const w = mountWith([]);
  await tick();
  assert.equal(w.findWithId("cc-reward-rw_mp_abc"), null);
});

test("a normal Mini App with no mission reward is unaffected", async () => {
  const w = mountWith([]);
  await tick();
  assert.equal(w.root.children.length, 0);
  assert.equal(w.missionRoot._children.length, 0);
});

// ---------------------------------------------------------------------------
// index.html markup (§4, §5, §23)
// ---------------------------------------------------------------------------

test("index.html places the mission reward mount point directly after #voucher-section", () => {
  const voucherCloseIdx = INDEX_HTML.indexOf('id="cc-mission-rewards-root"');
  const voucherOpenIdx = INDEX_HTML.indexOf('id="voucher-section"');
  const campaignsSectionIdx = INDEX_HTML.indexOf('id="ap-campaigns-section"');
  assert.ok(voucherOpenIdx !== -1 && voucherCloseIdx !== -1 && campaignsSectionIdx !== -1);
  assert.ok(voucherOpenIdx < voucherCloseIdx, "mount point must come after Live Drop opens");
  assert.ok(voucherCloseIdx < campaignsSectionIdx, "mount point must come before the Active Campaigns section");
});

test("index.html has no standalone Mission Rewards heading", () => {
  assert.equal(/>\s*Mission Rewards\s*</.test(INDEX_HTML), false);
  assert.equal(/>\s*Winner Rewards\s*</.test(INDEX_HTML), false);
});
