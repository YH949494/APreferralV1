/**
 * Campaign Rewards widget — Mission winner popup, reward highlight, and
 * backwards compatibility with existing reward rows (Phase 2, §15-§21).
 *
 * The point of the compatibility half of this file: Mission Pool adds
 * OPTIONAL keys to /api/campaign-rewards/me. A tournament or legacy row that
 * carries none of them must render through the exact same path it always
 * did, and must never trigger a popup or a highlight.
 *
 * Run with: node --test test_mission_winner_popup.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const SOURCE = fs.readFileSync(path.join(__dirname, "static", "campaign-centre-widget.js"), "utf8");

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
  const head = makeNode("head");
  const bodyEl = makeNode("body");
  const byId = { "campaign-centre-root": root };
  const calls = [];
  const posts = [];

  const document = {
    readyState: "complete",
    head,
    body: bodyEl,
    getElementById(id) {
      if (byId[id]) return byId[id];
      // Reward cards are registered as they are created with an id.
      return findWithId(root, id) || findWithId(bodyEl, id) || null;
    },
    createElement(tag) { return makeNode(tag); },
    addEventListener() {},
    dispatchEvent() { return true; },
  };

  function findWithId(node, id) {
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
  };
  sandbox.window.document = document;
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(SOURCE, sandbox);

  return {
    sandbox, root, bodyEl, calls, posts,
    api: sandbox.window.CampaignCentreWidget,
    events: sandbox.window.MissionPoolEvents,
    findWithId: (id) => findWithId(root, id) || findWithId(bodyEl, id),
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
  // Exactly the shape that shipped before Mission Pool existed.
  return Object.assign({
    reward_id: "rw_t_1",
    category: "tournament",
    campaign_id: "tourney-1",
    campaign_name: "July Tournament",
    tournament_id: "t1",
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
    tournament_id: null,
    rank: null,
    reward_label: "RM10 Voucher",
    voucher_code: "MISS-XYZ",
    assigned_at: "2026-09-01T00:00:00+00:00",
    expires_at: null,
    status: "assigned",
    mechanic: "mission_pool",
    is_winner: true,
    winner_popup_pending: true,
    notification_status: "sent",
  }, overrides || {});
}

function mountWith(rewards) {
  return loadWidget({
    routes: {
      [CAMPAIGNS_ROUTE]: () => ({ status: "ok", campaigns: [] }),
      [REWARDS_ROUTE]: () => ({ status: "ok", rewards }),
    },
  });
}

// ---------------------------------------------------------------------------
// Existing reward compatibility (§21, §49)
// ---------------------------------------------------------------------------

test("a tournament reward row renders exactly as before", async () => {
  const w = mountWith([tournamentReward()]);
  await tick();
  assert.ok(findByText(w.root, "🏆 July Tournament"));
  assert.ok(findByText(w.root, "Rank #2 — RM50 Voucher"));
  assert.ok(findByText(w.root, "TOUR-AAA"));
  assert.ok(findByText(w.root, "Copy Code"));
});

test("a legacy row with no mission fields at all still renders", async () => {
  const legacy = tournamentReward();
  delete legacy.mechanic;
  delete legacy.is_winner;
  delete legacy.winner_popup_pending;
  delete legacy.notification_status;
  const w = mountWith([legacy]);
  await tick();
  assert.ok(findByText(w.root, "🏆 July Tournament"));
  assert.equal(w.findWithId("cc-winner-popup"), null, "no popup for a legacy reward");
});

test("a tournament reward never triggers a winner popup or a highlight", async () => {
  const w = mountWith([tournamentReward()]);
  await tick();
  assert.equal(w.findWithId("cc-winner-popup"), null);
  assert.equal(w.events.length, 0);
});

test("copy still posts to the existing copy endpoint", async () => {
  const w = mountWith([tournamentReward()]);
  await tick();
  findByText(w.root, "Copy Code").click();
  await tick();
  assert.ok(w.posts.indexOf("/api/campaign-rewards/rw_t_1/copy") !== -1);
});

test("mission and legacy rewards render side by side", async () => {
  const w = mountWith([missionReward(), tournamentReward()]);
  await tick();
  assert.ok(findByText(w.root, "🎯 Mission Winner"));
  assert.ok(findByText(w.root, "🏆 July Tournament"));
  assert.ok(findByText(w.root, "MISS-XYZ"));
  assert.ok(findByText(w.root, "TOUR-AAA"));
});

// ---------------------------------------------------------------------------
// Mission reward card (§19)
// ---------------------------------------------------------------------------

test("a mission reward card carries the Mission Winner label, not a rank", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  assert.ok(findByText(w.root, "🎯 Mission Winner"));
  assert.ok(findByText(w.root, "Summer Quiz"));
  assert.equal(findByText(w.root, "Rank #"), null, "a mission win has no rank");
});

test("mission rewards live in the one Campaign Rewards section", async () => {
  // §19: no second "Mission Rewards" / "Winner Wallet" section is created.
  const w = mountWith([missionReward()]);
  await tick();
  const titles = allText(w.root).filter((t) => /Rewards$/.test(t));
  assert.deepEqual(titles, ["Campaign Rewards"]);
});

// ---------------------------------------------------------------------------
// Winner popup (§15-§18)
// ---------------------------------------------------------------------------

test("a pending mission win shows the winner popup", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  const popup = w.findWithId("cc-winner-popup");
  assert.ok(popup, "popup must appear for winner_popup_pending");
  assert.ok(findByText(popup, "🎉 Congratulations!"));
  assert.ok(findByText(popup, "Summer Quiz"));
  assert.ok(findByText(popup, "🎁 Redeem Now"));
  assert.ok(findByText(popup, "Later"));
});

test("the popup never renders a voucher code", async () => {
  // §16: Campaign Rewards stays the canonical reward surface.
  const w = mountWith([missionReward()]);
  await tick();
  const text = allText(w.findWithId("cc-winner-popup")).join(" ");
  assert.equal(text.indexOf("MISS-XYZ"), -1);
});

test("an already-acknowledged win shows no popup", async () => {
  const w = mountWith([missionReward({ winner_popup_pending: false })]);
  await tick();
  assert.equal(w.findWithId("cc-winner-popup"), null);
});

test("Redeem Now acknowledges, closes, and highlights the exact reward", async () => {
  const w = mountWith([tournamentReward(), missionReward()]);
  await tick();
  const popup = w.findWithId("cc-winner-popup");
  findByText(popup, "🎁 Redeem Now").click();
  await tick();
  assert.equal(w.findWithId("cc-winner-popup"), null, "popup must close");
  assert.ok(w.posts.indexOf("/api/campaign-rewards/rw_mp_abc/ack-popup") !== -1);
  const card = w.findWithId("cc-reward-rw_mp_abc");
  assert.ok(card.classList.contains("cc-highlight"), "the mission reward is highlighted");
  assert.ok(card._scrolled, "the mission reward is scrolled into view");
  const other = w.findWithId("cc-reward-rw_t_1");
  assert.equal(other.classList.contains("cc-highlight"), false, "only the mission reward is highlighted");
});

test("Later also acknowledges, so the popup does not return every open", async () => {
  // §17: both actions acknowledge — a winner is congratulated once.
  const w = mountWith([missionReward()]);
  await tick();
  findByText(w.findWithId("cc-winner-popup"), "Later").click();
  await tick();
  assert.equal(w.findWithId("cc-winner-popup"), null);
  assert.ok(w.posts.indexOf("/api/campaign-rewards/rw_mp_abc/ack-popup") !== -1);
});

test("Later does not highlight anything", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  findByText(w.findWithId("cc-winner-popup"), "Later").click();
  await tick();
  assert.equal(w.findWithId("cc-reward-rw_mp_abc").classList.contains("cc-highlight"), false);
});

test("a failed acknowledgement still closes the popup and keeps the voucher usable", async () => {
  // §17: never hide the voucher, never revoke the reward, never re-loop.
  const w = loadWidget({
    routes: {
      [CAMPAIGNS_ROUTE]: () => ({ status: "ok", campaigns: [] }),
      [REWARDS_ROUTE]: () => ({ status: "ok", rewards: [missionReward()] }),
      "POST /api/campaign-rewards/rw_mp_abc/ack-popup": () => "FAIL",
    },
  });
  await tick();
  findByText(w.findWithId("cc-winner-popup"), "🎁 Redeem Now").click();
  await tick();
  assert.equal(w.findWithId("cc-winner-popup"), null);
  assert.ok(findByText(w.root, "MISS-XYZ"), "the voucher code stays visible");
  assert.ok(findByText(w.root, "Copy Code"), "the voucher stays copyable");
});

test("multiple pending wins never stack modals — oldest first, one per session", async () => {
  // §18: one popup per Mini App session, oldest pending reward.
  const older = missionReward({ reward_id: "rw_mp_old", campaign_name: "Older Mission", assigned_at: "2026-08-01T00:00:00+00:00" });
  const newer = missionReward({ reward_id: "rw_mp_new", campaign_name: "Newer Mission", assigned_at: "2026-09-05T00:00:00+00:00" });
  const w = mountWith([newer, older]);
  await tick();
  const popups = [];
  (function walk(n) { if (n.id === "cc-winner-popup") popups.push(n); (n._children || []).forEach(walk); }(w.bodyEl));
  assert.equal(popups.length, 1, "exactly one modal");
  assert.ok(findByText(popups[0], "Older Mission"), "the oldest pending win is shown first");
});

test("a second maybeShowWinnerPopup in the same session is a no-op", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  findByText(w.findWithId("cc-winner-popup"), "Later").click();
  const again = w.api.maybeShowWinnerPopup([missionReward()]);
  assert.equal(again, null, "the popup does not repeat within one session");
});

test("popup lifecycle emits its documented events", async () => {
  const w = mountWith([missionReward()]);
  await tick();
  findByText(w.findWithId("cc-winner-popup"), "🎁 Redeem Now").click();
  await tick();
  const names = w.events.map((e) => e.event);
  assert.ok(names.indexOf("mission_winner_popup_shown") !== -1);
  assert.ok(names.indexOf("mission_winner_popup_acknowledged") !== -1);
  assert.ok(names.indexOf("mission_reward_highlighted") !== -1);
  const serialized = JSON.stringify(w.events);
  assert.equal(serialized.indexOf("MISS-XYZ"), -1, "no voucher code in an event payload");
});

// ---------------------------------------------------------------------------
// Highlight (§20)
// ---------------------------------------------------------------------------

test("the highlight is temporary and leaves no permanent style change", async () => {
  const w = mountWith([missionReward({ winner_popup_pending: false })]);
  await tick();
  w.api.highlightRewardById("rw_mp_abc");
  const card = w.findWithId("cc-reward-rw_mp_abc");
  assert.ok(card.classList.contains("cc-highlight"));
  await new Promise((r) => setTimeout(r, 3100));
  assert.equal(card.classList.contains("cc-highlight"), false, "the highlight must clear itself");
});

test("highlightRewardForCampaign resolves the campaign to its own reward", async () => {
  const w = mountWith([tournamentReward(), missionReward({ winner_popup_pending: false })]);
  await tick();
  assert.equal(w.api.highlightRewardForCampaign("m1"), true);
  assert.ok(w.findWithId("cc-reward-rw_mp_abc").classList.contains("cc-highlight"));
  assert.equal(w.findWithId("cc-reward-rw_t_1").classList.contains("cc-highlight"), false);
});

test("highlighting an unknown campaign falls back to the section, not an error", async () => {
  const w = mountWith([missionReward({ winner_popup_pending: false })]);
  await tick();
  assert.equal(w.api.highlightRewardForCampaign("not-mine"), false);
  assert.ok(w.root._scrolled);
});

test("reward dom ids are sanitised", () => {
  const w = loadWidget({ routes: {} });
  assert.equal(w.api.rewardDomId({ reward_id: "rw_mp_abc" }), "cc-reward-rw_mp_abc");
  assert.equal(w.api.rewardDomId({ reward_id: 'a"><img>' }), "cc-reward-aimg");
});

test("isMissionReward keys off the server category only", () => {
  const w = loadWidget({ routes: {} });
  assert.equal(w.api.isMissionReward({ category: "mission_pool" }), true);
  assert.equal(w.api.isMissionReward({ category: "tournament" }), false);
  // Never inferred from a campaign name or a mechanic hint alone.
  assert.equal(w.api.isMissionReward({ campaign_name: "Mission Quiz" }), false);
  assert.equal(w.api.isMissionReward({ mechanic: "mission_pool", category: "tournament" }), false);
});

// ---------------------------------------------------------------------------
// Section behaviour unchanged (§50)
// ---------------------------------------------------------------------------

test("an empty rewards response hides the section entirely, as before", async () => {
  const w = mountWith([]);
  await tick();
  assert.equal(w.root.children.length, 0);
  assert.equal(w.findWithId("cc-winner-popup"), null);
});
