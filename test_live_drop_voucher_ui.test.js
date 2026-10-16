/**
 * Live Drop compact voucher row (single-voucher reward slot) tests.
 *
 * Problem: when a user had an available voucher, the Live Drop hero card
 * rendered another complete campaign/drop card (title + schedule +
 * description + Claim panel) nested inside it via createCampaignCard(),
 * duplicating everything the hero already shows. With multiple vouchers
 * this stacked into several large cards.
 *
 * Fix (static/index.html, renderCampaignDrops / renderCompactVoucherRow):
 * when one or more active-and-unclaimed vouchers exist, the hero shows a
 * single compact reward row (channel-requirement status + Claim button)
 * instead of the nested card, plus a "N more reward(s) in My Rewards ›"
 * link when extra active vouchers exist. The drop/campaign name is never
 * repeated in this row — it already renders once, above, as #ap-hero-title.
 * All other vouchers stay fully visible/claimable in the existing extras
 * section (My Rewards, renamed from "past drops").
 *
 * Live Drop hero simplification: the badge reads "LIVE" (no separate
 * "LIMITED DROP" kicker), there's a single channel-requirement row
 * ("✓ Joined Official Channel" / "○ Join Official Channel ›") directly
 * above the Claim button, and Claim now stays disabled until that channel
 * check (checkChannelSubscription) resolves subscribed — the same check
 * re-runs on `visibilitychange` so returning to the Mini App after joining
 * the channel refreshes the gate.
 *
 * The functions live inline in static/index.html (no build step), so they
 * are extracted as source text and executed in a sandboxed vm context with
 * mocked DOM/fetch/timer globals, mirroring the approach already used by
 * test_rejoin_buffer_claim_ui.test.js and test_mission_reward_placement.test.js.
 *
 * Run with: node --test test_live_drop_voucher_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "    function resetVoucherButton() {";
const END_MARKER =
  'document.addEventListener("click", async (e) => {\n      const btn = e.target.closest(".copy-drop-id-btn");';

const INDEX_HTML = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");

function loadSource() {
  const start = INDEX_HTML.indexOf(START_MARKER);
  assert.ok(start !== -1, "resetVoucherButton not found in static/index.html");
  const end = INDEX_HTML.indexOf(END_MARKER, start);
  assert.ok(end !== -1, "end-of-claim-handler marker not found in static/index.html");
  return INDEX_HTML.slice(start, end);
}

// ---------------------------------------------------------------------
// Minimal DOM: just enough to run renderCampaignDrops/createCampaignCard/
// renderCompactVoucherRow/renderActiveCampaignsSection and the delegated
// claim click handler.
// ---------------------------------------------------------------------

function parseSimpleSelector(sel) {
  const m = /^([a-zA-Z]*)((?:\.[\w-]+)*)((?:\[[^\]]+\])*)$/.exec(sel.trim());
  if (!m) return null;
  const tag = m[1] ? m[1].toLowerCase() : null;
  const classes = (m[2].match(/\.[\w-]+/g) || []).map((c) => c.slice(1));
  const attrs = [];
  const attrRe = /\[([^=\]]+)(?:=['"]?([^'"\]]*)['"]?)?\]/g;
  let am;
  while ((am = attrRe.exec(m[3]))) attrs.push({ name: am[1], value: am[2] });
  return { tag, classes, attrs };
}

function nodeMatches(node, sel) {
  const parsed = parseSimpleSelector(sel);
  if (!parsed) return false;
  if (parsed.tag && node.tagName.toLowerCase() !== parsed.tag) return false;
  for (const c of parsed.classes) {
    if (!node.className.split(/\s+/).includes(c)) return false;
  }
  for (const a of parsed.attrs) {
    if (!(a.name in node.attrs)) return false;
    if (a.value !== undefined && String(node.attrs[a.name]) !== a.value) return false;
  }
  return true;
}

function makeDataset(node) {
  return new Proxy(
    {},
    {
      get(_, prop) {
        const attrName = "data-" + String(prop).replace(/[A-Z]/g, (m) => "-" + m.toLowerCase());
        return node.attrs[attrName];
      },
      set(_, prop, value) {
        const attrName = "data-" + String(prop).replace(/[A-Z]/g, (m) => "-" + m.toLowerCase());
        node.attrs[attrName] = String(value);
        return true;
      },
      deleteProperty(_, prop) {
        const attrName = "data-" + String(prop).replace(/[A-Z]/g, (m) => "-" + m.toLowerCase());
        delete node.attrs[attrName];
        return true;
      },
      has(_, prop) {
        const attrName = "data-" + String(prop).replace(/[A-Z]/g, (m) => "-" + m.toLowerCase());
        return attrName in node.attrs;
      },
    }
  );
}

function makeNode(tag) {
  const node = {
    tagName: String(tag || "div").toUpperCase(),
    className: "",
    style: {},
    attrs: {},
    _children: [],
    _listeners: {},
    _text: "",
    parentNode: null,
    appendChild(child) {
      child.parentNode = node;
      node._children.push(child);
      return child;
    },
    removeChild(child) {
      node._children = node._children.filter((c) => c !== child);
      child.parentNode = null;
    },
    insertAdjacentElement(_pos, child) {
      node._children.push(child);
      return child;
    },
    setAttribute(k, v) {
      node.attrs[k] = String(v);
    },
    getAttribute(k) {
      return k in node.attrs ? node.attrs[k] : null;
    },
    removeAttribute(k) {
      delete node.attrs[k];
    },
    addEventListener(type, fn) {
      (node._listeners[type] = node._listeners[type] || []).push(fn);
    },
    click() {
      (node._listeners.click || []).forEach((fn) => fn({ target: node }));
    },
    closest(sel) {
      let cur = node;
      while (cur) {
        if (sel.split(",").some((s) => nodeMatches(cur, s))) return cur;
        cur = cur.parentNode;
      }
      return null;
    },
    scrollIntoView() {
      node._scrolled = true;
    },
    classList: {
      add(c) {
        if (!node.className.split(/\s+/).includes(c)) node.className = (node.className + " " + c).trim();
      },
      remove(c) {
        node.className = node.className.split(/\s+/).filter((x) => x !== c).join(" ");
      },
      contains(c) {
        return node.className.split(/\s+/).includes(c);
      },
    },
  };
  Object.defineProperty(node, "children", { get: () => node._children });
  Object.defineProperty(node, "innerHTML", {
    get: () => node._html || "",
    set(v) {
      node._html = v;
      if (!v) node._children = [];
    },
  });
  Object.defineProperty(node, "textContent", {
    get: () => node._text,
    set(v) {
      node._text = v == null ? "" : String(v);
      node._children = [];
    },
  });
  Object.defineProperty(node, "innerText", {
    get: () => node._text,
    set(v) {
      node._text = v == null ? "" : String(v);
    },
  });
  Object.defineProperty(node, "disabled", {
    get: () => "disabled" in node.attrs,
    set(v) {
      if (v) node.attrs.disabled = "disabled";
      else delete node.attrs.disabled;
    },
  });
  Object.defineProperty(node, "dataset", { get: () => makeDataset(node) });
  Object.defineProperty(node, "id", {
    get: () => node.attrs.id || "",
    set(v) {
      node.attrs.id = String(v);
    },
  });
  return node;
}

function allText(node, out) {
  out = out || [];
  if (node.textContent) out.push(node.textContent);
  (node._children || []).forEach((c) => allText(c, out));
  return out;
}

function findByClass(node, cls) {
  if (node.className && node.className.split(/\s+/).includes(cls)) return node;
  for (const c of node._children || []) {
    const hit = findByClass(c, cls);
    if (hit) return hit;
  }
  return null;
}

function findAllByClass(node, cls, out) {
  out = out || [];
  if (node.className && node.className.split(/\s+/).includes(cls)) out.push(node);
  (node._children || []).forEach((c) => findAllByClass(c, cls, out));
  return out;
}

const HERO_IDS = [
  "ap-hero-drop-state",
  "ap-hero-nodrop-state",
  "campaign-voucher-list",
  "ap-hero-title",
  "ap-hero-subtitle",
  "ap-hero-img",
  "ap-hero-stock",
  "ap-hero-remaining",
  "ap-hero-stock-fill",
  "ap-hero-countdown",
  "campaign-voucher-message",
  "campaign-voucher-btn",
  "claim-error",
  "ap-campaigns-section",
  "ap-campaigns-list",
];

function buildSandbox({ fetchImpl } = {}) {
  const byId = {};
  HERO_IDS.forEach((id) => {
    const n = makeNode(id === "campaign-voucher-btn" ? "button" : "div");
    n.id = id;
    byId[id] = n;
  });
  byId["ap-hero-nodrop-state"].style.display = "block";
  byId["ap-hero-drop-state"].style.display = "none";
  byId["ap-campaigns-section"].style.display = "none";

  const toasts = [];
  const alerts = [];
  const calls = [];

  const document = {
    getElementById(id) {
      // The extracted source range also contains admin-only DOM wiring
      // (delete/edit-dates modals etc.) unrelated to the Live Drop voucher
      // flow under test. Rather than hand-enumerate every admin element id,
      // hand back an inert dummy node for anything we didn't pre-register —
      // it supports the same no-op DOM surface, so that unrelated wiring
      // code runs harmlessly instead of throwing on a null lookup.
      if (!(id in byId)) {
        const dummy = makeNode("div");
        dummy.id = id;
        byId[id] = dummy;
      }
      return byId[id];
    },
    createElement(tag) {
      return makeNode(tag);
    },
    // Recorded by type so tests can simulate `visibilitychange` (returning
    // to the Mini App) and confirm it re-runs the same channel-subscription
    // check renderCompactVoucherRow used on first render.
    _listeners: {},
    addEventListener(type, fn) {
      (document._listeners[type] = document._listeners[type] || []).push(fn);
    },
    visibilityState: "visible",
    body: { contains: () => true },
  };

  const fetchLog = [];
  const defaultFetch = async () => ({ ok: true, status: 200, json: async () => ({ status: "ok" }) });

  const sandbox = {
    document,
    console,
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
    navigator: { clipboard: { writeText: async () => {} } },
    t(key) {
      const known = {
        claim_now_label: "Claim now",
        channel_joined: "Joined Official Channel",
        channel_join_required: "Join Official Channel",
        channel_required_to_claim: "Required to claim",
        my_rewards: "My Rewards",
        claim_reward: "Claim Reward",
        claiming: "Claiming…",
        copy: "Copy",
        reward_claimed_toast: "🎁 Reward claimed!",
        claim_failed_toast: "❌ Claim failed. Please try again.",
      };
      return known[key] || key;
    },
    hapticNotify() {},
    fmtKL: (d) => String(d || ""),
    OFFICIAL_CHANNEL_LINK: "https://t.me/advantplayofficial",
    tg: {},
    tgAlert: (msg) => alerts.push(msg),
    getLatestInitData: () => "init-data",
    API_V2: "/api/v2",
    userId: "u1",
    updateJourneyUI() {},
    async loadWelcomeProgress() {},
    ensureCodeBlock() {},
    showWelcomeVoucherGuidePopup() {},
    showPlatformFinderCta() {},
    showToast(msg) {
      toasts.push(msg);
    },
    isWelcomeAudienceDrop() {
      return false;
    },
    syncWelcomeStatusCard() {},
    buildAdminQueryTail: () => "",
    v2Fetch: async (url, init) => {
      fetchLog.push({ url, init });
      const impl = fetchImpl || defaultFetch;
      return impl(url, init);
    },
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);

  return { sandbox, byId, toasts, alerts, fetchLog };
}

function run(sandbox) {
  vm.runInContext(loadSource(), sandbox);
}

function drop(overrides) {
  return Object.assign(
    {
      dropId: "d1",
      name: "$5 Voucher",
      isActive: true,
      userClaimed: false,
      type: "pooled",
      startsAt: "2026-08-01T00:00:00Z",
      endsAt: "2026-08-10T00:00:00Z",
      visible_remaining: 5,
    },
    overrides || {}
  );
}

function checkOnlyCallCount(fetchLog) {
  return fetchLog.filter((c) => {
    try {
      return !!JSON.parse((c.init && c.init.body) || "{}").check_only;
    } catch {
      return false;
    }
  }).length;
}

// ---------------------------------------------------------------------
// 1. No active voucher -> original Live Drop layout untouched
// ---------------------------------------------------------------------

test("no drops at all -> no-drop state shown, hero untouched", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(null, [], null);

  assert.equal(byId["ap-hero-drop-state"].style.display, "none");
  assert.equal(byId["ap-hero-nodrop-state"].style.display, "block");
  assert.equal(findByClass(byId["campaign-voucher-list"], "ap-reward-compact"), null);
});

test("primary already claimed, no other active voucher -> falls back to the original full campaign card (no compact row)", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  const claimed = drop({ userClaimed: true, code: "ABC123", claimedAt: "2026-08-01T00:00:00Z" });
  sandbox.renderCampaignDrops(claimed, [], null);

  assert.equal(byId["ap-hero-drop-state"].style.display, "block");
  assert.equal(findByClass(byId["campaign-voucher-list"], "ap-reward-compact"), null);
  assert.ok(findByClass(byId["campaign-voucher-list"], "campaign-card"), "original nested card must still render");
  assert.notEqual(byId["ap-hero-subtitle"].style.display, "none", "description stays visible in the unchanged state");
});

// ---------------------------------------------------------------------
// 2-4. One / two / three active vouchers
// ---------------------------------------------------------------------

test("one active unclaimed voucher -> single compact reward row, no 'more' link, no duplicated title", async () => {
  const { sandbox, byId, fetchLog } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1", name: "$5 Voucher" }), [], null);

  assert.equal(byId["ap-hero-title"].textContent, "$5 Voucher", "campaign name renders exactly once, in the hero title");

  const rows = findAllByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  assert.equal(rows.length, 1);
  assert.doesNotMatch(allText(rows[0]).join(" "), /\$5 Voucher/, "the drop name must not be repeated inside the reward row");

  const claimBtn = findByClass(rows[0], "ap-reward-compact-claim");
  assert.ok(claimBtn);
  assert.equal(claimBtn.dataset.dropid, "d1");
  // Card render never live-probes channel membership (b2b1e45): a cache
  // miss is "unknown", not "not subscribed", so Claim now is enabled and the
  // real /vouchers/claim request stays the sole authoritative gate.
  assert.equal(claimBtn.disabled, false, "Claim now is enabled on render; the claim request is the authoritative channel gate");

  assert.equal(findByClass(byId["campaign-voucher-list"], "ap-reward-compact-more"), null);
  assert.equal(byId["ap-hero-subtitle"].style.display, "none", "campaign description must not be repeated");
  assert.equal(byId["campaign-voucher-message"].style.display, "none", "'tap below to claim' must not be repeated");
  assert.equal(findByClass(byId["campaign-voucher-list"], "campaign-card"), null, "no nested full campaign card");

  for (let i = 0; i < 10; i++) await Promise.resolve();
  const channelRow = findByClass(rows[0], "ap-reward-compact-channel");
  assert.equal(channelRow.style.display, "none", "no channel status row without a positive cached flag");
  assert.equal(claimBtn.disabled, false);
  assert.equal(checkOnlyCallCount(fetchLog), 0, "render must not fire a check_only channel probe");
});

test("a cached positive subscription (drop.channelSubscribed) shows '✓ Joined Official Channel' with Claim now enabled", async () => {
  const { sandbox, byId, fetchLog } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1", channelSubscribed: true }), [], null);
  for (let i = 0; i < 10; i++) await Promise.resolve();

  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const channelRow = findByClass(row, "ap-reward-compact-channel");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  assert.equal(channelRow.dataset.joined, "true");
  assert.equal(channelRow.dataset.channelState, "subscribed");
  assert.equal(channelRow.style.display, "flex");
  assert.match(channelRow.textContent, /Joined Official Channel/);
  assert.equal(claimBtn.disabled, false);
  assert.equal(checkOnlyCallCount(fetchLog), 0, "the cached flag is read from /vouchers/visible, never probed");
});

test("two active unclaimed vouchers -> compact row plus '1 more reward in My Rewards'", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(
    drop({ dropId: "d1", name: "$5 Voucher", startsAt: "2026-08-02T00:00:00Z" }),
    [drop({ dropId: "d2", name: "$10 Voucher", startsAt: "2026-08-01T00:00:00Z" })],
    null
  );

  assert.equal(byId["ap-hero-title"].textContent, "$5 Voucher", "newer voucher's name takes the hero title");
  assert.equal(findAllByClass(byId["campaign-voucher-list"], "ap-reward-compact").length, 1);
  const more = findByClass(byId["campaign-voucher-list"], "ap-reward-compact-more");
  assert.ok(more);
  assert.equal(more.textContent, "1 more reward in My Rewards ›");

  // The second voucher must still be visible/claimable in the extras list.
  const extraCards = findAllByClass(byId["ap-campaigns-list"], "campaign-card");
  assert.equal(extraCards.length, 1);
});

test("three active unclaimed vouchers -> compact row plus '2 more rewards in My Rewards'", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(
    drop({ dropId: "d1", name: "$5 Voucher" }),
    [drop({ dropId: "d2", name: "$10 Voucher" }), drop({ dropId: "d3", name: "$20 Voucher" })],
    null
  );

  const more = findByClass(byId["campaign-voucher-list"], "ap-reward-compact-more");
  assert.equal(more.textContent, "2 more rewards in My Rewards ›");
  assert.equal(findAllByClass(byId["ap-campaigns-list"], "campaign-card").length, 2);
});

test("newest unclaimed voucher (by startsAt) takes the compact slot, not array order", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(
    drop({ dropId: "old", name: "Old Voucher", startsAt: "2026-08-01T00:00:00Z" }),
    [drop({ dropId: "new", name: "New Voucher", startsAt: "2026-08-05T00:00:00Z" })],
    null
  );

  assert.equal(byId["ap-hero-title"].textContent, "New Voucher");
  assert.ok(findByClass(byId["campaign-voucher-list"], "ap-reward-compact"));
});

test("'more' link scrolls to the My Rewards (#ap-campaigns-section) area", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1" }), [drop({ dropId: "d2" })], null);

  const more = findByClass(byId["campaign-voucher-list"], "ap-reward-compact-more");
  more.click();
  assert.equal(byId["ap-campaigns-section"]._scrolled, true);
});

test("a claimable personalised-type voucher gets the compact row too, not just pooled drops", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1", name: "Personal Voucher", type: "personalised" }), [], null);

  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  assert.ok(row, "a claimable personalised voucher must use the compact slot");
  assert.equal(byId["ap-hero-title"].textContent, "Personal Voucher");

  // Non-pooled drops carry no channel-subscription requirement, so the
  // status row never appears and Claim now is enabled immediately.
  const channelRow = findByClass(row, "ap-reward-compact-channel");
  assert.equal(channelRow.style.display, "none");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  assert.equal(claimBtn.disabled, false);
});

// Channel membership: the claim request is the authoritative gate. These
// replace the pre-b2b1e45 tests that pinned a per-render check_only probe
// (and a disabled-until-verified Claim now) which was removed on purpose:
// that probe hit Telegram getChatMember on every card render and blocked
// subscribed users on any latency/429/error.

function captureIntervals(sandbox) {
  // renderClaimError starts 1s countdown intervals (rejoin buffer: up to an
  // hour). Track them so each test clears what it started and never leaves
  // a live handle keeping the runner open.
  const ids = [];
  const realSetInterval = sandbox.setInterval;
  sandbox.setInterval = (fn, ms) => {
    const id = realSetInterval(fn, ms);
    ids.push(id);
    return id;
  };
  return () => ids.forEach((id) => clearInterval(id));
}

test("an unsubscribed user's Claim is rejected server-side (403 not_subscribed) and surfaces the Join Official Channel CTA", async () => {
  const claimBodies = [];
  const { sandbox, byId, fetchLog } = buildSandboxWithClickCapture({
    fetchImpl: async (url, init) => {
      if (String(url).includes("/vouchers/visible")) {
        // Cache says "not subscribed" (or unknown): the flag is not true.
        return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [drop({ dropId: "d1", channelSubscribed: false })] }) };
      }
      claimBodies.push(JSON.parse((init && init.body) || "{}"));
      return { ok: false, status: 403, json: async () => ({ code: "not_subscribed", message: "Subscribe to unlock" }) };
    },
  });
  run(sandbox);
  const onClick = findClaimListener(sandbox);
  const opened = [];
  sandbox.tg.openTelegramLink = (url) => opened.push(url);

  await sandbox.loadVouchers();
  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  const channelRow = findByClass(row, "ap-reward-compact-channel");
  assert.equal(channelRow.style.display, "none", "a cache miss/false never renders a guessed 'not joined' state");
  assert.notEqual(channelRow.dataset.joined, "true", "an unconfirmed user is never shown as joined");
  assert.equal(checkOnlyCallCount(fetchLog), 0);

  await onClick({ target: claimBtn });

  assert.equal(claimBodies.length, 1, "exactly one real claim request");
  assert.ok(!claimBodies[0].check_only, "the gate is the real claim, not a probe");
  const errorCard = findByClass(byId["claim-error"], "claim-error-card");
  assert.ok(errorCard, "the server rejection must be rendered, not swallowed");
  assert.equal(findByClass(errorCard, "claim-error-title").textContent, "subscribe_required");
  const cta = findAllByClass(errorCard, "btn-ghost")[0];
  assert.ok(cta, "a Join Official Channel CTA is offered");
  assert.equal(cta.textContent, "join_official_channel_btn");
  cta.click();
  assert.deepEqual(opened, ["https://t.me/advantplayofficial"], "CTA opens the Official Channel via tg.openTelegramLink");

  const rowAfter = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  assert.equal(findByClass(rowAfter, "ap-reward-compact-claim").dataset.dropid, "d1", "voucher stays in the slot for a retry after joining");
  assert.equal(sandbox.__claimInFlight, false, "in-flight lock released");
});

test("returning to the Mini App (visibilitychange) re-reads /vouchers/visible and picks up the refreshed cached subscription", async () => {
  let subscribed = false;
  let visibleCalls = 0;
  const { sandbox, byId, fetchLog } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) {
        visibleCalls += 1;
        return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [drop({ dropId: "d1", channelSubscribed: subscribed })] }) };
      }
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    },
  });
  run(sandbox);
  await sandbox.loadVouchers();
  let channelRow = findByClass(findByClass(byId["campaign-voucher-list"], "ap-reward-compact"), "ap-reward-compact-channel");
  assert.equal(channelRow.style.display, "none");
  assert.equal(visibleCalls, 1);

  // User joins the channel elsewhere; the audit/claim path refreshes the cache.
  subscribed = true;
  const listeners = sandbox.document._listeners.visibilitychange || [];
  assert.ok(listeners.length > 0, "a visibilitychange listener must be registered");
  listeners.forEach((fn) => fn());
  for (let i = 0; i < 40; i++) await Promise.resolve();

  assert.equal(visibleCalls, 2, "returning to the app re-reads /vouchers/visible exactly once");
  channelRow = findByClass(findByClass(byId["campaign-voucher-list"], "ap-reward-compact"), "ap-reward-compact-channel");
  assert.equal(channelRow.dataset.joined, "true");
  assert.match(channelRow.textContent, /Joined Official Channel/);
  assert.equal(checkOnlyCallCount(fetchLog), 0, "refresh is cache-only: no live Telegram probe");
});

test("a rejoin-buffer claim rejection keeps the user labelled as a subscriber and shows the cooldown, not 'Join Official Channel'", async () => {
  // rejoin_buffer_active is a claim-eligibility cooldown for a user who IS
  // subscribed; it has its own countdown UI and must never be presented as
  // "not joined" (test_rejoin_buffer_claim_ui.test.js covers the countdown).
  const { sandbox, byId } = buildSandboxWithClickCapture({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) {
        return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [drop({ dropId: "d1", channelSubscribed: true })] }) };
      }
      return {
        ok: false,
        status: 403,
        json: async () => ({
          code: "rejoin_buffer_active",
          reason: "rejoin_buffer_active",
          retry_after_sec: 3600,
          message: "You recently rejoined @AdvantPlayOfficial.",
        }),
      };
    },
  });
  const clearIntervals = captureIntervals(sandbox);
  try {
    run(sandbox);
    const onClick = findClaimListener(sandbox);
    await sandbox.loadVouchers();
    const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
    const claimBtn = findByClass(row, "ap-reward-compact-claim");
    const channelRow = findByClass(row, "ap-reward-compact-channel");
    assert.equal(claimBtn.disabled, false, "Claim now must be reachable so the rejoin-buffer countdown can surface");

    await onClick({ target: claimBtn });

    assert.equal(channelRow.dataset.joined, "true", "the user is subscribed; only the rejoin cooldown blocks claiming");
    const errorCard = findByClass(byId["claim-error"], "claim-error-card");
    assert.ok(errorCard);
    assert.equal(findByClass(errorCard, "claim-error-title").textContent, "Please stay subscribed");
    assert.doesNotMatch(allText(errorCard).join(" "), /Join Official Channel|join_official_channel_btn/);
    assert.match(findByClass(errorCard, "claim-error-countdown").textContent, /Try again in/);
  } finally {
    clearIntervals();
  }
});

// ---------------------------------------------------------------------
// 9. Claimed / expired / sold-out vouchers never occupy the Live Drop slot
// ---------------------------------------------------------------------

test("a claimed drop is skipped for the compact slot in favour of the next unclaimed one", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(
    drop({ dropId: "claimed", name: "Claimed Voucher", userClaimed: true, code: "XYZ" }),
    [drop({ dropId: "fresh", name: "Fresh Voucher" })],
    null
  );

  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  assert.ok(row, "an unclaimed voucher exists so the compact row must render");
  assert.equal(byId["ap-hero-title"].textContent, "Fresh Voucher");
});

test("a sold-out (publicly) drop is not treated as an active voucher", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1", sold_out: true }), [], null);

  assert.equal(findByClass(byId["campaign-voucher-list"], "ap-reward-compact"), null);
});

test("an inactive/ended drop is not treated as an active voucher", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1", isActive: false, visible_remaining: 0 }), [], null);

  assert.equal(byId["ap-hero-drop-state"].style.display, "none");
  assert.equal(findByClass(byId["campaign-voucher-list"], "ap-reward-compact"), null);
});

// ---------------------------------------------------------------------
// 10. "Past Drops" -> "My Rewards" label rename, IDs/routes unchanged
// ---------------------------------------------------------------------

test("the extras toggle reads 'My Rewards', not 'past drops', while keeping its element id", () => {
  const { sandbox, byId } = buildSandbox();
  run(sandbox);
  // No active extras, but one past (ended, unclaimed) drop -> toggle renders.
  sandbox.renderCampaignDrops(
    drop({ dropId: "d1" }),
    [drop({ dropId: "old", isActive: false, visible_remaining: 0 })],
    null
  );

  const toggle = byId["ap-campaigns-list"]._children.find((c) => c.id === "ap-my-rewards-toggle");
  assert.ok(toggle, "toggle button must exist");
  assert.match(toggle.textContent, /My Rewards/);
  assert.doesNotMatch(toggle.textContent.toLowerCase(), /past drop/);

  toggle.click();
  assert.match(toggle.textContent, /My Rewards/);
});

// ---------------------------------------------------------------------
// 5-8. Claim flow via the real delegated click handler
// ---------------------------------------------------------------------

function findClaimListener(sandbox) {
  // The extracted source range registers several top-level document click
  // listeners (admin row actions, admin modals, and the claim handler we
  // actually want to test). Dispatch to all of them, exactly like a real
  // `click` event would — the ones that don't match our button's selector
  // (via `closest`) simply no-op and return early.
  const listeners = sandbox.document._clickListeners;
  assert.ok(Array.isArray(listeners) && listeners.length > 0, "expected at least one click handler to be registered");
  return async (evt) => {
    for (const fn of listeners) {
      await fn(evt);
    }
  };
}

function buildSandboxWithClickCapture(opts) {
  const ctx = buildSandbox(opts);
  ctx.sandbox.document._clickListeners = [];
  ctx.sandbox.document.addEventListener = (type, fn) => {
    if (type === "click") ctx.sandbox.document._clickListeners.push(fn);
  };
  return ctx;
}

// Routes a v2Fetch mock by endpoint + intent. Card render no longer sends a
// `check_only: true` channel probe to /vouchers/claim (b2b1e45), but the
// routing stays so a reintroduced probe could never be misattributed as the
// test's real claim response by a naive call-order queue.
function routedFetch({ visibleResponses, claimResponses }) {
  let visibleCall = 0;
  let claimCall = 0;
  return async (url, init) => {
    if (String(url).includes("/vouchers/visible")) {
      return { ok: true, status: 200, json: async () => visibleResponses[visibleCall++] };
    }
    if (String(url).includes("/vouchers/claim")) {
      let body = {};
      try {
        body = JSON.parse((init && init.body) || "{}");
      } catch {
        /* ignore */
      }
      if (body.check_only) {
        return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
      }
      return claimResponses[claimCall++]();
    }
    return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
  };
}

test("successful claim on the compact row refreshes state and shows the next voucher", async () => {
  const { sandbox, byId } = buildSandboxWithClickCapture({
    fetchImpl: routedFetch({
      visibleResponses: [
        { status: "ok", drops: [drop({ dropId: "d1", name: "$5 Voucher" }), drop({ dropId: "d2", name: "$10 Voucher" })] },
        { status: "ok", drops: [drop({ dropId: "d2", name: "$10 Voucher" })] },
      ],
      claimResponses: [
        async () => ({ ok: true, status: 200, json: async () => ({ ok: true, status: "ok", voucher: { code: "CODE1" } }) }),
      ],
    }),
  });
  run(sandbox);
  const onClick = findClaimListener(sandbox);

  await sandbox.loadVouchers();
  let row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  assert.equal(claimBtn.dataset.dropid, "d1");

  await onClick({ target: claimBtn });

  row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  assert.ok(row, "next voucher must take over the compact slot");
  assert.equal(byId["ap-hero-title"].textContent, "$10 Voucher");
});

test("claiming the last remaining voucher restores the original no-voucher layout", async () => {
  const responses = [
    { status: "ok", drops: [drop({ dropId: "d1", name: "$5 Voucher" })] },
    { ok: true, status: "ok", voucher: { code: "CODE1" } },
    { status: "ok", drops: [] },
  ];
  let call = 0;
  const { sandbox, byId } = buildSandboxWithClickCapture({
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => responses[call++] }),
  });
  run(sandbox);
  const onClick = findClaimListener(sandbox);

  await sandbox.loadVouchers();
  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");

  await onClick({ target: claimBtn });

  assert.equal(byId["ap-hero-drop-state"].style.display, "none");
  assert.equal(byId["ap-hero-nodrop-state"].style.display, "block");
});

test("failed claim keeps the voucher visible in the compact slot for a safe retry", async () => {
  const { sandbox, byId, toasts } = buildSandboxWithClickCapture({
    fetchImpl: async (url, init) => {
      if (String(url).includes("/vouchers/visible")) {
        return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [drop({ dropId: "d1", name: "$5 Voucher" })] }) };
      }
      let body = {};
      try {
        body = JSON.parse((init && init.body) || "{}");
      } catch {
        /* ignore */
      }
      // Channel check_only probe passes (subscribed) so Claim now is
      // enabled going into the claim attempt — only the real claim call
      // fails, which is what this test exercises.
      if (body.check_only) {
        return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
      }
      return { ok: false, status: 500, json: async () => ({ status: "error", code: "server_error" }) };
    },
  });
  run(sandbox);
  const onClick = findClaimListener(sandbox);

  await sandbox.loadVouchers();
  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  for (let i = 0; i < 10; i++) await Promise.resolve();
  assert.equal(claimBtn.disabled, false, "channel check passed, so Claim now is enabled before the claim attempt");

  await onClick({ target: claimBtn });

  const rowAfter = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  assert.ok(rowAfter, "voucher must remain visible after a failed claim");
  const claimBtnAfter = findByClass(rowAfter, "ap-reward-compact-claim");
  assert.equal(claimBtnAfter.dataset.dropid, "d1", "same voucher must still be claimable");
  assert.equal(claimBtnAfter.disabled, false, "button must be re-enabled for retry");
  assert.ok(toasts.length >= 1, "an error toast must be shown");
});

test("repeated Claim clicks in flight only submit one claim request", async () => {
  let resolveClaim;
  const claimPromise = new Promise((resolve) => {
    resolveClaim = resolve;
  });
  const claimCalls = [];
  const { sandbox, byId } = buildSandboxWithClickCapture({
    fetchImpl: async (url, init) => {
      if (String(url).includes("/vouchers/visible")) {
        return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [drop({ dropId: "d1" })] }) };
      }
      // The compact row's own channel-gate probe (check_only: true) hits
      // this same claim endpoint on render; only a real claim submission
      // should wait on claimPromise / count toward claimCalls.
      let body = {};
      try {
        body = JSON.parse((init && init.body) || "{}");
      } catch {
        /* ignore */
      }
      if (body.check_only) {
        return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
      }
      claimCalls.push(url);
      await claimPromise;
      return { ok: true, status: 200, json: async () => ({ ok: true, status: "ok", voucher: { code: "C" } }) };
    },
  });
  run(sandbox);
  const onClick = findClaimListener(sandbox);

  await sandbox.loadVouchers();
  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");

  const first = onClick({ target: claimBtn });
  const second = onClick({ target: claimBtn });
  const third = onClick({ target: claimBtn });

  resolveClaim();
  await Promise.all([first, second, third]);

  assert.equal(claimCalls.length, 1, "only one claim request should have been submitted");
});
