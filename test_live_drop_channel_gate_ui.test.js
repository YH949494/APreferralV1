/**
 * Live Drop channel-gate audit — frontend regression tests (P2, P4).
 *
 * Root cause: checkChannelSubscription() only recognized two outcomes
 * (subscribed / a confirmed 403 not_subscribed) and threw on anything else,
 * and renderCompactVoucherRow's .catch() then called applyChannelState(false)
 * — rendering "○ Join Official Channel ›" for 401s, 429s, 5xxs, the new
 * verification_failed code, and plain network failures. That produced a
 * false "Join Official Channel" state for already-subscribed users whenever
 * the backend channel check merely hiccuped.
 *
 * Fix: checkChannelSubscription() now returns one of three explicit states
 * ("subscribed" / "not_subscribed" / "unverified"), with one bounded retry
 * for a transient "unverified" result (never an infinite loop), and
 * renderCompactVoucherRow renders a third neutral "Checking… / Retry" UI
 * state for "unverified" instead of ever guessing "not subscribed".
 *
 * Also covers P4: loadVouchers() collapses concurrent calls into the one
 * in-flight promise, so opening the Mini App fires one /vouchers/visible
 * request and one check_only probe per Live Drop, not two.
 *
 * Same sandboxed-vm extraction approach as test_live_drop_voucher_ui.test.js
 * (no build step for the inline <script> in static/index.html).
 *
 * Run with: node --test test_live_drop_channel_gate_ui.test.js
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
      return null;
    },
    scrollIntoView() {},
    classList: {
      add() {},
      remove() {},
      contains() {
        return false;
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
  Object.defineProperty(node, "dataset", {
    get: () =>
      new Proxy(
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
        }
      ),
  });
  Object.defineProperty(node, "id", {
    get: () => node.attrs.id || "",
    set(v) {
      node.attrs.id = String(v);
    },
  });
  return node;
}

function findByClass(node, cls) {
  if (!node) return null;
  if (node.className && node.className.split(/\s+/).includes(cls)) return node;
  for (const c of node._children || []) {
    const hit = findByClass(c, cls);
    if (hit) return hit;
  }
  return null;
}

function findAllByClass(node, cls, out) {
  out = out || [];
  if (!node) return out;
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

  const document = {
    getElementById(id) {
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
        channel_checking_status: "Checking channel status…",
        retry_label: "Retry",
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
    tgAlert() {},
    getLatestInitData: () => "init-data",
    API_V2: "/api/v2",
    userId: "u1",
    updateJourneyUI() {},
    async loadWelcomeProgress() {},
    ensureCodeBlock() {},
    showWelcomeVoucherGuidePopup() {},
    showPlatformFinderCta() {},
    showToast() {},
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

  return { sandbox, byId, fetchLog };
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

async function flush(n = 40) {
  for (let i = 0; i < n; i++) await Promise.resolve();
}

function checkOnlyCallCount(fetchLog) {
  return fetchLog.filter((c) => {
    try {
      const body = JSON.parse((c.init && c.init.body) || "{}");
      return !!body.check_only;
    } catch {
      return false;
    }
  }).length;
}

// ---------------------------------------------------------------------
// checkChannelSubscription tri-state contract, direct unit tests
// ---------------------------------------------------------------------

test("checkChannelSubscription: subscribed response -> state 'subscribed'", async () => {
  const { sandbox } = buildSandbox({
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ status: "ok", subscribed: true }) }),
  });
  run(sandbox);
  const state = await sandbox.checkChannelSubscription("d1");
  assert.equal(state.state, "subscribed");
});

test("checkChannelSubscription: confirmed not_subscribed (403) -> state 'not_subscribed'", async () => {
  const { sandbox } = buildSandbox({
    fetchImpl: async () => ({ ok: false, status: 403, json: async () => ({ code: "not_subscribed", message: "join" }) }),
  });
  run(sandbox);
  const state = await sandbox.checkChannelSubscription("d1");
  assert.equal(state.state, "not_subscribed");
});

test("checkChannelSubscription: verification_failed (503) -> state 'unverified', never 'not_subscribed'", async () => {
  const { sandbox, fetchLog } = buildSandbox({
    fetchImpl: async () => ({
      ok: false,
      status: 503,
      json: async () => ({ code: "verification_failed", subscribed: false, verified: false, reason: "network_error", retry_after_sec: 3 }),
    }),
  });
  run(sandbox);
  const state = await sandbox.checkChannelSubscription("d1");
  assert.equal(state.state, "unverified");
  // Bounded retry: at most 2 attempts for a persistent failure.
  assert.equal(fetchLog.length, 2);
});

test("checkChannelSubscription: 401 -> state 'unverified'", async () => {
  const { sandbox } = buildSandbox({
    fetchImpl: async () => ({ ok: false, status: 401, json: async () => ({ code: "auth_failed" }) }),
  });
  run(sandbox);
  const state = await sandbox.checkChannelSubscription("d1");
  assert.equal(state.state, "unverified");
});

test("checkChannelSubscription: 429 -> state 'unverified'", async () => {
  const { sandbox } = buildSandbox({
    fetchImpl: async () => ({ ok: false, status: 429, json: async () => ({ code: "busy" }) }),
  });
  run(sandbox);
  const state = await sandbox.checkChannelSubscription("d1");
  assert.equal(state.state, "unverified");
});

test("checkChannelSubscription: 500 -> state 'unverified'", async () => {
  const { sandbox } = buildSandbox({
    fetchImpl: async () => ({ ok: false, status: 500, json: async () => ({}) }),
  });
  run(sandbox);
  const state = await sandbox.checkChannelSubscription("d1");
  assert.equal(state.state, "unverified");
});

test("checkChannelSubscription: network failure (fetch throws) -> state 'unverified'", async () => {
  const { sandbox } = buildSandbox({
    fetchImpl: async () => {
      throw new Error("network down");
    },
  });
  run(sandbox);
  const state = await sandbox.checkChannelSubscription("d1");
  assert.equal(state.state, "unverified");
});

test("checkChannelSubscription: transient failure then success -> bounded retry recovers to 'subscribed'", async () => {
  let call = 0;
  const { sandbox, fetchLog } = buildSandbox({
    fetchImpl: async () => {
      call += 1;
      if (call === 1) {
        return { ok: false, status: 500, json: async () => ({}) };
      }
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    },
  });
  run(sandbox);
  const state = await sandbox.checkChannelSubscription("d1");
  assert.equal(state.state, "subscribed");
  assert.equal(fetchLog.length, 2, "exactly one bounded retry, not an unbounded loop");
});

test("checkChannelSubscription: persistent failure never exceeds the one bounded retry (no infinite loop)", async () => {
  const { sandbox, fetchLog } = buildSandbox({
    fetchImpl: async () => ({ ok: false, status: 500, json: async () => ({}) }),
  });
  run(sandbox);
  await sandbox.checkChannelSubscription("d1");
  assert.equal(fetchLog.length, 2);
});

// ---------------------------------------------------------------------
// renderCompactVoucherRow: the 3 UI states
// ---------------------------------------------------------------------

test("UI state A: verified + subscribed -> '✓ Joined Official Channel', Claim now enabled", async () => {
  const { sandbox, byId } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    },
  });
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1" }), [], null);
  await flush();

  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  const channelRow = findByClass(row, "ap-reward-compact-channel");

  assert.equal(channelRow.dataset.channelState, "subscribed");
  assert.match(channelRow.textContent, /Joined Official Channel/);
  assert.equal(claimBtn.disabled, false);
});

test("UI state B: verified + not subscribed -> 'Join Official Channel', Claim now disabled", async () => {
  const { sandbox, byId } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
      return { ok: false, status: 403, json: async () => ({ code: "not_subscribed", message: "join" }) };
    },
  });
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1" }), [], null);
  await flush();

  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  const channelRow = findByClass(row, "ap-reward-compact-channel");

  assert.equal(channelRow.dataset.channelState, "not_subscribed");
  assert.match(channelRow.textContent, /Join Official Channel/);
  assert.equal(claimBtn.disabled, true);
});

async function assertNeutralState(fetchImpl) {
  const { sandbox, byId } = buildSandbox({ fetchImpl });
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1" }), [], null);
  await flush();

  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  const channelRow = findByClass(row, "ap-reward-compact-channel");

  assert.equal(channelRow.dataset.channelState, "unverified");
  assert.doesNotMatch(channelRow.textContent, /Join Official Channel/, "must never render the false not-subscribed state");
  assert.match(channelRow.textContent, /Checking channel status|Retry/);
  assert.equal(claimBtn.disabled, true);
}

test("UI state C: verification_failed -> neutral Retry state, Claim now disabled (never 'Join Official Channel')", async () => {
  await assertNeutralState(async (url) => {
    if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
    return {
      ok: false,
      status: 503,
      json: async () => ({ code: "verification_failed", subscribed: false, verified: false, reason: "network_error", retry_after_sec: 3 }),
    };
  });
});

test("UI state C: 401 -> neutral state, never 'Join Official Channel'", async () => {
  await assertNeutralState(async (url) => {
    if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
    return { ok: false, status: 401, json: async () => ({ code: "auth_failed" }) };
  });
});

test("UI state C: 429 -> neutral state, never 'Join Official Channel'", async () => {
  await assertNeutralState(async (url) => {
    if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
    return { ok: false, status: 429, json: async () => ({ code: "busy" }) };
  });
});

test("UI state C: 500 -> neutral state, never 'Join Official Channel'", async () => {
  await assertNeutralState(async (url) => {
    if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
    return { ok: false, status: 500, json: async () => ({}) };
  });
});

test("UI state C: network failure -> neutral state, never 'Join Official Channel'", async () => {
  await assertNeutralState(async (url) => {
    if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
    throw new Error("network down");
  });
});

test("transient failure then success -> row recovers to '✓ Joined Official Channel', Claim now enabled", async () => {
  let claimCalls = 0;
  const { sandbox, byId } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
      claimCalls += 1;
      if (claimCalls === 1) {
        return { ok: false, status: 500, json: async () => ({}) };
      }
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    },
  });
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1" }), [], null);
  await flush();

  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  const channelRow = findByClass(row, "ap-reward-compact-channel");

  assert.equal(channelRow.dataset.channelState, "subscribed");
  assert.match(channelRow.textContent, /Joined Official Channel/);
  assert.equal(claimBtn.disabled, false);
});

test("visibilitychange can recover a previously-failed verification once the backend is healthy again", async () => {
  let healthy = false;
  const { sandbox, byId } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
      if (!healthy) return { ok: false, status: 500, json: async () => ({}) };
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    },
  });
  run(sandbox);
  sandbox.renderCampaignDrops(drop({ dropId: "d1" }), [], null);
  await flush();

  const row = findByClass(byId["campaign-voucher-list"], "ap-reward-compact");
  const claimBtn = findByClass(row, "ap-reward-compact-claim");
  const channelRow = findByClass(row, "ap-reward-compact-channel");
  assert.equal(channelRow.dataset.channelState, "unverified");
  assert.equal(claimBtn.disabled, true);

  healthy = true;
  const listeners = sandbox.document._listeners.visibilitychange || [];
  assert.ok(listeners.length > 0, "a visibilitychange listener must be registered");
  listeners.forEach((fn) => fn());
  await flush();

  assert.equal(channelRow.dataset.channelState, "subscribed");
  assert.equal(claimBtn.disabled, false);
});

// ---------------------------------------------------------------------
// P4: loadVouchers() collapses concurrent calls into one in-flight load
// ---------------------------------------------------------------------

test("P4: two concurrent loadVouchers() calls collapse into a single /vouchers/visible request and a single check_only probe per drop", async () => {
  let visibleCalls = 0;
  const { sandbox, fetchLog } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) {
        visibleCalls += 1;
        return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [drop({ dropId: "d1" })] }) };
      }
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    },
  });
  run(sandbox);

  // Boot fires loadVouchers() from two independent code paths for the same
  // page load; simulate that here without awaiting the first call.
  const first = sandbox.loadVouchers();
  const second = sandbox.loadVouchers();
  await Promise.all([first, second]);
  await flush();

  assert.equal(visibleCalls, 1, "concurrent loadVouchers() calls must collapse into one /vouchers/visible request");
  assert.equal(checkOnlyCallCount(fetchLog), 1, "only one check_only probe per live drop, not two duplicate getChatMember checks");
});

test("P4/Codex: reloadVouchersForUpdatedState() waits out an in-flight load then starts a guaranteed-fresh one (not a stale join)", async () => {
  // Regression for a Codex review finding on PR #498: joining an in-flight
  // loadVouchers() call via loadVouchers() itself after state it depends on
  // (e.g. the user's region) just changed would silently hand back a
  // pre-state-change result and skip the reload entirely.
  let visibleCalls = 0;
  let resolveFirst;
  const firstGate = new Promise((resolve) => {
    resolveFirst = resolve;
  });
  const { sandbox } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) {
        visibleCalls += 1;
        if (visibleCalls === 1) await firstGate; // hold the first request open
        return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
      }
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    },
  });
  run(sandbox);

  const firstLoad = sandbox.loadVouchers(); // starts the in-flight load
  await flush(5);
  const reload = sandbox.reloadVouchersForUpdatedState(); // must NOT just join firstLoad
  resolveFirst();
  await Promise.all([firstLoad, reload]);
  await flush();

  assert.equal(visibleCalls, 2, "reloadVouchersForUpdatedState() must trigger a second, fresh /vouchers/visible request");
});

test("P4: a loadVouchers() call after the previous one settles starts a fresh (uncached) load", async () => {
  let visibleCalls = 0;
  const { sandbox } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/vouchers/visible")) {
        visibleCalls += 1;
        return { ok: true, status: 200, json: async () => ({ status: "ok", drops: [] }) };
      }
      return { ok: true, status: 200, json: async () => ({ status: "ok" }) };
    },
  });
  run(sandbox);

  await sandbox.loadVouchers();
  await sandbox.loadVouchers();

  assert.equal(visibleCalls, 2, "sequential calls after the in-flight load settles must not be permanently cached/collapsed");
});

if (require.main === module) {
  // node --test discovers tests automatically; nothing else to do here.
}
