/**
 * Mission Reward Pool — §27 operator walkthrough, run headlessly.
 *
 * The Phase 2.1 spec asks for a manual browser pass over a specific list of
 * scenarios, all of which exist to answer one question: can an action on one
 * campaign ever touch another?
 *
 * This file scripts those exact scenarios against the REAL admin module and a
 * stateful fake of the real API (create stores a campaign, PUT mutates one
 * campaign, GET returns what is stored). It is not a replacement for opening
 * a browser — it cannot catch a CSS or focus problem — but it does make the
 * wrong-campaign-write question a permanent, executable assertion rather than
 * something re-checked by hand each release.
 *
 * Run with: node --test test_mission_admin_qa.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

const MODULE_PATH = path.join(__dirname, "static", "mission-admin.js");

function freshModule() {
  delete require.cache[require.resolve(MODULE_PATH)];
  return require(MODULE_PATH);
}

// ---------------------------------------------------------------------------
// A stateful stand-in for the admin API, close enough to the real one that a
// wrong-campaign write would actually show up as corrupted stored state.
// ---------------------------------------------------------------------------

function makeBackend() {
  const campaigns = {};
  const pools = {
    "MISSION-5": { pool_id: "MISSION-5", name: "September Mission $5", pool_type: "voucher_drop",
      allocation_scope: "campaign_rewards", status: "active", codes: 50 },
    WEEKEND: { pool_id: "WEEKEND", name: "Weekend Reward", pool_type: "vip",
      allocation_scope: "campaign_rewards", status: "active", codes: 210 },
  };
  const writes = [];

  function serialize(c) {
    return JSON.parse(JSON.stringify(c));
  }

  function editStateFor(id) {
    const c = campaigns[id];
    if (!c) return { status: "error", code: "not_found" };
    const pool = pools[c.mission_pool.pool_id];
    const available = pool ? pool.codes : 0;
    const winner = c.mission_pool.winner_count;
    const state = c.status === "live" ? "live" : (c.status === "draft" ? "draft" : c.status);
    return {
      status: "ok", campaign_id: id, campaign_status: c.status, state,
      entries: c.__entries || 0,
      mission_config_locked: (c.__entries || 0) > 0,
      locked_fields: [], schedule_editable: c.status !== "ended" && c.status !== "archived",
      cancelled: false, closed_at: null, processing_stage: "pending",
      mission_link: "https://t.me/AdvantPlayBot?startapp=mission_" + id,
      mission_link_unavailable_reason: null,
      reward: {
        pool_id: c.mission_pool.pool_id, pool_name: pool ? pool.name : "",
        pool_type: pool ? pool.pool_type : null, pool_exists: !!pool,
        pool_selectable: !!pool, pool_active: !!pool,
        allocation_scope: pool ? pool.allocation_scope : null,
        winner_count: winner, available, issued: 0,
        shortfall: Math.max(0, winner - available), sufficient: !!pool && winner > 0 && available >= winner,
        allocation_method: c.mission_pool.allocation_method,
        allocation_started: false, pool_editable: true,
      },
    };
  }

  function handle(method, url, body) {
    const [pathname] = url.split("?");
    const query = url.includes("?")
      ? Object.fromEntries(url.split("?")[1].split("&").map((p) => p.split("=").map(decodeURIComponent)))
      : {};

    if (method === "GET" && pathname === "/api/admin/mission-pool/campaigns") {
      return {
        status: "ok",
        campaigns: Object.values(campaigns).map((c) => ({
          campaign_id: c.campaign_id, name: c.name,
          state: c.status === "live" ? "live" : "draft",
          submissions: 0, qualified: 0, winners: 0, rewards_allocated: 0,
          winner_count: c.mission_pool.winner_count,
          pool_available: (pools[c.mission_pool.pool_id] || {}).codes || 0,
          processing_stage: "pending", starts_at: c.schedule.starts_at, ends_at: c.schedule.ends_at,
        })),
      };
    }
    if (method === "GET" && pathname === "/api/admin/mission-pool/pools") {
      return {
        status: "ok",
        pool_types: ["voucher_drop", "vip", "tournament_reward", "other"],
        pools: Object.values(pools).map((p) => ({
          pool_id: p.pool_id, name: p.name, pool_type: p.pool_type,
          allocation_scope: p.allocation_scope, status: p.status,
          stock: { available: p.codes, issued: 0 },
        })),
      };
    }
    if (method === "GET" && pathname === "/api/admin/mission-pool/inventory-check") {
      const p = pools[query.pool_id];
      const winner = parseInt(query.winner_count, 10) || 0;
      const available = p ? p.codes : 0;
      return {
        status: "ok", pool_id: query.pool_id, pool_exists: !!p,
        pool_type: p ? p.pool_type : null, pool_selectable: !!p,
        winner_count: winner, available, shortfall: Math.max(0, winner - available),
        sufficient: !!p && winner > 0 && available >= winner,
      };
    }
    let m = pathname.match(/^\/api\/admin\/mission-pool\/([^/]+)\/edit-state$/);
    if (method === "GET" && m) return editStateFor(m[1]);
    m = pathname.match(/^\/api\/admin\/mission-pool\/([^/]+)\/summary$/);
    if (method === "GET" && m) return { status: "ok", grains: {}, disqualification_reasons: {} };
    m = pathname.match(/^\/api\/admin\/gc-campaigns\/([^/]+)$/);
    if (method === "GET" && m) {
      return campaigns[m[1]]
        ? { status: "ok", campaign: serialize(campaigns[m[1]]) }
        : { status: "error", code: "not_found" };
    }
    if (method === "POSTJ" && pathname === "/api/admin/gc-campaigns") {
      if (campaigns[body.campaign_id]) return { status: "error", code: "duplicate_campaign_id" };
      writes.push({ op: "create", campaign_id: body.campaign_id });
      campaigns[body.campaign_id] = Object.assign({ status: "draft" }, JSON.parse(JSON.stringify(body)));
      return { status: "ok", campaign_id: body.campaign_id };
    }
    if (method === "PUTJ" && m === null) { /* fall through */ }
    m = pathname.match(/^\/api\/admin\/gc-campaigns\/([^/]+)$/);
    if (method === "PUTJ" && m) {
      const c = campaigns[m[1]];
      if (!c) return { status: "error", code: "not_found" };
      writes.push({ op: "update", campaign_id: m[1], body: JSON.parse(JSON.stringify(body)) });
      Object.assign(c, JSON.parse(JSON.stringify(body)));
      return { status: "ok" };
    }
    m = pathname.match(/^\/api\/admin\/gc-campaigns\/([^/]+)\/publish$/);
    if (method === "POST" && m) {
      if (!campaigns[m[1]]) return { status: "error", code: "not_found" };
      writes.push({ op: "publish", campaign_id: m[1] });
      campaigns[m[1]].status = "live";
      return { status: "ok", campaign_status: "live" };
    }
    if (method === "POSTJ" && pathname === "/api/admin/reward-pools") {
      pools[body.pool_id] = { pool_id: body.pool_id, name: body.name, pool_type: body.pool_type,
        allocation_scope: "campaign_rewards", status: "active", codes: 0 };
      writes.push({ op: "pool_create", pool_id: body.pool_id });
      return { status: "ok", pool_id: body.pool_id };
    }
    m = pathname.match(/^\/api\/admin\/reward-pools\/([^/]+)\/upload-codes$/);
    if (method === "POSTJ" && m) {
      const p = pools[m[1]];
      if (!p) return { status: "error", code: "pool_not_found" };
      p.codes += body.codes.length;
      writes.push({ op: "pool_upload", pool_id: m[1], count: body.codes.length });
      return { status: "ok", inserted: body.codes.length, skipped_duplicates: 0 };
    }
    return undefined;
  }

  return { campaigns, pools, writes, handle };
}

function makeHarness(backend, opts) {
  opts = opts || {};
  const nodes = {};
  const toasts = [];
  const copied = [];
  const deferred = [];

  function mkNode(props) {
    return Object.assign({ id: "", value: "", checked: false, disabled: false, dataset: {},
      addEventListener() {}, contains() { return true; } }, props);
  }
  const unesc = (s) => String(s).replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, "&");
  const attr = (a, n) => { const m = new RegExp(n + '="([^"]*)"').exec(a); return m ? m[1] : null; };

  function rescan(html) {
    Object.keys(nodes).forEach((k) => { if (k !== "#mp-root") delete nodes[k]; });
    let m;
    const inputRe = /<input\b([^>]*)>/g;
    while ((m = inputRe.exec(html))) {
      const id = attr(m[1], "id");
      if (id) nodes["#" + id] = mkNode({ id: id, value: unesc(attr(m[1], "value") || ""),
        checked: /\bchecked\b/.test(m[1]), disabled: /\bdisabled\b/.test(m[1]) });
    }
    const taRe = /<textarea\b([^>]*)>([\s\S]*?)<\/textarea>/g;
    while ((m = taRe.exec(html))) {
      const id = attr(m[1], "id");
      if (id) nodes["#" + id] = mkNode({ id: id, value: unesc(m[2]), disabled: /\bdisabled\b/.test(m[1]) });
    }
    const selRe = /<select\b([^>]*)>([\s\S]*?)<\/select>/g;
    while ((m = selRe.exec(html))) {
      const id = attr(m[1], "id");
      if (!id) continue;
      const optRe = /<option value="([^"]*)"([^>]*)>/g;
      let o, first = null, value = null;
      while ((o = optRe.exec(m[2]))) {
        if (first === null) first = unesc(o[1]);
        if (/\bselected\b/.test(o[2])) { value = unesc(o[1]); break; }
      }
      nodes["#" + id] = mkNode({ id: id, value: value === null ? (first || "") : value,
        disabled: /\bdisabled\b/.test(m[1]) });
    }
  }

  const listeners = {};
  const root = {
    dataset: {}, _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = v; rescan(v); },
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
    contains() { return true; },
  };
  nodes["#mp-root"] = root;

  function respond(method, url, body) {
    const value = backend.handle(method, url, body);
    if (value === undefined) return Promise.reject(new Error("no route " + method + " " + url));
    if (opts.holdGet && method === "GET" && opts.holdGet.test(url)) {
      return new Promise((resolve) => deferred.push(() => resolve(value)));
    }
    return Promise.resolve(value);
  }

  const host = {
    $: (sel) => nodes[sel],
    esc: (v) => String(v === null || v === undefined ? "" : v)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;"),
    api: (p) => respond("GET", p),
    apiPost: (p) => respond("POST", p),
    apiPostJson: (p, b) => respond("POSTJ", p, b).then((d) => ({ ok: d.status === "ok", d })),
    apiPutJson: (p, b) => respond("PUTJ", p, b).then((d) => ({ ok: d.status === "ok", d })),
    toast: (msg, kind) => toasts.push({ msg, kind }),
    confirm: () => true,
    copy: (text) => copied.push(text),
  };

  return {
    host, toasts, copied, deferred,
    html: () => root.innerHTML,
    node: (id) => nodes["#" + id],
    set: (id, v) => { const n = nodes["#" + id]; assert.ok(n, "control not on screen: " + id); n.value = v; },
    fireChange: (id) => (listeners.change || []).forEach((fn) => fn({ target: nodes["#" + id] })),
    releaseHeld: () => { deferred.splice(0).forEach((fn) => fn()); },
    flush: async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); },
  };
}

/** Walk the real four-step wizard the way an operator would. */
async function createMission(mod, h, spec) {
  mod.dispatch("create");
  await h.flush();

  h.set("mp-c-name", spec.name);
  h.set("mp-c-id", spec.campaign_id);
  h.set("mp-c-prompt", spec.prompt);
  h.set("mp-c-options", spec.options);
  h.set("mp-c-correct", spec.correct);
  mod.dispatch("wizard-next");
  await h.flush();

  if (spec.newPool) {
    h.node("mp-c-mode-new").checked = true;
    h.fireChange("mp-c-mode-new");   // the real change event, as a browser fires it
    await h.flush();
    h.set("mp-c-newpool-id", spec.newPool.pool_id);
    h.set("mp-c-newpool-name", spec.newPool.name);
    h.set("mp-c-newpool-codes", spec.newPool.codes);
    h.set("mp-c-newpool-usage", spec.newPool.usage || "");
  } else {
    h.set("mp-c-pool", spec.pool_id);
  }
  h.set("mp-c-winners", String(spec.winners));
  mod.dispatch("wizard-next");
  await h.flush();

  h.set("mp-c-starts", spec.starts);
  h.set("mp-c-ends", spec.ends);
  mod.dispatch("wizard-next");
  await h.flush();

  mod.dispatch(spec.publish ? "publish-new" : "save-draft");
  await h.flush();
}

function setup(opts) {
  const backend = makeBackend();
  const h = makeHarness(backend, opts);
  const mod = freshModule();
  mod.init(h.host);
  return { backend, h, mod };
}

const BASE = {
  prompt: "Which feature do you prefer?",
  options: "a | Free Spins\nb | Cashback",
  correct: "a",
  starts: "2026-09-05T20:00:00",
  ends: "2026-09-05T22:00:00",
};

// ---------------------------------------------------------------------------
// QA 1 — create two missions through the wizard
// ---------------------------------------------------------------------------

test("QA: create A and create B, each landing as its own draft", async () => {
  const { backend, h, mod } = setup();
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission A", campaign_id: "camp-a", pool_id: "MISSION-5", winners: 5 }));
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission B", campaign_id: "camp-b", pool_id: "WEEKEND", winners: 7 }));

  assert.deepEqual(Object.keys(backend.campaigns).sort(), ["camp-a", "camp-b"]);
  assert.equal(backend.campaigns["camp-a"].mission_pool.pool_id, "MISSION-5");
  assert.equal(backend.campaigns["camp-a"].mission_pool.winner_count, 5);
  // Each pool's REAL registered type is stored, not a guess.
  assert.equal(backend.campaigns["camp-a"].mission_pool.pool_type, "voucher_drop");
  assert.equal(backend.campaigns["camp-b"].mission_pool.pool_type, "vip");
  assert.equal(backend.campaigns["camp-b"].mission_pool.winner_count, 7);
  // Both are drafts: nothing was published implicitly.
  assert.equal(backend.campaigns["camp-a"].status, "draft");
  assert.equal(backend.campaigns["camp-b"].status, "draft");
  assert.deepEqual(backend.writes.map((w) => w.op), ["create", "create"]);
  assert.deepEqual(
    backend.campaigns["camp-a"].mission_config.options,
    [{ id: "a", label: "Free Spins" }, { id: "b", label: "Cashback" }],
    "player-facing labels survive the wizard");
});

// ---------------------------------------------------------------------------
// QA 2 — open A, edit A, save: only A changes
// ---------------------------------------------------------------------------

test("QA: editing A writes to A and nothing else", async () => {
  const { backend, h, mod } = setup();
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission A", campaign_id: "camp-a", pool_id: "MISSION-5", winners: 5 }));
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission B", campaign_id: "camp-b", pool_id: "WEEKEND", winners: 7 }));
  const bBefore = JSON.stringify(backend.campaigns["camp-b"]);
  backend.writes.length = 0;

  mod.open("camp-a");
  await h.flush();
  mod.dispatch("edit", "camp-a");
  await h.flush();
  h.set("mp-e-winners", "9");
  mod.dispatch("save-edit", "camp-a");
  await h.flush();

  assert.deepEqual(backend.writes.map((w) => [w.op, w.campaign_id]), [["update", "camp-a"]]);
  assert.equal(backend.campaigns["camp-a"].mission_pool.winner_count, 9);
  assert.equal(JSON.stringify(backend.campaigns["camp-b"]), bBefore, "B is untouched");
});

// ---------------------------------------------------------------------------
// QA 3 — open B immediately after A: B only
// ---------------------------------------------------------------------------

test("QA: opening B while A is still loading shows and writes only B", async () => {
  const { backend, h, mod } = setup();
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission A", campaign_id: "camp-a", pool_id: "MISSION-5", winners: 5 }));
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission B", campaign_id: "camp-b", pool_id: "WEEKEND", winners: 7 }));
  const aBefore = JSON.stringify(backend.campaigns["camp-a"]);
  backend.writes.length = 0;

  // Hold every read for A, then start editing B before A ever answers.
  const held = makeHarness(backend, { holdGet: /camp-a/ });
  const mod2 = freshModule();
  mod2.init(held.host);

  mod2.dispatch("edit", "camp-a");
  await held.flush();
  mod2.dispatch("edit", "camp-b");
  await held.flush();
  held.releaseHeld();              // A's responses land last
  await held.flush();

  assert.equal(mod2.session().editModeTarget(), "camp-b");
  assert.ok(held.html().includes("camp-b"));
  assert.ok(!held.html().includes("camp-a"), "A's late response must not paint over B");
  // The form must hold B's OWN values, not A's under B's label: A and B were
  // created with different names and different reward pools precisely so a
  // hydration mix-up is visible rather than plausible.
  assert.ok(held.html().includes("Mission B"), "the form shows B's name");
  assert.ok(!held.html().includes("Mission A"), "A's values must not be in B's form");
  assert.equal(held.node("mp-e-pool").value, "WEEKEND", "B's own pool is selected");
  assert.equal(held.node("mp-e-winners").value, "7", "B's own winner target is shown");

  held.set("mp-e-winners", "11");
  mod2.dispatch("save-edit", "camp-b");
  await held.flush();
  assert.deepEqual(backend.writes.map((w) => [w.op, w.campaign_id]), [["update", "camp-b"]]);
  const written = backend.writes[0].body;
  assert.equal(written.name, "Mission B");
  assert.equal(written.mission_pool.pool_id, "WEEKEND", "A's pool must not be written onto B");
  assert.equal(written.mission_pool.pool_type, "vip");
  assert.equal(backend.campaigns["camp-b"].mission_pool.winner_count, 11);
  assert.equal(JSON.stringify(backend.campaigns["camp-a"]), aBefore, "A is untouched");
});

// ---------------------------------------------------------------------------
// QA 4 — leave edit, start create: no trace of A
// ---------------------------------------------------------------------------

test("QA: starting a create after editing A carries no A state and creates a new campaign", async () => {
  const { backend, h, mod } = setup();
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission A", campaign_id: "camp-a", pool_id: "MISSION-5", winners: 5 }));
  const aBefore = JSON.stringify(backend.campaigns["camp-a"]);
  backend.writes.length = 0;

  mod.open("camp-a");
  await h.flush();
  mod.dispatch("edit", "camp-a");
  await h.flush();
  mod.dispatch("back-to-list");
  await h.flush();

  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission C", campaign_id: "camp-c", pool_id: "WEEKEND", winners: 3 }));

  assert.deepEqual(backend.writes.map((w) => [w.op, w.campaign_id]), [["create", "camp-c"]],
    "a create never issues an update against the previously edited campaign");
  assert.equal(JSON.stringify(backend.campaigns["camp-a"]), aBefore, "A is untouched");
  assert.equal(backend.campaigns["camp-c"].name, "Mission C");
  // A's option labels did not leak into C.
  assert.deepEqual(backend.campaigns["camp-c"].mission_config.options,
    [{ id: "a", label: "Free Spins" }, { id: "b", label: "Cashback" }]);
});

// ---------------------------------------------------------------------------
// QA 5 — edit A's pool, save: only A
// ---------------------------------------------------------------------------

test("QA: changing A's reward pool writes only A, with the new pool's real type", async () => {
  const { backend, h, mod } = setup();
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission A", campaign_id: "camp-a", pool_id: "MISSION-5", winners: 5 }));
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission B", campaign_id: "camp-b", pool_id: "WEEKEND", winners: 7 }));
  const bBefore = JSON.stringify(backend.campaigns["camp-b"]);
  backend.writes.length = 0;

  mod.dispatch("edit", "camp-a");
  await h.flush();
  h.set("mp-e-pool", "WEEKEND");
  mod.dispatch("save-edit", "camp-a");
  await h.flush();

  assert.deepEqual(backend.writes.map((w) => [w.op, w.campaign_id]), [["update", "camp-a"]]);
  assert.equal(backend.campaigns["camp-a"].mission_pool.pool_id, "WEEKEND");
  assert.equal(JSON.stringify(backend.campaigns["camp-b"]), bBefore);
});

// ---------------------------------------------------------------------------
// QA 6 — edit B's schedule, save: only B, and A's instants stay exact
// ---------------------------------------------------------------------------

test("QA: changing B's schedule writes only B and leaves untouched instants byte-identical", async () => {
  const { backend, h, mod } = setup();
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission A", campaign_id: "camp-a", pool_id: "MISSION-5", winners: 5 }));
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Mission B", campaign_id: "camp-b", pool_id: "WEEKEND", winners: 7,
    starts: "2026-09-06T20:00:37", ends: "2026-09-06T22:00:11" }));
  const aBefore = JSON.stringify(backend.campaigns["camp-a"]);
  const bEndsBefore = backend.campaigns["camp-b"].schedule.ends_at;
  backend.writes.length = 0;

  mod.dispatch("edit", "camp-b");
  await h.flush();
  h.set("mp-e-starts", "2026-09-07T09:30:15");
  mod.dispatch("save-edit", "camp-b");
  await h.flush();

  assert.deepEqual(backend.writes.map((w) => [w.op, w.campaign_id]), [["update", "camp-b"]]);
  assert.equal(backend.campaigns["camp-b"].schedule.starts_at,
    new Date("2026-09-07T09:30:15").toISOString());
  // The end instant was never touched, so it must come back byte-identical —
  // schedule.ends_at is one of Phase 1's eligibility cutoffs.
  assert.equal(backend.campaigns["camp-b"].schedule.ends_at, bEndsBefore);
  assert.equal(JSON.stringify(backend.campaigns["camp-a"]), aBefore);
});

// ---------------------------------------------------------------------------
// QA 7 — create with an inline pool, publish, copy the link
// ---------------------------------------------------------------------------

test("QA: create with a new inline pool, publish, and copy the mission link", async () => {
  const { backend, h, mod } = setup();
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "September Feedback Mission", campaign_id: "sep-feedback", winners: 3, publish: true,
    newPool: { pool_id: "SEP-5", name: "September Mission $5", usage: "MYR 5",
      codes: "ABC001\nABC002\nABC001\nABC003\nABC004" },
  }));

  // The pool went through the canonical Voucher Centre endpoints, duplicates
  // removed, before the campaign was created.
  assert.deepEqual(backend.writes.map((w) => w.op),
    ["pool_create", "pool_upload", "create", "publish"]);
  assert.equal(backend.pools["SEP-5"].codes, 4, "one duplicate code was dropped");
  assert.equal(backend.campaigns["sep-feedback"].mission_pool.pool_id, "SEP-5");
  assert.equal(backend.campaigns["sep-feedback"].status, "live");

  // The operator lands on the mission's own detail view with a copyable link.
  assert.ok(h.html().includes("Mission Link"));
  mod.dispatch("copy-link", "https://t.me/AdvantPlayBot?startapp=mission_sep-feedback");
  assert.deepEqual(h.copied, ["https://t.me/AdvantPlayBot?startapp=mission_sep-feedback"]);
});

test("QA: publishing is refused when the inline pool cannot cover the winner target", async () => {
  const { backend, h, mod } = setup();
  await createMission(mod, h, Object.assign({}, BASE, {
    name: "Short Mission", campaign_id: "short-mission", winners: 10, publish: true,
    newPool: { pool_id: "SHORT-1", name: "Short pool", codes: "A1\nA2" },
  }));

  assert.equal(backend.campaigns["short-mission"], undefined, "no campaign is created");
  assert.equal(backend.writes.some((w) => w.op === "publish"), false);
  assert.ok(h.toasts.some((t) => /Publishing blocked/.test(t.msg)));
  // The already-created pool is named so the operator reuses it on retry
  // instead of registering a second one.
  assert.ok(h.toasts.some((t) => /SHORT-1 was already created/.test(t.msg)));
});
