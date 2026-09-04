/**
 * Mission Reward Pool — dedicated admin experience (Phase 2.1).
 *
 * The Phase 2 version of this suite asserted on slices of
 * static/admin-dashboard.js source text, because Mission creation and
 * Mission editing shared that file's Campaign Centre form and there was no
 * seam to test through. The shared form is gone: the Mission surface is now
 * static/mission-admin.js, whose decision-making core is importable, so the
 * safety properties below are exercised for real rather than pattern-matched
 * in a comment.
 *
 * What these tests exist to protect:
 *
 *   * a CREATE action can never modify an existing campaign,
 *   * an EDIT action can never target a different campaign,
 *   * a stale async response can never overwrite the active campaign's state,
 *   * the operator can go create -> configure -> reward -> schedule ->
 *     review -> publish -> copy link without leaving the Mission surface,
 *   * Phase 1's freeze / lifecycle / pool rules are obeyed, never re-derived,
 *   * Standard Drop and the Campaign Centre form are untouched.
 *
 * Run with: node --test test_mission_admin_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const MODULE_PATH = path.join(__dirname, "static", "mission-admin.js");
const MISSION_JS = fs.readFileSync(MODULE_PATH, "utf8");
const DASH_JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");
const HTML = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.html"), "utf8");

const CORE = require(MODULE_PATH).core;

/**
 * Assertions about what the CODE does must not be satisfied — or broken — by
 * prose in a comment.
 */
function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
}
const MISSION_CODE = stripComments(MISSION_JS);

/** A fresh module instance — the view layer keeps module-level state. */
function freshModule() {
  delete require.cache[require.resolve(MODULE_PATH)];
  return require(MODULE_PATH);
}

// ---------------------------------------------------------------------------
// Minimal DOM + host harness. The module only ever reaches the page through
// host.$, so a registry of plain objects is enough to drive it end to end.
// ---------------------------------------------------------------------------

function makeHarness(routes) {
  const nodes = {};
  const calls = [];
  const toasts = [];
  const copied = [];
  const pending = [];

  function mkNode(props) {
    return Object.assign({ id: "", value: "", checked: false, disabled: false, dataset: {},
      addEventListener() {}, contains() { return true; } }, props);
  }

  function unesc(s) {
    return String(s).replace(/&lt;/g, "<").replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, "&");
  }
  function attr(attrs, name) {
    const m = new RegExp(name + '="([^"]*)"').exec(attrs);
    return m ? m[1] : null;
  }

  /**
   * Rebuild the form-control registry from whatever the module last
   * rendered, the way a browser would. Controls that are not on screen are
   * simply absent, so a handler reading them gets `undefined` — exactly what
   * happens in the real dashboard.
   */
  function rescan(html) {
    Object.keys(nodes).forEach((k) => { if (k !== "#mp-root") delete nodes[k]; });
    let m;
    const inputRe = /<input\b([^>]*)>/g;
    while ((m = inputRe.exec(html))) {
      const id = attr(m[1], "id");
      if (id) nodes["#" + id] = mkNode({
        id: id, value: unesc(attr(m[1], "value") || ""),
        checked: /\bchecked\b/.test(m[1]), disabled: /\bdisabled\b/.test(m[1]),
      });
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

  function route(method, pathname) {
    const key = method + " " + pathname;
    for (const pattern of Object.keys(routes)) {
      if (key === pattern) return routes[pattern];
      if (pattern.endsWith("*") && key.startsWith(pattern.slice(0, -1))) return routes[pattern];
    }
    return undefined;
  }

  function respond(method, pathname, body) {
    calls.push({ method, path: pathname, body });
    const handler = route(method, pathname);
    const value = typeof handler === "function" ? handler(pathname, body) : handler;
    if (value === undefined) return Promise.reject(new Error("no route for " + method + " " + pathname));
    if (value && typeof value.then === "function") return value;
    // Deferred responses let a test resolve A after B on purpose.
    if (value && value.__deferred) {
      return new Promise((resolve) => pending.push({ path: pathname, resolve, value: value.value }));
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
    apiPostJson: (p, b) => respond("POSTJ", p, b).then((d) => ({ ok: true, d })),
    apiPutJson: (p, b) => respond("PUTJ", p, b).then((d) => ({ ok: true, d })),
    toast: (msg, kind) => toasts.push({ msg, kind }),
    confirm: () => true,
    copy: (text) => copied.push(text),
  };

  return {
    host, nodes, calls, toasts, copied, pending,
    root: () => root,
    html: () => root.innerHTML,
    node: (id) => nodes["#" + id],
    set: (id, value) => { (nodes["#" + id] = nodes["#" + id] || mkNode({ id })).value = value; },
    // Fire the real DOM event through the module's own listener, so the
    // wiring between a control changing and the panel reacting is covered.
    fireChange: (id) => (listeners.change || []).forEach((fn) => fn({ target: nodes["#" + id] })),
    flush: () => new Promise((r) => setImmediate(r)),
  };
}

function deferred(value) { return { __deferred: true, value }; }

const POOLS_OK = {
  status: "ok",
  pool_types: ["voucher_drop", "vip", "tournament_reward"],
  pools: [
    { pool_id: "MISSION-5", name: "September Mission $5", pool_type: "voucher_drop",
      allocation_scope: "campaign_rewards", status: "active", stock: { available: 50, issued: 0 } },
    { pool_id: "WEEKEND", name: "Weekend Reward", pool_type: "vip",
      allocation_scope: "campaign_rewards", status: "active", stock: { available: 210, issued: 3 } },
  ],
};

function campaignDoc(id, overrides) {
  return Object.assign({
    campaign_id: id,
    name: "Mission " + id,
    type: "mission_pool",
    status: "live",
    schedule: { starts_at: "2026-09-05T12:00:37+00:00", ends_at: "2026-09-05T14:00:00+00:00" },
    mission_config: {
      mission_type: "multiple_choice",
      prompt: "Which feature do you prefer?",
      options: [{ id: "a", label: "Free Spins" }, { id: "b", label: "Cashback" }],
      correct_answer: "a",
    },
    mission_pool: {
      pool_id: "MISSION-5", pool_type: "voucher_drop", winner_count: 20,
      allocation_method: "random_qualified",
      eligibility_policy: {
        require_correct_answer: true, exclude_voucher_hunter: true,
        exclude_multi_account_risk: true, exclude_blocked: true, require_gaming_account: false,
      },
    },
  }, overrides || {});
}

function editState(id, overrides) {
  return Object.assign({
    status: "ok", campaign_id: id, campaign_status: "live", state: "live", entries: 0,
    mission_config_locked: false, locked_fields: [], schedule_editable: true,
    cancelled: false, closed_at: null, processing_stage: "pending",
    mission_link: "https://t.me/AdvantPlayBot?startapp=mission_" + id,
    mission_link_unavailable_reason: null,
    reward: {
      pool_id: "MISSION-5", pool_name: "September Mission $5", pool_type: "voucher_drop",
      pool_exists: true, pool_selectable: true, pool_active: true, allocation_scope: "campaign_rewards",
      winner_count: 20, available: 50, issued: 0, shortfall: 0, sufficient: true,
      allocation_method: "random_qualified", allocation_started: false, pool_editable: true,
    },
  }, overrides || {});
}

const SUMMARY_OK = {
  status: "ok", processing_stage: "completed", winner_count_requested: 20,
  grains: {
    submissions_telegram_user_grain: 124, deduplicated_identity_grain: 118,
    qualified_identity_grain: 110, disqualified_telegram_user_grain: 14,
    winners_identity_grain: 20, rewards_allocated_voucher_grain: 20,
    notifications_sent_voucher_grain: 18, notifications_failed_voucher_grain: 2,
  },
  disqualification_reasons: { duplicate_identity: 9, voucher_hunter: 5 },
};

// ===========================================================================
// §14 — the shared-form failure class, removed structurally
// ===========================================================================

test("create mode has no campaign target, and never can", () => {
  const s = CORE.createSession();
  s.enterCreate({});
  assert.equal(s.mode(), "create");
  assert.equal(s.createModeTarget(), null);
  assert.equal(s.editModeTarget(), null);
  assert.equal(s.target(), null);
});

test("edit mode fixes exactly one target and create mode has none — never both", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  assert.equal(s.editModeTarget(), "camp-a");
  assert.equal(s.createModeTarget(), null);

  s.enterCreate({});
  assert.equal(s.editModeTarget(), null, "leaving edit drops the save target");
  assert.equal(s.createModeTarget(), null);
});

test("save is refused outside edit mode, not merely hidden", () => {
  const s = CORE.createSession();
  s.enterList();
  assert.deepEqual(s.authorizeSave(), { ok: false, code: "not_in_edit_mode" });
  s.enterCreate({});
  assert.deepEqual(s.authorizeSave(), { ok: false, code: "not_in_edit_mode" });
  s.enterView("camp-a");
  assert.deepEqual(s.authorizeSave(), { ok: false, code: "not_in_edit_mode" });
});

test("create is refused while editing, not merely hidden", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  assert.deepEqual(s.authorizeCreate(), { ok: false, code: "in_edit_mode" });
  s.enterList();
  assert.deepEqual(s.authorizeCreate(), { ok: false, code: "not_in_create_mode" });
  s.enterCreate({});
  assert.equal(s.authorizeCreate().ok, true);
  assert.equal(s.authorizeCreate().campaignId, null, "a create never carries a campaign target");
});

test("save is refused until hydration has actually completed", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  assert.deepEqual(s.authorizeSave(), { ok: false, code: "not_hydrated" });
  const load = s.beginLoad("camp-a");
  assert.equal(s.completeHydration(load, {}), true);
  assert.deepEqual(s.authorizeSave(), { ok: true, campaignId: "camp-a" });
});

test("a save aimed at a different campaign than the fixed target is refused", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  s.completeHydration(s.beginLoad("camp-a"), {});
  assert.deepEqual(s.authorizeSave("camp-b"), { ok: false, code: "wrong_campaign_target" });
  assert.deepEqual(s.authorizeSave("camp-a"), { ok: true, campaignId: "camp-a" });
});

test("hydration belonging to another campaign can never authorise a save", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  const loadA = s.beginLoad("camp-a");
  // A's response arrives after the operator has already switched to B.
  s.enterEdit("camp-b");
  assert.equal(s.completeHydration(loadA, {}), false, "A's load cannot hydrate B");
  assert.deepEqual(s.authorizeSave(), { ok: false, code: "not_hydrated" });
});

// ===========================================================================
// §15 — async race safety
// ===========================================================================

test("every async response is validated against the active mode AND target", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  const load = s.beginLoad("camp-a");
  assert.equal(s.accepts(load), true);

  s.enterEdit("camp-b");
  assert.equal(s.accepts(load), false, "a different target rejects it");

  s.enterEdit("camp-a");
  assert.equal(s.accepts(load), false, "the token moved on, so it is still stale");
});

test("re-entering the same mode for the same campaign still invalidates old loads", () => {
  const s = CORE.createSession();
  s.enterView("camp-a");
  const first = s.beginLoad("camp-a");
  s.enterView("camp-a");
  assert.equal(s.accepts(first), false, "a reopen must not accept the previous load's response");
  assert.equal(s.accepts(s.beginLoad("camp-a")), true);
});

test("a view-mode response cannot be accepted by edit mode for the same campaign", () => {
  const s = CORE.createSession();
  s.enterView("camp-a");
  const viewLoad = s.beginLoad("camp-a");
  s.enterEdit("camp-a");
  assert.equal(s.accepts(viewLoad), false);
});

test("no mode transition can leave a hydration record attached to a superseded token", () => {
  // authorizeSave refuses a stale hydration token as an assertion of last
  // resort; this is the property that makes that case unreachable today.
  const s = CORE.createSession();
  ["enterList", "enterCreate", "enterView", "enterEdit"].forEach((entry) => {
    s.enterEdit("camp-a");
    s.completeHydration(s.beginLoad("camp-a"), {});
    assert.equal(s.hydration().token, s.token());
    const before = s.token();
    s[entry]("camp-x");
    assert.ok(s.token() > before, entry + " must invalidate in-flight loads");
    assert.equal(s.hydration(), null, entry + " must clear the hydration record");
  });
});

test("leaving edit clears every piece of per-campaign state", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  s.completeHydration(s.beginLoad("camp-a"), {
    optionLabels: { a: "Alpha" }, starts: { iso: "X", display: "Y" },
    storedPool: { pool_id: "P", pool_type: "vip" },
  });
  assert.ok(s.editState().optionLabels);

  s.enterCreate({});
  assert.equal(s.editState(), null, "option label maps, schedule instants and pool metadata are dropped");
  assert.equal(s.hydration(), null);
  assert.equal(s.editModeTarget(), null);

  s.enterList();
  assert.equal(s.draft(), null, "the create draft does not survive either");
});

test("switching campaigns rapidly cannot hydrate the wrong one", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/camp-a": deferred({ status: "ok", campaign: campaignDoc("camp-a", { name: "AAA" }) }),
    "GET /api/admin/mission-pool/camp-a/edit-state": deferred(editState("camp-a")),
    "GET /api/admin/gc-campaigns/camp-b": { status: "ok", campaign: campaignDoc("camp-b", { name: "BBB" }) },
    "GET /api/admin/mission-pool/camp-b/edit-state": editState("camp-b"),
  });
  mod.init(h.host);
  const s = mod.session();

  mod.open("camp-a");                 // starts A's detail load (deferred)
  await h.flush();
  h.root().innerHTML = "";
  // Operator jumps straight into editing B while A is still in flight.
  s.enterEdit("camp-b");
  h.pending.forEach((p) => p.resolve(p.value));   // A finally answers
  await h.flush();
  await h.flush();
  assert.equal(h.html(), "", "A's late response must not paint anything");
  assert.equal(s.editModeTarget(), "camp-b");
});

test("a late pool response cannot repaint another campaign's edit form", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": deferred(POOLS_OK),
    "GET /api/admin/gc-campaigns/camp-a": { status: "ok", campaign: campaignDoc("camp-a") },
    "GET /api/admin/mission-pool/camp-a/edit-state": editState("camp-a"),
  });
  mod.init(h.host);
  const s = mod.session();
  s.enterEdit("camp-a");

  mod.open("camp-a");            // moves to VIEW mode, invalidating the edit load
  h.root().innerHTML = "";
  h.pending.forEach((p) => p.resolve(p.value));
  await h.flush();
  await h.flush();
  // The pool loader returns DATA; only an accepted load may render it.
  assert.ok(!h.html().includes("Edit Mission"), "a stale pool load cannot rebuild an edit form");
});

// ===========================================================================
// §4 — mission fields, options, ids and labels
// ===========================================================================

test("each mission type shows only the fields Phase 1 supports", () => {
  assert.deepEqual(CORE.MISSION_TYPE_FIELDS.multiple_choice, ["prompt", "options", "correct"]);
  assert.deepEqual(CORE.MISSION_TYPE_FIELDS.single_choice, ["prompt", "options", "correct"]);
  assert.deepEqual(CORE.MISSION_TYPE_FIELDS.keyword, ["prompt", "correct", "case_insensitive"]);
  assert.deepEqual(CORE.MISSION_TYPE_FIELDS.feedback, ["prompt", "min_chars", "max_chars"]);
  assert.equal(CORE.MISSION_TYPE_FIELDS.keyword.indexOf("options"), -1);
  assert.equal(CORE.MISSION_TYPE_FIELDS.feedback.indexOf("options"), -1);
});

test("options are one per line, and an id containing a comma stays one option", () => {
  const parsed = CORE.parseOptionLines("red,blue\ngreen\n", {});
  assert.deepEqual(parsed.options, [
    { id: "red,blue", label: "red,blue" },
    { id: "green", label: "green" },
  ]);
  assert.deepEqual(parsed.errors, []);
});

test("an option id and its player-facing label are both editable and preserved", () => {
  const parsed = CORE.parseOptionLines("red | Red Team\nblue | Blue Team", {});
  assert.deepEqual(parsed.options, [
    { id: "red", label: "Red Team" },
    { id: "blue", label: "Blue Team" },
  ]);
  // Labels are never collapsed back to ids on a round trip.
  assert.equal(CORE.formatOptionLines(parsed.options), "red | Red Team\nblue | Blue Team");
});

test("an untouched option keeps the label it already had", () => {
  // Rebuilding {id,label} from ids alone would rewrite every participant-facing
  // label with its raw id for a campaign created through the API.
  const known = { a: "Alpha", b: "Beta" };
  assert.deepEqual(CORE.parseOptionLines("a\nb", known).options,
    [{ id: "a", label: "Alpha" }, { id: "b", label: "Beta" }]);
  // A newly typed id falls back to id-as-label, matching the create flow.
  assert.deepEqual(CORE.parseOptionLines("a\nc", known).options,
    [{ id: "a", label: "Alpha" }, { id: "c", label: "c" }]);
});

test("a hydrated label map is never applied to a different campaign", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  s.completeHydration(s.beginLoad("camp-a"), { optionLabels: { a: "Alpha" } });
  s.enterEdit("camp-b");
  assert.equal(s.editState().optionLabels, undefined,
    "camp-a's labels cannot survive into camp-b's edit session");
});

test("option ids that cannot round-trip through the editor are made read-only", () => {
  assert.equal(CORE.optionsEditable([{ id: "a" }, { id: "b" }]), true);
  assert.equal(CORE.optionsEditable([{ id: "a|b" }]), false,
    "an id containing the label separator must not be silently split");
});

test("duplicate and empty option lines are reported, not silently accepted", () => {
  const dupe = CORE.parseOptionLines("a\na", {});
  assert.equal(dupe.options.length, 1);
  assert.match(dupe.errors[0], /Duplicate option id/);
  const blank = CORE.parseOptionLines(" | Label", {});
  assert.equal(blank.options.length, 0);
  assert.match(blank.errors[0], /no id/);
});

test("a campaign id that cannot carry a Mission link is rejected at creation", () => {
  assert.equal(CORE.campaignIdIsLinkSafe("september-feedback"), true);
  assert.equal(CORE.campaignIdIsLinkSafe("september feedback"), false);
  assert.equal(CORE.campaignIdIsLinkSafe("september/feedback"), false);
  assert.equal(CORE.campaignIdIsLinkSafe("x".repeat(CORE.MAX_LINK_SAFE_ID_CHARS + 1)), false);
  assert.equal(CORE.slugify("September Feedback Mission!"), "september-feedback-mission");
});

// ===========================================================================
// §7, §8 — inline reward pool + the publish gate
// ===========================================================================

test("voucher codes are one per line, with duplicates reported and removed", () => {
  const parsed = CORE.parseVoucherCodes("ABC001\nABC002\n ABC001 \n\nABC003\nABC002");
  assert.deepEqual(parsed.codes, ["ABC001", "ABC002", "ABC003"]);
  assert.equal(parsed.duplicates, 2);
});

test("the inventory gate is winner_count <= available_codes, inclusive", () => {
  assert.equal(CORE.inventoryGate(20, 10).ok, true);
  assert.equal(CORE.inventoryGate(20, 20).ok, true);
  assert.equal(CORE.inventoryGate(20, 21).ok, false);
  assert.equal(CORE.inventoryGate(20, 21).shortfall, 1);
  assert.equal(CORE.inventoryGate(20, 0).ok, false, "a zero winner target is never publishable");
});

test("publishing is blocked when the backend says inventory is short", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/mission-pool/inventory-check*": {
      status: "ok", pool_exists: true, pool_type: "voucher_drop",
      winner_count: 99, available: 50, shortfall: 49, sufficient: false,
    },
  });
  mod.init(h.host);
  const s = mod.session();
  s.enterList();
  mod.dispatch("create");
  await h.flush();
  const d = s.draft();
  Object.assign(d, {
    step: 3, name: "M", campaign_id: "m1", prompt: "Q", options_text: "a\nb",
    correct_answer: "a", reward_mode: "existing", pool_id: "MISSION-5", winner_count: 99,
    starts_at: "2026-09-05T20:00:00", ends_at: "2026-09-05T22:00:00",
  });
  h.root().innerHTML = "";
  // Drive the same path the "Publish Mission" button drives.
  await mod.dispatch("publish-new");
  await h.flush();

  assert.equal(h.calls.filter((c) => c.method === "POSTJ" && c.path === "/api/admin/gc-campaigns").length, 0,
    "no campaign is created when the publish gate fails");
  assert.ok(h.toasts.some((t) => /Publishing blocked/.test(t.msg)));
});

test("Save Draft creates a draft and never publishes", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/mission-pool/inventory-check*": {
      status: "ok", pool_exists: true, pool_type: "vip",
      winner_count: 2, available: 210, shortfall: 0, sufficient: true,
    },
    "POSTJ /api/admin/gc-campaigns": { status: "ok", campaign_id: "m1" },
    "GET /api/admin/gc-campaigns/m1": { status: "ok", campaign: campaignDoc("m1") },
    "GET /api/admin/mission-pool/m1/edit-state": editState("m1"),
    "GET /api/admin/mission-pool/m1/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  mod.dispatch("create");
  await h.flush();
  const d = mod.session().draft();
  Object.assign(d, {
    step: 3, name: "M", campaign_id: "m1", prompt: "Q", options_text: "a\nb",
    correct_answer: "a", reward_mode: "existing", pool_id: "WEEKEND", winner_count: 2,
    starts_at: "2026-09-05T20:00:00", ends_at: "2026-09-05T22:00:00",
  });
  await mod.dispatch("save-draft");
  await h.flush(); await h.flush();

  const create = h.calls.find((c) => c.method === "POSTJ" && c.path === "/api/admin/gc-campaigns");
  assert.ok(create, "the draft is created");
  assert.equal(h.calls.some((c) => c.path.endsWith("/publish")), false,
    "Save Draft must never publish implicitly");
  // The pool's REAL registered type is submitted, not a hardcoded guess.
  assert.equal(create.body.mission_pool.pool_type, "vip");
  assert.equal(create.body.mission_pool.pool_id, "WEEKEND");
  assert.equal(create.body.type, "mission_pool");
});

test("Publish Mission is a separate explicit action after creation", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/mission-pool/inventory-check*": {
      status: "ok", pool_exists: true, pool_type: "voucher_drop",
      winner_count: 20, available: 50, shortfall: 0, sufficient: true,
    },
    "POSTJ /api/admin/gc-campaigns": { status: "ok", campaign_id: "m1" },
    "POST /api/admin/gc-campaigns/m1/publish": { status: "ok", campaign_status: "live" },
    "GET /api/admin/gc-campaigns/m1": { status: "ok", campaign: campaignDoc("m1") },
    "GET /api/admin/mission-pool/m1/edit-state": editState("m1"),
    "GET /api/admin/mission-pool/m1/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  mod.dispatch("create");
  await h.flush();
  const d = mod.session().draft();
  Object.assign(d, {
    step: 3, name: "M", campaign_id: "m1", prompt: "Q", options_text: "a | Alpha\nb | Beta",
    correct_answer: "a", reward_mode: "existing", pool_id: "MISSION-5", winner_count: 20,
    starts_at: "2026-09-05T20:00:00", ends_at: "2026-09-05T22:00:00",
  });
  await mod.dispatch("publish-new");
  await h.flush(); await h.flush(); await h.flush();

  const order = h.calls.filter((c) => c.method === "POSTJ" || c.method === "POST").map((c) => c.path);
  assert.deepEqual(order, ["/api/admin/gc-campaigns", "/api/admin/gc-campaigns/m1/publish"]);
  const create = h.calls.find((c) => c.path === "/api/admin/gc-campaigns");
  assert.deepEqual(create.body.mission_config.options,
    [{ id: "a", label: "Alpha" }, { id: "b", label: "Beta" }]);
  // The detail view opens after publishing, never the create wizard again.
  assert.ok(h.html().includes("Mission Link"));
});

test("inline pool creation goes through the canonical Voucher Centre endpoints", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "POSTJ /api/admin/reward-pools": { status: "ok", pool_id: "SEP-5" },
    "POSTJ /api/admin/reward-pools/SEP-5/upload-codes": { status: "ok", inserted: 3, skipped_duplicates: 1 },
    "GET /api/admin/mission-pool/inventory-check*": {
      status: "ok", pool_exists: true, pool_type: "voucher_drop",
      winner_count: 2, available: 3, shortfall: 0, sufficient: true,
    },
    "POSTJ /api/admin/gc-campaigns": { status: "ok", campaign_id: "m1" },
    "GET /api/admin/gc-campaigns/m1": { status: "ok", campaign: campaignDoc("m1") },
    "GET /api/admin/mission-pool/m1/edit-state": editState("m1"),
    "GET /api/admin/mission-pool/m1/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  mod.dispatch("create");
  await h.flush();
  const d = mod.session().draft();
  Object.assign(d, {
    step: 3, name: "M", campaign_id: "m1", prompt: "Q", options_text: "a\nb", correct_answer: "a",
    reward_mode: "new", new_pool_id: "SEP-5", new_pool_name: "September Mission $5",
    new_pool_type: "voucher_drop", new_pool_reward_usage: "MYR 5",
    new_pool_codes_text: "ABC001\nABC002\nABC001\nABC003",
    winner_count: 2, starts_at: "2026-09-05T20:00:00", ends_at: "2026-09-05T22:00:00",
  });
  await mod.dispatch("save-draft");
  await h.flush(); await h.flush(); await h.flush();

  const register = h.calls.find((c) => c.path === "/api/admin/reward-pools");
  const upload = h.calls.find((c) => c.path === "/api/admin/reward-pools/SEP-5/upload-codes");
  assert.ok(register && upload, "the canonical register + upload endpoints are used");
  // Duplicates are removed client-side before upload; the backend still
  // enforces its own uniqueness.
  assert.deepEqual(upload.body.codes, ["ABC001", "ABC002", "ABC003"]);
  // Ownership metadata is never supplied by the client — upload_codes stamps
  // it from the registry and rejects an attempted override outright.
  assert.equal("pool_type" in upload.body, false);
  assert.equal("allocation_scope" in upload.body, false);
  const create = h.calls.find((c) => c.path === "/api/admin/gc-campaigns");
  assert.equal(create.body.mission_pool.pool_id, "SEP-5");
});

test("no Mission-specific inventory writer exists in the admin surface", () => {
  // §7/§22: reward inventory is only ever created through the shared
  // Voucher Centre / Campaign Rewards API.
  assert.ok(MISSION_JS.includes("/api/admin/reward-pools"));
  ["voucher_pools", "mission_entries", "/v2/miniapp/admin/drops", "/api/admin/pools/upload"]
    .forEach((bad) => assert.equal(MISSION_JS.indexOf('"' + bad), -1,
      "the Mission admin must not write or read " + bad + " directly"));
});

// ===========================================================================
// §9, §10, §17 — schedule handling
// ===========================================================================

test("schedule values keep seconds and round-trip to the same instant", () => {
  const out = CORE.isoToLocalInput("2026-09-30T12:34:37+00:00");
  assert.match(out, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/);
  assert.equal(new Date(out).getTime(), new Date("2026-09-30T12:34:37+00:00").getTime());
  assert.equal(CORE.isoToLocalInput(""), "");
  assert.equal(CORE.isoToLocalInput("not-a-date"), "");
});

test("an untouched schedule field resends the exact original instant", () => {
  const field = CORE.scheduleFieldFrom("2026-09-05T12:00:37.123456+00:00");
  // Unedited: the server's own string goes back verbatim, so no conversion
  // can perturb schedule.ends_at (a Phase 1 eligibility cutoff).
  assert.equal(CORE.scheduleValueForSave(field, field.display), "2026-09-05T12:00:37.123456+00:00");
  // Edited: converted from what the operator actually typed.
  const edited = CORE.scheduleValueForSave(field, "2026-09-05T13:00:00");
  assert.equal(edited, new Date("2026-09-05T13:00:00").toISOString());
  assert.equal(CORE.scheduleValueForSave(field, ""), null);
});

test("schedule instants from one campaign cannot leak into another", () => {
  const s = CORE.createSession();
  s.enterEdit("camp-a");
  s.completeHydration(s.beginLoad("camp-a"), { starts: CORE.scheduleFieldFrom("2026-01-01T00:00:00Z") });
  s.enterEdit("camp-b");
  assert.equal(s.editState().starts, undefined);
});

// ===========================================================================
// §19 — operations actions
// ===========================================================================

function actionKeys(state) { return CORE.actionsFor(state).map((a) => a[0]); }

test("only the actions valid for the current backend state are offered", () => {
  assert.deepEqual(actionKeys({ state: "draft" }), ["publish", "edit"]);
  assert.deepEqual(actionKeys({ state: "scheduled" }), ["publish", "edit"]);
  assert.deepEqual(actionKeys({ state: "live" }), ["edit", "pause", "close", "cancel"]);
  assert.deepEqual(actionKeys({ state: "paused" }), ["edit", "publish", "cancel"]);
  assert.deepEqual(actionKeys({ state: "closed" }), ["process"]);
  assert.deepEqual(actionKeys({ state: "processing" }), ["process"]);
  assert.deepEqual(actionKeys({ state: "completed" }), ["results"]);
  assert.deepEqual(actionKeys({ state: "cancelled" }), ["resume"]);
});

test("a live mission is never offered Process, and a draft is never offered Close", () => {
  assert.equal(actionKeys({ state: "live" }).indexOf("process"), -1);
  assert.equal(actionKeys({ state: "draft" }).indexOf("close"), -1);
});

test("in-flight processing offers Resume Processing, never a reroll", () => {
  assert.equal(CORE.actionsFor({ state: "processing" })[0][1], "Resume Processing");
  assert.equal(CORE.actionsFor({ state: "closed" })[0][1], "Process Campaign");
  ["Run Again", "Recalculate Winners", "Reroll", "Re-roll", "Re-run"].forEach((bad) => {
    assert.equal(MISSION_JS.indexOf(bad), -1, "dangerous action wording present: " + bad);
  });
});

test("Close and Cancel confirm with the documented copy", () => {
  assert.match(CORE.CONFIRM_COPY.close, /Close this mission now\?/);
  assert.match(CORE.CONFIRM_COPY.close, /New valid entries after the close cutoff will not be eligible for rewards\./);
  assert.match(CORE.CONFIRM_COPY.cancel, /Cancel this mission\?/);
  assert.match(CORE.CONFIRM_COPY.cancel, /New submissions and new reward distribution will stop\./);
  assert.match(CORE.CONFIRM_COPY.cancel, /Rewards already allocated to winners will remain valid\./);
});

test("lifecycle actions call the official endpoints and never write status", async () => {
  const seen = [];
  for (const action of ["close", "cancel", "resume", "process", "publish", "pause"]) {
    const mod = freshModule();
    const h = makeHarness({
      "POST *": (p) => { seen.push(p); return { status: "ok" }; },
      "GET /api/admin/gc-campaigns/m1": { status: "ok", campaign: campaignDoc("m1") },
      "GET /api/admin/mission-pool/m1/edit-state": editState("m1"),
      "GET /api/admin/mission-pool/m1/summary": SUMMARY_OK,
    });
    mod.init(h.host);
    await mod.dispatch(action, "m1");
    await h.flush(); await h.flush();
  }
  // publish/pause are the shared Campaign Centre lifecycle; the rest are the
  // official Phase 1 Mission endpoints.
  assert.deepEqual(seen, [
    "/api/admin/mission-pool/m1/close",
    "/api/admin/mission-pool/m1/cancel",
    "/api/admin/mission-pool/m1/resume",
    "/api/admin/mission-pool/m1/process",
    "/api/admin/gc-campaigns/m1/publish",
    "/api/admin/gc-campaigns/m1/pause",
  ]);
  assert.equal(/status:\s*["']ended["']/.test(MISSION_JS), false,
    "the admin UI must never set status=ended directly");
  assert.equal(/gc-campaigns[^"']*\/close/.test(MISSION_JS), false,
    "close must not be routed through the generic campaign endpoint");
});

test("publishing an existing mission is blocked when inventory cannot cover it", async () => {
  // The detail view says publishing is blocked, and the button must agree:
  // campaign_centre._transition only checks that a config and a pool id
  // exist, so nothing else stops an under-stocked mission going live.
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/draft-1/edit-state": editState("draft-1", {
      state: "draft", campaign_status: "draft",
      reward: Object.assign({}, editState("draft-1").reward,
        { winner_count: 20, available: 3, shortfall: 17, sufficient: false }),
    }),
    "GET /api/admin/gc-campaigns/draft-1": { status: "ok", campaign: campaignDoc("draft-1", { status: "draft" }) },
    "GET /api/admin/mission-pool/draft-1/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("publish", "draft-1");
  await h.flush(); await h.flush();

  assert.equal(h.calls.some((c) => c.method === "POST"), false, "no publish request is issued");
  assert.ok(h.toasts.some((t) => /Publishing blocked/.test(t.msg) && /MISSION-5/.test(t.msg)));
});

test("resuming a paused mission obeys the same inventory gate as publishing", async () => {
  // Resume IS the publish transition, so a drained pool blocks it too.
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/paused-1/edit-state": editState("paused-1", {
      state: "paused", campaign_status: "paused",
      reward: Object.assign({}, editState("paused-1").reward,
        { winner_count: 20, available: 0, shortfall: 20, sufficient: false }),
    }),
    "GET /api/admin/gc-campaigns/paused-1": { status: "ok", campaign: campaignDoc("paused-1", { status: "paused" }) },
    "GET /api/admin/mission-pool/paused-1/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("publish", "paused-1");
  await h.flush(); await h.flush();
  assert.equal(h.calls.some((c) => c.method === "POST"), false);
});

test("publishing proceeds once inventory covers the winner target", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/draft-1/edit-state": editState("draft-1", {
      state: "draft", campaign_status: "draft",
    }),
    "POST /api/admin/gc-campaigns/draft-1/publish": { status: "ok", campaign_status: "live" },
    "GET /api/admin/gc-campaigns/draft-1": { status: "ok", campaign: campaignDoc("draft-1") },
    "GET /api/admin/mission-pool/draft-1/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("publish", "draft-1");
  await h.flush(); await h.flush(); await h.flush();
  assert.ok(h.calls.some((c) => c.method === "POST" && c.path === "/api/admin/gc-campaigns/draft-1/publish"));
});

test("the inventory verdict is re-read at publish time, never trusted from the page", async () => {
  // Pool stock is shared and moves under the operator between rendering the
  // detail view and pressing Publish.
  const fn = MISSION_CODE.slice(MISSION_CODE.indexOf("function runAction"),
                                MISSION_CODE.indexOf("function onClick"));
  assert.ok(fn.includes("/edit-state"), "the gate refetches the campaign's live reward state");
  assert.ok(fn.includes("reward.sufficient"));
});

test("only publish is gated — close, cancel, resume and process are not", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "POST *": { status: "ok" },
    "GET /api/admin/gc-campaigns/m1": { status: "ok", campaign: campaignDoc("m1") },
    "GET /api/admin/mission-pool/m1/edit-state": editState("m1"),
    "GET /api/admin/mission-pool/m1/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("close", "m1");
  await h.flush(); await h.flush();
  // No inventory precheck stands between an operator and closing a mission.
  assert.equal(h.calls[0].path, "/api/admin/mission-pool/m1/close");
});

test("closing twice is safe because the UI never sends a close cutoff", () => {
  // closed_at is write-once server-side and a repeat close never moves it.
  assert.equal(/closed_at\s*[:=]\s*(new Date|Date\.)/.test(MISSION_JS), false);
});

// ===========================================================================
// §2, §12, §20 — landing list, detail view and results
// ===========================================================================

test("the landing list groups every state exactly once", () => {
  const covered = CORE.LIST_GROUPS.reduce((acc, g) => acc.concat(g.states), []);
  assert.deepEqual(covered.slice().sort(), Object.keys(CORE.STATE_LABELS).sort());
  assert.equal(new Set(covered).size, covered.length, "no state may appear in two groups");
});

test("the landing list is rendered from the server-side aggregate, not from raw entries", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/campaigns": {
      status: "ok", campaigns: [
        { campaign_id: "sep", name: "September Feedback Mission", state: "live",
          submissions: 124, qualified: 110, winners: 20, rewards_allocated: 0,
          pool_available: 35, winner_count: 20, starts_at: null, ends_at: null,
          processing_stage: "pending" },
        { campaign_id: "weekend", name: "Weekend Prediction", state: "scheduled",
          submissions: 0, winners: 0, pool_available: 10, winner_count: 5,
          starts_at: "2026-09-06T12:00:00Z", processing_stage: "pending" },
        { campaign_id: "test01", name: "Mission Test #01", state: "completed",
          submissions: 89, qualified: 72, winners: 10, rewards_allocated: 10,
          pool_available: 0, winner_count: 10, processing_stage: "completed" },
      ],
    },
  });
  mod.init(h.host);
  mod.load();
  await h.flush(); await h.flush();

  const html = h.html();
  assert.ok(html.includes("September Feedback Mission"));
  assert.ok(html.includes("124 submissions") && html.includes("35 codes available"));
  assert.ok(html.includes("Weekend Prediction") && html.includes("Scheduled"));
  assert.ok(html.includes("Mission Test #01") && html.includes("10 rewards issued"));
  assert.ok(html.includes("View Results"), "a completed mission opens its results, not an edit form");
  assert.ok(html.includes("+ Create Mission"));
  // One request, no per-campaign fan-out and no raw entry scan.
  assert.deepEqual(h.calls.map((c) => c.path), ["/api/admin/mission-pool/campaigns"]);
});

test("opening a mission shows the read-only operations view, never the create wizard", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep", { name: "September Feedback Mission" }) },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  mod.open("sep");
  await h.flush(); await h.flush();

  const html = h.html();
  assert.equal(mod.session().mode(), "view");
  assert.equal(mod.session().editModeTarget(), null, "opening a mission does not arm an edit target");
  assert.ok(html.includes("September Feedback Mission"));
  assert.ok(html.includes("Overview") && html.includes("Submitted"));
  assert.ok(html.includes("Free Spins") && html.includes("Cashback"), "the mission definition is shown");
  assert.ok(html.includes("Winner target"));
  assert.ok(html.includes("Mission Link") && html.includes("startapp=mission_sep"));
  assert.ok(html.includes("Actions"));
  assert.ok(!html.includes("Create Mission"), "the create wizard must not be reachable from a detail view");
  assert.ok(!html.includes("Save Draft"));
});

test("the completed view reports every §20 result line as aggregates only", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/gc-campaigns/done": { status: "ok", campaign: campaignDoc("done", { status: "ended" }) },
    "GET /api/admin/mission-pool/done/edit-state": editState("done", {
      state: "completed", campaign_status: "ended", processing_stage: "completed",
    }),
    "GET /api/admin/mission-pool/done/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  mod.open("done");
  await h.flush(); await h.flush();

  const html = h.html();
  ["Submissions", "Qualified", "Disqualified", "Winners", "Rewards allocated",
    "Notifications sent", "Notifications failed"].forEach((label) => {
    assert.ok(html.includes(label), "missing results line: " + label);
  });
  assert.ok(html.includes("Duplicate identity") && html.includes("Voucher hunter"),
    "machine-readable reasons are mapped to human labels");
  assert.ok(html.includes("aggregate, admin only"));
  assert.ok(html.includes("View Results"));
});

test("the mission link is server-generated and copied, never rebuilt in the UI", async () => {
  assert.equal(/["']https:\/\/t\.me\/["']\s*\+/.test(MISSION_JS), false,
    "the admin UI must not concatenate a t.me link");
  assert.equal(MISSION_JS.indexOf("?startapp=mission_"), -1,
    "the start param must not be built in the frontend");

  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  mod.open("sep");
  await h.flush(); await h.flush();
  await mod.dispatch("copy-link", "https://t.me/AdvantPlayBot?startapp=mission_sep");
  assert.deepEqual(h.copied, ["https://t.me/AdvantPlayBot?startapp=mission_sep"]);
});

test("an unavailable mission link is explained rather than shown broken", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep", {
      mission_link: null, mission_link_unavailable_reason: "bot_username_not_configured",
    }),
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  mod.open("sep");
  await h.flush(); await h.flush();
  assert.ok(h.html().includes("bot_username_not_configured"));
});

// ===========================================================================
// §13, §16, §18 — dedicated edit mode
// ===========================================================================

test("Edit opens a dedicated panel bound to one immutable campaign", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();

  const s = mod.session();
  assert.equal(s.mode(), "edit");
  assert.equal(s.editModeTarget(), "sep");
  assert.equal(s.createModeTarget(), null);
  const html = h.html();
  assert.ok(html.includes("Edit Mission"));
  assert.ok(html.includes("this is the only campaign this panel can write to"));
  assert.ok(!html.includes("Create Mission"), "no create action exists in edit mode");
  assert.ok(!html.includes("Save Draft"));
  assert.ok(!html.includes("Publish Mission"));
});

test("a frozen mission_config is disabled proactively and not resent on save", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep", {
      mission_config_locked: true, entries: 124,
      locked_fields: CORE.FROZEN_MISSION_FIELDS,
    }),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();

  const html = h.html();
  assert.ok(html.includes(CORE.FREEZE_NOTE), "the exact §16 explanation must be shown");
  assert.ok(html.includes("124 entries"));
  // Every mission_config control is rendered disabled.
  ["mp-e-mtype", "mp-e-prompt", "mp-e-options", "mp-e-correct"].forEach((id) => {
    const at = html.indexOf('id="' + id + '"');
    assert.ok(at !== -1, "missing control " + id);
    assert.ok(html.slice(at, at + 160).includes("disabled"), id + " must be disabled while frozen");
  });

  await mod.dispatch("save-edit", "sep");
  await h.flush();
  const put = h.calls.find((c) => c.method === "PUTJ");
  assert.ok(put, "operator fields are still saveable");
  assert.equal("mission_config" in put.body, false,
    "a frozen mission_config must not be resent, so an unchanged PUT never trips the freeze");
  assert.ok(put.body.mission_pool, "pool/winners/eligibility remain editable");
});

test("schedule editability is taken from the backend, never inferred from the freeze", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    // Frozen config, but the backend still reports the schedule editable.
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep", {
      mission_config_locked: true, entries: 5, schedule_editable: true,
    }),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  assert.ok(h.html().includes('id="mp-e-starts"'), "an editable schedule stays editable when config is frozen");

  await mod.dispatch("save-edit", "sep");
  await h.flush();
  const put = h.calls.find((c) => c.method === "PUTJ");
  // Sent, because campaign_centre leaves `schedule` untouched on a partial
  // update that omits it — and untouched fields resend the exact instant.
  assert.equal(put.body.schedule.starts_at, "2026-09-05T12:00:37+00:00");
  assert.equal(put.body.schedule.ends_at, "2026-09-05T14:00:00+00:00");
});

test("a read-only schedule is shown as values and never sent", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep", { status: "ended" }) },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep", { schedule_editable: false, state: "closed" }),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  assert.ok(h.html().includes("read-only for this campaign state"));
  assert.ok(!h.html().includes('id="mp-e-starts"'));

  await mod.dispatch("save-edit", "sep");
  await h.flush();
  const put = h.calls.find((c) => c.method === "PUTJ");
  assert.equal("schedule" in put.body, false, "a non-editable schedule is never rewritten");
});

test("a stored pool no longer offered for selection is preserved, not swapped", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": {
      status: "ok",
      campaign: campaignDoc("sep", {
        mission_pool: { pool_id: "MOVED", pool_type: "vip", winner_count: 4,
          allocation_method: "random_qualified", eligibility_policy: {} },
      }),
    },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep", {
      reward: Object.assign({}, editState("sep").reward,
        { pool_id: "MOVED", pool_type: "vip", pool_selectable: false }),
    }),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  assert.ok(h.html().includes("current pool, unavailable for new selection"));

  await mod.dispatch("save-edit", "sep");
  await h.flush();
  const put = h.calls.find((c) => c.method === "PUTJ");
  assert.equal(put.body.mission_pool.pool_id, "MOVED", "the campaign's own pool must survive the save");
  assert.equal(put.body.mission_pool.pool_type, "vip");
});

test("repointing the pool stores the NEW pool's registered type", async () => {
  // The processor passes mission_pool.pool_type to allocate_voucher as
  // expected_pool_type, which filters the inventory row on it. Carrying the
  // old pool's type across a repoint would match no rows in the new pool and
  // mark every winner out_of_stock while stock looked available.
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  // Stored pool is MISSION-5 (voucher_drop); switch to WEEKEND (vip).
  assert.equal(h.node("mp-e-pool").value, "MISSION-5");
  h.set("mp-e-pool", "WEEKEND");
  h.fireChange("mp-e-pool");
  await mod.dispatch("save-edit", "sep");
  await h.flush();

  const put = h.calls.find((c) => c.method === "PUTJ");
  assert.equal(put.body.mission_pool.pool_id, "WEEKEND");
  assert.equal(put.body.mission_pool.pool_type, "vip",
    "the new pool's registered type must be stored, not the old one's");
});

test("a save is refused when the selected pool's type cannot be confirmed", async () => {
  const mod = freshModule();
  const h = makeHarness({
    // The pool list came back empty, so nothing can vouch for a new selection.
    "GET /api/admin/mission-pool/pools": { status: "ok", pools: [], pool_types: [] },
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  h.set("mp-e-pool", "UNKNOWN-POOL");
  await mod.dispatch("save-edit", "sep");
  await h.flush();
  assert.equal(h.calls.some((c) => c.method === "PUTJ"), false,
    "a guessed pool_type must never be written");
  assert.ok(h.toasts.some((t) => /Could not confirm the reward type/.test(t.msg)));
});

test("changing the mission type re-renders the fields that type actually has", async () => {
  // Without the re-render the operator switches to feedback, sees no length
  // inputs, and the save writes the fallback bounds 1/500; switching to
  // keyword leaves no keyword input and the backend rejects the save.
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  assert.ok(h.node("mp-e-options"), "multiple_choice shows an options field");
  assert.equal(h.node("mp-e-min"), undefined, "and no length bounds");

  h.set("mp-e-mtype", "feedback");
  h.fireChange("mp-e-mtype");     // the real change event, through the module's own listener
  assert.ok(h.node("mp-e-min") && h.node("mp-e-max"), "feedback shows its length bounds");
  assert.equal(h.node("mp-e-options"), undefined, "and drops the option list");

  h.set("mp-e-min", "25");
  h.set("mp-e-max", "300");
  await mod.dispatch("save-edit", "sep");
  await h.flush();
  const put = h.calls.find((c) => c.method === "PUTJ");
  assert.equal(put.body.mission_config.mission_type, "feedback");
  assert.equal(put.body.mission_config.min_chars, 25, "the operator's own bounds are saved");
  assert.equal(put.body.mission_config.max_chars, 300);
});

test("switching to keyword offers a keyword input instead of saving an empty one", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": {
      status: "ok",
      campaign: campaignDoc("sep", {
        mission_config: { mission_type: "feedback", prompt: "Tell us more", min_chars: 10, max_chars: 200 },
      }),
    },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  assert.equal(h.node("mp-e-correct"), undefined, "feedback has no keyword field");

  h.set("mp-e-mtype", "keyword");
  h.fireChange("mp-e-mtype");
  assert.ok(h.node("mp-e-correct"), "keyword shows its keyword field");
  h.set("mp-e-correct", "JACKPOT");
  await mod.dispatch("save-edit", "sep");
  await h.flush();
  const put = h.calls.find((c) => c.method === "PUTJ");
  assert.equal(put.body.mission_config.mission_type, "keyword");
  assert.equal(put.body.mission_config.correct_answer, "JACKPOT");
});

test("a field the current mission type does not render keeps its stored value", async () => {
  // This is the failure mode in reverse: capturing an absent control would
  // overwrite the stored value with undefined, so switching type and back —
  // or saving after a switch — would silently discard the option list.
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  const originalOptions = h.node("mp-e-options").value;
  assert.equal(originalOptions, "a | Free Spins\nb | Cashback");

  h.set("mp-e-mtype", "feedback");
  h.fireChange("mp-e-mtype");
  assert.equal(h.node("mp-e-options"), undefined, "the options field is gone");

  h.set("mp-e-mtype", "multiple_choice");
  h.fireChange("mp-e-mtype");
  assert.equal(h.node("mp-e-options").value, originalOptions,
    "the option list survived a round trip through a type that does not show it");

  await mod.dispatch("save-edit", "sep");
  await h.flush();
  const put = h.calls.find((c) => c.method === "PUTJ");
  assert.deepEqual(put.body.mission_config.options,
    [{ id: "a", label: "Free Spins" }, { id: "b", label: "Cashback" }]);
});

test("a re-render keeps everything already typed into the edit panel", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
    "PUTJ /api/admin/gc-campaigns/sep": { status: "ok" },
    "GET /api/admin/mission-pool/sep/summary": SUMMARY_OK,
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  h.set("mp-e-name", "Renamed Mission");
  h.set("mp-e-winners", "42");
  h.fireChange("mp-e-winners");
  assert.equal(h.node("mp-e-name").value, "Renamed Mission");
  assert.equal(h.node("mp-e-winners").value, "42");

  await mod.dispatch("save-edit", "sep");
  await h.flush();
  const put = h.calls.find((c) => c.method === "PUTJ");
  assert.equal(put.body.name, "Renamed Mission");
  assert.equal(put.body.mission_pool.winner_count, 42);
});

test("the pool cannot be repointed once reward allocation has started", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "ok", campaign: campaignDoc("sep") },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep", {
      reward: Object.assign({}, editState("sep").reward, { allocation_started: true, pool_editable: false }),
    }),
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  const at = h.html().indexOf('id="mp-e-pool"');
  assert.ok(h.html().slice(at, at + 160).includes("disabled"));
  assert.ok(h.html().includes("Reward allocation has started"));
});

test("a failed hydration leaves the edit panel unable to save", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/sep": { status: "error", code: "not_found" },
    "GET /api/admin/mission-pool/sep/edit-state": editState("sep"),
  });
  mod.init(h.host);
  await mod.dispatch("edit", "sep");
  await h.flush(); await h.flush();
  assert.ok(h.html().includes("Could not load"));
  assert.deepEqual(mod.session().authorizeSave(), { ok: false, code: "not_hydrated" });

  await mod.dispatch("save-edit", "sep");
  await h.flush();
  assert.equal(h.calls.some((c) => c.method === "PUTJ"), false, "an unhydrated panel must not PUT");
  assert.ok(h.toasts.some((t) => t.msg.includes(CORE.SAVE_REFUSAL_COPY.not_hydrated)));
});

test("a save fired at the wrong campaign is refused before any request", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/camp-a": { status: "ok", campaign: campaignDoc("camp-a") },
    "GET /api/admin/mission-pool/camp-a/edit-state": editState("camp-a"),
  });
  mod.init(h.host);
  await mod.dispatch("edit", "camp-a");
  await h.flush(); await h.flush();
  h.calls.length = 0;

  await mod.dispatch("save-edit", "camp-b");
  await h.flush();
  assert.equal(h.calls.length, 0, "no write is issued for a mismatched target");
  assert.ok(h.toasts.some((t) => t.msg.includes(CORE.SAVE_REFUSAL_COPY.wrong_campaign_target)));
});

test("leaving edit for the create wizard leaves no trace of the edited campaign", async () => {
  const mod = freshModule();
  const h = makeHarness({
    "GET /api/admin/mission-pool/pools": POOLS_OK,
    "GET /api/admin/gc-campaigns/camp-a": { status: "ok", campaign: campaignDoc("camp-a") },
    "GET /api/admin/mission-pool/camp-a/edit-state": editState("camp-a"),
    "GET /api/admin/mission-pool/campaigns": { status: "ok", campaigns: [] },
  });
  mod.init(h.host);
  await mod.dispatch("edit", "camp-a");
  await h.flush(); await h.flush();
  await mod.dispatch("back-to-list");
  await h.flush(); await h.flush();
  await mod.dispatch("create");
  await h.flush();

  const s = mod.session();
  assert.equal(s.mode(), "create");
  assert.equal(s.editModeTarget(), null);
  assert.equal(s.editState(), null);
  const d = s.draft();
  assert.equal(d.name, "");
  assert.equal(d.pool_id, "");
  assert.equal(d.options_text, "");
  assert.ok(!h.html().includes("camp-a"), "the previously edited campaign is not on screen");
});

// ===========================================================================
// §21 — internal pilot preset
// ===========================================================================

test("the internal test preset only pre-fills the same wizard", async () => {
  const mod = freshModule();
  const h = makeHarness({ "GET /api/admin/mission-pool/pools": POOLS_OK });
  mod.init(h.host);
  await mod.dispatch("create-test");
  await h.flush();
  const d = mod.session().draft();
  assert.match(d.name, /^TEST — /, "the mission name is prefixed TEST");
  assert.equal(d.winner_count, 2);
  assert.ok(d.starts_at && d.ends_at, "a short duration is pre-filled");
  // Not a backend campaign type: it still creates an ordinary mission_pool
  // campaign through the same wizard.
  assert.equal(MISSION_JS.indexOf("internal_test"), -1);
  assert.equal(mod.session().mode(), "create");
});

// ===========================================================================
// §22 — Standard Drop and Campaign Centre isolation
// ===========================================================================

test("the Campaign Centre create form no longer builds missions", () => {
  // The shared form is what produced the wrong-campaign writes; the mission
  // fields, the mission ops panel and the mission_pool campaign type are all
  // gone from it.
  assert.equal(HTML.indexOf('id="gc-mission-fields"'), -1);
  assert.equal(HTML.indexOf('id="gc-mission-ops"'), -1);
  assert.equal(HTML.indexOf('id="gc-save-mission-btn"'), -1);
  assert.equal(HTML.indexOf('<option value="mission_pool">'), -1);
  ["gc-m-type", "gc-m-prompt", "gc-m-options", "gc-m-pool", "gc-m-winners"].forEach((id) => {
    assert.equal(HTML.indexOf('id="' + id + '"'), -1, "leftover mission control in the shared form: " + id);
  });
});

test("existing campaign types are still offered unchanged", () => {
  const start = HTML.indexOf('id="gc-c-type"');
  const block = HTML.slice(start, HTML.indexOf("</select>", start));
  ["tournament", "external_subscription_verification", "external_website"].forEach((t) => {
    assert.ok(block.includes('value="' + t + '"'), "existing campaign type removed: " + t);
  });
});

test("the Mission surface has its own view and its own module", () => {
  assert.ok(HTML.includes('id="view-missionPool"'));
  assert.ok(HTML.includes('id="mp-root"'));
  assert.ok(HTML.includes('src="/static/mission-admin.js"'));
  assert.ok(DASH_JS.includes('view: "missionPool"'), "the Mission surface is a nav destination");
  assert.ok(DASH_JS.includes('state.view === "missionPool"'), "and has a loader");
});

test("the Mission surface never touches Standard Drop or voucher-claim endpoints", () => {
  ["/v2/miniapp/admin/drops", "/api/admin/vouchers", "api_claim", "user_visible_drops",
    "/api/admin/pools/upload", "personalised", "/api/admin/affiliate"].forEach((bad) => {
    assert.equal(MISSION_CODE.indexOf(bad), -1, "mission admin reached into " + bad);
  });
});

test("neither the protected-pool list nor the allowed scopes are hardcoded in the UI", () => {
  // §6: the backend endpoint is what excludes Welcome / T1-T5 / affiliate
  // denomination pools, and what decides an existing pool's type.
  ['"WELCOME"', '"T1"', '"T2"', '"T3"', '"T4"', '"T5"', "RESERVED_LEGACY_POOL_IDS",
    "allocation_scope"].forEach((bad) => {
    assert.equal(MISSION_CODE.indexOf(bad), -1, "backend pool policy hardcoded in the UI: " + bad);
  });
  // The pool_type stored on a campaign is always the registry's answer.
  assert.ok(MISSION_CODE.includes("pool_type: ctx.verdict.pool_type"),
    "the created campaign must store the backend's real pool_type");
  assert.ok(MISSION_CODE.includes("pool_type: poolType"),
    "an edited campaign must store the type resolved for the pool actually selected");
  assert.equal(/mission_pool[\s\S]{0,300}pool_type:\s*["']/.test(MISSION_CODE), false,
    "a submitted mission_pool block must never carry a literal pool_type");
});

test("Duplicate still uses the existing Campaign Centre endpoint", () => {
  assert.ok(DASH_JS.includes('data-gc-action="duplicate"'));
  assert.ok(DASH_JS.includes('"/api/admin/gc-campaigns/" + id + "/duplicate"'));
});
