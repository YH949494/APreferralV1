/**
 * Lucky Games — admin dashboard UI wiring (static/admin-dashboard.js).
 *
 * Focus: create/edit mode isolation. The form has exactly one shared set of
 * inputs for both "add a new game" and "edit an existing game" — these
 * tests prove that editing game A can never be submitted as a create, and
 * can never silently retarget game B, by executing the actual
 * lgEnterCreateMode/lgEnterEditMode/bindLuckyGames source (extracted from
 * static/admin-dashboard.js) inside a sandboxed vm context, mirroring
 * test_mission_admin_ui.test.js / test_event_banner_frontend.test.js.
 *
 * Run with: node --test test_lucky_games_admin_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");

const START_MARKER = "var LG_VOLATILITY_OPTIONS";
const END_MARKER = "\n  function loadGcProviders(force) {";

function loadFeatureSource() {
  const start = JS.indexOf(START_MARKER);
  const end = JS.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "Lucky Games admin block not found in admin-dashboard.js");
  assert.ok(end > start, "end marker not found after Lucky Games admin block");
  return JS.slice(start, end);
}

/** Fake <input>/<select>/<checkbox> element. */
class FakeInput {
  constructor() {
    this.value = "";
    this.checked = false;
    this.style = {};
    this.disabled = false;
    this.textContent = "";
  }
  addEventListener(type, fn) {
    this._listeners = this._listeners || {};
    this._listeners[type] = fn;
  }
  trigger(type) {
    if (this._listeners && this._listeners[type]) this._listeners[type]();
  }
}

const FORM_IDS = [
  "lg-name", "lg-label", "lg-provider", "lg-volatility", "lg-max-win",
  "lg-image-url", "lg-game-url", "lg-sort-order", "lg-is-published",
  "lg-submit-btn", "lg-cancel-edit-btn", "lg-form-mode", "lg-body",
];

function makeContext({ apiResponses = {} } = {}) {
  const elements = {};
  FORM_IDS.forEach((id) => { elements[id] = new FakeInput(); });
  elements["lg-volatility"].value = "Medium";

  const calls = { patch: [], post: [], delete: [], toasts: [] };
  let documentClickHandler = null;

  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    document: {
      getElementById: (id) => elements[id] || null,
      addEventListener: (type, fn) => { if (type === "click") documentClickHandler = fn; },
    },
    window: {},
    $: (sel) => elements[String(sel).replace("#", "")] || null,
    esc: (v) => String(v == null ? "" : v),
    gcPill: (status) => `<span>${status}</span>`,
    statePanel: () => {},
    emptyState: (msg) => `<div class="empty">${msg}</div>`,
    toast: (msg) => calls.toasts.push(msg),
    confirm: () => true,
    api: (p) => Promise.resolve(apiResponses.get ? apiResponses.get(p) : { games: [] }),
    apiPostJson: (p, body) => {
      calls.post.push({ path: p, body });
      return Promise.resolve(apiResponses.post ? apiResponses.post(p, body) : { ok: true, d: { status: "ok", game: { id: "new-id", ...body } } });
    },
    apiPatchJson: (p, body) => {
      calls.patch.push({ path: p, body });
      return Promise.resolve(apiResponses.patch ? apiResponses.patch(p, body) : { ok: true, d: { status: "ok" } });
    },
    apiDelete: (p) => {
      calls.delete.push({ path: p });
      return Promise.resolve({ ok: true, d: { status: "ok" } });
    },
  };

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFeatureSource(), context, { filename: "admin-dashboard-extract-lucky-games.js" });
  vm.runInContext("bindLuckyGames();", context);

  function fireAction(action, id, extraDataset) {
    const dataset = { lgAction: action, id, ...extraDataset };
    const target = { closest: (sel) => (sel === "[data-lg-action]" ? { dataset } : null) };
    documentClickHandler({ target });
  }

  return { context, elements, calls, fireAction };
}

const GAME_A = {
  id: "game-a", name: "Infinity Ocean", label: "Lucky Game", provider: "PG Soft",
  volatility: "High-Med", max_win: "25000x", image_url: "", game_url: "", sort_order: 10, is_published: true,
};
const GAME_B = {
  id: "game-b", name: "Sugar Crush", label: "Lucky Game", provider: "",
  volatility: "Medium", max_win: "20000x", image_url: "", game_url: "", sort_order: 20, is_published: false,
};

test("1. starts in create mode with an empty, blank form", () => {
  const { context, elements } = makeContext();
  assert.equal(context.lgEditingId, null);
  assert.equal(elements["lg-name"].value, "");
  assert.equal(elements["lg-submit-btn"].textContent, "Add game");
  assert.equal(elements["lg-cancel-edit-btn"].style.display, "none");
});

test("2. entering edit mode on game A populates the form and records only game A's id", () => {
  const { context, elements } = makeContext();
  context.lgEnterEditMode(GAME_A);
  assert.equal(context.lgEditingId, "game-a");
  assert.equal(elements["lg-name"].value, "Infinity Ocean");
  assert.equal(elements["lg-volatility"].value, "High-Med");
  assert.equal(elements["lg-submit-btn"].textContent, "Save changes");
  assert.equal(elements["lg-cancel-edit-btn"].style.display, "inline-block");
});

test("3. switching from editing game A to editing game B retargets cleanly (never both)", () => {
  const { context, elements } = makeContext();
  context.lgEnterEditMode(GAME_A);
  assert.equal(context.lgEditingId, "game-a");
  context.lgEnterEditMode(GAME_B);
  assert.equal(context.lgEditingId, "game-b", "must fully switch, not merge with the previous edit target");
  assert.equal(elements["lg-name"].value, "Sugar Crush");
  assert.notEqual(elements["lg-name"].value, GAME_A.name);
});

test("4. cancelling an edit returns to create mode and clears the editing id", () => {
  const { context, elements } = makeContext();
  context.lgEnterEditMode(GAME_A);
  elements["lg-cancel-edit-btn"].trigger("click");
  assert.equal(context.lgEditingId, null);
  assert.equal(elements["lg-name"].value, "");
  assert.equal(elements["lg-submit-btn"].textContent, "Add game");
});

test("5. submitting in create mode POSTs, never PATCHes", async () => {
  const { context, elements, calls } = makeContext();
  elements["lg-name"].value = "New Game";
  elements["lg-submit-btn"].trigger("click");
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  assert.equal(calls.post.length, 1);
  assert.equal(calls.patch.length, 0);
  assert.equal(calls.post[0].body.name, "New Game");
});

test("6. submitting while editing game A PATCHes game A's id specifically, never POSTs", async () => {
  const { context, elements, calls } = makeContext();
  context.lgEnterEditMode(GAME_A);
  elements["lg-name"].value = "Infinity Ocean Renamed";
  elements["lg-submit-btn"].trigger("click");
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  assert.equal(calls.patch.length, 1);
  assert.equal(calls.post.length, 0);
  assert.ok(calls.patch[0].path.endsWith(encodeURIComponent("game-a")), "must PATCH the id captured at edit-mode entry, not a re-derived one");
  assert.equal(calls.patch[0].body.name, "Infinity Ocean Renamed");
});

test("7. editing game A then game B and submitting only ever PATCHes game B — game A is untouched", async () => {
  const { context, elements, calls } = makeContext();
  context.lgEnterEditMode(GAME_A);
  context.lgEnterEditMode(GAME_B); // admin changed their mind before submitting
  elements["lg-submit-btn"].trigger("click");
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  assert.equal(calls.patch.length, 1);
  assert.ok(calls.patch[0].path.endsWith(encodeURIComponent("game-b")));
  assert.ok(!calls.patch[0].path.includes("game-a"));
});

test("8. a successful edit submission returns the form to create mode", async () => {
  const { context, elements } = makeContext();
  context.lgEnterEditMode(GAME_A);
  elements["lg-submit-btn"].trigger("click");
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
  assert.equal(context.lgEditingId, null);
  assert.equal(elements["lg-submit-btn"].textContent, "Add game");
});

test("9. submit is disabled for the duration of the in-flight request (double-submit guard)", () => {
  const { elements } = makeContext();
  elements["lg-name"].value = "New Game";
  assert.equal(elements["lg-submit-btn"].disabled, false);
  elements["lg-submit-btn"].trigger("click");
  assert.equal(elements["lg-submit-btn"].disabled, true, "must disable synchronously before the request resolves");
});

test("10. a second click while a submission is in flight is ignored outright", () => {
  const { elements, calls } = makeContext();
  elements["lg-name"].value = "New Game";
  elements["lg-submit-btn"].trigger("click");
  elements["lg-submit-btn"].trigger("click"); // fired before the first request's promise resolves
  assert.equal(calls.post.length, 1, "the guarded click handler must not have issued a second POST");
});

test("11. blank name is rejected client-side without ever calling the API", () => {
  const { elements, calls } = makeContext();
  elements["lg-name"].value = "   ";
  elements["lg-submit-btn"].trigger("click");
  assert.equal(calls.post.length, 0);
  assert.equal(calls.patch.length, 0);
});

test("12. clicking Edit on a row action dispatches to lgEnterEditMode for that specific game", () => {
  const { context, elements, fireAction } = makeContext();
  context.window.__lgGamesById = { "game-a": GAME_A, "game-b": GAME_B };
  fireAction("edit", "game-b");
  assert.equal(context.lgEditingId, "game-b");
  assert.equal(elements["lg-name"].value, "Sugar Crush");
});

test("13. deleting the game currently being edited drops back to create mode", async () => {
  const { context, elements, fireAction } = makeContext();
  context.window.__lgGamesById = { "game-a": GAME_A };
  context.lgEnterEditMode(GAME_A);
  fireAction("delete", "game-a");
  await new Promise((r) => setImmediate(r));
  assert.equal(context.lgEditingId, null);
  assert.equal(elements["lg-name"].value, "");
});
