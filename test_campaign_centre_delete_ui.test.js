/**
 * Tests for the Campaign Centre "Delete" confirmation modal in
 * static/admin-dashboard.js (openGcDeleteModal / bindGcCampaigns wiring).
 *
 * The dashboard is one large inline-script-free file with no build step and
 * no jsdom in this repo, so (mirroring test_lucky_games_admin_ui.test.js and
 * test_admin_creator_access_ui.test.js) the relevant source is extracted as
 * text and executed in a sandboxed vm context against a small hand-rolled
 * DOM stub. openGcDeleteModal builds its modal via createElement/appendChild
 * (not innerHTML+querySelector like confirmTyped/confirmSimple), so unlike
 * those two, its DOM is directly testable here without a real HTML parser.
 *
 * Run with: node --test test_campaign_centre_delete_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const JS = fs.readFileSync(JS_PATH, "utf8");

// Two narrow, independent slices are concatenated rather than taking the
// whole span between them: the file in between is thousands of lines of
// unrelated dashboard sections, several of which wire themselves up via
// top-level `(function bindX() { ... })()` IIFEs that run immediately when
// executed and reach for globals (`$all`, dataset helpers, etc.) that are
// out of scope here. Pulling only the fetch/DOM helpers plus
// openGcDeleteModal itself avoids all of that dead weight.
const HELPERS_START = "  function esc(v) {";
const HELPERS_END = "\n  function setMeta(text) {";
const MODAL_START = "  // ---------- Permanent campaign deletion (Campaign Centre) ----------";
const MODAL_END = "\n  function loadGcCampaigns(force) {";

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

function loadFeatureSource() {
  return slice(JS, HELPERS_START, HELPERS_END) + "\n" + slice(JS, MODAL_START, MODAL_END);
}

// ---------------------------------------------------------------------
// Structural check: the Delete button must be in the Actions column,
// placed after Duplicate, on the raw row-template source (independent of
// runtime execution).
// ---------------------------------------------------------------------

// P0.4 moved every row action off the dense table into the "•••" overflow
// menu (gcOverflowMenuHtml) — Delete's position is now checked there
// instead of in loadGcCampaigns's own source (which no longer builds
// per-row action markup inline; see test_admin_dashboard_p0_4_campaign_list
// for the full P0.4 suite).
test("overflow menu places data-gc-action=\"delete\" immediately after duplicate", () => {
  const menuStart = JS.indexOf("function gcOverflowMenuHtml(campaign, actions) {");
  const menuEnd = JS.indexOf("function gcCampaignRowHtml(campaign, rows) {", menuStart);
  const menuSrc = JS.slice(menuStart, menuEnd);
  const duplicateIdx = menuSrc.indexOf('data-gc-action="duplicate"');
  const deleteIdx = menuSrc.indexOf('data-gc-action="delete"');
  assert.ok(duplicateIdx !== -1, "duplicate button not found");
  assert.ok(deleteIdx !== -1, "delete button not found");
  assert.ok(deleteIdx > duplicateIdx, "delete button must come after duplicate");
  assert.match(menuSrc.slice(deleteIdx - 40, deleteIdx), /danger/, "delete button should use the destructive style");
});

// ---------------------------------------------------------------------
// Minimal hand-rolled DOM
// ---------------------------------------------------------------------

class FakeElement {
  constructor(tag) {
    this.tagName = (tag || "div").toUpperCase();
    this.id = "";
    this.className = "";
    this.style = {};
    this.dataset = {};
    this.children = [];
    this.parent = null;
    this._text = "";
    this._html = "";
    this.value = "";
    this.disabled = false;
    this._listeners = {};
  }
  get textContent() { return this._text; }
  set textContent(v) { this._text = v == null ? "" : String(v); }
  get innerHTML() { return this._html; }
  set innerHTML(v) { this._html = v == null ? "" : String(v); }
  appendChild(node) { node.parent = this; this.children.push(node); return node; }
  remove() {
    if (this.parent) {
      this.parent.children = this.parent.children.filter((c) => c !== this);
      this.parent = null;
    }
  }
  addEventListener(evt, fn) { (this._listeners[evt] = this._listeners[evt] || []).push(fn); }
  _trigger(evt, evtObj) { (this._listeners[evt] || []).forEach((fn) => fn(evtObj || { target: this })); }
  focus() {}
  get classList() { return { add() {}, remove() {}, contains() { return false; } }; }
  isAttached() {
    let node = this;
    while (node.parent) node = node.parent;
    return node.tagName === "BODY";
  }
}

function makeDocument() {
  const body = new FakeElement("body");
  function walk(node, id) {
    for (const c of node.children) {
      if (c.id === id) return c;
      const found = walk(c, id);
      if (found) return found;
    }
    return null;
  }
  return {
    body,
    createElement: (tag) => new FakeElement(tag),
    // getElementById/querySelectorAll return nothing found: several
    // unrelated dashboard sections wire themselves up via top-level
    // `(function bindX() { ... })()` IIFEs that run as soon as this slice
    // of admin-dashboard.js is executed (same as on the real page); they
    // all guard on the element existing, so "not found" is a safe no-op
    // for every one of them, and none affects the Campaign Centre modal
    // under test here.
    getElementById: () => null,
    querySelectorAll: () => [],
    querySelector: (sel) => (sel[0] === "#" ? walk(body, sel.slice(1)) : null),
    addEventListener: () => {},
  };
}

function makeFetchQueue() {
  const calls = [];
  const queue = [];
  const fetchImpl = (fetchPath, opts) => {
    calls.push({ path: fetchPath, opts });
    const next = queue.shift();
    if (!next) throw new Error("unexpected fetch call: " + fetchPath);
    return Promise.resolve({
      status: next.status,
      ok: next.status >= 200 && next.status < 300,
      json: () => Promise.resolve(next.body),
    });
  };
  fetchImpl.calls = calls;
  fetchImpl.push = (status, body) => queue.push({ status, body });
  return fetchImpl;
}

function makeContext() {
  const document = makeDocument();
  const fetchImpl = makeFetchQueue();
  const toasts = [];
  const refreshes = [];

  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    document,
    window: { location: { href: "" } },
    fetch: fetchImpl,
    setTimeout,
    clearTimeout,
  };

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFeatureSource(), context, { filename: "admin-dashboard-extract-gc-delete.js" });
  // loadGcCampaigns (defined in the extracted slice) pulls in helpers
  // (statePanel/emptyState/gcPill/mission_pool wiring) that live thousands
  // of lines away in the real file — irrelevant to this modal's own logic,
  // so it's stubbed to a spy that just records the refresh request, the
  // same way test_admin_creator_access_ui.test.js stubs confirmSimple.
  vm.runInContext("this.loadGcCampaigns = function (force) { __refreshes.push(force); };", context);
  context.__refreshes = refreshes;
  vm.runInContext("this.toast = function (msg, kind) { __toasts.push({ msg: msg, kind: kind }); };", context);
  context.__toasts = toasts;

  return { context, document, fetchImpl, toasts, refreshes };
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

// ---------------------------------------------------------------------
// Behavioral tests
// ---------------------------------------------------------------------

test("modal starts with the Delete permanently button disabled", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  assert.ok(overlay.isAttached(), "modal overlay should be appended to document.body");
  const confirmBtn = findByText(overlay, "Delete permanently");
  assert.equal(confirmBtn.disabled, true);
});

test("typing anything other than the exact campaign id keeps the button disabled", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "summer-lucky-draw";
  input._trigger("input");
  assert.equal(confirmBtn.disabled, true, "partial match must not enable the button");

  input.value = "Summer Lucky Draw"; // display name, not the slug
  input._trigger("input");
  assert.equal(confirmBtn.disabled, true, "the campaign title must not satisfy the id check");
});

test("typing the exact campaign id enables Delete permanently", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "summer-lucky-draw-2026";
  input._trigger("input");
  assert.equal(confirmBtn.disabled, false);
});

test("clicking Delete permanently while disabled never calls the API", () => {
  const { context, document, fetchImpl } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const confirmBtn = findByText(overlay, "Delete permanently");
  confirmBtn._trigger("click");
  assert.equal(fetchImpl.calls.length, 0);
});

test("successful delete: closes the modal, toasts success, and refreshes the table", async () => {
  const { context, document, fetchImpl, toasts, refreshes } = makeContext();
  fetchImpl.push(200, { status: "ok", campaign_id: "summer-lucky-draw-2026" });

  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "summer-lucky-draw-2026";
  input._trigger("input");
  confirmBtn._trigger("click");
  await flush();

  assert.equal(fetchImpl.calls.length, 1);
  assert.equal(fetchImpl.calls[0].path, "/api/admin/campaign-centre/campaigns/summer-lucky-draw-2026");
  assert.equal(fetchImpl.calls[0].opts.method, "DELETE");
  assert.equal(overlay.isAttached(), false, "the modal must close on success");
  assert.equal(toasts.length, 1);
  assert.match(toasts[0].msg, /deleted/i);
  assert.equal(toasts[0].kind, "success");
  assert.deepEqual(refreshes, [true], "the campaigns table must be refreshed after a successful delete");
});

test("failed delete (409 live/paused): modal stays open and shows the backend error, row is not removed", async () => {
  const { context, document, fetchImpl, toasts, refreshes } = makeContext();
  fetchImpl.push(409, { status: "error", code: "invalid_status_for_deletion", campaign_status: "live" });

  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "summer-lucky-draw-2026";
  input._trigger("input");
  confirmBtn._trigger("click");
  await flush();

  assert.equal(overlay.isAttached(), true, "the modal must stay open on failure");
  assert.equal(toasts.length, 0, "no success toast on failure");
  assert.deepEqual(refreshes, [], "the table must not be refreshed when deletion failed");
  assert.equal(confirmBtn.disabled, false, "input still matches, so the button should be re-enabled for a retry");

  const errorNode = overlay.children[0].children.find((c) => c.style && c.style.display === "block");
  assert.ok(errorNode, "an error message node should now be visible");
  assert.match(errorNode.textContent, /live/);
});

test("generic failure keeps the modal open with the raw error code", async () => {
  const { context, document, fetchImpl, toasts } = makeContext();
  fetchImpl.push(500, { status: "error", code: "internal_error" });

  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "summer-lucky-draw-2026";
  input._trigger("input");
  confirmBtn._trigger("click");
  await flush();

  assert.equal(overlay.isAttached(), true);
  assert.equal(toasts.length, 0);
  const errorNode = overlay.children[0].children.find((c) => c.style && c.style.display === "block");
  assert.match(errorNode.textContent, /internal_error/);
});

test("Cancel closes the modal without calling the API", () => {
  const { context, document, fetchImpl } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const cancelBtn = findByText(overlay, "Cancel");
  cancelBtn._trigger("click");
  assert.equal(overlay.isAttached(), false);
  assert.equal(fetchImpl.calls.length, 0);
});

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

function walkAll(node, out) {
  out.push(node);
  node.children.forEach((c) => walkAll(c, out));
  return out;
}

function findByText(overlay, text) {
  const all = walkAll(overlay, []);
  const node = all.find((n) => n.tagName === "BUTTON" && n.textContent === text);
  assert.ok(node, `button with text "${text}" not found`);
  return node;
}

function findInput(overlay) {
  const all = walkAll(overlay, []);
  const node = all.find((n) => n.tagName === "INPUT");
  assert.ok(node, "confirmation input not found");
  return node;
}
