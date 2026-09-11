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
// P0.15 — the modal's error branch now maps codes through
// gcActionErrorMessage/GC_ACTION_ERROR_MESSAGES (never a raw snake_case
// code) instead of showing res.d.code verbatim.
const ERROR_MAP_START = "  var GC_ACTION_ERROR_MESSAGES = {";
const ERROR_MAP_END = "\n\n  // Refreshes whichever canonical view is currently showing this campaign";

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

function loadFeatureSource() {
  return slice(JS, ERROR_MAP_START, ERROR_MAP_END) + "\n" +
    slice(JS, HELPERS_START, HELPERS_END) + "\n" + slice(JS, MODAL_START, MODAL_END);
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
    this._attrs = {};
  }
  get textContent() { return this._text; }
  set textContent(v) { this._text = v == null ? "" : String(v); }
  get innerHTML() { return this._html; }
  set innerHTML(v) { this._html = v == null ? "" : String(v); }
  // P0.16 §G — openGcDeleteModal now stamps role/aria-modal/aria-labelledby
  // onto the modal box via setAttribute; this stub needs to support it the
  // same way a real DOM element would.
  setAttribute(name, value) { this._attrs[name] = String(value); }
  getAttribute(name) { return Object.prototype.hasOwnProperty.call(this._attrs, name) ? this._attrs[name] : null; }
  removeAttribute(name) { delete this._attrs[name]; }
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
  const docListeners = {};
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
    // P0.16 §G — openGcDeleteModal's Escape handling is now bound at
    // document level (not just the input) so it still works while the
    // input/confirm button are disabled during the DELETE request; close()
    // always unregisters it afterward. A real (not no-op) listener registry
    // is needed here so the new tests below can actually dispatch a
    // document-level Escape keydown.
    addEventListener: (evt, fn) => { (docListeners[evt] = docListeners[evt] || []).push(fn); },
    removeEventListener: (evt, fn) => {
      if (!docListeners[evt]) return;
      docListeners[evt] = docListeners[evt].filter((f) => f !== fn);
    },
    _trigger: (evt, evtObj) => { (docListeners[evt] || []).slice().forEach((fn) => fn(evtObj)); },
    _listenerCount: (evt) => (docListeners[evt] || []).length,
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

// P0.15 — the confirmation phrase is now the campaign NAME (beginner-
// visible), not the hidden technical campaign_id: typing the id must no
// longer satisfy the check, and typing the exact name must.
test("typing anything other than the exact campaign name keeps the button disabled", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "Summer Lucky";
  input._trigger("input");
  assert.equal(confirmBtn.disabled, true, "partial match must not enable the button");

  input.value = "summer-lucky-draw-2026"; // the technical campaign_id, not the name
  input._trigger("input");
  assert.equal(confirmBtn.disabled, true, "the campaign_id must not satisfy the name check");
});

test("typing the exact campaign name enables Delete permanently", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "Summer Lucky Draw";
  input._trigger("input");
  assert.equal(confirmBtn.disabled, false);
});

test("a campaign with no name falls back to campaign_id as the confirmation phrase", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "");
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

  input.value = "Summer Lucky Draw";
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

  input.value = "Summer Lucky Draw";
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

// P0.15 — was "keeps the modal open with the raw error code" (a bug this
// PR fixes): any code other than invalid_status_for_deletion now goes
// through gcActionErrorMessage/GC_ACTION_ERROR_MESSAGES, so an unmapped
// code like internal_error falls back to the generic friendly message —
// never the raw snake_case string.
test("generic/unmapped failure keeps the modal open with a friendly message, never the raw error code", async () => {
  const { context, document, fetchImpl, toasts } = makeContext();
  fetchImpl.push(500, { status: "error", code: "internal_error" });

  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "Summer Lucky Draw";
  input._trigger("input");
  confirmBtn._trigger("click");
  await flush();

  assert.equal(overlay.isAttached(), true);
  assert.equal(toasts.length, 0);
  const errorNode = overlay.children[0].children.find((c) => c.style && c.style.display === "block");
  assert.doesNotMatch(errorNode.textContent, /internal_error/, "must never leak the raw snake_case code");
  assert.match(errorNode.textContent, /Couldn't delete this campaign\. Try again\./);
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

// =======================================================================
// P0.16 §E/§G — modal dialog semantics + Escape-while-disabled
// =======================================================================

test("the modal box carries role=dialog, aria-modal=true, and an aria-labelledby pointing at a real element", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const box = overlay.children[0];
  assert.equal(box.getAttribute("role"), "dialog");
  assert.equal(box.getAttribute("aria-modal"), "true");
  const labelledBy = box.getAttribute("aria-labelledby");
  assert.ok(labelledBy, "aria-labelledby must be set");
  const titleEl = box.children.find((c) => c.id === labelledBy);
  assert.ok(titleEl, "aria-labelledby must point at an element that actually exists in the modal");
  assert.equal(titleEl.textContent, "Delete campaign permanently?");
});

// Codex review (P1): closing the modal via Escape does NOT cancel the
// underlying DELETE request — an admin who hits Escape mid-request would
// otherwise see it as "I backed out" while the campaign still gets deleted
// (or a failure gets written into a modal that's no longer on screen). So
// Escape must be a safe no-op while the request is in flight, bound at
// document level only so it can reliably resume working the moment the
// request settles (the input/button being disabled is exactly the in-flight
// window, so an input-only listener can't do this at all).
test("Escape is a no-op while the DELETE request is in flight (never silently 'cancels' an irreversible request)", async () => {
  const { context, document } = makeContext();
  // Never resolves within this test — simulates a still-in-flight DELETE.
  let resolveFetch;
  const pending = new Promise((resolve) => { resolveFetch = resolve; });
  context.fetch = () => pending;

  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "Summer Lucky Draw";
  input._trigger("input");
  confirmBtn._trigger("click");
  await Promise.resolve();

  assert.equal(input.disabled, true, "input must be disabled while the request is in flight");
  assert.equal(confirmBtn.disabled, true, "confirm button must be disabled while the request is in flight");

  document._trigger("keydown", { key: "Escape" });
  assert.equal(overlay.isAttached(), true, "Escape must not close the modal while its own request is still in flight");

  resolveFetch({ status: 200, ok: true, json: () => Promise.resolve({ status: "ok" }) });
  await flush();
  assert.equal(overlay.isAttached(), false, "the modal still closes on its own once the request actually succeeds");
});

test("Escape closes the modal normally before any request has started (idle state)", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  document._trigger("keydown", { key: "Escape" });
  assert.equal(overlay.isAttached(), false, "Escape must still close an idle (not in-flight) modal");
});

test("Escape works again immediately after a failed request re-enables the modal's controls", async () => {
  const { context, document, fetchImpl } = makeContext();
  fetchImpl.push(500, { status: "error", code: "internal_error" });

  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  const overlay = document.body.children[document.body.children.length - 1];
  const input = findInput(overlay);
  const confirmBtn = findByText(overlay, "Delete permanently");

  input.value = "Summer Lucky Draw";
  input._trigger("input");
  confirmBtn._trigger("click");
  await flush();

  assert.equal(overlay.isAttached(), true, "still open after the failed request");
  assert.equal(confirmBtn.disabled, false, "re-enabled after failure");

  document._trigger("keydown", { key: "Escape" });
  assert.equal(overlay.isAttached(), false, "Escape must work again once the request has settled");
});

test("closing the modal (Cancel) unregisters its document-level Escape listener — no leak across repeated opens", () => {
  const { context, document } = makeContext();
  context.openGcDeleteModal("summer-lucky-draw-2026", "Summer Lucky Draw");
  assert.equal(document._listenerCount("keydown"), 1);
  const overlay = document.body.children[document.body.children.length - 1];
  findByText(overlay, "Cancel")._trigger("click");
  assert.equal(document._listenerCount("keydown"), 0, "the keydown listener must be removed once the modal closes");
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
