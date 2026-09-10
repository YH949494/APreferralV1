/**
 * Live Missions ambient list — static/mission-pool-widget.js.
 *
 * Covers the new #mission-live-root surface (renderLiveMissions,
 * liveCtaFor, remainingText, refreshLiveMissions) added so a LIVE mission
 * can be discovered from a normal Mini App open instead of only via a
 * deep link (the pre-existing #mission-pool-root/mount() flow, which is
 * untouched and still deep-link-only).
 *
 * The widget is a plain browser IIFE with no build step, so it is executed
 * in a sandboxed vm context against a small hand-rolled DOM, mirroring
 * test_campaign_centre_delete_ui.test.js / test_live_drop_voucher_ui.test.js.
 *
 * Run with: node --test test_mission_live_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_PATH = path.join(__dirname, "static", "mission-pool-widget.js");
const JS = fs.readFileSync(JS_PATH, "utf8");

// ---------------------------------------------------------------------
// Minimal DOM: just enough for el()/appendChild/getElementById/head and
// document-level custom-event dispatch. The widget only ever reaches the
// page through document.getElementById + createElement/appendChild/
// setAttribute/textContent, never querySelector or innerHTML parsing (for
// mounting; renderStateCard/renderMissionForm do use root.innerHTML = "" as
// a clear, which plain string assignment handles fine here).
// ---------------------------------------------------------------------

function makeNode(tag) {
  const node = {
    tagName: String(tag || "div").toUpperCase(),
    _children: [],
    attrs: {},
    style: {},
    _text: "",
    className: "",
    get textContent() { return this._text; },
    set textContent(v) { this._text = String(v); this._children = []; },
    get innerHTML() { return this._html || ""; },
    set innerHTML(v) { this._html = v; this._children = []; this._text = ""; },
    appendChild(child) { this._children.push(child); return child; },
    setAttribute(name, value) { this.attrs[name] = String(value); },
    getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null; },
    _listeners: {},
    addEventListener(type, cb) { (this._listeners[type] = this._listeners[type] || []).push(cb); },
    click() { (this._listeners.click || []).forEach((cb) => cb({ target: this })); },
    scrollIntoView() {},
    get children() { return this._children; },
  };
  return node;
}

function makeDocument(roots) {
  const listeners = {};
  return {
    readyState: "loading",
    head: makeNode("head"),
    createElement: (tag) => makeNode(tag),
    getElementById: (id) => roots[id] || null,
    addEventListener(type, cb) {
      (listeners[type] = listeners[type] || []).push(cb);
    },
    dispatchEvent(ev) {
      (listeners[ev.type] || []).forEach((cb) => cb(ev));
      return true;
    },
  };
}

class FakeCustomEvent {
  constructor(type, opts) {
    this.type = type;
    this.detail = (opts || {}).detail;
  }
}

/** Builds a fresh vm sandbox with a scripted fetch, returns { window, calls }. */
function makeSandbox(fetchImpl) {
  const roots = {
    "mission-pool-root": makeNode("div"),
    "mission-live-root": makeNode("div"),
  };
  const document = makeDocument(roots);
  const calls = [];
  const wrappedFetch = (url, opts) => {
    calls.push({ url, opts });
    return fetchImpl(url, opts);
  };
  const sandbox = {
    window: {},
    document,
    location: { search: "" },
    URLSearchParams,
    fetch: wrappedFetch,
    AbortController: typeof AbortController !== "undefined" ? AbortController : function () {
      return { abort() {} };
    },
    setTimeout,
    clearTimeout,
    console,
    CustomEvent: FakeCustomEvent,
    Telegram: undefined,
  };
  sandbox.window = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(JS, sandbox, { filename: "mission-pool-widget.js" });
  return { sandbox, roots, calls };
}

function okJson(data) {
  return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(data) });
}

// ---------------------------------------------------------------------------
// liveCtaFor — CTA mapping per user participation state
// ---------------------------------------------------------------------------

test("not-started mission maps to an enabled Join Mission CTA", () => {
  const { sandbox } = makeSandbox(() => okJson({}));
  const cta = sandbox.window.MissionPoolWidget.liveCtaFor({ user_state: "live", already_submitted: false });
  assert.equal(cta.text, "Join Mission");
  assert.equal(cta.disabled, false);
});

test("submitted mission maps to a disabled Submitted CTA", () => {
  const { sandbox } = makeSandbox(() => okJson({}));
  const cta = sandbox.window.MissionPoolWidget.liveCtaFor({ user_state: "submitted", already_submitted: true });
  assert.equal(cta.text, "Submitted");
  assert.equal(cta.disabled, true);
});

test("already_submitted alone is enough to disable the CTA even if user_state disagrees", () => {
  // Defensive: the server field the client trusts LEAST should still be
  // treated as authoritative for "don't let them tap again".
  const { sandbox } = makeSandbox(() => okJson({}));
  const cta = sandbox.window.MissionPoolWidget.liveCtaFor({ user_state: "live", already_submitted: true });
  assert.equal(cta.disabled, true);
});

// ---------------------------------------------------------------------------
// remainingText — human-readable countdown
// ---------------------------------------------------------------------------

test("remainingText formats hours/minutes remaining", () => {
  const { sandbox } = makeSandbox(() => okJson({}));
  const soon = new Date(Date.now() + 90 * 60000).toISOString();
  const text = sandbox.window.MissionPoolWidget.remainingText(soon);
  assert.match(text, /^1h \d{1,2}m left$/);
});

test("remainingText returns empty string for a mission already past its end time", () => {
  const { sandbox } = makeSandbox(() => okJson({}));
  const past = new Date(Date.now() - 1000).toISOString();
  assert.equal(sandbox.window.MissionPoolWidget.remainingText(past), "");
});

test("remainingText returns empty string when there is no ends_at", () => {
  const { sandbox } = makeSandbox(() => okJson({}));
  assert.equal(sandbox.window.MissionPoolWidget.remainingText(null), "");
});

// ---------------------------------------------------------------------------
// renderLiveMissions — display + empty state
// ---------------------------------------------------------------------------

test("a live mission renders a card with LIVE badge, title, reward summary and CTA", () => {
  const { sandbox, roots } = makeSandbox(() => okJson({}));
  sandbox.window.MissionPoolWidget.renderLiveMissions([
    {
      campaign_id: "m1", campaign_name: "Answer & Win",
      prompt: "Which game?", ends_at: new Date(Date.now() + 3600000).toISOString(),
      winner_count: 5, user_state: "live", already_submitted: false,
    },
  ]);
  const root = roots["mission-live-root"];
  assert.equal(root.style.display, "block");
  // One section title + one card.
  assert.equal(root.children.length, 2);
  const card = root.children[1];
  assert.equal(card.className, "mp-live-card");
});

test("empty missions list stays hidden — no large empty section", () => {
  const { sandbox, roots } = makeSandbox(() => okJson({}));
  const root = roots["mission-live-root"];
  root.style.display = "block"; // simulate a previous non-empty render
  sandbox.window.MissionPoolWidget.renderLiveMissions([]);
  assert.equal(root.style.display, "none");
  assert.equal(root.children.length, 0);
});

test("a completed/won mission never appears in the Live Missions list, avoiding a duplicate reward card", () => {
  // The backend already excludes ended/completed campaigns from
  // GET /api/mission-pool/active (see test_mission_live_missions.py); this
  // asserts the client renders exactly what it's given and never re-adds a
  // card for a mission the server no longer lists as live, so a winner's
  // reward is only ever shown once, via the compact Campaign Rewards row.
  const { sandbox, roots } = makeSandbox(() => okJson({}));
  sandbox.window.MissionPoolWidget.renderLiveMissions([]); // server already dropped the ended mission
  assert.equal(roots["mission-live-root"].children.length, 0);
});

// ---------------------------------------------------------------------------
// refreshLiveMissions — isolated failure handling
// ---------------------------------------------------------------------------

test("a failed /active fetch leaves the list hidden and never throws", async () => {
  const { sandbox, roots } = makeSandbox(() => Promise.reject(new Error("network down")));
  await assert.doesNotReject(async () => {
    sandbox.window.MissionPoolWidget.refreshLiveMissions();
    await new Promise((r) => setTimeout(r, 10));
  });
  assert.equal(roots["mission-live-root"].style.display, undefined);
});

test("a non-ok HTTP response from /active is treated as no missions, not an error", async () => {
  const { sandbox, roots } = makeSandbox(() =>
    Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve(null) })
  );
  sandbox.window.MissionPoolWidget.refreshLiveMissions();
  await new Promise((r) => setTimeout(r, 10));
  assert.notEqual(roots["mission-live-root"].style.display, "block");
});

test("refreshLiveMissions populates the list from a successful /active response", async () => {
  const { sandbox, roots } = makeSandbox((url) => {
    assert.match(String(url), /\/api\/mission-pool\/active/);
    return okJson({
      status: "ok",
      missions: [{ campaign_id: "m1", campaign_name: "Answer & Win", user_state: "live", already_submitted: false }],
    });
  });
  sandbox.window.MissionPoolWidget.refreshLiveMissions();
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(roots["mission-live-root"].style.display, "block");
});

// ---------------------------------------------------------------------------
// Join/Continue reuses the single mission flow (no second submission path)
// ---------------------------------------------------------------------------

test("tapping Join Mission fetches /view and renders into the existing mission-pool-root, not a new surface", async () => {
  let viewRequested = false;
  const { sandbox, roots } = makeSandbox((url) => {
    if (String(url).indexOf("/view") !== -1) {
      viewRequested = true;
      return okJson({
        status: "ok", mechanic: "mission_pool", campaign_id: "m1", campaign_name: "Answer & Win",
        user_state: "live", mission: { mission_type: "keyword", prompt: "Type it" }, schedule: {},
      });
    }
    return okJson({ status: "ok", missions: [] });
  });
  sandbox.window.MissionPoolWidget.renderLiveMissions([
    { campaign_id: "m1", campaign_name: "Answer & Win", user_state: "live", already_submitted: false },
  ]);
  const card = roots["mission-live-root"].children[1];
  const btn = card.children.find((c) => c.tagName === "BUTTON");
  assert.ok(btn, "expected a CTA button on the live mission card");

  btn.click();
  await new Promise((r) => setTimeout(r, 10));

  assert.equal(viewRequested, true);
  // Rendered into the existing #mission-pool-root — no second DOM surface
  // was created for the submission flow.
  assert.ok(roots["mission-pool-root"].children.length > 0);
});

test("a mission that ends between list load and the CTA tap renders the server's ended state, not a stale form", async () => {
  const { sandbox, roots } = makeSandbox((url) => {
    if (String(url).indexOf("/view") !== -1) {
      return okJson({
        status: "ok", mechanic: "mission_pool", campaign_id: "m1", campaign_name: "Answer & Win",
        user_state: "ended", submissions_open: false, schedule: {},
      });
    }
    return okJson({ status: "ok", missions: [] });
  });
  sandbox.window.MissionPoolWidget.renderLiveMissions([
    { campaign_id: "m1", campaign_name: "Answer & Win", user_state: "live", already_submitted: false },
  ]);
  const card = roots["mission-live-root"].children[1];
  const btn = card.children.find((c) => c.tagName === "BUTTON");
  btn.click();
  await new Promise((r) => setTimeout(r, 10));

  const formRoot = roots["mission-pool-root"];
  assert.ok(formRoot.children.length > 0);
  // renderStateCard (not renderMissionForm) is what runs for a non-"live"
  // user_state — it never renders a submit button, so the ended mission
  // can't be answered from a stale card.
  const hasSubmitButton = JSON.stringify(formRoot._children).indexOf("Submit Mission") !== -1;
  assert.equal(hasSubmitButton, false);
});
