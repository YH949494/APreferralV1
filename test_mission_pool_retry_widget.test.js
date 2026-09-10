/**
 * Mission Reward Pool — retry UX widget tests.
 *
 * Exercises the new incorrect_retry / retry_cooldown / attempts_exhausted
 * submit states in static/mission-pool-widget.js: the form must stay active
 * on a wrong answer (never "Mission completed"), and only disable once the
 * server reports attempts_exhausted.
 *
 * Run with: node --test test_mission_pool_retry_widget.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const SOURCE = fs.readFileSync(path.join(__dirname, "static", "mission-pool-widget.js"), "utf8");

function makeNode(tag) {
  const node = {
    tagName: String(tag || "div").toUpperCase(),
    className: "",
    style: {},
    attributes: {},
    _children: [],
    _listeners: {},
    textContent: "",
    value: "",
    disabled: false,
    parentNode: null,
    appendChild(child) { child.parentNode = node; node._children.push(child); return child; },
    setAttribute(k, v) { node.attributes[k] = String(v); if (k === "id") node.id = String(v); },
    getAttribute(k) { return node.attributes[k]; },
    addEventListener(type, fn) { (node._listeners[type] = node._listeners[type] || []).push(fn); },
    click() { (node._listeners.click || []).forEach((fn) => fn({})); },
    dispatchEvent() { return true; },
    scrollIntoView() { node._scrolled = true; },
    classList: {
      add(c) { node.className += (node.className ? " " : "") + c; },
      remove(c) { node.className = node.className.split(" ").filter((x) => x !== c).join(" "); },
      contains(c) { return node.className.split(" ").indexOf(c) !== -1; },
    },
  };
  Object.defineProperty(node, "children", { get: () => node._children });
  Object.defineProperty(node, "innerHTML", {
    get: () => "",
    set(v) { if (!v) node._children = []; },
  });
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

function findByTag(node, tag) {
  const out = [];
  (function walk(n) {
    if (n.tagName === tag.toUpperCase()) out.push(n);
    (n._children || []).forEach(walk);
  }(node));
  return out;
}

function loadWidget(opts) {
  opts = opts || {};
  const root = makeNode("div");
  root.id = "mission-pool-root";
  const head = makeNode("head");
  const body = makeNode("body");
  const calls = [];
  const events = [];

  const document = {
    readyState: "complete",
    head,
    body,
    getElementById(id) { return id === "mission-pool-root" ? root : null; },
    createElement(tag) { return makeNode(tag); },
    addEventListener() {},
    dispatchEvent() { return true; },
  };

  function respond(method, url) {
    const clean = url.split("?")[0];
    const key = method + " " + clean;
    calls.push(key);
    const handler = (opts.routes || {})[key];
    if (!handler) return Promise.reject(new Error("no route " + key));
    const result = handler();
    if (result === "TIMEOUT") return Promise.reject(new Error("aborted"));
    return Promise.resolve({
      ok: result.httpOk !== false,
      status: result.httpStatus || 200,
      json: () => Promise.resolve(result.body),
    });
  }

  const sandbox = {
    document,
    console,
    setTimeout,
    clearTimeout,
    CustomEvent: function (type, init) { events.push((init && init.detail) || {}); return { type }; },
    AbortController: function () { this.signal = {}; this.abort = function () {}; },
    URLSearchParams,
    fetch: (url, init) => respond((init && init.method) || "GET", url),
    window: {
      Telegram: { WebApp: { initData: opts.initData !== undefined ? opts.initData : "signed-init-data", initDataUnsafe: { start_param: opts.startParam } } },
      MissionPoolEvents: [],
    },
    location: { search: opts.search || "" },
    navigator: { clipboard: { writeText() {} } },
  };
  sandbox.window.document = document;
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(SOURCE, sandbox);

  return { sandbox, root, calls, trackedEvents: sandbox.window.MissionPoolEvents, api: sandbox.window.MissionPoolWidget };
}

function viewBody(overrides) {
  return Object.assign({
    status: "ok",
    campaign_id: "m1",
    campaign_name: "Summer Quiz",
    mechanic: "mission_pool",
    user_state: "live",
    submissions_open: true,
    reason: "open",
    already_submitted: false,
    mission: {
      mission_type: "multiple_choice",
      prompt: "Which game?",
      options: [{ id: "a", label: "Alpha" }, { id: "b", label: "Beta" }],
      max_answer_chars: 2000,
    },
    schedule: { starts_at: "2026-09-01T00:00:00+00:00", ends_at: "2026-09-30T00:00:00+00:00" },
    winner_count: 3,
  }, overrides || {});
}

const VIEW_ROUTE = "GET /api/mission-pool/m1/view";
const SUBMIT_ROUTE = "POST /api/mission-pool/m1/submit";

function tick(times) {
  let p = Promise.resolve();
  for (let i = 0; i < (times || 6); i++) p = p.then(() => {});
  return p;
}

async function submitWith(submitHandler, extraRoutes) {
  const routes = Object.assign({
    [VIEW_ROUTE]: () => ({ body: viewBody() }),
    [SUBMIT_ROUTE]: submitHandler,
  }, extraRoutes || {});
  const w = loadWidget({ startParam: "mission_m1", routes });
  await tick();
  findByTag(w.root, "button").filter((b) => b.attributes["data-option-id"])[0].click();
  const submit = findByText(w.root, "Submit Mission");
  submit.click();
  await tick(10);
  return { w, submit };
}

// ---------------------------------------------------------------------------
// incorrect_retry
// ---------------------------------------------------------------------------

test("incorrect_retry keeps the mission form visible", async () => {
  const { w } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "incorrect_retry", retry_allowed: true },
  }));
  // The options/radiogroup are still in the DOM, not replaced by a state card.
  assert.ok(findByTag(w.root, "button").some((b) => b.attributes["data-option-id"]),
    "options must still be present");
});

test("incorrect_retry never shows Mission completed", async () => {
  const { w } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "incorrect_retry", retry_allowed: true },
  }));
  const text = allText(w.root).join(" ");
  assert.equal(text.indexOf("Mission completed"), -1);
  assert.equal(text.indexOf("reward pool"), -1);
});

test("incorrect_retry shows the generic retry copy", async () => {
  const { w } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "incorrect_retry", retry_allowed: true },
  }));
  assert.ok(findByText(w.root, "Not quite. Try again."));
});

test("user can resubmit after a wrong answer", async () => {
  const { w, submit } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "incorrect_retry", retry_allowed: true },
  }));
  assert.equal(submit.disabled, false, "submit must re-enable after a wrong answer");
  assert.equal(submit.textContent, "Submit Mission");
});

test("attempts_remaining is shown when the server supplies it", async () => {
  const { w } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "incorrect_retry", retry_allowed: true, attempts_remaining: 2 },
  }));
  assert.ok(findByText(w.root, "2 attempts remaining"));
});

test("a successful retry switches to the existing completed state", async () => {
  let call = 0;
  const { w, submit } = await submitWith(() => {
    call += 1;
    if (call === 1) return { body: { status: "ok", submitted: false, state: "incorrect_retry", retry_allowed: true } };
    return { body: { status: "ok", submitted: true, state: "submitted" } };
  });
  assert.equal(submit.disabled, false);
  submit.click();
  await tick(10);
  assert.ok(findByText(w.root, "✅ Mission completed"));
  assert.ok(findByText(w.root, "You're in the reward pool."));
});

test("no correct answer is ever exposed in the incorrect_retry response handling", async () => {
  const { w } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "incorrect_retry", retry_allowed: true },
  }));
  const text = allText(w.root).join(" ").toLowerCase();
  ["correct_answer", "hint", "option index"].forEach((bad) => {
    assert.equal(text.indexOf(bad), -1, `leaked "${bad}"`);
  });
});

// ---------------------------------------------------------------------------
// retry_cooldown
// ---------------------------------------------------------------------------

test("retry_cooldown keeps the form active and re-enables submit", async () => {
  const { w, submit } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "retry_cooldown", retry_allowed: true, retry_after_seconds: 2 },
  }));
  assert.equal(submit.disabled, false);
  const text = allText(w.root).join(" ");
  assert.equal(text.indexOf("Mission completed"), -1);
});

// ---------------------------------------------------------------------------
// attempts_exhausted
// ---------------------------------------------------------------------------

test("attempts_exhausted disables further submission", async () => {
  const { w, submit } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "attempts_exhausted", retry_allowed: false },
  }));
  assert.equal(submit.disabled, true);
  assert.ok(findByText(w.root, "No more attempts available for this mission."));
});

test("attempts_exhausted never mentions internal anti-abuse/disqualification wording", async () => {
  const { w } = await submitWith(() => ({
    body: { status: "ok", submitted: false, state: "attempts_exhausted", retry_allowed: false },
  }));
  const text = allText(w.root).join(" ").toLowerCase();
  ["disqualified", "abuse", "banned", "flagged"].forEach((bad) => {
    assert.equal(text.indexOf(bad), -1, `leaked "${bad}"`);
  });
});

// ---------------------------------------------------------------------------
// Opinion poll / feedback still lock after one submission (unaffected by
// retry logic — the server never returns incorrect_retry for these).
// ---------------------------------------------------------------------------

test("opinion poll still locks after one submission", async () => {
  const { w } = await submitWith(() => ({ body: { status: "ok", submitted: true, state: "submitted" } }));
  assert.ok(findByText(w.root, "✅ Mission completed"));
});

test("already_submitted (feedback/opinion re-submit) still shows the existing duplicate copy", async () => {
  const { w } = await submitWith(() => ({ body: { status: "ok", submitted: true, state: "already_submitted" } }));
  assert.ok(findByText(w.root, "✅ Mission already completed"));
});

// ---------------------------------------------------------------------------
// Existing normal flow unchanged
// ---------------------------------------------------------------------------

test("normal successful submission flow is unchanged by the retry states", async () => {
  const { w } = await submitWith(() => ({ body: { status: "ok", submitted: true, state: "submitted" } }));
  assert.ok(findByText(w.root, "✅ Mission completed"));
  assert.ok(findByText(w.root, "You're in the reward pool."));
});
