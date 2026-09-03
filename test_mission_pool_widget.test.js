/**
 * Mission Reward Pool — Mini App widget tests (Phase 2).
 *
 * Loads static/mission-pool-widget.js into a minimal DOM sandbox and drives
 * it through the real code path (mount -> fetch -> render -> submit), so the
 * activation rule, the state copy and the submission/timeout behaviour are
 * exercised as shipped rather than restated.
 *
 * Run with: node --test test_mission_pool_widget.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const SOURCE = fs.readFileSync(path.join(__dirname, "static", "mission-pool-widget.js"), "utf8");

// ---------------------------------------------------------------------------
// Minimal DOM
// ---------------------------------------------------------------------------

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

/**
 * @param {object} opts
 *   startParam  - Telegram initDataUnsafe.start_param
 *   search      - location.search
 *   routes      - { "METHOD /path": () => response | "TIMEOUT" }
 */
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
      Telegram: { WebApp: { initData: "signed-init-data", initDataUnsafe: { start_param: opts.startParam } } },
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
const STATUS_ROUTE = "GET /api/mission-pool/m1/status";

function tick(times) {
  let p = Promise.resolve();
  for (let i = 0; i < (times || 6); i++) p = p.then(() => {});
  return p;
}

// ---------------------------------------------------------------------------
// Activation (§5, §7, §22)
// ---------------------------------------------------------------------------

test("no mission deep link -> zero requests and zero DOM changes", async () => {
  const w = loadWidget({ startParam: undefined, routes: {} });
  await tick();
  assert.deepEqual(w.calls, [], "a normal Mini App open must not call any Mission API");
  assert.equal(w.root.children.length, 0);
});

test("the existing attr_ ad-attribution start param is not a mission link", async () => {
  const w = loadWidget({ startParam: "attr_deadbeef", routes: {} });
  await tick();
  assert.deepEqual(w.calls, []);
  assert.equal(w.root.children.length, 0);
});

test("mission deep link routes to the mission view endpoint", async () => {
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody() }) },
  });
  await tick();
  assert.deepEqual(w.calls, [VIEW_ROUTE]);
  assert.ok(findByText(w.root, "Summer Quiz"));
});

test("?mission= query fallback works for browser testing", async () => {
  const w = loadWidget({
    search: "?mission=m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody() }) },
  });
  await tick();
  assert.deepEqual(w.calls, [VIEW_ROUTE]);
});

test("server answer without mechanic=mission_pool renders nothing", async () => {
  // §5: the mechanic is never inferred from the route or the deep link.
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody({ mechanic: "standard_drop" }) }) },
  });
  await tick();
  assert.equal(w.root.children.length, 0);
});

test("a 404 for a forged campaign renders nothing", async () => {
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ httpOk: false, httpStatus: 404, body: { status: "error", code: "campaign_not_found" } }) },
  });
  await tick();
  assert.equal(w.root.children.length, 0);
});

test("parseMissionParam rejects unsafe references", () => {
  const w = loadWidget({ routes: {} });
  const p = w.api.parseMissionParam;
  assert.equal(p("mission_ok-1"), "ok-1");
  assert.equal(p("mission_bad id"), null);
  assert.equal(p("mission_bad/id"), null);
  assert.equal(p("mission_" + "x".repeat(100)), null);
  assert.equal(p("attr_x"), null);
  assert.equal(p(123), null);
});

// ---------------------------------------------------------------------------
// Mission input types (§9)
// ---------------------------------------------------------------------------

test("multiple choice renders one selectable option per server option", async () => {
  const w = loadWidget({ startParam: "mission_m1", routes: { [VIEW_ROUTE]: () => ({ body: viewBody() }) } });
  await tick();
  const buttons = findByTag(w.root, "button").filter((b) => b.attributes["data-option-id"]);
  assert.equal(buttons.length, 2);
  assert.deepEqual(buttons.map((b) => b.attributes["data-option-id"]), ["a", "b"]);
  buttons[1].click();
  assert.equal(buttons[1].getAttribute("aria-checked"), "true");
  assert.equal(buttons[0].getAttribute("aria-checked"), "false", "only one option may be selected");
});

test("keyword renders a bounded text input", async () => {
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody({ mission: { mission_type: "keyword", prompt: "Type the code", max_answer_chars: 2000 } }) }) },
  });
  await tick();
  const inputs = findByTag(w.root, "input");
  assert.equal(inputs.length, 1);
  assert.equal(inputs[0].getAttribute("maxlength"), "2000");
});

test("feedback renders a textarea with a live character counter", async () => {
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody({ mission: { mission_type: "feedback", prompt: "Tell us", min_chars: 10, max_chars: 200 } }) }) },
  });
  await tick();
  const ta = findByTag(w.root, "textarea")[0];
  assert.ok(ta);
  assert.equal(ta.getAttribute("maxlength"), "200");
  assert.ok(findByText(w.root, "0 / 200"));
  ta.value = "hello";
  (ta._listeners.input || []).forEach((fn) => fn({}));
  assert.ok(findByText(w.root, "5 / 200"));
});

// ---------------------------------------------------------------------------
// User campaign states (§13, §44)
// ---------------------------------------------------------------------------

const STATE_CASES = [
  ["scheduled", "This mission starts at"],
  ["paused", "⏸️ This mission is temporarily paused."],
  ["submitted", "You're in the reward pool."],
  ["closed_processing", "We're finalising the qualified entries and winners."],
  ["won", "🎉 You're a winner!"],
  ["not_won", "This round has ended."],
  ["ended", "This mission has ended."],
  ["cancelled", "This mission has been cancelled."],
];

for (const [state, needle] of STATE_CASES) {
  test(`state ${state} renders its documented copy`, async () => {
    const w = loadWidget({
      startParam: "mission_m1",
      routes: { [VIEW_ROUTE]: () => ({ body: viewBody({ user_state: state }) }) },
    });
    await tick();
    assert.ok(findByText(w.root, needle), `expected copy for ${state}`);
  });
}

test("no user-facing state ever exposes an internal abuse reason", async () => {
  // §44: the participant is told the round ended, never why they lost.
  const forbidden = ["voucher hunter", "voucher_hunter", "multi-account", "multi_account",
    "duplicate gaming", "identity key", "identity_key", "blocked", "disqualif",
    "selection seed", "selection_seed", "risk"];
  for (const [state] of STATE_CASES) {
    const w = loadWidget({
      startParam: "mission_m1",
      routes: { [VIEW_ROUTE]: () => ({ body: viewBody({ user_state: state }) }) },
    });
    await tick();
    const text = allText(w.root).join(" ").toLowerCase();
    for (const bad of forbidden) {
      assert.equal(text.indexOf(bad), -1, `state ${state} leaked "${bad}"`);
    }
  }
});

test("no state promises a reward before selection", async () => {
  // §10: "Mission completed" means participation, never a secured reward.
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody({ user_state: "submitted" }) }) },
  });
  await tick();
  const text = allText(w.root).join(" ").toLowerCase();
  ["reward secured", "voucher reserved", "you won"].forEach((bad) => {
    assert.equal(text.indexOf(bad), -1);
  });
});

test("the winner state links to Campaign Rewards and shows no voucher code", async () => {
  // §19: Campaign Rewards is the single canonical reward surface.
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody({ user_state: "won" }) }) },
  });
  await tick();
  assert.ok(findByText(w.root, "🎁 Go to Campaign Rewards"));
  const text = allText(w.root).join(" ");
  assert.equal(text.indexOf("Copy Code"), -1);
});

// ---------------------------------------------------------------------------
// Submission UX (§10, §11, §12)
// ---------------------------------------------------------------------------

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
  assert.equal(submit.disabled, true, "submit must disable immediately");
  assert.equal(submit.textContent, "Submitting…");
  await tick(10);
  return { w, submit };
}

test("successful submission shows the reward-pool confirmation", async () => {
  const { w } = await submitWith(() => ({ body: { status: "ok", submitted: true, state: "submitted" } }));
  assert.ok(findByText(w.root, "✅ Mission completed"));
  assert.ok(findByText(w.root, "You're in the reward pool."));
});

test("already_submitted is treated as success, never as a conflict", async () => {
  // §11: the user sees confirmation, never "409" / "Duplicate request".
  const { w } = await submitWith(() => ({ body: { status: "ok", submitted: true, state: "already_submitted" } }));
  assert.ok(findByText(w.root, "✅ Mission already completed"));
  const text = allText(w.root).join(" ").toLowerCase();
  ["duplicate", "409", "conflict", "already exists"].forEach((bad) => {
    assert.equal(text.indexOf(bad), -1, `leaked "${bad}"`);
  });
});

test("a timeout checks status instead of declaring failure", async () => {
  // §12: an ambiguous network failure must never be reported as a failed
  // submission when the write may in fact have landed.
  const { w } = await submitWith(() => "TIMEOUT", {
    [STATUS_ROUTE]: () => ({ body: { status: "ok", submitted: true, entry_status: "submitted" } }),
  });
  assert.ok(w.calls.indexOf(STATUS_ROUTE) !== -1, "must recheck server status after a timeout");
  assert.ok(findByText(w.root, "✅ Mission completed"), "a recovered submission is a success");
});

test("a timeout with no entry re-enables retry", async () => {
  const { w, submit } = await submitWith(() => "TIMEOUT", {
    [STATUS_ROUTE]: () => ({ body: { status: "ok", submitted: false, entry_status: null } }),
  });
  assert.equal(submit.disabled, false);
  assert.equal(submit.textContent, "Submit Mission");
  assert.ok(findByText(w.root, "We couldn't confirm your submission. Please try again."));
});

test("a server validation error is shown in plain language and allows retry", async () => {
  const { w, submit } = await submitWith(() => ({
    httpOk: false, httpStatus: 400, body: { status: "error", code: "invalid_option" },
  }));
  assert.equal(submit.disabled, false);
  assert.ok(findByText(w.root, "Please choose one of the options shown."));
});

test("a closed campaign rejection is shown without internal codes", async () => {
  const { w } = await submitWith(() => ({
    httpOk: false, httpStatus: 409, body: { status: "error", code: "campaign_closed" },
  }));
  assert.ok(findByText(w.root, "This mission has closed."));
});

test("submitting nothing is blocked client-side without a request", async () => {
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody() }) },
  });
  await tick();
  findByText(w.root, "Submit Mission").click();
  await tick();
  assert.equal(w.calls.filter((c) => c === SUBMIT_ROUTE).length, 0);
  assert.ok(findByText(w.root, "Please answer the mission first."));
});

test("the submitted payload is a flat string, never a nested object", async () => {
  // §43: no client-generated Mongo-style structure can originate here.
  let captured = null;
  const root = makeNode("div");
  const w = loadWidget({
    startParam: "mission_m1",
    routes: {
      [VIEW_ROUTE]: () => ({ body: viewBody() }),
      [SUBMIT_ROUTE]: () => ({ body: { status: "ok", submitted: true, state: "submitted" } }),
    },
  });
  const originalFetch = w.sandbox.fetch;
  w.sandbox.fetch = (url, init) => {
    if (init && init.method === "POST") captured = JSON.parse(init.body);
    return originalFetch(url, init);
  };
  await tick();
  findByTag(w.root, "button").filter((b) => b.attributes["data-option-id"])[0].click();
  findByText(w.root, "Submit Mission").click();
  await tick(10);
  assert.deepEqual(Object.keys(captured), ["answer"]);
  assert.equal(typeof captured.answer, "string");
  assert.equal(captured.answer, "a");
  void root;
});

// ---------------------------------------------------------------------------
// Observability (§45, §46)
// ---------------------------------------------------------------------------

test("mission UI events are tracked without any sensitive payload", async () => {
  const { w } = await submitWith(() => ({ body: { status: "ok", submitted: true, state: "submitted" } }));
  const names = w.trackedEvents.map((e) => e.event);
  assert.ok(names.indexOf("mission_ui_opened") !== -1);
  assert.ok(names.indexOf("mission_submit_clicked") !== -1);
  assert.ok(names.indexOf("mission_submit_success") !== -1);
  const serialized = JSON.stringify(w.trackedEvents);
  ["voucher_code", "answer", "gaming", "identity_key", "risk"].forEach((bad) => {
    assert.equal(serialized.indexOf(bad), -1, `event payload leaked "${bad}"`);
  });
});

// ---------------------------------------------------------------------------
// Startup cost (§22, §56)
// ---------------------------------------------------------------------------

test("a non-mission open adds ZERO startup requests", async () => {
  // The whole point of gating on the deep link: Phase 2 must not slow down
  // every ordinary Mini App open. This fails if the widget ever fetches
  // eagerly.
  const w = loadWidget({ startParam: undefined, routes: {} });
  await tick(10);
  assert.equal(w.calls.length, 0);
});

test("a mission open costs exactly ONE request", async () => {
  // §22: one consolidated /view call, not fetch-mission + fetch-winner +
  // fetch-popup + fetch-status.
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody() }) },
  });
  await tick(10);
  assert.deepEqual(w.calls, [VIEW_ROUTE]);
});

test("the widget never polls", async () => {
  // §23: no timer-driven refresh loop.
  const source = SOURCE.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
  assert.equal(/setInterval/.test(source), false, "no polling interval may exist");
  const w = loadWidget({
    startParam: "mission_m1",
    routes: { [VIEW_ROUTE]: () => ({ body: viewBody() }) },
  });
  await tick(10);
  const initial = w.calls.length;
  await new Promise((r) => setTimeout(r, 250));
  assert.equal(w.calls.length, initial, "no request may fire on a timer");
});

test("a duplicate submission emits its own event", async () => {
  const { w } = await submitWith(() => ({ body: { status: "ok", submitted: true, state: "already_submitted" } }));
  assert.ok(w.trackedEvents.map((e) => e.event).indexOf("mission_submit_duplicate") !== -1);
});
