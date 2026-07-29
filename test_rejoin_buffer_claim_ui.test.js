/**
 * Tests for the Mini App's rejoin-buffer claim-block handling
 * (claimErrorToUi / renderClaimError / recheckAfterRejoinBuffer) in
 * static/index.html. These functions turn a blocked
 * `rejoin_buffer_active` claim response into an explicit "Please stay
 * subscribed" card with an HH:MM:SS countdown, instead of the generic
 * "Temporarily unavailable" message.
 *
 * The functions live inline in static/index.html (no build step), so they
 * are extracted as source text and executed in a sandboxed vm context with
 * mocked DOM/fetch/timer globals.
 *
 * Run with: node --test test_rejoin_buffer_claim_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "let claimErrorCountdownTimer = null;";
const END_MARKER = "\n    async function loadVouchers() {";

function loadFunctionsSource() {
  const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "claimErrorCountdownTimer declaration not found in static/index.html");
  assert.ok(end !== -1, "loadVouchers end marker not found in static/index.html");
  return html.slice(start, end);
}

function makeNode() {
  const node = {
    style: {},
    className: "",
    textContent: "",
    innerHTML: "",
    children: [],
    attrs: {},
    listeners: {},
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    addEventListener(type, handler) {
      this.listeners[type] = handler;
    },
    setAttribute(name, value) {
      this.attrs[name] = value;
    },
    removeAttribute(name) {
      delete this.attrs[name];
    },
    get disabled() {
      return Object.prototype.hasOwnProperty.call(this.attrs, "disabled");
    },
  };
  return node;
}

function buildSandbox({ fetchImpl } = {}) {
  const container = makeNode();
  container.style.display = "none";

  const intervals = new Map();
  let nextIntervalId = 1;

  const translations = {
    retry: "Retry",
    retry_available_in: (vars) => `Retry available in ${vars.n}s`,
    you_can_retry: "You can retry now",
  };

  const sandbox = {
    document: {
      getElementById(id) {
        if (id === "claim-error") return container;
        return null;
      },
      createElement() {
        return makeNode();
      },
      contains() {
        return true;
      },
      body: { contains: () => true },
    },
    t(key, vars) {
      const entry = translations[key];
      if (typeof entry === "function") return entry(vars || {});
      return entry || key;
    },
    hapticNotify() {},
    API_V2: "/api/v2",
    tg: {},
    getLatestInitData() {
      return "init-data";
    },
    v2Fetch: fetchImpl || (async () => { throw new Error("v2Fetch not mocked"); }),
    console,
    setInterval(fn, ms) {
      const id = nextIntervalId++;
      intervals.set(id, { fn, ms, cleared: false });
      return id;
    },
    clearInterval(id) {
      const entry = intervals.get(id);
      if (entry) entry.cleared = true;
    },
  };

  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  return { sandbox, container, intervals };
}

function run(sandbox, extraSource) {
  const src = loadFunctionsSource() + (extraSource || "");
  vm.runInContext(src, sandbox);
}

function activeIntervalCount(intervals) {
  let n = 0;
  for (const entry of intervals.values()) {
    if (!entry.cleared) n += 1;
  }
  return n;
}

// ---------------------------------------------------------------------
// claimErrorToUi: rejoin_buffer_active recognition
// ---------------------------------------------------------------------

test("claimErrorToUi recognises rejoin_buffer_active via `code`", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi(
    { code: "rejoin_buffer_active", retry_after_sec: 5400, buffer_until: "2026-07-29T18:00:00Z" },
    403
  );
  assert.equal(uiModel.title, "Please stay subscribed");
  assert.equal(
    uiModel.message,
    "You recently rejoined the official channel. You can claim after the waiting period."
  );
  assert.equal(uiModel.isRejoinBuffer, true);
  assert.equal(uiModel.retryAfterSec, 5400);
});

test("claimErrorToUi recognises rejoin_buffer_active via `reason` only (no code)", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi(
    { reason: "rejoin_buffer_active", retry_after_sec: 120 },
    403
  );
  assert.equal(uiModel.isRejoinBuffer, true);
  assert.equal(uiModel.title, "Please stay subscribed");
});

test("claimErrorToUi recognises rejoin_buffer_active via `code` only (no reason)", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active" }, 403);
  assert.equal(uiModel.isRejoinBuffer, true);
  assert.equal(uiModel.retryAfterSec, null);
});

test("generic/unknown errors still map to the generic temporarily-unavailable message", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi({ code: "server_error" }, 500);
  assert.equal(uiModel.isRejoinBuffer, undefined);
  assert.notEqual(uiModel.title, "Please stay subscribed");
});

test("sold_out, not_subscribed, and rate_limited keep their existing (non-generic) behaviour", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);

  const soldOut = sandbox.claimErrorToUi({ code: "sold_out" }, 410);
  assert.equal(soldOut.hideRetry, true);

  const notSubscribed = sandbox.claimErrorToUi({ code: "not_subscribed" }, 403);
  assert.equal(notSubscribed.ctaAction, "join_channel");

  const rateLimited = sandbox.claimErrorToUi({ code: "rate_limited", retry_after_sec: 3 }, 429);
  assert.equal(rateLimited.retryAfterSec, 3);
  assert.notEqual(rateLimited.title, "Please stay subscribed");
});

// ---------------------------------------------------------------------
// renderClaimError: countdown behaviour
// ---------------------------------------------------------------------

test("renderClaimError shows an HH:MM:SS countdown and disables Retry while retry_after_sec > 0", () => {
  const { sandbox, intervals } = buildSandbox();
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi(
    { code: "rejoin_buffer_active", retry_after_sec: 3661 },
    403
  );
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  const card = sandbox.document.getElementById("claim-error").children[0];
  const actions = card.children.find((c) => c.children && c.children.length >= 2);
  const retryBtn = actions.children.find((c) => c.textContent === "Retry");
  const countdown = actions.children.find((c) => c !== retryBtn && "textContent" in c);

  assert.ok(retryBtn.disabled, "Retry must be disabled while retry_after_sec > 0");
  assert.equal(countdown.textContent, "Try again in 01:01:01");
  assert.equal(activeIntervalCount(intervals), 1, "exactly one countdown timer should be running");
});

test("countdown expiry re-enables Retry after a passing eligibility recheck", async () => {
  const fetchImpl = async () => ({
    ok: true,
    status: 200,
    json: async () => ({ status: "ok", check_only: true, subscribed: true }),
  });
  const { sandbox, intervals } = buildSandbox({ fetchImpl });
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 2 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  const card = sandbox.document.getElementById("claim-error").children[0];
  const actions = card.children.find((c) => c.children && c.children.length >= 2);
  const retryBtn = actions.children.find((c) => c.textContent === "Retry");

  assert.ok(retryBtn.disabled);

  const [id, entry] = [...intervals.entries()][0];
  await entry.fn(); // tick 1 -> remaining=1
  await entry.fn(); // tick 2 -> remaining=0 -> triggers recheck

  assert.equal(activeIntervalCount(intervals), 0, "timer must be cleared once countdown ends");
  assert.equal(sandbox.document.getElementById("claim-error").style.display, "none", "error card cleared once eligible");
});

test("countdown expiry re-renders the block when the rejoin buffer is still active", async () => {
  const fetchImpl = async () => ({
    ok: false,
    status: 403,
    json: async () => ({
      status: "blocked",
      code: "rejoin_buffer_active",
      reason: "rejoin_buffer_active",
      retry_after_sec: 30,
      buffer_until: "2026-07-29T19:00:00Z",
    }),
  });
  const { sandbox, intervals } = buildSandbox({ fetchImpl });
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 1 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  const [, firstEntry] = [...intervals.entries()][0];
  await firstEntry.fn(); // remaining=0 -> recheck -> still blocked -> re-render

  // Exactly one live interval after the re-render (the old one cleared, one new one started).
  assert.equal(activeIntervalCount(intervals), 1, "no duplicate timers after re-render");

  const card = sandbox.document.getElementById("claim-error").children[0];
  const title = card.children.find((c) => c.textContent === "Please stay subscribed");
  assert.ok(title, "re-rendered card should still show the rejoin-buffer title");
});

test("renderClaimError never creates more than one active countdown timer across repeated calls", () => {
  const { sandbox, intervals } = buildSandbox();
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 100 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  assert.equal(activeIntervalCount(intervals), 1, "re-rendering must clear the previous timer, not stack new ones");
});

test("clearClaimError stops any running rejoin-buffer countdown timer", () => {
  const { sandbox, intervals } = buildSandbox();
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 100 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });
  assert.equal(activeIntervalCount(intervals), 1);

  sandbox.clearClaimError();
  assert.equal(activeIntervalCount(intervals), 0);
});
