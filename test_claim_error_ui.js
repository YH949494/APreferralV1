const fs = require("node:fs");
const vm = require("node:vm");
const test = require("node:test");
const assert = require("node:assert/strict");

const html = fs.readFileSync("static/index.html", "utf8");

function extractFunction(name) {
  const start = html.indexOf(`    function ${name}(`);
  assert.notEqual(start, -1, `${name} missing`);
  const brace = html.indexOf(") {", start) + 2;
  let depth = 0;
  for (let i = brace; i < html.length; i += 1) {
    if (html[i] === "{") depth += 1;
    if (html[i] === "}") depth -= 1;
    if (depth === 0) return html.slice(start, i + 1);
  }
  throw new Error(`unterminated ${name}`);
}

class FakeElement {
  constructor() {
    this.children = [];
    this.attributes = new Set();
    this.listeners = {};
    this.style = {};
    this.textContent = "";
  }
  appendChild(child) { this.children.push(child); }
  setAttribute(name) { this.attributes.add(name); }
  removeAttribute(name) { this.attributes.delete(name); }
  addEventListener(name, fn) { this.listeners[name] = fn; }
  set innerHTML(value) { this.children = []; }
}

function makeContext() {
  const container = new FakeElement();
  const active = new Map();
  const cleared = [];
  let nextTimer = 1;
  const context = {
    console,
    Promise,
    claimErrorCountdownTimer: null,
    document: {
      getElementById: () => container,
      createElement: () => new FakeElement()
    },
    hapticNotify: () => {},
    t: (key) => key,
    setInterval: (fn) => { const id = nextTimer++; active.set(id, fn); return id; },
    clearInterval: (id) => { cleared.push(id); active.delete(id); }
  };
  vm.createContext(context);
  for (const name of ["normalizeRetryAfterSec", "formatClaimCountdown", "claimErrorToUi", "renderClaimError"]) {
    vm.runInContext(extractFunction(name), context);
  }
  return { context, container, active, cleared };
}

test("rejoin response with retry_after_sec maps to actionable UI", () => {
  const { context } = makeContext();
  const ui = context.claimErrorToUi({
    code: "rejoin_buffer_active",
    reason: "rejoin_buffer_active",
    retry_after_sec: 3661,
    buffer_until: "2026-07-29T12:00:00+00:00"
  }, 403);
  assert.equal(ui.title, "Please stay subscribed");
  assert.equal(ui.message, "You recently rejoined the official channel. You can claim after the waiting period.");
  assert.equal(ui.retryAfterSec, 3661);
  assert.equal(context.formatClaimCountdown(3661), "01:01:01");
});

test("code-only response is recognized", () => {
  const { context } = makeContext();
  assert.equal(context.claimErrorToUi({ code: "rejoin_buffer_active" }, 403).isRejoinBuffer, true);
});

test("reason-only response is recognized", () => {
  const { context } = makeContext();
  assert.equal(context.claimErrorToUi({ reason: "rejoin_buffer_active" }, 403).isRejoinBuffer, true);
});

test("countdown expiry runs preflight before enabling Retry", async () => {
  const { context, container, active } = makeContext();
  let preflights = 0;
  context.renderClaimError(
    context.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 1 }, 403),
    { preflightHandler: async () => { preflights += 1; return null; } }
  );
  const retry = container.children[0].children[2].children[0];
  assert.equal(retry.attributes.has("disabled"), true);
  await [...active.values()][0]();
  await Promise.resolve();
  assert.equal(preflights, 1);
  assert.equal(retry.attributes.has("disabled"), false);
});

test("repeated rendering keeps only one countdown timer", () => {
  const { context, active, cleared } = makeContext();
  const ui = context.claimErrorToUi({ reason: "rejoin_buffer_active", retry_after_sec: 5 }, 403);
  context.renderClaimError(ui);
  context.renderClaimError(ui);
  assert.equal(active.size, 1);
  assert.equal(cleared.length, 1);
});

test("generic errors remain generic", () => {
  const { context } = makeContext();
  const ui = context.claimErrorToUi({ code: "server_error" }, 500);
  assert.equal(ui.title, "temporarily_unavailable");
  assert.equal(ui.message, "please_try_again");
  assert.equal(ui.isRejoinBuffer, undefined);
});
