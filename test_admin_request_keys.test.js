/**
 * Dashboard side of server-side idempotency: one stable request id per submission
 * intent (reused on retry, renewed when the payload changes or after success),
 * sent as the Idempotency-Key header by create-drop, add-codes and XP adjust.
 *
 * Run with: node --test test_admin_request_keys.test.js
 */
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");
const start = JS.indexOf("var ADMIN_REQUEST_KEYS");
const end = JS.indexOf("  function dropWriteErrorMessage", start);
assert.ok(start !== -1 && end > start, "markers");

function load(crypto) {
  const sandbox = { window: { crypto } };
  vm.createContext(sandbox);
  vm.runInContext(JS.slice(start, end), sandbox);
  return sandbox;
}
const KEY_RE = /^[A-Za-z0-9._:\-]{8,128}$/; // mirror of admin_idempotency._KEY_RE

test("same payload reuses the key; changed payload gets a new one", () => {
  const s = load({ randomUUID: (() => { let n = 0; return () => "uuid-" + ++n; })() });
  const a = s.adminRequestKey("create_drop", { name: "x", codes: ["A"] });
  assert.equal(s.adminRequestKey("create_drop", { name: "x", codes: ["A"] }), a);
  const b = s.adminRequestKey("create_drop", { name: "x", codes: ["A", "B"] });
  assert.notEqual(b, a);
});

test("success clears the key so the next submission is a new request", () => {
  const s = load({ randomUUID: (() => { let n = 0; return () => "uuid-" + ++n; })() });
  const a = s.adminRequestKey("xp_adjust", { xp: 10 });
  s.adminRequestKeyDone("xp_adjust");
  assert.notEqual(s.adminRequestKey("xp_adjust", { xp: 10 }), a);
});

test("scopes are independent", () => {
  const s = load({ randomUUID: (() => { let n = 0; return () => "uuid-" + ++n; })() });
  assert.notEqual(s.adminRequestKey("add_codes:1", { c: 1 }), s.adminRequestKey("add_codes:2", { c: 1 }));
});

test("generated ids satisfy the server key format, with and without crypto.randomUUID", () => {
  const withCrypto = load({ randomUUID: () => "123e4567-e89b-12d3-a456-426614174000" });
  assert.match(withCrypto.adminRequestKey("s", {}), KEY_RE);
  const without = load(undefined);
  const k = without.adminRequestKey("s", {});
  assert.match(k, KEY_RE);
  without.adminRequestKeyDone("s");
  assert.notEqual(without.adminRequestKey("s", {}), k);
});

test("create-drop, add-codes and XP adjust send the Idempotency-Key header and clear it on success", () => {
  for (const [needle, scopeCall, doneCall] of [
    ['fetch("/v2/miniapp/admin/drops", {', 'adminRequestKey("create_drop", payload)', 'adminRequestKeyDone("create_drop")'],
    ['"/codes", {', "adminRequestKey(addScope, payload)", "adminRequestKeyDone(addScope)"],
    ['fetch("/api/add_xp", {', 'adminRequestKey("xp_adjust", xpPayload)', 'adminRequestKeyDone("xp_adjust")'],
  ]) {
    const hdr = '"Idempotency-Key": ' + scopeCall;
    const i = JS.indexOf(hdr);
    assert.ok(i !== -1, hdr + " missing");
    assert.ok(JS.slice(Math.max(0, i - 300), i).includes(needle), needle + " not the request carrying the header");
    assert.ok(JS.includes(doneCall), doneCall);
  }
});

test("idempotency refusals are explained in plain language", () => {
  const s = { window: {} };
  vm.createContext(s);
  const a = JS.indexOf("var DROP_UNKNOWN_OUTCOME_MESSAGE");
  const b = JS.indexOf("  function dropScheduleHtml", a);
  vm.runInContext(JS.slice(a, b), s);
  for (const code of ["request_in_progress", "idempotency_key_reused", "bad_idempotency_key"]) {
    const m = s.dropWriteErrorMessage({ code }, "created");
    assert.doesNotMatch(m, /[a-z]+_[a-z_]+/, code + " leaked: " + m);
  }
});
