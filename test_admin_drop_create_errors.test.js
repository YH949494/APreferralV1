/**
 * Dashboard side of the atomic drop-creation fix: failures of create-drop and
 * add-codes are explained in plain language instead of printing the raw
 * backend code (`duplicate_code`), say "Nothing was created/added" only when
 * the server actually rejected the request, and a dropped connection is
 * reported as an UNKNOWN outcome (the write may have committed).
 *
 * Run with: node --test test_admin_drop_create_errors.test.js
 */
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");
const start = JS.indexOf("var DROP_UNKNOWN_OUTCOME_MESSAGE");
const end = JS.indexOf("  function dropScheduleHtml", start);
assert.ok(start !== -1 && end > start, "markers");
const sandbox = {};
vm.createContext(sandbox);
vm.runInContext(JS.slice(start, end), sandbox);
const msg = (d, done) => sandbox.dropWriteErrorMessage(d, done);

test("duplicate_code names the count and a sample and says nothing was created", () => {
  const m = msg({ code: "duplicate_code", duplicates: 5, duplicateCodes: ["A1", "A2", "A3", "A4"] }, "created");
  assert.match(m, /5 of these codes are already used in another voucher drop/);
  assert.match(m, /A1, A2, A3/);
  assert.doesNotMatch(m, /A4/);
  assert.match(m, /Nothing was created/);
  assert.doesNotMatch(m, /duplicate_code/);
});

test("add-codes variant says nothing was added", () => {
  assert.match(msg({ code: "duplicate_code" }, "added"), /Nothing was added/);
});

test("known reasons are translated and never leak snake_case", () => {
  for (const reason of ["empty_codes", "codes_must_be_list", "empty_assignments", "duplicate_username", "bad_pool", "bad_priority", "bad_type", "creation_not_completed"]) {
    const m = msg({ code: "bad_request", reason }, "created");
    assert.doesNotMatch(m, /[a-z]+_[a-z_]+/, reason + " leaked: " + m);
    assert.match(m, /Nothing was created/);
  }
});

test("known codes are translated", () => {
  for (const code of ["bad_start", "bad_end", "end_before_start", "bad_request", "not_found", "drop_creating", "drop_deleting", "server_error"]) {
    const m = msg({ code }, "created");
    assert.doesNotMatch(m, /[a-z]+_[a-z_]+/, code + " leaked: " + m);
  }
});

test("unknown codes still produce a readable sentence", () => {
  assert.match(msg({ code: "zzz" }, "created"), /Something went wrong \(zzz\)\. Nothing was created\./);
  assert.match(msg(undefined, "created"), /unknown/);
});

test("transport failures are reported as an unknown outcome, not 'nothing was saved'", () => {
  assert.match(sandbox.DROP_UNKNOWN_OUTCOME_MESSAGE, /can't tell whether this was saved/);
  assert.match(sandbox.DROP_UNKNOWN_OUTCOME_MESSAGE, /Check Existing Drops/);
  assert.doesNotMatch(sandbox.DROP_UNKNOWN_OUTCOME_MESSAGE, /Nothing was/);
});

test("wiring: both handlers use the translator and the raw `throw new Error(res.d.code ...)` is gone", () => {
  assert.doesNotMatch(JS, /throw new Error\(res\.d\.code \|\| "unknown"\)/);
  assert.match(JS, /dropWriteErrorMessage\(res\.d, "created"\)/);
  assert.match(JS, /dropWriteErrorMessage\(res\.d, "added"\)/);
  assert.equal((JS.match(/e && e\.fromServer \? e\.message : DROP_UNKNOWN_OUTCOME_MESSAGE/g) || []).length, 2);
});
