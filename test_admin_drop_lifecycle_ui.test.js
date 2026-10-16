/**
 * Dashboard side of the drop Start/Pause/Resume fix.
 *
 *  - Start is never offered on a drop that is already live; paused drops get
 *    Resume (not Start); ended / still-being-created drops get no lifecycle
 *    buttons. The server's `availableOps` is authoritative; the fallback table
 *    mirrors it.
 *  - Every lifecycle action is confirmed, and the confirmation states what it
 *    will NOT change (the schedule) so nothing is silently reset.
 *  - Failures are shown in plain language, never as raw backend codes.
 *
 * Run with: node --test test_admin_drop_lifecycle_ui.test.js
 */
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");
const start = JS.indexOf("  function dropRowHtml(d) {");
const end = JS.indexOf("  // Mission Pool campaigns don't live in db.drops", start);
assert.ok(start !== -1 && end > start, "markers");

const sandbox = {
  esc: (v) => String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])),
  fmt: (n) => String(n),
  dropScheduleHtml: (a, b) => `${a}→${b}`,
  ccUtcToKlDisplay: (iso) => `KL(${iso})`,
};
vm.createContext(sandbox);
vm.runInContext(JS.slice(start, end), sandbox);

const row = (d) => sandbox.dropRowHtml(Object.assign({ dropId: "d1", name: "Weekend", type: "pooled", priority: 100, codesFree: 7, codesTotal: 10, startsAt: "S", endsAt: "2030-01-01T00:00:00+00:00" }, d));
const ops = (html) => (html.match(/data-drop-op="([a-z_]+)"/g) || []).map((m) => m.match(/"([a-z_]+)"/)[1]);

test("a live drop is never offered Start", () => {
  assert.deepEqual(ops(row({ status: "active", availableOps: ["pause", "end_now"] })), ["pause", "end_now"]);
  assert.deepEqual(ops(row({ status: "active" })), ["pause", "end_now"], "fallback must agree");
});

test("per-state buttons (server list and fallback agree)", () => {
  const cases = {
    upcoming: ["start_now", "pause", "end_now"],
    paused: ["resume", "end_now"],
    expired: [], creating: [], creation_failed: [],
  };
  for (const [status, expected] of Object.entries(cases)) {
    assert.deepEqual(ops(row({ status })), expected, "fallback " + status);
    assert.deepEqual(ops(row({ status, availableOps: expected })), expected, "server " + status);
  }
});

test("server availableOps wins over the status guess", () => {
  assert.deepEqual(ops(row({ status: "active", availableOps: [] })), [], "stored 'active' but window over -> no buttons");
});

test("a drop with no ops renders a dash, not an empty cell of dead buttons", () => {
  assert.match(row({ status: "expired" }), /<td>—<\/td><\/tr>$/);
});

test("buttons carry what the confirmation needs", () => {
  const html = row({ status: "active", name: "A & B", codesFree: 3, codesTotal: 10 });
  assert.match(html, /data-drop-name="A &amp; B"/);
  assert.match(html, /data-drop-ends="2030-01-01T00:00:00\+00:00"/);
  assert.match(html, /data-drop-claimed="7"/);
  assert.match(html, /data-drop-total="10"/);
});

test("labels: Resume is its own word and End stays the danger button", () => {
  const html = row({ status: "paused" });
  assert.match(html, />Resume</);
  assert.doesNotMatch(html, />Start</);
  assert.match(html, /class="btn danger" data-drop-op="end_now"/);
});

const msg = (op, extra) => sandbox.dropOpConfirmMessage(op, Object.assign({ name: "Weekend", ends: "E" }, extra));

test("every lifecycle op has a confirmation naming the drop", () => {
  for (const op of ["start_now", "pause", "resume", "end_now"]) {
    assert.match(msg(op), /Weekend/, op);
    assert.ok(msg(op).length > 30, op);
  }
  assert.equal(sandbox.dropOpConfirmMessage("delete", {}), "", "unknown ops are not prompted here");
});

test("start / pause / resume confirmations say the end time is NOT changed", () => {
  assert.match(msg("start_now"), /end time stays KL\(E\)/);
  assert.match(msg("resume"), /end time stays KL\(E\)/);
  assert.match(msg("pause"), /schedule is not changed \(ends KL\(E\)\)/);
  assert.doesNotMatch(msg("start_now"), /24 hours/);
});

test("end confirmation reports claims and irreversibility", () => {
  const m = msg("end_now", { claimed: "123", total: "480" });
  assert.match(m, /123 of 480 codes have been claimed/);
  assert.match(m, /can't be restarted with the same end time/);
  assert.doesNotMatch(msg("end_now", { claimed: "" }), /claimed\./, "unknown claim count is not invented");
});

test("server errors are translated, never shown as raw codes", () => {
  for (const code of ["already_live", "end_time_passed", "end_in_past", "bad_end", "drop_ended", "not_paused", "drop_changed", "drop_creating", "drop_deleting", "not_found"]) {
    const m = sandbox.dropActionErrorMessage({ code });
    assert.doesNotMatch(m, /[a-z]+_[a-z_]+/, code + ": " + m);
  }
  assert.match(sandbox.dropActionErrorMessage({ code: "weird" }), /\(weird\)/);
  assert.match(sandbox.dropActionErrorMessage(null), /unknown/);
});

test("wiring: the click handler confirms every op and no longer special-cases only End", () => {
  assert.doesNotMatch(JS, /op === "end_now" && !confirm\("End this drop now\?"\)/);
  assert.match(JS, /var confirmText = dropOpConfirmMessage\(op,/);
  assert.match(JS, /if \(confirmText && !confirm\(confirmText\)\) return;/);
  assert.doesNotMatch(JS, /Drop action failed: " \+ \(d\.code \|\| "unknown"\)/);
});
