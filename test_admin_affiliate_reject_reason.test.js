/**
 * P0 hotfix: rejecting an affiliate reward is irreversible, so Cancel on the
 * reason prompt must NEVER send a rejection, and a blank/too-short reason must
 * never reach the server.
 *
 * Root cause pinned here: the per-row handler did
 *     var reason = prompt("Reason for rejection (optional):") || "";
 * so prompt()'s null (Cancel) became "" and the irreversible POST still fired.
 * Bulk reject had the opposite gap: a blank reason silently became
 * "bulk_admin_reject".
 *
 * Same harness as the other admin-dashboard tests: no build step, no jsdom —
 * the relevant source range is extracted as text and run in a vm sandbox
 * against small stand-ins, plus source-level wiring assertions.
 *
 * Run with: node --test test_admin_affiliate_reject_reason.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

const CODE = slice(JS, "var AFFP_REJECT_REASON_MIN", "  function loadAffiliatePending");

async function flush(n) { for (let i = 0; i < (n || 20); i++) await Promise.resolve(); }

// answers: queue of prompt() results, consumed in order (null = Cancel).
function harness({ answers = [], confirmAnswer = true, response = { status: "ok" }, activeStatus = "PENDING_REVIEW", ids = ["a", "b"] } = {}) {
  const calls = { prompts: [], fetches: [], banners: [], toasts: [], reloads: 0, confirms: [], bulk: [] };
  const sandbox = {
    console, Promise, JSON, String, encodeURIComponent,
    prompt: (text, def) => { calls.prompts.push({ text, def }); return answers.length ? answers.shift() : null; },
    confirm: (msg) => { calls.confirms.push(msg); return confirmAnswer; },
    fetch: (url, opts) => { calls.fetches.push({ url, opts, body: opts && opts.body ? JSON.parse(opts.body) : null }); return Promise.resolve({ json: () => Promise.resolve(response) }); },
    banner: (m, k) => calls.banners.push({ m, k }),
    toast: (m, k) => calls.toasts.push({ m, k }),
    esc: (v) => String(v),
    loadAffiliatePending: () => { calls.reloads++; },
    $: () => ({ dataset: { status: activeStatus } }),
    affpLoaded: { status: "PENDING_REVIEW", ids, submitting: false, seq: 0 },
    affpSetBulkBusy: () => {},
    btnStart: () => true, btnStop: () => {},
    apiPostJson: (url, body) => { calls.bulk.push({ url, body: JSON.parse(JSON.stringify(body)) }); return Promise.resolve({ ok: true, status: 200, d: { status: "ok", rejected: ids.length } }); },
  };
  vm.createContext(sandbox);
  vm.runInContext(CODE, sandbox);
  return { sandbox, calls };
}

const btn = () => ({ disabled: false });

test("single reject: Cancel (prompt -> null) sends NO request and shows nothing", async () => {
  const { sandbox, calls } = harness({ answers: [null] });
  const b = btn();
  await sandbox.affpRejectOne("L1", b);
  await flush();
  assert.equal(calls.fetches.length, 0, "Cancel must never reach the server");
  assert.equal(calls.reloads, 0);
  assert.equal(b.disabled, false, "button stays usable after Cancel");
});

test("single reject: blank, whitespace-only and too-short reasons send NO request and explain why", async () => {
  for (const bad of ["", "   ", "\n\t", "ab", "  a "]) {
    const { sandbox, calls } = harness({ answers: [bad] });
    await sandbox.affpRejectOne("L1", btn());
    await flush();
    assert.equal(calls.fetches.length, 0, JSON.stringify(bad));
    assert.equal(calls.banners.length, 1, JSON.stringify(bad));
    assert.match(calls.banners[0].m, /reason/i);
    assert.match(calls.banners[0].m, /Nothing was rejected/);
  }
});

test("single reject: a valid reason sends exactly one trimmed request", async () => {
  const { sandbox, calls } = harness({ answers: ["  Suspected fake referrals  "] });
  const b = btn();
  await sandbox.affpRejectOne("L 1", b);
  await flush();
  assert.equal(calls.fetches.length, 1);
  assert.equal(calls.fetches[0].url, "/v2/miniapp/admin/affiliate/L%201/reject");
  assert.equal(calls.fetches[0].opts.method, "POST");
  assert.deepEqual(calls.fetches[0].body, { reason: "Suspected fake referrals" });
  assert.equal(calls.reloads, 1);
  assert.equal(b.disabled, false);
});

test("single reject: reason is capped at 200 characters before sending", async () => {
  const { sandbox, calls } = harness({ answers: ["x".repeat(500)] });
  await sandbox.affpRejectOne("L1", btn());
  await flush();
  assert.equal(calls.fetches[0].body.reason.length, 200);
});

test("single reject: the 200-character cap never splits an emoji into a lone surrogate", async () => {
  // 199 ASCII chars + an emoji: slice(0, 200) would keep only the first UTF-16 half.
  const { sandbox, calls } = harness({ answers: ["a".repeat(199) + "😀😀"] });
  await sandbox.affpRejectOne("L1", btn());
  await flush();
  const sent = calls.fetches[0].body.reason;
  assert.equal(Array.from(sent).length, 200);
  assert.ok(sent.endsWith("😀"), "whole emoji kept");
  assert.equal(sent.isWellFormed(), true, "no lone surrogate");
  assert.equal(JSON.stringify({ reason: sent }).includes("\\ud83d\""), false);
});

test("single reject: server refusal is shown, not swallowed", async () => {
  const { sandbox, calls } = harness({ answers: ["real reason"], response: { status: "error", reason: "reason_required" } });
  await sandbox.affpRejectOne("L1", btn());
  await flush();
  assert.equal(calls.reloads, 0);
  assert.match(calls.banners[0].m, /Reject failed/);
});

test("single reject: the prompt is labelled required and has no prefilled fallback", async () => {
  const { sandbox, calls } = harness({ answers: [null] });
  await sandbox.affpRejectOne("L1", btn());
  assert.match(calls.prompts[0].text, /required/i);
  assert.equal(calls.prompts[0].def, "");
});

test("bulk reject: Cancel at the reason prompt sends nothing", async () => {
  const { sandbox, calls } = harness({ answers: ["REJECT 2", null] });
  sandbox.affpRejectAll(btn());
  await flush();
  assert.equal(calls.bulk.length, 0);
  assert.equal(calls.banners.length, 0);
});

test("bulk reject: a blanked reason is refused instead of silently becoming the default", async () => {
  for (const bad of ["", "   ", "ab"]) {
    const { sandbox, calls } = harness({ answers: ["REJECT 2", bad] });
    sandbox.affpRejectAll(btn());
    await flush();
    assert.equal(calls.bulk.length, 0, JSON.stringify(bad));
    assert.match(calls.banners[0].m, /Nothing was rejected/);
  }
});

test("bulk reject: the prefilled default reason is accepted when the admin keeps it", async () => {
  const { sandbox, calls } = harness({ answers: ["REJECT 2", "bulk_admin_reject"] });
  sandbox.affpRejectAll(btn());
  await flush();
  assert.equal(calls.bulk.length, 1);
  assert.equal(calls.bulk[0].url, "/v2/miniapp/admin/affiliate/reject-bulk");
  assert.deepEqual(calls.bulk[0].body, { ledger_ids: ["a", "b"], reason: "bulk_admin_reject" });
});

test("bulk reject: earlier confirmations still gate the request", async () => {
  let h = harness({ confirmAnswer: false, answers: ["REJECT 2", "reason here"] });
  h.sandbox.affpRejectAll(btn());
  await flush();
  assert.equal(h.calls.bulk.length, 0);
  h = harness({ answers: ["REJECT 9", "reason here"] }); // wrong typed phrase
  h.sandbox.affpRejectAll(btn());
  await flush();
  assert.equal(h.calls.bulk.length, 0);
  assert.match(h.calls.banners[0].m, /did not match/);
});

test("wiring: the row click handler delegates to affpRejectOne and no longer maps Cancel to an empty reason", () => {
  assert.match(JS, /op === "reject"\) \{\s*affpRejectOne\(ledgerId, btn\);/);
  assert.doesNotMatch(JS, /prompt\("Reason for rejection \(optional\):"\)\s*\|\|\s*""/);
  assert.doesNotMatch(JS, /String\(reason\)\.trim\(\)\s*\|\|\s*"bulk_admin_reject"/);
  // the only /reject POST for an affiliate ledger goes through affpRejectOne
  const rejectPosts = JS.match(/\/v2\/miniapp\/admin\/affiliate\/" \+ encodeURIComponent\(ledgerId\) \+ "\/reject"/g) || [];
  assert.equal(rejectPosts.length, 1);
});
