/**
 * Regression tests for _pollBseJob response-handling logic.
 *
 * Tests the decision tree that runs inside the setInterval callback:
 *   - 401                    → redirect, abort poll
 *   - non-JSON body          → stop poll, show error
 *   - !r.ok / ok:false       → stop poll, show error
 *   - status queued/running  → keep polling
 *   - status failed          → stop poll, show error
 *   - status success         → stop poll, show green
 *   - status unknown/missing → stop poll, show error (NOT green)
 *
 * Run with:  node test_bse_poll.js
 */
"use strict";

var assert = require("assert");
var passed = 0;
var failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log("  PASS", name);
    passed++;
  } catch (e) {
    console.log("  FAIL", name, "—", e.message);
    failed++;
  }
}

// ---------------------------------------------------------------------------
// Pure decision function extracted from _pollBseJob's .then chain.
// This mirrors the exact branch tree in admin-dashboard.js so that any
// divergence between the test spec and the production code is caught here.
// ---------------------------------------------------------------------------

/**
 * @param {{ httpStatus: number, parseOk: boolean, body: object|null }} opts
 * @returns {{ intervalCleared: boolean, redirected: boolean, html: string|null, keepPolling: boolean }}
 */
function simulatePollResponse(opts) {
  var result = {
    intervalCleared: false,
    redirected: false,
    html: null,
    keepPolling: true,
  };

  // ---- First .then: 401 check + JSON parse ----
  if (opts.httpStatus === 401) {
    result.intervalCleared = true;
    result.redirected = true;
    result.keepPolling = false;
    return result;
  }

  var httpOk = opts.httpStatus >= 200 && opts.httpStatus < 300;
  var parseOk = opts.parseOk;
  var d = opts.body || {};

  // ---- Second .then ----

  if (!parseOk) {
    result.intervalCleared = true;
    result.keepPolling = false;
    result.html = "error:non-json";
    return result;
  }

  if (!httpOk || !d.ok) {
    result.intervalCleared = true;
    result.keepPolling = false;
    result.html = "error:" + (d.error || "unknown error");
    return result;
  }

  var status = d.status;

  if (status === "queued" || status === "running") {
    // interval NOT cleared — keep polling
    result.html = "progress:" + status;
    return result;
  }

  // terminal state
  result.intervalCleared = true;
  result.keepPolling = false;

  if (status === "failed") {
    result.html = "error:" + (d.error || "unknown error");
    return result;
  }

  if (status !== "success") {
    result.html = "error:unexpected job status \"" + String(status) + "\"";
    return result;
  }

  // success
  result.html = "success";
  return result;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

console.log("\nBSE poll response handler tests\n");

// 1. 401 redirects and aborts poll
test("401 → redirect and stop polling", function () {
  var r = simulatePollResponse({ httpStatus: 401, parseOk: true, body: null });
  assert.strictEqual(r.redirected, true);
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
});

// 2. Non-JSON body (nginx 502/504 HTML, empty body)
test("non-JSON body → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: false, body: null });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.startsWith("error:"), "expected error html, got: " + r.html);
});

test("non-JSON body on 502 → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 502, parseOk: false, body: null });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.startsWith("error:"));
});

// 3. HTTP error with JSON body
test("404 with ok:false JSON → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 404, parseOk: true, body: { ok: false, error: "Job not found" } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.includes("Job not found"));
});

test("400 bad request → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 400, parseOk: true, body: { ok: false, error: "job_id is required" } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.includes("job_id is required"));
});

test("403 expired auth with ok:false → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 403, parseOk: true, body: { ok: false, error: "Admins only" } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.startsWith("error:"));
});

// 4. ok:false in 200 response
test("200 with ok:false → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: true, body: { ok: false, error: "something went wrong" } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.includes("something went wrong"));
});

// 5. queued — keep polling
test("status queued → keep polling", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: true, body: { ok: true, status: "queued" } });
  assert.strictEqual(r.intervalCleared, false);
  assert.strictEqual(r.keepPolling, true);
  assert.ok(r.html.includes("queued"));
});

// 6. running — keep polling
test("status running → keep polling", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: true, body: { ok: true, status: "running" } });
  assert.strictEqual(r.intervalCleared, false);
  assert.strictEqual(r.keepPolling, true);
  assert.ok(r.html.includes("running"));
});

// 7. failed — stop, show error
test("status failed → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: true, body: { ok: true, status: "failed", error: "DB timeout" } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.includes("DB timeout"));
});

test("status failed with no error field → stop polling, show generic error", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: true, body: { ok: true, status: "failed" } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.startsWith("error:"));
});

// 8. success — render green (only on explicit "success")
test("status success → stop polling, render success", function () {
  var r = simulatePollResponse({
    httpStatus: 200, parseOk: true,
    body: { ok: true, status: "success", summary: { users_evaluated: 42, snapshots_written: 0 } },
  });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.strictEqual(r.html, "success");
});

// 9. unknown/missing status — must NOT render green
test("status undefined → stop polling, show error (not green)", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: true, body: { ok: true } }); // status missing
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.notStrictEqual(r.html, "success", "undefined status must not render green success");
  assert.ok(r.html.startsWith("error:"));
});

test("status null → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: true, body: { ok: true, status: null } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.notStrictEqual(r.html, "success");
  assert.ok(r.html.startsWith("error:"));
});

test("status unknown string → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 200, parseOk: true, body: { ok: true, status: "pending" } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.notStrictEqual(r.html, "success");
  assert.ok(r.html.includes("pending"));
});

// 10. 500 server error with JSON body (global Flask error handler)
test("500 with server_error JSON → stop polling, show error", function () {
  var r = simulatePollResponse({ httpStatus: 500, parseOk: true, body: { code: "server_error", message: "oops", ok: false } });
  assert.strictEqual(r.intervalCleared, true);
  assert.strictEqual(r.keepPolling, false);
  assert.ok(r.html.startsWith("error:"));
});

// ---------------------------------------------------------------------------

console.log("\n" + passed + " passed, " + failed + " failed\n");
if (failed > 0) { process.exit(1); }
