/**
 * Tests for the Community Centre "Retry failed post" modal content builder
 * (ccBuildRetryModalContent / ccFormatKlLong / CC_ERROR_GUIDANCE /
 * CC_RETRY_ERROR_MESSAGES) in static/admin-dashboard.js.
 *
 * These are pure string-building functions (no DOM), extracted as source
 * text and executed in a sandboxed vm context, same approach as
 * test_referral_entry_action.test.js.
 *
 * Run with: node --test test_community_retry_modal.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "  var CC_RETRY_ERROR_MESSAGES = {";
const END_MARKER = "  function ccOpenRetryModal(post) {";

function loadFunctionsSource() {
  const js = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");
  const start = js.indexOf(START_MARKER);
  const end = js.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "CC_RETRY_ERROR_MESSAGES not found in static/admin-dashboard.js");
  assert.ok(end !== -1, "ccOpenRetryModal end marker not found in static/admin-dashboard.js");
  return js.slice(start, end);
}

function esc(v) {
  return String(v === null || v === undefined ? "" : v)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function fmt(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") return v.toLocaleString();
  return String(v);
}

function makeContext() {
  const sandbox = { esc, fmt, console };
  vm.createContext(sandbox);
  const src = loadFunctionsSource() +
    "\nthis.__CC_ERROR_GUIDANCE = CC_ERROR_GUIDANCE;" +
    "\nthis.__CC_RETRY_ERROR_MESSAGES = CC_RETRY_ERROR_MESSAGES;" +
    "\nthis.__ccFormatKlLong = ccFormatKlLong;" +
    "\nthis.__ccBuildRetryModalContent = ccBuildRetryModalContent;";
  vm.runInContext(src, sandbox);
  return sandbox;
}

function basePost(overrides) {
  return Object.assign({
    _id: "post123",
    last_error_code: "telegram_forbidden",
    last_error_message: "Bot is not allowed to post in Official Channel.",
    retryable: false,
    attempt_count: 1,
    last_attempt_at_kl: "2026-07-23T04:03:00+08:00",
    latest_run_id: "67ab91cdaabbccddeeff0011",
    updated_at: "2026-07-23T04:03:00+08:00",
  }, overrides || {});
}

test("ccFormatKlLong renders KL wall-clock time without re-converting timezone", () => {
  const ctx = makeContext();
  assert.equal(ctx.__ccFormatKlLong("2026-07-23T04:03:00+08:00"), "23 Jul 2026, 4:03 AM");
  assert.equal(ctx.__ccFormatKlLong(""), "—");
  assert.equal(ctx.__ccFormatKlLong(null), "—");
});

test("retryable error shows enabled Confirm and queue-now copy", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({
    last_error_code: "network_timeout",
    last_error_message: "Network timeout contacting Telegram.",
    retryable: true,
  }));
  assert.equal(built.retryable, true);
  assert.match(built.html, /This will queue a new attempt now\./);
  assert.doesNotMatch(built.html, /disabled/);
});

test("non-retryable error disables Confirm and shows corrective action", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({ retryable: false }));
  assert.equal(built.retryable, false);
  assert.ok(built.guidance, "expected corrective-action guidance for telegram_forbidden");
  assert.match(built.html, /Fix Permissions/);
});

test("telegram_forbidden displays the exact sanitized backend message", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({
    last_error_code: "telegram_forbidden",
    last_error_message: "Bot is not allowed to post in Official Channel.",
    retryable: false,
  }));
  assert.match(built.html, /Bot is not allowed to post in Official Channel\./);
  assert.match(built.html, /telegram_forbidden/);
});

test("invalid_media_file_id displays media-specific corrective guidance", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({
    last_error_code: "invalid_media_file_id",
    last_error_message: "Saved media reference is no longer valid on Telegram.",
    retryable: false,
  }));
  assert.match(built.html, /Replace Media/);
  assert.match(built.html, /re-upload the media/i);
});

test("bot_loop_not_running displays worker-specific guidance, not Telegram blame", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({
    last_error_code: "bot_loop_not_running",
    last_error_message: "The publishing worker process is not currently running.",
    retryable: false,
  }));
  assert.match(built.html, /publishing worker process/);
  assert.match(built.html, /Duplicate Post/);
});

test("unknown_error does not claim a Telegram failure and cites the run reference", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({
    last_error_code: "unknown_error",
    last_error_message: "Internal publish error before a Telegram response was received.",
    retryable: false,
    latest_run_id: "67ab91cdaabbccddeeff0011",
  }));
  assert.match(built.html, /Internal publish error\. Check worker logs using reference: 67ab91cd\./);
  assert.doesNotMatch(built.html, /contacting Telegram/);
});

test("run_id (truncated reference) appears in the modal", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({ latest_run_id: "67ab91cdaabbccddeeff0011" }));
  assert.match(built.html, /Reference: 67ab91cd/);
});

test("frontend does not replace a known backend error with the old generic Telegram text", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({
    last_error_code: "invalid_poll",
    last_error_message: "Poll configuration was rejected by Telegram.",
    retryable: false,
  }));
  assert.doesNotMatch(built.html, /^.*Unexpected error contacting Telegram\..*$/m);
  assert.match(built.html, /Poll configuration was rejected by Telegram\./);
});

test("missing error fields fall back to the internal-error copy with run reference, not the old generic text", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({
    last_error_code: null,
    last_error_message: null,
    retryable: false,
    latest_run_id: "67ab91cdaabbccddeeff0011",
  }));
  assert.match(built.html, /Internal publish error\. Check worker logs using reference: 67ab91cd\./);
  assert.doesNotMatch(built.html, /Unexpected error contacting Telegram/);
});

test("local_type_error shows the real exception message, step and class instead of a generic fallback", () => {
  const ctx = makeContext();
  const built = ctx.__ccBuildRetryModalContent(basePost({
    last_error_code: "local_type_error",
    last_error_message: "Local error before Telegram was contacted: send_message() got an unexpected keyword argument 'style'",
    last_failed_step: "telegram_call",
    last_exception_class: "TypeError",
    retryable: false,
  }));
  assert.equal(built.retryable, false);
  assert.match(built.html, /unexpected keyword argument &#39;style&#39;/);
  assert.match(built.html, /Step: telegram_call/);
  assert.match(built.html, /Exception: TypeError/);
});
