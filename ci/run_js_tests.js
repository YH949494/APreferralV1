#!/usr/bin/env node
/**
 * Run every frontend JavaScript test file and fail on any failure.
 *
 * The frontend has no build step and no package.json: each test file extracts
 * inline <script> source from static/*.html and runs it in a node:vm sandbox
 * with stubbed DOM/fetch/Telegram globals. Files are plain `node --test`
 * modules (plus test_bse_poll.js, a self-reporting script that exits 1 on
 * failure, which node --test runs as a single test).
 *
 * Each file runs in its own process so global state, timers and memory
 * cannot leak between files, with a per-file timeout and heap cap so a
 * runaway retry loop (the rejoin-buffer storm fixed in #551 OOM'd the old
 * run) fails fast instead of hanging CI.
 *
 * There is no allowlist of known failures: any failing, cancelled or
 * timed-out file, or a file that ran zero tests, fails the run.
 *
 * Usage:
 *   node ci/run_js_tests.js                 # discovery order
 *   node ci/run_js_tests.js --shuffle       # random file order (seed printed)
 *   node ci/run_js_tests.js --seed 1234     # reproducible shuffled order
 */
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const ROOT = path.resolve(__dirname, "..");
const PER_FILE_TIMEOUT_MS = 120_000;
const MAX_OLD_SPACE_MB = 512;

// These four held every pre-existing JS failure on main before this gate
// existed; the gate must keep running them.
const REQUIRED_FILES = [
  "test_creator_share_ui.js",
  "test_live_drop_channel_gate_ui.test.js",
  "test_live_drop_voucher_ui.test.js",
  "test_affiliate_my_stats_frontend.test.js",
];

function discover() {
  return fs
    .readdirSync(ROOT)
    .filter((f) => /^test_.*\.js$/.test(f) || /\.test\.js$/.test(f))
    .filter((f) => fs.statSync(path.join(ROOT, f)).isFile())
    .sort();
}

// mulberry32: small seeded PRNG so a shuffled order can be reproduced.
function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffle(list, seed) {
  const rand = rng(seed);
  const out = list.slice();
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

function parseArgs(argv) {
  const opts = { seed: null };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--shuffle") opts.seed = opts.seed ?? Math.floor(Math.random() * 2 ** 31);
    else if (argv[i] === "--seed") opts.seed = Number.parseInt(argv[++i], 10);
    else throw new Error(`unknown argument: ${argv[i]}`);
  }
  if (opts.seed !== null && !Number.isFinite(opts.seed)) throw new Error("--seed needs an integer");
  return opts;
}

function count(output, key) {
  const m = output.match(new RegExp(`^# ${key} (\\d+)$`, "m"));
  return m ? Number(m[1]) : null;
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  let files = discover();

  const missing = REQUIRED_FILES.filter((f) => !files.includes(f));
  if (missing.length) {
    console.error(`::error::required JS test files not found: ${missing.join(", ")}`);
    process.exit(1);
  }
  if (opts.seed !== null) {
    files = shuffle(files, opts.seed);
    console.log(`file order shuffled with --seed ${opts.seed}`);
  }

  const totals = { files: 0, tests: 0, pass: 0, fail: 0, cancelled: 0, skipped: 0, todo: 0 };
  const broken = [];
  const started = Date.now();

  for (const file of files) {
    const t0 = Date.now();
    const res = spawnSync(
      process.execPath,
      [`--max-old-space-size=${MAX_OLD_SPACE_MB}`, "--test", "--test-reporter=tap", file],
      { cwd: ROOT, encoding: "utf8", timeout: PER_FILE_TIMEOUT_MS, maxBuffer: 64 * 1024 * 1024 }
    );
    const out = `${res.stdout || ""}${res.stderr || ""}`;
    const c = {};
    for (const key of ["tests", "pass", "fail", "cancelled", "skipped", "todo"]) c[key] = count(out, key);
    totals.files += 1;
    for (const key of Object.keys(c)) totals[key] += c[key] || 0;

    let problem = null;
    if (res.error && res.error.code === "ETIMEDOUT") problem = `timed out after ${PER_FILE_TIMEOUT_MS}ms`;
    else if (res.error) problem = `could not run: ${res.error.message}`;
    else if (res.status !== 0) problem = `exit ${res.status}${res.signal ? ` (${res.signal})` : ""}`;
    else if (!c.tests) problem = "ran zero tests";
    else if (c.fail || c.cancelled) problem = `${c.fail} failed, ${c.cancelled} cancelled`;

    const ms = Date.now() - t0;
    console.log(`${problem ? "FAIL" : "ok  "} ${file}  pass=${c.pass ?? "?"} fail=${c.fail ?? "?"} skip=${c.skipped ?? "?"}  ${ms}ms`);
    if (problem) {
      broken.push(file);
      console.log(`---- ${file}: ${problem}`);
      console.log(out.split("\n").filter((l) => !/^# (Subtest|tests|suites|pass|fail|cancelled|skipped|todo|duration_ms)/.test(l)).join("\n"));
      console.log(`---- end ${file}`);
    }
  }

  const secs = ((Date.now() - started) / 1000).toFixed(1);
  console.log(
    `\n${totals.files} files, ${totals.tests} tests: ${totals.pass} passed, ${totals.fail} failed, ` +
      `${totals.cancelled} cancelled, ${totals.skipped} skipped, ${totals.todo} todo (${secs}s)`
  );
  if (broken.length) {
    console.error(`::error::JavaScript tests failed in: ${broken.join(", ")}`);
    process.exit(1);
  }
}

main();
