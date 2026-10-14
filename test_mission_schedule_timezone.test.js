/**
 * Mission Reward Pool admin: schedule conversion must be Asia/Kuala_Lumpur
 * (GMT+8) regardless of the admin's browser timezone.
 *
 * Production bug: static/mission-admin.js used the browser's local zone
 * (new Date(value) / getHours()), and the API returned offset-less
 * timestamps that browsers read as local time, so a GMT+8 admin saw stored
 * 14:00Z as 14:00 instead of 22:00. The module is exercised in child
 * processes with TZ set, since a Node process fixes its zone at start-up.
 *
 * Run with: node --test test_mission_schedule_timezone.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const MODULE_PATH = path.join(__dirname, "static", "mission-admin.js");
const ZONES = ["Asia/Kuala_Lumpur", "UTC", "America/New_York", "Pacific/Auckland"];

const PROBE = `
const C = require(process.argv[1]).core;
const R = {};
R.tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
R.inputFromUtc = C.isoToLocalInput("2026-10-14T14:00:00+00:00");
R.inputFromZ = C.isoToLocalInput("2026-10-16T02:00:00Z");
R.inputFromNaiveLegacy = C.isoToLocalInput("2026-10-14T14:00:00");
R.isoFromInput = C.localInputToIso("2026-10-14T22:00:00");
R.isoFromInputNoSeconds = C.localInputToIso("2026-10-14T22:00");
R.display = C.formatKl("2026-10-14T14:00:00+00:00");
R.displayNaive = C.formatKl("2026-10-14T14:00:00");
R.crossMidnight = [C.isoToLocalInput("2026-10-14T16:30:00Z"), C.localInputToIso("2026-10-15T00:30:00")];
R.crossMonth = [C.isoToLocalInput("2026-10-31T17:00:00Z"), C.localInputToIso("2026-11-01T01:00:00")];
R.crossYear = [C.isoToLocalInput("2026-12-31T16:30:00Z"), C.localInputToIso("2027-01-01T00:30:00")];
R.seconds = [C.isoToLocalInput("2026-09-30T12:34:37Z"), C.localInputToIso("2026-09-30T20:34:37")];
// Round trip: stored -> edit form -> save untouched / save re-typed
const f = C.scheduleFieldFrom("2026-10-14T14:00:00+00:00");
R.untouchedSave = C.scheduleValueForSave(f, f.display);
R.retypedSave = C.scheduleValueForSave(f, f.display.replace("22:00:00", "22:00:00"));
R.legacyNaiveUntouched = (function () {
  const g = C.scheduleFieldFrom("2026-10-14T14:00:00");
  return [g.display, C.scheduleValueForSave(g, g.display)];
})();
R.invalid = [C.localInputToIso(""), C.localInputToIso("garbage"), C.localInputToIso("2026-02-31T10:00:00"),
  C.isoToLocalInput(""), C.isoToLocalInput("nope"), C.formatKl(""), C.formatKl("nope")];
R.explicitZone = C.localInputToIso("2026-10-14T14:00:00Z");
console.log(JSON.stringify(R));
`;

function probe(tz) {
  const r = spawnSync(process.execPath, ["-e", PROBE, MODULE_PATH], {
    env: Object.assign({}, process.env, { TZ: tz }), encoding: "utf8",
  });
  assert.equal(r.status, 0, r.stderr);
  return JSON.parse(r.stdout);
}

const results = {};
ZONES.forEach((z) => { results[z] = probe(z); });

for (const zone of ZONES) {
  test(`[browser TZ ${zone}] GMT+8 input <-> UTC round trip`, () => {
    const r = results[zone];
    assert.equal(r.tz, zone === "UTC" ? "UTC" : zone, "probe really ran in that zone");

    // stored 2026-10-14T14:00Z -> edit form shows 22:00 (not 14:00)
    assert.equal(r.inputFromUtc, "2026-10-14T22:00:00");
    assert.equal(r.inputFromZ, "2026-10-16T10:00:00");
    // typed 22:00 GMT+8 -> exactly 14:00Z
    assert.equal(r.isoFromInput, "2026-10-14T14:00:00.000Z");
    assert.equal(r.isoFromInputNoSeconds, "2026-10-14T14:00:00.000Z");
    // display
    assert.equal(r.display, "2026-10-14 22:00:00 GMT+8");
  });

  test(`[browser TZ ${zone}] offset-less (legacy/naive) server strings are UTC, never browser-local`, () => {
    const r = results[zone];
    assert.equal(r.inputFromNaiveLegacy, "2026-10-14T22:00:00");
    assert.equal(r.displayNaive, "2026-10-14 22:00:00 GMT+8");
    // untouched legacy field resends the server's own string verbatim
    assert.deepEqual(r.legacyNaiveUntouched, ["2026-10-14T22:00:00", "2026-10-14T14:00:00"]);
  });

  test(`[browser TZ ${zone}] cross-midnight / month / year / seconds`, () => {
    const r = results[zone];
    assert.deepEqual(r.crossMidnight, ["2026-10-15T00:30:00", "2026-10-14T16:30:00.000Z"]);
    assert.deepEqual(r.crossMonth, ["2026-11-01T01:00:00", "2026-10-31T17:00:00.000Z"]);
    assert.deepEqual(r.crossYear, ["2027-01-01T00:30:00", "2026-12-31T16:30:00.000Z"]);
    assert.deepEqual(r.seconds, ["2026-09-30T20:34:37", "2026-09-30T12:34:37.000Z"]);
  });

  test(`[browser TZ ${zone}] repeated saves without edits do not drift`, () => {
    const r = results[zone];
    assert.equal(r.untouchedSave, "2026-10-14T14:00:00+00:00");
    assert.equal(r.retypedSave, "2026-10-14T14:00:00+00:00");
  });

  test(`[browser TZ ${zone}] invalid or missing input yields null/empty, not a shifted date`, () => {
    const [a, b, c, d, e, f, g] = results[zone].invalid;
    assert.equal(a, null);
    assert.equal(b, null);
    assert.equal(c, null, "2026-02-31 must not roll into March");
    assert.equal(d, "");
    assert.equal(e, "");
    assert.equal(f, "—");
    assert.equal(g, "—");
    assert.equal(results[zone].explicitZone, "2026-10-14T14:00:00.000Z");
  });
}

test("every browser timezone produces byte-identical results", () => {
  const base = Object.assign({}, results["Asia/Kuala_Lumpur"]);
  delete base.tz;
  for (const zone of ZONES) {
    const r = Object.assign({}, results[zone]);
    delete r.tz;
    assert.deepEqual(r, base, zone);
  }
});

test("mission-admin.js has no browser-timezone-dependent date handling left", () => {
  const src = require("node:fs").readFileSync(MODULE_PATH, "utf8")
    // strip comments so prose cannot satisfy or break the checks
    .replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
  assert.equal(/Date\([^)]*\)\.toLocale/.test(src), false, "display must go through formatKl");
  assert.equal(/\.get(FullYear|Month|Date|Hours|Minutes|Seconds|TimezoneOffset)\(/.test(src), false);
  assert.equal(/\.set(Hours|Date|Month|FullYear)\(/.test(src), false);
});
