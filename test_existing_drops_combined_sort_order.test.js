/**
 * Regression test for Existing Drops' combined ordering (adm_list() in
 * static/index.html, GET /miniapp, table #adm_table).
 *
 * Requirement: standard drops and Mission Pool rows are normalized into one
 * shared render model (id / source / start timestamp / original record),
 * combined into a single array, and sorted together by absolute start
 * timestamp — descending (latest start first), with canonical id ascending
 * as a tie-breaker, and missing/invalid timestamps sinking to the bottom.
 * Neither source may be sorted independently and then concatenated — that
 * is the bug this fixes (missions always rendered after all standard drops
 * regardless of scheduled start time).
 *
 * No build step, no jsdom in this repo — the relevant source is extracted as
 * text and executed in a sandboxed vm context, mirroring
 * test_miniapp_admin_panel_mission_pool_drops.test.js /
 * test_existing_drops_mission_status_filter.test.js.
 *
 * Run with: node --test test_existing_drops_combined_sort_order.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const INDEX_HTML = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

// adm_parseStartTimestamp / adm_normalizeDropRecord / adm_normalizeMissionRecord /
// adm_compareExistingDropsRows / adm_list.
const ADM_LIST_SRC = slice(
  INDEX_HTML,
  "    // Existing Drops shows two data sources merged into one authoritative",
  '\n    const admCreateBtn = document.getElementById("adm_create_btn");'
);

function featureSource() {
  return ADM_LIST_SRC;
}

// ---------------------------------------------------------------------
// Minimal DOM: just #adm_table tbody plus generic node/query support.
// ---------------------------------------------------------------------

function makeNode(tag) {
  const node = {
    tagName: String(tag || "div").toUpperCase(),
    className: "",
    attrs: {},
    _children: [],
    appendChild(child) {
      node._children.push(child);
      return child;
    },
  };
  Object.defineProperty(node, "children", { get: () => node._children });
  Object.defineProperty(node, "innerHTML", {
    get: () => node._html || "",
    set(v) {
      node._html = v;
      if (!v) node._children = [];
    },
  });
  return node;
}

function rowNames(tbody) {
  // Extract the visible name from each rendered row, in DOM order.
  return tbody.children.map((tr) => {
    const m = />([^<]+?)(?:\s*<span[^>]*>\[[^\]]*\]<\/span>)?<\/td>/.exec(tr.innerHTML);
    return m ? m[1].trim() : tr.innerHTML;
  });
}

function buildSandbox({ fetchImpl, userId = "12345" } = {}) {
  const tbody = makeNode("tbody");
  const document = {
    querySelector(sel) {
      if (sel === "#adm_table tbody") return tbody;
      return null;
    },
    createElement(tag) {
      return makeNode(tag);
    },
  };

  const consoleErrors = [];

  const sandbox = {
    document,
    console: { log() {}, warn() {}, error: (...a) => consoleErrors.push(a.join(" ")) },
    escapeHtml: (s = "") => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])),
    fmtKL: (iso) => (iso ? "KL:" + iso : ""),
    getAdminUserId: () => userId,
    buildAdminQueryTail: () => "?user_id=" + userId,
    API_V2: "/v2/miniapp",
    v2Fetch: async (url, init) => fetchImpl(url, init),
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);

  return { sandbox, tbody, consoleErrors };
}

function run(sandbox) {
  vm.runInContext(featureSource(), sandbox);
}

function drop(overrides) {
  return Object.assign(
    {
      dropId: "d1",
      name: "Drop",
      type: "pooled",
      status: "active",
      priority: 100,
      startsAt: "2026-09-01T00:00:00Z",
      endsAt: "2026-09-10T00:00:00Z",
      codesFree: 3,
      codesTotal: 10,
    },
    overrides || {}
  );
}

function mission(overrides) {
  return Object.assign(
    {
      campaign_id: "camp1",
      name: "Mission",
      state: "live",
      campaign_status: "active",
      admin_only: false,
      visibility_reason: null,
      starts_at: "2026-09-17T00:00:00Z",
      ends_at: "2026-09-20T00:00:00Z",
      submissions: 5,
      winners: 2,
    },
    overrides || {}
  );
}

function fetchWith({ drops = [], missions = [] } = {}) {
  return async (url) => {
    if (String(url).includes("/admin/drops_v2")) {
      return { ok: true, status: 200, json: async () => ({ status: "ok", items: drops }) };
    }
    if (String(url).includes("/mission-pool/campaigns")) {
      return {
        ok: true,
        status: 200,
        json: async () => ({ status: "ok", campaigns: missions }),
        text: async () => JSON.stringify({ status: "ok", campaigns: missions }),
      };
    }
    throw new Error("unexpected fetch: " + url);
  };
}

// ---------------------------------------------------------------------
// 1/2. Standard and mission rows interleave correctly, descending by start
// ---------------------------------------------------------------------

test("standard drops and mission rows interleave by start time, descending (latest first)", async () => {
  const drops = [
    drop({ dropId: "d_20", name: "20 Sep", startsAt: "2026-09-20T00:00:00Z" }),
    drop({ dropId: "d_19", name: "19 Sep", startsAt: "2026-09-19T00:00:00Z" }),
    drop({ dropId: "d_18", name: "18 Sep", startsAt: "2026-09-18T00:00:00Z" }),
    drop({ dropId: "d_15", name: "15 Sep", startsAt: "2026-09-15T00:00:00Z" }),
    drop({ dropId: "d_13", name: "13 Sep", startsAt: "2026-09-13T00:00:00Z" }),
  ];
  const missions = [
    mission({ campaign_id: "m_17", name: "Mission: 17 Sep", starts_at: "2026-09-17T00:00:00Z" }),
    mission({ campaign_id: "m_16", name: "Mission: 16 Sep", starts_at: "2026-09-16T00:00:00Z" }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ drops, missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.deepEqual(rowNames(tbody), [
    "20 Sep",
    "19 Sep",
    "18 Sep",
    "Mission: 17 Sep",
    "Mission: 16 Sep",
    "15 Sep",
    "13 Sep",
  ]);
});

// ---------------------------------------------------------------------
// 3. Same timestamp -> canonical id ascending tie-break
// ---------------------------------------------------------------------

test("rows sharing an identical start datetime break ties by ascending canonical id", async () => {
  const sameStart = "2026-09-17T00:00:00Z";
  const drops = [drop({ dropId: "b_drop", name: "B Drop", startsAt: sameStart })];
  const missions = [
    mission({ campaign_id: "a_mission", name: "A Mission", starts_at: sameStart }),
    mission({ campaign_id: "c_mission", name: "C Mission", starts_at: sameStart }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ drops, missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.deepEqual(rowNames(tbody), ["A Mission", "B Drop", "C Mission"]);
});

test("adm_compareExistingDropsRows sorts equal-timestamp rows by id ascending regardless of input order", async () => {
  const { sandbox } = buildSandbox({ fetchImpl: fetchWith({}) });
  run(sandbox);

  const rows = [
    { id: "z", startTs: 1000 },
    { id: "a", startTs: 1000 },
    { id: "m", startTs: 1000 },
  ];
  rows.sort(sandbox.adm_compareExistingDropsRows);
  assert.deepEqual(rows.map((r) => r.id), ["a", "m", "z"]);
});

// ---------------------------------------------------------------------
// 4. Missing/invalid timestamps sink to the bottom
// ---------------------------------------------------------------------

test("missing or invalid start timestamps render last, after all validly-dated rows", async () => {
  const drops = [
    drop({ dropId: "d_valid", name: "Valid Drop", startsAt: "2026-09-18T00:00:00Z" }),
    drop({ dropId: "d_missing", name: "Missing Start Drop", startsAt: "" }),
    drop({ dropId: "d_bad", name: "Bad Start Drop", startsAt: "not-a-date" }),
  ];
  const missions = [
    mission({ campaign_id: "m_valid", name: "Valid Mission", starts_at: "2026-09-20T00:00:00Z" }),
    mission({ campaign_id: "m_missing", name: "Missing Start Mission", starts_at: null }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ drops, missions }) });
  run(sandbox);
  await sandbox.adm_list();

  const names = rowNames(tbody);
  assert.deepEqual(names.slice(0, 2), ["Valid Mission", "Valid Drop"]);
  assert.deepEqual(new Set(names.slice(2)), new Set(["Missing Start Drop", "Bad Start Drop", "Missing Start Mission"]));
});

test("adm_parseStartTimestamp returns NaN for missing/invalid input and a finite epoch ms for valid ISO input", async () => {
  const { sandbox } = buildSandbox({ fetchImpl: fetchWith({}) });
  run(sandbox);

  assert.ok(Number.isNaN(sandbox.adm_parseStartTimestamp("")));
  assert.ok(Number.isNaN(sandbox.adm_parseStartTimestamp(null)));
  assert.ok(Number.isNaN(sandbox.adm_parseStartTimestamp("not-a-date")));
  assert.equal(sandbox.adm_parseStartTimestamp("2026-09-17T00:00:00Z"), Date.parse("2026-09-17T00:00:00Z"));
});

// ---------------------------------------------------------------------
// 5/6. Mission status filter still applies before/around sorting
// ---------------------------------------------------------------------

test("completed missions stay hidden even when their start time would otherwise place them near the top", async () => {
  const missions = [
    mission({ campaign_id: "m_done", name: "Completed Mission", state: "completed", starts_at: "2026-09-25T00:00:00Z" }),
    mission({ campaign_id: "m_live", name: "Live Mission", state: "live", starts_at: "2026-09-10T00:00:00Z" }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.deepEqual(rowNames(tbody), ["Live Mission"]);
});

test("draft/live/paused missions remain visible and take their sorted position among standard drops", async () => {
  const drops = [drop({ dropId: "d_mid", name: "Mid Drop", startsAt: "2026-09-15T00:00:00Z" })];
  const missions = [
    mission({ campaign_id: "m_draft", name: "Draft Mission", state: "draft", starts_at: "2026-09-20T00:00:00Z" }),
    mission({ campaign_id: "m_live", name: "Live Mission", state: "live", starts_at: "2026-09-14T00:00:00Z" }),
    mission({ campaign_id: "m_paused", name: "Paused Mission", state: "paused", starts_at: "2026-09-13T00:00:00Z" }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ drops, missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.deepEqual(rowNames(tbody), ["Draft Mission", "Mid Drop", "Live Mission", "Paused Mission"]);
});

// ---------------------------------------------------------------------
// 7. Refresh produces the same order, no duplicates
// ---------------------------------------------------------------------

test("calling adm_list() again produces the same sorted order with no duplicates", async () => {
  const drops = [
    drop({ dropId: "d_20", name: "20 Sep", startsAt: "2026-09-20T00:00:00Z" }),
    drop({ dropId: "d_15", name: "15 Sep", startsAt: "2026-09-15T00:00:00Z" }),
  ];
  const missions = [mission({ campaign_id: "m_17", name: "Mission: 17 Sep", starts_at: "2026-09-17T00:00:00Z" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ drops, missions }) });
  run(sandbox);

  await sandbox.adm_list();
  const first = rowNames(tbody);
  await sandbox.adm_list();
  const second = rowNames(tbody);

  assert.deepEqual(first, ["20 Sep", "Mission: 17 Sep", "15 Sep"]);
  assert.deepEqual(second, first);
  assert.equal(tbody.children.length, 3);
});

// ---------------------------------------------------------------------
// 8/9. Partial-fetch failures still leave the surviving source correctly
// ordered.
// ---------------------------------------------------------------------

test("if the mission fetch fails, standard drops remain correctly (descending) ordered", async () => {
  const drops = [
    drop({ dropId: "d_19", name: "19 Sep", startsAt: "2026-09-19T00:00:00Z" }),
    drop({ dropId: "d_20", name: "20 Sep", startsAt: "2026-09-20T00:00:00Z" }),
    drop({ dropId: "d_13", name: "13 Sep", startsAt: "2026-09-13T00:00:00Z" }),
  ];
  const { sandbox, tbody, consoleErrors } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/admin/drops_v2")) {
        return { ok: true, status: 200, json: async () => ({ status: "ok", items: drops }) };
      }
      if (String(url).includes("/mission-pool/campaigns")) {
        return { ok: false, status: 500, statusText: "Internal Server Error", text: async () => '{"status":"error"}' };
      }
      throw new Error("unexpected fetch: " + url);
    },
  });
  run(sandbox);
  await sandbox.adm_list();

  const names = rowNames(tbody);
  assert.deepEqual(names.slice(-3), ["20 Sep", "19 Sep", "13 Sep"]);
  assert.ok(consoleErrors.length, "mission failure must still be logged");
});

test("if the standard-drop fetch fails, missions remain correctly (descending) ordered", async () => {
  const missions = [
    mission({ campaign_id: "m_16", name: "Mission: 16 Sep", starts_at: "2026-09-16T00:00:00Z" }),
    mission({ campaign_id: "m_18", name: "Mission: 18 Sep", starts_at: "2026-09-18T00:00:00Z" }),
    mission({ campaign_id: "m_14", name: "Mission: 14 Sep", starts_at: "2026-09-14T00:00:00Z" }),
  ];
  const { sandbox, tbody } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/admin/drops_v2")) {
        return { ok: false, status: 500, statusText: "Internal Server Error" };
      }
      if (String(url).includes("/mission-pool/campaigns")) {
        return { ok: true, status: 200, json: async () => ({ status: "ok", campaigns: missions }), text: async () => JSON.stringify({ status: "ok", campaigns: missions }) };
      }
      throw new Error("unexpected fetch: " + url);
    },
  });
  run(sandbox);
  await sandbox.adm_list();

  const names = rowNames(tbody);
  assert.deepEqual(names.slice(-3), ["Mission: 18 Sep", "Mission: 16 Sep", "Mission: 14 Sep"]);
});
