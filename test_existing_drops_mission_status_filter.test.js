/**
 * Regression test for the "Existing Drops" table's Mission Pool status
 * filter (adm_list() in static/index.html, served at GET /miniapp, rendered
 * into <table id="adm_table">).
 *
 * Requirement: this table is a current/actionable-campaigns view. Mission
 * Pool rows must render only when normalized status is upcoming/scheduled,
 * live/active, or paused — and must be hidden when completed, ended,
 * closed, archived, cancelled, or deleted. This is display filtering only:
 * adm_fetchMissionCampaigns() still fetches and returns *all* campaigns
 * (including completed ones) from /api/admin/mission-pool/campaigns, so
 * Campaign Centre, audit logs, and the admin API are unaffected — only this
 * table's render pass drops the finished rows.
 *
 * No build step, no jsdom in this repo — the relevant source is extracted
 * as text and executed in a sandboxed vm context, mirroring
 * test_miniapp_admin_panel_mission_pool_drops.test.js.
 *
 * Run with: node --test test_existing_drops_mission_status_filter.test.js
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

// adm_fetchMissionCampaigns / adm_missionVisibleInExistingDrops / adm_list.
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

function allText(node, out) {
  out = out || [];
  if (node._html) out.push(node._html);
  (node._children || []).forEach((c) => allText(c, out));
  return out;
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

  const fetchLog = [];
  const consoleErrors = [];

  const sandbox = {
    document,
    console: { log() {}, warn() {}, error: (...a) => consoleErrors.push(a.join(" ")) },
    escapeHtml: (s = "") => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])),
    fmtKL: (iso) => (iso ? "KL:" + iso : ""),
    getAdminUserId: () => userId,
    buildAdminQueryTail: () => "?user_id=" + userId,
    API_V2: "/v2/miniapp",
    v2Fetch: async (url, init) => {
      fetchLog.push({ url, init });
      return fetchImpl(url, init);
    },
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);

  return { sandbox, tbody, fetchLog, consoleErrors };
}

function run(sandbox) {
  vm.runInContext(featureSource(), sandbox);
}

function drop(overrides) {
  return Object.assign(
    {
      dropId: "d1",
      name: "Standard Pooled Drop",
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
      name: "Mission Campaign",
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
// 1-3. Live / upcoming / paused missions appear
// ---------------------------------------------------------------------

test("a live mission (state: 'live') appears in Existing Drops", async () => {
  const missions = [mission({ campaign_id: "c_live", name: "Live Mission", state: "live" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 1);
  assert.match(allText(tbody).join("\n"), /Live Mission/);
});

test("an active mission (campaign_status: 'active', no state) appears", async () => {
  const missions = [mission({ campaign_id: "c_active", name: "Active Mission", state: null, campaign_status: "active" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 1);
  assert.match(allText(tbody).join("\n"), /Active Mission/);
});

test("an upcoming/scheduled mission appears", async () => {
  const missions = [
    mission({ campaign_id: "c_upcoming", name: "Upcoming Mission", state: "upcoming" }),
    mission({ campaign_id: "c_scheduled", name: "Scheduled Mission", state: "scheduled" }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 2);
  const html = allText(tbody).join("\n");
  assert.match(html, /Upcoming Mission/);
  assert.match(html, /Scheduled Mission/);
});

test("a paused mission appears", async () => {
  const missions = [mission({ campaign_id: "c_paused", name: "Paused Mission", state: "paused" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 1);
  assert.match(allText(tbody).join("\n"), /Paused Mission/);
});

// Codex review follow-up: every campaign starts life with status="draft"
// (campaign_centre.create_campaign), and operational_state() in
// mission_pool_ux.py surfaces that unchanged as state="draft" until it's
// scheduled/launched. A brand-new, unpublished campaign is unfinished and
// actionable — it must not disappear from Existing Drops just because it
// hasn't been scheduled yet.
test("a draft (newly created, unpublished) mission appears", async () => {
  const missions = [mission({ campaign_id: "c_draft", name: "Draft Mission", state: "draft" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 1);
  assert.match(allText(tbody).join("\n"), /Draft Mission/);
});

// ---------------------------------------------------------------------
// 4. Completed mission is hidden
// ---------------------------------------------------------------------

test("a completed mission is hidden from Existing Drops", async () => {
  const missions = [mission({ campaign_id: "c_done", name: "Completed Mission", state: "completed" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 0);
  assert.doesNotMatch(allText(tbody).join("\n"), /Completed Mission/);
});

// ---------------------------------------------------------------------
// 5. Ended / closed / archived / cancelled / deleted are all hidden
// ---------------------------------------------------------------------

test("ended, closed, archived, cancelled, and deleted missions are all hidden, case/whitespace-insensitively", async () => {
  const hiddenStatuses = ["ended", "closed", "archived", "cancelled", "deleted", "  ENDED  ", "Closed"];
  const missions = hiddenStatuses.map((status, i) =>
    mission({ campaign_id: "c_hidden_" + i, name: "Hidden Mission " + i, state: status })
  );
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 0, "no hidden-status mission should render, regardless of case/whitespace");
});

test("campaign_status carrying a hidden value (e.g. 'closed') is also hidden when state is absent", async () => {
  const missions = [mission({ campaign_id: "c_hidden_cs", name: "Hidden Via CampaignStatus", state: null, campaign_status: "closed" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 0);
});

// ---------------------------------------------------------------------
// 6. Standard pooled drops remain unchanged (not status-filtered)
// ---------------------------------------------------------------------

test("standard pooled drops render regardless of status, including 'completed' — this filter is mission-only", async () => {
  const drops = [drop({ dropId: "d_completed", name: "Completed Standard Drop", status: "completed" })];
  const missions = [mission({ campaign_id: "c_done", name: "Completed Mission", state: "completed" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ drops, missions }) });
  run(sandbox);
  await sandbox.adm_list();

  assert.equal(tbody.children.length, 1, "the completed standard drop must still render");
  const html = allText(tbody).join("\n");
  assert.match(html, /Completed Standard Drop/);
  assert.doesNotMatch(html, /Completed Mission/, "the completed mission must still be filtered out");
});

// ---------------------------------------------------------------------
// 7. A refresh does not restore completed rows
// ---------------------------------------------------------------------

test("calling adm_list() again does not restore a previously hidden completed mission", async () => {
  const missions = [
    mission({ campaign_id: "c_live", name: "Live Mission", state: "live" }),
    mission({ campaign_id: "c_done", name: "Completed Mission", state: "completed" }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);

  await sandbox.adm_list();
  assert.equal(tbody.children.length, 1);

  await sandbox.adm_list(); // refresh
  assert.equal(tbody.children.length, 1, "refresh must not accumulate or restore the hidden completed row");
  const html = allText(tbody).join("\n");
  assert.match(html, /Live Mission/);
  assert.doesNotMatch(html, /Completed Mission/);
});

// ---------------------------------------------------------------------
// 8. Completed campaigns remain returned by the admin API — only hidden
// at this table's presentation layer.
// ---------------------------------------------------------------------

test("adm_fetchMissionCampaigns still returns completed campaigns from the API — only adm_list's render hides them", async () => {
  const missions = [
    mission({ campaign_id: "c_live", name: "Live Mission", state: "live" }),
    mission({ campaign_id: "c_done", name: "Completed Mission", state: "completed" }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: fetchWith({ missions }) });
  run(sandbox);

  // Direct call to the fetch function (as Campaign Centre / audit views would use) —
  // must return ALL campaigns, completed included.
  const apiResult = await sandbox.adm_fetchMissionCampaigns("?user_id=12345");
  assert.equal(apiResult.length, 2, "the admin API layer must not itself filter by status");
  assert.ok(apiResult.some((c) => c.campaign_id === "c_done"), "completed campaign must still be present in the API response");

  // But the Existing Drops table render must still hide it.
  await sandbox.adm_list();
  assert.equal(tbody.children.length, 1);
  const html = allText(tbody).join("\n");
  assert.match(html, /Live Mission/);
  assert.doesNotMatch(html, /Completed Mission/, "presentation layer must hide it even though the API returned it");
});
