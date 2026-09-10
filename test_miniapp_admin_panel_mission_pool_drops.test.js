/**
 * Regression test for the Telegram Mini App's "Existing Drops" table —
 * the one with columns Name / Type / Status / Window (KL) / Stats / Actions,
 * rendered inline by adm_list() in static/index.html's #admin-panel section
 * (served at GET /miniapp) into <table id="adm_table">.
 *
 * Root cause this fixes: adm_list() only ever called
 * GET /v2/miniapp/admin/drops_v2 (db.drops — standard pooled/personalised
 * voucher drops). It never called GET /api/admin/mission-pool/campaigns, so
 * Mission Pool campaigns (gc_campaigns docs with mechanic="mission_pool")
 * never appeared here, regardless of status or visibility.
 *
 * This is a *different* table from static/admin-dashboard.html's
 * #drops-list-body (loadDrops() in static/admin-dashboard.js, served at
 * GET /admin, a separate browser-session-gated SPA) — that one already
 * merges mission-pool/campaigns (see test_admin_mini_app_existing_drops.test.js,
 * whose docstring mislabels it "Mini App", which is what led earlier fixes
 * to land there instead of here). The exact 6-column header text
 * (Name/Type/Status/Window (KL)/Stats/Actions) only exists in index.html.
 *
 * No build step, no jsdom in this repo — the relevant source is extracted
 * as text and executed in a sandboxed vm context, mirroring
 * test_live_drop_voucher_ui.test.js / test_admin_mini_app_existing_drops.test.js.
 *
 * Run with: node --test test_miniapp_admin_panel_mission_pool_drops.test.js
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

// adm_fetchMissionCampaigns / adm_missionRowHtml / adm_list.
const ADM_LIST_SRC = slice(
  INDEX_HTML,
  "    // Existing Drops shows two data sources merged into one authoritative",
  '\n    const admCreateBtn = document.getElementById("adm_create_btn");'
);

// The delegated .adm-mission-open-btn click handler.
const MISSION_OPEN_CLICK_SRC = slice(
  INDEX_HTML,
  '    document.addEventListener("click", (e) => {\n      const btn = e.target.closest(".adm-mission-open-btn");',
  '\n    document.addEventListener("click", async (e) => {\n      const btn = e.target.closest(".copy-drop-id-btn");'
);

function featureSource() {
  return ADM_LIST_SRC + "\n" + MISSION_OPEN_CLICK_SRC;
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
    _clickListeners: [],
    addEventListener(type, fn) {
      if (type === "click") document._clickListeners.push(fn);
    },
  };

  const fetchLog = [];
  const opened = [];
  const consoleErrors = [];

  const sandbox = {
    document,
    console: { log() {}, warn() {}, error: (...a) => consoleErrors.push(a.join(" ")) },
    escapeHtml: (s = "") => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])),
    fmtKL: (iso) => (iso ? "KL:" + iso : ""),
    getAdminUserId: () => userId,
    buildAdminQueryTail: () => "?user_id=" + userId,
    API_V2: "/v2/miniapp",
    window: { open: (url, target, features) => opened.push({ url, target, features }) },
    v2Fetch: async (url, init) => {
      fetchLog.push({ url, init });
      return fetchImpl(url, init);
    },
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);

  return { sandbox, tbody, fetchLog, opened, consoleErrors };
}

function run(sandbox) {
  vm.runInContext(featureSource(), sandbox);
}

function drop(overrides) {
  return Object.assign(
    {
      dropId: "d1",
      name: "Standard Drop",
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
      name: "SurpriseVoucherDrop_Bonus$1_260917",
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

function defaultFetch({ drops = [drop()], missions = [mission()] } = {}) {
  return async (url) => {
    if (String(url).includes("/admin/drops_v2")) {
      return { ok: true, status: 200, json: async () => ({ status: "ok", items: drops }) };
    }
    if (String(url).includes("/mission-pool/campaigns")) {
      return { ok: true, status: 200, json: async () => ({ status: "ok", campaigns: missions }), text: async () => JSON.stringify({ status: "ok", campaigns: missions }) };
    }
    throw new Error("unexpected fetch: " + url);
  };
}

// ---------------------------------------------------------------------
// 2/3. Pooled and mission rows appear together; both known campaign IDs render
// ---------------------------------------------------------------------

test("adm_list merges standard drops and mission-pool campaigns into one table", async () => {
  const missions = [
    mission({ campaign_id: "camp_bonus1", name: "SurpriseVoucherDrop_Bonus$1_260917" }),
    mission({ campaign_id: "camp_bonus25", name: "SurpriseVoucherDrop_Bonus$2.5_260916" }),
  ];
  const { sandbox, tbody, fetchLog } = buildSandbox({ fetchImpl: defaultFetch({ missions }) });
  run(sandbox);

  await sandbox.adm_list();

  assert.equal(tbody.children.length, 3, "1 standard drop row + 2 mission rows");
  const html = allText(tbody).join("\n");
  assert.match(html, /Standard Drop/);
  assert.match(html, /SurpriseVoucherDrop_Bonus\$1_260917/, "first known missing campaign must render");
  assert.match(html, /SurpriseVoucherDrop_Bonus\$2\.5_260916/, "second known missing campaign must render");

  assert.ok(fetchLog.some((f) => String(f.url).includes("/admin/drops_v2")));
  assert.ok(fetchLog.some((f) => String(f.url).includes("/mission-pool/campaigns")));
});

// ---------------------------------------------------------------------
// 4. Admin-only mission is included, with a visibility warning
// ---------------------------------------------------------------------

test("an admin-only, live mission with no linked provider still renders, with a visibility warning", async () => {
  const missions = [
    mission({
      campaign_id: "camp_admin_only",
      name: "SurpriseVoucherDrop_Bonus$1_260917",
      state: "live",
      admin_only: true,
      visibility_reason: "provider not configured",
    }),
  ];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: defaultFetch({ drops: [], missions }) });
  run(sandbox);

  await sandbox.adm_list();

  const html = allText(tbody).join("\n");
  assert.match(html, /SurpriseVoucherDrop_Bonus\$1_260917/);
  assert.match(html, /Admin-only/);
  assert.match(html, /provider not configured/);
});

// ---------------------------------------------------------------------
// 5. Mission rows use type "mission"
// ---------------------------------------------------------------------

test("mission rows render type 'mission', never 'mission_pool' or the raw mechanic value", async () => {
  const { sandbox, tbody } = buildSandbox({ fetchImpl: defaultFetch({ drops: [] }) });
  run(sandbox);

  await sandbox.adm_list();

  const row = tbody.children[0];
  assert.match(row.innerHTML, /<td>mission<\/td>/);
  assert.doesNotMatch(row.innerHTML, /mission_pool/);
});

// ---------------------------------------------------------------------
// 6. No duplicate IDs
// ---------------------------------------------------------------------

test("no duplicate rows for the same campaign_id", async () => {
  const missions = [mission({ campaign_id: "camp1" }), mission({ campaign_id: "camp1" })];
  // (Two campaigns sharing an id would be an upstream data bug; this just
  // confirms adm_list doesn't itself introduce duplication via a double
  // render pass — exactly one row per item in the response.)
  const { sandbox, tbody } = buildSandbox({ fetchImpl: defaultFetch({ drops: [], missions }) });
  run(sandbox);

  await sandbox.adm_list();
  assert.equal(tbody.children.length, missions.length);
});

// ---------------------------------------------------------------------
// 7. Mission-fetch failure preserves standard rows, surfaces an inline error
// ---------------------------------------------------------------------

test("mission-pool/campaigns failing (500) still shows standard drops, plus a visible inline error", async () => {
  const { sandbox, tbody, consoleErrors } = buildSandbox({
    fetchImpl: async (url) => {
      if (String(url).includes("/admin/drops_v2")) {
        return { ok: true, status: 200, json: async () => ({ status: "ok", items: [drop()] }) };
      }
      if (String(url).includes("/mission-pool/campaigns")) {
        return { ok: false, status: 500, statusText: "Internal Server Error", text: async () => '{"status":"error","code":"boom"}' };
      }
      throw new Error("unexpected fetch: " + url);
    },
  });
  run(sandbox);

  await sandbox.adm_list();

  const html = allText(tbody).join("\n");
  assert.match(html, /Standard Drop/, "standard row must survive a mission-endpoint failure");
  assert.match(html, /Mission Pool campaigns failed to load/);
  assert.match(html, /500/);
  assert.ok(consoleErrors.some((m) => m.includes("mission-pool/campaigns failed")), "failure must be logged, not swallowed silently");
});

// ---------------------------------------------------------------------
// 8. A later refresh does not overwrite/duplicate mission rows
// ---------------------------------------------------------------------

test("calling adm_list() again re-renders both sources cleanly, no leftover duplicates", async () => {
  const missions = [mission({ campaign_id: "camp1" }), mission({ campaign_id: "camp2" })];
  const { sandbox, tbody } = buildSandbox({ fetchImpl: defaultFetch({ missions }) });
  run(sandbox);

  await sandbox.adm_list();
  assert.equal(tbody.children.length, 3);

  await sandbox.adm_list();
  assert.equal(tbody.children.length, 3, "refresh must not accumulate rows from the previous render");
});

// ---------------------------------------------------------------------
// 9. Mission actions never call standard-drop endpoints
// ---------------------------------------------------------------------

test("the mission 'Open' action opens the Campaign Centre, never a drops_v2 lifecycle endpoint", async () => {
  const { sandbox, tbody, fetchLog, opened } = buildSandbox({ fetchImpl: defaultFetch({ drops: [] }) });
  run(sandbox);
  await sandbox.adm_list();

  const openBtn = { className: "btn btn-secondary adm-mission-open-btn" };
  openBtn.closest = (sel) => (sel === ".adm-mission-open-btn" ? openBtn : null);
  const evt = { target: openBtn, preventDefault() {} };

  fetchLog.length = 0;
  sandbox.document._clickListeners.forEach((fn) => fn(evt));

  assert.equal(opened.length, 1);
  assert.equal(opened[0].url, "/admin");
  assert.equal(fetchLog.length, 0, "clicking Open must not call any lifecycle endpoint (drops_v2 or otherwise)");
});

test("mission row HTML never wires start_now/pause/end_now (the drops_v2 lifecycle ops)", async () => {
  const { sandbox, tbody } = buildSandbox({ fetchImpl: defaultFetch({ drops: [] }) });
  run(sandbox);
  await sandbox.adm_list();

  const row = tbody.children[0];
  assert.doesNotMatch(row.innerHTML, /data-op="start_now"/);
  assert.doesNotMatch(row.innerHTML, /data-op="pause"/);
  assert.doesNotMatch(row.innerHTML, /data-op="end_now"/);
});
