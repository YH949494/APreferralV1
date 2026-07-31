/**
 * Tests for the Referral Centre → Share Content → "Creator Access" sub-tab
 * in static/admin-dashboard.html / static/admin-dashboard.js.
 *
 * The dashboard is one large inline-script-free file with no build step and
 * no jsdom in this repo, so (mirroring test_cc_rich_text_editor.test.js and
 * test_creator_share_ui.js) the relevant source is extracted as text and
 * executed in a sandboxed vm context against a small hand-rolled DOM/fetch
 * stub — just enough querySelector/classList/dataset/closest support for
 * the code paths exercised here.
 *
 * Run with: node --test test_admin_creator_access_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const HTML_PATH = path.join(__dirname, "static", "admin-dashboard.html");
const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  assert.ok(start !== -1, "marker not found: " + startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(end !== -1, "end marker not found: " + endMarker);
  return src.slice(start, end);
}

function loadFunctionsSource() {
  const js = fs.readFileSync(JS_PATH, "utf8");
  const helpers = slice(js, "  function $(sel, root)", "  function banner(msg, kind) {");
  const toastBlock = slice(js, "  function toastStack() {", "  // ---------- Reusable button loading-state feedback ----------");
  const stateHelpers = slice(js, "  function statePanel(elId, kind, msg) {", "  function expandTable(headers, rows) {");
  const rscBlock = slice(js, '  var rsc = { subtab: "hooks" };', "  function loadSharePlayback() {");
  return [helpers, toastBlock, stateHelpers, rscBlock].join("\n");
}

// ---------------------------------------------------------------------
// Minimal hand-rolled DOM: just enough querySelector/classList/dataset/
// closest support for the extracted rsc (Referral Share Content) code.
// ---------------------------------------------------------------------

function toCamel(attr) {
  return attr.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
}

class FakeNode {
  constructor(tag) {
    this.tagName = (tag || "div").toUpperCase();
    this.id = "";
    this.children = [];
    this.parent = null;
    this._classes = new Set();
    this.dataset = {};
    this._text = "";
    this._html = "";
    this.value = "";
    this.checked = false;
    this.disabled = false;
    this._listeners = {};
    const self = this;
    this.classList = {
      add: (...c) => c.forEach((x) => self._classes.add(x)),
      remove: (...c) => c.forEach((x) => self._classes.delete(x)),
      toggle: (c, force) => {
        if (force === undefined) { self._classes.has(c) ? self._classes.delete(c) : self._classes.add(c); }
        else if (force) { self._classes.add(c); }
        else { self._classes.delete(c); }
      },
      contains: (c) => self._classes.has(c),
    };
  }
  get textContent() { return this._text; }
  set textContent(v) { this._text = v == null ? "" : String(v); }
  get innerHTML() { return this._html; }
  set innerHTML(v) { this._html = v == null ? "" : String(v); }
  appendChild(node) { node.parent = this; this.children.push(node); return node; }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((c) => c !== this); }
  addEventListener(evt, fn) { (this._listeners[evt] = this._listeners[evt] || []).push(fn); }
  _trigger(evt, evtObj) { (this._listeners[evt] || []).forEach((fn) => fn(evtObj || { target: this })); }
  closest(sel) {
    const m = /^\[data-([a-z-]+)\]$/.exec(sel);
    let node = this;
    while (node) {
      if (m && node.dataset[toCamel(m[1])] !== undefined) return node;
      node = node.parent;
    }
    return null;
  }
}

function buildDom() {
  const registry = new Map();
  function getOrCreate(id) {
    if (!registry.has(id)) {
      const n = new FakeNode("div");
      n.id = id;
      registry.set(id, n);
    }
    return registry.get(id);
  }

  function matchesSimple(node, token) {
    if (token[0] === ".") return node._classes.has(token.slice(1));
    return node.tagName === token.toUpperCase();
  }

  function queryAll(sel) {
    const parts = sel.trim().split(/\s+/);
    if (parts[0][0] !== "#") return [];
    const container = getOrCreate(parts[0].slice(1));
    if (parts.length === 1) return [container];
    const token = parts[parts.length - 1];
    const matches = [];
    (function walk(node) {
      node.children.forEach((c) => {
        if (matchesSimple(c, token)) matches.push(c);
        walk(c);
      });
    })(container);
    return matches;
  }

  const docListeners = {};
  const documentStub = {
    getElementById: (id) => getOrCreate(id),
    createElement: (tag) => new FakeNode(tag),
    body: new FakeNode("body"),
    querySelector: (sel) => queryAll(sel)[0] || null,
    querySelectorAll: (sel) => queryAll(sel),
    addEventListener: (evt, fn) => { (docListeners[evt] = docListeners[evt] || []).push(fn); },
    _trigger: (evt, evtObj) => { (docListeners[evt] || []).forEach((fn) => fn(evtObj)); },
  };

  // Pre-build the static status-filter button group exactly as it appears
  // in static/admin-dashboard.html, so rscStatusFilter()'s "#id .active"
  // lookup and the $all("#id button") wiring have real children to find.
  function buildStatusFilterGroup(id, statuses) {
    const container = getOrCreate(id);
    statuses.forEach((status, i) => {
      const btn = new FakeNode("button");
      btn.dataset.status = status;
      if (i === 0) btn.classList.add("active");
      container.appendChild(btn);
    });
  }
  buildStatusFilterGroup("rsc-creators-status-filter", ["", "active", "suspended", "removed"]);

  return { getOrCreate, documentStub, registry };
}

function buildSandbox({ fetchImpl } = {}) {
  const { getOrCreate, documentStub } = buildDom();
  const sandbox = {
    document: documentStub,
    window: { location: { href: "" } },
    fetch: fetchImpl,
    confirm: () => true,
    setTimeout: (fn) => setImmediate(fn),
    clearTimeout: () => {},
    console,
  };
  vm.createContext(sandbox);
  const src = loadFunctionsSource() +
    "\nthis.__loadReferralShareContent = loadReferralShareContent;" +
    "\nthis.__loadCreatorGroupSettings = loadCreatorGroupSettings;" +
    "\nthis.__loadCreatorAccess = loadCreatorAccess;" +
    "\nthis.__bindReferralShareContent = bindReferralShareContent;" +
    "\nthis.__rsc = rsc;";
  vm.runInContext(src, sandbox);
  return { sandbox, getOrCreate };
}

function jsonResponse(status, body) {
  return Promise.resolve({
    ok: status >= 200 && status < 300,
    status,
    json: () => Promise.resolve(body),
  });
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

// ---------------------------------------------------------------------
// 1. Tab exists, in the right place, in the right order
// ---------------------------------------------------------------------

const dashboardHtml = fs.readFileSync(HTML_PATH, "utf8");

function shareContentSectionHtml() {
  const start = dashboardHtml.indexOf('<section id="view-referralShareContent"');
  assert.ok(start !== -1, "view-referralShareContent section not found");
  const end = dashboardHtml.indexOf("</section>", start);
  return dashboardHtml.slice(start, end);
}

test("Share Content has three sub-tabs in order: Caption Hooks, Playback Pool, Creator Access", () => {
  const section = shareContentSectionHtml();
  const hooksIdx = section.indexOf('id="rsc-subtab-hooks"');
  const playbackIdx = section.indexOf('id="rsc-subtab-playback"');
  const creatorsIdx = section.indexOf('id="rsc-subtab-creators"');
  assert.ok(hooksIdx !== -1 && playbackIdx !== -1 && creatorsIdx !== -1, "all three sub-tab buttons must exist");
  assert.ok(hooksIdx < playbackIdx && playbackIdx < creatorsIdx, "tabs must appear in Caption Hooks, Playback Pool, Creator Access order");
  assert.match(section, />Caption Hooks</);
  assert.match(section, />Playback Pool</);
  assert.match(section, />Creator Access</);
});

test("Creator Access sub-tab is always present in the Share Content view (no reload/conditional render)", () => {
  const section = shareContentSectionHtml();
  // The button lives directly in the static markup, not injected conditionally.
  assert.match(section, /<button id="rsc-subtab-creators">Creator Access<\/button>/);
});

test("Creator Access panel renders both Creator Group Access and Creator Members sections", () => {
  const section = shareContentSectionHtml();
  const panelStart = section.indexOf('id="rsc-creators-panel"');
  assert.ok(panelStart !== -1, "rsc-creators-panel not found");
  const panel = section.slice(panelStart);
  assert.match(panel, /Creator Group Access/);
  assert.match(panel, /id="rsc-creator-group-chat-id"/);
  assert.match(panel, /id="rsc-creator-group-check-enabled"/);
  assert.match(panel, /id="rsc-creator-group-verify-btn"/);
  assert.match(panel, />Verify Group</);
  assert.match(panel, /id="rsc-creator-group-save-btn"/);
  assert.match(panel, />Save Changes</);
  assert.match(panel, /id="rsc-creator-group-chat-title"/);
  assert.match(panel, /id="rsc-creator-group-chat-type"/);
  assert.match(panel, /id="rsc-creator-group-bot-status"/);
  assert.match(panel, /id="rsc-creator-group-verified-at"/);

  assert.match(panel, /id="rsc-creators-summary"/);
  assert.match(panel, /id="rsc-creators-search"/);
  assert.match(panel, /id="rsc-creator-user-id"/);
  assert.match(panel, /id="rsc-creator-add-btn"/);
  assert.match(panel, /id="rsc-creators-bulk-text"/);
  assert.match(panel, /id="rsc-creators-bulk-btn"/);
  assert.match(panel, /id="rsc-creators-body"/);
});

test("Creator Group Chat ID is never masked and the bot token is never rendered anywhere on the page", () => {
  assert.doesNotMatch(dashboardHtml, /bot_token/i);
  assert.doesNotMatch(dashboardHtml, /BOT_TOKEN/);
  // The chat-id input must be a plain text field, not password/masked.
  const section = shareContentSectionHtml();
  const inputMatch = /<input[^>]*id="rsc-creator-group-chat-id"[^>]*>/.exec(section);
  assert.ok(inputMatch, "chat id input not found");
  assert.doesNotMatch(inputMatch[0], /type="password"/);
});

test("general Settings → Telegram Configuration section does not contain the Creator Group Chat ID field", () => {
  // Locate the general Telegram Configuration block by its heading and scan
  // only that block (not the whole file) so this assertion can't accidentally
  // pass just because the id happens to live elsewhere on the page.
  const headingIdx = dashboardHtml.indexOf("Telegram Configuration");
  assert.ok(headingIdx !== -1, "Telegram Configuration heading not found");
  // Telegram Configuration lives inside a <section>...</section>; find its bounds.
  const sectionStart = dashboardHtml.lastIndexOf("<section", headingIdx);
  const sectionEnd = dashboardHtml.indexOf("</section>", headingIdx);
  const telegramConfigBlock = dashboardHtml.slice(sectionStart, sectionEnd);
  assert.doesNotMatch(telegramConfigBlock, /rsc-creator-group-chat-id/);
  assert.doesNotMatch(telegramConfigBlock, /Creator Group Chat ID/i);
});

// ---------------------------------------------------------------------
// 2. Clicking the Creator Access tab reveals both sections, no reload
// ---------------------------------------------------------------------

test("clicking the Creator Access sub-tab reveals the creators panel and hides the others", () => {
  const { sandbox, getOrCreate } = buildSandbox({ fetchImpl: () => jsonResponse(200, {}) });
  sandbox.__rsc.subtab = "creators";
  sandbox.__loadReferralShareContent(true);
  assert.equal(getOrCreate("rsc-creators-panel").classList.contains("hidden"), false);
  assert.equal(getOrCreate("rsc-hooks-panel").classList.contains("hidden"), true);
  assert.equal(getOrCreate("rsc-playback-panel").classList.contains("hidden"), true);
  assert.equal(getOrCreate("rsc-subtab-creators").classList.contains("active"), true);
  assert.equal(getOrCreate("rsc-subtab-hooks").classList.contains("active"), false);
});

test("bindReferralShareContent wires a click handler that switches to the creators sub-tab in place (no reload)", async () => {
  const calls = [];
  const { sandbox, getOrCreate } = buildSandbox({
    fetchImpl: (url) => { calls.push(url); return jsonResponse(200, { settings: {}, creators: [], active_count: 0 }); },
  });
  sandbox.__bindReferralShareContent();
  getOrCreate("rsc-subtab-creators")._trigger("click");
  await flush();
  assert.equal(sandbox.__rsc.subtab, "creators");
  assert.equal(getOrCreate("rsc-creators-panel").classList.contains("hidden"), false);
  assert.ok(calls.some((u) => u.startsWith("/api/admin/referral/creator-settings")));
  assert.ok(calls.some((u) => u.startsWith("/api/admin/referral/creators")));
});

// ---------------------------------------------------------------------
// 3. Settings load from GET /api/admin/referral/creator-settings
// ---------------------------------------------------------------------

test("loadCreatorGroupSettings loads from GET /api/admin/referral/creator-settings and populates all fields", async () => {
  const calls = [];
  const { sandbox, getOrCreate } = buildSandbox({
    fetchImpl: (url, opts) => {
      calls.push({ url, method: (opts && opts.method) || "GET" });
      return jsonResponse(200, {
        settings: {
          creator_group_chat_id: "-1001234567890",
          membership_check_enabled: true,
          chat_title: "Creators HQ",
          chat_type: "supergroup",
          bot_membership_status: "administrator",
          verified_at: "2026-07-30T10:00:00Z",
          source: "db",
          config_version: 3,
        },
      });
    },
  });
  sandbox.__loadCreatorGroupSettings();
  await flush();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "/api/admin/referral/creator-settings");
  assert.equal(calls[0].method, "GET");
  assert.equal(getOrCreate("rsc-creator-group-chat-id").value, "-1001234567890");
  assert.equal(getOrCreate("rsc-creator-group-check-enabled").checked, true);
  assert.equal(getOrCreate("rsc-creator-group-chat-title").textContent, "Creators HQ");
  assert.equal(getOrCreate("rsc-creator-group-chat-type").textContent, "supergroup");
  assert.equal(getOrCreate("rsc-creator-group-bot-status").textContent, "administrator");
});

test("loadCreatorGroupSettings shows placeholders when unconfigured, without masking the chat id field", async () => {
  const { sandbox, getOrCreate } = buildSandbox({ fetchImpl: () => jsonResponse(200, { settings: {} }) });
  sandbox.__loadCreatorGroupSettings();
  await flush();
  assert.equal(getOrCreate("rsc-creator-group-chat-id").value, "");
  assert.equal(getOrCreate("rsc-creator-group-chat-title").textContent, "—");
});

// ---------------------------------------------------------------------
// 4. Verify Group / Save Changes call the correct endpoints
// ---------------------------------------------------------------------

test("Verify Group button POSTs to /api/admin/referral/creator-settings/verify-group with the entered chat id", async () => {
  const calls = [];
  const { sandbox, getOrCreate } = buildSandbox({
    fetchImpl: (url, opts) => {
      calls.push({ url, method: opts.method, body: opts.body && JSON.parse(opts.body) });
      return jsonResponse(200, { status: "ok", chat_title: "Creators HQ", chat_type: "supergroup", bot_membership_status: "administrator" });
    },
  });
  sandbox.__bindReferralShareContent();
  getOrCreate("rsc-creator-group-chat-id").value = "-1009876543210";
  getOrCreate("rsc-creator-group-verify-btn")._trigger("click");
  await flush();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "/api/admin/referral/creator-settings/verify-group");
  assert.equal(calls[0].method, "POST");
  assert.equal(calls[0].body.creator_group_chat_id, "-1009876543210");
  assert.match(getOrCreate("rsc-creator-group-verify-result").textContent, /Verified/);
});

test("Save Changes button PUTs to /api/admin/referral/creator-settings with chat id and membership toggle", async () => {
  const calls = [];
  const { sandbox, getOrCreate } = buildSandbox({
    fetchImpl: (url, opts) => {
      calls.push({ url, method: opts.method, body: opts.body && JSON.parse(opts.body) });
      return jsonResponse(200, { status: "ok" });
    },
  });
  sandbox.__bindReferralShareContent();
  getOrCreate("rsc-creator-group-chat-id").value = "-1009876543210";
  getOrCreate("rsc-creator-group-check-enabled").checked = true;
  getOrCreate("rsc-creator-group-save-btn")._trigger("click");
  await flush();
  const putCalls = calls.filter((c) => c.method === "PUT");
  assert.equal(putCalls.length, 1);
  assert.equal(putCalls[0].url, "/api/admin/referral/creator-settings");
  assert.equal(putCalls[0].body.creator_group_chat_id, "-1009876543210");
  assert.equal(putCalls[0].body.membership_check_enabled, true);
});

// ---------------------------------------------------------------------
// 5. Creator members: approve/suspend/activate/remove are rendered + wired
// ---------------------------------------------------------------------

test("loadCreatorAccess loads from GET /api/admin/referral/creators and renders all required table columns", async () => {
  const calls = [];
  const { sandbox, getOrCreate } = buildSandbox({
    fetchImpl: (url) => {
      calls.push(url);
      return jsonResponse(200, {
        active_count: 2,
        creators: [
          { user_id: "111", username: "alice", status: "active", creator_tier: "pilot", approved_at: "2026-07-01T00:00:00Z" },
          { user_id: "222", username: "bob", status: "suspended", creator_tier: "core", approved_at: "2026-07-02T00:00:00Z" },
        ],
      });
    },
  });
  sandbox.__loadCreatorAccess();
  await flush();
  assert.ok(calls[0].startsWith("/api/admin/referral/creators"));
  assert.equal(getOrCreate("rsc-creators-summary").textContent, "Active creators: 2");
  const body = getOrCreate("rsc-creators-body").innerHTML;
  assert.match(body, />111</);
  assert.match(body, />alice</);
  assert.match(body, />pilot</);
  assert.match(body, /data-rsc-creator-action="suspend"/);
  assert.match(body, /data-rsc-creator-action="activate"/);
  assert.match(body, /data-rsc-creator-action="remove"/);
});

test("Approve creator form POSTs to /api/admin/referral/creators with user id, username and tier", async () => {
  const calls = [];
  const { sandbox, getOrCreate } = buildSandbox({
    fetchImpl: (url, opts) => {
      calls.push({ url, method: opts && opts.method, body: opts && opts.body && JSON.parse(opts.body) });
      return jsonResponse(200, { status: "ok" });
    },
  });
  sandbox.__bindReferralShareContent();
  getOrCreate("rsc-creator-user-id").value = "555";
  getOrCreate("rsc-creator-username").value = "creator5";
  getOrCreate("rsc-creator-tier").value = "core";
  getOrCreate("rsc-creator-add-btn")._trigger("click");
  await flush();
  const postCalls = calls.filter((c) => c.url === "/api/admin/referral/creators" && c.method === "POST");
  assert.equal(postCalls.length, 1);
  assert.equal(postCalls[0].body.user_id, "555");
  assert.equal(postCalls[0].body.username, "creator5");
  assert.equal(postCalls[0].body.creator_tier, "core");
});

test("Bulk import POSTs to /api/admin/referral/creators/bulk with the entered user ids", async () => {
  const calls = [];
  const { sandbox, getOrCreate } = buildSandbox({
    fetchImpl: (url, opts) => {
      calls.push({ url, method: opts && opts.method, body: opts && opts.body && JSON.parse(opts.body) });
      return jsonResponse(200, { inserted: 2, skipped: 0, rejected: 0, results: [] });
    },
  });
  sandbox.__bindReferralShareContent();
  getOrCreate("rsc-creators-bulk-text").value = "111\n222";
  getOrCreate("rsc-creators-bulk-btn")._trigger("click");
  await flush();
  const call = calls.find((c) => c.url === "/api/admin/referral/creators/bulk");
  assert.ok(call, "expected a bulk import request");
  assert.equal(call.method, "POST");
  assert.equal(call.body.user_ids, "111\n222");
});

test("Activate/Suspend/Remove actions call the correct per-creator endpoints", async () => {
  const calls = [];
  const { sandbox } = buildSandbox({
    fetchImpl: (url, opts) => {
      calls.push({ url, method: (opts && opts.method) || "GET" });
      return jsonResponse(200, { status: "ok" });
    },
  });
  const doc = sandbox.document;
  sandbox.__bindReferralShareContent();

  function fakeActionButton(action, id) {
    return { dataset: { rscCreatorAction: action, id }, closest: (sel) => (sel === "[data-rsc-creator-action]" ? { dataset: { rscCreatorAction: action, id } } : null) };
  }

  doc._trigger("click", { target: fakeActionButton("suspend", "111") });
  await flush();
  doc._trigger("click", { target: fakeActionButton("activate", "222") });
  await flush();
  doc._trigger("click", { target: fakeActionButton("remove", "333") });
  await flush();

  assert.ok(calls.some((c) => c.url === "/api/admin/referral/creators/111/suspend" && c.method === "POST"));
  assert.ok(calls.some((c) => c.url === "/api/admin/referral/creators/222/activate" && c.method === "POST"));
  assert.ok(calls.some((c) => c.url === "/api/admin/referral/creators/333" && c.method === "DELETE"));
});
