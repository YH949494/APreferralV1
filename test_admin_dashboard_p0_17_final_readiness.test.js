/**
 * P0.17 — final production-readiness correctness pass on top of the stable
 * gc_campaigns core flow (P0.2-P0.16). Covers the frontend half of each of
 * the four gaps; the backend half of Parts A/C is test_campaign_centre.py's
 * own P0.17 section.
 *
 *  Part B — legacy telegram-less campaigns must show the SAME effective
 *  subscription default the runtime applies (ON when the telegram block is
 *  missing/incomplete), with a clear warning when no channel is configured
 *  to satisfy it, instead of silently reading `undefined` as OFF.
 *
 *  Part C3 — an active provider with no base_url must never read as a
 *  complete "Where users go" destination in Campaign Detail's checklist.
 *
 *  Part D — the Providers admin screen (create/activate/deactivate) is
 *  hardened onto the same gcRunAction choke point every gc_campaigns
 *  lifecycle action already uses: friendly errors (never a raw snake_case
 *  code or "HTTP 400"), an in-flight guard, button disable, no unhandled
 *  rejection.
 *
 *  Part A2 — the Registration Configuration screen gains a channel
 *  username field and saves both `registration` and `telegram` via
 *  read-merge-PUT, never a partial telegram fragment.
 *
 * Mirrors test_admin_dashboard_p0_14_reachability.test.js's exact harness
 * (same source-slice-into-vm pattern) — no build step, no jsdom.
 *
 * Run with: node --test test_admin_dashboard_p0_17_final_readiness.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const JS = fs.readFileSync(JS_PATH, "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

function plain(v) { return JSON.parse(JSON.stringify(v)); }

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function runInSandbox(code, sandboxExtra) {
  const sandbox = sandboxExtra || {};
  sandbox.console = sandbox.console || console;
  sandbox.Object = sandbox.Object || Object;
  sandbox.String = sandbox.String || String;
  sandbox.Date = sandbox.Date || Date;
  sandbox.Promise = sandbox.Promise || Promise;
  sandbox.JSON = sandbox.JSON || JSON;
  sandbox.Array = sandbox.Array || Array;
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

// ---------------------------------------------------------------------
// Real source slices (never re-implemented stand-ins).
// ---------------------------------------------------------------------
const PROVIDER_LABEL_SRC = slice(JS, "  function gcProviderOptionLabel(p) {", "\n  function renderGcProviderSelect()");
const KL_SRC = slice(JS, "  function ccPad2(n)", "\n  var CC_CONTENT_ICON");
const PURE_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  // ---- Composer + orchestration (DOM-touching)");
// Covers GC_ACTION_ERROR_MESSAGES, gcActionErrorMessage, gcErrorMessageForCode,
// gcRunAction, crCfg* and bindCampaignRegistrationConfig. Two real,
// independently-bounded slices (the Providers screen is declared much
// further down the file, well after the unrelated Campaign Registrations
// list/table section this range's own end marker starts) rather than one
// contiguous range spanning hundreds of irrelevant lines between them.
const ACTION_SRC = slice(
  JS,
  "  var GC_ACTION_ERROR_MESSAGES = {",
  "\n  // ---------- Campaign Registrations (campaign_registration.py) ----------"
);
// Covers gcProvidersBackCampaignId (referenced by bindGcProviders' third
// listener), loadGcProviders, GC_PROVIDER_ERROR_MESSAGES,
// gcProviderErrorMessage, bindGcProviders.
const PROVIDER_SRC = slice(JS, "  var gcProvidersBackCampaignId = null;", "\n  function loadGcResults(force) {");
// crCfg's save handler calls cdEffectiveRequireSubscription directly (Codex
// review fix) — it's declared in the PURE_SRC range, well before
// ACTION_SRC's own start marker, so it must be spliced in separately.
const EFFECTIVE_SUB_SRC = slice(JS, "  function cdEffectiveRequireSubscription(campaign) {", "\n  // ---- Pure checklist derivation");
const ACTION_AND_PROVIDER_SRC = EFFECTIVE_SUB_SRC + "\n" + ACTION_SRC + "\n" + PROVIDER_SRC;

function loadPure() {
  return runInSandbox(PROVIDER_LABEL_SRC + "\n" + KL_SRC + "\n" + PURE_SRC + "\nthis.__x = { " +
    "cdDestinationEditHtml, cdEffectiveRequireSubscription, cdHasConfiguredChannel, " +
    "computeSetupChecklist, gcCampaignDetailChecklistHtml, gcVisibilityReasonText, GC_PUBLISH_BLOCKING_REASONS };",
    { esc }).__x;
}

const P = loadPure();

function tournamentCampaign(overrides) {
  return Object.assign({
    campaign_id: "july-tournament",
    name: "July Tournament",
    type: "tournament",
    status: "draft",
    description: "Top 3 leaderboard prize",
    schedule: { starts_at: "2026-10-01T01:00:00+00:00", ends_at: "2026-10-31T15:59:00+00:00", timezone: "Asia/Kuala_Lumpur" },
    destination: { provider_id: "p1", open_mode: "telegram_web_app", path: "/game", ready: true },
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 3 } }] },
    registration: { enabled: false },
  }, overrides || {});
}

// ---------------------------------------------------------------------
// Part B — legacy telegram-less truth
// ---------------------------------------------------------------------

test("cdEffectiveRequireSubscription: no telegram block at all defaults to the runtime's ON default", () => {
  assert.equal(P.cdEffectiveRequireSubscription({}), true);
  assert.equal(P.cdEffectiveRequireSubscription({ telegram: {} }), true);
});

test("cdEffectiveRequireSubscription: an explicit false/true on file always wins", () => {
  assert.equal(P.cdEffectiveRequireSubscription({ telegram: { require_subscription: false } }), false);
  assert.equal(P.cdEffectiveRequireSubscription({ telegram: { require_subscription: true } }), true);
});

test("cdHasConfiguredChannel: true for either channel_id or channel_username, false for neither", () => {
  assert.equal(P.cdHasConfiguredChannel({ telegram: { channel_username: "AdvantPlayOfficial" } }), true);
  assert.equal(P.cdHasConfiguredChannel({ telegram: { channel_id: -100123 } }), true);
  assert.equal(P.cdHasConfiguredChannel({ telegram: {} }), false);
  assert.equal(P.cdHasConfiguredChannel({}), false);
});

test("computeSetupChecklist: legacy telegram-less campaign carries a subscriptionWarning on the destination row", () => {
  const campaign = tournamentCampaign({ telegram: undefined });
  const rows = P.computeSetupChecklist(campaign, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "https://mywin.example.com" }], []);
  const destRow = rows.filter((r) => r.key === "destination")[0];
  assert.match(destRow.subscriptionWarning, /Players are currently blocked/);
});

test("computeSetupChecklist: subscriptionWarning disappears once a channel is configured", () => {
  const campaign = tournamentCampaign({ telegram: { channel_username: "AdvantPlayOfficial" } });
  const rows = P.computeSetupChecklist(campaign, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "https://mywin.example.com" }], []);
  const destRow = rows.filter((r) => r.key === "destination")[0];
  assert.equal(destRow.subscriptionWarning, null);
});

test("computeSetupChecklist: subscriptionWarning disappears once explicitly turned off", () => {
  const campaign = tournamentCampaign({ telegram: { require_subscription: false } });
  const rows = P.computeSetupChecklist(campaign, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "https://mywin.example.com" }], []);
  const destRow = rows.filter((r) => r.key === "destination")[0];
  assert.equal(destRow.subscriptionWarning, null);
});

test("gcCampaignDetailChecklistHtml renders the subscriptionWarning text on the destination row's read view", () => {
  const campaign = tournamentCampaign({ telegram: undefined });
  const rows = P.computeSetupChecklist(campaign, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "https://mywin.example.com" }], []);
  const html = P.gcCampaignDetailChecklistHtml(rows, { editingSection: null, campaign, providers: [] });
  assert.match(html, /Players are currently blocked because channel subscription is required/);
});

test("cdDestinationEditHtml: legacy telegram-less campaign shows the switch CHECKED (matches runtime truth), not unchecked", () => {
  const campaign = tournamentCampaign({ telegram: undefined });
  const html = P.cdDestinationEditHtml(campaign, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "https://mywin.example.com" }]);
  assert.match(html, /id="cd-edit-tg-require-sub" checked/);
  assert.match(html, /Players are currently blocked because channel subscription is required/);
});

test("cdDestinationEditHtml: repair option A (turn off) — no warning once require_subscription is explicitly false", () => {
  const campaign = tournamentCampaign({ telegram: { require_subscription: false } });
  const html = P.cdDestinationEditHtml(campaign, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "https://mywin.example.com" }]);
  assert.doesNotMatch(html, /id="cd-edit-tg-require-sub" checked/);
  assert.doesNotMatch(html, /Players are currently blocked/);
});

test("cdDestinationEditHtml: repair option B (set channel) — no warning once a channel is configured, switch stays checked", () => {
  const campaign = tournamentCampaign({ telegram: undefined, destination: { provider_id: "p1", open_mode: "telegram_web_app", path: "/game", ready: true } });
  // Simulates the admin having just typed a channel username — the read
  // model that matters here is whatever the freshest canonical GET holds.
  const repaired = tournamentCampaign({ telegram: { channel_username: "AdvantPlayOfficial" } });
  const html = P.cdDestinationEditHtml(repaired, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "https://mywin.example.com" }]);
  assert.doesNotMatch(html, /Players are currently blocked/);
  assert.match(html, /id="cd-edit-tg-channel-username"[^>]*value="AdvantPlayOfficial"/);
});

// ---------------------------------------------------------------------
// Part C3 — active provider with no base_url is never a complete
// destination, and never silently shown as just "the provider's name".
// ---------------------------------------------------------------------

test("computeSetupChecklist: active provider with NO base_url is incomplete, with a specific summary", () => {
  const campaign = tournamentCampaign();
  const rows = P.computeSetupChecklist(campaign, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "" }], []);
  const destRow = rows.filter((r) => r.key === "destination")[0];
  assert.equal(destRow.complete, false);
  assert.match(destRow.summary, /no destination URL configured/);
});

test("computeSetupChecklist: active provider WITH a base_url is unaffected — still complete", () => {
  const campaign = tournamentCampaign();
  const rows = P.computeSetupChecklist(campaign, [{ provider_id: "p1", name: "MyWin", active: true, base_url: "https://mywin.example.com" }], []);
  const destRow = rows.filter((r) => r.key === "destination")[0];
  assert.equal(destRow.complete, true);
  assert.equal(destRow.summary, "MyWin");
});

test("gcVisibilityReasonText translates the new provider-base_url reason to plain English, never the raw string", () => {
  const text = P.gcVisibilityReasonText("The selected provider has no usable destination URL.");
  assert.equal(text, "The selected provider has no destination URL configured.");
});

test("gcVisibilityReasonText translates the registration-channel-gate reason to plain English", () => {
  const text = P.gcVisibilityReasonText("registration requires channel subscription but no channel is configured");
  assert.match(text, /Registration requires channel subscription/);
});

test("GC_PUBLISH_BLOCKING_REASONS includes the provider-base_url reason (mirrors the new _transition gate) but never the registration-channel one (which _transition doesn't gate on)", () => {
  assert.ok(P.GC_PUBLISH_BLOCKING_REASONS.indexOf("The selected provider has no usable destination URL.") !== -1);
  assert.equal(P.GC_PUBLISH_BLOCKING_REASONS.indexOf("registration requires channel subscription but no channel is configured"), -1);
});

// ---------------------------------------------------------------------
// Harness for the DOM-orchestration half (Providers screen + Registration
// Configuration screen) — mirrors p0_14's loadOrchestration() pattern:
// $() returns a per-selector node with value/checked/textContent/style,
// document.addEventListener captures delegated listeners so a synthetic
// click/change event can be dispatched against them.
// ---------------------------------------------------------------------

function makeDom() {
  const nodes = {};
  function node(sel) {
    if (!nodes[sel]) {
      const n = {
        value: "", checked: false, textContent: "", innerHTML: "", disabled: false,
        classList: { add() {}, remove() {}, toggle() {} }, style: {},
        _listeners: {},
        addEventListener(type, fn) { (n._listeners[type] || (n._listeners[type] = [])).push(fn); },
      };
      nodes[sel] = n;
    }
    return nodes[sel];
  }
  function fire(sel, type, evt) {
    const n = node(sel);
    (n._listeners[type] || []).forEach((fn) => fn(evt || { target: n }));
  }
  return { nodes, node, fire };
}

function makeDocument() {
  const listeners = { click: [], change: [] };
  return {
    listeners,
    addEventListener(type, fn) { (listeners[type] || (listeners[type] = [])).push(fn); },
    fireClick(target) { listeners.click.forEach((fn) => fn({ target })); },
    fireChange(target) { listeners.change.forEach((fn) => fn({ target })); },
    querySelectorAll() { return { forEach() {} }; }, // overridden per-test when needed
  };
}

async function flush(n) {
  for (let i = 0; i < (n || 12); i++) await Promise.resolve();
}

function loadActionAndProviderSandbox(overrides) {
  const dom = makeDom();
  const doc = makeDocument();
  const calls = { apiPostJson: [], apiPutJson: [], api: [], toast: [] };
  const apiQueue = [];
  const apiPostJsonQueue = [];

  const sandboxBase = {
    esc,
    document: doc,
    window: { CampaignRegistrationWidget: null, confirm: () => true },
    navigator: { clipboard: { writeText: () => {} } },
    state: { view: null, campaignId: null },
    $: (sel) => dom.node(sel),
    confirmSimple: () => Promise.resolve(true),
    btnStart: (btn) => { btn.disabled = true; },
    btnStop: (btn) => { btn.disabled = false; },
    toast: (msg, kind) => { calls.toast.push({ msg, kind }); },
    statePanel: () => {},
    emptyState: () => "",
    gcPill: (s) => "<span>" + esc(s) + "</span>",
    fmt: (n) => String(n),
    renderGcProviderSelect: () => {},
    fetchGcProviders: () => Promise.resolve([]),
    gcInvalidateCampaignsCache: () => {},
    api: (url) => {
      calls.api.push(url);
      const next = apiQueue.shift();
      return next ? Promise.resolve(next) : Promise.reject(new Error("no api() response queued"));
    },
    apiPost: (url) => {
      calls.apiPostJson.push({ url });
      const next = apiPostJsonQueue.shift();
      return next ? Promise.resolve(next) : Promise.reject(new Error("no apiPost() response queued"));
    },
    apiPostJson: (url, body) => {
      calls.apiPostJson.push({ url, body });
      const next = apiPostJsonQueue.shift();
      return next ? Promise.resolve(next) : Promise.reject(new Error("no apiPostJson() response queued"));
    },
    apiPutJson: (url, body) => {
      calls.apiPutJson.push({ url, body });
      const next = apiPostJsonQueue.shift();
      return next ? Promise.resolve(next) : Promise.reject(new Error("no apiPutJson() response queued"));
    },
  };
  Object.assign(sandboxBase, overrides || {});

  const fullSrc = ACTION_AND_PROVIDER_SRC +
    "\nthis.bindGcProviders = bindGcProviders; this.loadGcProviders = loadGcProviders; " +
    "this.bindCampaignRegistrationConfig = bindCampaignRegistrationConfig; " +
    "this.crCfgOnCampaignSelected = crCfgOnCampaignSelected; this.crCfgState = crCfgState; " +
    "this.GC_PROVIDER_ERROR_MESSAGES = GC_PROVIDER_ERROR_MESSAGES; this.gcProviderErrorMessage = gcProviderErrorMessage;";

  const sandbox = runInSandbox(fullSrc, sandboxBase);
  return { sandbox, dom, doc, calls, apiQueue, apiPostJsonQueue };
}

// ---------------------------------------------------------------------
// Part D — Providers screen hardening
// ---------------------------------------------------------------------

test("GC_PROVIDER_ERROR_MESSAGES maps every documented provider code to friendly, non-raw text", () => {
  const { sandbox } = loadActionAndProviderSandbox();
  ["invalid_base_url", "duplicate_provider_id", "missing_name", "invalid_allowed_campaign_types",
    "provider_base_url_required", "not_found"].forEach((code) => {
    assert.ok(sandbox.GC_PROVIDER_ERROR_MESSAGES[code], `missing friendly text for ${code}`);
    assert.doesNotMatch(sandbox.GC_PROVIDER_ERROR_MESSAGES[code], /_/, `${code}'s text must not look like a raw snake_case code`);
  });
});

test("gcProviderErrorMessage never leaks a raw code, and strips the invalid_allowed_campaign_types:<types> suffix", () => {
  const { sandbox } = loadActionAndProviderSandbox();
  assert.equal(sandbox.gcProviderErrorMessage({ d: { code: "invalid_allowed_campaign_types:tournament,external" } }),
    "Check the allowed campaign types.");
  assert.equal(sandbox.gcProviderErrorMessage({ d: { code: "some_future_code_not_yet_mapped" } }),
    "Couldn't save the provider. Try again.");
  assert.doesNotMatch(sandbox.gcProviderErrorMessage({ d: { code: "some_future_code_not_yet_mapped" } }), /_/);
});

test("provider create: a rejected response shows the mapped friendly error, never a raw code, and never a success toast", async () => {
  const { sandbox, dom, calls, apiPostJsonQueue } = loadActionAndProviderSandbox();
  dom.node("#gc-p-id").value = "mywin";
  dom.node("#gc-p-name").value = "MyWin";
  dom.node("#gc-p-type").value = "tournament";
  dom.node("#gc-p-base-url").value = "not-a-url";
  dom.node("#gc-p-url-mode").value = "query_parameter";
  apiPostJsonQueue.push({ ok: false, status: 400, d: { status: "error", code: "invalid_base_url" } });

  sandbox.bindGcProviders();
  dom.fire("#gc-create-provider-btn", "click");
  await flush();

  assert.equal(calls.toast.length, 1);
  assert.equal(calls.toast[0].msg, "❌ Enter a valid HTTPS Base URL.");
  assert.equal(calls.toast[0].kind, "error");
  assert.doesNotMatch(calls.toast[0].msg, /invalid_base_url/);
});

test("provider activate: rejected (no base_url) shows the friendly error — no unhandled rejection, no silent failure", async () => {
  const { sandbox, doc, calls, apiPostJsonQueue } = loadActionAndProviderSandbox();
  sandbox.bindGcProviders();
  apiPostJsonQueue.push({ ok: false, status: 400, d: { status: "error", code: "provider_base_url_required" } });

  const btn = { dataset: { gcpAction: "activate", id: "mywin" }, disabled: false };
  const target = { closest: () => btn };
  doc.fireClick(target);
  await flush();

  assert.equal(calls.apiPostJson.length, 1);
  assert.equal(calls.apiPostJson[0].url, "/api/admin/providers/mywin/activate");
  assert.equal(calls.toast.length, 1);
  assert.equal(calls.toast[0].msg, "❌ Add a Base URL before activating this provider.");
});

test("provider activate: a second click while the first is in flight is a no-op (in-flight guard)", async () => {
  const { sandbox, doc, calls, apiPostJsonQueue } = loadActionAndProviderSandbox();
  sandbox.bindGcProviders();
  // Never resolves during this test — simulates a slow in-flight request.
  let resolveFirst;
  const pending = new Promise((r) => { resolveFirst = r; });
  const originalApiPostJson = sandbox.apiPostJson;
  let callCount = 0;
  sandbox.apiPostJson = (url, body) => { callCount++; calls.apiPostJson.push({ url, body }); return pending; };

  const btn = { dataset: { gcpAction: "activate", id: "mywin" }, disabled: false };
  const target = { closest: () => btn };
  doc.fireClick(target);
  doc.fireClick(target); // second click, still in flight
  await flush();

  assert.equal(callCount, 1, "a second click while in flight must never issue a second request");
  resolveFirst({ ok: true, status: 200, d: { status: "ok" } });
  await flush();
});

test("provider deactivate: success shows a success toast and refreshes the list", async () => {
  const { sandbox, doc, calls, apiPostJsonQueue } = loadActionAndProviderSandbox();
  let refreshed = false;
  sandbox.loadGcProviders = () => { refreshed = true; };
  sandbox.bindGcProviders();
  apiPostJsonQueue.push({ ok: true, status: 200, d: { status: "ok" } });

  const btn = { dataset: { gcpAction: "deactivate", id: "mywin" }, disabled: false };
  const target = { closest: () => btn };
  doc.fireClick(target);
  await flush();

  assert.equal(calls.apiPostJson[0].url, "/api/admin/providers/mywin/deactivate");
  assert.ok(calls.toast.some((t) => t.kind === "success"));
  assert.ok(refreshed, "the provider list must refresh after a successful action");
});

// ---------------------------------------------------------------------
// Part A2 — Registration Configuration screen: channel field + read-merge-
// PUT of both registration and telegram.
// ---------------------------------------------------------------------

test("Registration Configuration save: sends both registration and telegram, preserving channel_id and require_subscription untouched", async () => {
  const { sandbox, dom, doc, calls, apiQueue, apiPostJsonQueue } = loadActionAndProviderSandbox();
  sandbox.crCfgState.campaignId = "reg-camp";
  sandbox.bindCampaignRegistrationConfig();

  dom.node("#cr-cfg-enabled").checked = true;
  dom.node("#cr-cfg-miniapp-visible").checked = true;
  dom.node("#cr-cfg-modal-enabled").checked = true;
  dom.node("#cr-cfg-require-channel").checked = true;
  dom.node("#cr-cfg-channel-username").value = "AdvantPlayOfficial";
  dom.node("#cr-cfg-reminder-hours").value = "24";
  dom.node("#cr-cfg-base-entries").value = "1";
  dom.node("#cr-cfg-audience-scope").value = "all";
  dom.node("#cr-cfg-audience-regions").value = "";
  dom.node("#cr-cfg-shipping-scope").value = "all";
  dom.node("#cr-cfg-shipping-regions").value = "";

  // Fresh canonical GET the save handler performs right before the PUT
  // (read-merge-PUT — never trusts whatever this form loaded with).
  apiQueue.push({
    status: "ok",
    campaign: {
      campaign_id: "reg-camp",
      telegram: { require_identity: true, require_subscription: false, channel_id: -100999, channel_username: "old" },
    },
  });
  apiPostJsonQueue.push({ ok: true, status: 200, d: { status: "ok" } });

  dom.fire("#cr-cfg-save-btn", "click");
  await flush();

  assert.equal(calls.apiPutJson.length, 1);
  const body = plain(calls.apiPutJson[0].body);
  assert.equal(body.registration.enabled, true);
  assert.equal(body.registration.require_channel_subscription, true);
  assert.equal(body.telegram.channel_username, "AdvantPlayOfficial");
  assert.equal(body.telegram.channel_id, -100999, "channel_id must be preserved untouched, never blanked");
  assert.equal(body.telegram.require_subscription, false, "the sibling require_subscription flag must be preserved untouched");
});

test("Registration Configuration save: a legacy campaign with require_subscription ABSENT preserves the effective ON default, never coerces to false", async () => {
  // Codex review (P0.17): `!!latestTelegram.require_subscription` reads
  // `undefined` as `false`, but the player runtime's actual default is
  // `true` — this must never silently disable an existing legacy
  // campaign's subscription gate via an unrelated Registration
  // Configuration save.
  const { sandbox, dom, calls, apiQueue, apiPostJsonQueue } = loadActionAndProviderSandbox();
  sandbox.crCfgState.campaignId = "legacy-camp";
  sandbox.bindCampaignRegistrationConfig();

  dom.node("#cr-cfg-enabled").checked = true;
  dom.node("#cr-cfg-require-channel").checked = false;
  dom.node("#cr-cfg-channel-username").value = "";
  dom.node("#cr-cfg-reminder-hours").value = "24";
  dom.node("#cr-cfg-base-entries").value = "1";
  dom.node("#cr-cfg-audience-scope").value = "all";
  dom.node("#cr-cfg-audience-regions").value = "";
  dom.node("#cr-cfg-shipping-scope").value = "all";
  dom.node("#cr-cfg-shipping-regions").value = "";

  apiQueue.push({
    status: "ok",
    // No `telegram` block at all — the legacy case.
    campaign: { campaign_id: "legacy-camp" },
  });
  apiPostJsonQueue.push({ ok: true, status: 200, d: { status: "ok" } });

  dom.fire("#cr-cfg-save-btn", "click");
  await flush();

  assert.equal(calls.apiPutJson.length, 1);
  const body = plain(calls.apiPutJson[0].body);
  assert.equal(body.telegram.require_subscription, true,
    "must preserve the runtime's effective ON default, never coerce an absent field to false");
});

test("Registration Configuration save: blocked client-side when requiring a channel with none configured and no channel_id on file", async () => {
  const { sandbox, dom, calls } = loadActionAndProviderSandbox();
  sandbox.crCfgState.campaignId = "reg-camp";
  sandbox.crCfgState.telegramChannelId = null;
  sandbox.bindCampaignRegistrationConfig();

  dom.node("#cr-cfg-enabled").checked = true;
  dom.node("#cr-cfg-require-channel").checked = true;
  dom.node("#cr-cfg-channel-username").value = ""; // left blank
  dom.node("#cr-cfg-reminder-hours").value = "24";
  dom.node("#cr-cfg-base-entries").value = "1";
  dom.node("#cr-cfg-audience-scope").value = "all";
  dom.node("#cr-cfg-audience-regions").value = "";
  dom.node("#cr-cfg-shipping-scope").value = "all";
  dom.node("#cr-cfg-shipping-regions").value = "";

  dom.fire("#cr-cfg-save-btn", "click");
  await flush();

  assert.equal(calls.apiPutJson.length, 0, "must never PUT an unsatisfiable require_channel_subscription=true/no-channel config");
  assert.ok(calls.toast.some((t) => /Add a channel username/.test(t.msg)));
});

test("Registration Configuration save: allowed when a channel_id is already on file, even with the username field blank", async () => {
  const { sandbox, dom, calls, apiQueue, apiPostJsonQueue } = loadActionAndProviderSandbox();
  sandbox.crCfgState.campaignId = "reg-camp";
  sandbox.crCfgState.telegramChannelId = -100777; // set by a prior crCfgWriteForm() from the canonical GET
  sandbox.bindCampaignRegistrationConfig();

  dom.node("#cr-cfg-enabled").checked = true;
  dom.node("#cr-cfg-require-channel").checked = true;
  dom.node("#cr-cfg-channel-username").value = "";
  dom.node("#cr-cfg-reminder-hours").value = "24";
  dom.node("#cr-cfg-base-entries").value = "1";
  dom.node("#cr-cfg-audience-scope").value = "all";
  dom.node("#cr-cfg-audience-regions").value = "";
  dom.node("#cr-cfg-shipping-scope").value = "all";
  dom.node("#cr-cfg-shipping-regions").value = "";

  apiQueue.push({
    status: "ok",
    campaign: { campaign_id: "reg-camp", telegram: { require_identity: true, require_subscription: false, channel_id: -100777, channel_username: "" } },
  });
  apiPostJsonQueue.push({ ok: true, status: 200, d: { status: "ok" } });

  dom.fire("#cr-cfg-save-btn", "click");
  await flush();

  assert.equal(calls.apiPutJson.length, 1);
  const body = plain(calls.apiPutJson[0].body);
  assert.equal(body.telegram.channel_id, -100777);
});
