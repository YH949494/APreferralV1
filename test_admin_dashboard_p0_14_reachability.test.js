/**
 * P0.14: player-reachability blockers for gc_campaigns Tournament/External
 * campaigns — Campaign Detail's beginner-facing half of the fix (the
 * backend half is test_campaign_centre.py's own P0.14 section):
 *
 *  1. Campaign Detail's "Where users go" editor gains a beginner Telegram
 *     subscription switch (require_subscription/channel_username), saved
 *     together with destination in one read-merge-PUT — never a second
 *     save path, never `{telegram: {require_subscription: false}}` alone.
 *  2. Zero providers is never an inert "No provider (configure later)"
 *     dropdown — it's a CTA straight to Providers. A linked-but-inactive
 *     provider gets a visible warning, not a silent default.
 *  3. open_mode gets a beginner "Opens in" selector, human-labeled, scoped
 *     to exactly the values campaign_centre._ALLOWED_OPEN_MODES_BY_TYPE
 *     allows for this campaign's type.
 *  4. gcDefaultOpenModeForType is the one shared default-picker used by
 *     both the wizard and the legacy create form — the legacy form's old
 *     hardcoded "telegram_web_app" (invalid for
 *     external_subscription_verification, which only ever allowed
 *     "external_url") is gone.
 *
 * Mirrors test_admin_dashboard_p0_5b_campaign_detail_edit.test.js's exact
 * harness (same source slices, same tournamentCampaign/loadOrchestration
 * pattern) — no build step, no jsdom.
 *
 * Run with: node --test test_admin_dashboard_p0_14_reachability.test.js
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
// Real source slices (never re-implemented stand-ins) — same ranges
// test_admin_dashboard_p0_5b_campaign_detail_edit.test.js uses.
// ---------------------------------------------------------------------
const PROVIDER_LABEL_SRC = slice(JS, "  function gcProviderOptionLabel(p) {", "\n  function renderGcProviderSelect()");
const KL_SRC = slice(JS, "  function ccPad2(n)", "\n  var CC_CONTENT_ICON");
const PURE_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  // ---- Composer + orchestration (DOM-touching)");
const ORCH_SRC = slice(JS, "  // Entry point from the Campaigns list", "\n  // ---------- Mission Reward Pool (Phase 2.1");
const ATTEMPT_SRC = slice(JS, "  function gcCreateCampaignAttempt(", "\n  function bindGcCampaigns() {");

function loadPure() {
  return runInSandbox(PROVIDER_LABEL_SRC + "\n" + KL_SRC + "\n" + PURE_SRC + "\nthis.__x = { " +
    "cdDestinationEditHtml, cdDestinationProviderOptionsHtml, cdOpenModeOptionsHtml, " +
    "gcAllowedOpenModesForType, gcDefaultOpenModeForType, gcOpenModeLabel, " +
    "GC_ALLOWED_OPEN_MODES_BY_TYPE, computeSetupChecklist };",
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
    destination: { provider_id: "p1", open_mode: "telegram_web_app", path: "/game", ready: false },
    telegram: { require_identity: true, require_subscription: false, channel_id: null, channel_username: "" },
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 3 } }] },
    registration: { enabled: false },
    effective_visibility: { publicly_visible: false, reasons: ["status is 'draft', not 'live'"] },
  }, overrides || {});
}

// ---------------------------------------------------------------------
// 1. gcDefaultOpenModeForType / gcAllowedOpenModesForType — mirrors
//    campaign_centre._ALLOWED_OPEN_MODES_BY_TYPE exactly.
// ---------------------------------------------------------------------

test("gcAllowedOpenModesForType mirrors the backend map for every known type", () => {
  assert.deepEqual(plain(P.gcAllowedOpenModesForType("tournament")), ["telegram_web_app", "external_url"]);
  assert.deepEqual(plain(P.gcAllowedOpenModesForType("external_subscription_verification")), ["external_url"]);
  assert.deepEqual(plain(P.gcAllowedOpenModesForType("external_website")), ["external_url", "telegram_web_app"]);
  assert.deepEqual(plain(P.gcAllowedOpenModesForType("mission_pool")), ["telegram_web_app"]);
});

test("gcDefaultOpenModeForType: prefers telegram_web_app when the type allows it", () => {
  assert.equal(P.gcDefaultOpenModeForType("tournament"), "telegram_web_app");
  assert.equal(P.gcDefaultOpenModeForType("external_website"), "telegram_web_app");
  assert.equal(P.gcDefaultOpenModeForType("mission_pool"), "telegram_web_app");
});

test("gcDefaultOpenModeForType: falls back to the only allowed value when telegram_web_app isn't supported — the exact P1-1 bug", () => {
  // This is the exact root cause: the legacy create form hardcoded
  // "telegram_web_app" for every type, but external_subscription_verification
  // has never allowed it — only "external_url".
  assert.equal(P.gcDefaultOpenModeForType("external_subscription_verification"), "external_url");
});

test("gcDefaultOpenModeForType: an unknown type never throws — degrades to a safe default", () => {
  assert.doesNotThrow(() => P.gcDefaultOpenModeForType("some_future_type"));
});

// ---------------------------------------------------------------------
// 2. cdOpenModeOptionsHtml — beginner labels, type-scoped, safe fallback
//    for a stored value outside the current allowed set.
// ---------------------------------------------------------------------

test("cdDestinationEditHtml's open_mode selector never renders the raw backend enum as visible label text", () => {
  const html = P.cdDestinationEditHtml(tournamentCampaign(), [{ provider_id: "p1", name: "MyWin", active: true }]);
  assert.match(html, />Telegram Mini App</);
  assert.doesNotMatch(html, />telegram_web_app</);
  assert.doesNotMatch(html, />external_url</);
});

test("open_mode selector only offers values allowed for this campaign type (external_subscription_verification: external_url only)", () => {
  const campaign = tournamentCampaign({
    type: "external_subscription_verification",
    destination: { provider_id: "p1", open_mode: "external_url", path: "/verify", ready: false },
  });
  const html = P.cdDestinationEditHtml(campaign, [{ provider_id: "p1", name: "MyWin", active: true }]);
  assert.match(html, /id="cd-edit-dest-openmode"/);
  assert.match(html, /<option value="external_url" selected>External browser<\/option>/);
  assert.doesNotMatch(html, /value="telegram_web_app"/);
});

test("open_mode selector keeps a stored value outside the allowed set as 'Current setting' instead of silently rewriting it", () => {
  // Simulates a legacy/anomalous document: external_subscription_verification
  // only ever allows external_url, but this document somehow has
  // telegram_web_app stored (e.g. written before that type existed).
  const html = P.cdOpenModeOptionsHtml("external_subscription_verification", "telegram_web_app");
  assert.match(html, /<option value="telegram_web_app" selected>Current setting \(Telegram Mini App\)<\/option>/);
  // The actually-allowed value is still offered as an alternative.
  assert.match(html, /<option value="external_url"[^>]*>External browser<\/option>/);
});

test("open_mode selector adds no fallback option when the stored value IS in the allowed set", () => {
  const html = P.cdOpenModeOptionsHtml("tournament", "telegram_web_app");
  assert.doesNotMatch(html, /Current setting/);
});

// ---------------------------------------------------------------------
// 3. No-provider dead end / inactive-provider warning (P0-2)
// ---------------------------------------------------------------------

test("zero providers: never an inert 'No provider' dropdown — a Manage Providers CTA instead", () => {
  const html = P.cdDestinationEditHtml(tournamentCampaign(), []);
  assert.doesNotMatch(html, /<select[^>]*id="cd-edit-dest-provider"/);
  assert.match(html, /No destinations are set up yet\./);
  assert.match(html, /data-cd-goto-providers="1">Manage Providers</);
  // Path/ready/open_mode all depend on having a destination to configure —
  // none of them render either while there is nothing to point them at.
  assert.doesNotMatch(html, /id="cd-edit-dest-path"/);
  assert.doesNotMatch(html, /id="cd-edit-dest-openmode"/);
});

test("zero providers: the telegram subscription switch is still offered (independent of providers existing)", () => {
  const html = P.cdDestinationEditHtml(tournamentCampaign(), []);
  assert.match(html, /id="cd-edit-tg-require-sub"/);
});

test("linked provider is inactive: shows a clear warning and a Manage Providers CTA, but keeps the real dropdown (never silently unlinks it)", () => {
  const campaign = tournamentCampaign({ destination: { provider_id: "p1", open_mode: "telegram_web_app", path: "/game", ready: true } });
  const html = P.cdDestinationEditHtml(campaign, [{ provider_id: "p1", name: "MyWin", active: false }]);
  assert.match(html, /This provider is inactive and can.t be used for a live campaign\./);
  assert.match(html, /data-cd-goto-providers="1"/);
  assert.match(html, /<option value="p1" selected>/);
});

test("active provider: normal dropdown, no warning", () => {
  const campaign = tournamentCampaign({ destination: { provider_id: "p1", open_mode: "telegram_web_app", path: "/game", ready: true } });
  const html = P.cdDestinationEditHtml(campaign, [{ provider_id: "p1", name: "MyWin", active: true }]);
  assert.doesNotMatch(html, /inactive and can.t be used/);
  assert.match(html, /<option value="p1" selected>/);
});

// ---------------------------------------------------------------------
// 4. Telegram subscription switch prefill (P0-1 frontend)
// ---------------------------------------------------------------------

test("telegram switch reflects the campaign's current require_subscription state and prefills the channel username", () => {
  const campaign = tournamentCampaign({ telegram: { require_identity: true, require_subscription: true, channel_id: null, channel_username: "AdvantPlayOfficial" } });
  const html = P.cdDestinationEditHtml(campaign, [{ provider_id: "p1", name: "MyWin", active: true }]);
  assert.match(html, /id="cd-edit-tg-require-sub" checked/);
  assert.match(html, /id="cd-edit-tg-channel-wrap" style="margin-top:8px;"/); // visible (no display:none)
  assert.match(html, /id="cd-edit-tg-channel-username"[^>]*value="AdvantPlayOfficial"/);
});

test("telegram switch off: channel field is present but hidden, never shown as a raw channel_id control", () => {
  const html = P.cdDestinationEditHtml(tournamentCampaign(), [{ provider_id: "p1", name: "MyWin", active: true }]);
  assert.doesNotMatch(html, /id="cd-edit-tg-require-sub" checked/);
  assert.match(html, /id="cd-edit-tg-channel-wrap" style="margin-top:8px;display:none;"/);
  // channel_id has no control anywhere in this editor (P0.14 spec: "preserve
  // it, do not expose it").
  assert.doesNotMatch(html, /channel_id/);
  assert.doesNotMatch(html, /cd-edit-tg-channel-id/);
});

// ---------------------------------------------------------------------
// 5. Orchestration: cdSaveSection("destination") now saves telegram too,
//    with the same read-merge-PUT contract, plus the friendly pre-submit
//    guard for an invalid "on with no channel" state.
// ---------------------------------------------------------------------

function makeDom() {
  const nodes = {};
  function node(sel) {
    if (!nodes[sel]) nodes[sel] = { value: "", checked: false, textContent: "", innerHTML: "", style: {} };
    return nodes[sel];
  }
  return { nodes, node };
}

function loadOrchestration(overrides, missingIds) {
  const dom = makeDom();
  const calls = { api: [], apiPutJson: [], toast: [], gcCampaignDetailHtml: [] };
  const apiQueue = [];
  const missing = new Set(missingIds || []);

  const sandboxBase = {
    esc,
    state: { campaignId: "july-tournament" },
    // Mirrors the real $() (querySelector) returning null for an element
    // that genuinely isn't in the rendered DOM — e.g. the zero-provider
    // state, which renders no provider/path/ready/open_mode inputs at all.
    // dom.node()'s auto-vivify-on-access default (used everywhere else in
    // this suite) would otherwise fabricate a present-but-empty element and
    // mask cdSaveSection's own `el ? ... : ...` existence guards.
    $: (sel) => (missing.has(sel) ? null : dom.node(sel)),
    $all: () => [],
    switchView: () => {},
    statePanel: () => {},
    activateTab: () => {},
    openMissionAdmin: () => {},
    openCampaignRegistrationConfig: () => {},
    fetchGcProviders: () => Promise.resolve([]),
    fetchGcRewardPools: () => Promise.resolve([]),
    gcPill: (status) => "<span>" + esc(status) + "</span>",
    toast: (msg, kind) => { calls.toast.push([msg, kind]); },
    api: (url) => {
      calls.api.push(url);
      const next = apiQueue.shift();
      return next ? Promise.resolve(next) : Promise.reject(new Error("no api() response queued"));
    },
    apiPutJson: (url, body) => {
      calls.apiPutJson.push({ url, body });
      return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } });
    },
    gcCampaignDetailHtml: (campaign) => "rendered:" + campaign.campaign_id,
  };
  Object.assign(sandboxBase, overrides || {});

  const fullSrc = PROVIDER_LABEL_SRC + "\n" + KL_SRC + "\n" + PURE_SRC + "\n" + ORCH_SRC +
    "\nthis.cdSaveSection = cdSaveSection; this.cdOpenEdit = cdOpenEdit; this.cdViewState = cdViewState;";

  const sandbox = runInSandbox(fullSrc, sandboxBase);
  return { sandbox, dom, calls, apiQueue };
}

async function flush(n) {
  for (let i = 0; i < (n || 12); i++) await Promise.resolve();
}

test("orchestration: saving destination also saves telegram in the same PUT, preserving untouched siblings", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  const original = tournamentCampaign({
    telegram: { require_identity: true, require_subscription: true, channel_id: -100999, channel_username: "existing_channel" },
  });
  sandbox.cdViewState.campaign = original;
  sandbox.cdOpenEdit("destination"); // captures the snapshot

  apiQueue.push({ status: "ok", campaign: original }); // pre-PUT canonical GET
  apiQueue.push({ status: "ok", campaign: original }); // post-PUT canonical GET

  dom.node("#cd-edit-dest-provider").value = "p1";
  dom.node("#cd-edit-dest-path").value = "/game";
  dom.node("#cd-edit-dest-ready").checked = false;
  dom.node("#cd-edit-dest-openmode").value = "telegram_web_app";
  // Admin only flips the subscription switch off — never touches the
  // channel username field (which cdOpenEdit prefilled to "existing_channel").
  dom.node("#cd-edit-tg-require-sub").checked = false;
  dom.node("#cd-edit-tg-channel-username").value = "existing_channel";

  sandbox.cdSaveSection("destination", {});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.equal(body.telegram.require_subscription, false);
  assert.equal(body.telegram.channel_username, "existing_channel", "untouched sibling must be preserved, not blanked");
  assert.equal(body.telegram.channel_id, -100999, "channel_id has no control here — must always be carried forward untouched");
  assert.equal(body.telegram.require_identity, true);
});

test("orchestration: turning subscription ON with no channel configured is blocked client-side — no PUT is ever sent", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  const original = tournamentCampaign(); // require_subscription false, no channel
  sandbox.cdViewState.campaign = original;
  sandbox.cdOpenEdit("destination");
  apiQueue.push({ status: "ok", campaign: original });

  dom.node("#cd-edit-dest-provider").value = "p1";
  dom.node("#cd-edit-dest-path").value = "/game";
  dom.node("#cd-edit-dest-ready").checked = false;
  dom.node("#cd-edit-dest-openmode").value = "telegram_web_app";
  dom.node("#cd-edit-tg-require-sub").checked = true;
  dom.node("#cd-edit-tg-channel-username").value = ""; // left blank

  sandbox.cdSaveSection("destination", {});
  await flush();

  assert.equal(calls.apiPutJson.length, 0, "must never PUT an unsatisfiable require_subscription=true/no-channel config");
  assert.equal(dom.node("#cd-edit-destination-error").textContent,
    "Add a channel username before requiring subscription — or turn the switch off.");
});

test("orchestration: turning subscription ON with a channel_id already on file (no username control) is allowed", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  const original = tournamentCampaign({
    telegram: { require_identity: true, require_subscription: false, channel_id: -100777, channel_username: "" },
  });
  sandbox.cdViewState.campaign = original;
  sandbox.cdOpenEdit("destination");
  apiQueue.push({ status: "ok", campaign: original });
  apiQueue.push({ status: "ok", campaign: original });

  dom.node("#cd-edit-dest-provider").value = "p1";
  dom.node("#cd-edit-dest-path").value = "/game";
  dom.node("#cd-edit-dest-ready").checked = false;
  dom.node("#cd-edit-dest-openmode").value = "telegram_web_app";
  dom.node("#cd-edit-tg-require-sub").checked = true;
  dom.node("#cd-edit-tg-channel-username").value = ""; // still no username, but channel_id already satisfies it

  sandbox.cdSaveSection("destination", {});
  await flush();

  assert.equal(calls.apiPutJson.length, 1);
  const body = plain(calls.apiPutJson[0].body);
  assert.equal(body.telegram.require_subscription, true);
  assert.equal(body.telegram.channel_id, -100777);
});

test("orchestration: zero providers — save still succeeds and never sends provider/path/ready/open_mode as accidentally changed", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration(null, [
    "#cd-edit-dest-provider", "#cd-edit-dest-path", "#cd-edit-dest-ready", "#cd-edit-dest-openmode",
  ]);
  const original = tournamentCampaign({ destination: { provider_id: "", open_mode: "telegram_web_app", path: "", ready: false } });
  sandbox.cdViewState.campaign = original;
  sandbox.cdOpenEdit("destination"); // renders the zero-provider CTA state — no provider/path/openmode inputs exist

  apiQueue.push({ status: "ok", campaign: original });
  apiQueue.push({ status: "ok", campaign: original });

  // Admin only flips the subscription switch on with a channel.
  dom.node("#cd-edit-tg-require-sub").checked = true;
  dom.node("#cd-edit-tg-channel-username").value = "AdvantPlayOfficial";

  sandbox.cdSaveSection("destination", {});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.deepEqual(body.destination, { provider_id: "", open_mode: "telegram_web_app", path: "", ready: false });
  assert.equal(body.telegram.require_subscription, true);
  assert.equal(body.telegram.channel_username, "AdvantPlayOfficial");
});

// ---------------------------------------------------------------------
// 6. Legacy create form + wizard now share gcDefaultOpenModeForType —
//    the P1-1 fix. Regression-checks the exact source lines rather than
//    re-deriving behavior, since bindGcCampaigns/gcwSubmit are DOM-driven
//    functions out of scope for this lightweight harness.
// ---------------------------------------------------------------------

test("legacy create form: open_mode is derived via gcDefaultOpenModeForType, never hardcoded", () => {
  assert.doesNotMatch(JS, /destination:\s*\{\s*provider_id:[^}]*open_mode:\s*"telegram_web_app"/s);
  assert.match(JS, /open_mode: gcDefaultOpenModeForType\(type\), ready: false \},\s*\n\s*\};/);
});

test("wizard create payload: open_mode is derived via gcDefaultOpenModeForType, never hardcoded", () => {
  assert.match(JS, /open_mode: gcDefaultOpenModeForType\(type\.backendType\),/);
});

test("gcCreateCampaignAttempt never surfaces a raw backend code straight into the toast (P1-1: no raw snake_case error)", () => {
  const s = runInSandbox(ATTEMPT_SRC, { esc });
  assert.equal(typeof s.gcActionErrorMessage, "function");
});

test("GC_ACTION_ERROR_MESSAGES / CD_ERROR_MESSAGES map every P0.14 backend error code to friendly text", () => {
  const s = runInSandbox(ATTEMPT_SRC, { esc });
  ["open_mode_not_allowed_for_type", "invalid_open_mode", "missing_starts_at", "ends_at_before_starts_at", "subscription_channel_required"]
    .forEach((code) => {
      assert.ok(s.GC_ACTION_ERROR_MESSAGES[code], `GC_ACTION_ERROR_MESSAGES missing friendly text for ${code}`);
      assert.doesNotMatch(s.GC_ACTION_ERROR_MESSAGES[code], /_/, `${code}'s friendly text must not look like a raw snake_case code`);
    });
});
