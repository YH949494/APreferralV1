/**
 * P0.5b: Campaign Detail — safe inline editing for the common gc_campaigns
 * setup fields (Campaign name/description, When/schedule, Registration
 * enabled+required_fields, Where users go/destination), built on top of
 * static/admin-dashboard.js's P0.5a Campaign Detail (test_admin_dashboard_
 * p0_5a_campaign_detail.test.js), which stays green and read-only for
 * Mission/Rewards.
 *
 * The central backend hazard this suite guards against (see the audit
 * comments above CD_EDITABLE_KEYS and cdSaveSection in admin-dashboard.js,
 * and campaign_centre._validate_body): a gc_campaigns PUT is partial only
 * at the TOP-LEVEL block level. The instant a nested block's key (schedule/
 * destination/registration) is present in the PUT body at all, the ENTIRE
 * nested block is reconstructed from only the sub-fields present in that
 * body, silently defaulting every sibling that isn't included — sending
 * `{destination: {ready: true}}` alone would erase provider_id/path/
 * open_mode back to their defaults. So every save here must:
 *   1. GET the canonical campaign immediately before building the PUT body
 *      (never merge against this page's possibly-stale in-memory snapshot)
 *   2. copy the COMPLETE existing nested block from that fresh GET
 *   3. override only the field(s) the form actually edits
 *   4. PUT the complete block
 *   5. GET canonical again before recomputing checklist/progress state
 *
 * Mirrors test_admin_dashboard_p0_5a_campaign_detail.test.js's harness: no
 * build step, no jsdom — relevant source ranges are extracted as text and
 * executed in sandboxed vm contexts against small DOM/fetch stand-ins.
 *
 * Run with: node --test test_admin_dashboard_p0_5b_campaign_detail_edit.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { execFileSync } = require("node:child_process");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const JS = fs.readFileSync(JS_PATH, "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

// Objects built inside a vm sandbox belong to that sandbox's own realm, so
// assert/strict's reference-sensitive checks (Object.prototype, Array.prototype)
// never treat them as equal to a plain literal written in this file even
// when every field matches — see test_admin_dashboard_p0_5a_campaign_detail
// .test.js's identical `plain()` helper. A JSON round-trip normalizes a
// vm-realm value back into this file's realm; safe here since every PUT
// body in play is JSON-safe (strings/numbers/booleans/null/arrays/objects).
function plain(v) { return JSON.parse(JSON.stringify(v)); }

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// Mutates and returns the SAME object passed in as `sandboxExtra` (rather
// than merging it into a fresh wrapper) — some callers below (loadOrchestration)
// need to keep setting properties (e.g. __putBehavior) on that exact object
// after the sandbox runs and have closures created inside the sandbox (e.g.
// apiPutJson, which is itself a host function referencing sandboxExtra by
// closure) observe the same live values.
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
// Real source slices (never re-implemented stand-ins) — same PURE_SRC
// range test_admin_dashboard_p0_5a_campaign_detail.test.js uses, plus the
// KL<->UTC fixed-offset converters (Composer's, reused as-is by the "When"
// editor) and gcProviderOptionLabel (P0.3's provider label formatter,
// reused as-is by the destination editor), plus the DOM-touching
// orchestration block that owns cdSaveSection/cdOpenEdit.
// ---------------------------------------------------------------------
const PROVIDER_LABEL_SRC = slice(JS, "  function gcProviderOptionLabel(p) {", "\n  function renderGcProviderSelect()");
const KL_SRC = slice(JS, "  function ccPad2(n)", "\n  var CC_CONTENT_ICON");
const PURE_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  // ---- Composer + orchestration (DOM-touching)");
const ORCH_SRC = slice(JS, "  // Entry point from the Campaigns list", "\n  // ---------- Mission Reward Pool (Phase 2.1");

function loadPure() {
  return runInSandbox(PROVIDER_LABEL_SRC + "\n" + KL_SRC + "\n" + PURE_SRC + "\nthis.__x = { " +
    "CD_EDITABLE_KEYS, cdDefaultRegistrationConfig, CD_REGISTRATION_FIELD_ORDER, " +
    "cdCampaignEditHtml, cdWhenEditHtml, cdRegistrationEditHtml, cdDestinationEditHtml, " +
    "cdDestinationProviderOptionsHtml, gcCampaignDetailChecklistHtml, computeSetupChecklist, " +
    "ccKlInputToUtcIso, ccUtcToKlInputValue, ccUtcToKlDisplay, gcProviderOptionLabel };",
    { esc }).__x;
}

const P = loadPure();

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------
function tournamentCampaign(overrides) {
  return Object.assign({
    campaign_id: "july-tournament",
    name: "July Tournament",
    type: "tournament",
    status: "draft",
    description: "Top 3 leaderboard prize",
    schedule: { starts_at: "2026-10-01T01:00:00+00:00", ends_at: "2026-10-31T15:59:00+00:00", timezone: "Asia/Kuala_Lumpur" },
    destination: { provider_id: "p1", open_mode: "telegram_web_app", path: "/game", ready: false },
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 3 } }] },
    registration: { enabled: false },
    effective_visibility: { publicly_visible: false, reasons: ["status is 'draft', not 'live'"] },
  }, overrides || {});
}

function fullRegistrationBlock(overrides) {
  return Object.assign({
    enabled: true,
    miniapp_visible: true,
    modal_enabled: false, // deliberately non-default to prove it's preserved
    required_fields: ["full_name", "contact_number"],
    reminder_hours: 48, // deliberately non-default
    audience: { scope: "selected", regions: ["KL", "Penang"] },
    shipping: { scope: "all", regions: [] },
    require_channel_subscription: true, // deliberately non-default
    base_entries: 3, // deliberately non-default
  }, overrides || {});
}

// ---------------------------------------------------------------------
// C. Schedule — KL <-> UTC conversion (real converters, not stand-ins)
// ---------------------------------------------------------------------
test("ccKlInputToUtcIso: KL datetime-local input converts to the correct UTC instant (fixed +8h offset)", () => {
  // 2026-10-01 08:00 in Kuala Lumpur (UTC+8) is 2026-10-01 00:00 UTC.
  const iso = P.ccKlInputToUtcIso("2026-10-01T08:00");
  assert.equal(new Date(iso).toISOString(), "2026-10-01T00:00:00.000Z");
});

test("ccUtcToKlInputValue: a UTC instant converts back to the correct KL datetime-local value", () => {
  assert.equal(P.ccUtcToKlInputValue("2026-10-01T00:00:00.000Z"), "2026-10-01T08:00");
});

test("KL -> UTC -> KL round-trip is exact for a range of instants", () => {
  ["2026-01-01T00:00", "2026-06-15T23:45", "2026-12-31T16:00"].forEach((klInput) => {
    const utcIso = P.ccKlInputToUtcIso(klInput);
    const backToKl = P.ccUtcToKlInputValue(utcIso);
    assert.equal(backToKl, klInput, "round-trip failed for " + klInput);
  });
});

test("ccKlInputToUtcIso returns null for an empty/invalid value (no end date case)", () => {
  assert.equal(P.ccKlInputToUtcIso(""), null);
  assert.equal(P.ccKlInputToUtcIso(null), null);
});

test("no end date displays as 'No end date', not a missing/invalid value", () => {
  assert.equal(P.ccUtcToKlInputValue(null), "");
  assert.equal(P.ccUtcToKlInputValue(""), "");
});

// ---------------------------------------------------------------------
// Pure edit-form builders — prefill correctness, no raw provider_id shown
// ---------------------------------------------------------------------
test("cdCampaignEditHtml prefills name/description from the campaign, never campaign_id/type", () => {
  const html = P.cdCampaignEditHtml(tournamentCampaign());
  assert.match(html, /value="July Tournament"/);
  assert.match(html, />Top 3 leaderboard prize</);
  assert.doesNotMatch(html, /july-tournament/);
  assert.doesNotMatch(html, /cd-edit-type/);
});

test("cdWhenEditHtml prefills start/end in KL local values and shows 'No end date' unchecked when an end exists", () => {
  const html = P.cdWhenEditHtml(tournamentCampaign());
  assert.match(html, /id="cd-edit-starts"[^>]*value="2026-10-01T09:00"/);
  assert.match(html, /id="cd-edit-ends"[^>]*value="2026-10-31T23:59"/);
  assert.doesNotMatch(html, /id="cd-edit-no-end"[^>]* checked/);
});

test("cdWhenEditHtml checks 'No end date' and hides the end field when there is no end date", () => {
  const campaign = tournamentCampaign({ schedule: { starts_at: "2026-10-01T01:00:00+00:00", ends_at: null } });
  const html = P.cdWhenEditHtml(campaign);
  assert.match(html, /id="cd-edit-no-end" checked/);
  assert.match(html, /id="cd-edit-ends-wrap" style="display:none;"/);
});

test("cdDestinationEditHtml never renders the raw provider_id as visible text (only as the option value)", () => {
  const providers = [{ provider_id: "RAW-PROVIDER-ID-42", name: "MyWin Tournament Site", type: "tournament", active: true }];
  const campaign = tournamentCampaign({ destination: { provider_id: "RAW-PROVIDER-ID-42", open_mode: "telegram_web_app", path: "/game", ready: false } });
  const html = P.cdDestinationEditHtml(campaign, providers);
  // The id appears exactly once, as the <option value="...">, never as
  // visible label text (human-readable name/type/active state instead).
  const idOccurrences = (html.match(/RAW-PROVIDER-ID-42/g) || []).length;
  assert.equal(idOccurrences, 1);
  assert.match(html, /value="RAW-PROVIDER-ID-42" selected>MyWin Tournament Site — tournament \(active\)</);
});

test("cdDestinationProviderOptionsHtml shows an inactive provider's state in its label (E: inactive provider handled)", () => {
  const providers = [{ provider_id: "p2", name: "Old Site", type: "external_url", active: false }];
  const html = P.cdDestinationProviderOptionsHtml(providers, "");
  assert.match(html, /Old Site — external_url \(inactive\)/);
});

// Codex review finding: /api/admin/providers caps at 200 rows, so a
// campaign's linked provider can be older than the newest 200 and absent
// from the fetched list. Without a fallback option the <select> silently
// defaults to "No provider", and saving path/ready without touching the
// dropdown would unlink a still-valid provider.
test("cdDestinationProviderOptionsHtml keeps a linked-but-unlisted provider selected (never silently defaults to 'No provider')", () => {
  const html = P.cdDestinationProviderOptionsHtml([{ provider_id: "some-other-provider", name: "Other", active: true }], "OLD-PROVIDER-NOT-IN-TOP-200");
  assert.match(html, /<option value="OLD-PROVIDER-NOT-IN-TOP-200" selected>/);
  // Never shown as visible label text — only as the option's value.
  const visibleTextMatch = html.match(/<option[^>]*value="OLD-PROVIDER-NOT-IN-TOP-200"[^>]*>([^<]*)</);
  assert.doesNotMatch(visibleTextMatch[1], /OLD-PROVIDER-NOT-IN-TOP-200/);
});

test("cdDestinationProviderOptionsHtml adds no fallback option when the linked provider IS in the list, or none is linked", () => {
  const providers = [{ provider_id: "p1", name: "MyWin", active: true }];
  assert.doesNotMatch(P.cdDestinationProviderOptionsHtml(providers, "p1"), /not in the loaded list/);
  assert.doesNotMatch(P.cdDestinationProviderOptionsHtml(providers, ""), /not in the loaded list/);
});

test("cdRegistrationEditHtml only offers the four backend-recognized required_fields keys", () => {
  const html = P.cdRegistrationEditHtml(tournamentCampaign({ registration: fullRegistrationBlock() }));
  ["full_name", "contact_number", "country_region", "delivery_address"].forEach((key) => {
    assert.match(html, new RegExp('value="' + key + '"'));
  });
  assert.match(html, /value="full_name" checked/);
  assert.match(html, /value="contact_number" checked/);
  assert.doesNotMatch(html, /value="country_region" checked/);
});

test("gcCampaignDetailChecklistHtml renders an Edit button (not a conditional Set-up button) for every editable row, complete or not", () => {
  const rows = P.computeSetupChecklist(tournamentCampaign(), [{ provider_id: "p1", name: "MyWin", active: true }], []);
  const html = P.gcCampaignDetailChecklistHtml(rows, { editingSection: null, campaign: tournamentCampaign(), providers: [] });
  assert.match(html, /data-cd-edit="campaign">Edit</);
  assert.match(html, /data-cd-edit="when">Edit</);
  assert.match(html, /data-cd-edit="destination">Edit</);
});

test("gcCampaignDetailChecklistHtml renders the destination edit form in place when editingSection matches, read view otherwise", () => {
  const campaign = tournamentCampaign();
  const rows = P.computeSetupChecklist(campaign, [{ provider_id: "p1", name: "MyWin", active: true }], []);
  const html = P.gcCampaignDetailChecklistHtml(rows, { editingSection: "destination", campaign: campaign, providers: [{ provider_id: "p1", name: "MyWin", active: true }] });
  assert.match(html, /id="cd-edit-dest-provider"/);
  assert.match(html, /data-cd-save="destination"/);
  // Campaign/When rows are untouched (still read view with an Edit button).
  assert.match(html, /data-cd-edit="campaign">Edit</);
  assert.doesNotMatch(html, /id="cd-edit-name"/);
});

// ---------------------------------------------------------------------
// Orchestration sandbox: cdSaveSection / cdOpenEdit — this is where the
// read-merge-PUT contract and the friendly-error mapping actually live.
// ---------------------------------------------------------------------
function makeDom() {
  const nodes = {};
  function node(sel) {
    if (!nodes[sel]) nodes[sel] = { value: "", checked: false, textContent: "", innerHTML: "", style: {} };
    return nodes[sel];
  }
  return { nodes, node };
}

// Runs the FULL chain (provider labels + KL converters + pure builders +
// orchestration) in one sandbox — cdSaveSection genuinely depends on
// cdDefaultRegistrationConfig/CD_REGISTRATION_FIELD_ORDER (PURE_SRC) and
// ccKlInputToUtcIso (KL_SRC), so a real end-to-end test needs all of it,
// not a re-implemented stand-in that could quietly drift from the source.
//
// The returned object IS the sandbox global object itself (not a separate
// "control" object) — apiPutJson/$all close over these exact properties
// (__putBehavior/__regFields), so a test sets them directly on what this
// function returns and the closures see the same live values.
function loadOrchestration(overrides) {
  const dom = makeDom();
  const calls = { api: [], apiPutJson: [], toast: [], gcCampaignDetailHtml: [] };
  const apiQueue = [];

  const sandboxBase = {
    esc,
    state: { campaignId: "july-tournament" },
    $: (sel) => dom.node(sel),
    $all: (sel) => (sandboxBase.__regFields && sel === ".cd-edit-reg-field" ? sandboxBase.__regFields : []),
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
      const behavior = sandboxBase.__putBehavior;
      if (behavior === "network_error") return Promise.reject(new Error("network down"));
      if (behavior === "400") return Promise.resolve({ ok: false, status: 400, d: { status: "error", code: "invalid_required_fields" } });
      if (behavior === "409") return Promise.resolve({ ok: false, status: 409, d: { status: "error", code: "conflict" } });
      return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } });
    },
    gcCampaignDetailHtml: (campaign, providers, pools, editingSection) => {
      calls.gcCampaignDetailHtml.push({ campaign, providers, pools, editingSection });
      return "rendered:" + campaign.campaign_id;
    },
  };
  Object.assign(sandboxBase, overrides || {});

  const fullSrc = PROVIDER_LABEL_SRC + "\n" + KL_SRC + "\n" + PURE_SRC + "\n" + ORCH_SRC +
    "\nthis.cdSaveSection = cdSaveSection; this.cdOpenEdit = cdOpenEdit; this.cdViewState = cdViewState; " +
    "this.renderCampaignDetail = renderCampaignDetail; this.loadCampaignDetail = loadCampaignDetail; " +
    "this.cdFriendlyErrorMessage = cdFriendlyErrorMessage;";

  const sandbox = runInSandbox(fullSrc, sandboxBase);
  // sandbox === sandboxBase (vm.createContext mutates the object passed in
  // and uses it as the global object), so cdSaveSection etc. and the
  // __putBehavior/__regFields control flags now live on the exact same
  // object apiPutJson/$all already close over.
  return { sandbox, dom, calls, apiQueue };
}

async function flush(n) {
  for (let i = 0; i < (n || 12); i++) await Promise.resolve();
}

// ---------------------------------------------------------------------
// A & H. Merge safety / no unsafe write paths — the critical suite.
// ---------------------------------------------------------------------
test("A/H: editing destination.ready only preserves provider_id/path/open_mode, built from a fresh GET (never the stale page snapshot)", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  // The page's in-memory snapshot is deliberately stale (different path)
  // to prove the save is built from the fresh GET below, not from this.
  sandbox.cdViewState.campaign = tournamentCampaign({ destination: { provider_id: "p1", path: "/STALE-PAGE-SNAPSHOT", open_mode: "telegram_web_app", ready: false } });
  const freshLatest = tournamentCampaign({ destination: { provider_id: "p1", path: "/game", open_mode: "telegram_web_app", ready: false } });
  apiQueue.push({ status: "ok", campaign: freshLatest }); // pre-PUT canonical GET
  apiQueue.push({ status: "ok", campaign: tournamentCampaign({ destination: { provider_id: "p1", path: "/game", open_mode: "telegram_web_app", ready: true } }) }); // post-PUT canonical GET

  dom.node("#cd-edit-dest-provider").value = "p1";
  dom.node("#cd-edit-dest-path").value = "/game";
  dom.node("#cd-edit-dest-ready").checked = true;
  dom.node("#cd-edit-dest-openmode").value = "telegram_web_app"; // unchanged in the form

  sandbox.cdSaveSection("destination", { disabled: false, textContent: "" });
  await flush();

  assert.equal(calls.apiPutJson.length, 1);
  const body = plain(calls.apiPutJson[0].body);
  // P0.14: the same "Where users go" editor now also owns the telegram
  // subscription switch, saved together in one PUT — never a second save
  // path for it.
  assert.deepEqual(Object.keys(body).sort(), ["destination", "telegram"]);
  assert.deepEqual(body.destination, { provider_id: "p1", open_mode: "telegram_web_app", path: "/game", ready: true });
  // Every key the backend's destination block recognizes is present — never
  // a partial fragment like {ready: true} alone (see campaign_centre.py's
  // full nested-block reconstruction).
  assert.deepEqual(Object.keys(body.destination).sort(), ["open_mode", "path", "provider_id", "ready"]);
  // tournamentCampaign() carries no telegram block at all — cdSaveSection
  // must still send a complete, backend-shaped one (defaulting
  // require_identity true, everything else off/empty) rather than crash or
  // omit the key.
  assert.deepEqual(body.telegram, { require_identity: true, require_subscription: false, channel_id: null, channel_username: "" });
});

test("A/H: editing provider only preserves path/open_mode/ready", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  const freshLatest = tournamentCampaign({ destination: { provider_id: "p1", path: "/game", open_mode: "telegram_web_app", ready: true } });
  apiQueue.push({ status: "ok", campaign: freshLatest });
  apiQueue.push({ status: "ok", campaign: freshLatest });

  dom.node("#cd-edit-dest-provider").value = "p2";
  dom.node("#cd-edit-dest-path").value = "/game"; // unchanged in the form
  dom.node("#cd-edit-dest-ready").checked = true; // unchanged in the form
  dom.node("#cd-edit-dest-openmode").value = "telegram_web_app"; // unchanged in the form

  sandbox.cdSaveSection("destination", {});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.deepEqual(body.destination, { provider_id: "p2", open_mode: "telegram_web_app", path: "/game", ready: true });
});

test("A/H: editing schedule never touches destination/registration/telegram (top-level partial — those keys are simply absent from the body)", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  const freshLatest = tournamentCampaign({ schedule: { starts_at: "2026-10-01T01:00:00+00:00", ends_at: "2026-10-31T15:59:00+00:00", timezone: "Asia/Kuala_Lumpur" } });
  apiQueue.push({ status: "ok", campaign: freshLatest });
  apiQueue.push({ status: "ok", campaign: freshLatest });

  dom.node("#cd-edit-starts").value = "2026-10-02T08:00";
  dom.node("#cd-edit-no-end").checked = false;
  dom.node("#cd-edit-ends").value = "2026-10-31T23:59";

  sandbox.cdSaveSection("when", {});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.deepEqual(Object.keys(body).sort(), ["schedule"]);
  assert.deepEqual(Object.keys(body.schedule).sort(), ["ends_at", "starts_at", "timezone"]);
  assert.equal(body.schedule.timezone, "Asia/Kuala_Lumpur");
  assert.equal(new Date(body.schedule.starts_at).toISOString(), "2026-10-02T00:00:00.000Z");
});

test("A/H: registration edit preserves every unknown/advanced sibling key (audience, shipping, reminder_hours, base_entries, ...)", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  const existingReg = fullRegistrationBlock({ enabled: true, required_fields: ["full_name", "contact_number"] });
  sandbox.cdViewState.campaign = tournamentCampaign({ registration: existingReg });
  const freshLatest = tournamentCampaign({ registration: existingReg });
  apiQueue.push({ status: "ok", campaign: freshLatest });
  apiQueue.push({ status: "ok", campaign: freshLatest });

  sandbox.__regFields = [
    { value: "full_name", checked: true },
    { value: "contact_number", checked: false },
    { value: "country_region", checked: true },
    { value: "delivery_address", checked: true },
  ];
  dom.node("#cd-edit-reg-enabled").checked = true;

  sandbox.cdSaveSection("registration", {});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.deepEqual(body.registration.required_fields, ["full_name", "country_region", "delivery_address"]);
  assert.equal(body.registration.enabled, true);
  // Every advanced/unknown-to-this-screen field survives verbatim.
  assert.equal(body.registration.reminder_hours, 48);
  assert.equal(body.registration.base_entries, 3);
  assert.equal(body.registration.require_channel_subscription, true);
  assert.equal(body.registration.modal_enabled, false);
  assert.deepEqual(body.registration.audience, { scope: "selected", regions: ["KL", "Penang"] });
  assert.deepEqual(body.registration.shipping, { scope: "all", regions: [] });
});

test("D: disabling registration still sends a complete, valid block (no required_fields validation applied when enabled=false)", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  const existingReg = fullRegistrationBlock({ enabled: true });
  sandbox.cdViewState.campaign = tournamentCampaign({ registration: existingReg });
  apiQueue.push({ status: "ok", campaign: tournamentCampaign({ registration: existingReg }) });
  apiQueue.push({ status: "ok", campaign: tournamentCampaign({ registration: Object.assign({}, existingReg, { enabled: false }) }) });

  sandbox.__regFields = []; // nothing checked
  dom.node("#cd-edit-reg-enabled").checked = false;

  sandbox.cdSaveSection("registration", {});
  await flush();

  assert.equal(calls.apiPutJson.length, 1, "disabling must not be blocked by the empty-required-fields guard");
  assert.equal(calls.apiPutJson[0].body.registration.enabled, false);
});

// ---------------------------------------------------------------------
// B. Identity — campaign_id/type are never editable
// ---------------------------------------------------------------------
test("B: saving the Campaign section body never contains campaign_id or type", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  apiQueue.push({ status: "ok", campaign: tournamentCampaign() });
  apiQueue.push({ status: "ok", campaign: tournamentCampaign({ name: "New Name" }) });

  dom.node("#cd-edit-name").value = "New Name";
  dom.node("#cd-edit-description").value = "New description";

  sandbox.cdSaveSection("campaign", {});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.deepEqual(Object.keys(body).sort(), ["description", "name"]);
  assert.equal(body.name, "New Name");
});

test("B: empty name is rejected client-side without ever calling the network", async () => {
  const { sandbox, dom, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  dom.node("#cd-edit-name").value = "   ";
  sandbox.cdSaveSection("campaign", {});
  await flush();
  assert.deepEqual(calls.api, []);
  assert.deepEqual(calls.apiPutJson, []);
  assert.equal(dom.node("#cd-edit-campaign-error").textContent, "Please check the highlighted fields.");
});

// ---------------------------------------------------------------------
// C. Schedule validation via cdSaveSection
// ---------------------------------------------------------------------
test("C: end-before-start is rejected client-side with a friendly message, no network call", async () => {
  const { sandbox, dom, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  dom.node("#cd-edit-starts").value = "2026-10-10T08:00";
  dom.node("#cd-edit-no-end").checked = false;
  dom.node("#cd-edit-ends").value = "2026-10-01T08:00";

  sandbox.cdSaveSection("when", {});
  await flush();

  assert.deepEqual(calls.api, []);
  assert.match(dom.node("#cd-edit-when-error").textContent, /End date must be after the start date/);
});

test("C: no end date sends ends_at: null, not an empty string or omitted key", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  const latest = tournamentCampaign();
  apiQueue.push({ status: "ok", campaign: latest });
  apiQueue.push({ status: "ok", campaign: latest });

  dom.node("#cd-edit-starts").value = "2026-10-01T08:00";
  dom.node("#cd-edit-no-end").checked = true;
  dom.node("#cd-edit-ends").value = "2026-10-31T23:59"; // must be ignored once "no end" is checked

  sandbox.cdSaveSection("when", {});
  await flush();

  assert.equal(calls.apiPutJson[0].body.schedule.ends_at, null);
});

// ---------------------------------------------------------------------
// D. Registration — empty required_fields blocked when enabled
// ---------------------------------------------------------------------
test("D: enabling registration with zero required fields checked is blocked client-side", async () => {
  const { sandbox, dom, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign({ registration: fullRegistrationBlock({ enabled: false }) });
  sandbox.__regFields = [{ value: "full_name", checked: false }];
  dom.node("#cd-edit-reg-enabled").checked = true;

  sandbox.cdSaveSection("registration", {});
  await flush();

  assert.deepEqual(calls.api, []);
  assert.equal(dom.node("#cd-edit-registration-error").textContent, "Select at least one required registration field.");
});

// ---------------------------------------------------------------------
// F. Refresh / canonical state
// ---------------------------------------------------------------------
test("F: after a successful save, the campaign is re-GET'd canonically (not trusted from the PUT response) before cdViewState updates", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  const latest = tournamentCampaign();
  const canonicalAfterSave = tournamentCampaign({ name: "Canonical Post-Save Name" });
  apiQueue.push({ status: "ok", campaign: latest });
  apiQueue.push({ status: "ok", campaign: canonicalAfterSave });

  dom.node("#cd-edit-name").value = "Whatever The Admin Typed";
  dom.node("#cd-edit-description").value = "";

  sandbox.cdSaveSection("campaign", {});
  await flush();

  assert.equal(calls.api.length, 2);
  assert.deepEqual(calls.api, ["/api/admin/gc-campaigns/july-tournament", "/api/admin/gc-campaigns/july-tournament"]);
  // cdViewState reflects the SECOND (post-save canonical) GET, not the PUT
  // body the admin typed and not the first (pre-save) GET.
  assert.equal(sandbox.cdViewState.campaign.name, "Canonical Post-Save Name");
  assert.equal(sandbox.cdViewState.editingSection, null);
  assert.equal(calls.toast[calls.toast.length - 1][0], "✅ Saved");
});

test("cdOpenEdit re-renders from the cached snapshot without any network call", () => {
  const { sandbox, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.providers = [];
  sandbox.cdViewState.pools = [];
  sandbox.cdOpenEdit("when");
  assert.deepEqual(calls.api, []);
  assert.equal(sandbox.cdViewState.editingSection, "when");
  assert.equal(calls.gcCampaignDetailHtml.length, 1);
  assert.equal(calls.gcCampaignDetailHtml[0].editingSection, "when");
});

// ---------------------------------------------------------------------
// Codex review fix #1 (P1): a slow loadCampaignDetail() response for a
// campaign the admin has since navigated away from must never overwrite
// cdViewState/the DOM with the wrong campaign's data — a later Save reads
// campaign_id from state.campaignId (correct), but would write whatever
// stale campaign's data is sitting in cdViewState/the form into it.
// ---------------------------------------------------------------------
test("loadCampaignDetail discards a response that resolves after the admin navigated to a different campaign", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  const campaignA = tournamentCampaign({ campaign_id: "campaign-a", name: "Campaign A" });
  const campaignB = tournamentCampaign({ campaign_id: "campaign-b", name: "Campaign B" });

  sandbox.state.campaignId = "campaign-a";
  apiQueue.push({ status: "ok", campaign: campaignA }); // A's GET — resolves late, after B is already selected
  sandbox.loadCampaignDetail(false);

  // Before A's response arrives, the admin navigates to campaign B.
  sandbox.state.campaignId = "campaign-b";
  apiQueue.push({ status: "ok", campaign: campaignB });
  sandbox.loadCampaignDetail(false);

  await flush(20);

  // Only B's data ever lands in cdViewState — A's late response must be
  // discarded, not applied on top of (or after) B's.
  assert.equal(sandbox.cdViewState.campaign.campaign_id, "campaign-b");
  assert.equal(dom.node("#cd-body").innerHTML, "rendered:campaign-b");
});

// ---------------------------------------------------------------------
// Codex review fix #3 (P2): a field this form displays but the admin did
// NOT change must take the freshest server value at save time, never the
// value merely sitting in the DOM from when the form opened — otherwise
// saving one field (e.g. "ready") silently reverts a concurrent edit to
// another displayed field (e.g. "provider") made by someone else in the
// meantime.
// ---------------------------------------------------------------------
test("destination save preserves a concurrent edit to the provider when only 'ready' was actually changed", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  const original = tournamentCampaign({ destination: { provider_id: "p1", path: "/game", open_mode: "telegram_web_app", ready: false } });
  sandbox.cdViewState.campaign = original;
  sandbox.cdOpenEdit("destination"); // captures the snapshot: provider p1, ready false

  // Someone else changes the provider via a different screen while this
  // form sits open.
  const concurrentlyEdited = tournamentCampaign({ destination: { provider_id: "p2-changed-by-someone-else", path: "/game", open_mode: "telegram_web_app", ready: false } });
  apiQueue.push({ status: "ok", campaign: concurrentlyEdited }); // pre-PUT canonical GET sees the concurrent change
  apiQueue.push({ status: "ok", campaign: concurrentlyEdited });

  // The admin never touched the provider dropdown — it still shows what
  // cdOpenEdit rendered (p1) — but does flip "ready".
  dom.node("#cd-edit-dest-provider").value = "p1";
  dom.node("#cd-edit-dest-path").value = "/game";
  dom.node("#cd-edit-dest-ready").checked = true;

  sandbox.cdSaveSection("destination", {});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.equal(body.destination.provider_id, "p2-changed-by-someone-else", "the untouched provider field must keep the concurrent edit, not this form's stale DOM value");
  assert.equal(body.destination.ready, true, "the field the admin actually changed must still be saved");
});

test("registration save preserves a concurrent edit to required_fields when only 'enabled' was actually changed", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration();
  const original = tournamentCampaign({ registration: fullRegistrationBlock({ enabled: false, required_fields: ["full_name", "contact_number"] }) });
  sandbox.cdViewState.campaign = original;
  sandbox.cdOpenEdit("registration"); // snapshot: enabled=false, fields=[full_name, contact_number]

  const concurrentlyEdited = tournamentCampaign({ registration: fullRegistrationBlock({ enabled: false, required_fields: ["delivery_address"] }) });
  apiQueue.push({ status: "ok", campaign: concurrentlyEdited });
  apiQueue.push({ status: "ok", campaign: concurrentlyEdited });

  // Admin leaves the required-fields checkboxes exactly as rendered
  // (full_name, contact_number) and only flips "enabled".
  sandbox.__regFields = [
    { value: "full_name", checked: true },
    { value: "contact_number", checked: true },
    { value: "country_region", checked: false },
    { value: "delivery_address", checked: false },
  ];
  dom.node("#cd-edit-reg-enabled").checked = true;

  sandbox.cdSaveSection("registration", {});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.deepEqual(body.registration.required_fields, ["delivery_address"], "the untouched required_fields must keep the concurrent edit, not this form's stale checkboxes");
  assert.equal(body.registration.enabled, true, "the field the admin actually changed must still be saved");
});

// ---------------------------------------------------------------------
// G. Failures — friendly mapping, raw backend detail never shown
// ---------------------------------------------------------------------
test("G: backend 400 (invalid_required_fields) shows the mapped friendly message, not the raw code", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration({});
  sandbox.__putBehavior = "400";
  sandbox.cdViewState.campaign = tournamentCampaign({ registration: fullRegistrationBlock() });
  apiQueue.push({ status: "ok", campaign: tournamentCampaign({ registration: fullRegistrationBlock() }) });

  sandbox.__regFields = [{ value: "full_name", checked: true }];
  dom.node("#cd-edit-reg-enabled").checked = true;

  const btn = { disabled: false, textContent: "Save" };
  sandbox.cdSaveSection("registration", btn);
  await flush();

  const shown = dom.node("#cd-edit-registration-error").textContent;
  assert.equal(shown, "Select at least one required registration field.");
  assert.doesNotMatch(shown, /invalid_required_fields/);
  assert.equal(btn.disabled, false, "Save must be re-enabled after a failed save");
  assert.equal(btn.textContent, "Save");
});

test("G: backend 409 shows the stale-campaign message, not a raw status code", async () => {
  const { sandbox, dom, calls, apiQueue } = loadOrchestration({});
  sandbox.__putBehavior = "409";
  sandbox.cdViewState.campaign = tournamentCampaign();
  apiQueue.push({ status: "ok", campaign: tournamentCampaign() });

  dom.node("#cd-edit-name").value = "Name";
  sandbox.cdSaveSection("campaign", {});
  await flush();

  assert.equal(dom.node("#cd-edit-campaign-error").textContent, "The campaign changed while you were editing. Reloaded the latest version.");
});

test("G: a network error (fetch rejects) shows the generic retry message", async () => {
  const { sandbox, dom, apiQueue } = loadOrchestration({});
  sandbox.__putBehavior = "network_error";
  sandbox.cdViewState.campaign = tournamentCampaign();
  apiQueue.push({ status: "ok", campaign: tournamentCampaign() });

  dom.node("#cd-edit-name").value = "Name";
  sandbox.cdSaveSection("campaign", {});
  await flush();

  assert.equal(dom.node("#cd-edit-campaign-error").textContent, "Couldn't save changes. Try again.");
});

test("G: the pre-PUT canonical GET itself failing shows the generic retry message, never a stack trace", async () => {
  const { sandbox, dom, apiQueue } = loadOrchestration({});
  sandbox.cdViewState.campaign = tournamentCampaign();
  apiQueue.push({ status: "error", code: "not_found" }); // malformed/failed GET (no .campaign)

  dom.node("#cd-edit-name").value = "Name";
  sandbox.cdSaveSection("campaign", {});
  await flush();

  assert.equal(dom.node("#cd-edit-campaign-error").textContent, "Couldn't save changes. Try again.");
});

// ---------------------------------------------------------------------
// Regression: P0.5a stays green, and the VIEWS/HTML/MODULES + P0.2/P0.3/
// P0.4 suites this PR must not disturb.
// ---------------------------------------------------------------------
test("P0.5a and the VIEWS/HTML/MODULES + P0.2-P0.4 regression suites still pass", () => {
  execFileSync(process.execPath, [
    "--test",
    "test_admin_dashboard_p0_5a_campaign_detail.test.js",
    "test_admin_dashboard_views_sync.test.js",
    "test_admin_dashboard_p0_2_filters.test.js",
    "test_admin_dashboard_p0_3_id_fields.test.js",
    "test_admin_dashboard_p0_4_campaign_list.test.js",
  ], { cwd: __dirname, stdio: "pipe" });
});
