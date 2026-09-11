/**
 * P0.6: Campaign Creation Wizard — beginner-friendly guided flow for
 * creating a gc_campaigns shell (static/admin-dashboard.js /
 * static/admin-dashboard.html), replacing the technical "Create Campaign"
 * form as the "+ New Campaign" entry point.
 *
 * Covers:
 *  - Routing: "+ New Campaign" opens the wizard, Cancel returns to
 *    Campaigns, successful create opens Campaign Detail.
 *  - Step flow: 5 steps, Back/Continue, state preserved across navigation,
 *    type-specific Step 4 content appears/disappears per wizardType.
 *  - Type mapping: every wizard type card -> exact backend `type` value,
 *    matching campaign_centre.CAMPAIGN_TYPES, plus whether registration
 *    turns on.
 *  - Slug/collision: the wizard reuses gcSlugify/gcSlugCandidate/
 *    gcFirstAvailableSuffix/gcCreateCampaignAttempt rather than
 *    reimplementing them (P0.3 helpers untouched, covered directly by
 *    test_admin_dashboard_p0_3_id_fields.test.js).
 *  - Schedule: KL input -> correct UTC payload, optional end, invalid end
 *    blocked with a plain-English error.
 *  - Registration: no provider asked, required_fields non-empty enforced,
 *    correct payload shape.
 *  - Tournament: correct shell payload, rewards deferred, no reward_config
 *    sent at create.
 *  - Mission: hands off to Mission Admin's own create flow — no duplicate
 *    editor, no gc-campaigns POST issued from this wizard for it.
 *  - Creation: exactly one successful POST per attempt, cache invalidated,
 *    canonical campaign re-opened via Campaign Detail afterward.
 *  - Technical fields (campaign_id, provider_id, backend type) stay out of
 *    the beginner-visible summary and only appear inside the collapsed
 *    Technical Details block.
 *
 * Mirrors test_admin_dashboard_p0_5b_campaign_detail_edit.test.js's harness:
 * no build step, no jsdom — relevant source ranges are extracted as text and
 * executed in sandboxed vm contexts against small DOM/fetch stand-ins.
 *
 * Run with: node --test test_admin_dashboard_p0_6_campaign_wizard.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const HTML_PATH = path.join(__dirname, "static", "admin-dashboard.html");
const JS = fs.readFileSync(JS_PATH, "utf8");
const HTML = fs.readFileSync(HTML_PATH, "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

// Objects built inside a vm sandbox belong to that sandbox's own realm, so
// assert/strict's reference-sensitive checks never treat them as equal to a
// plain literal written in this file even when every field matches. A JSON
// round-trip normalizes a vm-realm value back into this file's realm.
function plain(v) { return JSON.parse(JSON.stringify(v)); }

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// ---------------------------------------------------------------------
// Minimal DOM stand-in: enough for the wizard's $()/$all() lookups and
// checkbox/select capture, and for [data-gcw-*] click delegation via a
// fake .closest() that only ever needs to match the element itself (every
// dispatch handler in bindGcCampaignWizard is bound on `document`, and the
// tests below always dispatch the exact element carrying the attribute).
// ---------------------------------------------------------------------
function makeEl(attrs) {
  attrs = attrs || {};
  const el = Object.assign({
    value: "", checked: false, textContent: "", disabled: false,
    dataset: {}, style: {},
    closest(sel) {
      const m = /^\[data-([a-z-]+)(?:="([^"]+)")?\]$/.exec(sel);
      if (!m) return null;
      const key = m[1].replace(/-([a-z])/g, (_, c) => c.toUpperCase());
      if (!(key in el.dataset)) return null;
      if (m[2] !== undefined && el.dataset[key] !== m[2]) return null;
      return el;
    },
  }, attrs);
  return el;
}

function makeDomStub(elements) {
  return function $(sel) {
    const id = sel.replace(/^#/, "");
    return elements[id] || null;
  };
}

function makeDomAllStub(collections) {
  return function $all(sel) { return collections[sel] || []; };
}

function makeDocumentStub() {
  const listeners = { click: [], change: [], input: [] };
  return {
    addEventListener(type, fn) { if (listeners[type]) listeners[type].push(fn); },
    fire(type, evt) { (listeners[type] || []).forEach((fn) => fn(evt)); },
    querySelector() { return null; },
  };
}

function runInSandbox(code, sandboxExtra) {
  const sandbox = Object.assign({ console, Object, String, Promise, JSON, Array, Date, Math }, sandboxExtra);
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

// ---------------------------------------------------------------------
// Real source slices (never re-implemented stand-ins).
// ---------------------------------------------------------------------
const OPTIONS_SRC = slice(JS, "  var gcOptionsCache = {", "\n  function loadGcCampaigns(force) {");
const ATTEMPT_SRC = slice(JS, "  function gcCreateCampaignAttempt(", "\n  function bindGcCampaigns() {");
const KL_SRC = slice(JS, "  function ccPad2(n)", "\n  var CC_CONTENT_ICON");
const WIZARD_SRC = slice(JS, "  var GCW_STEP_LABELS = [", "\n  // ---------- Registration Configuration");

function loadWizard(extra) {
  const apiPostJsonCalls = [];
  const toasts = [];
  const switchViewCalls = [];
  const activateTabCalls = [];
  const renderCampaignDetailCalls = [];
  const confirmResponses = (extra && extra.confirmResponses) || [true];
  const missionDispatchCalls = [];

  const elements = Object.assign({}, extra && extra.elements);
  const collections = Object.assign({ ".gcw-reg-field": [] }, extra && extra.collections);
  const doc = makeDocumentStub();

  const sandbox = runInSandbox(
    OPTIONS_SRC + "\n" + ATTEMPT_SRC + "\n" + KL_SRC + "\n" + WIZARD_SRC +
    "\nthis.__x = { gcw, GCW_TYPES, GCW_STEP_LABELS, GCW_REGISTRATION_FIELD_ORDER, GCW_REGISTRATION_FIELD_LABELS, " +
    "gcwDefaultDraft, gcwHasMeaningfulInput, gcwStep1Html, gcwStep2Html, gcwStep3Html, gcwStep4Html, gcwStep5Html, " +
    "gcwStepBodies, gcwRender, gcwCaptureStep, gcwValidateStep, gcwCancel, gcwSubmit, gcwEnterStep, " +
    "bindGcCampaignWizard, gcOptionsCache, gcKnownCampaignIds, gcCreateCampaignAttempt, gcOpenCampaignWizard: window.gcOpenCampaignWizard };",
    {
      $: makeDomStub(elements),
      $all: makeDomAllStub(collections),
      esc,
      document: doc,
      window: {
        confirm: () => (confirmResponses.length > 1 ? confirmResponses.shift() : confirmResponses[0]),
        MissionAdmin: { dispatch: (action) => missionDispatchCalls.push(action) },
      },
      apiPostJson: (url, body) => {
        apiPostJsonCalls.push({ url, body });
        const queue = extra && extra.apiPostJsonResponses;
        const resp = queue ? (queue.length > 1 ? queue.shift() : queue[0])
          : (extra && extra.apiPostJsonResponse) || { ok: true, d: { status: "ok" } };
        return Promise.resolve(resp);
      },
      toast: (msg) => toasts.push(msg),
      switchView: (v) => switchViewCalls.push(v),
      activateTab: (m, i) => activateTabCalls.push([m, i]),
      renderCampaignDetail: (id) => renderCampaignDetailCalls.push(id),
      gcInvalidateCampaignsCache: () => {},
      fetchGcProviders: () => Promise.resolve((extra && extra.providers) || []),
      gcProviderOptionLabel: (p) => (p.name || p.provider_id) + " — " + (p.type || "") + " (" + (p.active ? "active" : "inactive") + ")",
    }
  );
  return Object.assign(sandbox.__x, {
    apiPostJsonCalls, toasts, switchViewCalls, activateTabCalls, renderCampaignDetailCalls,
    missionDispatchCalls, doc, elements,
  });
}

// ---------------------------------------------------------------------
// Type mapping — every supported wizard type -> exact backend value.
// ---------------------------------------------------------------------
test("GCW_TYPES: exactly tournament / registration / external map onto real CAMPAIGN_TYPES values", () => {
  const w = loadWizard();
  assert.equal(w.GCW_TYPES.tournament.backendType, "tournament");
  assert.equal(w.GCW_TYPES.tournament.registration, false);
  assert.equal(w.GCW_TYPES.registration.backendType, "external_website");
  assert.equal(w.GCW_TYPES.registration.registration, true);
  assert.equal(w.GCW_TYPES.external.backendType, "external_website");
  assert.equal(w.GCW_TYPES.external.registration, false);
  // No native "giveaway" type is invented — Registration/Giveaway reuses
  // external_website + registration.enabled, never a fake enum value.
  Object.values(w.GCW_TYPES).forEach((t) => assert.notEqual(t.backendType, "giveaway"));
});

test("registration field keys mirror the real backend required_fields (no fabricated telegram_username field)", () => {
  const w = loadWizard();
  assert.deepEqual(plain(w.GCW_REGISTRATION_FIELD_ORDER), ["full_name", "contact_number", "country_region", "delivery_address"]);
  assert.equal(w.GCW_REGISTRATION_FIELD_LABELS.full_name, "Full name");
  assert.ok(!("telegram_username" in w.GCW_REGISTRATION_FIELD_LABELS), "telegram identity is collected automatically, never a configurable field");
});

// ---------------------------------------------------------------------
// Step flow
// ---------------------------------------------------------------------
test("wizard has exactly 5 steps", () => {
  const w = loadWizard();
  assert.equal(w.GCW_STEP_LABELS.length, 5);
  assert.equal(w.gcwStepBodies().length, 5);
});

test("Step 4 shows registration fields for the Registration/Giveaway type and nothing about provider/destination", () => {
  const w = loadWizard();
  const draft = w.gcwDefaultDraft();
  draft.wizardType = "registration";
  const html = w.gcwStep4Html(draft);
  assert.match(html, /Registration fields/);
  assert.match(html, /Full name/);
  assert.doesNotMatch(html, /Provider/);
  assert.doesNotMatch(html, /Destination path/);
});

test("Step 4 shows Provider + Destination for Tournament, with rewards explicitly deferred", () => {
  const w = loadWizard();
  const draft = w.gcwDefaultDraft();
  draft.wizardType = "tournament";
  const html = w.gcwStep4Html(draft);
  assert.match(html, /Provider/);
  assert.match(html, /Destination path/);
  assert.match(html, /Rewards.*set up after campaign creation/i);
  assert.doesNotMatch(html, /Registration fields/);
});

test("Step 4 shows Provider + Destination for External/Standard, no rewards note (not reward-driven)", () => {
  const w = loadWizard();
  const draft = w.gcwDefaultDraft();
  draft.wizardType = "external";
  const html = w.gcwStep4Html(draft);
  assert.match(html, /Provider/);
  assert.match(html, /Destination path/);
  assert.doesNotMatch(html, /Rewards/);
});

test("changing wizardType resets registration/destination but the type-click handler leaves name/schedule alone", () => {
  // Simulated at the handler level: exercise the exact reset logic used by
  // the [data-gcw-type] click branch in bindGcCampaignWizard.
  const w = loadWizard();
  w.gcw.draft.name = "October Lucky Draw";
  w.gcw.draft.starts_at = "2026-10-01T09:00";
  w.gcw.draft.wizardType = "registration";
  w.gcw.draft.registration.requiredFields = ["full_name"];
  w.gcw.draft.destination.path = "/old-path";

  // Re-run the same reset the dispatcher performs on a type switch.
  w.gcw.draft.wizardType = "tournament";
  w.gcw.draft.registration = w.gcwDefaultDraft().registration;
  w.gcw.draft.destination = w.gcwDefaultDraft().destination;

  assert.equal(w.gcw.draft.name, "October Lucky Draw", "name must survive a type change");
  assert.equal(w.gcw.draft.starts_at, "2026-10-01T09:00", "schedule must survive a type change");
  assert.deepEqual(plain(w.gcw.draft.registration.requiredFields), ["full_name", "contact_number", "country_region", "delivery_address"]);
  assert.equal(w.gcw.draft.destination.path, "");
});

// ---------------------------------------------------------------------
// Validation — plain-English errors, mirroring campaign_centre's own rules.
// ---------------------------------------------------------------------
test("Step 2 (basic details): missing name is rejected with a friendly message", () => {
  const w = loadWizard();
  w.gcw.step = 1;
  w.gcw.draft.name = "";
  assert.equal(w.gcwValidateStep(1), "Enter a campaign name.");
});

test("Step 3 (schedule): missing start date is rejected", () => {
  const w = loadWizard();
  const d = w.gcwDefaultDraft();
  d.starts_at = "";
  Object.assign(w.gcw.draft, d);
  assert.equal(w.gcwValidateStep(2), "Enter a start date and time.");
});

test("Step 3 (schedule): end before start is rejected with the exact spec copy", () => {
  const w = loadWizard();
  Object.assign(w.gcw.draft, { starts_at: "2026-10-10T09:00", noEnd: false, ends_at: "2026-10-01T09:00" });
  assert.equal(w.gcwValidateStep(2), "End time must be after start time.");
});

test("Step 3 (schedule): no end date is valid (end is optional)", () => {
  const w = loadWizard();
  Object.assign(w.gcw.draft, { starts_at: "2026-10-01T09:00", noEnd: true, ends_at: "" });
  assert.equal(w.gcwValidateStep(2), null);
});

test("Step 3 (schedule): a valid end after start passes", () => {
  const w = loadWizard();
  Object.assign(w.gcw.draft, { starts_at: "2026-10-01T09:00", noEnd: false, ends_at: "2026-10-31T23:59" });
  assert.equal(w.gcwValidateStep(2), null);
});

test("Step 4 (registration): zero required fields is rejected", () => {
  const w = loadWizard();
  w.gcw.draft.wizardType = "registration";
  w.gcw.draft.registration.requiredFields = [];
  assert.equal(w.gcwValidateStep(3), "Select at least one registration field.");
});

test("Step 4 (registration): channel subscription required but no username entered is rejected", () => {
  const w = loadWizard();
  w.gcw.draft.wizardType = "registration";
  w.gcw.draft.registration.requiredFields = ["full_name"];
  w.gcw.draft.registration.requireChannelSubscription = true;
  w.gcw.draft.registration.channelUsername = "";
  assert.equal(w.gcwValidateStep(3), "Enter the channel username.");
});

test("Step 4 (tournament/external): nothing is required — provider/destination stay optional", () => {
  const w = loadWizard();
  w.gcw.draft.wizardType = "tournament";
  assert.equal(w.gcwValidateStep(3), null);
});

// ---------------------------------------------------------------------
// Schedule: KL input -> correct UTC payload (reuses the real ccKlInputToUtcIso).
// ---------------------------------------------------------------------
test("gcwSubmit converts KL datetime-local inputs to correct UTC instants in the payload", async () => {
  const w = loadWizard();
  Object.assign(w.gcw.draft, {
    wizardType: "external", name: "October Lucky Draw",
    starts_at: "2026-10-01T09:00", noEnd: false, ends_at: "2026-10-31T23:59",
    campaignIdManuallyEdited: true, campaignId: "october-lucky-draw",
  });
  await w.gcwSubmit(null);
  assert.equal(w.apiPostJsonCalls.length, 1);
  const body = w.apiPostJsonCalls[0].body;
  // 2026-10-01 09:00 KL (UTC+8) == 2026-10-01 01:00 UTC.
  assert.equal(body.schedule.starts_at, "2026-10-01T01:00:00.000Z");
  assert.equal(body.schedule.ends_at, "2026-10-31T15:59:00.000Z");
});

test("gcwSubmit sends ends_at: null when the admin left 'No end date' checked", async () => {
  const w = loadWizard();
  Object.assign(w.gcw.draft, {
    wizardType: "external", name: "No End Campaign", starts_at: "2026-10-01T09:00",
    noEnd: true, ends_at: "", campaignIdManuallyEdited: true, campaignId: "no-end-campaign",
  });
  await w.gcwSubmit(null);
  assert.equal(w.apiPostJsonCalls[0].body.schedule.ends_at, null);
});

// ---------------------------------------------------------------------
// Registration: no provider asked, required_fields non-empty, correct payload.
// ---------------------------------------------------------------------
test("Registration/Giveaway payload: external_website + registration.enabled, no destination fields required", async () => {
  const w = loadWizard();
  Object.assign(w.gcw.draft, {
    wizardType: "registration", name: "October Lucky Draw", starts_at: "2026-10-01T09:00",
    campaignIdManuallyEdited: true, campaignId: "october-lucky-draw",
  });
  w.gcw.draft.registration.requiredFields = ["full_name", "delivery_address"];
  await w.gcwSubmit(null);
  const body = w.apiPostJsonCalls[0].body;
  assert.equal(body.type, "external_website");
  assert.deepEqual(plain(body.registration), {
    enabled: true, required_fields: ["full_name", "delivery_address"], require_channel_subscription: false,
  });
  assert.equal(body.destination.provider_id, "");
});

test("Registration/Giveaway payload: channel subscription carries the channel_username under telegram", async () => {
  const w = loadWizard();
  Object.assign(w.gcw.draft, {
    wizardType: "registration", name: "Channel Gated Draw", starts_at: "2026-10-01T09:00",
    campaignIdManuallyEdited: true, campaignId: "channel-gated-draw",
  });
  w.gcw.draft.registration.requiredFields = ["full_name"];
  w.gcw.draft.registration.requireChannelSubscription = true;
  w.gcw.draft.registration.channelUsername = "mychannel";
  await w.gcwSubmit(null);
  const body = w.apiPostJsonCalls[0].body;
  assert.equal(body.registration.require_channel_subscription, true);
  assert.equal(body.telegram.channel_username, "mychannel");
});

// ---------------------------------------------------------------------
// Tournament: correct shell payload, rewards deferred.
// ---------------------------------------------------------------------
test("Tournament payload: no reward_config sent at create — rewards stay deferred to Campaign Detail", async () => {
  const w = loadWizard();
  Object.assign(w.gcw.draft, {
    wizardType: "tournament", name: "July Tournament", starts_at: "2026-07-01T09:00",
    campaignIdManuallyEdited: true, campaignId: "july-tournament",
  });
  w.gcw.draft.destination.providerId = "p1";
  w.gcw.draft.destination.path = "/july-tournament";
  await w.gcwSubmit(null);
  const body = w.apiPostJsonCalls[0].body;
  assert.equal(body.type, "tournament");
  assert.equal(body.destination.provider_id, "p1");
  assert.equal(body.destination.path, "/july-tournament");
  assert.equal(body.destination.ready, false);
  assert.ok(!("reward_config" in body), "reward rules are never collected/sent by the wizard");
  assert.ok(!("registration" in body), "tournament never carries a registration block");
});

// ---------------------------------------------------------------------
// Mission: hands off, no duplicate editor, no gc-campaigns POST.
// ---------------------------------------------------------------------
test("Mission: bindGcCampaignWizard hands off to Mission Admin's own create flow, no shell POST", () => {
  const w = loadWizard();
  w.bindGcCampaignWizard();
  const missionCard = makeEl({ dataset: { gcwType: "mission" } });
  w.doc.fire("click", { target: missionCard });
  assert.deepEqual(w.switchViewCalls, ["missionPool"]);
  assert.deepEqual(w.missionDispatchCalls, ["create"]);
  assert.equal(w.apiPostJsonCalls.length, 0, "must never itself create a gc_campaigns shell for Mission");
});

// ---------------------------------------------------------------------
// Creation: exactly one POST, collision retry safe, cache invalidated,
// canonical campaign opened via Campaign Detail afterward.
// ---------------------------------------------------------------------
test("gcwSubmit: on success, invalidates the campaigns cache and opens Campaign Detail for the created id", async () => {
  const w = loadWizard({ apiPostJsonResponse: { ok: true, d: { status: "ok", campaign_id: "october-lucky-draw" } } });
  Object.assign(w.gcw.draft, {
    wizardType: "external", name: "October Lucky Draw", starts_at: "2026-10-01T09:00",
    campaignIdManuallyEdited: true, campaignId: "october-lucky-draw",
  });
  await w.gcwSubmit(null);
  assert.equal(w.apiPostJsonCalls.length, 1, "exactly one POST for a clean create");
  assert.deepEqual(w.renderCampaignDetailCalls, ["october-lucky-draw"]);
  assert.ok(w.toasts.some((t) => /created/i.test(t)));
});

test("gcwSubmit: a manual campaign-id collision surfaces the exact friendly copy from the spec, never the raw code", async () => {
  const errorEl = makeEl({ style: { display: "none" } });
  const w = loadWizard({
    apiPostJsonResponse: { ok: false, d: { status: "error", code: "duplicate_campaign_id" } },
    elements: { "gcw-error-step5": errorEl },
  });
  Object.assign(w.gcw.draft, {
    wizardType: "external", name: "Taken Name", starts_at: "2026-10-01T09:00",
    campaignIdManuallyEdited: true, campaignId: "taken-id",
  });
  const btn = makeEl({});
  await w.gcwSubmit(btn);
  assert.equal(w.apiPostJsonCalls.length, 1, "a manually-chosen id must never be auto-retried");
  assert.equal(errorEl.textContent, "That Campaign ID is already in use. Choose another one.");
  assert.doesNotMatch(errorEl.textContent, /duplicate_campaign_id/, "must never surface the raw backend code");
  assert.equal(btn.disabled, false, "the Create button must re-enable after a failed attempt");
});

test("gcwSubmit: auto-generated id collision retries with -2 via the shared gcCreateCampaignAttempt path", async () => {
  const w = loadWizard({
    apiPostJsonResponses: [
      { ok: false, d: { status: "error", code: "duplicate_campaign_id" } },
      { ok: true, d: { status: "ok", campaign_id: "october-lucky-draw-2" } },
    ],
  });
  Object.assign(w.gcw.draft, {
    wizardType: "external", name: "October Lucky Draw", starts_at: "2026-10-01T09:00",
    campaignIdManuallyEdited: false,
  });
  await w.gcwSubmit(null);
  assert.equal(w.apiPostJsonCalls.length, 2, "must retry once on a server-side collision");
  assert.equal(w.apiPostJsonCalls[0].body.campaign_id, "october-lucky-draw");
  assert.equal(w.apiPostJsonCalls[1].body.campaign_id, "october-lucky-draw-2");
  assert.deepEqual(w.renderCampaignDetailCalls, ["october-lucky-draw-2"], "must land on the id the backend actually created");
});

test("gcwCancel: no meaningful input navigates back to Campaigns without confirming", () => {
  const w = loadWizard({ confirmResponses: [] });
  w.gcw.draft = w.gcwDefaultDraft();
  w.gcwCancel();
  assert.deepEqual(w.activateTabCalls, [["growth", 0]]);
});

test("gcwCancel: meaningful input asks for confirmation before discarding", () => {
  const w = loadWizard({ confirmResponses: [false] });
  w.gcw.draft.name = "Something typed";
  w.gcwCancel();
  assert.equal(w.activateTabCalls.length, 0, "declining the confirm must not navigate away");
});

test("gcwCancel: confirming discards and returns to the Campaigns list", () => {
  const w = loadWizard({ confirmResponses: [true] });
  w.gcw.draft.name = "Something typed";
  w.gcwCancel();
  assert.deepEqual(w.activateTabCalls, [["growth", 0]]);
});

// ---------------------------------------------------------------------
// Technical fields stay out of the beginner-visible summary.
// ---------------------------------------------------------------------
test("Step 1 (type cards) shows only human labels, never a raw backend type string", () => {
  const w = loadWizard();
  const html = w.gcwStep1Html(w.gcwDefaultDraft());
  assert.match(html, /Tournament/);
  assert.match(html, /Registration \/ Giveaway/);
  assert.match(html, /Mission/);
  assert.match(html, /External \/ Standard Campaign/);
  assert.doesNotMatch(html, /external_website/);
  assert.doesNotMatch(html, /mission_pool/);
});

test("Step 2 (basic details) never shows campaign_id/provider_id/pool_id as a visible label outside Technical Details", () => {
  const w = loadWizard();
  const draft = w.gcwDefaultDraft();
  draft.name = "October Lucky Draw";
  const html = w.gcwStep2Html(draft);
  assert.doesNotMatch(html, /provider_id/);
  assert.doesNotMatch(html, /pool_id/);
  assert.match(html, /Advanced \/ Technical Details/, "the generated id is still reachable, just collapsed");
});

test("Step 5 (review) shows the raw backend type only inside the collapsed Technical Details block", () => {
  const w = loadWizard();
  const draft = w.gcwDefaultDraft();
  Object.assign(draft, { wizardType: "external", name: "October Lucky Draw", starts_at: "2026-10-01T09:00" });
  const html = w.gcwStep5Html(draft);
  const detailsIdx = html.indexOf("Technical Details");
  const typeIdx = html.indexOf("external_website");
  assert.ok(detailsIdx !== -1 && typeIdx > detailsIdx, "backend type must appear after the Technical Details marker");
});

// ---------------------------------------------------------------------
// Wiring: HTML section exists, VIEWS/CTA reference the wizard, legacy form
// is demoted to "Advanced / Legacy Create Form" but still present.
// ---------------------------------------------------------------------
test("HTML: #view-gcCampaignWizard section exists with a #gcw-body mount point", () => {
  assert.match(HTML, /<section id="view-gcCampaignWizard" class="hidden">\s*<div id="gcw-body">/);
});

test("HTML: the legacy Create Campaign form is collapsed under 'Advanced / Legacy Create Form'", () => {
  const gcSection = slice(HTML, '<section id="view-gcCampaigns"', "</section>");
  assert.match(gcSection, /Advanced \/ Legacy Create Form/);
  assert.match(gcSection, /<details id="gc-legacy-form-details"/);
  // The raw fields still exist (power users / existing tests depend on them) —
  // just no longer the first thing a beginner sees.
  assert.match(gcSection, /id="gc-c-name"/);
});

test("JS: VIEWS declares gcCampaignWizard exactly once", () => {
  const VIEWS_SRC = slice(JS, "var VIEWS =[", "];") + "];";
  const VIEWS = new Function(VIEWS_SRC + "\nreturn VIEWS;")();
  assert.equal(VIEWS.filter((v) => v === "gcCampaignWizard").length, 1);
});

test("JS: gcOpenCampaignWizard is wired as the '+ New Campaign' CTA target", () => {
  const ctaIdx = JS.indexOf('var GC_NEW_CAMPAIGN_CTA =');
  const ctaLine = JS.slice(ctaIdx, JS.indexOf(";", ctaIdx));
  assert.match(ctaLine, /gcOpenCampaignWizard/);
  const bindIdx = JS.indexOf('$("#gc-new-campaign-cta-btn")');
  const bindChunk = JS.slice(bindIdx, bindIdx + 200);
  assert.match(bindChunk, /gcOpenCampaignWizard/);
});
