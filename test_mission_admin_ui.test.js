/**
 * Mission Reward Pool — admin UI wiring (Phase 2, §24-§41).
 *
 * These are source-level contract tests over static/admin-dashboard.js and
 * static/admin-dashboard.html. They exist to catch the specific P0 blockers
 * in §61 that are decided by the admin frontend:
 *
 *   * the Close UI must call the official close endpoint, never write status
 *   * mission_config fields must be disabled proactively once frozen
 *   * only backend-approved voucher pools may be offered
 *   * Mission Pool admin must live inside the existing Campaign Centre
 *
 * Run with: node --test test_mission_admin_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");
const HTML = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.html"), "utf8");

// Values evaluated inside a vm context carry that realm's prototypes, so
// deepEqual would compare structurally-identical arrays as unequal. Bring
// them back into this realm before asserting.
function plain(value) { return JSON.parse(JSON.stringify(value)); }

// Assertions about what the CODE does must not be satisfied (or broken) by
// prose in a comment.
function stripComments(src) {
  return src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
}

// ---------------------------------------------------------------------------
// Structure (§24, §25, §28)
// ---------------------------------------------------------------------------

test("Mission Pool is a campaign type in the existing Campaign Centre form", () => {
  // §24: no separate Mission admin application.
  assert.ok(HTML.includes('<option value="mission_pool">'));
  assert.ok(HTML.includes('id="gc-c-type"'));
  assert.ok(HTML.includes('id="gc-mission-fields"'), "mission fields live in the same Create Campaign section");
});

test("mission fields are hidden until Mission Pool is selected", () => {
  const block = HTML.slice(HTML.indexOf('id="gc-mission-fields"'), HTML.indexOf('id="gc-mission-fields"') + 200);
  assert.ok(/style="display:none/.test(block), "mission fields start hidden");
});

test("every §25 minimum field exists and maps onto the Phase 1 schema", () => {
  const required = [
    "gc-c-name",          // Campaign Name
    "gc-m-type",          // Mission Type
    "gc-m-prompt",        // Question / Instruction
    "gc-m-options",       // Options
    "gc-m-correct",       // Correct Answer
    "gc-c-starts",        // Start Time
    "gc-c-ends",          // End Time
    "gc-m-pool",          // Voucher Pool
    "gc-m-winners",       // Winner Count
    "gc-m-allocation",    // Allocation Method
    "gc-m-el-correct",    // Eligibility policy
    "gc-m-el-hunter",
    "gc-m-el-multi",
    "gc-m-el-blocked",
    "gc-m-el-gaming",
  ];
  required.forEach((id) => assert.ok(HTML.includes('id="' + id + '"'), "missing field " + id));
});

test("mission types offered match the Phase 1 enum exactly", () => {
  const start = HTML.indexOf('id="gc-m-type"');
  const block = HTML.slice(start, HTML.indexOf("</select>", start));
  const values = [...block.matchAll(/value="([a-z_]+)"/g)].map((m) => m[1]);
  assert.deepEqual(values.sort(), ["feedback", "keyword", "multiple_choice", "single_choice"]);
});

test("allocation methods offered match the Phase 1 enum exactly", () => {
  const start = HTML.indexOf('id="gc-m-allocation"');
  const block = HTML.slice(start, HTML.indexOf("</select>", start));
  const values = [...block.matchAll(/value="([a-z_]+)"/g)].map((m) => m[1]);
  assert.deepEqual(values.sort(), ["first_qualified", "random_qualified"]);
});

test("no frontend-only mission configuration is invented", () => {
  // §25: every mission_config key the form sends must be one Phase 1's
  // validate_mission_config actually accepts.
  const accepted = new Set([
    "mission_type", "prompt", "options", "correct_answer",
    "keyword_case_insensitive", "min_chars", "max_chars",
  ]);
  const fn = JS.slice(JS.indexOf("function missionConfigFromForm"), JS.indexOf("function missionPoolFromForm"));
  [...fn.matchAll(/cfg\.([a-z_]+)\s*=/g)].forEach((m) => {
    assert.ok(accepted.has(m[1]), "mission_config key not in the Phase 1 schema: " + m[1]);
  });
});

test("mission_pool block keys match validate_mission_pool_config", () => {
  const accepted = new Set(["pool_id", "pool_type", "winner_count", "allocation_method", "eligibility_policy"]);
  const fn = JS.slice(JS.indexOf("function missionPoolFromForm"), JS.indexOf("function applyMissionFreeze"));
  [...fn.matchAll(/^\s{6}([a-z_]+):/gm)].forEach((m) => {
    assert.ok(accepted.has(m[1]), "mission_pool key not in the Phase 1 schema: " + m[1]);
  });
});

// ---------------------------------------------------------------------------
// Conditional mission-type UI (§28)
// ---------------------------------------------------------------------------

test("each mission type shows only fields the backend supports", () => {
  const ctx = { module: {} };
  vm.createContext(ctx);
  const table = JS.slice(JS.indexOf("var MISSION_TYPE_FIELDS = {"), JS.indexOf("// mission_config fields Phase 1 freezes"));
  vm.runInContext(table + "\nmodule.exports = MISSION_TYPE_FIELDS;", ctx);
  const fields = plain(ctx.module.exports);

  assert.deepEqual(fields.multiple_choice, ["prompt", "options", "correct"]);
  assert.deepEqual(fields.single_choice, ["prompt", "options", "correct"]);
  // keyword: instruction + keyword + only the normalisation option Phase 1
  // actually implements (keyword_case_insensitive). No stemming/fuzzy knobs.
  assert.deepEqual(fields.keyword, ["prompt", "correct", "case_insensitive"]);
  // feedback: instruction + the length bounds Phase 1 validates.
  assert.deepEqual(fields.feedback, ["prompt", "min_chars", "max_chars"]);
  // No option list for a free-text type.
  assert.equal(fields.keyword.indexOf("options"), -1);
  assert.equal(fields.feedback.indexOf("options"), -1);
});

// ---------------------------------------------------------------------------
// mission_config freeze (§26, §61)
// ---------------------------------------------------------------------------

test("frozen mission_config fields are disabled proactively, not left to a 409", () => {
  const fn = JS.slice(JS.indexOf("function applyMissionFreeze"), JS.indexOf("// Only the actions valid"));
  assert.ok(fn.includes("node.disabled = locked"), "fields must be disabled, not merely warned about");
  assert.ok(fn.includes("Mission details can no longer be edited because participants have already submitted entries."),
    "the exact §26 explanation must be shown");
});

test("the frozen field list is the mission_config field list", () => {
  const ctx = { module: {} };
  vm.createContext(ctx);
  const decl = JS.slice(JS.indexOf("var MISSION_CONFIG_INPUT_IDS = ["), JS.indexOf("var gcMissionSelectedId"));
  vm.runInContext(decl + "\nmodule.exports = MISSION_CONFIG_INPUT_IDS;", ctx);
  assert.deepEqual(plain(ctx.module.exports).sort(), [
    "gc-m-case-insensitive", "gc-m-correct", "gc-m-max-chars",
    "gc-m-min-chars", "gc-m-options", "gc-m-prompt", "gc-m-type",
  ]);
});

test("a frozen mission_config is not resent on save", () => {
  // Avoids tripping the backend freeze with an unchanged PUT while still
  // allowing operator fields (pool/winners/eligibility) to be edited.
  const fn = JS.slice(JS.indexOf("function bindGcCampaigns"), JS.indexOf('var createBtn = $("#gc-create-campaign-btn");'));
  assert.ok(fn.includes('if (!$("#gc-m-prompt").disabled) body.mission_config = missionConfigFromForm();'));
  assert.ok(fn.includes("mission_config_locked"), "the backend rejection is still handled");
});

test("schedule editability is reported separately from the config freeze", () => {
  // §27: mission_config being frozen does not imply the schedule is.
  const fn = JS.slice(JS.indexOf("function applyMissionFreeze"), JS.indexOf("// Only the actions valid"));
  assert.ok(fn.includes("schedule_editable"), "schedule state comes from the backend, not from `locked`");
  assert.ok(!/gc-c-starts[\s\S]{0,80}locked/.test(fn), "schedule must not be disabled by the mission_config freeze");
});

// ---------------------------------------------------------------------------
// Lifecycle (§30-§34, §61)
// ---------------------------------------------------------------------------

function actionsFor(state) {
  const ctx = { module: {} };
  vm.createContext(ctx);
  const fn = JS.slice(JS.indexOf("function missionActionsFor"), JS.indexOf("function renderMissionOps"));
  vm.runInContext(fn + "\nmodule.exports = missionActionsFor;", ctx);
  return plain(ctx.module.exports(state).map((a) => a[0]));
}

test("only valid actions are offered for each backend state", () => {
  assert.deepEqual(actionsFor({ campaign_status: "draft", processing_stage: "pending" }), ["publish"]);
  assert.deepEqual(actionsFor({ campaign_status: "scheduled", processing_stage: "pending" }), ["publish"]);
  assert.deepEqual(actionsFor({ campaign_status: "live", processing_stage: "pending" }), ["pause", "close", "cancel"]);
  assert.deepEqual(actionsFor({ campaign_status: "paused", processing_stage: "pending" }), ["publish", "cancel"]);
  assert.deepEqual(actionsFor({ campaign_status: "ended", processing_stage: "pending" }), ["process"]);
  assert.deepEqual(actionsFor({ campaign_status: "ended", processing_stage: "allocating_rewards" }), ["process"]);
  assert.deepEqual(actionsFor({ campaign_status: "ended", processing_stage: "completed" }), ["summary"]);
  assert.deepEqual(actionsFor({ campaign_status: "live", cancelled: true }), ["resume"]);
});

test("a live campaign is never offered Process, and a draft is never offered Close", () => {
  assert.equal(actionsFor({ campaign_status: "live", processing_stage: "pending" }).indexOf("process"), -1);
  assert.equal(actionsFor({ campaign_status: "draft", processing_stage: "pending" }).indexOf("close"), -1);
});

test("in-flight processing offers Resume Processing, never a reroll", () => {
  const fn = JS.slice(JS.indexOf("function missionActionsFor"), JS.indexOf("function renderMissionOps"));
  assert.ok(fn.includes("Resume Processing"));
  assert.ok(fn.includes("Process Campaign"));
  // §34: dangerous wording must not appear anywhere in the admin surface.
  ["Run Again", "Recalculate Winners", "Reroll", "Re-roll"].forEach((bad) => {
    assert.equal(JS.indexOf(bad), -1, `dangerous action wording present: ${bad}`);
  });
});

test("Close goes through the official mission endpoint, never a status write", () => {
  // §31 / §61: the UI must not implement close by setting status itself.
  const fn = JS.slice(JS.indexOf("function runMissionAction"), JS.indexOf("function bindGcCampaigns"));
  assert.ok(fn.includes('"/api/admin/mission-pool/" + encodeURIComponent(id) + "/" + action'));
  assert.ok(!/status:\s*["']ended["']/.test(JS), "the admin UI must never set status=ended directly");
  assert.ok(!/gc-campaigns[^"']*\/close/.test(JS), "close must not be routed through the generic campaign endpoint");
});

test("Close and Cancel confirm with the documented copy", () => {
  const fn = JS.slice(JS.indexOf("var MISSION_CONFIRM = {"), JS.indexOf("function runMissionAction"));
  assert.ok(fn.includes("Close this mission now?"));
  assert.ok(fn.includes("New valid entries after the close cutoff will not be eligible for rewards."));
  assert.ok(fn.includes("Cancel this mission?"));
  assert.ok(fn.includes("New submissions and new reward distribution will stop."));
  // §32: never imply allocated rewards are revoked.
  assert.ok(fn.includes("Rewards already allocated to winners will remain valid."));
});

test("no admin control offers a way to move the close cutoff", () => {
  // §31/§61: closed_at is write-once server-side; nothing here may send it.
  assert.equal(/closed_at\s*[:=]\s*(new Date|Date\.)/.test(JS), false);
  const ops = JS.slice(JS.indexOf("function renderMissionOps"), JS.indexOf("function openMissionOps"));
  assert.ok(ops.includes('["Close cutoff (closed_at)", state.closed_at'), "closed_at is displayed read-only");
});

// ---------------------------------------------------------------------------
// Pools (§29, §61)
// ---------------------------------------------------------------------------

test("voucher pools come from the backend-filtered mission endpoint", () => {
  const fn = JS.slice(JS.indexOf("function loadMissionPools"), JS.indexOf("function missionConfigFromForm"));
  assert.ok(fn.includes("/api/admin/mission-pool/pools"));
  // Neither the protected-pool list nor the allowed-scope set may be
  // hardcoded in the Mission admin block — the backend endpoint is what
  // excludes them (§29). Scoped to the Mission block: unrelated pre-existing
  // admin code (the affiliate pool panel) legitimately names WELCOME/T1-T5.
  const missionCode = stripComments(
    JS.slice(JS.indexOf("// ---------- Mission Reward Pool (Phase 2 admin)"),
             JS.indexOf("// ---------- Event Banner"))
  );
  ['"WELCOME"', '"T1"', '"T2"', '"T3"', '"T4"', '"T5"', "RESERVED_LEGACY_POOL_IDS",
    "allocation_scope"].forEach((bad) => {
    assert.equal(missionCode.indexOf(bad), -1, `protected/allowed pool set hardcoded in the UI: ${bad}`);
  });
});

// ---------------------------------------------------------------------------
// Review fixes (Codex findings on 7944189)
// ---------------------------------------------------------------------------

test("the selected pool's real pool_type is submitted, not a hardcoded one", () => {
  // The processor passes mission_pool.pool_type to
  // voucher_pool_service.allocate_voucher as expected_pool_type, which
  // filters the inventory row on it. Storing "voucher_drop" for a pool
  // registered as tournament_reward/vip would make every allocation miss
  // and mark winners out_of_stock while the UI showed stock available.
  const fn = JS.slice(JS.indexOf("function selectedPoolType"), JS.indexOf("function applyMissionFreeze"));
  assert.ok(fn.includes('opt.getAttribute("data-pool-type")'), "the option's real type must be read");
  assert.ok(fn.includes("pool_type: selectedPoolType()"), "the real type must be submitted");
  assert.equal(/pool_type:\s*["']voucher_drop["']/.test(fn), false,
    "pool_type must not be hardcoded in the submitted body");
});

test("the pool dropdown carries each pool's real type", () => {
  const fn = JS.slice(JS.indexOf("function loadMissionPools"), JS.indexOf("function ensurePoolOption"));
  assert.ok(fn.includes('data-pool-type="'), "each option must carry its registered pool_type");
});

test("a stored pool that is no longer listed is preserved, not silently swapped", () => {
  const fn = JS.slice(JS.indexOf("function ensurePoolOption"), JS.indexOf("function missionConfigFromForm"));
  assert.ok(fn.includes("not currently listed as mission-compatible"));
  assert.ok(fn.includes("sel.value = poolId"), "the campaign's own pool must stay selected");
});

test("Mission Ops hydrates the form from the campaign before enabling Save", () => {
  // Without hydration the shared create/edit form still holds blank or
  // another campaign's values, and Save would PUT those over the selected
  // campaign's real operator settings.
  const fn = JS.slice(JS.indexOf("function openMissionOps"), JS.indexOf("var MISSION_CONFIRM"));
  assert.ok(fn.includes('apiSoft("/api/admin/gc-campaigns/"'), "the campaign document must be fetched");
  assert.ok(fn.includes("hydrateMissionForm(campaignResp.campaign)"));
  assert.ok(fn.includes('saveBtn.style.display = "none"'), "Save must be hidden until hydration succeeds");
  // Save must not be exposed on the failure path.
  const failBranch = fn.slice(fn.indexOf("Could not load campaign values") - 400, fn.indexOf("Could not load campaign values") + 120);
  assert.ok(failBranch.includes("return;"), "a failed hydration must return before Save is shown");
});

test("hydration populates every editable mission field", () => {
  const fn = JS.slice(JS.indexOf("function hydrateMissionForm"), JS.indexOf("function openMissionOps"));
  ["gc-c-name", "gc-m-type", "gc-m-prompt", "gc-m-options", "gc-m-correct",
    "gc-m-case-insensitive", "gc-m-min-chars", "gc-m-max-chars",
    "gc-m-winners", "gc-m-allocation", "gc-c-starts", "gc-c-ends",
    "gc-m-el-correct", "gc-m-el-hunter", "gc-m-el-multi", "gc-m-el-blocked", "gc-m-el-gaming",
    // Either form: a setValue("id", ...) call or a $("#id") lookup.
  ].forEach((id) => assert.ok(fn.includes('"' + id + '"') || fn.includes('"#' + id + '"'),
    "hydration misses " + id));
  assert.ok(fn.includes("ensurePoolOption(block.pool_id"), "the stored pool must be selected");
  // Schedule goes through setScheduleValue so the exact instant is retained.
  assert.ok(fn.includes('setScheduleValue("gc-c-starts"'));
  assert.ok(fn.includes('setScheduleValue("gc-c-ends"'));
});

test("the freeze is applied after hydration, so it wins", () => {
  const fn = JS.slice(JS.indexOf("function openMissionOps"), JS.indexOf("var MISSION_CONFIRM"));
  const hydrateIdx = fn.indexOf("hydrateMissionForm(campaignResp.campaign)");
  const freezeIdx = fn.indexOf("applyMissionFreeze(state)", hydrateIdx);
  assert.ok(hydrateIdx !== -1 && freezeIdx > hydrateIdx,
    "applyMissionFreeze must run after hydration re-enables fields");
});

test("an editable schedule is actually sent on save", () => {
  // campaign_centre._validate_body leaves `schedule` untouched on a partial
  // update that omits it, so omitting it means the operator sees
  // "Mission saved" while neither date is persisted.
  const fn = JS.slice(JS.indexOf("function bindGcCampaigns"), JS.indexOf('var createBtn = $("#gc-create-campaign-btn");'));
  assert.ok(fn.includes('if (!$("#gc-c-starts").disabled) {'), "schedule is sent only when editable");
  assert.ok(fn.includes("body.schedule = {"));
  assert.ok(fn.includes("starts_at:") && fn.includes("ends_at:"));
});

test("hydration converts stored ISO timestamps to local datetime-local values", () => {
  const ctx = { module: {} };
  vm.createContext(ctx);
  const fn = JS.slice(JS.indexOf("function isoToLocalInput"), JS.indexOf("function setValue"));
  vm.runInContext(fn + "\nmodule.exports = isoToLocalInput;", ctx);
  const isoToLocalInput = ctx.module.exports;
  assert.equal(isoToLocalInput(""), "");
  assert.equal(isoToLocalInput(null), "");
  assert.equal(isoToLocalInput("not-a-date"), "");
  const out = isoToLocalInput("2026-09-30T12:34:00+00:00");
  assert.match(out, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/, "must be a seconds-precision datetime-local value");
  // Round-trips back to the same instant the API gave us.
  assert.equal(new Date(out).getTime(), new Date("2026-09-30T12:34:00+00:00").getTime());
  // Seconds must survive: schedule.ends_at is a Phase 1 eligibility cutoff,
  // so truncating to the minute could drop valid submissions near the end.
  const withSeconds = isoToLocalInput("2026-09-30T12:34:37+00:00");
  assert.equal(new Date(withSeconds).getTime(), new Date("2026-09-30T12:34:37+00:00").getTime());
});

// ---------------------------------------------------------------------------
// Review fixes round 2 (Codex findings on aab3588)
// ---------------------------------------------------------------------------

test("hydration remembers option labels so a save cannot overwrite them", () => {
  // The options field edits IDs. Rebuilding {id, label} from ids alone would
  // replace every participant-facing label with its id for a campaign whose
  // labels differ from its ids — even when the operator never touched them.
  const hydrate = JS.slice(JS.indexOf("function hydrateMissionForm"), JS.indexOf("function openMissionOps"));
  assert.ok(hydrate.includes("labelMap[o.id] = o.label || o.id"), "an id->label map must be captured");
  assert.ok(hydrate.includes("optionsInput.dataset.labels"), "the map must be retained for save");

  const build = JS.slice(JS.indexOf("function missionConfigFromForm"), JS.indexOf("function selectedPoolType"));
  assert.ok(build.includes("knownLabels[o] || o"), "a known id must keep its existing label");
  assert.equal(/label:\s*o\s*\}/.test(build), false, "label must not be blindly set to the id");
});

test("option label preservation round-trips through the real functions", () => {
  const ctx = {
    module: {},
    JSON,
    fields: { "gc-m-type": { value: "multiple_choice" }, "gc-m-prompt": { value: "Q" },
              "gc-m-options": { value: "a, b", dataset: { labels: '{"a":"Alpha","b":"Beta"}' } },
              "gc-m-correct": { value: "a" } },
  };
  ctx.$ = (sel) => ctx.fields[sel.slice(1)];
  vm.createContext(ctx);
  const fn = JS.slice(JS.indexOf("function missionConfigFromForm"), JS.indexOf("function selectedPoolType"));
  vm.runInContext(fn + "\nmodule.exports = missionConfigFromForm;", ctx);
  const cfg = plain(ctx.module.exports());
  assert.deepEqual(cfg.options, [{ id: "a", label: "Alpha" }, { id: "b", label: "Beta" }],
    "untouched options keep their labels");

  // A newly typed id falls back to id-as-label, matching the create flow.
  ctx.fields["gc-m-options"].value = "a, c";
  const cfg2 = plain(ctx.module.exports());
  assert.deepEqual(cfg2.options, [{ id: "a", label: "Alpha" }, { id: "c", label: "c" }]);
});

test("a stale Mission Ops load can neither hydrate nor reveal Save", () => {
  // Opening campaign A then B, with A resolving last, must not hydrate the
  // shared form with A's values while B is selected — Save would PUT A onto B.
  const fn = JS.slice(JS.indexOf("function openMissionOps"), JS.indexOf("var MISSION_CONFIRM"));
  assert.ok(fn.includes("var token = ++gcMissionLoadToken"), "each load needs a token");
  assert.ok(fn.includes("token !== gcMissionLoadToken || gcMissionSelectedId !== campaignId"),
    "both the token and the selected id must be checked");
  // Guarded before issuing the requests, before hydrating, and again
  // immediately before Save is revealed.
  assert.ok(fn.split("stale()").length - 1 >= 3, "the guard must run at every stage");
  const revealIdx = fn.indexOf('saveBtn.style.display = ""');
  const lastGuardIdx = fn.lastIndexOf("if (stale()) return;", revealIdx);
  assert.ok(lastGuardIdx !== -1 && lastGuardIdx < revealIdx,
    "the last guard must sit immediately before Save is revealed");
});

test("an untouched schedule field resends the exact original instant", () => {
  // Converting a displayed value back to ISO can only lose precision; an
  // unedited field must resend what the server gave us, verbatim.
  const fn = JS.slice(JS.indexOf("function scheduleValueForSave"), JS.indexOf("function setValue"));
  assert.ok(fn.includes("node.dataset.hydratedValue === current && node.dataset.originalIso"));
  assert.ok(fn.includes("return node.dataset.originalIso;"));
});

test("schedule inputs accept seconds", () => {
  // Without step="1" a browser silently clamps datetime-local to the minute.
  const start = HTML.indexOf('id="gc-c-starts"');
  const end = HTML.indexOf('id="gc-c-ends"');
  assert.ok(/step="1"/.test(HTML.slice(start, start + 160)), "gc-c-starts must allow seconds");
  assert.ok(/step="1"/.test(HTML.slice(end, end + 160)), "gc-c-ends must allow seconds");
});

// ---------------------------------------------------------------------------
// Summary / disqualification reasons (§36, §37, §38)
// ---------------------------------------------------------------------------

test("the summary is read from the Phase 1 summary API, not computed client-side", () => {
  const fn = JS.slice(JS.indexOf("function openMissionOps"), JS.indexOf("var MISSION_CONFIRM"));
  assert.ok(fn.includes("/summary"));
  assert.ok(fn.includes("/edit-state"));
  const render = JS.slice(JS.indexOf("function renderMissionOps"), JS.indexOf("function openMissionOps"));
  // §36: no client-side aggregation over raw mission entries.
  assert.equal(render.indexOf("mission_entries"), -1);
  assert.ok(!/\.reduce\(/.test(render), "no client-side rollup over entries");
});

test("every §36 summary row is displayed", () => {
  const render = JS.slice(JS.indexOf("function renderMissionOps"), JS.indexOf("function openMissionOps"));
  ["Campaign Status", "Processing stage", "Submissions", "Deduplicated identities",
    "Qualified", "Disqualified", "Winner target", "Winners selected",
    "Rewards allocated", "Notifications sent", "Notification failures",
  ].forEach((label) => assert.ok(render.includes(label), "missing summary row: " + label));
});

test("machine-readable disqualification reasons are mapped to human labels", () => {
  const render = JS.slice(JS.indexOf("function renderMissionOps"), JS.indexOf("function openMissionOps"));
  ["duplicate_identity", "voucher_hunter", "multi_account_risk", "incorrect_answer",
    "missing_gaming_account", "other"].forEach((code) => {
    assert.ok(render.includes(code + ":"), "unmapped disqualification reason: " + code);
  });
  assert.ok(render.includes("(admin only)"), "reasons must be marked admin-only");
});

// ---------------------------------------------------------------------------
// Mission link (§41)
// ---------------------------------------------------------------------------

test("the mission link is server-generated, never hand-built in the UI", () => {
  // §41: link generation is centralised in mission_pool_ux.mission_deep_link.
  assert.equal(/["']https:\/\/t\.me\/["']\s*\+/.test(JS), false, "the admin UI must not concatenate a t.me link");
  assert.equal(JS.indexOf("?startapp=mission_"), -1, "the start param must not be built in the frontend");
  const render = JS.slice(JS.indexOf("function renderMissionOps"), JS.indexOf("function openMissionOps"));
  assert.ok(render.includes("state.mission_link"), "the link comes from the backend");
  assert.ok(render.includes("Copy Link"));
});

test("an unavailable mission link is explained rather than shown broken", () => {
  const render = JS.slice(JS.indexOf("function renderMissionOps"), JS.indexOf("function openMissionOps"));
  assert.ok(render.includes("mission_link_unavailable_reason"));
});

// ---------------------------------------------------------------------------
// Duplicate campaign (§35)
// ---------------------------------------------------------------------------

test("Duplicate uses the existing Campaign Centre duplicate endpoint", () => {
  // Phase 1 already resets worker-owned state there
  // (campaign_centre.duplicate_campaign -> duplicated_mission_pool_config).
  assert.ok(JS.includes('data-gc-action="duplicate"'));
  assert.ok(JS.includes('"/api/admin/gc-campaigns/" + id + "/duplicate"'));
});

// ---------------------------------------------------------------------------
// Standard Drop / existing admin surfaces untouched (§5, §50)
// ---------------------------------------------------------------------------

test("existing campaign types are still offered unchanged", () => {
  const start = HTML.indexOf('id="gc-c-type"');
  const block = HTML.slice(start, HTML.indexOf("</select>", start));
  ["tournament", "external_subscription_verification", "external_website"].forEach((t) => {
    assert.ok(block.includes('value="' + t + '"'), "existing campaign type removed: " + t);
  });
});

test("mission admin code never touches Standard Drop endpoints", () => {
  const missionCode = JS.slice(JS.indexOf("// ---------- Mission Reward Pool (Phase 2 admin)"),
                               JS.indexOf("// ---------- Event Banner"));
  ["/api/admin/drops", "/api/admin/vouchers", "api_claim", "user_visible_drops",
    "/api/admin/pools/upload"].forEach((bad) => {
    assert.equal(missionCode.indexOf(bad), -1, "mission admin reached into Standard Drop: " + bad);
  });
});
