/**
 * Mission Reward Pool — dedicated admin experience (Phase 2.1).
 *
 * WHY THIS IS ITS OWN FILE
 * ------------------------
 * Phase 2 put Mission creation and Mission editing on the SAME Campaign
 * Centre form. Every wrong-campaign write found in review came from that:
 * two intents sharing one set of controls, one set of hydrated values and
 * one save handler, distinguishable only by a flag. This module removes the
 * shared form instead of guarding it. There are four modes:
 *
 *     LIST  -> no campaign target at all
 *     CREATE-> no campaign target at all, ever
 *     VIEW  -> a fixed, read-only campaign target
 *     EDIT  -> a fixed, immutable campaign target
 *
 * They are mutually exclusive, each renders a full DOM replacement (so no
 * control from another mode can survive a transition), and the create and
 * save paths are authorised by the session state machine — not by whether a
 * button happens to be visible.
 *
 * WHAT THIS MODULE DELIBERATELY DOES NOT DO
 * -----------------------------------------
 *   * It does not introduce a Mission-specific voucher inventory. Inline
 *     "create a reward pool" calls the canonical Voucher Centre / Campaign
 *     Rewards endpoints (POST /api/admin/reward-pools and
 *     /api/admin/reward-pools/<id>/upload-codes) — the same ones the
 *     tournament reward flow uses.
 *   * It does not build Telegram links. `mission_link` comes from
 *     mission_pool_ux.mission_deep_link.
 *   * It does not decide which pools are eligible, what a pool's type is, or
 *     whether inventory is sufficient. Those come from the backend
 *     (/api/admin/mission-pool/pools and /inventory-check).
 *   * It does not aggregate raw mission entries. Every counter comes from
 *     /api/admin/mission-pool/campaigns or /summary.
 *   * It never touches Standard Drop, pooled/personalised vouchers, Welcome
 *     or Affiliate surfaces.
 *
 * The module is written so its decision-making core is importable and
 * testable without a DOM (see test_mission_admin_ui.test.js).
 */
(function (root, factory) {
  var mod = factory();
  if (typeof module === "object" && module.exports) module.exports = mod;
  if (root) root.MissionAdmin = mod;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  // =======================================================================
  // CORE (no DOM) — the part that decides what may happen.
  // =======================================================================

  var MODE_LIST = "list";
  var MODE_CREATE = "create";
  var MODE_VIEW = "view";
  var MODE_EDIT = "edit";

  // Mission types and allocation methods are the Phase 1 enums
  // (mission_pool.MISSION_TYPES / ALLOCATION_METHODS). Field visibility per
  // type is the set of mission_config keys validate_mission_config actually
  // reads for that type — nothing frontend-only is invented.
  var MISSION_TYPE_FIELDS = {
    multiple_choice: ["prompt", "options", "correct"],
    single_choice: ["prompt", "options", "correct"],
    keyword: ["prompt", "correct", "case_insensitive"],
    feedback: ["prompt", "min_chars", "max_chars"],
  };

  var MISSION_TYPE_LABELS = {
    multiple_choice: "Multiple Choice",
    single_choice: "Single Choice",
    keyword: "Keyword",
    feedback: "Feedback",
  };

  var ALLOCATION_LABELS = {
    random_qualified: "Random Qualified",
    first_qualified: "First Qualified",
  };

  var STATE_LABELS = {
    live: "LIVE", scheduled: "SCHEDULED", draft: "DRAFT", paused: "PAUSED",
    closed: "CLOSED", processing: "PROCESSING", completed: "COMPLETED",
    cancelled: "CANCELLED",
  };

  // Landing-page grouping. Every backend state maps to exactly one group, so
  // a campaign can never be listed twice or disappear.
  var LIST_GROUPS = [
    { key: "live", title: "Live", states: ["live", "paused"] },
    { key: "scheduled", title: "Scheduled", states: ["scheduled"] },
    { key: "draft", title: "Drafts", states: ["draft"] },
    { key: "processing", title: "Closed / Processing", states: ["closed", "processing"] },
    { key: "completed", title: "Completed", states: ["completed"] },
    { key: "cancelled", title: "Cancelled", states: ["cancelled"] },
  ];

  // Option ids may legally contain commas (validate_mission_config accepts
  // any non-empty string), so options are edited ONE PER LINE, never in a
  // comma-separated field. A pipe separates an optional player-facing label
  // from the id; an id that itself contains a pipe cannot round-trip through
  // this syntax, so the editor refuses to edit those rather than corrupting
  // them (see optionsEditable).
  var OPTION_LABEL_SEPARATOR = "|";

  /**
   * "one option per line" -> Phase 1's [{id, label}].
   *
   * `id`                -> {id: "id", label: <known label> || "id"}
   * `id | Player Label` -> {id: "id", label: "Player Label"}
   *
   * `knownLabels` is the id->label map captured when an existing campaign was
   * hydrated. Without it, editing an unrelated line would rewrite every other
   * option's participant-facing label to its raw id.
   */
  function parseOptionLines(text, knownLabels) {
    knownLabels = knownLabels || {};
    var options = [];
    var seen = {};
    var errors = [];
    String(text == null ? "" : text).split("\n").forEach(function (rawLine) {
      var line = rawLine.trim();
      if (!line) return;
      var id = line;
      var label = null;
      var cut = line.indexOf(OPTION_LABEL_SEPARATOR);
      if (cut !== -1) {
        id = line.slice(0, cut).trim();
        label = line.slice(cut + 1).trim();
      }
      if (!id) { errors.push("An option line has no id: " + rawLine); return; }
      if (Object.prototype.hasOwnProperty.call(seen, id)) {
        errors.push("Duplicate option id: " + id);
        return;
      }
      seen[id] = true;
      options.push({
        id: id,
        label: label || (Object.prototype.hasOwnProperty.call(knownLabels, id) ? knownLabels[id] : id),
      });
    });
    return { options: options, errors: errors };
  }

  /** Inverse of parseOptionLines, for hydrating the edit view. */
  function formatOptionLines(options) {
    return (options || []).map(function (o) {
      var id = (o && o.id) || "";
      var label = (o && o.label) || "";
      return (label && label !== id) ? id + " " + OPTION_LABEL_SEPARATOR + " " + label : id;
    }).join("\n");
  }

  /**
   * An option id containing the label separator cannot survive a text
   * round-trip. Rather than silently splitting it (which would rewrite the
   * mission, or invalidate correct_answer), the options editor is disabled
   * for that campaign and the operator is told why.
   */
  function optionsEditable(options) {
    return !(options || []).some(function (o) {
      return String((o && o.id) || "").indexOf(OPTION_LABEL_SEPARATOR) !== -1;
    });
  }

  /** One voucher code per line: trim, drop blanks, drop exact duplicates. */
  function parseVoucherCodes(text) {
    var codes = [];
    var seen = {};
    var duplicates = 0;
    String(text == null ? "" : text).split("\n").forEach(function (raw) {
      var code = raw.trim();
      if (!code) return;
      var key = "c:" + code;
      if (seen[key]) { duplicates++; return; }
      seen[key] = true;
      codes.push(code);
    });
    return { codes: codes, duplicates: duplicates };
  }

  /**
   * Local mirror of the backend publish gate, for instant feedback only.
   * The BLOCKING decision at publish/save time always uses the verdict
   * returned by /api/admin/mission-pool/inventory-check, so there is one
   * authority and it reads live inventory.
   */
  function inventoryGate(available, winnerCount) {
    var a = parseInt(available, 10) || 0;
    var w = parseInt(winnerCount, 10) || 0;
    return { ok: w > 0 && a >= w, available: a, winner_count: w, shortfall: Math.max(0, w - a) };
  }

  /**
   * Telegram start parameters are [A-Za-z0-9_-] and capped at 64 chars, and
   * mission_pool_ux prefixes "mission_" (8 chars). An id outside that budget
   * produces a campaign with no Mission link at all, which is only
   * discovered after publishing — so it is checked at creation time.
   */
  var MAX_LINK_SAFE_ID_CHARS = 64 - "mission_".length;

  function campaignIdIsLinkSafe(campaignId) {
    return /^[A-Za-z0-9_-]+$/.test(campaignId || "") &&
      String(campaignId).length <= MAX_LINK_SAFE_ID_CHARS;
  }

  function slugify(name) {
    return String(name || "").toLowerCase().trim()
      .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "")
      .slice(0, MAX_LINK_SAFE_ID_CHARS);
  }

  /**
   * State-specific operations only (§19). Never a reroll, a re-run or a
   * recalculation: Phase 1's processor is resumable and idempotent, and
   * offering to "run again" invites an operator to expect a different
   * winner set.
   */
  function actionsFor(state) {
    state = state || {};
    var s = state.state;
    if (s === "cancelled") return [["resume", "Resume (undo cancel)", false]];
    if (s === "draft" || s === "scheduled") return [["publish", "Publish", true], ["edit", "Edit", false]];
    if (s === "live") {
      return [["edit", "Edit", false], ["pause", "Pause", false],
              ["close", "Close Mission", true], ["cancel", "Cancel", true]];
    }
    if (s === "paused") return [["edit", "Edit", false], ["publish", "Resume", false], ["cancel", "Cancel", true]];
    if (s === "closed") return [["process", "Process Campaign", true]];
    if (s === "processing") {
      // Resume Processing only — the backend's process endpoint IS the
      // resume: it re-enters the same resumable state machine and never
      // re-selects winners once selection_seed is set.
      return [["process", "Resume Processing", true]];
    }
    if (s === "completed") return [["results", "View Results", false]];
    return [];
  }

  /**
   * Schedule values are carried as {iso, display} pairs owned by the active
   * edit session. An untouched field resends the EXACT instant the server
   * gave us; only an edited one is converted from what the operator typed.
   * schedule.ends_at is one of Phase 1's eligibility cutoffs, so a
   * conversion that moved it by even a second could drop valid submissions.
   */
  function isoToLocalInput(iso) {
    if (!iso) return "";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return "";
    var pad = function (n) { return (n < 10 ? "0" : "") + n; };
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
      "T" + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
  }

  function localInputToIso(value) {
    if (!value) return null;
    var d = new Date(value);
    return isNaN(d.getTime()) ? null : d.toISOString();
  }

  function scheduleFieldFrom(iso) {
    return { iso: iso || "", display: isoToLocalInput(iso) };
  }

  function scheduleValueForSave(field, currentValue) {
    field = field || { iso: "", display: "" };
    var current = currentValue || "";
    if (!current) return null;
    if (field.display === current && field.iso) return field.iso;
    return localInputToIso(current);
  }

  // -----------------------------------------------------------------------
  // The session state machine.
  // -----------------------------------------------------------------------

  /**
   * Invariant, enforced here rather than by button visibility (§14):
   *
   *     createModeTarget = none        (always, unconditionally)
   *     editModeTarget   = campaign_id (only while mode === "edit")
   *
   * never both. Every mode transition bumps a monotonic load token, so an
   * async response issued for the previous target is refused on arrival
   * rather than being allowed to mutate whatever is on screen now (§15).
   */
  function createSession() {
    var mode = MODE_LIST;
    var editTarget = null;
    var viewTarget = null;
    var token = 0;
    var hydration = null;   // {campaignId, token} once an edit load completed
    var editState = null;   // per-campaign edit state; never survives a mode change
    var draft = null;       // create-wizard state; never survives a mode change

    function reset(nextMode) {
      // Invalidate every in-flight load, then drop ALL per-campaign state:
      // option label maps, schedule instants, stored pool metadata, the
      // hydration record and the save target. Nothing campaign-specific can
      // be inherited by the next mode.
      token++;
      mode = nextMode;
      editTarget = null;
      viewTarget = null;
      hydration = null;
      editState = null;
      draft = null;
    }

    function targetForMode() {
      if (mode === MODE_EDIT) return editTarget;
      if (mode === MODE_VIEW) return viewTarget;
      return null;
    }

    return {
      MODE_LIST: MODE_LIST, MODE_CREATE: MODE_CREATE, MODE_VIEW: MODE_VIEW, MODE_EDIT: MODE_EDIT,

      mode: function () { return mode; },
      token: function () { return token; },
      /** ALWAYS null. A create action has no campaign target, by construction. */
      createModeTarget: function () { return null; },
      editModeTarget: function () { return mode === MODE_EDIT ? editTarget : null; },
      viewModeTarget: function () { return mode === MODE_VIEW ? viewTarget : null; },
      target: targetForMode,
      draft: function () { return draft; },
      editState: function () { return editState; },
      hydration: function () { return hydration; },

      enterList: function () { reset(MODE_LIST); },
      enterCreate: function (initialDraft) {
        reset(MODE_CREATE);
        draft = initialDraft || {};
        return draft;
      },
      enterView: function (campaignId) {
        reset(MODE_VIEW);
        viewTarget = campaignId || null;
      },
      enterEdit: function (campaignId) {
        reset(MODE_EDIT);
        editTarget = campaignId || null;
        editState = { campaignId: campaignId || null };
      },

      /** Start an async load owned by the current mode+target+token. */
      beginLoad: function (campaignId) {
        return { token: token, campaignId: campaignId === undefined ? targetForMode() : campaignId, mode: mode };
      },

      /**
       * Every async response that would mutate state must pass this — the
       * campaign fetch, the edit-state fetch, the summary fetch AND the pool
       * fetch. Guarding only the final consumer is what let an earlier
       * loader repaint a control for a campaign that was no longer open.
       */
      accepts: function (load) {
        return !!load && load.token === token && load.mode === mode &&
          load.campaignId === targetForMode();
      },

      /** Record that the edit form now holds THIS campaign's real values. */
      completeHydration: function (load, state) {
        if (mode !== MODE_EDIT) return false;
        if (!load || load.token !== token || load.campaignId !== editTarget) return false;
        hydration = { campaignId: editTarget, token: token };
        editState = state || editState || { campaignId: editTarget };
        editState.campaignId = editTarget;
        return true;
      },

      /**
       * Authorise a save. Refuses outside edit mode, without a fixed target,
       * before hydration finished, or when the hydration that filled the form
       * belongs to a different campaign or an invalidated load.
       */
      authorizeSave: function (requestedCampaignId) {
        if (mode !== MODE_EDIT) return { ok: false, code: "not_in_edit_mode" };
        if (!editTarget) return { ok: false, code: "no_edit_target" };
        if (requestedCampaignId != null && requestedCampaignId !== editTarget) {
          return { ok: false, code: "wrong_campaign_target" };
        }
        if (!hydration) return { ok: false, code: "not_hydrated" };
        if (hydration.campaignId !== editTarget) return { ok: false, code: "hydration_target_mismatch" };
        // Assertion of last resort: every mode transition bumps the token AND
        // drops the hydration record, so today this cannot fire. It stays so
        // that a future path which invalidates loads *without* leaving edit
        // mode cannot silently make a stale form saveable.
        if (hydration.token !== token) return { ok: false, code: "hydration_token_stale" };
        return { ok: true, campaignId: editTarget };
      },

      /** Authorise a create. Refuses whenever an edit target exists. */
      authorizeCreate: function () {
        if (mode === MODE_EDIT || editTarget) return { ok: false, code: "in_edit_mode" };
        if (mode !== MODE_CREATE) return { ok: false, code: "not_in_create_mode" };
        return { ok: true, campaignId: null };
      },
    };
  }

  var SAVE_REFUSAL_COPY = {
    not_in_edit_mode: "Not editing a mission — open the mission and choose Edit.",
    no_edit_target: "No mission selected for editing.",
    wrong_campaign_target: "This form is not editing that mission — reopen it.",
    not_hydrated: "Still loading this mission — wait for it to finish before saving.",
    hydration_target_mismatch: "This form holds another mission's values — reopen it.",
    hydration_token_stale: "This form is out of date — reopen the mission.",
    in_edit_mode: "Editing a mission — leave Edit before creating a new one.",
    not_in_create_mode: "Not in the Create Mission flow.",
  };

  // Mission fields Phase 1 freezes once any entry exists. The backend
  // (campaign_centre.update_campaign) is the authority; these are disabled
  // proactively so an operator never types a change that will 409.
  var FROZEN_MISSION_FIELDS = [
    "mission_type", "prompt", "options", "correct_answer",
    "keyword_case_insensitive", "min_chars", "max_chars",
  ];

  var FREEZE_NOTE =
    "Mission details can no longer be edited because participants have already submitted entries.";

  var CONFIRM_COPY = {
    close: "Close this mission now?\n\nNew valid entries after the close cutoff will not be eligible for rewards.",
    cancel: "Cancel this mission?\n\nNew submissions and new reward distribution will stop.\n\nRewards already allocated to winners will remain valid.",
    process: "Process this campaign now?\n\nProcessing resumes where it left off. Winners already selected are never re-selected.",
    publish: "Publish this mission?\n\nIt becomes visible to participants immediately.",
  };

  var CORE = {
    MODE_LIST: MODE_LIST, MODE_CREATE: MODE_CREATE, MODE_VIEW: MODE_VIEW, MODE_EDIT: MODE_EDIT,
    MISSION_TYPE_FIELDS: MISSION_TYPE_FIELDS,
    MISSION_TYPE_LABELS: MISSION_TYPE_LABELS,
    ALLOCATION_LABELS: ALLOCATION_LABELS,
    STATE_LABELS: STATE_LABELS,
    LIST_GROUPS: LIST_GROUPS,
    FROZEN_MISSION_FIELDS: FROZEN_MISSION_FIELDS,
    FREEZE_NOTE: FREEZE_NOTE,
    CONFIRM_COPY: CONFIRM_COPY,
    SAVE_REFUSAL_COPY: SAVE_REFUSAL_COPY,
    OPTION_LABEL_SEPARATOR: OPTION_LABEL_SEPARATOR,
    MAX_LINK_SAFE_ID_CHARS: MAX_LINK_SAFE_ID_CHARS,
    createSession: createSession,
    parseOptionLines: parseOptionLines,
    formatOptionLines: formatOptionLines,
    optionsEditable: optionsEditable,
    parseVoucherCodes: parseVoucherCodes,
    inventoryGate: inventoryGate,
    campaignIdIsLinkSafe: campaignIdIsLinkSafe,
    slugify: slugify,
    actionsFor: actionsFor,
    isoToLocalInput: isoToLocalInput,
    localInputToIso: localInputToIso,
    scheduleFieldFrom: scheduleFieldFrom,
    scheduleValueForSave: scheduleValueForSave,
  };

  // =======================================================================
  // VIEW — renders one mode at a time into a single container.
  // =======================================================================

  var host = null;          // injected admin-dashboard helpers
  var session = createSession();
  var poolsCache = null;    // {pools, pool_types} — data only, never DOM

  function esc(v) { return host.esc(v); }
  function el() { return host.$("#mp-root"); }

  function num(v) {
    if (v === null || v === undefined) return "—";
    var n = Number(v);
    return isNaN(n) ? String(v) : n.toLocaleString();
  }

  function dt(v) { return v ? new Date(v).toLocaleString() : "—"; }

  function statePill(state) {
    var kind = { live: "approved", completed: "approved", scheduled: "pending",
      paused: "pending", processing: "pending", closed: "neutral",
      draft: "neutral", cancelled: "rejected" }[state] || "neutral";
    return '<span class="pill ' + kind + '">' + esc(STATE_LABELS[state] || state || "—") + "</span>";
  }

  function softGet(path) {
    return host.api(path).catch(function (e) { return { status: "error", code: e.message }; });
  }

  /**
   * Pool data is fetched as DATA and cached. It is never allowed to reach
   * into the DOM by itself: the caller applies it only after
   * session.accepts(load), which is why a late pool response can no longer
   * repaint a control belonging to a campaign that is no longer open.
   */
  function loadPools(force) {
    if (poolsCache && !force) return Promise.resolve(poolsCache);
    return softGet("/api/admin/mission-pool/pools").then(function (r) {
      poolsCache = (r && r.status === "ok")
        ? { pools: r.pools || [], pool_types: r.pool_types || [], ok: true }
        : { pools: [], pool_types: [], ok: false, code: (r && r.code) || "pools_unavailable" };
      return poolsCache;
    });
  }

  function inventoryCheck(poolId, winnerCount) {
    return softGet("/api/admin/mission-pool/inventory-check?pool_id=" +
      encodeURIComponent(poolId || "") + "&winner_count=" + encodeURIComponent(winnerCount || 0));
  }

  function render(html) {
    var node = el();
    if (node) node.innerHTML = html;
  }

  function section(title, body) {
    return '<div class="section" style="margin-bottom:16px;">' +
      (title ? '<div class="section-title">' + esc(title) + "</div>" : "") + body + "</div>";
  }

  function kpi(label, value) {
    return '<div class="kpi"><div class="label">' + esc(label) + '</div>' +
      '<div class="value">' + esc(num(value)) + "</div></div>";
  }

  function btn(action, label, opts) {
    opts = opts || {};
    return '<button class="btn' + (opts.primary ? " primary" : "") + '"' +
      ' data-mp-action="' + esc(action) + '"' +
      (opts.id ? ' data-mp-id="' + esc(opts.id) + '"' : "") +
      (opts.step != null ? ' data-mp-step="' + esc(opts.step) + '"' : "") +
      (opts.disabled ? " disabled" : "") + ">" + esc(label) + "</button>";
  }

  // -----------------------------------------------------------------------
  // LIST mode
  // -----------------------------------------------------------------------

  function loadList() {
    session.enterList();
    render('<div class="section"><div class="section-title">Mission Reward Pool</div>' +
      '<div class="sub">Loading missions…</div></div>');
    var load = session.beginLoad(null);
    softGet("/api/admin/mission-pool/campaigns").then(function (r) {
      if (!session.accepts(load)) return;
      if (!r || r.status !== "ok") {
        render(section("Mission Reward Pool",
          '<div class="error">Failed to load missions: ' + esc((r && r.code) || "unknown") + "</div>"));
        return;
      }
      renderList(r.campaigns || []);
    });
  }

  function listCard(item) {
    var lines = [];
    if (item.state === "live" || item.state === "paused") {
      lines.push(num(item.submissions) + " submissions");
      lines.push(num(item.winners) + " winners");
      lines.push(num(item.pool_available) + " codes available");
    } else if (item.state === "scheduled" || item.state === "draft") {
      lines.push("Starts " + dt(item.starts_at));
      lines.push("Winner target " + num(item.winner_count));
      lines.push(num(item.pool_available) + " codes available");
    } else if (item.state === "completed") {
      lines.push(num(item.submissions) + " submissions");
      lines.push(num(item.qualified) + " qualified");
      lines.push(num(item.winners) + " winners");
      lines.push(num(item.rewards_allocated) + " rewards issued");
    } else {
      lines.push(num(item.submissions) + " submissions");
      lines.push("Stage: " + (item.processing_stage || "—"));
    }
    var open = item.state === "completed" ? "View Results" : "Open";
    return '<div class="section" style="margin:0 0 8px 0;">' +
      '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap;">' +
      '<div><div style="font-weight:600;">' + esc(item.name || item.campaign_id) + "</div>" +
      '<div class="sub">' + esc(item.campaign_id) + "</div>" +
      '<div style="margin-top:4px;">' + statePill(item.state) + "</div>" +
      '<div class="sub" style="margin-top:6px;">' + lines.map(esc).join(" &middot; ") + "</div></div>" +
      "<div>" + btn("open", open, { id: item.campaign_id }) + "</div>" +
      "</div></div>";
  }

  function renderList(items) {
    var header =
      '<div class="section" style="margin-bottom:16px;">' +
      '<div class="section-title">Mission Reward Pool</div>' +
      '<div class="sub" style="margin-bottom:10px;">Create, launch and operate Mission campaigns. ' +
      "Reward inventory is the same Voucher Centre inventory every other campaign reward uses.</div>" +
      '<div style="display:flex;gap:8px;flex-wrap:wrap;">' +
      btn("create", "+ Create Mission", { primary: true }) +
      btn("create-test", "Create Internal Test") +
      btn("refresh", "Refresh") +
      "</div></div>";

    if (!items.length) {
      render(header + section("", '<div class="sub">No Mission campaigns yet — create one above.</div>'));
      return;
    }

    var byState = {};
    items.forEach(function (it) { (byState[it.state] = byState[it.state] || []).push(it); });

    var groups = LIST_GROUPS.map(function (g) {
      var rows = [];
      g.states.forEach(function (s) { rows = rows.concat(byState[s] || []); });
      if (!rows.length) return "";
      return '<div class="section-title" style="margin:16px 0 8px;">' + esc(g.title) +
        ' <span class="sub">(' + rows.length + ")</span></div>" +
        rows.map(listCard).join("");
    }).join("");

    render(header + groups);
  }

  // -----------------------------------------------------------------------
  // CREATE mode — a four-step wizard. No campaign target, ever.
  // -----------------------------------------------------------------------

  var WIZARD_STEPS = ["Mission", "Reward", "Schedule", "Review"];

  function newDraft(preset) {
    var base = {
      step: 0,
      campaign_id: "",
      campaign_id_touched: false,
      name: "",
      mission_type: "multiple_choice",
      prompt: "",
      options_text: "",
      correct_answer: "",
      keyword_case_insensitive: true,
      min_chars: 10,
      max_chars: 500,
      reward_mode: "existing",       // existing | new
      pool_id: "",
      new_pool_id: "",
      new_pool_name: "",
      new_pool_type: "voucher_drop",
      new_pool_reward_usage: "",
      new_pool_codes_text: "",
      winner_count: 10,
      allocation_method: "random_qualified",
      starts_at: "",
      ends_at: "",
      eligibility: {
        require_correct_answer: true,
        exclude_voucher_hunter: true,
        exclude_multi_account_risk: true,
        exclude_blocked: true,
        require_gaming_account: false,
      },
      busy: false,
      notice: "",
    };
    Object.keys(preset || {}).forEach(function (k) { base[k] = preset[k]; });
    return base;
  }

  /**
   * §21 internal pilot preset. Not a backend campaign type — it only
   * pre-fills the same wizard with small, short, obviously-test defaults.
   */
  function internalTestPreset() {
    var start = new Date(Date.now() + 5 * 60 * 1000);
    var end = new Date(Date.now() + 65 * 60 * 1000);
    return {
      name: "TEST — Internal Mission Pilot",
      prompt: "Internal test — pick any option.",
      options_text: "a | Option A\nb | Option B",
      correct_answer: "a",
      winner_count: 2,
      starts_at: isoToLocalInput(start.toISOString()),
      ends_at: isoToLocalInput(end.toISOString()),
    };
  }

  function startCreate(preset) {
    var draft = session.enterCreate(newDraft(preset));
    if (preset && preset.name) {
      draft.campaign_id = slugify(preset.name) + "-" + String(Date.now()).slice(-5);
    }
    renderCreate();
    loadPools().then(function () {
      if (session.mode() !== MODE_CREATE || session.draft() !== draft) return;
      // Keep whatever the operator has already typed: the re-render exists
      // only to fill in the pool list once it arrives.
      captureCreateStep();
      renderCreate();
    });
  }

  function stepper(step) {
    return '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;">' +
      WIZARD_STEPS.map(function (label, i) {
        var cls = i === step ? "pill approved" : (i < step ? "pill neutral" : "pill pending");
        return '<span class="' + cls + '">' + (i + 1) + ". " + esc(label) + "</span>";
      }).join("") + "</div>";
  }

  function field(label, inner, sub) {
    return '<div style="margin-bottom:10px;">' +
      '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">' + esc(label) + "</label>" +
      inner + (sub ? '<div class="sub" style="margin-top:4px;">' + sub + "</div>" : "") + "</div>";
  }

  function textInput(id, value, placeholder, type, extra) {
    return '<input class="filter-input" id="' + id + '" type="' + (type || "text") + '"' +
      ' value="' + esc(value == null ? "" : value) + '"' +
      (placeholder ? ' placeholder="' + esc(placeholder) + '"' : "") +
      (extra || "") + ' style="width:100%;box-sizing:border-box;margin:0;" />';
  }

  function textArea(id, value, placeholder, rows) {
    return '<textarea class="filter-input" id="' + id + '" rows="' + (rows || 5) + '"' +
      (placeholder ? ' placeholder="' + esc(placeholder) + '"' : "") +
      ' style="width:100%;box-sizing:border-box;margin:0;">' + esc(value == null ? "" : value) + "</textarea>";
  }

  function select(id, value, options) {
    return '<select class="filter-input" id="' + id + '" style="width:100%;box-sizing:border-box;margin:0;">' +
      options.map(function (o) {
        return '<option value="' + esc(o[0]) + '"' + (o[0] === value ? " selected" : "") + ">" +
          esc(o[1]) + "</option>";
      }).join("") + "</select>";
  }

  function checkbox(id, checked, label) {
    return '<label class="sub" style="display:flex;align-items:center;gap:6px;margin:0 0 6px 0;">' +
      '<input type="checkbox" id="' + id + '"' + (checked ? " checked" : "") + " /> " + esc(label) + "</label>";
  }

  function createStep1(d) {
    var allowed = MISSION_TYPE_FIELDS[d.mission_type] || [];
    var parsed = parseOptionLines(d.options_text, {});
    var body =
      field("Campaign Name", textInput("mp-c-name", d.name, "September Feedback Mission")) +
      field("Campaign ID", textInput("mp-c-id", d.campaign_id, "september-feedback"),
        "Used in the Mission link. Letters, numbers, - and _ only, max " + MAX_LINK_SAFE_ID_CHARS + " characters." +
        (d.campaign_id && !campaignIdIsLinkSafe(d.campaign_id)
          ? ' <span style="color:#f5b63f;">This ID cannot be carried in a Telegram Mission link.</span>' : "")) +
      field("Mission Type", select("mp-c-mtype", d.mission_type, [
        ["multiple_choice", "Multiple Choice"], ["single_choice", "Single Choice"],
        ["keyword", "Keyword"], ["feedback", "Feedback"],
      ]));

    if (allowed.indexOf("prompt") !== -1) {
      body += field("Question / Instruction", textArea("mp-c-prompt", d.prompt, "Which feature do you prefer?", 2));
    }
    if (allowed.indexOf("options") !== -1) {
      body += field("Options — one per line",
        textArea("mp-c-options", d.options_text, "red | Red Team\nblue | Blue Team", 5),
        "One option per line. Write <code>id</code> on its own, or <code>id " + OPTION_LABEL_SEPARATOR +
        " Player-facing Label</code> when they differ. Commas in an option id are preserved.") +
        '<div class="sub" style="margin:-4px 0 10px;">' +
        (parsed.options.length
          ? "Parsed: " + parsed.options.map(function (o) {
              return "<code>" + esc(o.id) + "</code> → " + esc(o.label);
            }).join(", ")
          : "No options yet.") +
        (parsed.errors.length ? '<div style="color:#f5b63f;">' + parsed.errors.map(esc).join("<br/>") + "</div>" : "") +
        "</div>";
    }
    if (allowed.indexOf("correct") !== -1) {
      body += field(d.mission_type === "keyword" ? "Keyword" : "Correct answer (option id)",
        textInput("mp-c-correct", d.correct_answer, ""),
        d.mission_type === "keyword" ? "" : "Leave blank if there is no correct answer.");
    }
    if (allowed.indexOf("case_insensitive") !== -1) {
      body += checkbox("mp-c-ci", d.keyword_case_insensitive, "Case insensitive");
    }
    if (allowed.indexOf("min_chars") !== -1) {
      body += field("Minimum characters", textInput("mp-c-min", d.min_chars, "", "number")) +
        field("Maximum characters", textInput("mp-c-max", d.max_chars, "", "number"));
    }
    return body;
  }

  function poolOptionLabel(p) {
    return p.pool_id + " — " + (p.name || "") + " [" + (p.pool_type || "?") + "] (" +
      ((p.stock || {}).available || 0) + " available)";
  }

  function createStep2(d) {
    var pools = (poolsCache && poolsCache.pools) || [];
    var poolTypes = (poolsCache && poolsCache.pool_types) || [];
    var body =
      '<div class="sub" style="margin-bottom:10px;">Reward inventory is the shared Voucher Centre inventory. ' +
      "Creating a pool here calls the same Voucher Centre endpoint the Voucher Centre screen calls — " +
      "there is no separate Mission inventory.</div>" +
      '<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px;">' +
      '<label class="sub" style="display:flex;align-items:center;gap:6px;">' +
      '<input type="radio" name="mp-reward-mode" id="mp-c-mode-existing"' +
      (d.reward_mode === "existing" ? " checked" : "") + " /> Use Existing Reward Pool</label>" +
      '<label class="sub" style="display:flex;align-items:center;gap:6px;">' +
      '<input type="radio" name="mp-reward-mode" id="mp-c-mode-new"' +
      (d.reward_mode === "new" ? " checked" : "") + " /> Create New Reward Pool</label></div>";

    if (d.reward_mode === "existing") {
      body += field("Reward Pool",
        pools.length
          ? select("mp-c-pool", d.pool_id, [["", "— select a pool —"]].concat(pools.map(function (p) {
              return [p.pool_id, poolOptionLabel(p)];
            })))
          : '<div class="sub">No Mission-compatible pools are registered. Choose “Create New Reward Pool”.</div>',
        "Only pools the backend allows Campaign Rewards to allocate from are listed. " +
        "Protected Welcome / T1–T5 / affiliate denomination pools are excluded server-side.");
    } else {
      var parsed = parseVoucherCodes(d.new_pool_codes_text);
      body += field("Pool ID", textInput("mp-c-newpool-id", d.new_pool_id, "SEP-MISSION-5"),
          "Unique across the Voucher Centre pool registry.") +
        field("Pool Name", textInput("mp-c-newpool-name", d.new_pool_name, "September Mission $5")) +
        field("Reward / denomination description",
          textInput("mp-c-newpool-usage", d.new_pool_reward_usage, "MYR 5 free credit")) +
        field("Pool Type", select("mp-c-newpool-type", d.new_pool_type,
          (poolTypes.length ? poolTypes : ["voucher_drop"]).map(function (t) { return [t, t]; })),
          "The vocabulary comes from the Voucher Centre registry, not from this screen.") +
        field("Voucher Codes — one per line", textArea("mp-c-newpool-codes", d.new_pool_codes_text, "ABC001\nABC002\nABC003", 6)) +
        '<div class="sub" style="margin:-4px 0 10px;">' + parsed.codes.length + " valid codes" +
        (parsed.duplicates ? ", " + parsed.duplicates + " duplicates removed" : "") + "</div>";
    }

    body += field("Winner Count", textInput("mp-c-winners", d.winner_count, "", "number", ' min="1"'));

    var available = null;
    if (d.reward_mode === "existing" && d.pool_id) {
      var chosen = pools.filter(function (p) { return p.pool_id === d.pool_id; })[0];
      available = chosen ? ((chosen.stock || {}).available || 0) : null;
    } else if (d.reward_mode === "new") {
      available = parseVoucherCodes(d.new_pool_codes_text).codes.length;
    }
    if (available !== null) {
      var gate = inventoryGate(available, d.winner_count);
      body += '<div class="section" style="margin:0;">' +
        "<div>Winner Count: <b>" + esc(num(d.winner_count)) + "</b></div>" +
        "<div>Available Codes: <b>" + esc(num(available)) + "</b></div>" +
        (gate.ok
          ? '<div class="sub" style="color:#4caf50;">Inventory is sufficient.</div>'
          : '<div class="sub" style="color:#f5b63f;">Not enough codes — ' + esc(num(gate.shortfall)) +
            " more needed. Publishing is blocked until the pool covers the winner target.</div>") +
        "</div>";
    }

    body += '<div class="sub" style="margin-top:12px;">Eligibility policy</div>' +
      checkbox("mp-c-el-correct", d.eligibility.require_correct_answer, "require_correct_answer") +
      checkbox("mp-c-el-hunter", d.eligibility.exclude_voucher_hunter, "exclude_voucher_hunter") +
      checkbox("mp-c-el-multi", d.eligibility.exclude_multi_account_risk, "exclude_multi_account_risk") +
      checkbox("mp-c-el-blocked", d.eligibility.exclude_blocked, "exclude_blocked") +
      checkbox("mp-c-el-gaming", d.eligibility.require_gaming_account, "require_gaming_account");
    return body;
  }

  function createStep3(d) {
    return field("Start", textInput("mp-c-starts", d.starts_at, "", "datetime-local", ' step="1"'),
        "Times are entered and shown in this browser's local time (admin operations run on Kuala Lumpur time, GMT+8). Seconds are preserved.") +
      field("End", textInput("mp-c-ends", d.ends_at, "", "datetime-local", ' step="1"'),
        "The end time is one of the mission's eligibility cutoffs.") +
      field("Winner Selection", select("mp-c-allocation", d.allocation_method, [
        ["random_qualified", "Random Qualified"], ["first_qualified", "First Qualified"],
      ]));
  }

  function createStep4(d) {
    var parsed = parseOptionLines(d.options_text, {});
    var rows = [];
    rows.push(["MISSION", d.name + " (" + d.campaign_id + ")"]);
    rows.push(["Type", MISSION_TYPE_LABELS[d.mission_type] || d.mission_type]);
    rows.push(["Question", d.prompt]);
    if ((MISSION_TYPE_FIELDS[d.mission_type] || []).indexOf("options") !== -1) {
      rows.push(["Options", parsed.options.map(function (o) { return o.id + " — " + o.label; }).join(" / ")]);
      rows.push(["Correct answer", d.correct_answer || "(none)"]);
    }
    if (d.mission_type === "keyword") {
      rows.push(["Keyword", d.correct_answer]);
      rows.push(["Case insensitive", d.keyword_case_insensitive ? "Yes" : "No"]);
    }
    if (d.mission_type === "feedback") {
      rows.push(["Length", d.min_chars + "–" + d.max_chars + " characters"]);
    }
    rows.push(["REWARD", d.winner_count + " winners"]);
    rows.push(["Pool", d.reward_mode === "new"
      ? d.new_pool_id + " — " + d.new_pool_name + " (new, " +
        parseVoucherCodes(d.new_pool_codes_text).codes.length + " codes will be uploaded)"
      : d.pool_id]);
    rows.push(["SCHEDULE", dt(localInputToIso(d.starts_at)) + "  →  " + dt(localInputToIso(d.ends_at))]);
    rows.push(["SELECTION", ALLOCATION_LABELS[d.allocation_method] || d.allocation_method]);

    return '<table class="data-table"><tbody>' + rows.map(function (r) {
      return "<tr><td>" + esc(r[0]) + "</td><td>" + esc(r[1]) + "</td></tr>";
    }).join("") + "</tbody></table>" +
      (d.notice ? '<div class="sub" style="margin-top:10px;color:#f5b63f;">' + esc(d.notice) + "</div>" : "") +
      '<div class="sub" style="margin-top:10px;">Nothing is created until you choose an action below. ' +
      "Publishing is blocked when the reward pool holds fewer available codes than the winner target.</div>";
  }

  function renderCreate() {
    var d = session.draft();
    if (!d) return;
    var bodies = [createStep1, createStep2, createStep3, createStep4];
    var nav = d.step === WIZARD_STEPS.length - 1
      ? btn("save-draft", "Save Draft", { disabled: d.busy }) + " " +
        btn("publish-new", "Publish Mission", { primary: true, disabled: d.busy })
      : btn("wizard-next", "Next", { primary: true });
    render(
      '<div class="section" style="margin-bottom:16px;">' +
      btn("back-to-list", "← Mission Reward Pool") +
      '<div class="section-title" style="margin-top:10px;">Create Mission</div>' +
      stepper(d.step) +
      '<div id="mp-wizard-body">' + bodies[d.step](d) + "</div>" +
      '<div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap;">' +
      (d.step > 0 ? btn("wizard-back", "Back") + " " : "") + nav + "</div>" +
      "</div>");
  }

  /** Read whatever is on screen back into the draft before re-rendering. */
  function captureCreateStep() {
    var d = session.draft();
    if (!d) return;
    var v = function (id) { var n = host.$("#" + id); return n ? n.value : undefined; };
    var c = function (id) { var n = host.$("#" + id); return n ? !!n.checked : undefined; };
    var set = function (key, val) { if (val !== undefined) d[key] = val; };

    if (d.step === 0) {
      set("name", v("mp-c-name"));
      var typedId = v("mp-c-id");
      if (typedId !== undefined) {
        if (typedId !== d.campaign_id) d.campaign_id_touched = true;
        d.campaign_id = typedId;
      }
      if (!d.campaign_id_touched && d.name) d.campaign_id = slugify(d.name);
      set("mission_type", v("mp-c-mtype"));
      set("prompt", v("mp-c-prompt"));
      set("options_text", v("mp-c-options"));
      set("correct_answer", v("mp-c-correct"));
      set("keyword_case_insensitive", c("mp-c-ci"));
      set("min_chars", v("mp-c-min"));
      set("max_chars", v("mp-c-max"));
    } else if (d.step === 1) {
      var modeNew = host.$("#mp-c-mode-new");
      if (modeNew) d.reward_mode = modeNew.checked ? "new" : "existing";
      set("pool_id", v("mp-c-pool"));
      set("new_pool_id", v("mp-c-newpool-id"));
      set("new_pool_name", v("mp-c-newpool-name"));
      set("new_pool_reward_usage", v("mp-c-newpool-usage"));
      set("new_pool_type", v("mp-c-newpool-type"));
      set("new_pool_codes_text", v("mp-c-newpool-codes"));
      set("winner_count", v("mp-c-winners"));
      ["require_correct_answer:mp-c-el-correct", "exclude_voucher_hunter:mp-c-el-hunter",
        "exclude_multi_account_risk:mp-c-el-multi", "exclude_blocked:mp-c-el-blocked",
        "require_gaming_account:mp-c-el-gaming"].forEach(function (pair) {
        var parts = pair.split(":");
        var val = c(parts[1]);
        if (val !== undefined) d.eligibility[parts[0]] = val;
      });
    } else if (d.step === 2) {
      set("starts_at", v("mp-c-starts"));
      set("ends_at", v("mp-c-ends"));
      set("allocation_method", v("mp-c-allocation"));
    }
  }

  function validateCreateStep(d) {
    if (d.step === 0) {
      if (!d.name.trim()) return "Campaign Name is required.";
      if (!d.campaign_id.trim()) return "Campaign ID is required.";
      if (!campaignIdIsLinkSafe(d.campaign_id.trim())) {
        return "Campaign ID must be letters, numbers, - or _ (max " + MAX_LINK_SAFE_ID_CHARS + " chars) so the Mission link works.";
      }
      if (!d.prompt.trim()) return "Question / Instruction is required.";
      var allowed = MISSION_TYPE_FIELDS[d.mission_type] || [];
      if (allowed.indexOf("options") !== -1) {
        var parsed = parseOptionLines(d.options_text, {});
        if (parsed.errors.length) return parsed.errors[0];
        if (parsed.options.length < 2) return "At least two options are required.";
        if (d.correct_answer.trim() && !parsed.options.some(function (o) { return o.id === d.correct_answer.trim(); })) {
          return "The correct answer must be one of the option ids.";
        }
      }
      if (d.mission_type === "keyword" && !d.correct_answer.trim()) return "A keyword is required.";
      return null;
    }
    if (d.step === 1) {
      if (!(parseInt(d.winner_count, 10) > 0)) return "Winner Count must be at least 1.";
      if (d.reward_mode === "existing" && !d.pool_id) return "Select a reward pool, or create a new one.";
      if (d.reward_mode === "new") {
        if (!d.new_pool_id.trim()) return "Pool ID is required.";
        if (!d.new_pool_name.trim()) return "Pool Name is required.";
        if (!parseVoucherCodes(d.new_pool_codes_text).codes.length) return "Add at least one voucher code.";
      }
      return null;
    }
    if (d.step === 2) {
      if (!d.starts_at) return "A start time is required.";
      if (d.ends_at && localInputToIso(d.ends_at) <= localInputToIso(d.starts_at)) {
        return "The end time must be after the start time.";
      }
      return null;
    }
    return null;
  }

  /** A shallow copy of the draft pinned to one step, for re-validation. */
  function draftAtStep(d, step) {
    var copy = {};
    Object.keys(d).forEach(function (k) { copy[k] = d[k]; });
    copy.step = step;
    return copy;
  }

  function missionConfigFromDraft(d) {
    var cfg = { mission_type: d.mission_type, prompt: d.prompt.trim() };
    var allowed = MISSION_TYPE_FIELDS[d.mission_type] || [];
    if (allowed.indexOf("options") !== -1) {
      cfg.options = parseOptionLines(d.options_text, {}).options;
      cfg.correct_answer = d.correct_answer.trim();
    } else if (d.mission_type === "keyword") {
      cfg.correct_answer = d.correct_answer.trim();
      cfg.keyword_case_insensitive = !!d.keyword_case_insensitive;
    } else {
      cfg.min_chars = parseInt(d.min_chars, 10) || 1;
      cfg.max_chars = parseInt(d.max_chars, 10) || 500;
    }
    return cfg;
  }

  /**
   * Inline reward-pool creation, through the CANONICAL Voucher Centre /
   * Campaign Rewards endpoints. No Mission-specific inventory writer: this
   * is exactly what the Voucher Centre reward-pool screen posts, so every
   * server-side validation and protected-pool rule still applies (a reserved
   * pool id is refused with reserved_pool_id, ownership metadata is stamped
   * server-side, codes land in the shared db.voucher_pools table).
   */
  function createInlinePool(d) {
    var codes = parseVoucherCodes(d.new_pool_codes_text).codes;
    return host.apiPostJson("/api/admin/reward-pools", {
      pool_id: d.new_pool_id.trim(),
      name: d.new_pool_name.trim(),
      pool_type: d.new_pool_type,
      reward_usage: d.new_pool_reward_usage.trim(),
      campaign_id: d.campaign_id.trim(),
    }).then(function (res) {
      if (!res.ok || (res.d && res.d.status !== "ok")) {
        throw new Error((res.d && res.d.code) || "reward_pool_create_failed");
      }
      return host.apiPostJson("/api/admin/reward-pools/" + encodeURIComponent(d.new_pool_id.trim()) + "/upload-codes",
        { codes: codes, display_label: d.new_pool_name.trim(), value_hint: d.new_pool_reward_usage.trim() });
    }).then(function (res) {
      if (!res.ok || (res.d && res.d.status !== "ok")) {
        throw new Error((res.d && res.d.code) || "reward_pool_upload_failed");
      }
      host.toast("✅ Reward pool " + d.new_pool_id.trim() + ": " + (res.d.inserted || 0) +
        " codes added" + (res.d.skipped_duplicates ? ", " + res.d.skipped_duplicates + " duplicates skipped" : ""), "success");
      poolsCache = null;
      return d.new_pool_id.trim();
    });
  }

  /**
   * Save Draft / Publish. Publishing is a SEPARATE, explicit action: nothing
   * is ever published as a side effect of completing the form.
   */
  function submitCreate(publish) {
    var auth = session.authorizeCreate();
    if (!auth.ok) { host.toast("❌ " + (SAVE_REFUSAL_COPY[auth.code] || auth.code), "error"); return; }
    var d = session.draft();

    // Re-validate every step, not just the one on screen: the review step is
    // reachable, and a body that fails Phase 1 validation should be caught
    // here rather than as an opaque backend error code.
    for (var step = 0; step <= 2; step++) {
      var problem = validateCreateStep(draftAtStep(d, step));
      if (problem) { d.step = step; renderCreate(); host.toast("❌ " + problem, "error"); return; }
    }

    var load = session.beginLoad(null);
    var createdPool = null;
    d.busy = true; d.notice = ""; renderCreate();

    var fail = function (msg) {
      // The pool is registered before the campaign, so a later failure can
      // leave inventory with no campaign pointing at it. Say so rather than
      // letting the operator create a second pool for the retry.
      if (createdPool) {
        msg += " Reward pool " + createdPool + " was already created with its codes — " +
          "select it under “Use Existing Reward Pool” when you retry.";
      }
      host.toast("❌ " + msg, "error");
      if (!session.accepts(load)) return;
      d.busy = false; d.notice = msg; renderCreate();
    };

    var poolReady = d.reward_mode === "new"
      ? createInlinePool(d).then(function (poolId) { createdPool = poolId; return poolId; })
      : Promise.resolve(d.pool_id);

    poolReady.then(function (poolId) {
      if (!session.accepts(load)) return null;
      // The pool's REAL registered type comes from the backend, never from
      // the form: the processor passes mission_pool.pool_type to
      // voucher_pool_service.allocate_voucher as expected_pool_type, and a
      // wrong value makes every allocation miss while stock looks available.
      return inventoryCheck(poolId, d.winner_count).then(function (verdict) {
        return { poolId: poolId, verdict: verdict };
      });
    }).then(function (ctx) {
      if (!ctx || !session.accepts(load)) return null;
      if (!ctx.verdict || ctx.verdict.status !== "ok" || !ctx.verdict.pool_exists) {
        throw new Error("Reward pool " + ctx.poolId + " could not be verified.");
      }
      if (publish && !ctx.verdict.sufficient) {
        throw new Error("Publishing blocked: winner target " + ctx.verdict.winner_count +
          " exceeds the " + ctx.verdict.available + " available codes in " + ctx.poolId + ".");
      }
      var body = {
        campaign_id: d.campaign_id.trim(),
        name: d.name.trim(),
        type: "mission_pool",
        schedule: { starts_at: localInputToIso(d.starts_at), ends_at: localInputToIso(d.ends_at) },
        telegram: { channel_username: "" },
        // Mission Pool is answered inside the Mini App; campaign_centre only
        // allows telegram_web_app for this type and it has no provider path.
        destination: { open_mode: "telegram_web_app", ready: false },
        mission_config: missionConfigFromDraft(d),
        mission_pool: {
          pool_id: ctx.poolId,
          pool_type: ctx.verdict.pool_type,
          winner_count: parseInt(d.winner_count, 10) || 0,
          allocation_method: d.allocation_method,
          eligibility_policy: d.eligibility,
        },
      };
      return host.apiPostJson("/api/admin/gc-campaigns", body).then(function (res) {
        if (!res.ok || (res.d && res.d.status !== "ok")) {
          throw new Error((res.d && res.d.code) || "create_failed");
        }
        return body.campaign_id;
      });
    }).then(function (campaignId) {
      if (!campaignId || !session.accepts(load)) return null;
      host.toast("✅ Mission created as draft", "success");
      if (!publish) { openDetail(campaignId); return null; }
      return host.apiPost("/api/admin/gc-campaigns/" + encodeURIComponent(campaignId) + "/publish")
        .catch(function (e) { return { status: "error", code: e.message }; })
        .then(function (r) {
          if (!r || r.status !== "ok") {
            host.toast("⚠️ Mission created but publish failed: " + ((r && r.code) || "unknown"), "error");
          } else {
            host.toast("✅ Mission published", "success");
          }
          openDetail(campaignId);
        });
    }).catch(function (e) { fail(e.message || String(e)); });
  }

  // -----------------------------------------------------------------------
  // VIEW mode — read-only operations view. Never the create wizard.
  // -----------------------------------------------------------------------

  function openDetail(campaignId) {
    session.enterView(campaignId);
    render(section("Mission", '<div class="sub">Loading ' + esc(campaignId) + "…</div>"));
    var load = session.beginLoad(campaignId);
    Promise.all([
      softGet("/api/admin/gc-campaigns/" + encodeURIComponent(campaignId)),
      softGet("/api/admin/mission-pool/" + encodeURIComponent(campaignId) + "/edit-state"),
      softGet("/api/admin/mission-pool/" + encodeURIComponent(campaignId) + "/summary"),
    ]).then(function (res) {
      // Every one of these three responses is checked before it may touch
      // anything — not just the last one (§15).
      if (!session.accepts(load)) return;
      var campaign = (res[0] && res[0].status === "ok") ? res[0].campaign : null;
      var state = (res[1] && res[1].status === "ok") ? res[1] : null;
      var summary = (res[2] && res[2].status === "ok") ? res[2] : {};
      if (!campaign || !state) {
        render(section("Mission", '<div class="error">Could not load ' + esc(campaignId) + ": " +
          esc(((res[1] || {}).code) || ((res[0] || {}).code) || "unknown") + "</div>" +
          btn("back-to-list", "← Mission Reward Pool")));
        return;
      }
      renderDetail(campaign, state, summary);
    });
  }

  function detailMissionBlock(campaign) {
    var cfg = campaign.mission_config || {};
    var out = "<div>" + esc(MISSION_TYPE_LABELS[cfg.mission_type] || cfg.mission_type || "—") + "</div>" +
      '<div style="margin-top:6px;">' + esc(cfg.prompt || "") + "</div>";
    if ((cfg.options || []).length) {
      out += '<div class="sub" style="margin-top:6px;">' + cfg.options.map(function (o) {
        return esc(o.id) + " — " + esc(o.label || o.id);
      }).join("<br/>") + "</div>";
    }
    if (cfg.correct_answer) {
      out += '<div class="sub" style="margin-top:6px;">Correct answer: <code>' + esc(cfg.correct_answer) + "</code></div>";
    }
    if (cfg.mission_type === "keyword") {
      out += '<div class="sub">Case insensitive: ' + (cfg.keyword_case_insensitive === false ? "No" : "Yes") + "</div>";
    }
    if (cfg.mission_type === "feedback") {
      out += '<div class="sub">Length: ' + esc(cfg.min_chars) + "–" + esc(cfg.max_chars) + " characters</div>";
    }
    return out;
  }

  // States from which the mission can still be published, and therefore the
  // only ones where an inventory shortfall is a *blocker* rather than history.
  var PUBLISHABLE_STATES = ["draft", "scheduled", "paused", "live"];

  function detailRewardBlock(state) {
    var r = state.reward || {};
    return '<table class="data-table"><tbody>' +
      "<tr><td>Pool</td><td>" + esc(r.pool_id || "—") + (r.pool_name ? " — " + esc(r.pool_name) : "") + "</td></tr>" +
      "<tr><td>Pool type</td><td>" + esc(r.pool_type || "—") + "</td></tr>" +
      "<tr><td>Winner target</td><td>" + esc(num(r.winner_count)) + "</td></tr>" +
      "<tr><td>Codes available</td><td>" + esc(num(r.available)) + "</td></tr>" +
      "<tr><td>Selection</td><td>" + esc(ALLOCATION_LABELS[r.allocation_method] || r.allocation_method || "—") + "</td></tr>" +
      "</tbody></table>" +
      (r.pool_id && !r.pool_selectable
        ? '<div class="sub" style="margin-top:6px;color:#f5b63f;">Current pool — unavailable for new selection. ' +
          "It stays configured on this mission and is never rewritten by the admin UI.</div>"
        : "") +
      (r.pool_id && !r.sufficient && PUBLISHABLE_STATES.indexOf(state.state) !== -1
        ? '<div class="sub" style="margin-top:6px;color:#f5b63f;">Winner target ' + esc(num(r.winner_count)) +
          " exceeds the " + esc(num(r.available)) + " available codes — publishing is blocked.</div>"
        : "");
  }

  function detailResultsBlock(summary) {
    var g = summary.grains || {};
    var REASON_LABELS = {
      duplicate_identity: "Duplicate identity", duplicate_gaming_account: "Duplicate gaming account",
      voucher_hunter: "Voucher hunter", multi_account_risk: "Multi-account risk",
      incorrect_answer: "Incorrect answer", missing_gaming_account: "Missing gaming account",
      blocked: "Blocked", already_rewarded: "Already rewarded", invalid_submission: "Invalid submission",
      campaign_cancelled: "Campaign cancelled", submitted_after_close: "Submitted after close",
      out_of_stock: "Out of stock", other: "Other",
    };
    var rows = [
      ["Submissions", g.submissions_telegram_user_grain],
      ["Deduplicated identities", g.deduplicated_identity_grain],
      ["Qualified", g.qualified_identity_grain],
      ["Disqualified", g.disqualified_telegram_user_grain],
      ["Winner target", summary.winner_count_requested],
      ["Winners", g.winners_identity_grain],
      ["Rewards allocated", g.rewards_allocated_voucher_grain],
      ["Notifications sent", g.notifications_sent_voucher_grain],
      ["Notifications failed", g.notifications_failed_voucher_grain],
    ].map(function (r) {
      return "<tr><td>" + esc(r[0]) + "</td><td>" + esc(num(r[1])) + "</td></tr>";
    }).join("");
    var reasons = summary.disqualification_reasons || {};
    var reasonRows = Object.keys(reasons).map(function (k) {
      return "<tr><td>" + esc(REASON_LABELS[k] || k) + "</td><td>" + esc(num(reasons[k])) + "</td></tr>";
    }).join("");
    return '<table class="data-table"><tbody>' + rows + "</tbody></table>" +
      (reasonRows
        // Aggregate counts only. No individual identity is ever listed here.
        ? '<div class="sub" style="margin-top:8px;">Disqualification reasons (aggregate, admin only)</div>' +
          '<table class="data-table"><tbody>' + reasonRows + "</tbody></table>"
        : "");
  }

  function renderDetail(campaign, state, summary) {
    var g = summary.grains || {};
    var r = state.reward || {};
    var schedule = campaign.schedule || {};
    var actions = actionsFor(state).map(function (a) {
      return btn(a[0], a[1], { id: state.campaign_id, primary: a[0] === "publish" });
    }).join(" ");

    var link = state.mission_link
      ? "<code>" + esc(state.mission_link) + "</code> " + btn("copy-link", "Copy", { id: state.mission_link })
      : '<span class="sub">Mission link unavailable (' + esc(state.mission_link_unavailable_reason || "unknown") + ")</span>";

    render(
      '<div class="section" style="margin-bottom:16px;">' +
      btn("back-to-list", "← Mission Reward Pool") +
      '<div class="section-title" style="margin-top:10px;">' + esc(campaign.name || state.campaign_id) + " " +
      statePill(state.state) + "</div>" +
      '<div class="sub">' + esc(state.campaign_id) + "</div>" +
      "</div>" +

      section("Overview",
        '<div class="card-grid">' +
        kpi("Submitted", g.submissions_telegram_user_grain) +
        kpi("Qualified", g.qualified_identity_grain) +
        kpi("Winners", g.winners_identity_grain) +
        kpi("Codes Available", r.available) +
        "</div>") +

      section("Mission", detailMissionBlock(campaign) +
        (state.mission_config_locked
          ? '<div class="sub" style="margin-top:8px;color:#f5b63f;">' + esc(FREEZE_NOTE) +
            " (" + esc(num(state.entries)) + " entries)</div>"
          : "")) +

      section("Reward", detailRewardBlock(state)) +

      section("Schedule",
        '<table class="data-table"><tbody>' +
        "<tr><td>Starts</td><td>" + esc(dt(schedule.starts_at)) + "</td></tr>" +
        "<tr><td>Ends</td><td>" + esc(dt(schedule.ends_at)) + "</td></tr>" +
        "<tr><td>Close cutoff (closed_at)</td><td>" + esc(dt(state.closed_at)) + "</td></tr>" +
        "<tr><td>Processing stage</td><td>" + esc(state.processing_stage || "—") + "</td></tr>" +
        "</tbody></table>") +

      section("Mission Link", link) +

      section("Actions", actions || '<div class="sub">No actions available in this state.</div>') +

      (state.state === "completed" || state.state === "processing"
        ? section("Results", detailResultsBlock(summary))
        : ""));
  }

  // -----------------------------------------------------------------------
  // EDIT mode — a dedicated panel with a fixed, immutable target.
  // -----------------------------------------------------------------------

  function openEdit(campaignId) {
    // Fix the target FIRST; every subsequent response is validated against it.
    session.enterEdit(campaignId);
    render(section("Edit Mission", '<div class="sub">Loading ' + esc(campaignId) + "…</div>"));
    var load = session.beginLoad(campaignId);

    Promise.all([
      loadPools(),
      softGet("/api/admin/gc-campaigns/" + encodeURIComponent(campaignId)),
      softGet("/api/admin/mission-pool/" + encodeURIComponent(campaignId) + "/edit-state"),
    ]).then(function (res) {
      if (!session.accepts(load)) return;
      var campaign = (res[1] && res[1].status === "ok") ? res[1].campaign : null;
      var state = (res[2] && res[2].status === "ok") ? res[2] : null;
      if (!campaign || !state || campaign.campaign_id !== campaignId) {
        // Never leave a half-loaded edit form armed: hydration did not
        // complete, so authorizeSave() refuses regardless of what renders.
        render(section("Edit Mission",
          '<div class="error">Could not load ' + esc(campaignId) + " for editing.</div>" +
          btn("back-to-detail", "← Back", { id: campaignId })));
        return;
      }
      var cfg = campaign.mission_config || {};
      var block = campaign.mission_pool || {};
      var schedule = campaign.schedule || {};
      var labels = {};
      (cfg.options || []).forEach(function (o) { if (o && o.id) labels[o.id] = o.label || o.id; });

      var policy = block.eligibility_policy || {};
      var starts = scheduleFieldFrom(schedule.starts_at);
      var ends = scheduleFieldFrom(schedule.ends_at);

      var accepted = session.completeHydration(load, {
        campaignId: campaignId,
        campaign: campaign,
        state: state,
        // Per-campaign state, owned by THIS edit session only. It is dropped
        // wholesale on any mode change, so it can never leak into another
        // campaign's form or into a create.
        optionLabels: labels,
        optionsEditable: optionsEditable(cfg.options),
        starts: starts,
        ends: ends,
        storedPool: { pool_id: block.pool_id || "", pool_type: block.pool_type || "" },
        // The working copy the panel renders from and saves from. Rendering
        // straight off `campaign` meant a re-render (needed when the mission
        // type changes the set of fields) would discard everything typed, and
        // saving straight off the DOM meant a field the current type does not
        // render silently contributed a fallback value.
        form: {
          name: campaign.name || "",
          mission_type: cfg.mission_type || "multiple_choice",
          prompt: cfg.prompt || "",
          options_text: formatOptionLines(cfg.options),
          correct_answer: cfg.correct_answer || "",
          keyword_case_insensitive: cfg.keyword_case_insensitive !== false,
          min_chars: cfg.min_chars == null ? 10 : cfg.min_chars,
          max_chars: cfg.max_chars == null ? 500 : cfg.max_chars,
          pool_id: block.pool_id || "",
          winner_count: block.winner_count == null ? "" : block.winner_count,
          allocation_method: block.allocation_method || "random_qualified",
          eligibility: {
            require_correct_answer: policy.require_correct_answer !== false,
            exclude_voucher_hunter: policy.exclude_voucher_hunter !== false,
            exclude_multi_account_risk: policy.exclude_multi_account_risk !== false,
            exclude_blocked: policy.exclude_blocked !== false,
            require_gaming_account: !!policy.require_gaming_account,
          },
          starts_display: starts.display,
          ends_display: ends.display,
        },
      });
      if (!accepted) return;
      renderEdit();
    });
  }

  function renderEdit() {
    var es = session.editState();
    if (!es || !es.campaign || !es.form) return;
    var f = es.form;
    var state = es.state;
    var locked = !!state.mission_config_locked;
    var scheduleEditable = !!state.schedule_editable;
    var poolEditable = !!(state.reward || {}).pool_editable;
    var pools = (poolsCache && poolsCache.pools) || [];
    var allowed = MISSION_TYPE_FIELDS[f.mission_type] || [];
    var dis = locked ? " disabled" : "";

    var missionBody = "";
    if (locked) {
      missionBody += '<div class="sub" style="margin-bottom:8px;color:#f5b63f;">' + esc(FREEZE_NOTE) +
        " (" + esc(num(state.entries)) + " entries)</div>";
    }
    missionBody +=
      field("Campaign Name", textInput("mp-e-name", f.name, "")) +
      field("Mission Type", '<select class="filter-input" id="mp-e-mtype" style="width:100%;box-sizing:border-box;margin:0;"' + dis + ">" +
        ["multiple_choice", "single_choice", "keyword", "feedback"].map(function (t) {
          return '<option value="' + t + '"' + (t === f.mission_type ? " selected" : "") + ">" +
            esc(MISSION_TYPE_LABELS[t]) + "</option>";
        }).join("") + "</select>",
        // Changing the type changes which fields exist; the panel re-renders
        // so the operator edits (and saves) the fields the new type actually
        // has, instead of the previous type's controls silently contributing
        // fallback values.
        locked ? "" : "Changing the type updates the fields below.") +
      field("Question / Instruction",
        '<textarea class="filter-input" id="mp-e-prompt" rows="2" style="width:100%;box-sizing:border-box;margin:0;"' +
        dis + ">" + esc(f.prompt) + "</textarea>");

    if (allowed.indexOf("options") !== -1) {
      var optionsDisabled = locked || !es.optionsEditable;
      missionBody += field("Options — one per line",
        '<textarea class="filter-input" id="mp-e-options" rows="5" style="width:100%;box-sizing:border-box;margin:0;"' +
        (optionsDisabled ? " disabled" : "") + ">" + esc(f.options_text) + "</textarea>",
        es.optionsEditable
          ? "Write <code>id</code>, or <code>id " + OPTION_LABEL_SEPARATOR + " Player-facing Label</code>."
          : '<span style="color:#f5b63f;">One or more option ids contain a “' + OPTION_LABEL_SEPARATOR +
            "”, which this editor cannot round-trip safely. Options are read-only for this mission.</span>");
    }
    if (allowed.indexOf("correct") !== -1) {
      missionBody += field(f.mission_type === "keyword" ? "Keyword" : "Correct answer (option id)",
        textInput("mp-e-correct", f.correct_answer, "", "text", dis));
    }
    if (allowed.indexOf("case_insensitive") !== -1) {
      missionBody += '<label class="sub" style="display:flex;align-items:center;gap:6px;">' +
        '<input type="checkbox" id="mp-e-ci"' + (f.keyword_case_insensitive ? " checked" : "") +
        dis + " /> Case insensitive</label>";
    }
    if (allowed.indexOf("min_chars") !== -1) {
      missionBody += field("Minimum characters", textInput("mp-e-min", f.min_chars, "", "number", dis)) +
        field("Maximum characters", textInput("mp-e-max", f.max_chars, "", "number", dis));
    }

    // The stored pool is ALWAYS offered, even when the backend no longer
    // lists it: silently falling back to whichever pool sorted first would
    // rewrite this campaign's pool on the next save.
    var poolOptions = pools.map(function (p) { return [p.pool_id, poolOptionLabel(p)]; });
    if (es.storedPool.pool_id && !poolOptions.some(function (o) { return o[0] === es.storedPool.pool_id; })) {
      poolOptions.unshift([es.storedPool.pool_id,
        es.storedPool.pool_id + " — current pool, unavailable for new selection"]);
    }
    var rewardBody =
      field("Reward Pool",
        '<select class="filter-input" id="mp-e-pool" style="width:100%;box-sizing:border-box;margin:0;"' +
        (poolEditable ? "" : " disabled") + ">" +
        poolOptions.map(function (o) {
          return '<option value="' + esc(o[0]) + '"' + (o[0] === f.pool_id ? " selected" : "") +
            ">" + esc(o[1]) + "</option>";
        }).join("") + "</select>",
        poolEditable ? "" : "Reward allocation has started — the pool can no longer be changed.") +
      field("Winner Count", textInput("mp-e-winners", f.winner_count, "", "number", ' min="1"')) +
      field("Winner Selection", select("mp-e-allocation", f.allocation_method, [
        ["random_qualified", "Random Qualified"], ["first_qualified", "First Qualified"],
      ]));

    // Live inventory feedback against whichever pool is currently selected —
    // the same winner_count <= available rule the publish gate enforces.
    var chosen = pools.filter(function (p) { return p.pool_id === f.pool_id; })[0];
    var available = chosen ? ((chosen.stock || {}).available || 0)
      : (f.pool_id === es.storedPool.pool_id ? (state.reward || {}).available : null);
    if (available != null) {
      var gate = inventoryGate(available, f.winner_count);
      rewardBody += '<div class="sub">Winner Count: <b>' + esc(num(f.winner_count)) +
        "</b> &middot; Available Codes: <b>" + esc(num(available)) + "</b>" +
        (gate.ok ? "" : ' <span style="color:#f5b63f;">— ' + esc(num(gate.shortfall)) +
          " more needed before this mission can be published.</span>") + "</div>";
    }

    rewardBody += '<div class="sub" style="margin-top:8px;">Eligibility policy</div>' +
      checkbox("mp-e-el-correct", f.eligibility.require_correct_answer, "require_correct_answer") +
      checkbox("mp-e-el-hunter", f.eligibility.exclude_voucher_hunter, "exclude_voucher_hunter") +
      checkbox("mp-e-el-multi", f.eligibility.exclude_multi_account_risk, "exclude_multi_account_risk") +
      checkbox("mp-e-el-blocked", f.eligibility.exclude_blocked, "exclude_blocked") +
      checkbox("mp-e-el-gaming", f.eligibility.require_gaming_account, "require_gaming_account");

    // Schedule editability comes from the backend's own answer, never
    // inferred from the mission_config freeze — the two are independent.
    var scheduleBody = scheduleEditable
      ? field("Start", textInput("mp-e-starts", f.starts_display, "", "datetime-local", ' step="1"')) +
        field("End", textInput("mp-e-ends", f.ends_display, "", "datetime-local", ' step="1"'))
      : '<div class="sub">The schedule is read-only for this campaign state.</div>' +
        '<table class="data-table"><tbody>' +
        "<tr><td>Starts</td><td>" + esc(dt(es.starts.iso)) + "</td></tr>" +
        "<tr><td>Ends</td><td>" + esc(dt(es.ends.iso)) + "</td></tr></tbody></table>";

    render(
      '<div class="section" style="margin-bottom:16px;">' +
      btn("back-to-detail", "← Back to mission", { id: es.campaignId }) +
      '<div class="section-title" style="margin-top:10px;">Edit Mission</div>' +
      '<div class="sub">Editing <b>' + esc(es.campaignId) + "</b> — this is the only campaign this panel can write to.</div>" +
      "</div>" +
      section("Mission", missionBody) +
      section("Reward", rewardBody) +
      section("Schedule", scheduleBody) +
      section("", btn("save-edit", "Save changes", { primary: true, id: es.campaignId }) + " " +
        btn("back-to-detail", "Discard", { id: es.campaignId })));
  }

  /**
   * Read whatever the edit panel currently shows back into the working copy.
   * A control the current mission type does not render is simply absent, so
   * its stored value is left alone rather than being replaced by a fallback.
   */
  function captureEditForm() {
    var es = session.editState();
    if (!es || !es.form) return;
    var f = es.form;
    var v = function (id) { var n = host.$("#" + id); return n ? n.value : undefined; };
    var c = function (id) { var n = host.$("#" + id); return n ? !!n.checked : undefined; };
    var set = function (key, value) { if (value !== undefined) f[key] = value; };

    set("name", v("mp-e-name"));
    set("mission_type", v("mp-e-mtype"));
    set("prompt", v("mp-e-prompt"));
    set("options_text", v("mp-e-options"));
    set("correct_answer", v("mp-e-correct"));
    set("keyword_case_insensitive", c("mp-e-ci"));
    set("min_chars", v("mp-e-min"));
    set("max_chars", v("mp-e-max"));
    set("pool_id", v("mp-e-pool"));
    set("winner_count", v("mp-e-winners"));
    set("allocation_method", v("mp-e-allocation"));
    set("starts_display", v("mp-e-starts"));
    set("ends_display", v("mp-e-ends"));
    ["require_correct_answer:mp-e-el-correct", "exclude_voucher_hunter:mp-e-el-hunter",
      "exclude_multi_account_risk:mp-e-el-multi", "exclude_blocked:mp-e-el-blocked",
      "require_gaming_account:mp-e-el-gaming"].forEach(function (pair) {
      var parts = pair.split(":");
      var value = c(parts[1]);
      if (value !== undefined) f.eligibility[parts[0]] = value;
    });
  }

  /**
   * The pool_type stored on the campaign must be the REGISTRY's type for the
   * pool actually selected. The processor passes it to allocate_voucher as
   * expected_pool_type, which filters the inventory row on it: keeping the
   * previous pool's type after repointing the campaign would match no rows
   * and mark every winner out_of_stock while stock looked available.
   *
   * Returns null when the selection changed to a pool the backend gave us no
   * metadata for — the caller refuses the save rather than guessing.
   */
  function resolveEditPoolType(es, poolId) {
    if (poolId === es.storedPool.pool_id) return es.storedPool.pool_type;
    var listed = ((poolsCache && poolsCache.pools) || []).filter(function (p) {
      return p.pool_id === poolId;
    })[0];
    return listed ? listed.pool_type : null;
  }

  function saveEdit(requestedId) {
    var auth = session.authorizeSave(requestedId);
    if (!auth.ok) {
      host.toast("❌ " + (SAVE_REFUSAL_COPY[auth.code] || auth.code), "error");
      return;
    }
    var es = session.editState();
    captureEditForm();
    var f = es.form;
    var state = es.state;
    var campaignId = auth.campaignId;

    // The pool select is disabled once allocation has started, so its value
    // is still the stored one; either way the stored pool is preserved rather
    // than being swapped for whatever sorted first.
    var poolId = f.pool_id || es.storedPool.pool_id;
    var poolType = resolveEditPoolType(es, poolId);
    if (!poolType) {
      host.toast("❌ Could not confirm the reward type of pool " + poolId +
        " — reopen this mission and try again.", "error");
      return;
    }

    var body = {
      name: (f.name || es.campaign.name || "").trim(),
      mission_pool: {
        pool_id: poolId,
        pool_type: poolType,
        winner_count: parseInt(f.winner_count, 10) || 0,
        allocation_method: f.allocation_method || "random_qualified",
        eligibility_policy: {
          require_correct_answer: !!f.eligibility.require_correct_answer,
          exclude_voucher_hunter: !!f.eligibility.exclude_voucher_hunter,
          exclude_multi_account_risk: !!f.eligibility.exclude_multi_account_risk,
          exclude_blocked: !!f.eligibility.exclude_blocked,
          require_gaming_account: !!f.eligibility.require_gaming_account,
        },
      },
    };

    // A frozen mission_config is simply not sent, so an unchanged PUT can
    // never trip the backend freeze check.
    if (!state.mission_config_locked) {
      var type = f.mission_type || (es.campaign.mission_config || {}).mission_type;
      var cfg = { mission_type: type, prompt: (f.prompt || "").trim() };
      var allowed = MISSION_TYPE_FIELDS[type] || [];
      if (allowed.indexOf("options") !== -1) {
        if (!es.optionsEditable) {
          cfg.options = (es.campaign.mission_config || {}).options || [];
        } else {
          var parsed = parseOptionLines(f.options_text, es.optionLabels);
          if (parsed.errors.length) { host.toast("❌ " + parsed.errors[0], "error"); return; }
          cfg.options = parsed.options;
        }
        cfg.correct_answer = (f.correct_answer || "").trim();
      } else if (type === "keyword") {
        cfg.correct_answer = (f.correct_answer || "").trim();
        cfg.keyword_case_insensitive = !!f.keyword_case_insensitive;
      } else {
        cfg.min_chars = parseInt(f.min_chars, 10) || 1;
        cfg.max_chars = parseInt(f.max_chars, 10) || 500;
      }
      body.mission_config = cfg;
    }

    // Only sent when the backend says the schedule is editable, and an
    // untouched field resends the exact original instant.
    if (state.schedule_editable) {
      body.schedule = {
        starts_at: scheduleValueForSave(es.starts, f.starts_display),
        ends_at: scheduleValueForSave(es.ends, f.ends_display),
      };
    }

    var load = session.beginLoad(campaignId);
    // Re-authorised immediately before the write: nothing between the first
    // check and here may have changed the mode or the target.
    var recheck = session.authorizeSave(campaignId);
    if (!recheck.ok) {
      host.toast("❌ " + (SAVE_REFUSAL_COPY[recheck.code] || recheck.code), "error");
      return;
    }
    host.apiPutJson("/api/admin/gc-campaigns/" + encodeURIComponent(campaignId), body).then(function (res) {
      var d = res.d || res;
      if (d.status !== "ok") {
        host.toast(d.code === "mission_config_locked"
          ? "❌ Mission details are frozen — participants have already submitted entries."
          : "❌ " + (d.code || "save_failed"), "error");
        return;
      }
      host.toast("✅ Mission saved", "success");
      if (!session.accepts(load)) return;
      openDetail(campaignId);
    }).catch(function (e) { host.toast("❌ " + e.message, "error"); });
  }

  // -----------------------------------------------------------------------
  // Operations actions
  // -----------------------------------------------------------------------

  function postAction(action, campaignId) {
    // publish/pause are the shared Campaign Centre lifecycle; close, cancel,
    // resume and process are the official Phase 1 Mission endpoints. The UI
    // never writes campaign status itself.
    var path = (action === "publish" || action === "pause")
      ? "/api/admin/gc-campaigns/" + encodeURIComponent(campaignId) + "/" + action
      : "/api/admin/mission-pool/" + encodeURIComponent(campaignId) + "/" + action;
    return host.apiPost(path).catch(function (e) { return { status: "error", code: e.message }; })
      .then(function (r) {
        if (!r || r.status !== "ok") host.toast("❌ " + ((r && r.code) || "action_failed"), "error");
        else host.toast("✅ " + action + " ok", "success");
        openDetail(campaignId);
      });
  }

  function runAction(action, campaignId) {
    if (CONFIRM_COPY[action] && !host.confirm(CONFIRM_COPY[action])) return Promise.resolve();
    if (action !== "publish") return postAction(action, campaignId);

    // §8: publishing (and resuming, which is the same transition) is blocked
    // when the pool cannot cover the winner target. campaign_centre._transition
    // checks only that a mission config and a pool id exist, so without this
    // an operator could publish a mission whose shared pool has since been
    // drained — or whose winner target was raised — and winners would end up
    // with no reward. The verdict is re-read here rather than trusted from
    // the rendered page: stock is shared and moves under us.
    return softGet("/api/admin/mission-pool/" + encodeURIComponent(campaignId) + "/edit-state")
      .then(function (state) {
        if (!state || state.status !== "ok") {
          host.toast("❌ Could not confirm reward inventory before publishing (" +
            ((state && state.code) || "unknown") + ")", "error");
          return null;
        }
        var reward = state.reward || {};
        if (!reward.sufficient) {
          host.toast("❌ Publishing blocked: winner target " + num(reward.winner_count) +
            " exceeds the " + num(reward.available) + " available codes in " +
            (reward.pool_id || "the configured pool") + ".", "error");
          openDetail(campaignId);
          return null;
        }
        return postAction(action, campaignId);
      });
  }

  // -----------------------------------------------------------------------
  // Wiring
  // -----------------------------------------------------------------------

  function onClick(event) {
    var target = event.target;
    var button = target && target.closest && target.closest("[data-mp-action]");
    if (!button) return;
    var root = el();
    if (!root || !root.contains(button)) return;
    event.preventDefault();
    dispatch(button.getAttribute("data-mp-action"), button.getAttribute("data-mp-id"));
  }

  /**
   * The single entry point for every operator action on this surface. Kept
   * separate from the DOM event so the whole surface can be driven — and
   * tested — without a browser.
   */
  function dispatch(action, id) {
    if (action === "refresh") { loadList(); return; }
    if (action === "back-to-list") { loadList(); return; }
    if (action === "create") { startCreate(null); return; }
    if (action === "create-test") { startCreate(internalTestPreset()); return; }
    if (action === "open") { openDetail(id); return; }
    if (action === "results") { openDetail(id); return; }
    if (action === "back-to-detail") { openDetail(id); return; }
    if (action === "edit") { openEdit(id); return; }
    if (action === "save-edit") { saveEdit(id); return; }
    if (action === "copy-link") {
      host.copy(id);
      host.toast("✅ Mission link copied", "success");
      return;
    }
    if (action === "edit-refresh") {
      if (session.mode() !== MODE_EDIT || !session.editState()) return;
      captureEditForm();
      renderEdit();
      return;
    }
    if (action === "wizard-refresh") {
      if (session.mode() !== MODE_CREATE || !session.draft()) return;
      captureCreateStep();
      renderCreate();
      return;
    }
    if (action === "wizard-next" || action === "wizard-back") {
      var d = session.draft();
      if (!d) return;
      captureCreateStep();
      if (action === "wizard-back") { d.step = Math.max(0, d.step - 1); renderCreate(); return; }
      var err = validateCreateStep(d);
      if (err) { host.toast("❌ " + err, "error"); return; }
      d.step = Math.min(WIZARD_STEPS.length - 1, d.step + 1);
      renderCreate();
      return;
    }
    if (action === "save-draft") { captureCreateStep(); submitCreate(false); return; }
    if (action === "publish-new") { captureCreateStep(); submitCreate(true); return; }
    if (["publish", "pause", "close", "cancel", "resume", "process"].indexOf(action) !== -1) {
      runAction(action, id);
      return;
    }
  }

  // Controls whose value changes what the current wizard step must show
  // (conditional mission fields, existing-vs-new pool, live code counts).
  var WIZARD_REACTIVE_IDS = [
    "mp-c-mtype", "mp-c-mode-existing", "mp-c-mode-new", "mp-c-pool",
    "mp-c-newpool-codes", "mp-c-winners", "mp-c-options", "mp-c-newpool-type",
  ];

  // The same, for the edit panel. Mission Type decides which mission_config
  // controls exist at all, so it MUST re-render: without it the operator
  // switches to feedback, sees no length inputs, and saves the fallback
  // bounds — or switches to keyword, sees no keyword input, and the backend
  // rejects the save.
  var EDIT_REACTIVE_IDS = ["mp-e-mtype", "mp-e-pool", "mp-e-winners"];

  function onChange(event) {
    var id = event.target && event.target.id;
    if (WIZARD_REACTIVE_IDS.indexOf(id) !== -1) { dispatch("wizard-refresh"); return; }
    if (EDIT_REACTIVE_IDS.indexOf(id) !== -1) { dispatch("edit-refresh"); }
  }

  return {
    core: CORE,
    /** Test/diagnostic access to the live session. */
    session: function () { return session; },
    init: function (hostApi) {
      host = hostApi;
      var root = el();
      if (root && !root.dataset.mpBound) {
        root.dataset.mpBound = "1";
        root.addEventListener("click", onClick);
        root.addEventListener("change", onChange);
      }
    },
    load: function () { loadList(); },
    open: function (campaignId) { openDetail(campaignId); },
    dispatch: dispatch,
  };
});
