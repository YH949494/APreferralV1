/**
 * Mission Reward Pool — Mini App mission card (Phase 2).
 *
 * ACTIVATION RULE (§5, the single most important thing in this file)
 * -----------------------------------------------------------------
 * This widget renders NOTHING unless BOTH are true:
 *   1. a Mission deep-link reference is present in this app open, and
 *   2. the server answers /api/mission-pool/<id>/view with
 *      mechanic === "mission_pool".
 *
 * The mechanic is never inferred from a campaign name, a voucher pool, the
 * presence of a mission_config, or the route alone. A normal Mini App open
 * has no mission reference, so this file makes ZERO network requests and
 * touches no existing DOM — Standard Voucher Drop, pooled/personalised
 * drops, the Welcome flow, tournament rewards and Campaign Rewards behave
 * exactly as before (§5, §22, §50).
 *
 * The deep-link value is a NAVIGATION REFERENCE ONLY (§6). Identity,
 * eligibility, winner state and reward ownership all come from the
 * authenticated session server-side; a forged campaign id can only produce a
 * 404 here.
 */
(function () {
  "use strict";

  var ROOT_ID = "mission-pool-root";
  var API_TIMEOUT_MS = 12000;

  // Telegram Web/Desktop deliver initData/initDataUnsafe to the WebApp
  // bridge asynchronously (a postMessage round trip with the host client),
  // not synchronously at script-parse time the way native clients usually
  // do — this app's own waitForInitData() in index.html exists for the
  // exact same reason. A single synchronous read of start_param on mount
  // can therefore run before Telegram has delivered it, silently treating
  // a real Mission deep link as a normal open. So the Telegram-sourced
  // check below is retried briefly before concluding there is no deep
  // link; the query-string fallbacks are synchronous and never need it.
  var START_PARAM_WAIT_MS = 1500;
  var START_PARAM_POLL_MS = 100;

  // ---------------------------------------------------------------------
  // Telegram plumbing (mirrors campaign-centre-widget.js so both widgets
  // authenticate identically — verified initData, never a client uid).
  // ---------------------------------------------------------------------

  function tgApp() {
    try { return (window.Telegram && window.Telegram.WebApp) || null; } catch (e) { return null; }
  }

  function getInitData() {
    var tg = tgApp();
    try {
      if (tg && typeof tg.initData === "string" && tg.initData.length) return tg.initData;
    } catch (e) {}
    try { return new URLSearchParams(location.search).get("init_data") || ""; } catch (e) { return ""; }
  }

  function withInitData(path) {
    var initData = getInitData();
    if (!initData) return path;
    return path + (path.indexOf("?") === -1 ? "?" : "&") + "init_data=" + encodeURIComponent(initData);
  }

  /**
   * fetch with an explicit timeout. Distinguishes three outcomes, because
   * §12 requires a timed-out submission to be treated as "unknown", never as
   * "failed": {ok:true,data} | {ok:false,timeout:true} | {ok:false,data}.
   */
  function apiCall(method, path, body) {
    var controller = null;
    var timer = null;
    try { controller = new AbortController(); } catch (e) {}
    var opts = {
      method: method,
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
    };
    if (body) opts.body = JSON.stringify(body);
    if (controller) opts.signal = controller.signal;
    if (controller) timer = setTimeout(function () { try { controller.abort(); } catch (e) {} }, API_TIMEOUT_MS);

    return fetch(withInitData(path), opts)
      .then(function (r) {
        return r.json().catch(function () { return null; }).then(function (d) {
          return { ok: r.ok, httpStatus: r.status, data: d };
        });
      })
      .catch(function () { return { ok: false, timeout: true, data: null }; })
      .then(function (res) { if (timer) clearTimeout(timer); return res; });
  }

  function apiGet(path) { return apiCall("GET", path, null); }
  function apiPost(path, body) { return apiCall("POST", path, body || {}); }

  // ---------------------------------------------------------------------
  // Deep link resolution (§6, §7)
  // ---------------------------------------------------------------------

  var MISSION_PREFIX = "mission_";
  var SAFE_PARAM = /^[A-Za-z0-9_-]{1,64}$/;

  function parseMissionParam(raw) {
    if (typeof raw !== "string") return null;
    raw = raw.trim();
    if (raw.indexOf(MISSION_PREFIX) !== 0) return null; // e.g. the existing attr_ ad param
    var id = raw.slice(MISSION_PREFIX.length);
    if (!id || !SAFE_PARAM.test(MISSION_PREFIX + id)) return null;
    return id;
  }

  /**
   * Reads the campaign reference from, in order: the Telegram start
   * parameter (t.me/<bot>?startapp=mission_<id>), the tgWebAppStartParam
   * query fallback Telegram uses on some clients, and an explicit
   * ?mission=<id> for browser testing. No other source is consulted.
   */
  function resolveCampaignRef() {
    var tg = tgApp();
    try {
      var sp = tg && tg.initDataUnsafe && tg.initDataUnsafe.start_param;
      if (sp !== undefined && sp !== null) log("raw start_param", sp);
      var fromStart = parseMissionParam(sp);
      if (fromStart) return fromStart;
    } catch (e) {}
    try {
      var qs = new URLSearchParams(location.search);
      var fromQsStart = parseMissionParam(qs.get("tgWebAppStartParam"));
      if (fromQsStart) return fromQsStart;
      var explicit = qs.get("mission");
      if (explicit && SAFE_PARAM.test(MISSION_PREFIX + explicit)) return explicit;
    } catch (e) {}
    return null;
  }

  /**
   * Polls `check()` until it returns a truthy value or the retry budget is
   * spent, then calls `cb` exactly once with whatever `check()` last
   * returned (a value or null). Shared by the campaign-reference wait and
   * the signed-initData wait below — both are waiting on the same
   * Telegram postMessage delivery, just different fields of it.
   */
  function waitFor(check, cb) {
    var immediate = check();
    if (immediate) { cb(immediate); return; }
    var waited = 0;
    (function poll() {
      var found = check();
      if (found) { cb(found); return; }
      waited += START_PARAM_POLL_MS;
      if (waited >= START_PARAM_WAIT_MS) { cb(null); return; }
      var t = setTimeout(poll, START_PARAM_POLL_MS);
      try { if (t && typeof t.unref === "function") t.unref(); } catch (e) {}
    }());
  }

  /**
   * Resolves the campaign reference, retrying briefly if Telegram has not
   * yet delivered initDataUnsafe (§ race noted above). `cb` is called
   * exactly once, with the campaign id or null. On the (overwhelmingly
   * common) normal open with no deep link at all, this costs a bounded
   * ~1.5s of no-op timers and, per §7/§22, still zero network requests and
   * zero DOM changes in the meantime.
   */
  function waitForCampaignRef(cb) { waitFor(resolveCampaignRef, cb); }

  /**
   * The signed `initData` string can arrive on its own schedule, separate
   * from `initDataUnsafe.start_param` — Telegram Web can expose one before
   * the other. Firing the authenticated /view call before initData is
   * ready gets an auth failure that this widget must not render, and would
   * otherwise leave a genuinely valid Mission deep link blank exactly like
   * the start_param race above. Retried the same way; giving up after the
   * budget still lets the request go out; if that's a true logged-out
   * unauthenticated call, /view correctly rejects it.
   */
  function waitForInitData(cb) { waitFor(getInitData, cb); }

  // ---------------------------------------------------------------------
  // Observability (§45). Campaign ids and states only — never a voucher
  // code, a feedback answer, a gaming account, an identity key or a risk
  // flag (§45, §46).
  // ---------------------------------------------------------------------

  // Diagnostic console logging only — campaign ids and states, exactly like
  // track() above. Never Telegram initData, a voucher code, an identity or
  // any submitted answer.
  function log(msg, data) {
    try { console.log("[MissionPool] " + msg, data === undefined ? "" : data); } catch (e) {}
  }

  function track(event, detail) {
    try {
      if (window.MissionPoolEvents && typeof window.MissionPoolEvents.push === "function") {
        window.MissionPoolEvents.push({ event: event, detail: detail || {}, at: Date.now() });
      }
      document.dispatchEvent(new CustomEvent("mission-pool-event", {
        detail: { event: event, detail: detail || {} },
      }));
    } catch (e) {}
  }

  // ---------------------------------------------------------------------
  // DOM helpers
  // ---------------------------------------------------------------------

  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    attrs = attrs || {};
    Object.keys(attrs).forEach(function (k) {
      if (k === "class") node.className = attrs[k];
      else if (k === "text") node.textContent = attrs[k];
      else node.setAttribute(k, attrs[k]);
    });
    (children || []).forEach(function (c) { if (c) node.appendChild(c); });
    return node;
  }

  function formatWhen(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (isNaN(d.getTime())) return "";
      return d.toLocaleString(undefined, {
        year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
      });
    } catch (e) { return ""; }
  }

  var stylesInjected = false;
  function injectStyles() {
    if (stylesInjected) return;
    stylesInjected = true;
    var style = document.createElement("style");
    style.textContent = [
      "#mission-pool-root{margin:0 0 16px;}",
      ".mp-card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:14px 16px;}",
      ".mp-title{font-weight:700;font-size:16px;margin-bottom:8px;}",
      ".mp-prompt{font-size:14px;line-height:1.45;margin-bottom:12px;}",
      ".mp-meta{font-size:12px;opacity:.75;margin-top:10px;display:flex;gap:12px;flex-wrap:wrap;}",
      ".mp-options{display:flex;flex-direction:column;gap:8px;margin-bottom:12px;}",
      ".mp-option{display:flex;align-items:center;gap:10px;padding:12px 14px;border-radius:10px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.03);cursor:pointer;font-size:14px;text-align:left;width:100%;color:inherit;}",
      ".mp-option[aria-checked=\"true\"]{border-color:#ff8a3d;background:rgba(255,138,61,.12);}",
      ".mp-option-dot{width:16px;height:16px;border-radius:50%;border:2px solid rgba(255,255,255,.35);flex:0 0 auto;}",
      ".mp-option[aria-checked=\"true\"] .mp-option-dot{border-color:#ff8a3d;background:#ff8a3d;}",
      ".mp-input,.mp-textarea{width:100%;box-sizing:border-box;border-radius:10px;border:1px solid rgba(255,255,255,.12);background:rgba(0,0,0,.2);color:inherit;padding:12px 14px;font-size:14px;font-family:inherit;}",
      ".mp-textarea{min-height:96px;resize:vertical;}",
      ".mp-counter{font-size:11px;opacity:.7;text-align:right;margin-top:4px;}",
      ".mp-btn{border:none;border-radius:10px;padding:12px 16px;font-size:14px;font-weight:700;cursor:pointer;width:100%;margin-top:12px;background:linear-gradient(90deg,#ff8a3d,#f5b63f);color:#1a1200;}",
      ".mp-btn[disabled]{opacity:.55;cursor:default;}",
      ".mp-msg{margin-top:10px;font-size:13px;line-height:1.45;}",
      ".mp-msg-error{color:#ff8a8a;}",
      ".mp-state{font-size:14px;line-height:1.5;white-space:pre-line;}",
      ".mp-state-title{font-weight:700;font-size:16px;margin-bottom:6px;}",
      ".mp-link-btn{border:none;border-radius:10px;padding:10px 14px;font-size:13px;font-weight:700;cursor:pointer;margin-top:12px;background:rgba(255,255,255,.08);color:inherit;}",
    ].join("\n");
    document.head.appendChild(style);
  }

  // ---------------------------------------------------------------------
  // Copy (§10, §11, §13, §44)
  //
  // Participation language only. Nothing here ever says "reward secured",
  // "voucher reserved" or "you won" at submission time, and no state ever
  // reveals WHY a participant was excluded — a disqualified entry gets the
  // identical text a non-winning entry gets.
  // ---------------------------------------------------------------------

  var STATE_COPY = {
    scheduled: { title: "", body: "This mission starts at {starts_at}." },
    paused: { title: "⏸️ This mission is temporarily paused.", body: "Please check back later." },
    submitted: { title: "✅ Mission completed", body: "You're in the reward pool.\n\nResults will be announced after the campaign closes." },
    closed_processing: { title: "⏳ Mission closed", body: "We're finalising the qualified entries and winners." },
    won: { title: "🎉 You're a winner!", body: "Your reward is available in Campaign Rewards." },
    not_won: { title: "", body: "Thanks for joining this mission.\n\nThis round has ended. Keep an eye out for the next one." },
    ended: { title: "", body: "This mission has ended." },
    cancelled: { title: "", body: "This mission has been cancelled." },
  };

  var SUBMIT_ERROR_COPY = {
    invalid_option: "Please choose one of the options shown.",
    empty_answer: "Please enter your answer.",
    answer_too_short: "Your answer is a little too short.",
    answer_too_long: "Your answer is too long.",
    invalid_answer_type: "Please enter your answer as text.",
    invalid_mission_config: "This mission isn't available right now.",
    campaign_paused: "This mission is temporarily paused. Please check back later.",
    campaign_closed: "This mission has closed.",
    campaign_cancelled: "This mission has been cancelled.",
    campaign_not_started: "This mission hasn't started yet.",
    campaign_not_live: "This mission isn't open right now.",
    mission_pool_disabled: "This mission isn't available right now.",
  };

  function submitErrorText(code) {
    return SUBMIT_ERROR_COPY[code] || "We couldn't submit your answer. Please try again.";
  }

  // ---------------------------------------------------------------------
  // Rendering
  // ---------------------------------------------------------------------

  function renderStateCard(root, view) {
    var copy = STATE_COPY[view.user_state];
    if (!copy) return;
    var card = el("div", { class: "mp-card" });
    card.appendChild(el("div", { class: "mp-title", text: "🎯 " + (view.campaign_name || "") }));
    if (copy.title) card.appendChild(el("div", { class: "mp-state-title", text: copy.title }));

    var body = copy.body.replace("{starts_at}",
      formatWhen((view.schedule || {}).starts_at) || "a later time");
    card.appendChild(el("div", { class: "mp-state", text: body }));

    // A winner is pointed at the canonical reward surface — the Mission card
    // never renders a voucher code of its own (§19).
    if (view.user_state === "won") {
      var btn = el("button", { class: "mp-link-btn", type: "button", text: "🎁 Go to Campaign Rewards" });
      btn.addEventListener("click", function () {
        highlightCampaignReward(view.campaign_id);
      });
      card.appendChild(btn);
    }
    root.appendChild(card);
  }

  function renderMissionForm(root, view) {
    var mission = view.mission || {};
    var card = el("div", { class: "mp-card" });
    card.appendChild(el("div", { class: "mp-title", text: "🎯 " + (view.campaign_name || "") }));
    card.appendChild(el("div", { class: "mp-prompt", text: mission.prompt || "" }));

    var readAnswer = null;

    if (mission.mission_type === "multiple_choice" || mission.mission_type === "single_choice") {
      var selected = null;
      var wrap = el("div", { class: "mp-options", role: "radiogroup" });
      (mission.options || []).forEach(function (opt) {
        var btn = el("button", {
          class: "mp-option", type: "button", role: "radio",
          "aria-checked": "false", "data-option-id": opt.id,
        });
        btn.appendChild(el("span", { class: "mp-option-dot" }));
        btn.appendChild(el("span", { text: opt.label || opt.id }));
        btn.addEventListener("click", function () {
          selected = opt.id;
          Array.prototype.forEach.call(wrap.children, function (c) { c.setAttribute("aria-checked", "false"); });
          btn.setAttribute("aria-checked", "true");
        });
        wrap.appendChild(btn);
      });
      card.appendChild(wrap);
      // Always a plain string — never a nested object — so a Mongo-operator
      // shaped payload cannot originate here (§43).
      readAnswer = function () { return selected; };

    } else if (mission.mission_type === "keyword") {
      var input = el("input", {
        class: "mp-input", type: "text", maxlength: String(mission.max_answer_chars || 2000),
        placeholder: "Type your answer",
      });
      card.appendChild(input);
      readAnswer = function () { return String(input.value || "").trim(); };

    } else {
      var max = mission.max_chars || mission.max_answer_chars || 500;
      var ta = el("textarea", { class: "mp-textarea", maxlength: String(max), placeholder: "Share your feedback" });
      var counter = el("div", { class: "mp-counter", text: "0 / " + max });
      ta.addEventListener("input", function () {
        counter.textContent = String(ta.value.length) + " / " + max;
      });
      card.appendChild(ta);
      card.appendChild(counter);
      readAnswer = function () { return String(ta.value || "").trim(); };
    }

    var msg = el("div", { class: "mp-msg", style: "display:none;" });
    var submit = el("button", { class: "mp-btn", type: "button", text: "Submit Mission" });

    submit.addEventListener("click", function () {
      var answer = readAnswer();
      if (answer === null || answer === "") {
        msg.className = "mp-msg mp-msg-error";
        msg.textContent = "Please answer the mission first.";
        msg.style.display = "block";
        return;
      }
      track("mission_submit_clicked", { campaign_id: view.campaign_id, mission_type: mission.mission_type });
      submit.disabled = true;
      submit.textContent = "Submitting…";
      msg.style.display = "none";
      doSubmit(root, view, answer, submit, msg);
    });

    card.appendChild(submit);
    card.appendChild(msg);

    var meta = el("div", { class: "mp-meta" });
    var endsAt = formatWhen((view.schedule || {}).ends_at);
    if (endsAt) meta.appendChild(el("span", { text: "⏳ Ends " + endsAt }));
    if (view.winner_count) meta.appendChild(el("span", { text: "🏆 " + view.winner_count + " winners" }));
    if (meta.children.length) card.appendChild(meta);

    root.appendChild(card);
  }

  function showSubmitted(root, view) {
    root.innerHTML = "";
    renderStateCard(root, Object.assign({}, view, { user_state: "submitted" }));
  }

  /**
   * §10/§11/§12. Submission is idempotent server-side (a unique index on
   * campaign+user), so:
   *   - already_submitted is a SUCCESS for the user, never a "409 conflict";
   *   - a timeout is UNKNOWN, never a failure — we ask the server what
   *     actually happened rather than guessing, and only offer a retry once
   *     the server confirms no entry exists.
   * No client-side duplicate suppression is used as the protection; the
   * unique index remains the authority.
   */
  function doSubmit(root, view, answer, submitBtn, msg) {
    apiPost("/api/mission-pool/" + encodeURIComponent(view.campaign_id) + "/submit", { answer: answer })
      .then(function (res) {
        if (res.timeout) {
          msg.className = "mp-msg";
          msg.textContent = "We couldn't confirm your submission.\nChecking your mission status…";
          msg.style.display = "block";
          return recoverAfterTimeout(root, view, submitBtn, msg);
        }
        var data = res.data || {};
        if (data.status === "ok" && data.state === "already_submitted") {
          track("mission_submit_duplicate", { campaign_id: view.campaign_id });
          root.innerHTML = "";
          var card = el("div", { class: "mp-card" });
          card.appendChild(el("div", { class: "mp-title", text: "🎯 " + (view.campaign_name || "") }));
          card.appendChild(el("div", { class: "mp-state-title", text: "✅ Mission already completed" }));
          card.appendChild(el("div", { class: "mp-state", text: "You're already in the reward pool." }));
          root.appendChild(card);
          return;
        }
        if (data.status === "ok" && data.submitted) {
          track("mission_submit_success", { campaign_id: view.campaign_id });
          showSubmitted(root, view);
          return;
        }
        track("mission_submit_error", { campaign_id: view.campaign_id, code: (data && data.code) || "unknown" });
        submitBtn.disabled = false;
        submitBtn.textContent = "Submit Mission";
        msg.className = "mp-msg mp-msg-error";
        msg.textContent = submitErrorText(data && data.code);
        msg.style.display = "block";
      });
  }

  function recoverAfterTimeout(root, view, submitBtn, msg) {
    return apiGet("/api/mission-pool/" + encodeURIComponent(view.campaign_id) + "/status")
      .then(function (res) {
        var data = res.data || {};
        if (data.status === "ok" && data.submitted) {
          track("mission_submit_success", { campaign_id: view.campaign_id, recovered: true });
          showSubmitted(root, view);
          return;
        }
        track("mission_submit_error", { campaign_id: view.campaign_id, code: "timeout_unconfirmed" });
        submitBtn.disabled = false;
        submitBtn.textContent = "Submit Mission";
        msg.className = "mp-msg mp-msg-error";
        msg.textContent = "We couldn't confirm your submission. Please try again.";
        msg.style.display = "block";
      });
  }

  /**
   * Hands off to the Campaign Rewards widget, which owns the reward surface.
   * Never copies a code into mission-local state (§16, §19).
   */
  function highlightCampaignReward(campaignId) {
    try {
      if (window.CampaignCentreWidget && typeof window.CampaignCentreWidget.highlightRewardForCampaign === "function") {
        window.CampaignCentreWidget.highlightRewardForCampaign(campaignId);
        return;
      }
    } catch (e) {}
    try {
      var target = document.getElementById("campaign-centre-root");
      if (target && target.scrollIntoView) target.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch (e) {}
  }

  // ---------------------------------------------------------------------
  // Mount
  // ---------------------------------------------------------------------

  function render(root, view) {
    root.innerHTML = "";
    injectStyles();
    if (view.user_state === "live") renderMissionForm(root, view);
    else renderStateCard(root, view);
  }

  function mount() {
    var root = document.getElementById(ROOT_ID);
    if (!root) return;

    waitForCampaignRef(function (campaignId) {
      // No mission reference -> not a Mission open. Zero requests, zero DOM
      // changes, existing Mini App behaviour completely unchanged (§7, §22).
      if (!campaignId) { log("no mission deep link; skipping"); return; }
      log("parsed mission campaign id", campaignId);

      // Wait for the signed initData too — start_param and initData are
      // delivered by the same Telegram postMessage handshake but are not
      // guaranteed to land at the same instant (see waitForInitData above).
      // Firing /view before it arrives would only earn an auth failure.
      waitForInitData(function (initData) {
        if (!initData) log("proceeding without confirmed init data", campaignId);

        apiGet("/api/mission-pool/" + encodeURIComponent(campaignId) + "/view").then(function (res) {
          var view = res.data || {};
          log("mission /view response", { campaign_id: campaignId, http_status: res.httpStatus, timeout: !!res.timeout });
          // The server is the only thing that may switch Mission UI on (§5).
          if (!res.ok || view.status !== "ok" || view.mechanic !== "mission_pool") {
            log("render decision: skip (not a confirmed mission_pool view)", { campaign_id: campaignId });
            return;
          }
          if (STATE_COPY[view.user_state] === undefined && view.user_state !== "live") {
            log("render decision: skip (unrecognised user_state)", { campaign_id: campaignId, user_state: view.user_state });
            return;
          }
          log("render decision: render", { campaign_id: campaignId, user_state: view.user_state });
          track("mission_ui_opened", { campaign_id: view.campaign_id, user_state: view.user_state });
          render(root, view);
        });
      });
    });
  }

  // Exposed for tests and for the winner popup's "Redeem Now" handoff.
  window.MissionPoolWidget = {
    parseMissionParam: parseMissionParam,
    resolveCampaignRef: resolveCampaignRef,
    waitForCampaignRef: waitForCampaignRef,
    waitForInitData: waitForInitData,
    userStateCopy: STATE_COPY,
    render: render,
    mount: mount,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mount);
  } else {
    mount();
  }
}());
