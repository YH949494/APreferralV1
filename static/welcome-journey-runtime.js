/* APReferral Welcome Journey Runtime Dashboard
   Standalone JS — talks to /api/admin/dashboard/welcome-journey-runtime
   Read-only observability: reports live scheduler/reminder/funnel state,
   never reminder timing, eligibility, voucher rules, XP or scheduler config. */
(function () {
  "use strict";

  function $(sel, root) { return (root || document).querySelector(sel); }
  function $all(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function esc(v) {
    return String(v === null || v === undefined ? "" : v)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  function fmt(v) {
    if (v === null || v === undefined) return "—";
    if (typeof v === "number") return v.toLocaleString();
    return String(v);
  }

  function fmtTime(v) {
    if (!v) return "—";
    try {
      var d = new Date(v);
      if (isNaN(d.getTime())) return String(v);
      return d.toLocaleString();
    } catch (e) {
      return String(v);
    }
  }

  function fmtDuration(s) {
    if (s === null || s === undefined) return "—";
    return Number(s) >= 10 ? Math.round(s) + " sec" : Number(s).toFixed(1) + " sec";
  }

  function fmtPct(v) {
    if (v === null || v === undefined) return "—";
    return Number(v).toFixed(1) + "%";
  }

  function api(path) {
    return fetch(path, { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then(function (r) {
        if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
        return r.json().then(function (j) {
          if (!r.ok) throw new Error((j && j.message) || "HTTP " + r.status);
          return j;
        });
      });
  }

  function initSectionNav() {
    $all(".nav-item[data-section]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        $all(".nav-item[data-section]").forEach(function (b) { b.classList.remove("active"); });
        btn.classList.add("active");
        var target = document.getElementById("section-" + btn.dataset.section);
        if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    });
  }

  function summaryCard(label, value, cls) {
    return '<div class="rs-summary-card"><div class="label">' + esc(label) + '</div>' +
      '<div class="value' + (cls ? " " + cls : "") + '">' + esc(value) + "</div></div>";
  }

  function statusPill(status) {
    return '<span class="rs-status-pill">' + esc(status || "—") + "</span>";
  }

  function renderAlerts(alerts) {
    if (!alerts || !alerts.length) {
      $("#alerts-list").innerHTML = '<div class="rs-note">No active alerts.</div>';
      return;
    }
    $("#alerts-list").innerHTML = alerts.map(function (a) {
      var icon = a.level === "critical" ? "🔴" : "🟠";
      return '<div class="wj-alert ' + esc(a.level) + '">' + icon + " " + esc(a.message) + "</div>";
    }).join("");
  }

  function renderScheduler(scheduler) {
    var reminders = scheduler.reminders || {};
    var lifecycle = scheduler.lifecycle || {};
    var html =
      summaryCard("Reminders — Status", reminders.status || "—") +
      summaryCard("Reminders — Last Run", fmtTime(reminders.last_run)) +
      summaryCard("Reminders — Next Run", fmtTime(reminders.next_run)) +
      summaryCard("Reminders — Duration", fmtDuration(reminders.last_run_duration_s)) +
      summaryCard("Lifecycle — Status", lifecycle.status || "—") +
      summaryCard("Lifecycle — Last Run", fmtTime(lifecycle.last_run)) +
      summaryCard("Lifecycle — Next Run", fmtTime(lifecycle.next_run)) +
      summaryCard("Lifecycle — Duration", fmtDuration(lifecycle.last_run_duration_s));
    $("#scheduler-summary").innerHTML = html;
  }

  function renderLastRun(lastRun) {
    lastRun = lastRun || {};
    $("#lastrun-meta").textContent = lastRun.at
      ? "Last run at " + fmtTime(lastRun.at) + " (" + fmtDuration(lastRun.duration_s) + ")"
      : "No run recorded yet.";
    $("#lastrun-summary").innerHTML =
      summaryCard("Users Scanned", fmt(lastRun.users_scanned)) +
      summaryCard("Eligible Day 2 (20h)", fmt(lastRun.eligible_20h)) +
      summaryCard("Eligible Day 2 Final (28h)", fmt(lastRun.eligible_28h)) +
      summaryCard("Eligible Day 3", fmt(lastRun.eligible_day3)) +
      summaryCard("20h Reminders Sent", fmt(lastRun.reminders_20h_sent)) +
      summaryCard("28h Reminders Sent", fmt(lastRun.reminders_28h_sent)) +
      summaryCard("Day 3 Reminders Sent", fmt(lastRun.day3_reminders_sent)) +
      summaryCard("Telegram Failed", fmt(lastRun.telegram_failed), lastRun.telegram_failed ? "bad" : "ok") +
      summaryCard("Blocked Users", fmt(lastRun.blocked_users), lastRun.blocked_users ? "bad" : "ok") +
      summaryCard("Skipped Users", fmt((lastRun.skipped_users || {}).total));

    var skipped = lastRun.skipped_users || {};
    $("#skipped-summary").innerHTML =
      summaryCard("Already Claimed", fmt(skipped.already_claimed)) +
      summaryCard("Expired", fmt(skipped.expired)) +
      summaryCard("Already Sent", fmt(skipped.already_sent)) +
      summaryCard("Risk Blocked", fmt(skipped.risk_blocked)) +
      summaryCard("Multi Account", fmt(skipped.multi_account)) +
      summaryCard("Left Channel", fmt(skipped.left_channel)) +
      summaryCard("Bot Blocked", fmt(skipped.bot_blocked)) +
      summaryCard("Missing Data", fmt(skipped.missing_data));
  }

  function renderFunnel(funnel) {
    funnel = funnel || {};
    var s = funnel.summary || {};
    $("#funnel-meta").textContent = "Window: " + (funnel.window_label || funnel.window || "all time");

    var started = (s.welcome_eligible_users || {}).value;
    var d1 = null; // Day 1 completion isn't tracked as a distinct funnel metric today; journey starts counted at eligibility.
    var d2Rate = (s.welcome_d2_rate_pct || {}).value;
    var d3Rate = (s.welcome_d3_rate_pct || {}).value;
    var completionRate = (s.welcome_completion_rate_pct || {}).value;
    var claimRate = (s.welcome_claim_rate_pct || {}).value;

    // The panel reports rates (day2/day3 as % of the prior stage); derive
    // display counts only where the panel itself gives us a count.
    var steps = [
      { name: "Started Journey (Eligible)", count: started, conv: null },
      { name: "Completed Day 2", count: null, conv: d2Rate },
      { name: "Completed Day 3", count: null, conv: d3Rate },
      { name: "Unlocked Welcome Voucher", count: null, conv: completionRate },
      { name: "Claimed Welcome Voucher", count: null, conv: claimRate },
    ];

    $("#funnel-list").innerHTML = steps.map(function (step, i) {
      var row = '<div class="wj-funnel-step"><div class="name">' + esc(step.name) + '</div>' +
        '<div class="count">' + (step.count === null || step.count === undefined ? "—" : fmt(step.count)) + '</div>' +
        '<div class="conv">' + (step.conv === null || step.conv === undefined ? "" : fmtPct(step.conv) + " conv.") + '</div></div>';
      return i === 0 ? row : '<div class="wj-funnel-arrow">↓</div>' + row;
    }).join("");
  }

  function renderRecent(rows) {
    var tbody = $("#recent-table tbody");
    if (!rows || !rows.length) {
      tbody.innerHTML = '<tr><td colspan="7">No runs recorded yet.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(function (r) {
      return "<tr>" +
        "<td>" + esc(fmtTime(r.time)) + "</td>" +
        "<td>" + esc(fmt(r.users_scanned)) + "</td>" +
        "<td>" + esc(fmt(r.sent_20h)) + "</td>" +
        "<td>" + esc(fmt(r.sent_28h)) + "</td>" +
        "<td>" + esc(fmt(r.sent_day3)) + "</td>" +
        "<td>" + esc(fmt(r.failed)) + "</td>" +
        "<td>" + esc(fmtDuration(r.duration_s)) + "</td>" +
        "</tr>";
    }).join("");
  }

  function renderDrilldown(d) {
    $("#drilldown-json").textContent = JSON.stringify({
      scheduler: d.scheduler,
      last_run: d.last_run,
      recent_runs: d.recent_runs,
      alerts: d.alerts,
    }, null, 2);
  }

  function loadDashboard(forceRefresh) {
    $("#dashboard-meta").textContent = "Loading…";
    api("/api/admin/dashboard/welcome-journey-runtime" + (forceRefresh ? "?refresh=1" : ""))
      .then(function (d) {
        $("#dashboard-meta").textContent = "Generated at " + fmtTime(d.generated_at);
        $("#global-banner").innerHTML = "";
        renderAlerts(d.alerts);
        renderScheduler(d.scheduler || {});
        renderLastRun(d.last_run);
        renderFunnel(d.funnel);
        renderRecent(d.recent_runs);
        renderDrilldown(d);
      })
      .catch(function (e) {
        $("#dashboard-meta").textContent = "Failed to load";
        $("#global-banner").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
      });
  }

  function loadAdminChip() {
    api("/api/admin/auth/me")
      .then(function (d) {
        var a = d.admin || {};
        if (a.username || a.id) $("#admin-chip").textContent = "@" + (a.username || a.id);
      })
      .catch(function () {});
  }

  $("#refresh-btn").addEventListener("click", function () { loadDashboard(true); });

  initSectionNav();
  loadAdminChip();
  loadDashboard();
})();
