/* APReferral Activation Funnel Dashboard
   Standalone JS — talks to /api/admin/funnel-dashboard */
(function () {
  "use strict";

  // ---- utilities ----
  function $(sel, root) { return (root || document).querySelector(sel); }
  function $all(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function fmt(v) {
    if (v === null || v === undefined) return "—";
    if (typeof v === "number") return v.toLocaleString();
    return String(v);
  }

  function pct(v) {
    if (v === null || v === undefined) return "—";
    return v.toFixed(1) + "%";
  }

  function esc(v) {
    return String(v === null || v === undefined ? "" : v)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
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

  function dqTag(q) {
    q = q || "exact";
    return '<span class="tag ' + esc(q) + '">' + esc(q) + "</span>";
  }

  // Color based on prev-stage conversion rate
  function barColor(prevConv) {
    if (prevConv === null || prevConv === undefined) return "accent";
    if (prevConv >= 70) return "green";
    if (prevConv >= 40) return "yellow";
    return "red";
  }

  // ---- State ----
  var state = {
    window: "7d",
    dateFrom: "",
    dateTo: "",
    data: null,
  };

  // ---- Section nav ----
  function initSectionNav() {
    $all(".nav-item[data-section]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        $all(".nav-item[data-section]").forEach(function (b) { b.classList.remove("active"); });
        btn.classList.add("active");
        var id = "section-" + btn.dataset.section;
        var target = document.getElementById(id);
        if (target) {
          target.scrollIntoView({ behavior: "smooth", block: "start" });
        }
      });
    });
  }

  // ---- Window selector ----
  function initWindowSel() {
    $all("#window-seg button").forEach(function (btn) {
      btn.addEventListener("click", function () {
        $all("#window-seg button").forEach(function (b) { b.classList.remove("active"); });
        btn.classList.add("active");
        state.window = btn.dataset.window;
        var cust = $("#custom-range");
        if (state.window === "custom") {
          cust.classList.add("visible");
        } else {
          cust.classList.remove("visible");
          loadDashboard();
        }
      });
    });

    $("#apply-custom").addEventListener("click", function () {
      state.dateFrom = $("#date-from").value;
      state.dateTo = $("#date-to").value;
      if (!state.dateFrom || !state.dateTo) {
        alert("Please select both From and To dates.");
        return;
      }
      loadDashboard();
    });
  }

  // ---- Build API URL ----
  function buildUrl(refresh) {
    var u = "/api/admin/funnel-dashboard?window=" + encodeURIComponent(state.window);
    if (state.window === "custom" && state.dateFrom && state.dateTo) {
      u += "&date_from=" + encodeURIComponent(state.dateFrom);
      u += "&date_to=" + encodeURIComponent(state.dateTo);
    }
    if (refresh) u += "&refresh=1";
    return u;
  }

  // ---- Meta ----
  function setMeta(msg) { $("#dashboard-meta").textContent = msg; }

  // ---- Load ----
  function loadDashboard(refresh) {
    setMeta("Loading…");
    setLoadingAll();
    api(buildUrl(refresh))
      .then(function (d) {
        state.data = d;
        var asOf = d.as_of ? new Date(d.as_of).toLocaleString() : "";
        setMeta("Window: " + (d.window_label || d.window) + " · Cohort: " + fmt(d.cohort_size) + " users · As of " + asOf);
        renderAll(d);
      })
      .catch(function (e) {
        if (e.message === "unauthorized") return;
        setMeta("Failed to load");
        setErrorAll("Failed: " + e.message);
      });
  }

  function setLoadingAll() {
    ["funnel-vis-body", "stage-table-body", "dropoff-body", "welcome-body", "split-body", "retention-body"]
      .forEach(function (id) { document.getElementById(id).innerHTML = '<div class="loading">Loading…</div>'; });
    $("#insights-panel").style.display = "none";
  }

  function setErrorAll(msg) {
    ["funnel-vis-body", "stage-table-body", "dropoff-body", "welcome-body", "split-body", "retention-body"]
      .forEach(function (id) {
        document.getElementById(id).innerHTML = '<div class="banner error">' + esc(msg) + "</div>";
      });
  }

  function renderAll(d) {
    var stages = d.stages || [];
    var topCount = stages.length && stages[0].count ? stages[0].count : 0;

    renderFunnelVis(stages, topCount, d);
    renderStageTable(stages, d);
    renderDropoff(d);
    renderWelcome(d);
    renderSplit(d);
    renderRetention(stages);
  }

  // ---- Section 1: Funnel Visualization ----
  function renderFunnelVis(stages, topCount, d) {
    var body = $("#funnel-vis-body");
    if (!stages.length) {
      body.innerHTML = '<div class="empty">No data for selected window.</div>';
      return;
    }

    var RETENTION_IDS = { "d7_retention": true, "d30_retention": true };
    var PROGRESS_IDS = { "first_checkin": true, "progress_2_3": true, "progress_3_3": true };
    var html = "";

    for (var i = 0; i < stages.length; i++) {
      var s = stages[i];
      if (RETENTION_IDS[s.id]) continue; // retention shown separately

      var missing = s.count === null;
      var barPct = (!missing && topCount > 0) ? Math.max(2, Math.round(100 * s.count / topCount)) : 2;
      var color = missing ? "gray" : barColor(s.prev_stage_conversion);

      if (s.id === "first_checkin") {
        // Progress sub-section header
        html += '<div class="note" style="padding:8px 0 4px;font-weight:600;color:var(--muted);">Welcome Progress</div>';
      }

      var indent = PROGRESS_IDS[s.id] ? "padding-left:16px;" : "";
      var stageName = esc(s.name);
      var countStr = missing ? "—" : fmt(s.count);
      var convStr = missing ? "—" : pct(s.conversion_from_top) + " of cohort";

      if (!missing && s.prev_stage_dropoff !== null && s.prev_stage_dropoff !== undefined && i > 0) {
        var prevS = stages[i - 1];
        if (prevS && prevS.count !== null) {
          var dropN = prevS.count - s.count;
          if (dropN > 0) {
            html += '<div class="fs-drop-arrow" style="' + indent + '">↓ ' + fmt(dropN) + ' lost (' + pct(s.prev_stage_dropoff) + ' drop)</div>';
          }
        }
      }

      html += '<div class="funnel-stage-row" style="' + indent + '">' +
        '<div class="fs-label">' + stageName + (s.data_quality !== "exact" ? ' ' + dqTag(s.data_quality) : "") + "</div>" +
        '<div class="fs-bar-wrap">' +
        '<div class="fs-bar ' + color + '" style="width:' + barPct + '%">' +
        (barPct > 15 ? (missing ? "N/A" : pct(s.conversion_from_top)) : "") +
        "</div></div>" +
        '<div class="fs-count">' + countStr + "</div>" +
        '<div class="fs-meta">' + (missing ? "" : convStr) + "</div>" +
        "</div>";

      if (s.id === "welcome_claim") {
        html += '<hr class="fs-separator">';
      }
    }

    body.innerHTML = html;

    // Insights
    var insights = buildInsights(stages, d);
    if (insights.length) {
      var insPanel = $("#insights-panel");
      insPanel.style.display = "";
      var list = $("#insights-list");
      list.innerHTML = insights.map(function (ins) {
        return '<li class="' + ins.level + '">' + esc(ins.text) + "</li>";
      }).join("");
    }
  }

  function buildInsights(stages, d) {
    var insights = [];
    var byId = {};
    stages.forEach(function (s) { byId[s.id] = s; });

    // Largest drop
    if (d.largest_dropoff && d.largest_dropoff.stage_name) {
      var ld = d.largest_dropoff;
      var level = ld.dropoff_pct >= 60 ? "bad" : ld.dropoff_pct >= 40 ? "warn" : "info";
      insights.push({
        level: level,
        text: "Largest drop-off: " + ld.stage_name + " — " + pct(ld.dropoff_pct) + " of previous-stage users lost at this step.",
      });
    }

    // Start Bot conversion
    var sb = byId["start_bot"];
    if (sb && sb.prev_stage_conversion !== null && sb.prev_stage_conversion < 50) {
      insights.push({
        level: "warn",
        text: "Only " + pct(sb.prev_stage_conversion) + " of channel joiners start the bot. Consider a stronger call-to-action in the channel welcome message.",
      });
    }

    // Check-in drop
    var ci = byId["first_checkin"];
    if (ci && ci.prev_stage_conversion !== null && ci.prev_stage_conversion < 40) {
      insights.push({
        level: "warn",
        text: "First check-in conversion is " + pct(ci.prev_stage_conversion) + ". Users are not discovering the daily check-in feature.",
      });
    }

    // Welcome claim
    var wc = byId["welcome_claim"];
    var wu = byId["welcome_unlock"];
    if (wc && wu && wu.count && wc.count !== null) {
      var claimRate = wu.count > 0 ? Math.round(100 * wc.count / wu.count) : 0;
      if (claimRate < 50) {
        insights.push({
          level: "warn",
          text: "Welcome Voucher claim rate: " + claimRate + "% of eligible users claim. Consider a reminder message.",
        });
      } else {
        insights.push({
          level: "ok",
          text: "Welcome Voucher claim rate is healthy: " + claimRate + "% of eligible users claim.",
        });
      }
    }

    // First Bet
    var fb = byId["first_bet"];
    if (fb && fb.count === null) {
      insights.push({
        level: "info",
        text: "First Bet data requires marketing data upload with coupon_code matching. Upload weekly marketing data to see this metric.",
      });
    }

    return insights;
  }

  // ---- Section 2: Stage Breakdown Table ----
  function renderStageTable(stages, d) {
    var body = $("#stage-table-body");
    var largestDropId = d.largest_dropoff && d.largest_dropoff.stage_id;

    var rows = stages.map(function (s) {
      var missing = s.count === null;
      var isLargest = s.id === largestDropId;
      var rowClass = isLargest ? ' class="largest-drop"' : "";
      var countCell = missing
        ? '<span style="color:var(--muted)">—</span>'
        : '<strong>' + fmt(s.count) + "</strong>";
      var convCell = missing ? "—" : pct(s.conversion_from_top);
      var dropCell = missing ? "—" : pct(s.dropoff_from_top);
      var prevConvCell = s.prev_stage_conversion === null || s.prev_stage_conversion === undefined
        ? "—"
        : pct(s.prev_stage_conversion);
      var prevDropCell = s.prev_stage_dropoff === null || s.prev_stage_dropoff === undefined
        ? "—"
        : pct(s.prev_stage_dropoff);

      // Color prev conversion
      if (s.prev_stage_conversion !== null && s.prev_stage_conversion !== undefined) {
        var c = s.prev_stage_conversion >= 70 ? "var(--ok)" : s.prev_stage_conversion >= 40 ? "var(--warn)" : "var(--bad)";
        prevConvCell = '<span style="color:' + c + ';font-weight:600">' + pct(s.prev_stage_conversion) + "</span>";
      }

      return "<tr" + rowClass + ">" +
        "<td><strong>" + esc(s.name) + "</strong>" +
        (isLargest ? ' <span class="tag missing" style="font-size:10px">⚠ Highest Drop</span>' : "") +
        (s.note ? '<div class="note" style="margin-top:4px">' + esc(s.note) + "</div>" : "") +
        "</td>" +
        "<td class=\"num\">" + countCell + "</td>" +
        "<td class=\"num\">" + convCell + "</td>" +
        "<td class=\"num\">" + dropCell + "</td>" +
        "<td class=\"num\">" + prevConvCell + "</td>" +
        "<td class=\"num\">" + prevDropCell + "</td>" +
        "<td>" + dqTag(s.data_quality) + "</td>" +
        "</tr>";
    }).join("");

    body.innerHTML =
      '<table class="funnel-table">' +
      "<thead><tr>" +
      "<th>Stage</th>" +
      '<th class="num">Users</th>' +
      '<th class="num">Conv from Top</th>' +
      '<th class="num">Drop from Top</th>' +
      '<th class="num">Conv from Prev</th>' +
      '<th class="num">Drop from Prev</th>' +
      "<th>Quality</th>" +
      "</tr></thead>" +
      "<tbody>" + rows + "</tbody>" +
      "</table>" +
      '<div class="note" style="margin-top:10px">Conv/Drop "from Top" = relative to Join Channel count. "from Prev" = relative to the immediately preceding stage.</div>';
  }

  // ---- Section 3: Drop-off Analysis ----
  function renderDropoff(d) {
    var body = $("#dropoff-body");
    var stages = d.stages || [];

    // Sort stages by prev_stage_dropoff desc, exclude join_channel
    var sortable = stages.filter(function (s) {
      return s.id !== "join_channel" && s.count !== null && s.prev_stage_dropoff !== null;
    }).slice().sort(function (a, b) {
      return (b.prev_stage_dropoff || 0) - (a.prev_stage_dropoff || 0);
    });

    if (!sortable.length) {
      body.innerHTML = '<div class="empty">Not enough data to analyse drop-offs.</div>';
      return;
    }

    var largest = sortable[0];
    var cardColor = largest.prev_stage_dropoff >= 60 ? "var(--bad)" : largest.prev_stage_dropoff >= 40 ? "var(--warn)" : "var(--ok)";

    var cardHtml =
      '<div class="dropoff-card" style="border-color:' + cardColor + '">' +
      '<div class="title" style="color:' + cardColor + '">Highest Drop-off Stage</div>' +
      '<div class="stage-name">' + esc(largest.name) + "</div>" +
      '<div class="pct" style="color:' + cardColor + '">' + pct(largest.prev_stage_dropoff) + "</div>" +
      '<div class="sub">of previous-stage users are lost at this step</div>' +
      '<div class="sub" style="margin-top:8px;">' +
      "Conversion from previous stage: <strong>" + pct(largest.prev_stage_conversion) + "</strong>" +
      "</div>" +
      "</div>";

    // All stages ranked
    var tableRows = sortable.map(function (s, i) {
      var dpct = s.prev_stage_dropoff || 0;
      var color = dpct >= 60 ? "var(--bad)" : dpct >= 40 ? "var(--warn)" : "var(--ok)";
      return "<tr>" +
        "<td>" + (i + 1) + "</td>" +
        "<td><strong>" + esc(s.name) + "</strong></td>" +
        "<td class=\"num\" style=\"color:" + color + ";font-weight:700\">" + pct(dpct) + "</td>" +
        "<td class=\"num\">" + pct(s.prev_stage_conversion) + "</td>" +
        "<td class=\"num\">" + fmt(s.count) + "</td>" +
        "</tr>";
    }).join("");

    var tableHtml =
      '<div class="wide-panel" style="margin-top:20px">' +
      '<div class="section-title" style="margin-bottom:12px;">All Stages Ranked by Drop-off</div>' +
      '<table class="funnel-table">' +
      "<thead><tr><th>#</th><th>Stage</th><th class=\"num\">Drop from Prev</th><th class=\"num\">Conv from Prev</th><th class=\"num\">Users</th></tr></thead>" +
      "<tbody>" + tableRows + "</tbody>" +
      "</table></div>";

    // Diagnosis text
    var diagnosis = buildDiagnosis(largest, d);
    var diagHtml =
      '<div class="wide-panel">' +
      '<div class="section-title" style="margin-bottom:12px;">Why Users Leave at This Stage</div>' +
      '<ul class="insight-list">' +
      diagnosis.map(function (t) {
        return '<li class="' + t.level + '">' + esc(t.text) + "</li>";
      }).join("") +
      "</ul></div>";

    body.innerHTML = cardHtml + diagHtml + tableHtml;
  }

  function buildDiagnosis(stage, d) {
    var tips = [];
    var id = stage.id;

    if (id === "start_bot") {
      tips.push({ level: "info", text: "Users join the channel but don't start the bot. The channel message may not clearly direct users to message the bot." });
      tips.push({ level: "info", text: "Consider pinning a message with a direct /start deep-link button." });
    } else if (id === "first_checkin") {
      tips.push({ level: "info", text: "Users start the bot but don't check in. The check-in feature may not be prominent enough in the bot menu." });
      tips.push({ level: "info", text: "A bot reminder 24h after /start can significantly improve first check-in rates." });
    } else if (id === "progress_2_3" || id === "progress_3_3") {
      tips.push({ level: "info", text: "Users do their first check-in but don't maintain the habit. Daily reminder notifications help maintain streaks." });
      tips.push({ level: "info", text: "Consider showing progress visually in the bot to motivate continued check-ins." });
    } else if (id === "welcome_unlock") {
      tips.push({ level: "info", text: "Users complete check-ins but aren't being marked as eligible. Verify welcome_eligibility records are being created correctly for all users." });
    } else if (id === "welcome_claim") {
      tips.push({ level: "info", text: "Users are eligible but don't claim. A push notification or in-bot message when they become eligible can boost claim rates." });
      tips.push({ level: "info", text: "Check whether the claim flow has friction — too many steps or unclear CTAs reduce conversions." });
    } else if (id === "first_bet") {
      tips.push({ level: "info", text: "Users claim the voucher but don't bet. The voucher may not provide sufficient value to drive the first bet." });
      tips.push({ level: "info", text: "Increasing voucher value or simplifying the betting onboarding process can improve first bet conversion." });
    } else {
      tips.push({ level: "info", text: "High drop-off at " + stage.name + " (" + pct(stage.prev_stage_dropoff) + "). Review the user journey at this specific step for friction points." });
    }

    return tips;
  }

  // ---- Section 4: Welcome Voucher Effectiveness ----
  function renderWelcome(d) {
    var body = $("#welcome-body");
    var wv = d.welcome_voucher_effectiveness;
    var cohort = d.cohort_size || 0;

    if (!wv) {
      body.innerHTML = '<div class="empty">No welcome voucher data available.</div>';
      return;
    }

    var cp = wv.checkin_progress || {};
    var p1 = cp.at_1_of_3 || 0;
    var p2 = cp.at_2_of_3 || 0;
    var p3 = cp.at_3_of_3 || 0;

    function progBar(label, count) {
      var pctVal = cohort > 0 ? Math.round(100 * count / cohort) : 0;
      return '<div class="progress-bar-row">' +
        '<div class="pb-label">' + esc(label) + "</div>" +
        '<div class="pb-wrap"><div class="pb-fill" style="width:' + Math.min(100, pctVal) + '%"></div></div>' +
        '<div class="pb-val">' + fmt(count) + "</div>" +
        "</div>";
    }

    var kpiCards =
      '<div class="welcome-grid">' +
      kpiCard("Join → Unlock", pct(wv.join_to_unlock_rate), "Users who completed 3 check-ins") +
      kpiCard("Join → Claim", pct(wv.join_to_claim_rate), "Users who claimed the voucher") +
      kpiCard("Unlock → Claim", pct(wv.unlock_to_claim_rate), "Eligible users who claimed") +
      kpiCard("Unlocked", fmt(wv.unlock_count), "Users eligible to claim") +
      kpiCard("Claimed", fmt(wv.claim_count), "Users who successfully claimed") +
      "</div>";

    var progressSection =
      '<div class="wide-panel">' +
      '<div class="section-title" style="margin-bottom:16px;">Check-in Progress Funnel</div>' +
      '<div class="note" style="margin-bottom:12px;">How far users progress through the 3 required check-ins:</div>' +
      progBar("1/3 done", p1) +
      progBar("2/3 done", p2) +
      progBar("3/3 done", p3) +
      '<div class="note" style="margin-top:12px;">Based on distinct check-in days per user within the selected window.</div>' +
      "</div>";

    body.innerHTML = kpiCards + progressSection;
  }

  function kpiCard(label, value, sub) {
    return '<div class="kpi">' +
      '<div class="label">' + esc(label) + "</div>" +
      '<div class="value">' + esc(value) + "</div>" +
      (sub ? '<div class="sub">' + esc(sub) + "</div>" : "") +
      "</div>";
  }

  // ---- Section 5: New vs Returning Player ----
  function renderSplit(d) {
    var body = $("#split-body");
    var ps = d.player_split;
    var newStages = d.new_player_funnel || [];
    var retStages = d.returning_player_funnel || [];

    if (!ps) {
      body.innerHTML = '<div class="empty">Player split data not available.</div>';
      return;
    }

    var summaryHtml =
      '<div class="card-grid" style="margin-bottom:20px">' +
      kpiCard("New Players", fmt(ps.new_player_count), "Identified via is_new_player=1") +
      kpiCard("Returning Players", fmt(ps.returning_player_count), "Identified via is_new_player=0") +
      kpiCard("Unknown", fmt(ps.unknown_count), "No marketing data or ambiguous is_new_player") +
      "</div>" +
      (ps.note ? '<div class="note" style="margin-bottom:16px">' + esc(ps.note) + "</div>" : "");

    var newHtml = renderMiniSplitFunnel(newStages, "New Player", "new");
    var retHtml = renderMiniSplitFunnel(retStages, "Returning Player", "returning");

    var splitHtml = "";
    if (!newStages.length && !retStages.length) {
      splitHtml = '<div class="banner warn">Not enough identified users to show per-segment funnels. Upload weekly marketing data with coupon_code and is_new_player fields to enable this view.</div>';
    } else {
      splitHtml =
        '<div class="split-grid">' +
        '<div class="split-panel">' +
        '<div class="panel-title">New Player Funnel</div>' +
        newHtml +
        "</div>" +
        '<div class="split-panel returning">' +
        '<div class="panel-title" style="color:var(--accent-2)">Returning Player Funnel</div>' +
        retHtml +
        "</div>" +
        "</div>";
    }

    body.innerHTML = summaryHtml + splitHtml;
  }

  function renderMiniSplitFunnel(stages, label, kind) {
    if (!stages || !stages.length) {
      return '<div class="empty" style="padding:20px 0">No ' + esc(label) + " data available.</div>";
    }
    var topCount = stages[0] && stages[0].count ? stages[0].count : 0;
    var rows = stages.map(function (s) {
      var missing = s.count === null;
      var barPct = (!missing && topCount > 0) ? Math.max(2, Math.round(100 * s.count / topCount)) : 2;
      var color = missing ? "gray" : barColor(s.prev_stage_conversion);
      return '<div class="funnel-stage-row" style="gap:10px">' +
        '<div class="fs-label" style="width:130px;font-size:12px">' + esc(s.name) + "</div>" +
        '<div class="fs-bar-wrap" style="height:22px">' +
        '<div class="fs-bar ' + color + '" style="width:' + barPct + '%;font-size:10px">' +
        (barPct > 20 && !missing ? pct(s.conversion_from_top) : "") +
        "</div></div>" +
        '<div class="fs-count" style="width:60px;font-size:13px">' + (missing ? "—" : fmt(s.count)) + "</div>" +
        "</div>";
    }).join("");

    return rows;
  }

  // ---- Section 6: Retention ----
  function renderRetention(stages) {
    var body = $("#retention-body");
    var d7 = null, d30 = null;
    stages.forEach(function (s) {
      if (s.id === "d7_retention") d7 = s;
      if (s.id === "d30_retention") d30 = s;
    });

    function retCard(stage, dayLabel) {
      if (!stage) {
        return '<div class="ret-card">' +
          '<div class="ret-label">' + dayLabel + " Retention</div>" +
          '<div class="ret-rate muted">—</div>' +
          '<div class="ret-sub">No data available.</div>' +
          "</div>";
      }
      if (stage.count === null) {
        return '<div class="ret-card">' +
          '<div class="ret-label">' + dayLabel + " Retention</div>" +
          '<div class="ret-rate muted">—</div>' +
          '<div class="ret-sub">' + esc(stage.note || "Insufficient data.") + "</div>" +
          "</div>";
      }
      var rate = stage.retention_rate || stage.conversion_from_top || 0;
      var rateColor = rate >= 20 ? "green" : rate >= 10 ? "yellow" : "red";
      var eligible = stage.eligible_count || stage.count;
      return '<div class="ret-card">' +
        '<div class="ret-label">' + dayLabel + " Retention</div>" +
        '<div class="ret-rate ' + rateColor + '">' + pct(rate) + "</div>" +
        '<div class="ret-sub">' + fmt(stage.count) + " of " + fmt(eligible) + " eligible users retained</div>" +
        (stage.note ? '<div class="ret-sub" style="margin-top:6px">' + esc(stage.note) + "</div>" : "") +
        "</div>";
    }

    var html =
      '<div class="retention-grid">' +
      retCard(d7, "D7") +
      retCard(d30, "D30") +
      "</div>" +
      '<div class="wide-panel">' +
      '<div class="section-title" style="margin-bottom:12px;">Understanding Retention Metrics</div>' +
      '<ul class="insight-list">' +
      '<li class="info">D7 Retention: % of users who had activity exactly 7 days (±24h) after joining the channel.</li>' +
      '<li class="info">D30 Retention: % of users who had activity exactly 30 days (±24h) after joining.</li>' +
      '<li class="info">Activity signals: check-ins, voucher claims, miniapp sessions.</li>' +
      '<li class="info">Only users who joined ≥7/30 days ago are counted as "eligible". Recent joiners are excluded from the denominator.</li>' +
      '<li class="warn">Retention is computed as a point-in-time signal (activity on that specific day), not cumulative. For broader retention curves, see the Retention KPIs report.</li>' +
      "</ul></div>";

    body.innerHTML = html;
  }

  // ---- Refresh button ----
  $("#refresh-btn").addEventListener("click", function () {
    loadDashboard(true);
  });

  // ---- Admin chip ----
  function loadAdminChip() {
    api("/api/admin/auth/me")
      .then(function (d) {
        var a = d.admin || {};
        if (a.username || a.id) {
          $("#admin-chip").textContent = "@" + (a.username || a.id);
        }
      })
      .catch(function () {});
  }

  // ---- Init ----
  initSectionNav();
  initWindowSel();
  loadAdminChip();
  loadDashboard();

})();
