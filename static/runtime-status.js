/* APReferral Runtime Status Dashboard
   Standalone JS — talks to /api/admin/dashboard/runtime-status
   Read-only: reports live runtime state only, never configuration. */
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

  function renderTable(tbodySel, rows, cols) {
    var tbody = $(tbodySel);
    if (!rows || !rows.length) {
      tbody.innerHTML = '<tr><td colspan="' + cols.length + '">No data</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(function (row) {
      return "<tr>" + cols.map(function (c) {
        var val = typeof c.render === "function" ? c.render(row) : fmt(row[c.key]);
        return "<td>" + val + "</td>";
      }).join("") + "</tr>";
    }).join("");
  }

  function summaryCard(label, value, cls) {
    return '<div class="rs-summary-card"><div class="label">' + esc(label) + '</div>' +
      '<div class="value' + (cls ? " " + cls : "") + '">' + esc(value) + "</div></div>";
  }

  function statusPill(status) {
    return '<span class="rs-status-pill">' + esc(status || "—") + "</span>";
  }

  function countByStatus(rows, key) {
    var counts = {};
    (rows || []).forEach(function (r) {
      var s = r[key || "status"] || "unknown";
      counts[s] = (counts[s] || 0) + 1;
    });
    return counts;
  }

  function renderOverview(d) {
    var counts = countByStatus(d.features);
    var summaryHtml = Object.keys(counts).map(function (s) {
      return summaryCard(s, counts[s]);
    }).join("");
    $("#overview-summary").innerHTML = summaryHtml;

    renderTable("#overview-table tbody", d.features, [
      { key: "feature" },
      { render: function (r) { return statusPill(r.status); } },
      { key: "trigger" },
      { render: function (r) { return fmtTime(r.last_run); } },
      { render: function (r) { return fmtTime(r.next_run); } },
      { render: function (r) { return fmtTime(r.last_success); } },
      { render: function (r) { return '<span class="rs-note">' + esc(r.notes || "") + "</span>"; } },
    ]);
  }

  function renderScheduler(d) {
    renderTable("#scheduler-table tbody", d.scheduler, [
      { key: "job_name" },
      { key: "cron" },
      { render: function (r) { return fmt(r.enabled); } },
      { render: function (r) { return r.worker_only ? "Yes" : "No"; } },
      { render: function (r) { return fmtTime(r.last_run); } },
      { render: function (r) { return statusPill(r.status); } },
      { render: function (r) { return '<span class="rs-note">' + esc(r.notes || "") + "</span>"; } },
    ]);
  }

  function fmtAge(seconds) {
    if (seconds === null || seconds === undefined) return "—";
    var s = Math.max(0, Math.floor(seconds));
    if (s < 60) return s + "s ago";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    if (s < 86400) return Math.floor(s / 3600) + "h ago";
    return Math.floor(s / 86400) + "d ago";
  }

  function fmtSkipBreakdown(breakdown) {
    if (!breakdown || typeof breakdown !== "object") return "—";
    var parts = Object.keys(breakdown)
      .filter(function (k) { return breakdown[k]; })
      .sort(function (a, b) { return breakdown[b] - breakdown[a]; })
      .map(function (k) { return esc(k) + ": " + breakdown[k]; });
    return parts.length ? parts.join(", ") : "none today";
  }

  function renderPm(d) {
    renderTable("#pm-table tbody", d.pm_automation, [
      { key: "name" },
      { render: function (r) { return fmt(r.enabled); } },
      { render: function (r) { return fmtTime(r.last_sent); } },
      { render: function (r) { return fmt(r.sent_today); } },
      { render: function (r) { return fmt(r.failed_today); } },
      { render: function (r) { return fmt(r.skipped_today); } },
      { render: function (r) { return fmtAge(r.last_run_age_s); } },
      { render: function (r) { return '<span class="rs-note">' + fmtSkipBreakdown(r.skip_breakdown) + "</span>"; } },
      { render: function (r) { return fmt(r.queue_size); } },
      { key: "trigger" },
      { render: function (r) { return statusPill(r.status); } },
      { render: function (r) { return '<span class="rs-note">' + esc(r.notes || "") + "</span>"; } },
    ]);
  }

  function renderQueues(d) {
    var html = (d.queues || []).map(function (q) {
      var cls = q.size ? "" : "ok";
      return summaryCard(q.name, q.size === null || q.size === undefined ? "—" : q.size, cls) ;
    }).join("");
    $("#queues-summary").innerHTML = html;
  }

  function renderWorker(d) {
    var w = d.worker_health || {};
    var html =
      summaryCard("Worker Running", w.worker_running ? "Yes" : "No", w.worker_running ? "ok" : "bad") +
      summaryCard("Scheduler Running", w.scheduler_running ? "Yes" : "No", w.scheduler_running ? "ok" : "bad") +
      summaryCard("Mongo Connected", w.mongo_connected ? "Yes" : "No", w.mongo_connected ? "ok" : "bad") +
      summaryCard("Telegram Connected", w.telegram_connected ? "Yes" : "No", w.telegram_connected ? "ok" : "bad") +
      summaryCard("Snapshot Freshness", w.snapshot_freshness_seconds === null || w.snapshot_freshness_seconds === undefined ? "—" : Math.round(w.snapshot_freshness_seconds) + "s") +
      summaryCard("Deployment Version", w.deployment_version || "—") +
      summaryCard("Git Commit", (w.git_commit || "—").toString().slice(0, 12)) +
      summaryCard("Last Heartbeat", fmtTime(w.last_heartbeat));
    $("#worker-summary").innerHTML = html;
  }

  function loadDashboard(forceRefresh) {
    $("#dashboard-meta").textContent = "Loading…";
    api("/api/admin/dashboard/runtime-status" + (forceRefresh ? "?refresh=1" : ""))
      .then(function (d) {
        $("#dashboard-meta").textContent = "Generated at " + fmtTime(d.generated_at);
        $("#global-banner").innerHTML = "";
        renderOverview(d);
        renderScheduler(d);
        renderPm(d);
        renderQueues(d);
        renderWorker(d);
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
