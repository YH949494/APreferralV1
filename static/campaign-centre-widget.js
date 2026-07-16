/**
 * Campaign Centre + Campaign Rewards — Mini App widget.
 *
 * Self-contained and defensive: it never throws into the host page, and it
 * hides itself completely when there is nothing to show (no "Coming Soon"
 * placeholders, no empty Campaign Rewards section). Server-side visibility
 * (/api/campaigns/active) is the only source of truth for what's shown.
 */
(function () {
  "use strict";

  function getInitData() {
    try {
      var tg = window.Telegram && window.Telegram.WebApp;
      if (tg && typeof tg.initData === "string" && tg.initData.length) return tg.initData;
    } catch (e) {}
    try {
      var qs = new URLSearchParams(location.search);
      return qs.get("init_data") || "";
    } catch (e) {
      return "";
    }
  }

  function withInitData(path) {
    var initData = getInitData();
    if (!initData) return path;
    var sep = path.indexOf("?") === -1 ? "?" : "&";
    return path + sep + "init_data=" + encodeURIComponent(initData);
  }

  function apiGet(path) {
    return fetch(withInitData(path), { credentials: "same-origin" })
      .then(function (r) { return r.json(); })
      .catch(function () { return null; });
  }

  function apiPost(path, body) {
    return fetch(withInitData(path), {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) { return r.json(); }).catch(function () { return null; });
  }

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

  function renderCampaignCard(campaign, root) {
    var card = el("div", { class: "cc-card" });
    card.appendChild(el("div", { class: "cc-card-title", text: campaign.name || "" }));
    if (campaign.description) card.appendChild(el("div", { class: "cc-card-desc", text: campaign.description }));

    var actions = el("div", { class: "cc-card-actions" });
    var telegramCfg = campaign.telegram || {};

    if (telegramCfg.require_subscription && telegramCfg.channel_username) {
      var subBtn = el("button", { class: "cc-btn cc-btn-secondary", text: "Subscribe Channel" });
      subBtn.addEventListener("click", function () {
        window.open("https://t.me/" + telegramCfg.channel_username, "_blank");
      });
      actions.appendChild(subBtn);
    }

    var playBtn = el("button", { class: "cc-btn cc-btn-primary", text: campaign.button_text || "Open" });
    var statusMsg = el("div", { class: "cc-status-msg", style: "display:none;" });
    playBtn.addEventListener("click", function () {
      statusMsg.style.display = "none";
      apiPost("/api/campaigns/" + encodeURIComponent(campaign.campaign_id) + "/play").then(function (resp) {
        if (!resp) return;
        if (resp.status === "ok" && resp.url) {
          try {
            if (resp.open_mode === "telegram_web_app" && window.Telegram && window.Telegram.WebApp && window.Telegram.WebApp.openLink) {
              window.Telegram.WebApp.openLink(resp.url);
            } else {
              window.open(resp.url, "_blank");
            }
          } catch (e) {
            window.open(resp.url, "_blank");
          }
          return;
        }
        if (resp.code === "subscription_required") {
          statusMsg.textContent = "Please subscribe to the official channel, then tap Play again.";
          statusMsg.style.display = "block";
          return;
        }
        statusMsg.textContent = "This campaign is currently unavailable.";
        statusMsg.style.display = "block";
      });
    });
    actions.appendChild(playBtn);

    card.appendChild(actions);
    card.appendChild(statusMsg);
    root.appendChild(card);
  }

  function renderRewardCard(reward, root) {
    var card = el("div", { class: "cc-reward-card" });
    card.appendChild(el("div", { class: "cc-card-title", text: "🏆 " + (reward.campaign_name || reward.campaign_id || "") }));
    card.appendChild(el("div", { class: "cc-reward-meta", text: "Rank #" + reward.rank + " — " + (reward.reward_label || "") }));
    var codeRow = el("div", { class: "cc-reward-code" });
    codeRow.appendChild(el("span", { text: reward.voucher_code || "" }));
    var copyBtn = el("button", { class: "cc-btn cc-btn-secondary cc-btn-sm", text: "Copy Code" });
    copyBtn.addEventListener("click", function () {
      try { navigator.clipboard.writeText(reward.voucher_code || ""); } catch (e) {}
      apiPost("/api/campaign-rewards/" + encodeURIComponent(reward.reward_id) + "/copy");
      copyBtn.textContent = "Copied!";
      setTimeout(function () { copyBtn.textContent = "Copy Code"; }, 1500);
    });
    codeRow.appendChild(copyBtn);
    card.appendChild(codeRow);
    root.appendChild(card);
    apiPost("/api/campaign-rewards/" + encodeURIComponent(reward.reward_id) + "/view");
  }

  function injectStyles() {
    var style = document.createElement("style");
    style.textContent = [
      "#campaign-centre-root{margin:0 0 16px;}",
      ".cc-section{margin-bottom:14px;}",
      ".cc-section-title{font-weight:700;font-size:14px;margin-bottom:8px;opacity:.85;}",
      ".cc-card,.cc-reward-card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:12px 14px;margin-bottom:10px;}",
      ".cc-card-title{font-weight:700;font-size:15px;margin-bottom:4px;}",
      ".cc-card-desc{font-size:13px;opacity:.8;margin-bottom:10px;}",
      ".cc-card-actions{display:flex;gap:8px;flex-wrap:wrap;}",
      ".cc-btn{border:none;border-radius:8px;padding:8px 14px;font-size:13px;font-weight:600;cursor:pointer;}",
      ".cc-btn-primary{background:linear-gradient(90deg,#ff8a3d,#f5b63f);color:#1a1200;}",
      ".cc-btn-secondary{background:rgba(255,255,255,.08);color:inherit;}",
      ".cc-btn-sm{padding:5px 10px;font-size:12px;}",
      ".cc-status-msg{margin-top:8px;font-size:12px;color:#f5b63f;}",
      ".cc-reward-meta{font-size:13px;opacity:.8;margin-bottom:8px;}",
      ".cc-reward-code{display:flex;align-items:center;justify-content:space-between;background:rgba(0,0,0,.25);border-radius:8px;padding:8px 10px;font-family:monospace;font-size:14px;}",
    ].join("\n");
    document.head.appendChild(style);
  }

  function mount() {
    var root = document.getElementById("campaign-centre-root");
    if (!root) return;

    Promise.all([
      apiGet("/api/campaigns/active"),
      apiGet("/api/campaign-rewards/me"),
    ]).then(function (results) {
      var campaignsResp = results[0];
      var rewardsResp = results[1];

      var campaigns = (campaignsResp && campaignsResp.status === "ok" && campaignsResp.campaigns) || [];
      var rewards = (rewardsResp && rewardsResp.status === "ok" && rewardsResp.rewards) || [];

      if (!campaigns.length && !rewards.length) return; // hide section entirely

      injectStyles();

      if (rewards.length) {
        var rewardsSection = el("div", { class: "cc-section" });
        rewardsSection.appendChild(el("div", { class: "cc-section-title", text: "Campaign Rewards" }));
        rewards.forEach(function (r) { renderRewardCard(r, rewardsSection); });
        root.appendChild(rewardsSection);
      }

      if (campaigns.length) {
        var campaignsSection = el("div", { class: "cc-section" });
        campaignsSection.appendChild(el("div", { class: "cc-section-title", text: "Campaigns" }));
        campaigns.forEach(function (c) { renderCampaignCard(c, campaignsSection); });
        root.appendChild(campaignsSection);
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mount);
  } else {
    mount();
  }
}());
