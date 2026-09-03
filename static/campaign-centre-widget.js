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

  /**
   * Mission Pool rewards are additive (§21): every Mission-specific key
   * (mechanic / is_winner / winner_popup_pending / notification_status) is
   * OPTIONAL. A legacy or tournament row that carries none of them renders
   * through exactly the same code path it always did — the only difference
   * for a Mission row is the label line and a stable element id used by the
   * highlight.
   */
  function isMissionReward(reward) {
    return reward && reward.category === "mission_pool";
  }

  function rewardDomId(reward) {
    return "cc-reward-" + String(reward.reward_id || "").replace(/[^A-Za-z0-9_-]/g, "");
  }

  function renderRewardCard(reward, root) {
    var mission = isMissionReward(reward);
    var card = el("div", { class: "cc-reward-card", id: rewardDomId(reward) });
    if (mission) {
      card.appendChild(el("div", { class: "cc-reward-tag", text: "🎯 Mission Winner" }));
      card.appendChild(el("div", { class: "cc-card-title", text: reward.campaign_name || reward.campaign_id || "" }));
      if (reward.reward_label) card.appendChild(el("div", { class: "cc-reward-meta", text: reward.reward_label }));
    } else {
      card.appendChild(el("div", { class: "cc-card-title", text: "🏆 " + (reward.campaign_name || reward.campaign_id || "") }));
      card.appendChild(el("div", { class: "cc-reward-meta", text: "Rank #" + reward.rank + " — " + (reward.reward_label || "") }));
    }
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

  // -------------------------------------------------------------------
  // Winner popup (§15-§18) and reward highlight (§20).
  //
  // The popup is driven ENTIRELY by winner_popup_pending on the reward row
  // that /api/campaign-rewards/me already returns, so it costs no extra
  // startup request (§22) and it can only ever be shown for a reward the
  // authenticated caller owns — the server filters that list by the verified
  // Telegram identity, and the acknowledgement endpoint re-checks ownership
  // before writing (§42).
  //
  // Campaign Rewards remains the canonical reward surface: the popup never
  // renders a voucher code and never holds reward state of its own (§16).
  // -------------------------------------------------------------------

  var HIGHLIGHT_MS = 3000;
  var popupShownThisSession = false;
  var lastRewards = [];

  function highlightRewardById(rewardId) {
    var node = document.getElementById("cc-reward-" + String(rewardId || "").replace(/[^A-Za-z0-9_-]/g, ""));
    if (!node) return false;
    try { node.scrollIntoView({ behavior: "smooth", block: "center" }); } catch (e) {}
    node.classList.add("cc-highlight");
    setTimeout(function () { node.classList.remove("cc-highlight"); }, HIGHLIGHT_MS);
    trackEvent("mission_reward_highlighted", { reward_id: rewardId });
    return true;
  }

  function highlightRewardForCampaign(campaignId) {
    for (var i = 0; i < lastRewards.length; i++) {
      if (lastRewards[i].campaign_id === campaignId) return highlightRewardById(lastRewards[i].reward_id);
    }
    var root = document.getElementById("campaign-centre-root");
    try { if (root && root.scrollIntoView) root.scrollIntoView({ behavior: "smooth", block: "start" }); } catch (e) {}
    return false;
  }

  function trackEvent(event, detail) {
    try {
      if (window.MissionPoolEvents && typeof window.MissionPoolEvents.push === "function") {
        window.MissionPoolEvents.push({ event: event, detail: detail || {}, at: Date.now() });
      }
      document.dispatchEvent(new CustomEvent("mission-pool-event", { detail: { event: event, detail: detail || {} } }));
    } catch (e) {}
  }

  /**
   * Acknowledge, then act. Both "Redeem Now" and "Later"/close acknowledge,
   * so a winner is never congratulated again on every app open (§17).
   *
   * If the acknowledgement request fails the popup still closes and the
   * voucher stays fully usable — the reward is never hidden or revoked, and
   * popupShownThisSession stops it reopening in this session (§17).
   */
  function acknowledgeAndClose(reward, backdrop, thenHighlight) {
    try { if (backdrop && backdrop.parentNode) backdrop.parentNode.removeChild(backdrop); } catch (e) {}
    apiPost("/api/campaign-rewards/" + encodeURIComponent(reward.reward_id) + "/ack-popup");
    trackEvent("mission_winner_popup_acknowledged", { reward_id: reward.reward_id, campaign_id: reward.campaign_id });
    if (thenHighlight) highlightRewardById(reward.reward_id);
  }

  function showWinnerPopup(reward) {
    var backdrop = el("div", { class: "cc-modal-backdrop", id: "cc-winner-popup" });
    var modal = el("div", { class: "cc-modal", role: "dialog", "aria-modal": "true" });
    modal.appendChild(el("div", { class: "cc-modal-title", text: "🎉 Congratulations!" }));
    modal.appendChild(el("div", {
      class: "cc-modal-body",
      text: "You've been selected as a winner of\n" + (reward.campaign_name || "") + "!\n\nRedeem your code now.",
    }));
    var actions = el("div", { class: "cc-modal-actions" });
    var redeem = el("button", { class: "cc-btn cc-btn-primary", type: "button", text: "🎁 Redeem Now" });
    redeem.addEventListener("click", function () { acknowledgeAndClose(reward, backdrop, true); });
    var later = el("button", { class: "cc-btn cc-btn-secondary", type: "button", text: "Later" });
    later.addEventListener("click", function () { acknowledgeAndClose(reward, backdrop, false); });
    actions.appendChild(redeem);
    actions.appendChild(later);
    modal.appendChild(actions);
    backdrop.appendChild(modal);
    document.body.appendChild(backdrop);
    popupShownThisSession = true;
    trackEvent("mission_winner_popup_shown", { reward_id: reward.reward_id, campaign_id: reward.campaign_id });
  }

  /**
   * §18: never stack modals. Exactly ONE popup per Mini App session, for the
   * OLDEST pending Mission win; any other pending win simply shows again on
   * the next open, still unacknowledged, still redeemable. Chosen over a
   * queue because it needs no new state machine in a widget that has none.
   */
  function maybeShowWinnerPopup(rewards) {
    if (popupShownThisSession) return null;
    var pending = rewards.filter(function (r) {
      return isMissionReward(r) && r.winner_popup_pending === true;
    });
    if (!pending.length) return null;
    pending.sort(function (a, b) {
      return String(a.assigned_at || "").localeCompare(String(b.assigned_at || ""));
    });
    showWinnerPopup(pending[0]);
    return pending[0];
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
      ".cc-reward-tag{display:inline-block;font-size:11px;font-weight:700;letter-spacing:.02em;opacity:.9;margin-bottom:4px;}",
      // Highlight is PRESENTATIONAL ONLY and self-clearing (§20): a class is
      // added on arrival and removed after the animation, so no reward card
      // is ever left permanently restyled.
      ".cc-reward-card.cc-highlight{border-color:#ff8a3d;animation:cc-pulse 1s ease-in-out 3;}",
      "@keyframes cc-pulse{0%,100%{box-shadow:0 0 0 0 rgba(255,138,61,0);}50%{box-shadow:0 0 0 4px rgba(255,138,61,.35);}}",
      ".cc-modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.6);display:flex;align-items:center;justify-content:center;padding:20px;z-index:9999;}",
      ".cc-modal{background:#1b1b1f;border:1px solid rgba(255,255,255,.12);border-radius:16px;padding:20px;max-width:320px;width:100%;text-align:center;color:#fff;}",
      ".cc-modal-title{font-size:18px;font-weight:800;margin-bottom:8px;}",
      ".cc-modal-body{font-size:14px;line-height:1.5;margin-bottom:16px;white-space:pre-line;}",
      ".cc-modal-actions{display:flex;flex-direction:column;gap:8px;}",
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
        lastRewards = rewards;
        maybeShowWinnerPopup(rewards);
      }

      if (campaigns.length) {
        var campaignsSection = el("div", { class: "cc-section" });
        campaignsSection.appendChild(el("div", { class: "cc-section-title", text: "Campaigns" }));
        campaigns.forEach(function (c) { renderCampaignCard(c, campaignsSection); });
        root.appendChild(campaignsSection);
      }
    });
  }

  // Small, documented surface the Mission widget hands off to (§16): the
  // Campaign Rewards section keeps sole ownership of reward rendering.
  window.CampaignCentreWidget = {
    mount: mount,
    highlightRewardById: highlightRewardById,
    highlightRewardForCampaign: highlightRewardForCampaign,
    maybeShowWinnerPopup: maybeShowWinnerPopup,
    renderRewardCard: renderRewardCard,
    isMissionReward: isMissionReward,
    rewardDomId: rewardDomId,
    _setRewards: function (r) { lastRewards = r || []; },
    _resetPopupSession: function () { popupShownThisSession = false; },
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mount);
  } else {
    mount();
  }
}());
