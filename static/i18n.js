/**
 * APReferral Mini App — Internationalisation (Phase 1)
 * Supports: English (en), Thai (th), Indonesian (id)
 * Priority: localStorage("ap_language") > Telegram language_code > en
 */
(function () {
  "use strict";

  /* ------------------------------------------------------------------ */
  /* TRANSLATION DICTIONARY                                               */
  /* ------------------------------------------------------------------ */
  const I18N = {
    en: {
      /* Header / Region overlay */
      page_title: "AdvantPlay Rewards",
      region_welcome: "Welcome to AdvantPlay",
      region_sub: "Choose your region to continue. You won't be asked again.",
      region_foot: "Your region tailors vouchers & events. Asked once.",
      region_select_btn: "Select a region",
      region_malaysia: "Malaysia",
      region_thailand: "Thailand",
      region_indonesia: "Indonesia",
      region_other: "Other region",

      /* Hero / Voucher drops */
      live_drop: "LIVE DROP",
      limited_drop: "LIMITED DROP",
      vouchers_remaining: "Vouchers remaining",
      no_active_drop: "No active drop right now",
      no_active_drop_sub: "Subscribe to our official channel to be the first to know when the next drop goes live.",
      join_channel: "Join Channel",
      campaign_reward: "🎉 Campaign Reward",
      coming_soon: "Coming soon…",
      copy_to_clipboard: "📋 Copy to clipboard",
      affiliate_rewards: "🎁 Your Affiliate Rewards",

      /* Identity modal */
      close: "Close",
      share_my_rank: "📣 Share My Rank",
      weekly_rank_not_ranked: "Weekly Rank: Not ranked yet",

      /* Journey card */
      your_rewards_journey: "Your rewards journey",
      check_in_today: "Check in today",
      xp_streak_sub: "+20 XP · keeps your streak alive",
      check_in: "Check in",
      checked_in: "Checked in",
      invite_friends: "Invite 3 friends",
      unlock_xp: "unlock +400 XP",
      get_link: "Get link",
      copy: "📋 Copy",
      share: "📤 Share",
      claim_a_live_drop: "Claim a live drop",
      campaign_reward_top: "Campaign reward at the top ↑",

      /* Action tiles */
      lucky_game: "Lucky Game",
      loading_today: "Loading today's pick…",
      invite_earn: "Invite & earn",
      xp_per_friend: "+60 XP per friend",

      /* Chips */
      leaderboard_chip: "Leaderboard",
      channel_chip: "Channel",
      info_hub_chip: "Info Hub",

      /* Leaderboard section */
      weekly_leaderboard: "🏆 Weekly Leaderboard",
      view_leaderboard: "View Leaderboard",
      refresh_cadence: "🔄 Refresh every 5 minutes",
      refreshing: "Refreshing…",
      leaderboard_tab_default: "XP + Ref",
      leaderboard_tab_affiliate: "Affiliate",
      click_to_load: "Click to load latest rankings...",
      click_to_load_affiliate: "Click to load latest affiliate rankings...",
      retry: "Retry",
      loading_weekly_stats: "⏳ Loading weekly stats…",
      xp_leaderboard: "⭐ XP Leaderboard",
      referral_leaderboard: "🔗 Referral Leaderboard",
      login_to_see_progress: "Login to see your progress.",
      login_affiliate_stats: "Login to see your affiliate stats.",
      no_affiliate_data: "No affiliate leaderboard data yet.",
      no_data: "No data yet.",
      no_additional_ranks: "No additional ranks yet.",
      show_more: "Show More",
      view_less: "View Less",
      xp_champion: "XP Champion",
      referral_champion: "Referral Champion",
      elite: "Elite",
      earn_xp_nudge: "Earn XP via daily check-in and community actions.",
      invite_friends_nudge: "Invite friends to climb the referral ranks.",

      /* Welcome voucher progress */
      welcome_voucher_progress: "🎁 Welcome Voucher Progress",
      welcome_checkin_done: "✅ Check in 3/3",
      welcome_subscribe: "❌ Subscribe to @AdvantPlayOfficial",
      join_channel_to_unlock: "Join the channel to unlock your Welcome Voucher.",
      verify_subscription: "Verify Subscription",
      claim_welcome_voucher: "Claim Welcome Voucher",
      welcome_voucher_waiting: "🎁 Welcome Voucher Waiting",
      welcome_voucher: "🎁 Welcome Voucher",
      voucher_unlocked: "🎉 Voucher Unlocked",
      voucher_ready: "Your Welcome Voucher is ready.",
      voucher_temporarily_unavailable: "You've completed all requirements. Voucher distribution is temporarily unavailable. Please check again later.",
      one_more_day: "1 more day to go.",

      /* Info Hub */
      info_hub_title: "ℹ️ Info Hub",
      how_it_works_tab: "🎯 How It Works",
      xp_tab: "⭐ XP",
      vip_tab: "👑 VIP",
      events_tab: "🎉 Events",
      info_hub_how_works: "How AdvantPlay Works",
      info_hub_step1: "1️⃣ Check-in daily → Earn XP",
      info_hub_step2: "2️⃣ Invite friends → Earn bonus XP",
      info_hub_step3: "3️⃣ Join weekly events → Win vouchers",
      info_hub_step4: "4️⃣ Weekly Top 10 → Extra rewards",
      more_activity: "More activity = More rewards.",
      xp_top10: "Weekly Top 10 XP = Extra Voucher Code",
      how_to_earn_xp: "How to Earn XP",
      daily_checkin_label: "✅ Daily Check-in",
      streak_bonus: "🔥 Streak Bonus",
      vip_system: "VIP System (Monthly Reset)",
      vip_crown: "👑 Crown = VIP1",
      vip_800: "800 Monthly XP → VIP1",
      vip_below: "Below 800 at refresh → Normal",
      vip_unlocks: "VIP1 unlocks Bonus Voucher",
      vip_limited: "VIP vouchers limited • first come first served",
      vip_maintain: "Maintain XP to keep VIP status.",
      event_highlights: "Event Highlights",
      event1: "🎁 Surprise Voucher Drops",
      event2: "🎟️ Weekly Lucky Draw",
      event3: "🎄 #ComebackIsReal",
      campaign_link: "Campaign link",
      info_hub_close: "Close",

      /* Dynamic JS strings */
      loading: "Loading...",
      getting: "Getting…",
      preparing: "Preparing…",
      claiming: "Claiming…",
      saving: "Saving…",
      ending: "Ending…",
      copied: "✅ Copied",
      copied_simple: "Copied!",
      fully_redeemed: "Fully redeemed",
      fully_claimed: "Fully Claimed",
      claim_reward: "Claim Reward",
      claim_now_label: "Claim now",
      view_reward: "View Reward",
      stay_subscribed: "Stay subscribed to channel — Next drop coming soon.",
      claim_your_reward: "Tap below to claim your reward.",
      no_code_available: "No code available",
      new_member_reward: "🎁 New Member Reward",
      unlock_after_checkins: "Unlock your reward after 3 check-ins.",

      /* Referral */
      referral_progress: "Referral Progress",
      open_via_telegram: "Please open this mini app from Telegram to view your referral progress.",
      referral_load_error: "Could not load referral progress right now. Please try again later.",
      referral_empty: "Your referrals will appear here after someone joins using your invite link.",
      near_miss_referral: "⚡ 1 more referral to unlock +400 XP",
      referral_target_done: "✅ Current referral target completed. Keep sharing for your next reward tier.",
      link_expires: "⏳ This link expires in 24h",
      qualified: "Qualified",
      checking: "Checking",
      not_eligible: "Not eligible",
      n_more_referrals: "{n} more referrals to unlock +400 XP",

      /* Progress counter */
      n_of_3_done: "{n} of 3 done",
      progress_label: "Progress: {completed}/{required} ✅",
      complete_n_more: "Complete {n} more check-ins to unlock your voucher.",

      /* Error / claim states */
      temporarily_blocked: "Temporarily blocked",
      please_try_later: "Please try later.",
      too_fast: "Too fast",
      profile_photo_required: "Profile photo required",
      set_profile_photo: "Set a Telegram profile photo, then retry.",
      subscribe_required: "Subscribe required",
      join_official_channel: "Join the official channel to claim this pool drop.",
      verification_busy: "Verification busy",
      cant_verify: "Can't verify subscription now. Try again shortly.",
      temporarily_unavailable: "Temporarily unavailable",
      please_try_again: "Please try again.",
      join_official_channel_btn: "Join official channel",
      set_profile_photo_btn: "Set profile photo",
      you_can_retry: "You can retry now.",
      retry_available_in: "Retry available in {n}s",
      copy_failed: "❌ Copy failed. Please copy manually.",
      could_not_get_link: "❌ Could not get your referral link. Please try again.",
      claim_not_available: "❌ Claim slot is not available yet. Please check again later.",
      please_generate_link: "Please generate your referral link first.",
      failed_to_copy_voucher: "Failed to copy voucher.",
      no_voucher_to_copy: "❌ No voucher code available to copy yet.",
      no_code_to_copy: "No code available to copy yet. Please claim first.",
      unable_to_copy: "Unable to copy automatically. Please copy the code manually: ",

      /* XP toast */
      check_in_reward: "Check-in reward",

      /* Leaderboard empty/unavailable */
      affiliate_leaderboard_unavailable: "No affiliate leaderboard data yet.",
    },

    th: {
      page_title: "AdvantPlay Rewards",
      region_welcome: "ยินดีต้อนรับสู่ AdvantPlay",
      region_sub: "เลือกภูมิภาคของคุณเพื่อดำเนินการต่อ ระบบจะถามเพียงครั้งเดียว",
      region_foot: "ภูมิภาคของคุณจะปรับแต่งคูปองและอีเวนต์ ถามเพียงครั้งเดียว",
      region_select_btn: "เลือกภูมิภาค",
      region_malaysia: "มาเลเซีย",
      region_thailand: "ไทย",
      region_indonesia: "อินโดนีเซีย",
      region_other: "ภูมิภาคอื่น",

      live_drop: "LIVE DROP",
      limited_drop: "LIMITED DROP",
      vouchers_remaining: "คูปองที่เหลือ",
      no_active_drop: "ยังไม่มีดรอปที่ใช้งานอยู่",
      no_active_drop_sub: "ติดตามช่องทางการของเราเพื่อรับรู้ก่อนใครเมื่อดรอปครั้งต่อไปเริ่มขึ้น",
      join_channel: "เข้าร่วมช่อง",
      campaign_reward: "🎉 รางวัลแคมเปญ",
      coming_soon: "เร็วๆ นี้…",
      copy_to_clipboard: "📋 คัดลอกไปยังคลิปบอร์ด",
      affiliate_rewards: "🎁 รางวัลพาร์ทเนอร์ของคุณ",

      close: "ปิด",
      share_my_rank: "📣 แชร์อันดับของฉัน",
      weekly_rank_not_ranked: "อันดับประจำสัปดาห์: ยังไม่มีอันดับ",

      your_rewards_journey: "เส้นทางรางวัลของคุณ",
      check_in_today: "เช็คอินวันนี้",
      xp_streak_sub: "+20 XP · รักษาสตรีคของคุณ",
      check_in: "เช็คอิน",
      checked_in: "เช็คอินแล้ว",
      invite_friends: "ชวน 3 เพื่อน",
      unlock_xp: "ปลดล็อค +400 XP",
      get_link: "รับลิงก์",
      copy: "📋 คัดลอก",
      share: "📤 แชร์",
      claim_a_live_drop: "รับดรอปที่กำลังใช้งาน",
      campaign_reward_top: "รางวัลแคมเปญอยู่ด้านบน ↑",

      lucky_game: "เกมโชคลาภ",
      loading_today: "กำลังโหลดวันนี้…",
      invite_earn: "ชวนเพื่อน & รับรางวัล",
      xp_per_friend: "+60 XP ต่อเพื่อน",

      leaderboard_chip: "ลีดเดอร์บอร์ด",
      channel_chip: "ช่อง",
      info_hub_chip: "คลังข้อมูล",

      weekly_leaderboard: "🏆 ลีดเดอร์บอร์ดประจำสัปดาห์",
      view_leaderboard: "ดูลีดเดอร์บอร์ด",
      refresh_cadence: "🔄 รีเฟรชทุก 5 นาที",
      refreshing: "กำลังรีเฟรช…",
      leaderboard_tab_default: "XP + Ref",
      leaderboard_tab_affiliate: "พาร์ทเนอร์",
      click_to_load: "คลิกเพื่อโหลดอันดับล่าสุด...",
      click_to_load_affiliate: "คลิกเพื่อโหลดอันดับพาร์ทเนอร์ล่าสุด...",
      retry: "ลองอีกครั้ง",
      loading_weekly_stats: "⏳ กำลังโหลดสถิติประจำสัปดาห์…",
      xp_leaderboard: "⭐ ลีดเดอร์บอร์ด XP",
      referral_leaderboard: "🔗 ลีดเดอร์บอร์ดการชวน",
      login_to_see_progress: "เข้าสู่ระบบเพื่อดูความคืบหน้าของคุณ",
      login_affiliate_stats: "เข้าสู่ระบบเพื่อดูสถิติพาร์ทเนอร์ของคุณ",
      no_affiliate_data: "ยังไม่มีข้อมูลลีดเดอร์บอร์ดพาร์ทเนอร์",
      no_data: "ยังไม่มีข้อมูล",
      no_additional_ranks: "ยังไม่มีอันดับเพิ่มเติม",
      show_more: "ดูเพิ่มเติม",
      view_less: "ดูน้อยลง",
      xp_champion: "แชมป์ XP",
      referral_champion: "แชมป์การชวน",
      elite: "Elite",
      earn_xp_nudge: "รับ XP ผ่านการเช็คอินประจำวันและกิจกรรมชุมชน",
      invite_friends_nudge: "ชวนเพื่อนเพื่อขึ้นลีดเดอร์บอร์ดการชวน",

      welcome_voucher_progress: "🎁 ความคืบหน้าคูปองต้อนรับ",
      welcome_checkin_done: "✅ เช็คอิน 3/3",
      welcome_subscribe: "❌ ติดตาม @AdvantPlayOfficial",
      join_channel_to_unlock: "เข้าร่วมช่องเพื่อปลดล็อคคูปองต้อนรับของคุณ",
      verify_subscription: "ยืนยันการติดตาม",
      claim_welcome_voucher: "รับคูปองต้อนรับ",
      welcome_voucher_waiting: "🎁 กำลังรอคูปองต้อนรับ",
      welcome_voucher: "🎁 คูปองต้อนรับ",
      voucher_unlocked: "🎉 ปลดล็อคคูปองแล้ว",
      voucher_ready: "คูปองต้อนรับของคุณพร้อมแล้ว",
      voucher_temporarily_unavailable: "คุณได้ทำตามเงื่อนไขครบแล้ว การแจกคูปองหยุดให้บริการชั่วคราว กรุณาตรวจสอบอีกครั้งในภายหลัง",
      one_more_day: "อีก 1 วัน",

      info_hub_title: "ℹ️ คลังข้อมูล",
      how_it_works_tab: "🎯 วิธีการทำงาน",
      xp_tab: "⭐ XP",
      vip_tab: "👑 VIP",
      events_tab: "🎉 อีเวนต์",
      info_hub_how_works: "AdvantPlay ทำงานอย่างไร",
      info_hub_step1: "1️⃣ เช็คอินทุกวัน → รับ XP",
      info_hub_step2: "2️⃣ ชวนเพื่อน → รับ XP โบนัส",
      info_hub_step3: "3️⃣ เข้าร่วมอีเวนต์รายสัปดาห์ → รับคูปอง",
      info_hub_step4: "4️⃣ Top 10 ประจำสัปดาห์ → รางวัลพิเศษ",
      more_activity: "ยิ่งทำกิจกรรมมาก = รางวัลยิ่งมาก",
      xp_top10: "XP Top 10 ประจำสัปดาห์ = โค้ดคูปองพิเศษ",
      how_to_earn_xp: "วิธีรับ XP",
      daily_checkin_label: "✅ เช็คอินประจำวัน",
      streak_bonus: "🔥 โบนัสสตรีค",
      vip_system: "ระบบ VIP (รีเซ็ตรายเดือน)",
      vip_crown: "👑 มงกุฎ = VIP1",
      vip_800: "800 XP รายเดือน → VIP1",
      vip_below: "ต่ำกว่า 800 เมื่อรีเฟรช → Normal",
      vip_unlocks: "VIP1 ปลดล็อคคูปองโบนัส",
      vip_limited: "คูปอง VIP จำกัด • มาก่อนได้ก่อน",
      vip_maintain: "รักษา XP เพื่อคงสถานะ VIP ไว้",
      event_highlights: "ไฮไลท์อีเวนต์",
      event1: "🎁 Surprise Voucher Drops",
      event2: "🎟️ Lucky Draw รายสัปดาห์",
      event3: "🎄 #ComebackIsReal",
      campaign_link: "ลิงก์แคมเปญ",
      info_hub_close: "ปิด",

      loading: "กำลังโหลด...",
      getting: "กำลังดึงข้อมูล…",
      preparing: "กำลังเตรียม…",
      claiming: "กำลังรับ…",
      saving: "กำลังบันทึก…",
      ending: "กำลังสิ้นสุด…",
      copied: "✅ คัดลอกแล้ว",
      copied_simple: "คัดลอกแล้ว!",
      fully_redeemed: "ใช้หมดแล้ว",
      fully_claimed: "รับไปหมดแล้ว",
      claim_reward: "รับรางวัล",
      claim_now_label: "รับเลย",
      view_reward: "ดูรางวัล",
      stay_subscribed: "ติดตามช่องไว้ — ดรอปครั้งต่อไปกำลังจะมาเร็วๆ นี้",
      claim_your_reward: "แตะด้านล่างเพื่อรับรางวัลของคุณ",
      no_code_available: "ยังไม่มีโค้ด",
      new_member_reward: "🎁 รางวัลสมาชิกใหม่",
      unlock_after_checkins: "ปลดล็อครางวัลของคุณหลังจากเช็คอิน 3 ครั้ง",

      referral_progress: "ความคืบหน้าการชวน",
      open_via_telegram: "กรุณาเปิดมินิแอปนี้จาก Telegram เพื่อดูความคืบหน้าการชวน",
      referral_load_error: "ไม่สามารถโหลดความคืบหน้าการชวนได้ กรุณาลองอีกครั้งในภายหลัง",
      referral_empty: "การชวนของคุณจะปรากฏที่นี่หลังจากมีคนเข้าร่วมโดยใช้ลิงก์ชวนของคุณ",
      near_miss_referral: "⚡ อีก 1 การชวนเพื่อปลดล็อค +400 XP",
      referral_target_done: "✅ เป้าหมายการชวนสำเร็จแล้ว แชร์ต่อไปเพื่อรับรางวัลระดับถัดไป",
      link_expires: "⏳ ลิงก์นี้หมดอายุใน 24 ชั่วโมง",
      qualified: "ผ่านเกณฑ์",
      checking: "กำลังตรวจสอบ",
      not_eligible: "ไม่มีสิทธิ์",
      n_more_referrals: "อีก {n} การชวนเพื่อปลดล็อค +400 XP",

      n_of_3_done: "{n} จาก 3 เสร็จแล้ว",
      progress_label: "ความคืบหน้า: {completed}/{required} ✅",
      complete_n_more: "เช็คอินอีก {n} ครั้งเพื่อปลดล็อคคูปองของคุณ",

      temporarily_blocked: "ถูกบล็อคชั่วคราว",
      please_try_later: "กรุณาลองในภายหลัง",
      too_fast: "เร็วเกินไป",
      profile_photo_required: "ต้องการรูปโปรไฟล์",
      set_profile_photo: "ตั้งค่ารูปโปรไฟล์ Telegram แล้วลองอีกครั้ง",
      subscribe_required: "ต้องการการติดตาม",
      join_official_channel: "เข้าร่วมช่องทางการเพื่อรับดรอปนี้",
      verification_busy: "การยืนยันไม่ว่าง",
      cant_verify: "ไม่สามารถยืนยันการติดตามได้ขณะนี้ ลองอีกครั้งในไม่ช้า",
      temporarily_unavailable: "ไม่สามารถใช้งานได้ชั่วคราว",
      please_try_again: "กรุณาลองอีกครั้ง",
      join_official_channel_btn: "เข้าร่วมช่องทางการ",
      set_profile_photo_btn: "ตั้งค่ารูปโปรไฟล์",
      you_can_retry: "คุณสามารถลองอีกครั้งได้แล้ว",
      retry_available_in: "ลองอีกครั้งได้ใน {n}s",
      copy_failed: "❌ คัดลอกไม่สำเร็จ กรุณาคัดลอกด้วยตนเอง",
      could_not_get_link: "❌ ไม่สามารถดึงลิงก์ชวนได้ กรุณาลองอีกครั้ง",
      claim_not_available: "❌ ยังไม่สามารถรับได้ขณะนี้ กรุณาตรวจสอบอีกครั้งในภายหลัง",
      please_generate_link: "กรุณาสร้างลิงก์ชวนของคุณก่อน",
      failed_to_copy_voucher: "คัดลอกคูปองไม่สำเร็จ",
      no_voucher_to_copy: "❌ ยังไม่มีโค้ดคูปองให้คัดลอก",
      no_code_to_copy: "ยังไม่มีโค้ดให้คัดลอก กรุณารับก่อน",
      unable_to_copy: "ไม่สามารถคัดลอกอัตโนมัติได้ กรุณาคัดลอกโค้ดด้วยตนเอง: ",

      check_in_reward: "รางวัลการเช็คอิน",
      affiliate_leaderboard_unavailable: "ยังไม่มีข้อมูลลีดเดอร์บอร์ดพาร์ทเนอร์",
    },

    id: {
      page_title: "AdvantPlay Rewards",
      region_welcome: "Selamat Datang di AdvantPlay",
      region_sub: "Pilih wilayah Anda untuk melanjutkan. Tidak akan ditanya lagi.",
      region_foot: "Wilayah Anda menyesuaikan voucher & event. Ditanya sekali.",
      region_select_btn: "Pilih wilayah",
      region_malaysia: "Malaysia",
      region_thailand: "Thailand",
      region_indonesia: "Indonesia",
      region_other: "Wilayah lainnya",

      live_drop: "LIVE DROP",
      limited_drop: "LIMITED DROP",
      vouchers_remaining: "Voucher tersisa",
      no_active_drop: "Belum ada drop aktif saat ini",
      no_active_drop_sub: "Berlangganan saluran resmi kami untuk mengetahui pertama kali saat drop berikutnya tersedia.",
      join_channel: "Gabung Saluran",
      campaign_reward: "🎉 Hadiah Kampanye",
      coming_soon: "Segera hadir…",
      copy_to_clipboard: "📋 Salin ke clipboard",
      affiliate_rewards: "🎁 Hadiah Afiliasi Anda",

      close: "Tutup",
      share_my_rank: "📣 Bagikan Peringkat Saya",
      weekly_rank_not_ranked: "Peringkat Mingguan: Belum peringkat",

      your_rewards_journey: "Perjalanan hadiah Anda",
      check_in_today: "Check-in hari ini",
      xp_streak_sub: "+20 XP · jaga streak Anda",
      check_in: "Check-in",
      checked_in: "Sudah check-in",
      invite_friends: "Ajak 3 teman",
      unlock_xp: "buka +400 XP",
      get_link: "Dapatkan link",
      copy: "📋 Salin",
      share: "📤 Bagikan",
      claim_a_live_drop: "Klaim drop yang sedang aktif",
      campaign_reward_top: "Hadiah kampanye di atas ↑",

      lucky_game: "Lucky Game",
      loading_today: "Memuat pilihan hari ini…",
      invite_earn: "Ajak & dapatkan",
      xp_per_friend: "+60 XP per teman",

      leaderboard_chip: "Leaderboard",
      channel_chip: "Saluran",
      info_hub_chip: "Info Hub",

      weekly_leaderboard: "🏆 Leaderboard Mingguan",
      view_leaderboard: "Lihat Leaderboard",
      refresh_cadence: "🔄 Refresh setiap 5 menit",
      refreshing: "Memuat ulang…",
      leaderboard_tab_default: "XP + Ref",
      leaderboard_tab_affiliate: "Afiliasi",
      click_to_load: "Klik untuk memuat peringkat terbaru...",
      click_to_load_affiliate: "Klik untuk memuat peringkat afiliasi terbaru...",
      retry: "Coba lagi",
      loading_weekly_stats: "⏳ Memuat statistik mingguan…",
      xp_leaderboard: "⭐ Leaderboard XP",
      referral_leaderboard: "🔗 Leaderboard Referral",
      login_to_see_progress: "Masuk untuk melihat progres Anda.",
      login_affiliate_stats: "Masuk untuk melihat statistik afiliasi Anda.",
      no_affiliate_data: "Belum ada data leaderboard afiliasi.",
      no_data: "Belum ada data.",
      no_additional_ranks: "Belum ada peringkat tambahan.",
      show_more: "Tampilkan Lebih",
      view_less: "Tampilkan Lebih Sedikit",
      xp_champion: "Juara XP",
      referral_champion: "Juara Referral",
      elite: "Elite",
      earn_xp_nudge: "Dapatkan XP melalui check-in harian dan kegiatan komunitas.",
      invite_friends_nudge: "Ajak teman untuk naik peringkat referral.",

      welcome_voucher_progress: "🎁 Progress Voucher Selamat Datang",
      welcome_checkin_done: "✅ Check-in 3/3",
      welcome_subscribe: "❌ Berlangganan @AdvantPlayOfficial",
      join_channel_to_unlock: "Bergabung dengan saluran untuk membuka Voucher Selamat Datang Anda.",
      verify_subscription: "Verifikasi Langganan",
      claim_welcome_voucher: "Klaim Voucher Selamat Datang",
      welcome_voucher_waiting: "🎁 Menunggu Voucher Selamat Datang",
      welcome_voucher: "🎁 Voucher Selamat Datang",
      voucher_unlocked: "🎉 Voucher Terbuka",
      voucher_ready: "Voucher Selamat Datang Anda siap.",
      voucher_temporarily_unavailable: "Anda telah memenuhi semua syarat. Distribusi voucher sementara tidak tersedia. Silakan coba lagi nanti.",
      one_more_day: "1 hari lagi.",

      info_hub_title: "ℹ️ Info Hub",
      how_it_works_tab: "🎯 Cara Kerja",
      xp_tab: "⭐ XP",
      vip_tab: "👑 VIP",
      events_tab: "🎉 Event",
      info_hub_how_works: "Cara Kerja AdvantPlay",
      info_hub_step1: "1️⃣ Check-in harian → Dapatkan XP",
      info_hub_step2: "2️⃣ Ajak teman → Dapatkan bonus XP",
      info_hub_step3: "3️⃣ Ikuti event mingguan → Menangkan voucher",
      info_hub_step4: "4️⃣ Top 10 Mingguan → Hadiah ekstra",
      more_activity: "Semakin aktif = Semakin banyak hadiah.",
      xp_top10: "XP Top 10 Mingguan = Kode Voucher Ekstra",
      how_to_earn_xp: "Cara Mendapatkan XP",
      daily_checkin_label: "✅ Check-in Harian",
      streak_bonus: "🔥 Bonus Streak",
      vip_system: "Sistem VIP (Reset Bulanan)",
      vip_crown: "👑 Mahkota = VIP1",
      vip_800: "800 XP Bulanan → VIP1",
      vip_below: "Di bawah 800 saat refresh → Normal",
      vip_unlocks: "VIP1 membuka Voucher Bonus",
      vip_limited: "Voucher VIP terbatas • siapa cepat dia dapat",
      vip_maintain: "Pertahankan XP untuk menjaga status VIP.",
      event_highlights: "Sorotan Event",
      event1: "🎁 Surprise Voucher Drops",
      event2: "🎟️ Lucky Draw Mingguan",
      event3: "🎄 #ComebackIsReal",
      campaign_link: "Link kampanye",
      info_hub_close: "Tutup",

      loading: "Memuat...",
      getting: "Mengambil…",
      preparing: "Menyiapkan…",
      claiming: "Mengklaim…",
      saving: "Menyimpan…",
      ending: "Berakhir…",
      copied: "✅ Disalin",
      copied_simple: "Disalin!",
      fully_redeemed: "Sudah ditukar semua",
      fully_claimed: "Sudah Diklaim Semua",
      claim_reward: "Klaim Hadiah",
      claim_now_label: "Klaim sekarang",
      view_reward: "Lihat Hadiah",
      stay_subscribed: "Tetap berlangganan saluran — Drop berikutnya segera hadir.",
      claim_your_reward: "Ketuk di bawah untuk mengklaim hadiah Anda.",
      no_code_available: "Kode belum tersedia",
      new_member_reward: "🎁 Hadiah Anggota Baru",
      unlock_after_checkins: "Buka hadiah Anda setelah 3 kali check-in.",

      referral_progress: "Progres Referral",
      open_via_telegram: "Buka mini app ini dari Telegram untuk melihat progres referral Anda.",
      referral_load_error: "Tidak dapat memuat progres referral saat ini. Coba lagi nanti.",
      referral_empty: "Referral Anda akan muncul di sini setelah seseorang bergabung menggunakan link undangan Anda.",
      near_miss_referral: "⚡ 1 referral lagi untuk buka +400 XP",
      referral_target_done: "✅ Target referral selesai. Terus bagikan untuk hadiah tingkat berikutnya.",
      link_expires: "⏳ Link ini kadaluarsa dalam 24j",
      qualified: "Memenuhi syarat",
      checking: "Memeriksa",
      not_eligible: "Tidak memenuhi syarat",
      n_more_referrals: "{n} referral lagi untuk buka +400 XP",

      n_of_3_done: "{n} dari 3 selesai",
      progress_label: "Progres: {completed}/{required} ✅",
      complete_n_more: "Selesaikan {n} check-in lagi untuk membuka voucher Anda.",

      temporarily_blocked: "Diblokir sementara",
      please_try_later: "Coba lagi nanti.",
      too_fast: "Terlalu cepat",
      profile_photo_required: "Foto profil diperlukan",
      set_profile_photo: "Setel foto profil Telegram Anda, lalu coba lagi.",
      subscribe_required: "Berlangganan diperlukan",
      join_official_channel: "Bergabung dengan saluran resmi untuk mengklaim drop ini.",
      verification_busy: "Verifikasi sibuk",
      cant_verify: "Tidak dapat memverifikasi langganan sekarang. Coba lagi sebentar.",
      temporarily_unavailable: "Sementara tidak tersedia",
      please_try_again: "Silakan coba lagi.",
      join_official_channel_btn: "Bergabung saluran resmi",
      set_profile_photo_btn: "Setel foto profil",
      you_can_retry: "Anda bisa mencoba lagi sekarang.",
      retry_available_in: "Coba lagi dalam {n}s",
      copy_failed: "❌ Salin gagal. Silakan salin secara manual.",
      could_not_get_link: "❌ Tidak dapat mendapatkan link referral Anda. Silakan coba lagi.",
      claim_not_available: "❌ Slot klaim belum tersedia. Silakan cek lagi nanti.",
      please_generate_link: "Silakan buat link referral Anda terlebih dahulu.",
      failed_to_copy_voucher: "Gagal menyalin voucher.",
      no_voucher_to_copy: "❌ Belum ada kode voucher untuk disalin.",
      no_code_to_copy: "Belum ada kode untuk disalin. Silakan klaim terlebih dahulu.",
      unable_to_copy: "Tidak dapat menyalin otomatis. Silakan salin kode ini secara manual: ",

      check_in_reward: "Hadiah check-in",
      affiliate_leaderboard_unavailable: "Belum ada data leaderboard afiliasi.",
    }
  };

  /* ------------------------------------------------------------------ */
  /* LANGUAGE DETECTION                                                   */
  /* ------------------------------------------------------------------ */
  const SUPPORTED = ["en", "th", "id"];

  function mapTgLang(raw) {
    if (!raw) return null;
    const lc = String(raw).toLowerCase().slice(0, 5);
    if (lc.startsWith("th")) return "th";
    if (lc.startsWith("id") || lc.startsWith("in")) return "id";
    if (lc.startsWith("en")) return "en";
    return null;
  }

  function detectLang() {
    // 1. localStorage manual selection
    try {
      const stored = localStorage.getItem("ap_language");
      if (stored && SUPPORTED.includes(stored)) return stored;
    } catch (_) {}

    // 2. Telegram language_code
    try {
      const tgLang = window.Telegram?.WebApp?.initDataUnsafe?.user?.language_code;
      const mapped = mapTgLang(tgLang);
      if (mapped) return mapped;
    } catch (_) {}

    // 3. Default
    return "en";
  }

  /* ------------------------------------------------------------------ */
  /* TRANSLATION HELPER                                                   */
  /* ------------------------------------------------------------------ */
  function t(key, params) {
    const lang = window.currentLanguage || "en";
    let str = (I18N[lang] && I18N[lang][key]) || (I18N.en && I18N.en[key]) || key;
    if (params && typeof params === "object") {
      str = str.replace(/\{(\w+)\}/g, (_, k) => (k in params ? params[k] : `{${k}}`));
    }
    return str;
  }

  /* ------------------------------------------------------------------ */
  /* APPLY TRANSLATIONS TO DOM                                            */
  /* ------------------------------------------------------------------ */
  function applyTranslations() {
    // data-i18n attributes — simple textContent replacement
    // Skip elements marked data-i18n-dynamic: JS owns their content after first load
    document.querySelectorAll("[data-i18n]:not([data-i18n-dynamic])").forEach(function (el) {
      const key = el.getAttribute("data-i18n");
      el.textContent = t(key);
    });

    // data-i18n-placeholder — input placeholders
    document.querySelectorAll("[data-i18n-placeholder]").forEach(function (el) {
      el.placeholder = t(el.getAttribute("data-i18n-placeholder"));
    });

    // page title
    document.title = t("page_title");

    // update lang selector active state
    document.querySelectorAll(".ap-lang-btn").forEach(function (btn) {
      btn.classList.toggle("active", btn.dataset.lang === window.currentLanguage);
    });
  }

  /* ------------------------------------------------------------------ */
  /* SET LANGUAGE                                                          */
  /* ------------------------------------------------------------------ */
  function setLang(lang) {
    if (!SUPPORTED.includes(lang)) lang = "en";
    try { localStorage.setItem("ap_language", lang); } catch (_) {}
    window.currentLanguage = lang;
    applyTranslations();

    // fire custom event so app JS can react if needed
    try {
      window.dispatchEvent(new CustomEvent("ap:langchange", { detail: { lang } }));
    } catch (_) {}
  }

  /* ------------------------------------------------------------------ */
  /* INJECT LANGUAGE SELECTOR CSS                                         */
  /* ------------------------------------------------------------------ */
  (function injectStyles() {
    const style = document.createElement("style");
    style.id = "ap-i18n-styles";
    style.textContent = [
      "#ap-lang-switcher{display:flex;gap:4px;align-items:center;margin-right:6px;}",
      ".ap-lang-btn{padding:3px 8px;border-radius:20px;border:1px solid rgba(255,255,255,.15);",
      "background:transparent;color:rgba(242,239,233,.55);font-size:11px;font-weight:600;",
      "cursor:pointer;transition:all .18s;letter-spacing:.3px;line-height:1.3;}",
      ".ap-lang-btn:hover{background:rgba(255,255,255,.1);color:var(--text-main);}",
      ".ap-lang-btn.active{background:rgba(255,94,0,.18);border-color:rgba(255,94,0,.45);",
      "color:var(--ap-orange-strong);}",
    ].join("");
    document.head.appendChild(style);
  })();

  /* ------------------------------------------------------------------ */
  /* INITIALISE                                                           */
  /* ------------------------------------------------------------------ */
  window.currentLanguage = detectLang();
  window.t = t;
  window.setLang = setLang;
  window.applyTranslations = applyTranslations;
  window.detectLang = detectLang;

  // Apply once DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", applyTranslations);
  } else {
    applyTranslations();
  }
})();
