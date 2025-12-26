# app.py — one-off Telegram sender (runs once per deploy)
import os, time, json, sys, urllib.request, urllib.parse

# ==== EDIT THESE ====
IDS = [
    8239720056,
    6760825915,
    8584235099,
    7881548506,
    5326243398,
    6719191538,
    5509097730,
    606581041,
    8589123768,
    7451216517,
    5859971320,
    5693736914,
    2048398637,
    8490261874,
    5722397989,
    6456891229,
    7428814851,
    8348012879,
    5217182179,
    7795268274,
    6211273918,
    2117574594,
    5949880251,
    8010889061,
    6604653391,
    1800308343,
    6271037959,
    8030962555,
    7477143007,
    7168219125,
    8365152334,
    8521615467,
    6651150491,
    7509126866,
    190815383,
    1984913563,
    7034993250,
    7157412698,
    7294870028,
    8295747859,
    7521097685,
    7190736753,
    8407689670,
    6442542348,
    1779531622,
    8235147007,
    1191481779,
    6401015114,
    7924680699,
    7326720502,
    852299367,
    8405954796,
    8135055063,
    7009778988,
    6926733724,
    8309956640,
    8596892611,
    5669881611,
    8063374806,
    8270605625,
    8178821376,
    7527290425,
    6045557433,
    1730466205,
    6149739555,
    1062235675,
    8500047176,
    8056993016,
    8491966889,
    8059674638,
    5406068938,
    2100653102,
    1123476492,
    5005324449,
    6065702193,
    8416087451,
    7084124495,
    5172818659,
    7628214025,
    7931441353,
    7692804327,
    5301445168,
    59867340,
    6110160636,
    1371073899,
    8472613535,
    7665918070,
    7571803695,
    8036875311,
    6973214920,
    7810786939,
    8332566229,
    8448300380,
    5834753223,
    7012523643,
    7429100670,
    7910545113,
    7605385515,
    5297649910,
    6583581901,
    8047118326,
    1741863728,
    6035744332,
    6719846286,
    7728957757,
    7481328064,
    7561490061,
    1642433247,
    5788293168,
    6398540686,
    8068456063,
    5138279755,
    7226569418,
    8359826325,
    7041136911,
    6981091403,
    7743838975,
    8550608773,
    7321271784,
    7024550183,
    6465192802,
    1409726010,
    6748266972,
    5626093686,
    8269797715,
    6046701980,
    8388006607,
    5943944669,
    7642942199,
    654981354,
    7138965086,
    8586062648,
    8080785630,
    8523003936,
    7774110687,
    1894027314,
    7694654762,
    8465292259,
    6738983094,
    6461905883,
    1620652311,
    5139983168,
    5774725073,
    7633274575,
    8093341808,
    5031769584,
    8041399992,
    8101592820,
    5617991622,
    5715681604,
    6506835308,
    7843189187,
    6780293523,
    8091165780,
    503900643,
    8258784983,
    5348311469,
    6401247285,
    6677063263,
    6827042810,
    5529512694,
    7729887500,
    6338844824,
    767071686,
    5398051246,
    8489608644,
    6251846034,
    1003988209,
    7999776099,
    8499431424,
    7870008880,
    6312327659,
    7556681929,
    8073035675,
    8198061501,
    7396271296,
    5877999958,
    7668289330,
    8078091605,
    8490850628,
    1632029022,
    8276266714,
    2004788634,
    1575978083,
    5640302039,
    6649530175,
    6064302583,
    7634199859,
    8228877533,
    1839169122,
    7776500888,
    6335527263,
    45357652,
    8290030963,
    8359234643,
    6926988158,
    8083635867,
    6419655143,
    8394530486,
    8402902722,
    1329748443
]

MESSAGE = (
    "Congrats! 🎉 You've won AdvantPlay Limited Edition Merch for Xmas Gift Delight.\n"
    "Please fill in the details before 02/01/2026.\n"
)

BUTTON_TEXT = "Fill in the details now"     # <-- added
BUTTON_URL  = "https://forms.gle/9416ftWWXR7uSqcJ7"   # <-- add your URL or leave ""

THROTTLE_SECONDS = 0.12
MAX_RETRIES_429  = 3
# ====================

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN not set. Run: fly secrets set BOT_TOKEN=...", file=sys.stderr)
    sys.exit(1)

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

def tg_request(method: str, data: dict):
    url = f"{API_BASE}/{method}"
    payload = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", "ignore")
            return resp.getcode(), body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        return e.code, body
    except Exception as e:
        return None, str(e)

def send_message(chat_id: int, text: str):
    data = {"chat_id": str(chat_id), "text": text, "disable_web_page_preview": "true"}
    if BUTTON_TEXT and BUTTON_URL:
        data["reply_markup"] = json.dumps({"inline_keyboard": [[{"text": BUTTON_TEXT, "url": BUTTON_URL}]]})

    retries = 0
    while True:
        code, body = tg_request("sendMessage", data)
        if code is None:
            return False, f"NETWORK_ERR: {body}"
        if 200 <= code < 300:
            return True, "OK"
        if code == 429 and retries < MAX_RETRIES_429:
            try:
                retry_after = json.loads(body).get("parameters", {}).get("retry_after", 1)
            except Exception:
                retry_after = 1
            time.sleep(max(1, int(retry_after)))
            retries += 1
            continue
        return False, f"{code}: {body}"

def main():
    sent = failed = 0
    failures = []

    print(f"Starting one-off send to {len(IDS)} IDs...")
    for i, uid in enumerate(IDS, 1):
        ok, info = send_message(uid, MESSAGE)
        if ok:
            sent += 1
            print(f"[{i}/{len(IDS)}] ✅ {uid}")
        else:
            failed += 1
            failures.append((uid, info[:300]))
            print(f"[{i}/{len(IDS)}] ❌ {uid} -> {info}")
        time.sleep(THROTTLE_SECONDS)

    print("\n=== SUMMARY ===")
    print(f"Sent: {sent}  |  Failed: {failed}")
    if failures:
        print("Failures:")
        for uid, why in failures:
            print(f"- {uid} -> {why}")
    if any("403" in f[1] for f in failures):
        print("\nNote: 403 Forbidden usually means the user hasn’t started the bot yet.", file=sys.stderr)

if __name__ == "__main__":
    main()
