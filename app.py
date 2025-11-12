# app.py — one-off Telegram sender (runs once per deploy)
import os, time, json, sys, urllib.request, urllib.parse

# ==== EDIT THESE ====
IDS = [
    # Put your winners' Telegram user IDs here (one per line)
    5903782683,
    8187206891,
    7496639456,
    7625447265,
    7830348618,
    5914553194,
    6284628114,
    8104331744,
    7683845013,
    6430617267,
    1773182874,
    8047005890,
    7274610227,
    6821008489,
    8270028615,
    5485178162,
    1501645134,
    7126164380,
    6721766831,
    2013429005,
    6198555987,
    6005055310,
    7907857815,
    6879748957,
    1859327009,
    1326562647,
    7496723798,
    6535139433,
    8280608017,
    7166342626,
    6009331175,
    7815366180,
    8320396001,
    5406068938,
    7967691399,
    556470617,
    6976547279,
    1786827854,
    1699078939,
    7224761407,
    1329748443
]

MESSAGE = (
    "Congrats! 🎉 You’re among our AdvantPlay Community October Lucky Draw winners.\n"
    "Please submit your details within 48hours so we can arrange your prize:\n"
    "https://forms.gle/wdgJSEM4WGHNCfCc9"
)

# Optional inline button; leave empty strings to disable
BUTTON_TEXT = "Open Form"
BUTTON_URL  = "https://forms.gle/ssz7JAwWTKzo16Cs5"

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
