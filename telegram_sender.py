"""CLI tool to broadcast a winners message to selected Telegram users.

Usage examples:

    python telegram_sender.py --ids "111,222,333"
    python telegram_sender.py --ids "111:Alice,222:Bob"
    python telegram_sender.py            # reads ids.csv by default

The script respects Telegram rate limits, retries on 429 responses, and logs
blocked users (403 responses).
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Sequence

import requests 

BOT_TOKEN = os.environ.get("BOT_TOKEN")
API_BASE = "https://api.telegram.org/bot{token}/sendMessage"
BUTTON_URL = "https://forms.gle/ssz7JAwWTKzo16Cs5"
MESSAGE_TEMPLATE = (
    "🎉 Hi {name}, congrats — you’re among the AdvantPlay Community October Lucky Draw Winners!\n"
    "Tap below to verify 👇\n"
    "(Reply /stop to opt out)"
)
THROTTLE_PER_SECOND = 8
PAUSE_EVERY = 50
PAUSE_DURATION = 5
DEFAULT_NAME = "Player"


@dataclass
class Recipient:
    chat_id: int
    name: str = DEFAULT_NAME


class Sender:
    def __init__(self, token: str, throttle_per_second: int = THROTTLE_PER_SECOND):
        if not token:
            raise ValueError("BOT_TOKEN environment variable is required")
        self.token = token
        self.api_url = API_BASE.format(token=token)
        self.session = requests.Session()
        self.min_interval = 1.0 / float(throttle_per_second)

    def close(self) -> None:
        self.session.close()

    def send(self, recipient: Recipient) -> bool:
        payload = {
            "chat_id": recipient.chat_id,
            "text": MESSAGE_TEMPLATE.format(name=recipient.name or DEFAULT_NAME),
            "reply_markup": {
                "inline_keyboard": [
                    [
                        {
                            "text": "Open Form",
                            "url": BUTTON_URL,
                        }
                    ]
                ]
            },
        }

        while True:
            try:
                response = self.session.post(self.api_url, json=payload, timeout=20)
            except requests.RequestException as exc:
                logging.error("Network error sending to %s: %s", recipient.chat_id, exc)
                return False

            retry = self._handle_response(recipient, response)
            if retry < 0:
                return False
            if retry == 0:
                return True

            time.sleep(retry)

    def _handle_response(self, recipient: Recipient, response: requests.Response) -> float:
        """Process Telegram response.

        Returns:
            0  -> success, no retry
            >0 -> wait seconds then retry
            -1 -> fail without retry
        """
        data: Optional[dict] = None
        try:
            data = response.json()
        except json.JSONDecodeError:
            logging.error(
                "Unexpected response for %s: %s", recipient.chat_id, response.text
            )
            return -1

        ok = data.get("ok")
        description = data.get("description")

        if response.status_code == 200 and ok:
            logging.info("Sent message to %s", recipient.chat_id)
            return 0

        error_code = data.get("error_code", response.status_code)
        parameters = data.get("parameters", {}) or {}

        if error_code == 429:
            retry_after = float(parameters.get("retry_after", 1))
            logging.warning(
                "Rate limited for %s, retrying after %ss", recipient.chat_id, retry_after
            )
            return max(retry_after, 1.0)

        if error_code == 403:
            logging.warning(
                "User %s blocked the bot or cannot be reached: %s",
                recipient.chat_id,
                description,
            )
            return -1

        logging.error(
            "Failed to send to %s (HTTP %s): %s",
            recipient.chat_id,
            error_code,
            description or response.text,
        )
        return -1


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send Telegram notifications")
    parser.add_argument(
        "--ids",
        help="Comma-separated list of chat IDs, optionally with names (e.g. 123:Alice)",
    )
    parser.add_argument(
        "--csv",
        default="ids.csv",
        help="Path to CSV file with chat_id[,name] per line (default: ids.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse inputs and show summary without sending messages",
    )
    return parser.parse_args(argv)


def load_recipients(ids_arg: Optional[str], csv_path: str) -> List[Recipient]:
    recipients: List[Recipient] = []

    if ids_arg:
        entries = [item.strip() for item in ids_arg.split(",") if item.strip()]
        for entry in entries:
            if ":" in entry:
                chat_id_str, name = entry.split(":", 1)
            else:
                chat_id_str, name = entry, DEFAULT_NAME
            try:
                chat_id = int(chat_id_str)
            except ValueError:
                logging.error("Invalid chat id: %s", chat_id_str)
                continue
            recipients.append(Recipient(chat_id=chat_id, name=name.strip() or DEFAULT_NAME))

    elif csv_path:
        try:
            with open(csv_path, newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                for row in reader:
                    if not row:
                        continue
                    chat_id_str = row[0].strip()
                    if not chat_id_str or chat_id_str.startswith("#"):
                        continue
                    try:
                        chat_id = int(chat_id_str)
                    except ValueError:
                        logging.error("Invalid chat id in CSV: %s", chat_id_str)
                        continue
                    name = row[1].strip() if len(row) > 1 and row[1].strip() else DEFAULT_NAME
                    recipients.append(Recipient(chat_id=chat_id, name=name))
        except FileNotFoundError:
            logging.error("CSV file %s not found", csv_path)
    return recipients


def pace(index: int, min_interval: float) -> None:
    if index > 0 and index % PAUSE_EVERY == 0:
        logging.info("Reached %s messages, pausing for %ss", index, PAUSE_DURATION)
        time.sleep(PAUSE_DURATION)
    else:
        time.sleep(min_interval)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = parse_args(argv)
    recipients = load_recipients(args.ids, args.csv)
    total = len(recipients)

    if total == 0:
        logging.error("No recipients found")
        return 1

    logging.info("Loaded %s recipients", total)

    if args.dry_run:
        print(json.dumps({"sent": 0, "failed": 0, "total": total}))
        return 0

    token = os.environ.get("BOT_TOKEN")
    try:
        sender = Sender(token=token)
    except ValueError as exc:
        logging.error(str(exc))
        return 1

    sent = 0
    failed = 0

    try:
        for index, recipient in enumerate(recipients, start=1):
            success = sender.send(recipient)
            if success:
                sent += 1
            else:
                failed += 1
            pace(index, sender.min_interval)
    finally:
        sender.close()

    summary = {"sent": sent, "failed": failed, "total": total}
    print(json.dumps(summary))
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
