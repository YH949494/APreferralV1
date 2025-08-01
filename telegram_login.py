# telegram_login.py

import hashlib
import hmac
import time
import urllib.parse
from dataclasses import dataclass

@dataclass
class TelegramUser:
    id: int
    first_name: str
    last_name: str
    username: str
    photo_url: str
    auth_date: int

@dataclass
class WebAppInitData:
    query_id: str
    user: TelegramUser
    auth_date: int
    hash: str

    @staticmethod
    def parse(init_data: str, bot_token: str) -> 'WebAppInitData':
        data = dict(urllib.parse.parse_qsl(init_data, strict_parsing=True))
        received_hash = data.pop("hash")
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
        secret_key = hashlib.sha256(bot_token.encode()).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if calculated_hash != received_hash:
            raise ValueError("Hash mismatch. Invalid initData.")

        user_data = eval(data["user"]) if isinstance(data["user"], str) else data["user"]
        user = TelegramUser(
            id=int(user_data["id"]),
            first_name=user_data.get("first_name", ""),
            last_name=user_data.get("last_name", ""),
            username=user_data.get("username", ""),
            photo_url=user_data.get("photo_url", ""),
            auth_date=int(user_data.get("auth_date", 0)),
        )

        return WebAppInitData(
            query_id=data.get("query_id", ""),
            user=user,
            auth_date=int(data.get("auth_date", 0)),
            hash=received_hash,
        )
