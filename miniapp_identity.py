"""Shared "authenticated Mini App user" resolver.

Every regular (non-admin) user endpoint in this app authenticates the same
way: there is no separate server-side session for Telegram Mini App users,
so Telegram's own signed ``initData`` *is* the session — it's issued fresh
by Telegram on every app launch and verified server-side
(``vouchers.verify_telegram_init_data``). This module centralizes that
resolution behind one function so features like Campaign Rewards depend on
"the authenticated Mini App user", not on hand-rolled initData parsing
copy-pasted per feature. If the app ever adds a different session
mechanism, only this module needs to change.

Security note: this intentionally still verifies initData under the hood —
removing verification would violate "never trust a frontend-supplied
Telegram user id". What changed is *where* that logic lives, not whether
identity is verified.
"""

from __future__ import annotations

import json

from flask import jsonify, request


def resolve_authenticated_telegram_user_id() -> tuple[int | None, tuple | None]:
    """Returns (telegram_user_id, None) on success, or (None, (response, status))
    on failure. Never derives identity from a client-supplied user_id/uid."""
    from vouchers import extract_raw_init_data_from_query, verify_telegram_init_data

    init_data_raw = extract_raw_init_data_from_query(request)
    if not init_data_raw:
        body = request.get_json(silent=True) or {}
        init_data_raw = body.get("init_data", "")

    if not init_data_raw:
        return None, (jsonify({"status": "error", "code": "not_authenticated"}), 401)

    ok, data, reason = verify_telegram_init_data(init_data_raw)
    if not ok:
        return None, (jsonify({"status": "error", "code": f"not_authenticated:{reason}"}), 401)

    try:
        user_json = json.loads(data.get("user", "{}"))
        uid = int(user_json.get("id"))
    except Exception:
        return None, (jsonify({"status": "error", "code": "not_authenticated:invalid_user"}), 401)

    return uid, None
