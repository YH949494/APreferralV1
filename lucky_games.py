"""Lucky Games — admin-managed catalogue of "Lucky Game" cards shown in the
Telegram Mini App (name, label, volatility, max win, image, deep link).

Collection: ``lucky_games``. A separate, lightweight collection rather than
folding into the existing ``DAILY_GAME_SLOTS`` hardcoded pool in main.py
(used by the unrelated ``/v2/miniapp/daily-game`` deterministic daily-pick
endpoint) — that mechanism picks one slot per Kuala-Lumpur day from a fixed
in-code list and has no admin, image, or deep-link concept. This module
gives admins full CRUD/publish/reorder control over a distinct, richer set
of game cards without touching that existing rotation logic at all.

Follows the same conventions as event_banner.py: admin auth via
``vouchers.require_admin``, a ``_validate_body`` allowlist for both create
and PATCH (so PATCH can never write an arbitrary field), and a public
endpoint that only ever returns published, non-admin fields.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId
from flask import Blueprint, jsonify, request

import database

logger = logging.getLogger(__name__)

lucky_games_admin_bp = Blueprint("lucky_games_admin", __name__)
lucky_games_public_bp = Blueprint("lucky_games_public", __name__)

COLLECTION = "lucky_games"

VOLATILITY_OPTIONS = ("Low", "Low-Med", "Medium", "High-Med", "High")
DEFAULT_LABEL = "Lucky Game"
DEFAULT_VOLATILITY = "Medium"

# Fields an admin may ever set. PATCH builds its update dict exclusively
# from this allowlist, so an unexpected/extra key in the request body is
# silently ignored rather than reaching the database.
_EDITABLE_FIELDS = (
    "name", "label", "volatility", "max_win",
    "image_url", "game_url", "provider", "sort_order", "is_published",
)

# Fields ever exposed to the public (unauthenticated) endpoint. Internal
# bookkeeping — _id aside (re-exposed as a string "id"), sort_order,
# is_published, created_at/updated_at — never leaves this module.
_PUBLIC_FIELDS = ("name", "label", "volatility", "max_win", "image_url", "game_url", "provider")


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    try:
        col = database.db[COLLECTION]
        # Backs the public listing's query+sort: published games ordered by
        # sort_order then creation time.
        col.create_index(
            [("is_published", 1), ("sort_order", 1), ("created_at", 1)],
            name="ix_lucky_games_published_order",
        )
    except Exception:
        logger.warning("[LUCKY_GAMES] index_creation_failed", exc_info=True)


_ensure_indexes()


def _validate_url(url: str, *, allow_tg: bool = True) -> bool:
    """Same acceptance rule as event_banner.py: https:// always accepted;
    tg:// deep links accepted only when allow_tg is set. Never javascript:/
    data:/other executable schemes."""
    if not isinstance(url, str) or not url.strip():
        return False
    url = url.strip()
    lowered = url.lower()
    if lowered.startswith("javascript:") or lowered.startswith("data:"):
        return False
    if any(ch in url for ch in ("\r", "\n", "\t")):
        return False
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return bool(parsed.netloc)
    if allow_tg and parsed.scheme == "tg":
        return True
    return False


def _parse_object_id(game_id: str):
    """Returns (ObjectId, None) or (None, error_code)."""
    try:
        return ObjectId(str(game_id)), None
    except (InvalidId, TypeError, ValueError):
        return None, "invalid_id"


def _validate_body(body: dict, *, partial: bool = False) -> tuple[dict | None, str | None]:
    """Validates and normalizes an admin-supplied lucky-game payload.
    Only keys in _EDITABLE_FIELDS are ever considered — anything else in
    ``body`` is dropped on the floor. When ``partial`` is False (create),
    ``name`` must be present; every other field is optional and gets a
    sane default when omitted. When ``partial`` is True (PATCH), only the
    fields present in ``body`` are validated/returned."""
    if not isinstance(body, dict):
        return None, "invalid_body"

    updates: dict = {}

    if not partial or "name" in body:
        name = str(body.get("name") or "").strip()
        if not name:
            return None, "missing_name"
        updates["name"] = name

    if not partial or "label" in body:
        label = str(body.get("label") or "").strip()
        updates["label"] = label or DEFAULT_LABEL

    if not partial or "volatility" in body:
        volatility = str(body.get("volatility") or "").strip()
        if not volatility:
            volatility = DEFAULT_VOLATILITY
        if volatility not in VOLATILITY_OPTIONS:
            return None, "invalid_volatility"
        updates["volatility"] = volatility

    if not partial or "max_win" in body:
        updates["max_win"] = str(body.get("max_win") or "").strip()

    if not partial or "provider" in body:
        updates["provider"] = str(body.get("provider") or "").strip()

    if not partial or "image_url" in body:
        image_url = str(body.get("image_url") or "").strip()
        if image_url and not _validate_url(image_url, allow_tg=False):
            return None, "invalid_image_url"
        updates["image_url"] = image_url

    if not partial or "game_url" in body:
        game_url = str(body.get("game_url") or "").strip()
        if game_url and not _validate_url(game_url, allow_tg=True):
            return None, "invalid_game_url"
        updates["game_url"] = game_url

    if not partial or "sort_order" in body:
        raw_sort = body.get("sort_order", 0)
        if isinstance(raw_sort, bool) or not isinstance(raw_sort, int):
            # Reject floats/strings/bools outright rather than silently
            # truncating — an admin typing "10.5" should see an error, not
            # have it become 10.
            try:
                if isinstance(raw_sort, str) and raw_sort.strip().lstrip("-").isdigit():
                    raw_sort = int(raw_sort)
                else:
                    raise ValueError()
            except (TypeError, ValueError):
                return None, "invalid_sort_order"
        updates["sort_order"] = int(raw_sort)

    if not partial or "is_published" in body:
        updates["is_published"] = bool(body.get("is_published", False))

    return updates, None


def _serialize(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    for k in ("created_at", "updated_at"):
        v = out.get(k)
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            out[k] = v.isoformat()
    return out


def _log_audit(action: str, admin: dict, game_id: str, details: dict | None = None) -> None:
    try:
        database.db["campaign_admin_audit_log"].insert_one({
            "action": action,
            "entity": "lucky_game",
            "entity_id": game_id,
            "admin": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
            "details": details or {},
            "at": datetime.now(timezone.utc),
        })
    except Exception:
        logger.warning("[LUCKY_GAMES] audit_write_failed", exc_info=True)


# ---------------------------------------------------------------------------
# Admin CRUD
# ---------------------------------------------------------------------------


@lucky_games_admin_bp.get("/api/admin/lucky-games")
def list_lucky_games():
    _, err = _require_admin()
    if err:
        return err
    docs = list(database.db[COLLECTION].find({}, sort=[("sort_order", 1), ("created_at", 1)]))
    return jsonify({"status": "ok", "games": [_serialize(d) for d in docs]})


@lucky_games_admin_bp.post("/api/admin/lucky-games")
def create_lucky_game():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    updates, code = _validate_body(body, partial=False)
    if code:
        logger.warning("[LUCKY_GAMES][INVALID_CONFIG] reason=%s name=%s", code, body.get("name"))
        return jsonify({"status": "error", "code": code}), 400

    now = datetime.now(timezone.utc)
    doc = {
        **updates,
        "created_at": now,
        "updated_at": now,
        "created_by": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
    }
    try:
        result = database.db[COLLECTION].insert_one(doc)
    except Exception:
        logger.exception("[LUCKY_GAMES] create_failed")
        return jsonify({"status": "error", "code": "internal_error"}), 500

    doc["_id"] = result.inserted_id
    _log_audit("create", admin, str(result.inserted_id), {"name": doc.get("name")})
    return jsonify({"status": "ok", "game": _serialize(doc)}), 201


@lucky_games_admin_bp.patch("/api/admin/lucky-games/<game_id>")
def update_lucky_game(game_id: str):
    admin, err = _require_admin()
    if err:
        return err
    oid, code = _parse_object_id(game_id)
    if code:
        return jsonify({"status": "error", "code": code}), 400

    doc = database.db[COLLECTION].find_one({"_id": oid})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    body = request.get_json(silent=True) or {}
    updates, code = _validate_body(body, partial=True)
    if code:
        logger.warning("[LUCKY_GAMES][INVALID_CONFIG] reason=%s game_id=%s", code, game_id)
        return jsonify({"status": "error", "code": code}), 400
    if not updates:
        return jsonify({"status": "error", "code": "no_fields_to_update"}), 400

    updates["updated_at"] = datetime.now(timezone.utc)
    updates["updated_by"] = (admin or {}).get("usernameLower") or str((admin or {}).get("id", ""))
    database.db[COLLECTION].update_one({"_id": oid}, {"$set": updates})
    doc = database.db[COLLECTION].find_one({"_id": oid})
    _log_audit("update", admin, game_id, {"fields": list(updates.keys())})
    return jsonify({"status": "ok", "game": _serialize(doc)})


@lucky_games_admin_bp.delete("/api/admin/lucky-games/<game_id>")
def delete_lucky_game(game_id: str):
    admin, err = _require_admin()
    if err:
        return err
    oid, code = _parse_object_id(game_id)
    if code:
        return jsonify({"status": "error", "code": code}), 400

    result = database.db[COLLECTION].delete_one({"_id": oid})
    if not getattr(result, "deleted_count", 0):
        return jsonify({"status": "error", "code": "not_found"}), 404
    _log_audit("delete", admin, game_id)
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Public read-only endpoint
# ---------------------------------------------------------------------------


@lucky_games_public_bp.get("/api/lucky-games")
def list_public_lucky_games():
    resp_payload = {"status": "ok", "games": []}
    try:
        docs = list(
            database.db[COLLECTION].find(
                {"is_published": True},
                sort=[("sort_order", 1), ("created_at", 1)],
            )
        )
        games = []
        for doc in docs:
            try:
                card = {"id": str(doc["_id"])}
                for field in _PUBLIC_FIELDS:
                    card[field] = doc.get(field) or ""
                games.append(card)
            except Exception:
                logger.warning("[LUCKY_GAMES][SERIALIZE_ERROR] id=%s", doc.get("_id"), exc_info=True)
                continue
        resp_payload["games"] = games
    except Exception:
        logger.warning("[LUCKY_GAMES][API_ERROR]", exc_info=True)
        resp_payload = {"status": "ok", "games": []}

    resp = jsonify(resp_payload)
    resp.headers["Cache-Control"] = "no-store"
    return resp
