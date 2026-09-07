"""Lucky Games — admin-managed catalogue of "Lucky Game" cards shown in the
Telegram Mini App (name, label, volatility, max win, image, deep link).

Collection: ``lucky_games``. This is also the single source of truth for the
Mini App's "Lucky Game" daily-pick tile (``/v2/miniapp/daily-game`` in
main.py): the tile shows exactly one weighted-random game per Kuala-Lumpur
calendar day, selected from this collection's published rows and persisted
in ``lucky_game_daily_selection`` (see ``get_daily_game_selection`` below).
A legacy hardcoded pool (``DAILY_GAME_SLOTS`` in main.py) used to drive that
tile independently of this admin-managed catalogue; it has been retired in
favor of this one mechanism so the "Lucky Game" feature never has two
competing selection systems running at once.

Follows the same conventions as event_banner.py: admin auth via
``vouchers.require_admin``, a ``_validate_body`` allowlist for both create
and PATCH (so PATCH can never write an arbitrary field), and a public
endpoint that only ever returns published, non-admin fields.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId
from flask import Blueprint, jsonify, request

import database
from config import KL_TZ

logger = logging.getLogger(__name__)

lucky_games_admin_bp = Blueprint("lucky_games_admin", __name__)
lucky_games_public_bp = Blueprint("lucky_games_public", __name__)

COLLECTION = "lucky_games"
DAILY_SELECTION_COLLECTION = "lucky_game_daily_selection"

VOLATILITY_OPTIONS = ("Low", "Low-Med", "Medium", "High-Med", "High")
DEFAULT_LABEL = "Lucky Game"
DEFAULT_VOLATILITY = "Medium"

# 1 = very low probability, 5 = low, 10 = normal, 20 = high, 30 = featured.
DEFAULT_SELECTION_WEIGHT = 10

# Fields an admin may ever set. PATCH builds its update dict exclusively
# from this allowlist, so an unexpected/extra key in the request body is
# silently ignored rather than reaching the database.
_EDITABLE_FIELDS = (
    "name", "label", "volatility", "max_win",
    "image_url", "game_url", "provider", "sort_order",
    "selection_weight", "is_published",
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


def _backfill_selection_weight_defaults() -> None:
    """Idempotent backfill: any pre-existing lucky_games row (seeded before
    ``selection_weight`` existed, or migrated by seed_lucky_games.py before
    this field was added there) gets the normal default weight. Safe to run
    on every boot / every Gunicorn worker — a row that already has the field
    is never touched again, so an admin's explicit weight always survives."""
    try:
        database.db[COLLECTION].update_many(
            {"selection_weight": {"$exists": False}},
            {"$set": {"selection_weight": DEFAULT_SELECTION_WEIGHT}},
        )
    except Exception:
        logger.warning("[LUCKY_GAMES] selection_weight_backfill_failed", exc_info=True)


_ensure_indexes()
_backfill_selection_weight_defaults()


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

    if not partial or "selection_weight" in body:
        raw_weight = body.get("selection_weight", DEFAULT_SELECTION_WEIGHT)
        if isinstance(raw_weight, bool) or not isinstance(raw_weight, int):
            try:
                if isinstance(raw_weight, str) and raw_weight.strip().isdigit():
                    raw_weight = int(raw_weight)
                else:
                    raise ValueError()
            except (TypeError, ValueError):
                return None, "invalid_selection_weight"
        # Must be a positive integer — a weight of 0 or below would make a
        # game mathematically impossible to select while still cluttering
        # the eligible pool, which is never the admin's intent.
        if raw_weight <= 0:
            return None, "invalid_selection_weight"
        updates["selection_weight"] = int(raw_weight)

    if not partial or "is_published" in body:
        raw_published = body.get("is_published", False)
        if not isinstance(raw_published, bool):
            # A JSON string like "false" is truthy in Python — coercing it
            # with bool() would silently publish a game the caller meant to
            # keep unpublished. Reject anything that isn't a real boolean.
            return None, "invalid_is_published"
        updates["is_published"] = raw_published

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
        # body can be a non-dict (e.g. a JSON array) when code == "invalid_body" —
        # guard the .get() so a malformed request still gets a clean 400
        # instead of an unhandled AttributeError turning into a 500.
        name = body.get("name") if isinstance(body, dict) else None
        logger.warning("[LUCKY_GAMES][INVALID_CONFIG] reason=%s name=%s", code, name)
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


@lucky_games_admin_bp.post("/api/admin/lucky-games/bulk-weight")
def bulk_update_selection_weight():
    """Set the same ``selection_weight`` on many games in one call — e.g. an
    admin marking a batch of games "featured" (weight 30) without editing
    each row individually. Body: {"ids": [...], "selection_weight": N}.
    Unknown/invalid ids are skipped rather than failing the whole batch."""
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"status": "error", "code": "invalid_body"}), 400

    raw_ids = body.get("ids")
    if not isinstance(raw_ids, list) or not raw_ids:
        return jsonify({"status": "error", "code": "missing_ids"}), 400

    weight_updates, code = _validate_body({"selection_weight": body.get("selection_weight")}, partial=True)
    if code:
        return jsonify({"status": "error", "code": code}), 400
    weight = weight_updates["selection_weight"]

    oids = []
    for raw_id in raw_ids:
        oid, id_code = _parse_object_id(raw_id)
        if id_code:
            continue
        oids.append(oid)
    if not oids:
        return jsonify({"status": "error", "code": "invalid_id"}), 400

    now = datetime.now(timezone.utc)
    updated_by = (admin or {}).get("usernameLower") or str((admin or {}).get("id", ""))
    result = database.db[COLLECTION].update_many(
        {"_id": {"$in": oids}},
        {"$set": {"selection_weight": weight, "updated_at": now, "updated_by": updated_by}},
    )
    modified = getattr(result, "modified_count", None)
    if modified is None:
        modified = getattr(result, "matched_count", len(oids))
    _log_audit("bulk_update_weight", admin, ",".join(str(o) for o in oids), {"selection_weight": weight, "count": modified})
    return jsonify({"status": "ok", "selection_weight": weight, "updated_count": modified})


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


# ---------------------------------------------------------------------------
# Daily Lucky Game selection (weighted random, one winner per KL day)
# ---------------------------------------------------------------------------
#
# Backs the Mini App's single "Lucky Game" tile (main.py's
# ``/v2/miniapp/daily-game``): exactly one published game is chosen per
# Kuala-Lumpur calendar day, weighted by ``selection_weight``, and the
# result is persisted in ``lucky_game_daily_selection`` keyed by that date
# so every worker / machine / request serves the same winner for the rest
# of the day without recomputing anything.


def _kl_date_str(now: datetime | None = None) -> str:
    ref = now.astimezone(KL_TZ) if now else datetime.now(KL_TZ)
    return ref.strftime("%Y-%m-%d")


def _normalize_weight(raw) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
        return DEFAULT_SELECTION_WEIGHT
    return raw


def _doc_to_game_dict(doc: dict) -> dict:
    out = {"id": str(doc["_id"])}
    for field in _PUBLIC_FIELDS:
        out[field] = doc.get(field) or ""
    out["selection_weight"] = _normalize_weight(doc.get("selection_weight"))
    return out


def _eligible_games_for_daily_selection() -> list[dict]:
    """Published games with a normalized positive-integer weight. Mirrors
    the public catalogue's eligibility rule (``is_published: True``) so the
    daily pick and the card list always agree on what's "live"."""
    games = []
    for doc in database.db[COLLECTION].find({"is_published": True}):
        try:
            games.append(_doc_to_game_dict(doc))
        except Exception:
            logger.warning("[LUCKY_GAMES][DAILY][SERIALIZE_ERROR] id=%s", doc.get("_id"), exc_info=True)
            continue
    return games


def _load_game_by_id(game_id) -> dict | None:
    """Returns the live, currently-published game for ``game_id``, or None
    if it was deleted or unpublished since selection — the trigger for a
    controlled reselection (e.g. an admin unpublishes/deletes today's
    winner mid-day)."""
    if not game_id:
        return None
    oid, code = _parse_object_id(game_id)
    if code:
        return None
    doc = database.db[COLLECTION].find_one({"_id": oid, "is_published": True})
    if not doc:
        return None
    return _doc_to_game_dict(doc)


def _weighted_pick(games: list[dict], rng=None) -> dict:
    """Weighted random selection: P(game) = game.weight / sum(weights).
    ``rng`` accepts any object exposing ``choices`` (e.g. a seeded
    ``random.Random`` instance) so tests can make the pick deterministic;
    production uses the module-level ``random`` — this only affects which
    slot machine gets a marketing highlight, not anything security-sensitive."""
    rng = rng or random
    weights = [g["selection_weight"] for g in games]
    return rng.choices(games, weights=weights, k=1)[0]


def _build_daily_slot(game: dict) -> dict:
    """Public payload for the daily-pick tile. Keeps the pre-existing
    ``tag``/``maxwin`` keys the Mini App's ``renderDailyGame()`` already
    reads (backward compatibility with cached/older clients) alongside the
    richer lucky_games field set the admin catalogue exposes. Never
    includes ``selection_weight`` — internal probability weighting is not
    exposed publicly."""
    slot = {field: game.get(field, "") for field in _PUBLIC_FIELDS}
    slot["id"] = game["id"]
    slot["tag"] = game.get("volatility", "")
    slot["maxwin"] = game.get("max_win", "")
    return slot


def _select_and_build_doc(date_kl: str, *, rng=None) -> dict | None:
    games = _eligible_games_for_daily_selection()
    if not games:
        return None
    chosen = _weighted_pick(games, rng=rng)
    return {
        "_id": date_kl,
        "date_kl": date_kl,
        "game_id": chosen["id"],
        "game_name": chosen.get("name", ""),
        "selection_weight": chosen.get("selection_weight"),
        "selected_at_utc": datetime.now(timezone.utc),
    }


def get_daily_game_selection(now: datetime | None = None, *, rng=None) -> dict:
    """Returns today's (KL) single Lucky Game pick, selecting and
    persisting it on first request of the day. ``rng`` is test-only (an
    injectable ``random.Random`` for deterministic assertions).

    Concurrency-safe across Gunicorn workers / Fly machines: the first
    selection is written with an atomic upsert keyed by the KL date string
    (``_id``) via ``$setOnInsert`` — two racing first-of-day requests can
    only ever produce one stored winner (the loser's upsert is a no-op
    match against the winner's row, mirroring the same
    upsert-with-$setOnInsert pattern used by seed_lucky_games.py). A
    mid-day reselection (today's winner got unpublished/deleted) uses an
    optimistic compare-and-swap on the stale ``game_id`` so concurrent
    reselections also collapse to one canonical replacement.

    Returns ``{"ok": True, "date_kl": ..., "slot": {...}}`` or
    ``{"ok": False, "date_kl": ..., "error": "no_eligible_games"}`` —
    never raises."""
    date_kl = _kl_date_str(now)
    col = database.db[DAILY_SELECTION_COLLECTION]

    try:
        existing = col.find_one({"_id": date_kl})
    except Exception:
        logger.warning("[LUCKY_GAMES][DAILY] read_failed date_kl=%s", date_kl, exc_info=True)
        existing = None

    if existing:
        game = _load_game_by_id(existing.get("game_id"))
        if game is not None:
            return {"ok": True, "date_kl": date_kl, "slot": _build_daily_slot(game)}

        # Today's previously-selected game was unpublished or deleted since
        # selection — controlled reselection, replacing only if nobody else
        # has already replaced it (optimistic CAS on the stale game_id).
        logger.info(
            "[LUCKY_GAMES][DAILY][RESELECT] date_kl=%s stale_game_id=%s",
            date_kl, existing.get("game_id"),
        )
        new_doc = _select_and_build_doc(date_kl, rng=rng)
        if new_doc is None:
            return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
        try:
            result = col.update_one(
                {"_id": date_kl, "game_id": existing.get("game_id")},
                {"$set": new_doc},
            )
        except Exception:
            logger.warning("[LUCKY_GAMES][DAILY] reselect_write_failed date_kl=%s", date_kl, exc_info=True)
            return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
        if getattr(result, "matched_count", 0) == 0:
            # A concurrent request already replaced today's selection —
            # defer to the canonical row rather than overwrite it again.
            existing = col.find_one({"_id": date_kl}) or {}
            game = _load_game_by_id(existing.get("game_id"))
            if game is not None:
                return {"ok": True, "date_kl": date_kl, "slot": _build_daily_slot(game)}
            return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
        game = _load_game_by_id(new_doc["game_id"])
        if game is None:
            return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
        return {"ok": True, "date_kl": date_kl, "slot": _build_daily_slot(game)}

    # No selection recorded yet today — first request of the day.
    new_doc = _select_and_build_doc(date_kl, rng=rng)
    if new_doc is None:
        return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
    try:
        col.update_one({"_id": date_kl}, {"$setOnInsert": new_doc}, upsert=True)
        canonical = col.find_one({"_id": date_kl})
    except Exception:
        logger.warning("[LUCKY_GAMES][DAILY] first_selection_write_failed date_kl=%s", date_kl, exc_info=True)
        return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
    game = _load_game_by_id((canonical or {}).get("game_id"))
    if game is None:
        # Rare race: the winner was deleted/unpublished between the upsert
        # and this read. Leave the stale row for the next request's
        # reselection branch above rather than retrying in a loop here.
        return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
    return {"ok": True, "date_kl": date_kl, "slot": _build_daily_slot(game)}
