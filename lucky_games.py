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
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId
from flask import Blueprint, jsonify, request
from pymongo.errors import DuplicateKeyError

import database
from config import KL_TZ

logger = logging.getLogger(__name__)

lucky_games_admin_bp = Blueprint("lucky_games_admin", __name__)
lucky_games_public_bp = Blueprint("lucky_games_public", __name__)

COLLECTION = "lucky_games"
DAILY_SELECTION_COLLECTION = "lucky_game_daily_selection"
EVENTS_COLLECTION = "lucky_game_events"

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

# Click-tracking: the Lucky Game card always points at the same AdvantPlay
# games page, tagged for attribution. Not admin-configurable — this is a
# fixed marketing destination, not part of the per-game catalogue.
TRACKING_SURFACE = "miniapp_lucky_game"
LUCKY_GAME_DESTINATION_URL = (
    "https://advantplay.com/our-games.html"
    "?utm_source=telegram&utm_medium=miniapp&utm_campaign=lucky_game"
)
ALLOWED_TRACK_EVENTS = ("impression", "click")
MAX_TRACKING_KEY_LEN = 200
ANALYTICS_DEFAULT_DAYS = 7
ANALYTICS_MIN_DAYS = 1
ANALYTICS_MAX_DAYS = 90


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


def _ensure_event_indexes() -> None:
    """Indexes for lucky_game_events, created through the project's existing
    ``database.safe_create_index`` convention (idempotent, tolerant of a
    pre-existing equivalent index under a different name)."""
    try:
        col = database.db[EVENTS_COLLECTION]
        # General lookups: "has this user already interacted with today's
        # selection" / per-tracking-key breakdowns.
        database.safe_create_index(
            col, [("user_id", 1), ("tracking_key", 1), ("event_type", 1)],
            name="ix_lucky_game_events_user_tracking_type",
        )
        # Impression uniqueness: one user + tracking_key may only ever have
        # one impression row. Scoped to event_type == "impression" via a
        # partial filter so click rows (intentionally not unique) are
        # entirely unaffected by this constraint.
        database.safe_create_index(
            col, [("user_id", 1), ("tracking_key", 1), ("event_type", 1)],
            name="ux_lucky_game_events_impression_unique",
            unique=True,
            partialFilterExpression={"event_type": "impression"},
        )
        database.safe_create_index(
            col, [("tracking_key", 1), ("event_type", 1)],
            name="ix_lucky_game_events_tracking_type",
        )
        database.safe_create_index(
            col, [("selection_date_kl", 1), ("event_type", 1)],
            name="ix_lucky_game_events_date_type",
        )
        database.safe_create_index(
            col, [("event_type", 1), ("created_at_utc", 1)],
            name="ix_lucky_game_events_type_created",
        )
    except Exception:
        logger.warning("[LUCKY_GAMES] event_index_creation_failed", exc_info=True)


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
_ensure_event_indexes()
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


def _build_tracking_key(date_kl: str, game_id: str) -> str:
    """Canonical click-tracking identity for a day's selection. Deliberately
    keyed on the persisted ``game_id`` (not the display name) so a game
    rename never fragments a single day's tracking_key into two."""
    return f"daily_game:{date_kl}:{game_id}"


def _build_selection_result(date_kl: str, game: dict) -> dict:
    """Public payload for ``get_daily_game_selection`` — extends the
    pre-existing {"ok", "date_kl", "slot"} shape with a ``tracking_key`` an
    older cached client simply ignores, so this is additive-only and never
    breaks ``renderDailyGame()``'s existing ``data.date_kl``/``data.slot``
    checks in static/index.html."""
    return {
        "ok": True,
        "date_kl": date_kl,
        "slot": _build_daily_slot(game),
        "tracking_key": _build_tracking_key(date_kl, game["id"]),
    }


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
            return _build_selection_result(date_kl, game)

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
                return _build_selection_result(date_kl, game)
            return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
        game = _load_game_by_id(new_doc["game_id"])
        if game is None:
            return {"ok": False, "date_kl": date_kl, "error": "no_eligible_games"}
        return _build_selection_result(date_kl, game)

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
    return _build_selection_result(date_kl, game)


# ---------------------------------------------------------------------------
# Click tracking (impressions + clicks on the Mini App's Lucky Game card)
# ---------------------------------------------------------------------------
#
# Collection: ``lucky_game_events``. Analytics only — never grants XP,
# vouchers, or referral credit. Every event's identity (user_id, game_id,
# selection_date_kl, tracking_key) is resolved server-side from the same
# persisted daily selection ``get_daily_game_selection`` already serves, not
# from client-supplied fields — a caller can only ever confirm the current
# canonical selection, never assert a different one. user_id is always the
# Telegram id verified from initData (miniapp_identity), never a
# client-supplied value.
#
# Impressions are deduped to one per (user_id, tracking_key) via a partial
# unique index scoped to event_type == "impression" (see
# _ensure_event_indexes) — reopening the Mini App repeatedly on the same day
# therefore still counts as exactly one unique viewer. Clicks are never
# deduped: every intentional tap is its own row, so repeat engagement by the
# same user still shows up in "total clicks" while "unique clickers" is
# computed separately by distinct user_id.


def record_lucky_game_event(
    *, event_type: str, user_id: int, client_tracking_key: str | None = None
) -> tuple[bool, str, dict]:
    """Best-effort analytics write. Never raises. Resolves today's canonical
    persisted selection itself; if ``client_tracking_key`` is given it must
    match that canonical value or the event is rejected — a client can
    corroborate the current selection but never dictate a different one.

    Returns ``(accepted, reason, info)``:
      accepted=True,  reason="recorded"            -- new row written
      accepted=True,  reason="duplicate"           -- impression already seen today
      accepted=False, reason="no_active_selection" -- no eligible game right now
      accepted=False, reason="invalid_tracking_key"-- client value didn't match canonical
      accepted=False, reason="write_failed"        -- Mongo error (logged, swallowed)
    """
    selection = get_daily_game_selection()
    if not selection.get("ok"):
        return False, "no_active_selection", {}

    date_kl = selection["date_kl"]
    slot = selection["slot"]
    tracking_key = selection["tracking_key"]
    game_id = slot.get("id", "")
    game_name = slot.get("name", "")

    if client_tracking_key and client_tracking_key != tracking_key:
        return False, "invalid_tracking_key", {"tracking_key": tracking_key}

    doc = {
        "event_type": event_type,
        "user_id": int(user_id),
        "game_id": game_id,
        "game_name": game_name,
        "selection_date_kl": date_kl,
        "tracking_key": tracking_key,
        "surface": TRACKING_SURFACE,
        "destination": LUCKY_GAME_DESTINATION_URL,
        "created_at_utc": datetime.now(timezone.utc),
    }
    try:
        database.db[EVENTS_COLLECTION].insert_one(doc)
    except DuplicateKeyError:
        logger.info(
            "[LUCKY_GAME][IMPRESSION] uid=%s game_id=%s date_kl=%s duplicate=1",
            user_id, game_id, date_kl,
        )
        return True, "duplicate", {"tracking_key": tracking_key, "game_id": game_id, "date_kl": date_kl}
    except Exception:
        logger.warning("[LUCKY_GAME] event_write_failed event_type=%s", event_type, exc_info=True)
        return False, "write_failed", {}

    if event_type == "impression":
        logger.info(
            "[LUCKY_GAME][IMPRESSION] uid=%s game_id=%s date_kl=%s duplicate=0",
            user_id, game_id, date_kl,
        )
    else:
        logger.info(
            "[LUCKY_GAME][CLICK] uid=%s game_id=%s date_kl=%s",
            user_id, game_id, date_kl,
        )
    return True, "recorded", {"tracking_key": tracking_key, "game_id": game_id, "date_kl": date_kl}


@lucky_games_public_bp.post("/api/lucky-game/track")
def track_lucky_game_event():
    """Best-effort click/impression analytics — never blocks or fails the
    caller. Always returns 200 with a fast, minimal body so a
    fire-and-forget frontend call never has to branch on the response before
    continuing navigation to the AdvantPlay destination.

    Auth: user_id always comes from verified Telegram initData
    (``miniapp_identity.resolve_authenticated_telegram_user_id``), never
    from the request body — an unverifiable caller cannot spoof another
    user's clicks. An unauthenticated/unverifiable request is discarded
    (not written to analytics) but still answers 200, mirroring
    event_banner.py's ``/api/event-banner/track``."""
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        body = {}

    event_type = str(body.get("event") or "").strip().lower()
    if event_type not in ALLOWED_TRACK_EVENTS:
        logger.info("[LUCKY_GAME][TRACK_REJECT] reason=invalid_event")
        return jsonify({"success": False, "error": "invalid_event"}), 400

    client_tracking_key = body.get("tracking_key")
    if client_tracking_key is not None and (
        not isinstance(client_tracking_key, str) or len(client_tracking_key) > MAX_TRACKING_KEY_LEN
    ):
        logger.info("[LUCKY_GAME][TRACK_REJECT] reason=invalid_tracking_key")
        return jsonify({"success": False, "error": "invalid_tracking_key"}), 400

    try:
        from miniapp_identity import resolve_authenticated_telegram_user_id

        user_id, auth_err = resolve_authenticated_telegram_user_id()
    except Exception:
        user_id, auth_err = None, True
    if auth_err or user_id is None:
        logger.info("[LUCKY_GAME][TRACK_REJECT] reason=unauthenticated")
        return jsonify({"success": True}), 200

    accepted, reason, _info = record_lucky_game_event(
        event_type=event_type, user_id=user_id, client_tracking_key=client_tracking_key,
    )
    if not accepted and reason == "invalid_tracking_key":
        logger.info("[LUCKY_GAME][TRACK_REJECT] reason=invalid_tracking_key")
        return jsonify({"success": False, "error": "invalid_tracking_key"}), 400
    if not accepted:
        # no_active_selection / write_failed -- never the caller's fault; the
        # AdvantPlay page has already opened client-side by the time this
        # resolves, so there is nothing to retry or surface.
        return jsonify({"success": True}), 200

    return jsonify({"success": True, "duplicate": reason == "duplicate"}), 200


# ---------------------------------------------------------------------------
# Admin analytics
# ---------------------------------------------------------------------------


def _safe_rate(numerator: int, denominator: int) -> float:
    if not denominator:
        return 0
    return round(numerator / denominator, 4)


def _kl_date_bounds(days: int) -> tuple[str, str]:
    today = datetime.now(KL_TZ).date()
    start = today - timedelta(days=days - 1)
    return start.isoformat(), today.isoformat()


def _build_breakdown(docs: list[dict], *, key_field: str, key_name: str) -> list[dict]:
    """Groups event docs by ``key_field`` (e.g. selection_date_kl or
    game_id) and computes impressions/unique_viewers/clicks/unique_clickers/
    ctr per bucket. Plain-Python grouping (not an aggregation pipeline) so
    this works identically against the real MongoDB driver and the
    in-memory FakeDb used in tests."""
    buckets: dict = {}
    for d in docs:
        key = d.get(key_field)
        if key is None:
            continue
        bucket = buckets.setdefault(
            key, {"impressions": 0, "viewers": set(), "clicks": 0, "clickers": set(), "game_id": None, "game_name": None}
        )
        bucket["game_id"] = d.get("game_id") or bucket["game_id"]
        bucket["game_name"] = d.get("game_name") or bucket["game_name"]
        uid = d.get("user_id")
        if d.get("event_type") == "impression":
            bucket["impressions"] += 1
            if uid is not None:
                bucket["viewers"].add(uid)
        elif d.get("event_type") == "click":
            bucket["clicks"] += 1
            if uid is not None:
                bucket["clickers"].add(uid)

    rows = []
    for key in sorted(buckets.keys()):
        bucket = buckets[key]
        rows.append({
            key_name: key,
            "game_id": bucket["game_id"],
            "game_name": bucket["game_name"],
            "impressions": bucket["impressions"],
            "unique_viewers": len(bucket["viewers"]),
            "clicks": bucket["clicks"],
            "unique_clickers": len(bucket["clickers"]),
            "ctr": _safe_rate(len(bucket["clickers"]), len(bucket["viewers"])),
        })
    return rows


@lucky_games_admin_bp.get("/api/admin/lucky-game/analytics")
def lucky_game_analytics():
    """Impressions/clicks/CTR summary for the Lucky Game card. CTR is always
    unique_clickers / unique_viewers (never raw clicks / impressions, which
    repeat clicks or reopens would inflate); 0 when there were no viewers.
    ``days`` is bounded to [1, 90] so this can never trigger an unbounded
    full-collection scan."""
    _, err = _require_admin()
    if err:
        return err

    try:
        days = int(request.args.get("days", ANALYTICS_DEFAULT_DAYS))
    except (TypeError, ValueError):
        days = ANALYTICS_DEFAULT_DAYS
    days = max(ANALYTICS_MIN_DAYS, min(ANALYTICS_MAX_DAYS, days))

    start_str, end_str = _kl_date_bounds(days)
    col = database.db[EVENTS_COLLECTION]
    match = {"selection_date_kl": {"$gte": start_str, "$lte": end_str}}
    docs = list(col.find(match))

    impressions = [d for d in docs if d.get("event_type") == "impression"]
    clicks = [d for d in docs if d.get("event_type") == "click"]
    unique_viewers = {d.get("user_id") for d in impressions if d.get("user_id") is not None}
    unique_clickers = {d.get("user_id") for d in clicks if d.get("user_id") is not None}

    summary = {
        "impressions": len(impressions),
        "unique_viewers": len(unique_viewers),
        "clicks": len(clicks),
        "unique_clickers": len(unique_clickers),
        "unique_ctr": _safe_rate(len(unique_clickers), len(unique_viewers)),
    }

    return jsonify({
        "period": {"from": start_str, "to": end_str, "days": days},
        "summary": summary,
        "by_day": _build_breakdown(docs, key_field="selection_date_kl", key_name="date"),
        "by_game": _build_breakdown(docs, key_field="game_id", key_name="game_id"),
    })
