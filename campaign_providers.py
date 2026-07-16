"""Campaign Centre — Provider management.

Providers represent external systems (tournament sites, subscription-voucher
sites) that a campaign links to. Provider secrets (HMAC keys) are never
stored in the document returned to any frontend — they live in environment
variables referenced by ``secret_env_var``.

Collection: ``gc_providers``
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

from flask import Blueprint, jsonify, request

import database

logger = logging.getLogger(__name__)

campaign_providers_bp = Blueprint("campaign_providers", __name__)

PROVIDER_TYPES = ["tournament", "external_subscription_verification", "external_website"]
AUTH_MODES = ["hmac_sha256", "none"]
URL_MODES = ["query_parameter", "path_parameter", "custom_template"]


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    try:
        col = database.db["gc_providers"]
        col.create_index([("provider_id", 1)], name="ux_gc_providers_provider_id", unique=True)
        col.create_index([("active", 1)], name="ix_gc_providers_active")
        col.create_index([("type", 1)], name="ix_gc_providers_type")
    except Exception:
        logger.warning("[CAMPAIGN_PROVIDERS] index creation failed", exc_info=True)


_ensure_indexes()


def _log_audit(action: str, admin: dict, provider_id: str, details: dict | None = None) -> None:
    try:
        database.db["campaign_admin_audit_log"].insert_one({
            "action": action,
            "entity": "provider",
            "entity_id": provider_id,
            "admin": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
            "details": details or {},
            "at": datetime.now(timezone.utc),
        })
    except Exception:
        logger.warning("[CAMPAIGN_PROVIDERS] audit_write_failed", exc_info=True)


def get_provider(provider_id: str) -> dict | None:
    return database.db["gc_providers"].find_one({"provider_id": provider_id})


def provider_secret(provider: dict) -> str:
    """Resolve a provider's HMAC secret from its environment mapping. Never
    persisted, never returned to any API response."""
    env_var = (provider or {}).get("secret_env_var") or ""
    if not env_var:
        return ""
    return os.environ.get(env_var, "")


def provider_is_usable_for_results(provider: dict | None) -> bool:
    return bool(provider) and provider.get("active") is True


def _valid_https_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if os.environ.get("FLASK_ENV") == "development":
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    return parsed.scheme == "https" and bool(parsed.netloc)


def build_effective_url(provider: dict, campaign: dict, telegram_user_id: int) -> str | None:
    """Build the Phase-1 UID deep-link destination URL for a tournament campaign."""
    base_url = (provider.get("base_url") or "").rstrip("/")
    if not base_url:
        return None
    destination = campaign.get("destination") or {}
    url_mode = provider.get("url_mode") or "query_parameter"
    path = (destination.get("path") or "").strip("/")

    if url_mode == "path_parameter":
        return f"{base_url}/{telegram_user_id}"
    if url_mode == "custom_template":
        template = provider.get("url_template") or "{base_url}/play/{telegram_uid}"
        return template.format(
            base_url=base_url,
            telegram_uid=telegram_user_id,
            campaign_id=campaign.get("campaign_id", ""),
            path=path,
        )
    # default: query_parameter
    suffix = f"/{path}" if path else "/play"
    return f"{base_url}{suffix}?uid={telegram_user_id}"


def _serialize(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    for k in ("created_at", "updated_at"):
        if out.get(k):
            out[k] = out[k].isoformat()
    return out


def _validate_body(body: dict, *, partial: bool = False) -> tuple[dict | None, str | None]:
    updates: dict = {}

    if not partial or "name" in body:
        name = (body.get("name") or "").strip()
        if not name:
            return None, "missing_name"
        updates["name"] = name

    if not partial or "type" in body:
        ptype = (body.get("type") or "").strip()
        if ptype not in PROVIDER_TYPES:
            return None, "invalid_type"
        updates["type"] = ptype

    if not partial or "base_url" in body:
        base_url = (body.get("base_url") or "").strip()
        if base_url and not _valid_https_url(base_url):
            return None, "invalid_base_url"
        if base_url.endswith("//"):
            return None, "duplicated_trailing_slash"
        updates["base_url"] = base_url

    if "url_mode" in body or not partial:
        url_mode = (body.get("url_mode") or "query_parameter").strip()
        if url_mode not in URL_MODES:
            return None, "invalid_url_mode"
        updates["url_mode"] = url_mode

    if "url_template" in body:
        updates["url_template"] = (body.get("url_template") or "").strip()

    if "allowed_campaign_types" in body or not partial:
        allowed = body.get("allowed_campaign_types") or []
        from campaign_centre import CAMPAIGN_TYPES

        invalid = [t for t in allowed if t not in CAMPAIGN_TYPES]
        if invalid:
            return None, f"invalid_allowed_campaign_types:{','.join(invalid)}"
        updates["allowed_campaign_types"] = list(allowed)

    if "auth_mode" in body or not partial:
        auth_mode = (body.get("auth_mode") or "hmac_sha256").strip()
        if auth_mode not in AUTH_MODES:
            return None, "invalid_auth_mode"
        updates["auth_mode"] = auth_mode

    if "secret_env_var" in body:
        updates["secret_env_var"] = (body.get("secret_env_var") or "").strip()

    if "notes" in body:
        updates["notes"] = (body.get("notes") or "").strip()

    return updates, None


@campaign_providers_bp.get("/api/admin/providers")
def list_providers():
    _, err = _require_admin()
    if err:
        return err
    docs = list(database.db["gc_providers"].find({}, sort=[("created_at", -1)], limit=200))
    providers = [_serialize(d) for d in docs]
    for p in providers:
        p["secret_configured"] = bool(provider_secret(get_provider(p["provider_id"]) or {}))
        campaign_count = database.db["gc_campaigns"].count_documents(
            {"destination.provider_id": p["provider_id"]}
        )
        p["linked_campaign_count"] = campaign_count
    return jsonify({"status": "ok", "providers": providers})


@campaign_providers_bp.post("/api/admin/providers")
def create_provider():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    provider_id = (body.get("provider_id") or "").strip()
    if not provider_id:
        return jsonify({"status": "error", "code": "missing_provider_id"}), 400

    updates, code = _validate_body(body)
    if code:
        return jsonify({"status": "error", "code": code}), 400

    now = datetime.now(timezone.utc)
    doc = {
        "provider_id": provider_id,
        "active": False,
        **updates,
        "created_at": now,
        "updated_at": now,
    }
    try:
        result = database.db["gc_providers"].insert_one(doc)
    except Exception as exc:
        if "duplicate" in str(exc).lower():
            return jsonify({"status": "error", "code": "duplicate_provider_id"}), 409
        logger.exception("[CAMPAIGN_PROVIDERS] create_failed")
        return jsonify({"status": "error", "code": "internal_error"}), 500

    _log_audit("provider_created", admin, provider_id, {"type": updates.get("type")})
    from campaign_events import emit_campaign_event
    emit_campaign_event(event_type="provider_created", provider_id=provider_id, source="admin")
    return jsonify({"status": "ok", "id": str(result.inserted_id), "provider_id": provider_id}), 201


@campaign_providers_bp.get("/api/admin/providers/<provider_id>")
def get_provider_route(provider_id: str):
    _, err = _require_admin()
    if err:
        return err
    doc = get_provider(provider_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    out = _serialize(doc)
    out["secret_configured"] = bool(provider_secret(doc))
    return jsonify({"status": "ok", "provider": out})


@campaign_providers_bp.put("/api/admin/providers/<provider_id>")
def update_provider(provider_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = get_provider(provider_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404

    body = request.get_json(force=True, silent=True) or {}
    updates, code = _validate_body(body, partial=True)
    if code:
        return jsonify({"status": "error", "code": code}), 400
    updates["updated_at"] = datetime.now(timezone.utc)

    database.db["gc_providers"].update_one({"provider_id": provider_id}, {"$set": updates})
    _log_audit("provider_updated", admin, provider_id, {"fields": list(updates.keys())})
    from campaign_events import emit_campaign_event
    emit_campaign_event(event_type="provider_updated", provider_id=provider_id, source="admin")
    return jsonify({"status": "ok"})


@campaign_providers_bp.post("/api/admin/providers/<provider_id>/activate")
def activate_provider(provider_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = get_provider(provider_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    if doc.get("auth_mode") == "hmac_sha256" and not provider_secret(doc):
        return jsonify({"status": "error", "code": "secret_not_configured"}), 400
    database.db["gc_providers"].update_one(
        {"provider_id": provider_id},
        {"$set": {"active": True, "updated_at": datetime.now(timezone.utc)}},
    )
    _log_audit("provider_activated", admin, provider_id)
    from campaign_events import emit_campaign_event
    emit_campaign_event(event_type="provider_activated", provider_id=provider_id, source="admin")
    return jsonify({"status": "ok"})


@campaign_providers_bp.post("/api/admin/providers/<provider_id>/deactivate")
def deactivate_provider(provider_id: str):
    admin, err = _require_admin()
    if err:
        return err
    doc = get_provider(provider_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    database.db["gc_providers"].update_one(
        {"provider_id": provider_id},
        {"$set": {"active": False, "updated_at": datetime.now(timezone.utc)}},
    )
    _log_audit("provider_deactivated", admin, provider_id)
    from campaign_events import emit_campaign_event
    emit_campaign_event(event_type="provider_deactivated", provider_id=provider_id, source="admin")
    return jsonify({"status": "ok"})


@campaign_providers_bp.get("/api/admin/providers/<provider_id>/preview")
def preview_provider_destination(provider_id: str):
    _, err = _require_admin()
    if err:
        return err
    doc = get_provider(provider_id)
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    path = request.args.get("path", "")
    sample_uid = request.args.get("sample_uid", "123456789")
    fake_campaign = {"campaign_id": request.args.get("campaign_id", ""), "destination": {"path": path}}
    try:
        effective_url = build_effective_url(doc, fake_campaign, int(sample_uid))
    except (TypeError, ValueError):
        effective_url = build_effective_url(doc, fake_campaign, 123456789)
    return jsonify({
        "status": "ok",
        "base_url": doc.get("base_url", ""),
        "url_mode": doc.get("url_mode", "query_parameter"),
        "path": path,
        "effective_url": effective_url,
    })
