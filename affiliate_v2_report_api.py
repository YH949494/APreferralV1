"""Private, token-authenticated, read-only route serving the V2 reporting snapshot.

Consumer: Databot's Community Dashboard only. Mirrors the access pattern of
Databot's own private UIM feed (``app/dashboard/uim_redemption_feed_routes.py``)
in the opposite direction:

* The bearer secret is ``AFFILIATE_V2_REPORT_TOKEN`` if set, else the shared
  ``DATABOT_API_KEY`` (already configured on both services). No secret ->
  every request answers 404, and the whole feature is additionally off until
  ``AFFILIATE_V2_REPORT_ENABLED=true``.
* ``Authorization: Bearer <token>``, constant-time comparison, else 401.
* Requests that came through Fly's public edge proxy (it always adds
  ``Fly-Client-IP`` / ``Fly-Forwarded-Port``) are refused with 403 unless
  ``AFFILIATE_V2_REPORT_ALLOW_PUBLIC=true``. The intended path is Fly private
  networking (``http://<apreferral-app>.internal:8080``).
* The handler only reads the precomputed snapshot (see ``affiliate_v2_report``);
  it never runs the activation preview, never aggregates source collections
  and never writes.

Logs carry counts and statuses only.
"""
from __future__ import annotations

import hmac
import logging
import os
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

import affiliate_v2_report as report

logger = logging.getLogger(__name__)

PREFIX = "/internal/affiliate-v2-report/v1"
_PUBLIC_EDGE_HEADERS = ("Fly-Client-IP", "Fly-Forwarded-Port")


def resolve_token() -> str:
    return (os.getenv("AFFILIATE_V2_REPORT_TOKEN", "").strip() or os.getenv("DATABOT_API_KEY", "").strip())


def _json(body: dict, status: int = 200):
    response = jsonify(body)
    response.status_code = status
    response.headers["Cache-Control"] = "no-store"
    return response


def _refusal():
    token = resolve_token()
    if not report.enabled() or not token:
        return _json({"error": "not_found"}, 404)
    allow_public = os.getenv("AFFILIATE_V2_REPORT_ALLOW_PUBLIC", "false").strip().lower() == "true"
    if not allow_public and any(h in request.headers for h in _PUBLIC_EDGE_HEADERS):
        logger.warning("[V2_REPORT][REFUSED] reason=public_edge")
        return _json({"error": "private_network_only"}, 403)
    supplied = request.headers.get("Authorization", "")
    if not supplied.startswith("Bearer ") or not hmac.compare_digest(
        supplied[len("Bearer "):].encode(), token.encode()
    ):
        logger.warning("[V2_REPORT][REFUSED] reason=bad_token")
        return _json({"error": "unauthorized"}, 401)
    return None


def make_blueprint(get_db) -> Blueprint:
    bp = Blueprint("affiliate_v2_report", __name__)

    @bp.get(f"{PREFIX}/summary")
    def summary():
        refused = _refusal()
        if refused is not None:
            return refused
        try:
            limit = int(request.args.get("limit", report.DEFAULT_LIMIT))
        except ValueError:
            return _json({"error": "invalid_limit"}, 400)
        try:
            body = report.build_report(
                get_db(), start_date=request.args.get("start_date"), end_date=request.args.get("end_date"),
                now_utc=datetime.now(timezone.utc), limit=limit)
        except report.WindowError as exc:
            return _json({"error": "invalid_window", "detail": str(exc)}, 400)
        except Exception:
            logger.exception("[V2_REPORT][ERROR]")
            return _json({"error": "report_unavailable"}, 503)
        logger.info("[V2_REPORT][SERVED] status=%s affiliates=%s", body["status"], body["affiliates_total"])
        return _json(body)

    return bp
