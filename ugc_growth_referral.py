import hashlib
import os
import re
import secrets
from datetime import datetime, timedelta, timezone

from pymongo import ReturnDocument


REQUIRED_UGC_ENV_VARS = ("UGC_T1_SMALL_DROP_ID", "UGC_T2_MID_DROP_ID")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def require_env() -> dict[str, str]:
    values = {key: (os.getenv(key) or "").strip() for key in REQUIRED_UGC_ENV_VARS}
    missing = [k for k, v in values.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required UGC env vars: {', '.join(missing)}")
    return values


def require_ugc_growth_env() -> dict[str, str]:
    return require_env()


def generate_referral_token() -> str:
    for _ in range(10):
        token = secrets.token_urlsafe(9)
        if _TOKEN_RE.fullmatch(token) and len(f"r_{token}") <= 64:
            return token
    raise RuntimeError("failed_to_generate_valid_referral_token")


def hash_value(raw: str | None) -> str | None:
    value = (raw or "").strip()
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def create_referral_token(referral_tokens_col, *, owner_uid: int, now: datetime | None = None) -> tuple[str, datetime]:
    now = now or datetime.now(timezone.utc)
    token = generate_referral_token()
    expires_at = now + timedelta(hours=24)
    referral_tokens_col.insert_one(
        {
            "_id": token,
            "owner_uid": int(owner_uid),
            "created_at": now,
            "expires_at": expires_at,
        }
    )
    return token, expires_at


def resolve_token(referral_tokens_col, token: str, *, now: datetime | None = None) -> int | None:
    now = now or datetime.now(timezone.utc)
    if not token or not _TOKEN_RE.fullmatch(token):
        return None
    doc = referral_tokens_col.find_one({"_id": token}, {"owner_uid": 1, "expires_at": 1})
    if not doc:
        return None
    expires_at = doc.get("expires_at")
    if not expires_at or expires_at <= now:
        return None
    owner_uid = doc.get("owner_uid")
    return int(owner_uid) if owner_uid is not None else None


def first_touch_claim(
    referral_claims_col,
    *,
    viewer_uid: int,
    owner_uid: int,
    token: str,
    ip_hash: str | None,
    subnet_hash: str | None,
    device_hash: str | None,
    now: datetime | None = None,
):
    now = now or datetime.now(timezone.utc)
    due_at = now + timedelta(hours=48)
    existing = referral_claims_col.find_one({"viewer_uid": viewer_uid})
    if existing:
        return existing, False
    referral_claims_col.insert_one(
        {
            "viewer_uid": int(viewer_uid),
            "source_uid": int(owner_uid),
            "token": token,
            "status": "pending",
            "created_at": now,
            "due_at": due_at,
            "validated_at": None,
            "viewer_reward_issued": False,
            "viewer_reward_drop_id": None,
            "credited": False,
            "credit_reason": None,
            "ip_hash": ip_hash,
            "subnet_hash": subnet_hash,
            "device_hash": device_hash,
        }
    )
    return referral_claims_col.find_one({"viewer_uid": viewer_uid}), True


def lock_referral_claim(referral_claims_col, **kwargs):
    return first_touch_claim(referral_claims_col, **kwargs)


def should_allow_viewer_reward(referral_claims_col, claim: dict) -> bool:
    for field in ("ip_hash", "subnet_hash", "device_hash"):
        value = claim.get(field)
        if not value:
            continue
        prior = referral_claims_col.find_one(
            {
                field: value,
                "status": "validated",
                "viewer_uid": {"$ne": claim.get("viewer_uid")},
            }
        )
        if prior:
            return False
    return True


def burst_pattern_detected(recent_claims: list[dict]) -> bool:
    return len(recent_claims) >= 5


def is_expansion_qualified(viewer_uid: int, source_uid: int, claim: dict, recent_claims: list[dict]) -> bool:
    _ = (viewer_uid, source_uid)
    device_hash = claim.get("device_hash")
    subnet_hash = claim.get("subnet_hash")
    recent_devices = {row.get("device_hash") for row in recent_claims if row.get("device_hash")}
    recent_subnets = {row.get("subnet_hash") for row in recent_claims if row.get("subnet_hash")}
    if device_hash and device_hash in recent_devices:
        return False
    if subnet_hash and subnet_hash in recent_subnets:
        return False
    if burst_pattern_detected(recent_claims):
        return False
    return True


def upsert_growth_credit(growth_credit_col, source_uid: int, now: datetime):
    _ = now
    return growth_credit_col.find_one_and_update(
        {"_id": source_uid},
        {
            "$setOnInsert": {
                "credited_count": 0,
                "last_credit_at": None,
                "tier_awarded": {"s2": False, "s3": False, "s4": False},
            },
            "$set": {"_id": source_uid},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )


def apply_growth_credit(growth_credit_col, source_uid: int, now: datetime) -> tuple[bool, str, dict]:
    doc = upsert_growth_credit(growth_credit_col, source_uid, now)
    last_credit_at = doc.get("last_credit_at")
    if last_credit_at and last_credit_at > (now - timedelta(hours=24)):
        return False, "throttle", doc
    updated = growth_credit_col.find_one_and_update(
        {"_id": source_uid},
        {"$inc": {"credited_count": 1}, "$set": {"last_credit_at": now}},
        return_document=ReturnDocument.AFTER,
    )
    return True, "credited", updated or doc
