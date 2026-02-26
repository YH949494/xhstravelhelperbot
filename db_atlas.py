import logging
import os
from datetime import datetime, timezone

from pymongo import DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

log = logging.getLogger("db_atlas")

MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
MONGODB_DB = os.getenv("MONGODB_DB", "referral_bot").strip() or "referral_bot"

_client: MongoClient | None = None
_db: Database | None = None
_init_error: str | None = None


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _init_client() -> None:
    global _client, _db, _init_error
    if _client is not None or _init_error is not None:
        return
    if not MONGODB_URI:
        _init_error = "missing MONGODB_URI"
        return
    try:
        _client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=8000,
            socketTimeoutMS=8000,
        )
        _db = _client[MONGODB_DB]
    except Exception:
        log.exception("mongodb client init failed")
        _init_error = "DB unavailable"


def get_db() -> Database | None:
    _init_client()
    return _db


def get_cols() -> tuple[Collection, Collection, Collection] | None:
    db = get_db()
    if db is None:
        return None
    return db["xhs_skill_ingests"], db["xhs_skill_rules"], db["xhs_skill_logs"]


def get_db_error() -> str | None:
    _init_client()
    if _init_error == "missing MONGODB_URI":
        return "DB unavailable: missing MONGODB_URI"
    if _init_error:
        return "DB unavailable"
    return None


def ping() -> bool:
    db = get_db()
    if db is None:
        return False
    try:
        db.command("ping")
        return True
    except Exception:
        log.exception("mongodb ping failed")
        return False


def ensure_indexes() -> bool:
    cols = get_cols()
    if cols is None:
        return False
    ingests, rules, logs = cols
    try:
        ingests.create_index([("created_at_utc", DESCENDING)])
        ingests.create_index("analysis.content_type")
        ingests.create_index("tg.chat_id")

        rules.create_index("content_type")
        rules.create_index([("seen_count", DESCENDING)])
        rules.create_index([("last_seen_at_utc", DESCENDING)])

        logs.create_index([("log_type", 1), ("created_at_utc", DESCENDING)])
        return True
    except Exception:
        log.exception("ensure indexes failed")
        return False
