import logging
import os

from pymongo.collection import Collection
from pymongo.database import Database

log = logging.getLogger("skills_store")

_SKILLS_DB_LOGGED = False


def get_skills_db(client) -> Database:
    db_name = os.getenv("SKILLS_DB", "xhs_travel").strip() or "xhs_travel"
    return client[db_name]


def get_skills_collection(client, name: str) -> Collection:
    global _SKILLS_DB_LOGGED
    db = get_skills_db(client)
    collection = db[name]
    if not _SKILLS_DB_LOGGED:
        log.info("Skills DB: %s (collection: %s)", db.name, name)
        _SKILLS_DB_LOGGED = True
    return collection
