import os

from pymongo import MongoClient

from skills_store import get_skills_collection, get_skills_db


def main() -> None:
    client = MongoClient("mongodb://localhost:27017", connect=False)
    db = get_skills_db(client)
    col = get_skills_collection(client, "xhs_skill_ingests")
    print(f"skills_db={db.name}")
    print(f"collection_db={col.database.name}")
    print(f"collection_name={col.name}")


if __name__ == "__main__":
    os.environ.setdefault("SKILLS_DB", "xhs_travel")
    main()
