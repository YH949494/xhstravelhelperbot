from db_atlas import get_cols


def _truncate(text: str, max_len: int = 3900) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - len("\n…(truncated)")].rstrip() + "\n…(truncated)"


def _short(text: str, size: int = 80) -> str:
    t = (text or "").strip().replace("\n", " ")
    if len(t) <= size:
        return t
    return t[: size - 1].rstrip() + "…"


def build_skill_audit_message() -> str | None:
    cols = get_cols()
    if cols is None:
        return None

    skill_ingests, skill_rules, skill_logs = cols

    total_ingests = skill_ingests.count_documents({})
    total_rules = skill_rules.count_documents({})

    top_types = list(
        skill_rules.aggregate(
            [
                {"$group": {"_id": "$content_type", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5},
            ]
        )
    )

    top_rules = list(skill_rules.find({}, {"rule": 1, "seen_count": 1}).sort("seen_count", -1).limit(5))
    latest_wins = list(
        skill_logs.find({"log_type": "win"}, {"created_at_utc": 1, "summary": 1, "hook_text": 1})
        .sort("created_at_utc", -1)
        .limit(5)
    )
    latest_failures = list(
        skill_logs.find({"log_type": "failure"}, {"created_at_utc": 1, "do_not_learn": 1})
        .sort("created_at_utc", -1)
        .limit(3)
    )

    lines: list[str] = [
        "Skill audit (MongoDB)",
        "",
        f"Total ingests: {total_ingests}",
        f"Total unique rules: {total_rules}",
        "",
        "Top content_type by rule count:",
    ]

    if top_types:
        for item in top_types:
            lines.append(f"- {item.get('_id') or 'unknown'}: {item.get('count', 0)}")
    else:
        lines.append("- (none)")

    lines.extend(["", "Top rules by seen_count:"])
    if top_rules:
        for item in top_rules:
            lines.append(f"- [{item.get('seen_count', 0)}] {_short(str(item.get('rule') or ''))}")
    else:
        lines.append("- (none)")

    lines.extend(["", "Latest 5 wins:"])
    if latest_wins:
        for item in latest_wins:
            ts = item.get("created_at_utc", "")
            summary = _short(str(item.get("summary") or item.get("hook_text") or ""), 100)
            lines.append(f"- {ts} | {summary}")
    else:
        lines.append("- (none)")

    lines.extend(["", "Latest 3 failures:"])
    if latest_failures:
        for item in latest_failures:
            ts = item.get("created_at_utc", "")
            dnl = item.get("do_not_learn") or []
            first = _short(str(dnl[0] if dnl else ""), 100) or "(empty)"
            lines.append(f"- {ts} | {first}")
    else:
        lines.append("- (none)")

    return _truncate("\n".join(lines))
