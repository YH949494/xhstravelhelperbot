import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from openai import OpenAI
from db_atlas import get_cols, now_utc_iso
LEARN_MODEL = os.getenv("OPENAI_MODEL_LEARN_SCRIPT", os.getenv("OPENAI_MODEL_NOTE", "gpt-4o-mini"))
SKILLS_DIR = Path("skills")
SKILLS_WRITE_FILES = os.getenv("SKILLS_WRITE_FILES", "0").strip() == "1"
CONTENT_FILE_MAP = {
    "cost_breakdown": "cost_breakdown.md",
    "avoid_pitfalls": "avoid_pitfalls.md",
    "local_weekend": "local_weekend.md",
    "booking_strategy": "booking_strategy.md",
    "tools": "tools.md",
    "other": "misc.md",
    "hidden_tips": "misc.md",
}
SCHEMA: dict[str, Any] = {
    "name": "script_learning",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "platform": {"type": "string", "enum": ["xhs", "tiktok", "ig", "other"]},
            "content_type": {
                "type": "string",
                "enum": [
                    "cost_breakdown",
                    "avoid_pitfalls",
                    "local_weekend",
                    "booking_strategy",
                    "hidden_tips",
                    "tools",
                    "other",
                ],
            },
            "hook": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "text": {"type": "string"},
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "number",
                                "conflict",
                                "avoid_pitfalls",
                                "comparison",
                                "decision_tension",
                                "other",
                            ],
                        },
                    },
                },
                "required": ["text", "type"],
            },
            "target_audience": {"type": "array", "items": {"type": "string"}},
            "decision_tension": {"type": "array", "items": {"type": "string"}},
            "structure_steps": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "step": {"type": "integer"},
                        "what": {"type": "string"},
                    },
                    "required": ["step", "what"],
                },
            },
            "save_worthy_lines": {"type": "array", "items": {"type": "string"}},
            "cta": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "type": {"type": "array", "items": {"type": "string", "enum": ["follow", "save", "comment", "dm"]}},
                    "text": {"type": "string"},
                },
                "required": ["type", "text"],
            },
            "reusable_rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "rule": {"type": "string"},
                        "why": {"type": "string"},
                        "example_from_script": {"type": "string"},
                    },
                    "required": ["rule", "why", "example_from_script"],
                },
            },
            "do_not_learn": {"type": "array", "items": {"type": "string"}},
            "tags": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "platform",
            "content_type",
            "hook",
            "target_audience",
            "decision_tension",
            "structure_steps",
            "save_worthy_lines",
            "cta",
            "reusable_rules",
            "do_not_learn",
            "tags",
        ],
    },
}
def parse_learn_script_message(text: str) -> tuple[dict[str, str], str]:
    raw = (text or "").strip()
    body = re.sub(r"^/learn_script(?:@\w+)?", "", raw, count=1).lstrip()
    if not body:
        return {}, ""
    lines = body.splitlines()
    metadata: dict[str, str] = {}
    script_start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            script_start = i + 1
            break
        if ":" not in stripped:
            script_start = i
            break
        key, val = stripped.split(":", 1)
        key_norm = key.strip().lower()
        if key_norm in {"platform", "type", "performance"}:
            metadata[key_norm] = val.strip()
            script_start = i + 1
        else:
            script_start = i
            break
    script_text = "\n".join(lines[script_start:]).strip()
    return metadata, script_text
def analyze_script(client: OpenAI, script_text: str, metadata: dict[str, str]) -> dict[str, Any]:
    meta = json.dumps(metadata, ensure_ascii=False)
    resp = client.chat.completions.create(
        model=LEARN_MODEL,
        temperature=0.1,
        response_format={"type": "json_schema", "json_schema": SCHEMA},
        messages=[
            {
                "role": "system",
                "content": "Extract reusable content rules from a viral script. Return JSON only and follow schema strictly.",
            },
            {
                "role": "user",
                "content": (
                    "Analyze the script and output only the requested JSON schema. "
                    "Do not include markdown. Keep fields concise and actionable. "
                    f"Metadata hints: {meta}\n\nScript:\n{script_text}"
                ),
            },
        ],
    )
    content = (resp.choices[0].message.content or "{}").strip()
    data = json.loads(content)
    if not isinstance(data, dict):
        raise ValueError("Invalid analysis payload")
    return data
def _meta_line(metadata: dict[str, str], analysis: dict[str, Any]) -> str:
    platform = metadata.get("platform") or str(analysis.get("platform") or "")
    ctype = metadata.get("type") or str(analysis.get("content_type") or "")
    performance = metadata.get("performance") or ""
    parts = [f"platform={platform}", f"type={ctype}"]
    if performance:
        parts.append(f"performance={performance}")
    return " | ".join(parts)
def _entry_text(ts: str, metadata: dict[str, str], analysis: dict[str, Any], script_hash: str, excerpt: str) -> str:
    hook = analysis.get("hook") or {}
    hook_text = str(hook.get("text") or "")
    hook_types = ", ".join(hook.get("type") or [])
    lines = analysis.get("save_worthy_lines") or []
    rules = analysis.get("reusable_rules") or []
    line_items = lines[:3]
    while len(line_items) < 3:
        line_items.append("")
    rule_lines = "\n".join(
        f"- {r.get('rule','')} — why: {r.get('why','')} | example: {r.get('example_from_script','')[:120]}"
        for r in rules
    )
    why_worked = "The script pairs a concrete hook with decision-useful details and clear CTA, making it save-worthy and easy to act on."
    excerpt_line = f"excerpt: {excerpt}" if excerpt else "excerpt:"
    return (
        f"\n## {ts}\n"
        f"- timestamp_utc: {ts}\n"
        f"- metadata: {_meta_line(metadata, analysis)}\n"
        f"- hook: {hook_text} ({hook_types})\n"
        f"- script_hash: {script_hash}\n"
        f"- {excerpt_line}\n"
        f"- save_worthy_lines:\n"
        f"  - {line_items[0]}\n"
        f"  - {line_items[1]}\n"
        f"  - {line_items[2]}\n"
        f"- reusable_rules:\n{rule_lines if rule_lines else '- (none)'}\n"
        f"- why_it_worked: {why_worked}\n"
    )
def _failure_entry(ts: str, metadata: dict[str, str], analysis: dict[str, Any], script_hash: str) -> str:
    blocked = analysis.get("do_not_learn") or []
    lines = "\n".join(f"- {x}" for x in blocked) if blocked else "- (none)"
    return (
        f"\n## {ts}\n"
        f"- timestamp_utc: {ts}\n"
        f"- metadata: {_meta_line(metadata, analysis)}\n"
        f"- script_hash: {script_hash}\n"
        f"- do_not_learn:\n{lines}\n"
    )
def _ensure_skill_files() -> None:
    SKILLS_DIR.mkdir(parents=True, exist_ok=True)
    for name in [
        "hooks.md",
        "cost_breakdown.md",
        "avoid_pitfalls.md",
        "local_weekend.md",
        "booking_strategy.md",
        "tools.md",
        "misc.md",
        "win_log.md",
        "failure_log.md",
    ]:
        fp = SKILLS_DIR / name
        if not fp.exists():
            fp.write_text(f"# {name.replace('_', ' ').replace('.md', '').title()}\n", encoding="utf-8")
def store_learning(metadata: dict[str, str], analysis: dict[str, Any], script_text: str, tg: dict[str, int]) -> dict[str, Any]:
    cols = get_cols()
    if cols is None:
        raise RuntimeError("DB unavailable")
    skill_ingests, skill_rules, skill_logs = cols
    now = now_utc_iso()
    script_hash = hashlib.sha256(script_text.encode("utf-8")).hexdigest()
    ing_id = f"ing:{script_hash[:16]}"
    hook = analysis.get("hook") or {}
    hook_text = str(hook.get("text") or "")
    hook_types = hook.get("type") or []
    if not isinstance(hook_types, list):
        hook_types = []
    content_type = str(analysis.get("content_type") or "other")
    tags = analysis.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    rules = analysis.get("reusable_rules") or []
    if not isinstance(rules, list):
        rules = []
    skill_ingests.update_one(
        {"_id": ing_id},
        {
            "$setOnInsert": {
                "_id": ing_id,
                "script_hash": script_hash,
                "created_at_utc": now,
            },
            "$set": {
                "tg": tg,
                "meta": {
                    "platform": metadata.get("platform") or "",
                    "type": metadata.get("type") or "",
                    "performance": metadata.get("performance") or "",
                },
                "analysis": analysis,
                "hook_text": hook_text,
                "content_type": content_type,
                "tags": tags,
                "rules_count": len(rules),
            },
        },
        upsert=True,
    )
    new_rules = 0
    updated_rules = 0
    rules_valid = 0
    for item in rules:
        if not isinstance(item, dict):
            continue
        rule_text = str(item.get("rule") or "").strip()
        if not rule_text:
            continue
        rules_valid += 1
        rid = f"rule:{hashlib.sha1(rule_text.encode('utf-8')).hexdigest()}"
        result = skill_rules.update_one(
            {"_id": rid},
            {
                "$setOnInsert": {
                    "rule": rule_text,
                    "first_seen_at_utc": now,
                    "sources": [],
                },
                "$set": {
                    "why": str(item.get("why") or ""),
                    "example_from_script": str(item.get("example_from_script") or ""),
                    "content_type": content_type,
                    "hook_type": [str(x) for x in hook_types],
                    "tags": [str(x) for x in tags],
                    "last_seen_at_utc": now,
                },
                "$inc": {"seen_count": 1},
                "$addToSet": {
                    "sources": {
                        "ing_id": ing_id,
                        "script_hash": script_hash,
                        "created_at_utc": now,
                    }
                },
            },
            upsert=True,
        )
        if result.upserted_id:
            new_rules += 1
        else:
            updated_rules += 1
    win_summary = f"{hook_text[:120]} | rules={rules_valid} | type={content_type}".strip(" |")
    win_now = now_utc_iso()
    tg_chat_id = int(tg.get("chat_id") or 0)
    tg_message_id = int(tg.get("message_id") or 0)
    win_key = f"win{script_hash}{win_now}{tg_chat_id}{tg_message_id}"
    skill_logs.insert_one(
        {
            "_id": f"log:{hashlib.sha1(win_key.encode('utf-8')).hexdigest()}",
            "log_type": "win",
            "created_at_utc": win_now,
            "script_hash": script_hash,
            "hook_text": hook_text,
            "content_type": content_type,
            "summary": win_summary,
            "do_not_learn": [],
        }
    )
    do_not_learn = analysis.get("do_not_learn") or []
    if isinstance(do_not_learn, list) and do_not_learn:
        fail_now = now_utc_iso()
        fail_key = f"failure{script_hash}{fail_now}{tg_chat_id}{tg_message_id}"
        skill_logs.insert_one(
            {
                "_id": f"log:{hashlib.sha1(fail_key.encode('utf-8')).hexdigest()}",
                "log_type": "failure",
                "created_at_utc": fail_now,
                "script_hash": script_hash,
                "hook_text": hook_text,
                "content_type": content_type,
                "summary": "",
                "do_not_learn": [str(x) for x in do_not_learn],
            }
        )
    updated_files: list[str] = []
    if SKILLS_WRITE_FILES:
        _ensure_skill_files()
        ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        excerpt = script_text[:200].replace("\n", " ").strip()
        entry = _entry_text(ts, metadata, analysis, script_hash, excerpt)
        type_file = CONTENT_FILE_MAP.get(content_type, "misc.md")
        for name in ["hooks.md", type_file, "win_log.md"]:
            with (SKILLS_DIR / name).open("a", encoding="utf-8") as f:
                f.write(entry)
            updated_files.append(f"skills/{name}")
        failure_entry = _failure_entry(ts, metadata, analysis, script_hash)
        with (SKILLS_DIR / "failure_log.md").open("a", encoding="utf-8") as f:
            f.write(failure_entry)
        updated_files.append("skills/failure_log.md")
    return {
        "script_hash": script_hash,
        "rules_processed": rules_valid,
        "new_rules": new_rules,
        "updated_rules": updated_rules,
        "updated_files": updated_files,
    }
