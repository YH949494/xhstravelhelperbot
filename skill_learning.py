import fcntl
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from openai import OpenAI

LEARN_MODEL = os.getenv("OPENAI_MODEL_LEARN_SCRIPT", os.getenv("OPENAI_MODEL_NOTE", "gpt-4o-mini"))
SKILLS_DIR = Path("skills")

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


def _append_with_lock(path: Path, entry: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(entry)
        f.flush()
        os.fsync(f.fileno())
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


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


def store_learning(metadata: dict[str, str], analysis: dict[str, Any], script_text: str) -> tuple[list[str], str]:
    _ensure_skill_files()
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    script_hash = hashlib.sha256(script_text.encode("utf-8")).hexdigest()
    excerpt = script_text[:200].replace("\n", " ").strip()

    entry = _entry_text(ts, metadata, analysis, script_hash, excerpt)
    content_type = str(analysis.get("content_type") or "other")
    type_file = CONTENT_FILE_MAP.get(content_type, "misc.md")

    updated = []
    for name in ["hooks.md", type_file, "win_log.md"]:
        _append_with_lock(SKILLS_DIR / name, entry)
        updated.append(f"skills/{name}")

    failure_entry = _failure_entry(ts, metadata, analysis, script_hash)
    _append_with_lock(SKILLS_DIR / "failure_log.md", failure_entry)
    updated.append("skills/failure_log.md")
    return updated, script_hash
