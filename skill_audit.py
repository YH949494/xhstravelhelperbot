from pathlib import Path

SKILLS_DIR = Path("skills")
AUDIT_FILES = [
    "hooks.md",
    "cost_breakdown.md",
    "avoid_pitfalls.md",
    "local_weekend.md",
    "booking_strategy.md",
    "tools.md",
    "misc.md",
]


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []


def _count_bullets(lines: list[str]) -> int:
    return sum(1 for line in lines if line.lstrip().startswith(("-", "•")))


def _split_entries(text: str) -> list[str]:
    normalized = text.replace("\r\n", "\n")
    if "\n## " not in normalized and not normalized.lstrip().startswith("## "):
        return []
    chunks = [chunk.strip() for chunk in normalized.split("\n## ") if chunk.strip()]
    if normalized.lstrip().startswith("## ") and chunks:
        chunks[0] = f"## {chunks[0]}"
    return chunks


def _first_value_line(entry: str, key: str) -> str:
    key_prefix = f"- {key}:"
    for line in entry.splitlines():
        stripped = line.strip()
        if stripped.startswith(key_prefix):
            return stripped[len(key_prefix):].strip()
    return ""


def _first_non_empty(entry: str) -> str:
    for line in entry.splitlines():
        stripped = line.strip().lstrip("- ").strip()
        if stripped and not stripped.startswith("## "):
            return stripped
    return "(empty)"


def _latest_win_lines(path: Path, limit: int = 5) -> list[str]:
    if not path.exists():
        return []
    entries = _split_entries(path.read_text(encoding="utf-8"))
    out: list[str] = []
    for entry in reversed(entries):
        hook = _first_value_line(entry, "hook")
        out.append(hook if hook else _first_non_empty(entry))
        if len(out) >= limit:
            break
    return out


def _latest_failure_lines(path: Path, limit: int = 3) -> list[str]:
    if not path.exists():
        return []
    entries = _split_entries(path.read_text(encoding="utf-8"))
    out: list[str] = []
    for entry in reversed(entries):
        value = _first_value_line(entry, "do_not_learn")
        out.append(value if value else _first_non_empty(entry))
        if len(out) >= limit:
            break
    return out


def _truncate(text: str, max_len: int = 3900) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - len("\n…(truncated)")].rstrip() + "\n…(truncated)"


def build_skill_audit_message() -> str | None:
    if not SKILLS_DIR.exists() or not SKILLS_DIR.is_dir():
        return None

    lines: list[str] = ["Skill audit", ""]

    total_rules = 0
    lines.append("Files:")
    for name in AUDIT_FILES:
        path = SKILLS_DIR / name
        file_lines = _read_lines(path) if path.exists() else []
        bullet_count = _count_bullets(file_lines)
        total_rules += bullet_count
        exists = "yes" if path.exists() else "no"
        lines.append(f"- {name}: exists={exists}, lines={len(file_lines)}, bullets={bullet_count}")

    lines.append("")
    lines.append(f"Total rules learned (approx): {total_rules}")

    win_lines = _latest_win_lines(SKILLS_DIR / "win_log.md", limit=5)
    lines.append("")
    lines.append("Latest wins:")
    if win_lines:
        for item in win_lines:
            lines.append(f"- {item}")
    else:
        lines.append("- (none)")

    fail_lines = _latest_failure_lines(SKILLS_DIR / "failure_log.md", limit=3)
    lines.append("")
    lines.append("Latest failures:")
    if fail_lines:
        for item in fail_lines:
            lines.append(f"- {item}")
    else:
        lines.append("- (none)")

    return _truncate("\n".join(lines))
