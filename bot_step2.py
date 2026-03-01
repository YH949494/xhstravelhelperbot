import os
import json
import random
import asyncio
import string
import logging
import re
import shlex
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from openai import OpenAI
from db_atlas import ensure_indexes, get_db_error, ping
from skill_learning import analyze_script, parse_learn_script_message, store_learning
from skill_audit import build_skill_audit_message

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("step2")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
APPROVAL_CHAT_ID = int(os.getenv("APPROVAL_CHAT_ID", "0"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
MODEL_TITLES = os.getenv("OPENAI_MODEL_TITLES", "gpt-4o-mini")
OPENAI_MODEL_NOTE = os.getenv("OPENAI_MODEL_NOTE", "gpt-4o-mini")
OPENAI_MODEL_SCRIPT = os.getenv("OPENAI_MODEL_SCRIPT", OPENAI_MODEL_NOTE)
MAX_NOTES_PER_DAY = int(os.getenv("MAX_NOTES_PER_DAY", "2"))
NOTE_MAX_TOKENS = int(os.getenv("NOTE_MAX_TOKENS", "900"))
TZ = os.getenv("TZ", "Asia/Kuala_Lumpur")
RUN_HOUR = int(os.getenv("RUN_HOUR", "21"))
RUN_MIN = int(os.getenv("RUN_MIN", "30"))
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()}
ALLOWED_GROUP_CHAT_IDS_RAW = os.getenv("ALLOWED_GROUP_CHAT_IDS", "").strip()
ALLOWED_GROUP_CHAT_IDS = {
    int(x.strip()) for x in ALLOWED_GROUP_CHAT_IDS_RAW.split(",") if x.strip().lstrip("-").isdigit()
}
WINS_FILE = Path("/data/wins.json")
SKILL_PATH = Path("skills/xhs_travel_skill.md")
SKILLS_DIR = Path("skills")
SKILL_TEXT_CAP = 16000
SKILL_PRIORITY_ORDER = [
    "growth_rules.md",
    "script_framework.md",
    "hook_library.md",
    "series_registry.md",
    "performance_log.md",
    "failure_log.md",
]
RULE_SKILL_FILES = {"growth_rules.md", "script_framework.md", "hook_library.md"}
MEMORY_SKILL_FILES = {"performance_log.md", "failure_log.md", "series_registry.md"}
MY_LOCAL_KEYWORDS = ["马来西亚", "大马", "malaysia", "my", "kl", "吉隆坡", "雪兰莪", "森美兰", "槟城", "怡保", "马六甲", "金马仑", "波德申", "云顶", "东海岸"]
OVERSEAS_KEYWORDS = ["日本", "韩国", "欧洲", "美国", "泰国", "越南", "巴厘", "新加坡"]
DEFAULT_REGIONS_POOL = ["penang", "genting", "melaka", "selangor", "kl", "perak", "johor", "sabah"]

if not TG_TOKEN or not OPENAI_API_KEY or not APPROVAL_CHAT_ID:
    raise RuntimeError("Missing env: TELEGRAM_BOT_TOKEN / OPENAI_API_KEY / APPROVAL_CHAT_ID")

client = OpenAI(api_key=OPENAI_API_KEY)
tzinfo = ZoneInfo(TZ)


def make_content_id(now: datetime) -> str:
    rand4 = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return now.strftime("%Y%m%d-%H%M") + "-" + rand4


def load_skill_text() -> str:
    try:
        if SKILL_PATH.exists():
            return SKILL_PATH.read_text(encoding="utf-8").strip()
        return ""
    except Exception:
        return ""


def ensure_default_skill_files(skills_dir: str = "skills") -> None:
    base = Path(skills_dir)
    base.mkdir(parents=True, exist_ok=True)
    defaults = {
        "growth_rules.md": "# Growth Rules\n\n## Placeholder\n- Add growth rules here.\n",
        "hook_library.md": "# Hook Library\n\n## Placeholder\n- Add proven hook patterns here.\n",
        "script_framework.md": "# Script Framework\n\n## Placeholder\n- Add script framework notes here.\n",
        "performance_log.md": "# Performance Log\n\n## Placeholder\n- Add win records and insights here.\n",
        "failure_log.md": "# Failure Log\n\n## Placeholder\n- Add failed hooks/titles and lessons here.\n",
        "series_registry.md": "# Series Registry\n\n## Placeholder\n- Add recurring content series notes here.\n",
    }
    for name, content in defaults.items():
        fp = base / name
        if not fp.exists():
            fp.write_text(content, encoding="utf-8")


def load_skill_texts(skills_dir: str = "skills") -> list[tuple[str, str]]:
    base = Path(skills_dir)
    if not base.exists():
        return []
    ordered: list[Path] = []
    for name in SKILL_PRIORITY_ORDER:
        fp = base / name
        if fp.exists() and fp.suffix.lower() == ".md":
            ordered.append(fp)
    others = sorted(
        [p for p in base.glob("*.md") if p.name not in {x.name for x in ordered}],
        key=lambda x: x.name.lower(),
    )
    files = ordered + others
    loaded: list[tuple[str, str]] = []
    for fp in files:
        try:
            text = fp.read_text(encoding="utf-8").strip()
            if text:
                loaded.append((fp.name, text))
        except Exception:
            log.exception("failed to load skill file: %s", fp)

    if not loaded:
        return loaded

    kept = list(loaded)
    protected = set(RULE_SKILL_FILES)
    while sum(len(t) for _, t in kept) > SKILL_TEXT_CAP:
        idx = None
        for i in range(len(kept) - 1, -1, -1):
            if kept[i][0] not in protected:
                idx = i
                break
        if idx is None:
            idx = len(kept) - 1
        kept.pop(idx)
        if not kept:
            break
    return kept


def _build_skill_sections(skills: list[tuple[str, str]]) -> tuple[str, str]:
    system_sections = []
    context_sections = []
    for name, text in skills:
        block = f"=== SKILL: {name} ===\n{text}"
        if name in RULE_SKILL_FILES:
            system_sections.append(block)
        elif name in MEMORY_SKILL_FILES:
            context_sections.append(block)
        else:
            system_sections.append(block)

    if SKILL_PATH.exists() and not any(name == SKILL_PATH.name for name, _ in skills):
        legacy = load_skill_text()
        if legacy:
            system_sections.append(f"=== SKILL: {SKILL_PATH.name} ===\n{legacy}")
    return "\n\n".join(system_sections).strip(), "\n\n".join(context_sections).strip()


def is_valid_hook(title: str) -> bool:
    return _hook_validation_reason(title)[0]


def _hook_validation_reason(title: str) -> tuple[bool, str]:
    t = (title or "").strip()
    if len(t) < 10 or len(t) > 32:
        return False, "length_not_in_10_32"
    specificity_ok = bool(re.search(r"\d", t)) or any(x in t for x in ["RM", "¥", "元", "%", "分钟", "小时", "天"])
    if not specificity_ok:
        return False, "missing_specificity_signal"
    tension_keywords = ["千万别", "别做", "别买", "别去", "避坑", "踩雷", "坑", "亏", "浪费", "后悔", "被宰", "被骗", "隐藏", "真相", "规则", "别按", "别选", "不要", "别", "错"]
    has_tension = any(k in t for k in tension_keywords)
    if not has_tension:
        return False, "missing_tension_signal"
    if any(k in t for k in ["分享", "合集", "推荐", "攻略"]) and not has_tension:
        return False, "generic_filler_without_tension"
    return True, "ok"


def append_failure_log_line(title: str, reason: str, skills_dir: str = "skills") -> None:
    fp = Path(skills_dir) / "failure_log.md"
    line = f"- {datetime.now(tzinfo).isoformat()} | rejected_title={title} | reason={reason}\n"
    try:
        with fp.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        log.exception("failed to append failure log")


TITLE_PROMPT = """
你是一个小红书旅行增长标题生成引擎。

创作者定位：
- 聪明旅行者
- 成本敏感
- 效率优化
- 旅行hack分享者
- 决策辅助型内容

任务：
生成 6 个小红书选题。

分布要求：
- 2 条 增长型（高收藏/高传播）
- 2 条 转化型（提升关注）
- 2 条 信任型（花费拆解/避坑/清单）

每条必须输出 JSON 对象字段：
- bucket (growth|conversion|trust)
- title（中文标题，适合小红书）
- angle（中文，具体角度）
- target_audience（中文）
- cta（固定为：Follow / 收藏小红书）

硬性规则：
- 标题必须是中文
- 必须包含数字 / 金额 / 时间 / 对比 / 避坑 等至少一个
- 必须具体，不允许泛泛而谈
- 禁止英文标题
- 返回格式：{"items":[ ...6条... ]}
- 不要使用 ```json 代码块
- 选题必须聚焦马来西亚本地旅行（如KL/雪兰莪/槟城/怡保/马六甲/金马仑/波德申/云顶/东海岸）
- 每条标题至少包含一个元素：RM预算 OR 周末 OR 2天1夜/1天
- 禁止出现海外目的地关键词：日本/韩国/欧洲/美国/泰国/越南/巴厘/新加坡
"""


NOTE_SYSTEM = """🎯 角色定义
你是一个 旅行增长内容引擎。
核心目标：
帮助用户做更好的旅行决策，并生产：
可收藏内容
决策辅助内容
可转化内容
实用旅行洞察
内容优先级：实用 > 共鸣 > 娱乐
🎯 创作者人设
创作者定位为：
聪明旅行者
效率优化者
成本敏感旅行者
旅行 hack 分享者
真实经验验证者
禁止输出：
纯打卡分享
情绪日记
仅美学内容
无决策价值内容
🎯 内容范围（必须命中）
所有内容必须属于以下之一：
旅行 hack
避坑指南
花费拆解
隐藏技巧
预订策略
机场生存
工具推荐
行程优化
防骗指南
旅行效率洞察
若 topic 不匹配，自动重构。
🎯 Hook 规则（强制）
Hook 必须同时包含 ≥2：
地点背景
明确收益
好奇触发
情绪触发
决策框架
禁止抽象模糊。
Hook ≤12字。
🎯 内容真实感规则（合并版）
每条内容必须同时包含：
① 场景感
至少1句感官描述：
声音
温度
氛围
环境体验
② 行为证据
至少1句真实行为或情绪：
做了什么
当时发生什么
体验反应
目的：形成“在场感”。
🎯 决策辅助规则
每条内容必须帮助回答：
👉 我要不要去？
因此必须包含至少1项：
适合谁
不适合谁
优点 vs 缺点
期待管理
🎯 Caption 结构规则
Caption 必须包含：
情境共鸣
价值定位
2–3 个信息点
体验句
Save 触发
避免泛形容词。
🎯 情绪 + 实用平衡
内容必须同时具备：
情绪画面感
决策信息
禁止单维内容。
🎯 输出格式规则
必须：
中文（小红书语境）
短句
可扫读
可复制
Hook ≤12字
优先 bullet
🎯 选题规则
优先：
高具体度
决策相关
mistake framing
成本 / 时间优化
禁止：
泛城市攻略
🎯 飞轮规则
若用户说内容表现好：
生成5个相关角度
构建 topic cluster
保持定位
🎯 变现感知
内容可自然支持：
酒店决策
预订决策
工具使用
旅行消费
禁止硬推 affiliate。
🎯 默认输出模板（必须）
🎬 POST SCRIPT
Hook
[≤12字]
Point 1
[洞察]
Point 2
[洞察]
Point 3（可选）
[洞察]
Credibility line
[真实信号]
Save trigger
[收藏理由]
✍️ CAPTION
[结构化短 caption]
🏷 HASHTAGS
5–8个垂类标签
💡 VISUAL IDEA
描述拍摄建议"""


def build_note_user_prompt(title: str, angle: str, audience: str) -> str:
    return (
        "请基于以下输入，生成 1 条完整小红书旅行笔记。\n"
        f"标题: {title}\n"
        f"角度: {angle}\n"
        f"目标人群: {audience}\n"
        "强制要求:\n"
        "1) 使用默认输出模板且字段顺序完全一致。\n"
        "2) CTA 必须包含：Follow / 收藏小红书。\n"
        "3) 不能硬推 affiliate。\n"
        "4) 必须中文、短句、可扫读、可复制。\n"
    )


def _extract_hook_line(note_text: str) -> tuple[int | None, str]:
    lines = note_text.splitlines()
    for i, line in enumerate(lines):
        if line.strip().lower() == "hook":
            for j in range(i + 1, len(lines)):
                cand = lines[j].strip()
                if cand:
                    return j, cand
            return None, ""
    return None, ""


def _hook_elements_count(hook: str) -> int:
    groups = [
        ["机场", "酒店", "吉隆坡", "槟城", "曼谷", "东京", "首尔", "海关", "航站楼", "城市"],  # 地点背景
        ["省", "省钱", "省时", "便宜", "少花", "不踩坑", "效率", "值", "更快", "更稳"],  # 明确收益
        ["为什么", "竟然", "原来", "你不知道", "才发现", "真相"],  # 好奇触发
        ["崩溃", "后悔", "焦虑", "救命", "血亏", "安心", "庆幸"],  # 情绪触发
        ["适合", "不适合", "优缺点", "要不要", "vs", "对比", "先看"],  # 决策框架
    ]
    return sum(1 for kws in groups if any(k in hook for k in kws))


def _hook_valid(hook: str) -> bool:
    return bool(hook) and len(hook) <= 12 and _hook_elements_count(hook) >= 2


def _repair_hook(hook: str, title: str, angle: str, audience: str) -> str | None:
    try:
        prompt = (
            "把下面的 Hook 改写成 <=12 字，且至少包含以下 5 类中的 2 类："
            "地点背景/明确收益/好奇触发/情绪触发/决策框架。"
            "只输出一行 Hook，不要任何解释。\n"
            f"原标题: {title}\n角度: {angle}\n目标人群: {audience}\n原Hook: {hook}"
        )
        resp = client.chat.completions.create(
            model=OPENAI_MODEL_NOTE,
            messages=[
                {"role": "system", "content": "你是小红书旅行文案编辑，只返回最终 Hook 一行。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0.4,
            max_tokens=60,
        )
        fixed = (resp.choices[0].message.content or "").strip().splitlines()[0].strip()
        return fixed or None
    except Exception:
        log.exception("hook repair failed")
        return None


def _replace_hook(note_text: str, new_hook: str) -> str:
    lines = note_text.splitlines()
    idx, _ = _extract_hook_line(note_text)
    if idx is None:
        return note_text
    lines[idx] = new_hook
    return "\n".join(lines)


def _extract_json(text: str) -> str:
    s = (text or "").strip()
    if s.startswith("```"):
        lines = s.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    return s


def _wins_default() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": datetime.now(tzinfo).isoformat(),
        "items": [],
    }


def _persist_wins_doc(doc: dict[str, Any]) -> bool:
    data_dir = WINS_FILE.parent
    if not data_dir.exists():
        log.error("Wins volume is not mounted: %s", data_dir)
        return False
    doc["updated_at"] = datetime.now(tzinfo).isoformat()
    data_dir.mkdir(parents=True, exist_ok=True)
    tmp_file = WINS_FILE.with_name(f"{WINS_FILE.name}.tmp")
    tmp_file.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_file.replace(WINS_FILE)
    return True


def load_wins() -> tuple[list[dict], str | None]:
    data_dir = WINS_FILE.parent
    if not data_dir.exists():
        log.error("Wins volume is not mounted: %s", data_dir)
        return [], "⚠️ /data 未挂载，已跳过爆款学习。"

    if not WINS_FILE.exists():
        _persist_wins_doc(_wins_default())
        return [], None

    try:
        doc = json.loads(WINS_FILE.read_text(encoding="utf-8"))
        items = doc.get("items") if isinstance(doc, dict) else []
        if not isinstance(items, list):
            raise ValueError("wins items is not list")
        return items, None
    except Exception:
        log.exception("wins.json corrupted, backing up and resetting")
        ts = datetime.now(tzinfo).strftime("%Y%m%d_%H%M%S")
        bak = WINS_FILE.with_name(f"wins.json.bak.{ts}")
        try:
            if WINS_FILE.exists():
                WINS_FILE.replace(bak)
        except Exception:
            log.exception("failed to backup corrupted wins file")
        _persist_wins_doc(_wins_default())
        return [], f"⚠️ wins.json 已损坏，已备份为 {bak.name} 并重置。"


def append_win(item: dict[str, Any]) -> tuple[bool, str | None]:
    wins, warning = load_wins()
    doc = _wins_default()
    doc["items"] = wins
    doc["items"].append(item)
    ok = _persist_wins_doc(doc)
    return ok, warning


def summarize_wins(wins: list[dict]) -> str:
    recent = wins[-30:]
    if not recent:
        return "- 最近爆款结构：暂无样本\n- 高频元素：优先测试 RM预算 + 周末/2天1夜\n- 建议延伸：1) 本地低预算 2) 交通避坑 3) 花费拆解 4) 清单模板 5) 冷门短途"

    texts = []
    for w in recent:
        texts.append(" ".join([
            str(w.get("title") or ""),
            str(w.get("notes") or ""),
            " ".join(w.get("tags") or []),
        ]))
    merged = " ".join(texts)

    rm_hits = re.findall(r"RM\s*\d+", merged, flags=re.IGNORECASE)
    places = [k for k in ["KL", "雪兰莪", "Selangor", "槟城", "Penang", "怡保", "Ipoh", "马六甲", "Melaka"] if re.search(re.escape(k), merged, flags=re.IGNORECASE)]
    topics = [k for k in ["2D1N", "3D2N", "周末", "staycation", "森林", "冷门", "避坑", "花费拆解", "清单"] if re.search(re.escape(k), merged, flags=re.IGNORECASE)]

    rm_top = "/".join(rm_hits[:3]) if rm_hits else "RM预算"
    place_top = "、".join(places[:4]) if places else "KL/雪兰莪"
    topic_top = "、".join(topics[:6]) if topics else "周末、避坑、花费拆解"

    return (
        f"- 最近爆款结构：以本地短途 + 具体预算切入，常见金额锚点 {rm_top}。\n"
        f"- 高频元素：地区 {place_top}；题材 {topic_top}。\n"
        "- 建议延伸：1) RM100-300周末路线 2) 2天1夜交通组合 3) 酒店/景点避坑 4) 花费拆解模板 5) 冷门森林staycation"
    )


def _parse_win_command(text: str) -> tuple[dict[str, Any] | None, str | None]:
    payload = (text or "").strip()
    try:
        parts = shlex.split(payload)
    except Exception:
        return None, "❌ 参数解析失败，请检查引号。"
    if not parts or not parts[0].startswith("/win"):
        return None, "❌ 用法：/win <url> saves= likes= comments= follows= title=\"...\" note=\"...\" tags=a,b"
    if len(parts) < 2 or not parts[1].startswith("http"):
        return None, "❌ 请提供有效链接：/win <url> ..."

    data: dict[str, Any] = {
        "source": "xhs",
        "url": parts[1],
        "title": "",
        "notes": "",
        "metrics": {"saves": None, "likes": None, "comments": None, "follows": None},
        "tags": [],
        "region_focus": "MY_LOCAL",
    }

    for p in parts[2:]:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        key = k.strip().lower()
        val = v.strip().strip('"').strip("'")
        if key in ("saves", "likes", "comments", "follows"):
            data["metrics"][key] = int(val) if val.isdigit() else None
        elif key == "title":
            data["title"] = val
        elif key in ("note", "notes"):
            data["notes"] = val
        elif key == "tags":
            data["tags"] = [x.strip() for x in val.split(",") if x.strip()]

    now = datetime.now(tzinfo)
    rand4 = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    data["id"] = f"win_{now.strftime('%Y%m%d_%H%M%S')}_{rand4}"
    data["created_at"] = now.isoformat()
    return data, None


def _is_admin_user(user_id: int | None) -> bool:
    return bool(user_id and ADMIN_IDS and user_id in ADMIN_IDS)


async def summarize_script_for_learning(text: str) -> dict:
    resp = client.chat.completions.create(
        model=OPENAI_MODEL_NOTE,
        messages=[
            {"role": "system", "content": "你是一个旅行内容结构分析引擎。你不会改写内容，只提炼结构模式。"},
            {
                "role": "user",
                "content": (
                    "分析以下小红书旅行脚本，提炼：\n"
                    "1) 标题结构公式\n"
                    "2) Hook类型\n"
                    "3) 决策框架\n"
                    "4) 情绪触发点\n"
                    "5) 可复用信息结构\n"
                    "6) 5个延伸角度\n"
                    "返回JSON对象：\n"
                    "{\n"
                    " 'title_formula': '',\n"
                    " 'hook_type': '',\n"
                    " 'decision_frame': '',\n"
                    " 'emotional_trigger': '',\n"
                    " 'info_pattern': '',\n"
                    " 'topic_cluster': []\n"
                    "}\n\n"
                    f"脚本文本：\n{text}"
                ),
            },
        ],
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=600,
    )
    content = (resp.choices[0].message.content or "{}").strip()
    data = json.loads(_extract_json(content))
    return data if isinstance(data, dict) else {}


def _parse_wintext_message(text: str) -> tuple[dict[str, int], str]:
    raw = (text or "").strip()
    if not raw:
        return {}, ""
    lines = raw.splitlines()
    first = lines[0].strip()
    body = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""

    metrics: dict[str, int] = {}
    for key in ("saves", "likes", "comments", "follows"):
        m = re.search(rf"{key}\s*=\s*(\d+)", first, flags=re.IGNORECASE)
        if m:
            metrics[key] = int(m.group(1))

    if not body:
        if first.startswith("/wintext"):
            body = first[len("/wintext"):].strip()
        else:
            body = first
    return metrics, body


async def generate_note(title: str, angle: str, audience: str) -> tuple[str, bool]:
    skill_pairs = load_skill_texts(str(SKILLS_DIR))
    system_skills, context_skills = _build_skill_sections(skill_pairs)
    system_text = NOTE_SYSTEM
    if system_skills:
        system_text = f"{NOTE_SYSTEM}\n\n{system_skills}" if NOTE_SYSTEM else system_skills
    user_prompt = build_note_user_prompt(title, angle, audience)
    if context_skills:
        user_prompt = f"{user_prompt}\n\n【参考记忆，仅供参考】\n{context_skills}"
    resp = client.chat.completions.create(
        model=OPENAI_MODEL_NOTE,
        messages=[
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.7,
        max_tokens=NOTE_MAX_TOKENS,
    )
    note_text = (resp.choices[0].message.content or "").strip()
    idx, hook = _extract_hook_line(note_text)
    needs_warning = False
    if idx is not None and not _hook_valid(hook):
        repaired = _repair_hook(hook, title, angle, audience)
        if repaired and _hook_valid(repaired):
            note_text = _replace_hook(note_text, repaired)
        else:
            needs_warning = True
    return note_text, needs_warning


def score_item(item: dict) -> dict:
    """
    Simple deterministic-ish scoring (0-40).
    """
    title = (item.get("title") or "").lower()
    angle = (item.get("angle") or "").lower()
    audience = (item.get("target_audience") or "").lower()

    def has_any(s: str, kws: list[str]) -> bool:
        return any(k in s for k in kws)

    save_score = 0
    follow_score = 0
    clarity_score = 0
    exec_score = 0

    # Save potential
    if has_any(title, ["避坑", "坑", "清单", "checklist", "别", "不要", "攻略", "省", "rm", "预算", "花费", "cost"]):
        save_score += 6
    if any(ch.isdigit() for ch in title):
        save_score += 2
    if has_any(title, ["对比", "vs", "比较"]):
        save_score += 2

    # Follow potential (series vibe / audience clarity)
    if has_any(audience, ["新手", "第一次", "懒人", "budget", "穷游", "亲子", "情侣", "上班族", "独旅", "小白"]):
        follow_score += 5
    if has_any(title, ["系列", "第", "part", "合集"]):
        follow_score += 3
    if has_any(angle, ["系列", "模板", "框架"]):
        follow_score += 2

    # Clarity
    if len(title) <= 28:
        clarity_score += 5
    if has_any(title, ["怎么", "如何", "3", "5", "7", "10", "秒", "分钟", "小时", "rm", "usd"]):
        clarity_score += 5

    # Execution (actionable)
    if has_any(angle, ["步骤", "step", "清单", "模板", "流程", "策略", "预订", "booking", "机场", "骗局", "scam"]):
        exec_score += 6
    if has_any(title, ["准备", "带什么", "买什么", "用什么", "订"]):
        exec_score += 4

    has_local = has_any(title + " " + angle + " " + audience, MY_LOCAL_KEYWORDS)
    has_budget_or_duration = has_any(title + " " + angle, ["rm", "周末", "2天1夜", "1天", "2d1n", "3d2n"])
    has_overseas = has_any(title + " " + angle + " " + audience, [x.lower() for x in OVERSEAS_KEYWORDS])

    if has_overseas:
        return {
            "save": 0,
            "follow": 0,
            "clarity": 0,
            "exec": 0,
            "total": 0,
        }

    local_bonus = 0
    if has_local and has_any(title + " " + angle, ["rm"]):
        local_bonus += 6
    elif has_local and has_budget_or_duration:
        local_bonus += 4
    elif has_local:
        local_bonus += 2

    # cap each to 0-10
    save_score = min(save_score, 10)
    follow_score = min(follow_score, 10)
    clarity_score = min(clarity_score, 10)
    exec_score = min(exec_score, 10)

    total = save_score + follow_score + clarity_score + exec_score + local_bonus
    return {
        "save": save_score,
        "follow": follow_score,
        "clarity": clarity_score,
        "exec": exec_score,
        "total": total,
    }


async def generate_6_titles(app: Application | None = None) -> list[dict]:
    skill_pairs = load_skill_texts(str(SKILLS_DIR))
    system_skills, context_skills = _build_skill_sections(skill_pairs)
    wins, warning = load_wins()
    if warning:
        log.warning(warning)
        if app:
            try:
                await app.bot.send_message(chat_id=APPROVAL_CHAT_ID, text=warning)
            except Exception:
                log.exception("failed to send wins warning")
    dynamic_prompt = (
        TITLE_PROMPT
        + "\n\n【近期爆款学习摘要】\n"
        + summarize_wins(wins)
        + "\n\n请严格按本地旅行策略出题。"
    )
    if context_skills:
        dynamic_prompt = f"{dynamic_prompt}\n\n【参考记忆，仅供参考】\n{context_skills}"

    system_text = "你是一个小红书旅行增长引擎。"
    if system_skills:
        system_text = f"{system_text}\n\n{system_skills}"
    resp = client.chat.completions.create(
        model=MODEL_TITLES,
        messages=[
            {
                "role": "system",
                "content": system_text,
            },
            {
                "role": "system",
                "content": "全部输出必须为中文（小红书语境）。只输出JSON，不要代码块。"
            },
            {
                "role": "user",
                "content": dynamic_prompt
            },
        ],
        response_format={"type": "json_object"},
        temperature=0.8,
        max_tokens=900,
    )
    content = resp.choices[0].message.content or "{}"
    try:
        data = json.loads(content)
    except Exception:
        data = json.loads(_extract_json(content))
    try:
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list) or len(items) != 6:
            raise ValueError("Not 6 items")
        return items
    except Exception as e:
        log.error("JSON parse failed: %s | raw=%s", e, content[:5000])
        raise


def _get_regions_pool() -> list[str]:
    raw = (os.getenv("REGIONS_POOL", "") or "").strip()
    if not raw:
        return DEFAULT_REGIONS_POOL
    vals = [x.strip().lower() for x in raw.split(",") if x.strip()]
    return vals if len(vals) >= 2 else DEFAULT_REGIONS_POOL


def _pick_regions_for_day(now: datetime) -> tuple[str, str]:
    regions = _get_regions_pool()
    idx = (now.timetuple().tm_yday * 2) % len(regions)
    return regions[idx], regions[(idx + 1) % len(regions)]


async def generate_5_title_candidates(region_a: str, region_b: str) -> list[dict]:
    prompt = (
        "你是小红书马来西亚旅行标题编辑。\n"
        "输出5条标题候选，必须覆盖两个地区。\n"
        f"今日地区: {region_a}, {region_b}\n"
        "硬性要求:\n"
        "1) 仅输出JSON，格式为 {\"items\":[...]}。\n"
        "2) items长度必须为5。\n"
        "3) 每条必须包含且仅包含: title, region, location_hint。\n"
        "4) region必须严格等于今日地区之一。\n"
        "5) 每条标题14-18个中文字符。\n"
        "6) 每条标题必须包含可搜索的具体地点名，不能只写州名/大区。\n"
        "7) 风格配比: 第一人称2条、价格/冲突2条、时间/条件1条。\n"
        "8) 禁止海外目的地。"
    )

    banned_generic = {"好去处", "攻略", "推荐", "周末"}
    region_words = {region_a.lower(), region_b.lower(), "penang", "genting", "melaka", "selangor", "kl", "perak", "johor", "sabah"}

    def _validate_items(items: Any) -> tuple[bool, str]:
        if not isinstance(items, list) or len(items) != 5:
            return False, "items must be list of 5"
        seen_regions = set()
        location_signal_fails = 0
        for i, it in enumerate(items, start=1):
            if not isinstance(it, dict):
                return False, f"item#{i} must be object"
            title = str(it.get("title", "")).strip()
            region = str(it.get("region", "")).strip().lower()
            location_hint = str(it.get("location_hint", "")).strip()
            if not title or not region or not location_hint:
                return False, f"item#{i} has empty fields"
            cjk_chars = re.findall(r"[\u4e00-\u9fff]", title)
            cjk_len = len(cjk_chars)
            latin_tokens = re.findall(r"[A-Za-z0-9]+", title)
            bonus = 2 if latin_tokens else 0
            effective_len = cjk_len + bonus
            if effective_len < 14 or effective_len > 18:
                return False, f"item#{i} title length out of range"
            location_keywords = ["Hotel", "Resort", "Cabin", "Airbnb", "Forest", "Villa", "Homestay"]
            has_upper_word = bool(re.search(r"\b[A-Z][a-zA-Z]+\b", title))
            has_location_kw = any(k in title for k in location_keywords)
            if not has_upper_word and not has_location_kw:
                location_signal_fails += 1
            if region not in {region_a, region_b}:
                return False, f"item#{i} region invalid"
            if len(location_hint) < 2 or location_hint.lower() == region:
                return False, f"item#{i} location_hint too generic"
            if any(b in title for b in banned_generic):
                return False, f"item#{i} title contains banned generic phrase"
            compact = re.sub(r"[\s/、，,。.!！?？\-]+", "", title).lower()
            if compact in region_words:
                return False, f"item#{i} title is generic region word"
            seen_regions.add(region)
        if location_signal_fails > 1:
            return False, "too many items lack concrete location signal"
        if region_a not in seen_regions or region_b not in seen_regions:
            return False, "items do not cover both regions"
        return True, "ok"

    last_err = ""
    for _ in range(3):
        resp = client.chat.completions.create(
            model=MODEL_TITLES,
            messages=[
                {"role": "system", "content": "你只返回合法JSON对象，不要代码块，不要解释。"},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.6,
            max_tokens=700,
        )
        content = (resp.choices[0].message.content or "{}").strip()
        try:
            data = json.loads(_extract_json(content))
        except Exception:
            last_err = "invalid JSON"
            continue
        items = data.get("items") if isinstance(data, dict) else None
        ok, reason = _validate_items(items)
        if ok:
            return items
        last_err = reason

    raise ValueError(f"title candidates validation failed after retries: {last_err}")


def format_titles_message(content_id: str, regions: list[str], items: list[dict]) -> str:
    lines = [
        "📌 今日 5 条标题候选（待选择）",
        f"🆔 content_id: {content_id}",
        f"🌍 区域轮换: {regions[0]} / {regions[1]}",
        "",
    ]
    for i, it in enumerate(items, start=1):
        lines.append(f"{i}. {it.get('title','').strip()}")
    return "\n".join(lines).strip()


def approval_keyboard(content_id: str) -> InlineKeyboardMarkup:
    kb = [
        [
            InlineKeyboardButton("✅ 选 1", callback_data=f"pick:1:{content_id}"),
            InlineKeyboardButton("✅ 选 2", callback_data=f"pick:2:{content_id}"),
            InlineKeyboardButton("✅ 选 3", callback_data=f"pick:3:{content_id}"),
        ],
        [
            InlineKeyboardButton("✅ 选 4", callback_data=f"pick:4:{content_id}"),
            InlineKeyboardButton("✅ 选 5", callback_data=f"pick:5:{content_id}"),
        ],
        [
            InlineKeyboardButton("🧾 生成脚本", callback_data=f"generate:{content_id}"),
            InlineKeyboardButton("🧹 清空选择", callback_data=f"clear:{content_id}"),
        ],
        [
            InlineKeyboardButton("🔁 重生成", callback_data=f"regen:{content_id}"),
        ],
    ]
    return InlineKeyboardMarkup(kb)


def build_script_prompt(title: str, region: str, location_hint: str, loaded_skill_texts: list[tuple[str, str]]) -> str:
    skill_blocks = []
    for name, text in loaded_skill_texts:
        skill_blocks.append(f"[RULES FILE: {name}]\n{text}")
    skills_text = "\n\n".join(skill_blocks)
    return (
        "请生成1条完整小红书旅行脚本。严格按以下格式输出，标题和顺序不能变：\n\n"
        "🎬 POST SCRIPT\n"
        "Hook\n"
        "<...>\n\n"
        "正文\n"
        "<...>\n\n"
        "Save trigger\n"
        "<...>\n\n"
        "✍️ CAPTION\n"
        "<...>\n\n"
        "🏷 HASHTAGS\n"
        "<...>\n\n"
        "💡 VISUAL SHOTLIST\n"
        "- Shot 1:\n"
        "- Shot 2:\n"
        "- Shot 3:\n"
        "- Shot 4:\n"
        "- Shot 5:\n\n"
        "硬性规则:\n"
        "- 中文为主，可少量自然MY口吻词（eh/tight/chill/menu/local），不可连续英文重句。\n"
        "- 必须第一人称真实踩点感，不要出现：第一/其次/总结/今天来分享/很多人问我。\n"
        "- 必须出现1个具体地点（优先使用location_hint）、3个可拍细节、1个人物元素。\n"
        "- 结尾必须略带不完美（如 tight/没有很爽/不要expect太多）。\n"
        f"- 题目: {title}\n"
        f"- region: {region}\n"
        f"- location_hint: {location_hint}\n\n"
        f"参考规则:\n{skills_text}"
    )


def _split_script_for_telegram(script_text: str, limit: int = 3500) -> list[str]:
    sections = [x for x in re.split(r"\n(?=🎬 POST SCRIPT|✍️ CAPTION|🏷 HASHTAGS|💡 VISUAL SHOTLIST)", script_text.strip()) if x]
    if not sections:
        return [script_text]
    chunks: list[str] = []
    cur = ""
    for sec in sections:
        candidate = sec if not cur else f"{cur}\n\n{sec}"
        if len(candidate) <= limit:
            cur = candidate
        else:
            if cur:
                chunks.append(cur)
            if len(sec) <= limit:
                cur = sec
            else:
                sec_lines = sec.splitlines()
                if not sec_lines:
                    continue
                head = sec_lines[0]
                body = sec_lines[1:]
                part = head
                for line in body:
                    cand = f"{part}\n{line}" if part else line
                    if len(cand) <= limit:
                        part = cand
                    else:
                        chunks.append(part)
                        part = f"{head}\n{line}" if len(f"{head}\n{line}") <= limit else line[:limit]
                if part:
                    cur = part
    if cur:
        chunks.append(cur)
    return chunks


async def _generate_selected_scripts(app: Application, content_id: str, draft: dict[str, Any]) -> None:
    selected = draft.get("selected") or []
    if len(selected) < 2:
        return
    if draft.get("status") == "generated":
        return
    skill_pairs = load_skill_texts(str(SKILLS_DIR))
    scripts = draft.setdefault("scripts", {})
    for idx in selected[:2]:
        if str(idx) in scripts:
            continue
        item = draft["items"][idx - 1]
        prompt = build_script_prompt(
            item.get("title", "").strip(),
            item.get("region", "").strip(),
            item.get("location_hint", "").strip(),
            skill_pairs,
        )
        resp = client.chat.completions.create(
            model=OPENAI_MODEL_SCRIPT,
            messages=[
                {"role": "system", "content": "你是小红书旅行脚本编辑。严格按用户给定格式输出。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0.7,
            max_tokens=1600,
        )
        script_text = (resp.choices[0].message.content or "").strip()
        scripts[str(idx)] = script_text
        header = f"🧾 脚本 #{idx} | {item.get('title','').strip()}"
        chunks = _split_script_for_telegram(script_text)
        if chunks:
            chunks[0] = f"{header}\n\n{chunks[0]}"
        for ch in chunks:
            await app.bot.send_message(chat_id=APPROVAL_CHAT_ID, text=ch, disable_web_page_preview=True)
    draft["status"] = "generated"


async def run_daily_job(app: Application) -> None:
    now = datetime.now(tzinfo)
    content_id = make_content_id(now)
    log.info("Running daily job content_id=%s", content_id)
    region_a, region_b = _pick_regions_for_day(now)
    try:
        items = await generate_5_title_candidates(region_a, region_b)
    except Exception:
        log.exception("daily title generation failed content_id=%s", content_id)
        await app.bot.send_message(chat_id=APPROVAL_CHAT_ID, text="❌ 今日标题生成失败，请稍后重试。")
        return

    msg = format_titles_message(content_id, [region_a, region_b], items)
    await app.bot.send_message(chat_id=APPROVAL_CHAT_ID, text=msg, reply_markup=approval_keyboard(content_id), disable_web_page_preview=True)

    app.bot_data.setdefault("drafts", {})[content_id] = {
        "created_at": now.isoformat(),
        "regions": [region_a, region_b],
        "items": items,
        "selected": [],
        "scripts": {},
        "status": "pending",
    }


async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    data = q.data or ""
    parts = data.split(":")
    if len(parts) < 2:
        log.warning("Malformed callback data: %s", data)
        return

    drafts = context.application.bot_data.setdefault("drafts", {})
    action = parts[0]

    if action == "pick":
        if len(parts) < 3:
            await q.edit_message_text("❌ 指令格式错误，请重试。")
            return
        try:
            pick_idx = int(parts[1])
        except Exception:
            await q.edit_message_text("❌ 选择序号无效，请重试。")
            return
        if pick_idx < 1 or pick_idx > 5:
            await q.edit_message_text("❌ 选择序号无效，请重试。")
            return
        content_id = parts[2]
        d = drafts.get(content_id)
        if not d:
            await q.edit_message_text("❌ 找不到该 content_id（可能重启后丢失）。请点 🔁 重生成。")
            return
        selected = d.setdefault("selected", [])

        if pick_idx in selected:
            selected.remove(pick_idx)
            selected.sort()
            d["status"] = "pending"
            await q.edit_message_text(
                format_titles_message(content_id, d.get("regions", ["-", "-"]), d.get("items", []))
                + f"\n\n✅ 已选择: {selected}",
                reply_markup=approval_keyboard(content_id),
                disable_web_page_preview=True,
            )
            return

        if len(selected) >= 2:
            await q.edit_message_text(
                format_titles_message(content_id, d.get("regions", ["-", "-"]), d.get("items", []))
                + f"\n\n⚠️ 已选满2条：{selected[:2]}，点 🧾 生成脚本 或 🔁 重生成",
                reply_markup=approval_keyboard(content_id),
                disable_web_page_preview=True,
            )
            return

        selected.append(pick_idx)
        selected.sort()

        if len(selected) >= 2:
            try:
                await _generate_selected_scripts(context.application, content_id, d)
                await q.edit_message_text(
                    format_titles_message(content_id, d.get("regions", ["-", "-"]), d.get("items", []))
                    + f"\n\n✅ 已选择: {selected[:2]}，脚本已生成。",
                    reply_markup=approval_keyboard(content_id),
                    disable_web_page_preview=True,
                )
            except Exception:
                log.exception("script generation failed content_id=%s", content_id)
                await context.application.bot.send_message(chat_id=APPROVAL_CHAT_ID, text="❌ 脚本生成失败，请稍后再试。")
        else:
            await q.edit_message_text(
                format_titles_message(content_id, d.get("regions", ["-", "-"]), d.get("items", []))
                + f"\n\n✅ 已选择: {selected}（再选1条后自动生成脚本）",
                reply_markup=approval_keyboard(content_id),
                disable_web_page_preview=True,
            )
        return

    if action == "generate":
        content_id = parts[1] if len(parts) >= 2 else ""
        d = drafts.get(content_id)
        if not d:
            await q.edit_message_text("❌ 找不到该 content_id（可能重启后丢失）。请点 🔁 重生成。")
            return
        if len(d.get("selected", [])) < 2:
            await q.edit_message_text(
                format_titles_message(content_id, d.get("regions", ["-", "-"]), d.get("items", []))
                + f"\n\n⚠️ 当前仅选中 {len(d.get('selected', []))} 条，请先选满2条。",
                reply_markup=approval_keyboard(content_id),
                disable_web_page_preview=True,
            )
            return
        try:
            await _generate_selected_scripts(context.application, content_id, d)
            await q.edit_message_text(
                format_titles_message(content_id, d.get("regions", ["-", "-"]), d.get("items", []))
                + f"\n\n✅ 已选择: {d.get('selected', [])[:2]}，脚本已生成。",
                reply_markup=approval_keyboard(content_id),
                disable_web_page_preview=True,
            )
        except Exception:
            log.exception("manual generate failed")
            await context.application.bot.send_message(chat_id=APPROVAL_CHAT_ID, text="❌ 脚本生成失败，请稍后再试。")
        return

    if action == "clear":
        content_id = parts[1] if len(parts) >= 2 else ""
        d = drafts.get(content_id)
        if not d:
            await q.edit_message_text("❌ 找不到该 content_id（可能重启后丢失）。请点 🔁 重生成。")
            return
        d["selected"] = []
        d["status"] = "pending"
        await q.edit_message_text(
            format_titles_message(content_id, d.get("regions", ["-", "-"]), d.get("items", [])) + "\n\n✅ 已清空选择。",
            reply_markup=approval_keyboard(content_id),
            disable_web_page_preview=True,
        )
        return

    if action == "regen":
        content_id = parts[1] if len(parts) >= 2 else ""
        try:
            now = datetime.now(tzinfo)
            old = drafts.get(content_id) or {}
            regions = old.get("regions") if isinstance(old.get("regions"), list) and len(old.get("regions")) >= 2 else list(_pick_regions_for_day(now))
            region_a, region_b = regions[0], regions[1]
            items = await generate_5_title_candidates(region_a, region_b)
            drafts[content_id] = {
                "created_at": now.isoformat(),
                "regions": [region_a, region_b],
                "items": items,
                "selected": [],
                "scripts": {},
                "status": "pending",
            }
            msg = format_titles_message(content_id, [region_a, region_b], items)
            await q.edit_message_text(msg, reply_markup=approval_keyboard(content_id), disable_web_page_preview=True)
        except Exception:
            log.exception("regen failed")
            await q.edit_message_text("❌ 重生成失败（OpenAI 或 JSON 格式错误）。再点一次或看日志。")
        return


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    await update.message.reply_text(
        f"chat_id={chat.id}\nchat_type={chat.type}\nuser={user.username or user.id}"
    )


async def win(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not ADMIN_IDS:
        await update.message.reply_text("❌ 请先设置 ADMIN_IDS。")
        return
    if not _is_admin_user(user.id if user else None):
        await update.message.reply_text("❌ 无权限。")
        return

    item, err = _parse_win_command(update.message.text or "")
    if err:
        await update.message.reply_text(err)
        return
    ok, warning = append_win(item)
    if warning:
        await update.message.reply_text(warning)
    if ok:
        await update.message.reply_text(f"✅ 已记录爆款样本：{item['id']}")
    else:
        await update.message.reply_text("❌ 写入失败：请检查 /data volume 挂载。")


async def wins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not ADMIN_IDS:
        await update.message.reply_text("❌ 请先设置 ADMIN_IDS。")
        return
    if not _is_admin_user(user.id if user else None):
        await update.message.reply_text("❌ 无权限。")
        return

    items, warning = load_wins()
    if warning:
        await update.message.reply_text(warning)
    last10 = items[-10:]
    if not last10:
        await update.message.reply_text("暂无爆款样本。")
        return
    lines = ["📚 最近 10 条爆款样本："]
    for it in reversed(last10):
        m = it.get("metrics") or {}
        lines.append(
            f"• {it.get('id','-')}\n"
            f"  {it.get('url','')}\n"
            f"  saves={m.get('saves')} likes={m.get('likes')}\n"
            f"  note={it.get('notes','')[:60]}"
        )
    await update.message.reply_text("\n".join(lines), disable_web_page_preview=True)


async def wintext(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not ADMIN_IDS:
        await update.message.reply_text("❌ 请先设置 ADMIN_IDS。")
        return
    if not _is_admin_user(user.id if user else None):
        await update.message.reply_text("❌ 无权限。")
        return

    msg_text = update.message.text if update.message else ""
    metrics, script_text = _parse_wintext_message(msg_text or "")
    if len(script_text) < 200:
        await update.message.reply_text("❌ 脚本文本过短，至少需要 200 字。")
        return

    try:
        learning = await summarize_script_for_learning(script_text)
    except Exception:
        log.exception("wintext summarize failed")
        await update.message.reply_text("❌ 学习失败：OpenAI 请求异常，请稍后重试。")
        return

    now = datetime.now(tzinfo)
    rand4 = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    item = {
        "id": f"win_{now.strftime('%Y%m%d_%H%M%S')}_{rand4}",
        "created_at": now.isoformat(),
        "source": "manual_script",
        "raw_length": len(script_text),
        "metrics": metrics,
        "learning": learning,
    }
    ok, warning = append_win(item)
    if warning:
        await update.message.reply_text(warning)
    if not ok:
        await update.message.reply_text("❌ 写入失败：请检查 /data volume 挂载。")
        return

    items, _ = load_wins()
    title_formula = str((learning or {}).get("title_formula") or "-")
    topic_cluster = (learning or {}).get("topic_cluster") or []
    if not isinstance(topic_cluster, list):
        topic_cluster = []
    topic_text = "、".join(str(x) for x in topic_cluster[:5]) if topic_cluster else "-"
    await update.message.reply_text(
        "✅ 已学习结构\n"
        f"标题公式: {title_formula}\n"
        f"延伸角度: {topic_text}\n"
        f"当前累计样本: {len(items)} 条"
    )


async def learn_script(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Please use /learn_script in the group.")
        return
    if ALLOWED_GROUP_CHAT_IDS and chat.id not in ALLOWED_GROUP_CHAT_IDS:
        await update.message.reply_text("❌ This group is not allowed.")
        return

    msg_text = update.message.text if update.message else ""
    metadata, script_text = parse_learn_script_message(msg_text or "")
    if len(script_text) < 200:
        await update.message.reply_text("Need full script")
        return

    db_error = get_db_error()
    if db_error:
        await update.message.reply_text(db_error)
        return

    try:
        analysis = await asyncio.to_thread(analyze_script, client, script_text, metadata)
    except Exception:
        log.exception("learn_script analyze failed")
        await update.message.reply_text("OpenAI error: failed to analyze script")
        return

    try:
        tg = {
            "chat_id": int(chat.id),
            "user_id": int(update.effective_user.id) if update.effective_user else 0,
            "message_id": int(update.message.message_id) if update.message else 0,
        }
        store_result = await asyncio.to_thread(store_learning, metadata, analysis, script_text, tg)
    except Exception:
        log.exception("learn_script store failed")
        await update.message.reply_text("Storage error: failed to save learning")
        return

    hook_text = str(((analysis.get("hook") or {}).get("text") or "")).strip()
    platform = str(analysis.get("platform") or "other")
    content_type = str(analysis.get("content_type") or "other")
    script_hash = str(store_result.get("script_hash") or "")
    rules_processed = int(store_result.get("rules_processed") or 0)
    new_rules = int(store_result.get("new_rules") or 0)
    updated_rules = int(store_result.get("updated_rules") or 0)
    await update.message.reply_text(
        f"✅ Learned & stored (MongoDB: {os.getenv('SKILLS_DB', 'xhs_travel').strip() or 'xhs_travel'})\n"
        f"platform/type: {platform}/{content_type}\n"
        f"hook: {hook_text}\n"
        f"rules processed: {rules_processed} (new: {new_rules}, updated: {updated_rules})\n"
        "db: xhs_skill_ingests, xhs_skill_rules, xhs_skill_logs\n"
        f"script_hash: {script_hash[:10]}"
    )


async def skill_audit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_error = get_db_error()
    if db_error:
        await update.message.reply_text(db_error)
        return
    message = build_skill_audit_message()
    if not message:
        await update.message.reply_text("DB unavailable")
        return
    await update.message.reply_text(message)


def main() -> None:
    ensure_default_skill_files(str(SKILLS_DIR))
    if ping():
        ensure_indexes()
    else:
        log.warning("MongoDB ping failed at startup")
    app = Application.builder().token(TG_TOKEN).build()

    # commands / handlers
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("win", win))
    app.add_handler(CommandHandler("wins", wins))
    app.add_handler(CommandHandler("wintext", wintext))
    app.add_handler(CommandHandler("learn_script", learn_script))
    app.add_handler(CommandHandler("skill_audit", skill_audit))
    app.add_handler(CallbackQueryHandler(cb_handler))

    # scheduler: 21:30 KL daily
    scheduler = AsyncIOScheduler(timezone=tzinfo)
    scheduler.add_job(
        run_daily_job,
        CronTrigger(hour=RUN_HOUR, minute=RUN_MIN, timezone=tzinfo),
        args=[app],        
        id="daily_titles",
        replace_existing=True,
        misfire_grace_time=300,
    )
    scheduler.start()

    log.info("Bot started. Daily schedule %02d:%02d %s", RUN_HOUR, RUN_MIN, TZ)
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
