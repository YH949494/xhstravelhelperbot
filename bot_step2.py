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
MAX_NOTES_PER_DAY = int(os.getenv("MAX_NOTES_PER_DAY", "2"))
NOTE_MAX_TOKENS = int(os.getenv("NOTE_MAX_TOKENS", "900"))
TZ = os.getenv("TZ", "Asia/Kuala_Lumpur")
RUN_HOUR = int(os.getenv("RUN_HOUR", "21"))
RUN_MIN = int(os.getenv("RUN_MIN", "30"))
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()}
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


def _score_and_pick_top2(items: list[dict]) -> tuple[list[dict], list[dict]]:
    scored = []
    for it in items:
        sc = score_item(it)
        it2 = dict(it)
        it2["_score"] = sc
        scored.append(it2)
    scored.sort(key=lambda x: x["_score"]["total"], reverse=True)
    return scored, scored[:2]


async def generate_validated_top2(app: Application | None = None, max_retries: int = 3) -> tuple[list[dict], list[dict], bool]:
    best_scored: list[dict] = []
    best_top2: list[dict] = []
    best_valid_count = -1
    for attempt in range(max_retries + 1):
        items = await generate_6_titles(app)
        scored, top2 = _score_and_pick_top2(items)
        valid_count = 0
        all_valid = True
        for it in top2:
            title = (it.get("title") or "").strip()
            ok, reason = _hook_validation_reason(title)
            if ok:
                valid_count += 1
            else:
                all_valid = False
                append_failure_log_line(title, reason, str(SKILLS_DIR))
                log.warning("title rejected by hook validation: %s | reason=%s", title, reason)
        if valid_count > best_valid_count:
            best_valid_count = valid_count
            best_scored = scored
            best_top2 = top2
        if all_valid:
            return scored, top2, False
    log.warning("hook validation retries exhausted; using best available attempt")
    return best_scored, best_top2, True


def format_top2_message(content_id: str, top2: list[dict]) -> str:
    lines = []
    lines.append("📌 今日最佳 2 条选题（待审批）")
    lines.append(f"🆔 content_id: {content_id}")
    lines.append("")

    for i, it in enumerate(top2, start=1):
        s = it["_score"]
        lines.append(f"{i}️⃣ {it.get('title','').strip()}  （{s['total']}/40）")
        lines.append(f"• 角度：{it.get('angle','').strip()}")
        lines.append(f"• 目标人群：{it.get('target_audience','').strip()}")
        lines.append(f"• CTA：{it.get('cta','Follow / 收藏小红书')}")
        lines.append("")
    return "\n".join(lines).strip()


def approval_keyboard(content_id: str) -> InlineKeyboardMarkup:
    kb = [
        [
            InlineKeyboardButton("✅ 选 1", callback_data=f"approve:1:{content_id}"),
            InlineKeyboardButton("✅ 选 2", callback_data=f"approve:2:{content_id}"),
        ],
        [
            InlineKeyboardButton("🔥 两条都做", callback_data=f"approve:both:{content_id}"),
            InlineKeyboardButton("🔁 重生成", callback_data=f"regen:{content_id}"),
        ],
    ]
    return InlineKeyboardMarkup(kb)


async def run_daily_job(app: Application) -> None:
    now = datetime.now(tzinfo)
    content_id = make_content_id(now)
    log.info("Running daily job content_id=%s", content_id)

    scored, top2, has_validation_warning = await generate_validated_top2(app)

    msg = format_top2_message(content_id, top2)
    if has_validation_warning:
        msg = msg + "\n\n⚠️ Hook校验多次未完全通过，已返回最佳候选。"
    await app.bot.send_message(
        chat_id=APPROVAL_CHAT_ID,
        text=msg,
        reply_markup=approval_keyboard(content_id),
        disable_web_page_preview=True,
    )

    # store in bot_data for callback usage
    app.bot_data.setdefault("drafts", {})[content_id] = {
        "created_at": now.isoformat(),
        "items": scored,   # keep all 6
        "top2": top2,
        "approved": None,
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

    if action == "approve":
        if len(parts) < 3:
            log.warning("Malformed approve callback: %s", data)
            await q.edit_message_text("❌ 指令格式错误，请重试。")
            return
        choice = parts[1]  # 1 / 2 / both
        content_id = parts[2]
        d = drafts.get(content_id)
        if not d:
            await q.edit_message_text("❌ 找不到该 content_id（可能重启后丢失）。请点 🔁 重生成。")
            return

        d["approved"] = choice
        top2 = d["top2"]
        chosen = []
        if choice == "1":
            chosen = [top2[0]]
        elif choice == "2":
            chosen = [top2[1]]
        elif choice == "both":
            chosen = top2
        else:
            await q.edit_message_text("❌ 未知审批选项，请重试。")
            return

        daily_counts = context.application.bot_data.setdefault("daily_counts", {})
        day_key = datetime.now(tzinfo).strftime("%Y%m%d")
        used = int(daily_counts.get(day_key, 0))
        remaining = max(0, MAX_NOTES_PER_DAY - used)
        if remaining <= 0:
            await q.edit_message_text(f"⚠️ 今日已达上限（{MAX_NOTES_PER_DAY}/{MAX_NOTES_PER_DAY}），明天再生成。")
            return
        selected = chosen[:remaining]
        over_limit = len(chosen) > len(selected)

        generated_titles = []
        for it in selected:
            try:
                note_text, needs_warning = await generate_note(
                    it.get("title", "").strip(),
                    it.get("angle", "").strip(),
                    it.get("target_audience", "").strip(),
                )
                if needs_warning:
                    note_text = note_text + "\n\n⚠️ Hook 可能超字数，请手动微调"
                await context.application.bot.send_message(
                    chat_id=APPROVAL_CHAT_ID,
                    text=note_text,
                    disable_web_page_preview=True,
                )
                generated_titles.append(it.get("title", "").strip())
                used += 1
                daily_counts[day_key] = used
            except Exception:
                log.exception("note generation failed content_id=%s title=%s", content_id, it.get("title", ""))
                await context.application.bot.send_message(
                    chat_id=APPROVAL_CHAT_ID,
                    text=f"❌ 笔记生成失败：{it.get('title','').strip()}",
                )

        lines = ["✅ 已生成笔记："]
        for title in generated_titles:
            lines.append(f"• {title}")
        if not generated_titles:
            lines.append("• 无（生成失败，请查看日志）")
        lines.append("")
        lines.append(f"今日计数：{daily_counts.get(day_key, used)}/{MAX_NOTES_PER_DAY}")
        if over_limit:
            lines.append("⚠️ 超出今日上限，本次仅生成 1 条。" if remaining == 1 else "⚠️ 超出今日上限，已按剩余额度生成。")
        await q.edit_message_text("\n".join(lines).strip())
        return

    if action == "regen":
        content_id = parts[1] if len(parts) >= 2 else ""
        # regenerate immediately and replace the message
        try:
            now = datetime.now(tzinfo)
            new_id = make_content_id(now)
            scored, top2, has_validation_warning = await generate_validated_top2(context.application)

            drafts[new_id] = {
                "created_at": now.isoformat(),
                "items": scored,
                "top2": top2,
                "approved": None,
            }

            msg = format_top2_message(new_id, top2)
            if has_validation_warning:
                msg = msg + "\n\n⚠️ Hook校验多次未完全通过，已返回最佳候选。"
            await q.edit_message_text(msg, reply_markup=approval_keyboard(new_id), disable_web_page_preview=True)
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


def main() -> None:
    ensure_default_skill_files(str(SKILLS_DIR))
    app = Application.builder().token(TG_TOKEN).build()

    # commands / handlers
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("win", win))
    app.add_handler(CommandHandler("wins", wins))
    app.add_handler(CommandHandler("wintext", wintext))
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
