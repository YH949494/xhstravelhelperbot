import os
import json
import random
import asyncio
import string
import logging
from datetime import datetime
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

if not TG_TOKEN or not OPENAI_API_KEY or not APPROVAL_CHAT_ID:
    raise RuntimeError("Missing env: TELEGRAM_BOT_TOKEN / OPENAI_API_KEY / APPROVAL_CHAT_ID")

client = OpenAI(api_key=OPENAI_API_KEY)
tzinfo = ZoneInfo(TZ)


def make_content_id(now: datetime) -> str:
    rand4 = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return now.strftime("%Y%m%d-%H%M") + "-" + rand4


TITLE_PROMPT = """You are a Smart Travel Growth Title Engine for XiaoHongShu.
Creator positioning: smart traveler, cost-aware, efficiency optimizer, hack-focused, decision-helper.
Never produce: generic diary, emotional storytelling, aesthetic-only content.

Task:
Generate exactly 6 candidate XiaoHongShu travel post ideas.
Distribute as:
- 2 Growth (maximize reach/saves)
- 2 Conversion (maximize follow)
- 2 Trust (cost breakdown / avoid mistakes / checklist)

For EACH item, output JSON with keys:
bucket (growth|conversion|trust),
title,
angle,
target_audience,
cta (must be "Follow / 收藏小红书")

Constraints:
- Title must contain at least ONE of: number, savings, time, mistake, hidden tip, comparison.
- Titles must be specific and actionable.
Return ONLY this exact JSON shape: {"items":[...6 objects...]}
No code fences. No extra text.
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


async def generate_note(title: str, angle: str, audience: str) -> tuple[str, bool]:
    resp = client.chat.completions.create(
        model=OPENAI_MODEL_NOTE,
        messages=[
            {"role": "system", "content": NOTE_SYSTEM},
            {"role": "user", "content": build_note_user_prompt(title, angle, audience)},
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

    # cap each to 0-10
    save_score = min(save_score, 10)
    follow_score = min(follow_score, 10)
    clarity_score = min(clarity_score, 10)
    exec_score = min(exec_score, 10)

    total = save_score + follow_score + clarity_score + exec_score
    return {
        "save": save_score,
        "follow": follow_score,
        "clarity": clarity_score,
        "exec": exec_score,
        "total": total,
    }


async def generate_6_titles() -> list[dict]:
    resp = client.chat.completions.create(
        model=MODEL_TITLES,
        messages=[
            {"role": "system", "content": "You output strict JSON only."},
            {"role": "user", "content": TITLE_PROMPT},
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

    items = await generate_6_titles()

    # score + attach
    scored = []
    for it in items:
        sc = score_item(it)
        it2 = dict(it)
        it2["_score"] = sc
        scored.append(it2)

    # pick top2
    scored.sort(key=lambda x: x["_score"]["total"], reverse=True)
    top2 = scored[:2]

    msg = format_top2_message(content_id, top2)
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
            items = await generate_6_titles()

            scored = []
            for it in items:
                it2 = dict(it)
                it2["_score"] = score_item(it)
                scored.append(it2)
            scored.sort(key=lambda x: x["_score"]["total"], reverse=True)
            top2 = scored[:2]

            drafts[new_id] = {
                "created_at": now.isoformat(),
                "items": scored,
                "top2": top2,
                "approved": None,
            }

            msg = format_top2_message(new_id, top2)
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


def main() -> None:
    app = Application.builder().token(TG_TOKEN).build()

    # commands / handlers
    app.add_handler(CommandHandler("whoami", whoami))
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
