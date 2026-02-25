import os
import json
import random
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
Return ONLY a JSON array of 6 objects. No extra text.
"""


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
        temperature=0.8,
        max_tokens=900,
    )
    content = resp.choices[0].message.content or "[]"
    try:
        data = json.loads(content)
        if not isinstance(data, list) or len(data) != 6:
            raise ValueError("Not 6 items")
        return data
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
        return

    drafts = context.application.bot_data.setdefault("drafts", {})
    action = parts[0]

    if action == "approve":
        choice = parts[1]  # 1 / 2 / both
        content_id = parts[2] if len(parts) >= 3 else ""
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
        else:
            chosen = top2

        # Step 2 先只确认审批；Step 3（生成笔记）我们下一步接
        lines = ["✅ 已批准："]
        for it in chosen:
            lines.append(f"• {it.get('title','').strip()}")
        lines.append("")
        lines.append("下一步：我会把选中的标题交给你的 XHS Travel Assistant 生成笔记（Step 3）。")
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
        lambda: run_daily_job(app),
        CronTrigger(hour=RUN_HOUR, minute=RUN_MIN, timezone=tzinfo),
        id="daily_titles",
        replace_existing=True,
        misfire_grace_time=300,
    )
    scheduler.start()

    log.info("Bot started. Daily schedule %02d:%02d %s", RUN_HOUR, RUN_MIN, TZ)
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
