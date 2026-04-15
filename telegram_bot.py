import os
import threading
import asyncio
import re
import time
import sqlite3
from contextlib import contextmanager
from typing import List, Dict, Any, Tuple
from datetime import datetime

from flask import Flask, request, jsonify
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from order_service import fetch_orders, format_orders_for_telegram
from tracking_service import detect_tracking_carrier, fetch_tracking_spx, fetch_tracking_ghn

# =======================
# Flask keep-alive (Render)
# =======================
web_app = Flask(__name__)

@web_app.get("/")
def home():
    return "check_order_shopee is running", 200

@web_app.get("/ping")
def ping():
    return "pong", 200

def run_web():
    port = int(os.getenv("PORT", "10000"))
    web_app.run(host="0.0.0.0", port=port)

# =======================
# GHN PROXY
# =======================
SHEET_API_KEY = os.getenv("SHEET_API_KEY", "").strip()
RL_MAX = int(os.getenv("SHEET_RL_MAX", "30"))
RL_WINDOW_SEC = int(os.getenv("SHEET_RL_WINDOW_SEC", "60"))
CACHE_TTL_SEC = int(os.getenv("SHEET_CACHE_TTL_SEC", "30"))
_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_rl: Dict[str, Tuple[float, int]] = {}

def _client_ip() -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"

def _rate_limited(ip: str) -> bool:
    now = time.time()
    win_start, cnt = _rl.get(ip, (now, 0))
    if now - win_start >= RL_WINDOW_SEC:
        _rl[ip] = (now, 1)
        return False
    if cnt >= RL_MAX:
        return True
    _rl[ip] = (win_start, cnt + 1)
    return False

def _cache_get(code: str) -> Dict[str, Any] | None:
    now = time.time()
    item = _cache.get(code)
    if not item:
        return None
    exp, data = item
    if now >= exp:
        _cache.pop(code, None)
        return None
    return data

def _cache_set(code: str, data: Dict[str, Any]) -> None:
    _cache[code] = (time.time() + CACHE_TTL_SEC, data)

@web_app.post("/api/ghn-track")
def ghn_track_proxy():
    if SHEET_API_KEY:
        if request.headers.get("x-api-key", "") != SHEET_API_KEY:
            return jsonify({"ok": False, "error": "Unauthorized"}), 403
    ip = _client_ip()
    if _rate_limited(ip):
        return jsonify({"ok": False, "error": "Rate limited"}), 429
    data = request.get_json(silent=True) or {}
    code = (data.get("order_code") or "").strip().upper()
    if not code:
        return jsonify({"ok": False, "error": "Missing order_code"}), 400
    cached = _cache_get(code)
    if cached:
        out = dict(cached)
        out["_cached"] = True
        return jsonify(out), 200
    try:
        result = fetch_tracking_ghn(code)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "code": code}), 500
    _cache_set(code, result)
    return jsonify(result), 200

# =======================
# Config
# =======================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
AUTO_CHECK_INTERVAL = int(os.getenv("AUTO_CHECK_INTERVAL", "60"))
DB_PATH = os.getenv("DB_PATH", "watchlist.db")

# =======================
# SQLite
# =======================
_db_lock = threading.Lock()

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    with _db_lock:
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                chat_id     INTEGER NOT NULL,
                code        TEXT    NOT NULL,
                carrier     TEXT    NOT NULL DEFAULT '',
                last_status TEXT    NOT NULL DEFAULT '',
                added_at    TEXT    NOT NULL DEFAULT '',
                PRIMARY KEY (chat_id, code)
            )
        """)
        conn.commit()
        conn.close()

@contextmanager
def db():
    with _db_lock:
        conn = get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

# =======================
# Watchlist CRUD
# =======================
SUCCESS_KEYWORDS = [
    "đã giao", "giao thành công", "delivered", "delivery successful",
    "giao hàng thành công", "hoàn thành", "completed",
    "đã giao hàng", "giao thành công cho khách",
]

def is_delivered(status: str) -> bool:
    s = (status or "").lower().strip()
    return any(kw in s for kw in SUCCESS_KEYWORDS)

def db_add(chat_id: int, code: str, carrier: str, current_status: str = ""):
    added_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (chat_id, code, carrier, last_status, added_at) VALUES (?, ?, ?, ?, ?)",
            (chat_id, code.upper(), carrier, current_status, added_at),
        )

def db_remove(chat_id: int, code: str):
    with db() as conn:
        conn.execute("DELETE FROM watchlist WHERE chat_id=? AND code=?", (chat_id, code.upper()))

def db_update_status(chat_id: int, code: str, new_status: str):
    with db() as conn:
        conn.execute(
            "UPDATE watchlist SET last_status=? WHERE chat_id=? AND code=?",
            (new_status, chat_id, code.upper()),
        )

def db_get_all(chat_id: int) -> List[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM watchlist WHERE chat_id=? ORDER BY added_at DESC", (chat_id,)
        ).fetchall()

def db_get_all_users() -> Dict[int, List[Dict]]:
    with db() as conn:
        rows = conn.execute("SELECT * FROM watchlist").fetchall()
    result: Dict[int, List[Dict]] = {}
    for r in rows:
        cid = r["chat_id"]
        if cid not in result:
            result[cid] = []
        result[cid].append(dict(r))
    return result

def db_exists(chat_id: int, code: str) -> bool:
    with db() as conn:
        row = conn.execute(
            "SELECT 1 FROM watchlist WHERE chat_id=? AND code=?", (chat_id, code.upper())
        ).fetchone()
    return row is not None

# =======================
# Validation
# =======================
SPC_ST_PATTERN = re.compile(r"(?:^|;\s*)SPC_ST=([^;]{15,})", re.IGNORECASE)

def is_probably_shopee_cookie(s: str) -> bool:
    if not s:
        return False
    t = s.strip()
    if len(t) < 20:
        return False
    return SPC_ST_PATTERN.search(t) is not None

def _get_any(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    return default

def is_real_order(order: Dict[str, Any]) -> bool:
    if not isinstance(order, dict):
        return False
    order_id = _get_any(order, ["order_id", "orderid", "id"], "")
    tracking = _get_any(order, ["tracking_number", "tracking_no", "tracking"], "")
    products = order.get("product_info") or order.get("products") or []
    has_product = False
    if isinstance(products, list) and products:
        p0 = products[0] if isinstance(products[0], dict) else {}
        pname = _get_any(p0, ["name", "product_name", "title"], "")
        has_product = bool(pname)
    return bool(order_id) or bool(tracking) or has_product

def count_real_orders_from_api(data: Dict[str, Any]) -> int:
    accs = data.get("allOrderDetails") or []
    total = 0
    for a in accs:
        orders = a.get("orderDetails") or []
        for od in orders:
            if is_real_order(od):
                total += 1
    return total

# =======================
# Tracking formatter
# =======================
def _e(s: Any) -> str:
    """Escape HTML entities để gửi an toàn với parse_mode=HTML."""
    import html as _html
    return _html.escape(str(s) if s is not None else "")

def format_tracking_for_telegram(tdata: Dict[str, Any], max_events: int = 5) -> str:
    lines: List[str] = []
    if tdata.get("carrier"):
        lines.append(f"🚚 Đơn vị: {_e(tdata['carrier'])}")
    if tdata.get("code"):
        lines.append(f"🧾 MVĐ: {_e(tdata['code'])}")
    if tdata.get("current_status"):
        lines.append(f"📌 Trạng thái: {_e(tdata['current_status'])}")
    if tdata.get("from_address") and tdata.get("to_address"):
        lines.append(f"📦 Tuyến: {_e(tdata['from_address'])} ➜ {_e(tdata['to_address'])}")
    if tdata.get("to_name"):
        lines.append(f"👤 Người nhận: {_e(tdata['to_name'])}")
    if tdata.get("raw_sls_tn"):
        lines.append(f"🔎 Mã liên kết: {_e(tdata['raw_sls_tn'])}")

    evs = tdata.get("events") or []
    if evs:
        lines.append("\n📍 Hành trình gần nhất (mới nhất ở trên):")
        for e in evs[:max_events]:
            t = _e(e.get("time") or "")
            st = _e(e.get("status") or "")
            de = _e(e.get("detail") or "")
            one = " - ".join([x for x in [t, st, de] if x])
            if one:
                lines.append(f"• {one}")
        remain = len(evs) - max_events
        if remain > 0:
            lines.append(f"… +{remain} dòng khác (Mở link để xem full hành trình)")

    if tdata.get("link"):
        lines.append(f"\n🔗 {_e(tdata['link'])}")

    return "\n".join(lines).strip()

# =======================
# UI
# =======================
BTN_CHECK = "📦 Check MVĐ"
BTN_WATCHLIST = "📋 Danh sách theo dõi"
CB_CONTINUE = "continue_check"

def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton(BTN_CHECK)], [KeyboardButton(BTN_WATCHLIST)]],
        resize_keyboard=True,
    )

def continue_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔁 Bấm để tiếp tục check", callback_data=CB_CONTINUE)]]
    )

PROMPT_TEXT = (
    "🍪 Gửi Cookie theo định dạng:\n"
    "SPC_ST=....\n\n"
    "📦 Hoặc gửi Mã vận đơn để xem hành trình:\n"
    "- SPX / SPXVN... (Shopee Express)\n"
    "- GY... (GHN)\n\n"
    "💡 Cookie: tối đa 10 dòng (mỗi cookie 1 dòng).\n"
    "📌 MVĐ sẽ tự động thêm vào danh sách theo dõi."
)

# =======================
# Auto-check job
# =======================
async def auto_check_job(context):
    all_users = await asyncio.to_thread(db_get_all_users)

    for chat_id, items in all_users.items():
        for info in items:
            code = info["code"]
            carrier = info["carrier"]
            old_status = info["last_status"]

            try:
                if carrier == "SPX":
                    tdata = await asyncio.to_thread(fetch_tracking_spx, code, "vi")
                else:
                    tdata = await asyncio.to_thread(fetch_tracking_ghn, code)

                if not tdata.get("ok"):
                    continue

                new_status = tdata.get("current_status", "").strip()
                if not new_status or new_status == old_status:
                    continue

                await asyncio.to_thread(db_update_status, chat_id, code, new_status)

                msg = (
                    f"🔔 <b>Cập nhật vận đơn {code}</b>\n"
                    f"🚚 {tdata.get('carrier', '')}\n"
                    f"📌 Trạng thái mới: <b>{new_status}</b>\n"
                    f"↩️ Trước: {old_status or 'chưa rõ'}\n"
                )
                if tdata.get("link"):
                    msg += f"\n🔗 {tdata['link']}"

                if is_delivered(new_status):
                    await asyncio.to_thread(db_remove, chat_id, code)
                    msg += "\n\n✅ <b>Giao thành công! Đã xoá khỏi danh sách theo dõi.</b>"

                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")

            except Exception:
                pass

# =======================
# Handlers
# =======================
async def send_prompt(update: Update, *, via_query: bool = False):
    if via_query and update.callback_query:
        await update.callback_query.message.reply_text(PROMPT_TEXT)
    else:
        await update.message.reply_text(PROMPT_TEXT)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting"] = False
    await update.message.reply_text(
        "✅ Bot Check Đơn Shopee\n\nBấm nút bên dưới để bắt đầu.",
        reply_markup=main_keyboard(),
    )

async def handle_check_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting"] = True
    await send_prompt(update)

async def show_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    rows = await asyncio.to_thread(db_get_all, chat_id)

    if not rows:
        await update.message.reply_text(
            "📋 Danh sách theo dõi trống.\nGửi mã vận đơn để thêm vào danh sách.",
            reply_markup=main_keyboard(),
        )
        return

    lines = [f"📋 <b>Danh sách theo dõi ({len(rows)} MVĐ)</b>\n"]
    for r in rows:
        status = r["last_status"] or "Chưa có thông tin"
        delivered_mark = " ✅" if is_delivered(status) else ""
        lines.append(
            f"🧾 <b>{r['code']}</b> [{r['carrier']}]{delivered_mark}\n"
            f"   📌 {status}\n"
            f"   🕐 Thêm lúc: {r['added_at']}"
        )

    lines.append(f"\n🔄 Tự động check mỗi <b>{AUTO_CHECK_INTERVAL}s</b>")
    lines.append("Dùng /remove &lt;MVĐ&gt; để xoá thủ công.")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=main_keyboard())

async def remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Cú pháp: /remove <MVĐ>\nVí dụ: /remove SPXVN123456")
        return
    code = context.args[0].strip().upper()
    if not await asyncio.to_thread(db_exists, chat_id, code):
        await update.message.reply_text(f"❌ Không tìm thấy {code} trong danh sách theo dõi.")
        return
    await asyncio.to_thread(db_remove, chat_id, code)
    await update.message.reply_text(f"✅ Đã xoá {code} khỏi danh sách theo dõi.")

async def continue_check_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["awaiting"] = True
    await q.message.reply_text("🔁 OK, gửi Cookie hoặc MVĐ để check tiếp nhé!")
    await send_prompt(update, via_query=True)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == BTN_CHECK:
        await handle_check_button(update, context)
        return

    if text == BTN_WATCHLIST:
        await show_watchlist(update, context)
        return

    if not context.user_data.get("awaiting"):
        await start(update, context)
        return

    raw = text
    if not raw:
        await update.message.reply_text("❌ Bạn chưa gửi gì cả. Gửi lại giúp mình nhé.")
        return

    chat_id = update.effective_chat.id

    # --- MVĐ? ---
    single = raw.replace(" ", "").strip()
    carrier = detect_tracking_carrier(single)
    if carrier:
        await update.message.reply_text("⏳ Đang check hành trình vận đơn...")
        try:
            if carrier == "SPX":
                tdata = await asyncio.to_thread(fetch_tracking_spx, single, "vi")
            else:
                tdata = await asyncio.to_thread(fetch_tracking_ghn, single)

            if not tdata.get("ok"):
                await update.message.reply_text(
                    "❌ Không lấy được hành trình vận đơn.\n"
                    f"Chi tiết: {tdata.get('error', '')}",
                    reply_markup=continue_inline_keyboard(),
                )
                return

            current_status = tdata.get("current_status", "")
            msg = format_tracking_for_telegram(tdata, max_events=5)

            if not is_delivered(current_status):
                already = await asyncio.to_thread(db_exists, chat_id, single)
                if not already:
                    await asyncio.to_thread(db_add, chat_id, single, carrier, current_status)
                    msg += f"\n\n📌 <i>Đã thêm vào danh sách theo dõi (check mỗi {AUTO_CHECK_INTERVAL}s)</i>"
                else:
                    msg += "\n\n🔄 <i>MVĐ này đang được theo dõi rồi.</i>"
            else:
                msg += "\n\n✅ <i>Đơn đã giao thành công, không thêm vào theo dõi.</i>"

            await update.message.reply_text(
                msg, parse_mode="HTML", reply_markup=continue_inline_keyboard()
            )
        except Exception as e:
            await update.message.reply_text(
                f"❌ Lỗi check vận đơn: {e}", reply_markup=continue_inline_keyboard()
            )
        return

    # --- Cookie? ---
    lines_raw = [l.strip() for l in raw.splitlines() if l.strip()]
    cookies = [l for l in lines_raw if is_probably_shopee_cookie(l)]

    if not cookies:
        await update.message.reply_text(
            "❓ Không nhận diện được Cookie hay MVĐ hợp lệ.\n\n" + PROMPT_TEXT
        )
        return

    if len(cookies) > 10:
        cookies = cookies[:10]
        await update.message.reply_text("⚠️ Chỉ xử lý tối đa 10 cookie. Đã cắt bớt.")

    await update.message.reply_text(f"⏳ Đang check {len(cookies)} cookie...")
    try:
        data = await asyncio.to_thread(fetch_orders, cookies)
        msg = format_orders_for_telegram(data)
        count = count_real_orders_from_api(data)

        chunks = [msg[i: i + 4000] for i in range(0, len(msg), 4000)]
        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode="HTML")

        await update.message.reply_text(
            f"✅ Xong! Tìm thấy {count} đơn.", reply_markup=continue_inline_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Lỗi lấy đơn hàng: {e}", reply_markup=continue_inline_keyboard()
        )

    context.user_data["awaiting"] = False

# =======================
# Main
# =======================
def main():
    if not TOKEN:
        raise RuntimeError("Thiếu TELEGRAM_BOT_TOKEN trong Environment Variables.")

    init_db()
    print(f"✅ SQLite DB sẵn sàng tại: {DB_PATH}")

    threading.Thread(target=run_web, daemon=True).start()

    app = ApplicationBuilder().token(TOKEN).build()

    app.job_queue.run_repeating(
        auto_check_job,
        interval=AUTO_CHECK_INTERVAL,
        first=10,
        name="auto_watchlist_check",
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("remove", remove_cmd))
    app.add_handler(CallbackQueryHandler(continue_check_callback, pattern=f"^{CB_CONTINUE}$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print(f"✅ Bot đang chạy... (auto-check mỗi {AUTO_CHECK_INTERVAL}s | DB: {DB_PATH})")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
