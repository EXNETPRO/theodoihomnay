import requests
import json
import html
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

BASE_URL = "https://us-central1-get-feedback-a0119.cloudfunctions.net/app"
API_ENDPOINT = "/api/shopee/getOrderDetailsForCookie"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://autopee.vercel.app",
    "Referer": "https://autopee.vercel.app/",
}


def fetch_orders(cookies_list: List[str]) -> Dict[str, Any]:
    url = BASE_URL + API_ENDPOINT
    payload = {"cookies": cookies_list}

    response = requests.post(
        url,
        data=json.dumps(payload),
        headers=HEADERS,
        timeout=60
    )
    if response.status_code != 200:
        raise Exception(response.text)
    return response.json()


# ---------------- helpers ----------------

def h(x: Any) -> str:
    """Escape để dùng an toàn với parse_mode='HTML'."""
    return html.escape("" if x is None else str(x))

def _get(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return default


def _fmt_ts(ts: Any) -> str:
    if ts in (None, ""):
        return ""
    try:
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(ts)


def _fmt_money_from_api(v: Any) -> str:
    try:
        return f"{(float(v) / 100000):,.0f} đ"
    except Exception:
        return str(v)


def _build_shopee_link(shop_id: Any, item_id: Any) -> Optional[str]:
    try:
        if shop_id and item_id:
            return f"https://shopee.vn/product/{int(shop_id)}/{int(item_id)}"
    except Exception:
        pass
    return None


def _safe_trim(s: Any, n: int) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[:n] + "…"


def _split_address_for_ui(full_address: Any) -> Tuple[str, str]:
    s = "" if full_address is None else str(full_address).strip()
    if not s:
        return "", ""

    parts = [p.strip() for p in s.split(",") if p.strip()]
    if len(parts) >= 2:
        main = ", ".join(parts[:-1]).strip()
        city = parts[-1].strip()
        return main, city

    return s, ""


def _detect_carrier_from_tracking(tracking_id: Any) -> str:
    t = "" if tracking_id is None else str(tracking_id).strip().upper()
    if not t:
        return ""

    prefix_map = [
        ("SPXVN", "Shopee Express"),
        ("SPX", "Shopee Express"),
        ("GY", "Giao Hàng Nhanh"),
    ]
    for pref, name in prefix_map:
        if t.startswith(pref):
            return name
    return ""


# ---------------- formatter ----------------

def format_orders_for_telegram(
    data: Dict[str, Any],
    max_orders_per_cookie: int = 5,
    max_products_per_order: int = 5,
) -> List[str]:
    """
    TRẢ VỀ HTML-SAFE:
    - Dùng với parse_mode='HTML' ở telegram_bot.py
    - Cookie + MVĐ bọc <code>...</code> => copy dễ
    """
    messages: List[str] = []

    accounts = data.get("allOrderDetails", [])
    if not accounts:
        return ["❌ Không có dữ liệu đơn hàng. (API trả rỗng)"]

    for account in accounts:
        cookie = account.get("cookie", "")
        orders = account.get("orderDetails", []) or []
        if not orders:
            messages.append(f"🍪 Cookie: <code>{h(cookie[:20])}...</code>\n❌ Không có đơn hàng.")
            continue

        blocks: List[str] = []
        header = f"🍪 Cookie: <code>{h(cookie[:20])}...</code>\n📦 Tổng đơn: {len(orders)}"
        blocks.append(header)

        shown = 0
        for idx, order in enumerate(orders, start=1):
            if shown >= max_orders_per_cookie:
                break

            order_id = _get(order, ["order_id", "orderid", "id"], "")
            status = _get(order, ["tracking_info_description", "status_description", "status", "order_status"], "")
            tracking = _get(order, ["tracking_number", "tracking_no", "tracking"], "")
            # order_time = _fmt_ts(_get(order, ["create_time", "order_time", "ctime", "created_at"], ""))

            address = order.get("address", {}) or {}
            name = _get(address, ["shipping_name", "name", "receiver_name"], "")
            phone = _get(address, ["shipping_phone", "phone", "receiver_phone"], "")
            full_address = _get(address, ["shipping_address", "address", "full_address"], "")
            addr_main, addr_city = _split_address_for_ui(full_address)

            shipping = order.get("shipping", {}) or {}
            carrier_api = _get(shipping, ["shipping_carrier", "carrier"], "") or _get(order, ["shipping_carrier"], "")
            tracking_id = _get(order, ["tracking_number"], tracking)

            carrier_detected = _detect_carrier_from_tracking(tracking_id)
            carrier = carrier_detected or carrier_api

            products = order.get("product_info", []) or order.get("products", []) or []
            prod_lines: List[str] = []

            for p in products[:max_products_per_order]:
                pname = _safe_trim(_get(p, ["name", "product_name", "title"], ""), 160)
                variation = _safe_trim(_get(p, ["model_name", "variation", "classification", "model"], ""), 80)

                line = pname
                if variation:
                    line += f" [{variation}]"
                prod_lines.append(line)

            if len(products) > max_products_per_order:
                prod_lines.append(f"(… +{len(products) - max_products_per_order} sản phẩm khác)")

            block_parts: List[str] = []

            block_parts.append(f"\n📌 ĐƠN HÀNG {idx} :")
            if order_id:
                block_parts.append(f"🧾 Order ID: {h(order_id)}")

            block_parts.append("ℹ️ THÔNG TIN")
            if name:
                block_parts.append(f"👤 Người nhận: {h(name)}")
            if phone:
                block_parts.append(f"📞 SDT: {h(phone)}")
            if addr_main:
                block_parts.append(f"📍 Địa chỉ: {h(addr_main)}")
            if addr_city:
                prefix = "TP. " if not str(addr_city).lower().startswith(("tp", "thành phố", "tỉnh")) else ""
                block_parts.append(f"{h(prefix + str(addr_city))}")

            if prod_lines:
                if len(prod_lines) == 1:
                    block_parts.append(f"\n🎁 Sản phẩm: {h(prod_lines[0])}")
                else:
                    block_parts.append("\n🎁 Sản phẩm:")
                    for i, pl in enumerate(prod_lines, start=1):
                        block_parts.append(f"Sản phẩm {i} : {h(pl)}")

            if carrier:
                block_parts.append(f"\n🚚 Đơn vị vận chuyển: {h(carrier)}")
            if tracking_id:
                # <code> giúp copy dễ
                block_parts.append(f"🧾 MVD: <code>{h(tracking_id)}</code>")
            if status:
                block_parts.append(f"📊 Trạng thái: {h(status)}")

            block_text = "\n".join([x for x in block_parts if x]).strip()
            blocks.append(block_text)

            if len(orders) > 1 and shown < max_orders_per_cookie and idx < min(len(orders), max_orders_per_cookie):
                blocks.append("---------------------------------------")
            else:
                blocks.append("")

            shown += 1

        if len(orders) > shown:
            blocks.append(f"… (ẩn {len(orders) - shown} đơn, tăng giới hạn nếu muốn)")

        blocks.append("ℹ️ Tap vào MVD để copy nhanh.")

        full_text = "\n".join(blocks).strip()

        # Telegram giới hạn ~4096, để an toàn cắt 3500
        while len(full_text) > 3500:
            messages.append(full_text[:3500])
            full_text = full_text[3500:]
        messages.append(full_text)

    return messages
