# tracking_service.py
from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, Any, List

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

SPX_PATTERN = re.compile(r"^(SPXVN|SPX)[A-Z0-9]{6,}$", re.IGNORECASE)
GHN_PATTERN = re.compile(r"^GY[A-Z0-9]{4,}$", re.IGNORECASE)


def detect_tracking_carrier(code: str) -> str:
    """
    Trả về:
      - "SPX" nếu MVĐ dạng SPX/SPXVN...
      - "GHN" nếu MVĐ dạng GY...
      - "" nếu không nhận diện được
    """
    t = (code or "").strip().upper()
    if GHN_PATTERN.match(t):
        return "GHN"
    if SPX_PATTERN.match(t):
        return "SPX"
    return ""


def spx_tracking_link(spx_tn: str) -> str:
    tn = (spx_tn or "").strip().upper()
    return f"https://spx.vn/track?{tn}"


def ghn_tracking_link(order_code: str) -> str:
    oc = (order_code or "").strip().upper()
    return f"https://donhang.ghn.vn/?order_code={oc}"


def _fmt_epoch(ts: Any) -> str:
    """
    SPX: actual_time thường là epoch seconds
    """
    try:
        ts = int(ts)
        return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""


def _fmt_iso_z(s: Any) -> str:
    """
    GHN: action_at dạng ISO: 2026-02-10T13:05:32.974Z
    """
    try:
        t = str(s).strip()
        if not t:
            return ""
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        dt = datetime.fromisoformat(t)
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""


# =========================
# SPX: JSON API (public)
# =========================
def fetch_tracking_spx(spx_tn: str, language_code: str = "vi", timeout: int = 25) -> Dict[str, Any]:
    """
    GET https://spx.vn/shipment/order/open/order/get_order_info?spx_tn=...&language_code=vi

    Return:
      {
        ok: bool,
        carrier_code: "SPX",
        carrier: str,
        code: str,
        current_status: str,
        events: [{time,status,detail}],
        link: str,
        raw_sls_tn: str (nếu có),
        error: str (nếu lỗi)
      }
    """
    code = (spx_tn or "").strip().upper()
    link = spx_tracking_link(code)

    url = "https://spx.vn/shipment/order/open/order/get_order_info"
    params = {"spx_tn": code, "language_code": language_code}

    try:
        r = requests.get(
            url,
            params=params,
            timeout=timeout,
            headers={
                "User-Agent": UA,
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://spx.vn/",
            },
        )
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        return {
            "ok": False,
            "carrier_code": "SPX",
            "carrier": "Shopee Express (SPX)",
            "code": code,
            "current_status": "Không gọi được API SPX.",
            "events": [],
            "link": link,
            "raw_sls_tn": "",
            "error": str(e),
        }

    if not isinstance(j, dict) or j.get("retcode") != 0:
        return {
            "ok": False,
            "carrier_code": "SPX",
            "carrier": "Shopee Express (SPX)",
            "code": code,
            "current_status": "Không lấy được tracking SPX (retcode != 0).",
            "events": [],
            "link": link,
            "raw_sls_tn": "",
            "error": str(j),
        }

    data = j.get("data") or {}
    sls = data.get("sls_tracking_info") or {}
    records = sls.get("records") or []

    events: List[Dict[str, str]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue

        t = _fmt_epoch(rec.get("actual_time"))
        detail = (rec.get("buyer_description") or rec.get("description") or "").strip()
        status = (rec.get("tracking_name") or rec.get("milestone_name") or "").strip()

        if t or status or detail:
            events.append({"time": t, "status": status, "detail": detail})

    # SPX thường trả record mới nhất trước (theo bạn test), nên current_status lấy events[0]
    current_status = ""
    if events:
        e0 = events[0]
        current_status = (e0.get("detail") or e0.get("status") or "").strip()

    return {
        "ok": True,
        "carrier_code": "SPX",
        "carrier": "Shopee Express (SPX)",
        "code": code,
        "current_status": current_status or "Đã lấy dữ liệu nhưng không có record.",
        "events": events,
        "link": link,
        "raw_sls_tn": (sls.get("sls_tn") or "").strip(),
    }


# =========================
# GHN: JSON public API
# =========================
def fetch_tracking_ghn(order_code: str, timeout: int = 25) -> Dict[str, Any]:
    """
    POST https://fe-online-gateway.ghn.vn/order-tracking/public-api/client/tracking-logs
    Payload: {"order_code": "GY...."}

    Return:
      {
        ok: bool,
        carrier_code: "GHN",
        carrier: str,
        code: str,
        current_status: str,
        events: [{time,status,detail}]  # ✅ đảm bảo mới nhất ở trên
        link: str,
        from_address/to_address/to_name: str,
        error: str (nếu lỗi)
      }
    """
    code = (order_code or "").strip().upper()
    link = ghn_tracking_link(code)

    url = "https://fe-online-gateway.ghn.vn/order-tracking/public-api/client/tracking-logs"
    headers = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://donhang.ghn.vn",
        "Referer": "https://donhang.ghn.vn/",
    }
    payload = {"order_code": code}

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
        j = resp.json()
    except Exception as e:
        return {
            "ok": False,
            "carrier_code": "GHN",
            "carrier": "Giao Hàng Nhanh (GHN)",
            "code": code,
            "current_status": "Không gọi được API GHN.",
            "events": [],
            "link": link,
            "from_address": "",
            "to_address": "",
            "to_name": "",
            "error": str(e),
        }

    if not isinstance(j, dict) or j.get("code") != 200:
        return {
            "ok": False,
            "carrier_code": "GHN",
            "carrier": "Giao Hàng Nhanh (GHN)",
            "code": code,
            "current_status": "GHN API trả lỗi (code != 200).",
            "events": [],
            "link": link,
            "from_address": "",
            "to_address": "",
            "to_name": "",
            "error": str(j),
        }

    data = j.get("data") or {}
    order_info = data.get("order_info") or {}
    tracking_logs = data.get("tracking_logs") or []

    current_status = (order_info.get("status_name") or "").strip() or (order_info.get("status") or "").strip()

    events: List[Dict[str, str]] = []
    for lg in tracking_logs:
        if not isinstance(lg, dict):
            continue

        t = _fmt_iso_z(lg.get("action_at"))
        status = (lg.get("status_name") or lg.get("status") or "").strip()

        loc = lg.get("location") or {}
        detail = (loc.get("address") or "").strip()

        if t or status or detail:
            events.append({"time": t, "status": status, "detail": detail})

    # ✅ GHN thường trả cũ -> mới => đảo lại để mới nhất ở trên
    if len(events) >= 2:
        events = events[::-1]

    return {
        "ok": True,
        "carrier_code": "GHN",
        "carrier": "Giao Hàng Nhanh (GHN)",
        "code": code,
        "current_status": current_status or "Không rõ trạng thái",
        "events": events,
        "link": link,
        "from_address": (order_info.get("from_address") or "").strip(),
        "to_address": (order_info.get("to_address") or "").strip(),
        "to_name": (order_info.get("to_name") or "").strip(),
    }
