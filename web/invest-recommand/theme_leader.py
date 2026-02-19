from __future__ import annotations

import re
import json
from pathlib import Path
from datetime import datetime, UTC
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
import yfinance as yf


TEMA_API = "http://127.0.0.1:3010/api/themes"
SNAPSHOT_DIR = Path(__file__).resolve().parent / "snapshots-theme-leaders"
_PRICE_CACHE: Dict[str, pd.Series | None] = {}


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").replace("%", "").replace("+", "").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else 0.0


def _norm(arr: List[float]) -> List[float]:
    if not arr:
        return []
    a = np.array(arr, dtype=float)
    lo, hi = float(np.min(a)), float(np.max(a))
    if hi - lo < 1e-9:
        return [50.0 for _ in arr]
    return [float((x - lo) / (hi - lo) * 100) for x in a]


def _download_close(code: str) -> pd.Series | None:
    code = str(code or "").strip()
    if not code:
        return None
    if code in _PRICE_CACHE:
        return _PRICE_CACHE[code]

    for sym in (f"{code}.KS", f"{code}.KQ", code):
        try:
            df = yf.Ticker(sym).history(period="6mo", auto_adjust=True)
            if df is not None and (not df.empty) and "Close" in df:
                s = df["Close"].dropna()
                if len(s) >= 30:
                    _PRICE_CACHE[code] = s
                    return s
        except Exception:
            pass

    _PRICE_CACHE[code] = None
    return None


def _estimate_plan(row: Dict[str, Any]) -> Dict[str, Any]:
    price = max(1.0, _to_float(row.get("price")))
    chg = _to_float(row.get("changeRatePct"))

    s = _download_close(str(row.get("code") or ""))
    if s is not None and len(s) >= 30:
        cur = float(s.iloc[-1])
        atrp = float(s.pct_change().abs().tail(14).mean()) if len(s) >= 20 else 0.03
        m1 = float(s.iloc[-1] / s.iloc[-21] - 1) if len(s) >= 22 else 0.0

        loss_pct = -max(0.035, min(0.12, atrp * 1.8)) * 100
        ret_pct = max(6.0, min(24.0, (0.08 + max(-0.02, m1 * 0.6)) * 100))
        base = cur
        basis = "price-series"
    else:
        # 데이터가 없으면 당일 변동 기반 보수적 추정
        loss_pct = -max(4.0, min(12.0, abs(chg) * 0.7 + 3.0))
        ret_pct = max(6.0, min(20.0, abs(chg) * 0.9 + 5.0))
        base = price
        basis = "fallback"

    rr = ret_pct / abs(loss_pct) if loss_pct < 0 else 0.0
    stop = base * (1 + loss_pct / 100)
    tp1 = base * (1 + ret_pct / 100)

    return {
        "expectedReturnPct": round(ret_pct, 2),
        "expectedLossPct": round(loss_pct, 2),
        "riskReward": round(rr, 2),
        "stopLoss": round(stop, 2),
        "takeProfit1": round(tp1, 2),
        "basis": basis,
    }


def build_theme_leader_report(limit_themes: int = 12, per_theme_pick: int = 2) -> Dict[str, Any]:
    r = requests.get(
        TEMA_API,
        params={
            "exclude_bigcaps": "1",
            "sort": "trade_value",
            "limit": str(limit_themes),
            "preview_n": "12",
        },
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()

    themes = data.get("themes", []) or []

    # gather all stock candidates first for normalization
    all_rows: List[Dict[str, Any]] = []
    for t in themes:
        title = t.get("title") or "(untitled)"
        rank = int(t.get("rank") or 0)
        trade_sum = _to_float(t.get("trade_sum"))
        preview = t.get("preview", []) or []

        for s in preview:
            all_rows.append({
                "themeTitle": title,
                "themeRank": rank,
                "themeTradeSum": trade_sum,
                "name": s.get("name") or "",
                "code": s.get("code") or "",
                "changeRatePct": _to_float(s.get("change_rate")),
                "tradeValue": _to_float(s.get("trade_value")),
                "volume": _to_float(s.get("volume")),
                "price": _to_float(s.get("price")),
                "marketCap": s.get("market_cap"),
                "chartUrl": s.get("chart_url"),
            })

    if not all_rows:
        return {
            "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "date": data.get("date"),
            "themes": [],
            "leaders": [],
        }

    chg_n = _norm([x["changeRatePct"] for x in all_rows])
    tv_n = _norm([x["tradeValue"] for x in all_rows])
    for i, row in enumerate(all_rows):
        # 사용자 요청: 리더점수에서 거래량 제외 + 거래대금 비중 강화(75:25)
        row["leadershipScore"] = round(0.25 * chg_n[i] + 0.75 * tv_n[i], 2)

    # per-theme leaders
    by_theme: Dict[str, List[Dict[str, Any]]] = {}
    for row in all_rows:
        by_theme.setdefault(row["themeTitle"], []).append(row)

    theme_cards: List[Dict[str, Any]] = []
    all_leaders: List[Dict[str, Any]] = []

    for t in themes:
        title = t.get("title") or "(untitled)"
        rows = by_theme.get(title, [])
        if not rows:
            continue
        rows.sort(key=lambda x: x["leadershipScore"], reverse=True)
        leaders = rows[: max(1, per_theme_pick)]

        pos_cnt = sum(1 for x in rows if x["changeRatePct"] > 0)
        breadth = (pos_cnt / len(rows)) * 100.0 if rows else 0.0
        theme_score = round(0.75 * float(np.mean([x["leadershipScore"] for x in leaders])) + 0.25 * breadth, 2)

        card = {
            "title": title,
            "rank": int(t.get("rank") or 0),
            "tradeSum": _to_float(t.get("trade_sum")),
            "themeScore": theme_score,
            "breadthPct": round(breadth, 2),
            "leaders": leaders,
        }
        theme_cards.append(card)
        all_leaders.extend(leaders)

    theme_cards.sort(key=lambda x: x["themeScore"], reverse=True)
    all_leaders.sort(key=lambda x: x["leadershipScore"], reverse=True)

    # 주도주 매수 가정(당일 기준) 기대수익/손절률 추정
    plan_cache: Dict[str, Dict[str, Any]] = {}
    for row in all_leaders:
        code = str(row.get("code") or "")
        if code not in plan_cache:
            plan_cache[code] = _estimate_plan(row)
        row["plan"] = plan_cache[code]

    return {
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "date": data.get("date"),
        "methodology": "ThemeScore = 0.75*LeaderScore(avg top picks) + 0.25*Breadth; LeaderScore = 0.25*Change + 0.75*TradeValue (cross-theme normalized)",
        "themes": theme_cards,
        "leaders": all_leaders[:20],
    }


def save_theme_leader_snapshot(force: bool = False, limit_themes: int = 12, per_theme_pick: int = 2) -> Dict[str, Any]:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    report = build_theme_leader_report(limit_themes=limit_themes, per_theme_pick=per_theme_pick)
    day = report.get("date") or datetime.now(UTC).strftime("%y%m%d")
    path = SNAPSHOT_DIR / f"{day}.json"

    if path.exists() and not force:
        old = json.loads(path.read_text(encoding="utf-8"))
        return {"saved": False, "reason": "already_exists", "date": day, "path": str(path), "generatedAt": old.get("generatedAt")}

    payload = {
        "date": day,
        "savedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "generatedAt": report.get("generatedAt"),
        "methodology": report.get("methodology"),
        "topThemes": (report.get("themes") or [])[:10],
        "topLeaders": (report.get("leaders") or [])[:30],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"saved": True, "date": day, "path": str(path), "generatedAt": payload.get("generatedAt")}


def get_theme_leader_snapshot(date: str) -> Dict[str, Any] | None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    if not re.match(r"^\d{6}$", date or ""):
        return None
    p = SNAPSHOT_DIR / f"{date}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def list_theme_leader_snapshots(limit: int = 60) -> List[Dict[str, Any]]:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    out = []
    files = sorted(SNAPSHOT_DIR.glob("*.json"), reverse=True)[: max(1, limit)]
    for p in files:
        try:
            j = json.loads(p.read_text(encoding="utf-8"))
            out.append({
                "date": j.get("date") or p.stem,
                "generatedAt": j.get("generatedAt"),
                "topTheme": ((j.get("topThemes") or [{}])[0]).get("title"),
                "path": str(p),
            })
        except Exception:
            out.append({"date": p.stem, "generatedAt": None, "topTheme": None, "path": str(p)})
    return out
