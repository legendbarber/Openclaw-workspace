from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf


@dataclass
class Asset:
    symbol: str
    name: str
    category: str


UNIVERSE = [
    # 단일 주식 중심(상대적으로 모멘텀/변동성 높은 티커 우선)
    Asset("NVDA", "NVIDIA Corp", "single-stock"),
    Asset("TSLA", "Tesla Inc", "single-stock"),
    Asset("AMD", "Advanced Micro Devices", "single-stock"),
    Asset("PLTR", "Palantir Technologies", "single-stock"),
    Asset("SMCI", "Super Micro Computer", "single-stock"),
    Asset("META", "Meta Platforms", "single-stock"),
    Asset("NFLX", "Netflix Inc", "single-stock"),
    Asset("AMZN", "Amazon.com Inc", "single-stock"),
    Asset("GOOGL", "Alphabet Inc", "single-stock"),
    Asset("MSFT", "Microsoft Corp", "single-stock"),
    Asset("COIN", "Coinbase Global", "single-stock"),
    Asset("MSTR", "MicroStrategy", "single-stock"),
    Asset("SOFI", "SoFi Technologies", "single-stock"),
    Asset("RIVN", "Rivian Automotive", "single-stock"),
]

STATE_PATH = Path(__file__).resolve().parent / "state_log.json"
POSITIVE = ["beat", "strong", "upgrade", "rally", "surge", "record", "gain"]
NEGATIVE = ["miss", "downgrade", "drop", "fall", "weak", "risk", "lawsuit"]


def _download_close(symbol: str, period: str = "1y") -> pd.Series | None:
    try:
        df = yf.Ticker(symbol).history(period=period, auto_adjust=True)
        if df is None or df.empty or "Close" not in df:
            return None
        s = df["Close"].dropna()
        if len(s) < 80:
            return None
        return s
    except Exception:
        return None


def _pct(s: pd.Series, d: int) -> float:
    if len(s) <= d:
        return 0.0
    return float(s.iloc[-1] / s.iloc[-d - 1] - 1)


def _vol(s: pd.Series) -> float:
    r = s.pct_change().dropna()
    return float(r.std() * np.sqrt(252)) if len(r) > 20 else 0.0


def _mdd(s: pd.Series) -> float:
    arr = s.values
    peak = np.maximum.accumulate(arr)
    dd = arr / peak - 1
    return float(dd.min())


def _consensus(symbol: str) -> Dict:
    try:
        info = yf.Ticker(symbol).info or {}
        cur = info.get("currentPrice")
        target = info.get("targetMeanPrice")
        mean = info.get("recommendationMean")
        key = info.get("recommendationKey")
        n = info.get("numberOfAnalystOpinions")
        up = None
        if cur and target:
            up = (target / cur - 1) * 100

        score = 50.0
        if up is not None:
            score += float(np.clip(up / 2.5, -20, 30))
        if isinstance(mean, (int, float)):
            score += float(np.clip((3.0 - mean) * 10, -20, 20))
        if isinstance(n, (int, float)):
            score += float(np.clip(n / 2, 0, 10))

        return {
            "targetMeanPrice": target,
            "upsidePct": None if up is None else round(float(up), 2),
            "recommendationMean": mean,
            "recommendationKey": key,
            "analystOpinions": n,
            "score": round(float(np.clip(score, 0, 100)), 2),
        }
    except Exception:
        return {
            "targetMeanPrice": None,
            "upsidePct": None,
            "recommendationMean": None,
            "recommendationKey": None,
            "analystOpinions": None,
            "score": 50.0,
        }


def _news(symbol: str, name: str, limit: int = 8) -> Dict:
    try:
        q = urllib.parse.quote(f"{symbol} {name} outlook")
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        xml = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", "ignore")
        items = re.findall(r"<item>(.*?)</item>", xml, re.S)
        titles = []
        links = []
        for raw in items[:limit]:
            t = re.search(r"<title><!\[CDATA\[(.*?)\]\]></title>", raw)
            l = re.search(r"<link>(.*?)</link>", raw)
            if t:
                titles.append(t.group(1).strip())
            if l:
                links.append(l.group(1).strip())

        tone = 0
        for t in titles:
            lo = t.lower()
            tone += sum(1 for w in POSITIVE if w in lo)
            tone -= sum(1 for w in NEGATIVE if w in lo)

        score = float(np.clip(50 + tone * 6 + len(titles) * 1.2, 0, 100))
        return {
            "headlineCount": len(titles),
            "tone": tone,
            "score": round(score, 2),
            "headlines": titles[:4],
            "links": links[:4],
        }
    except Exception:
        return {"headlineCount": 0, "tone": 0, "score": 50.0, "headlines": [], "links": []}


def _momentum_score(s: pd.Series) -> Dict:
    m1 = _pct(s, 21) * 100
    m3 = _pct(s, 63) * 100
    m6 = _pct(s, 126) * 100
    ma20 = float(s.rolling(20).mean().iloc[-1])
    ma60 = float(s.rolling(60).mean().iloc[-1])
    cur = float(s.iloc[-1])
    trend_boost = 10 if (cur > ma20 and ma20 > ma60) else (-8 if cur < ma20 else 0)
    raw = m1 * 0.35 + m3 * 0.4 + m6 * 0.25 + trend_boost
    score = float(np.clip(50 + raw, 0, 100))
    return {
        "m1Pct": round(m1, 2),
        "m3Pct": round(m3, 2),
        "m6Pct": round(m6, 2),
        "trendBoost": trend_boost,
        "score": round(score, 2),
    }


def _liquidity_score(symbol: str) -> float:
    try:
        q = yf.Ticker(symbol).fast_info
        avg_vol = q.get("threeMonthAverageVolume") or q.get("tenDayAverageVolume") or 0
        score = np.clip(np.log10(max(float(avg_vol), 1.0)) * 13, 0, 100)
        return round(float(score), 2)
    except Exception:
        return 50.0


def _risk_score(s: pd.Series) -> Dict:
    v = _vol(s) * 100
    dd = abs(_mdd(s)) * 100
    # 단일주식 모드: 너무 낮은 변동성은 감점, 중간~중상 변동성 선호, 과열은 감점
    # vol sweet spot: 22%~45%
    if v < 22:
        vol_component = 45 + (v / 22) * 25  # 45~70
    elif v <= 45:
        vol_component = 70 + ((v - 22) / 23) * 25  # 70~95
    else:
        vol_component = max(20, 95 - (v - 45) * 2.2)

    dd_penalty = max(0, dd - 38) * 1.6
    score = float(np.clip(vol_component - dd_penalty, 0, 100))
    return {"volPct": round(v, 2), "maxDrawdownPct": round(dd, 2), "score": round(score, 2)}


def evaluate_asset(asset: Asset) -> Dict | None:
    s = _download_close(asset.symbol, "1y")
    if s is None:
        return None

    report_consensus = _consensus(asset.symbol)
    momentum = _momentum_score(s)
    crowd = _news(asset.symbol, asset.name)
    liquidity = _liquidity_score(asset.symbol)
    risk = _risk_score(s)

    score = (
        0.35 * report_consensus["score"] +
        0.25 * momentum["score"] +
        0.20 * crowd["score"] +
        0.10 * liquidity +
        0.10 * risk["score"]
    )

    cur = float(s.iloc[-1])
    atrp = float(s.pct_change().abs().tail(14).mean()) if len(s) > 20 else 0.03
    stop = cur * (1 - max(0.04, min(0.14, atrp * 1.8)))
    tp1 = cur * (1 + max(0.06, min(0.22, (_pct(s, 63) * 0.6 + 0.06))))
    tp2 = cur * (1 + max(0.1, min(0.35, (_pct(s, 126) * 0.8 + 0.12))))

    invalidation = "종가가 20일선 하회 + 거래강도 둔화가 2거래일 연속이면 추천 무효"

    return {
        "symbol": asset.symbol,
        "name": asset.name,
        "category": asset.category,
        "score": round(float(score), 2),
        "currentPrice": round(cur, 2),
        "components": {
            "reportConsensus": report_consensus,
            "momentum": momentum,
            "crowd": crowd,
            "liquidityScore": liquidity,
            "risk": risk,
        },
        "plan": {
            "entryZone": [round(cur * 0.99, 2), round(cur * 1.01, 2)],
            "stopLoss": round(stop, 2),
            "takeProfit1": round(tp1, 2),
            "takeProfit2": round(tp2, 2),
            "ttlTradingDays": 3,
            "invalidationRule": invalidation,
        },
        "links": {
            "yahoo": f"https://finance.yahoo.com/quote/{asset.symbol}",
            "analysis": f"https://finance.yahoo.com/quote/{asset.symbol}/analysis/",
            "news": f"https://news.google.com/search?q={urllib.parse.quote(asset.symbol)}",
        },
    }


def build_report() -> Dict:
    rows = []
    failed = []
    for a in UNIVERSE:
        r = evaluate_asset(a)
        if r is None:
            failed.append(a.symbol)
        else:
            rows.append(r)

    rows.sort(key=lambda x: x["score"], reverse=True)
    top = rows[0] if rows else None

    no_trade = False
    no_trade_reason = None
    if top:
        if top["score"] < 62:
            no_trade = True
            no_trade_reason = "종합점수가 기준치(62) 미만이라 오늘은 관망 권장"
        if top["components"]["risk"]["volPct"] > 55:
            no_trade = True
            no_trade_reason = "변동성 과열(초고변동) 구간으로 진입 보류 권장"

    report = {
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "model": "Single-Stock Consensus Momentum v2",
        "methodology": "S=0.35R+0.25M+0.20C+0.10L+0.10V",
        "topPick": top,
        "rankings": rows,
        "noTrade": no_trade,
        "noTradeReason": no_trade_reason,
        "failed": failed,
    }

    _append_log(report)
    return report


def _append_log(report: Dict):
    state = []
    if STATE_PATH.exists():
        try:
            state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            state = []

    item = {
        "time": report.get("generatedAt"),
        "symbol": report.get("topPick", {}).get("symbol") if report.get("topPick") else None,
        "score": report.get("topPick", {}).get("score") if report.get("topPick") else None,
        "entry": report.get("topPick", {}).get("plan", {}).get("entryZone") if report.get("topPick") else None,
    }
    state.append(item)
    state = state[-200:]
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
