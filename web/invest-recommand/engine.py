from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf


@dataclass
class Asset:
    symbol: str
    name: str
    category: str


UNIVERSE = [
    # US stocks
    Asset("NVDA", "NVIDIA Corp", "us-stock"),
    Asset("TSLA", "Tesla Inc", "us-stock"),
    Asset("AMD", "Advanced Micro Devices", "us-stock"),
    Asset("PLTR", "Palantir Technologies", "us-stock"),
    Asset("SMCI", "Super Micro Computer", "us-stock"),
    Asset("META", "Meta Platforms", "us-stock"),
    Asset("NFLX", "Netflix Inc", "us-stock"),
    Asset("AMZN", "Amazon.com Inc", "us-stock"),
    Asset("COIN", "Coinbase Global", "us-stock"),
    Asset("MSTR", "MicroStrategy", "us-stock"),

    # KR stocks (Yahoo Finance suffix)
    Asset("005930.KS", "Samsung Electronics", "kr-stock"),
    Asset("000660.KS", "SK hynix", "kr-stock"),
    Asset("035420.KS", "NAVER", "kr-stock"),
    Asset("035720.KS", "Kakao", "kr-stock"),
    Asset("068270.KS", "Celltrion", "kr-stock"),
    Asset("207940.KS", "Samsung Biologics", "kr-stock"),
    Asset("051910.KS", "LG Chem", "kr-stock"),
    Asset("105560.KS", "KB Financial", "kr-stock"),
    Asset("012330.KS", "Hyundai Motor", "kr-stock"),
    Asset("034020.KS", "Doosan Enerbility", "kr-stock"),
]

STATE_PATH = Path(__file__).resolve().parent / "state_log.json"
SNAPSHOT_DIR = Path(__file__).resolve().parent / "snapshots"
KST = ZoneInfo("Asia/Seoul")
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


def _technical_score(s: pd.Series) -> Dict:
    """가격/차트 기반 기술적 진입 타이밍 점수 (모멘텀과 분리)."""
    close = s.astype(float)
    cur = float(close.iloc[-1])

    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma60 = float(close.rolling(60).mean().iloc[-1])
    ma120 = float(close.rolling(120).mean().iloc[-1]) if len(close) >= 120 else ma60

    # RSI(14)
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta.where(delta < 0, 0.0))
    avg_gain = float(gain.rolling(14).mean().iloc[-1])
    avg_loss = float(loss.rolling(14).mean().iloc[-1])
    if avg_loss == 0:
        rsi14 = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi14 = 100 - (100 / (1 + rs))

    dist_ma20 = (cur / ma20 - 1) * 100 if ma20 else 0.0
    high20 = float(close.tail(20).max())
    from_high20 = (cur / high20 - 1) * 100 if high20 else 0.0

    score = 50.0

    # 1) 추세 정합성: 장기적으로 우상향 구조 선호
    if ma20 > ma60 > ma120:
        score += 15
    elif ma20 > ma60:
        score += 8
    else:
        score -= 8

    # 2) RSI 과열/침체 필터: 과열 추격 페널티, 적정 눌림 선호
    if 38 <= rsi14 <= 55:
        score += 20
        setup = "pullback-in-uptrend"
    elif 55 < rsi14 <= 68:
        score += 8
        setup = "healthy-trend"
    elif rsi14 > 72:
        score -= 20
        setup = "overbought"
    elif rsi14 < 30:
        score -= 8
        setup = "falling-knife-risk"
    else:
        setup = "neutral"

    # 3) 이격도: 너무 벌어진 추격 매수 방지
    if dist_ma20 > 8:
        score -= 18
    elif dist_ma20 > 4:
        score -= 8
    elif -6 <= dist_ma20 <= -1:
        score += 8

    # 4) 최근 고점 대비 눌림
    if -10 <= from_high20 <= -3:
        score += 10
    elif from_high20 < -18:
        score -= 10

    score = float(np.clip(score, 0, 100))

    return {
        "score": round(score, 2),
        "rsi14": round(float(rsi14), 2),
        "distMa20Pct": round(float(dist_ma20), 2),
        "from20dHighPct": round(float(from_high20), 2),
        "ma20": round(ma20, 2),
        "ma60": round(ma60, 2),
        "ma120": round(ma120, 2),
        "setup": setup,
    }


def evaluate_asset(asset: Asset) -> Dict | None:
    s = _download_close(asset.symbol, "1y")
    if s is None:
        return None

    report_consensus = _consensus(asset.symbol)
    momentum = _momentum_score(s)
    crowd = _news(asset.symbol, asset.name)
    liquidity = _liquidity_score(asset.symbol)
    risk = _risk_score(s)
    technical = _technical_score(s)

    # 사용자 요청 반영: 모멘텀 제외 + 기술적 분석(차트/가격) 반영
    score = (
        0.35 * report_consensus["score"] +
        0.25 * crowd["score"] +
        0.15 * liquidity +
        0.10 * risk["score"] +
        0.15 * technical["score"]
    )

    cur = float(s.iloc[-1])
    atrp = float(s.pct_change().abs().tail(14).mean()) if len(s) > 20 else 0.03
    stop = cur * (1 - max(0.04, min(0.14, atrp * 1.8)))
    tp1 = cur * (1 + max(0.06, min(0.22, (_pct(s, 63) * 0.6 + 0.06))))
    tp2 = cur * (1 + max(0.1, min(0.35, (_pct(s, 126) * 0.8 + 0.12))))

    invalidation = "종가가 20일선 하회 + 거래강도 둔화가 2거래일 연속이면 추천 무효"

    expected_loss_pct = (stop / cur - 1) * 100
    expected_return1_pct = (tp1 / cur - 1) * 100
    expected_return2_pct = (tp2 / cur - 1) * 100
    rr_ratio = expected_return1_pct / abs(expected_loss_pct) if expected_loss_pct < 0 else 0.0

    return {
        "symbol": asset.symbol,
        "name": asset.name,
        "category": asset.category,
        "score": round(float(score), 2),
        "currentPrice": round(cur, 2),
        "expectedLossPct": round(float(expected_loss_pct), 2),
        "expectedReturnPct": round(float(expected_return1_pct), 2),
        "expectedReturn2Pct": round(float(expected_return2_pct), 2),
        "riskReward": round(float(rr_ratio), 2),
        "components": {
            "reportConsensus": report_consensus,
            "momentum": momentum,
            "technical": technical,
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


def _is_etf_like(row: Dict) -> bool:
    name = str(row.get("name", "")).lower()
    category = str(row.get("category", "")).lower()
    symbol = str(row.get("symbol", "")).upper()
    if "etf" in name:
        return True
    if category in {"equity", "reit", "bond", "metal", "commodity", "etf"}:
        return True
    # US ETF symbols often end with known ETF tickers (fallback guard)
    return symbol in {"SPY", "QQQ", "EEM", "EFA", "VNQ", "TLT", "IEF", "LQD", "GLD", "SLV", "USO", "DBC"}


def build_report(market: str = "all") -> Dict:
    rows = []
    failed = []
    for a in UNIVERSE:
        r = evaluate_asset(a)
        if r is None:
            failed.append(a.symbol)
        else:
            rows.append(r)

    # 사용자 요청: ETF 제외 (단일 주식만 허용)
    rows = [r for r in rows if not _is_etf_like(r)]

    mk = (market or "all").strip().lower()
    if mk == "us":
        rows = [r for r in rows if str(r.get("category", "")).startswith("us-")]
    elif mk == "kr":
        rows = [r for r in rows if str(r.get("category", "")).startswith("kr-")]

    rows.sort(key=lambda x: x["score"], reverse=True)

    # 위험대비 기대수익(리스크/리워드) 우선 랭킹
    risk_adjusted = sorted(
        rows,
        key=lambda x: (
            x.get("riskReward", 0),
            x.get("expectedReturnPct", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    # 절대 기대수익(1차 익절 기준) 우선 랭킹
    high_return = sorted(
        rows,
        key=lambda x: (
            x.get("expectedReturnPct", 0),
            x.get("riskReward", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    top = risk_adjusted[0] if risk_adjusted else None

    no_trade = False
    no_trade_reason = None
    if top:
        if top["score"] < 58:
            no_trade = True
            no_trade_reason = "종합점수가 기준치(58) 미만이라 오늘은 관망 권장"
        if top["components"]["risk"]["volPct"] > 65:
            no_trade = True
            no_trade_reason = "변동성 과열(초고변동) 구간으로 진입 보류 권장"

    market_label = {"all": "KR+US", "kr": "KR", "us": "US"}.get(mk, "KR+US")

    report = {
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "model": f"{market_label} Single-Stock Dual Ranking v5 (No Momentum + Technical)",
        "market": mk,
        "methodology": "S=0.35R+0.25C+0.15L+0.10V+0.15T + Dual Rank(RR/Return)",
        "topPick": top,
        "rankings": rows,
        "riskAdjustedRankings": risk_adjusted,
        "highReturnRankings": high_return,
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


def save_daily_snapshot(force: bool = False) -> Dict:
    """Save one snapshot per KST day for backtesting validation.

    Default behavior: create/update only once per day.
    If force=True, always refresh today's snapshot.
    """
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    now_kst = datetime.now(KST)
    day = now_kst.strftime("%Y-%m-%d")
    path = SNAPSHOT_DIR / f"{day}.json"

    if path.exists() and not force:
        saved = json.loads(path.read_text(encoding="utf-8"))
        return {"saved": False, "reason": "already_exists", "path": str(path), "date": day, "generatedAt": saved.get("generatedAt")}

    report = build_report()
    payload = {
        "dateKST": day,
        "savedAtKST": now_kst.isoformat(),
        "generatedAt": report.get("generatedAt"),
        "model": report.get("model"),
        "methodology": report.get("methodology"),
        "topPick": report.get("topPick"),
        "riskAdjustedTop5": (report.get("riskAdjustedRankings") or [])[:5],
        "highReturnTop5": (report.get("highReturnRankings") or [])[:5],
        "failed": report.get("failed", []),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"saved": True, "path": str(path), "date": day, "generatedAt": payload.get("generatedAt")}


def list_snapshots(limit: int = 60) -> List[Dict]:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(SNAPSHOT_DIR.glob("*.json"), reverse=True)[: max(1, limit)]
    out = []
    for p in files:
        try:
            j = json.loads(p.read_text(encoding="utf-8"))
            out.append({
                "dateKST": j.get("dateKST") or p.stem,
                "generatedAt": j.get("generatedAt"),
                "topSymbol": (j.get("topPick") or {}).get("symbol"),
                "path": str(p),
            })
        except Exception:
            out.append({"dateKST": p.stem, "generatedAt": None, "topSymbol": None, "path": str(p)})
    return out


def get_snapshot(date_kst: str) -> Dict | None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_kst or ""):
        return None
    p = SNAPSHOT_DIR / f"{date_kst}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def list_snapshot_dates_by_month(ym: str) -> List[str]:
    """Return available snapshot dates for YYYY-MM."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    if not re.match(r"^\d{4}-\d{2}$", ym or ""):
        return []
    files = sorted(SNAPSHOT_DIR.glob(f"{ym}-*.json"))
    return [p.stem for p in files]


def get_current_change_vs_snapshot(date_kst: str) -> Dict:
    """For snapshot symbols, compute change(%) from snapshot-date price to latest price."""
    snap = get_snapshot(date_kst)
    if not snap:
        return {"error": "not_found", "dateKST": date_kst, "items": []}

    # collect unique symbols from snapshot cards
    seen = {}
    for key in ["riskAdjustedTop5", "highReturnTop5"]:
        for r in snap.get(key, []) or []:
            sym = r.get("symbol")
            base = r.get("currentPrice")
            name = r.get("name")
            if sym and isinstance(base, (int, float)) and base > 0 and sym not in seen:
                seen[sym] = {"symbol": sym, "name": name, "basePrice": float(base)}

    symbols = list(seen.keys())
    if not symbols:
        return {"dateKST": date_kst, "items": []}

    latest_map: Dict[str, float] = {}

    # try batch download first
    try:
        data = yf.download(symbols, period="5d", auto_adjust=True, progress=False, threads=True)
        if data is not None and not data.empty:
            if isinstance(data.columns, pd.MultiIndex):
                close = data.get("Close")
                if close is not None:
                    for sym in symbols:
                        try:
                            s = close[sym].dropna()
                            if len(s) > 0:
                                latest_map[sym] = float(s.iloc[-1])
                        except Exception:
                            pass
            elif "Close" in data:
                s = data["Close"].dropna()
                if len(s) > 0 and len(symbols) == 1:
                    latest_map[symbols[0]] = float(s.iloc[-1])
    except Exception:
        pass

    # fallback per symbol
    for sym in symbols:
        if sym in latest_map:
            continue
        try:
            s = _download_close(sym, "1mo")
            if s is not None and len(s) > 0:
                latest_map[sym] = float(s.iloc[-1])
        except Exception:
            pass

    items = []
    for sym in symbols:
        base = seen[sym]["basePrice"]
        cur = latest_map.get(sym)
        chg = None if cur is None else ((cur / base) - 1) * 100
        items.append({
            "symbol": sym,
            "name": seen[sym]["name"],
            "basePrice": round(base, 4),
            "currentPrice": None if cur is None else round(float(cur), 4),
            "changePct": None if chg is None else round(float(chg), 2),
        })

    return {
        "dateKST": date_kst,
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "items": items,
    }
