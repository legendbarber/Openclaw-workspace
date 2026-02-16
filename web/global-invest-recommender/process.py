from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf


@dataclass
class Asset:
    symbol: str
    name: str
    category: str


UNIVERSE: List[Asset] = [
    Asset("QQQ", "Nasdaq 100 ETF", "equity"),
    Asset("SPY", "S&P 500 ETF", "equity"),
    Asset("EFA", "MSCI EAFE ETF", "equity"),
    Asset("EEM", "MSCI Emerging ETF", "equity"),
    Asset("ARKK", "ARK Innovation ETF", "equity"),
    Asset("SOXL", "Semiconductor 3x ETF", "equity"),
    Asset("TQQQ", "Nasdaq 100 3x ETF", "equity"),
    Asset("TLT", "20Y Treasury ETF", "bond"),
    Asset("IEF", "7-10Y Treasury ETF", "bond"),
    Asset("LQD", "Investment Grade Corp Bond ETF", "bond"),
    Asset("HYG", "High Yield Bond ETF", "bond"),
    Asset("GLD", "Gold ETF", "metal"),
    Asset("SLV", "Silver ETF", "metal"),
    Asset("USO", "Crude Oil ETF", "commodity"),
    Asset("DBC", "Broad Commodity ETF", "commodity"),
    Asset("VNQ", "US REIT ETF", "reit"),
]

MACRO_SYMBOLS = {"VIX": "^VIX", "DXY": "DX-Y.NYB"}

POSITIVE_NEWS = ["surge", "beat", "rally", "upgrade", "strong", "record", "gain", "bull"]
NEGATIVE_NEWS = ["drop", "miss", "downgrade", "weak", "lawsuit", "fall", "risk", "bear"]


def fetch_article_preview(url: str) -> str:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        html = urllib.request.urlopen(req, timeout=6).read().decode("utf-8", "ignore")
        m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
        if not m:
            m = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
        if m:
            return re.sub(r'\s+', ' ', m.group(1)).strip()[:220]
        return ""
    except Exception:
        return ""


def fetch_news_digest(query: str, limit: int = 6) -> Dict:
    try:
        q = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        xml = urllib.request.urlopen(req, timeout=12).read().decode("utf-8", "ignore")

        items = re.findall(r"<item>(.*?)</item>", xml, re.S)
        parsed = []
        for raw in items[:limit]:
            t = re.search(r"<title><!\[CDATA\[(.*?)\]\]></title>", raw)
            l = re.search(r"<link>(.*?)</link>", raw)
            s = re.search(r"<source[^>]*>(.*?)</source>", raw)
            if t and l:
                parsed.append({
                    "title": t.group(1).strip(),
                    "link": l.group(1).strip(),
                    "source": s.group(1).strip() if s else "",
                })

        headlines = [x["title"] for x in parsed]

        score = 0
        for h in headlines:
            low = h.lower()
            score += sum(1 for w in POSITIVE_NEWS if w in low)
            score -= sum(1 for w in NEGATIVE_NEWS if w in low)

        insights = []
        for it in parsed[:2]:
            preview = fetch_article_preview(it["link"])
            insights.append({
                "title": it["title"],
                "source": it["source"],
                "url": it["link"],
                "summary": preview if preview else "미리보기 요약을 가져오지 못했습니다. 링크 원문 확인 필요."
            })

        return {
            "source": "Google News RSS + article metadata",
            "query": query,
            "headlineCount": len(headlines),
            "sentimentScore": score,
            "headlines": headlines,
            "insights": insights,
        }
    except Exception:
        return {
            "source": "Google News RSS + article metadata",
            "query": query,
            "headlineCount": 0,
            "sentimentScore": 0,
            "headlines": [],
            "insights": [],
        }


def research_links(asset: Asset) -> Dict:
    base_q = urllib.parse.quote(f"{asset.name} {asset.symbol} outlook report")
    links = {
        "googleNews": f"https://news.google.com/search?q={urllib.parse.quote(asset.symbol)}",
        "googleReports": f"https://www.google.com/search?q={base_q}",
        "yahooAnalysis": f"https://finance.yahoo.com/quote/{asset.symbol}/analysis/",
    }
    if asset.category == "crypto":
        links["coinDesk"] = f"https://www.coindesk.com/search?s={urllib.parse.quote(asset.symbol)}"
    if asset.category in {"equity", "reit", "bond", "metal", "commodity"}:
        links["seekingAlphaSearch"] = f"https://seekingalpha.com/search?query={urllib.parse.quote(asset.symbol)}"
    return links


def safe_download(symbol: str, period: str = "1y") -> pd.Series | None:
    try:
        df = yf.Ticker(symbol).history(period=period, auto_adjust=True)
        if df is None or df.empty or "Close" not in df:
            return None
        s = df["Close"].dropna()
        return s if len(s) > 40 else None
    except Exception:
        return None


def fetch_consensus_snapshot(symbol: str) -> Dict:
    try:
        info = yf.Ticker(symbol).info or {}
        current = info.get("currentPrice")
        target = info.get("targetMeanPrice")
        rec_mean = info.get("recommendationMean")
        rec_key = info.get("recommendationKey")
        analysts = info.get("numberOfAnalystOpinions")

        upside = None
        if current and target:
            upside = (target / current - 1) * 100

        bonus = 0.0
        if upside is not None:
            bonus += float(np.clip(upside / 8, -5, 8))
        if isinstance(rec_mean, (int, float)):
            bonus += float(np.clip((3.0 - rec_mean) * 4, -6, 8))

        return {
            "currentPrice": current,
            "targetMeanPrice": target,
            "upsidePct": None if upside is None else round(float(upside), 2),
            "recommendationMean": rec_mean,
            "recommendationKey": rec_key,
            "analystOpinions": analysts,
            "consensusBonus": round(float(bonus), 2),
            "status": "ok" if (target is not None or rec_mean is not None) else "empty",
            "usedSymbol": symbol,
            "fallbackFrom": None,
        }
    except Exception:
        return {
            "currentPrice": None,
            "targetMeanPrice": None,
            "upsidePct": None,
            "recommendationMean": None,
            "recommendationKey": None,
            "analystOpinions": None,
            "consensusBonus": 0.0,
            "status": "error",
            "usedSymbol": symbol,
            "fallbackFrom": None,
        }


def fetch_consensus_with_fallback(symbol: str) -> Dict:
    # 레버리지/특수 ETF 등은 컨센서스가 비는 경우가 많아 기초자산으로 보완
    fallback_map = {
        "SOXL": "NVDA",
        "TQQQ": "QQQ",
        "USO": "CL=F",
        "DBC": "DJP",
        "GLD": "GC=F",
        "SLV": "SI=F",
        "VNQ": "XLRE",
    }

    base = fetch_consensus_snapshot(symbol)
    if base.get("status") == "ok":
        return base

    fb = fallback_map.get(symbol)
    if not fb:
        return base

    sub = fetch_consensus_snapshot(fb)
    sub["fallbackFrom"] = symbol
    if sub.get("status") == "ok":
        sub["status"] = "fallback"
    return sub


def pct_change(series: pd.Series, days: int) -> float:
    if len(series) <= days:
        return 0.0
    return float(series.iloc[-1] / series.iloc[-days - 1] - 1)


def max_drawdown(series: pd.Series) -> float:
    arr = series.values
    peak = np.maximum.accumulate(arr)
    dd = (arr / peak) - 1
    return float(dd.min())


def annualized_vol(series: pd.Series) -> float:
    rets = series.pct_change().dropna()
    if len(rets) < 20:
        return 0.0
    return float(rets.std() * np.sqrt(252))


def macro_regime() -> Dict[str, float]:
    vix_s = safe_download(MACRO_SYMBOLS["VIX"], "3mo")
    dxy_s = safe_download(MACRO_SYMBOLS["DXY"], "3mo")

    vix = float(vix_s.iloc[-1]) if vix_s is not None else 20.0
    dxy_mom = pct_change(dxy_s, 20) if dxy_s is not None else 0.0

    vix_score = np.clip((28 - vix) / 18, 0, 1)
    dxy_score = np.clip((-dxy_mom + 0.03) / 0.06, 0, 1)
    risk_on = float(0.6 * vix_score + 0.4 * dxy_score)

    return {"risk_on": round(risk_on, 4), "vix": round(vix, 2), "dxy_1m_pct": round(dxy_mom * 100, 2)}


def category_bias(category: str, risk_on: float, mode: str = "balanced") -> float:
    if mode == "aggressive":
        if category in {"equity", "crypto", "reit"}:
            return (risk_on - 0.45) * 18
        if category in {"bond", "metal"}:
            return (0.45 - risk_on) * 8
        return 0.0

    if category in {"equity", "crypto", "reit"}:
        return (risk_on - 0.5) * 12
    if category in {"bond", "metal"}:
        return (0.5 - risk_on) * 10
    return 0.0


def buy_guide(category: str) -> Dict[str, str]:
    if category == "equity":
        return {"where": "국내증권사 해외주식(MTS/HTS)에서 티커로 매수", "note": "환전 스프레드/거래시간 확인"}
    if category == "bond":
        return {"where": "해외ETF로 증권사에서 매수", "note": "금리 민감도(듀레이션) 확인"}
    if category in {"metal", "commodity", "reit"}:
        return {"where": "해외ETF(GLD/SLV/USO/VNQ 등)로 증권사에서 매수", "note": "ETF 괴리율/보수 확인"}
    if category == "crypto":
        return {"where": "국내 원화거래소 또는 해외거래소 현물 매수", "note": "레버리지 금지, 지갑/보안 우선"}
    return {"where": "증권/거래 플랫폼 확인", "note": "유동성/수수료 확인"}


def make_plan(asset: Asset, last: float, expected_3m: float, vol: float, mode: str = "balanced") -> Dict:
    entry_low = last * 0.985
    entry_high = last * 1.015

    if mode == "aggressive":
        stop_pct = min(max(vol * 0.7, 0.07), 0.22)
        tp1_pct = max(expected_3m * 0.65, 6.0)
        tp2_pct = max(expected_3m * 1.15, 10.0)
        holding = "2~10주"
        sizing = "단일 자산 최대 30%, 4개 이상 분산 권장"
    else:
        stop_pct = min(max(vol * 0.55, 0.05), 0.16)
        tp1_pct = max(expected_3m * 0.55, 4.0)
        tp2_pct = max(expected_3m, 7.0)
        holding = "4~12주"
        sizing = "단일 자산 최대 20%, 5개 분산 권장"

    guide = buy_guide(asset.category)

    return {
        "whereToBuy": guide["where"],
        "executionNote": guide["note"],
        "entryZone": [round(entry_low, 2), round(entry_high, 2)],
        "stopLoss": round(last * (1 - stop_pct), 2),
        "takeProfit1": round(last * (1 + tp1_pct / 100), 2),
        "takeProfit2": round(last * (1 + tp2_pct / 100), 2),
        "holdingPeriod": holding,
        "rebalancingRule": "주 1회 점검, 점수 하락(상위 5위 이탈) 시 비중 축소",
        "positionSizing": sizing
    }


def build_why(asset: Asset, m1: float, m3: float, m6: float, trend: float, regime_bias: float, vol: float, dd: float, news_score: int, consensus: Dict) -> List[str]:
    reasons = []
    if m3 > 0:
        reasons.append(f"최근 3개월 모멘텀이 플러스({m3*100:.2f}%)입니다.")
    if m1 > 0:
        reasons.append(f"최근 1개월 모멘텀이 유지되고 있습니다({m1*100:.2f}%).")
    if trend > 0:
        reasons.append("가격이 20/50일 이동평균 대비 우상향 추세입니다.")
    if regime_bias > 0:
        reasons.append("현재 매크로 레짐(리스크온/오프)에 유리한 자산군입니다.")
    if vol < 0.28:
        reasons.append(f"연환산 변동성이 상대적으로 관리 가능한 수준입니다({vol*100:.2f}%).")
    if dd < 0.25:
        reasons.append(f"과거 1년 최대낙폭이 과도하지 않은 편입니다({-dd*100:.2f}%).")
    if news_score > 0:
        reasons.append("최근 뉴스/리포트 헤드라인 톤이 우호적입니다.")
    if consensus.get("upsidePct") is not None:
        reasons.append(f"애널리스트 평균 목표가 대비 괴리율은 {consensus.get('upsidePct')}% 입니다.")
    if consensus.get("recommendationMean") is not None:
        reasons.append(f"애널리스트 평균 추천지수는 {consensus.get('recommendationMean')} (낮을수록 우호)입니다.")

    if not reasons:
        reasons.append("상대점수 기반으로 상위권에 올라 추천 후보로 선정되었습니다.")
    return reasons[:4]


def score_asset(asset: Asset, prices: pd.Series, risk_on: float, mode: str = "balanced") -> Dict:
    m1 = pct_change(prices, 21)
    m3 = pct_change(prices, 63)
    m6 = pct_change(prices, 126)

    ma20 = float(prices.rolling(20).mean().iloc[-1])
    ma50 = float(prices.rolling(50).mean().iloc[-1])
    last = float(prices.iloc[-1])

    trend = (1.0 if last > ma20 else -1.0) + (1.0 if ma20 > ma50 else -1.0)
    vol = annualized_vol(prices)
    dd = abs(max_drawdown(prices))

    momentum_score = (m1 * 0.35 + m3 * 0.4 + m6 * 0.25) * 220
    trend_score = trend * 8
    if mode == "aggressive":
        vol_penalty = np.clip(vol - 0.32, 0, 1.0) * 7
        dd_penalty = np.clip(dd - 0.30, 0, 1.0) * 6
    else:
        vol_penalty = np.clip(vol - 0.22, 0, 1.0) * 18
        dd_penalty = np.clip(dd - 0.18, 0, 1.0) * 15
    regime_bias = category_bias(asset.category, risk_on, mode)

    news = fetch_news_digest(f"{asset.symbol} {asset.name}", limit=6)
    news_bonus = np.clip(news.get("sentimentScore", 0), -3, 3) * 1.5
    consensus = fetch_consensus_with_fallback(asset.symbol)
    consensus_bonus = consensus.get("consensusBonus", 0.0)

    expected_3m = (m1 * 0.30 + m3 * 0.55 + m6 * 0.15) * 100

    aggressive_return_boost = 0.0
    aggressive_risk_bonus = 0.0
    aggressive_safety_penalty = 0.0
    if mode == "aggressive":
        aggressive_return_boost = expected_3m * 0.65
        if asset.category in {"equity", "crypto", "reit"}:
            aggressive_risk_bonus += 8.0
        if asset.category in {"bond", "metal"}:
            aggressive_safety_penalty += 14.0

    total = momentum_score + trend_score + regime_bias - vol_penalty - dd_penalty + news_bonus + consensus_bonus + aggressive_return_boost + aggressive_risk_bonus - aggressive_safety_penalty

    return {
        "symbol": asset.symbol,
        "name": asset.name,
        "category": asset.category,
        "score": round(float(total), 2),
        "expected3mPct": round(float(expected_3m), 2),
        "currentPrice": round(last, 2),
        "whyRecommended": build_why(asset, m1, m3, m6, trend, regime_bias, vol, dd, int(news.get("sentimentScore", 0)), consensus),
        "metrics": {
            "m1Pct": round(m1 * 100, 2),
            "m3Pct": round(m3 * 100, 2),
            "m6Pct": round(m6 * 100, 2),
            "volAnnPct": round(vol * 100, 2),
            "maxDrawdownPct": round(-dd * 100, 2),
            "trend": round(trend, 2),
            "regimeBias": round(regime_bias, 2),
        },
        "source": {
            "priceData": "Yahoo Finance (yfinance)",
            "newsData": news,
            "consensusData": consensus,
            "symbol": asset.symbol,
            "lookback": "1y daily close",
            "macroInputs": [MACRO_SYMBOLS["VIX"], MACRO_SYMBOLS["DXY"]]
        },
        "plan": make_plan(asset, last, float(expected_3m), vol, mode=mode),
        "links": {
            "yahoo": f"https://finance.yahoo.com/quote/{asset.symbol}",
            "tradingview": f"https://www.tradingview.com/symbols/{asset.symbol.replace('-', '')}/",
            **research_links(asset),
        },
    }


def run_process(top_n: int = 7, mode: str = "balanced") -> Dict:
    mode = (mode or "balanced").lower()
    if mode not in {"balanced", "aggressive"}:
        mode = "balanced"

    regime = macro_regime()
    risk_on = regime["risk_on"]

    rows = []
    failed = []
    for asset in UNIVERSE:
        s = safe_download(asset.symbol, "1y")
        if s is None:
            failed.append(asset.symbol)
            continue
        rows.append(score_asset(asset, s, risk_on, mode=mode))

    rows.sort(key=lambda x: x["score"], reverse=True)
    risk_rows = [x for x in rows if x["category"] in {"equity", "crypto", "reit"}]

    return {
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "mode": mode,
        "model": "Global Multi-Asset Momentum-Regime v3", 
        "methodology": "1M/3M/6M 모멘텀 + 20/50일 추세 + 변동성/낙폭 패널티 + VIX/DXY 기반 레짐 바이어스",
        "dataSources": [
            "Yahoo Finance price history via yfinance",
            f"Macro inputs: {MACRO_SYMBOLS['VIX']}, {MACRO_SYMBOLS['DXY']}"
        ],
        "macro": regime,
        "topPicks": rows[:top_n],
        "topRiskPicks": risk_rows[:top_n],
        "allRankings": rows,
        "failed": failed,
        "disclaimer": "본 정보는 투자 권유가 아닙니다. 손익 책임은 투자자 본인에게 있으며, 실제 매매 전 추가 검증이 필요합니다.",
    }


def main():
    report = run_process(top_n=7)
    print("\n=== Global Top Picks ===")
    for i, r in enumerate(report["topPicks"], 1):
        print(f"{i}. {r['symbol']} | score={r['score']} | exp3m={r['expected3mPct']}% | entry={r['plan']['entryZone']}")

    with open("latest_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("Saved: latest_report.json")


if __name__ == "__main__":
    main()
