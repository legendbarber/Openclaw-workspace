from __future__ import annotations

import json
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
    Asset("TLT", "20Y Treasury ETF", "bond"),
    Asset("IEF", "7-10Y Treasury ETF", "bond"),
    Asset("LQD", "Investment Grade Corp Bond ETF", "bond"),
    Asset("HYG", "High Yield Bond ETF", "bond"),
    Asset("GLD", "Gold ETF", "metal"),
    Asset("SLV", "Silver ETF", "metal"),
    Asset("USO", "Crude Oil ETF", "commodity"),
    Asset("DBC", "Broad Commodity ETF", "commodity"),
    Asset("VNQ", "US REIT ETF", "reit"),
    Asset("BTC-USD", "Bitcoin", "crypto"),
    Asset("ETH-USD", "Ethereum", "crypto"),
]

MACRO_SYMBOLS = {"VIX": "^VIX", "DXY": "DX-Y.NYB"}


def safe_download(symbol: str, period: str = "1y") -> pd.Series | None:
    try:
        df = yf.Ticker(symbol).history(period=period, auto_adjust=True)
        if df is None or df.empty or "Close" not in df:
            return None
        s = df["Close"].dropna()
        return s if len(s) > 40 else None
    except Exception:
        return None


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


def category_bias(category: str, risk_on: float) -> float:
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


def make_plan(asset: Asset, last: float, expected_3m: float, vol: float) -> Dict:
    entry_low = last * 0.985
    entry_high = last * 1.015

    stop_pct = min(max(vol * 0.55, 0.05), 0.16)
    tp1_pct = max(expected_3m * 0.55, 4.0)
    tp2_pct = max(expected_3m, 7.0)

    guide = buy_guide(asset.category)

    return {
        "whereToBuy": guide["where"],
        "executionNote": guide["note"],
        "entryZone": [round(entry_low, 2), round(entry_high, 2)],
        "stopLoss": round(last * (1 - stop_pct), 2),
        "takeProfit1": round(last * (1 + tp1_pct / 100), 2),
        "takeProfit2": round(last * (1 + tp2_pct / 100), 2),
        "holdingPeriod": "4~12주",
        "rebalancingRule": "주 1회 점검, 점수 하락(상위 5위 이탈) 시 비중 축소",
        "positionSizing": "단일 자산 최대 20%, 5개 분산 권장"
    }


def score_asset(asset: Asset, prices: pd.Series, risk_on: float) -> Dict:
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
    vol_penalty = np.clip(vol - 0.22, 0, 1.0) * 18
    dd_penalty = np.clip(dd - 0.18, 0, 1.0) * 15
    regime_bias = category_bias(asset.category, risk_on)

    total = momentum_score + trend_score + regime_bias - vol_penalty - dd_penalty
    expected_3m = (m1 * 0.30 + m3 * 0.55 + m6 * 0.15) * 100

    return {
        "symbol": asset.symbol,
        "name": asset.name,
        "category": asset.category,
        "score": round(float(total), 2),
        "expected3mPct": round(float(expected_3m), 2),
        "currentPrice": round(last, 2),
        "metrics": {
            "m1Pct": round(m1 * 100, 2),
            "m3Pct": round(m3 * 100, 2),
            "m6Pct": round(m6 * 100, 2),
            "volAnnPct": round(vol * 100, 2),
            "maxDrawdownPct": round(-dd * 100, 2),
            "trend": round(trend, 2),
            "regimeBias": round(regime_bias, 2),
        },
        "plan": make_plan(asset, last, float(expected_3m), vol),
        "links": {
            "yahoo": f"https://finance.yahoo.com/quote/{asset.symbol}",
            "tradingview": f"https://www.tradingview.com/symbols/{asset.symbol.replace('-', '')}/",
        },
    }


def run_process(top_n: int = 7) -> Dict:
    regime = macro_regime()
    risk_on = regime["risk_on"]

    rows = []
    failed = []
    for asset in UNIVERSE:
        s = safe_download(asset.symbol, "1y")
        if s is None:
            failed.append(asset.symbol)
            continue
        rows.append(score_asset(asset, s, risk_on))

    rows.sort(key=lambda x: x["score"], reverse=True)

    return {
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "model": "Global Multi-Asset Momentum-Regime v2",
        "macro": regime,
        "topPicks": rows[:top_n],
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
