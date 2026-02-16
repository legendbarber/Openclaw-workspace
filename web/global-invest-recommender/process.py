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
    # Equity / Growth
    Asset("QQQ", "Nasdaq 100 ETF", "equity"),
    Asset("SPY", "S&P 500 ETF", "equity"),
    Asset("EFA", "MSCI EAFE ETF", "equity"),
    Asset("EEM", "MSCI Emerging ETF", "equity"),
    Asset("ARKK", "ARK Innovation ETF", "equity"),

    # Bonds
    Asset("TLT", "20Y Treasury ETF", "bond"),
    Asset("IEF", "7-10Y Treasury ETF", "bond"),
    Asset("LQD", "Investment Grade Corp Bond ETF", "bond"),
    Asset("HYG", "High Yield Bond ETF", "bond"),

    # Commodities / Metals / Energy
    Asset("GLD", "Gold ETF", "metal"),
    Asset("SLV", "Silver ETF", "metal"),
    Asset("USO", "Crude Oil ETF", "commodity"),
    Asset("DBC", "Broad Commodity ETF", "commodity"),

    # Real assets
    Asset("VNQ", "US REIT ETF", "reit"),

    # Crypto proxies + spot (where available)
    Asset("BTC-USD", "Bitcoin", "crypto"),
    Asset("ETH-USD", "Ethereum", "crypto"),
]

# Macro regime inputs
MACRO_SYMBOLS = {
    "VIX": "^VIX",
    "DXY": "DX-Y.NYB",  # sometimes unavailable depending on region/source
}


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
    # risk_on in [0,1], higher = risk assets favored
    vix_s = safe_download(MACRO_SYMBOLS["VIX"], "3mo")
    dxy_s = safe_download(MACRO_SYMBOLS["DXY"], "3mo")

    vix = float(vix_s.iloc[-1]) if vix_s is not None else 20.0
    dxy_mom = pct_change(dxy_s, 20) if dxy_s is not None else 0.0

    vix_score = np.clip((28 - vix) / 18, 0, 1)  # low VIX => risk-on
    dxy_score = np.clip((-dxy_mom + 0.03) / 0.06, 0, 1)  # weak dollar => risk-on

    risk_on = float(0.6 * vix_score + 0.4 * dxy_score)
    return {
        "risk_on": round(risk_on, 4),
        "vix": round(vix, 2),
        "dxy_1m_pct": round(dxy_mom * 100, 2),
    }


def category_bias(category: str, risk_on: float) -> float:
    # regime tilt in score points
    if category in {"equity", "crypto", "reit"}:
        return (risk_on - 0.5) * 12
    if category in {"bond", "metal"}:
        return (0.5 - risk_on) * 10
    return 0.0


def score_asset(asset: Asset, prices: pd.Series, risk_on: float) -> Dict:
    m1 = pct_change(prices, 21)
    m3 = pct_change(prices, 63)
    m6 = pct_change(prices, 126)

    ma20 = float(prices.rolling(20).mean().iloc[-1])
    ma50 = float(prices.rolling(50).mean().iloc[-1])
    last = float(prices.iloc[-1])

    trend = 0.0
    trend += 1.0 if last > ma20 else -1.0
    trend += 1.0 if ma20 > ma50 else -1.0

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
        "metrics": {
            "m1Pct": round(m1 * 100, 2),
            "m3Pct": round(m3 * 100, 2),
            "m6Pct": round(m6 * 100, 2),
            "volAnnPct": round(vol * 100, 2),
            "maxDrawdownPct": round(-dd * 100, 2),
            "trend": round(trend, 2),
            "regimeBias": round(regime_bias, 2),
        },
        "links": {
            "yahoo": f"https://finance.yahoo.com/quote/{asset.symbol}",
            "tradingview": f"https://www.tradingview.com/symbols/{asset.symbol.replace('-', '')}/",
        },
    }


def run_process(top_n: int = 5) -> Dict:
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

    report = {
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "model": "Global Multi-Asset Momentum-Regime v1",
        "macro": regime,
        "topPicks": rows[:top_n],
        "allRankings": rows,
        "failed": failed,
        "disclaimer": "투자 권유가 아니며, 실제 투자 전 본인 책임 하에 추가 검증이 필요합니다.",
    }
    return report


def main():
    report = run_process(top_n=7)

    print("\n=== Global Top Picks ===")
    for i, r in enumerate(report["topPicks"], 1):
        print(f"{i}. {r['symbol']} ({r['name']}) | score={r['score']} | exp3m={r['expected3mPct']}%")

    with open("latest_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("\nSaved: latest_report.json")


if __name__ == "__main__":
    main()
