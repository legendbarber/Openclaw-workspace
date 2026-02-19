from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf


@dataclass
class Theme:
    name: str
    etf: str
    stocks: List[str]


THEMES: List[Theme] = [
    Theme("AI 반도체", "SOXX", ["NVDA", "AMD", "AVGO", "TSM"]),
    Theme("전력 인프라", "XLU", ["CEG", "VST", "ETR", "PEG"]),
    Theme("사이버보안", "BUG", ["CRWD", "PANW", "FTNT", "ZS"]),
    Theme("에너지(원유/가스)", "XLE", ["XOM", "CVX", "COP", "SLB"]),
    Theme("비만/헬스케어 혁신", "XLV", ["LLY", "NVO", "ISRG", "VRTX"]),
]

OUT_PATH = Path(__file__).resolve().parent / "public" / "theme-now.json"


def _get_close_map(tickers: List[str], period: str = "9mo") -> Dict[str, pd.Series]:
    df = yf.download(tickers=tickers, period=period, auto_adjust=True, progress=False, group_by="ticker", threads=True)
    out: Dict[str, pd.Series] = {}

    if df is None or df.empty:
        return out

    if isinstance(df.columns, pd.MultiIndex):
        for t in tickers:
            if t in df.columns.get_level_values(0):
                s = df[t]["Close"].dropna() if "Close" in df[t] else pd.Series(dtype=float)
                if len(s) > 70:
                    out[t] = s.astype(float)
    else:
        # 단일 티커 fallback
        s = df["Close"].dropna() if "Close" in df else pd.Series(dtype=float)
        if len(s) > 70 and len(tickers) == 1:
            out[tickers[0]] = s.astype(float)

    return out


def _score(s: pd.Series) -> Dict:
    cur = float(s.iloc[-1])
    r1 = float(s.iloc[-1] / s.iloc[-21] - 1) * 100 if len(s) > 21 else 0.0
    r3 = float(s.iloc[-1] / s.iloc[-63] - 1) * 100 if len(s) > 63 else 0.0
    r6 = float(s.iloc[-1] / s.iloc[-126] - 1) * 100 if len(s) > 126 else 0.0

    ma20 = float(s.rolling(20).mean().iloc[-1])
    ma60 = float(s.rolling(60).mean().iloc[-1])
    trend = 12 if cur > ma20 > ma60 else (2 if cur > ma20 else -10)

    vol = float(s.pct_change().dropna().std() * np.sqrt(252) * 100)
    vol_penalty = max(0.0, vol - 42) * 0.9

    raw = (r1 * 0.35) + (r3 * 0.4) + (r6 * 0.25) + trend - vol_penalty
    score = float(np.clip(50 + raw, 0, 100))

    return {
        "score": round(score, 2),
        "price": round(cur, 2),
        "m1": round(r1, 2),
        "m3": round(r3, 2),
        "m6": round(r6, 2),
        "vol": round(vol, 2),
    }


def build_theme_report() -> Dict:
    tickers: List[str] = []
    for th in THEMES:
        tickers.append(th.etf)
        tickers.extend(th.stocks)
    tickers = sorted(set(tickers))

    closes = _get_close_map(tickers)

    themes_out = []
    for th in THEMES:
        etf_score = _score(closes[th.etf]) if th.etf in closes else {"score": 50.0}
        members = []
        for s in th.stocks:
            if s in closes:
                d = _score(closes[s])
                d["symbol"] = s
                members.append(d)
        members.sort(key=lambda x: x["score"], reverse=True)

        theme_score = round(float(np.mean([etf_score.get("score", 50.0)] + [m["score"] for m in members])) if members else etf_score.get("score", 50.0), 2)
        themes_out.append({
            "theme": th.name,
            "proxy": th.etf,
            "themeScore": theme_score,
            "etf": etf_score,
            "leaders": members[:3],
        })

    themes_out.sort(key=lambda x: x["themeScore"], reverse=True)
    top_picks = []
    for t in themes_out[:3]:
        for m in t["leaders"][:2]:
            top_picks.append({
                "theme": t["theme"],
                "symbol": m["symbol"],
                "score": m["score"],
                "m1": m["m1"],
                "m3": m["m3"],
            })

    return {
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "method": "9개월 가격데이터 기반 모멘텀(1/3/6개월)+추세+변동성 패널티",
        "themes": themes_out,
        "topPicks": sorted(top_picks, key=lambda x: x["score"], reverse=True)[:6],
        "note": "투자 권유가 아닌 참고용 아이디어입니다.",
    }


def save_theme_report(path: Path = OUT_PATH) -> Dict:
    report = build_theme_report()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    data = save_theme_report()
    print(json.dumps({"generatedAt": data["generatedAt"], "topPicks": data["topPicks"]}, ensure_ascii=False, indent=2))
