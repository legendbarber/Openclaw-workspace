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
class KrTheme:
    name: str
    stocks: List[str]


KR_THEMES: List[KrTheme] = [
    KrTheme("반도체", ["005930.KS", "000660.KS", "042700.KS", "000990.KS"]),
    KrTheme("전력기기/인프라", ["267260.KS", "298040.KS", "010120.KS", "017800.KS"]),
    KrTheme("조선/해양", ["009540.KS", "042660.KS", "329180.KS", "011200.KS"]),
    KrTheme("방산", ["012450.KS", "079550.KS", "272210.KS", "047810.KS"]),
    KrTheme("자동차/부품", ["005380.KS", "000270.KS", "012330.KS", "161390.KS"]),
    KrTheme("바이오", ["207940.KS", "068270.KS", "196170.KQ", "000100.KS"]),
]

KR_NAMES = {
    "005930.KS": "삼성전자", "000660.KS": "SK하이닉스", "042700.KS": "한미반도체", "000990.KS": "DB하이텍",
    "267260.KS": "HD현대일렉트릭", "298040.KS": "효성중공업", "010120.KS": "LS ELECTRIC", "017800.KS": "현대엘리베이터",
    "009540.KS": "HD한국조선해양", "042660.KS": "한화오션", "329180.KS": "HD현대중공업", "011200.KS": "HMM",
    "012450.KS": "한화에어로스페이스", "079550.KS": "LIG넥스원", "272210.KS": "한화시스템", "047810.KS": "한국항공우주",
    "005380.KS": "현대차", "000270.KS": "기아", "012330.KS": "현대모비스", "161390.KS": "한국타이어앤테크놀로지",
    "207940.KS": "삼성바이오로직스", "068270.KS": "셀트리온", "196170.KQ": "알테오젠", "000100.KS": "유한양행",
}

OUT_PATH = Path(__file__).resolve().parent / "public" / "theme-now-kr.json"


def _download_close_map(symbols: List[str], period: str = "10mo") -> Dict[str, pd.Series]:
    out: Dict[str, pd.Series] = {}
    data = yf.download(tickers=symbols, period=period, auto_adjust=True, progress=False, threads=True, group_by="ticker")
    if data is None or data.empty:
        return out

    if isinstance(data.columns, pd.MultiIndex):
        for s in symbols:
            if s in data.columns.get_level_values(0):
                d = data[s]
                if "Close" in d:
                    c = d["Close"].dropna()
                    if len(c) > 70:
                        out[s] = c.astype(float)
    else:
        if len(symbols) == 1 and "Close" in data:
            c = data["Close"].dropna()
            if len(c) > 70:
                out[symbols[0]] = c.astype(float)

    return out


def _calc_score(close: pd.Series) -> Dict:
    cur = float(close.iloc[-1])
    m1 = float(close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) > 21 else 0.0
    m3 = float(close.iloc[-1] / close.iloc[-63] - 1) * 100 if len(close) > 63 else 0.0
    m6 = float(close.iloc[-1] / close.iloc[-126] - 1) * 100 if len(close) > 126 else 0.0

    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma60 = float(close.rolling(60).mean().iloc[-1])
    trend = 12 if cur > ma20 > ma60 else (2 if cur > ma20 else -10)

    ret = close.pct_change().dropna()
    vol = float(ret.std() * np.sqrt(252) * 100) if len(ret) > 20 else 0.0
    vol_penalty = max(0.0, vol - 45) * 0.8

    raw = (m1 * 0.35) + (m3 * 0.40) + (m6 * 0.25) + trend - vol_penalty
    score = float(np.clip(50 + raw, 0, 100))

    return {
        "score": round(score, 2),
        "price": round(cur, 2),
        "m1": round(m1, 2),
        "m3": round(m3, 2),
        "m6": round(m6, 2),
        "vol": round(vol, 2),
    }


def build_kr_theme_report() -> Dict:
    symbols = sorted(set(sum([t.stocks for t in KR_THEMES], [])))
    close_map = _download_close_map(symbols)

    themes = []
    picks = []

    for t in KR_THEMES:
        members = []
        for s in t.stocks:
            c = close_map.get(s)
            if c is None:
                continue
            st = _calc_score(c)
            st["symbol"] = s
            st["name"] = KR_NAMES.get(s, s)
            members.append(st)

        members.sort(key=lambda x: x["score"], reverse=True)
        if members:
            theme_score = round(float(np.mean([m["score"] for m in members])), 2)
            themes.append({"theme": t.name, "themeScore": theme_score, "leaders": members[:4]})

            for m in members[:2]:
                picks.append({
                    "theme": t.name,
                    "symbol": m["symbol"],
                    "name": m["name"],
                    "score": m["score"],
                    "m1": m["m1"],
                    "m3": m["m3"],
                })

    themes.sort(key=lambda x: x["themeScore"], reverse=True)
    picks.sort(key=lambda x: x["score"], reverse=True)

    return {
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "market": "KR",
        "method": "KR 종목 1/3/6개월 모멘텀 + 추세 + 변동성 패널티",
        "themes": themes,
        "topPicks": picks[:10],
        "note": "투자 권유가 아니라 참고용 데이터입니다.",
    }


def save_kr_theme_report(path: Path = OUT_PATH) -> Dict:
    report = build_kr_theme_report()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    r = save_kr_theme_report()
    print(json.dumps({"generatedAt": r["generatedAt"], "topPicks": r["topPicks"][:5]}, ensure_ascii=False, indent=2))
