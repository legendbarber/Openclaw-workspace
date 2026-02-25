from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, UTC, timedelta
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


DEFAULT_UNIVERSE = [
    # fallback set
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


def _load_universe_from_files() -> List[Asset]:
    base = Path(__file__).resolve().parent
    us_path = base / "universe_us_top300.json"
    kr_path = base / "universe_kr_top300.json"

    items: List[Asset] = []
    try:
        for p in [us_path, kr_path]:
            if not p.exists():
                continue
            arr = json.loads(p.read_text(encoding="utf-8"))
            for r in arr:
                sym = str(r.get("symbol", "")).strip().upper()
                name = str(r.get("name", "")).strip()
                cat = str(r.get("category", "")).strip().lower()
                if not sym or not name or cat not in {"us-stock", "kr-stock"}:
                    continue
                items.append(Asset(sym, name, cat))
    except Exception:
        return DEFAULT_UNIVERSE

    # dedupe by symbol
    uniq = {}
    for a in items:
        uniq[a.symbol] = a
    out = list(uniq.values())
    return out if out else DEFAULT_UNIVERSE


UNIVERSE = _load_universe_from_files()


def reload_universe() -> Dict:
    global UNIVERSE
    UNIVERSE = _load_universe_from_files()
    us_n = sum(1 for a in UNIVERSE if a.category == "us-stock")
    kr_n = sum(1 for a in UNIVERSE if a.category == "kr-stock")
    return {"total": len(UNIVERSE), "us": us_n, "kr": kr_n}


def _fetch_text(url: str, encoding: str = "utf-8") -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return urllib.request.urlopen(req, timeout=20).read().decode(encoding, "ignore")


def _refresh_us_top300(base_dir: Path) -> int:
    rows = []
    for page in range(1, 5):
        html = _fetch_text(f"https://companiesmarketcap.com/usa/largest-companies-in-the-usa-by-market-cap/?page={page}", "utf-8")
        for m in re.finditer(r'<tr>.*?<div class="company-name">(.*?)</div>.*?<div class="company-code">.*?([A-Z\.\-]{1,12})</div>.*?<td class="td-right" data-sort="([0-9]+)"><span class="currency-symbol-left">\$</span>.*?</td>', html, re.S):
            name = re.sub(r"\s+", " ", m.group(1)).strip()
            symbol = m.group(2).strip().upper()
            mcap = int(m.group(3))
            rows.append((symbol, name, mcap))

    best = {}
    for s, n, c in rows:
        if s not in best or c > best[s][1]:
            best[s] = (n, c)

    arr = sorted([(s, v[0], v[1]) for s, v in best.items()], key=lambda x: x[2], reverse=True)[:300]
    out = [{"symbol": s, "name": n, "category": "us-stock", "marketCap": c} for s, n, c in arr]
    (base_dir / "universe_us_top300.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(out)


def _strip_tags(x: str) -> str:
    x = re.sub(r"<[^>]+>", "", x)
    return x.replace("&nbsp;", " ").replace(",", "").strip()


def _refresh_kr_top300(base_dir: Path) -> int:
    rows = []
    for page in range(1, 9):
        html = _fetch_text(f"https://finance.naver.com/sise/sise_market_sum.naver?sosok=0&page={page}", "euc-kr")
        for tr in re.findall(r"<tr[^>]*>.*?</tr>", html, re.S):
            if "item/main.naver?code=" not in tr:
                continue
            code_m = re.search(r"item/main\.naver\?code=(\d{6})", tr)
            name_m = re.search(r'class="tltle">(.*?)</a>', tr, re.S)
            if not code_m or not name_m:
                continue
            code = code_m.group(1)
            name = _strip_tags(name_m.group(1))
            tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
            cols = [_strip_tags(td) for td in tds]
            if len(cols) < 7:
                continue
            try:
                mcap_eok = float(cols[6])
            except Exception:
                continue
            rows.append((code + ".KS", name, int(mcap_eok * 100000000)))

    if len(rows) < 300:
        for page in range(1, 13):
            html = _fetch_text(f"https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page={page}", "euc-kr")
            for tr in re.findall(r"<tr[^>]*>.*?</tr>", html, re.S):
                if "item/main.naver?code=" not in tr:
                    continue
                code_m = re.search(r"item/main\.naver\?code=(\d{6})", tr)
                name_m = re.search(r'class="tltle">(.*?)</a>', tr, re.S)
                if not code_m or not name_m:
                    continue
                code = code_m.group(1)
                name = _strip_tags(name_m.group(1))
                tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
                cols = [_strip_tags(td) for td in tds]
                if len(cols) < 7:
                    continue
                try:
                    mcap_eok = float(cols[6])
                except Exception:
                    continue
                rows.append((code + ".KQ", name, int(mcap_eok * 100000000)))

    best = {}
    for s, n, c in rows:
        if s not in best or c > best[s][1]:
            best[s] = (n, c)

    arr = sorted([(s, v[0], v[1]) for s, v in best.items()], key=lambda x: x[2], reverse=True)[:300]
    out = [{"symbol": s, "name": n, "category": "kr-stock", "marketCap": c} for s, n, c in arr]
    (base_dir / "universe_kr_top300.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(out)


def refresh_universe_top300() -> Dict:
    base = Path(__file__).resolve().parent
    us_n = _refresh_us_top300(base)
    kr_n = _refresh_kr_top300(base)
    loaded = reload_universe()
    return {"ok": True, "us": us_n, "kr": kr_n, "loaded": loaded, "updatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z")}


def get_universe_stats() -> Dict:
    base = Path(__file__).resolve().parent
    us_path = base / "universe_us_top300.json"
    kr_path = base / "universe_kr_top300.json"
    return {
        "loaded": reload_universe(),
        "files": {
            "us": {"path": str(us_path), "exists": us_path.exists(), "updatedAt": datetime.fromtimestamp(us_path.stat().st_mtime, UTC).isoformat().replace("+00:00", "Z") if us_path.exists() else None},
            "kr": {"path": str(kr_path), "exists": kr_path.exists(), "updatedAt": datetime.fromtimestamp(kr_path.stat().st_mtime, UTC).isoformat().replace("+00:00", "Z") if kr_path.exists() else None},
        },
    }

STATE_PATH = Path(__file__).resolve().parent / "state_log.json"
SNAPSHOT_DIR = Path(__file__).resolve().parent / "snapshots"
KST = ZoneInfo("Asia/Seoul")
POSITIVE = ["beat", "strong", "upgrade", "rally", "surge", "record", "gain"]
NEGATIVE = ["miss", "downgrade", "drop", "fall", "weak", "risk", "lawsuit"]
_THEME_META_CACHE: Dict[str, Dict] = {}


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


_CONS_CACHE: Dict[str, Dict] = {}
_CONS_TTL_SEC = 60 * 60 * 6  # 6h


def _safe_fetch_text(url: str, encoding: str = "utf-8") -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, timeout=2.5).read()
    return raw.decode(encoding, "ignore")


def _recommendation_to_score(rec: str | None) -> float | None:
    if not rec:
        return None
    r = rec.strip().lower()
    if "strong buy" in r:
        return 1.0
    if "buy" in r:
        return 2.0
    if "hold" in r or "neutral" in r:
        return 3.0
    if "sell" in r:
        return 4.0
    return None


def _recommendation_bucket(rec: str | None) -> str | None:
    if not rec:
        return None
    r = rec.strip().lower()
    if "strong buy" in r or ("buy" in r and "sell" not in r):
        return "buy"
    if "hold" in r or "neutral" in r:
        return "hold"
    if "sell" in r:
        return "sell"
    return None


def _consensus_from_naver_or_hk(symbol: str) -> Dict:
    """KR 종목은 네이버 증권 리서치 보고서에서 최근 목표주가 평균을 계산한다.
    (요청사항: yfinance 컨센서스 미사용)
    """
    code_match = re.match(r"^(\d{6})\.(KS|KQ)$", symbol or "")
    if not code_match:
        return {
            "targetMeanPrice": None,
            "upsidePct": None,
            "recommendationMean": None,
            "recommendationKey": None,
            "analystOpinions": 0,
            "opinionDistribution": {"buy": 0, "hold": 0, "sell": 0},
            "source": "naver_research",
            "confidence": 0.0,
            "score": 50.0,
        }

    code = code_match.group(1)
    list_url = f"https://finance.naver.com/research/company_list.naver?searchType=itemCode&itemCode={code}&page=1"

    try:
        html = _safe_fetch_text(list_url, encoding="euc-kr")
        nids = re.findall(r"company_read\.naver\?nid=(\d+)&page=1&searchType=itemCode&itemCode=" + re.escape(code), html)
        nids = list(dict.fromkeys(nids))[:12]

        targets = []
        target_ages = []
        recs = []
        rec_ages = []
        used_brokers = set()
        cutoff = datetime.now(KST).date() - timedelta(days=31)

        for nid in nids:
            try:
                read_url = f"https://finance.naver.com/research/company_read.naver?nid={nid}&page=1&searchType=itemCode&itemCode={code}"
                body = _safe_fetch_text(read_url, encoding="euc-kr")

                # 리포트 날짜 필터: 최근 1개월 이내만 반영
                m_date = re.search(r'<p class="source">[\s\S]*?<b class="bar">\|</b>\s*(\d{4}\.\d{2}\.\d{2})\s*<b class="bar">\|</b>', body)
                if not m_date:
                    continue
                try:
                    d = datetime.strptime(m_date.group(1), "%Y.%m.%d").date()
                    if d < cutoff:
                        continue
                    age_days = max(0, (datetime.now(KST).date() - d).days)
                except Exception:
                    continue

                # 동일 증권사 중복 방지 (가장 최신 리포트 1개만 사용)
                m_broker = re.search(r'<p class="source">\s*([^<|]+?)\s*<b class="bar">\|</b>', body)
                broker = m_broker.group(1).strip() if m_broker else None
                if broker and broker in used_brokers:
                    continue

                m_price = re.search(r'class="money"><strong>([\d,]+)</strong>', body)
                if not m_price:
                    continue
                price_val = float(m_price.group(1).replace(",", ""))
                targets.append(price_val)
                target_ages.append(age_days)

                if broker:
                    used_brokers.add(broker)

                m_rec = re.search(r'class="coment">([^<]+)</em>', body)
                if m_rec:
                    recs.append(m_rec.group(1).strip())
                    rec_ages.append(age_days)

                if len(targets) >= 6:
                    break
            except Exception:
                continue

        cur = None
        try:
            fi = yf.Ticker(symbol).fast_info or {}
            cur = fi.get("lastPrice") or fi.get("regularMarketPrice")
            if cur is not None:
                cur = float(cur)
        except Exception:
            cur = None

        target = None
        target_trend_pct = 0.0
        recency_bonus = 0.0
        if targets:
            # 최근성 가중(최신 리포트일수록 가중치↑)
            w = np.array([np.exp(-float(a) / 21.0) for a in target_ages], dtype=float)
            t = np.array(targets, dtype=float)
            if len(t) >= 5:
                # 이상치 완화: 상하위 1개 제거 후 가중평균
                idx = np.argsort(t)
                keep = idx[1:-1]
                t = t[keep]
                w = w[keep]
            target = float(np.average(t, weights=w)) if w.sum() > 0 else float(np.mean(t))

            # 목표가 추세(최근 대비 과거)
            if len(t) >= 2:
                latest_i = int(np.argmin(np.array(target_ages)[:len(targets)]))
                oldest_i = int(np.argmax(np.array(target_ages)[:len(targets)]))
                latest_t = float(targets[latest_i])
                oldest_t = float(targets[oldest_i])
                if oldest_t > 0:
                    target_trend_pct = (latest_t / oldest_t - 1) * 100

            # 리포트 최신도 보너스
            avg_age = float(np.average(np.array(target_ages, dtype=float), weights=w)) if w.sum() > 0 else float(np.mean(target_ages))
            recency_bonus = float(np.clip((31 - avg_age) / 31 * 8, 0, 8))

        up = ((target / cur - 1) * 100) if (target and cur) else None

        rec_scores = [x for x in (_recommendation_to_score(r) for r in recs) if isinstance(x, (int, float))]
        mean = float(np.mean(rec_scores)) if rec_scores else None

        # 최근성 가중 투자의견 평균
        w_rec = np.array([np.exp(-float(a) / 21.0) for a in rec_ages], dtype=float) if rec_ages else np.array([], dtype=float)
        if len(rec_scores) == len(rec_ages) and len(rec_scores) > 0 and w_rec.sum() > 0:
            mean_w = float(np.average(np.array(rec_scores, dtype=float), weights=w_rec))
        else:
            mean_w = mean

        b = {"buy": 0, "hold": 0, "sell": 0}
        b_w = {"buy": 0.0, "hold": 0.0, "sell": 0.0}
        for i, r in enumerate(recs):
            k = _recommendation_bucket(r)
            if k:
                b[k] += 1
                wi = float(np.exp(-float(rec_ages[i]) / 21.0)) if i < len(rec_ages) else 1.0
                b_w[k] += wi
        total_op = max(1, sum(b.values()))
        buy_ratio = b["buy"] / total_op
        sell_ratio = b["sell"] / total_op
        total_w = max(1e-9, sum(b_w.values()))
        buy_ratio_w = b_w["buy"] / total_w
        sell_ratio_w = b_w["sell"] / total_w

        score = 50.0
        # 업사이드는 점수에서 제외: 방향/분포/표본/추세/최신도 반영
        if isinstance(mean_w, (int, float)):
            score += float(np.clip((3.2 - mean_w) * 10, -15, 20))
        # 분포(가중 + 비가중 평균 혼합)
        dist_raw = 0.7 * (buy_ratio_w - sell_ratio_w) + 0.3 * (buy_ratio - sell_ratio)
        score += float(np.clip(dist_raw * 25, -15, 25))
        # 표본 수
        score += float(np.clip(len(targets) * 3.0, 0, 20))
        # 목표가 추세(상향 조정 가점)
        score += float(np.clip(target_trend_pct / 2.0, -8, 8))
        # 최신도 보너스
        score += recency_bonus

        return {
            "targetMeanPrice": None if target is None else round(target, 2),
            "upsidePct": None if up is None else round(float(up), 2),
            "recommendationMean": None if mean_w is None else round(float(mean_w), 2),
            "recommendationKey": None,
            "analystOpinions": len(targets),
            "opinionDistribution": b,
            "targetTrendPct": round(float(target_trend_pct), 2),
            "freshnessBonus": round(float(recency_bonus), 2),
            "source": "naver_research",
            "confidence": round(float(np.clip((len(targets) / 6) * 100, 0, 100)), 2),
            "score": round(float(np.clip(score, 0, 100)), 2),
        }
    except Exception:
        return {
            "targetMeanPrice": None,
            "upsidePct": None,
            "recommendationMean": None,
            "recommendationKey": None,
            "analystOpinions": 0,
            "opinionDistribution": {"buy": 0, "hold": 0, "sell": 0},
            "source": "naver_research",
            "confidence": 0.0,
            "score": 50.0,
        }


def _consensus_from_yfinance(symbol: str) -> Dict:
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

        b = {"buy": 0, "hold": 0, "sell": 0}
        k = _recommendation_bucket(key)
        if k:
            b[k] = 1

        score = 50.0
        # 업사이드는 점수에서 제외: 투자의견 방향 + 표본 수만 반영
        if isinstance(mean, (int, float)):
            score += float(np.clip((3.2 - mean) * 10, -15, 20))

        buy_ratio = 1.0 if b["buy"] else 0.0
        sell_ratio = 1.0 if b["sell"] else 0.0
        score += float(np.clip((buy_ratio - sell_ratio) * 10, -10, 10))

        if isinstance(n, (int, float)):
            # 표본 수 가중 강화
            score += float(np.clip(n * 1.2, 0, 20))

        return {
            "targetMeanPrice": target,
            "upsidePct": None if up is None else round(float(up), 2),
            "recommendationMean": mean,
            "recommendationKey": key,
            "analystOpinions": n,
            "opinionDistribution": b,
            "source": "yfinance",
            "confidence": round(float(np.clip(((float(n) if isinstance(n, (int, float)) else 0.0) / 20.0) * 100, 0, 100)), 2),
            "score": round(float(np.clip(score, 0, 100)), 2),
        }
    except Exception:
        return {
            "targetMeanPrice": None,
            "upsidePct": None,
            "recommendationMean": None,
            "recommendationKey": None,
            "analystOpinions": None,
            "opinionDistribution": {"buy": 0, "hold": 0, "sell": 0},
            "source": "yfinance",
            "confidence": 0.0,
            "score": 50.0,
        }


def _consensus(symbol: str) -> Dict:
    now = time.time()
    cached = _CONS_CACHE.get(symbol)
    if cached and (now - cached.get("ts", 0) < _CONS_TTL_SEC):
        return cached["data"]

    is_kr = bool(re.match(r"^(\d{6})\.(KS|KQ)$", symbol or ""))
    data = _consensus_from_naver_or_hk(symbol) if is_kr else _consensus_from_yfinance(symbol)
    _CONS_CACHE[symbol] = {"ts": now, "data": data}
    return data


def _to_theme_label(symbol: str, name: str, sector: str | None, industry: str | None) -> str:
    s = (sector or "").lower()
    i = (industry or "").lower()
    n = (name or "")
    sym = (symbol or "").upper()

    # KR keyword mapping (직관형)
    kr_rules = [
        (lambda: any(k in n for k in ["반도체", "하이닉스", "삼성전자", "한미반도체", "리노공업", "이오테크닉스"]), "반도체"),
        (lambda: any(k in n for k in ["2차전지", "배터리", "에코프로", "포스코퓨처엠", "엘앤에프", "삼성sdi", "lg에너지솔루션"]), "2차전지"),
        (lambda: any(k in n for k in ["자동차", "현대차", "기아", "모비스", "만도", "한온시스템"]), "자동차/부품"),
        (lambda: any(k in n for k in ["방산", "한화에어로", "lignex1", "lig넥스원", "현대로템", "한국항공우주"]), "방산/우주"),
        (lambda: any(k in n for k in ["조선", "현대중공업", "한화오션", "삼성중공업"]), "조선/해양"),
        (lambda: any(k in n for k in ["전력", "변압기", "효성중공업", "ls electric", "일렉트릭"]), "전력기기/인프라"),
        (lambda: any(k in n for k in ["바이오", "제약", "셀트리온", "삼성바이오", "유한양행", "한미약품"]), "바이오/헬스케어"),
        (lambda: any(k in n for k in ["인터넷", "플랫폼", "네이버", "카카오"]), "인터넷/플랫폼"),
        (lambda: any(k in n for k in ["은행", "금융", "신한", "kb", "하나금융", "우리금융"]), "은행/금융"),
    ]
    for cond, label in kr_rules:
        try:
            if cond():
                return label
        except Exception:
            pass

    # Global mapping
    if "semiconductor" in s or "semiconductor" in i:
        return "반도체"
    if "software" in s or "internet" in s or "interactive media" in i:
        return "인터넷/소프트웨어"
    if "banks" in i or "financial" in s:
        return "은행/금융"
    if "oil" in i or "gas" in i or "energy" in s:
        return "에너지"
    if "aerospace" in i or "defense" in i:
        return "방산/우주"
    if "auto" in i or "autom" in i:
        return "자동차/부품"
    if "biotech" in i or "pharma" in i or "health" in s:
        return "바이오/헬스케어"
    if "utility" in s or "electrical" in i or "power" in i:
        return "전력/유틸리티"

    if sector and industry:
        return f"{sector} > {industry}"
    return sector or industry or "UNKNOWN"


def _get_symbol_theme_meta(symbol: str) -> Dict:
    sym = (symbol or "").upper()
    if sym in _THEME_META_CACHE:
        return _THEME_META_CACHE[sym]

    out = {"theme": "UNKNOWN", "sector": None, "industry": None}
    try:
        info = yf.Ticker(sym).info or {}
        sector = (info.get("sector") or "").strip()
        industry = (info.get("industry") or "").strip()

        theme = _to_theme_label(sym, "", sector, industry)
        out = {"theme": theme, "sector": sector or None, "industry": industry or None}
    except Exception:
        pass

    _THEME_META_CACHE[sym] = out
    return out


def _apply_runtime_theme_scores(rows: List[Dict]) -> List[Dict]:
    """각 종목 분석 후 종목별 theme를 추정하고 테마점수를 계산해 종목점수에 반영한다."""
    if not rows:
        return rows

    # 1) 각 종목 theme 메타 부착
    for r in rows:
        sym = r.get("symbol")
        nm = r.get("name", "")
        meta = _get_symbol_theme_meta(sym)
        theme_label = _to_theme_label(sym, nm, meta.get("sector"), meta.get("industry"))
        r.setdefault("components", {})["theme"] = {
            "theme": theme_label,
            "sector": meta.get("sector"),
            "industry": meta.get("industry"),
            "themeScore": 50.0,
            "leaderScore": None,
            "score": 50.0,
            "matched": False,
            "source": "runtime-grouping",
        }

    # 2) theme별 그룹 스코어 계산
    groups: Dict[str, List[Dict]] = {}
    for r in rows:
        t = r.get("components", {}).get("theme", {}).get("theme") or "UNKNOWN"
        groups.setdefault(t, []).append(r)

    theme_scores: Dict[str, float] = {}
    for t, arr in groups.items():
        base_avg = float(np.mean([float(x.get("scoreBase", x.get("score", 50.0))) for x in arr]))
        tech_avg = float(np.mean([float((x.get("components", {}).get("technical", {}) or {}).get("score", 50.0)) for x in arr]))
        news_avg = float(np.mean([float((x.get("components", {}).get("crowd", {}) or {}).get("score", 50.0)) for x in arr]))
        breadth = np.clip((len(arr) / 10.0) * 100, 0, 100)
        theme_score = float(np.clip(0.5 * base_avg + 0.2 * tech_avg + 0.2 * news_avg + 0.1 * breadth, 0, 100))
        theme_scores[t] = theme_score

    # 3) 종목별 리더점수(해당 테마 내 상대강도) + 최종 점수 재보정
    for t, arr in groups.items():
        arr_sorted = sorted(arr, key=lambda x: float(x.get("scoreBase", x.get("score", 50.0))), reverse=True)
        n = len(arr_sorted)
        for i, r in enumerate(arr_sorted):
            leader = 100.0 if n <= 1 else float(np.clip(100 - (i / (n - 1)) * 35, 65, 100))
            th = r.get("components", {}).get("theme", {})
            th.update({
                "themeScore": round(theme_scores[t], 2),
                "leaderScore": round(leader, 2),
                "score": round(float(np.clip(0.7 * theme_scores[t] + 0.3 * leader, 0, 100)), 2),
                "matched": t != "UNKNOWN",
            })
            r["components"]["theme"] = th

            # 테마점수를 종합점수에 반영 (요청: 테마 우선)
            base = float(r.get("scoreBase", r.get("score", 50.0)))
            conf = float(r.get("confidence", 50.0))
            final_score = 0.75 * base + 0.25 * th["score"]
            final_score = 0.9 * final_score + 0.1 * conf

            # 밸류에이션 갭(목표가-현재가) 소폭 반영
            up = (r.get("components", {}).get("reportConsensus", {}) or {}).get("upsidePct")
            if isinstance(up, (int, float)):
                # 과도한 왜곡 방지를 위해 캡 적용: -20%~+40%를 -4~+8점으로 반영
                valuation_adj = float(np.clip(up, -20, 40)) * 0.2
                final_score += valuation_adj
                r.setdefault("components", {})["valuation"] = {
                    "upsidePct": round(float(up), 2),
                    "adjustment": round(float(valuation_adj), 2),
                    "capRangePct": [-20, 40],
                }
            else:
                r.setdefault("components", {})["valuation"] = {
                    "upsidePct": None,
                    "adjustment": 0.0,
                    "capRangePct": [-20, 40],
                }

            r["score"] = round(float(np.clip(final_score, 0, 100)), 2)

    return rows


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


def _technical_score(s: pd.Series, target_price: float | None = None) -> Dict:
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

    headroom_pct = None

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
        "headroomPct": None if headroom_pct is None else round(float(headroom_pct), 2),
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
    technical = _technical_score(s, target_price=report_consensus.get("targetMeanPrice"))

    # 1차 기본점수(테마 제외): R/C/T
    base_score = (
        0.60 * report_consensus["score"] +
        0.25 * crowd["score"] +
        0.15 * technical["score"]
    )

    # 데이터 품질 기반 신뢰도
    r_conf = float(report_consensus.get("confidence", 50.0) or 0.0)
    c_conf = float(np.clip(((crowd.get("headlineCount", 0) or 0) / 8.0) * 100, 0, 100))
    t_conf = 85.0 if technical.get("setup") in {"pullback-in-uptrend", "healthy-trend"} else 70.0
    confidence = 0.60 * r_conf + 0.25 * c_conf + 0.15 * t_conf

    score = 0.9 * base_score + 0.1 * confidence

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
        "scoreBase": round(float(base_score), 2),
        "confidence": round(float(confidence), 2),
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


def build_report(market: str = "all", candidate_limit: int | None = None, progress_cb=None) -> Dict:
    rows = []
    failed = []

    mk = (market or "all").strip().lower()
    assets = UNIVERSE
    if mk == "us":
        assets = [a for a in UNIVERSE if a.category.startswith("us-")]
    elif mk == "kr":
        assets = [a for a in UNIVERSE if a.category.startswith("kr-")]

    if isinstance(candidate_limit, int) and candidate_limit > 0:
        assets = assets[:candidate_limit]

    total_assets = len(assets)
    for i, a in enumerate(assets, start=1):
        r = evaluate_asset(a)
        if r is None:
            failed.append(a.symbol)
        else:
            rows.append(r)

        if callable(progress_cb):
            try:
                progress_cb(done=i, total=total_assets, symbol=a.symbol)
            except Exception:
                pass

    # 사용자 요청: ETF 제외 (단일 주식만 허용)
    rows = [r for r in rows if not _is_etf_like(r)]

    # 종목분석 완료 후, 런타임 테마 추정/테마점수 산출/최종점수 반영
    rows = _apply_runtime_theme_scores(rows)

    rows.sort(key=lambda x: x["score"], reverse=True)

    # 보조 랭킹(참고용)
    risk_adjusted = sorted(
        rows,
        key=lambda x: (
            x.get("riskReward", 0),
            x.get("expectedReturnPct", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    high_return = sorted(
        rows,
        key=lambda x: (
            x.get("expectedReturnPct", 0),
            x.get("riskReward", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    # 메인 추천/순위는 종합점수(score) 기준
    top = rows[0] if rows else None

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
        "candidateLimit": total_assets,
        "methodology": "S=RuntimeThemeAdjusted: base(R/C/T)+runtime-theme(TH)+small valuation-gap adjustment(upside capped)",
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
