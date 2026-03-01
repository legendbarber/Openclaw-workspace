from __future__ import annotations

import json
import re
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Any, Dict, List
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

ARCHIVE_PATH = Path(__file__).resolve().parent / "archive_top_picks.json"
_ARCHIVE_LOCK = threading.Lock()


def _load_archive() -> Dict[str, Dict]:
    if not ARCHIVE_PATH.exists():
        return {}
    try:
        data = json.loads(ARCHIVE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _save_archive(data: Dict[str, Dict]) -> None:
    try:
        with _ARCHIVE_LOCK:
            ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
            ARCHIVE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def save_archive_entry(entry: Dict) -> bool:
    symbol = str(entry.get("symbol") or "").upper().strip()
    if not symbol:
        return False
    archived = entry.copy()
    archived["symbol"] = symbol
    archive = _load_archive()
    existing = archive.get(symbol, {})
    merged = {**existing, **archived}
    archive[symbol] = merged
    _save_archive(archive)
    return True


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


def clear_runtime_caches() -> None:
    _CONS_CACHE.clear()
    _HK_REPORT_CACHE.clear()
    _THEME_META_CACHE.clear()
    _NAVER_THEME_CACHE["ts"] = 0.0
    _NAVER_THEME_CACHE["map"] = {}


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
_HK_REPORT_CACHE: Dict[str, Dict] = {}
_NAVER_THEME_CACHE: Dict[str, object] = {"ts": 0.0, "map": {}}


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


def _split_js_args(s: str) -> List[str]:
    out: List[str] = []
    cur: List[str] = []
    q = None
    esc = False
    depth = 0
    for ch in s:
        if q:
            cur.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == q:
                q = None
            continue
        if ch in {'"', "'"}:
            q = ch
            cur.append(ch)
            continue
        if ch in "([{":
            depth += 1
            cur.append(ch)
            continue
        if ch in ")]}":
            depth = max(0, depth - 1)
            cur.append(ch)
            continue
        if ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
            continue
        cur.append(ch)
    if cur:
        out.append("".join(cur).strip())
    return out


def _js_atom(v: str):
    x = (v or "").strip()
    if x == "null":
        return None
    if x == "true":
        return True
    if x == "false":
        return False
    if len(x) >= 2 and ((x[0] == '"' and x[-1] == '"') or (x[0] == "'" and x[-1] == "'")):
        s = x[1:-1]
        return s.replace("\\/", "/").replace('\\"', '"').replace("\\'", "'")
    try:
        if "." in x:
            return float(x)
        return int(x)
    except Exception:
        return x


def _hankyung_view_fields(report_idx: str) -> Dict:
    rid = str(report_idx or "").strip()
    if not rid:
        return {}
    c = _HK_REPORT_CACHE.get(rid)
    if c is not None:
        return c

    out: Dict = {}
    try:
        html = _safe_fetch_text(f"https://markets.hankyung.com/consensus/view/{rid}", encoding="utf-8")
        m_decl = re.search(r"window\.__NUXT__=\(function\((.*?)\)\{return", html, re.S)
        m_call = re.search(r"\}\((.*)\)\);</script>", html, re.S)
        if not m_decl or not m_call:
            _HK_REPORT_CACHE[rid] = out
            return out

        names = [x.strip() for x in m_decl.group(1).split(",")]
        vals = _split_js_args(m_call.group(1))

        for fld in ["TARGET_STOCK_PRICES", "GRADE_VALUE", "OLD_TARGET_STOCK_PRICES"]:
            m_f = re.search(rf"{fld}:([a-zA-Z_][a-zA-Z0-9_]*|\"[^\"]*\"|'[^']*'|[0-9\.]+)", html)
            if not m_f:
                continue
            tok = m_f.group(1)
            if tok and tok[0] in {'"', "'"}:
                out[fld] = _js_atom(tok)
                continue
            if re.match(r"^[0-9.]+$", tok):
                out[fld] = _js_atom(tok)
                continue
            if tok in names:
                i = names.index(tok)
                if 0 <= i < len(vals):
                    out[fld] = _js_atom(vals[i])
    except Exception:
        out = {}

    _HK_REPORT_CACHE[rid] = out
    return out


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


def _consensus_from_naver_or_hk(symbol: str, name: str | None = None) -> Dict:
    """KR 종목은 한경 컨센서스 리포트 목록을 우선 사용해 컨센서스 점수를 계산한다.
    - 최근 1개월 기업 리포트만 사용
    - 동일 증권사 중복 제거
    - 목표가(텍스트 추출 가능 시) + 투자의견 분포 + 표본수 + 최신도 반영
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
            "reportLinks": [],
            "source": "hankyung_consensus",
            "confidence": 0.0,
            "score": 50.0,
        }

    stock_name = (name or "").strip()
    if not stock_name:
        try:
            info = yf.Ticker(symbol).info or {}
            stock_name = str(info.get("shortName") or info.get("longName") or "").strip()
        except Exception:
            stock_name = ""
    stock_name = re.sub(r"\(.*?\)", "", stock_name).strip()

    if not stock_name:
        return {
            "targetMeanPrice": None,
            "upsidePct": None,
            "recommendationMean": None,
            "recommendationKey": None,
            "analystOpinions": 0,
            "opinionDistribution": {"buy": 0, "hold": 0, "sell": 0},
            "reportLinks": [],
            "source": "hankyung_consensus",
            "confidence": 0.0,
            "score": 50.0,
        }

    try:
        today = datetime.now(KST).date()
        sdate = (today - timedelta(days=31)).strftime("%Y-%m-%d")
        edate = today.strftime("%Y-%m-%d")
        url = (
            "https://consensus.hankyung.com/analysis/list?"
            + urllib.parse.urlencode({
                "sdate": sdate,
                "edate": edate,
                "search_value": "REPORT_TITLE",
                "search_text": code_match.group(1),
                "report_type": "CO",
                "pagenum": "80",
                "now_page": "1",
            })
        )
        html = _safe_fetch_text(url, encoding="euc-kr")

        def _txt(x: str) -> str:
            x = re.sub(r"<[^>]+>", " ", x)
            return re.sub(r"\s+", " ", x).strip()

        targets: List[float] = []
        target_ages: List[int] = []
        recs: List[str] = []
        rec_ages: List[int] = []
        used_brokers = set()
        hk_reports: List[Dict] = []

        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S)
        for tr in rows:
            if "class=\"text_l\"" not in tr or "/analysis/downpdf?report_idx=" not in tr:
                continue

            m_date = re.search(r'class="first txt_number">\s*(\d{4}-\d{2}-\d{2})\s*</td>', tr)
            if not m_date:
                continue
            try:
                d = datetime.strptime(m_date.group(1), "%Y-%m-%d").date()
            except Exception:
                continue
            if d < (today - timedelta(days=31)):
                continue
            age_days = max(0, (today - d).days)

            m_title = re.search(r'class="text_l">[\s\S]*?<a [^>]*>(.*?)</a>', tr)
            title = _txt(m_title.group(1)) if m_title else ""
            if stock_name not in title and code_match.group(1) not in title:
                continue

            m_idx = re.search(r'/analysis/downpdf\?report_idx=(\d+)', tr)

            tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
            cols = [_txt(td) for td in tds]
            broker = cols[5] if len(cols) >= 6 else (cols[4] if len(cols) >= 5 else None)
            if broker and broker in used_brokers:
                continue

            detail_text = _txt(tr)

            # 목록 표의 목표주가 컬럼 우선 사용 (예: 1,550,000)
            row_target = None
            if len(cols) >= 3:
                m_col_tp = re.search(r"([0-9][0-9,]{3,})", cols[2] or "")
                if m_col_tp:
                    try:
                        row_target = float(m_col_tp.group(1).replace(",", ""))
                    except Exception:
                        row_target = None
            if isinstance(row_target, (int, float)) and row_target > 0:
                targets.append(float(row_target))
                target_ages.append(age_days)

            # 컬럼이 비어 있으면 텍스트에서 목표가 추출
            m_tp = None if row_target else (
                re.search(r"목표\s*주가\s*[:：]?\s*([0-9][0-9,]{3,})\s*원", detail_text, re.I)
                or re.search(r"적정\s*주가\s*[:：]?\s*([0-9][0-9,]{3,})\s*원", detail_text, re.I)
                or re.search(r"\bTP\s*[:=]?\s*([0-9][0-9,]{3,})\b", detail_text, re.I)
            )
            if m_tp:
                try:
                    targets.append(float(m_tp.group(1).replace(",", "")))
                    target_ages.append(age_days)
                except Exception:
                    pass

            rec_text = ""
            for kw in ["매수", "중립", "보유", "매도", "BUY", "HOLD", "SELL", "Outperform", "Underperform", "Neutral"]:
                if re.search(re.escape(kw), detail_text, re.I):
                    rec_text = kw
                    break

            # 목록에 값이 없으면 markets view 페이지에서 TARGET_STOCK_PRICES/GRADE_VALUE 보강
            if m_idx and (not m_tp or not rec_text):
                extra = _hankyung_view_fields(m_idx.group(1))
                x_tp = extra.get("TARGET_STOCK_PRICES")
                if (not m_tp) and isinstance(x_tp, (int, float, str)):
                    try:
                        tpv = float(str(x_tp).replace(",", ""))
                        if tpv > 0:
                            targets.append(tpv)
                            target_ages.append(age_days)
                    except Exception:
                        pass
                x_grade = str(extra.get("GRADE_VALUE") or "").strip()
                if (not rec_text) and x_grade:
                    rec_text = x_grade
            if rec_text:
                recs.append(rec_text)
                rec_ages.append(age_days)

            if m_idx:
                rid = m_idx.group(1)
                # 리포트별 목표가 보관(표 컬럼/본문/상세보강 순)
                rpt_target = None
                if isinstance(row_target, (int, float)):
                    rpt_target = float(row_target)
                elif m_tp:
                    try:
                        rpt_target = float(m_tp.group(1).replace(",", ""))
                    except Exception:
                        rpt_target = None
                else:
                    extra2 = _hankyung_view_fields(rid)
                    x_tp2 = extra2.get("TARGET_STOCK_PRICES")
                    try:
                        if isinstance(x_tp2, (int, float, str)):
                            rpt_target = float(str(x_tp2).replace(",", ""))
                    except Exception:
                        rpt_target = None

                hk_reports.append({
                    "date": d.strftime("%Y-%m-%d"),
                    "title": title,
                    "broker": broker,
                    "targetPrice": None if rpt_target is None else round(float(rpt_target), 2),
                    "url": f"https://consensus.hankyung.com/analysis/downpdf?report_idx={rid}",
                })

            if broker:
                used_brokers.add(broker)

            if len(used_brokers) >= 6:
                break

        cur = None
        try:
            fi = yf.Ticker(symbol).fast_info or {}
            cur = fi.get("lastPrice") or fi.get("regularMarketPrice")
            if cur is not None:
                cur = float(cur)
        except Exception:
            cur = None

        target = None
        if targets and target_ages:
            n_pair = min(len(targets), len(target_ages))
            t = np.array(targets[:n_pair], dtype=float)
            ages = np.array(target_ages[:n_pair], dtype=float)
            w = np.array([np.exp(-float(a) / 21.0) for a in ages], dtype=float)
            if len(t) >= 5:
                idx = np.argsort(t)
                keep = idx[1:-1]
                t = t[keep]
                w = w[keep]
            target = float(np.average(t, weights=w)) if (len(w) == len(t) and w.sum() > 0) else float(np.mean(t))

        # 한경 목록에서 목표가 추출이 안 되는 종목은 yfinance 목표가를 보조값으로 사용
        target_fallback = False
        if target is None:
            try:
                yfi = yf.Ticker(symbol).info or {}
                y_target = yfi.get("targetMeanPrice")
                if isinstance(y_target, (int, float)) and float(y_target) > 0:
                    target = float(y_target)
                    target_fallback = True
            except Exception:
                pass

        up = ((target / cur - 1) * 100) if (target and cur) else None

        rec_scores = [x for x in (_recommendation_to_score(r) for r in recs) if isinstance(x, (int, float))]
        mean = float(np.mean(rec_scores)) if rec_scores else None

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

        sample_n = max(len(used_brokers), len(targets), len(recs))
        score = 50.0
        if isinstance(mean_w, (int, float)):
            score += float(np.clip((3.2 - mean_w) * 10, -15, 20))
        dist_raw = 0.7 * (buy_ratio_w - sell_ratio_w) + 0.3 * (buy_ratio - sell_ratio)
        score += float(np.clip(dist_raw * 25, -15, 25))
        score += float(np.clip(sample_n * 3.0, 0, 20))

        return {
            "targetMeanPrice": None if target is None else round(target, 2),
            "upsidePct": None if up is None else round(float(up), 2),
            "recommendationMean": None if mean_w is None else round(float(mean_w), 2),
            "recommendationKey": None,
            "analystOpinions": sample_n,
            "opinionDistribution": b,
            "freshnessBonus": 0.0,
            "reportLinks": hk_reports,
            "source": "hankyung_consensus+yf_target_fallback" if target_fallback else "hankyung_consensus",
            "confidence": round(float(np.clip((sample_n / 6) * 100, 0, 100)), 2),
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
            "reportLinks": [],
            "source": "hankyung_consensus",
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
            "reportLinks": [],
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
            "reportLinks": [],
            "source": "yfinance",
            "confidence": 0.0,
            "score": 50.0,
        }


def _consensus(symbol: str, name: str | None = None) -> Dict:
    now = time.time()
    cached = _CONS_CACHE.get(symbol)
    if cached and (now - cached.get("ts", 0) < _CONS_TTL_SEC):
        return cached["data"]

    is_kr = bool(re.match(r"^(\d{6})\.(KS|KQ)$", symbol or ""))
    data = _consensus_from_naver_or_hk(symbol, name=name) if is_kr else _consensus_from_yfinance(symbol)
    _CONS_CACHE[symbol] = {"ts": now, "data": data}
    return data


def _load_naver_theme_map() -> Dict[str, Dict]:
    now = time.time()
    cached = _NAVER_THEME_CACHE.get("map", {}) or {}
    ts = float(_NAVER_THEME_CACHE.get("ts", 0.0) or 0.0)
    if cached and (now - ts) < 60 * 60 * 6:
        return cached

    out: Dict[str, Dict] = {}

    def _fetch(url: str) -> str:
        return _safe_fetch_text(url, encoding="euc-kr")

    try:
        # theme list pages
        theme_links = []
        for p in range(1, 8):
            html = _fetch(f"https://finance.naver.com/sise/theme.naver?&page={p}")
            links = re.findall(r'href="(/sise/theme_detail\.naver\?no=\d+)"', html)
            if not links:
                continue
            for lk in links:
                if lk not in theme_links:
                    theme_links.append(lk)

        # each theme detail -> stock code mapping
        for rel in theme_links[:400]:
            try:
                detail = _fetch("https://finance.naver.com" + rel)
                m_theme = re.search(r'<div class="h_company">\s*<h2>\s*([^<]+?)\s*</h2>', detail)
                theme_name = m_theme.group(1).strip() if m_theme else None
                if not theme_name:
                    continue

                for code, nm in re.findall(r'item/main\.naver\?code=(\d{6})[^>]*>([^<]+)</a>', detail):
                    code6 = code.strip()
                    if not code6:
                        continue
                    rec = out.get(code6)
                    cand = {"theme": theme_name, "name": nm.strip(), "source": "naver_theme"}
                    if rec is None:
                        out[code6] = cand
            except Exception:
                continue
    except Exception:
        pass

    _NAVER_THEME_CACHE["ts"] = now
    _NAVER_THEME_CACHE["map"] = out
    return out


def _get_symbol_theme_meta(symbol: str) -> Dict:
    sym = (symbol or "").upper()
    if sym in _THEME_META_CACHE:
        return _THEME_META_CACHE[sym]

    out = {"theme": "UNKNOWN", "sector": None, "industry": None, "source": "unknown"}

    # KR 종목은 네이버 증권 테마를 최우선 사용
    m = re.match(r"^(\d{6})\.(KS|KQ)$", sym)
    if m:
        code6 = m.group(1)
        theme_map = _load_naver_theme_map()
        rec = theme_map.get(code6)
        if rec:
            out = {
                "theme": rec.get("theme") or "UNKNOWN",
                "sector": None,
                "industry": None,
                "source": "naver_theme",
            }
            _THEME_META_CACHE[sym] = out
            return out

    # non-KR fallback (기존 유지)
    try:
        info = yf.Ticker(sym).info or {}
        sector = (info.get("sector") or "").strip()
        industry = (info.get("industry") or "").strip()
        theme = f"{sector} > {industry}" if (sector and industry) else (sector or industry or "UNKNOWN")
        out = {"theme": theme, "sector": sector or None, "industry": industry or None, "source": "yfinance"}
    except Exception:
        pass

    _THEME_META_CACHE[sym] = out
    return out


SCORE_PRESETS: Dict[str, Dict[str, float]] = {
    # 사용자 기본 요청: 종목점수:테마점수 = 6:4
    "default_6_4": {
        "stock": 0.60,
        "theme": 0.40,
        "news": 0.00,
        "technical": 0.00,
        "confidence": 0.10,
        "valuation": 0.20,
    },
    # 기존 방식에 가까운 밸런스
    "balanced": {
        "stock": 0.50,
        "theme": 0.30,
        "news": 0.10,
        "technical": 0.10,
        "confidence": 0.10,
        "valuation": 0.20,
    },
    # 테마 중심
    "theme_focus": {
        "stock": 0.40,
        "theme": 0.50,
        "news": 0.05,
        "technical": 0.05,
        "confidence": 0.10,
        "valuation": 0.20,
    },
}


def _normalize_score_config(score_config: Dict[str, Any] | None) -> Dict[str, Any]:
    raw = score_config or {}
    preset = str(raw.get("preset") or "default_6_4").strip().lower()
    if preset not in SCORE_PRESETS:
        preset = "default_6_4"

    base = SCORE_PRESETS[preset].copy()
    comp = raw.get("components") if isinstance(raw.get("components"), dict) else {}

    out_comp: Dict[str, float] = {}
    for k in ("stock", "theme", "news", "technical"):
        v = comp.get(k, base.get(k, 0.0))
        try:
            out_comp[k] = max(0.0, float(v))
        except Exception:
            out_comp[k] = float(base.get(k, 0.0))

    total = sum(out_comp.values())
    if total <= 0:
        out_comp = {k: float(base.get(k, 0.0)) for k in ("stock", "theme", "news", "technical")}
        total = sum(out_comp.values())
    if total <= 0:
        out_comp = {"stock": 1.0, "theme": 0.0, "news": 0.0, "technical": 0.0}
        total = 1.0

    for k in out_comp:
        out_comp[k] = out_comp[k] / total

    def _num(name: str) -> float:
        dv = float(base.get(name, 0.0))
        v = raw.get(name, dv)
        try:
            return float(v)
        except Exception:
            return dv

    confidence_weight = max(0.0, _num("confidence"))
    valuation_scale = max(0.0, _num("valuation"))

    return {
        "preset": preset,
        "components": out_comp,
        "confidence": confidence_weight,
        "valuation": valuation_scale,
    }


def _score_methodology_text(cfg: Dict[str, Any]) -> str:
    c = cfg.get("components", {})
    return (
        "S=(1-conf)*Core+conf*Confidence+valuationAdj; "
        f"Core={c.get('stock', 0):.2f}Stock+{c.get('theme', 0):.2f}Theme+{c.get('news', 0):.2f}News+{c.get('technical', 0):.2f}Technical; "
        f"conf={cfg.get('confidence', 0):.2f}; valuationScale={cfg.get('valuation', 0):.2f}; "
        "Technical=direction/cross/distance, KR theme=Naver theme, no clipping"
    )


def _apply_runtime_theme_scores(rows: List[Dict], score_config: Dict[str, Any] | None = None) -> List[Dict]:
    """각 종목 분석 후 종목별 theme를 추정하고 테마점수를 계산해 종목점수에 반영한다."""
    if not rows:
        return rows

    cfg = _normalize_score_config(score_config)

    # 1) 각 종목 theme 메타 부착
    for r in rows:
        sym = r.get("symbol")
        meta = _get_symbol_theme_meta(sym)
        theme_label = meta.get("theme") or "UNKNOWN"
        r.setdefault("components", {})["theme"] = {
            "theme": theme_label,
            "sector": meta.get("sector"),
            "industry": meta.get("industry"),
            "themeScore": 50.0,
            "leaderScore": None,
            "score": 50.0,
            "matched": False,
            "source": meta.get("source") or "runtime-grouping",
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
        theme_score = float(0.5 * base_avg + 0.2 * tech_avg + 0.2 * news_avg + 0.1 * breadth)
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
                "score": round(float(0.7 * theme_scores[t] + 0.3 * leader), 2),
                "matched": t != "UNKNOWN",
            })
            r["components"]["theme"] = th

            # 사용자 설정 비율 반영 (UI에서 가중치/포함항목 조정)
            base = float(r.get("scoreBase", r.get("score", 50.0)))
            news_s = float((r.get("components", {}).get("crowd", {}) or {}).get("score", 50.0))
            tech_s = float((r.get("components", {}).get("technical", {}) or {}).get("score", 50.0))
            conf = float(r.get("confidence", 50.0))
            w = cfg.get("components", {})
            core = (
                float(w.get("stock", 0.0)) * base
                + float(w.get("theme", 0.0)) * float(th.get("score", 50.0))
                + float(w.get("news", 0.0)) * news_s
                + float(w.get("technical", 0.0)) * tech_s
            )
            conf_w = float(np.clip(cfg.get("confidence", 0.10), 0.0, 0.50))
            final_score = (1.0 - conf_w) * core + conf_w * conf

            r.setdefault("components", {})["scoreMix"] = {
                "stockWeight": round(float(w.get("stock", 0.0)), 4),
                "themeWeight": round(float(w.get("theme", 0.0)), 4),
                "newsWeight": round(float(w.get("news", 0.0)), 4),
                "technicalWeight": round(float(w.get("technical", 0.0)), 4),
                "confidenceWeight": round(float(conf_w), 4),
                "valuationScale": round(float(cfg.get("valuation", 0.20)), 4),
                "coreScore": round(float(core), 2),
                "confidence": round(float(conf), 2),
            }

            # 밸류에이션 갭(목표가-현재가) 소폭 반영
            up = (r.get("components", {}).get("reportConsensus", {}) or {}).get("upsidePct")
            if isinstance(up, (int, float)):
                # 사용자 요청: 클리핑 없이 괴리율을 그대로 반영
                valuation_adj = float(up) * float(cfg.get("valuation", 0.20))
                final_score += valuation_adj
                r.setdefault("components", {})["valuation"] = {
                    "upsidePct": round(float(up), 2),
                    "adjustment": round(float(valuation_adj), 2),
                    "capRangePct": None,
                }
            else:
                r.setdefault("components", {})["valuation"] = {
                    "upsidePct": None,
                    "adjustment": 0.0,
                    "capRangePct": None,
                }

            r["score"] = round(float(final_score), 2)

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
    """기술적 분석: 방향성 분석 + 크로스 분석 + 이격도 분석만 사용."""
    close = s.astype(float)
    cur = float(close.iloc[-1])

    ma20_series = close.rolling(20).mean()
    ma60_series = close.rolling(60).mean()
    ma120_series = close.rolling(120).mean()

    ma20 = float(ma20_series.iloc[-1])
    ma60 = float(ma60_series.iloc[-1])
    ma120 = float(ma120_series.iloc[-1]) if len(close) >= 120 else ma60

    # 1) 방향성 분석: MA20 / MA60 기울기
    ma20_prev = float(ma20_series.iloc[-6]) if len(ma20_series.dropna()) >= 6 else ma20
    ma60_prev = float(ma60_series.iloc[-6]) if len(ma60_series.dropna()) >= 6 else ma60
    dir20 = (ma20 / ma20_prev - 1) * 100 if ma20_prev else 0.0
    dir60 = (ma60 / ma60_prev - 1) * 100 if ma60_prev else 0.0

    # 2) 크로스 분석: MA20-MA60 관계 변화
    spread_now = ma20 - ma60
    spread_prev = (float(ma20_series.iloc[-6]) - float(ma60_series.iloc[-6])) if (len(ma20_series.dropna()) >= 6 and len(ma60_series.dropna()) >= 6) else spread_now

    # 3) 이격도 분석: 현재가 vs MA20
    dist_ma20 = (cur / ma20 - 1) * 100 if ma20 else 0.0

    score = 50.0

    # 방향성 점수
    if dir20 > 0.2 and dir60 >= 0:
        score += 10
    elif dir20 < -0.2 and dir60 <= 0:
        score -= 10

    # 크로스 점수
    if spread_prev <= 0 < spread_now:
        score += 10  # 골든크로스 전환
        cross_state = "golden-cross"
    elif spread_prev >= 0 > spread_now:
        score -= 10  # 데드크로스 전환
        cross_state = "dead-cross"
    else:
        cross_state = "above-ma60" if spread_now > 0 else "below-ma60"
        score += 4 if spread_now > 0 else -4

    # 이격도 점수 (먹을 자리/과열)
    if -8 <= dist_ma20 <= -2:
        score += 16
        setup = "adjustment-zone"
        regime = "adjustment"
    elif dist_ma20 >= 8:
        score -= 16
        setup = "overheat-zone"
        regime = "overheat"
    else:
        setup = "neutral-zone"
        regime = "neutral"

    score = float(np.clip(score, 0, 100))

    return {
        "score": round(score, 2),
        "rsi14": None,
        "distMa20Pct": round(float(dist_ma20), 2),
        "from20dHighPct": None,
        "ma20": round(ma20, 2),
        "ma60": round(ma60, 2),
        "ma120": round(ma120, 2),
        "setup": setup,
        "regime": regime,
        "direction20Pct": round(float(dir20), 2),
        "direction60Pct": round(float(dir60), 2),
        "crossState": cross_state,
        "headroomPct": None,
    }


def evaluate_asset(asset: Asset) -> Dict | None:
    s = _download_close(asset.symbol, "1y")
    if s is None:
        return None

    report_consensus = _consensus(asset.symbol, asset.name)

    # 사용자 요청: 목표주가 컨센서스가 없는 종목은 추천 제외
    if report_consensus.get("targetMeanPrice") is None:
        return None

    momentum = _momentum_score(s)
    crowd = _news(asset.symbol, asset.name)
    liquidity = _liquidity_score(asset.symbol)
    risk = _risk_score(s)
    technical = _technical_score(s, target_price=report_consensus.get("targetMeanPrice"))

    # 종목 자체 점수(요청): 리서치/컨센서스 점수를 종목 점수로 사용
    base_score = float(report_consensus["score"])

    # 데이터 품질 기반 신뢰도
    r_conf = float(report_consensus.get("confidence", 50.0) or 0.0)
    c_conf = float(np.clip(((crowd.get("headlineCount", 0) or 0) / 8.0) * 100, 0, 100))
    t_setup = technical.get("setup")
    if t_setup == "adjustment-zone":
        t_conf = 85.0
    elif t_setup == "overheat-zone":
        t_conf = 65.0
    else:
        t_conf = 72.0
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

    company_info = {"summary": None, "sector": None, "industry": None, "website": None}
    try:
        info = yf.Ticker(asset.symbol).info or {}
        company_info = {
            "summary": (info.get("longBusinessSummary") or "")[:700] or None,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "website": info.get("website"),
        }
    except Exception:
        pass

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
            "companyInfo": company_info,
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


def build_report(market: str = "all", candidate_limit: int | None = None, progress_cb=None, score_config: Dict[str, Any] | None = None) -> Dict:
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

    cfg = _normalize_score_config(score_config)

    # 종목분석 완료 후, 런타임 테마 추정/테마점수 산출/최종점수 반영
    rows = _apply_runtime_theme_scores(rows, score_config=cfg)

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
        "methodology": _score_methodology_text(cfg),
        "scoreConfig": cfg,

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
    top = report.get("topPick") or {}
    if top:
        entry = {
            "symbol": top.get("symbol"),
            "name": top.get("name"),
            "score": top.get("score"),
            "expectedReturnPct": top.get("expectedReturnPct"),
            "riskReward": top.get("riskReward"),
            "generatedAt": report.get("generatedAt"),
            "market": report.get("market"),
            "candidateLimit": report.get("candidateLimit"),
            "methodology": report.get("methodology"),
            "plan": top.get("plan"),
            "components": top.get("components"),
        }
        save_archive_entry(entry)


def list_archived_picks() -> List[Dict]:
    archive = _load_archive()
    items = list(archive.values())
    items.sort(key=lambda x: x.get("generatedAt") or "", reverse=True)
    return items


def get_archived_pick(symbol: str) -> Dict | None:
    if not symbol:
        return None
    return _load_archive().get(symbol.upper().strip())


def delete_archived_pick(symbol: str) -> bool:
    key = str(symbol or "").upper().strip()
    if not key:
        return False
    archive = _load_archive()
    if key not in archive:
        return False
    del archive[key]
    _save_archive(archive)
    return True


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
