from __future__ import annotations

import os
import re
import csv
import uuid
import threading
from datetime import datetime, timedelta
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

TEMA_ROOT = Path(os.environ.get("TEMA_ROOT", r"C:\project\04.app\temaWeb\tema"))

ENABLE_REFRESH_RAW = os.environ.get("ENABLE_REFRESH", "false")
ENABLE_REFRESH = ENABLE_REFRESH_RAW.strip().lower() in ("1", "true", "yes", "y", "on")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN", "")

app = FastAPI(title="Tema Server", version="final-final-1.4.0")
static_dir = Path(__file__).parent / "static"

DATE_RE = re.compile(r"^\d{6}$")
# 테마 CSV는 보통 "1.테마명_1,234.csv" 형태지만, 앞으로는 prefix가 없어도 동작하게 만든다.
LEADING_RANK_RE = re.compile(r"^\d{1,3}\.")

# "삼성전자(우 포함)" + "SK하이닉스"만 제외 (테마 선정 왜곡 제거용)
def _is_bigcap(name: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False
    if n.startswith("삼성전자"):
        return True
    return n == "SK하이닉스"


_refresh_lock = threading.Lock()
_refresh_state: Dict[str, Any] = {
    "in_progress": False,
    "started_at": None,
    "ended_at": None,
    "last_result": None,
    "last_error": None,
    "refresh_id": 0,
}


# --- record.csv 저장(사용자 기록) ---------------------------------------
_record_lock = threading.Lock()
RECORD_PATH = Path(os.environ.get("RECORD_PATH", str(TEMA_ROOT / "record.csv")))
RECORD_COLUMNS = [
    "기록ID",
    "기록시각",
    "날짜",
    "테마명",
    "테마랭크",
    "테마파일",
    "차트링크",
    "종목명",
    "종목코드",
    "시가총액",
    "거래대금",
    "등락률",
    "알파값",
    "베타값",
    "익일거래일",
    "익일종가",
    "익일고가",
    "익일종가수익률",
    "익일고가수익률",
]


def _ensure_record_schema() -> None:
    """record.csv가 기존(구버전) 헤더로 존재하면, 신규 컬럼을 추가해서 마이그레이션한다."""
    if (not RECORD_PATH.exists()) or (RECORD_PATH.stat().st_size == 0):
        return

    try:
        # 첫 줄(헤더)만 확인
        with RECORD_PATH.open("r", encoding="utf-8-sig", newline="") as f:
            first = f.readline().strip("\n").strip("\r")
    except Exception:
        try:
            with RECORD_PATH.open("r", encoding="utf-8", newline="") as f:
                first = f.readline().strip("\n").strip("\r")
        except Exception:
            return

    if not first:
        return

    header = [h.strip() for h in first.split(",")]
    # 이미 최신 스키마면 종료
    if header == RECORD_COLUMNS:
        return

    # 기존 헤더가 부분집합(구버전)인 경우만 마이그레이션
    # (사용자가 임의로 편집한 경우는 보수적으로 그대로 둔다)
    if not set(header).issubset(set(RECORD_COLUMNS)):
        return

    try:
        df = pd.read_csv(RECORD_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    except Exception:
        try:
            df = pd.read_csv(RECORD_PATH, encoding="utf-8", dtype=str, keep_default_na=False)
        except Exception:
            return

    for c in RECORD_COLUMNS:
        if c not in df.columns:
            df[c] = ""

    # 구버전 record.csv에 기록ID가 없거나 비어있는 경우, 삭제 기능을 위해 자동 부여
    if "기록ID" in df.columns:
        try:
            mask = df["기록ID"].astype(str).str.strip() == ""
            if bool(mask.any()):
                df.loc[mask, "기록ID"] = [uuid.uuid4().hex for _ in range(int(mask.sum()))]
        except Exception:
            pass

    # 컬럼 순서 정렬 후 덮어쓰기
    df = df[RECORD_COLUMNS]
    tmp = RECORD_PATH.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(RECORD_PATH)

def _append_record_csv(payload: Dict[str, Any]) -> Dict[str, Any]:
    """record.csv에 1행 append. 없으면 헤더 포함 생성. (구버전 헤더면 자동 확장)"""
    row = {k: "" for k in RECORD_COLUMNS}

    # 기록ID/기록시각
    row["기록ID"] = str(payload.get("record_id", "") or "").strip() or uuid.uuid4().hex
    row["기록시각"] = str(payload.get("saved_at", "") or "").strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # payload keys(영문)을 한글 컬럼에 매핑
    row["날짜"] = str(payload.get("date", "") or "").strip()
    row["테마명"] = str(payload.get("theme_title", "") or "").strip()
    row["테마랭크"] = str(payload.get("theme_rank", "") or "").strip()
    row["테마파일"] = str(payload.get("theme_filename", "") or "").strip()
    row["차트링크"] = str(payload.get("chart_url", "") or "").strip()

    row["종목명"] = str(payload.get("name", "") or "").strip()
    row["종목코드"] = str(payload.get("code", "") or "").strip()
    row["시가총액"] = str(payload.get("market_cap", "") or "").strip()
    row["거래대금"] = str(payload.get("trade_value", "") or "").strip()
    row["등락률"] = str(payload.get("change_rate", "") or "").strip()
    row["알파값"] = str(payload.get("alpha", "") or "").strip()
    row["베타값"] = str(payload.get("beta", "") or "").strip()

    row["익일거래일"] = str(payload.get("next_trade_date", "") or "").strip()
    row["익일종가"] = str(payload.get("next_close", "") or "").strip()
    row["익일고가"] = str(payload.get("next_high", "") or "").strip()
    row["익일종가수익률"] = str(payload.get("d1_close_rate", "") or "").strip()
    row["익일고가수익률"] = str(payload.get("d1_high_rate", "") or "").strip()

    RECORD_PATH.parent.mkdir(parents=True, exist_ok=True)

    with _record_lock:
        _ensure_record_schema()
        new_file = (not RECORD_PATH.exists()) or (RECORD_PATH.stat().st_size == 0)

        with RECORD_PATH.open("a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=RECORD_COLUMNS)
            if new_file:
                w.writeheader()
            w.writerow(row)

    return {"path": str(RECORD_PATH), "row": row}


# --- record forward-field correction (server-side) ---
def _recompute_next_ohlcv_for_record(payload: dict) -> None:
    """record 저장 직전에 익일 종가/고가가 비어있거나 잘못될 수 있어 서버에서 재계산해 보정한다.

    - date(yymmdd/yyyymmdd)와 code가 있을 때만 시도
    - pykrx 사용 가능할 때만 수행
    - 성공하면 next_trade_date/next_close/next_high 및 수익률을 채운다
    """
    try:
        date = str(payload.get("date", "") or "").strip()
        code = str(payload.get("code", "") or "").strip()
        if not date or not code:
            return

        # 종목코드 정규화: A005930 / 5930 등도 6자리로 통일
        code6 = _norm_ticker(code)
        if not code6:
            return
        payload["code"] = code6
        code = code6

        # date -> yyyymmdd
        date8 = ""
        if re.match(r"^\d{6}$", date):
            date8 = _yymmdd_to_yyyymmdd(date)
        elif re.match(r"^\d{8}$", date):
            date8 = date
            # record.csv는 yymmdd로 저장하는 편이 보기 좋아서 보정
            payload["date"] = _yyyymmdd_to_yymmdd(date)
        else:
            return
        if not date8:
            return

        # date8이 휴일이면 직전 거래일을 base로 잡는다
        base8 = _prev_business_day(date8) or date8
        next8 = _next_business_day(base8)
        if not next8:
            return

        payload["next_trade_date"] = _yyyymmdd_to_yymmdd(next8)

        b = _ohlcv_one_day(base8, code)
        n = _ohlcv_one_day(next8, code)
        if not b or not n:
            return

        base_close = b.get("close")
        next_close = n.get("close")
        next_high = n.get("high")
        if base_close is None or float(base_close) <= 0:
            return

        if next_close is not None:
            try:
                payload["next_close"] = str(int(next_close))
            except Exception:
                payload["next_close"] = str(next_close)
        if next_high is not None:
            try:
                payload["next_high"] = str(int(next_high))
            except Exception:
                payload["next_high"] = str(next_high)

        # 수익률도 같이 채움 (없을 때만)
        try:
            if payload.get("d1_close_rate", "") in ("", None) and next_close is not None:
                payload["d1_close_rate"] = _fmt_pct((float(next_close) - float(base_close)) / float(base_close) * 100.0)
        except Exception:
            pass
        try:
            if payload.get("d1_high_rate", "") in ("", None) and next_high is not None:
                payload["d1_high_rate"] = _fmt_pct((float(next_high) - float(base_close)) / float(base_close) * 100.0)
        except Exception:
            pass

    except Exception:
        # 기록 저장 자체가 막히면 안 되므로 조용히 무시
        return

def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _list_date_dirs() -> List[str]:
    if not TEMA_ROOT.exists():
        return []
    dates = [p.name for p in TEMA_ROOT.iterdir() if p.is_dir() and DATE_RE.match(p.name)]
    dates.sort()
    return dates


def _latest_date_dir() -> str:
    dates = _list_date_dirs()
    if not dates:
        raise HTTPException(404, f"날짜 폴더를 찾을 수 없습니다. TEMA_ROOT={TEMA_ROOT}")
    return dates[-1]


def _safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    except Exception:
        return pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)


def _parse_theme_title(filename: str) -> str:
    """
    지원 형태:
      - 1.전기차_1,045,470.csv
      - 전기차_1,045,470.csv
      - 1.전기차.csv
      - 전기차.csv
    """
    base = filename[:-4] if filename.lower().endswith(".csv") else filename
    base = base.strip()

    # leading rank: "1." / "01." / "123." 제거
    if "." in base and LEADING_RANK_RE.match(base):
        base = base.split(".", 1)[1]

    # trailing metric: "_1,045,470" 제거
    if "_" in base:
        base = base.rsplit("_", 1)[0]

    return base.strip()


def _pick_col(df: pd.DataFrame, contains: str) -> Optional[str]:
    if contains in df.columns:
        return contains
    for c in df.columns:
        if contains in str(c):
            return c
    return None


_NUM_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")


def _to_float(x: Any) -> float:
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace(",", "").replace("%", "").replace("+", "")
    m = _NUM_RE.search(s)
    if not m:
        return 0.0
    try:
        return float(m.group(0))
    except Exception:
        return 0.0


def _to_int(x: Any) -> int:
    # 거래대금/거래량/현재가/시총 등: 정렬/합계용
    try:
        return int(_to_float(x))
    except Exception:
        return 0


def _apply_exclude_bigcaps(df: pd.DataFrame) -> pd.DataFrame:
    name_col = _pick_col(df, "종목명")
    if not name_col:
        return df.reset_index(drop=True)

    mask = df[name_col].astype(str).map(_is_bigcap)
    out = df.loc[~mask].reset_index(drop=True)
    return out


def _compute_theme_metric_sum(df: pd.DataFrame) -> int:
    """
    테마 선정(상위 N개) 기준: 거래대금 합계
    - 기존 파일명 생성 로직(01today_tema.py)과 호환되게
      '거래대금(백만)'이어도 *1,000,000 하지 않고, 표의 숫자 그대로 합한다.
    """
    tv_col = _pick_col(df, "거래대금")
    if not tv_col:
        return 0
    s = df[tv_col].astype(str).str.replace(",", "", regex=False).str.strip()
    vals = pd.to_numeric(s, errors="coerce").fillna(0)
    try:
        return int(vals.sum())
    except Exception:
        return int(float(vals.sum()))


def _sort_df_for_response(df: pd.DataFrame, sort_key: str) -> pd.DataFrame:
    key = (sort_key or "changerate").strip().lower()

    if key in {"등락률", "change_rate", "changerate", "change", "rate"}:
        col = _pick_col(df, "등락률")
        if not col:
            return df.reset_index(drop=True)
        metric = (
            df[col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.replace("+", "", regex=False)
            .str.strip()
        )
        metric = pd.to_numeric(metric, errors="coerce").fillna(-1e18)
        return df.assign(_m=metric).sort_values("_m", ascending=False, kind="mergesort").drop(columns=["_m"]).reset_index(drop=True)

    if key in {"거래대금", "trade_value", "tradevalue", "trade", "value"}:
        col = _pick_col(df, "거래대금")
        if not col:
            return df.reset_index(drop=True)
        metric = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
        metric = pd.to_numeric(metric, errors="coerce").fillna(-1)
        # (백만)도 정렬용이면 *1,000,000 보정 (표시값은 그대로)
        if "백만" in str(col):
            metric = metric * 1_000_000
        return df.assign(_m=metric).sort_values("_m", ascending=False, kind="mergesort").drop(columns=["_m"]).reset_index(drop=True)

    if key in {"거래량", "volume"}:
        col = _pick_col(df, "거래량")
        if not col:
            return df.reset_index(drop=True)
        metric = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
        metric = pd.to_numeric(metric, errors="coerce").fillna(-1)
        return df.assign(_m=metric).sort_values("_m", ascending=False, kind="mergesort").drop(columns=["_m"]).reset_index(drop=True)

    return df.reset_index(drop=True)


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    def pick_key(substr: str) -> Optional[str]:
        for k in row.keys():
            if substr in str(k):
                return k
        return None

    name_k = "종목명" if "종목명" in row else pick_key("종목명")
    code_k = "종목코드" if "종목코드" in row else pick_key("종목코드")
    chg_k = pick_key("등락률")
    tv_k = pick_key("거래대금")
    vol_k = pick_key("거래량")
    price_k = "현재가" if "현재가" in row else pick_key("현재가")
    cap_k = "시가총액" if "시가총액" in row else pick_key("시가총액")
    chart_k = "차트링크" if "차트링크" in row else pick_key("차트")

    return {
        "name": row.get(name_k, "") if name_k else "",
        "code": row.get(code_k, "") if code_k else "",
        "change_rate": row.get(chg_k, "") if chg_k else "",
        "price": row.get(price_k, "") if price_k else "",
        "trade_value": row.get(tv_k, "") if tv_k else "",
        "volume": row.get(vol_k, "") if vol_k else "",
        "market_cap": row.get(cap_k, "") if cap_k else "",
        "chart_url": row.get(chart_k, "") if chart_k else "",
        "raw": row,
    }




# --- D+1(다음 거래일) 수익률/고가수익률 계산 (pykrx 사용) -----------------------

def _yymmdd_to_yyyymmdd(yymmdd: str) -> str:
    yymmdd = (yymmdd or "").strip()
    if len(yymmdd) != 6 or not yymmdd.isdigit():
        return ""
    yy = int(yymmdd[:2])
    mm = yymmdd[2:4]
    dd = yymmdd[4:6]
    # 00~69 => 2000~2069, 70~99 => 1970~1999 (보수적 처리)
    yyyy = 2000 + yy if yy <= 69 else 1900 + yy
    return f"{yyyy:04d}{mm}{dd}"


def _yyyymmdd_to_yymmdd(yyyymmdd: str) -> str:
    s = (yyyymmdd or "").replace("-", "").strip()
    if len(s) != 8 or not s.isdigit():
        return ""
    return s[2:]


def _norm_ticker(code: Any) -> str:
    s = str(code or "").strip()
    # 종목코드가 'A005930' 같은 형태일 수도 있어서 숫자만 추출
    m = re.search(r"(\d{5,6})", s)
    if not m:
        return ""
    return m.group(1).zfill(6)


def _fmt_pct(v: float) -> str:
    try:
        return f"{float(v):+.2f}%"
    except Exception:
        return ""


def _try_import_pykrx():
    try:
        from pykrx import stock  # type: ignore
        return stock, None
    except Exception as e:
        return None, f"pykrx import 실패: {type(e).__name__}: {e}"


def _pick_ohlcv_col(df: pd.DataFrame, kind: str) -> Optional[str]:
    # kind: "close" | "high"
    if df is None or df.empty:
        return None
    keys = []
    k = (kind or "").lower().strip()
    if k == "close":
        keys = ["종가", "close", "Close", "CLOSE"]
    elif k == "high":
        keys = ["고가", "high", "High", "HIGH"]
    else:
        return None

    for key in keys:
        if key in df.columns:
            return key

    # 부분일치도 허용
    for c in df.columns:
        cs = str(c)
        for key in keys:
            if key in cs:
                return c
    return None


def _df_last_date8(df: Any) -> str:
    """get_market_ohlcv_by_date 결과의 마지막 거래일을 yyyymmdd로 반환."""
    try:
        if df is None or getattr(df, "empty", True):
            return ""
        idx = getattr(df, "index", None)
        if idx is None or len(idx) == 0:
            return ""
        last = idx[-1]
        # pandas Timestamp / datetime
        if hasattr(last, "strftime"):
            return last.strftime("%Y%m%d")
        s = str(last).strip()
        # 'YYYY-MM-DD' or 'YYYY/MM/DD' or 'YYYYMMDD'
        m = re.search(r"(\d{4})[-/]?(\d{2})[-/]?(\d{2})", s)
        if not m:
            return ""
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    except Exception:
        return ""


def _df_has_exact_date(df: Any, date8: str) -> bool:
    """pykrx가 휴일에 '가장 가까운 거래일'로 보정해주는 경우를 방지하기 위해 정확히 일치하는지 확인."""
    if not date8:
        return False
    last = _df_last_date8(df)
    return bool(last) and last == date8

def _prev_business_day(yyyymmdd: str) -> Optional[str]:
    """yyyymmdd 이하에서 '가장 최근' 거래일(휴일/주말이면 직전 거래일)."""
    stock, _err = _try_import_pykrx()
    if stock is None:
        return None

    try:
        base = datetime.strptime(yyyymmdd, "%Y%m%d")
    except Exception:
        return None

    # 최대 60일 역방향 스캔
    for i in range(0, 60):
        d = (base - timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv_by_date(d, d, "005930")
        except Exception:
            df = None
        if _df_has_exact_date(df, d):
            return d

    return None
@lru_cache(maxsize=64)
def _next_business_day(yyyymmdd: str) -> Optional[str]:
    """yyyymmdd 이후에서 '다음' 거래일(주말/휴일 포함 시에도 안전)."""
    stock, _err = _try_import_pykrx()
    if stock is None:
        return None

    try:
        base = datetime.strptime(yyyymmdd, "%Y%m%d")
    except Exception:
        return None

    # 최대 60일 정방향 스캔
    for i in range(1, 60):
        d = (base + timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv_by_date(d, d, "005930")
        except Exception:
            df = None
        if _df_has_exact_date(df, d):
            return d

    return None

@lru_cache(maxsize=16)
def _ohlcv_all_by_ticker(yyyymmdd: str) -> Optional[pd.DataFrame]:
    stock, err = _try_import_pykrx()
    if stock is None:
        return None

    dfs: List[pd.DataFrame] = []
    # KOSPI/KOSDAQ 합쳐서 lookup
    for market in ("KOSPI", "KOSDAQ"):
        try:
            df = stock.get_market_ohlcv_by_ticker(yyyymmdd, market=market)
        except TypeError:
            # 구버전 시그니처 대비
            try:
                df = stock.get_market_ohlcv_by_ticker(yyyymmdd, market)
            except Exception:
                df = None
        except Exception:
            df = None

        if df is not None and not getattr(df, "empty", True):
            dfs.append(df)

    if not dfs:
        # 최후: market 파라미터 없는 형태
        try:
            df = stock.get_market_ohlcv_by_ticker(yyyymmdd)
            if df is None or getattr(df, "empty", True):
                return None
            dfs = [df]
        except Exception:
            return None

    out = pd.concat(dfs, axis=0)
    out = out[~out.index.duplicated(keep="first")].copy()

    # index를 6자리 종목코드로 정규화
    try:
        idx = out.index.astype(str)
        idx = idx.str.extract(r"(\d+)")[0].fillna(idx)
        out.index = idx.str.zfill(6)
    except Exception:
        pass

    return out



@lru_cache(maxsize=20000)
def _ohlcv_one_day(yyyymmdd: str, ticker: str) -> Optional[Dict[str, float]]:
    """
    (Fallback) 특정 종목 1일치 OHLCV만 가져온다.
    - bulk(get_market_ohlcv_by_ticker) 조회가 환경/네트워크 이슈로 실패하는 경우를 대비.
    """
    stock, err = _try_import_pykrx()
    if stock is None:
        return None
    try:
        df = stock.get_market_ohlcv_by_date(yyyymmdd, yyyymmdd, ticker)
        if df is None or getattr(df, "empty", True):
            return None
        close_col = _pick_ohlcv_col(df, "close")
        high_col = _pick_ohlcv_col(df, "high")
        if not close_col or not high_col:
            return None
        row = df.iloc[-1]
        close_v = float(row[close_col])
        high_v = float(row[high_col])
        if close_v == 0:
            return None
        return {"close": close_v, "high": high_v}
    except Exception:
        return None

def _forward_ctx_for_date_dir(date_dir: str) -> Dict[str, Any]:
    """
    과거 조회(date 파라미터)에서만 사용.
    - base_trade_date: date_dir 기준(휴일/주말이면 직전) 종가 기준일
    - next_trade_date: date_dir 다음 거래일
    """
    ctx: Dict[str, Any] = {
        "ok": False,
        "error": None,
        "warn": None,
        "base_trade_date": None,   # yymmdd
        "next_trade_date": None,   # yymmdd
        "base_trade_date8": None,  # yyyymmdd
        "next_trade_date8": None,  # yyyymmdd
        "base_df": None,
        "next_df": None,
        "base_close_col": None,
        "next_close_col": None,
        "next_high_col": None,
    }

    stock, err = _try_import_pykrx()
    if stock is None:
        ctx["error"] = err
        return ctx

    d8 = _yymmdd_to_yyyymmdd(date_dir)
    if not d8:
        ctx["error"] = "date_dir를 yyyymmdd로 변환하지 못했습니다."
        return ctx

    base8 = _prev_business_day(d8)
    next8 = _next_business_day(base8) if base8 else None
    if not base8:
        ctx["error"] = f"거래일 계산 실패 (base={base8}, next={next8})"
        return ctx

    # 익일(다음 거래일) 데이터가 아직 없으면, forward 정보만 제공하고 수익률/익일값은 비운다.
    if not next8:
        ctx["warn"] = f"익일(다음 거래일) 데이터가 아직 없습니다. (base={base8})"
        ctx.update({
            "ok": True,
            "base_trade_date8": base8,
            "next_trade_date8": None,
            "base_trade_date": _yyyymmdd_to_yymmdd(base8),
            "next_trade_date": "",
        })
        return ctx

    base_df = _ohlcv_all_by_ticker(base8)
    next_df = _ohlcv_all_by_ticker(next8)
    if base_df is None or next_df is None or base_df.empty or next_df.empty:
        # 일부 환경에서 KRX 대량 응답이 차단/실패할 수 있다.
        # 이 경우, 후속 단계에서 종목 단위(1일치) 조회로 fallback 한다.
        ctx["warn"] = f"pykrx 가격 데이터(OHLCV) 대량조회 실패 - 종목 단위 조회로 대체합니다. (base={base8}, next={next8})"
        ctx["error"] = None
        ctx.update({"ok": True,
                    "base_trade_date8": base8, "next_trade_date8": next8,
                    "base_trade_date": _yyyymmdd_to_yymmdd(base8), "next_trade_date": _yyyymmdd_to_yymmdd(next8)})
        return ctx

    base_close_col = _pick_ohlcv_col(base_df, "close")
    next_close_col = _pick_ohlcv_col(next_df, "close")
    next_high_col = _pick_ohlcv_col(next_df, "high")
    if not base_close_col or not next_close_col or not next_high_col:
        ctx["error"] = "OHLCV 컬럼 매칭 실패"
        return ctx

    ctx.update(
        {
            "ok": True,
            "base_trade_date8": base8,
            "next_trade_date8": next8,
            "base_trade_date": _yyyymmdd_to_yymmdd(base8),
            "next_trade_date": _yyyymmdd_to_yymmdd(next8),
            "base_df": base_df,
            "next_df": next_df,
            "base_close_col": base_close_col,
            "next_close_col": next_close_col,
            "next_high_col": next_high_col,
        }
    )
    return ctx


def _enrich_rows_with_forward_metrics(rows: List[Dict[str, Any]], ctx: Dict[str, Any]) -> None:
    """
    rows에 D+1 등락률을 주입한다.
    - 기본: get_market_ohlcv_by_ticker(대량) 결과(base_df/next_df) 사용
    - 실패 시: 종목 단위(1일치) 조회로 fallback
    """
    if not rows or not ctx:
        return

    base8: Optional[str] = ctx.get("base_trade_date8")
    next8: Optional[str] = ctx.get("next_trade_date8")

    base_df: Optional[pd.DataFrame] = ctx.get("base_df") if ctx.get("ok") else None
    next_df: Optional[pd.DataFrame] = ctx.get("next_df") if ctx.get("ok") else None
    base_close_col: Optional[str] = ctx.get("base_close_col") if ctx.get("ok") else None
    next_close_col: Optional[str] = ctx.get("next_close_col") if ctx.get("ok") else None
    next_high_col: Optional[str] = ctx.get("next_high_col") if ctx.get("ok") else None

    for r in rows:
        code = _norm_ticker(r.get("code"))
        if not code:
            continue

        base_close: Optional[float] = None
        next_close: Optional[float] = None
        next_high: Optional[float] = None

        # 1) bulk path
        if (
            base_df is not None
            and next_df is not None
            and base_close_col
            and next_close_col
            and next_high_col
            and code in base_df.index
            and code in next_df.index
        ):
            base_close = _to_float(base_df.at[code, base_close_col])
            next_close = _to_float(next_df.at[code, next_close_col])
            next_high = _to_float(next_df.at[code, next_high_col])
        else:
            # 2) fallback path (per-ticker)
            if not base8 or not next8:
                continue
            b = _ohlcv_one_day(base8, code)
            n = _ohlcv_one_day(next8, code)
            if not b or not n:
                continue
            base_close = b.get("close")
            next_close = n.get("close")
            next_high = n.get("high")

        if not base_close or base_close <= 0 or next_close is None or next_high is None:
            continue

        d1_close_rate = (next_close - base_close) / base_close * 100.0
        d1_high_rate = (next_high - base_close) / base_close * 100.0

        r["d1_close_rate"] = _fmt_pct(d1_close_rate)
        r["d1_high_rate"] = _fmt_pct(d1_high_rate)




        # 익일 값(요청 시 record.csv 저장에 사용)
        try:
            r["d1_next_close"] = str(int(next_close)) if next_close is not None else ""
        except Exception:
            r["d1_next_close"] = str(next_close) if next_close is not None else ""
        try:
            r["d1_next_high"] = str(int(next_high)) if next_high is not None else ""
        except Exception:
            r["d1_next_high"] = str(next_high) if next_high is not None else ""
def _list_theme_csv_files(date_dir: str) -> List[Path]:
    folder = TEMA_ROOT / date_dir
    if not folder.exists():
        return []
    out: List[Path] = []
    for p in folder.iterdir():
        if not p.is_file() or p.suffix.lower() != ".csv":
            continue
        # 시스템용 파일 제외 (겹치는종목 / 거래대금 TOP 등)
        if p.name.startswith("00_") or p.name.startswith("00."):
            continue
        out.append(p)
    return out


def _compute_ranked_themes(date_dir: str, exclude_bigcaps: bool) -> List[Dict[str, Any]]:
    """
    여기서 "상위 테마"를 계산한다.
    - exclude_bigcaps=True면 '삼성전자/하이닉스'를 빼고 합계를 낸 뒤 테마를 정렬한다.
    - False면 전체 종목을 포함해서 정렬한다.
    """
    files = _list_theme_csv_files(date_dir)
    folder = TEMA_ROOT / date_dir

    records: List[Tuple[int, str, str]] = []  # (metric_sum, filename, title)
    for p in files:
        try:
            df = _safe_read_csv(p)
        except Exception:
            continue

        if exclude_bigcaps:
            df = _apply_exclude_bigcaps(df)

        metric_sum = _compute_theme_metric_sum(df)
        title = _parse_theme_title(p.name)
        records.append((metric_sum, p.name, title))

    # 큰 값 우선 + 타이브레이크: title, filename
    records.sort(key=lambda x: (-x[0], x[2], x[1]))

    ranked: List[Dict[str, Any]] = []
    for idx, (metric_sum, filename, title) in enumerate(records, start=1):
        ranked.append(
            {
                "rank": idx,
                "filename": filename,
                "title": title,
                "trade_sum": metric_sum,
                "path": str(folder / filename),
            }
        )
    return ranked


def _compute_theme_insights(lookback: int = 20, top_n: int = 10, exclude_bigcaps: bool = False) -> Dict[str, Any]:
    dates = _list_date_dirs()
    if not dates:
        return {"dates": [], "hottest": [], "rising": []}

    use_dates = dates[-max(1, int(lookback)):]
    theme_hist: Dict[str, List[Tuple[str, int, int]]] = {}

    for d in use_dates:
        ranked = _compute_ranked_themes(d, exclude_bigcaps=exclude_bigcaps)
        for row in ranked[:max(1, int(top_n))]:
            title = row.get("title") or ""
            if not title:
                continue
            theme_hist.setdefault(title, []).append((d, int(row.get("rank", 9999)), int(row.get("trade_sum", 0))))

    hottest = []
    for title, rows in theme_hist.items():
        freq = len(rows)
        avg_rank = sum(r[1] for r in rows) / max(1, freq)
        avg_trade = sum(r[2] for r in rows) / max(1, freq)
        # 최근 가중치: 뒤쪽 날짜일수록 가중치↑
        weighted = 0.0
        for d, rk, _tv in rows:
            w = (use_dates.index(d) + 1) / len(use_dates)
            weighted += w * (top_n + 1 - min(rk, top_n + 1))

        hottest.append({
            "title": title,
            "freq": freq,
            "avg_rank": round(avg_rank, 2),
            "avg_trade_sum": int(avg_trade),
            "momentum_score": round(weighted, 2),
            "last_seen": rows[-1][0],
            "last_rank": rows[-1][1],
        })

    hottest.sort(key=lambda x: (-x["freq"], x["avg_rank"], -x["momentum_score"], -x["avg_trade_sum"]))

    # rising: 최근 절반 평균랭크 개선 폭
    rising = []
    split = max(1, len(use_dates) // 2)
    prev_dates = set(use_dates[:split])
    recent_dates = set(use_dates[split:])

    for title, rows in theme_hist.items():
        prev = [rk for (d, rk, _tv) in rows if d in prev_dates]
        recent = [rk for (d, rk, _tv) in rows if d in recent_dates]
        if not prev or not recent:
            continue
        prev_avg = sum(prev) / len(prev)
        recent_avg = sum(recent) / len(recent)
        improvement = prev_avg - recent_avg  # +면 상승
        rising.append({
            "title": title,
            "improvement": round(improvement, 2),
            "prev_avg_rank": round(prev_avg, 2),
            "recent_avg_rank": round(recent_avg, 2),
            "recent_freq": len(recent),
        })

    rising.sort(key=lambda x: (-x["improvement"], x["recent_avg_rank"], -x["recent_freq"]))

    return {
        "dates": use_dates,
        "hottest": hottest[:20],
        "rising": rising[:20],
    }


def _theme_history_by_title(title: str, lookback: int = 60, exclude_bigcaps: bool = False) -> List[Dict[str, Any]]:
    needle = (title or "").strip().lower()
    if not needle:
        return []
    dates = _list_date_dirs()[-max(1, int(lookback)):]
    out: List[Dict[str, Any]] = []
    for d in dates:
        ranked = _compute_ranked_themes(d, exclude_bigcaps=exclude_bigcaps)
        for r in ranked:
            t = str(r.get("title") or "")
            if needle in t.lower():
                out.append({
                    "date": d,
                    "title": t,
                    "rank": r.get("rank"),
                    "trade_sum": r.get("trade_sum"),
                    "filename": r.get("filename"),
                })
                break
    return out


class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        res = await super().get_response(path, scope)
        if res.status_code == 200:
            res.headers["Cache-Control"] = "no-store"
        return res


app.mount("/static", NoCacheStaticFiles(directory=str(static_dir)), name="static")


@app.get("/favicon.ico")
def favicon():
    # 브라우저가 /favicon.ico를 찾을 때 404 안 뜨게
    return FileResponse(str(static_dir / "favicon.ico"))


@app.get("/", response_class=HTMLResponse)
def index():
    html = (static_dir / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@app.get("/theme", response_class=HTMLResponse)
def theme_page():
    html = (static_dir / "theme.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@app.get("/record", response_class=HTMLResponse)
def record_page():
    html = (static_dir / "record.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})




@app.get("/api/status")
def api_status():
    dates = _list_date_dirs()
    latest = dates[-1] if dates else None
    return {
        "tema_root": str(TEMA_ROOT),
        # UI에서 과거 날짜 선택을 위해 전체 날짜 목록을 내려준다.
        "dates": dates,
        "latest": latest,
        "enable_refresh": ENABLE_REFRESH,
        "enable_refresh_raw": ENABLE_REFRESH_RAW,
        "refresh": _refresh_state,
    }


@app.get("/api/insights/summary")
def api_insights_summary(
    lookback: int = 20,
    top_n: int = 10,
    exclude_bigcaps: bool = False,
):
    lookback = max(5, min(int(lookback), 120))
    top_n = max(3, min(int(top_n), 30))
    data = _compute_theme_insights(lookback=lookback, top_n=top_n, exclude_bigcaps=exclude_bigcaps)
    return {
        "lookback": lookback,
        "top_n": top_n,
        "exclude_bigcaps": exclude_bigcaps,
        **data,
    }


@app.get("/api/insights/theme-history")
def api_insights_theme_history(
    title: str,
    lookback: int = 60,
    exclude_bigcaps: bool = False,
):
    lookback = max(10, min(int(lookback), 240))
    rows = _theme_history_by_title(title=title, lookback=lookback, exclude_bigcaps=exclude_bigcaps)
    return {
        "title": title,
        "lookback": lookback,
        "exclude_bigcaps": exclude_bigcaps,
        "count": len(rows),
        "rows": rows,
    }


@app.get("/api/themes")
def api_themes(
    limit: int = 4,
    preview_n: int = 4,
    date: Optional[str] = None,
    exclude_bigcaps: bool = False,
    sort: str = "changerate",
):
    date_dir = date or _latest_date_dir()
    if date and not DATE_RE.match(date_dir):
        raise HTTPException(400, "date는 yymmdd 형식이어야 합니다.")

    forward_ctx = _forward_ctx_for_date_dir(date_dir) if date else {"ok": False, "error": None, "base_trade_date": None, "next_trade_date": None}

    ranked = _compute_ranked_themes(date_dir, exclude_bigcaps=exclude_bigcaps)
    ranked = ranked[: max(0, int(limit))]

    folder = TEMA_ROOT / date_dir
    themes_out: List[Dict[str, Any]] = []

    for t in ranked:
        path = folder / t["filename"]
        if not path.exists():
            continue

        df = _safe_read_csv(path)
        if exclude_bigcaps:
            df = _apply_exclude_bigcaps(df)

        df = _sort_df_for_response(df, sort_key=sort)

        preview = [_normalize_row(r) for r in df.head(int(preview_n)).to_dict(orient="records")]
        _enrich_rows_with_forward_metrics(preview, forward_ctx)
        themes_out.append(
            {
                "rank": t["rank"],  # 현재 설정(exclude_bigcaps)에 따른 랭킹
                "title": t["title"],
                "trade_sum": t["trade_sum"],
                "filename": t["filename"],
                "preview": preview,
            }
        )

    return {
        "date": date_dir,
        "exclude_bigcaps": exclude_bigcaps,
        "sort": sort,
        "forward": {
            "ok": bool(forward_ctx.get("ok")),
            "error": forward_ctx.get("error"),
            "warn": forward_ctx.get("warn"),
            "base_trade_date": forward_ctx.get("base_trade_date"),
            "next_trade_date": forward_ctx.get("next_trade_date"),
        },
        "themes": themes_out,
    }


@app.get("/api/themes/{rank}")
def api_theme_detail(
    rank: int,
    date: Optional[str] = None,
    exclude_bigcaps: bool = False,
    sort: str = "changerate",
):
    date_dir = date or _latest_date_dir()
    if date and not DATE_RE.match(date_dir):
        raise HTTPException(400, "date는 yymmdd 형식이어야 합니다.")

    forward_ctx = _forward_ctx_for_date_dir(date_dir) if date else {"ok": False, "error": None, "base_trade_date": None, "next_trade_date": None}

    ranked = _compute_ranked_themes(date_dir, exclude_bigcaps=exclude_bigcaps)
    if rank < 1 or rank > len(ranked):
        raise HTTPException(404, f"{date_dir} 폴더에서 rank={rank} 테마를 찾지 못했습니다.")

    target = ranked[rank - 1]
    path = TEMA_ROOT / date_dir / target["filename"]
    df = _safe_read_csv(path)
    if exclude_bigcaps:
        df = _apply_exclude_bigcaps(df)

    df = _sort_df_for_response(df, sort_key=sort)
    rows = [_normalize_row(r) for r in df.to_dict(orient="records")]
    _enrich_rows_with_forward_metrics(rows, forward_ctx)

    return {
        "date": date_dir,
        "exclude_bigcaps": exclude_bigcaps,
        "sort": sort,
        "forward": {
            "ok": bool(forward_ctx.get("ok")),
            "error": forward_ctx.get("error"),
            "warn": forward_ctx.get("warn"),
            "base_trade_date": forward_ctx.get("base_trade_date"),
            "next_trade_date": forward_ctx.get("next_trade_date"),
        },
        "rank": rank,
        "title": target["title"],
        "trade_sum": target["trade_sum"],
        "filename": target["filename"],
        "rows": rows,
    }


@app.get("/api/file/{date_dir}/{filename}")
def api_download(date_dir: str, filename: str):
    if not DATE_RE.match(date_dir):
        raise HTTPException(400, "date_dir 형식이 올바르지 않습니다.")
    if "/" in filename or "\\" in filename:
        raise HTTPException(400, "filename 형식이 올바르지 않습니다.")

    path = TEMA_ROOT / date_dir / filename
    if not path.exists():
        raise HTTPException(404, "file not found")

    return FileResponse(
        str(path),
        filename=filename,
        media_type="text/csv",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/record")
def api_record_download():
    """record.csv 다운로드"""
    if not RECORD_PATH.exists():
        raise HTTPException(404, "record.csv 파일이 없습니다.")
    return FileResponse(
        str(RECORD_PATH),
        filename="record.csv",
        media_type="text/csv",
        headers={"Cache-Control": "no-store"},
    )



@app.get("/api/record/json")
def api_record_json(order: str = "desc", fix: int = 0):
    """record.csv를 JSON으로 반환 (웹페이지 표시에 사용).
    - order: asc/desc
    - fix=1이면 date+code가 있는 행의 익일 OHLCV를 pykrx로 재계산해 정정(필요시 record.csv도 갱신)
    """
    if not RECORD_PATH.exists():
        return {"ok": True, "columns": RECORD_COLUMNS, "records": [], "count": 0}

    # 1) 파일 로드 + (옵션) 정정은 잠금 하에서 처리
    with _record_lock:
        _ensure_record_schema()

        try:
            df = pd.read_csv(RECORD_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        except Exception:
            try:
                df = pd.read_csv(RECORD_PATH, encoding="utf-8", dtype=str, keep_default_na=False)
            except Exception as e:
                raise HTTPException(500, f"record.csv 읽기 실패: {type(e).__name__}: {e}")

        for c in RECORD_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        df = df[RECORD_COLUMNS]

        fix_flag = str(fix).lower().strip() in ("1", "true", "yes", "y", "on")
        fixed = 0
        if fix_flag and len(df) > 0:
            for i in df.index:
                payload = {
                    "date": df.at[i, "날짜"],
                    "code": df.at[i, "종목코드"],
                    "next_trade_date": df.at[i, "익일거래일"],
                    "next_close": df.at[i, "익일종가"],
                    "next_high": df.at[i, "익일고가"],
                    "d1_close_rate": df.at[i, "익일종가수익률"],
                    "d1_high_rate": df.at[i, "익일고가수익률"],
                }
                before = (
                    str(payload.get("code", "")),
                    str(payload.get("next_trade_date", "")),
                    str(payload.get("next_close", "")),
                    str(payload.get("next_high", "")),
                    str(payload.get("d1_close_rate", "")),
                    str(payload.get("d1_high_rate", "")),
                )

                _recompute_next_ohlcv_for_record(payload)

                after = (
                    str(payload.get("code", "")),
                    str(payload.get("next_trade_date", "")),
                    str(payload.get("next_close", "")),
                    str(payload.get("next_high", "")),
                    str(payload.get("d1_close_rate", "")),
                    str(payload.get("d1_high_rate", "")),
                )

                if after != before:
                    fixed += 1
                    if payload.get("code"):
                        df.at[i, "종목코드"] = str(payload.get("code", ""))
                    df.at[i, "익일거래일"] = str(payload.get("next_trade_date", "") or "")
                    df.at[i, "익일종가"] = str(payload.get("next_close", "") or "")
                    df.at[i, "익일고가"] = str(payload.get("next_high", "") or "")
                    df.at[i, "익일종가수익률"] = str(payload.get("d1_close_rate", "") or "")
                    df.at[i, "익일고가수익률"] = str(payload.get("d1_high_rate", "") or "")

            if fixed > 0:
                tmp = RECORD_PATH.with_suffix(".csv.tmp")
                df.to_csv(tmp, index=False, encoding="utf-8-sig")
                tmp.replace(RECORD_PATH)

    # 2) 날짜 정렬(정렬용 key 생성): yymmdd/yyyymmdd 지원
    def to_key(s: Any) -> str:
        v = str(s or "").strip().replace("-", "")
        if len(v) == 6 and v.isdigit():
            return _yymmdd_to_yyyymmdd(v)
        if len(v) == 8 and v.isdigit():
            return v
        return ""

    df["_date_key"] = df["날짜"].map(to_key)
    asc = str(order or "").lower().strip() in ("asc", "up", "1", "true", "yes", "y")
    # 빈 날짜는 맨 뒤로
    df["_empty"] = df["_date_key"].eq("").astype(int)
    df = df.sort_values(by=["_empty", "_date_key"], ascending=[True, asc], kind="mergesort").drop(columns=["_empty"])

    if not asc:
        df = df.sort_values(by=["_date_key"], ascending=False, kind="mergesort")

    df = df.drop(columns=["_date_key"], errors="ignore")

    return {
        "ok": True,
        "columns": RECORD_COLUMNS,
        "count": int(len(df)),
        "order": "asc" if asc else "desc",
        "fixed": int(fixed) if str(fix).lower().strip() in ("1","true","yes","y","on") else 0,
        "records": df.to_dict(orient="records"),
    }



@app.delete("/api/record/{record_id}")
def api_record_delete(record_id: str):
    """record.csv에서 특정 기록(기록ID) 1건 삭제"""
    if not record_id:
        raise HTTPException(400, "record_id가 비었습니다.")
    if not RECORD_PATH.exists():
        raise HTTPException(404, "record.csv가 없습니다.")

    with _record_lock:
        _ensure_record_schema()

        try:
            df = pd.read_csv(RECORD_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        except Exception:
            try:
                df = pd.read_csv(RECORD_PATH, encoding="utf-8", dtype=str, keep_default_na=False)
            except Exception as e:
                raise HTTPException(500, f"record.csv 읽기 실패: {type(e).__name__}: {e}")

        if "기록ID" not in df.columns:
            raise HTTPException(400, "record.csv에 기록ID 컬럼이 없습니다.")

        before = int(len(df))
        df = df[df["기록ID"].astype(str) != str(record_id)].copy()
        after = int(len(df))
        deleted = before - after
        if deleted <= 0:
            raise HTTPException(404, "해당 기록을 찾지 못했습니다.")

        # 저장
        df = df[RECORD_COLUMNS] if set(RECORD_COLUMNS).issubset(set(df.columns)) else df
        tmp = RECORD_PATH.with_suffix(".csv.tmp")
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        tmp.replace(RECORD_PATH)

    return {"ok": True, "deleted": deleted, "count": after}

@app.post("/api/record")
async def api_record(req: Request):
    """프론트의 '기록' 버튼 → record.csv에 append"""
    try:
        payload = await req.json()
    except Exception:
        raise HTTPException(400, "JSON body가 필요합니다.")

    name = str(payload.get("name", "") or "").strip()
    code = str(payload.get("code", "") or "").strip()
    date = str(payload.get("date", "") or "").strip()

    if not name or not code:
        raise HTTPException(400, "name/code가 필요합니다.")

    # date는 yymmdd(6) 또는 yyyymmdd(8) 허용 (비워도 됨)
    if date:
        if not (DATE_RE.match(date) or re.match(r"^\d{8}$", date)):
            raise HTTPException(400, "date 형식이 올바르지 않습니다. (yymmdd 또는 yyyymmdd)")


    _recompute_next_ohlcv_for_record(payload)

    info = _append_record_csv(payload)
    return {"ok": True, "record_path": info["path"]}

def _refresh_worker(local_refresh_id: int):
    try:
        from run_crawler import run_once

        result = run_once(str(TEMA_ROOT))
        _refresh_state["last_result"] = result
        _refresh_state["last_error"] = None
    except Exception as e:
        _refresh_state["last_error"] = f"{type(e).__name__}: {e}"
    finally:
        _refresh_state["in_progress"] = False
        _refresh_state["ended_at"] = _now_iso()
        _refresh_state["refresh_id"] = local_refresh_id
        try:
            _refresh_lock.release()
        except RuntimeError:
            pass


@app.post("/api/refresh")
def api_refresh(req: Request):
    if not ENABLE_REFRESH:
        raise HTTPException(403, "ENABLE_REFRESH=false 입니다.")

    # 토큰이 설정돼 있으면 헤더로 검증
    if REFRESH_TOKEN:
        token = req.headers.get("X-Refresh-Token", "")
        if token != REFRESH_TOKEN:
            raise HTTPException(403, "토큰이 올바르지 않습니다.")

    if not _refresh_lock.acquire(blocking=False):
        raise HTTPException(409, "이미 갱신 작업이 진행 중입니다.")

    _refresh_state["in_progress"] = True
    _refresh_state["started_at"] = _now_iso()
    _refresh_state["ended_at"] = None
    _refresh_state["last_error"] = None

    local_id = int(_refresh_state.get("refresh_id", 0)) + 1

    th = threading.Thread(target=_refresh_worker, args=(local_id,), daemon=True)
    th.start()

    return {"ok": True, "started_at": _refresh_state["started_at"], "refresh_id": local_id}
