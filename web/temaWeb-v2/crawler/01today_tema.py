import os
import re
import time
import random
import numbers
from io import StringIO
from typing import Optional
from urllib.parse import urljoin, urlparse, parse_qs
import shutil  # 추가

import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE = "https://finance.naver.com"
THEME_LIST_URL = "https://finance.naver.com/sise/theme.naver?&page={page}"
# 종목별 외국인/기관 수급(순매매) 페이지
ITEM_FRGN_URL = "https://finance.naver.com/item/frgn.naver?code={code}"
# 종목별 메인(시가총액 등) 페이지
ITEM_MAIN_URL = "https://finance.naver.com/item/main.naver?code={code}"


def sanitize_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[\\/:*?"<>|]', "_", name)  # Windows 금지 문자 제거
    name = re.sub(r"\s+", " ", name)
    return name[:120] if name else "UNKNOWN_THEME"


def prepare_output_dir(out_dir: str, clean_csv: bool = True) -> None:
    os.makedirs(out_dir, exist_ok=True)
    if clean_csv:
        for fn in os.listdir(out_dir):
            if fn.lower().endswith(".csv"):
                try:
                    os.remove(os.path.join(out_dir, fn))
                except OSError:
                    pass
def reset_dir(path: str):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)

def rename_theme_files_by_rank(theme_records, out_dir: str):
    """테마 CSV 파일명을 거래대금(또는 거래량) 합 기준으로 내림차순 랭킹을 붙여 정렬이 되게 변경.

    예) 01.전기차_1,045,470.csv
    """
    if not theme_records:
        return theme_records

    os.makedirs(out_dir, exist_ok=True)

    sorted_records = sorted(
        theme_records,
        key=lambda r: (-int(r.get("trade_sum") or 0), str(r.get("theme") or "")),
    )

    used = set()
    for rank, r in enumerate(sorted_records, start=1):
        theme_name = r.get("theme", "")
        metric = int(r.get("trade_sum") or 0)
        metric_str = f"{metric:,}"
        safe_theme = sanitize_filename(theme_name)

        base_name = f"{rank:02d}.{safe_theme}_{metric_str}.csv"
        new_path = os.path.join(out_dir, base_name)

        # 혹시 모를 파일명 충돌 방지
        if new_path in used or os.path.exists(new_path):
            k = 2
            while True:
                alt = os.path.join(out_dir, f"{rank:02d}.{safe_theme}_{metric_str}_{k}.csv")
                if (alt not in used) and (not os.path.exists(alt)):
                    new_path = alt
                    break
                k += 1

        old_path = r.get("path")
        try:
            if old_path and os.path.exists(old_path) and old_path != new_path:
                os.replace(old_path, new_path)
        except OSError:
            # rename 실패해도 크롤링 전체가 죽지 않게
            new_path = old_path or new_path

        r["path"] = new_path
        r["rank"] = rank
        used.add(new_path)

    return sorted_records

def get_html(session: requests.Session, url: str, timeout: int = 10, retry: int = 3) -> str:
    last_err = None
    for _ in range(retry):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            r.encoding = "euc-kr"
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(0.7 + random.random() * 0.7)
    raise RuntimeError(f"Failed to fetch: {url} ({last_err})")


def _flatten_cols(cols) -> list[str]:
    out = []
    for c in cols:
        if isinstance(c, tuple):
            out.append(" ".join(str(x).strip() for x in c if str(x).strip()))
        else:
            out.append(str(c).strip())
    return out


def _find_frgn_daily_table(tables: list[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """네이버 '외국인/기관 순매매(일별)' 테이블을 최대한 안전하게 찾는다."""
    for t in tables:
        cols = _flatten_cols(t.columns)
        # 일별 테이블은 보통 날짜/거래량/기관/외국인 같은 헤더를 같이 가진다.
        if (
            any("날짜" in c for c in cols)
            and any("거래량" in c for c in cols)
            and any("기관" in c for c in cols)
            and any("외국인" in c for c in cols)
        ):
            return t
    return None


def _date_to_int_yyyymmdd(s: str) -> int:
    """'YYYY.MM.DD' 또는 'YY.MM.DD' 형태를 비교 가능한 int(YYYYMMDD)로 변환."""
    if not s:
        return 0
    m = re.search(r"(\d{2,4})\.(\d{2})\.(\d{2})", str(s))
    if not m:
        return 0
    y = int(m.group(1))
    if y < 100:
        y += 2000
    mm = int(m.group(2))
    dd = int(m.group(3))
    return y * 10000 + mm * 100 + dd


def fetch_investor_net_flow(session: requests.Session, code: str) -> dict:
    """종목별 최근 1일 수급(순매매량)을 가져온다.

    - 네이버 금융 '투자자별 매매동향'에서 제공되는 일별 "기관"/"외국인" 순매매량(가장 최근 1일)을 사용.
    - '개인' 수급은 이 페이지에서 제공되지 않거나 분류가 섞일 수 있어 **추정하지 않고 제외**한다.
    """
    code = normalize_stock_code(code)
    if not code:
        return {"수급일자": "", "기관순매수": None, "외국인순매수": None}

    try:
        html = get_html(session, ITEM_FRGN_URL.format(code=code))
        tables = pd.read_html(StringIO(html))
        t = _find_frgn_daily_table(tables)
        if t is None or t.empty:
            return {"수급일자": "", "기관순매수": None, "외국인순매수": None}

        df = t.copy()
        df.columns = _flatten_cols(df.columns)

        # 날짜 컬럼 찾기
        date_col = next((c for c in df.columns if "날짜" in c), df.columns[0])

        # 빈 행 제거(테이블 중간 구분선/헤더 중복 등)
        df[date_col] = df[date_col].astype(str).str.strip()
        df = df[df[date_col].str.contains(r"\d{2,4}\.\d{2}\.\d{2}", regex=True, na=False)].copy()
        if df.empty:
            return {"수급일자": "", "기관순매수": None, "외국인순매수": None}

        # '기관', '외국인' 순매매량 컬럼 찾기 (외국인 보유주수/비중 컬럼은 제외)
        inst_col = next((c for c in df.columns if ("기관" in c and "보유" not in c and "비중" not in c)), None)
        frgn_col = next(
            (
                c
                for c in df.columns
                if ("외국인" in c and all(x not in c for x in ("보유", "주수", "비중")))
            ),
            None,
        )

        # 가장 최근 날짜(최대 YYYYMMDD)를 선택
        df["_d"] = df[date_col].apply(_date_to_int_yyyymmdd)
        df = df.sort_values("_d", ascending=False).drop(columns=["_d"]).reset_index(drop=True)
        row0 = df.iloc[0]

        date_str = str(row0.get(date_col, "")).strip()
        inst = safe_to_int(row0.get(inst_col, None)) if inst_col else None
        frgn = safe_to_int(row0.get(frgn_col, None)) if frgn_col else None

        return {"수급일자": date_str, "기관순매수": inst, "외국인순매수": frgn}

    except Exception:
        # 수급 정보는 보조 지표라 실패해도 전체 크롤링은 계속
        return {"수급일자": "", "기관순매수": None, "외국인순매수": None}


def add_investor_flow_columns(
    df: pd.DataFrame,
    session: requests.Session,
    cache: dict,
    *,
    max_workers: int = 6,
    jitter: tuple = (0.0, 0.08),
    delay: tuple = (0.12, 0.28),
) -> pd.DataFrame:
    """테마 종목 DataFrame에 수급 컬럼(기관/외국인)을 추가한다.

    성능 최적화:
    - 기존: 종목을 순차로 요청 + 매 요청마다 sleep(delay)
    - 개선: 캐시에 없는 종목만 먼저 모아 병렬로 가져온 뒤 DataFrame에 채움

    주의:
    - requests.Session은 스레드 세이프가 아니라서, 워커마다 Session을 새로 만들어 사용한다.
    - 너무 과격한 병렬(예: 30+)은 네이버 측 차단/오류를 유발할 수 있으니 max_workers는 보수적으로.
    """
    if df is None or df.empty:
        return df
    if "종목코드" not in df.columns:
        return df

    out = df.copy()
    out["수급일자"] = ""
    out["기관순매수"] = None
    out["외국인순매수"] = None

    # 1) 우선 코드 정규화
    codes = [normalize_stock_code(c) for c in out["종목코드"].tolist()]
    codes = [c for c in codes if c]

    # 2) 캐시에 없는 종목만 모아서(중복 제거) 병렬로 수집
    missing = []
    seen = set()
    for c in codes:
        if c in cache or c in seen:
            continue
        seen.add(c)
        missing.append(c)

    def _make_worker_session() -> requests.Session:
        s = requests.Session()
        # headers/cookies를 복사(있으면)
        try:
            s.headers.update(session.headers)
        except Exception:
            pass
        try:
            s.cookies.update(session.cookies)
        except Exception:
            pass
        return s

    def _worker(code_: str) -> tuple[str, dict]:
        # 작은 지연으로 요청 분산(동시에 꽂히는 것 완화)
        if jitter and isinstance(jitter, tuple):
            time.sleep(random.uniform(*jitter))
        s = _make_worker_session()
        try:
            flow = fetch_investor_net_flow(s, code_)
        except Exception:
            flow = {"수급일자": "", "기관순매수": None, "외국인순매수": None}
        finally:
            try:
                s.close()
            except Exception:
                pass
        return code_, flow

    # 병렬 수집(없으면 기존 방식으로)
    if missing:
        if max_workers and int(max_workers) > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            mw = max(1, int(max_workers))
            with ThreadPoolExecutor(max_workers=mw) as ex:
                futures = [ex.submit(_worker, c) for c in missing]
                for fut in as_completed(futures):
                    c, flow = fut.result()
                    cache[c] = flow
        else:
            # 기존 순차 + delay 유지(차단 회피용)
            for c in missing:
                cache[c] = fetch_investor_net_flow(session, c)
                time.sleep(random.uniform(*delay))

    # 3) DataFrame 채우기
    # index 기반 .at은 "실제 인덱스 값"을 쓰므로 reset_index로 안전하게
    out = out.reset_index(drop=True)
    for idx, raw_code in enumerate(out["종목코드"].tolist()):
        c = normalize_stock_code(raw_code)
        if not c:
            continue
        flow = cache.get(c, {}) if isinstance(cache, dict) else {}
        out.at[idx, "수급일자"] = flow.get("수급일자", "")
        out.at[idx, "기관순매수"] = flow.get("기관순매수", None)
        out.at[idx, "외국인순매수"] = flow.get("외국인순매수", None)

    return out


# --- 시가총액(억원) 수집 ---------------------------------------------------
def fetch_market_cap(session: requests.Session, code: str) -> dict:
    """종목별 시가총액(억원)을 가져온다.

    - 네이버 금융 종목 메인(/item/main.naver)에서 '시가총액' 값을 파싱.
    - 반환: {"시가총액": "<콤마포함>억"} 또는 {"시가총액": ""} (실패/미존재)

    주의:
    - 네이버 페이지 구조 변경에 대비해서 여러 fallback 셀렉터/정규식을 사용한다.
    """
    code = normalize_stock_code(code)
    if not code:
        return {"시가총액": ""}

    try:
        html = get_html(session, ITEM_MAIN_URL.format(code=code))
        soup = BeautifulSoup(html, "lxml")

        # 1) 가장 흔한 형태: id="_market_sum" (숫자만)
        em = soup.select_one("#_market_sum")
        if em:
            v = safe_to_int(em.get_text(strip=True))
            if v is not None and v > 0:
                return {"시가총액": f"{v:,}억"}

        # 2) 요약 테이블에서 '시가총액' th 찾기
        th = soup.find("th", string=re.compile(r"\s*시가총액\s*"))
        if th is not None:
            td = th.find_next_sibling("td")
            if td is not None:
                txt = td.get_text(" ", strip=True)
                v = safe_to_int(txt)
                if v is not None and v > 0:
                    return {"시가총액": f"{v:,}억"}

        # 3) 마지막 fallback: html 텍스트 정규식
        m = re.search(r"시가총액[^0-9]{0,40}([0-9][0-9,]+)\s*억", html)
        if m:
            v = safe_to_int(m.group(1))
            if v is not None and v > 0:
                return {"시가총액": f"{v:,}억"}

    except Exception:
        pass

    return {"시가총액": ""}


def add_market_cap_columns(
    df: pd.DataFrame,
    session: requests.Session,
    cache: dict,
    *,
    max_workers: int = 10,
    jitter: tuple = (0.0, 0.06),
    delay: tuple = (0.10, 0.22),
) -> pd.DataFrame:
    """테마 종목 DataFrame에 '시가총액' 컬럼을 추가/보완한다.

    - cache: 종목코드 -> {"시가총액": "...억"} 형태
    - 병렬 수집 + 캐시로 중복 요청 최소화
    """
    if df is None or df.empty:
        return df
    if "종목코드" not in df.columns:
        return df

    out = df.copy()
    if "시가총액" not in out.columns:
        out["시가총액"] = ""
    else:
        out["시가총액"] = out["시가총액"].astype(str).fillna("")

    # 1) 코드 정규화 + 유효 코드만
    codes = [normalize_stock_code(c) for c in out["종목코드"].tolist()]
    codes = [c for c in codes if c]

    # 2) 캐시에 없는 종목만 모아서(중복 제거) 병렬로 수집
    missing = []
    seen = set()
    for c in codes:
        if c in cache or c in seen:
            continue
        seen.add(c)
        missing.append(c)

    def _make_worker_session() -> requests.Session:
        s = requests.Session()
        try:
            s.headers.update(session.headers)
        except Exception:
            pass
        try:
            s.cookies.update(session.cookies)
        except Exception:
            pass
        return s

    def _worker(code_: str) -> tuple[str, dict]:
        if jitter and isinstance(jitter, tuple):
            import time, random
            time.sleep(random.uniform(*jitter))
        s = _make_worker_session()
        try:
            cap = fetch_market_cap(s, code_)
        except Exception:
            cap = {"시가총액": ""}
        finally:
            try:
                s.close()
            except Exception:
                pass
        return code_, cap

    if missing:
        if max_workers and int(max_workers) > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            mw = max(1, int(max_workers))
            with ThreadPoolExecutor(max_workers=mw) as ex:
                futures = [ex.submit(_worker, c) for c in missing]
                for fut in as_completed(futures):
                    c, cap = fut.result()
                    cache[c] = cap
        else:
            import time, random
            for c in missing:
                cache[c] = fetch_market_cap(session, c)
                time.sleep(random.uniform(*delay))

    # 3) DataFrame 채우기 (빈 값만 보완)
    out = out.reset_index(drop=True)
    for idx, raw_code in enumerate(out["종목코드"].tolist()):
        c = normalize_stock_code(raw_code)
        if not c:
            continue
        cur = str(out.at[idx, "시가총액"] or "").strip()
        if cur and cur.lower() not in {"nan", "none", "-"}:
            continue
        cap = cache.get(c, {}) if isinstance(cache, dict) else {}
        out.at[idx, "시가총액"] = cap.get("시가총액", "")

    return out


def parse_theme_list(html: str):
    """theme list page -> [(theme_name, theme_no, theme_url), ...]"""
    soup = BeautifulSoup(html, "lxml")
    themes = []
    seen = set()

    for a in soup.select('a[href*="sise_group_detail.naver?type=theme&no="]'):
        href = a.get("href", "")
        theme_name = a.get_text(strip=True)
        if not theme_name:
            continue

        theme_url = urljoin(BASE, href)
        qs = parse_qs(urlparse(theme_url).query)
        theme_no = qs.get("no", [None])[0]
        if not theme_no:
            continue

        if theme_no in seen:
            continue
        seen.add(theme_no)
        themes.append((theme_name, theme_no, theme_url))

    return themes


def extract_stock_code_map(detail_soup: BeautifulSoup):
    """detail page에서 종목명 -> 종목코드 매핑 추출"""
    m = {}
    for a in detail_soup.select('a[href*="/item/main.naver?code="]'):
        name = a.get_text(strip=True)
        href = a.get("href", "")
        code = parse_qs(urlparse(urljoin(BASE, href)).query).get("code", [None])[0]
        if name and code:
            m[name] = code
            m[name.replace("*", "").strip()] = code
    return m


def find_trade_value_col(df: pd.DataFrame):
    # "거래대금" 또는 "거래대금(백만)" 같은 형태 대응
    for c in df.columns:
        if "거래대금" in str(c):
            return c
    return None


def find_volume_col(df: pd.DataFrame):
    # "거래량" / "거래량(주)" 같은 변형도 잡기
    for c in df.columns:
        if "거래량" in str(c):
            return c
    return None


def find_change_rate_col(df: pd.DataFrame):
    # "등락률" / "등락률(%)" 같은 변형도 잡기
    for c in df.columns:
        if "등락률" in str(c):
            return c
    return None


def safe_to_int(v) -> Optional[int]:
    """콤마/기호가 섞인 숫자를 정수로 변환. 실패 시 None.

    ✅ 핵심: pandas가 CSV를 읽을 때 정수열을 float(예: 12345.0)로 해석하는 경우가 많음.
    이때 문자열로 바꾸고 '.'을 제거하면 123450(=10배) 오차가 생길 수 있으니
    숫자 타입은 먼저 타입 기반으로 안전하게 처리한다.
    """
    if v is None:
        return None

    # NaN 처리
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # 숫자 타입(파이썬/넘파이) 우선 처리
    if isinstance(v, bool):
        return None
    if isinstance(v, numbers.Integral):
        return int(v)
    if isinstance(v, numbers.Real):
        # 12345.0 -> 12345, 12345.6 -> 12346
        return int(round(float(v)))

    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None

    s = s.replace(",", "")
    s = re.sub(r"[^0-9\-\.]", "", s)
    if s.count(".") > 1:
        head, *rest = s.split(".")
        s = head + "." + "".join(rest)
    if not s or s == "-":
        return None

    try:
        return int(round(float(s))) if "." in s else int(s)
    except Exception:
        return None


def compute_trade_value_sum(df: pd.DataFrame) -> int:
    col = find_trade_value_col(df)
    if col is None:
        return 0

    s = pd.to_numeric(
        df[col].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0)
    return int(s.sum())


def compute_volume_sum(df: pd.DataFrame) -> int:
    col = find_volume_col(df)
    if col is None:
        return 0

    s = pd.to_numeric(
        df[col].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0)
    return int(s.sum())


def apply_exclude_patterns(df: pd.DataFrame, patterns: list[str]) -> pd.DataFrame:
    if not patterns:
        return df

    # 종목명 정리(* 제거 등) 후 regex 매칭
    names = df["종목명"].astype(str).str.replace("*", "", regex=False).str.strip()
    regex = "|".join(f"(?:{p})" for p in patterns)
    mask = names.str.contains(regex, regex=True, na=False)
    return df.loc[~mask].copy()


def parse_theme_detail(
    theme_name: str,
    theme_no: str,
    theme_url: str,
    html: str,
    exclude_patterns: Optional[list[str]] = None
) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    code_map = extract_stock_code_map(soup)

    # FutureWarning 방지: literal html -> StringIO
    tables = pd.read_html(StringIO(html))

    target = None
    for t in tables:
        if any(str(c).strip() == "종목명" for c in t.columns):
            target = t
            break
    if target is None:
        raise ValueError(f"Could not find stock table for theme {theme_no} ({theme_name})")

    df = target.copy()
    df = df[df["종목명"].notna()].copy()

    # 종목코드 추가
    df["종목명_정리"] = df["종목명"].astype(str).str.replace("*", "", regex=False).str.strip()
    df["종목코드"] = df["종목명_정리"].map(code_map)
    df.drop(columns=["종목명_정리"], inplace=True, errors="ignore")

    # 불필요 컬럼 제거(페이지 구조가 바뀌어도 errors='ignore'로 안전)
    drop_cols = ["종목명.1", "전일비", "매수호가", "매도호가", "토론", "Unnamed: 11"]
    df.drop(columns=drop_cols, inplace=True, errors="ignore")

    unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed")]
    if unnamed_cols:
        df.drop(columns=unnamed_cols, inplace=True, errors="ignore")

    exclude_patterns = exclude_patterns or []
    df = apply_exclude_patterns(df, exclude_patterns)
    # TradingView 차트 링크(마지막 열)
    if "종목코드" in df.columns:
        df["차트링크"] = df["종목코드"].apply(lambda x: build_tradingview_chart_url(x, locale="kr"))
    else:
        df["차트링크"] = ""
    return df


def normalize_stock_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).replace("*", "").strip())


def normalize_stock_code(code) -> str:
    """종목코드를 6자리 문자열로 정규화.
    - pandas가 005930을 5930.0 같은 float로 읽는 케이스 방지
    - 숫자 이외 문자 제거 후 zfill(6)
    """
    if code is None:
        return ""
    s = str(code).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    s = re.sub(r"\D", "", s)
    if not s:
        return ""
    # 대부분 국내 종목코드 6자리
    return s.zfill(6) if len(s) <= 6 else s


def build_tradingview_chart_url(code: str, locale: str = "kr") -> str:
    """TradingView 종목 차트 URL 생성.

    TradingView는 국내 주식을 KRX-<6자리코드> 형태로 제공한다.
    예) https://kr.tradingview.com/symbols/KRX-005930/
    """
    c = normalize_stock_code(code)
    if not c:
        return ""
    host = "kr.tradingview.com" if str(locale).lower().startswith("kr") else "www.tradingview.com"
    return f"https://{host}/symbols/KRX-{c}/"

    #return f"https://{host}/chart/?symbols=KRX%{c}/"

def build_overlap_stocks_csv(
    theme_records,
    out_dir: str,
    min_theme_overlap: int = 2,
    min_trade_value: int = 100000,
    output_filename: str = "00_겹치는종목_2개이상테마.csv",
    use_trade_value: bool = True,
) -> Optional[str]:
    """여러 테마 CSV에서 겹치는 종목을 모아 추가 CSV 생성.

    - 겹치는 테마수(min_theme_overlap) 이상인 종목만
    - use_trade_value=True  -> 거래대금 기준으로 min_trade_value 필터
      use_trade_value=False -> 거래량 기준으로 min_trade_value 필터

    CSV output columns:
      종목명, 종목코드, 등락률, 거래대금, 거래량, 기준, 기준값, 겹치는 테마수, 존재하는 테마1...
    """
    if not theme_records:
        return None

    stock_map = {}   # key(종목코드 우선) -> info
    name_to_key = {} # 정규화 종목명 -> key(종목코드) 매핑 (코드 누락/형변환 이슈 보정)

    for r in theme_records:
        path = r.get("path")
        theme = r.get("theme")
        if not path or not theme or not os.path.exists(path):
            continue

        # dtype 추론 때문에 종목코드가 float(5930.0)로 깨질 수 있어 전부 문자열로 읽음
        try:
            df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        except Exception:
            df = pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)

        change_col = find_change_rate_col(df)
        trade_col = find_trade_value_col(df)
        vol_col = find_volume_col(df)

        trade_is_million = bool(trade_col) and ("백만" in str(trade_col))

        for _, row in df.iterrows():
            name = normalize_stock_name(row.get("종목명", ""))
            code = normalize_stock_code(row.get("종목코드", ""))

            if not name and not code:
                continue

            # key 결정 + (이전에 name으로 들어간 entry를 code로 합치기)
            if code:
                key = code
                old_key = name_to_key.get(name)
                if old_key and old_key != key and old_key in stock_map:
                    # old_key(name 기반) -> code 기반으로 병합
                    old = stock_map.pop(old_key)
                    cur = stock_map.get(key)
                    if cur is None:
                        stock_map[key] = old
                        stock_map[key]["종목코드"] = code
                    else:
                        cur["themes"].update(old.get("themes", set()))
                        if not cur.get("종목명") and old.get("종목명"):
                            cur["종목명"] = old["종목명"]
                        if not cur.get("종목코드"):
                            cur["종목코드"] = code
                        # 수치는 큰 값 유지
                        for kcol in ("거래대금", "거래량"):
                            ov = old.get(kcol)
                            cv = cur.get(kcol)
                            if cv is None:
                                cur[kcol] = ov
                            elif ov is not None and ov > cv:
                                cur[kcol] = ov

                name_to_key[name] = key
            else:
                key = name_to_key.get(name, name)

            rate_val = ""
            if change_col is not None:
                rate_val = str(row.get(change_col, "")).strip()

            tv = safe_to_int(row.get(trade_col, None)) if trade_col is not None else None
            if tv is not None and trade_is_million:
                tv *= 1_000_000  # (백만) -> 원 단위로 통일

            vol = safe_to_int(row.get(vol_col, None)) if vol_col is not None else None

            info = stock_map.get(key)
            if info is None:
                stock_map[key] = {
                    "종목명": name,
                    "종목코드": code,
                    "등락률": rate_val,
                    "거래대금": tv,
                    "거래량": vol,
                    "themes": set([theme]),
                }
            else:
                if (not info.get("종목명")) and name:
                    info["종목명"] = name
                if (not info.get("종목코드")) and code:
                    info["종목코드"] = code
                if (not info.get("등락률")) and rate_val:
                    info["등락률"] = rate_val

                # 거래대금/거래량은 가장 큰 값으로 유지
                if info.get("거래대금") is None:
                    info["거래대금"] = tv
                elif tv is not None and tv > info["거래대금"]:
                    info["거래대금"] = tv

                if info.get("거래량") is None:
                    info["거래량"] = vol
                elif vol is not None and vol > info["거래량"]:
                    info["거래량"] = vol

                info["themes"].add(theme)

    # 후보 추리기
    기준 = "거래대금" if use_trade_value else "거래량"
    candidates = []
    max_k = 0
    for info in stock_map.values():
        themes = sorted(info["themes"])
        k = len(themes)
        if k < int(min_theme_overlap):
            continue

        metric_val = info.get(기준)
        metric_val = int(metric_val) if metric_val is not None else 0
        if metric_val <= int(min_trade_value):
            continue

        max_k = max(max_k, k)
        candidates.append((info, themes, metric_val))

    max_k = max(max_k, int(min_theme_overlap))

    base_cols = ["종목명", "종목코드", "등락률", "거래대금", "거래량", "기준", "기준값", "겹치는 테마수"]
    theme_cols = [f"존재하는 테마{i}" for i in range(1, max_k + 1)]
    out_cols = base_cols + theme_cols + ["차트링크"]
    out_path = os.path.join(out_dir, output_filename)

    if not candidates:
        pd.DataFrame(columns=out_cols).to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path
    rows = []
    for info, themes, metric_val in candidates:
        row_out = {
            "종목명": info.get("종목명", ""),
            "종목코드": info.get("종목코드", ""),
            "등락률": info.get("등락률", ""),
            "거래대금": int(info.get("거래대금") or 0),
            "거래량": int(info.get("거래량") or 0),
            "기준": 기준,
            "기준값": int(metric_val),
            "겹치는 테마수": len(themes),
        }
        for i in range(max_k):
            row_out[f"존재하는 테마{i+1}"] = themes[i] if i < len(themes) else ""

        # TradingView 차트 링크(마지막 열)
        row_out["차트링크"] = build_tradingview_chart_url(row_out.get("종목코드", ""), locale="kr")
        rows.append(row_out)

    out_df = pd.DataFrame(rows, columns=out_cols)

    out_df.sort_values(
        by=["겹치는 테마수", "기준값", "종목명"],
        ascending=[False, False, True],
        inplace=True,
        kind="mergesort",
    )
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


# =========================
# 추가: 거래대금 TOP3 row 추출 CSV
# =========================
def build_top_trade_value_csv(
    theme_records,
    out_dir: str,
    top_n: int = 3,
    output_filename: str = "00_거래대금3등.csv",
) -> Optional[str]:
    """
    (필터링 후 남은) theme CSV들을 전부 뒤져서
    '거래대금'이 가장 큰 row TOP N개를 뽑아 CSV로 저장.

    - 출력 컬럼: 원본 CSV 컬럼 그대로 + '테마명'을 '종목명' 바로 뒤에 삽입
    - 거래대금 컬럼이 '거래대금(백만)'이면 비교 시 *1,000,000으로 원 단위 환산(비교용)
      저장은 원본 값 그대로 둠(열속성/값 형태 유지)
    """
    if not theme_records:
        out_path = os.path.join(out_dir, output_filename)
        pd.DataFrame(columns=["종목명", "테마명"]).to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, output_filename)

    entries = []  # (trade_value_won:int, theme:str, row_dict:dict, base_cols:list[str])

    for r in theme_records:
        path = r.get("path")
        theme = r.get("theme", "")
        if not path or not os.path.exists(path):
            continue

        # 원본 모양을 최대한 유지하려고 문자열로 읽음
        try:
            df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        except Exception:
            df = pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)

        df.columns = [str(c).strip() for c in df.columns]
        trade_col = find_trade_value_col(df)
        if trade_col is None:
            continue

        trade_is_million = "백만" in str(trade_col)

        for _, row in df.iterrows():
            tv = safe_to_int(row.get(trade_col, None))
            if tv is None:
                continue

            tv_won = int(tv) * 1_000_000 if trade_is_million else int(tv)

            entries.append(
                (tv_won, str(theme), row.to_dict(), list(df.columns))
            )

    if not entries:
        pd.DataFrame(columns=["종목명", "테마명"]).to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path

    # 거래대금 큰 순으로 TOP N
    entries.sort(key=lambda x: x[0], reverse=True)
    top_entries = entries[: int(top_n)]

    # 출력 컬럼은 "다른 CSV와 동일"하게: 첫번째 row의 컬럼을 기준으로
    base_cols = top_entries[0][3]

    # '종목명' 뒤에 '테마명' 삽입
    if "종목명" in base_cols:
        out_cols = ["종목명", "테마명"] + [c for c in base_cols if c != "종목명"]
    else:
        # 예외(거의 없겠지만): 종목명 컬럼이 없으면 맨 앞에 테마명
        out_cols = ["테마명"] + base_cols


    # 차트링크는 항상 마지막 열로 고정
    if "차트링크" in out_cols:
        out_cols = [c for c in out_cols if c != "차트링크"]
    out_cols.append("차트링크")
    out_rows = []
    for _tv_won, theme, row_dict, _cols in top_entries:
        row_out = {}

        if "종목명" in out_cols:
            row_out["종목명"] = row_dict.get("종목명", "")
        row_out["테마명"] = theme

        for c in out_cols:
            if c in ("종목명", "테마명"):
                continue
            row_out[c] = row_dict.get(c, "")

        # TradingView 차트 링크(마지막 열)
        code_norm = normalize_stock_code(row_dict.get("종목코드", ""))
        row_out["차트링크"] = build_tradingview_chart_url(code_norm, locale="kr") if code_norm else row_dict.get("차트링크", "")

        out_rows.append(row_out)

    out_df = pd.DataFrame(out_rows, columns=out_cols)
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path

def sort_df_by_metric(df: pd.DataFrame, use_trade_value: bool = True) -> pd.DataFrame:
    """
    use_trade_value=True  -> '거래대금' 컬럼 기준 내림차순 정렬
    use_trade_value=False -> '거래량' 컬럼 기준 내림차순 정렬
    """
    col = find_trade_value_col(df) if use_trade_value else find_volume_col(df)
    if col is None or col not in df.columns:
        return df.reset_index(drop=True)

    # 문자열 콤마 제거 후 숫자 변환
    s = pd.to_numeric(
        df[col].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(-1)

    # 거래대금(백만)도 숫자 기준 정렬이 목적이므로(단위 보정은 옵션) 안전하게 보정해둠
    if use_trade_value and ("백만" in str(col)):
        s = s * 1_000_000

    # 정렬(동일값일 때 기존 순서 유지: mergesort)
    out = df.assign(_sort_metric=s).sort_values("_sort_metric", ascending=False, kind="mergesort")
    out = out.drop(columns=["_sort_metric"]).reset_index(drop=True)
    return out

def sort_df_by_change_rate(df: pd.DataFrame, descending: bool = True) -> pd.DataFrame:
    col = find_change_rate_col(df)
    if col is None or col not in df.columns:
        return df.reset_index(drop=True)

    s = (
        df[col].astype(str)
        .str.replace('%', '', regex=False)
        .str.replace(',', '', regex=False)
        .str.replace('+', '', regex=False)
        .str.strip()
    )
    metric = pd.to_numeric(s, errors="coerce").fillna(-1e18 if descending else 1e18)

    out = df.assign(_sort_metric=metric).sort_values(
        "_sort_metric", ascending=not descending, kind="mergesort"
    )
    return out.drop(columns=["_sort_metric"]).reset_index(drop=True)


def sort_df_for_save(df: pd.DataFrame, row_sort: str = "metric", use_trade_value: bool = True) -> pd.DataFrame:
    key = (row_sort or "metric").strip().lower()

    if key in {"등락률", "change_rate", "changerate", "change", "rate"}:
        return sort_df_by_change_rate(df, descending=True)
    if key in {"거래대금", "trade_value", "tradevalue", "trade", "value"}:
        return sort_df_by_metric(df, use_trade_value=True)
    if key in {"거래량", "volume"}:
        return sort_df_by_metric(df, use_trade_value=False)
    if key in {"metric", "auto", "default"}:
        return sort_df_by_metric(df, use_trade_value=use_trade_value)

    return df.reset_index(drop=True)

def crawl_themes(
    pages: int = 3,
    out_dir: str = "./domestic_stock/tema",
    delay: tuple = (0.35, 0.9),
    include_market_cap: bool = True,
    market_cap_delay: tuple = (0.10, 0.22),
    market_cap_max_workers: int = 10,
    market_cap_jitter: tuple = (0.0, 0.06),
    include_investor_flow: bool = True,
    investor_flow_delay: tuple = (0.12, 0.28),
    investor_flow_max_workers: int = 6,
    investor_flow_jitter: tuple = (0.0, 0.08),
    investor_flow_after_filter: bool = True,
    investor_flow_top_themes: Optional[int] = 10,
    clean_csv: bool = True,
    quartile_filter: bool = True,
    ValueOrVolume: bool = True,
    row_sort: str = "changerate",
    make_overlap_csv: bool = True,
    overlap_min_themes: int = 2,
    overlap_min_trade_value: int = 100000,
    overlap_csv_name: str = "00_겹치는종목_2개이상테마.csv",
    exclude_patterns: Optional[list[str]] = None
):
    prepare_output_dir(out_dir, clean_csv=clean_csv)
    exclude_patterns = exclude_patterns or []

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://finance.naver.com/",
        }
    )

    # 1) 테마 목록 수집
    all_themes = []
    for page in range(1, pages + 1):
        list_url = THEME_LIST_URL.format(page=page)
        html = get_html(session, list_url)
        all_themes.extend(parse_theme_list(html))
        time.sleep(random.uniform(*delay))

    # theme_no 기준 중복 제거
    uniq = {}
    for name, no, url in all_themes:
        uniq[no] = (name, no, url)
    themes = list(uniq.values())

    # 2) 테마별 저장 + (거래대금합 또는 거래량합) 기록
    records = []  # [{path, theme, trade_sum}]
    flow_cache = {}  # 종목코드 -> {수급일자, 기관순매수, 외국인순매수}
    cap_cache = {}   # 종목코드 -> {시가총액}

    for idx, (theme_name, theme_no, theme_url) in enumerate(themes, start=1):
        try:
            detail_html = get_html(session, theme_url)
            df = parse_theme_detail(
                theme_name, theme_no, theme_url, detail_html, exclude_patterns=exclude_patterns
            )

            # (선택) 시가총액 컬럼 추가
            if include_market_cap:
                df = add_market_cap_columns(
                    df,
                    session=session,
                    cache=cap_cache,
                    delay=market_cap_delay,
                    max_workers=market_cap_max_workers,
                    jitter=market_cap_jitter,
                )

            # (선택) 종목별 수급(기관/외국인) 컬럼 추가
            if include_investor_flow and (not investor_flow_after_filter):
                df = add_investor_flow_columns(
                    df,
                    session=session,
                    cache=flow_cache,
                    delay=investor_flow_delay,
                    max_workers=investor_flow_max_workers,
                    jitter=investor_flow_jitter,
                )

            total_metric = compute_trade_value_sum(df) if ValueOrVolume else compute_volume_sum(df)
            total_metric_str = f"{total_metric:,}"

            safe_theme = sanitize_filename(theme_name)
            filename = f"{safe_theme}_{total_metric_str}.csv"
            path = os.path.join(out_dir, filename)
            df = sort_df_for_save(df, row_sort=row_sort, use_trade_value=ValueOrVolume)

            df.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"[{idx}/{len(themes)}] saved: {path}")

            records.append({"path": path, "theme": theme_name, "trade_sum": total_metric})
            time.sleep(random.uniform(*delay))

        except Exception as e:
            print(f"[{idx}/{len(themes)}] SKIP theme_no={theme_no} theme={theme_name} reason={e}")

    if not records:
        print("No files saved. Nothing to do.")
        return

    # 3) 상위 1분위(=상위 25%)만 남기기
    kept_records = records

    if quartile_filter:
        metric_name = "거래대금" if ValueOrVolume else "거래량"
        s = pd.Series([r["trade_sum"] for r in records], dtype="float64")
        q3 = float(s.quantile(0.90))  
        print(f"Q3({metric_name} 기준 상위 1분위, 90% 분위수) = {q3:,.0f}")

        kept_records = []
        kept = 0
        deleted = 0
        for r in records:
            if r["trade_sum"] >= q3:
                kept_records.append(r)
                kept += 1
                continue
            try:
                os.remove(r["path"])
                deleted += 1
            except OSError:
                pass
        print(f"Kept (>= Q3): {kept}, Deleted (< Q3): {deleted}")
    else:
        print("Quartile filter disabled. Keeping all theme CSV files.")
    # 3.2) 거래대금(또는 거래량) 합 기준으로 파일명에 랭킹 prefix 추가 (예: 1.전기차_1,045,470.csv)
    kept_records = rename_theme_files_by_rank(kept_records, out_dir)

    # 3.3) (개선) 수급(기관/외국인)은 '주요 테마'에만 적용해 속도 개선
    # - 테마별 CSV를 먼저 저장(수급 없이) -> 거래대금/거래량 합으로 주요 테마 선정 -> 해당 파일에만 수급 컬럼을 추가해 덮어씀
    if include_investor_flow and investor_flow_after_filter:
        targets = list(kept_records)

        # investor_flow_top_themes:
        # - None 또는 0 이하: kept_records 전체에 수급 적용
        # - 양수: 거래대금(또는 거래량) 합 기준 상위 N개 테마에만 적용
        top_n = investor_flow_top_themes
        try:
            top_n = int(top_n) if top_n is not None else None
        except Exception:
            top_n = None

        if top_n is not None and top_n > 0 and len(targets) > top_n:
            targets = sorted(
                targets,
                key=lambda r: (-int(r.get("trade_sum") or 0), str(r.get("theme") or "")),
            )[:top_n]

        print(f"Applying investor flow to {len(targets)} theme file(s) ...")

        for t_idx, r in enumerate(targets, start=1):
            p = r.get("path")
            if (not p) or (not os.path.exists(p)):
                continue
            try:
                # 문자열로 읽어 dtype 깨짐 방지
                try:
                    df0 = pd.read_csv(p, encoding="utf-8-sig", dtype=str, keep_default_na=False)
                except Exception:
                    df0 = pd.read_csv(p, encoding="utf-8", dtype=str, keep_default_na=False)

                df1 = add_investor_flow_columns(
                    df0,
                    session=session,
                    cache=flow_cache,
                    delay=investor_flow_delay,
                    max_workers=investor_flow_max_workers,
                    jitter=investor_flow_jitter,
                )
                df1 = sort_df_for_save(df1, row_sort=row_sort, use_trade_value=ValueOrVolume)
                df1.to_csv(p, index=False, encoding="utf-8-sig")
                print(f"  [{t_idx}/{len(targets)}] flow updated: {os.path.basename(p)}")
            except Exception as e:
                print(f"  [{t_idx}/{len(targets)}] flow SKIP: {p} reason={e}")

    '''
    # 3.5) 추가: 거래대금 TOP3 row CSV 생성(필터링 후 남은 CSV만 대상으로)
    top3_path = build_top_trade_value_csv(
        theme_records=kept_records,
        out_dir=out_dir,
        top_n=3,
        output_filename="00_거래대금3등.csv",
    )
    if top3_path:
        print(f"Top3 거래대금 CSV saved: {top3_path}")
    '''
    # 4) 겹치는 종목 CSV 생성
    if make_overlap_csv:
        overlap_path = build_overlap_stocks_csv(
            kept_records,
            out_dir=out_dir,
            min_theme_overlap=overlap_min_themes,
            min_trade_value=overlap_min_trade_value,
            output_filename=overlap_csv_name,
            use_trade_value=ValueOrVolume,
        )
        if overlap_path:
            print(f"Overlap CSV saved: {overlap_path}")
    

from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+


if __name__ == "__main__":
    import argparse
    from datetime import datetime
    from zoneinfo import ZoneInfo

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--exclude",
        default="",
        help="콤마로 구분한 정규식(종목명 기준) 제외 패턴들. 예: '^삼성,SK하이닉스'",
    )
    args = parser.parse_args()
    exclude_patterns = [p.strip() for p in args.exclude.split(",") if p.strip()]

    date_tag = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d")
    out_dir = f"./domestic_stock/tema/{date_tag}"
    crawl_themes(
        pages=2,
        out_dir=out_dir,
        delay=(0.35, 0.9),
        clean_csv=True,
        quartile_filter=False,
        ValueOrVolume=True,
        row_sort= "changerate",
        make_overlap_csv=True,
        overlap_min_themes=2,
        overlap_min_trade_value=100000,
        overlap_csv_name="00_겹치는종목_2개이상테마.csv",
        exclude_patterns=exclude_patterns,
    )

