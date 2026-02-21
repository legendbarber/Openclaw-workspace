from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from zoneinfo import ZoneInfo
import importlib.util


@dataclass
class RefreshResult:
    date_tag: str
    out_dir: str
    seconds: float
    files: int


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "")
    if raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _load_crawler_module(crawler_path: Path):
    spec = importlib.util.spec_from_file_location("tema_crawler", str(crawler_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"crawler 모듈 로드 실패: {crawler_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    return module


def run_once(
    tema_root: Optional[str] = None,
    pages: int = 2,
    exclude: str = "",  # 기본: 아무것도 제외하지 않음(=항상 모든 종목 데이터)
) -> Dict[str, Any]:
    """
    테마 CSV 생성(오늘 날짜 폴더에 저장)

    IMPORTANT:
      - exclude 기본값을 비워서, 크롤링 단계에서는 항상 모든 종목을 저장한다.
      - '삼성전자/하이닉스 제외'는 서버(API)에서 ON/OFF하며,
        테마 상위 선정(Top4)도 그 토글에 따라 다시 계산한다.
    """
    root = Path(tema_root or os.environ.get("TEMA_ROOT", r"C:\project\04.app\temaWeb\tema"))
    root.mkdir(parents=True, exist_ok=True)

    crawler_path = Path(os.environ.get("CRAWLER_PATH", str(Path(__file__).parent / "crawler" / "01today_tema.py")))
    if not crawler_path.exists():
        raise FileNotFoundError(f"CRAWLER_PATH가 가리키는 파일이 없습니다: {crawler_path}")

    mod = _load_crawler_module(crawler_path)

    # yymmdd (KST)
    date_tag = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d")
    out_dir = root / date_tag
    out_dir.mkdir(parents=True, exist_ok=True)

    # env로도 오버라이드 가능
    exclude_arg = (exclude or "").strip()
    if not exclude_arg:
        exclude_arg = os.environ.get("CRAWL_EXCLUDE", "").strip()
    exclude_patterns = [p.strip() for p in exclude_arg.split(",") if p.strip()]

    t0 = time.time()

    if not hasattr(mod, "crawl_themes"):
        raise RuntimeError("crawler 파일에 crawl_themes 함수가 없습니다. (01today_tema.py 확인)")

    mod.crawl_themes(
        pages=int(os.environ.get("CRAWL_PAGES", str(pages))),
        out_dir=str(out_dir),
        delay=(0.35, 0.9),
        clean_csv=True,
        # 동적 Top4 계산(삼성/하이닉스 제외 ON/OFF)을 위해 기본은 전체 테마 파일 보존
        quartile_filter=_env_bool("CRAWL_QUARTILE_FILTER", False),
        ValueOrVolume=_env_bool("CRAWL_USE_TRADE_VALUE", True),
        row_sort=os.environ.get("CRAWL_ROW_SORT", "changerate"),
        make_overlap_csv=True,
        overlap_min_themes=int(os.environ.get("OVERLAP_MIN_THEMES", "2")),
        overlap_min_trade_value=int(os.environ.get("OVERLAP_MIN_TRADE_VALUE", "100000")),
        overlap_csv_name=os.environ.get("OVERLAP_CSV_NAME", "00_겹치는종목_2개이상테마.csv"),
        exclude_patterns=exclude_patterns,
    )

    secs = time.time() - t0
    files = len(list(out_dir.glob("*.csv")))

    return RefreshResult(date_tag=date_tag, out_dir=str(out_dir), seconds=secs, files=files).__dict__
