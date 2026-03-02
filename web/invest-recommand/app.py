import json
import os
from pathlib import Path
from flask import Flask, jsonify, send_from_directory, redirect, Response, request
from engine import (
    build_report,
    save_daily_snapshot,
    save_archive_entry,
    list_archived_picks,
    get_archived_pick,
    delete_archived_pick,
    list_snapshots,
    get_snapshot,
    list_snapshot_dates_by_month,
    get_current_change_vs_snapshot,
    refresh_universe_top300,
    get_universe_stats,
    clear_runtime_caches,
)
from theme_leader import (
    build_theme_leader_report,
    save_theme_leader_snapshot,
    get_theme_leader_snapshot,
    list_theme_leader_snapshots,
)
from theme_logic_kr import save_kr_theme_report
from pywebpush import WebPushException, webpush
import threading
import time
import uuid
from datetime import datetime, UTC
from zoneinfo import ZoneInfo
import urllib.request
import urllib.parse
import urllib.error
import yfinance as yf

app = Flask(__name__, static_folder="public")
KST = ZoneInfo("Asia/Seoul")
TEMA_WEB_V2_ORIGIN = "http://127.0.0.1:3010"

# lightweight in-memory cache for faster UI response
_REPORT_CACHE = {}
_REPORT_TTL_SEC = 60
_REPORT_PROGRESS = {}
_REPORT_LOCK = threading.Lock()
_CHART_CACHE = {}
_CHART_TTL_SEC = 300

VAPID_PUBLIC_KEY = "BPl-6O7KJvhPwqLM_P2XVpUgOJ9ojjYMaaHtBPUlz1m--u52HTchETpBES5iZG1zhizz_MLKbOI8Xq53rq-cQ0o"
VAPID_PRIVATE_KEY = "IOFjthCc8giC_JQRFiDNVh9C6H0-KEGEHbTiHf__6mQ"
_PUSH_SUBSCRIPTIONS_FILE = os.path.join(os.path.dirname(__file__), "push_subscriptions.json")
_PUSH_SUBSCRIPTIONS_LOCK = threading.Lock()


def _load_push_subscriptions():
    if not os.path.exists(_PUSH_SUBSCRIPTIONS_FILE):
        return []
    try:
        with open(_PUSH_SUBSCRIPTIONS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


_PUSH_SUBSCRIPTIONS = _load_push_subscriptions()


def _save_push_subscriptions():
    try:
        dir_path = os.path.dirname(_PUSH_SUBSCRIPTIONS_FILE)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(_PUSH_SUBSCRIPTIONS_FILE, "w", encoding="utf-8") as fh:
            json.dump(_PUSH_SUBSCRIPTIONS, fh, ensure_ascii=False, indent=2)
    except Exception:
        pass


def register_push_subscription(sub: dict) -> bool:
    if not sub or not isinstance(sub, dict):
        return False
    endpoint = sub.get("endpoint")
    if not endpoint:
        return False
    with _PUSH_SUBSCRIPTIONS_LOCK:
        for item in _PUSH_SUBSCRIPTIONS:
            if item.get("endpoint") == endpoint:
                item.clear()
                item.update(sub)
                _save_push_subscriptions()
                return True
        _PUSH_SUBSCRIPTIONS.append(sub)
        _save_push_subscriptions()
    return True


def _build_push_payload(report: dict) -> dict | None:
    if not report:
        return None
    top = report.get("topPick")
    if not top:
        return None
    symbol = str(top.get("symbol") or "").upper() or "투자상품"
    name = top.get("name") or ""
    score = top.get("score")
    expected = top.get("expectedReturnPct")
    score_txt = f"{score:.2f}" if isinstance(score, (int, float)) else "-"
    expected_txt = f"{expected:.2f}%" if isinstance(expected, (int, float)) else "-"
    market = report.get("market") or "all"
    limit = report.get("candidateLimit") or 300
    body = f"{symbol} ({name}) · 점수 {score_txt} · 예상수익 {expected_txt}"
    return {
        "title": "추천 분석 완료",
        "body": body,
        "icon": "/invest-recommend/favicon.ico",
        "tag": "invest-recommend-ready",
        "data": {
            "symbol": symbol,
            "market": market,
            "limit": limit,
            "url": f"/invest-recommend?market={market}&limit={limit}",
        },
    }


def _notify_push_subscribers(payload: dict) -> None:
    if not payload:
        return
    subs = []
    with _PUSH_SUBSCRIPTIONS_LOCK:
        subs = list(_PUSH_SUBSCRIPTIONS)
    if not subs:
        return
    failed_endpoints = []
    for sub in subs:
        try:
            webpush(
                subscription_info=sub,
                data=json.dumps(payload, ensure_ascii=False),
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": "mailto:alert@legendbarber.tailcaaac5.ts.net"},
            )
        except WebPushException as exc:
            if hasattr(exc, "response") and exc.response and exc.response.status_code in {404, 410}:
                failed_endpoints.append(sub.get("endpoint"))
        except Exception:
            pass
    if not failed_endpoints:
        return
    with _PUSH_SUBSCRIPTIONS_LOCK:
        before = len(_PUSH_SUBSCRIPTIONS)
        _PUSH_SUBSCRIPTIONS[:] = [s for s in _PUSH_SUBSCRIPTIONS if s.get("endpoint") not in failed_endpoints]
        if len(_PUSH_SUBSCRIPTIONS) != before:
            _save_push_subscriptions()


def _normalize_candidate_limit(v) -> int:
    try:
        n = int(v)
    except Exception:
        n = 300
    return n if n in {50, 100, 200, 300} else 300


def _parse_score_config_from_request() -> dict:
    preset = (request.args.get('scorePreset', default='default_6_4', type=str) or 'default_6_4').strip().lower()

    def _num(name: str, dv: float) -> float:
        try:
            return float(request.args.get(name, default=dv, type=float))
        except Exception:
            return dv

    return {
        'preset': preset,
        'components': {
            'stock': _num('wStock', 0.8),
            'theme': _num('wTheme', 0.0),
            'news': _num('wNews', 0.0),
            'technical': _num('wTechnical', 0.2),
        },
        'subcomponents': {
            'stock': {
                'reportConsensus': _num('swStockReport', 1.0),
                'momentum': _num('swStockMomentum', 0.0),
                'liquidity': _num('swStockLiquidity', 0.0),
                'risk': _num('swStockRisk', 0.0),
            },
            'theme': {
                'blended': _num('swThemeBlended', 1.0),
                'themeScore': _num('swThemeScore', 0.0),
                'leaderScore': _num('swThemeLeader', 0.0),
            },
            'news': {
                'crowdScore': _num('swNewsCrowd', 1.0),
                'headlineCount': _num('swNewsHeadline', 0.0),
                'tone': _num('swNewsTone', 0.0),
            },
            'technical': {
                'technicalScore': _num('swTechScore', 1.0),
                'momentumTrend': _num('swTechTrend', 0.0),
                'riskScore': _num('swTechRisk', 0.0),
            },
        },
        'confidence': _num('wConfidence', 0.10),
        'valuation': _num('wValuation', 0.20),
    }


def _score_config_key(score_config: dict) -> str:
    sc = score_config or {}
    c = sc.get('components') or {}
    sub = sc.get('subcomponents') or {}
    s_stock = sub.get('stock') or {}
    s_theme = sub.get('theme') or {}
    s_news = sub.get('news') or {}
    s_tech = sub.get('technical') or {}
    return (
        f"{sc.get('preset', 'default_6_4')}"
        f":{float(c.get('stock', 0.0)):.4f}"
        f":{float(c.get('theme', 0.0)):.4f}"
        f":{float(c.get('news', 0.0)):.4f}"
        f":{float(c.get('technical', 0.0)):.4f}"
        f":{float(sc.get('confidence', 0.0)):.4f}"
        f":{float(sc.get('valuation', 0.0)):.4f}"
        f":{float(s_stock.get('reportConsensus', 0.0)):.3f}"
        f":{float(s_stock.get('momentum', 0.0)):.3f}"
        f":{float(s_stock.get('liquidity', 0.0)):.3f}"
        f":{float(s_stock.get('risk', 0.0)):.3f}"
        f":{float(s_theme.get('blended', 0.0)):.3f}"
        f":{float(s_theme.get('themeScore', 0.0)):.3f}"
        f":{float(s_theme.get('leaderScore', 0.0)):.3f}"
        f":{float(s_news.get('crowdScore', 0.0)):.3f}"
        f":{float(s_news.get('headlineCount', 0.0)):.3f}"
        f":{float(s_news.get('tone', 0.0)):.3f}"
        f":{float(s_tech.get('technicalScore', 0.0)):.3f}"
        f":{float(s_tech.get('momentumTrend', 0.0)):.3f}"
        f":{float(s_tech.get('riskScore', 0.0)):.3f}"
    )


def _report_key(market: str, candidate_limit: int, score_config: dict) -> str:
    return f"{market}:{candidate_limit}:{_score_config_key(score_config)}"


def _snapshot_worker():
    """매일 KST 20:00~20:04 사이 1회 스냅샷 저장."""
    last_saved_day = None
    while True:
        try:
            now = datetime.now(KST)
            today = now.strftime("%Y-%m-%d")
            if now.hour == 20 and now.minute <= 4 and last_saved_day != today:
                save_daily_snapshot(force=False)
                last_saved_day = today
        except Exception:
            pass
        time.sleep(60)


threading.Thread(target=_snapshot_worker, daemon=True).start()


def _run_report_job(key: str, market: str, candidate_limit: int, score_config: dict, task_id: str):
    def _progress_cb(done: int, total: int, symbol: str):
        with _REPORT_LOCK:
            st = _REPORT_PROGRESS.get(key, {})
            if st.get("taskId") != task_id:
                return
            st.update({
                "status": "running",
                "done": done,
                "total": total,
                "symbol": symbol,
                "progressPct": round((done / total) * 100, 2) if total else 0.0,
                "updatedAt": datetime.now(KST).isoformat(),
            })
            _REPORT_PROGRESS[key] = st

    try:
        clear_runtime_caches()
        data = build_report(market=market, candidate_limit=candidate_limit, progress_cb=_progress_cb, score_config=score_config)
        with _REPORT_LOCK:
            _REPORT_CACHE[key] = {"ts": time.time(), "data": data}
            st = _REPORT_PROGRESS.get(key, {})
            st.update({
                "status": "done",
                "done": st.get("total", 0),
                "progressPct": 100.0,
                "updatedAt": datetime.now(KST).isoformat(),
                "error": None,
            })
            _REPORT_PROGRESS[key] = st
        payload = _build_push_payload(data)
        if payload:
            _notify_push_subscribers(payload)
    except Exception as e:
        with _REPORT_LOCK:
            st = _REPORT_PROGRESS.get(key, {})
            st.update({
                "status": "error",
                "error": str(e),
                "updatedAt": datetime.now(KST).isoformat(),
            })
            _REPORT_PROGRESS[key] = st


@app.get('/api/report')
def api_report():
    market = (request.args.get('market', default='all', type=str) or 'all').lower()
    if market not in {'all', 'kr', 'us'}:
        market = 'all'
    candidate_limit = _normalize_candidate_limit(request.args.get('limit', default=300, type=int))
    score_config = _parse_score_config_from_request()
    key = _report_key(market, candidate_limit, score_config)

    st = _REPORT_PROGRESS.get(key)
    if st and st.get("status") == "running":
        return jsonify({"status": "running", "market": market, "limit": candidate_limit, "progress": st}), 202

    cached = _REPORT_CACHE.get(key)
    if cached and cached.get('data') is not None:
        return jsonify(cached['data'])

    return jsonify({"status": "idle", "market": market, "limit": candidate_limit, "message": "no_cached_report"}), 404


@app.get('/api/report/refresh')
def api_report_refresh():
    market = (request.args.get('market', default='all', type=str) or 'all').lower()
    if market not in {'all', 'kr', 'us'}:
        market = 'all'
    candidate_limit = _normalize_candidate_limit(request.args.get('limit', default=300, type=int))
    score_config = _parse_score_config_from_request()
    key = _report_key(market, candidate_limit, score_config)

    with _REPORT_LOCK:
        st = _REPORT_PROGRESS.get(key)
        if st and st.get("status") == "running":
            return jsonify({"status": "running", "market": market, "limit": candidate_limit, "progress": st}), 202

        task_id = str(uuid.uuid4())
        _REPORT_PROGRESS[key] = {
            "taskId": task_id,
            "status": "running",
            "done": 0,
            "total": 0,
            "symbol": None,
            "progressPct": 0.0,
            "startedAt": datetime.now(KST).isoformat(),
            "updatedAt": datetime.now(KST).isoformat(),
            "error": None,
        }
        threading.Thread(target=_run_report_job, args=(key, market, candidate_limit, score_config, task_id), daemon=True).start()

    return jsonify({"status": "running", "market": market, "limit": candidate_limit, "progress": _REPORT_PROGRESS.get(key)}), 202


@app.get('/api/report/progress')
def api_report_progress():
    market = (request.args.get('market', default='all', type=str) or 'all').lower()
    if market not in {'all', 'kr', 'us'}:
        market = 'all'
    candidate_limit = _normalize_candidate_limit(request.args.get('limit', default=300, type=int))
    score_config = _parse_score_config_from_request()
    key = _report_key(market, candidate_limit, score_config)

    st = _REPORT_PROGRESS.get(key)
    if not st:
        return jsonify({"status": "idle", "market": market, "limit": candidate_limit})
    return jsonify({"status": st.get("status", "idle"), "market": market, "limit": candidate_limit, "progress": st})


@app.get('/api/archive')
def api_archive_list():
    return jsonify({"items": list_archived_picks()})


@app.get('/api/archive/<symbol>')
def api_archive_detail(symbol: str):
    if not symbol:
        return jsonify({"ok": False, "error": "invalid_symbol"}), 400
    item = get_archived_pick(symbol)
    if not item:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, "item": item})


@app.delete('/api/archive/<symbol>')
def api_archive_delete(symbol: str):
    if not symbol:
        return jsonify({"ok": False, "error": "invalid_symbol"}), 400
    ok = delete_archived_pick(symbol)
    if not ok:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, "symbol": str(symbol).upper().strip()})


@app.post('/api/notifications/subscribe')
def api_notifications_subscribe():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"ok": False, "error": "invalid_payload"}), 400
    sub = payload.get("subscription")
    if not isinstance(sub, dict) or not sub.get("endpoint"):
        return jsonify({"ok": False, "error": "invalid_subscription"}), 400
    register_push_subscription(sub)
    return jsonify({"ok": True, "publicKey": VAPID_PUBLIC_KEY})


@app.get('/api/universe/stats')
def api_universe_stats():
    return jsonify(get_universe_stats())


@app.post('/api/universe/update')
def api_universe_update():
    # 시가총액 상위 300 파일 재생성 + 메모리 유니버스 재로드
    data = refresh_universe_top300()

    # 기존 캐시 무효화
    with _REPORT_LOCK:
        _REPORT_CACHE.clear()
        _REPORT_PROGRESS.clear()

    return jsonify(data)


@app.get('/api/symbol/<symbol>/detail')
def api_symbol_detail(symbol: str):
    sym = (symbol or '').upper().strip()
    if not sym:
        return jsonify({"error": "invalid_symbol"}), 400

    # 최신 캐시부터 탐색해 해당 종목 상세 반환
    items = sorted(_REPORT_CACHE.items(), key=lambda kv: kv[1].get('ts', 0), reverse=True)
    for key, cached in items:
        data = cached.get('data') or {}
        rankings = data.get('rankings') or []
        for r in rankings:
            if str(r.get('symbol', '')).upper() == sym:
                return jsonify({
                    "ok": True,
                    "cacheKey": key,
                    "market": data.get('market'),
                    "candidateLimit": data.get('candidateLimit'),
                    "generatedAt": data.get('generatedAt'),
                    "item": r,
                })

    return jsonify({"ok": False, "error": "not_found_in_cache", "symbol": sym}), 404


@app.get('/api/snapshot/save')
def api_snapshot_save():
    return jsonify(save_daily_snapshot(force=False))


@app.get('/api/snapshot/force')
def api_snapshot_force():
    return jsonify(save_daily_snapshot(force=True))


@app.get('/api/snapshots')
def api_snapshots():
    return jsonify({"items": list_snapshots(limit=365)})


@app.get('/api/snapshots/month/<ym>')
def api_snapshots_by_month(ym: str):
    return jsonify({"month": ym, "dates": list_snapshot_dates_by_month(ym)})


@app.get('/api/snapshots/<date_kst>')
def api_snapshot_by_date(date_kst: str):
    data = get_snapshot(date_kst)
    if not data:
        return jsonify({"error": "not_found", "dateKST": date_kst}), 404
    return jsonify(data)


@app.get('/api/snapshots/<date_kst>/performance')
def api_snapshot_performance(date_kst: str):
    data = get_current_change_vs_snapshot(date_kst)
    if data.get("error") == "not_found":
        return jsonify(data), 404
    return jsonify(data)


@app.get('/api/theme-leaders')
def api_theme_leaders():
    limit = request.args.get('limit', default=12, type=int) or 12
    pick = request.args.get('pick', default=2, type=int) or 2
    try:
        return jsonify(build_theme_leader_report(limit_themes=max(3, min(limit, 30)), per_theme_pick=max(1, min(pick, 5))))
    except Exception as e:
        return jsonify({"error": "theme_leader_unavailable", "message": str(e)}), 502


@app.get('/api/theme-leaders/save')
def api_theme_leaders_save():
    limit = request.args.get('limit', default=12, type=int) or 12
    pick = request.args.get('pick', default=2, type=int) or 2
    force = str(request.args.get('force', '0')).lower() in {'1', 'true', 'yes', 'y'}
    try:
        return jsonify(save_theme_leader_snapshot(force=force, limit_themes=max(3, min(limit, 30)), per_theme_pick=max(1, min(pick, 5))))
    except Exception as e:
        return jsonify({"error": "theme_leader_save_failed", "message": str(e)}), 502


@app.get('/api/theme-leaders/snapshots')
def api_theme_leaders_snapshots():
    limit = request.args.get('limit', default=60, type=int) or 60
    return jsonify({"items": list_theme_leader_snapshots(limit=max(1, min(limit, 365)))})


@app.get('/api/theme-leaders/snapshots/<date>')
def api_theme_leaders_snapshot_by_date(date: str):
    data = get_theme_leader_snapshot(date)
    if not data:
        return jsonify({"error": "not_found", "date": date}), 404
    return jsonify(data)


@app.get('/')
def home():
    return """
    <html><body style='font-family:Arial;padding:24px;background:#0b1220;color:#e5e7eb'>
      <h2>legendbarber Web Hub</h2>
      <p>이 주소를 앞으로 모든 웹서버의 메인 허브로 사용합니다.</p>

      <h3 style='margin-top:22px'>📁 /invest-recommend</h3>
      <ul>
        <li><a style='color:#93c5fd' href='/invest-recommend'>/invest-recommend</a> (투자 추천: KR+US)</li>
        <li><a style='color:#93c5fd' href='/invest-recommend-us'>/invest-recommend-us</a> (미국주식 추천)</li>
        <li><a style='color:#93c5fd' href='/invest-recommend-kr'>/invest-recommend-kr</a> (한국주식 추천)</li>
      </ul>

      <h3 style='margin-top:22px'>📁 /theme</h3>
      <ul>
        <li><a style='color:#93c5fd' href='/tema-web-v2'>/tema-web-v2</a> (테마주 업그레이드 v2)</li>
        <li><a style='color:#93c5fd' href='/theme-leaders'>/theme-leaders</a> (당일 주도테마/주도주 탐색)</li>
        <li><a style='color:#93c5fd' href='/theme-now-kr'>/theme-now-kr</a> (한국주식 테마 실시간 스코어보드)</li>
      </ul>

      <h3 style='margin-top:22px'>📁 /game</h3>
      <ul>
        <li><a style='color:#93c5fd' href='/game-demo'>/game-demo</a> (스와이프 게임 데모 v1)</li>
        <li><a style='color:#93c5fd' href='/game-demo-v2'>/game-demo-v2</a> (퍼즐 머지 데모 v2)</li>
        <li><a style='color:#93c5fd' href='/game-foldlight'>/game-foldlight</a> (독창 퍼즐 Foldlight 프로토)</li>
        <li><a style='color:#93c5fd' href='/game-tap-lights'>/game-tap-lights</a> (직관형 탭 퍼즐 신작)</li>
        <li><a style='color:#93c5fd' href='/game-tap-burst'>/game-tap-burst</a> (초캐주얼 탭 버스트)</li>
        <li><a style='color:#93c5fd' href='/game-one-line-shift'>/game-one-line-shift</a> (독창 퍼즐: 한 줄만 바꾸기)</li>
      </ul>
    </body></html>
    """


@app.get('/invest-recommend')
def invest_recommend_page():
    return send_from_directory(app.static_folder, 'index.html')


@app.get('/invest-recommend/archive')
def invest_archive_page():
    return send_from_directory(app.static_folder, 'archive.html')


@app.get('/invest-recommend-us')
def invest_recommend_us_page():
    return redirect('/invest-recommend?market=us', code=302)


@app.get('/invest-recommend-kr')
def invest_recommend_kr_page():
    return redirect('/invest-recommend?market=kr', code=302)


@app.get('/invest-recommend/history')
def invest_history_page():
    return send_from_directory(f"{app.static_folder}/invest-history", 'index.html')


@app.get('/invest-recommend/history/<path:filename>')
def invest_history_assets(filename):
    return send_from_directory(f"{app.static_folder}/invest-history", filename)


@app.get('/invest-recommend/symbol/<symbol>')
def invest_symbol_detail_page(symbol: str):
    return send_from_directory(app.static_folder, 'symbol-detail.html')


@app.get('/invest-recommend/ui-candidates')
def invest_ui_candidates_index():
    return send_from_directory(f"{app.static_folder}/ui-candidates", 'index.html')


@app.get('/invest-recommend/ui-candidates/<name>')
def invest_ui_candidates_page(name: str):
    fname = f"{name}.html" if not name.endswith('.html') else name
    return send_from_directory(f"{app.static_folder}/ui-candidates", fname)


@app.get('/invest-recommend/sw-notify.js')
def invest_sw_notify_js():
    return send_from_directory(app.static_folder, 'sw-notify.js')


@app.get('/theme-leaders')
def theme_leaders_page():
    return send_from_directory(app.static_folder, 'theme-leaders.html')


@app.get('/theme-leaders/<date>')
def theme_leaders_page_by_date(date: str):
    # 하위 URL 파싱: /theme-leaders/260219 형태
    return send_from_directory(app.static_folder, 'theme-leaders.html')


@app.get('/theme-leaders/calendar')
def theme_leaders_calendar_page():
    return send_from_directory(app.static_folder, 'theme-leaders-calendar.html')


@app.get('/theme-now')
def theme_now_page():
    return send_from_directory(app.static_folder, 'theme-now.html')


@app.get('/theme-now-kr')
def theme_now_kr_page():
    return send_from_directory(app.static_folder, 'theme-now-kr.html')


@app.get('/api/theme-now-kr/refresh')
def api_theme_now_kr_refresh():
    try:
        data = save_kr_theme_report()
        return jsonify({"ok": True, "generatedAt": data.get("generatedAt")})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 500


def _fetch_chart_data(symbol: str, period: str = "6mo", interval: str = "1d", force_refresh: bool = False) -> tuple[dict, int]:
    key = f"{symbol}|{period}|{interval}"
    now = time.time()
    cached = _CHART_CACHE.get(key)
    if (not force_refresh) and cached and (now - cached.get("ts", 0) <= _CHART_TTL_SEC):
        return cached["data"], 200

    try:
        hist = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=True)
        if hist is None or hist.empty:
            hist = yf.download(tickers=symbol, period=period, interval=interval, auto_adjust=True, progress=False, threads=False)
            if hist is None or hist.empty:
                return {"ok": False, "message": "no_data", "symbol": symbol}, 404
            if 'Close' not in hist:
                if hasattr(hist, 'columns') and getattr(hist.columns, 'nlevels', 1) > 1 and symbol in hist.columns.get_level_values(0):
                    hist = hist[symbol]

        if 'Close' not in hist:
            return {"ok": False, "message": "close_not_found", "symbol": symbol}, 404

        for col in ['Open', 'High', 'Low', 'Close']:
            if col not in hist:
                return {"ok": False, "message": f"{col.lower()}_not_found", "symbol": symbol}, 404

        cols = ['Open', 'High', 'Low', 'Close'] + (['Volume'] if 'Volume' in hist else [])
        ohlcv = hist[cols].dropna(subset=['Open', 'High', 'Low', 'Close'])
        if ohlcv.empty:
            return {"ok": False, "message": "no_ohlc", "symbol": symbol}, 404

        labels = [idx.strftime('%Y-%m-%d') for idx in ohlcv.index]
        open_ = [round(float(v), 4) for v in ohlcv['Open'].tolist()]
        high = [round(float(v), 4) for v in ohlcv['High'].tolist()]
        low = [round(float(v), 4) for v in ohlcv['Low'].tolist()]
        close = [round(float(v), 4) for v in ohlcv['Close'].tolist()]
        volume = [int(float(v)) for v in ohlcv['Volume'].fillna(0).tolist()] if 'Volume' in ohlcv else []

        data = {
            "ok": True,
            "symbol": symbol,
            "period": period,
            "interval": interval,
            "labels": labels,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
        _CHART_CACHE[key] = {"ts": now, "data": data}
        return data, 200
    except Exception as e:
        return {"ok": False, "message": str(e), "symbol": symbol}, 500


@app.get('/api/chart/<symbol>')
def api_chart_symbol(symbol: str):
    period = request.args.get('period', default='6mo', type=str) or '6mo'
    interval = request.args.get('interval', default='1d', type=str) or '1d'
    force_refresh = str(request.args.get('refresh', '0')).lower() in {'1', 'true', 'yes', 'y'}
    data, status = _fetch_chart_data(symbol, period, interval, force_refresh)
    return jsonify(data), status


@app.post('/api/archive')
def api_archive_save():
    payload = request.get_json(silent=True) or {}
    symbol = str(payload.get('symbol') or '').upper().strip()
    if not symbol:
        return jsonify({'ok': False, 'error': 'invalid_symbol'}), 400
    market = (str(payload.get('market') or 'all') or 'all').lower()
    if market not in {'all', 'kr', 'us'}:
        market = 'all'
    candidate_limit = _normalize_candidate_limit(payload.get('limit', 300))
    score_config = payload.get('scoreConfig') if isinstance(payload.get('scoreConfig'), dict) else {'preset': 'default_6_4'}
    key = _report_key(market, candidate_limit, score_config)
    cached = _REPORT_CACHE.get(key)

    # 저장은 반드시 "현재 화면의 market/limit/scoreConfig와 정확히 일치하는 리포트"에서만 허용
    # (fallback 허용 시 사용자가 의도하지 않은 결과셋 기준으로 저장될 수 있음)
    if not cached or not cached.get('data'):
        return jsonify({'ok': False, 'error': 'report_not_ready_for_current_config'}), 404

    report = cached['data']
    item = None
    if report.get('topPick') and str(report['topPick'].get('symbol') or '').upper() == symbol:
        item = report['topPick']
    else:
        for r in report.get('rankings') or []:
            if str(r.get('symbol') or '').upper() == symbol:
                item = r
                break
    if not item:
        return jsonify({'ok': False, 'error': 'symbol_not_in_report'}), 404
    entry = {
        'symbol': symbol,
        'name': item.get('name'),
        'category': item.get('category'),
        'score': item.get('score'),
        'expectedReturnPct': item.get('expectedReturnPct'),
        'riskReward': item.get('riskReward'),
        'currentPrice': item.get('currentPrice'),
        'generatedAt': report.get('generatedAt'),
        'market': report.get('market') or market,
        'candidateLimit': report.get('candidateLimit') or candidate_limit,
        'methodology': report.get('methodology'),
        'plan': item.get('plan') or {},
        'components': item.get('components') or {},
        'links': item.get('links') or {},
    }
    chart_period = str(payload.get('chartPeriod') or '1y')
    chart_interval = str(payload.get('chartInterval') or '1d')
    chart, chart_status = _fetch_chart_data(symbol, chart_period, chart_interval, True)
    entry['chart'] = chart
    entry['chartPeriod'] = chart_period
    entry['chartInterval'] = chart_interval
    entry['chartFetchedAt'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
    save_archive_entry(entry)
    return jsonify({'ok': True, 'item': entry, 'chartStatus': chart_status})


@app.get('/api/archive/<symbol>/chart')
def api_archive_chart(symbol: str):
    item = get_archived_pick(symbol)
    if not item:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({
        'ok': True,
        'chart': item.get('chart'),
        'chartPeriod': item.get('chartPeriod'),
        'chartInterval': item.get('chartInterval'),
        'chartFetchedAt': item.get('chartFetchedAt'),
    })


@app.post('/api/archive/<symbol>/chart/refresh')
def api_archive_chart_refresh(symbol: str):
    item = get_archived_pick(symbol)
    if not item:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    period = request.args.get('period') or item.get('chartPeriod') or '1y'
    interval = request.args.get('interval') or item.get('chartInterval') or '1d'
    chart, status = _fetch_chart_data(symbol, period, interval, True)
    updated = item.copy()
    updated['chart'] = chart
    updated['chartPeriod'] = period
    updated['chartInterval'] = interval
    updated['chartFetchedAt'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
    save_archive_entry(updated)
    return jsonify({'ok': chart.get('ok', False), 'chart': chart, 'chartPeriod': period, 'chartInterval': interval}), status


# invest-recommend 하위 캘린더 경로
@app.get('/invest-recommend/calendar')
def invest_calendar_page_nested():
    return send_from_directory(f"{app.static_folder}/invest-history", 'index.html')


@app.get('/invest-recommend/calendar/<path:filename>')
def invest_calendar_assets_nested(filename):
    return send_from_directory(f"{app.static_folder}/invest-history", filename)


# 기존 경로 호환
@app.get('/invest-history')
def invest_history_root_page():
    return send_from_directory(f"{app.static_folder}/invest-history", 'index.html')


@app.get('/invest-history/<path:filename>')
def invest_history_root_assets(filename):
    return send_from_directory(f"{app.static_folder}/invest-history", filename)


@app.get('/calendar')
def invest_calendar_page():
    return redirect('/invest-recommend/calendar', code=302)


@app.get('/calendar/<path:filename>')
def invest_calendar_assets(filename):
    return redirect(f'/invest-recommend/calendar/{filename}', code=302)


# backward compatibility
def _proxy_to_tema_v2(subpath: str = ""):
    target = f"{TEMA_WEB_V2_ORIGIN}/{subpath.lstrip('/')}"
    qs = request.query_string.decode('utf-8', errors='ignore')
    if qs:
        target = f"{target}?{qs}"

    method = request.method.upper()
    body = request.get_data() if method in {"POST", "PUT", "PATCH", "DELETE"} else None

    headers = {"User-Agent": request.headers.get("User-Agent", "Mozilla/5.0")}
    ct = request.headers.get("Content-Type")
    if ct:
        headers["Content-Type"] = ct

    req = urllib.request.Request(target, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body_bytes = resp.read()
            content_type = resp.headers.get("Content-Type", "text/html; charset=utf-8")
            out = Response(body_bytes, status=resp.status, content_type=content_type)
            # subpath deploy를 위한 base href 주입 (html에만)
            if "text/html" in content_type.lower():
                text = body_bytes.decode('utf-8', errors='ignore')
                if '<head>' in text and 'base href="/tema-web-v2/"' not in text:
                    text = text.replace('<head>', '<head><base href="/tema-web-v2/">', 1)
                out = Response(text, status=resp.status, content_type=content_type)

            # tema-web-v2는 캐시 고정으로 인한 갱신 문제를 피하기 위해 no-store 강제
            out.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            out.headers["Pragma"] = "no-cache"
            out.headers["Expires"] = "0"
            return out
    except urllib.error.HTTPError as e:
        return Response(e.read(), status=e.code, content_type=e.headers.get("Content-Type", "text/plain"))
    except Exception as e:
        return Response(f"temaWeb-v2 backend unavailable: {e}", status=502, content_type="text/plain; charset=utf-8")


@app.route('/tema-web-v2', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'])
def tema_web_v2_root():
    return _proxy_to_tema_v2('')


@app.route('/tema-web-v2/<path:subpath>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'])
def tema_web_v2_subpath(subpath: str):
    return _proxy_to_tema_v2(subpath)


@app.get('/invest-recommand')
def invest_recommand_alias():
    return send_from_directory(app.static_folder, 'index.html')


@app.get('/game-demo')
def game_demo_redirect():
    return redirect('/game-demo/', code=302)


@app.get('/game-demo/')
def game_demo_page():
    return send_from_directory(f"{app.static_folder}/game-demo", 'index.html')


@app.get('/game-demo/<path:filename>')
def game_demo_assets(filename):
    return send_from_directory(f"{app.static_folder}/game-demo", filename)


@app.get('/game-demo-v2')
def game_demo_v2_redirect():
    return redirect('/game-demo-v2/', code=302)


@app.get('/game-demo-v2/')
def game_demo_v2_page():
    return send_from_directory(f"{app.static_folder}/game-demo-v2", 'index.html')


@app.get('/game-demo-v2/<path:filename>')
def game_demo_v2_assets(filename):
    return send_from_directory(f"{app.static_folder}/game-demo-v2", filename)


@app.get('/game-foldlight')
def game_foldlight_redirect():
    return redirect('/game-foldlight/', code=302)


@app.get('/game-foldlight/')
def game_foldlight_page():
    return send_from_directory(f"{app.static_folder}/game-foldlight", 'index.html')


@app.get('/game-foldlight/<path:filename>')
def game_foldlight_assets(filename):
    return send_from_directory(f"{app.static_folder}/game-foldlight", filename)


@app.get('/game-tap-lights')
def game_tap_lights_redirect():
    return redirect('/game-tap-lights/', code=302)


@app.get('/game-tap-lights/')
def game_tap_lights_page():
    return send_from_directory(f"{app.static_folder}/game-tap-lights", 'index.html')


@app.get('/game-tap-lights/<path:filename>')
def game_tap_lights_assets(filename):
    return send_from_directory(f"{app.static_folder}/game-tap-lights", filename)


@app.get('/game-tap-burst')
def game_tap_burst_redirect():
    return redirect('/game-tap-burst/', code=302)


@app.get('/game-tap-burst/')
def game_tap_burst_page():
    return send_from_directory(f"{app.static_folder}/game-tap-burst", 'index.html')


@app.get('/game-tap-burst/<path:filename>')
def game_tap_burst_assets(filename):
    return send_from_directory(f"{app.static_folder}/game-tap-burst", filename)


@app.get('/game-one-line-shift')
def game_one_line_shift_redirect():
    return redirect('/game-one-line-shift/', code=302)


@app.get('/game-one-line-shift/')
def game_one_line_shift_page():
    return send_from_directory(f"{app.static_folder}/game-one-line-shift", 'index.html')


@app.get('/game-one-line-shift/<path:filename>')
def game_one_line_shift_assets(filename):
    return send_from_directory(f"{app.static_folder}/game-one-line-shift", filename)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=False)
