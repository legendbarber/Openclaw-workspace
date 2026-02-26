from flask import Flask, jsonify, send_from_directory, redirect, Response, request
from engine import (
    build_report,
    save_daily_snapshot,
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
import threading
import time
import uuid
from datetime import datetime
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


def _normalize_candidate_limit(v) -> int:
    try:
        n = int(v)
    except Exception:
        n = 300
    return n if n in {50, 100, 200, 300} else 300


def _report_key(market: str, candidate_limit: int) -> str:
    return f"{market}:{candidate_limit}"


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


def _run_report_job(key: str, market: str, candidate_limit: int, task_id: str):
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
        data = build_report(market=market, candidate_limit=candidate_limit, progress_cb=_progress_cb)
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
    key = _report_key(market, candidate_limit)

    cached = _REPORT_CACHE.get(key)
    if cached and cached.get('data') is not None:
        return jsonify(cached['data'])

    st = _REPORT_PROGRESS.get(key)
    if st and st.get("status") == "running":
        return jsonify({"status": "running", "market": market, "limit": candidate_limit, "progress": st}), 202

    return jsonify({"status": "idle", "market": market, "limit": candidate_limit, "message": "no_cached_report"}), 404


@app.get('/api/report/refresh')
def api_report_refresh():
    market = (request.args.get('market', default='all', type=str) or 'all').lower()
    if market not in {'all', 'kr', 'us'}:
        market = 'all'
    candidate_limit = _normalize_candidate_limit(request.args.get('limit', default=300, type=int))
    key = _report_key(market, candidate_limit)

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
        threading.Thread(target=_run_report_job, args=(key, market, candidate_limit, task_id), daemon=True).start()

    return jsonify({"status": "running", "market": market, "limit": candidate_limit, "progress": _REPORT_PROGRESS.get(key)}), 202


@app.get('/api/report/progress')
def api_report_progress():
    market = (request.args.get('market', default='all', type=str) or 'all').lower()
    if market not in {'all', 'kr', 'us'}:
        market = 'all'
    candidate_limit = _normalize_candidate_limit(request.args.get('limit', default=300, type=int))
    key = _report_key(market, candidate_limit)

    st = _REPORT_PROGRESS.get(key)
    if not st:
        return jsonify({"status": "idle", "market": market, "limit": candidate_limit})
    return jsonify({"status": st.get("status", "idle"), "market": market, "limit": candidate_limit, "progress": st})


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


@app.get('/api/chart/<symbol>')
def api_chart_symbol(symbol: str):
    period = request.args.get('period', default='6mo', type=str) or '6mo'
    interval = request.args.get('interval', default='1d', type=str) or '1d'
    key = f"{symbol}|{period}|{interval}"
    now = time.time()

    cached = _CHART_CACHE.get(key)
    if cached and (now - cached.get("ts", 0) <= _CHART_TTL_SEC):
        return jsonify(cached["data"])

    try:
        hist = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=True)
        if hist is None or hist.empty:
            # fallback path (sometimes history intermittently fails)
            hist = yf.download(tickers=symbol, period=period, interval=interval, auto_adjust=True, progress=False, threads=False)
            if hist is None or hist.empty:
                return jsonify({"ok": False, "message": "no_data", "symbol": symbol}), 404
            if 'Close' not in hist:
                if hasattr(hist, 'columns') and getattr(hist.columns, 'nlevels', 1) > 1 and symbol in hist.columns.get_level_values(0):
                    hist = hist[symbol]

        if 'Close' not in hist:
            return jsonify({"ok": False, "message": "close_not_found", "symbol": symbol}), 404

        for col in ['Open', 'High', 'Low', 'Close']:
            if col not in hist:
                return jsonify({"ok": False, "message": f"{col.lower()}_not_found", "symbol": symbol}), 404

        cols = ['Open', 'High', 'Low', 'Close'] + (['Volume'] if 'Volume' in hist else [])
        ohlcv = hist[cols].dropna(subset=['Open', 'High', 'Low', 'Close'])
        if ohlcv.empty:
            return jsonify({"ok": False, "message": "no_ohlc", "symbol": symbol}), 404

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
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok": False, "message": str(e), "symbol": symbol}), 500


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
