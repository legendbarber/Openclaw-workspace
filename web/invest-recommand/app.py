from flask import Flask, jsonify, send_from_directory, redirect, Response, request
from engine import (
    build_report,
    save_daily_snapshot,
    list_snapshots,
    get_snapshot,
    list_snapshot_dates_by_month,
    get_current_change_vs_snapshot,
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
from datetime import datetime
from zoneinfo import ZoneInfo
import urllib.request
import urllib.parse
import urllib.error

app = Flask(__name__, static_folder="public")
KST = ZoneInfo("Asia/Seoul")
TEMA_WEB_V2_ORIGIN = "http://127.0.0.1:3010"

# lightweight in-memory cache for faster UI response
_REPORT_CACHE = {"ts": 0.0, "data": None}
_REPORT_TTL_SEC = 60


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


@app.get('/api/report')
def api_report():
    now = time.time()
    if _REPORT_CACHE["data"] is not None and (now - _REPORT_CACHE["ts"] <= _REPORT_TTL_SEC):
        return jsonify(_REPORT_CACHE["data"])

    data = build_report()
    _REPORT_CACHE["data"] = data
    _REPORT_CACHE["ts"] = now
    return jsonify(data)


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
      <ul>
        <li><a style='color:#93c5fd' href='/invest-recommend'>/invest-recommend</a> (투자 추천)</li>
        <li><a style='color:#93c5fd' href='/invest-history'>/invest-history</a> (추천 히스토리 캘린더)</li>
        <li><a style='color:#93c5fd' href='/tema-web-v2'>/tema-web-v2</a> (테마주 업그레이드 v2)</li>
        <li><a style='color:#93c5fd' href='/theme-leaders'>/theme-leaders</a> (당일 주도테마/주도주 탐색)</li>
        <li><a style='color:#93c5fd' href='/theme-now-kr'>/theme-now-kr</a> (한국주식 테마 실시간 스코어보드)</li>
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


@app.get('/invest-recommend/history')
def invest_history_page():
    return send_from_directory(f"{app.static_folder}/invest-history", 'index.html')


@app.get('/invest-recommend/history/<path:filename>')
def invest_history_assets(filename):
    return send_from_directory(f"{app.static_folder}/invest-history", filename)


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
