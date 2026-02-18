from flask import Flask, jsonify, send_from_directory, redirect
from engine import build_report, save_daily_snapshot, list_snapshots, get_snapshot
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

app = Flask(__name__, static_folder="public")
KST = ZoneInfo("Asia/Seoul")


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
    return jsonify(build_report())


@app.get('/api/snapshot/save')
def api_snapshot_save():
    return jsonify(save_daily_snapshot(force=False))


@app.get('/api/snapshot/force')
def api_snapshot_force():
    return jsonify(save_daily_snapshot(force=True))


@app.get('/api/snapshots')
def api_snapshots():
    return jsonify({"items": list_snapshots(limit=365)})


@app.get('/api/snapshots/<date_kst>')
def api_snapshot_by_date(date_kst: str):
    data = get_snapshot(date_kst)
    if not data:
        return jsonify({"error": "not_found", "dateKST": date_kst}), 404
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


# 별도 디렉터리/경로 제공: /invest-history
@app.get('/invest-history')
def invest_history_root_page():
    return send_from_directory(f"{app.static_folder}/invest-history", 'index.html')


@app.get('/invest-history/<path:filename>')
def invest_history_root_assets(filename):
    return send_from_directory(f"{app.static_folder}/invest-history", filename)


# 짧은 URL 별칭: /calendar
@app.get('/calendar')
def invest_calendar_page():
    return send_from_directory(f"{app.static_folder}/invest-history", 'index.html')


@app.get('/calendar/<path:filename>')
def invest_calendar_assets(filename):
    return send_from_directory(f"{app.static_folder}/invest-history", filename)


# backward compatibility
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
