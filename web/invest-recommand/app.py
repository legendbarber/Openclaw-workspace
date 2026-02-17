from flask import Flask, jsonify, send_from_directory, redirect
from engine import build_report

app = Flask(__name__, static_folder="public")


@app.get('/api/report')
def api_report():
    return jsonify(build_report())


@app.get('/')
def home():
    return """
    <html><body style='font-family:Arial;padding:24px;background:#0b1220;color:#e5e7eb'>
      <h2>legendbarber Web Hub</h2>
      <p>이 주소를 앞으로 모든 웹서버의 메인 허브로 사용합니다.</p>
      <ul>
        <li><a style='color:#93c5fd' href='/invest-recommend'>/invest-recommend</a> (투자 추천)</li>
        <li><a style='color:#93c5fd' href='/game-demo'>/game-demo</a> (스와이프 게임 데모 v1)</li>
        <li><a style='color:#93c5fd' href='/game-demo-v2'>/game-demo-v2</a> (퍼즐 머지 데모 v2)</li>
        <li><a style='color:#93c5fd' href='/game-foldlight'>/game-foldlight</a> (독창 퍼즐 Foldlight 프로토)</li>
        <li><a style='color:#93c5fd' href='/game-tap-lights'>/game-tap-lights</a> (직관형 탭 퍼즐 신작)</li>
      </ul>
    </body></html>
    """


@app.get('/invest-recommend')
def invest_recommend_page():
    return send_from_directory(app.static_folder, 'index.html')


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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=False)
