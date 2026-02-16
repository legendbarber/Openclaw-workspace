from flask import Flask, jsonify, request
from process import run_process

app = Flask(__name__)

HTML = """
<!doctype html>
<html lang='ko'><head><meta charset='utf-8'/><meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>Global Invest Recommender</title>
<style>
body{font-family:Arial,sans-serif;background:#0b1220;color:#e5e7eb;margin:0}
.wrap{max-width:1100px;margin:0 auto;padding:20px}
.card{background:#111827;border:1px solid #1f2937;border-radius:12px;padding:14px;margin-top:12px}
button{background:#2563eb;border:0;color:#fff;padding:8px 12px;border-radius:8px;cursor:pointer}
small{color:#9ca3af}.grid{display:grid;grid-template-columns:1fr;gap:10px}
@media(min-width:900px){.grid{grid-template-columns:1fr 1fr}}
a{color:#93c5fd}
</style></head><body><div class='wrap'>
<h1>🌍 Global Invest Recommender</h1>
<small>전세계 자산군 자동 스코어링 + 투자운용 계획</small><br><br>
<button onclick='load()'>새로 조회</button>
<div id='macro' class='card'></div>
<div id='picks' class='grid'></div>
<div id='disc' class='card'></div>
</div>
<script>
async function load(){
  const res=await fetch('/api/report?top=7');
  const d=await res.json();
  document.getElementById('macro').innerHTML=`<b>생성:</b> ${new Date(d.generatedAt).toLocaleString()}<br><b>레짐:</b> risk_on=${d.macro.risk_on}, VIX=${d.macro.vix}, DXY 1M=${d.macro.dxy_1m_pct}%`;
  document.getElementById('disc').innerHTML=d.disclaimer;
  document.getElementById('picks').innerHTML=d.topPicks.map((x,i)=>`
    <div class='card'>
      <h3>#${i+1} ${x.symbol} (${x.name})</h3>
      <div>카테고리: ${x.category} | 점수: <b>${x.score}</b> | 기대3개월: <b>${x.expected3mPct}%</b></div>
      <div>현재가: ${x.currentPrice}</div>
      <hr>
      <div><b>운용 계획</b></div>
      <div>진입가 구간: ${x.plan.entryZone[0]} ~ ${x.plan.entryZone[1]}</div>
      <div>손절가: ${x.plan.stopLoss}</div>
      <div>1차 익절: ${x.plan.takeProfit1}</div>
      <div>2차 익절: ${x.plan.takeProfit2}</div>
      <div>보유기간: ${x.plan.holdingPeriod}</div>
      <div>매수방법: ${x.plan.whereToBuy}</div>
      <div>집행메모: ${x.plan.executionNote}</div>
      <div>리밸런싱: ${x.plan.rebalancingRule}</div>
      <div>비중: ${x.plan.positionSizing}</div>
      <div><a href='${x.links.yahoo}' target='_blank'>Yahoo</a> · <a href='${x.links.tradingview}' target='_blank'>TradingView</a></div>
    </div>`).join('');
}
load();
</script></body></html>
"""

@app.get('/')
def index():
    return HTML

@app.get('/api/report')
def api_report():
    top = int(request.args.get('top', '7'))
    top = max(1, min(top, 20))
    report = run_process(top_n=top)
    return jsonify(report)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3010, debug=False)
