import requests, json

BASE='http://127.0.0.1:3000'
PARAMS={
  'market':'kr','limit':50,
  'scorePreset':'default_6_4',
  'wStock':'0.4','wTheme':'0.4','wNews':'0','wTechnical':'0.2','wConfidence':'0.1','wValuation':'0.2',
  't':'1'
}

print('STEP 0) INPUT PARAMS')
print(json.dumps(PARAMS, ensure_ascii=False, indent=2))

r=requests.get(BASE+'/api/report', params=PARAMS, timeout=30)
j=r.json()
if j.get('status'):
    print('STATUS=', j.get('status'))
    raise SystemExit

rows=j.get('rankings') or []
naver=None
for idx,x in enumerate(rows, start=1):
    if str(x.get('symbol','')).upper()=='035420.KS':
        naver=x
        rank=idx
        break

if not naver:
    print('NAVER NOT FOUND')
    raise SystemExit

c=naver.get('components') or {}
rc=c.get('reportConsensus') or {}
th=c.get('theme') or {}
tech=c.get('technical') or {}
news=c.get('crowd') or {}
mix=c.get('scoreMix') or {}
val=c.get('valuation') or {}

print('\nSTEP 1) REPORT META')
print(json.dumps({
  'generatedAt': j.get('generatedAt'),
  'methodology': j.get('methodology'),
  'scoreConfig': j.get('scoreConfig'),
  'rankings_count': len(rows),
}, ensure_ascii=False, indent=2))

print('\nSTEP 2) TARGET SYMBOL FOUND')
print(json.dumps({'rank':rank,'symbol':naver.get('symbol'),'name':naver.get('name')}, ensure_ascii=False, indent=2))

print('\nSTEP 3) reportConsensus (stock score source)')
print(json.dumps(rc, ensure_ascii=False, indent=2))

print('\nSTEP 4) technical component')
print(json.dumps(tech, ensure_ascii=False, indent=2))

print('\nSTEP 5) news component')
print(json.dumps(news, ensure_ascii=False, indent=2))

print('\nSTEP 6) theme component')
print(json.dumps(th, ensure_ascii=False, indent=2))

print('\nSTEP 7) scoreMix')
print(json.dumps(mix, ensure_ascii=False, indent=2))

print('\nSTEP 8) valuation(excluded?)')
print(json.dumps(val, ensure_ascii=False, indent=2))

stock=float(naver.get('scoreBase') or 0)
theme=float(th.get('score') or 0)
news_s=float(news.get('score') or 0)
tech_s=float(tech.get('score') or 0)
conf=float(naver.get('confidence') or 0)

core=0.4*stock + 0.4*theme + 0.0*news_s + 0.2*tech_s
final=0.9*core + 0.1*conf

print('\nSTEP 9) FORMULA TRACE (manual recompute)')
print(json.dumps({
  'input':{'stock':stock,'theme':theme,'news':news_s,'technical':tech_s,'confidence':conf},
  'core_calc':'0.4*stock + 0.4*theme + 0.0*news + 0.2*technical',
  'core':core,
  'final_calc':'0.9*core + 0.1*confidence',
  'final':final,
  'final_rounded':round(final,2),
  'reported_final':naver.get('score'),
}, ensure_ascii=False, indent=2))

print('\nSTEP 10) FINAL OUTPUT')
print(json.dumps({
  'symbol':naver.get('symbol'),
  'name':naver.get('name'),
  'score':naver.get('score'),
  'scoreBase':naver.get('scoreBase'),
  'confidence':naver.get('confidence'),
  'currentPrice':naver.get('currentPrice'),
  'plan':naver.get('plan'),
}, ensure_ascii=False, indent=2))
