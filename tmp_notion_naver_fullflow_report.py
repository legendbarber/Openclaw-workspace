import os, json, datetime, pathlib, requests

KEY_PATH = pathlib.Path.home()/'.config'/'notion'/'api_key'
key = KEY_PATH.read_text(encoding='utf-8').strip() if KEY_PATH.exists() else os.environ.get('NOTION_API_KEY','').strip()
if not key:
    raise SystemExit('NOTION_API_KEY not found')

headers = {
    'Authorization': f'Bearer {key}',
    'Notion-Version': '2025-09-03',
    'Content-Type': 'application/json',
}

def title_from_page(obj):
    props = obj.get('properties', {})
    for v in props.values():
        if v.get('type') == 'title':
            arr = v.get('title', [])
            return ''.join([x.get('plain_text', '') for x in arr]).strip()
    return ''

def search_root():
    r = requests.post('https://api.notion.com/v1/search', headers=headers, json={'query':'03.openclaw','page_size':50}, timeout=30)
    r.raise_for_status()
    results = r.json().get('results', [])
    for x in results:
        if x.get('object') == 'page' and title_from_page(x) == '03.openclaw':
            return x['id']
    for x in results:
        if x.get('object') == 'page':
            return x['id']
    raise RuntimeError('03.openclaw root page not found')

def rt(text):
    return [{'type':'text','text':{'content':text[:1900]}}]

def h2(text):
    return {'object':'block','type':'heading_2','heading_2':{'rich_text':rt(text)}}

def h3(text):
    return {'object':'block','type':'heading_3','heading_3':{'rich_text':rt(text)}}

def p(text):
    return {'object':'block','type':'paragraph','paragraph':{'rich_text':rt(text)}}

def bul(text):
    return {'object':'block','type':'bulleted_list_item','bulleted_list_item':{'rich_text':rt(text)}}

def code_block(text):
    return {'object':'block','type':'code','code':{'rich_text':rt(text), 'language':'plain text'}}

def append_children(page_id, blocks):
    for i in range(0, len(blocks), 80):
        chunk = blocks[i:i+80]
        r = requests.patch(f'https://api.notion.com/v1/blocks/{page_id}/children', headers=headers, json={'children':chunk}, timeout=40)
        if r.status_code >= 400:
            raise RuntimeError(f'append failed: {r.status_code} {r.text[:1200]}')

log_path = pathlib.Path('tmp_trace_naver_fullflow.log')
log_text = log_path.read_text(encoding='utf-8', errors='ignore') if log_path.exists() else '(log file missing)'

def sanitize_text(s: str) -> str:
    # Notion 저장 불가 유니코드/제어문자 제거
    out = []
    for ch in s:
        o = ord(ch)
        if 0xD800 <= o <= 0xDFFF:
            continue  # surrogate 제거
        if o in (0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x0B, 0x0C, 0x0E, 0x0F):
            continue
        out.append(ch)
    return ''.join(out)

log_text = sanitize_text(log_text)

root_id = search_root()
stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
title = f'NAVER(035420) 전체 플로우 함수 로그 감사 보고서 ({stamp})'

create = requests.post('https://api.notion.com/v1/pages', headers=headers, json={
    'parent': {'page_id': root_id},
    'properties': {'title': {'title': [{'type':'text','text':{'content': title}}]}},
}, timeout=40)
create.raise_for_status()
page = create.json()
page_id = page['id']

blocks = []
blocks += [
    p('요청사항: NAVER(035420.KS) 단일 종목 기준으로 전체 플로우에서 함수 연결, 함수 입출력, 내부 값(중간값/최종값)을 모두 로그로 남기고 보고한다.'),
    p('테스트 환경: /api/report 실서버 응답(kr, limit=50, stock=0.4/theme=0.4/news=0/technical=0.2/conf=0.1) 기준'),

    h2('1) 함수 연결(콜 플로우) 요약'),
    bul('build_report()'),
    bul('  -> evaluate_asset(asset)'),
    bul('      -> _download_close(symbol, 1y)'),
    bul('      -> _consensus(symbol, name)'),
    bul('          -> _consensus_from_naver_or_hk()  # KR'),
    bul('      -> _momentum_score(series)'),
    bul('      -> _news(symbol, name)'),
    bul('      -> _liquidity_score(symbol)'),
    bul('      -> _risk_score(series)'),
    bul('      -> _technical_score(series, target_price)'),
    bul('      -> base_score/confidence/plan 계산'),
    bul('  -> _apply_runtime_theme_scores(rows, score_config)'),
    bul('      -> _get_symbol_theme_meta(symbol)'),
    bul('      -> 그룹별 themeScore/leaderScore/scoreMix/final_score 계산'),

    h2('2) 핵심 산식(실행 기준)'),
    code_block('Core = 0.40*Stock + 0.40*Theme + 0.00*News + 0.20*Technical\nFinal = 0.90*Core + 0.10*Confidence\nvaluation = excluded'),

    h2('3) NAVER(035420) 값 추적 결과(요약)'),
    bul('reportConsensus.score(Stock) = 111.0'),
    bul('theme.score = 85.29'),
    bul('technical.score = 74.88'),
    bul('news.score = 50.0'),
    bul('confidence = 90.8'),
    bul('core = 93.492'),
    bul('final = 93.2228 -> rounded 93.22'),
    bul('reported final score = 93.22 (수동 재계산 일치)'),

    h2('4) 원본 실행 로그 (전체)'),
    p('아래는 tmp_trace_from_report_api.py 실행 원문 로그를 그대로 첨부한다.'),
]

append_children(page_id, blocks)

# long log as chunked code blocks
chunks = []
chunk_size = 1700
for i in range(0, len(log_text), chunk_size):
    part = log_text[i:i+chunk_size]
    chunks.append(code_block(part))
append_children(page_id, chunks)

print(json.dumps({'ok': True, 'title': title, 'url': page.get('url'), 'page_id': page_id}, ensure_ascii=False))
