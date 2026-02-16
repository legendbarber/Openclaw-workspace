from flask import Flask, jsonify, send_from_directory
from collections import defaultdict
from datetime import datetime
import html
import re
import urllib.request

app = Flask(__name__, static_folder='public')

COMPANY_LIST_URL = 'https://finance.naver.com/research/company_list.naver'

POSITIVE_KEYWORDS = [
    '상향', '개선', '견조', '성장', '서프라이즈', '기대', '확장', '수익', '호조',
    '반등', '매수', '원년', '강화', '탄탄', '유망', '열려', '좋', '환호'
]

NEGATIVE_KEYWORDS = [
    '둔화', '부진', '하향', '부담', '우려', '악화', '조정', '감소', '약세',
    '리스크', '비용', '조롱', '하락'
]


def fetch_text(url: str, encoding='euc-kr'):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=20) as res:
        data = res.read()
    return data.decode(encoding, 'ignore')


def clean_html(raw: str):
    return html.unescape(re.sub('<.*?>', '', raw or '')).strip()


def sentiment_score(title: str):
    t = title.lower()
    p = sum(1 for k in POSITIVE_KEYWORDS if k in t)
    n = sum(1 for k in NEGATIVE_KEYWORDS if k in t)
    return p - n


def scrape_reports(limit=80):
    s = fetch_text(COMPANY_LIST_URL)
    rows = re.findall(r'<tr>(.*?)</tr>', s, re.S)
    out = []

    for idx, r in enumerate(rows):
        m = re.search(r'/item/main.naver\?code=(\d+)"[^>]*>(.*?)</a>', r, re.S)
        t = re.search(r'href="company_read.naver\?nid=(\d+)[^"]*"[^>]*>(.*?)</a>', r, re.S)
        tds = re.findall(r'<td[^>]*>(.*?)</td>', r, re.S)

        if m and t:
            vals = [clean_html(x) for x in tds]
            out.append({
                'rank': idx + 1,
                'code': m.group(1),
                'name': clean_html(m.group(2)),
                'title': clean_html(t.group(2)),
                'broker': vals[2] if len(vals) > 2 else '',
                'date': vals[5] if len(vals) > 5 else '',
                'nid': t.group(1)
            })
        if len(out) >= limit:
            break

    return out


def fetch_stock_snapshot(code: str):
    url = f'https://finance.naver.com/item/main.naver?code={code}'
    s = fetch_text(url)

    price = None
    change = None

    sec_today = re.search(r'<p class="no_today">(.*?)</p>', s, re.S)
    if sec_today:
        blind_vals = re.findall(r'<span class="blind">([\d,]+)</span>', sec_today.group(1))
        if blind_vals:
            price = int(blind_vals[0].replace(',', ''))

    sec_ex = re.search(r'<p class="no_exday">(.*?)</p>', s, re.S)
    if sec_ex:
        blind_vals = re.findall(r'<span class="blind">([^<]+)</span>', sec_ex.group(1))
        if blind_vals:
            # 예: 상승, 1,400 형태라 마지막 숫자 값 사용
            nums = [x.strip() for x in blind_vals if re.search(r'[\d,]+', x)]
            if nums:
                change = nums[-1]

    return {
        'price': price,
        'changeText': change
    }


def build_rankings(reports):
    grouped = defaultdict(list)
    for r in reports:
        grouped[r['code']].append(r)

    rankings = []

    for code, rows in grouped.items():
        name = rows[0]['name']
        count = len(rows)

        sentiments = [sentiment_score(x['title']) for x in rows]
        avg_sent = sum(sentiments) / len(sentiments)

        # 최신에 가까울수록 가점 (목록 상단일수록 최근)
        recency = sum(max(0, 100 - x['rank']) for x in rows) / len(rows)

        # 브로커 다양성(여러 증권사에서 동시에 언급되면 신뢰도 소폭 가점)
        broker_div = len({x['broker'] for x in rows if x['broker']})

        score = (
            count * 12 +
            avg_sent * 18 +
            recency * 0.35 +
            broker_div * 5
        )

        recent_titles = [
            {
                'title': x['title'],
                'broker': x['broker'],
                'date': x['date'],
                'nid': x['nid']
            }
            for x in rows[:4]
        ]

        rankings.append({
            'code': code,
            'name': name,
            'reportCount': count,
            'avgSentiment': round(avg_sent, 2),
            'recency': round(recency, 2),
            'brokerDiversity': broker_div,
            'score': round(score, 2),
            'recentReports': recent_titles
        })

    rankings.sort(key=lambda x: x['score'], reverse=True)

    # 상위 종목은 현재가까지 붙여줌
    for row in rankings[:10]:
        try:
            row['snapshot'] = fetch_stock_snapshot(row['code'])
        except Exception:
            row['snapshot'] = {'price': None, 'changeText': None}

    return rankings


@app.route('/api/picks')
def api_picks():
    reports = scrape_reports(limit=80)
    rankings = build_rankings(reports)

    return jsonify({
        'generatedAt': datetime.utcnow().isoformat() + 'Z',
        'source': 'Naver Finance 리서치 리포트(인터넷 공개 데이터)',
        'algorithm': {
            'name': 'K-Research Sentiment Composite v1',
            'note': '증권사 리포트 언급 빈도 + 제목 감성 점수 + 최신성 + 증권사 다양성 기반 랭킹',
            'disclaimer': '참고용 자동 집계입니다. 투자 판단과 손익 책임은 본인에게 있습니다.'
        },
        'topPick': rankings[0] if rankings else None,
        'rankings': rankings,
        'rawReportCount': len(reports)
    })


@app.route('/')
def index():
    return send_from_directory('public', 'index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=False)
