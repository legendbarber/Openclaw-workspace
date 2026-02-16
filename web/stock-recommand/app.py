from flask import Flask, jsonify, send_from_directory, request
from collections import defaultdict
from datetime import datetime, UTC
import html
import re
import urllib.request
import importlib.util
import sys
from pathlib import Path
import yfinance as yf

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

RATING_SCORE = {
    'strong_buy': 100,
    'buy': 80,
    'hold': 45,
    'underperform': 20,
    'sell': 0
}


def clamp(v, low, high):
    return min(high, max(low, v))


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


def scrape_reports(limit=100):
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
            nums = [x.strip() for x in blind_vals if re.search(r'[\d,\.]+', x)]
            if nums:
                change = nums[-1]

    return {'price': price, 'changeText': change}


def scrape_leading_themes(limit=8):
    url = 'https://finance.naver.com/sise/theme.naver'
    s = fetch_text(url)
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', s, re.S)

    out = []
    for r in rows:
        if 'sise_group_detail.naver?type=theme' not in r:
            continue

        theme_m = re.search(r'/sise/sise_group_detail.naver\?type=theme&no=(\d+)"[^>]*>(.*?)</a>', r, re.S)
        rate_m = re.search(r'class="number col_type2">\s*<span[^>]*>(.*?)</span>', r, re.S)
        rise_m = re.search(r'class="number col_type4">\s*(\d+)\s*</td>', r, re.S)
        stocks = re.findall(r'/item/main.naver\?code=(\d+)"[^>]*>(.*?)</a>', r, re.S)

        if not theme_m:
            continue

        clean = lambda x: clean_html(x)
        theme_name = clean(theme_m.group(2))
        change_rate = clean(rate_m.group(1)) if rate_m else '-'

        leaders = []
        for code, name in stocks[:2]:
            leaders.append({'code': code, 'name': clean(name)})

        leader_snapshot = None
        if leaders:
            try:
                leader_snapshot = fetch_stock_snapshot(leaders[0]['code'])
            except Exception:
                leader_snapshot = {'price': None, 'changeText': None}

        out.append({
            'themeNo': theme_m.group(1),
            'theme': theme_name,
            'changeRate': change_rate,
            'risingCount': int(rise_m.group(1)) if rise_m else None,
            'leaders': leaders,
            'leaderSnapshot': leader_snapshot,
            'detailUrl': f"https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={theme_m.group(1)}"
        })

        if len(out) >= limit:
            break

    return out


def calc_rating_score(rec_key, rec_mean):
    base = RATING_SCORE.get(rec_key, 50)
    if rec_mean is not None and 1 <= rec_mean <= 5:
        mean_score = clamp(((5 - rec_mean) / 4) * 100, 0, 100)
        return (base * 0.55) + (mean_score * 0.45)
    return base


def pct(v):
    if v is None:
        return None
    return round(v * 100, 2)


def fetch_quant_metrics(code: str):
    symbols = [f'{code}.KS', f'{code}.KQ']

    for symbol in symbols:
        try:
            t = yf.Ticker(symbol)
            info = t.info or {}
            hist = t.history(period='1y')

            if hist is None or hist.empty:
                continue

            closes = hist['Close'].dropna().tolist()
            if len(closes) < 30:
                continue

            latest = float(closes[-1])
            p90 = float(closes[-min(91, len(closes))])
            p252 = float(closes[-min(253, len(closes))])
            m90 = (latest / p90) - 1 if p90 else 0
            m252 = (latest / p252) - 1 if p252 else 0

            current = info.get('currentPrice') or latest
            target = info.get('targetMeanPrice')
            upside = ((target / current) - 1) if (target and current) else None

            rec_key = info.get('recommendationKey')
            rec_mean = info.get('recommendationMean')
            analyst_score = calc_rating_score(rec_key, rec_mean)
            upside_score = 50 if upside is None else clamp((upside + 0.2) * 250, 0, 100)

            growth = info.get('earningsGrowth') or 0
            revenue_growth = info.get('revenueGrowth') or 0
            roe = info.get('returnOnEquity') or 0
            margin = info.get('profitMargins') or 0
            quality_raw = (growth * 40) + (revenue_growth * 30) + (roe * 20) + (margin * 10)
            quality_score = clamp((quality_raw + 10) * 3.2, 0, 100)

            momentum_raw = (m90 * 0.45) + (m252 * 0.55)
            momentum_score = clamp((momentum_raw + 0.2) * 250, 0, 100)

            pe = info.get('trailingPE') or info.get('forwardPE')
            valuation_score = 50
            if pe and pe > 0:
                valuation_score = clamp(100 - ((pe - 10) * 2), 0, 100)

            quant_score = (
                analyst_score * 0.32 +
                upside_score * 0.23 +
                quality_score * 0.22 +
                momentum_score * 0.15 +
                valuation_score * 0.08
            )

            return {
                'symbol': symbol,
                'score': round(quant_score, 2),
                'metrics': {
                    'currentPrice': current,
                    'targetPrice': target,
                    'upsidePct': None if upside is None else round(upside * 100, 2),
                    'recommendationKey': rec_key,
                    'recommendationMean': rec_mean,
                    'trailingPE': pe,
                    'earningsGrowthPct': pct(growth),
                    'revenueGrowthPct': pct(revenue_growth),
                    'roePct': pct(roe),
                    'profitMarginPct': pct(margin),
                    'momentum90dPct': round(m90 * 100, 2),
                    'momentum1yPct': round(m252 * 100, 2)
                }
            }
        except Exception:
            continue

    return None


def load_global_report(top_n: int = 7):
    process_path = Path(__file__).resolve().parent.parent / 'global-invest-recommender' / 'process.py'
    if not process_path.exists():
        return {
            'error': 'global-invest-recommender/process.py not found',
            'topPicks': [],
            'allRankings': [],
            'failed': []
        }

    spec = importlib.util.spec_from_file_location('global_process_module', str(process_path))
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.run_process(top_n=top_n)


def build_rankings(reports, q_filter='', min_reports=1):
    grouped = defaultdict(list)
    for r in reports:
        grouped[r['code']].append(r)

    rankings = []

    for code, rows in grouped.items():
        name = rows[0]['name']
        if q_filter and q_filter not in name and q_filter not in code:
            continue

        count = len(rows)
        if count < min_reports:
            continue

        sentiments = [sentiment_score(x['title']) for x in rows]
        avg_sent = sum(sentiments) / len(sentiments)
        recency = sum(max(0, 100 - x['rank']) for x in rows) / len(rows)
        broker_div = len({x['broker'] for x in rows if x['broker']})

        sentiment_composite = (
            count * 12 +
            avg_sent * 18 +
            recency * 0.35 +
            broker_div * 5
        )
        sentiment_norm = clamp(sentiment_composite, 0, 140) / 140 * 100

        quant = fetch_quant_metrics(code)
        quant_score = quant['score'] if quant else None

        if quant_score is None:
            final_score = sentiment_norm
        else:
            final_score = sentiment_norm * 0.45 + quant_score * 0.55

        recent_titles = [{
            'title': x['title'],
            'broker': x['broker'],
            'date': x['date'],
            'nid': x['nid'],
            'reportUrl': f"https://finance.naver.com/research/company_read.naver?nid={x['nid']}",
            'stockUrl': f"https://finance.naver.com/item/main.naver?code={code}",
            'newsSearchUrl': f"https://search.naver.com/search.naver?where=news&query={name}%20{code}"
        } for x in rows[:4]]

        row = {
            'code': code,
            'name': name,
            'reportCount': count,
            'avgSentiment': round(avg_sent, 2),
            'recency': round(recency, 2),
            'brokerDiversity': broker_div,
            'sentimentScore': round(sentiment_norm, 2),
            'quantScore': quant_score,
            'score': round(final_score, 2),
            'recentReports': recent_titles,
            'quant': quant,
            'links': {
                'stock': f"https://finance.naver.com/item/main.naver?code={code}",
                'research': "https://finance.naver.com/research/company_list.naver",
                'news': f"https://search.naver.com/search.naver?where=news&query={name}%20{code}",
                'discussion': f"https://finance.naver.com/item/board.naver?code={code}"
            }
        }

        try:
            row['snapshot'] = fetch_stock_snapshot(code)
        except Exception:
            row['snapshot'] = {'price': None, 'changeText': None}

        rankings.append(row)

    rankings.sort(key=lambda x: x['score'], reverse=True)
    return rankings


@app.route('/api/picks')
def api_picks():
    top_n = int(request.args.get('top', '10'))
    min_reports = int(request.args.get('minReports', '1'))
    q_filter = (request.args.get('q', '') or '').strip()

    top_n = clamp(top_n, 1, 50)
    min_reports = clamp(min_reports, 1, 10)

    reports = scrape_reports(limit=120)
    rankings = build_rankings(reports, q_filter=q_filter, min_reports=min_reports)
    themes = scrape_leading_themes(limit=8)

    return jsonify({
        'generatedAt': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
        'source': 'Naver Finance 리서치 + Yahoo Finance 공개 데이터',
        'filters': {
            'top': top_n,
            'minReports': min_reports,
            'q': q_filter
        },
        'algorithm': {
            'name': 'Hybrid Research+Quant Composite v2',
            'note': '리포트 기반 감성점수(45%) + 정량지표 점수(55%) 결합 랭킹',
            'disclaimer': '참고용 자동 집계입니다. 투자 판단과 손익 책임은 본인에게 있습니다.'
        },
        'topPick': rankings[0] if rankings else None,
        'rankings': rankings[:top_n],
        'themes': themes,
        'rawReportCount': len(reports)
    })


@app.route('/api/global-report')
def api_global_report():
    top_n = int(request.args.get('top', '7'))
    top_n = clamp(top_n, 1, 20)
    return jsonify(load_global_report(top_n=top_n))


@app.route('/')
def index():
    return """
    <html><body style='font-family:Arial;padding:24px'>
      <h2>Investment Services</h2>
      <ul>
        <li><a href='/stock'>/stock</a> - 국내/주도주 추천</li>
        <li><a href='/global'>/global</a> - 전세계 멀티자산 추천 + 운용계획</li>
      </ul>
    </body></html>
    """


@app.route('/stock')
def stock_page():
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/global')
def global_page():
    return send_from_directory(app.static_folder, 'global.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=False)
