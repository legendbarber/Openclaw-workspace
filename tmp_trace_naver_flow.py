import importlib.util
import pathlib
import sys
import json
import traceback

p = pathlib.Path('web/invest-recommand/engine.py').resolve()
spec = importlib.util.spec_from_file_location('eng', p)
eng = importlib.util.module_from_spec(spec)
sys.modules['eng'] = eng
spec.loader.exec_module(eng)

TARGET_SYMBOL = '035420.KS'
TARGET_NAME = 'NAVER'
ASSET = eng.Asset(TARGET_SYMBOL, TARGET_NAME, 'kr-stock')


def pjson(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def section(title):
    print('\n' + '=' * 30)
    print(title)
    print('=' * 30)


try:
    section('STEP 0) INPUT')
    print('asset=', ASSET)

    section('STEP 1) _download_close(symbol, period=1y)')
    s = eng._download_close(ASSET.symbol, '1y')
    if s is None:
        print('close_series=None -> evaluate 중단')
        raise SystemExit(0)
    print('len(series)=', len(s))
    print('head(3)=', [float(x) for x in s.head(3).values])
    print('tail(3)=', [float(x) for x in s.tail(3).values])
    print('current(close[-1])=', float(s.iloc[-1]))

    section('STEP 2) _consensus(symbol, name)')
    cons = eng._consensus(ASSET.symbol, ASSET.name)
    pjson(cons)

    section('STEP 3) early filter: targetMeanPrice is None ?')
    tmean = cons.get('targetMeanPrice')
    print('targetMeanPrice=', tmean)
    print('pass_filter=', tmean is not None)

    section('STEP 4) _momentum_score(series)')
    momentum = eng._momentum_score(s)
    pjson(momentum)

    section('STEP 5) _news(symbol, name)')
    crowd = eng._news(ASSET.symbol, ASSET.name)
    pjson(crowd)

    section('STEP 6) _liquidity_score(symbol)')
    liquidity = eng._liquidity_score(ASSET.symbol)
    print('liquidityScore=', liquidity)

    section('STEP 7) _risk_score(series)')
    risk = eng._risk_score(s)
    pjson(risk)

    section('STEP 8) _technical_score(series, target_price)')
    technical = eng._technical_score(s, target_price=cons.get('targetMeanPrice'))
    pjson(technical)

    section('STEP 9) base score / confidence calc')
    base_score = float(cons['score'])
    r_conf = float(cons.get('confidence', 50.0) or 0.0)
    c_conf = float(((crowd.get('headlineCount', 0) or 0) / 8.0) * 100)
    t_setup = technical.get('setup')
    if t_setup == 'adjustment-zone':
        t_conf = 85.0
    elif t_setup == 'overheat-zone':
        t_conf = 65.0
    else:
        t_conf = 72.0
    confidence = 0.60 * r_conf + 0.25 * c_conf + 0.15 * t_conf
    score_pre_theme_apply = 0.9 * base_score + 0.1 * confidence

    pjson({
        'base_score(reportConsensus.score)': base_score,
        'r_conf': r_conf,
        'c_conf': c_conf,
        't_setup': t_setup,
        't_conf': t_conf,
        'confidence': confidence,
        'score_before_runtime_theme_apply': score_pre_theme_apply,
    })

    section('STEP 10) plan calc (entry/stop/tp)')
    cur = float(s.iloc[-1])
    atrp = float(s.pct_change().abs().tail(14).mean()) if len(s) > 20 else 0.03
    stop = cur * (1 - max(0.04, min(0.14, atrp * 1.8)))
    tp1 = cur * (1 + max(0.06, min(0.22, (eng._pct(s, 63) * 0.6 + 0.06))))
    tp2 = cur * (1 + max(0.1, min(0.35, (eng._pct(s, 126) * 0.8 + 0.12))))
    expected_loss_pct = (stop / cur - 1) * 100
    expected_return1_pct = (tp1 / cur - 1) * 100
    rr_ratio = expected_return1_pct / abs(expected_loss_pct) if expected_loss_pct < 0 else 0.0

    pjson({
        'cur': cur,
        'atrp': atrp,
        'stopLoss': stop,
        'takeProfit1': tp1,
        'takeProfit2': tp2,
        'expectedLossPct': expected_loss_pct,
        'expectedReturnPct': expected_return1_pct,
        'riskReward': rr_ratio,
    })

    section('STEP 11) evaluate_asset() 실제 반환')
    row = eng.evaluate_asset(ASSET)
    if row is None:
        print('evaluate_asset -> None')
        raise SystemExit(0)
    pjson({
        'symbol': row.get('symbol'),
        'name': row.get('name'),
        'score': row.get('score'),
        'scoreBase': row.get('scoreBase'),
        'confidence': row.get('confidence'),
        'components.reportConsensus': (row.get('components') or {}).get('reportConsensus'),
        'components.theme': (row.get('components') or {}).get('theme'),
        'components.technical': (row.get('components') or {}).get('technical'),
        'components.crowd': (row.get('components') or {}).get('crowd'),
        'plan': row.get('plan'),
    })

    section('STEP 12) _apply_runtime_theme_scores() 전/후')
    # NAVER + KAKAO 2종목으로 테마 그룹화 재현
    r2 = eng.evaluate_asset(eng.Asset('035720.KS', 'Kakao', 'kr-stock'))
    rows = [r for r in [row, r2] if r]
    print('before_apply_scores=')
    for rr in rows:
        print(rr['symbol'], 'score=', rr.get('score'), 'scoreBase=', rr.get('scoreBase'))

    cfg = {
        'components': {'stock': 0.4, 'theme': 0.4, 'news': 0.0, 'technical': 0.2},
        'confidence': 0.1,
        'valuation': 0.2,
        'preset': 'default_6_4',
    }
    out = eng._apply_runtime_theme_scores(rows, score_config=cfg)

    print('\nafter_apply_scores=')
    for rr in out:
        c = rr.get('components', {})
        th = c.get('theme', {})
        mix = c.get('scoreMix', {})
        print('-' * 20)
        print('symbol=', rr['symbol'])
        print('score=', rr.get('score'))
        print('scoreBase=', rr.get('scoreBase'))
        print('confidence=', rr.get('confidence'))
        print('theme.source=', th.get('source'))
        print('theme.theme=', th.get('theme'))
        print('theme.themeScore=', th.get('themeScore'))
        print('theme.leaderScore=', th.get('leaderScore'))
        print('theme.appliedScore=', th.get('score'))
        print('scoreMix=', json.dumps(mix, ensure_ascii=False))

    section('DONE')

except Exception as e:
    print('ERROR:', e)
    traceback.print_exc()
    raise
