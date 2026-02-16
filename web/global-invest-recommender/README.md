# Global Invest Recommender (MVP)

전세계 투자 가능 자산(주식/채권/금/원자재/리츠/코인 등)을 같은 룰로 점수화해서
현재 상대적으로 기대수익이 높은 후보를 추천하는 프로세스입니다.

## 실행
```bash
cd C:\Users\mangi\.openclaw\workspace\web\global-invest-recommender
python process.py
```

## 출력
- 콘솔 Top 추천 목록
- `latest_report.json` 저장

## 점수 로직(요약)
- 모멘텀(1M/3M/6M)
- 추세(20일/50일 이동평균)
- 변동성 패널티
- 최대낙폭 패널티
- 매크로 레짐(리스크온/오프: VIX, DXY 반영)

> 주의: 투자 권유가 아니라 데이터 기반 참고용 스코어링입니다.
