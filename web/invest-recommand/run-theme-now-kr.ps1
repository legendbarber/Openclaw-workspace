$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

Write-Host '[1/2] KR 테마 데이터 생성 중...'
python .\theme_logic_kr.py

Write-Host '[2/2] 완료. 웹에서 확인: /theme-now-kr'
Write-Host '직접 파일 확인: .\public\theme-now-kr.json'
