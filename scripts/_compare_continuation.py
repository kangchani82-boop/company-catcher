"""
2025_annual vs 2026_q1 비교 후속 체인
(1단계 batch_compare 444건은 이미 실행 중 → 이 스크립트는 그 다음부터)
1단계: annual biz_content 미수집 수집 (249건)
2단계: 2026_q1 biz_content 미수집 수집 (82건)
3단계: 새로 수집된 기업 AI 비교 (남은 것 전부)
4단계: 취재 단서 재탐지
"""
import subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPTS = ROOT / "scripts"

def run(label, args):
    print(f'\n{"="*55}')
    print(f'▶ {label}')
    print(f'{"="*55}')
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, '-X', 'utf8'] + args,
        cwd=str(ROOT)
    )
    elapsed = (time.time() - t0) / 60
    status = '✓ 완료' if result.returncode == 0 else f'✗ 오류(exit={result.returncode})'
    print(f'{status} ({elapsed:.1f}분)')
    return result.returncode

print('=== 사업내용 수집 + 추가 AI 비교 체인 시작 ===')
print(f'시작: {time.strftime("%Y-%m-%d %H:%M:%S")}')

# 1단계: annual biz_content 미수집 수집
run('1단계: 2025_annual biz_content 미수집 수집',
    [str(SCRIPTS / 'fetch_biz_content.py'), '--types', 'annual'])

time.sleep(5)

# 2단계: 2026_q1 biz_content 미수집 수집
run('2단계: 2026_q1 biz_content 미수집 수집',
    [str(SCRIPTS / 'fetch_biz_content.py'), '--types', 'q1_2026'])

time.sleep(5)

# 3단계: 새로 수집된 기업 포함 AI 비교 잔여분 전부
run('3단계: 잔여 AI 비교 분석 (신규 수집 포함)',
    [str(SCRIPTS / 'batch_compare.py'),
     '--type-a', '2025_annual', '--type-b', '2026_q1',
     '--limit', '300', '--delay', '6.5'])

time.sleep(5)

# 4단계: 취재 단서 재탐지 (새 비교 결과 반영)
run('4단계: 취재 단서 재탐지',
    [str(SCRIPTS / 'detect_leads.py')])

print(f'\n{"="*55}')
print('전체 완료!')
print(f'완료: {time.strftime("%Y-%m-%d %H:%M:%S")}')
