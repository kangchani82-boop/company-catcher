"""
2025_annual vs 2026_q1 전체 비교 완성 체인
1단계: 미완료 444건 AI 비교 (biz_content 이미 있는 것)
2단계: 사업내용 미수집 수집 (annual 249건 + 2026_q1 82건)
3단계: 새로 수집된 기업 AI 비교
"""
import subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPTS = ROOT / "scripts"

def run(label, args, timeout=None):
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

print('=== 2025_annual vs 2026_q1 전체 비교 체인 시작 ===')
print(f'시작: {time.strftime("%Y-%m-%d %H:%M:%S")}')

# 1단계: 미완료 444건 AI 비교 (biz_content 있는 것)
run('1단계: 미완료 444건 AI 비교 분석',
    [str(SCRIPTS / 'batch_compare.py'),
     '--type-a', '2025_annual', '--type-b', '2026_q1',
     '--limit', '500', '--delay', '6.5'])

time.sleep(5)

# 2단계: annual biz_content 미수집 수집
run('2단계: 2025_annual biz_content 미수집 수집',
    [str(SCRIPTS / 'fetch_biz_content.py'), '--types', 'annual'])

time.sleep(5)

# 3단계: 2026_q1 biz_content 미수집 수집
run('3단계: 2026_q1 biz_content 미수집 수집',
    [str(SCRIPTS / 'fetch_biz_content.py'), '--types', 'q1_2026'])

time.sleep(5)

# 4단계: 새로 수집된 기업 AI 비교 (남은 것 전부)
run('4단계: 추가 수집 후 AI 비교 분석',
    [str(SCRIPTS / 'batch_compare.py'),
     '--type-a', '2025_annual', '--type-b', '2026_q1',
     '--limit', '300', '--delay', '6.5'])

time.sleep(5)

# 5단계: 취재 단서 재탐지 (새 비교 결과 반영)
run('5단계: 취재 단서 재탐지',
    [str(SCRIPTS / 'detect_leads.py')])

print(f'\n{"="*55}')
print('전체 체인 완료!')
print(f'완료: {time.strftime("%Y-%m-%d %H:%M:%S")}')
