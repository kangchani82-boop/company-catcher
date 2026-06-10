"""
검증 체인 순차 실행: K-3b → L-2b → N-1
각 단계 완료 후 다음 단계 자동 실행
"""
import subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).parent.parent
SCRIPTS = ROOT / "scripts"
LOGS = ROOT / "logs"

def run(script, log_name):
    print(f'\n{"="*50}')
    print(f'▶ 시작: {script}')
    print(f'{"="*50}')
    log_path = LOGS / log_name
    with open(log_path, 'w', encoding='utf-8') as lf:
        result = subprocess.run(
            [sys.executable, '-X', 'utf8', str(SCRIPTS / script)],
            cwd=str(ROOT),
            stdout=lf, stderr=subprocess.STDOUT
        )
    # 결과 출력
    content = log_path.read_text(encoding='utf-8', errors='replace')
    print(content[-3000:] if len(content) > 3000 else content)
    print(f'✓ 완료: {script} (exit={result.returncode})')
    return result.returncode

print('검증 체인 시작: K-3b → L-2b → N-1')
print(f'시작 시각: {time.strftime("%Y-%m-%d %H:%M:%S")}')

# 1단계: K-3b cross-verify
rc = run('_k3_cross_verify_flash.py', 'k3b_chain.log')
print(f'\n[1/3] K-3b 완료 (exit={rc})')
time.sleep(10)  # API 냉각

# 2단계: L-2b UNCERTAIN 재분류
rc = run('_l2b_uncertain_retry.py', 'l2b_chain.log')
print(f'\n[2/3] L-2b 완료 (exit={rc})')
time.sleep(10)

# 3단계: N-1 뉴스화 검증
rc = run('_n1_newsability_verify.py', 'n1_chain.log')
print(f'\n[3/3] N-1 완료 (exit={rc})')

print(f'\n{"="*50}')
print('전체 체인 완료!')
print(f'완료 시각: {time.strftime("%Y-%m-%d %H:%M:%S")}')
