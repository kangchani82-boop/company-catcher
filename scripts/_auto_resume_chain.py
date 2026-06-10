"""
API 한도 회복 감지 → verify chain 자동 재실행
- 30분마다 API 테스트
- 가용 시 _run_verify_chain.py 실행
- K-3b 완료 기준: cross_verified=NULL 0건
"""
import os, sys, json, time, io, subprocess
import urllib.request, urllib.error
from pathlib import Path
from datetime import datetime

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except: pass

ROOT = Path(__file__).parent.parent
_env = ROOT / ".env"
if _env.exists():
    for line in _env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

KEYS = [os.environ.get(k, "").strip() for k in ["GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"]]
KEYS = [k for k in KEYS if k]
MODELS = ["gemini-flash-latest", "gemini-2.5-flash-lite", "gemini-2.5-flash"]
INTERVAL = 30 * 60  # 30분
MAX_TRIES = 48       # 최대 24시간

def api_alive():
    for model in MODELS:
        for k in KEYS:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={k}"
            payload = json.dumps({"contents": [{"parts": [{"text": "1+1=?"}]}],
                                  "generationConfig": {"maxOutputTokens": 5}}).encode()
            req = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type": "application/json"}, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    json.loads(r.read())
                    return True, model, k
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    continue
            except Exception:
                continue
    return False, None, None

def remaining():
    import sqlite3
    db = sqlite3.connect(str(ROOT / "data" / "dart" / "dart_reports.db"))
    k3_null = db.execute("SELECT COUNT(*) FROM story_leads WHERE cross_verified IS NULL AND info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')").fetchone()[0]
    n1_null = db.execute("SELECT COUNT(*) FROM story_leads WHERE n1_verified_at IS NULL AND info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')").fetchone()[0]
    db.close()
    return k3_null, n1_null

print(f'[{datetime.now():%H:%M}] API 자동 재개 폴링 시작 (30분 간격)')
print(f'  대상: K-3b + L-2b + N-1 체인 완료까지')

for attempt in range(1, MAX_TRIES + 1):
    k3_null, n1_null = remaining()
    print(f'\n[{datetime.now():%H:%M}] 시도 {attempt}/{MAX_TRIES} — K-3 잔여 {k3_null}건 / N-1 잔여 {n1_null}건')

    if k3_null == 0 and n1_null == 0:
        print('  ✅ 모든 검증 완료! 폴링 종료')
        break

    alive, model, key_used = api_alive()
    if alive:
        print(f'  ✅ API 가용 ({model}) — 체인 시작')
        log_path = ROOT / "logs" / f"chain_resume_{attempt}.log"
        result = subprocess.run(
            [sys.executable, '-X', 'utf8',
             str(ROOT / "scripts" / "_run_verify_chain.py")],
            cwd=str(ROOT),
            stdout=open(log_path, 'w', encoding='utf-8'),
            stderr=subprocess.STDOUT
        )
        print(f'  체인 완료 (exit={result.returncode})')
        # 결과 확인
        k3_null2, n1_null2 = remaining()
        print(f'  체인 후: K-3 잔여 {k3_null2}건 / N-1 잔여 {n1_null2}건')
        if k3_null2 == 0 and n1_null2 == 0:
            print('  🏆 전체 완료!')
            break
    else:
        print(f'  ❌ API 한도 소진 — {INTERVAL//60}분 후 재시도')

    time.sleep(INTERVAL)

print(f'\n[{datetime.now():%H:%M}] 폴링 종료')
