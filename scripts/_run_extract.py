"""백그라운드 추출 래퍼 — 출력을 파일로 저장"""
import subprocess, sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
LOG = ROOT / "logs" / "extract_sc_run.log"

with open(LOG, "w", encoding="utf-8") as f:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "extract_supply_chain_claude.py")],
        stdout=f, stderr=f,
        cwd=str(ROOT),
    )

with open(LOG, "a", encoding="utf-8") as f:
    f.write(f"\n=== 종료 코드: {result.returncode} ===\n")
