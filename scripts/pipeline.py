"""
scripts/pipeline.py
───────────────────
Company Catcher 전체 데이터 파이프라인 오케스트레이터

실행 순서:
  1. fetch_biz_content.py  — DART 신규 보고서 수집 (DART API)
  2. batch_compare.py      — AI 비교 분석 (Gemini Flash)
  3. detect_leads.py       — 취재 단서 탐지
  4. generate_draft.py     — 고심각도 기사 초안 생성 (Gemini Pro)

실행 예시:
  python scripts/pipeline.py                    # 전체 파이프라인 실행
  python scripts/pipeline.py --step collect     # 수집만
  python scripts/pipeline.py --step compare     # 비교 분석만
  python scripts/pipeline.py --step detect      # 단서 탐지만
  python scripts/pipeline.py --step draft       # 초안 생성만
  python scripts/pipeline.py --dry-run          # 실제 실행 없이 플랜만 출력
  python scripts/pipeline.py --check            # 현재 파이프라인 상태 확인

DART 공시 접수 기간 (자동 감지):
  사업보고서(annual)  : 매년 1/1 ~ 4/30 (12월 결산법인 기준)
  반기보고서(h1)      : 매년 8/1 ~ 8/14
  3분기보고서(q3)     : 매년 11/1 ~ 11/14
  1분기보고서(q1)     : 매년 5/1 ~ 5/15
"""

import io
import json
import os
import subprocess
import sys
import argparse
from datetime import datetime, date
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT       = Path(__file__).parent.parent
SCRIPTS    = ROOT / "scripts"
DB_PATH    = ROOT / "data" / "dart" / "dart_reports.db"
LOG_PATH   = ROOT / "data" / "pipeline.log"

PYTHON     = sys.executable

# ── DART 공시 접수 기간 ──────────────────────────────────────────────────────
FILING_WINDOWS = [
    # (month_start, day_start, month_end, day_end, report_type, label)
    (1,  1,  4, 30, "annual", "사업보고서"),
    (5,  1,  5, 15, "q1",     "1분기보고서"),
    (8,  1,  8, 14, "h1",     "반기보고서"),
    (11, 1, 11, 14, "q3",     "3분기보고서"),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str):
    ts = now_str()
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def get_active_report_types(today: date | None = None) -> list[str]:
    """현재 날짜 기준 활성 공시 기간의 report_type 목록 반환"""
    if today is None:
        today = date.today()
    year = today.year
    active = []
    for ms, ds, me, de, rtype, label in FILING_WINDOWS:
        start = date(year, ms, ds)
        end   = date(year, me, de)
        if start <= today <= end:
            active.append((rtype, label))
    return active


def run_script(script_name: str, args: list[str], dry_run: bool = False) -> bool:
    """스크립트 실행. 성공 시 True 반환."""
    cmd = [PYTHON, str(SCRIPTS / script_name)] + args
    log(f"실행: {' '.join(cmd)}")
    if dry_run:
        log("  [DRY RUN] 실제 실행 생략")
        return True
    try:
        result = subprocess.run(
            cmd, capture_output=False, text=True, encoding="utf-8",
            errors="replace", cwd=str(ROOT)
        )
        if result.returncode != 0:
            log(f"  ✗ 오류 (종료코드 {result.returncode})")
            return False
        log(f"  ✓ 완료 (종료코드 {result.returncode})")
        return True
    except Exception as e:
        log(f"  ✗ 예외: {e}")
        return False


def check_status():
    """현재 파이프라인 상태 출력"""
    import sqlite3
    if not DB_PATH.exists():
        print(f"[error] DB 없음: {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row

    print("=" * 60)
    print("  Company Catcher 파이프라인 현황")
    print(f"  기준 시간: {now_str()}")
    print("=" * 60)

    # 보고서 수
    for rtype in ["2025_annual", "2025_q1", "2025_h1", "2025_q3"]:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM reports WHERE report_type=? AND biz_content IS NOT NULL", (rtype,)
        ).fetchone()[0]
        print(f"  보고서 [{rtype}]: {cnt}건")

    print()
    # AI 비교
    ok = conn.execute("SELECT COUNT(*) FROM ai_comparisons WHERE status='ok'").fetchone()[0]
    err = conn.execute("SELECT COUNT(*) FROM ai_comparisons WHERE status='error'").fetchone()[0]
    print(f"  AI 비교 완료: {ok}건 (오류: {err}건)")

    # 취재 단서
    leads = conn.execute("SELECT COUNT(*) FROM story_leads").fetchone()[0]
    sev5  = conn.execute("SELECT COUNT(*) FROM story_leads WHERE severity=5").fetchone()[0]
    sev4  = conn.execute("SELECT COUNT(*) FROM story_leads WHERE severity=4").fetchone()[0]
    print(f"  취재 단서: {leads}건 (sev5={sev5}, sev4={sev4})")

    # 기사 초안
    arts = conn.execute("SELECT COUNT(*) FROM article_drafts").fetchone()[0]
    print(f"  기사 초안: {arts}건")

    # 활성 공시 기간
    active = get_active_report_types()
    print()
    if active:
        print("  📋 현재 공시 접수 기간:")
        for rtype, label in active:
            print(f"    {label} ({rtype})")
    else:
        print("  📋 현재 활성 공시 기간 없음")

    print("=" * 60)
    conn.close()


def step_collect(dry_run: bool, year: int, types: list[str] | None):
    """1단계: DART 신규 보고서 수집"""
    log("=" * 50)
    log("STEP 1: DART 보고서 수집")
    log("=" * 50)

    today = date.today()
    active = get_active_report_types(today)

    if types:
        collect_types = types
    elif active:
        collect_types = [rt for rt, _ in active]
    else:
        log("현재 공시 접수 기간이 아닙니다. 수집 생략.")
        return True

    log(f"수집 대상: {collect_types}")

    args = ["--types"] + collect_types + ["--year", str(year)]
    return run_script("fetch_biz_content.py", args, dry_run)


def step_compare(dry_run: bool, limit: int):
    """2단계: AI 비교 분석"""
    log("=" * 50)
    log("STEP 2: AI 비교 분석")
    log("=" * 50)

    import sqlite3
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    done = conn.execute(
        "SELECT COUNT(*) FROM ai_comparisons WHERE status='ok'"
    ).fetchone()[0]
    conn.close()

    args = ["--resume", str(done), "--limit", str(limit), "--delay", "6.5"]
    return run_script("batch_compare.py", args, dry_run)


def step_detect(dry_run: bool):
    """3단계: 취재 단서 탐지"""
    log("=" * 50)
    log("STEP 3: 취재 단서 탐지")
    log("=" * 50)
    return run_script("detect_leads.py", [], dry_run)


def step_draft(dry_run: bool, limit: int):
    """4단계: 기사 초안 생성"""
    log("=" * 50)
    log("STEP 4: 기사 초안 생성")
    log("=" * 50)
    args = ["--severity", "4", "--limit", str(limit)]
    return run_script("generate_draft.py", args, dry_run)


# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Company Catcher 데이터 파이프라인")
    parser.add_argument("--step", choices=["collect","compare","detect","draft","all"],
                        default="all", help="실행할 단계 (기본: all)")
    parser.add_argument("--dry-run", action="store_true", help="실행 계획만 출력")
    parser.add_argument("--check",   action="store_true", help="현재 상태 확인 후 종료")
    parser.add_argument("--year",    type=int, default=datetime.now().year, help="대상 연도")
    parser.add_argument("--types",   nargs="+", default=None,
                        choices=["annual","q1","h1","q3"],
                        help="수집할 보고서 유형 (기본: 활성 기간 자동 감지)")
    parser.add_argument("--compare-limit", type=int, default=500,
                        help="AI 비교 최대 건수 (기본: 500)")
    parser.add_argument("--draft-limit",   type=int, default=20,
                        help="기사 초안 최대 건수 (기본: 20)")
    args = parser.parse_args()

    if args.check:
        check_status()
        return

    log(f"파이프라인 시작 (step={args.step}, dry_run={args.dry_run})")
    log(f"실행 환경: {PYTHON}")

    results = {}

    if args.step in ("all", "collect"):
        results["collect"] = step_collect(args.dry_run, args.year, args.types)

    if args.step in ("all", "compare"):
        results["compare"] = step_compare(args.dry_run, args.compare_limit)

    if args.step in ("all", "detect"):
        results["detect"] = step_detect(args.dry_run)

    if args.step in ("all", "draft"):
        results["draft"] = step_draft(args.dry_run, args.draft_limit)

    # 결과 요약
    log("=" * 50)
    log("파이프라인 완료")
    for step, ok in results.items():
        status = "✓ 성공" if ok else "✗ 실패"
        log(f"  {step:<12}: {status}")
    log("=" * 50)


if __name__ == "__main__":
    main()
