"""
scripts/run_daily_q1_cycle.py
─────────────────────────────
Q1 2026 분기보고서 일일 처리 사이클 — 5/15 마감 대비.

순서:
  1) fetch_biz_content     : 신규 Q1 보고서 + biz_content 수집
  2) batch_compare         : 2025_annual vs 2026_q1 비교 (미처리만)
  3) detect_leads          : 비교 결과에서 단서 추출
  4) generate_draft        : 신규 Q1 단서에 대해 기사 초안 생성 (이미 있는 건 스킵)
  5) generate_questionnaire: 새 기사 → 질문지
  6) collect_news          : pending 질문지 회사용 뉴스 수집 (24h 캐시 자동 적용)

각 단계는 멱등(idempotent) — 매일 실행해도 신규분만 처리.

실행:
  python scripts/run_daily_q1_cycle.py                # 전체 사이클
  python scripts/run_daily_q1_cycle.py --skip-news    # 뉴스 단계 스킵
  python scripts/run_daily_q1_cycle.py --dry-run      # 무엇을 할지만 출력
  python scripts/run_daily_q1_cycle.py --only-stats   # 현재 진행률만 보고
"""
import argparse
import io
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Windows cp949 콘솔에서도 한글·이모지·U+2550 같은 박스 문자가 깨지지 않도록
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
SCRIPTS = ROOT / "scripts"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

PYTHON = sys.executable


def db_query_one(sql: str, params=()) -> int:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        return conn.execute(sql, params).fetchone()[0]
    finally:
        conn.close()


def db_query_all(sql: str, params=()):
    conn = sqlite3.connect(str(DB_PATH))
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def hr(label=""):
    line = "─" * 78
    if label:
        print(f"\n{line}")
        print(f"  {label}")
        print(line)
    else:
        print(line)


def stage(name: str, todo_count: int) -> bool:
    """단계 시작 헤더. todo_count=0 이면 스킵."""
    hr(f"[{name}]  대상 {todo_count}건")
    return todo_count > 0


def run(cmd: list[str], log_name: str) -> int:
    """서브프로세스 실행 — stdout/stderr 동시에 콘솔+로그 파일에 기록."""
    log_path = LOG_DIR / log_name
    print(f"  $ {' '.join(cmd[1:])}")
    print(f"  log: {log_path}")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as logf:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            cwd=str(ROOT), env=env, text=True, encoding="utf-8",
            errors="replace",
        )
        for line in proc.stdout:
            logf.write(line)
            # 핵심 라인만 콘솔에 (verbose 줄이기)
            stripped = line.rstrip()
            if any(k in stripped for k in ("[완료]", "성공", "오류", "error", "Error",
                                           "병렬 배치 완료", "✓ 생성", "✅", "❌",
                                           "처리:", "분석 가능", "취재 단서",
                                           "누적 질문지", "저장 뉴스")):
                print(f"  │ {stripped}")
        rc = proc.wait()
    dt = time.time() - t0
    print(f"  └ exit={rc}, {dt:.1f}s")
    return rc


def show_overall_stats():
    hr("현재 시스템 현황")
    n_q1 = db_query_one("SELECT COUNT(*) FROM reports WHERE report_type='2026_q1'")
    n_q1_biz = db_query_one(
        "SELECT COUNT(*) FROM reports WHERE report_type='2026_q1' "
        "AND biz_content IS NOT NULL AND LENGTH(biz_content) >= 500"
    )
    n_cmp = db_query_one(
        "SELECT COUNT(*) FROM ai_comparisons "
        "WHERE report_type_a='2025_annual' AND report_type_b='2026_q1' AND status='ok'"
    )
    n_lead_q1 = db_query_one(
        "SELECT COUNT(*) FROM story_leads "
        "WHERE report_type_a='2025_annual' AND report_type_b='2026_q1'"
    )
    n_qst = db_query_one("SELECT COUNT(*) FROM ir_questionnaires")
    n_qst_pending = db_query_one(
        "SELECT COUNT(*) FROM ir_questionnaires WHERE status='pending'"
    )
    n_qst_appr = db_query_one(
        "SELECT COUNT(*) FROM ir_questionnaires WHERE status='approved'"
    )
    n_qst_sent = db_query_one(
        "SELECT COUNT(*) FROM ir_questionnaires WHERE status='sent'"
    )
    n_news = db_query_one(
        "SELECT COUNT(*) FROM external_sources WHERE source_type='news'"
    )

    print(f"  Q1 2026 보고서       : {n_q1}건 (분석 가능 {n_q1_biz}건, ≥500자)")
    print(f"  AI 비교 (annual×Q1)   : {n_cmp}건 완료")
    print(f"  Q1 단서              : {n_lead_q1}건")
    print(f"  질문지              : {n_qst}건 "
          f"(pending {n_qst_pending} / approved {n_qst_appr} / sent {n_qst_sent})")
    print(f"  뉴스 자료           : {n_news:,}건")

    # 5/15 D-day
    deadline = datetime(2026, 5, 15)
    today = datetime.now()
    days_left = (deadline - today).days
    print(f"\n  ⏰ 분기보고서 마감: 2026-05-15 (D-{days_left})")


def count_targets():
    """각 단계별 처리 대기 건수 사전 산출."""
    # Stage 2: 비교 미처리 (양쪽 ≥500자 보유했으나 ai_comparisons에 없는 corp)
    n_compare_todo = db_query_one("""
        SELECT COUNT(*) FROM (
          SELECT q.corp_code FROM reports q
          JOIN reports a ON q.corp_code=a.corp_code
          WHERE q.report_type='2026_q1' AND LENGTH(COALESCE(q.biz_content,''))>=500
            AND a.report_type='2025_annual' AND LENGTH(COALESCE(a.biz_content,''))>=500
            AND NOT EXISTS (
              SELECT 1 FROM ai_comparisons c
              WHERE c.corp_code=q.corp_code
                AND c.report_type_a='2025_annual' AND c.report_type_b='2026_q1'
                AND c.status='ok'
            )
          GROUP BY q.corp_code
        )
    """)

    # Stage 3: 단서 미추출 비교
    # detect_leads.py 가 처리하지 않은 ai_comparisons.id 카운트는 비결정적이라 생략
    # 대신 "Q1 비교 중 단서 0개로 끝난 + 신규 비교"로 근사
    n_detect_todo = db_query_one("""
        SELECT COUNT(*) FROM ai_comparisons c
        WHERE c.report_type_a='2025_annual' AND c.report_type_b='2026_q1'
          AND c.status='ok'
          AND NOT EXISTS (
            SELECT 1 FROM story_leads sl
            WHERE sl.comparison_id = c.id
          )
    """)

    # Stage 4: Q1 단서 중 기사 초안 미생성
    n_draft_todo = db_query_one("""
        SELECT COUNT(*) FROM story_leads sl
        WHERE sl.report_type_a='2025_annual' AND sl.report_type_b='2026_q1'
          AND NOT EXISTS (SELECT 1 FROM article_drafts a WHERE a.lead_id=sl.id)
    """)

    # Stage 5: 기사 중 질문지 미생성
    n_qst_todo = db_query_one("""
        SELECT COUNT(*) FROM article_drafts a
        WHERE NOT EXISTS (SELECT 1 FROM ir_questionnaires q WHERE q.article_id=a.id)
    """)

    return {
        "compare": n_compare_todo,
        "detect":  n_detect_todo,
        "draft":   n_draft_todo,
        "qst":     n_qst_todo,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-news",  action="store_true", help="뉴스 수집 단계 스킵")
    ap.add_argument("--skip-fetch", action="store_true", help="DART fetch 스킵 (이미 받았으면)")
    ap.add_argument("--dry-run",    action="store_true", help="대상만 출력, 실행 X")
    ap.add_argument("--only-stats", action="store_true", help="현재 통계만 출력")
    ap.add_argument("--model",      default="flash-lite",
                    choices=["flash", "flash-lite", "pro"],
                    help="비교 분석 Gemini 모델")
    ap.add_argument("--workers",    type=int, default=2,
                    help="비교 병렬 워커 (1~2)")
    ap.add_argument("--news-limit", type=int, default=400,
                    help="뉴스 수집 회사 한도")
    args = ap.parse_args()

    print("\n" + "═" * 78)
    print(f"  Q1 2026 일일 사이클 시작 — {datetime.now():%Y-%m-%d %H:%M}")
    print("═" * 78)

    show_overall_stats()

    if args.only_stats:
        return

    # 사전 산출
    targets = count_targets()
    hr("사이클 처리 대기")
    print(f"  ② 비교 미처리           : {targets['compare']}건")
    print(f"  ③ 단서 미추출 비교      : {targets['detect']}건")
    print(f"  ④ 기사 미생성 단서      : {targets['draft']}건")
    print(f"  ⑤ 질문지 미생성 기사    : {targets['qst']}건")

    if args.dry_run:
        hr("DRY RUN — 종료")
        return

    rc_total = 0

    # ① fetch
    if not args.skip_fetch:
        if stage("① fetch_biz_content (신규 Q1 공시)", 1):
            rc = run([PYTHON, str(SCRIPTS / "fetch_biz_content.py"),
                      "--types", "q1_2026"],
                     log_name="cycle_01_fetch.log")
            rc_total += rc

    # 다시 산출 (fetch 후 신규 보고서가 늘었을 수 있음)
    targets = count_targets()

    # ② batch_compare
    if stage("② batch_compare (2025_annual × 2026_q1)", targets['compare']):
        rc = run([PYTHON, str(SCRIPTS / "batch_compare.py"),
                  "--type-a", "2025_annual", "--type-b", "2026_q1",
                  "--model", args.model, "--workers", str(args.workers)],
                 log_name="cycle_02_compare.log")
        rc_total += rc
        targets = count_targets()
    else:
        print("  → 건너뜀 (모두 처리됨)")

    # ③ detect_leads
    if stage("③ detect_leads", targets['detect']):
        rc = run([PYTHON, str(SCRIPTS / "detect_leads.py")],
                 log_name="cycle_03_leads.log")
        rc_total += rc
        targets = count_targets()
    else:
        print("  → 건너뜀")

    # ④ generate_draft (Q1 단서만 — lead-id 단위 호출)
    if stage("④ generate_draft (Q1 단서 한정)", targets['draft']):
        # Q1 단서 ID 목록 추출 후 1건씩 호출
        ids = [r[0] for r in db_query_all("""
            SELECT sl.id FROM story_leads sl
            WHERE sl.report_type_a='2025_annual' AND sl.report_type_b='2026_q1'
              AND NOT EXISTS (SELECT 1 FROM article_drafts a WHERE a.lead_id=sl.id)
            ORDER BY sl.severity DESC, sl.id
        """)]
        print(f"  → 처리 대상 lead_ids: {ids}")
        for lid in ids:
            rc = run([PYTHON, str(SCRIPTS / "generate_draft.py"),
                      "--lead-id", str(lid)],
                     log_name=f"cycle_04_draft_{lid}.log")
            rc_total += rc
        targets = count_targets()
    else:
        print("  → 건너뜀")

    # ⑤ generate_questionnaire
    if stage("⑤ generate_questionnaire", targets['qst']):
        rc = run([PYTHON, str(SCRIPTS / "generate_questionnaire.py")],
                 log_name="cycle_05_qst.log")
        rc_total += rc
    else:
        print("  → 건너뜀")

    # ⑥ collect_news (24h 캐시 자동, 신규 회사만 실수집)
    if not args.skip_news:
        hr("⑥ collect_news_for_questionnaires (24h 캐시 자동)")
        rc = run([PYTHON, str(SCRIPTS / "collect_news_for_questionnaires.py"),
                  "--status", "pending", "--days", "30",
                  "--limit", str(args.news_limit)],
                 log_name="cycle_06_news.log")
        rc_total += rc

    print("\n" + "═" * 78)
    print(f"  사이클 완료 — exit_sum={rc_total}, {datetime.now():%H:%M}")
    print("═" * 78)
    show_overall_stats()


if __name__ == "__main__":
    main()
