"""
scripts/_resume_chain.py
─────────────────────────
quota 회복 후 batch_compare 잔여 + 후속 분석 chain 자동 진행.

순서:
  1. batch_compare 잔여 — workers=3, flash-lite (Gemini)
  2. detect_leads — 룰 기반 단서 추출 (멱등)
  3. verify_growth_signals — growth_signals 채우기 (Gemini)
  4. _h1_recover_missed_leads — story_leads에 info_gap_label='HIGH' 부여하며 추가 발굴
  5. check_news_coverage --all --limit 5000 — 새 단서 news_status 부여
  6. _verify_high_news — Naver로 news 카운트 → info_gap_label HIGH_CONFIRMED/HIGH_REPORTED/HIGH_PARTIAL
  7. _enrich_high_leads — 로컬 강화
  8. _j1_fact_verify — Gemini (fact_match 라벨)
  9. _k3_cross_verify_flash — Gemini (cross verify)
 10. _l2_value_direction — Gemini (가치 방향)
 11. _l2b_uncertain_retry — Gemini (UNCERTAIN 재처리)
 12. _n1_newsability_verify — Gemini (뉴스화 점수)

각 단계는 자체적으로 quota 한도 도달 시 종료. 다음 단계는 계속 시도.
"""
import os
import subprocess
import sys
import time
import sqlite3
from pathlib import Path

ROOT = Path(__file__).parent.parent
os.chdir(ROOT)
try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
except Exception:
    pass

DB_PATH = "data/dart/dart_reports.db"


def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def run(name, label, args=None):
    args = args or []
    print(f"\n{'='*60}\n{now()}  ▶ {label}\n{'='*60}", flush=True)
    rc = subprocess.call(
        [sys.executable, "-u", "-X", "utf8", f"scripts/{name}.py"] + args,
        cwd=str(ROOT),
    )
    print(f"{now()}  ◀ {label} 종료 (rc={rc})", flush=True)
    return rc


def summary(title):
    db = sqlite3.connect(DB_PATH, timeout=15)
    print(f"\n[{title}]", flush=True)
    n_ok = db.execute(
        "SELECT COUNT(*) FROM ai_comparisons "
        "WHERE report_type_a='2025_annual' AND report_type_b='2026_q1' "
        "AND status='ok'"
    ).fetchone()[0]
    print(f"  ai_comparisons(2025_annual,2026_q1) ok: {n_ok}", flush=True)
    print(f"  story_leads 총: "
          f"{db.execute('SELECT COUNT(*) FROM story_leads').fetchone()[0]}건", flush=True)
    for r in db.execute(
        "SELECT info_gap_label, COUNT(*) FROM story_leads GROUP BY 1 ORDER BY 2 DESC"
    ):
        print(f"  info_gap_label={r[0]!r:<22} {r[1]}건", flush=True)
    for c in ["enriched_at", "fact_verified_at", "cross_verify_at",
              "value_classified_at", "n1_verified_at"]:
        n = db.execute(f"SELECT COUNT(*) FROM story_leads WHERE {c} IS NOT NULL").fetchone()[0]
        print(f"  {c}: {n}건", flush=True)
    db.close()


print(f"{now()}  === resume chain 시작 ===", flush=True)
summary("시작 시")

# 1. batch_compare 잔여
run("batch_compare", "STEP 1/12 batch_compare (잔여)",
    ["--workers", "3", "--model", "flash-lite", "--limit", "3000"])

# 2~12
for step, (name, label, args) in enumerate([
    ("detect_leads",              "detect_leads (룰 기반)", []),
    ("verify_growth_signals",     "verify_growth_signals (Gemini)", []),
    ("_h1_recover_missed_leads",  "_h1_recover_missed_leads (info_gap=HIGH 부여)", []),
    ("check_news_coverage",       "check_news_coverage (Google RSS)", ["--all", "--limit", "5000"]),
    ("_verify_high_news",         "_verify_high_news (Naver — HIGH 정밀화)", []),
    ("_enrich_high_leads",        "_enrich_high_leads (로컬)", []),
    ("_j1_fact_verify",           "_j1_fact_verify (Gemini)", []),
    ("_k3_cross_verify_flash",    "_k3_cross_verify_flash (Gemini)", []),
    ("_l2_value_direction",       "_l2_value_direction (Gemini)", []),
    ("_l2b_uncertain_retry",      "_l2b_uncertain_retry (Gemini)", []),
    ("_n1_newsability_verify",    "_n1_newsability_verify (Gemini)", []),
], start=2):
    run(name, f"STEP {step}/12 {label}", args)

summary("종료 시")
print(f"\n{now()}  === resume chain 종료 ===", flush=True)
