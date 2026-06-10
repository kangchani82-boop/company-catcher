"""
scripts/_finalize_after_chain.py
─────────────────────────────────
_resume_chain.py 가 백그라운드로 도는 동안 별도 백그라운드에서 함께 도는 wrapper.
chain 이 끝나는 (혹은 15분 이상 변화 없는) 시점을 감지 후 자동으로:

  1. NULL info_gap_label 단서에 SQL 매핑 재적용
     (새로 추가된 단서 — _h1/verify_growth_signals 가 거의 부여 못 하는 시스템 한계 보완)
  2. _enrich_high_leads (LIKE 'HIGH%' AND enriched_at IS NULL 멱등)
  3. _j1_fact_verify (fact_verified_at 빈 단서만)
  4. _k3_cross_verify_flash (HIGH_CONFIRMED + EXACT/STRONG, cross_verify_at NULL)
  5. _l2_value_direction (value_classified_at NULL)
  6. _l2b_uncertain_retry
  7. _n1_newsability_verify (n1_verified_at NULL)

각 후속 스크립트는 멱등 — 이미 처리된 단서는 자동 skip.
quota 한도 도달 시 각자 자체 종료.
"""
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
os.chdir(ROOT)
try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
except Exception:
    pass

DB_PATH = "data/dart/dart_reports.db"
POLL_INTERVAL = 120          # 2분
STALL_THRESHOLD = 15 * 60    # 15분간 변화 없으면 chain 끝났다고 간주


def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def progress_signature():
    """진행 변화 감지용 종합 시그니처."""
    db = sqlite3.connect(DB_PATH, timeout=15)
    sig = {}
    sig["batch_ok"] = db.execute(
        "SELECT COUNT(*) FROM ai_comparisons "
        "WHERE report_type_a='2025_annual' AND report_type_b='2026_q1' "
        "AND status='ok'"
    ).fetchone()[0]
    sig["story_leads"] = db.execute("SELECT COUNT(*) FROM story_leads").fetchone()[0]
    for c in ["enriched_at", "fact_verified_at", "cross_verify_at",
              "value_classified_at", "n1_verified_at"]:
        sig[c] = db.execute(
            f"SELECT COUNT(*) FROM story_leads WHERE {c} IS NOT NULL"
        ).fetchone()[0]
    db.close()
    return sig


def run(name, label, args=None):
    args = args or []
    print(f"\n{'='*60}\n{now()}  ▶ {label}\n{'='*60}", flush=True)
    rc = subprocess.call(
        [sys.executable, "-u", "-X", "utf8", f"scripts/{name}.py"] + args,
        cwd=str(ROOT),
    )
    print(f"{now()}  ◀ {label} 종료 (rc={rc})", flush=True)
    return rc


# ── 1) chain 완료 대기 (DB 변화 폴링) ────────────────────────────────────
print(f"{now()}  === finalize wrapper 시작 ===", flush=True)
print(f"{now()}  resume_chain 진행 감지 모드. {STALL_THRESHOLD//60}분간 변화 없으면 chain 끝났다고 판단.", flush=True)

last_sig = None
last_change_t = time.time()
while True:
    try:
        sig = progress_signature()
    except Exception as e:
        print(f"{now()}  ⚠ DB 조회 실패: {e}", flush=True)
        time.sleep(POLL_INTERVAL)
        continue

    if sig != last_sig:
        if last_sig is None:
            print(f"{now()}  [현재] {sig}", flush=True)
        else:
            diffs = {k: sig[k] - last_sig[k] for k in sig if sig[k] != last_sig.get(k)}
            print(f"{now()}  [변화] {diffs}  ▸ 누적 {sig}", flush=True)
        last_sig = sig
        last_change_t = time.time()

    if time.time() - last_change_t >= STALL_THRESHOLD:
        print(f"\n{now()}  ⏹ {STALL_THRESHOLD//60}분간 변화 없음 — chain 종료 판단", flush=True)
        break

    time.sleep(POLL_INTERVAL)


# ── 2) SQL 매핑 (NULL → 라벨) ───────────────────────────────────────────
print(f"\n{'='*60}\n{now()}  ▶ STEP A: info_gap_label SQL 매핑 (NULL 단서)\n{'='*60}", flush=True)
db = sqlite3.connect(DB_PATH, timeout=15)
before_null = db.execute("SELECT COUNT(*) FROM story_leads WHERE info_gap_label IS NULL").fetchone()[0]
print(f"  NULL 단서: {before_null}건", flush=True)
n = db.execute("""
UPDATE story_leads SET info_gap_label = CASE
  WHEN severity >= 4 AND news_status = 'exclusive' THEN 'HIGH_CONFIRMED'
  WHEN severity >= 4 AND news_status = 'partial'   THEN 'HIGH_PARTIAL'
  WHEN severity >= 4 AND news_status = 'covered'   THEN 'HIGH_REPORTED'
  WHEN severity >= 4                                THEN 'HIGH'
  WHEN severity = 3                                 THEN 'MEDIUM'
  ELSE 'LOW'
END WHERE info_gap_label IS NULL
""").rowcount
db.commit()
print(f"  매핑 적용: {n}건", flush=True)
print("\n  [매핑 후 분포]", flush=True)
for r in db.execute("SELECT info_gap_label, COUNT(*) FROM story_leads GROUP BY 1 ORDER BY 2 DESC"):
    print(f"    {r[0]!r:<22} {r[1]}건", flush=True)
db.close()


# ── 3) 후속 chain 재실행 (모두 멱등) ────────────────────────────────────
for step_no, (name, label, args) in enumerate([
    ("_enrich_high_leads",       "_enrich_high_leads", []),
    ("_j1_fact_verify",          "_j1_fact_verify (Gemini)", []),
    ("_k3_cross_verify_flash",   "_k3_cross_verify_flash (Gemini)", []),
    ("_l2_value_direction",      "_l2_value_direction (Gemini)", []),
    ("_l2b_uncertain_retry",     "_l2b_uncertain_retry (Gemini)", []),
    ("_n1_newsability_verify",   "_n1_newsability_verify (Gemini)", []),
], start=1):
    run(name, f"STEP B-{step_no}/6 {label}", args)


# ── 4) 최종 요약 ────────────────────────────────────────────────────────
print(f"\n{'='*60}\n{now()}  === finalize 완료 ===\n{'='*60}", flush=True)
db = sqlite3.connect(DB_PATH, timeout=15)

print("\n[ai_comparisons 페어별 분포]", flush=True)
for r in db.execute("""
    SELECT report_type_a, report_type_b, status, COUNT(*)
    FROM ai_comparisons GROUP BY 1,2,3 ORDER BY 1,2,3
"""):
    print(f"  a={r[0]:<14} b={r[1]:<14} status={r[2]:<25} cnt={r[3]}", flush=True)

print("\n[story_leads info_gap × fact_match]", flush=True)
for r in db.execute("""
    SELECT info_gap_label, fact_match_label, COUNT(*)
    FROM story_leads
    WHERE info_gap_label LIKE 'HIGH%'
    GROUP BY 1,2 ORDER BY 1,2
"""):
    print(f"  {r[0]:<20} {r[1]!r:<20} {r[2]}건", flush=True)

print("\n[단계별 처리 누적]", flush=True)
for c in ["enriched_at", "fact_verified_at", "cross_verify_at",
          "value_classified_at", "n1_verified_at"]:
    n = db.execute(f"SELECT COUNT(*) FROM story_leads WHERE {c} IS NOT NULL").fetchone()[0]
    print(f"  {c:<22}: {n}건", flush=True)

print("\n🏆 GOLD (HIGH_CONFIRMED + EXACT/STRONG):", flush=True)
n_gold = db.execute(
    "SELECT COUNT(*) FROM story_leads "
    "WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')"
).fetchone()[0]
print(f"  {n_gold}건", flush=True)

print("\n🏆 ULTRA GOLD (위 + cross_verified='CONFIRMED'):", flush=True)
n_ug = db.execute(
    "SELECT COUNT(*) FROM story_leads "
    "WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG') "
    "AND cross_verified='CONFIRMED'"
).fetchone()[0]
print(f"  {n_ug}건", flush=True)

print("\n[N-1 출고 후보 (newsability_score>=7)]:", flush=True)
for r in db.execute("""
    SELECT corp_name, newsability_score, valence, publish_risk, headline_ko
    FROM story_leads
    WHERE n1_verified_at IS NOT NULL AND newsability_score >= 7
    ORDER BY newsability_score DESC, publish_risk ASC
    LIMIT 30
"""):
    print(f"  {r[1]}점 {r[2]}/{r[3]}  {r[0]:<22} {r[4] or ''}", flush=True)
db.close()

print(f"\n{now()}  종료.", flush=True)
