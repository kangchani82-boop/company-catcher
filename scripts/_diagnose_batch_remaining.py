"""
scripts/_diagnose_batch_remaining.py
─────────────────────────────────────
batch_compare 미처리 회사 진단.

분석 가능 회사 (KOSPI+KOSDAQ + 두 페어 모두 biz_content) 중
ai_comparisons.status='ok' 아닌 회사들을 사유별로 분류.
"""
import sqlite3
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row

PAIR_A = "2025_annual"
PAIR_B = "2026_q1"

# 1. 자동 페어 도출
sys.path.insert(0, str(ROOT))
try:
    from scripts._compare_pair import get_latest_compare_pair
    pair = get_latest_compare_pair(db)
    if pair:
        PAIR_A, PAIR_B = pair
        print(f"[자동 페어] {PAIR_A} → {PAIR_B}\n")
except Exception as e:
    print(f"[자동 페어 실패: {e}]\n")

# 2. 분석 가능 회사 (현 페어)
eligible = db.execute(f"""
    SELECT a.corp_code,
           MAX(a.corp_name) AS corp_name,
           MAX(CASE WHEN a.report_type=? THEN a.id END) AS id_a,
           MAX(CASE WHEN a.report_type=? THEN a.id END) AS id_b,
           MAX(CASE WHEN a.report_type=? THEN LENGTH(a.biz_content) END) AS len_a,
           MAX(CASE WHEN a.report_type=? THEN LENGTH(a.biz_content) END) AS len_b
    FROM reports a
    JOIN companies c ON c.corp_code = a.corp_code
                    AND c.market IN ('KOSPI','KOSDAQ')
    WHERE a.biz_content IS NOT NULL
      AND LENGTH(a.biz_content) >= 500
      AND a.report_type IN (?, ?)
    GROUP BY a.corp_code
    HAVING MAX(CASE WHEN a.report_type=? THEN 1 ELSE 0 END) = 1
       AND MAX(CASE WHEN a.report_type=? THEN 1 ELSE 0 END) = 1
""", [PAIR_A, PAIR_B, PAIR_A, PAIR_B, PAIR_A, PAIR_B, PAIR_A, PAIR_B]).fetchall()

total = len(eligible)
print(f"[분석 가능 회사 (현 페어 둘 다 biz_content ≥ 500)]: {total}개")

# 3. ok / error / deprecated / 미처리
ok_codes = set(r[0] for r in db.execute(
    "SELECT corp_code FROM ai_comparisons "
    "WHERE report_type_a=? AND report_type_b=? AND status='ok'",
    [PAIR_A, PAIR_B]
).fetchall())
err_codes = set(r[0] for r in db.execute(
    "SELECT corp_code FROM ai_comparisons "
    "WHERE report_type_a=? AND report_type_b=? AND status='error'",
    [PAIR_A, PAIR_B]
).fetchall())
dep_codes = set(r[0] for r in db.execute(
    "SELECT corp_code FROM ai_comparisons "
    "WHERE report_type_a=? AND report_type_b=? AND status='deprecated_label_flip'",
    [PAIR_A, PAIR_B]
).fetchall())

ok_n = sum(1 for r in eligible if r["corp_code"] in ok_codes)
err_n = sum(1 for r in eligible if r["corp_code"] in err_codes and r["corp_code"] not in ok_codes)
dep_only = sum(1 for r in eligible
               if r["corp_code"] in dep_codes
               and r["corp_code"] not in ok_codes
               and r["corp_code"] not in err_codes)
never = [r for r in eligible
         if r["corp_code"] not in ok_codes
         and r["corp_code"] not in err_codes
         and r["corp_code"] not in dep_codes]

print(f"  ok     : {ok_n}")
print(f"  error  : {err_n}")
print(f"  deprecated만 (ok·error 없음): {dep_only}")
print(f"  never tried (한 번도 시도 안 됨): {len(never)}")
print(f"  합계: {ok_n + err_n + dep_only + len(never)}")

# 4. error 상세 (있다면)
if err_n > 0:
    print(f"\n[error 회사 (최대 20건)]")
    err_rows = db.execute("""
        SELECT corp_name, error_msg FROM ai_comparisons
        WHERE report_type_a=? AND report_type_b=? AND status='error'
        LIMIT 20
    """, [PAIR_A, PAIR_B]).fetchall()
    for r in err_rows:
        print(f"  {r[0]:<22} → {(r[1] or '')[:80]}")

# 5. never tried 분석
if never:
    print(f"\n[never tried — 한 번도 batch에 들어간 적 없는 회사 {len(never)}개]")
    print(f"  biz_content 길이 분포:")
    bucket = {"<1K": 0, "1K~5K": 0, "5K~20K": 0, "20K~60K": 0, "60K+": 0}
    for r in never:
        avg_len = ((r["len_a"] or 0) + (r["len_b"] or 0)) // 2
        if avg_len < 1000: bucket["<1K"] += 1
        elif avg_len < 5000: bucket["1K~5K"] += 1
        elif avg_len < 20000: bucket["5K~20K"] += 1
        elif avg_len < 60000: bucket["20K~60K"] += 1
        else: bucket["60K+"] += 1
    for k, v in bucket.items():
        print(f"    {k:<10} {v}")

    print(f"\n  샘플 20건:")
    for r in never[:20]:
        print(f"    {r['corp_name']:<22}  a_len={r['len_a']:>6}  b_len={r['len_b']:>6}")

# 6. deprecated만 있는 회사 (재분석 시 ok로 덮어쓰일 후보)
if dep_only > 0:
    print(f"\n[deprecated만 있는 회사 {dep_only}개 — 재분석 시 ok로 자동 변환됨]")
    dep_list = [r for r in eligible
                if r["corp_code"] in dep_codes
                and r["corp_code"] not in ok_codes
                and r["corp_code"] not in err_codes][:15]
    for r in dep_list:
        print(f"  {r['corp_name']:<22}  a_len={r['len_a']:>6}  b_len={r['len_b']:>6}")

print(f"\n[요약]")
remaining = total - ok_n
print(f"  잔여 (재분석 대상): {remaining}")
print(f"    - error 재시도 필요: {err_n}")
print(f"    - deprecated 덮어쓰기 필요: {dep_only}")
print(f"    - never tried: {len(never)}")

db.close()
