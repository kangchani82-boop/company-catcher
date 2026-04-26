"""
scripts/supply_chain_overview.py
공급망 데이터 전체 조망 리포트
"""
import sqlite3, io, sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
db = sqlite3.connect(DB_PATH)

# ── 1. 전체 현황 ──────────────────────────────────────────
print("=" * 60)
print("  공급망 데이터 전체 조망")
print("=" * 60)
r = db.execute("SELECT COUNT(*), COUNT(DISTINCT corp_code), COUNT(DISTINCT partner_name) FROM supply_chain").fetchone()
print(f"  총 관계: {r[0]:,}건 | 보고기업: {r[1]:,}개 | 파트너명: {r[2]:,}개\n")

for row in db.execute("SELECT relation_type, COUNT(*), COUNT(DISTINCT corp_code) FROM supply_chain GROUP BY relation_type ORDER BY COUNT(*) DESC"):
    print(f"  {row[0]:<12}: {row[1]:,}건 ({row[2]:,}개 기업)")

# ── 2. 허브 기업 (노이즈 제거 후) ──────────────────────────
NOISE_LIKE = [
    "생산", "원재료", "매입", "조직도", "현황", "사항", "실적",
    "설비", "시장", "수주", "비교우위", "매출처", "가격변동",
]

def noise_where():
    parts = [f"partner_name NOT LIKE '%{n}%'" for n in NOISE_LIKE]
    parts += [
        "partner_name NOT LIKE '가. %'",
        "partner_name NOT LIKE '나. %'",
        "partner_name NOT LIKE '다. %'",
        "partner_name NOT LIKE '라. %'",
        "partner_name NOT LIKE '마. %'",
        "partner_name NOT LIKE '1) %'",
        "partner_name NOT LIKE '2. %'",
        "partner_name NOT IN ('A사','B사','C사','D사')",
        "LENGTH(partner_name) >= 2",
    ]
    return " AND ".join(parts)

print("\n" + "=" * 60)
print("  공급망 허브 기업 TOP 50 (많이 언급된 파트너)")
print("=" * 60)
rows = db.execute(f"""
    SELECT partner_name, COUNT(*) as cnt,
           COUNT(DISTINCT corp_code) as reporters,
           SUM(CASE WHEN relation_type='customer' THEN 1 ELSE 0 END),
           SUM(CASE WHEN relation_type='supplier' THEN 1 ELSE 0 END),
           SUM(CASE WHEN relation_type='partner'  THEN 1 ELSE 0 END),
           SUM(CASE WHEN relation_type='competitor' THEN 1 ELSE 0 END)
    FROM supply_chain
    WHERE {noise_where()}
    GROUP BY partner_name
    ORDER BY cnt DESC LIMIT 50
""").fetchall()

for row in rows:
    tags = []
    if row[3]: tags.append(f"고객{row[3]}")
    if row[4]: tags.append(f"공급{row[4]}")
    if row[5]: tags.append(f"파트너{row[5]}")
    if row[6]: tags.append(f"경쟁{row[6]}")
    print(f"  {row[0]:<22} {row[1]:3d}건 (보고{row[2]:3d}개) | {' '.join(tags)}")

# ── 3. 섹터별 분류 ────────────────────────────────────────
SECTORS = {
    "반도체·전자부품": ["반도체", "전자", "디스플레이", "LED", "PCB", "칩", "웨이퍼", "회로"],
    "자동차·부품":     ["자동차", "모비스", "모터", "타이어", "브레이크", "차량", "부품"],
    "바이오·제약":     ["바이오", "제약", "의약", "헬스케어", "의료기기", "진단", "백신"],
    "IT·소프트웨어":   ["소프트웨어", "시스템", "솔루션", "정보기술", "데이터", "인공지능", "플랫폼"],
    "통신":            ["통신", "텔레콤", "네트워크", "방송"],
    "철강·소재":       ["철강", "스틸", "소재", "금속", "알루미늄", "니켈", "코팅"],
    "화학·에너지":     ["화학", "에너지", "석유", "가스", "배터리", "이차전지", "태양광"],
    "건설·엔지니어링": ["건설", "건축", "시공", "엔지니어링", "주택"],
    "유통·물류":       ["유통", "물류", "마트", "쇼핑", "편의점", "배송"],
    "식품·음료":       ["식품", "음료", "제과", "농업", "축산", "수산", "사료"],
    "게임·엔터":       ["게임", "엔터테인먼트", "미디어", "콘텐츠", "음악", "영화"],
}

print("\n" + "=" * 60)
print("  섹터별 공급망 현황")
print("=" * 60)
print(f"  {'섹터':<18} {'기업':>5} {'총건':>6} {'고객':>5} {'공급':>5} {'파트너':>6} {'경쟁':>5}")
print("  " + "-" * 56)

for sector, kws in SECTORS.items():
    conds = " OR ".join([f"sc.corp_name LIKE '%{kw}%'" for kw in kws])
    row = db.execute(f"""
        SELECT COUNT(*) as total, COUNT(DISTINCT sc.corp_code),
               SUM(CASE WHEN sc.relation_type='customer' THEN 1 ELSE 0 END),
               SUM(CASE WHEN sc.relation_type='supplier' THEN 1 ELSE 0 END),
               SUM(CASE WHEN sc.relation_type='partner'  THEN 1 ELSE 0 END),
               SUM(CASE WHEN sc.relation_type='competitor' THEN 1 ELSE 0 END)
        FROM supply_chain sc WHERE {conds}
    """).fetchone()
    if row[0]:
        print(f"  {sector:<18} {row[1]:>5} {row[0]:>6} {row[2]:>5} {row[3]:>5} {row[4]:>6} {row[5]:>5}")

# ── 4. 섹터별 TOP 허브 기업 ──────────────────────────────
print("\n" + "=" * 60)
print("  섹터별 주요 허브 파트너 (상위 5개)")
print("=" * 60)

for sector, kws in SECTORS.items():
    conds = " OR ".join([f"sc.corp_name LIKE '%{kw}%'" for kw in kws])
    rows2 = db.execute(f"""
        SELECT sc.partner_name, COUNT(*) as cnt
        FROM supply_chain sc
        WHERE ({conds})
          AND {noise_where()}
        GROUP BY sc.partner_name ORDER BY cnt DESC LIMIT 5
    """).fetchall()
    if rows2:
        hubs = ", ".join([f"{r[0]}({r[1]})" for r in rows2])
        print(f"  [{sector}] {hubs}")

# ── 5. 기업당 관계 수 분포 ───────────────────────────────
print("\n" + "=" * 60)
print("  기업당 공급망 관계 수 분포")
print("=" * 60)
for row in db.execute("""
    SELECT cnt_range, COUNT(*) FROM (
        SELECT CASE WHEN cnt >= 20 THEN '20건 이상'
                    WHEN cnt >= 10 THEN '10-19건'
                    WHEN cnt >= 5  THEN '5-9건'
                    WHEN cnt >= 2  THEN '2-4건'
                    ELSE '1건'
               END as cnt_range, cnt
        FROM (SELECT corp_code, COUNT(*) as cnt FROM supply_chain GROUP BY corp_code)
    ) GROUP BY cnt_range ORDER BY MIN(cnt) DESC
"""):
    print(f"  {row[0]:<12}: {row[1]:,}개 기업")

# ── 6. 멀티섹터 허브 기업 (여러 섹터에서 공통 언급) ─────
print("\n" + "=" * 60)
print("  크로스섹터 허브 기업 TOP 20 (여러 업종 기업들이 함께 언급)")
print("=" * 60)
rows3 = db.execute(f"""
    SELECT partner_name, COUNT(DISTINCT corp_code) as reporters, COUNT(*) as total
    FROM supply_chain
    WHERE {noise_where()}
    GROUP BY partner_name
    HAVING reporters >= 10
    ORDER BY reporters DESC LIMIT 20
""").fetchall()
for row in rows3:
    print(f"  {row[0]:<22} {row[1]:3d}개 기업이 언급 (총 {row[2]}건)")

db.close()
print("\n완료.")
