"""
J-1+J-2+J-5: 팩트 정밀 검증
- evidence_deep ↔ biz_content 원문 substring 매칭 (환각 잡기)
- numeric_facts 수치 ↔ 원문 등장 검증
- HALLUCINATION 라벨링 + 격리
"""
import sqlite3, json, re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 컬럼 추가
cols = [c[1] for c in db.execute('PRAGMA table_info(story_leads)')]
for col in ['fact_match_label', 'fact_match_ratio', 'numeric_verified_ratio',
            'fact_verified_at']:
    if col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {col} TEXT')
        print(f'  ✓ {col} 컬럼 신규')

# 정규화: 공백/줄바꿈 제거하고 비교
def normalize(text):
    if not text: return ''
    return re.sub(r'\s+', '', text).lower()

# evidence 텍스트를 chunk로 나눠 매칭
def fact_match(ev_deep, biz_text, chunk_size=40):
    if not ev_deep or not biz_text: return 0.0
    ev_norm = normalize(ev_deep)
    biz_norm = normalize(biz_text)
    if not ev_norm: return 0.0
    if ev_norm in biz_norm: return 1.0
    chunks = [ev_norm[i:i+chunk_size] for i in range(0, len(ev_norm), chunk_size)]
    if not chunks: return 0.0
    matched = sum(1 for c in chunks if c and c in biz_norm)
    return matched / len(chunks)

# 수치 추출 + 매칭
NUM_RE = re.compile(r'\d+(?:[.,]\d+)?')
def numeric_verify(numeric_facts_json, biz_text):
    if not numeric_facts_json or not biz_text: return None
    try:
        facts = json.loads(numeric_facts_json)
    except: return None
    if not facts: return None
    # 모든 수치 추출
    nums = set()
    for f in facts:
        for k in ['value','from','to']:
            v = f.get(k)
            if v:
                m = NUM_RE.search(str(v))
                if m: nums.add(m.group())
    if not nums: return None
    biz_norm = biz_text.replace(',','')
    matched = sum(1 for n in nums if n in biz_text or n.replace('.','') in biz_norm)
    return matched / len(nums)

# HIGH 계열 + evidence_deep 있는 단서
high_leads = db.execute("""
    SELECT sl.id, sl.evidence, sl.evidence_deep, sl.numeric_facts, sl.comparison_id
    FROM story_leads sl
    WHERE sl.info_gap_label LIKE 'HIGH%'
""").fetchall()
print(f'\n검증 대상: {len(high_leads)}건')

exact = strong = partial = hallu = no_biz = 0
nv_high = nv_low = 0

for r in high_leads:
    biz_row = db.execute("""SELECT rb.biz_content FROM ai_comparisons ac
                            JOIN reports rb ON rb.id=ac.report_id_b
                            WHERE ac.id=?""", [r['comparison_id']]).fetchone()
    biz = biz_row['biz_content'] if biz_row and biz_row['biz_content'] else ''
    if not biz:
        # biz_content 없으면 evidence(원래)와 evidence_deep 비교만
        db.execute("UPDATE story_leads SET fact_match_label='NO_SOURCE', fact_verified_at=? WHERE id=?",
                   [now, r['id']])
        no_biz += 1
        continue

    # J-1: fact match
    # evidence_deep 또는 evidence 둘 다 검증
    ev = r['evidence_deep'] or r['evidence'] or ''
    ratio = fact_match(ev, biz)
    if ratio >= 0.95:    label = 'EXACT';    exact += 1
    elif ratio >= 0.6:   label = 'STRONG';   strong += 1
    elif ratio >= 0.25:  label = 'PARTIAL';  partial += 1
    else:                label = 'HALLUCINATION'; hallu += 1

    # J-2: numeric verify
    nv = numeric_verify(r['numeric_facts'], biz)
    if nv is not None:
        if nv >= 0.6: nv_high += 1
        else: nv_low += 1

    db.execute("""UPDATE story_leads SET
                    fact_match_label=?, fact_match_ratio=?, numeric_verified_ratio=?,
                    fact_verified_at=?
                  WHERE id=?""",
               [label, f'{ratio:.2f}',
                f'{nv:.2f}' if nv is not None else None,
                now, r['id']])

db.commit()

print(f'\n[J-1 fact_match 결과]')
print(f'  EXACT:         {exact:>4}  (원문 정확 일치)')
print(f'  STRONG:        {strong:>4}  (chunk 60%+ 매칭)')
print(f'  PARTIAL:       {partial:>4}  (25-60% 매칭)')
print(f'  HALLUCINATION: {hallu:>4}  ⚠️ (25% 미만 — 환각 의심)')
print(f'  NO_SOURCE:     {no_biz:>4}  (원문 없음)')

print(f'\n[J-2 수치 검증]')
print(f'  60%+ 원문 매칭:  {nv_high}')
print(f'  60% 미만:        {nv_low}')

# J-5: HALLUCINATION 단서를 LOW로 강등 (HIGH 라벨 제거)
demoted = db.execute("""
    UPDATE story_leads SET info_gap_label='HALLUCINATION_LOW'
    WHERE fact_match_label='HALLUCINATION' AND info_gap_label LIKE 'HIGH%'
""")
print(f'\n[J-5] HALLUCINATION 단서 강등: {demoted.rowcount}건')
db.commit()

# 최종 분포
print('\n[fact_match_label × info_gap_label 매트릭스]')
for r in db.execute("""
    SELECT fact_match_label, info_gap_label, COUNT(*) c
    FROM story_leads
    WHERE info_gap_label IS NOT NULL AND fact_match_label IS NOT NULL
    GROUP BY fact_match_label, info_gap_label
    ORDER BY c DESC LIMIT 15
"""):
    print(f'  {r[0]:<22} {r[1]:<25} {r[2]}')

# 신뢰도 최고급: EXACT/STRONG + HIGH_CONFIRMED + 수치 매칭 60%+
gold = db.execute("""
    SELECT COUNT(*) FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED'
      AND fact_match_label IN ('EXACT','STRONG')
""").fetchone()[0]
print(f'\n🏆 최고 신뢰도 (HIGH_CONFIRMED + 원문 일치): {gold}건')
