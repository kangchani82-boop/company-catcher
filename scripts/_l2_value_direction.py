"""
L-2: Gemini 정밀 미래 가치 분류 (배치 버전)
- 입력: 골드 단서(HIGH_CONFIRMED + EXACT/STRONG) 중 industry_alignment IS NULL
- 출력: 산업 트렌드 대비 방향 + 단/중/장기 + 시총 영향
- ★ 배치: 10건을 한 Gemini 호출로 묶어 처리 (호출 1/10) — scripts/_gemini_batch.py 사용
"""
import sqlite3, json, os, sys, io, time
from pathlib import Path
from datetime import datetime

try: sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except: pass

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

from scripts._gemini_batch import process_in_batches

BATCH_SIZE = 10  # _l2 단건 출력 ~300토큰 → 10건 ≈ 3000토큰 (8192 한도 내)

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 컬럼 추가
cols = [c[1] for c in db.execute('PRAGMA table_info(story_leads)')]
NEW_COLS = ['industry_alignment','short_term','medium_term','long_term',
            'impact_magnitude','value_confidence','value_rationale_ai']
for col in NEW_COLS:
    if col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {col} TEXT')

INSTRUCTION = """당신은 증권사 산업 분석가입니다. 각 회사의 사업보고서 변화 시그널을 평가하세요.

평가 기준:
- value_direction: 회사 가치에 명확히 긍정(POSITIVE_GROWTH)/부정(NEGATIVE_DECLINE)/전환(TRANSFORMATION)/리스크(RISK_WARNING)/불확실(UNCERTAIN)
- industry_alignment: 산업 트렌드와 같은 방향(ALIGNED) / 역행(CONTRARIAN) / 무관(NEUTRAL)
- impact_magnitude: 시총 5%+ 영향(STRONG) / 1-5%(MEDIUM) / <1%(WEAK)
- 단/중/장기 영향은 구체적으로 (예: "신제품 인지도 확산", "경쟁사 진입으로 점유율 압박")"""

OUTPUT_EXAMPLE = """{
  "id": <입력 id 그대로>,
  "value_direction": "POSITIVE_GROWTH|NEGATIVE_DECLINE|TRANSFORMATION|RISK_WARNING|UNCERTAIN",
  "confidence": 1~5,
  "industry_alignment": "ALIGNED|CONTRARIAN|NEUTRAL",
  "industry_trend": "이 업종의 최근 일반 트렌드 (1문장)",
  "short_term": "단기(1-3개월) 영향 한 문장",
  "medium_term": "중기(6-12개월) 영향 한 문장",
  "long_term": "장기(1년+) 영향 한 문장",
  "impact_magnitude": "STRONG|MEDIUM|WEAK",
  "rationale": "종합 판단 근거 (2-3문장, 산업 트렌드 대비)"
}"""


def render_item(r):
    ctx = {}
    try:
        ctx = json.loads(r['company_context'] or '{}') if r['company_context'] else {}
    except Exception:
        pass
    fin = ctx.get('recent_financials') or {}
    return (f"[회사] {r['corp_name']}  /  [업종] {ctx.get('sector','-')}  /  [시장] {ctx.get('market','-')}\n"
            f"[최근 재무] 매출 {fin.get('revenue_billion_krw','-')}억, "
            f"영업이익 {fin.get('operating_income_billion_krw','-')}억, "
            f"영업이익률 {fin.get('operating_margin','-')}%\n"
            f"[단서 제목] {r['title'] or ''}\n"
            f"[Evidence] {(r['evidence'] or '')[:400]}\n"
            f"[원문 발췌] {(r['evidence_deep'] or '')[:1200]}")


stats = {}


def on_results(matched):
    cnt = 0
    for r, o in matched:
        vd = o.get('value_direction', 'UNCERTAIN')
        stats[vd] = stats.get(vd, 0) + 1
        db.execute("""UPDATE story_leads SET
                        value_direction=?, industry_alignment=?,
                        short_term=?, medium_term=?, long_term=?,
                        impact_magnitude=?, value_confidence=?,
                        value_rationale_ai=?, value_classified_at=?
                      WHERE id=?""",
                   [vd,
                    o.get('industry_alignment', 'NEUTRAL'),
                    (o.get('short_term', '') or '')[:300],
                    (o.get('medium_term', '') or '')[:300],
                    (o.get('long_term', '') or '')[:300],
                    o.get('impact_magnitude', 'MEDIUM'),
                    str(o.get('confidence', 0)),
                    json.dumps({'trend': o.get('industry_trend', ''),
                                'rationale': o.get('rationale', '')}, ensure_ascii=False),
                    now, r['id']])
        cnt += 1
    db.commit()
    return cnt


rows = db.execute("""
    SELECT sl.id, sl.corp_name, sl.title, sl.severity, sl.evidence, sl.evidence_deep,
           sl.company_context
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH_CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND sl.industry_alignment IS NULL
""").fetchall()
print(f'대상: {len(rows)}건  (배치 크기 {BATCH_SIZE} → 예상 호출 ~{(len(rows)+BATCH_SIZE-1)//BATCH_SIZE}회)')

t0 = time.time()

def progress(ok, fail, calls, exhausted=False):
    el = (time.time() - t0) / 60
    tag = ' (한도 소진)' if exhausted else ''
    print(f'  ok={ok} fail={fail} 호출={calls} ({el:.1f}분){tag}')

ok = err = 0
try:
    ok, err, calls = process_in_batches(
        rows, item_id=lambda r: r['id'], render_item=render_item,
        instruction=INSTRUCTION, output_example=OUTPUT_EXAMPLE,
        on_results=on_results, max_items=BATCH_SIZE, max_output_tokens=8192,
        temperature=0.2, throttle=4.0, progress=progress)
    print(f'\n[완료] ok={ok} fail={err} 호출={calls}회  소요 {(time.time()-t0)/60:.1f}분')
except RuntimeError:
    print(f'\n[중단] Gemini 한도 소진 — 다음 실행 시 이어서 처리(증분)')
print('\n[방향 분포]')
for k,v in sorted(stats.items(), key=lambda x:-x[1]):
    print(f'  {k:<22} {v}')

# alignment 분포
print('\n[산업 트렌드 대비]')
for r in db.execute("""SELECT industry_alignment, COUNT(*) c
                       FROM story_leads
                       WHERE info_gap_label='HIGH_CONFIRMED'
                         AND fact_match_label IN ('EXACT','STRONG')
                         AND industry_alignment IS NOT NULL
                       GROUP BY industry_alignment ORDER BY c DESC"""):
    print(f'  {r[0]:<14} {r[1]}')

# impact magnitude
print('\n[시총 영향]')
for r in db.execute("""SELECT impact_magnitude, COUNT(*) c
                       FROM story_leads
                       WHERE info_gap_label='HIGH_CONFIRMED'
                         AND fact_match_label IN ('EXACT','STRONG')
                         AND impact_magnitude IS NOT NULL
                       GROUP BY impact_magnitude ORDER BY c DESC"""):
    print(f'  {r[0]:<14} {r[1]}')
