"""
N-1: 뉴스화 가능성 최종 검증 (배치 버전)
- 입력: HIGH_CONFIRMED + EXACT/STRONG 중 n1_verified_at IS NULL
- 평가: 뉴스가치·헤드라인·악재/호재·기사화 리스크·추가확인
- ★ 배치: 6건을 한 Gemini 호출로 묶어 처리 (호출 1/6) — scripts/_gemini_batch.py 사용
"""
import sqlite3, json, os, sys, io, time
from pathlib import Path
from datetime import datetime

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except: pass

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

from scripts._gemini_batch import process_in_batches

BATCH_SIZE = 6  # 출력 필드 많음(headline/story_angle/ir_hint 등) → 6건 보수적

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 컬럼 추가
cols = [c[1] for c in db.execute('PRAGMA table_info(story_leads)')]
NEW_COLS = {
    'newsability_score': 'INTEGER', 'headline_ko': 'TEXT', 'story_angle': 'TEXT',
    'valence': 'TEXT', 'publish_risk': 'TEXT', 'missing_verification': 'TEXT',
    'already_public': 'TEXT', 'n1_model': 'TEXT', 'n1_verified_at': 'TEXT',
}
for col, dtype in NEW_COLS.items():
    if col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {col} {dtype}')

INSTRUCTION = """당신은 경제 전문지 데스크 에디터이자 취재 기자입니다.
각 취재 단서로 **실제 기사화 가능성**을 평가하세요. 악재(부정 정보)도 기사 가치가 있습니다.

newsability_score 기준:
- 9~10: 즉시 출고 (SCOOP급, 시장 미공개 중요 팩트)
- 7~8:  이번 주 출고 (명확한 방향성, 충분한 근거)
- 5~6:  추가 취재 후 출고
- 3~4:  보류 / 1~2: 폐기
story_type: SCOOP(미보도 단독) / ANALYSIS(데이터 심층) / TREND(산업 흐름) / WARNING(위험 경고) / PROFILE(변화 스토리)
valence: GOOD|BAD|MIXED,  publish_risk: HIGH|MEDIUM|LOW,  urgency: IMMEDIATE|THIS_WEEK|THIS_MONTH"""

OUTPUT_EXAMPLE = """{
  "id": <입력 id 그대로>,
  "newsability_score": 1~10,
  "headline_ko": "헤드라인 후보 (30자 이내, 구체 팩트)",
  "story_angle": "취재 각도 — 독자 가치 (2문장)",
  "valence": "GOOD|BAD|MIXED",
  "valence_reason": "호재/악재 근거 (1문장)",
  "publish_risk": "HIGH|MEDIUM|LOW",
  "publish_risk_reason": "기사화 리스크 (예: 계획 단계, 법적 리스크)",
  "missing_verification": "출고 전 반드시 확인할 사항 (없으면 NONE)",
  "already_public": "YES|LIKELY_NO|UNKNOWN",
  "story_type": "SCOOP|ANALYSIS|TREND|WARNING|PROFILE",
  "urgency": "IMMEDIATE|THIS_WEEK|THIS_MONTH",
  "ir_question_hint": "IR 담당자에게 물어볼 핵심 질문 1개"
}"""


def render_item(r):
    # 정량 데이터
    num_summary = ''
    try:
        nf = json.loads(r['numeric_facts'] or '[]')
        parts = []
        for f in nf[:3]:
            if f.get('type') == 'change':
                parts.append(f"{f.get('from')} → {f.get('to')} {f.get('unit','')}")
            elif f.get('type') == 'money':
                parts.append(f"{f.get('value')} {f.get('unit','')}")
        num_summary = ' / '.join(parts)
    except Exception:
        pass
    # 공급망
    sc_summary = ''
    try:
        sc = json.loads(r['supply_chain_impact'] or '{}')
        as_sup = sc.get('as_supplier_to', [])
        if as_sup:
            sc_summary = "거래처: " + ', '.join(x.get('name', '') for x in as_sup[:3])
    except Exception:
        pass
    return (f"[회사] {r['corp_name']}  /  [분류] {r['lead_type']}\n"
            f"[단서 제목] {r['title'] or ''}\n"
            f"[미래 가치] {r['value_direction'] or '미분류'}  /  [산업 대비] {r['industry_alignment'] or '미분류'}\n"
            f"[원문 매칭] {r['fact_match_label']} ({r['fact_match_ratio']})  /  [교차검증] {r['cross_verified'] or '미실행'}\n"
            f"[Evidence] {(r['evidence'] or '')[:500]}\n"
            f"[원문 발췌] {(r['evidence_deep'] or '')[:1200]}\n"
            f"[정량] {num_summary or '없음'}  /  [공급망] {sc_summary or '없음'}\n"
            f"[단기] {r['short_term'] or '-'} [중기] {r['medium_term'] or '-'} [장기] {r['long_term'] or '-'}")


scores = []


def on_results(matched):
    cnt = 0
    for r, o in matched:
        score = o.get('newsability_score', 0)
        try:
            score = int(score)
        except Exception:
            score = 0
        scores.append(score)
        db.execute("""UPDATE story_leads SET
                        newsability_score=?, headline_ko=?, story_angle=?,
                        valence=?, publish_risk=?, missing_verification=?,
                        already_public=?, n1_model=?, n1_verified_at=?,
                        value_rationale_ai=COALESCE(
                            json_patch(COALESCE(value_rationale_ai,'{}'),
                            json_object(
                              'story_type', ?, 'urgency', ?, 'ir_question_hint', ?,
                              'valence_reason', ?, 'publish_risk_reason', ?
                            )),
                            value_rationale_ai)
                      WHERE id=?""",
                   [score,
                    (o.get('headline_ko', '') or '')[:100],
                    (o.get('story_angle', '') or '')[:400],
                    o.get('valence', 'MIXED'),
                    o.get('publish_risk', 'MEDIUM'),
                    (o.get('missing_verification', '') or '')[:300],
                    o.get('already_public', 'UNKNOWN'),
                    'batch', now,
                    o.get('story_type', ''),
                    o.get('urgency', ''),
                    (o.get('ir_question_hint', '') or '')[:200],
                    (o.get('valence_reason', '') or '')[:200],
                    (o.get('publish_risk_reason', '') or '')[:200],
                    r['id']])
        cnt += 1
    db.commit()
    return cnt


rows = db.execute("""
    SELECT sl.id, sl.corp_name, sl.title, sl.lead_type, sl.severity,
           sl.evidence, sl.evidence_deep, sl.numeric_facts,
           sl.company_context, sl.supply_chain_impact,
           sl.value_direction, sl.industry_alignment, sl.impact_magnitude,
           sl.short_term, sl.medium_term, sl.long_term,
           sl.cross_verified, sl.freshness_label,
           sl.fact_match_label, sl.fact_match_ratio
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH_CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND sl.n1_verified_at IS NULL
""").fetchall()
print(f'N-1 뉴스화 검증 대상: {len(rows)}건  (배치 {BATCH_SIZE} → 예상 호출 ~{(len(rows)+BATCH_SIZE-1)//BATCH_SIZE}회)')

t0 = time.time()

def progress(ok, fail, calls, exhausted=False):
    avg = sum(scores) / len(scores) if scores else 0
    el = (time.time() - t0) / 60
    tag = ' (한도 소진)' if exhausted else ''
    print(f'  ok={ok} fail={fail} 호출={calls} avg={avg:.1f} ({el:.1f}분){tag}')

try:
    ok, err, calls = process_in_batches(
        rows, item_id=lambda r: r['id'], render_item=render_item,
        instruction=INSTRUCTION, output_example=OUTPUT_EXAMPLE,
        on_results=on_results, max_items=BATCH_SIZE, max_output_tokens=8192,
        temperature=0.2, throttle=4.0, progress=progress)
    avg = sum(scores) / len(scores) if scores else 0
    print(f'\n[N-1 완료] ok={ok} fail={err} 호출={calls}회  평균 newsability={avg:.1f}  소요 {(time.time()-t0)/60:.1f}분')
except RuntimeError:
    print(f'\n[중단] Gemini 한도 소진 — 다음 실행 시 이어서(증분)')

# 결과 분포
print('\n[Newsability 분포]')
for row in db.execute("""SELECT newsability_score, COUNT(*) c FROM story_leads
    WHERE n1_verified_at IS NOT NULL GROUP BY newsability_score ORDER BY newsability_score DESC"""):
    print(f'  {row[0]:>2}점: {row[1]:>3}건 {"█"*row[1]}')

print('\n[즉시 출고 SCOOP 후보 (score>=8)]')
for row in db.execute("""SELECT corp_name, headline_ko, newsability_score, valence
    FROM story_leads WHERE n1_verified_at IS NOT NULL AND newsability_score >= 8
    ORDER BY newsability_score DESC LIMIT 20"""):
    print(f'  {row[2]}점/{row[3]} {row[0]} | {(row[1] or "")[:40]}')
