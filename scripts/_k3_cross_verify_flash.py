"""
K-3b: 골드 단서 cross-verify (배치 버전)
- CONFIRMED / NEED_MORE / REJECT 분류
- ★ 배치: 12건을 한 Gemini 호출로 묶어 처리 (호출 1/12) — scripts/_gemini_batch.py 사용
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

BATCH_SIZE = 12  # verdict 위주 짧은 출력 → 12건 안전

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

cols = [c[1] for c in db.execute('PRAGMA table_info(story_leads)')]
for col in ['cross_verified', 'cross_verify_model', 'cross_verify_at', 'cross_evidence']:
    if col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {col} TEXT')

INSTRUCTION = """당신은 한국 증권부 시니어 에디터입니다. 각 취재 단서를 **보수적 관점**으로 평가하세요.

판단 기준:
- CONFIRMED: 원문에 명확한 변화+근거, 회사 가치 영향 분명
- NEED_MORE: 변화 시그널 있으나 추가 확인(재무·뉴스·IR) 필요
- REJECT: 단순 서술·계획 단계·증거 부족
- confidence 5=즉시 출고 / 3=후속 확인 / 1=폐기 권장"""

OUTPUT_EXAMPLE = """{
  "id": <입력 id 그대로>,
  "verdict": "CONFIRMED|NEED_MORE|REJECT",
  "confidence": 1~5,
  "core_fact": "팩트 1문장 (50자 이내)",
  "missing_evidence": "추가 확인 자료 (없으면 NONE)",
  "risk_note": "발송 전 주의점 (없으면 NONE)"
}"""


def render_item(r):
    return (f"[회사] {r['corp_name']}\n"
            f"[단서 제목] {r['title'] or ''}\n"
            f"[분류] {r['lead_type']}  /  [심각도] {r['severity']}/5\n"
            f"[미래 가치 방향] {r['value_direction'] or '-'}  /  [산업 트렌드 대비] {r['industry_alignment'] or '-'}\n"
            f"[Evidence] {(r['evidence'] or '')[:500]}\n"
            f"[원문 발췌] {(r['evidence_deep'] or '')[:1500]}")


stats = {"CONFIRMED": 0, "NEED_MORE": 0, "REJECT": 0}


def on_results(matched):
    cnt = 0
    for r, o in matched:
        verdict = o.get("verdict", "NEED_MORE")
        if verdict not in stats:
            verdict = "NEED_MORE"
        stats[verdict] += 1
        ev = json.dumps({
            "core_fact":  o.get("core_fact", ""),
            "missing":    o.get("missing_evidence", ""),
            "risk":       o.get("risk_note", "NONE"),
            "confidence": o.get("confidence", 0),
        }, ensure_ascii=False)
        db.execute("""UPDATE story_leads SET
                        cross_verified=?, cross_verify_model=?, cross_verify_at=?, cross_evidence=?
                      WHERE id=?""",
                   [verdict, o.get("_model", "batch"), now, ev, r['id']])
        cnt += 1
    db.commit()
    return cnt


golds = db.execute("""
    SELECT id, corp_name, title, lead_type, severity, evidence, evidence_deep,
           value_direction, industry_alignment
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED'
      AND fact_match_label IN ('EXACT','STRONG')
      AND cross_verified IS NULL
""").fetchall()
print(f'K-3 cross-verify 대상: {len(golds)}건  (배치 {BATCH_SIZE} → 예상 호출 ~{(len(golds)+BATCH_SIZE-1)//BATCH_SIZE}회)')

t0 = time.time()

def progress(ok, fail, calls, exhausted=False):
    el = (time.time() - t0) / 60
    tag = ' (한도 소진)' if exhausted else ''
    print(f'  ok={ok} fail={fail} 호출={calls} CONFIRMED={stats["CONFIRMED"]} NEED_MORE={stats["NEED_MORE"]} REJECT={stats["REJECT"]} ({el:.1f}분){tag}')

try:
    ok, err, calls = process_in_batches(
        golds, item_id=lambda r: r['id'], render_item=render_item,
        instruction=INSTRUCTION, output_example=OUTPUT_EXAMPLE,
        on_results=on_results, max_items=BATCH_SIZE, max_output_tokens=8192,
        temperature=0.1, throttle=4.0, progress=progress)
    print(f'\n[K-3 완료] ok={ok} fail={err} 호출={calls}회  소요 {(time.time()-t0)/60:.1f}분')
    print(f'  CONFIRMED={stats["CONFIRMED"]} NEED_MORE={stats["NEED_MORE"]} REJECT={stats["REJECT"]}')
except RuntimeError:
    print(f'\n[중단] Gemini 한도 소진 — 다음 실행 시 이어서(증분)')

# ULTRA GOLD
ultra = db.execute("""
    SELECT COUNT(*) FROM story_leads sl
    WHERE sl.cross_verified='CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND EXISTS (SELECT 1 FROM ir_contacts ic
                  WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
                    AND ic.ir_email IS NOT NULL AND ic.ir_email<>''
                    AND substr(ic.ir_email,1,1)<>'_')
""").fetchone()[0]
print(f'\n🏆 ULTRA GOLD (CONFIRMED + IR 이메일): {ultra}건')
