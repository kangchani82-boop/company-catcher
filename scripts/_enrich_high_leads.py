"""
scripts/_enrich_high_leads.py
──────────────────────────────
HIGH 단서 (info_gap_label='HIGH') 의 깊이감·증거·출처·컨텍스트 강화.

G-1: evidence_deep (사업보고서에서 주변 ±5문장 확장, 2,000자)
G-2: numeric_facts (수치 정규식 추출, JSON)
G-4: company_context (sector / market / 최근 실적, JSON)
G-6: source_path (보고서 + 공시일 + 추정 섹션)

API 0 — 모두 로컬 SQL/Python.
"""
import sqlite3, json, re
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row

# ─────────── 컬럼 추가 ───────────
cols = [c[1] for c in db.execute('PRAGMA table_info(story_leads)')]
for new_col in ['evidence_deep', 'numeric_facts', 'company_context',
                'source_path', 'enriched_at']:
    if new_col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {new_col} TEXT')
        print(f'  ✓ story_leads.{new_col} 컬럼 추가')

# ─────────── 도우미: 수치 추출 ───────────
PERCENT_RE = re.compile(r'(\d+(?:[.,]\d+)?)\s*(?:%|퍼센트)')
MONEY_RE = re.compile(r'(\d+(?:[.,]\d+)?)\s*(억|조|만|천)?\s*원')
CHANGE_RE = re.compile(r'(\d+(?:[.,]\d+)?)\s*[→\-~]\s*(\d+(?:[.,]\d+)?)\s*(%|배|원|개|명)?')
MULTIPLE_RE = re.compile(r'(\d+(?:[.,]\d+)?)\s*배')
GROWTH_RE = re.compile(r'(?:전년|YoY)\s*(?:대비\s*)?\s*([+\-±]?\s*\d+(?:[.,]\d+)?)\s*(%|배|p|ppt|포인트)')

def extract_numeric_facts(text):
    facts = []
    for m in CHANGE_RE.finditer(text):
        facts.append({"type":"change","from":m.group(1),"to":m.group(2),"unit":m.group(3) or ""})
    for m in PERCENT_RE.finditer(text):
        facts.append({"type":"percent","value":m.group(1)})
    for m in MONEY_RE.finditer(text):
        facts.append({"type":"money","value":m.group(1),"unit":(m.group(2) or "")+"원"})
    for m in MULTIPLE_RE.finditer(text):
        facts.append({"type":"multiple","value":m.group(1)})
    for m in GROWTH_RE.finditer(text):
        facts.append({"type":"growth_yoy","value":m.group(1).strip(),"unit":m.group(2)})
    # 중복 제거 (간단)
    seen = set()
    uniq = []
    for f in facts:
        k = json.dumps(f, sort_keys=True)
        if k in seen: continue
        seen.add(k); uniq.append(f)
    return uniq[:30]  # 최대 30개

# ─────────── 도우미: evidence_deep ───────────
SENT_SPLIT = re.compile(r'(?<=[.。!?])\s+|(?:[.。!?])\s*\n')

def extract_deep_evidence(biz_text, keywords, max_chars=2000):
    """매칭 키워드 주변 ±5문장 확장 (중복 없는 윈도우)"""
    if not biz_text or not keywords: return ""
    text = biz_text
    # 키워드 위치 찾기
    positions = []
    for kw in keywords[:5]:  # 처음 5개 키워드만
        if not kw: continue
        start = 0
        while True:
            pos = text.lower().find(kw.lower(), start)
            if pos < 0: break
            positions.append((pos, kw))
            start = pos + len(kw)
    if not positions: return text[:max_chars]
    positions.sort()

    # 첫 매칭 위치 기준 ±2000자 윈도우
    pos, kw = positions[0]
    start = max(0, pos - 800)
    end = min(len(text), pos + 1200)
    chunk = text[start:end]
    # 문장 경계로 정리
    if start > 0:
        # 가장 가까운 마침표/줄바꿈 이후로
        m = re.search(r'[.。!?\n]\s*', chunk[:200])
        if m: chunk = chunk[m.end():]
    return ("…" if start > 0 else "") + chunk[:max_chars] + ("…" if end < len(text) else "")

# ─────────── 도우미: 회사 컨텍스트 ───────────
def get_company_context(corp_code):
    c = db.execute("""SELECT corp_name, sector, market, induty_code, ceo_nm, est_dt,
                             corp_name_eng
                      FROM companies WHERE corp_code=?""", [corp_code]).fetchone()
    if not c: return {}
    ctx = dict(c)
    # 최근 재무
    fin = db.execute("""SELECT fiscal_year, report_type, revenue, operating_income,
                               net_income, operating_margin, roe, debt_ratio
                        FROM financials WHERE corp_code=?
                        ORDER BY fiscal_year DESC, report_type DESC LIMIT 1""", [corp_code]).fetchone()
    if fin:
        ctx['recent_financials'] = dict(fin)
        # 매출 단위 정리 (원 → 억 원)
        if ctx['recent_financials'].get('revenue'):
            ctx['recent_financials']['revenue_billion_krw'] = round(ctx['recent_financials']['revenue']/1e8, 1)
        if ctx['recent_financials'].get('operating_income'):
            ctx['recent_financials']['operating_income_billion_krw'] = round(ctx['recent_financials']['operating_income']/1e8, 1)
    # ir_contacts 보유 여부
    has_ir = db.execute("""SELECT COUNT(*) FROM ir_contacts
                           WHERE corp_code=? AND is_active=1
                             AND ir_email IS NOT NULL AND ir_email<>''
                             AND substr(ir_email,1,1)<>'_'""", [corp_code]).fetchone()[0]
    ctx['has_ir_email'] = has_ir > 0
    ctx['ir_count'] = has_ir
    return ctx

# ─────────── 도우미: 출처 경로 ───────────
REPORT_TYPE_KO = {
    '2025_annual': '2024년 사업보고서',
    '2025_q1': '2025 1분기보고서',
    '2025_h1': '2025 반기보고서',
    '2025_q3': '2025 3분기보고서',
    '2026_annual': '2025년 사업보고서',
    '2026_q1': '2026 1분기보고서',
    '2026_h1': '2026 반기보고서',
    '2026_q3': '2026 3분기보고서',
}
def build_source_path(corp_name, report_type_b, comparison_id):
    """단서가 어느 비교에서 왔는지, 어느 보고서 발췌인지"""
    cmp = db.execute("""SELECT ac.report_type_a, ac.report_type_b, ra.rcept_dt as dt_a, rb.rcept_dt as dt_b
                        FROM ai_comparisons ac
                        LEFT JOIN reports ra ON ra.id=ac.report_id_a
                        LEFT JOIN reports rb ON rb.id=ac.report_id_b
                        WHERE ac.id=?""", [comparison_id]).fetchone()
    if not cmp:
        return {"primary_report": REPORT_TYPE_KO.get(report_type_b, report_type_b)}
    src = {
        "primary_report": REPORT_TYPE_KO.get(cmp['report_type_b'], cmp['report_type_b']),
        "comparison_against": REPORT_TYPE_KO.get(cmp['report_type_a'], cmp['report_type_a']),
        "filing_date_recent": cmp['dt_b'],
        "filing_date_prior":  cmp['dt_a'],
        "section": "III. 사업의 내용 — 1. 사업의 개요 / 2. 주요 제품·매출 / 3. 원재료 / 4. 생산·설비 / 5. 위험관리",
    }
    # 시차 계산
    try:
        from datetime import datetime as dt
        d_a = dt.strptime(cmp['dt_a'], '%Y%m%d') if cmp['dt_a'] else None
        d_b = dt.strptime(cmp['dt_b'], '%Y%m%d') if cmp['dt_b'] else None
        if d_a and d_b:
            months = (d_b.year-d_a.year)*12 + (d_b.month-d_a.month)
            src['time_gap_months'] = months
    except: pass
    return src

# ─────────── 메인 ───────────
def main():
    high_leads = db.execute("""
        SELECT id, corp_code, corp_name, keywords, evidence, comparison_id,
               report_type_b, title
        FROM story_leads
        WHERE info_gap_label LIKE 'HIGH%'
          AND (enriched_at IS NULL OR evidence_deep IS NULL)
        ORDER BY severity DESC, id DESC
    """).fetchall()
    print(f'HIGH 단서 강화 대상: {len(high_leads)}건\n')

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ok = 0; fail = 0
    for r in high_leads:
        try:
            keywords = json.loads(r['keywords'] or '[]') if r['keywords'] else []
            # biz_content (해당 비교의 b 보고서)
            biz_row = db.execute("""SELECT rb.biz_content FROM ai_comparisons ac
                                    JOIN reports rb ON rb.id=ac.report_id_b
                                    WHERE ac.id=?""", [r['comparison_id']]).fetchone()
            biz = biz_row['biz_content'] if biz_row else ''

            ev_deep = extract_deep_evidence(biz, keywords + [r['title'] or ''])
            num_facts = extract_numeric_facts((r['evidence'] or '') + ev_deep)
            ctx = get_company_context(r['corp_code'])
            src = build_source_path(r['corp_name'], r['report_type_b'], r['comparison_id'])

            db.execute("""UPDATE story_leads SET
                            evidence_deep=?, numeric_facts=?, company_context=?,
                            source_path=?, enriched_at=?
                          WHERE id=?""",
                       [ev_deep, json.dumps(num_facts, ensure_ascii=False),
                        json.dumps(ctx, ensure_ascii=False, default=str),
                        json.dumps(src, ensure_ascii=False, default=str),
                        now, r['id']])
            ok += 1
            if ok % 50 == 0:
                db.commit()
                print(f'  진행 {ok}/{len(high_leads)}')
        except Exception as e:
            fail += 1
            print(f'  ✗ id={r["id"]}: {str(e)[:80]}')

    db.commit()
    print(f'\n[완료] 강화 {ok}건 / 실패 {fail}건')

    # 검증 샘플
    print('\n[샘플 — HIGH 단서 1건]')
    s = db.execute("""SELECT corp_name, evidence, evidence_deep, numeric_facts, source_path
                      FROM story_leads
                      WHERE info_gap_label='HIGH' AND evidence_deep IS NOT NULL
                      ORDER BY severity DESC LIMIT 1""").fetchone()
    if s:
        print(f"  회사: {s['corp_name']}")
        print(f"  evidence(원래): {(s['evidence'] or '')[:200]}")
        print(f"  evidence_deep(강화): {(s['evidence_deep'] or '')[:300]}...")
        print(f"  수치 추출: {(s['numeric_facts'] or '')[:200]}")
        print(f"  출처: {(s['source_path'] or '')[:300]}")

if __name__ == "__main__":
    main()
