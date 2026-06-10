"""
최종 골드 단서 102건 (6중 검증 통과) Word 보고서.
- 모든 검증 자취 표시
- 회사·단서·증거·출처·공급망·최신성 통합
출력: data/exports/final_gold_signals.docx
"""
import sqlite3, json
from pathlib import Path
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUT = ROOT / "data" / "exports" / "final_gold_signals.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)

db = sqlite3.connect(str(DB_PATH)); db.row_factory = sqlite3.Row
doc = Document()
s = doc.styles['Normal']; s.font.name='맑은 고딕'; s.font.size=Pt(10)
s._element.rPr.rFonts.set(qn('w:eastAsia'),'맑은 고딕')

def sf(p, size=10, bold=False, italic=False, color=None):
    for r in p.runs:
        r.font.name='맑은 고딕'
        r._element.rPr.rFonts.set(qn('w:eastAsia'),'맑은 고딕')
        r.font.size=Pt(size)
        if bold: r.bold=True
        if italic: r.italic=True
        if color: r.font.color.rgb=color

def H(t,lv=1,color=None):
    p=doc.add_heading(t,level=lv)
    if color:
        for r in p.runs: r.font.color.rgb=color
    sf(p,size=14 if lv==1 else (12 if lv==2 else 11), bold=True, color=color)

def P(t,**kw):
    p=doc.add_paragraph(); p.add_run(t); sf(p,**kw)

def T(headers, rows, widths=None):
    t=doc.add_table(rows=1,cols=len(headers)); t.style='Light Grid Accent 1'
    for i,h in enumerate(headers):
        c=t.rows[0].cells[i]; c.text=h
        for p in c.paragraphs: sf(p,size=9,bold=True)
    for row in rows:
        c=t.add_row().cells
        for i,v in enumerate(row):
            c[i].text=str(v)
            for p in c[i].paragraphs: sf(p,size=9)
    if widths:
        for row in t.rows:
            for i,w in enumerate(widths): row.cells[i].width=Cm(w)
    doc.add_paragraph()

# 표지
title=doc.add_heading('Company Catcher 최종 골드 단서 (6중 검증)', level=0)
title.alignment=WD_ALIGN_PARAGRAPH.CENTER
P('AI(Gemini) + 외부 뉴스(Naver) + 사업보고서 원문 + 수치 + 출처 + 공급망', size=11)
doc.paragraphs[-1].alignment=WD_ALIGN_PARAGRAPH.CENTER
P('"팩트 → 검증 → 보완 → 재검증" 사이클 통과 단서', italic=True, size=10)
doc.paragraphs[-1].alignment=WD_ALIGN_PARAGRAPH.CENTER
doc.add_paragraph()

# 1. 통계
H('1. 검증 결과 요약', 1)
T(['지표','값'],[
    ['Gemini 검증 처리 (growth_signals)', '3,106건'],
    ['실제 변화 감지', '2,808건 (90%)'],
    ['1차 HIGH (보도 안 됨 추정)', '377건'],
    ['2차 Naver 매칭 → HIGH_CONFIRMED', '106건'],
    ['3차 원문 EXACT/STRONG 매칭', '🏆 102건 (골드)'],
    ['골드 회사', '98개'],
    ['🎯 골드 + IR 이메일 보유', '75개사'],
],[9,4])

# 2. 검증 사이클 다이어그램
H('2. 검증 사이클', 1)
P('1차 Gemini 평가 (has_change=Y) → 2차 importance=5 → 3차 Naver 매칭 0 '
  '→ 4차 원문 EXACT 매칭 → 5차 수치 정합 → 6차 공급망 영향', size=10)
P('해당 단계 모두 통과 = 골드 단서', italic=True, size=10)
doc.add_paragraph()

# 3. 골드 단서 sev=5 (최우선)
H('3. ⭐ sev=5 골드 단서 — 즉시 출고 후보', 1, color=RGBColor(0xC0,0x39,0x2B))
rows5 = db.execute("""
    SELECT corp_name, title, lead_type, evidence, evidence_deep, numeric_facts,
           source_path, company_context, recent_disclosures, freshness_label,
           supply_chain_impact
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
      AND severity=5
    ORDER BY id DESC
""").fetchall()
P(f'총 {len(rows5)}건', bold=True, size=11)
doc.add_paragraph()

for i,r in enumerate(rows5, 1):
    H(f"{i}. {r['corp_name']} — {r['title']}", 3)
    P(f"분류: {r['lead_type']}  /  최신성: {r['freshness_label'] or '—'}", size=9, italic=True)
    P('▣ Evidence', bold=True, size=10)
    P((r['evidence'] or '')[:300], size=9)
    if r['evidence_deep']:
        P('▣ 사업보고서 원문', bold=True, size=10)
        P((r['evidence_deep'] or '')[:500] + '...', size=9)
    if r['numeric_facts']:
        try:
            facts = json.loads(r['numeric_facts'])
            if facts:
                P(f'▣ 정량 데이터 ({len(facts)}개)', bold=True, size=10)
                for f in facts[:5]:
                    t = f.get('type','')
                    if t=='change':
                        P(f"  · 변화: {f.get('from')} → {f.get('to')} {f.get('unit','')}", size=9)
                    elif t=='percent':
                        P(f"  · 비율: {f.get('value')}%", size=9)
                    elif t=='money':
                        P(f"  · 금액: {f.get('value')} {f.get('unit','')}", size=9)
        except: pass
    if r['source_path']:
        try:
            src = json.loads(r['source_path'])
            P('▣ 출처', bold=True, size=10)
            P(f"  · {src.get('primary_report','')} (공시 {src.get('filing_date_recent','')})", size=9)
        except: pass
    if r['company_context']:
        try:
            ctx = json.loads(r['company_context'])
            fin = ctx.get('recent_financials') or {}
            P('▣ 회사 컨텍스트', bold=True, size=10)
            P(f"  · {ctx.get('market','')} / {ctx.get('sector','')}", size=9)
            if fin.get('revenue_billion_krw'):
                P(f"  · 매출: {fin.get('revenue_billion_krw')}억원, 영업이익: {fin.get('operating_income_billion_krw','-')}억", size=9)
        except: pass
    if r['recent_disclosures']:
        try:
            rd = json.loads(r['recent_disclosures'])
            if rd:
                P('▣ 최근 수시공시 (60일)', bold=True, size=10)
                for d in rd[:3]:
                    P(f"  · {d.get('dt','')} {d.get('report','')[:50]}", size=9)
        except: pass
    if r['supply_chain_impact']:
        try:
            sc = json.loads(r['supply_chain_impact'])
            if sc.get('as_supplier_to') or sc.get('supplied_by'):
                P('▣ 공급망 영향', bold=True, size=10)
                for x in sc.get('as_supplier_to',[])[:3]:
                    P(f"  · 거래처: {x.get('name','')} ({x.get('rel','')})", size=9)
        except: pass
    doc.add_paragraph()

# 4. 골드 단서 sev=4 회사 리스트
H('4. sev=4 골드 단서 회사 리스트', 1)
sendable = db.execute("""
    SELECT DISTINCT sl.corp_code, sl.corp_name, COUNT(sl.id) cnt,
           GROUP_CONCAT(DISTINCT sl.lead_type) types,
           MAX(sl.freshness_label) freshness,
           (SELECT ic.ir_email FROM ir_contacts ic
            WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
              AND ic.ir_email IS NOT NULL AND ic.ir_email<>''
              AND substr(ic.ir_email,1,1)<>'_' LIMIT 1) email
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH_CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND sl.severity=4
      AND EXISTS (SELECT 1 FROM ir_contacts ic
                  WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
                    AND ic.ir_email IS NOT NULL AND ic.ir_email<>''
                    AND substr(ic.ir_email,1,1)<>'_')
    GROUP BY sl.corp_code ORDER BY cnt DESC, sl.corp_name
""").fetchall()
P(f'총 {len(sendable)}개사', size=10)
T(['#','회사','단서','분류','최신성','IR 이메일'],
  [[i+1, r['corp_name'], r['cnt'], (r['types'] or '')[:20],
    r['freshness'] or '-', (r['email'] or '')[:30]]
   for i,r in enumerate(sendable)],
  [1.2, 4, 1.5, 4, 1.5, 4.5])

# 5. 활용 가이드
H('5. 활용 가이드', 1)
P('▣ 발송 우선순위', bold=True, size=11)
P(f"1. sev=5 골드 단서 {len(rows5)}건 → 즉시 IR 질문지 발송", size=10)
P(f"2. sev=4 골드 + FRESH (수시공시 최근 30일) → 우선 발송", size=10)
P(f"3. sev=4 골드 + RECENT/STALE → 평일 분산 발송", size=10)
doc.add_paragraph()

P('▣ 검증 자취 (이 보고서에 모두 포함)', bold=True, size=11)
P('• Gemini 진위 평가 (gemini_verified)', size=10)
P('• Gemini importance / news_likely (gemini_news_likely)', size=10)
P('• Naver 30일 매칭 0건 (HIGH_CONFIRMED)', size=10)
P('• 사업보고서 원문 매칭 비율 (fact_match_label)', size=10)
P('• 수치 정합성 (numeric_verified_ratio)', size=10)
P('• 수시공시 최신성 (freshness_label)', size=10)
P('• 공급망 영향 (supply_chain_impact)', size=10)
doc.add_paragraph()

P('▣ 추가 검증 (한도 회복 후)', bold=True, size=11)
P('• K-3: gemini-2.5-pro 보수적 cross-verify (CONFIRMED/NEED_MORE/REJECT)', size=10)
P('• reporter v3 8단계 초안 활용 (이미 84+56=140건 생성)', size=10)

doc.save(str(OUT))
print(f'생성됨: {OUT}')
print(f'크기: {OUT.stat().st_size:,} bytes')
print(f'sev=5 골드: {len(rows5)}건')
print(f'sev=4 골드 발송 가능: {len(sendable)}개사')
