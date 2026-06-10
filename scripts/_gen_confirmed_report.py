"""
HIGH_CONFIRMED 단서 (Gemini + Naver 이중 검증) 발송 후보 Word.
출력: data/exports/confirmed_info_gap.docx
"""
import sqlite3, json
from pathlib import Path
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUT = ROOT / "data" / "exports" / "confirmed_info_gap.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)

db = sqlite3.connect(str(DB_PATH)); db.row_factory = sqlite3.Row

doc = Document()
s = doc.styles['Normal']; s.font.name='맑은 고딕'; s.font.size=Pt(10)
s._element.rPr.rFonts.set(qn('w:eastAsia'),'맑은 고딕')

def setf(p, size=10, bold=False, italic=False, color=None):
    for r in p.runs:
        r.font.name='맑은 고딕'; r._element.rPr.rFonts.set(qn('w:eastAsia'),'맑은 고딕')
        r.font.size=Pt(size)
        if bold: r.bold=True
        if italic: r.italic=True
        if color: r.font.color.rgb=color
def H(t,lv=1,color=None):
    p=doc.add_heading(t,level=lv)
    if color:
        for r in p.runs: r.font.color.rgb=color
    setf(p, size=14 if lv==1 else (12 if lv==2 else 11), bold=True, color=color)
def P(t,**kw):
    p=doc.add_paragraph(); p.add_run(t); setf(p,**kw)
def T(headers, rows, widths=None):
    t=doc.add_table(rows=1,cols=len(headers)); t.style='Light Grid Accent 1'
    for i,h in enumerate(headers):
        c=t.rows[0].cells[i]; c.text=h
        for p in c.paragraphs: setf(p,size=9,bold=True)
    for row in rows:
        c=t.add_row().cells
        for i,v in enumerate(row):
            c[i].text=str(v)
            for p in c[i].paragraphs: setf(p,size=9)
    if widths:
        for row in t.rows:
            for i,w in enumerate(widths): row.cells[i].width=Cm(w)
    doc.add_paragraph()

# 표지
title=doc.add_heading('Company Catcher — 진짜 정보 격차 단서 (이중 검증)', level=0)
title.alignment=WD_ALIGN_PARAGRAPH.CENTER
P('Gemini 진위 검증 ✓  +  Naver 뉴스 30일 매칭 0 = HIGH_CONFIRMED', size=11)
doc.paragraphs[-1].alignment=WD_ALIGN_PARAGRAPH.CENTER
P('즉시 발송·취재 가능한 우선 단서 정리', italic=True, size=10)
doc.paragraphs[-1].alignment=WD_ALIGN_PARAGRAPH.CENTER
doc.add_paragraph()

# 1. 요약
H('1. 통계 요약', 1)
T(['지표','값'],[
    ['총 HIGH 단서 (1차 분류)', '293건'],
    ['🔥 HIGH_CONFIRMED (Naver 매칭 0)', '85건'],
    ['HIGH_PARTIAL (1-2건 매칭)', '57건'],
    ['HIGH_REPORTED (3건+ 매칭)', '151건'],
    ['HIGH_CONFIRMED + IR 이메일 보유', '72개사'],
    ['🌟 HIGH_CONFIRMED + sev=5', '8건 (최우선)'],
],[8,4])

# 2. 검증 방법
H('2. 검증 방법론', 1)
P('▣ 1차 검증 — Gemini (사업보고서 분석)', bold=True, size=11)
P('• 14 카테고리 키워드 매칭', size=10)
P('• 각 단서를 Gemini가 평가: 진위 / 중요도 / 보도 여부 추정', size=10)
P('• news_likely=N → HIGH 라벨 부여', size=10)
doc.add_paragraph()
P('▣ 2차 검증 — Naver 뉴스 실 매칭 (최근 30일)', bold=True, size=11)
P('• 각 HIGH 단서 회사명 + 핵심 키워드로 Naver 검색', size=10)
P('• 매칭 0건 → HIGH_CONFIRMED (진짜 격차)', size=10)
P('• 매칭 1-2건 → HIGH_PARTIAL (부분 보도)', size=10)
P('• 매칭 3건+ → HIGH_REPORTED (후속 가치)', size=10)
doc.add_paragraph()
P('▣ Gemini 추정 정확도', bold=True, size=11)
P('• news_likely=N 293건 중 실제 매칭 0 = 85건 (29%)', size=10)
P('• → Gemini 추정만으로는 부족, Naver 실 매칭 필수 검증 단계', size=10)
doc.add_paragraph()

# 3. HIGH_CONFIRMED + sev=5 (최우선 8건) — 전체 상세
H('3. ⭐ 최우선 발송 후보 — sev=5 HIGH_CONFIRMED 8건', 1, color=RGBColor(0xC0,0x39,0x2B))
P('Gemini + Naver 이중 검증 통과 + 최고 심각도. 즉시 IR 질문지 발송 가능.', italic=True, size=10)
doc.add_paragraph()

rows = db.execute("""
    SELECT id, corp_code, corp_name, title, lead_type, evidence,
           evidence_deep, numeric_facts, source_path, company_context
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND severity=5
    ORDER BY id DESC
""").fetchall()

for i, r in enumerate(rows, 1):
    H(f"{i}. {r['corp_name']} — {r['title']}", 3)
    P(f"분류: {r['lead_type']}", italic=True, size=9)
    P('▣ Evidence (원본 요약)', bold=True, size=10)
    P((r['evidence'] or '')[:300], size=9)

    if r['evidence_deep']:
        P('▣ 사업보고서 원문 (강화 evidence)', bold=True, size=10)
        P((r['evidence_deep'] or '')[:600] + '...', size=9)

    if r['numeric_facts']:
        try:
            facts = json.loads(r['numeric_facts'])
            if facts:
                P(f"▣ 정량 데이터: {len(facts)}개 추출", bold=True, size=10)
                for f in facts[:5]:
                    if f.get('type')=='change':
                        P(f"  · 변화: {f.get('from')} → {f.get('to')} {f.get('unit','')}", size=9)
                    elif f.get('type')=='percent':
                        P(f"  · 비율: {f.get('value')}%", size=9)
                    elif f.get('type')=='money':
                        P(f"  · 금액: {f.get('value')} {f.get('unit','')}", size=9)
        except: pass

    if r['source_path']:
        try:
            src = json.loads(r['source_path'])
            P('▣ 출처', bold=True, size=10)
            P(f"  · 주 보고서: {src.get('primary_report','')} (공시 {src.get('filing_date_recent','')})", size=9)
            P(f"  · 비교: {src.get('comparison_against','')} (공시 {src.get('filing_date_prior','')})", size=9)
            if src.get('time_gap_months'):
                P(f"  · 시차: {src['time_gap_months']}개월", size=9)
        except: pass

    if r['company_context']:
        try:
            ctx = json.loads(r['company_context'])
            P('▣ 회사 컨텍스트', bold=True, size=10)
            P(f"  · 시장: {ctx.get('market','-')} / 업종: {ctx.get('sector','-')}", size=9)
            fin = ctx.get('recent_financials')
            if fin:
                P(f"  · 최근 실적: 매출 {fin.get('revenue_billion_krw','-')}억 / 영업이익 {fin.get('operating_income_billion_krw','-')}억", size=9)
            P(f"  · IR 이메일 보유: {'✓' if ctx.get('has_ir_email') else '✗'} ({ctx.get('ir_count',0)}건)", size=9)
        except: pass

    doc.add_paragraph()

# 4. HIGH_CONFIRMED + sev=4 — 발송 후보 풀 전체 회사 리스트
H('4. HIGH_CONFIRMED + sev=4 단서 회사 리스트', 1)
P(f'총 77건 / 72개사. 회사명·단서·IR 이메일 한눈 보기.', italic=True, size=10)

sendable = db.execute("""
    SELECT DISTINCT sl.corp_code, sl.corp_name, COUNT(sl.id) as cnt,
           GROUP_CONCAT(DISTINCT sl.lead_type) as types,
           (SELECT ic.ir_email FROM ir_contacts ic
            WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
              AND ic.ir_email IS NOT NULL AND ic.ir_email<>''
              AND substr(ic.ir_email,1,1)<>'_'
            LIMIT 1) as email
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH_CONFIRMED'
      AND sl.severity>=4
      AND EXISTS (SELECT 1 FROM ir_contacts ic
                  WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
                    AND ic.ir_email IS NOT NULL AND ic.ir_email<>''
                    AND substr(ic.ir_email,1,1)<>'_')
    GROUP BY sl.corp_code
    ORDER BY cnt DESC, sl.corp_name
""").fetchall()
T(['#','회사','단서수','분류','IR 이메일'],
  [[i+1, r['corp_name'], r['cnt'], (r['types'] or '')[:25], (r['email'] or '')[:35]]
   for i,r in enumerate(sendable)],
  [1.2, 4.5, 1.3, 4.5, 5])

# 5. HIGH_PARTIAL — 부분 보도 (후속 가치)
H('5. HIGH_PARTIAL — 후속 취재 후보 TOP 20', 1)
P('Naver 매칭 1-2건. 보도됐지만 깊이 부족 → 후속 보강 가치.', italic=True, size=10)

partial = db.execute("""
    SELECT corp_name, title, lead_type, actual_news_count
    FROM story_leads
    WHERE info_gap_label='HIGH_PARTIAL'
    ORDER BY severity DESC, id DESC LIMIT 20
""").fetchall()
T(['#','회사','단서','분류','매칭'],
  [[i+1, r['corp_name'], (r['title'] or '')[:40], r['lead_type'], r['actual_news_count']]
   for i,r in enumerate(partial)],
  [1.2, 4, 5, 3, 1.5])

# 6. 활용 가이드
H('6. 활용 가이드', 1)
P('▣ 우선순위 (발송 순서)', bold=True, size=11)
P('1. sev=5 HIGH_CONFIRMED 8건 — 즉시 (이번 주)', size=10)
P('2. sev=4 HIGH_CONFIRMED 77건 — 평일 분산 발송', size=10)
P('3. HIGH_PARTIAL — 후속 취재 패키지로 묶어 발송', size=10)
doc.add_paragraph()

P('▣ 발송 검수 체크리스트', bold=True, size=11)
P('  □ evidence_deep 보고서 원문과 매칭 확인', size=10)
P('  □ 정량 데이터 수치 일관성 확인 (numeric_facts)', size=10)
P('  □ IR 이메일 active=1 / bounced<3 확인', size=10)
P('  □ 24h 중복 발송 차단 확인 (자동)', size=10)
doc.add_paragraph()

P('▣ 검증 한계 — 인지 필요', bold=True, size=11)
P('• Naver 검색이 모든 매체 커버 X (전자공시 / 영문 매체 제외)', size=10)
P('• 30일 cutoff — 그 이전 보도는 신규 변화일 수 있음', size=10)
P('• 회사명 동음이의어 가능 (예: "OO" 일반 단어와 충돌)', size=10)

doc.save(str(OUT))
print(f'생성됨: {OUT}')
print(f'크기: {OUT.stat().st_size:,} bytes')
print(f'sev=5 HIGH_CONFIRMED: {len(rows)}건')
print(f'발송 후보 (sev>=4 + 이메일): {len(sendable)}개사')
