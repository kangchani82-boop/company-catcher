"""
scripts/_gen_sendable_report.py
─────────────────────────────────
발송 후보 235개사 (HIGH 단서 + IR 이메일) Word 보고서.
출력: data/exports/sendable_candidates.docx
"""
import sqlite3, json
from pathlib import Path
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUT = ROOT / "data" / "exports" / "sendable_candidates.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row

doc = Document()
s = doc.styles['Normal']; s.font.name = '맑은 고딕'; s.font.size = Pt(10)
s._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')

def setfont(p, size=10, bold=False, color=None, italic=False):
    for r in p.runs:
        r.font.name = '맑은 고딕'
        r._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')
        r.font.size = Pt(size)
        if bold: r.bold = True
        if italic: r.italic = True
        if color: r.font.color.rgb = color

def H(t, lv=1, color=None):
    p = doc.add_heading(t, level=lv)
    if color:
        for run in p.runs: run.font.color.rgb = color
    setfont(p, size=14 if lv==1 else (12 if lv==2 else 11), bold=True, color=color)
def P(t, **kw):
    p = doc.add_paragraph(); r = p.add_run(t)
    setfont(p, **kw)

def T(headers, rows, widths=None):
    t = doc.add_table(rows=1, cols=len(headers)); t.style='Light Grid Accent 1'
    for i,h in enumerate(headers):
        c = t.rows[0].cells[i]; c.text = h
        for para in c.paragraphs: setfont(para, size=9, bold=True)
    for row in rows:
        c = t.add_row().cells
        for i,v in enumerate(row):
            c[i].text = str(v)
            for para in c[i].paragraphs: setfont(para, size=9)
    if widths:
        for row in t.rows:
            for i,w in enumerate(widths): row.cells[i].width = Cm(w)
    doc.add_paragraph()

# 표지
title = doc.add_heading('Company Catcher 발송 후보 단서 정리', level=0)
title.alignment = WD_ALIGN_PARAGRAPH.CENTER
P('— HIGH 단서 + IR 이메일 보유 회사 235개사 —', size=11)
doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
P('Gemini 진위 검증 Y + 보도 안 됨 추정 = 즉시 취재·발송 가능', italic=True, size=10)
doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
doc.add_paragraph()

H('1. 통계 요약', 1)
T(['지표','값'],[
    ['HIGH 단서 (즉시 취재)', '293건'],
    ['HIGH + IR 이메일 보유 회사', '235개사'],
    ['MEDIUM 단서', '68건'],
    ['LOW 단서 (미검증/보류)', '1,491건'],
    ['Gemini growth_signals 누적', '3,106건'],
    ['추가 발굴 잠재 회사', '205개사'],
],[8,4])

H('2. 발송 후보 회사 전체 리스트 (235개사)', 1, color=RGBColor(0xC0,0x39,0x2B))
P('각 회사의 HIGH 단서 + IR 이메일 + 단서 유형 정리. 즉시 IR 질문지 발송 가능.', italic=True, size=10)
doc.add_paragraph()

# 발송 후보 회사 목록
sendable = db.execute('''
    SELECT DISTINCT sl.corp_code, sl.corp_name,
           GROUP_CONCAT(DISTINCT sl.lead_type) as types,
           COUNT(sl.id) as cnt,
           (SELECT GROUP_CONCAT(DISTINCT ic.ir_email) FROM ir_contacts ic
            WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
              AND ic.ir_email IS NOT NULL AND ic.ir_email<>'' AND substr(ic.ir_email,1,1)<>'_') as emails
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH'
      AND EXISTS (
        SELECT 1 FROM ir_contacts ic
        WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
          AND ic.ir_email IS NOT NULL AND ic.ir_email<>'' AND substr(ic.ir_email,1,1)<>'_'
      )
    GROUP BY sl.corp_code
    ORDER BY cnt DESC, sl.corp_name
''').fetchall()

T(['#','회사명','단서수','lead_type','대표 IR 이메일'],
  [[i+1, r['corp_name'], r['cnt'], r['types'][:30] if r['types'] else '',
    (r['emails'] or '').split(',')[0][:35]]
   for i, r in enumerate(sendable)],
  [1.2, 4.5, 1.5, 4.5, 5])

H('3. HIGH 단서 상세 — TOP 50 (최신순)', 1)
P('각 단서의 evidence + 추천 취재 포인트 수록.', italic=True, size=10)
doc.add_paragraph()

leads = db.execute('''
    SELECT sl.corp_name, sl.title, sl.severity, sl.lead_type, sl.evidence,
           sl.gemini_news_likely, sl.gemini_angles
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH'
    ORDER BY sl.severity DESC, sl.id DESC
    LIMIT 50
''').fetchall()

for i, r in enumerate(leads, 1):
    H(f"{i}. [{r['severity']}] {r['corp_name']} — {r['title'] or '(no title)'}", 3)
    P(f"분류: {r['lead_type']}  /  보도 추정: {r['gemini_news_likely'] or '—'}", size=9, italic=True)
    if r['evidence']:
        P(f"근거: {r['evidence'][:300]}", size=9)
    if r['gemini_angles']:
        try:
            angles = json.loads(r['gemini_angles'])
            for a in angles[:3]:
                P(f"  → {a[:150]}", size=9)
        except:
            pass

H('4. 추가 발굴 후보 회사 (growth_signals importance=5 / 단서 약함)', 1)
P('아래 회사들은 growth_signals에서 importance=5 + 보도 안 됨으로 잡혔지만 '
  'story_leads 단서가 sev≥4로 잡히지 않은 경우 — 단서 풀에 신규 추가 후보.', italic=True, size=9)
doc.add_paragraph()

extra = db.execute('''
    SELECT gs.corp_name, gs.change_categories, gs.evidence
    FROM growth_signals gs
    WHERE gs.importance=5 AND gs.has_change=1 AND gs.news_likely='N'
      AND NOT EXISTS (SELECT 1 FROM story_leads sl
                      WHERE sl.corp_code=gs.corp_code AND sl.severity>=4)
    ORDER BY gs.id DESC
    LIMIT 50
''').fetchall()

KO = {'new_product':'신규 제품','new_business':'신규 사업','facility_investment':'시설투자',
      'customer_change':'고객 변화','new_project':'신규 과제','exit_business':'사업철수',
      'global_customer':'글로벌 고객','corp_acquire':'기업 인수','new_patent':'신규 특허',
      'corp_setup':'법인 설립','regulator_risk':'규제 리스크','corp_close_sell':'법인 매각',
      'gov_grant':'정부 과제','equity_invest':'타법인 출자'}

for i, r in enumerate(extra, 1):
    cats = json.loads(r['change_categories'] or '[]')
    cat_ko = ' · '.join(KO.get(c,c) for c in cats[:2])
    H(f"{i}. {r['corp_name']}  [{cat_ko}]", 3)
    P((r['evidence'] or '')[:300], size=9)

H('5. 활용 가이드', 1)
P('▣ 발송 우선순위', bold=True, size=11)
P('1. 단서 다수 보유 회사 (top of list — 단서수 기준 내림차순)', size=10)
P('2. severity=5 우선 + lead_type=market_shift / risk_alert 즉시 출고 후보', size=10)
P('3. 추가 발굴 후보 (Section 4)는 단서 보강 후 발송', size=10)
P('▣ 검수 워크플로우', bold=True, size=11)
P('• /quality_review 페이지에서 score 4 일괄 approve', size=10)
P('• Gemini news_likely=N 확인 (보도 됐다면 후속 취재로 전환)', size=10)
P('• ir_questionnaires status=approved → gmail_send 발송', size=10)

doc.save(str(OUT))
print(f'생성됨: {OUT}')
print(f'크기: {OUT.stat().st_size:,} bytes')
print(f'발송 후보 회사: {len(sendable)}개')
print(f'HIGH 단서 TOP 50 수록')
print(f'추가 발굴 후보: {len(extra)}개')
