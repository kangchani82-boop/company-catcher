"""
scripts/_gen_signals_report.py
───────────────────────────────
파급력 큰 단서 전체 정리 보고서 (Word).
- importance=5 + has_change=1 단서 전부 (308건)
- importance>=4 + news_likely=N (보도 안 됨) 단서 풍부
- 카테고리별 분포
출력: data/exports/impact_signals_full.docx
"""
import sqlite3, json
from pathlib import Path
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUT = ROOT / "data" / "exports" / "impact_signals_full.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row

doc = Document()

# 폰트
style = doc.styles['Normal']
style.font.name = '맑은 고딕'
style.font.size = Pt(10)
style._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')

def setfont(p, size=10, bold=False, color=None):
    for r in p.runs:
        r.font.name = '맑은 고딕'
        r._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')
        r.font.size = Pt(size)
        if bold: r.bold = True
        if color: r.font.color.rgb = color

def H(text, level=1, color=None):
    p = doc.add_heading(text, level=level)
    if color:
        for run in p.runs: run.font.color.rgb = color
    setfont(p, size=14 if level==1 else (12 if level==2 else 11), bold=True, color=color)
    return p

def P(text, bold=False, italic=False, size=10, color=None):
    p = doc.add_paragraph()
    r = p.add_run(text)
    r.bold = bold
    r.italic = italic
    setfont(p, size=size, bold=bold, color=color)
    return p

def make_table(headers, rows, col_widths=None):
    t = doc.add_table(rows=1, cols=len(headers))
    t.style = 'Light Grid Accent 1'
    for i, h in enumerate(headers):
        c = t.rows[0].cells[i]
        c.text = h
        for para in c.paragraphs:
            setfont(para, size=9, bold=True)
    for row in rows:
        c = t.add_row().cells
        for i, v in enumerate(row):
            c[i].text = str(v)
            for para in c[i].paragraphs:
                setfont(para, size=9)
    if col_widths:
        for row in t.rows:
            for i, w in enumerate(col_widths):
                row.cells[i].width = Cm(w)
    doc.add_paragraph()
    return t

# ─────────── 표지 ───────────
title = doc.add_heading('Company Catcher 파급력 큰 단서 종합 정리', level=0)
title.alignment = WD_ALIGN_PARAGRAPH.CENTER
P('— AI(Gemini) 정밀 검증 결과 —', size=11).alignment = WD_ALIGN_PARAGRAPH.CENTER
P('importance=5 (최고 가치) + 보도 안 됨 단서 풍부 수록', italic=True, size=10).alignment = WD_ALIGN_PARAGRAPH.CENTER
doc.add_paragraph()

# ─────────── 1. 통계 요약 ───────────
H('1. 통계 요약', 1)

stats = {}
stats['total'] = db.execute('SELECT COUNT(*) FROM growth_signals').fetchone()[0]
stats['has_change'] = db.execute('SELECT COUNT(*) FROM growth_signals WHERE has_change=1').fetchone()[0]
stats['imp5'] = db.execute('SELECT COUNT(*) FROM growth_signals WHERE importance=5 AND has_change=1').fetchone()[0]
stats['imp4'] = db.execute('SELECT COUNT(*) FROM growth_signals WHERE importance=4 AND has_change=1').fetchone()[0]
stats['gap'] = db.execute('SELECT COUNT(*) FROM growth_signals WHERE news_likely="N" AND has_change=1').fetchone()[0]
stats['imp5_gap'] = db.execute('SELECT COUNT(*) FROM growth_signals WHERE importance=5 AND has_change=1 AND news_likely="N"').fetchone()[0]

make_table(
    ['지표', '값'],
    [
        ['Gemini 검증 처리 총합', f"{stats['total']:,}"],
        ['실제 변화 감지 (has_change=1)', f"{stats['has_change']:,}"],
        ['⭐ importance=5 (최고 가치)', f"{stats['imp5']:,}"],
        ['importance=4', f"{stats['imp4']:,}"],
        ['🔥 보도 안 됨 추정 (news_likely=N)', f"{stats['gap']:,}"],
        ['🌟 importance=5 + 보도 안 됨 (= 최우선)', f"{stats['imp5_gap']:,}"],
    ],
    col_widths=[10, 4]
)

# ─────────── 2. 카테고리별 단서 분포 ───────────
H('2. 카테고리별 단서 분포', 1)

cat_imp5, cat_imp4 = {}, {}
for r in db.execute('SELECT change_categories, importance FROM growth_signals WHERE has_change=1 AND importance>=4'):
    try:
        cats = json.loads(r['change_categories'] or '[]')
        target = cat_imp5 if r['importance']==5 else cat_imp4
        for c in cats:
            target[c] = target.get(c, 0) + 1
    except: pass

# 카테고리 한글 라벨
KO = {
    'new_product':'신규 제품 출시',
    'new_business':'신규 사업',
    'facility_investment':'시설투자',
    'customer_change':'고객사 변화',
    'new_project':'신규 과제',
    'exit_business':'사업철수',
    'global_customer':'글로벌 고객 확보',
    'corp_acquire':'기업/법인 인수',
    'new_patent':'신규 특허',
    'corp_setup':'법인 설립',
    'regulator_risk':'규제 리스크',
    'corp_close_sell':'법인 폐쇄·매각',
    'gov_grant':'정부 과제',
    'equity_invest':'타법인 출자·지분',
}

all_cats = sorted(set(cat_imp5) | set(cat_imp4),
                   key=lambda c: -(cat_imp5.get(c,0)+cat_imp4.get(c,0)))
make_table(
    ['카테고리', 'importance=5', 'importance=4', '합계'],
    [[KO.get(c, c), cat_imp5.get(c,0), cat_imp4.get(c,0),
      cat_imp5.get(c,0)+cat_imp4.get(c,0)] for c in all_cats],
    col_widths=[6, 3, 3, 3]
)

# ─────────── 3. importance=5 단서 전체 (308건) ───────────
H('3. importance=5 단서 전체 (최고 파급력)', 1, color=RGBColor(0xC0, 0x39, 0x2B))
P('아래 단서들은 Gemini가 "회사 가치에 즉시 영향을 줄 변화"로 판단한 최고 등급입니다. '
  '🔥 표시는 보도 안 됐을 가능성이 높은 진짜 정보 격차 단서.', italic=True, size=9)
doc.add_paragraph()

rows_imp5 = db.execute('''
    SELECT corp_name, change_categories, evidence, news_likely
    FROM growth_signals
    WHERE importance=5 AND has_change=1
    ORDER BY (news_likely='N') DESC, corp_name ASC
''').fetchall()

P(f'총 {len(rows_imp5)}건', bold=True, size=11)
doc.add_paragraph()

for i, r in enumerate(rows_imp5, 1):
    cats = json.loads(r['change_categories'] or '[]')
    cat_ko = ' · '.join(KO.get(c, c) for c in cats[:3])
    marker = '🔥 ' if r['news_likely'] == 'N' else ''
    H(f"{i}. {marker}{r['corp_name']}  [{cat_ko}]", 3)
    P((r['evidence'] or '').strip()[:500], size=10)

# ─────────── 4. importance=4 + 보도 안 됨 (1,000+) ───────────
H('4. importance=4 + 🔥 보도 안 됨 단서 — 카테고리별 추천 TOP 5', 1)
P(f'importance=4 + news_likely=N 단서가 약 {stats["imp4"]-cat_imp5.get("",0):,}건 (보도 안 됨만). '
  '전체 목록은 데이터 너무 많아 카테고리별 TOP 5만 수록.', italic=True, size=9)
doc.add_paragraph()

for cat in ['new_product','new_business','facility_investment','customer_change',
            'global_customer','corp_acquire','new_patent','exit_business',
            'corp_close_sell','equity_invest','regulator_risk','gov_grant']:
    rows = db.execute('''
        SELECT corp_name, evidence
        FROM growth_signals
        WHERE importance=4 AND has_change=1 AND news_likely='N'
          AND change_categories LIKE ?
        ORDER BY id DESC LIMIT 5
    ''', [f'%"{cat}"%']).fetchall()
    if not rows: continue
    H(f"4-{cat} — {KO.get(cat, cat)} ({len(rows)}건 샘플)", 2)
    for r in rows:
        P(f"• {r['corp_name']}: {(r['evidence'] or '').strip()[:200]}", size=9)
    doc.add_paragraph()

# ─────────── 5. 활용 가이드 ───────────
H('5. 활용 가이드', 1)
P('▣ 우선순위', bold=True, size=11)
P('1. 🔥 + importance=5: 즉시 취재 (예: 멕아이씨에스 FDA 승인, 휴니드 보잉 의존도 급증)', size=10)
P('2. importance=5 + news_likely=Y: 후속 취재·심층 분석', size=10)
P('3. 🔥 + importance=4: 보강 단서 / 산업 트렌드 묶음 기사', size=10)
doc.add_paragraph()

P('▣ 데이터 원천', bold=True, size=11)
P('• DART 사업보고서 + 분기보고서 본문 "2. 사업의 내용" 섹션', size=10)
P('• Gemini 무료 API (gemini-flash-latest / 2.5-flash-lite 등 fallback 체인)', size=10)
P('• 14개 사용자 정의 키워드 카테고리 grep + AI 평가', size=10)
doc.add_paragraph()

P('▣ 단서 한계', bold=True, size=11)
P('• Gemini "news_likely" 판단은 AI 추정 — 실제 뉴스 매칭 검증 필요', size=10)
P('• 1차 평가 후 사용자/기자 직접 검수 권장', size=10)
P('• regulator_risk (공정위·검찰) 단서는 사업보고서 "2.사업의 내용"에 드물게 등장 — 보조 source 필요', size=10)

doc.save(str(OUT))
print(f'생성됨: {OUT}')
print(f'크기: {OUT.stat().st_size:,} bytes')
print(f'importance=5 단서: {len(rows_imp5)}건 수록')
