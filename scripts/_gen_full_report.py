"""
종합 풀리포트 + 취재·운영 가이드.
출력: data/exports/FULL_REPORT_with_guide.docx
"""
import sqlite3, json
from pathlib import Path
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn

ROOT = Path(__file__).parent.parent
DB = ROOT / "data" / "dart" / "dart_reports.db"
OUT = ROOT / "data" / "exports" / "FULL_REPORT_20260526.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)
db = sqlite3.connect(str(DB)); db.row_factory = sqlite3.Row

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
    p = doc.add_heading(t,level=lv)
    if color:
        for r in p.runs: r.font.color.rgb=color
    sf(p, size=15 if lv==1 else (12 if lv==2 else 11), bold=True, color=color)

def P(t,**kw):
    p = doc.add_paragraph(); p.add_run(t); sf(p,**kw)

def T(headers, rows, widths=None):
    t = doc.add_table(rows=1,cols=len(headers)); t.style='Light Grid Accent 1'
    for i,h in enumerate(headers):
        c = t.rows[0].cells[i]; c.text=h
        for p in c.paragraphs: sf(p,size=9,bold=True)
    for row in rows:
        c = t.add_row().cells
        for i,v in enumerate(row):
            c[i].text=str(v)
            for p in c[i].paragraphs: sf(p,size=9)
    if widths:
        for row in t.rows:
            for i,w in enumerate(widths): row.cells[i].width=Cm(w)
    doc.add_paragraph()

# ────────────── 표지 ──────────────
title = doc.add_heading('Company Catcher — 종합 풀리포트 + 취재·운영 가이드', level=0)
title.alignment=WD_ALIGN_PARAGRAPH.CENTER
P('2026-05-26 (화) 시점 — K-3b Flash cross-verify + N-1 뉴스화 검증 진행 중', size=11)
doc.paragraphs[-1].alignment=WD_ALIGN_PARAGRAPH.CENTER
P('8단계 검증 사이클 + 산업 트렌드 대비 + 뉴스화 가능성 + 악재/호재 명확화', italic=True, size=10)
doc.paragraphs[-1].alignment=WD_ALIGN_PARAGRAPH.CENTER
doc.add_paragraph()

# ═══════════════ PART 1: 시스템 진단 ═══════════════
H('PART 1 — 시스템 진단', 1)

H('1-1. 검증 사이클 (7단계)', 2)
T(['단계','내용','결과'],[
    ['1차', 'AI 비교 (Gemini Flash-Lite)', 'KOSPI+KOSDAQ 2,164/2,164 (100%)'],
    ['2차', 'detect_leads 룰 매칭 (33룰)', 'story_leads 1,852건'],
    ['3차', 'Gemini growth_signals 평가', '3,106건 (실제 변화 2,808)'],
    ['4차', 'Naver 30일 뉴스 매칭', 'HIGH_CONFIRMED 106 / PARTIAL 75 / REPORTED 489'],
    ['5차', '사업보고서 원문 EXACT 매칭', '환각 0건 / 골드 102건'],
    ['6차', '공급망·재무·수시공시 cross-ref', 'freshness 라벨링'],
    ['7차', 'Gemini 정밀 가치 분류 (L-2)', '29건 통과 / 73건 재시도'],
], [2.5, 7.5, 6])

H('1-2. 검증 8단계 완성 현황', 2)
# K-3b 현황
k3_stats = dict(db.execute("SELECT cross_verified, COUNT(*) FROM story_leads WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG') GROUP BY cross_verified").fetchall())
k3_confirmed = k3_stats.get('CONFIRMED', 0)
k3_need = k3_stats.get('NEED_MORE', 0)
k3_null = k3_stats.get(None, 0)
ultra = db.execute("""SELECT COUNT(*) FROM story_leads sl WHERE sl.cross_verified='CONFIRMED'
    AND sl.fact_match_label IN ('EXACT','STRONG')
    AND EXISTS (SELECT 1 FROM ir_contacts ic WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
    AND ic.ir_email IS NOT NULL AND ic.ir_email<>'' AND substr(ic.ir_email,1,1)<>'_')""").fetchone()[0]
n1_done = db.execute("SELECT COUNT(*) FROM story_leads WHERE n1_verified_at IS NOT NULL").fetchone()[0]
T(['단계','내용','상태'],
  [['1차', 'AI 비교 (Gemini)', '✅ 2,164/2,164 (100%)'],
   ['2차', 'detect_leads 룰 (33룰)', '✅ story_leads 1,852건'],
   ['3차', 'Gemini growth_signals', '✅ 3,106건 (변화 2,808)'],
   ['4차', 'Naver 뉴스 매칭', '✅ HIGH_CONFIRMED 106건'],
   ['5차', '원문 EXACT 매칭', '✅ 환각 0 / 골드 102건'],
   ['6차', '공급망·수시공시 freshness', '✅ 라벨링 완료'],
   ['7차 (L-2)', '미래 가치 방향 분류', f'⚡ 63/102 방향 확정 / UNCERTAIN 39건 잔여'],
   ['8차 (K-3b)', 'Flash cross-verify', f'⚡ {k3_confirmed+k3_need}/102 처리 | CONFIRMED {k3_confirmed} / ULTRA GOLD {ultra}건'],
   ['(N-1)', '뉴스화 가능성 검증', f'⏳ {n1_done}/102 (API 한도 소진 → 내일 재개)'],
  ], [1.5, 6, 6])

H('1-3. 단서 풀 누적 (시작 → 현재)', 2)
T(['지표','시작 (5/16)','현재 (5/25)','배수'],[
    ['ai_comparisons (KK)', '203', '2,164', '× 10.7'],
    ['story_leads', '50', '1,852', '× 37'],
    ['article_drafts', '357', '1,362', '× 3.8'],
    ['score 4', '39', '102+ (골드)', '× 2.6+'],
    ['HIGH_CONFIRMED', '0', '106', '신규'],
    ['🏆 골드 (6중 검증)', '0', '102', '신규'],
],[6,2.5,2.5,2])

H('1-3. 인프라 변화', 2)
P('• Gemini 모델 SSOT 통합 (config/gemini_models.py)', size=10)
P('• 신규 33개 alert_rules (M&A·매각·기술이전·고객의존도 등 14 카테고리)', size=10)
P('• ir_contacts_2 신규 600건 (검증 IR 풀 확대)', size=10)
P('• KRX 시장 매핑 → KOSPI/KOSDAQ 정밀 타겟팅', size=10)
P('• 75 파일 GitHub push 완료', size=10)
doc.add_paragraph()

# ═══════════════ PART 2: 단서 정밀 분류 ═══════════════
H('PART 2 — 단서 정밀 분류 (L-2 결과)', 1)

# value_direction 분포
H('2-1. 미래 가치 방향 분포 (Gemini 검증)', 2)
vd_rows = list(db.execute("""
    SELECT value_direction, COUNT(*) c
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
      AND value_direction IS NOT NULL AND value_direction != ''
    GROUP BY value_direction ORDER BY c DESC
"""))
T(['방향','건수','해석'],
  [[r['value_direction'], r['c'],
    {'POSITIVE_GROWTH':'성장·확대 → 긍정 기사',
     'NEGATIVE_DECLINE':'위기·축소 → 부정 기사',
     'TRANSFORMATION':'사업 전환 → 시리즈 기사',
     'RISK_WARNING':'잠재 리스크 → 경보 기사',
     'UNCERTAIN':'추가 평가 필요'}.get(r['value_direction'],'')]
   for r in vd_rows],
  [5,2,8])

# industry_alignment
H('2-2. 산업 트렌드 대비 (순행 vs 역행)', 2)
ia_rows = list(db.execute("""
    SELECT industry_alignment, COUNT(*) c
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
      AND industry_alignment IS NOT NULL
    GROUP BY industry_alignment ORDER BY c DESC
"""))
T(['alignment','건수','해석'],
  [[r['industry_alignment'], r['c'],
    {'ALIGNED':'산업 트렌드 동행 → 평이한 기사',
     'CONTRARIAN':'산업 역행 → 🔥 가장 흥미로운 단독 기사',
     'NEUTRAL':'산업 무관'}.get(r['industry_alignment'],'')]
   for r in ia_rows],
  [4,2,9])

# impact magnitude
H('2-3. 시총 영향 강도', 2)
im_rows = list(db.execute("""
    SELECT impact_magnitude, COUNT(*) c
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
      AND impact_magnitude IS NOT NULL
    GROUP BY impact_magnitude ORDER BY c DESC
"""))
T(['강도','건수','기준'],
  [[r['impact_magnitude'], r['c'],
    {'STRONG':'시총 5%+ 영향',
     'MEDIUM':'1-5% 영향',
     'WEAK':'1% 미만'}.get(r['impact_magnitude'],'')]
   for r in im_rows],
  [3,2,8])

# ═══════════════ PART 3: 우선 취재 단서 ═══════════════
# ═══════════════ PART 2-b: K-3b cross-verify 결과 ═══════════════
H('PART 2-b — K-3b Cross-Verify 결과 (Flash 모델)', 1)
H('K-3b 처리 현황', 2)
k3_all = db.execute("""SELECT cross_verified, COUNT(*) c FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
    GROUP BY cross_verified ORDER BY c DESC""").fetchall()
T(['verdict','건수','의미'],
  [[r[0] or '미처리', r[1],
    {'CONFIRMED':'즉시 출고 가능 (2중 검증 완료)',
     'NEED_MORE':'추가 자료 필요',
     'REJECT':'폐기 권장',
     'PARSE_FAIL':'재처리 필요',
     None:'API 한도 소진으로 대기 중'}.get(r[0], '')] for r in k3_all],
  [3,2,9])

H('CONFIRMED 단서 상세 (즉시 출고 후보)', 2)
confirmed_rows = db.execute("""
    SELECT corp_name, title, value_direction, industry_alignment, impact_magnitude,
           cross_evidence, short_term, medium_term
    FROM story_leads
    WHERE cross_verified='CONFIRMED' AND info_gap_label='HIGH_CONFIRMED'
    ORDER BY severity DESC
""").fetchall()
if confirmed_rows:
    for r in confirmed_rows:
        H(f"{r['corp_name']} — {r['title']}", 3, color=RGBColor(0x1A, 0x53, 0x76))
        P(f"방향: {r['value_direction'] or '-'}  /  alignment: {r['industry_alignment'] or '-'}  /  영향: {r['impact_magnitude'] or '-'}", italic=True, size=9)
        if r['cross_evidence']:
            try:
                ce = json.loads(r['cross_evidence'])
                if ce.get('core_fact'): P(f"✓ 핵심 팩트: {ce['core_fact']}", size=9)
                if ce.get('risk') and ce['risk'] != 'NONE': P(f"⚠ 리스크: {ce['risk']}", size=9)
            except: pass
        if r['short_term']: P(f"단기: {(r['short_term'] or '')[:150]}", size=9)
        if r['medium_term']: P(f"중기: {(r['medium_term'] or '')[:150]}", size=9)
        doc.add_paragraph()
else:
    P('(API 한도 소진으로 대부분 미처리 — 내일 재개 후 여기에 표시됩니다)', italic=True, size=10)
doc.add_paragraph()

H('PART 3 — 우선 취재 단서 (L-2 검증 통과)', 1, color=RGBColor(0xC0,0x39,0x2B))

# CONTRARIAN + STRONG = 최고 가치
P('🌟🌟 STRONG + CONTRARIAN (산업 역행 + 시총 영향 큼)', bold=True, size=12)
top_rows = list(db.execute("""
    SELECT corp_name, title, value_direction, industry_alignment,
           impact_magnitude, short_term, medium_term, long_term,
           value_rationale_ai
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
      AND impact_magnitude='STRONG' AND industry_alignment='CONTRARIAN'
    ORDER BY severity DESC
"""))
if not top_rows:
    P('  (현재 0건 — L-2 추가 검증 필요)', italic=True, size=10)
else:
    for r in top_rows:
        H(f"{r['corp_name']} — {r['title']}", 3, color=RGBColor(0xC0,0x39,0x2B))
        P(f"방향: {r['value_direction']}  /  alignment: {r['industry_alignment']}  /  영향: {r['impact_magnitude']}", italic=True, size=9)
        if r['short_term']: P(f"단기: {r['short_term']}", size=9)
        if r['medium_term']: P(f"중기: {r['medium_term']}", size=9)
        if r['long_term']: P(f"장기: {r['long_term']}", size=9)
        if r['value_rationale_ai']:
            try:
                rat = json.loads(r['value_rationale_ai'])
                P(f"근거: {rat.get('rationale','')}", size=9)
            except: pass
doc.add_paragraph()

# STRONG + ALIGNED
P('⭐ STRONG + ALIGNED (산업 동행 + 영향 큼) — 순항 성장/위기', bold=True, size=12)
aligned = list(db.execute("""
    SELECT corp_name, title, value_direction, short_term, medium_term, long_term, value_rationale_ai
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
      AND impact_magnitude='STRONG' AND industry_alignment='ALIGNED'
    ORDER BY severity DESC LIMIT 10
"""))
for r in aligned:
    H(f"{r['corp_name']} — {r['title']}", 3)
    P(f"방향: {r['value_direction']}  /  ALIGNED  /  STRONG", italic=True, size=9)
    if r['short_term']: P(f"단기: {r['short_term'][:200]}", size=9)
    if r['medium_term']: P(f"중기: {r['medium_term'][:200]}", size=9)
    if r['long_term']: P(f"장기: {r['long_term'][:200]}", size=9)
doc.add_paragraph()

# RISK_WARNING
P('⚠️ RISK_WARNING — 잠재 리스크 알림 기사', bold=True, size=12)
risk = list(db.execute("""
    SELECT corp_name, title, short_term, medium_term, value_rationale_ai
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
      AND value_direction='RISK_WARNING'
    ORDER BY severity DESC LIMIT 10
"""))
for r in risk:
    H(f"{r['corp_name']} — {r['title']}", 3)
    if r['short_term']: P(f"단기 리스크: {(r['short_term'] or '')[:200]}", size=9)
    if r['medium_term']: P(f"중기 리스크: {(r['medium_term'] or '')[:200]}", size=9)
doc.add_paragraph()

# ═══════════════ PART 4: 취재 가이드 ═══════════════
H('PART 4 — 취재 가이드', 1)

H('4-1. 시그널별 취재 앵글', 2)
T(['시그널 방향','취재 앵글','IR 질문 키워드'],[
    ['POSITIVE_GROWTH + ALIGNED','"○○, 업계 트렌드 타고 사업 확장"','확장 속도, 경쟁사 격차, 다음 단계'],
    ['POSITIVE_GROWTH + CONTRARIAN','🔥 "○○, 업계 부진에도 나홀로 성장"','차별화 요인, 지속성, 외부 리스크'],
    ['NEGATIVE_DECLINE + ALIGNED','"○○ 포함 ○○업계 동시 위기"','매출 전망, 구조조정 계획, 회복 시점'],
    ['NEGATIVE_DECLINE + CONTRARIAN','🔥 "○○, 호조 업계에서 유일한 부진"','부진 원인, 경쟁력 회복, 대응책'],
    ['TRANSFORMATION','"○○, 사업 구조 재편 본격화"','전환 배경, 기존 사업 비중, 신규 사업 자원'],
    ['RISK_WARNING','"○○ ○○ 리스크 부상"','대응 계획, 영향 범위, 자체 평가'],
], [5,7,5])

H('4-2. 단서별 IR 질문 5가지 (기본 템플릿)', 2)
P('• 시그널 사실 확인 → "○○ 변화가 ○○○ 정도 정확한가?"', size=10)
P('• 시점 확인 → "언제 시작/완료될 예정인가?"', size=10)
P('• 영향 정량 → "매출/이익 영향은 ○○억/○○% 인가?"', size=10)
P('• 후속 계획 → "이 변화의 다음 단계는?"', size=10)
P('• 외부 변수 → "○○ (경쟁사·정책·시장) 변화 영향은?"', size=10)
doc.add_paragraph()
P('* reporter v3 초안에는 단서별 맞춤 IR 질문 5가지 포함되어 있음 (article_drafts.reporter_brief)', italic=True, size=9)
doc.add_paragraph()

H('4-3. 외부 자료 보강 체크리스트', 2)
P('☐ 재무 데이터 비교 (companies + financials)', size=10)
P('☐ Naver 뉴스 매칭 (최근 30/60/90일)', size=10)
P('☐ 회사 IR 홈페이지 직접 접속 (수시공시 / 보도자료)', size=10)
P('☐ 산업 협회 / 증권사 리포트', size=10)
P('☐ 경쟁사 공시 비교 (DART 동종 업종)', size=10)
P('☐ 공급망 거래처 확인 (supply_chain 테이블, 11K 관계)', size=10)
doc.add_paragraph()

H('4-4. 인터뷰 우선순위 매트릭스', 2)
T(['우선도','조건','대응'],[
    ['🌟🌟 최우선','STRONG + CONTRARIAN + sev=5','즉시 IR 전화, 24h 내 출고'],
    ['🌟 매우 높음','STRONG + ALIGNED + sev=5','금주 내 출고, 보강 1-2일'],
    ['⭐ 높음','STRONG + sev=4 또는 MEDIUM + sev=5','평일 분산 발송'],
    ['보통','MEDIUM + sev=4','후속 취재 패키지'],
    ['낮음','WEAK 또는 UNCERTAIN','검증 보강 후'],
],[3,7,7])

# ═══════════════ PART 5: 운영 최적화 ═══════════════
H('PART 5 — 운영 최적화 가이드', 1)

H('5-1. 발송 캘린더 (주간 권장)', 2)
T(['요일','시간대','대상','이유'],[
    ['월','09:30~10:30','STRONG + ALIGNED','주초 IR 응답률 높음'],
    ['월','14:00~15:00','STRONG + CONTRARIAN','오후 검토 시간 확보'],
    ['화-목','09:30~11:00','MEDIUM 일반','평일 안정 시간대'],
    ['화-목','14:00~16:00','TRANSFORMATION','심층 자료 첨부'],
    ['금','오전만','RISK_WARNING','주말 전 알림'],
    ['주말','발송 X','—','IR 담당자 휴무'],
],[2,3,5,7])

H('5-2. 검수 워크플로우', 2)
P('1. `data/exports/FULL_REPORT_with_guide.docx` 검토 (오프라인)', size=10)
P('2. `/quality_review` 페이지에서 score 4 단서 검수', size=10)
P('3. 단서별 reporter v3 초안 확인 (article_drafts)', size=10)
P('4. IR 질문지 검토 → status=approved', size=10)
P('5. `gmail_send.py` 실행 (24h 중복 차단 자동)', size=10)
P('6. 답장 수신 후 `gmail_sync_inbox.py` 자동 매칭', size=10)
P('7. `learn_from_replies.py` IR 담당자 학습', size=10)
doc.add_paragraph()

H('5-3. 답장 학습 사이클', 2)
P('▣ 답장 받은 회사 → ir_contacts user_verified=1 자동 표시', size=10)
P('▣ 답장 패턴 → IR 담당자 응답 시간/태도 학습 (다음 발송 우선순위)', size=10)
P('▣ "답장 받은 회사 기사 우선 출고" 원칙', size=10)
doc.add_paragraph()

H('5-4. 다음 사이클 (Q2 보고서 시즌)', 2)
P('• 2026년 반기보고서 마감: 2026년 8월 14일 (D-약 80)', size=10)
P('• 권장 준비:', size=10)
P('  – 8/1: Q2 보고서 fetch 시작', size=10)
P('  – 8/15~8/20: H1 vs Q1 비교 분석 batch', size=10)
P('  – 8/20~8/25: 검증 사이클 (G→H→J→L)', size=10)
P('  – 8/25~: 발송 시작', size=10)
P('• Q1 단서 결과 학습 → Q2 룰 보강', size=10)
doc.add_paragraph()

H('5-5. 진행 중 / 보류 작업 (2026-05-26 현재)', 2)
# N-1 현황
n1_cnt = db.execute("SELECT COUNT(*) FROM story_leads WHERE n1_verified_at IS NOT NULL").fetchone()[0]
unc_cnt = db.execute("SELECT COUNT(*) FROM story_leads WHERE value_direction='UNCERTAIN' AND info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')").fetchone()[0]
k3_remain = db.execute("SELECT COUNT(*) FROM story_leads WHERE cross_verified IS NULL AND info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')").fetchone()[0]
P(f'• K-3b cross-verify: {k3_remain}건 잔여 → API 한도 리셋 후 _run_verify_chain.py 재실행', size=10)
P(f'• L-2b UNCERTAIN: {unc_cnt}건 → K-3b 완료 후 재분류', size=10)
P(f'• N-1 뉴스화 검증: {n1_cnt}/{102}건 완료 → chain 재실행 시 자동 처리', size=10)
P('• Gemini Pro (K-3 원본): pro 한도 회복 시 추가 검증', size=10)
P('• 답장 학습 시스템 (gmail_sync_inbox) → 첫 발송 후 가동', size=10)
P('• 재실행 방법: python -X utf8 scripts/_run_verify_chain.py', italic=True, size=9)
doc.add_paragraph()

# ═══════════════ PART 6: 부록 ═══════════════
H('PART 6 — 부록', 1)

H('6-1. 검증 자취 (각 단서 데이터 컬럼)', 2)
P('• gemini_verified — 1차 Gemini 진위', size=10)
P('• gemini_news_likely — Gemini 보도 추정', size=10)
P('• info_gap_label — HIGH/HIGH_CONFIRMED/HIGH_PARTIAL/HIGH_REPORTED', size=10)
P('• actual_news_count — Naver 실 매칭 카운트', size=10)
P('• fact_match_label — 원문 매칭 (EXACT/STRONG/PARTIAL/HALLUCINATION)', size=10)
P('• fact_match_ratio — 원문 매칭 비율', size=10)
P('• numeric_verified_ratio — 수치 정합 비율', size=10)
P('• freshness_label — 수시공시 최신성', size=10)
P('• supply_chain_impact — 공급망 영향', size=10)
P('• value_direction — 미래 가치 방향', size=10)
P('• industry_alignment — 산업 트렌드 대비', size=10)
P('• short/medium/long_term — 시간 horizon 영향', size=10)
P('• impact_magnitude — 시총 영향 강도', size=10)
P('• value_confidence — 평가 신뢰도', size=10)
doc.add_paragraph()

H('6-2. 33개 alert_rules 카테고리', 2)
P('• 기본 룰 16개 + 신규 13개 (M&A·매각·기술이전 등) + 추가 5개 (제품·특허·정부과제 등) - 비활성 1', size=10)
P('• 사용자 정의 14 카테고리 매칭', size=10)
doc.add_paragraph()

H('6-3. GitHub Repo', 2)
P('https://github.com/kangchani82-boop/company-catcher', size=10)
P('마지막 push: 5/19 (75 파일, +25,934줄)', size=10)
doc.add_paragraph()

H('6-4. 작성한 Word 보고서 5개', 2)
P('• 3day_report.docx — 인프라 개선', size=10)
P('• impact_signals_full.docx — 파급력 단서 308건', size=10)
P('• sendable_candidates.docx — HIGH 235개사', size=10)
P('• confirmed_info_gap.docx — Gemini+Naver 이중 검증', size=10)
P('• final_gold_signals.docx — 6중 검증 골드 102', size=10)
P('• FULL_REPORT_with_guide.docx — 이 보고서 (종합)', size=10, bold=True)

doc.save(str(OUT))
print(f'생성됨: {OUT}')
print(f'크기: {OUT.stat().st_size:,} bytes')
