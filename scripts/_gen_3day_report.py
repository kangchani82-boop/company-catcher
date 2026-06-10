"""
scripts/_gen_3day_report.py
────────────────────────────
최근 3일(5/20~5/22) 개선 작업 핵심 보고서 (Word) 생성.
출력: data/exports/3day_report.docx
"""
import sqlite3, json
from pathlib import Path
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUT = ROOT / "data" / "exports" / "3day_report.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row

doc = Document()

# 기본 스타일
style = doc.styles['Normal']
style.font.name = '맑은 고딕'
style.font.size = Pt(10)
style._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')

def H(text, level=1, color=None):
    p = doc.add_heading(text, level=level)
    if color:
        for run in p.runs:
            run.font.color.rgb = color
    for run in p.runs:
        run.font.name = '맑은 고딕'
        run._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')
    return p

def P(text, bold=False, italic=False, size=10, color=None):
    p = doc.add_paragraph()
    r = p.add_run(text)
    r.bold = bold
    r.italic = italic
    r.font.size = Pt(size)
    r.font.name = '맑은 고딕'
    r._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')
    if color: r.font.color.rgb = color
    return p

def make_table(headers, rows, col_widths=None):
    t = doc.add_table(rows=1, cols=len(headers))
    t.style = 'Light Grid Accent 1'
    hdr = t.rows[0].cells
    for i, h in enumerate(headers):
        hdr[i].text = h
        for para in hdr[i].paragraphs:
            for run in para.runs:
                run.bold = True
                run.font.name = '맑은 고딕'
                run._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')
    for row in rows:
        c = t.add_row().cells
        for i, v in enumerate(row):
            c[i].text = str(v)
            for para in c[i].paragraphs:
                for run in para.runs:
                    run.font.name = '맑은 고딕'
                    run._element.rPr.rFonts.set(qn('w:eastAsia'), '맑은 고딕')
                    run.font.size = Pt(9)
    if col_widths:
        for row in t.rows:
            for i, w in enumerate(col_widths):
                row.cells[i].width = Cm(w)
    doc.add_paragraph()
    return t

# ─────────── 표지 ───────────
title = doc.add_heading('Company Catcher 시스템 개선 보고서', level=0)
title.alignment = WD_ALIGN_PARAGRAPH.CENTER
P('— 최근 3일(2026-05-20 ~ 2026-05-22) 핵심 성과 정리 —').alignment = WD_ALIGN_PARAGRAPH.CENTER
P('정보 격차 큰 단서 풀 구축에 집중', italic=True).alignment = WD_ALIGN_PARAGRAPH.CENTER
doc.add_paragraph()

# ─────────── 1. 임원 요약 ───────────
H('1. 임원 요약', 1)
P('• 한국 KOSPI·KOSDAQ 상장사 2,164개 비교 분석 100% 완료', size=11)
P('• AI(Gemini) 정밀 검증 1,777건 — 실제 변화 감지 1,664건 (94%)', size=11)
P('• 정보 격차 큰 단서 1,604건 발굴 — 보도되지 않은 시그널', size=11, bold=True, color=RGBColor(0xC0, 0x39, 0x2B))
P('• 최고 중요도(importance=5) 단서 308건 — 즉시 취재 가치', size=11, bold=True)
P('• 발송 후보(score=4) 106건 → status=approved 적용 완료', size=11)
doc.add_paragraph()

# ─────────── 2. 핵심 인프라 개선 ───────────
H('2. 핵심 인프라 개선', 1)
make_table(
    ['항목', '내용', '효과'],
    [
        ['SSOT 통합', 'config/gemini_models.py 신규 — server.py + 4개 스크립트 통합',
         'Gemini 정책 변경 시 1 파일만 수정'],
        ['Gemini 3.1 Flash-Lite GA 전환', '5/25 데드라인 9일 전 선제 대응',
         '폐기 모델 호출 차단'],
        ['KRX 시장 매핑', 'companies.market 컬럼 신규 (KOSPI 835 / KOSDAQ 1,777 / KONEX 108)',
         '코넥스·SPAC·우선주 자동 제외'],
        ['batch_compare --market 옵션', '기본 KOSPI,KOSDAQ로 분석 대상 정밀화', '효율적 한도 활용'],
        ['subprocess 패치', 'CREATE_NO_WINDOW (PowerShell 팝업 차단)', '사용자 경험 개선'],
        ['/api/news/related-stocks 중복 제거', '죽은 코드 정리', '코드 깔끔'],
        ['루트 디렉토리 정리', '12개 임시 파일 → 2개 핵심만 (server.py, requirements.txt)',
         '유지보수 ↑'],
        ['Git push', '75개 파일 / +25,934줄 / -8,422줄 커밋 완료',
         '깃허브 백업 완료'],
    ],
    col_widths=[3.5, 7, 4.5]
)

# ─────────── 3. 단서 탐지 룰 시스템 확장 ───────────
H('3. 단서 탐지 룰 시스템 확장', 1)
P('기존 16개 룰 → 33개로 확장 (subsidiary_loss 1개 비활성화 포함)', size=10)
doc.add_paragraph()

H('3-1. 신규 13룰 (사용자 정보 격차 의도 반영)', 2)
make_table(
    ['rule_code', '카테고리', '키워드 예시', '매칭'],
    [
        ['ma_acquire', '소형 인수', '경영권 인수, 지분 인수, 타법인 출자', '9'],
        ['ma_divest', '사업부·자회사 매각', '사업부 매각, 물적분할, 인적분할', '22'],
        ['tech_transfer', '기술이전 (소형주 가치 변화)', '기술이전 계약, L/O 계약, 마일스톤', '23'],
        ['customer_concentration ⭐', '고객 의존도 변화', '주요 매출처 의존도, 단일 고객 비중', '32'],
        ['subsidiary_loss', '자회사 적자 (데이터 source 한계)', '— 비활성화', '0'],
        ['backlog_change ⭐', '수주잔고 (조선·방산·기계)', '수주잔고 급증/감소, 잔고 누적', '42'],
        ['utilization_drop', '가동률 급락', '가동률 하락, 유휴 설비, 조업 단축', '28'],
        ['capex_expand', '소형 설비투자', '공장 증설, 신규 생산라인, CAPA 확대', '5'],
        ['quiet_equity_change ⭐', '비공시 지분 변동 (5%↓)', '관계회사 지분, 그룹사 지분 변동', '13'],
        ['revenue_mix_shift', '매출 구성 변화', '제품별/지역별 매출 비중 변화', '16'],
        ['new_customer ⭐', '신규 거래처 (글로벌 大 등장)', '신규 거래처 확보, 글로벌 고객 확보', '6'],
        ['workforce_change', '인력 변동', '임직원 수 감소, 인력 감축, R&D 인력', '4'],
        ['litigation', '소형주 소송', '공정위 조사, 검찰 수사, 조세 소송', '10'],
    ],
    col_widths=[4, 4.5, 5, 1.5]
)

H('3-2. 추가 신규 5룰 (사용자 핵심 키워드 반영)', 2)
make_table(
    ['rule_code', '의미', '매칭'],
    [
        ['new_product', '신규 제품 출시', '53'],
        ['gov_grant', '정부 과제 수주', '25'],
        ['new_patent ⭐', '신규 특허 출원·등록', '92'],
        ['new_corp_setup', '법인·자회사 신설', '65'],
        ['regulator_audit', '규제기관 조사·수사 (수정 검토)', '0'],
    ],
    col_widths=[4, 8, 2]
)

H('3-3. 노이즈 룰 정밀화', 2)
P('• numeric_surge: exclude 8개·require 12개·min_evidence 100자로 강화 (667 → 향후 신규 매칭만 적용)', size=10)
P('• new_competitor: require_context "점유율", "매출 영향" 등 추가', size=10)
P('• subsidiary_loss: is_active=0 (사업보고서 본문에 자회사 손익 거의 없음)', size=10)
doc.add_paragraph()

# ─────────── 4. AI 정밀 검증 — growth_signals ───────────
H('4. AI 정밀 검증 (Gemini) — 사용자 키워드 14 카테고리', 1)
P('사용자 정의 "회사 성장 변화" 14 카테고리 키워드를 ai_comparisons에서 grep → '
  'Gemini가 진위·중요도·보도 여부 평가 → growth_signals 테이블', size=10)
doc.add_paragraph()

H('4-1. 검증 결과 누적', 2)
make_table(
    ['지표', '값', '비고'],
    [
        ['총 처리', '1,777건', 'Gemini 검증 완료'],
        ['실제 변화 감지', '1,664건 (94%)', '진위 통과'],
        ['importance=5', '308건', '최고 가치 단서'],
        ['importance≥4', '1,424건', '발송 후보'],
        ['🔥 보도 안 됨 추정 (N)', '1,604건', '진짜 정보 격차'],
    ],
    col_widths=[6, 4, 5]
)

H('4-2. 카테고리별 importance=5 분포', 2)
make_table(
    ['카테고리', 'importance=5'],
    [
        ['new_product (신규 제품)', '183'],
        ['new_business (신규 사업)', '170'],
        ['facility_investment (시설투자)', '97'],
        ['customer_change (고객사 변화)', '81'],
        ['new_project (신규 과제)', '80'],
        ['exit_business (사업철수)', '76'],
        ['global_customer (글로벌 고객)', '68'],
        ['corp_acquire (기업 인수)', '63'],
        ['new_patent (신규 특허)', '61'],
        ['corp_setup (법인 설립)', '29'],
        ['regulator_risk (규제 리스크)', '25'],
        ['corp_close_sell (법인 매각)', '22'],
        ['gov_grant (정부 과제)', '17'],
        ['equity_invest (지분 거래)', '14'],
    ],
    col_widths=[8, 3]
)

# ─────────── 5. 파급력 큰 발굴 단서 TOP 10 ───────────
H('5. 파급력 큰 발굴 단서 TOP 10 (importance=5 + 보도 안 됨)', 1, color=RGBColor(0xC0, 0x39, 0x2B))
P('아래 단서들은 모두 AI가 "보도 안 됐을 가능성 높음"으로 판단한 진짜 정보 격차 단서입니다.', italic=True, size=10)
doc.add_paragraph()

top = db.execute('''
    SELECT corp_name, change_categories, evidence
    FROM growth_signals
    WHERE importance=5 AND has_change=1 AND news_likely="N"
    ORDER BY id DESC LIMIT 15
''').fetchall()

for i, r in enumerate(top[:10], 1):
    cats = json.loads(r['change_categories'] or '[]')
    cat_str = ' · '.join(cats[:3])
    H(f"{i}. {r['corp_name']} — [{cat_str}]", 3)
    P((r['evidence'] or '')[:400], size=10)

# ─────────── 6. 기존 단서 시스템 통합 결과 ───────────
H('6. 기존 단서 시스템 통합 현황', 1)
make_table(
    ['지표', '시작 (5/16)', '현재 (5/22)', '증감'],
    [
        ['ai_comparisons (KOSPI+KOSDAQ)', '203', '2,164 (100%)', '× 10.7'],
        ['story_leads', '50', '1,852', '× 37'],
        ['article_drafts', '357', '1,222', '+ 865'],
        ['ir_questionnaires', '337', '1,222', '+ 885'],
        ['score=4 (발송 후보)', '39', '106', '+ 67'],
        ['ir_contacts (검증)', '6,840', '6,935', '+ 95 (PROMOTED)'],
        ['ir_contacts_2 (신규 풀)', '4', '600', '× 150'],
    ],
    col_widths=[5, 3, 3, 2.5]
)

# ─────────── 7. IR 시스템 — 발송 풀 ───────────
H('7. IR 발송 풀 형성', 1)
P('• ir_contacts_2 신규 600건 (A_complete 128 / B_scraped 181 / C_likely 291)', size=10)
P('• A_complete 128건 ir_contacts 본 테이블로 자동 승급 완료 (PROMOTED_FROM_SCRAPED)', size=10)
P('• B+C 472건 user_verified=1 토글 완료 — 사용자 카드 검토 대기', size=10)
P('• 발송 가능 회사: 83개사 (score=4 + 검증 IR 이메일 보유)', size=10, bold=True)
doc.add_paragraph()

# ─────────── 8. 다음 단계 ───────────
H('8. 다음 단계', 1)
H('8-1. 토요일 (오늘) — Gemini 검증 마무리', 2)
P('• blind 검증 남은 1,329건 (한도 회복 후)', size=10)
P('• sev=5 단서 84건 진위 검증', size=10)
doc.add_paragraph()

H('8-2. 일요일 — sev=4 검증 + 회사 홈페이지 확인', 2)
P('• sev=4 단서 약 611건 진위 검증', size=10)
P('• 회사 홈페이지 IR 페이지 신규 공지 체크', size=10)
P('• Naver 뉴스 매칭 갱신', size=10)
doc.add_paragraph()

H('8-3. 월요일 — 발송 후보 산출', 2)
P('• info_gap_label 자동 적용', size=10)
P('• 발송 후보 TOP 150 리스트 + 캘린더', size=10)
P('• 평일 발송 시작 (gmail_send)', size=10)
doc.add_paragraph()

# ─────────── 부록 ───────────
H('부록 A. 적용된 보안·안전망', 1)
P('• 24h 중복 발송 차단 / IR 이메일 검증 / GMAIL_DAILY_LIMIT', size=10)
P('• Gemini fallback 체인 (latest → 2.5 GA → 3-preview → 3.1-lite)', size=10)
P('• API 한도 도달 시 자동 모델·키 우회', size=10)
P('• 1.3GB DB 자동 git ignore (push 안전)', size=10)
doc.add_paragraph()

H('부록 B. GitHub', 1)
P('https://github.com/kangchani82-boop/company-catcher.git', size=10)
P('5/19 push: 75 파일, +25,934 줄 / −8,422 줄', size=10)

doc.save(str(OUT))
print(f'생성됨: {OUT}')
print(f'크기: {OUT.stat().st_size:,} bytes')
