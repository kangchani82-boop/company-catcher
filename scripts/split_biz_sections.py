"""
scripts/split_biz_sections.py
──────────────────────────────
reports.biz_content 를 표준 섹션으로 분리하여 biz_sections 테이블에 저장.

표준 섹션 (사업보고서 II. 사업의 내용):
  1. 사업의 개요 (overview)
  2. 주요 제품 및 서비스 (products)
  3. 원재료 및 생산설비 (materials)
  4. 매출 및 수주현황 (revenue)
  5. 위험관리 및 파생거래 (risk)
  6. 주요계약 및 연구개발 (contracts)
  7. 기타 참고사항 (etc)

실행:
  python scripts/split_biz_sections.py --report-type 2025_annual    # 사업보고서만
  python scripts/split_biz_sections.py                              # 전체
  python scripts/split_biz_sections.py --reset                      # 기존 삭제 후 재실행
"""
import io, re, sys, sqlite3, argparse
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# 섹션 패턴 — (section_code, regex_pattern, title)
# 정기보고서 II. 사업의 내용 표준 7개 섹션
SECTION_PATTERNS = [
    ('overview',  r'(?:^|\n)\s*\d+\.\s*사업의\s*개요',           '사업의 개요'),
    ('products',  r'(?:^|\n)\s*\d+\.\s*주요\s*제품(?:\s*및\s*서비스)?', '주요 제품 및 서비스'),
    ('materials', r'(?:^|\n)\s*\d+\.\s*원재료(?:\s*및\s*생산설비)?', '원재료 및 생산설비'),
    ('revenue',   r'(?:^|\n)\s*\d+\.\s*매출\s*(?:및\s*)?수주(?:\s*현황)?', '매출 및 수주현황'),
    ('risk',      r'(?:^|\n)\s*\d+\.\s*위험관리(?:\s*및\s*파생거래)?', '위험관리 및 파생거래'),
    ('contracts', r'(?:^|\n)\s*\d+\.\s*주요계약(?:\s*및\s*연구개발(?:활동)?)?', '주요계약 및 연구개발'),
    ('etc',       r'(?:^|\n)\s*\d+\.\s*기타\s*참고(?:사항)?', '기타 참고사항'),
]

# 다음 큰 섹션 (III, IV ... 또는 다음 II.) 시작 — 종료 시그널
END_PATTERNS = [
    r'(?:^|\n)\s*III\.\s',
    r'(?:^|\n)\s*IV\.\s',
    r'(?:^|\n)\s*Ⅲ\.\s',
    r'(?:^|\n)\s*Ⅳ\.\s',
]


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def find_section_starts(text):
    """본문에서 각 섹션 시작 위치 찾기 — [(section_code, start, end_estimate, title)]"""
    matches = []
    for code, pat, title in SECTION_PATTERNS:
        for m in re.finditer(pat, text, re.MULTILINE):
            matches.append((m.start(), code, title))
    matches.sort()  # 위치순
    return matches


def split_into_sections(text):
    """biz_content → 섹션 리스트"""
    if not text or len(text) < 100:
        return []

    starts = find_section_starts(text)
    if not starts:
        return []

    # 종료점 (다음 섹션 시작 또는 III/IV)
    end_positions = []
    for pat in END_PATTERNS:
        for m in re.finditer(pat, text, re.MULTILINE):
            end_positions.append(m.start())
    end_positions.sort()

    sections = []
    for i, (start, code, title) in enumerate(starts):
        # 다음 섹션 시작 또는 종료점 중 가까운 것
        if i + 1 < len(starts):
            end = starts[i + 1][0]
        else:
            # 마지막 섹션 — 종료점 또는 텍스트 끝
            after_end = [e for e in end_positions if e > start]
            end = min(after_end) if after_end else len(text)
        content = text[start:end].strip()
        if len(content) < 20:  # 너무 짧으면 스킵
            continue
        sections.append({
            'code': code, 'title': title,
            'content': content, 'char_count': len(content),
        })
    return sections


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--report-type', default='', help='필터 (예: 2025_annual)')
    ap.add_argument('--reset', action='store_true')
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()

    db = get_db()
    if args.reset:
        n = db.execute("DELETE FROM biz_sections").rowcount
        db.commit()
        print(f'기존 biz_sections {n}건 삭제\n')

    where_sql = "WHERE biz_content IS NOT NULL AND length(biz_content) > 1000"
    params = []
    if args.report_type:
        where_sql += " AND report_type = ?"
        params.append(args.report_type)

    rows = db.execute(f"""
        SELECT r.id, r.corp_code, r.corp_name, r.report_type, r.biz_content,
               c.sector
        FROM reports r LEFT JOIN companies c ON r.corp_code = c.corp_code
        {where_sql}
    """, params).fetchall()

    if args.limit > 0:
        rows = rows[:args.limit]

    print(f'처리 대상: {len(rows):,}건')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    stats = {'reports': 0, 'sections': 0, 'no_section': 0, 'errors': 0}
    section_counts = {code: 0 for code, _, _ in SECTION_PATTERNS}

    for r in rows:
        try:
            sections = split_into_sections(r['biz_content'])
            stats['reports'] += 1
            if not sections:
                stats['no_section'] += 1
                continue
            for s in sections:
                db.execute("""
                    INSERT INTO biz_sections
                        (corp_code, corp_name, sector, report_id, report_type,
                         section_code, section_title, content, char_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [r['corp_code'], r['corp_name'], r['sector'] or '', r['id'],
                      r['report_type'], s['code'], s['title'], s['content'],
                      s['char_count'], now])
                stats['sections'] += 1
                section_counts[s['code']] += 1
            if stats['reports'] % 500 == 0:
                db.commit()
                print(f"  진행 {stats['reports']:,} / 섹션 {stats['sections']:,}")
        except Exception as e:
            stats['errors'] += 1
            if stats['errors'] <= 3:
                print(f"  ERR {r['corp_name']}: {str(e)[:60]}")

    db.commit()
    print('\n' + '━' * 60)
    print(f"  처리 완료")
    print('━' * 60)
    print(f"  보고서        : {stats['reports']:,}건")
    print(f"  추출 섹션      : {stats['sections']:,}건")
    print(f"  섹션 없음      : {stats['no_section']:,}건")
    print(f"  에러          : {stats['errors']:,}건")
    print(f"\n  섹션별 분포:")
    for code, _, title in SECTION_PATTERNS:
        print(f"    {code:10s} ({title:20s}): {section_counts[code]:>5,}건")


if __name__ == '__main__':
    main()
