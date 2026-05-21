"""
scripts/tag_corp_taxonomy.py
─────────────────────────────
biz_sections (사업보고서 II. 사업의 내용 분리됨) → 자동 태깅.

각 회사별로 sector 안에서 4개 분류축 (제품군/공정/가치사슬/적용처) 카테고리 매칭.

키워드 매치 빈도로 신뢰도 결정:
  HIGH   : 5+회 등장 + products/materials 섹션
  MEDIUM : 2-4회
  LOW    : 1회

산출:
  corp_taxonomy 테이블 (corp_code, axis, category, confidence, keyword_count)

실행:
  python scripts/tag_corp_taxonomy.py            # 전체
  python scripts/tag_corp_taxonomy.py --reset    # 기존 삭제 후 재태깅
  python scripts/tag_corp_taxonomy.py --sector 반도체·전자  # 특정 sector만
"""
import io, sys, json, sqlite3, argparse, re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

sys.path.insert(0, str(ROOT / 'scripts'))
from _data_taxonomy import SECTOR_TAXONOMY


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def ensure_table(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS corp_taxonomy (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            corp_code TEXT NOT NULL,
            corp_name TEXT,
            sector TEXT,
            axis TEXT,
            category TEXT,
            confidence TEXT,
            keyword_count INTEGER DEFAULT 0,
            matched_keywords TEXT,
            created_at TEXT,
            UNIQUE(corp_code, axis, category)
        )
    ''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_tax_corp ON corp_taxonomy(corp_code)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_tax_sector ON corp_taxonomy(sector, axis, category)')
    db.commit()


def determine_confidence(count, in_core_section):
    """매치 횟수 + 핵심 섹션 등장 여부로 신뢰도 결정"""
    if count >= 5 and in_core_section:
        return 'HIGH'
    elif count >= 5 or (count >= 2 and in_core_section):
        return 'MEDIUM'
    elif count >= 1:
        return 'LOW'
    return None


def tag_company(db, corp_code, corp_name, sector, sections):
    """한 회사 분석 — sections: dict[section_code] = content"""
    axes_def = SECTOR_TAXONOMY.get(sector)
    if not axes_def:
        return []

    # 핵심 섹션 (products/materials/overview)
    core_text = '\n'.join([sections.get(s, '') for s in ['products', 'materials', 'overview']])
    full_text = '\n'.join(sections.values())

    tags = []
    for axis_name, categories in axes_def.items():
        for category_name, keywords in categories.items():
            count_full = 0
            count_core = 0
            matched = []
            for kw in keywords:
                # 정확 매칭 (대소문자 무시)
                pattern = re.escape(kw)
                full_count = len(re.findall(pattern, full_text, re.IGNORECASE))
                core_count = len(re.findall(pattern, core_text, re.IGNORECASE))
                count_full += full_count
                count_core += core_count
                if full_count > 0:
                    matched.append(kw)

            in_core = count_core > 0
            conf = determine_confidence(count_full, in_core)
            if conf:
                tags.append({
                    'axis': axis_name,
                    'category': category_name,
                    'confidence': conf,
                    'keyword_count': count_full,
                    'matched_keywords': matched[:10],
                })
    return tags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--reset', action='store_true')
    ap.add_argument('--sector', default='', help='특정 sector만')
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()

    db = get_db()
    ensure_table(db)
    if args.reset:
        n = db.execute("DELETE FROM corp_taxonomy").rowcount
        db.commit()
        print(f'기존 corp_taxonomy {n}건 삭제\n')

    # 회사별 섹션 모으기
    where = "WHERE sector IS NOT NULL AND sector != ''"
    params = []
    if args.sector:
        where += " AND sector = ?"
        params.append(args.sector)

    rows = db.execute(f"""
        SELECT corp_code, corp_name, sector, section_code, content
        FROM biz_sections {where}
    """, params).fetchall()

    # 회사 단위로 그룹화
    corp_data = defaultdict(lambda: {'sector': '', 'corp_name': '', 'sections': {}})
    for r in rows:
        corp_data[r['corp_code']]['corp_name'] = r['corp_name']
        corp_data[r['corp_code']]['sector'] = r['sector']
        corp_data[r['corp_code']]['sections'][r['section_code']] = r['content']

    if args.limit > 0:
        keys = list(corp_data.keys())[:args.limit]
        corp_data = {k: corp_data[k] for k in keys}

    print('━' * 60)
    print(f'  자동 태깅 — 대상 {len(corp_data):,}개사')
    print('━' * 60)

    stats = {'companies': 0, 'tags': 0, 'no_tag': 0, 'sector_unsupported': 0}
    confidence_count = defaultdict(int)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for corp_code, info in corp_data.items():
        sector = info['sector']
        if sector not in SECTOR_TAXONOMY or not SECTOR_TAXONOMY.get(sector):
            stats['sector_unsupported'] += 1
            continue
        tags = tag_company(db, corp_code, info['corp_name'], sector, info['sections'])
        if not tags:
            stats['no_tag'] += 1
            continue
        stats['companies'] += 1
        for t in tags:
            try:
                db.execute("""
                    INSERT OR REPLACE INTO corp_taxonomy
                        (corp_code, corp_name, sector, axis, category,
                         confidence, keyword_count, matched_keywords, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [corp_code, info['corp_name'], sector,
                      t['axis'], t['category'], t['confidence'],
                      t['keyword_count'],
                      json.dumps(t['matched_keywords'], ensure_ascii=False),
                      now])
                stats['tags'] += 1
                confidence_count[t['confidence']] += 1
            except Exception as e:
                pass
        if stats['companies'] % 200 == 0:
            db.commit()
            print(f"  진행 {stats['companies']:,} / 태그 {stats['tags']:,}")

    db.commit()
    print('\n' + '━' * 60)
    print('  결과')
    print('━' * 60)
    print(f"  태깅된 회사    : {stats['companies']:,}개")
    print(f"  생성 태그      : {stats['tags']:,}건")
    print(f"  태그 없음      : {stats['no_tag']:,}개")
    print(f"  미지원 sector  : {stats['sector_unsupported']:,}개")
    print(f"\n  신뢰도 분포: {dict(confidence_count)}")

    # 샘플 — 반도체·전자 메모리 분류 회사
    print('\n[샘플: 반도체·전자 → 제품군 → 메모리 (HIGH 등급)]')
    for r in db.execute("""
        SELECT corp_name, keyword_count
        FROM corp_taxonomy
        WHERE sector='반도체·전자' AND axis='제품군' AND category='메모리' AND confidence='HIGH'
        ORDER BY keyword_count DESC LIMIT 10
    """):
        print(f'  {r[0]:25s} ({r[1]} 회 매치)')


if __name__ == '__main__':
    main()
