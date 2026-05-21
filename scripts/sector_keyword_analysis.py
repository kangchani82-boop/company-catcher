"""
scripts/sector_keyword_analysis.py
───────────────────────────────────
업종별·섹션별 키워드 + 수치 분석. (Gemini 미사용)

분석 대상:
  - biz_sections 테이블 (사업보고서 II. 사업의 내용 분리됨)
  - 업종(sector) × 섹션(section_code) 매트릭스

추출:
  1) 업종별 자주 등장하는 키워드 (TF-IDF 기반)
  2) 가동률 수치 (정규식: XX% / XX.X% 패턴)
  3) 매출 수치 / 영업이익 수치
  4) 증설 / 신설 키워드 분포

산출물:
  - sector_keyword_summary 테이블 (업종 × 섹션별 인사이트)
  - JSON: data/sector_analysis.json
"""
import io, re, sys, json, sqlite3, argparse, math
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUT_JSON = ROOT / "data" / "sector_analysis.json"

# 일반 불용어 (회사·문법어)
STOPWORDS = set([
    '사업', '회사', '제품', '서비스', '주요', '관련', '대한', '있습니다', '없습니다',
    '있으며', '등의', '등이', '같은', '에는', '으로', '으로부터', '에서', '하여',
    '되는', '되었습니다', '예정', '계획', '고려', '판단', '경우', '대해', '현재',
    '존재', '발생', '관리', '운영', '진행', '확대', '강화', '개발', '제공', '추진',
    '활용', '구성', '포함', '기준', '의해', '으로써', '및', '이', '가', '를', '의',
    '이며', '입니다', '하고', '하는', '하기', '한', '될', '될', '수', '것', '때',
    '내', '외', '간', '및', '등', '또는', '그', '이', '저', '본', '시', '월', '년',
    'II', 'III', 'IV', '가', '나', '다', '라', '마', '바', '사',
    '주식회사', '주식', '회사의', '시장', '시장의', '국내', '글로벌', '세계', '국제',
    '당사', '본사', '저희', '귀사',
])

# 한국어 명사 후보 (간단 — 2~5자 한글)
TOKEN_RE = re.compile(r'[가-힣]{2,6}')

# 가동률 / 매출 패턴
UTILIZATION_RE = re.compile(r'(?:가동률|가동\s*율|이용률|capacity|utilization)[\s\D]{0,30}?(\d{1,3}(?:\.\d{1,2})?)\s*[%％]', re.IGNORECASE)
REVENUE_RE = re.compile(r'(?:매출|매출액|영업\s*이익|순이익)\s*(?:[은는이가]\s*)?\D{0,10}?(\d{1,3}(?:,\d{3})+|\d+\.\d+)\s*(?:억|조|백만)\s*원?')
EXPAND_RE = re.compile(r'(?:증설|신설|신축|확장|준공|착공|투자\s*확대|capacity\s*증가)')


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def tokenize(text):
    """한국어 텍스트 토큰화 (간단)"""
    if not text: return []
    tokens = TOKEN_RE.findall(text)
    return [t for t in tokens if t not in STOPWORDS and len(t) >= 2]


def compute_tfidf(sector_docs):
    """업종별 TF-IDF — 다른 업종 대비 차별 키워드"""
    # sector_docs: {sector: [doc1_tokens, doc2_tokens, ...]}
    # IDF 기준: 업종 단위 (각 업종 = 하나의 큰 doc)
    sector_terms = {}
    for sector, docs in sector_docs.items():
        all_tokens = []
        for d in docs:
            all_tokens.extend(d)
        sector_terms[sector] = Counter(all_tokens)

    # IDF
    n_sectors = len(sector_terms)
    df = Counter()
    for sector, cnts in sector_terms.items():
        for term in cnts:
            df[term] += 1

    # TF-IDF per sector
    result = {}
    for sector, cnts in sector_terms.items():
        scores = {}
        total = sum(cnts.values()) or 1
        for term, freq in cnts.items():
            tf = freq / total
            idf = math.log(n_sectors / (1 + df[term]))
            scores[term] = tf * idf
        # 상위 30개
        top = sorted(scores.items(), key=lambda x: -x[1])[:30]
        result[sector] = top
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--section', default='', help='특정 섹션만 (overview/products/materials/...)')
    args = ap.parse_args()

    db = get_db()
    print('━' * 60)
    print('  업종별 키워드 + 수치 분석')
    print('━' * 60)

    # 1. 업종 × 섹션 데이터 로드
    where = "WHERE sector IS NOT NULL AND sector != '' AND content IS NOT NULL"
    if args.section:
        where += f" AND section_code = '{args.section}'"
    rows = db.execute(f"""
        SELECT corp_code, corp_name, sector, section_code, content
        FROM biz_sections {where}
    """).fetchall()
    print(f'  대상 섹션: {len(rows):,}건')

    # 섹션별 분리
    section_data = defaultdict(lambda: defaultdict(list))  # {section: {sector: [content,...]}}
    for r in rows:
        section_data[r['section_code']][r['sector']].append({
            'corp_code': r['corp_code'], 'corp_name': r['corp_name'],
            'content': r['content']
        })

    out = {'generated_at': datetime.now().isoformat(), 'sections': {}}

    # 2. 섹션별 분석
    for section_code, sector_docs in section_data.items():
        print(f'\n=== 섹션: {section_code} ===')
        # 토큰화
        sector_tokens = {}
        for sector, docs in sector_docs.items():
            sector_tokens[sector] = [tokenize(d['content']) for d in docs]

        # TF-IDF
        tfidf = compute_tfidf(sector_tokens)

        # 가동률·매출·증설 통계 (섹션·업종별)
        nums = defaultdict(lambda: {'utilization': [], 'revenues': [], 'expand_count': 0, 'companies': set()})
        for sector, docs in sector_docs.items():
            for d in docs:
                txt = d['content']
                for m in UTILIZATION_RE.finditer(txt):
                    try:
                        v = float(m.group(1))
                        if 0 < v <= 110:  # 정상 범위
                            nums[sector]['utilization'].append({'corp': d['corp_name'], 'value': v})
                    except: pass
                for m in REVENUE_RE.finditer(txt):
                    nums[sector]['revenues'].append({'corp': d['corp_name'], 'value': m.group(0)[:30]})
                if EXPAND_RE.search(txt):
                    nums[sector]['expand_count'] += 1
                    nums[sector]['companies'].add(d['corp_name'])

        section_summary = {}
        for sector in sector_docs:
            uti = nums[sector]['utilization']
            avg_uti = sum(u['value'] for u in uti) / len(uti) if uti else None
            section_summary[sector] = {
                'company_count': len(sector_docs[sector]),
                'top_keywords': [t for t, _ in tfidf.get(sector, [])[:15]],
                'avg_utilization': round(avg_uti, 1) if avg_uti else None,
                'utilization_samples': uti[:5],
                'expand_companies': list(nums[sector]['companies'])[:10],
                'expand_count': nums[sector]['expand_count'],
                'revenue_mentions': len(nums[sector]['revenues']),
            }
            print(f"  {sector:25s} {len(sector_docs[sector]):>4}개사 | 가동률 평균 {avg_uti or '-':>6} | 증설 언급 {nums[sector]['expand_count']:>3}개사")

        out['sections'][section_code] = section_summary

    # 저장
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(f'\n💾 {OUT_JSON} 저장')


if __name__ == '__main__':
    main()
