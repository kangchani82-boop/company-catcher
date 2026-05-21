# -*- coding: utf-8 -*-
"""
dart/news_analyzer.py
─────────────────────
뉴스 텍스트 → 관련주 탐색 엔진

사용법:
    from dart.news_analyzer import find_related_stocks
    result = find_related_stocks("삼성전자, 하이닉스 메모리 감산...")
"""

import re
import sqlite3
from pathlib import Path
from collections import defaultdict

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# ── 섹터 키워드 매핑 (뉴스 텍스트 → 해당 섹터 기업 연관도) ─────────────────
NEWS_SECTOR_KEYWORDS: dict[str, list[str]] = {
    '반도체·전자': [
        '반도체', 'D램', 'HBM', '낸드', '메모리', '파운드리', 'OLED', 'LCD',
        '디스플레이', '패키징', '웨이퍼', 'AI칩', '팹리스', '적층',
    ],
    '바이오·제약': [
        '바이오', '의약품', '신약', '임상', '항암', '의료기기', '바이오시밀러',
        'mRNA', '세포치료', '유전자', 'ADC', 'FDA', '허가',
    ],
    '자동차·배터리': [
        '전기차', 'EV', '배터리', '이차전지', '양극재', '음극재', '전해질',
        '자동차', '완성차', '수주', '타이어', 'ADAS', '자율주행',
    ],
    '화학·소재': [
        '화학', '소재', '도료', '페인트', '필름', '수지', '에탄올', '올레핀',
        '석유화학', '폴리머', '촉매',
    ],
    '철강·금속': [
        '철강', '제철', '강판', '알루미늄', '동', '구리', '스테인리스',
        '특수강', '철근', '메탈',
    ],
    '건설·인프라': [
        '건설', '분양', '재개발', '재건축', '레미콘', '시멘트', '아파트',
        '인프라', '플랜트', '발주',
    ],
    'IT·소프트웨어': [
        '소프트웨어', 'AI', '인공지능', '클라우드', '데이터센터', '반도체 소프트웨어',
        '게임', '플랫폼', '핀테크', '사이버보안', '빅데이터',
    ],
    '에너지·자원': [
        '원유', '가스', 'LNG', '발전', '태양광', '풍력', '수소', '전력',
        '원전', '신재생', '탄소',
    ],
    '금융·보험': [
        '금리', '환율', '증시', '주가', '주식시장',
        '은행업', '보험업', '증권사', '금융주',
        '저축은행', '캐피탈사', '카드사',
    ],
    '미디어·엔터': [
        '엔터', 'K팝', '드라마', 'OTT', '콘텐츠', '영화', '음원',
        '웹툰', '유튜브', '광고',
    ],
    '유통·물류': [
        '유통', '물류', '이커머스', '배송', '항공', '해운', '수출',
        '무역', '관세',
    ],
    '조선·방산': [
        '조선', '선박', '방산', '군수', '항공기', '무기', '방위',
        '수출', '드론',
    ],
    '통신': [
        '5G', '6G', '통신', '이동통신', 'SKT', 'KT', 'LGU',
        '통신망', '위성',
    ],
    '식품·소비재': [
        '식품', '음료', '주류', '화장품', '뷰티', '의류', '패션',
        '소비재', '프랜차이즈',
    ],
}

# ── 제외 패턴 (노이즈 — 뉴스에 자주 등장하는 비기업명) ─────────────────────
_NOISE_WORDS = {
    '정부', '금융위', '공정위', '국세청', '법원', '검찰', '경찰', '국회',
    '대통령', '장관', '의원', '시장', '도지사', '시의회',
    '한국', '미국', '중국', '일본', '유럽', '독일', '인도', '베트남',
    '서울', '부산', '인천', '수도권',
    '기자', '대표', '회장', '사장', '부회장', '대표이사',
}

# ── 점수 기준 ───────────────────────────────────────────────────────────────
SCORE_DIRECT_MENTION  = 100   # 뉴스에 기업명 직접 언급
SCORE_SUPPLY_1ST      = 60    # 1차 공급망 관계
SCORE_SUPPLY_2ND      = 25    # 2차 공급망 관계
SCORE_SECTOR_MATCH    = 30    # 섹터 키워드 일치
SCORE_BIZ_MATCH       = 15    # biz_content 키워드 일치 (건당)
SCORE_BIZ_MAX         = 40    # biz_content 최대 점수


def _get_db() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


_KO_PARTICLES = re.compile(
    r'(은|는|이|가|을|를|의|에|에서|으로|로|과|와|도|만|부터|까지|보다|처럼|라서|이어서|으로서|에게|한테|께서|이다|입니다|합니다|했다|한다|된다|된다고|하는|한|된|으로는|에는|이는|가는)$'
)

def _strip_particles(word: str) -> str:
    """한글 단어 끝 조사 제거"""
    return _KO_PARTICLES.sub('', word)


def _extract_news_keywords(text: str) -> list[str]:
    """뉴스 텍스트에서 의미 있는 키워드 추출"""
    text = re.sub(r'\s+', ' ', text).strip()

    # 한글 어절(공백 구분) 추출 → 조사 제거
    raw_words = re.findall(r'[가-힣]{2,}', text)
    words = list(dict.fromkeys(_strip_particles(w) for w in raw_words))  # dedup

    # 영문 약어/기업명 (2글자 이상, 숫자 포함 가능)
    eng_words = re.findall(r'\b[A-Z][A-Za-z0-9&\-]{1,}\b', text)

    filtered = [w for w in words if w not in _NOISE_WORDS and len(w) >= 2]
    filtered += eng_words

    seen = set()
    result = []
    for w in filtered:
        if w not in seen:
            seen.add(w)
            result.append(w)
    return result[:100]


def _detect_sectors(keywords: list[str]) -> list[tuple[str, int]]:
    """키워드 리스트 → 섹터별 매칭 점수"""
    sector_scores: dict[str, int] = defaultdict(int)
    for kw in keywords:
        for sector, sector_kws in NEWS_SECTOR_KEYWORDS.items():
            for skw in sector_kws:
                if skw in kw or kw in skw:
                    sector_scores[sector] += 1
    return sorted(sector_scores.items(), key=lambda x: -x[1])


def _clean_corp_name(name: str) -> str:
    """기업명에서 법인 형태 제거"""
    return re.sub(r'(주식회사|㈜|\(주\)|\(株\)|주식|회사|그룹|\(코스닥\)|\(코스피\))', '', name).strip()


# 한글 조사 패턴 (기업명 뒤에 올 수 있는 것들)
_KO_SUFFIX = r'(?:[은는이가을를의에에서으로로과와도만이다이라고가]|$|\s|[.,;:!?()\'\"·—…])'


def _find_companies_by_name(news_text: str, all_corps: list[dict]) -> dict[str, int]:
    """뉴스 텍스트에서 기업명 직접 언급 탐지.
    한국어 조사 처리: '삼성전자는', '삼성전자가' 등 매칭.
    """
    matches: dict[str, int] = {}

    for corp in all_corps:
        name = corp['corp_name']
        clean = _clean_corp_name(name)
        if len(clean) < 3:  # 3글자 미만 — 오탐 위험
            continue

        # 1. 원본 이름 또는 정제 이름이 텍스트에 포함되는지 확인
        if clean not in news_text and name not in news_text:
            continue

        # 2. 더 긴 이름의 일부가 아닌지 확인 (e.g., "SK"가 "SK하이닉스" 내부에 있지 않아야)
        # 접두어 패턴: 앞에 한글/영숫자가 없어야 함
        pattern = r'(?<![가-힣A-Za-z0-9])' + re.escape(clean) + _KO_SUFFIX
        if re.search(pattern, news_text):
            matches[corp['corp_code']] = SCORE_DIRECT_MENTION

    return matches


def _find_supply_chain_related(
    direct_codes: list[str], db: sqlite3.Connection
) -> dict[str, tuple[int, str, str]]:
    """
    직접 언급 기업들의 1·2차 공급망 관계사 탐색
    Returns: {corp_code: (score, relation_type, via_corp_name)}
    """
    result: dict[str, tuple[int, str, str]] = {}
    if not direct_codes:
        return result

    # 1차 공급망 (직접 언급 기업의 supplier / customer)
    placeholders = ','.join('?' * len(direct_codes))
    rows = db.execute(f"""
        SELECT DISTINCT sc.corp_code, sc.corp_name, sc.relation_type,
               c2.corp_name AS via_name
        FROM supply_chain sc
        JOIN companies c2 ON c2.corp_code IN ({placeholders})
        WHERE sc.partner_name = c2.corp_name
        LIMIT 300
    """, direct_codes).fetchall()

    for r in rows:
        cc = r['corp_code']
        if cc not in result:
            result[cc] = (SCORE_SUPPLY_1ST, r['relation_type'], r['via_name'])

    # 추가: direct_codes가 partner_name으로 언급되는 기업들
    rows2 = db.execute(f"""
        SELECT DISTINCT corp_code, corp_name, relation_type
        FROM supply_chain
        WHERE partner_name IN (
            SELECT corp_name FROM companies WHERE corp_code IN ({placeholders})
        )
        AND corp_code NOT IN ({placeholders})
    """, direct_codes * 2).fetchall()

    for r in rows2:
        cc = r['corp_code']
        if cc not in result:
            via = '직접 언급 기업의 ' + ('공급사' if r['relation_type'] == 'supplier' else '고객사')
            result[cc] = (SCORE_SUPPLY_1ST, r['relation_type'], via)

    return result


def find_related_stocks(
    news_text: str,
    top_n: int = 30,
    include_biz_match: bool = True
) -> dict:
    """
    뉴스 텍스트 → 관련주 분석 메인 함수

    Args:
        news_text: 뉴스 본문 (제목 포함 권장)
        top_n: 반환할 관련주 최대 수
        include_biz_match: biz_content 키워드 매칭 포함 여부

    Returns:
        {
            "keywords": [...],          # 추출된 키워드
            "matched_sectors": [...],   # 관련 섹터 (점수 순)
            "direct_mentions": [...],   # 뉴스에 직접 언급된 기업
            "related_stocks": [
                {
                    "corp_code": str,
                    "corp_name": str,
                    "sector": str,
                    "stock_code": str | None,
                    "score": int,
                    "reason": str,
                    "relation_type": "direct|supply_chain|sector|keyword",
                    "supply_via": str | None,
                }
            ]
        }
    """
    db = db = _get_db()

    try:
        # 1. 전체 기업 목록 로드 (이름 매칭용)
        all_corps = db.execute(
            "SELECT corp_code, corp_name, sector, stock_code FROM companies"
        ).fetchall()
        all_corps = [dict(r) for r in all_corps]

        # 제외 섹터 (공급망 의미 없음)
        EXCLUDE_SECTORS = {'금융·보험', '공공·기관'}
        all_corps_filtered = [c for c in all_corps if c.get('sector') not in EXCLUDE_SECTORS]

        # 2. 키워드 추출
        keywords = _extract_news_keywords(news_text)

        # 3. 섹터 매칭
        sector_scores = _detect_sectors(keywords)
        top_sectors = [s for s, score in sector_scores if score > 0]

        # 4. 직접 언급 기업 탐지
        direct_matches = _find_companies_by_name(news_text, all_corps_filtered)

        # 5. 공급망 연관 기업 탐색
        supply_related = _find_supply_chain_related(
            list(direct_matches.keys()), db
        )

        # 6. biz_content 키워드 매칭 (선택적)
        biz_scores: dict[str, int] = defaultdict(int)
        if include_biz_match and keywords:
            # 주요 키워드로만 검색 (성능 고려 상위 20개)
            top_kws = keywords[:20]
            for corp in all_corps_filtered:
                cc = corp['corp_code']
                if cc in direct_matches:
                    continue  # 이미 직접 언급
                # biz_content 조회 (캐시 없이)
            # biz_content는 공급망 데이터로 간접 매칭 (섹터 기반)

        # 7. 섹터 기반 관련주 (상위 섹터의 기업들)
        sector_related: dict[str, int] = {}
        sector_set = set(top_sectors[:3])  # 상위 3개 섹터
        for corp in all_corps_filtered:
            cc = corp['corp_code']
            if cc in direct_matches or cc in supply_related:
                continue
            if corp.get('sector') in sector_set:
                sector_related[cc] = SCORE_SECTOR_MATCH

        # 8. 통합 점수 계산 및 정렬
        all_scores: dict[str, dict] = {}

        # 기업 정보 룩업
        corp_lookup = {c['corp_code']: c for c in all_corps}

        def _add(cc: int, score: int, reason: str, rtype: str, via: str | None = None):
            if cc not in corp_lookup:
                return
            corp = corp_lookup[cc]
            if corp.get('sector') in EXCLUDE_SECTORS:
                return
            if cc not in all_scores:
                all_scores[cc] = {
                    'corp_code': cc,
                    'corp_name': corp['corp_name'],
                    'sector': corp.get('sector') or '기타',
                    'stock_code': corp.get('stock_code'),
                    'score': 0,
                    'reasons': [],
                    'relation_type': rtype,
                    'supply_via': via,
                }
            all_scores[cc]['score'] += score
            all_scores[cc]['reasons'].append(reason)
            # 가장 강한 relation_type 유지
            priority = {'direct': 0, 'supply_chain': 1, 'sector': 2, 'keyword': 3}
            if priority.get(rtype, 9) < priority.get(all_scores[cc]['relation_type'], 9):
                all_scores[cc]['relation_type'] = rtype
                if via:
                    all_scores[cc]['supply_via'] = via

        for cc, score in direct_matches.items():
            corp = corp_lookup.get(cc, {})
            _add(cc, score, '뉴스 직접 언급', 'direct')

        for cc, (score, rel, via) in supply_related.items():
            rel_label = '공급사' if rel == 'supplier' else '고객사'
            _add(cc, score, f'공급망 연관 ({rel_label} via {via})', 'supply_chain', via)

        for cc, score in sector_related.items():
            sector = corp_lookup.get(cc, {}).get('sector', '')
            _add(cc, score, f'관련 섹터: {sector}', 'sector')

        # 9. 최종 정렬
        ranked = sorted(all_scores.values(), key=lambda x: (-x['score'], x['corp_name']))

        # reason 합치기
        for item in ranked:
            item['reason'] = ' · '.join(item['reasons'][:3])
            del item['reasons']

        # direct_mentions 목록
        direct_names = [
            corp_lookup[cc]['corp_name']
            for cc in direct_matches if cc in corp_lookup
        ]

        return {
            'keywords': keywords[:30],
            'matched_sectors': [{'sector': s, 'score': sc} for s, sc in sector_scores[:5]],
            'direct_mentions': direct_names,
            'related_stocks': ranked[:top_n],
            'total_found': len(ranked),
        }

    finally:
        db.close()


if __name__ == '__main__':
    # 테스트
    test_news = """
    삼성전자와 SK하이닉스가 HBM 메모리 수요 급증으로 올해 반도체 설비투자를
    대폭 확대한다고 밝혔다. 엔비디아가 AI GPU 수요 증가로 HBM3E 공급 확대를
    요청했으며, 국내 소재·부품업체들의 수혜가 예상된다.
    한편 TSMC는 파운드리 가동률이 90%를 초과했다고 발표했다.
    """
    result = find_related_stocks(test_news)
    print(f"키워드: {result['keywords'][:10]}")
    print(f"관련 섹터: {result['matched_sectors']}")
    print(f"직접 언급: {result['direct_mentions']}")
    print(f"관련주 ({result['total_found']}개):")
    for s in result['related_stocks'][:10]:
        print(f"  [{s['score']:3d}] {s['corp_name']:<20s} {s['sector']:<15s} {s['reason']}")
