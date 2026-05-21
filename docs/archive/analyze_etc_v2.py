# -*- coding: utf-8 -*-
"""
남은 기타(116개) 기업 심층 분석 및 자체 그룹화
- 금융·보험 / 공공기관 제외 원칙 반영
- TOC-only 패턴 탐지
- 이름·biz_content 기반 서브그룹 분류
"""
import sqlite3
from pathlib import Path
from collections import defaultdict
import re

DB_PATH = Path('data/dart/dart_reports.db')
db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row

rows = db.execute('''
    SELECT c.corp_code, c.corp_name, r.biz_content
    FROM companies c
    LEFT JOIN (
        SELECT corp_code, biz_content
        FROM reports
        WHERE biz_content IS NOT NULL AND LENGTH(biz_content) > 50
        GROUP BY corp_code
        HAVING MAX(LENGTH(biz_content))
    ) r ON c.corp_code = r.corp_code
    WHERE c.sector = '기타'
    ORDER BY c.corp_name
''').fetchall()

print(f'[기타 기업 총 {len(rows)}개]\n')

TOC_PATTERNS = [
    r'II\. 사업의 내용[」"]?\s*참고',
    r'II\. 사업의 내용[」"]?\s*을 참조',
    r'사업의 내용.*를 참고하시기 바랍니다',
    r'사업의 내용.*이를 참고하여 주시기 바랍니다',
    r'주요 제품 및 서비스.*부터.*기타 참고사항.*까지.*참고',
]
toc_re = [re.compile(p, re.DOTALL) for p in TOC_PATTERNS]

def is_toc_only(biz: str) -> bool:
    if not biz:
        return True
    if len(biz) < 300:
        return True
    head = biz[:600]
    return any(p.search(head) for p in toc_re)

# 서브그룹 규칙 (금융/공공 제외 원칙 반영)
# 이름 패턴 + biz_content 키워드 조합
SUBGROUPS = [
    # 금융계열 — 공급망 의미 없음
    ('금융계열(제외대상)', {
        'name': ['캐피탈', '파이낸스', '저축은행', '증권', '투자', 'AMC', '리츠', '자산운용', '신탁'],
        'biz':  ['여신전문', '대출업', '자산운용업', '펀드 운용', '증권 중개', '캐피탈 업무'],
    }),
    # 지주사 — 공급망 의미 없음
    ('지주사(제외대상)', {
        'name': ['홀딩스', '지주', 'Holdings'],
        'biz':  ['지주회사로서', '순수 지주', '자회사 관리', '지배회사'],
    }),
    # 공공·공기업 — 이미 공공·기관으로 분류됐어야 하나 누락
    ('공공·준공기업', {
        'name': ['공사', '공단', '공기업', '한국철도', '한국전력', '연구원', '기술원'],
        'biz':  ['국가 기관', '공익 사업', '정부 위탁', '공공 서비스'],
    }),
    # 복합사업 — 진짜 다각화
    ('복합·다각화', {
        'name': [],
        'biz':  ['사업부문별', '여러 사업', '다양한 사업', '복합 사업', '독립적인 사업', '개별 사업부'],
    }),
    # 자동차 부품 계열
    ('잠재_자동차부품', {
        'name': ['자동차', '오토', 'Auto', '모터스', '브레이크', '트랜스', '샤시', '무브'],
        'biz':  ['자동차부품', '완성차', 'OEM 납품', '차량용', '자동차 시장'],
    }),
    # 방산·항공 계열
    ('잠재_방산·항공', {
        'name': ['항공', '우주', '방산', '테크윈', '퍼스텍'],
        'biz':  ['방산', '군수', '항공 부품', '방위산업', '무기'],
    }),
    # 종합상사·무역
    ('잠재_종합상사', {
        'name': ['인터내셔널', '상사', '트레이딩', '글로벌'],
        'biz':  ['수출입', '무역', '종합 상사', '원자재 트레이딩'],
    }),
    # 포장재·제지
    ('잠재_포장·제지', {
        'name': ['제지', '페이퍼', '포장', '박스'],
        'biz':  ['판지', '제지', '포장재', '종이 박스', '패키징'],
    }),
    # 의류·섬유
    ('잠재_의류·섬유', {
        'name': ['텍스타일', '어패럴', 'Apparel', '의류', '패션'],
        'biz':  ['의류', '봉제', '직물', '원단', '섬유'],
    }),
    # 화장품·뷰티
    ('잠재_화장품', {
        'name': ['뷰티', '화장품', '코스메틱', 'Beauty', 'Cosmetic'],
        'biz':  ['화장품', '뷰티', 'ODM', 'OEM 화장'],
    }),
    # IT·통신기기
    ('잠재_통신기기', {
        'name': ['통신', '케스피온', '빅솔론', '포스링크'],
        'biz':  ['무선통신기기', '통신기기', '단말기', '프린터 제조'],
    }),
    # 환경·폐기물
    ('잠재_환경', {
        'name': ['환경', 'E&A', 'ENS', 'ECO'],
        'biz':  ['폐기물', '환경 사업', '재활용', '수처리', '환경오염'],
    }),
    # 건설·레미콘
    ('잠재_건설', {
        'name': ['건설', '건축', '토건', '엔지니어링'],
        'biz':  ['건설 사업', '레미콘', '시공', '플랜트', '건설업을 주요'],
    }),
]

groups = defaultdict(list)
toc_only_list = []
unmatched = []

for row in rows:
    corp_name = row['corp_name']
    biz = row['biz_content'] or ''

    # 먼저 TOC 패턴 확인
    if is_toc_only(biz):
        toc_only_list.append((corp_name, biz[:80]))
        continue

    # 서브그룹 매칭
    matched_group = None
    for group_name, criteria in SUBGROUPS:
        name_match = any(k in corp_name for k in criteria['name'])
        biz_match  = any(k in biz[:3000] for k in criteria['biz'])
        if name_match or biz_match:
            matched_group = group_name
            break

    if matched_group:
        groups[matched_group].append(corp_name)
    else:
        unmatched.append((corp_name, biz[:150]))

print('━' * 60)
print(f'[TOC 목차만 있어 분류 불가: {len(toc_only_list)}개]')
print('→ 연간보고서 biz_content 섹션이 목차 참조로만 작성됨')
print('  (세부 내용은 2주요제품서비스 등 다른 섹션에 분산)')
for name, snippet in toc_only_list[:8]:
    print(f'  · {name}')
if len(toc_only_list) > 8:
    print(f'  ... 외 {len(toc_only_list)-8}개')

print()
print('━' * 60)
print('[서브그룹 분류 결과]')
total_sub = 0
for group_name, cnames in sorted(groups.items(), key=lambda x: -len(x[1])):
    if cnames:
        total_sub += len(cnames)
        flag = ' ⚠ 제외권고' if '제외대상' in group_name else ''
        print(f'  {group_name}: {len(cnames)}개{flag}')
        for n in cnames[:6]:
            print(f'    · {n}')
        if len(cnames) > 6:
            print(f'    ... 외 {len(cnames)-6}개')
        print()

print(f'서브그룹 합계: {total_sub}개')
print()
print('━' * 60)
print(f'[패턴 매칭 실패(진짜 기타): {len(unmatched)}개]')
for name, snippet in unmatched[:15]:
    print(f'  · {name}: {snippet[:100]}')

db.close()
