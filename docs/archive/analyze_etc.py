import sqlite3
from pathlib import Path
from collections import defaultdict

DB_PATH = Path('data/dart/dart_reports.db')
db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row

rows = db.execute('''
    SELECT c.corp_code, c.corp_name, r.biz_content
    FROM companies c
    LEFT JOIN (
        SELECT corp_code, biz_content
        FROM reports
        WHERE biz_content IS NOT NULL AND LENGTH(biz_content) > 100
        GROUP BY corp_code
        HAVING MAX(LENGTH(biz_content))
    ) r ON c.corp_code = r.corp_code
    WHERE c.sector = '기타'
''').fetchall()

print(f'기타 기업 총: {len(rows)}')

groups = [
    ('건설·부동산', ['건설공사', '시공', '분양', '임대사업', '부동산', '아파트', '오피스텔', '토목공사', '건축공사']),
    ('도소매·유통', ['도매업', '소매업', '유통업', '판매업', '무역업', '수출입', '상사업', '트레이딩']),
    ('음식·식품', ['식품제조', '식품판매', '음식점', '음료제조', '주류제조', '조미료', '외식업', '베이커리']),
    ('교육·출판', ['교육서비스', '학원업', '출판업', '도서', '인쇄업', '교재', '학습지']),
    ('물류·운송', ['물류업', '운송업', '배송업', '택배업', '운수업', '항만운영', '창고업']),
    ('농업·수산', ['농업', '농산물', '수산업', '어업', '양식업', '축산업', '농작물']),
    ('섬유·의류', ['섬유', '의류', '패션', '직물', '봉제', '원단', '니트']),
    ('일반서비스', ['용역업', '청소용역', '경비업', '인력공급', '아웃소싱', '시설관리업']),
    ('스포츠·레저', ['스포츠', '레저', '관광', '여행업', '골프장', '리조트', '호텔업']),
    ('화학·소재', ['화학제품', '플라스틱', '합성수지', '고무제품', '화학원료', '도료']),
    ('지주·투자', ['지주회사', '투자사업', '벤처투자', '사모펀드', '자산운용업']),
    ('폐기물·환경', ['폐기물', '환경', '재활용', '수처리', '폐수처리', '대기오염방지']),
    ('숙박·관광', ['숙박업', '호텔', '펜션', '여행사', '관광지']),
    ('광고·마케팅', ['광고업', '광고대행', '마케팅', '홍보', '브랜드']),
]

group_counts = defaultdict(list)
unclassified = []

for row in rows:
    biz = (row['biz_content'] or '')[:2000]
    matched = False
    for g, kws in groups:
        if any(kw in biz for kw in kws):
            group_counts[g].append(row['corp_name'])
            matched = True
            break
    if not matched:
        unclassified.append((row['corp_name'], biz[:200] if biz else '없음'))

print()
total_grouped = 0
for g, kws in groups:
    names = group_counts[g]
    if names:
        print(f'{g}: {len(names)}개 -- {", ".join(names[:5])}{"..." if len(names) > 5 else ""}')
        total_grouped += len(names)

print(f'\n그룹 분류됨: {total_grouped}개')
print(f'미분류(진짜 기타): {len(unclassified)}개')
print()
print('--- 미분류 샘플 (처음 20개) ---')
for name, biz in unclassified[:20]:
    print(f'  {name}: {biz[:120]}')

db.close()
