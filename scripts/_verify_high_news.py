"""
scripts/_verify_high_news.py
─────────────────────────────
HIGH 단서 회사들의 실제 Naver 뉴스 매칭 — Gemini 추정 검증.

각 HIGH 단서:
  1. 회사명 + 핵심 키워드(lead_type 매핑)로 Naver 뉴스 검색 (최근 30일)
  2. 매칭 카운트 + URL 저장
  3. 매칭 0건 → 정보 격차 확정 (gap_label='HIGH_CONFIRMED')
  4. 매칭 多 → 후속 취재 (gap_label='HIGH_REPORTED')

API: Naver 검색 (시간당 1,000회 무료)
"""
import os, sys, json, sqlite3, time
import urllib.request, urllib.parse, urllib.error
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# .env
_env = ROOT / ".env"
if _env.exists():
    for line in _env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

CID = os.environ.get("NAVER_CLIENT_ID","").strip()
CSC = os.environ.get("NAVER_CLIENT_SECRET","").strip()
if not (CID and CSC):
    print("[오류] NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정")
    sys.exit(1)

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row

# 컬럼 추가
cols = [c[1] for c in db.execute('PRAGMA table_info(story_leads)')]
for c in ['actual_news_count', 'news_match_urls', 'news_verified_at']:
    if c not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {c} TEXT')

# lead_type별 핵심 키워드
TYPE_KW = {
    'market_shift':    ['시장 점유율', '경쟁'],
    'strategy_change': ['사업', '신규', '진출'],
    'risk_alert':      ['리스크', '손실'],
    'numeric_change':  ['실적', '매출'],
    'supply_chain':    ['공급', '거래처'],
}

# Naver 검색
def naver_news(query, display=10):
    q = urllib.parse.quote(query)
    url = f"https://openapi.naver.com/v1/search/news.json?query={q}&display={display}&sort=date"
    req = urllib.request.Request(url, headers={
        "X-Naver-Client-Id": CID,
        "X-Naver-Client-Secret": CSC,
        "User-Agent": "Mozilla/5.0"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 429:
            time.sleep(10)
            return None
        return None
    except Exception:
        return None

# 30일 cutoff
CUTOFF = datetime.now() - timedelta(days=30)

# HIGH 단서 가져오기
leads = db.execute("""
    SELECT id, corp_code, corp_name, title, lead_type, keywords
    FROM story_leads
    WHERE info_gap_label='HIGH' AND actual_news_count IS NULL
    ORDER BY severity DESC, id DESC
""").fetchall()

print(f'검증 대상 HIGH 단서: {len(leads)}건\n')

now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
total_zero = 0   # 진짜 정보 격차 (매칭 0)
total_match = 0  # 보도 매칭

for i, r in enumerate(leads, 1):
    # 쿼리: 회사명 + lead_type 핵심 키워드 1개
    extra = TYPE_KW.get(r['lead_type'], [''])[0]
    query = f"{r['corp_name']} {extra}".strip()

    res = naver_news(query, display=15)
    if res is None:
        time.sleep(2)
        continue

    items = res.get('items', []) or []
    # 30일 cutoff 필터
    recent_items = []
    for it in items:
        try:
            pd = datetime.strptime(it.get('pubDate','')[:25],
                                   '%a, %d %b %Y %H:%M:%S')
            if pd >= CUTOFF:
                recent_items.append({
                    'title': it.get('title','').replace('<b>','').replace('</b>','')[:120],
                    'link': it.get('link','')[:200],
                    'date': pd.strftime('%Y-%m-%d')
                })
        except: pass

    cnt = len(recent_items)
    db.execute("""UPDATE story_leads SET
                    actual_news_count=?, news_match_urls=?, news_verified_at=?
                  WHERE id=?""",
               [cnt, json.dumps(recent_items[:5], ensure_ascii=False), now, r['id']])

    if cnt == 0: total_zero += 1
    else: total_match += 1

    if i % 30 == 0:
        db.commit()
        print(f"  진행 {i}/{len(leads)}  zero={total_zero}  match={total_match}")
    time.sleep(0.3)  # Naver throttle

db.commit()
print(f'\n[완료] zero={total_zero}  match={total_match}  total={len(leads)}')

# gap_label 자동 정밀화
db.execute("""UPDATE story_leads SET info_gap_label='HIGH_CONFIRMED'
              WHERE info_gap_label='HIGH' AND actual_news_count='0'""")
db.execute("""UPDATE story_leads SET info_gap_label='HIGH_REPORTED'
              WHERE info_gap_label='HIGH' AND CAST(actual_news_count AS INTEGER) >= 3""")
db.execute("""UPDATE story_leads SET info_gap_label='HIGH_PARTIAL'
              WHERE info_gap_label='HIGH' AND CAST(actual_news_count AS INTEGER) BETWEEN 1 AND 2""")
db.commit()

print()
for r in db.execute("SELECT info_gap_label, COUNT(*) FROM story_leads WHERE info_gap_label LIKE 'HIGH%' GROUP BY info_gap_label"):
    print(f'  {r[0]:<20} {r[1]}')
