"""
scripts/fetch_ksic_codes.py
────────────────────────────
DART company.json API에서 4,091개 회사의 induty_code (한국표준산업분류 KSIC) 일괄 수집.

추가 컬럼:
  - induty_code     : KSIC 5자리 코드 (예: 26111)
  - hm_url          : 회사 홈페이지 URL
  - phn_no, fax_no  : 회사 대표 전화/팩스
  - adres           : 회사 주소
  - est_dt          : 설립일
  - acc_mt          : 결산월

실행:
  python scripts/fetch_ksic_codes.py            # 전체
  python scripts/fetch_ksic_codes.py --limit 50  # 테스트
  python scripts/fetch_ksic_codes.py --resume    # 미수집 회사만
"""
import os, time, requests, sys, io, sqlite3, argparse
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# 환경변수 로드
for line in open(ENV_PATH, encoding='utf-8'):
    if '=' in line and not line.startswith('#'):
        k, v = line.strip().split('=', 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

DART_KEY = os.environ.get('DART_API_KEY')


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def ensure_columns(db):
    """companies 테이블에 필요한 컬럼 추가"""
    add_cols = [
        ('induty_code', 'TEXT'),
        ('hm_url', 'TEXT'),
        ('phn_no', 'TEXT'),
        ('fax_no', 'TEXT'),
        ('adres', 'TEXT'),
        ('est_dt', 'TEXT'),
        ('acc_mt', 'TEXT'),
        ('ceo_nm', 'TEXT'),
        ('jurir_no', 'TEXT'),
        ('bizr_no', 'TEXT'),
        ('dart_fetched_at', 'TEXT'),
    ]
    cols = [r[1] for r in db.execute('PRAGMA table_info(companies)').fetchall()]
    for name, typ in add_cols:
        if name not in cols:
            try:
                db.execute(f'ALTER TABLE companies ADD COLUMN {name} {typ}')
                print(f'  + 컬럼 추가: {name}')
            except: pass
    db.commit()


def fetch_one(corp_code, key):
    """단일 회사 정보 가져오기"""
    url = 'https://opendart.fss.or.kr/api/company.json'
    try:
        r = requests.get(url, params={'crtfc_key': key, 'corp_code': corp_code}, timeout=10)
        if r.status_code != 200:
            return None
        d = r.json()
        if d.get('status') != '000':
            return None
        return d
    except:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--resume', action='store_true', help='미수집 회사만')
    args = ap.parse_args()

    db = get_db()
    ensure_columns(db)

    # 대상 회사
    where = "WHERE corp_code IS NOT NULL"
    if args.resume:
        where += " AND (induty_code IS NULL OR induty_code = '')"
    rows = db.execute(f"SELECT corp_code, corp_name FROM companies {where}").fetchall()
    if args.limit > 0:
        rows = rows[:args.limit]

    print('━' * 60)
    print(f'  DART KSIC 코드 수집 — {len(rows):,}개 회사')
    print('━' * 60)

    stats = {'ok': 0, 'no_data': 0, 'err': 0}
    start = time.time()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for i, r in enumerate(rows, 1):
        d = fetch_one(r['corp_code'], DART_KEY)
        if d:
            try:
                db.execute("""
                    UPDATE companies SET
                        induty_code = ?,
                        hm_url = ?,
                        phn_no = ?,
                        fax_no = ?,
                        adres = ?,
                        est_dt = ?,
                        acc_mt = ?,
                        ceo_nm = ?,
                        jurir_no = ?,
                        bizr_no = ?,
                        dart_fetched_at = ?
                    WHERE corp_code = ?
                """, [
                    d.get('induty_code',''), d.get('hm_url',''),
                    d.get('phn_no',''), d.get('fax_no',''),
                    d.get('adres',''), d.get('est_dt',''),
                    d.get('acc_mt',''), d.get('ceo_nm',''),
                    d.get('jurir_no',''), d.get('bizr_no',''),
                    now, r['corp_code']
                ])
                stats['ok'] += 1
            except Exception as e:
                stats['err'] += 1
        else:
            stats['no_data'] += 1

        if i % 100 == 0:
            db.commit()
            elapsed = time.time() - start
            rate = i / elapsed
            remain = (len(rows) - i) / rate if rate > 0 else 0
            print(f'  [{i:5d}/{len(rows):5d}] OK {stats["ok"]:5d} / 빈 {stats["no_data"]:4d} / 에러 {stats["err"]:4d}  '
                  f'{rate:.1f}건/초, 남은 시간 ~{remain/60:.0f}분')

        # 분당 ~150회 페이스 (안전)
        time.sleep(0.4)

    db.commit()
    elapsed = time.time() - start

    print('\n' + '━' * 60)
    print(f'  완료')
    print('━' * 60)
    print(f'  처리 시간   : {elapsed/60:.1f}분')
    print(f'  ✅ 성공     : {stats["ok"]:,}건')
    print(f'  ⚠ 데이터 없음 : {stats["no_data"]:,}건')
    print(f'  ❌ 에러     : {stats["err"]:,}건')

    # KSIC 분포
    print('\n[KSIC 코드 분포 — 상위 15개]')
    for r in db.execute("""
        SELECT induty_code, COUNT(*) c FROM companies
        WHERE induty_code IS NOT NULL AND induty_code != ''
        GROUP BY induty_code ORDER BY c DESC LIMIT 15
    """):
        print(f'  {r[0]:6s} : {r[1]:>4}개사')


if __name__ == '__main__':
    main()
