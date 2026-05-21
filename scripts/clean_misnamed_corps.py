"""
scripts/clean_misnamed_corps.py
────────────────────────────────
잘못 매칭된 ir_contacts 회사명 일괄 정리.

전략:
  1) corp_name이 사람 이름 패턴인 contact 검출
     - 한국어 성씨 + 2~4자 (예: "김성민", "Robin Kim")
     - 영문 First Last 패턴 (예: "Jiyu Baek")
     - "이름(부서)" 패턴 (예: "Jiyu Baek (IDC)")
  2) 자동 수정 시도:
     a) 이메일 도메인 → companies 테이블 매칭
     b) PR 대행사 도메인이면 본문 재파싱 (Gmail)
  3) 매칭 성공 → corp_code/corp_name 갱신, confidence 격상
     매칭 실패 → corp_name='[검토필요]' + notes에 원본 회사명 기록

실행:
  python scripts/clean_misnamed_corps.py --dry-run    # 시뮬레이션
  python scripts/clean_misnamed_corps.py              # 실제 실행
  python scripts/clean_misnamed_corps.py --stats      # 통계만
"""
import io, os, re, sys, json, sqlite3, argparse
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

KOREAN_SURNAMES = set('김이박최정강조윤장임한오서신권황안송류전홍고문양손배백허유남심노하곽성차주우구민나진지엄채원천방공현함변염여추도소석선설마길연위표명기반왕금옥육인맹제모탁국어은편복단연봉경사부')

# PR/IR 대행사 도메인 (이런 도메인 contact은 회사 매칭 못 한 경우 많음)
PR_AGENCY_DOMAINS = {
    'irkudos.co.kr', 'irmed.co.kr', 'irup.co.kr', 'their.co.kr',
    'upcom.co.kr', 'kggroup.co.kr', 'irbiznet.com', 'kpr.co.kr',
    'kookjepr.com', 'ifgpartners.com', 'seoulir.co.kr', 'edelman.com',
    'fleishmanhillard.com', 'prain.co.kr', 'fnsbiz.co.kr',
    'communicaco.kr', 'wiscom.kr', 'media2.co.kr', 'pr-one.co.kr',
    'kosdaqca.or.kr', 'klca.or.kr',
}

# 일반 메일 도메인 (회사 매칭 불가)
GENERIC_DOMAINS = {
    'gmail.com', 'naver.com', 'daum.net', 'hanmail.net', 'kakao.com',
    'naver.net', 'yahoo.com', 'outlook.com', 'hotmail.com',
}


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


# 회사명 끝 접미사 — 이게 있으면 회사명 거의 확실 (사람 이름 아님)
CORP_SUFFIXES = ['산업','화학','제약','은행','식품','통신','전자','제강','건설',
                 '그룹','홀딩스','코퍼레이션','금융','증권','보험','카드','텔레콤',
                 '엔터테인먼트','시스템','테크','솔루션','네트웍스','바이오','과학',
                 '디스플레이','전기','중공업','조선','제지','양행','제과','컴퍼니',
                 'Corp','Inc','Ltd','Co','Group','Holdings','Tech',
                 '(주)','㈜','주식회사']


def looks_like_corp(name):
    """회사명 같은가 — 접미사 / 한자어 패턴"""
    if not name: return False
    n = name.strip()
    for suf in CORP_SUFFIXES:
        if n.endswith(suf) or suf in n:
            return True
    # 4자 이상 + 한자어 어휘
    return False


def is_english_personal_name(name):
    """영문 First Last 패턴 (Robin Kim, Jiyu Baek 등)"""
    if not name: return False
    n = name.strip()
    # "First Last" 또는 "First Last (xxx)" 또는 "First Last [xxx]"
    if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+(\s*[\(\[][^)\]]+[\)\]])?$', n):
        return True
    # "First Last More" 3단어 영문
    if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+[A-Z][a-z]+$', n):
        return True
    return False


def is_misnamed_corp(corp_name):
    """corp_name이 사람 이름 패턴인지 — 영문만 자동 검출 (안전)"""
    if not corp_name: return False
    cn = corp_name.strip()
    # 회사명 접미사 있으면 사람 이름 아님
    if looks_like_corp(cn):
        return False
    # 영문 사람 이름
    if is_english_personal_name(cn):
        return True
    return False


def normalize_corp_name(name):
    """회사명 정규화"""
    n = (name or '').strip()
    n = re.sub(r'\(주\)|주식회사|㈜|（주）', '', n)
    n = n.replace(' ', '').replace('\t', '')
    return n.strip()


def build_domain_index(db):
    """{domain_root: (corp_code, corp_name)}"""
    from urllib.parse import urlparse
    rows = db.execute("SELECT corp_code, corp_name, stock_code FROM companies").fetchall()
    by_domain = {}
    by_name = {}
    for r in rows:
        cn = (r['corp_name'] or '').strip()
        if not cn: continue
        norm = normalize_corp_name(cn)
        by_name[norm] = (r['corp_code'], cn)
    return by_domain, by_name


def build_email_to_corp_map(db):
    """기존 ir_contacts 중 corp_code가 명확한 것의 도메인 → corp 매핑.
    같은 도메인의 다른 contact은 같은 회사일 가능성 높음."""
    domain_corp = {}  # {domain: {corp_code: count}}
    for r in db.execute("""
        SELECT corp_code, corp_name, ir_email FROM ir_contacts
        WHERE corp_code != 'UNKNOWN'
          AND corp_code IS NOT NULL
          AND ir_email IS NOT NULL AND ir_email != ''
          AND substr(ir_email,1,1) != '_'
    """):
        em = (r['ir_email'] or '').lower()
        if '@' not in em: continue
        domain = em.split('@')[-1]
        if domain in PR_AGENCY_DOMAINS or domain in GENERIC_DOMAINS:
            continue
        domain_corp.setdefault(domain, {}).setdefault(r['corp_code'], {'count': 0, 'name': r['corp_name']})
        domain_corp[domain][r['corp_code']]['count'] += 1
    # 도메인 → 가장 많은 corp_code
    result = {}
    for d, corps in domain_corp.items():
        best = max(corps.items(), key=lambda x: x[1]['count'])
        result[d] = (best[0], best[1]['name'])
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='시뮬레이션만')
    ap.add_argument('--stats', action='store_true', help='통계만')
    ap.add_argument('--limit', type=int, default=0, help='처리 최대 (0=전부)')
    args = ap.parse_args()

    db = get_db()

    # 통계
    if args.stats:
        n_total = db.execute("SELECT COUNT(*) FROM ir_contacts").fetchone()[0]
        n_unknown = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE corp_code='UNKNOWN'").fetchone()[0]
        print(f'전체: {n_total} / UNKNOWN: {n_unknown}')
        misnamed = 0
        for r in db.execute("SELECT corp_name FROM ir_contacts"):
            if is_misnamed_corp(r['corp_name'] or ''):
                misnamed += 1
        print(f'사람 이름이 회사명에: {misnamed}건')
        return

    print('━' * 70)
    print('  잘못 매칭된 ir_contacts 정리 시작')
    print('━' * 70)

    _, by_name = build_domain_index(db)
    domain_to_corp = build_email_to_corp_map(db)
    print(f'도메인 → 회사 매핑 인덱스: {len(domain_to_corp)}개\n')

    # 1단계: 사람 이름 패턴 검출
    candidates = []
    for r in db.execute("""
        SELECT id, corp_code, corp_name, ir_email, ir_phone, ir_mobile,
               ir_name, source
        FROM ir_contacts
    """):
        if is_misnamed_corp(r['corp_name'] or ''):
            candidates.append(dict(r))

    print(f'사람 이름 패턴 발견: {len(candidates)}건')
    if args.limit > 0:
        candidates = candidates[:args.limit]

    stats = {
        'matched_by_domain': 0, 'cleared': 0, 'kept': 0, 'errors': 0,
        'dry_run': args.dry_run,
    }
    samples = {'matched': [], 'cleared': []}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for c in candidates:
        try:
            old_corp_code = c['corp_code']
            old_corp_name = c['corp_name']
            email = (c['ir_email'] or '').lower()
            domain = email.split('@')[-1] if '@' in email else ''

            # 자동 수정 시도
            new_corp_code = None
            new_corp_name = None

            # ① 도메인 → 알려진 회사 매핑
            if domain and domain not in PR_AGENCY_DOMAINS and domain not in GENERIC_DOMAINS:
                if domain in domain_to_corp:
                    new_corp_code, new_corp_name = domain_to_corp[domain]
                    stats['matched_by_domain'] += 1
                    if len(samples['matched']) < 8:
                        samples['matched'].append(f'  {old_corp_name} → {new_corp_name} (도메인: {domain})')

            # ② 매칭 못 한 경우 — corp_name을 사람이름→ir_name 으로 이동, corp_name 비움
            if not new_corp_code:
                # 만약 ir_name이 비어있다면 corp_name을 ir_name으로 이전
                if not c['ir_name'] and c['corp_name']:
                    new_ir_name = c['corp_name']
                else:
                    new_ir_name = c['ir_name']
                # corp_name = '[검토필요]' 표시
                new_corp_code = 'UNKNOWN'
                new_corp_name = '[검토필요] ' + (c['corp_name'] or '')
                stats['cleared'] += 1
                if len(samples['cleared']) < 8:
                    samples['cleared'].append(f"  '{old_corp_name}' (도메인: {domain or '없음'}) → 검토필요로 표시 + ir_name='{new_ir_name}'")

                if not args.dry_run:
                    db.execute("""
                        UPDATE ir_contacts SET corp_name=?, ir_name=COALESCE(NULLIF(ir_name,''),?), updated_at=?
                        WHERE id=?
                    """, [new_corp_name, new_ir_name, now, c['id']])
            else:
                # 도메인 매칭 성공
                if not args.dry_run:
                    db.execute("""
                        UPDATE ir_contacts SET corp_code=?, corp_name=?, confidence='A_email_only', updated_at=?
                        WHERE id=?
                    """, [new_corp_code, new_corp_name, now, c['id']])
        except Exception as e:
            stats['errors'] += 1

    if not args.dry_run:
        db.commit()

    print()
    print('━' * 70)
    print('  정리 결과' + (' [DRY-RUN]' if args.dry_run else ''))
    print('━' * 70)
    print(f"  검출된 잘못된 매칭   : {len(candidates)}건")
    print(f"  ✅ 도메인으로 매칭됨 : {stats['matched_by_domain']}건")
    print(f"  ⚠ [검토필요] 표시    : {stats['cleared']}건")
    print(f"  ❌ 에러              : {stats['errors']}건")
    if samples['matched']:
        print(f'\n[도메인 매칭 샘플]')
        for s in samples['matched']:
            print(s)
    if samples['cleared']:
        print(f'\n[검토필요 표시 샘플]')
        for s in samples['cleared']:
            print(s)


if __name__ == '__main__':
    main()
