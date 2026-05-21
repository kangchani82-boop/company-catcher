"""
scripts/match_corps_by_domain.py
─────────────────────────────────
이메일 도메인 → 회사명 자동 매칭.

전략:
  1) 도메인별 contact 그룹화 + 분류 (A/B/C/D)
  2) D(회사 도메인)에 대해 매칭 시도:
     a) 같은 도메인의 다른 contact이 이미 매칭됨 → 동일 적용
     b) companies.hm_url 도메인 매칭
     c) 회사명 영문 키워드 ↔ 도메인 root 매칭
  3) 안전장치: PR 대행사·일반 메일·user_verified=1 은 건드리지 않음

실행:
  python scripts/match_corps_by_domain.py --dry-run    # 시뮬레이션
  python scripts/match_corps_by_domain.py              # 실제 적용
  python scripts/match_corps_by_domain.py --stats      # 분석만
"""
import io, os, re, sys, sqlite3, argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


# PR/IR 대행사 + 협회 도메인 (이건 본문 의존이라 자동 변경 X)
PR_AGENCY_DOMAINS = {
    'irkudos.co.kr', 'irmed.co.kr', 'irup.co.kr', 'their.co.kr',
    'upcom.co.kr', 'kggroup.co.kr', 'irbiznet.com', 'kpr.co.kr',
    'kookjepr.com', 'ifgpartners.com', 'seoulir.co.kr', 'edelman.com',
    'fleishmanhillard.com', 'prain.co.kr', 'fnsbiz.co.kr',
    'communicaco.kr', 'wiscom.kr', 'media2.co.kr', 'pr-one.co.kr',
    'sunnypr.co.kr', 'and-rb.com',
    'kosdaqca.or.kr', 'klca.or.kr', 'kirs.or.kr',
}

# 일반 메일 도메인 (회사 매칭 불가)
GENERIC_DOMAINS = {
    'gmail.com', 'naver.com', 'daum.net', 'hanmail.net', 'kakao.com',
    'naver.net', 'yahoo.com', 'outlook.com', 'hotmail.com',
    'finance-scope.com', 'me.com', 'icloud.com',
}


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def domain_root(domain):
    """example.com → example, www.samsung.com → samsung"""
    d = (domain or '').lower().replace('www.', '').strip()
    parts = d.split('.')
    if len(parts) >= 2:
        return parts[0]
    return d


def normalize_corp_name(name):
    n = (name or '').strip()
    n = re.sub(r'\(주\)|주식회사|㈜|（주）', '', n)
    n = n.replace(' ', '').replace('\t', '')
    return n.strip()


def domain_categorize(domain):
    """도메인 분류 — A/B/C/D"""
    if not domain or '@' in domain:
        return 'X'  # 비정상
    d = domain.lower()
    if d in GENERIC_DOMAINS:
        return 'A_generic'
    if d in PR_AGENCY_DOMAINS:
        return 'B_agency'
    if d.endswith('.or.kr') or d.endswith('.go.kr'):
        return 'C_org'  # 협회/기관
    return 'D_company'


def build_company_index(db):
    """companies 테이블 인덱스
    Returns:
      by_domain: {domain_root: (corp_code, corp_name)} (hm_url 기반)
      by_name_root: {normalized_name: (corp_code, corp_name)}
    """
    by_domain = {}
    by_name_root = {}
    rows = db.execute("SELECT corp_code, corp_name, stock_code FROM companies").fetchall()

    # 또한 ir_contacts의 homepage 활용
    for r in db.execute("""
        SELECT DISTINCT corp_code, corp_name, homepage FROM ir_contacts
        WHERE homepage IS NOT NULL AND homepage != ''
    """).fetchall():
        try:
            d = urlparse(r['homepage'] if r['homepage'].startswith('http') else 'http://' + r['homepage']).netloc
            root = domain_root(d)
            if root and len(root) >= 2 and root not in {'naver','daum','gmail','kakao','google'}:
                # 이미 있으면 첫 번째 유지
                if root not in by_domain:
                    by_domain[root] = (r['corp_code'], r['corp_name'])
        except: pass

    for r in rows:
        cn = (r['corp_name'] or '').strip()
        if not cn: continue
        norm = normalize_corp_name(cn)
        if norm:
            by_name_root[norm] = (r['corp_code'], cn)

    return by_domain, by_name_root


def build_existing_domain_map(db):
    """기존 ir_contacts에서 도메인별 가장 많이 매칭된 corp_code"""
    domain_corp = defaultdict(lambda: defaultdict(int))
    for r in db.execute("""
        SELECT corp_code, corp_name, ir_email FROM ir_contacts
        WHERE corp_code != 'UNKNOWN' AND corp_code IS NOT NULL
          AND ir_email IS NOT NULL AND ir_email != ''
          AND substr(ir_email,1,1) != '_'
    """).fetchall():
        em = (r['ir_email'] or '').lower()
        if '@' not in em: continue
        d = em.split('@')[-1]
        cat = domain_categorize(d)
        if cat != 'D_company': continue  # 회사 도메인만
        domain_corp[d][(r['corp_code'], r['corp_name'])] += 1

    # 가장 많이 매칭된 corp 1개씩
    result = {}
    for d, corps in domain_corp.items():
        best = max(corps.items(), key=lambda x: x[1])
        result[d] = (best[0][0], best[0][1], best[1])  # (corp_code, corp_name, count)
    return result


def fuzzy_match_by_name(domain, by_name_root):
    """도메인 root와 회사명 매칭 시도"""
    root = domain_root(domain)
    if len(root) < 3: return None
    # 정확 매칭
    if root in by_name_root:
        return by_name_root[root]
    # 부분 매칭 — 회사명 안에 root 단어 포함
    for norm, (cc, cn) in by_name_root.items():
        if root in norm.lower():
            if len(root) >= 4:  # 짧은 단어는 거짓 매칭 위험
                return (cc, cn)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='시뮬레이션만')
    ap.add_argument('--stats', action='store_true', help='분석만')
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()

    db = get_db()
    print('━' * 70)
    print(f'  이메일 도메인 → 회사명 매칭 {"[DRY-RUN]" if args.dry_run else ""}')
    print('━' * 70)

    # 인덱스 구축
    print('인덱스 구축 중...')
    by_domain, by_name_root = build_company_index(db)
    existing_map = build_existing_domain_map(db)
    print(f'  hm_url 도메인 매핑: {len(by_domain)}개')
    print(f'  회사명 정규화: {len(by_name_root)}개')
    print(f'  기존 도메인 매핑 (다른 contact 통해): {len(existing_map)}개\n')

    # 모든 ir_contacts 도메인 그룹화
    domain_contacts = defaultdict(list)
    for r in db.execute("""
        SELECT id, corp_code, corp_name, ir_email, ir_name, source, user_verified
        FROM ir_contacts
        WHERE ir_email IS NOT NULL AND ir_email != ''
          AND substr(ir_email,1,1) != '_'
    """).fetchall():
        em = (r['ir_email'] or '').lower()
        if '@' not in em: continue
        d = em.split('@')[-1]
        domain_contacts[d].append(dict(r))

    print(f'고유 도메인 수: {len(domain_contacts)}\n')

    # 카테고리별 분류
    cat_counts = defaultdict(lambda: {'domains': 0, 'contacts': 0})
    for d, contacts in domain_contacts.items():
        cat = domain_categorize(d)
        cat_counts[cat]['domains'] += 1
        cat_counts[cat]['contacts'] += len(contacts)

    print('카테고리별 분포:')
    for cat in ['A_generic', 'B_agency', 'C_org', 'D_company', 'X']:
        c = cat_counts[cat]
        print(f'  {cat:14s}: 도메인 {c["domains"]:4d} / contact {c["contacts"]:5d}')

    if args.stats:
        return

    # 매칭 시도
    print('\n' + '━' * 70)
    print('  매칭 시도 (D_company만)')
    print('━' * 70)

    stats = {
        'matched_existing': 0,  # 기존 매핑 활용
        'matched_hmurl': 0,     # companies.hm_url
        'matched_fuzzy': 0,     # 회사명 fuzzy
        'no_match': 0,
        'skipped_verified': 0,
        'skipped_already_ok': 0,
        'updated': 0,
    }
    samples = {'matched_existing': [], 'matched_hmurl': [], 'matched_fuzzy': [], 'no_match': []}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    processed_count = 0
    for d, contacts in sorted(domain_contacts.items()):
        if domain_categorize(d) != 'D_company':
            continue

        # 매칭 시도 — 도메인 단위로
        match = None
        match_method = None

        # 1. 기존 매핑 (같은 도메인 contact)
        if d in existing_map:
            cc, cn, count = existing_map[d]
            if count >= 1:  # 최소 1개라도 매칭됐으면 신뢰
                match = (cc, cn)
                match_method = 'matched_existing'

        # 2. companies.hm_url 도메인 매칭
        if not match:
            root = domain_root(d)
            if root in by_domain:
                match = by_domain[root]
                match_method = 'matched_hmurl'

        # 3. 회사명 fuzzy 매칭
        if not match:
            r = fuzzy_match_by_name(d, by_name_root)
            if r:
                match = r
                match_method = 'matched_fuzzy'

        # 매칭된 contact들에 적용
        for c in contacts:
            if args.limit > 0 and processed_count >= args.limit:
                break
            processed_count += 1

            if c['user_verified']:
                stats['skipped_verified'] += 1
                continue

            if match:
                cc_new, cn_new = match
                # 이미 같은 corp_code면 스킵
                if c['corp_code'] == cc_new:
                    stats['skipped_already_ok'] += 1
                    continue
                # corp_name이 [검토필요]거나 다른 corp_code면 업데이트
                stats[match_method] += 1
                if len(samples[match_method]) < 5:
                    samples[match_method].append(
                        f"  @{d:30s} {c['corp_name'] or '(빈)':<25s} → {cn_new}"
                    )
                if not args.dry_run:
                    try:
                        db.execute("""
                            UPDATE ir_contacts SET corp_code=?, corp_name=?,
                                confidence=CASE
                                    WHEN confidence IN ('guessed','C_guessed','none') THEN 'A_email_only'
                                    WHEN ?='matched_existing' THEN 'A_verified'
                                    ELSE COALESCE(confidence, 'B_extracted')
                                END,
                                updated_at=?
                            WHERE id=?
                        """, [cc_new, cn_new, match_method, now, c['id']])
                        stats['updated'] += 1
                    except sqlite3.IntegrityError:
                        # 같은 (corp_code, ir_email)이 이미 있음 → 현재 contact은 중복이므로 삭제
                        try:
                            db.execute("DELETE FROM ir_contacts WHERE id=?", [c['id']])
                            stats.setdefault('dedup_deleted', 0)
                            stats['dedup_deleted'] += 1
                        except: pass
            else:
                stats['no_match'] += 1
                if len(samples['no_match']) < 5:
                    samples['no_match'].append(f"  @{d:30s} {c['corp_name'] or '(빈)'}")

    if not args.dry_run:
        db.commit()

    # 결과
    print(f'  ✅ 기존 도메인 매핑 적용 : {stats["matched_existing"]:>5d}건')
    print(f'  ✅ companies.hm_url 매칭 : {stats["matched_hmurl"]:>5d}건')
    print(f'  ✅ 회사명 fuzzy 매칭     : {stats["matched_fuzzy"]:>5d}건')
    print(f'  ⏭ 이미 동일 매칭         : {stats["skipped_already_ok"]:>5d}건')
    print(f'  ⏭ user_verified 보호     : {stats["skipped_verified"]:>5d}건')
    print(f'  ❌ 매칭 실패              : {stats["no_match"]:>5d}건')
    if not args.dry_run:
        print(f'\n  💾 DB 업데이트            : {stats["updated"]:>5d}건')
        print(f'  🗑 중복 삭제              : {stats.get("dedup_deleted",0):>5d}건')

    if samples['matched_existing']:
        print('\n[기존 매핑 샘플]')
        for s in samples['matched_existing']: print(s)
    if samples['matched_hmurl']:
        print('\n[hm_url 매칭 샘플]')
        for s in samples['matched_hmurl']: print(s)
    if samples['matched_fuzzy']:
        print('\n[fuzzy 매칭 샘플]')
        for s in samples['matched_fuzzy']: print(s)
    if samples['no_match']:
        print('\n[매칭 실패 샘플]')
        for s in samples['no_match']: print(s)


if __name__ == '__main__':
    main()
