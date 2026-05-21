"""
scripts/collect_irgo_irbooks.py
────────────────────────────────
IRGO IR자료에서 회사별 IR북 수집.

전략:
  1) ID 순회 (73710부터 거꾸로) — 5초 간격 (차단 방지)
  2) 각 페이지 title에서 회사명 추출 → companies 테이블 매칭
  3) 매칭된 회사의 PDF 다운로드 (회사당 가장 최근 1개)
  4) 다운로드 파일을 data/ir_books/IRGO/ 에 저장

실행:
  python scripts/collect_irgo_irbooks.py --start-id 73710 --count 100
  python scripts/collect_irgo_irbooks.py --start-id 73710 --count 1000
"""
import io, os, re, sys, time, json, sqlite3, random, argparse
import requests
from pathlib import Path
from datetime import datetime
from urllib.parse import unquote

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
BOOKS_DIR = ROOT / "data" / "ir_books" / "IRGO"
BOOKS_DIR.mkdir(parents=True, exist_ok=True)
META_FILE = BOOKS_DIR / "_metadata.json"

USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 12; SM-S908N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def normalize_corp_name(name):
    n = name.strip()
    n = re.sub(r'\(주\)|주식회사|㈜|（주）', '', n)
    n = n.replace(' ', '').replace('\t', '')
    return n.strip()


def build_corp_index(db):
    """{normalized_name: (corp_code, corp_name, stock_code)}"""
    index = {}
    partial = {}
    for r in db.execute("SELECT corp_code, corp_name, stock_code FROM companies"):
        cn = (r['corp_name'] or '').strip()
        if not cn:
            continue
        norm = normalize_corp_name(cn)
        if norm and norm not in index:
            index[norm] = (r['corp_code'], cn, r['stock_code'])
        # 부분 매칭용
        for w in re.split(r'[\s\(\)\[\]/_\-,\.]+', cn):
            wn = normalize_corp_name(w)
            if len(wn) >= 2:
                partial.setdefault(wn, []).append((r['corp_code'], cn, r['stock_code']))
    return index, partial


def match_corp(corp_hint, by_name, by_partial):
    """회사명 hint로 매칭"""
    if not corp_hint:
        return None
    norm = normalize_corp_name(corp_hint)
    if norm in by_name:
        return by_name[norm]
    # 부분 매칭
    if len(norm) >= 3 and norm in by_partial:
        cands = by_partial[norm]
        if len(cands) == 1:
            return cands[0]
        # 같은 이름 2개면 첫번째 (대부분 동일 회사)
        return cands[0]
    # split해서 단어 매칭
    for w in re.split(r'[\s\(\)\[\]/_\-,\.]+', corp_hint):
        wn = normalize_corp_name(w)
        if len(wn) >= 3 and wn in by_partial:
            cands = by_partial[wn]
            if len(cands) == 1:
                return cands[0]
    return None


def safe_filename(s, maxlen=80):
    """파일명 안전화"""
    s = re.sub(r'[\\/:*?"<>|]', '_', s)
    s = re.sub(r'\s+', '_', s)
    return s[:maxlen]


def load_meta():
    if META_FILE.exists():
        return json.loads(META_FILE.read_text(encoding='utf-8'))
    return {'processed_ids': [], 'downloaded': {}, 'matched_companies': {}}


def save_meta(meta):
    META_FILE.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')


def fetch_irgo_page(irgo_id, session):
    """IRGO IR자료 ID 페이지 가져오기"""
    url = f'https://m.irgo.co.kr/IR%EC%9E%90%EB%A3%8C/{irgo_id}/TB/x'
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'ko-KR,ko;q=0.9',
        'Referer': 'https://m.irgo.co.kr/IR%EC%9E%90%EB%A3%8C',
    }
    r = session.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        return None, None, None
    # title 파싱
    m = re.search(r'<title>([^<]+)</title>', r.text)
    title = m.group(1).strip() if m else ''
    # PDF 링크
    pdf_match = re.search(r'href="(https?://file\.irgo\.co\.kr/data/[^"]+)"', r.text)
    pdf_url = pdf_match.group(1) if pdf_match else None
    return title, pdf_url, r.text


def extract_corp_name(title):
    """IRGO title에서 회사명 추출
    예: "두산 두산, 2026년 1분기 경영실적 발표 - IRGO"
    예: "에이프릴바이오 [Growth Research] ... - IRGO"
    """
    if not title:
        return ''
    # "- IRGO" 제거
    t = re.sub(r'\s*-\s*IRGO\s*$', '', title).strip()
    # 회사명은 보통 첫 단어 또는 첫 단어+두번째 단어 (반복되는 경우)
    # 예: "두산 두산, 2026년..." → 첫 토큰 "두산"
    parts = t.split(' ')
    if len(parts) >= 2:
        # 첫 두 단어 반복 패턴 (회사명 회사명, 제목)
        if parts[0] == parts[1].rstrip(','):
            return parts[0]
        # 첫 단어, 둘째 단어가 쉼표로 끝나는 경우
        if parts[1].endswith(','):
            return parts[0]
        return parts[0]
    return parts[0] if parts else ''


def download_pdf(pdf_url, save_path, session):
    """PDF 다운로드"""
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'ko-KR,ko;q=0.9',
        'Referer': 'https://m.irgo.co.kr/',
    }
    r = session.get(pdf_url, headers=headers, timeout=30, stream=True)
    if r.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start-id', type=int, default=73710)
    ap.add_argument('--count', type=int, default=100, help='시도할 ID 개수')
    ap.add_argument('--delay', type=float, default=5.0, help='요청 간 딜레이 (초)')
    ap.add_argument('--max-per-corp', type=int, default=1, help='회사당 최대 PDF')
    ap.add_argument('--dry-run', action='store_true', help='다운로드 안 함')
    args = ap.parse_args()

    db = get_db()
    by_name, by_partial = build_corp_index(db)
    print(f'회사 인덱스: 정규 {len(by_name)} / 부분 {len(by_partial)}')

    # story_leads 우선순위
    priority_corps = set(r[0] for r in db.execute(
        "SELECT DISTINCT corp_code FROM story_leads"
    ).fetchall())
    print(f'우선순위 회사 (story_leads): {len(priority_corps)}개')

    meta = load_meta()
    processed_ids = set(meta['processed_ids'])
    downloaded = meta['downloaded']  # {corp_code: [{irgo_id, pdf_path, title, ...}]}
    matched_companies = meta['matched_companies']

    session = requests.Session()
    stats = {
        'fetched': 0, 'matched': 0, 'priority_matched': 0,
        'downloaded': 0, 'skipped_already': 0, 'errors': 0,
        'no_pdf': 0, 'no_match': 0,
    }

    print(f'\n시작: ID {args.start_id} 부터 {args.count}개 거꾸로 순회')
    print(f'딜레이: {args.delay}초, 회사당 최대 {args.max_per_corp}개')
    print('-' * 70)

    for offset in range(args.count):
        irgo_id = args.start_id - offset
        if irgo_id in processed_ids:
            continue

        try:
            title, pdf_url, _ = fetch_irgo_page(irgo_id, session)
            stats['fetched'] += 1

            if not pdf_url:
                stats['no_pdf'] += 1
                processed_ids.add(irgo_id)
                continue

            corp_hint = extract_corp_name(title)
            match = match_corp(corp_hint, by_name, by_partial)

            if not match:
                stats['no_match'] += 1
                processed_ids.add(irgo_id)
                if stats['fetched'] % 50 == 0:
                    print(f'  [{stats["fetched"]:4d}] 처리 중... 매칭 {stats["matched"]}, 다운로드 {stats["downloaded"]}')
                continue

            corp_code, corp_name, stock_code = match
            stats['matched'] += 1

            is_priority = corp_code in priority_corps
            if is_priority:
                stats['priority_matched'] += 1

            # 회사당 max-per-corp 체크
            existing = downloaded.get(corp_code, [])
            if len(existing) >= args.max_per_corp:
                stats['skipped_already'] += 1
                processed_ids.add(irgo_id)
                continue

            # PDF 다운로드
            ext = '.pdf'
            filename = f'{corp_code}_{safe_filename(corp_name)}_{irgo_id}{ext}'
            save_path = BOOKS_DIR / filename

            if args.dry_run:
                print(f'  [{stats["fetched"]:4d}] DRY [매칭{"★" if is_priority else ""}] '
                      f'{corp_name} | {title[:60]}')
            else:
                ok = download_pdf(pdf_url, save_path, session)
                if ok:
                    stats['downloaded'] += 1
                    downloaded.setdefault(corp_code, []).append({
                        'irgo_id': irgo_id,
                        'pdf_path': str(save_path.relative_to(ROOT)),
                        'title': title,
                        'pdf_url': pdf_url,
                        'downloaded_at': datetime.now().isoformat(),
                    })
                    matched_companies[corp_code] = corp_name
                    print(f'  [{stats["fetched"]:4d}] ✓ {"★" if is_priority else " "} '
                          f'{corp_name[:20]:<20} | {filename[:60]}')
                else:
                    stats['errors'] += 1

            processed_ids.add(irgo_id)

            # 메타 주기적 저장
            if stats['fetched'] % 25 == 0:
                meta['processed_ids'] = list(processed_ids)
                meta['downloaded'] = downloaded
                meta['matched_companies'] = matched_companies
                save_meta(meta)

        except Exception as e:
            stats['errors'] += 1
            if stats['errors'] <= 3:
                print(f'  ERR ID {irgo_id}: {str(e)[:80]}')

        # 차단 방지 — 5초 ± 1초 랜덤
        time.sleep(args.delay + random.uniform(-1, 1))

    # 최종 메타 저장
    meta['processed_ids'] = sorted(processed_ids, reverse=True)
    meta['downloaded'] = downloaded
    meta['matched_companies'] = matched_companies
    save_meta(meta)

    # 결과
    print('\n' + '=' * 70)
    print(f'  IRGO 수집 완료')
    print('=' * 70)
    print(f'  ID 시도          : {args.count}')
    print(f'  실제 fetch       : {stats["fetched"]}')
    print(f'  PDF 없음         : {stats["no_pdf"]}')
    print(f'  매칭 실패        : {stats["no_match"]}')
    print(f'  ✅ 회사 매칭     : {stats["matched"]}')
    print(f'    └ 우선순위     : {stats["priority_matched"]}')
    print(f'  ⏭ 이미 보유     : {stats["skipped_already"]}')
    print(f'  📥 다운로드       : {stats["downloaded"]}')
    print(f'  ❌ 에러          : {stats["errors"]}')
    print(f'\n  누적 회사        : {len(matched_companies)}개')
    print(f'  누적 PDF         : {sum(len(v) for v in downloaded.values())}개')


if __name__ == '__main__':
    main()
