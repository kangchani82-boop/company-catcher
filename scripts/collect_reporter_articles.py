"""
scripts/collect_reporter_articles.py
─────────────────────────────────────
파이낸스스코프 고종민 기자 기사를 수집해 reference_articles 테이블에 저장.
기사 스타일 학습용 (generate_yonhap_draft.py / generate_draft.py 프롬프트에 반영).

- print 페이지(finance-scope.com/article/print/{id})를 urllib로 fetch
- 제목 / 리드 / 본문 추출, 편집자주·프리미엄 안내 필터
- 본문 200자 미만(유료 차단)이면 skip
- reference_articles UNIQUE(url) — 멱등

시드 URL은 WebSearch로 확보한 고종민 기자 기사. 추가 시 SEED_IDS에 append.

실행:
  python scripts/collect_reporter_articles.py
  python scripts/collect_reporter_articles.py --add scp202601010001  # 단건 추가
"""
import argparse
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
REPORTER = "고종민"

# WebSearch(구글 뉴스)로 확보한 고종민 기자 기사 ID (시드).
# byline 필터(parse_article)가 비고종민 기사를 자동 제거하므로 후보를 넉넉히 둠.
SEED_IDS = list(dict.fromkeys([
    # 1차 시드
    "scp202605260005", "scp202512010031", "scp202511270028", "scp202605120022",
    "scp202605130008", "scp202602020011", "scp202606160016", "scp202511250006",
    "scp202601120007", "scp202602190014", "scp202605210017", "scp202605190010",
    "scp202602120012", "scp202603170011", "scp202503240001", "scp202602260002",
    # 반도체/HBM
    "scp202510310016", "scp202602230023", "scp202601290032", "scp202601290028",
    "scp202502210006", "scp202408260011", "scp202601290027", "scp202601290030",
    "scp202507240021",
    # 2차전지/배터리
    "scp202510200024", "scp202601280025", "scp202510170016", "scp202602050019",
    "scp202511030019", "scp202602190021", "scp202510300017",
    # 바이오/제약
    "scp202512080008", "scp202602200025", "scp202502110010", "scp202509170026",
    "scp202411010003", "scp202511050022", "scp202512290017", "scp202602270023",
    "scp202507150019",
    # 실적/수주
    "scp202511030027", "scp202510310018", "scp202511130018", "scp202603040013",
    "scp202510240017", "scp202602040025", "scp202602090025",
    # 적자/부진 (부정 톤)
    "scp202603040001", "scp202602260004", "scp202505130017", "scp202603040016",
    "scp202602130021",
    # 신사업/특허/투자
    "scp202409260008", "scp202603100023", "scp202601230018", "scp202601050001",
    # 로봇/AI/우주/방산
    "scp202602260001", "scp202601280024", "scp202602100026", "scp202601300016",
    "scp202511030009", "scp202508060007", "scp202601290027",
]))

PRINT_URL = "https://www.finance-scope.com/article/print/{aid}"
VIEW_URL = "https://www.finance-scope.com/article/view/{aid}"

# 섹션별 기사 목록 (고종민 기자가 주로 쓰는 섹터). 크롤링 모드(--crawl)에서 사용.
SECTION_LISTS = [
    "scp_SC007000000",  # 전체기사
    "scp_SC005001000",  # 반도체
    "scp_SC005002000",  # 이차전지
    "scp_SC005003000",  # 바이오
    "scp_SC006004000",  # AI
    "scp_SC001000000",  # FS
]
LIST_URL = "https://www.finance-scope.com/article/list/{sec}"
# 페이지네이션 (page 파라미터) — 최신 N페이지까지
LIST_PAGES = 5

# 필터링할 안내 문구
SKIP_LINE_PATTERNS = [
    "편집자주", "프리미엄 회원", "유료 출고", "무단 전재", "재배포 금지",
    "저작권", "ⓒ", "Copyright", "기자 =", "@finance-scope.com",
    "작성 :", "수정 :", "(사진=", "CI.", "▲",
]

# 섹터 키워드 (간단 분류)
SECTOR_KW = {
    "반도체": ["반도체", "HBM", "웨이퍼", "PCB", "파운드리", "DRAM", "낸드"],
    "2차전지": ["2차전지", "배터리", "전고체", "양극재", "음극재", "리튬"],
    "소부장": ["소재", "부품", "장비", "소부장", "MLCC", "기판"],
    "바이오": ["바이오", "제약", "임상", "치료제", "신약", "MDR", "FDA"],
    "핀테크": ["핀테크", "전자금융", "결제", "페이", "금융"],
    "로봇": ["로봇", "자동화", "협동로봇"],
    "우주항공": ["위성", "발사체", "누리호", "방산"],
}


def _http_get(url: str) -> str | None:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception:
        return None


def fetch(aid: str) -> str | None:
    for base in (PRINT_URL, VIEW_URL):
        html = _http_get(base.format(aid=aid))
        if html:
            return html
    return None


def crawl_article_ids() -> list[str]:
    """섹션 목록 페이지들을 크롤링해서 scp 기사 ID를 모두 수집 (중복 제거)."""
    ids = []
    seen = set()
    for sec in SECTION_LISTS:
        for page in range(1, LIST_PAGES + 1):
            url = LIST_URL.format(sec=sec)
            if page > 1:
                url += f"?page={page}"
            html = _http_get(url)
            if not html:
                continue
            # scp + YYYYMMDD(8) + 4자리 = scp + 12자리. 연도 2024~2027만 (비정상 ID 배제)
            found = re.findall(r"scp202[4-7]\d{8}", html)
            new = [a for a in found if a not in seen]
            for a in new:
                seen.add(a)
                ids.append(a)
            time.sleep(0.5)
        print(f"  [crawl] {sec}: 누적 {len(ids)}개 ID")
    return ids


# 사이트 기본/에러 페이지 식별 (없는 기사가 홈으로 리다이렉트될 때)
_GENERIC_TITLE_MARKERS = [
    "대한민국 개인투자자를 위한", "신뢰할 수 있는 투자 전문지",
    "파이낸스스코프 - ",
]


def parse_article(html: str) -> dict | None:
    # 실제 작성 기자가 고종민인지 — 기사 메타 영역의 기자 식별자로 확인
    # (푸터의 기자 목록만으로 통과하지 않도록 "고종민 기자" 또는 기자 이메일 요구)
    if "고종민" not in html:
        return None
    has_byline = ("고종민 기자" in html) or ("kjm@finance-scope.com" in html) \
                 or re.search(r"고종민\s*기자", html) is not None
    if not has_byline:
        return None

    # script/style 제거 후 태그 제거
    text = re.sub(r"<script.*?</script>", "", html, flags=re.DOTALL | re.I)
    text = re.sub(r"<style.*?</style>", "", text, flags=re.DOTALL | re.I)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&[a-z]+;", " ", text)
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # 제목 = 첫 번째 긴 라인 (15자+)
    title = ""
    body_lines = []
    for l in lines:
        if len(l) < 15:
            continue
        if any(p in l for p in SKIP_LINE_PATTERNS):
            continue
        if not title:
            title = l
            continue
        body_lines.append(l)

    if not title or not body_lines:
        return None

    # 사이트 기본/에러 페이지 reject
    if any(mk in title for mk in _GENERIC_TITLE_MARKERS):
        return None

    lead = body_lines[0] if body_lines else ""
    body = "\n".join(body_lines)
    if len(body) < 250:  # 유료 차단/빈 페이지 추정
        return None

    # 섹터 분류
    sector = "기타"
    joined = title + " " + body[:500]
    for sec, kws in SECTOR_KW.items():
        if any(k in joined for k in kws):
            sector = sec
            break

    return {"title": title, "lead": lead, "body": body, "sector": sector}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--add", help="단건 기사 ID 추가 수집")
    ap.add_argument("--crawl", action="store_true",
                    help="섹션 목록 크롤링으로 고종민 기자 기사 자동 발굴")
    args = ap.parse_args()

    if args.add:
        ids = [args.add]
    elif args.crawl:
        print("[크롤링] 섹션 목록에서 기사 ID 수집 중...")
        ids = crawl_article_ids()
        print(f"[크롤링] 총 {len(ids)}개 기사 ID 확보 → 고종민 기자 필터링\n")
    else:
        ids = SEED_IDS
    db = sqlite3.connect(str(DB_PATH))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    quiet = args.crawl  # 크롤링 모드에선 ok만 출력 (fail/skip 폭주 방지)
    ok = skip = fail = 0
    for i, aid in enumerate(ids, 1):
        url = VIEW_URL.format(aid=aid)
        if db.execute("SELECT 1 FROM reference_articles WHERE url=?", [url]).fetchone():
            skip += 1
            if not quiet:
                print(f"  [skip] {aid} (이미 수집됨)")
            continue
        html = fetch(aid)
        if not html:
            fail += 1
            if not quiet:
                print(f"  [fail] {aid} (fetch 실패)")
            continue
        art = parse_article(html)
        if not art:
            fail += 1
            if not quiet:
                print(f"  [fail] {aid} (파싱 실패/유료/비고종민)")
            continue
        m = re.match(r"scp(\d{4})(\d{2})(\d{2})", aid)
        pub = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""
        db.execute("""
            INSERT OR IGNORE INTO reference_articles
              (reporter, title, lead, body, sector, url, published_at, collected_at, char_count)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, [REPORTER, art["title"], art["lead"], art["body"], art["sector"],
              url, pub, now, len(art["body"])])
        db.commit()
        ok += 1
        print(f"  [ok {ok}] {aid} [{art['sector']}] {art['title'][:48]} ({len(art['body'])}자)")
        time.sleep(0.4 if quiet else 1)
        if quiet and i % 50 == 0:
            print(f"    … {i}/{len(ids)} 처리 (ok={ok})")

    print(f"\n[완료] ok={ok} skip={skip} fail={fail}")
    total = db.execute("SELECT COUNT(*) FROM reference_articles WHERE reporter=?", [REPORTER]).fetchone()[0]
    print(f"reference_articles 총 {REPORTER} 기사: {total}건")
    print("\n[섹터 분포]")
    for r in db.execute("SELECT sector, COUNT(*) FROM reference_articles WHERE reporter=? GROUP BY 1 ORDER BY 2 DESC", [REPORTER]):
        print(f"  {r[0]}: {r[1]}")
    db.close()


if __name__ == "__main__":
    main()
