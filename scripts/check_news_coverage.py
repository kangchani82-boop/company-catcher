"""
scripts/check_news_coverage.py
────────────────────────────────
취재단서(story_leads)의 뉴스 보도 여부를 자동 확인

판정 기준 (최근 30일):
  0건  → exclusive  🔴 독점 (뉴스가치 최고)
  1~2건 → partial   🟡 일부보도
  3건+  → covered   🟢 기보도

검색 채널:
  1. Google News RSS (무료, 무제한)
  2. Naver 검색 API  (NAVER_CLIENT_ID/SECRET 있을 때만)

실행:
  python scripts/check_news_coverage.py --all          # 전체 미확인 단서 체크
  python scripts/check_news_coverage.py --id 42        # 특정 단서만
  python scripts/check_news_coverage.py --all --force  # 기확인 포함 재체크
  python scripts/check_news_coverage.py --stats        # 현황 통계
"""

import io, sys, os, json, time, re, sqlite3, argparse
import urllib.request, urllib.error, urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# ── 환경변수 ──────────────────────────────────────────────────────────────────
def _load_env():
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
_load_env()

NAVER_ID     = os.environ.get("NAVER_CLIENT_ID", "").strip()
NAVER_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "").strip()
API_DELAY    = 0.5   # 채널 간 딜레이 (초)
RECENT_DAYS  = 30    # 최근 N일 이내 보도만 카운트


# ── DB 연결 ──────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ── 검색 쿼리 생성 ────────────────────────────────────────────────────────────
def build_queries(corp_name: str, title: str, keywords: list[str]) -> list[str]:
    """검색에 쓸 쿼리 목록 생성 (중복 제거)."""
    queries = []
    # 1) 기업명 + 제목 주요어 (첫 10자)
    title_short = title[:20].strip() if title else ""
    if corp_name and title_short:
        queries.append(f"{corp_name} {title_short}")
    # 2) 기업명 + 첫 키워드
    if corp_name and keywords:
        queries.append(f"{corp_name} {keywords[0]}")
    # 3) 기업명만 (fallback)
    if corp_name:
        queries.append(corp_name)
    return list(dict.fromkeys(queries))  # 순서 유지 중복 제거


# ── Google News RSS 검색 ──────────────────────────────────────────────────────
def search_google_news(query: str) -> list[dict]:
    """
    Google News RSS에서 최근 30일 기사 목록 반환.
    반환: [{"title": ..., "url": ..., "published": ...}]
    """
    enc = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={enc}&hl=ko&gl=KR&ceid=KR:ko"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
        root = ET.fromstring(raw)
        ns = {}
        cutoff = datetime.now() - timedelta(days=RECENT_DAYS)
        results = []
        for item in root.findall(".//item"):
            pub_text = item.findtext("pubDate", "")
            try:
                # RFC 2822 형식 파싱
                from email.utils import parsedate_to_datetime
                pub_dt = parsedate_to_datetime(pub_text).replace(tzinfo=None)
            except Exception:
                pub_dt = datetime.now()  # 파싱 실패 시 최신으로 처리

            if pub_dt < cutoff:
                continue

            title = item.findtext("title", "")
            link  = item.findtext("link",  "")
            results.append({
                "title":     title,
                "url":       link,
                "published": pub_dt.strftime("%Y-%m-%d"),
                "source":    "google",
            })
        return results
    except Exception as e:
        return []


# ── Naver 검색 API ─────────────────────────────────────────────────────────────
def search_naver_news(query: str) -> list[dict]:
    """네이버 뉴스 검색 API (키 있을 때만)."""
    if not NAVER_ID or not NAVER_SECRET:
        return []
    enc = urllib.parse.quote(query)
    url = f"https://openapi.naver.com/v1/search/news.json?query={enc}&display=20&sort=date"
    try:
        req = urllib.request.Request(url, headers={
            "X-Naver-Client-Id":     NAVER_ID,
            "X-Naver-Client-Secret": NAVER_SECRET,
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        cutoff = datetime.now() - timedelta(days=RECENT_DAYS)
        results = []
        for item in data.get("items", []):
            pub_text = item.get("pubDate", "")
            try:
                from email.utils import parsedate_to_datetime
                pub_dt = parsedate_to_datetime(pub_text).replace(tzinfo=None)
            except Exception:
                pub_dt = datetime.now()
            if pub_dt < cutoff:
                continue
            # HTML 태그 제거
            title = re.sub(r"<[^>]+>", "", item.get("title", ""))
            results.append({
                "title":     title,
                "url":       item.get("originallink", item.get("link", "")),
                "published": pub_dt.strftime("%Y-%m-%d"),
                "source":    "naver",
            })
        return results
    except Exception:
        return []


# ── 보도 여부 판정 ────────────────────────────────────────────────────────────
def classify(total_articles: int) -> str:
    if total_articles == 0:
        return "exclusive"
    elif total_articles <= 2:
        return "partial"
    else:
        return "covered"


# ── 단일 취재단서 체크 ─────────────────────────────────────────────────────────
def check_lead(lead: sqlite3.Row) -> dict:
    """
    단일 취재단서의 뉴스 보도 여부 확인.
    반환: {news_status, news_count, news_urls, queries}
    """
    corp_name = lead["corp_name"] or ""
    title     = lead["title"]     or ""
    try:
        keywords = json.loads(lead["keywords"] or "[]")
    except Exception:
        keywords = []

    queries = build_queries(corp_name, title, keywords)
    all_articles = []
    seen_urls = set()

    for q in queries[:2]:  # 상위 2개 쿼리만 (과도한 호출 방지)
        # Google News
        articles = search_google_news(q)
        for a in articles:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                all_articles.append(a)
        time.sleep(API_DELAY)

        # Naver (키 있을 때)
        articles_n = search_naver_news(q)
        for a in articles_n:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                all_articles.append(a)
        if articles_n:
            time.sleep(API_DELAY)

    status  = classify(len(all_articles))
    top_urls = [a["url"] for a in all_articles[:5]]  # 최대 5개 URL 저장

    return {
        "news_status": status,
        "news_count":  len(all_articles),
        "news_urls":   json.dumps(top_urls, ensure_ascii=False),
        "queries":     queries,
    }


# ── DB 업데이트 ──────────────────────────────────────────────────────────────
def save_result(conn: sqlite3.Connection, lead_id: int, result: dict):
    conn.execute("""
        UPDATE story_leads
        SET news_status     = ?,
            news_checked_at = datetime('now','localtime'),
            news_urls       = ?
        WHERE id = ?
    """, [result["news_status"], result["news_urls"], lead_id])
    conn.commit()


# ── 통계 출력 ─────────────────────────────────────────────────────────────────
def print_stats(conn: sqlite3.Connection):
    rows = conn.execute("""
        SELECT news_status, COUNT(*) as cnt
        FROM story_leads
        GROUP BY news_status
    """).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM story_leads").fetchone()[0]
    print(f"\n{'='*50}")
    print(f"  뉴스 보도 여부 현황  (전체 {total}건)")
    print(f"{'='*50}")
    labels = {
        "exclusive": "🔴 독점    (미보도)",
        "partial":   "🟡 일부보도",
        "covered":   "🟢 기보도  (3건+)",
        "unknown":   "🔵 미확인",
        None:        "🔵 미확인",
    }
    for r in rows:
        label = labels.get(r["news_status"], r["news_status"])
        pct   = r["cnt"] / total * 100 if total else 0
        bar   = "█" * int(pct / 2)
        print(f"  {label:<20} {r['cnt']:>5}건  {pct:5.1f}%  {bar}")
    print(f"{'='*50}\n")


# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="취재단서 뉴스 보도 여부 자동 확인")
    ap.add_argument("--all",   action="store_true", help="전체 미확인 단서 체크")
    ap.add_argument("--force", action="store_true", help="기확인 포함 재체크")
    ap.add_argument("--id",    type=int,            help="특정 단서 ID만 체크")
    ap.add_argument("--limit", type=int, default=50, help="최대 처리 건수 (기본 50)")
    ap.add_argument("--stats", action="store_true", help="현황 통계만 출력")
    args = ap.parse_args()

    conn = get_conn()

    if args.stats:
        print_stats(conn)
        return

    if args.id:
        lead = conn.execute("SELECT * FROM story_leads WHERE id=?", [args.id]).fetchone()
        if not lead:
            print(f"[오류] 취재단서 id={args.id} 없음")
            return
        leads = [lead]
    elif args.all:
        if args.force:
            where = "status != 'archived'"
        else:
            where = "status != 'archived' AND (news_status IS NULL OR news_status = 'unknown')"
        leads = conn.execute(
            f"SELECT * FROM story_leads WHERE {where} ORDER BY severity DESC, created_at DESC LIMIT ?",
            [args.limit]
        ).fetchall()
    else:
        ap.print_help()
        return

    total = len(leads)
    print(f"\n체크 대상: {total}건  (Naver API: {'있음 ✓' if NAVER_ID else '없음 — Google만 사용'})\n")

    status_count = {"exclusive": 0, "partial": 0, "covered": 0}

    for i, lead in enumerate(leads, 1):
        corp  = lead["corp_name"] or lead["corp_code"] or "-"
        title = (lead["title"] or "")[:30]
        sys.stdout.write(f"  [{i:>3}/{total}] {corp:<20} {title:<30} ")
        sys.stdout.flush()

        try:
            result = check_lead(lead)
            save_result(conn, lead["id"], result)
            icon = {"exclusive": "🔴", "partial": "🟡", "covered": "🟢"}.get(result["news_status"], "?")
            print(f"{icon} {result['news_status']} ({result['news_count']}건)")
            status_count[result["news_status"]] = status_count.get(result["news_status"], 0) + 1
        except Exception as e:
            print(f"⚠ 오류: {e}")
            continue

    print(f"\n완료: 독점 {status_count.get('exclusive',0)}건 / 일부 {status_count.get('partial',0)}건 / 기보도 {status_count.get('covered',0)}건")
    print_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
