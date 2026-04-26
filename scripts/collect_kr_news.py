"""
scripts/collect_kr_news.py
──────────────────────────
한국 미디어 외부 자료 수집기 (Phase B1)

Naver News Search API 활용:
  - 무료 한도: 25,000회/일
  - corp_name + lead 키워드로 검색
  - media_whitelist.yaml의 T1/T2 매체만 필터링
  - external_sources 테이블에 적재

Phase C 스키마 자동 생성:
  - external_sources       : 수집된 외부 자료 풀
  - lead_external_match    : 단서-자료 매칭
  - article_external_refs  : 기사-인용 매핑

실행:
  python scripts/collect_kr_news.py --init                    # 스키마만
  python scripts/collect_kr_news.py --lead-id 12345           # 단일 단서
  python scripts/collect_kr_news.py --severity 4              # severity 4+ 전체
  python scripts/collect_kr_news.py --recent 30               # 최근 30일 단서
  python scripts/collect_kr_news.py --stats
"""

import io
import os
import re
import sys
import json
import time
import sqlite3
import argparse
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from datetime import datetime, timedelta

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
WHITELIST_YAML = ROOT / "rules" / "media_whitelist.yaml"

# ── .env 로드 ─────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

NAVER_CLIENT_ID = os.environ.get("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "").strip()


# ══════════════════════════════════════════════════════════════════════════════
# DB 스키마 (Phase C)
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS external_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,        -- news / research_report / public_research / industry_stats
    outlet_name TEXT NOT NULL,        -- 매체명
    outlet_tier TEXT,                 -- T1 / T2 / RESEARCH / BLOCKED / UNKNOWN
    outlet_weight REAL DEFAULT 1.0,
    title TEXT NOT NULL,
    summary TEXT,
    url TEXT NOT NULL,
    published_at TEXT,                -- 보도일 YYYY-MM-DD
    related_corp_code TEXT,
    related_corp_name TEXT,
    related_keywords TEXT,            -- JSON list
    raw_meta TEXT,                    -- JSON: 원본 응답
    fetched_at TEXT NOT NULL,
    UNIQUE(outlet_name, url)
);
CREATE INDEX IF NOT EXISTS idx_es_corp     ON external_sources(related_corp_code);
CREATE INDEX IF NOT EXISTS idx_es_pub      ON external_sources(published_at);
CREATE INDEX IF NOT EXISTS idx_es_tier     ON external_sources(outlet_tier);
CREATE INDEX IF NOT EXISTS idx_es_type     ON external_sources(source_type);

CREATE TABLE IF NOT EXISTS lead_external_match (
    lead_id INTEGER NOT NULL,
    source_id INTEGER NOT NULL,
    match_score REAL NOT NULL,        -- 0.0~1.0
    keyword_match TEXT,               -- 매칭된 키워드 (JSON)
    fact_check_status TEXT DEFAULT 'NOT_CHECKED',
    fact_check_score INTEGER,
    matched_at TEXT NOT NULL,
    PRIMARY KEY (lead_id, source_id),
    FOREIGN KEY (lead_id)   REFERENCES story_leads(id),
    FOREIGN KEY (source_id) REFERENCES external_sources(id)
);
CREATE INDEX IF NOT EXISTS idx_lem_lead   ON lead_external_match(lead_id);
CREATE INDEX IF NOT EXISTS idx_lem_score  ON lead_external_match(match_score);

CREATE TABLE IF NOT EXISTS article_external_refs (
    article_id INTEGER NOT NULL,
    source_id INTEGER NOT NULL,
    citation_text TEXT,
    citation_position INTEGER,
    inserted_at TEXT NOT NULL,
    PRIMARY KEY (article_id, source_id),
    FOREIGN KEY (article_id) REFERENCES article_drafts(id),
    FOREIGN KEY (source_id)  REFERENCES external_sources(id)
);
CREATE INDEX IF NOT EXISTS idx_aer_article ON article_external_refs(article_id);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# 매체 화이트리스트 로드
# ══════════════════════════════════════════════════════════════════════════════

def load_whitelist() -> dict[str, dict]:
    try:
        import yaml
    except ImportError:
        return {}
    if not WHITELIST_YAML.exists():
        return {}
    with open(WHITELIST_YAML, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    out: dict[str, dict] = {}
    for m in data.get("media", []):
        d = m["domain"].lower().lstrip(".")
        out[d] = {
            "name": m.get("name", d),
            "tier": m.get("tier", "UNKNOWN"),
            "category": m.get("category", ""),
            "weight": m.get("weight", 0.5),
            "note": m.get("note", ""),
        }
    return out


def identify_outlet(url: str, whitelist: dict) -> dict:
    if not url:
        return {"name": "unknown", "tier": "UNKNOWN", "weight": 0.0, "domain": ""}
    try:
        host = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return {"name": "unknown", "tier": "UNKNOWN", "weight": 0.0, "domain": ""}
    if host in whitelist:
        info = whitelist[host].copy()
        info["domain"] = host
        return info
    for d, info in whitelist.items():
        if host.endswith("." + d) or host == d:
            r = info.copy()
            r["domain"] = d
            return r
    return {"name": host, "tier": "UNKNOWN", "weight": 0.0, "domain": host}


# ══════════════════════════════════════════════════════════════════════════════
# Naver News Search API
# ══════════════════════════════════════════════════════════════════════════════

NAVER_API_URL = "https://openapi.naver.com/v1/search/news.json"


def search_naver(query: str, display: int = 30, sort: str = "sim") -> list[dict]:
    """Naver News API 검색.
    sort: sim (정확도) / date (최신)
    """
    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SECRET):
        raise RuntimeError("NAVER_CLIENT_ID/SECRET 미설정")

    q = urllib.parse.quote(query)
    url = f"{NAVER_API_URL}?query={q}&display={min(display,100)}&sort={sort}"
    req = urllib.request.Request(url, headers={
        "X-Naver-Client-Id":     NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        "User-Agent":            "Mozilla/5.0 (CompanyCatcher)",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data.get("items", [])
    except urllib.error.HTTPError as e:
        print(f"  ⚠ Naver API HTTP {e.code}")
        return []
    except Exception as e:
        print(f"  ⚠ Naver API 오류: {e}")
        return []


def strip_html(text: str) -> str:
    """Naver 응답의 <b>···</b> 등 제거."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def parse_pubdate(s: str) -> str | None:
    """RFC 2822 날짜 → YYYY-MM-DD."""
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(s)
        return d.strftime("%Y-%m-%d") if d else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 단서 → 검색 쿼리 생성
# ══════════════════════════════════════════════════════════════════════════════

def build_queries(corp_name: str, lead_keywords: list[str], lead_type: str) -> list[str]:
    """단서 정보로 검색 쿼리 여러 개 생성."""
    cn = corp_name.replace("(주)", "").replace("주식회사", "").strip()
    if not cn:
        return []

    queries = [cn]   # 회사명 단독
    # 회사명 + 핵심 키워드 1개씩
    for kw in (lead_keywords or [])[:3]:
        if kw and len(kw) >= 2:
            queries.append(f"{cn} {kw}")

    # 유형별 보강
    type_aux = {
        "numeric_change": ["실적", "매출", "영업이익"],
        "market_shift":   ["점유율", "시장", "경쟁"],
        "risk_alert":     ["리스크", "위기"],
        "strategy_change":["신사업", "진출", "변화"],
        "supply_chain":   ["공급망", "거래처"],
    }
    for kw in type_aux.get(lead_type, [])[:1]:
        queries.append(f"{cn} {kw}")

    # 중복 제거 + 최대 4개
    return list(dict.fromkeys(queries))[:4]


# ══════════════════════════════════════════════════════════════════════════════
# 수집 + 저장
# ══════════════════════════════════════════════════════════════════════════════

def collect_for_lead(conn: sqlite3.Connection, lead: sqlite3.Row,
                     whitelist: dict, days_window: int = 90,
                     verbose: bool = True) -> dict:
    """
    단일 단서에 대해 외부 자료 수집.
    시간 정책: today(기사 작성일) 기준 days_window 이내만 수집.
    Returns: {fetched: int, saved: int, by_tier: {T1: n, T2: n, OTHER: n}}
    """
    corp_name = (lead["corp_name"] or "").strip()
    if not corp_name:
        return {"fetched": 0, "saved": 0, "by_tier": {}}

    keywords = lead["keywords"] or "[]"
    try:
        kw_list = json.loads(keywords)
    except Exception:
        kw_list = []

    queries = build_queries(corp_name, kw_list, lead["lead_type"] or "")

    # 시간 윈도우: today 기준 days_window일 이내만 (정책 v2)
    now = datetime.now()
    earliest = now - timedelta(days=days_window)

    by_tier = {"T1": 0, "T2": 0, "OTHER": 0}
    seen_urls = set()
    fetched_total = 0
    saved_total = 0

    for q in queries:
        if verbose:
            print(f"  🔎 검색: '{q}'")
        items = search_naver(q, display=30, sort="sim")
        fetched_total += len(items)
        time.sleep(0.15)   # rate limit

        for it in items:
            url = it.get("originallink") or it.get("link") or ""
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            outlet = identify_outlet(url, whitelist)
            tier = outlet["tier"]

            # T1/T2만 저장 (BLOCKED/UNKNOWN 제외)
            if tier not in ("T1", "T2"):
                by_tier["OTHER"] += 1
                continue

            pub_date = parse_pubdate(it.get("pubDate", ""))
            # 시간 윈도우 체크
            if pub_date:
                try:
                    pd = datetime.strptime(pub_date, "%Y-%m-%d")
                    if pd < earliest:
                        continue
                except Exception:
                    pass

            title = strip_html(it.get("title", ""))
            summary = strip_html(it.get("description", ""))

            # corp_name이 제목 또는 요약에 등장하는지 추가 검증
            cn_short = corp_name.replace("(주)", "").replace("주식회사", "").strip()
            if cn_short not in title and cn_short not in summary:
                # 너무 느슨한 매칭은 스킵
                continue

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO external_sources
                        (source_type, outlet_name, outlet_tier, outlet_weight,
                         title, summary, url, published_at,
                         related_corp_code, related_corp_name, related_keywords,
                         raw_meta, fetched_at)
                    VALUES ('news', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
                """, [
                    outlet["name"], tier, outlet.get("weight", 0.7),
                    title, summary, url, pub_date,
                    lead["corp_code"], corp_name,
                    json.dumps(kw_list, ensure_ascii=False),
                    json.dumps({"naver_pubdate": it.get("pubDate")}, ensure_ascii=False),
                ])
                if conn.total_changes:
                    saved_total += 1
                    by_tier[tier] += 1
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    return {"fetched": fetched_total, "saved": saved_total, "by_tier": by_tier}


# ══════════════════════════════════════════════════════════════════════════════
# 통계
# ══════════════════════════════════════════════════════════════════════════════

def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 65)
    print("  외부 자료 수집 현황")
    print("=" * 65)
    total = conn.execute("SELECT COUNT(*) FROM external_sources").fetchone()[0]
    print(f"\n  전체: {total:,}건")

    print("\n  [매체 tier 별]")
    for r in conn.execute("""
        SELECT outlet_tier, COUNT(*) cnt FROM external_sources
        GROUP BY outlet_tier ORDER BY cnt DESC
    """):
        print(f"    {r['outlet_tier']:<10}: {r['cnt']:,}건")

    print("\n  [매체별 Top 15]")
    for r in conn.execute("""
        SELECT outlet_name, outlet_tier, COUNT(*) cnt FROM external_sources
        GROUP BY outlet_name ORDER BY cnt DESC LIMIT 15
    """):
        print(f"    {r['outlet_name']:<20} ({r['outlet_tier']:<3}) : {r['cnt']:,}건")

    matched = conn.execute("SELECT COUNT(*) FROM lead_external_match").fetchone()[0]
    print(f"\n  단서-자료 매칭: {matched:,}건")

    refs = conn.execute("SELECT COUNT(*) FROM article_external_refs").fetchone()[0]
    print(f"  기사-인용 사용: {refs:,}건")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="한국 미디어 외부 자료 수집")
    p.add_argument("--init",     action="store_true", help="DB 스키마만 생성")
    p.add_argument("--lead-id",  type=int, help="단일 단서 처리")
    p.add_argument("--severity", type=int, help="severity N 이상 단서 전체")
    p.add_argument("--recent",   type=int, help="최근 N일 내 생성된 단서")
    p.add_argument("--limit",    type=int, default=20, help="처리 단서 수 한도")
    p.add_argument("--days-window", type=int, default=90,
                   help="today 기준 며칠 이내 자료만 수집 (기본 90일, 정책 v2)")
    p.add_argument("--stats",    action="store_true")
    args = p.parse_args()

    conn = get_conn()
    ensure_schema(conn)

    if args.init:
        print("✓ external_sources 외 3개 테이블 스키마 생성 완료")
        return

    if args.stats:
        print_stats(conn); return

    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SECRET):
        print("[오류] .env에 NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 설정 필요")
        sys.exit(1)

    whitelist = load_whitelist()
    print(f"[info] 매체 화이트리스트 로드: {len(whitelist)}개")

    # 대상 단서 선정
    if args.lead_id:
        leads = conn.execute(
            "SELECT * FROM story_leads WHERE id = ?", [args.lead_id]
        ).fetchall()
    elif args.severity:
        leads = conn.execute("""
            SELECT * FROM story_leads
            WHERE severity >= ?
              AND NOT EXISTS (
                SELECT 1 FROM lead_external_match lem
                WHERE lem.lead_id = story_leads.id
              )
            ORDER BY severity DESC, id DESC LIMIT ?
        """, [args.severity, args.limit]).fetchall()
    elif args.recent:
        leads = conn.execute("""
            SELECT * FROM story_leads
            WHERE created_at >= datetime('now', ?)
              AND NOT EXISTS (
                SELECT 1 FROM lead_external_match lem
                WHERE lem.lead_id = story_leads.id
              )
            ORDER BY severity DESC LIMIT ?
        """, [f"-{args.recent} days", args.limit]).fetchall()
    else:
        # 기본: severity 4+ 미수집
        leads = conn.execute("""
            SELECT * FROM story_leads
            WHERE severity >= 4
              AND NOT EXISTS (
                SELECT 1 FROM lead_external_match lem
                WHERE lem.lead_id = story_leads.id
              )
            ORDER BY severity DESC, id DESC LIMIT ?
        """, [args.limit]).fetchall()

    if not leads:
        print("[info] 처리할 단서 없음")
        print_stats(conn); return

    print(f"[info] 처리 대상: {len(leads)}개 단서\n")

    grand_total = {"fetched": 0, "saved": 0,
                   "by_tier": {"T1": 0, "T2": 0, "OTHER": 0}}
    for i, lead in enumerate(leads, 1):
        print(f"[{i:3d}/{len(leads)}] #{lead['id']} {lead['corp_name']} | {lead['lead_type']} sev{lead['severity']}")
        r = collect_for_lead(conn, lead, whitelist,
                             days_window=args.days_window, verbose=True)
        print(f"          → 검색 {r['fetched']:>3} / 저장 {r['saved']:>2} "
              f"(T1 {r['by_tier']['T1']} / T2 {r['by_tier']['T2']} / 기타 {r['by_tier']['OTHER']})\n")
        for k in ("fetched", "saved"):
            grand_total[k] += r[k]
        for k in ("T1", "T2", "OTHER"):
            grand_total["by_tier"][k] += r["by_tier"][k]
        time.sleep(0.3)

    print(f"\n=== 완료 ===")
    print(f"  검색 총 {grand_total['fetched']}건 / 저장 {grand_total['saved']}건")
    print(f"  T1 {grand_total['by_tier']['T1']} / T2 {grand_total['by_tier']['T2']} / 기타 {grand_total['by_tier']['OTHER']}")
    print()
    print_stats(conn)


if __name__ == "__main__":
    main()
