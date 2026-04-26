"""
scripts/collect_event_disclosures.py
────────────────────────────────────
Phase S — DART 수시공시·주요사항보고서 통합 수집

목적: 분기·반기 보고서 사이의 즉시 변화 감지
  - 단일판매·공급계약 (매출 변화 즉시 신호)
  - 주요사항보고서 (M&A, 자산 양수·양도)
  - 임원·주요주주 변동 (지배구조)
  - 공정공시 (실적 가이던스)
  - 회사합병/분할 결정
  - 유상증자 결정

DART OpenAPI Free:
  https://opendart.fss.or.kr/api/list.json
  (이미 사용 중인 기존 API 활용)

DB:
  event_disclosures — 수시공시 통합 저장
  event_leads      — 수시공시에서 발견된 단서 (story_leads와 별도)

실행:
  python scripts/collect_event_disclosures.py --init
  python scripts/collect_event_disclosures.py --recent 30  # 최근 30일
  python scripts/collect_event_disclosures.py --stats
  python scripts/collect_event_disclosures.py --build-leads # 수집된 공시→단서 변환
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

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()


def _ensure_utf8_io():
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS event_disclosures (
    rcept_no TEXT PRIMARY KEY,           -- DART 접수번호 (고유)
    corp_code TEXT NOT NULL,
    corp_name TEXT,
    stock_code TEXT,
    report_nm TEXT NOT NULL,             -- 공시 제목
    report_type TEXT,                    -- 공시 유형 분류 (CONTRACT/MA/ETC ...)
    flr_nm TEXT,                         -- 공시제출인
    rcept_dt TEXT,                       -- 접수일자
    rm TEXT,                             -- 비고
    raw_url TEXT,
    fetched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ed_corp ON event_disclosures(corp_code);
CREATE INDEX IF NOT EXISTS idx_ed_dt   ON event_disclosures(rcept_dt);
CREATE INDEX IF NOT EXISTS idx_ed_type ON event_disclosures(report_type);

CREATE TABLE IF NOT EXISTS event_leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rcept_no TEXT NOT NULL,
    corp_code TEXT,
    corp_name TEXT,
    event_type TEXT NOT NULL,            -- CONTRACT/MA/CAPITAL/INSIDER/GUIDANCE
    title TEXT,
    severity INTEGER DEFAULT 3,
    metadata TEXT,                       -- JSON
    article_id INTEGER,                  -- 기사화된 경우
    status TEXT DEFAULT 'new',
    created_at TEXT NOT NULL,
    UNIQUE(rcept_no, event_type),
    FOREIGN KEY (rcept_no) REFERENCES event_disclosures(rcept_no)
);
CREATE INDEX IF NOT EXISTS idx_el_corp ON event_leads(corp_code);
CREATE INDEX IF NOT EXISTS idx_el_type ON event_leads(event_type);
CREATE INDEX IF NOT EXISTS idx_el_sev  ON event_leads(severity);
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
# DART 공시 검색 API
# ══════════════════════════════════════════════════════════════════════════════

DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"

# 공시 유형 분류 (report_nm 키워드 매칭)
EVENT_TYPE_PATTERNS = {
    "CONTRACT":  [r"단일판매.*공급계약", r"공급계약\s*체결", r"수주공시"],
    "MA":        [r"타법인\s*주식.*취득", r"타법인\s*주식.*양수", r"기업합병", r"분할",
                  r"합병계약", r"영업양수", r"영업양도", r"흡수합병"],
    "CAPITAL":   [r"유상증자", r"무상증자", r"감자", r"전환사채", r"신주인수권부사채"],
    "INSIDER":   [r"임원.*주요주주.*소유", r"주식\s*등의\s*대량보유", r"임원\s*변경"],
    "GUIDANCE":  [r"영업\s*\(잠정\)", r"매출액\s*\(잠정\)", r"공정공시", r"실적전망"],
    "GOVERNANCE":[r"이사회.*변경", r"감사.*변경", r"지분변경"],
    "DIVIDEND":  [r"현금\.?현물배당", r"주식배당"],
    "OTHER":     [],
}


def classify_event_type(report_nm: str) -> str:
    """공시 제목에서 유형 분류."""
    if not report_nm:
        return "OTHER"
    for etype, pats in EVENT_TYPE_PATTERNS.items():
        for pat in pats:
            if re.search(pat, report_nm):
                return etype
    return "OTHER"


def fetch_disclosure_list(start_dt: str, end_dt: str, pblntf_ty: str,
                          page_no: int = 1, page_count: int = 100) -> dict:
    """단일 공시 유형에 대한 DART 조회.
    pblntf_ty: B(주요사항)/C(발행)/D(지분)/E(기타) 중 하나."""
    if not DART_API_KEY:
        raise RuntimeError("DART_API_KEY 미설정")
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de":    start_dt,
        "end_de":    end_dt,
        "page_no":   page_no,
        "page_count": page_count,
        "pblntf_ty": pblntf_ty,
    }
    url = DART_LIST_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


# ══════════════════════════════════════════════════════════════════════════════
# 단서 변환 — 수시공시 → event_leads
# ══════════════════════════════════════════════════════════════════════════════

EVENT_SEVERITY = {
    "MA":         5,  # M&A는 항상 큰 변화
    "CONTRACT":   4,  # 대형 공급계약
    "CAPITAL":    4,  # 자본금 변동
    "GUIDANCE":   4,  # 실적 가이던스 변경
    "INSIDER":    3,  # 임원 변동
    "GOVERNANCE": 3,
    "DIVIDEND":   2,
    "OTHER":      2,
}


def make_lead_from_disclosure(d: dict) -> dict | None:
    """공시 한 건을 event_lead 형식으로 변환."""
    etype = d["report_type"]
    if etype == "OTHER":
        return None  # 기타는 단서화 안 함

    title = f"{d['corp_name']} — {d['report_nm']}"
    severity = EVENT_SEVERITY.get(etype, 3)

    return {
        "rcept_no":   d["rcept_no"],
        "corp_code":  d["corp_code"],
        "corp_name":  d["corp_name"],
        "event_type": etype,
        "title":      title,
        "severity":   severity,
        "metadata": {
            "report_nm": d["report_nm"],
            "rcept_dt":  d["rcept_dt"],
            "stock_code": d.get("stock_code"),
            "url": d.get("raw_url"),
        },
    }


def save_disclosure(conn: sqlite3.Connection, item: dict) -> bool:
    """DART 공시 한 건 저장. 신규면 True."""
    rcept_no = item.get("rcept_no", "")
    if not rcept_no:
        return False
    etype = classify_event_type(item.get("report_nm", ""))
    raw_url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
    cur = conn.execute("""
        INSERT OR IGNORE INTO event_disclosures
            (rcept_no, corp_code, corp_name, stock_code,
             report_nm, report_type, flr_nm, rcept_dt, rm,
             raw_url, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
    """, [
        rcept_no, item.get("corp_code", ""), item.get("corp_name", ""),
        item.get("stock_code", ""), item.get("report_nm", ""), etype,
        item.get("flr_nm", ""), item.get("rcept_dt", ""), item.get("rm", ""),
        raw_url,
    ])
    if cur.rowcount > 0:
        # 단서 변환 시도
        d_dict = {
            "rcept_no": rcept_no,
            "corp_code": item.get("corp_code", ""),
            "corp_name": item.get("corp_name", ""),
            "report_nm": item.get("report_nm", ""),
            "rcept_dt":  item.get("rcept_dt", ""),
            "stock_code": item.get("stock_code", ""),
            "raw_url": raw_url,
            "report_type": etype,
        }
        lead = make_lead_from_disclosure(d_dict)
        if lead:
            conn.execute("""
                INSERT OR IGNORE INTO event_leads
                    (rcept_no, corp_code, corp_name, event_type,
                     title, severity, metadata, created_at)
                VALUES (?,?,?,?,?,?,?,datetime('now','localtime'))
            """, [
                lead["rcept_no"], lead["corp_code"], lead["corp_name"],
                lead["event_type"], lead["title"], lead["severity"],
                json.dumps(lead["metadata"], ensure_ascii=False),
            ])
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# 일괄 수집
# ══════════════════════════════════════════════════════════════════════════════

def collect_recent(conn: sqlite3.Connection, days: int = 30) -> dict:
    end = datetime.now()
    start = end - timedelta(days=days)
    bgn = start.strftime("%Y%m%d")
    edt = end.strftime("%Y%m%d")
    print(f"[info] DART 수시공시 수집: {bgn} ~ {edt}")

    # 4가지 공시 유형 각각 호출
    types = [("B", "주요사항보고서"), ("C", "발행공시"),
             ("D", "지분공시"),     ("E", "기타공시")]

    fetched_total = 0
    saved_total = 0

    for pty, plabel in types:
        print(f"\n  📋 {plabel} ({pty}) 수집 중...")
        page = 1
        while True:
            try:
                data = fetch_disclosure_list(bgn, edt, pty,
                                             page_no=page, page_count=100)
            except Exception as e:
                print(f"    ⚠ API 오류: {e}")
                break
            status = data.get("status")
            if status not in ("000", 0, "0"):
                msg = data.get("message", "")
                if "조회된 데이터가 없습니다" in msg:
                    break
                print(f"    ⚠ status={status}, msg={msg}")
                break
            items = data.get("list", [])
            local_saved = 0
            for it in items:
                fetched_total += 1
                if save_disclosure(conn, it):
                    saved_total += 1
                    local_saved += 1
            total_pages = int(data.get("total_page", 1) or 1)
            if page >= total_pages or page >= 50:
                break
            page += 1
            time.sleep(0.3)
            if page % 5 == 0:
                conn.commit()
        conn.commit()
        print(f"    → {plabel} 완료 (누적 수집 {fetched_total} / 저장 {saved_total})")

    return {"fetched": fetched_total, "saved": saved_total}


# ══════════════════════════════════════════════════════════════════════════════
# 통계
# ══════════════════════════════════════════════════════════════════════════════

def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 65)
    print("  DART 수시공시 수집 현황 (Phase S)")
    print("=" * 65)
    total = conn.execute("SELECT COUNT(*) FROM event_disclosures").fetchone()[0]
    print(f"\n  전체 공시: {total:,}건")
    if total == 0:
        print("  (아직 수집 안 됨 — --recent 30 실행)")
        return

    print(f"\n  [공시 유형 분포]")
    for r in conn.execute("""
        SELECT report_type, COUNT(*) cnt FROM event_disclosures
        GROUP BY report_type ORDER BY cnt DESC
    """):
        icon = {"MA":"💼","CONTRACT":"📝","CAPITAL":"💰","INSIDER":"👤",
                "GUIDANCE":"📊","GOVERNANCE":"🏛","DIVIDEND":"💵","OTHER":"📂"}
        print(f"    {icon.get(r['report_type'],'⚪')} {r['report_type']:<12} {r['cnt']:>5}건")

    leads_n = conn.execute("SELECT COUNT(*) FROM event_leads").fetchone()[0]
    print(f"\n  [수시공시 단서 (event_leads)]: {leads_n:,}건")
    for r in conn.execute("""
        SELECT event_type, COUNT(*) cnt, AVG(severity) avg_s
        FROM event_leads GROUP BY event_type ORDER BY cnt DESC
    """):
        print(f"    {r['event_type']:<12} {r['cnt']:>4}건 (sev 평균 {r['avg_s']:.1f})")

    # severity 5 단서 샘플
    print(f"\n  [severity 5 단서 — 즉시 출고 우선]")
    for r in conn.execute("""
        SELECT corp_name, event_type, title FROM event_leads
        WHERE severity = 5 ORDER BY id DESC LIMIT 10
    """):
        print(f"    [{r['event_type']:<10}] {r['title'][:60]}")


def main():
    p = argparse.ArgumentParser(description="DART 수시공시 통합 (Phase S)")
    p.add_argument("--init",    action="store_true")
    p.add_argument("--recent",  type=int, default=0, help="최근 N일치 수집")
    p.add_argument("--stats",   action="store_true")
    args = p.parse_args()

    conn = get_conn()
    ensure_schema(conn)

    if args.init:
        print("✓ event_disclosures + event_leads 스키마 생성")
        return
    if args.stats:
        print_stats(conn)
        return
    if args.recent:
        if not DART_API_KEY:
            print("[오류] DART_API_KEY 미설정")
            sys.exit(1)
        r = collect_recent(conn, args.recent)
        print(f"\n[완료] 수집 {r['fetched']}건 / 저장 {r['saved']}건")
        print()
        print_stats(conn)
        return

    print(__doc__)


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
