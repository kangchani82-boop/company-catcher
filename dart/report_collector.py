"""
dart/report_collector.py
─────────────────────────
DART(전자공시) 보고서 수집기

- OpenDART API 기반
- 149개 기업 (config/kospi200_additions.json) 대상
- 연간 보고서 + Q3 보고서 수집
- data/dart/dart_reports.db SQLite 저장

환경변수:
  DART_API_KEY — OpenDART API 키 (필수, 일일 10,000 호출)

사용법:
  python dart/report_collector.py --company 삼성전자 --limit 1
  python dart/report_collector.py --all --limit 10
"""

import argparse
import json
import logging
import os
import sqlite3
import time
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

logger = logging.getLogger("dart.collector")

ROOT = Path(__file__).parent.parent
DB_DIR = ROOT / "data" / "dart"
DB_PATH = DB_DIR / "dart_reports.db"
KOSPI_PATH = ROOT / "config" / "kospi200_additions.json"

DART_SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    corp_code TEXT PRIMARY KEY,
    corp_name TEXT,
    stock_code TEXT,
    sector TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corp_code TEXT NOT NULL,
    corp_name TEXT,
    report_type TEXT,
    report_name TEXT,
    rcept_no TEXT UNIQUE,
    rcept_dt TEXT,
    flr_nm TEXT,
    raw_text TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS supply_chain (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corp_code TEXT NOT NULL,
    corp_name TEXT,
    relation_type TEXT,
    partner_name TEXT,
    context TEXT,
    source_report TEXT,
    analyzed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_reports_corp ON reports(corp_code);
CREATE INDEX IF NOT EXISTS idx_supply_corp ON supply_chain(corp_code);
CREATE INDEX IF NOT EXISTS idx_supply_partner ON supply_chain(partner_name);
"""


class DARTCollector:
    """OpenDART API 보고서 수집기"""

    BASE_URL = "https://opendart.fss.or.kr/api"
    MIN_INTERVAL = 0.15  # 초
    MAX_RETRIES = 3

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("DART_API_KEY", "")
        if not self.api_key:
            logger.warning("DART_API_KEY 미설정. .env에 DART_API_KEY=... 추가 필요")
        self._last_call_time = 0.0
        DB_DIR.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(DART_SCHEMA)
        self.db.commit()

    def _rate_limit(self):
        elapsed = time.time() - self._last_call_time
        if elapsed < self.MIN_INTERVAL:
            time.sleep(self.MIN_INTERVAL - elapsed)
        self._last_call_time = time.time()

    def _request(self, endpoint: str, params: dict = None) -> dict:
        """OpenDART API 요청"""
        if not self.api_key:
            return {"status": "error", "message": "DART_API_KEY 미설정"}

        import urllib.parse as _up
        self._rate_limit()
        query = f"crtfc_key={self.api_key}"
        if params:
            for k, v in params.items():
                query += f"&{k}={_up.quote(str(v))}"
        url = f"{self.BASE_URL}/{endpoint}.json?{query}"

        for attempt in range(self.MAX_RETRIES):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "FinanceScope/1.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(5 * (attempt + 1))
                    continue
                logger.error(f"DART API {e.code}: {endpoint}")
                return {}
            except Exception as e:
                logger.error(f"DART 요청 실패 ({attempt+1}): {e}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(3)
        return {}

    def search_company(self, corp_name: str) -> list[dict]:
        """기업 검색 (corp_code 조회)"""
        # OpenDART는 기업 목록을 ZIP으로 제공 — 여기서는 공시 검색으로 대체
        data = self._request("list", {
            "corp_name": corp_name,
            "page_count": "5",
        })
        return data.get("list", [])

    def get_filings(self, corp_code: str, report_type: str = None,
                    start_date: str = "20240101", limit: int = 10) -> list[dict]:
        """
        공시 목록 조회

        report_type (pblntf_ty):
          A = 정기공시 (사업보고서·반기·분기)
          None = 전체
        """
        params = {
            "corp_code": corp_code,
            "bgn_de": start_date,
            "page_count": str(limit),
        }
        if report_type:
            params["pblntf_ty"] = report_type
        data = self._request("list", params)
        filings = data.get("list", [])

        for f in filings:
            try:
                self.db.execute(
                    """INSERT OR IGNORE INTO reports
                       (corp_code, corp_name, report_type, report_name, rcept_no, rcept_dt, flr_nm, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        corp_code,
                        f.get("corp_name", ""),
                        report_type,
                        f.get("report_nm", ""),
                        f.get("rcept_no", ""),
                        f.get("rcept_dt", ""),
                        f.get("flr_nm", ""),
                        datetime.now().isoformat(),
                    ),
                )
            except Exception:
                pass
        self.db.commit()
        return filings

    def save_supply_chain(self, corp_code: str, corp_name: str,
                          relations: list[dict], source_report: str = ""):
        """공급망 분석 결과 저장"""
        for rel in relations:
            try:
                self.db.execute(
                    """INSERT INTO supply_chain
                       (corp_code, corp_name, relation_type, partner_name, context, source_report, analyzed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        corp_code,
                        corp_name,
                        rel.get("type", ""),
                        rel.get("partner", ""),
                        rel.get("context", ""),
                        source_report,
                        datetime.now().isoformat(),
                    ),
                )
            except Exception:
                pass
        self.db.commit()

    def fetch_report_text(self, rcept_no: str, max_chars: int = 15000) -> str:
        """보고서 본문 텍스트 수집 (OpenDART document.xml)"""
        import re as _re
        import zipfile, io
        if not self.api_key or not rcept_no:
            return ""
        self._rate_limit()
        url = f"{self.BASE_URL}/document.xml?crtfc_key={self.api_key}&rcept_no={rcept_no}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "FinanceScope/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                content_type = resp.headers.get("Content-Type", "")
                raw = resp.read()
            # ZIP 파일인 경우 (일반적)
            if b"PK" in raw[:4]:
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    texts = []
                    for name in zf.namelist():
                        if name.endswith((".xml", ".html", ".htm")):
                            try:
                                data = zf.read(name).decode("utf-8", errors="ignore")
                                # HTML/XML 태그 제거
                                clean = _re.sub(r'<[^>]+>', ' ', data)
                                clean = _re.sub(r'\s+', ' ', clean).strip()
                                texts.append(clean[:max_chars])
                            except Exception:
                                pass
                    text = "\n".join(texts)[:max_chars]
            else:
                # 직접 XML/HTML
                text = raw.decode("utf-8", errors="ignore")
                text = _re.sub(r'<[^>]+>', ' ', text)
                text = _re.sub(r'\s+', ' ', text).strip()[:max_chars]

            # DB에 저장
            if text:
                self.db.execute(
                    "UPDATE reports SET raw_text = ?, updated_at = ? WHERE rcept_no = ?",
                    (text, datetime.now().isoformat(), rcept_no)
                )
                self.db.commit()
                logger.info(f"본문 수집: {rcept_no} ({len(text)}자)")
            return text
        except Exception as e:
            logger.warning(f"본문 수집 실패 ({rcept_no}): {e}")
            return ""

    def fetch_all_texts(self, limit: int = 30) -> int:
        """raw_text가 없는 보고서의 본문을 일괄 수집"""
        rows = self.db.execute(
            "SELECT rcept_no, corp_name FROM reports WHERE raw_text IS NULL OR raw_text = '' LIMIT ?",
            (limit,)
        ).fetchall()
        fetched = 0
        for row in rows:
            text = self.fetch_report_text(row["rcept_no"])
            if text:
                fetched += 1
                logger.info(f"  [{fetched}/{len(rows)}] {row['corp_name']} — {len(text)}자")
        return fetched

    def get_supply_map(self) -> dict[str, list[str]]:
        """DB에서 공급망 매핑 로드 (reverse_supply_chain.py 연동용)"""
        rows = self.db.execute(
            "SELECT partner_name, corp_name FROM supply_chain"
        ).fetchall()
        supply_map = {}
        for row in rows:
            partner = row["partner_name"].lower()
            if partner not in supply_map:
                supply_map[partner] = []
            if row["corp_name"] not in supply_map[partner]:
                supply_map[partner].append(row["corp_name"])
        return supply_map

    def get_stats(self) -> dict:
        """DB 통계"""
        stats = {}
        for table in ["companies", "reports", "supply_chain"]:
            row = self.db.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
            stats[table] = row["cnt"]
        return stats

    def close(self):
        self.db.close()


def main():
    parser = argparse.ArgumentParser(description="DART 보고서 수집기")
    parser.add_argument("--company", type=str, help="기업명")
    parser.add_argument("--all", action="store_true", help="KOSPI200 전체 수집")
    parser.add_argument("--limit", type=int, default=5, help="수집 건수 제한")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    collector = DARTCollector()

    if args.company:
        results = collector.search_company(args.company)
        print(f"검색 결과: {len(results)}건")
        for r in results[:args.limit]:
            print(f"  {r.get('corp_name', '?')} ({r.get('rcept_no', '?')})")
    elif args.all:
        if KOSPI_PATH.exists():
            data = json.loads(KOSPI_PATH.read_text(encoding="utf-8"))
            companies = data.get("companies", [])[:args.limit]
            print(f"대상: {len(companies)}개 기업")
        else:
            print("config/kospi200_additions.json 없음")
    else:
        stats = collector.get_stats()
        print(f"DART DB 통계: {stats}")

    collector.close()


if __name__ == "__main__":
    main()
