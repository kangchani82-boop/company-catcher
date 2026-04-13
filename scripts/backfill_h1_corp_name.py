"""
scripts/backfill_h1_corp_name.py
─────────────────────────────────
h1(반기보고서) 레코드의 corp_name이 전부 공백인 문제 수정

Step 1-A: annual/q3/q1 레코드에 동일 corp_code가 있으면 → DB 내부 UPDATE (빠름)
Step 1-B: h1 전용 기업(다른 보고서 없음) → DART /api/company.json 조회 후 UPDATE

사용법:
  python scripts/backfill_h1_corp_name.py            # 전체 실행
  python scripts/backfill_h1_corp_name.py --dry-run  # 통계만 출력
  python scripts/backfill_h1_corp_name.py --step a   # Step 1-A만
  python scripts/backfill_h1_corp_name.py --step b   # Step 1-B만
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
DART_BASE_URL = "https://opendart.fss.or.kr/api"
RATE_LIMIT_SEC = 0.15
DEFAULT_WORKERS = 5
REQUEST_TIMEOUT = 20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill_h1")

# 전역 레이트 리미터
_rate_lock = threading.Lock()
_last_call = [0.0]


def _rate_limited_get(url: str) -> bytes:
    with _rate_lock:
        elapsed = time.time() - _last_call[0]
        if elapsed < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - elapsed)
        _last_call[0] = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return resp.read()


def get_dart_api_key() -> str:
    """환경변수 또는 .env에서 DART API 키 로드"""
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    key = os.environ.get("DART_API_KEY", "")
    if not key:
        # app_config.yaml fallback
        try:
            import yaml
            cfg = yaml.safe_load((ROOT / "config" / "app_config.yaml").read_text(encoding="utf-8"))
            key = (cfg.get("dart", {}) or {}).get("api_key", "")
        except Exception:
            pass
    return key


def step_a_db_join(db: sqlite3.Connection, dry_run: bool = False) -> int:
    """
    Step 1-A: annual/q3/q1에 동일 corp_code가 있는 h1 기업 → DB 내부 UPDATE
    빠름, API 호출 없음
    """
    log.info("[Step 1-A] DB 내부 JOIN으로 corp_name 채우기 시작...")

    # 채울 수 있는 corp_code → corp_name 매핑 조회
    rows = db.execute("""
        SELECT DISTINCT h.corp_code, other.corp_name
        FROM reports h
        JOIN (
            SELECT corp_code, corp_name
            FROM reports
            WHERE report_type IN ('2025_annual','2025_q3','2025_q1','A')
              AND corp_name IS NOT NULL AND corp_name != ''
            GROUP BY corp_code
            HAVING MAX(LENGTH(corp_name)) > 0
        ) other ON h.corp_code = other.corp_code
        WHERE h.report_type = '2025_h1'
          AND (h.corp_name IS NULL OR h.corp_name = '')
    """).fetchall()

    log.info(f"  매칭 가능: {len(rows)}개 기업")

    if dry_run:
        log.info("  [dry-run] 실제 UPDATE 건너뜀")
        return len(rows)

    updated = 0
    for corp_code, corp_name in rows:
        db.execute(
            "UPDATE reports SET corp_name=?, updated_at=? WHERE report_type='2025_h1' AND corp_code=?",
            (corp_name, datetime.now().isoformat(), corp_code)
        )
        updated += 1

    db.commit()
    log.info(f"  ✓ {updated}개 기업 UPDATE 완료")
    return updated


def fetch_company_name(api_key: str, corp_code: str) -> str | None:
    """DART /api/company.json 에서 기업명 조회"""
    try:
        url = f"{DART_BASE_URL}/company.json?crtfc_key={api_key}&corp_code={corp_code}"
        raw = _rate_limited_get(url)
        d = json.loads(raw)
        if d.get("status") == "000" and d.get("corp_name"):
            return d["corp_name"].strip()
    except Exception as e:
        log.debug(f"  DART API 오류 {corp_code}: {e}")
    return None


def step_b_dart_api(db: sqlite3.Connection, api_key: str,
                    dry_run: bool = False, workers: int = DEFAULT_WORKERS) -> dict:
    """
    Step 1-B: h1 전용 기업(다른 보고서 없음) → DART company API로 이름 조회
    """
    log.info("[Step 1-B] DART API로 h1 단독 기업명 조회 시작...")

    if not api_key:
        log.error("  DART API 키 없음 — Step 1-B 스킵")
        return {"total": 0, "success": 0, "failed": 0}

    # h1 전용 corp_code 목록 (annual/q3/q1에도 없는 것)
    rows = db.execute("""
        SELECT DISTINCT corp_code
        FROM reports
        WHERE report_type = '2025_h1'
          AND (corp_name IS NULL OR corp_name = '')
    """).fetchall()
    pending = [r[0] for r in rows]

    log.info(f"  조회 대상: {len(pending)}개 기업 (워커: {workers}개)")
    log.info(f"  예상 소요: ~{len(pending) * RATE_LIMIT_SEC / workers:.0f}초")

    if dry_run:
        log.info("  [dry-run] 실제 조회 건너뜀")
        return {"total": len(pending), "success": 0, "failed": 0}

    stats = {"total": len(pending), "success": 0, "failed": 0}
    db_lock = threading.Lock()
    completed = [0]

    def fetch_one(corp_code: str) -> tuple[str, str | None]:
        name = fetch_company_name(api_key, corp_code)
        return corp_code, name

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, cc): cc for cc in pending}
        for future in as_completed(futures):
            corp_code = futures[future]
            try:
                cc, name = future.result()
            except Exception as e:
                log.warning(f"  워커 예외 {corp_code}: {e}")
                stats["failed"] += 1
                completed[0] += 1
                continue

            if name:
                with db_lock:
                    db.execute(
                        "UPDATE reports SET corp_name=?, updated_at=? WHERE report_type='2025_h1' AND corp_code=?",
                        (name, datetime.now().isoformat(), cc)
                    )
                    stats["success"] += 1
            else:
                stats["failed"] += 1

            completed[0] += 1
            if completed[0] % 100 == 0:
                with db_lock:
                    db.commit()
                log.info(f"  진행: {completed[0]}/{len(pending)} "
                         f"(성공 {stats['success']}, 실패 {stats['failed']})")

    db.commit()
    log.info(f"  ✓ Step 1-B 완료: 성공 {stats['success']}개, 실패 {stats['failed']}개")
    return stats


def print_summary(db: sqlite3.Connection):
    """최종 현황 출력"""
    row = db.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN corp_name IS NULL OR corp_name='' THEN 1 ELSE 0 END) as empty,
               SUM(CASE WHEN corp_name IS NOT NULL AND corp_name != '' THEN 1 ELSE 0 END) as filled
        FROM reports WHERE report_type='2025_h1'
    """).fetchone()
    log.info("")
    log.info("=" * 60)
    log.info("  h1 corp_name 백필 결과")
    log.info("=" * 60)
    log.info(f"  전체:   {row[0]:,}건")
    log.info(f"  이름 채움: {row[2]:,}건 ({row[2]/row[0]*100:.1f}%)")
    log.info(f"  이름 없음: {row[1]:,}건")
    log.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="h1 corp_name 백필")
    parser.add_argument("--dry-run", action="store_true", help="통계만 출력, DB 수정 없음")
    parser.add_argument("--step", choices=["a", "b"], help="특정 단계만 실행 (기본: 전체)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()

    if not DB_PATH.exists():
        log.error(f"DB 없음: {DB_PATH}")
        sys.exit(1)

    api_key = get_dart_api_key()
    log.info(f"DART API 키: {'설정됨' if api_key else '없음'}")

    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")

    try:
        # 시작 전 현황
        before = db.execute("""
            SELECT SUM(CASE WHEN corp_name='' THEN 1 ELSE 0 END) FROM reports WHERE report_type='2025_h1'
        """).fetchone()[0]
        log.info(f"시작 전 빈 corp_name: {before}건")
        log.info("")

        run_a = args.step in (None, "a")
        run_b = args.step in (None, "b")

        if run_a:
            step_a_db_join(db, dry_run=args.dry_run)
            log.info("")

        if run_b:
            step_b_dart_api(db, api_key, dry_run=args.dry_run, workers=args.workers)
            log.info("")

        print_summary(db)

    finally:
        db.close()


if __name__ == "__main__":
    main()
