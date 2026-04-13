"""
scripts/backfill_missing_reports.py
─────────────────────────────────────
h1(반기)만 수집된 1,533개 기업의 누락된 보고서(annual, q3, q1) 보충 수집

문제 배경:
  - 반기(h1) 수집 시 전체 DART 스캔으로 3,016개 기업을 광범위하게 수집
  - annual, q3, q1은 기존 DB에 있던 기업만 재처리 → 신규 발견 1,533개 기업 누락

처리 순서:
  1) h1만 있고 annual/q3/q1 이 없는 corp_code 목록 추출
  2) 기업별 DART API 조회 → 해당 유형 보고서 찾기
  3) ZIP 다운로드 → 사업의 내용 추출 → DB 저장

사용법:
  python scripts/backfill_missing_reports.py              # 전체 (annual+q3+q1)
  python scripts/backfill_missing_reports.py --types annual q3
  python scripts/backfill_missing_reports.py --types q1
  python scripts/backfill_missing_reports.py --check      # 현황만 확인
  python scripts/backfill_missing_reports.py --limit 50   # 테스트용
"""

import argparse
import io
import json
import logging
import re
import sqlite3
import sys
import threading
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# ── 설정 ──────────────────────────────────────────────────────────────────────
DB_PATH          = ROOT / "data" / "dart" / "dart_reports.db"
CHECKPOINT_PATH  = ROOT / "data" / "dart" / "backfill_checkpoint.json"
DART_BASE_URL    = "https://opendart.fss.or.kr/api"
RATE_LIMIT_SEC   = 0.15
DEFAULT_WORKERS  = 5
REQUEST_TIMEOUT  = 45
MAX_BIZ_CONTENT  = 80000
MIN_BIZ_CONTENT  = 300

# 보고서 유형별 조회 기간 & 필터
REPORT_CONFIG = {
    "annual": {
        "db_type":   "2025_annual",
        "bgn_de":    "20260101",
        "end_de":    "20260430",
        "label":     "2025 사업보고서",
        "nm_include": ["사업보고서"],
        "nm_exclude": ["반기", "분기"],
    },
    "q3": {
        "db_type":   "2025_q3",
        "bgn_de":    "20251001",
        "end_de":    "20251130",
        "label":     "2025 3분기보고서",
        "nm_include": ["분기보고서", "3분기"],
        "nm_exclude": ["반기"],
    },
    "q1": {
        "db_type":   "2025_q1",
        "bgn_de":    "20250401",
        "end_de":    "20250630",
        "label":     "2025 1분기보고서",
        "nm_include": ["분기보고서"],
        "nm_exclude": ["반기", "3분기"],
    },
}

# 사업의 내용 섹션 패턴
BIZ_START_PATTERNS = [
    r'II\s*[.\s]\s*사\s*업\s*의\s*내\s*용',
    r'2\s*[.\s]\s*사\s*업\s*의\s*내\s*용',
    r'제\s*2\s*장\s*사\s*업\s*의\s*내\s*용',
    r'사\s*업\s*의\s*내\s*용\s*(?=\s)',
]
BIZ_END_PATTERNS = [
    r'III\s*[.\s]\s*재\s*무',
    r'3\s*[.\s]\s*재\s*무',
    r'제\s*3\s*장\s*재\s*무',
    r'III\s*[.\s]\s*주\s*요',
    r'3\s*[.\s]\s*주\s*요',
    r'제\s*3\s*장\s*주\s*요',
    r'IV\s*[.\s]',
    r'4\s*[.\s]\s*감\s*사',
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "logs" / "backfill.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("backfill")

# ── DB ────────────────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    return db


def get_missing_corps(db: sqlite3.Connection, target_type: str) -> list[dict]:
    """
    h1은 있으나 target_type(annual/q3/q1)이 없는 기업 목록 반환
    """
    db_type = REPORT_CONFIG[target_type]["db_type"]
    rows = db.execute(f"""
        SELECT DISTINCT h.corp_code, h.corp_name
        FROM reports h
        WHERE h.report_type = '2025_h1'
          AND (h.biz_content IS NOT NULL AND LENGTH(h.biz_content) >= ?)
          AND h.corp_code NOT IN (
              SELECT DISTINCT corp_code FROM reports
              WHERE report_type = ?
                AND (biz_content IS NOT NULL AND LENGTH(biz_content) >= ?)
          )
        ORDER BY h.corp_name COLLATE NOCASE
    """, (MIN_BIZ_CONTENT, db_type, MIN_BIZ_CONTENT)).fetchall()
    return [dict(r) for r in rows]


# ── DART API ──────────────────────────────────────────────────────────────────
_rate_lock = threading.Lock()
_last_call  = 0.0


def rate_limit():
    global _last_call
    with _rate_lock:
        elapsed = time.time() - _last_call
        if elapsed < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - elapsed)
        _last_call = time.time()


def dart_get(url: str) -> bytes:
    rate_limit()
    req = urllib.request.Request(url, headers={"User-Agent": "FinanceScope/1.0"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return resp.read()


def fetch_corp_filing(api_key: str, corp_code: str, corp_name: str,
                      type_key: str) -> dict | None:
    """
    특정 기업의 특정 유형 보고서 접수정보 반환
    Returns: {rcept_no, report_nm, rcept_dt, corp_code, corp_name} or None
    """
    cfg = REPORT_CONFIG[type_key]
    url = (
        f"{DART_BASE_URL}/list.json"
        f"?crtfc_key={api_key}"
        f"&corp_code={corp_code}"
        f"&pblntf_ty=A"
        f"&bgn_de={cfg['bgn_de']}&end_de={cfg['end_de']}"
        f"&page_count=20"
    )
    try:
        data = json.loads(dart_get(url).decode("utf-8"))
        if data.get("status") not in ("000", None):
            return None
        items = data.get("list", [])
        for item in items:
            nm = item.get("report_nm", "")
            # 포함 조건: ALL include 키워드 중 하나라도 있어야
            if not any(kw in nm for kw in cfg["nm_include"]):
                continue
            # 제외 조건: 하나라도 있으면 스킵
            if any(kw in nm for kw in cfg["nm_exclude"]):
                continue
            return {
                "rcept_no":  item.get("rcept_no", ""),
                "report_nm": nm,
                "rcept_dt":  item.get("rcept_dt", ""),
                "corp_code": corp_code,
                "corp_name": corp_name,
            }
    except Exception as e:
        log.debug(f"[API 조회 실패] {corp_name} ({corp_code}) {type_key}: {e}")
    return None


def fetch_zip(api_key: str, rcept_no: str) -> bytes | None:
    url = f"{DART_BASE_URL}/document.xml?crtfc_key={api_key}&rcept_no={rcept_no}"
    try:
        data = dart_get(url)
        if data[:2] == b'PK':
            return data
    except Exception as e:
        log.debug(f"ZIP 다운로드 실패 {rcept_no}: {e}")
    return None


# ── 사업의 내용 추출 ───────────────────────────────────────────────────────────
def strip_html(html: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'&nbsp;|&amp;|&lt;|&gt;|&quot;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\r\n|\r', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_biz_content(zip_data: bytes) -> str | None:
    all_texts = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            for name in sorted(zf.namelist()):
                if not name.lower().endswith((".xml", ".html", ".htm")):
                    continue
                try:
                    raw = zf.read(name)
                    for enc in ("utf-8", "euc-kr", "cp949"):
                        try:
                            text = raw.decode(enc)
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        text = raw.decode("utf-8", errors="ignore")
                    clean = strip_html(text)
                    if len(clean) > 100:
                        all_texts.append(clean)
                except Exception:
                    pass
    except zipfile.BadZipFile:
        return None

    if not all_texts:
        return None

    full_text = "\n\n".join(all_texts)

    start_idx = -1
    for pat in BIZ_START_PATTERNS:
        matches = list(re.finditer(pat, full_text))
        if matches:
            start_idx = matches[1].start() if len(matches) >= 2 else matches[0].start()
            break

    if start_idx == -1:
        return None

    search_text = full_text[start_idx + 10:]
    end_idx = len(search_text)
    for pat in BIZ_END_PATTERNS:
        m = re.search(pat, search_text)
        if m and m.start() > MIN_BIZ_CONTENT and m.start() < end_idx:
            end_idx = m.start()

    biz_text = full_text[start_idx: start_idx + 10 + end_idx].strip()
    if len(biz_text) < MIN_BIZ_CONTENT:
        return None

    return biz_text[:MAX_BIZ_CONTENT] + ("\n[이하 생략]" if len(biz_text) > MAX_BIZ_CONTENT else "")


# ── 체크포인트 ────────────────────────────────────────────────────────────────
def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_checkpoint(data: dict):
    CHECKPOINT_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── 핵심 수집 함수 ────────────────────────────────────────────────────────────
def backfill_type(db: sqlite3.Connection, api_key: str, type_key: str,
                  limit: int = 0, checkpoint: dict = None,
                  workers: int = DEFAULT_WORKERS) -> dict:
    """
    특정 보고서 유형의 누락 기업을 보충 수집
    """
    cfg    = REPORT_CONFIG[type_key]
    ck_key = f"backfill_{type_key}"
    if checkpoint is None:
        checkpoint = {}

    done_codes = set(checkpoint.get(ck_key, {}).get("done", []))

    # 누락 기업 목록
    missing = get_missing_corps(db, type_key)
    if not missing:
        log.info(f"[{type_key}] 누락 기업 없음 — 완료")
        return {"total": 0, "new": 0, "no_filing": 0, "failed": 0, "skipped": 0}

    # 이미 처리된 기업 제외
    pending = [c for c in missing if c["corp_code"] not in done_codes]
    skipped = len(missing) - len(pending)

    if limit:
        pending = pending[:limit]

    log.info(f"\n[{type_key}] {cfg['label']}")
    log.info(f"  누락 기업: {len(missing)}개 | 처리 대상: {len(pending)}개 | 이전완료: {skipped}개")

    stats = {"total": len(missing), "new": 0, "no_filing": 0, "failed": 0, "skipped": skipped}
    db_lock = threading.Lock()
    completed = [0]

    def process_one(corp: dict) -> dict:
        """기업 1개 처리 — API 조회 + ZIP 다운로드 + 추출"""
        corp_code = corp["corp_code"]
        corp_name = corp["corp_name"]

        filing = fetch_corp_filing(api_key, corp_code, corp_name, type_key)
        biz_content = None

        if filing and filing.get("rcept_no"):
            zip_data = fetch_zip(api_key, filing["rcept_no"])
            if zip_data:
                biz_content = extract_biz_content(zip_data)

        return {
            "corp_code":   corp_code,
            "corp_name":   corp_name,
            "filing":      filing,
            "biz_content": biz_content,
        }

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_one, c): c for c in pending}

        for future in as_completed(futures):
            try:
                res = future.result()
            except Exception as e:
                stats["failed"] += 1
                log.warning(f"  워커 예외: {e}")
                continue

            corp_code   = res["corp_code"]
            corp_name   = res["corp_name"]
            filing      = res.get("filing")
            biz_content = res.get("biz_content")
            now         = datetime.now().isoformat()

            completed[0] += 1
            done_codes.add(corp_code)

            if not filing:
                stats["no_filing"] += 1
                log.debug(f"  - 미공시: {corp_name}")
            elif biz_content:
                stats["new"] += 1
                log.info(f"  ✓ [{completed[0]}/{len(pending)}] {corp_name} ({len(biz_content):,}자)")
            else:
                stats["failed"] += 1
                log.warning(f"  ✗ [{completed[0]}/{len(pending)}] {corp_name} — 섹션 추출 실패")

            # DB 저장
            if filing and biz_content:
                rcept_no = filing["rcept_no"]
                with db_lock:
                    existing = db.execute(
                        "SELECT id FROM reports WHERE rcept_no=?", (rcept_no,)
                    ).fetchone()
                    if existing:
                        db.execute(
                            "UPDATE reports SET biz_content=?, updated_at=? WHERE rcept_no=?",
                            (biz_content, now, rcept_no)
                        )
                    else:
                        db.execute(
                            """INSERT INTO reports
                               (corp_code, corp_name, report_type, report_name, rcept_no, rcept_dt, biz_content, updated_at)
                               VALUES (?,?,?,?,?,?,?,?)""",
                            (corp_code, corp_name, cfg["db_type"],
                             filing.get("report_nm", ""), rcept_no,
                             filing.get("rcept_dt", ""), biz_content, now)
                        )

            # 50건마다 commit + 체크포인트
            if completed[0] % 50 == 0:
                with db_lock:
                    db.commit()
                checkpoint[ck_key] = {"done": list(done_codes), "updated": now}
                save_checkpoint(checkpoint)
                log.info(
                    f"  → 진행 {completed[0]}/{len(pending)} | "
                    f"수집:{stats['new']} 미공시:{stats['no_filing']} 실패:{stats['failed']}"
                )

    with db_lock:
        db.commit()
    checkpoint[ck_key] = {
        "done": list(done_codes),
        "completed": datetime.now().isoformat(),
        "total": len(missing),
    }
    save_checkpoint(checkpoint)
    return stats


# ── 현황 통계 ─────────────────────────────────────────────────────────────────
def print_status(db: sqlite3.Connection):
    print(f"\n{'='*65}")
    print("  보충 수집 현황")
    print(f"{'='*65}")

    # 전체 h1 보유 기업
    total_h1 = db.execute(
        "SELECT COUNT(DISTINCT corp_code) FROM reports WHERE report_type='2025_h1' "
        "AND biz_content IS NOT NULL AND LENGTH(biz_content)>=?", (MIN_BIZ_CONTENT,)
    ).fetchone()[0]
    print(f"\n  h1(반기) 보유 전체: {total_h1:,}개 기업")

    for type_key, cfg in REPORT_CONFIG.items():
        db_type = cfg["db_type"]
        # 전체 수집 기업
        total = db.execute(
            "SELECT COUNT(DISTINCT corp_code) FROM reports WHERE report_type=? "
            "AND biz_content IS NOT NULL AND LENGTH(biz_content)>=?",
            (db_type, MIN_BIZ_CONTENT)
        ).fetchone()[0]
        # 누락 기업
        missing_cnt = db.execute(f"""
            SELECT COUNT(DISTINCT corp_code) FROM reports
            WHERE report_type='2025_h1'
              AND biz_content IS NOT NULL AND LENGTH(biz_content)>={MIN_BIZ_CONTENT}
              AND corp_code NOT IN (
                  SELECT DISTINCT corp_code FROM reports
                  WHERE report_type=? AND biz_content IS NOT NULL AND LENGTH(biz_content)>={MIN_BIZ_CONTENT}
              )
        """, (db_type,)).fetchone()[0]

        print(f"\n  [{type_key}] {cfg['label']}")
        print(f"    수집 완료: {total:,}개 기업")
        print(f"    아직 누락: {missing_cnt:,}개 기업")

    print(f"\n{'='*65}\n")


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="누락 보고서 보충 수집")
    parser.add_argument("--types", nargs="+", choices=["annual", "q3", "q1"],
                        default=["annual", "q3", "q1"],
                        help="수집할 유형 (기본: 전체)")
    parser.add_argument("--limit", type=int, default=0,
                        help="기업 수 제한 (0=무제한)")
    parser.add_argument("--check", action="store_true",
                        help="현황 확인만")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"병렬 워커 수 (기본: {DEFAULT_WORKERS})")
    parser.add_argument("--reset", action="store_true",
                        help="체크포인트 초기화 후 재시작")
    args = parser.parse_args()

    # API 키
    env_path = ROOT / ".env"
    api_key = ""
    if env_path.exists():
        m = re.search(r'DART_API_KEY\s*=\s*(\S+)', env_path.read_text(encoding="utf-8"))
        if m:
            api_key = m.group(1)
    if not api_key:
        log.error(".env에 DART_API_KEY 없음")
        sys.exit(1)

    (ROOT / "logs").mkdir(exist_ok=True)
    db = get_db()

    if args.check:
        print_status(db)
        db.close()
        return

    if args.reset and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
        log.info("체크포인트 초기화")

    checkpoint = load_checkpoint()
    start_time = time.time()
    all_stats  = {}

    log.info("=" * 65)
    log.info("  누락 보고서 보충 수집 시작")
    log.info("=" * 65)
    print_status(db)

    for type_key in args.types:
        log.info(f"\n{'─'*50}")
        log.info(f"  [{type_key}] {REPORT_CONFIG[type_key]['label']} 수집 시작")
        log.info(f"{'─'*50}")
        stats = backfill_type(
            db=db, api_key=api_key, type_key=type_key,
            limit=args.limit, checkpoint=checkpoint, workers=args.workers,
        )
        all_stats[type_key] = stats
        log.info(f"\n  [{type_key}] 완료: {stats}")

    elapsed = time.time() - start_time
    print(f"\n{'='*65}")
    print(f"  전체 완료 ({elapsed/60:.1f}분)")
    print(f"{'='*65}")
    for type_key, stats in all_stats.items():
        print(f"\n  [{type_key}] {REPORT_CONFIG[type_key]['label']}")
        for k, v in stats.items():
            print(f"    {k}: {v:,}")

    print()
    print_status(db)
    db.close()


if __name__ == "__main__":
    main()
