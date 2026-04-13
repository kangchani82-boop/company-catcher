"""
scripts/fetch_biz_content.py
─────────────────────────────
DART 보고서에서 "2. 사업의 내용" 섹션만 추출하여 biz_content 컬럼에 저장

대상:
  1) 2025 사업보고서 (annual)  — 재파싱 + 미수집분 신규수집
  2) 2025 분기보고서 Q3         — 재파싱 + 미수집분 신규수집
  3) 2025 반기보고서 H1         — DART 목록 조회 후 신규 수집
  4) 2025 분기보고서 Q1         — DART 목록 조회 후 신규 수집

사용법:
  python scripts/fetch_biz_content.py            # 전체 (annual+q3 재파싱 + h1+q1 신규수집)
  python scripts/fetch_biz_content.py --types annual q3
  python scripts/fetch_biz_content.py --types h1 q1
  python scripts/fetch_biz_content.py --types annual --limit 50
  python scripts/fetch_biz_content.py --check    # 현황 통계만
  python scripts/fetch_biz_content.py --year 2024  # 미래: 이전년도 수집
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

# ── 설정 ─────────────────────────────────────────────────────────────────────
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
CHECKPOINT_PATH = ROOT / "data" / "dart" / "biz_content_checkpoint.json"
DART_BASE_URL = "https://opendart.fss.or.kr/api"
RATE_LIMIT_SEC = 0.15    # DART API 호출 간격 (스레드당)
DEFAULT_WORKERS = 5      # 기본 병렬 워커 수 (DART 제한 고려: 최대 8 권장)
REQUEST_TIMEOUT = 45     # ZIP 다운로드 타임아웃
MAX_BIZ_CONTENT = 80000  # 사업의 내용 최대 저장 길이 (약 80KB)
MIN_BIZ_CONTENT = 300    # 이 이하면 추출 실패로 간주

# ── 리포트 타입 정의 ───────────────────────────────────────────────────────────
REPORT_TYPES = {
    "annual": {
        "db_type": "2025_annual",
        "dart_type": "A",         # 사업보고서
        "year": "2025",
        "month_range": ("202601", "202612"),  # 제출일 기준
        "description": "2025 사업보고서",
    },
    "q3": {
        "db_type": "2025_q3",
        "dart_type": "Q",         # 분기보고서
        "year": "2025",
        "month_range": ("202510", "202511"),  # Q3: 10월~11월 제출
        "description": "2025 3분기 분기보고서",
    },
    "h1": {
        "db_type": "2025_h1",
        "dart_type": "A",         # 정기공시 (반기보고서 포함, pblntf_ty=A 가 올바른 코드)
        "year": "2025",
        "month_range": ("202507", "202509"),  # H1: 7~9월 제출 (조기 제출 포함)
        "description": "2025 상반기 반기보고서",
    },
    "q1": {
        "db_type": "2025_q1",
        "dart_type": "A",         # 정기공시 (1분기 분기보고서 포함)
        "year": "2025",
        "month_range": ("202504", "202507"),  # Q1: 4~7월 제출 (조기 제출 포함)
        "description": "2025 1분기 분기보고서",
    },
}

# ── 사업의 내용 섹션 패턴 ─────────────────────────────────────────────────────
# 시작 패턴: "2. 사업의 내용" 또는 "II. 사업의 내용" 등
BIZ_START_PATTERNS = [
    r'II\s*[.\s]\s*사\s*업\s*의\s*내\s*용',
    r'2\s*[.\s]\s*사\s*업\s*의\s*내\s*용',
    r'제\s*2\s*장\s*사\s*업\s*의\s*내\s*용',
    r'사\s*업\s*의\s*내\s*용\s*(?=\s)',   # fallback
]

# 다음 섹션 패턴 (사업의 내용 끝 지점)
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
)
log = logging.getLogger("fetch_biz")


# ── DB 유틸리티 ───────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    return db


def ensure_biz_column(db: sqlite3.Connection):
    """biz_content 컬럼 존재 확인 및 생성"""
    cols = [r[1] for r in db.execute("PRAGMA table_info(reports)").fetchall()]
    if "biz_content" not in cols:
        db.execute("ALTER TABLE reports ADD COLUMN biz_content TEXT")
        db.commit()
        log.info("biz_content 컬럼 생성 완료")
    else:
        log.info("biz_content 컬럼 이미 존재")


def ensure_report_type_table(db: sqlite3.Connection):
    """report_type별 인덱스 확인"""
    db.execute("CREATE INDEX IF NOT EXISTS idx_reports_type ON reports(report_type)")
    db.commit()


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


# ── DART API 유틸리티 ─────────────────────────────────────────────────────────
_rate_lock = threading.Lock()
_last_call = 0.0


def rate_limit():
    """스레드-세이프 rate limit (전역 슬롯 공유)"""
    global _last_call
    with _rate_lock:
        elapsed = time.time() - _last_call
        if elapsed < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - elapsed)
        _last_call = time.time()


def dart_get(url: str) -> bytes:
    """DART API GET 요청"""
    rate_limit()
    req = urllib.request.Request(url, headers={"User-Agent": "FinanceScope/1.0"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return resp.read()


def fetch_dart_list(api_key: str, pblntf_ty: str, bgn_de: str, end_de: str,
                    page_no: int = 1, page_count: int = 100) -> dict:
    """DART 공시 목록 조회"""
    url = (
        f"{DART_BASE_URL}/list.json"
        f"?crtfc_key={api_key}"
        f"&pblntf_ty={pblntf_ty}"
        f"&bgn_de={bgn_de}&end_de={end_de}"
        f"&page_no={page_no}&page_count={page_count}"
    )
    data = dart_get(url)
    return json.loads(data.decode("utf-8"))


def fetch_document_zip(api_key: str, rcept_no: str) -> bytes | None:
    """DART document.xml ZIP 다운로드"""
    url = f"{DART_BASE_URL}/document.xml?crtfc_key={api_key}&rcept_no={rcept_no}"
    try:
        data = dart_get(url)
        if data[:2] == b'PK':
            return data
        # JSON 에러 응답인 경우
        try:
            err = json.loads(data.decode("utf-8"))
            log.warning(f"ZIP 다운로드 오류 {rcept_no}: {err.get('message', 'unknown')}")
        except Exception:
            pass
        return None
    except Exception as e:
        log.warning(f"ZIP 다운로드 실패 {rcept_no}: {e}")
        return None


# ── 섹션 추출 ─────────────────────────────────────────────────────────────────
def strip_html(html: str) -> str:
    """HTML 태그 제거 + 공백 정리"""
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'&nbsp;|&amp;|&lt;|&gt;|&quot;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    # 줄바꿈 보존 (섹션 구분에 중요)
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'\r', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_biz_content(zip_data: bytes) -> str | None:
    """
    ZIP에서 사업의 내용 섹션 추출
    Returns: 추출된 텍스트 or None (추출 실패)
    """
    all_texts = []

    try:
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            names = sorted(zf.namelist())
            for name in names:
                if not name.lower().endswith((".xml", ".html", ".htm")):
                    continue
                try:
                    raw = zf.read(name)
                    # 인코딩 감지
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

    # 전체 텍스트 합치기
    full_text = "\n\n".join(all_texts)

    # 사업의 내용 시작점 찾기
    start_idx = -1
    start_pattern_matched = ""
    for pat in BIZ_START_PATTERNS:
        m = re.search(pat, full_text)
        if m:
            # 매칭 위치가 충분히 뒤에 있어야 함 (앞부분 목차 아닌 본문)
            # 단, 목차에서도 나올 수 있으므로 최대한 뒤쪽 매칭 사용
            all_matches = list(re.finditer(pat, full_text))
            if len(all_matches) >= 2:
                # 두 번째 이후 매칭 = 본문 (첫 번째는 목차)
                start_idx = all_matches[1].start()
            else:
                start_idx = all_matches[0].start()
            start_pattern_matched = pat
            break

    if start_idx == -1:
        return None

    # 사업의 내용 끝점 찾기 (시작점 이후)
    search_text = full_text[start_idx + 10:]
    end_idx = len(search_text)

    for pat in BIZ_END_PATTERNS:
        m = re.search(pat, search_text)
        if m and m.start() > MIN_BIZ_CONTENT:
            if m.start() < end_idx:
                end_idx = m.start()

    biz_text = full_text[start_idx: start_idx + 10 + end_idx].strip()

    # 너무 짧으면 실패
    if len(biz_text) < MIN_BIZ_CONTENT:
        return None

    # 최대 길이 제한
    if len(biz_text) > MAX_BIZ_CONTENT:
        biz_text = biz_text[:MAX_BIZ_CONTENT] + "\n[이하 생략]"

    return biz_text


def try_extract_from_existing_raw(raw_text: str) -> str | None:
    """
    기존 raw_text에서 사업의 내용 추출 시도
    이미 잘렸을 가능성이 높지만 빠르게 확인용
    """
    if not raw_text:
        return None

    for pat in BIZ_START_PATTERNS:
        all_matches = list(re.finditer(pat, raw_text))
        if not all_matches:
            continue

        # 두 번째 매칭 우선
        start_match = all_matches[1] if len(all_matches) >= 2 else all_matches[0]
        start_idx = start_match.start()
        search_text = raw_text[start_idx + 10:]

        end_idx = len(search_text)
        for end_pat in BIZ_END_PATTERNS:
            m = re.search(end_pat, search_text)
            if m and m.start() > MIN_BIZ_CONTENT:
                if m.start() < end_idx:
                    end_idx = m.start()

        biz_text = raw_text[start_idx: start_idx + 10 + end_idx].strip()
        if len(biz_text) >= MIN_BIZ_CONTENT:
            return biz_text[:MAX_BIZ_CONTENT]

    return None


# ── 통계 출력 ─────────────────────────────────────────────────────────────────
def print_stats(db: sqlite3.Connection):
    print(f"\n{'='*60}")
    print(f"  DART biz_content 현황")
    print(f"{'='*60}")

    for type_key, type_info in REPORT_TYPES.items():
        db_type = type_info["db_type"]
        row = db.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN biz_content IS NOT NULL AND LENGTH(biz_content)>=? THEN 1 ELSE 0 END) as has_biz,
                      AVG(CASE WHEN biz_content IS NOT NULL THEN LENGTH(biz_content) END) as avg_biz,
                      SUM(CASE WHEN raw_text IS NOT NULL AND LENGTH(raw_text)>0 THEN 1 ELSE 0 END) as has_raw
               FROM reports WHERE report_type=?""",
            (MIN_BIZ_CONTENT, db_type)
        ).fetchone()

        total = row["total"] or 0
        has_biz = row["has_biz"] or 0
        avg_biz = row["avg_biz"] or 0
        pct = has_biz / total * 100 if total else 0

        print(f"\n  [{type_key}] {type_info['description']}")
        print(f"    DB 보유:    {total:,}건")
        print(f"    biz_content 추출완료: {has_biz:,}건 ({pct:.1f}%)")
        print(f"    평균 길이:  {avg_biz:,.0f}자")

    print(f"\n{'='*60}\n")


# ── 기존 보고서 재파싱 (annual + q3) ─────────────────────────────────────────
def _process_one_report(row_data: tuple, api_key: str) -> dict:
    """
    단일 보고서 처리 (병렬 워커용)
    Returns: {id, rcept_no, biz_content, source, error}
    """
    row_id, corp_name, rcept_no, raw_text = row_data
    result = {"id": row_id, "rcept_no": rcept_no, "corp_name": corp_name,
              "biz_content": None, "source": None, "error": None}

    # Step 1: raw_text에서 추출 시도 (잘리지 않은 경우)
    raw_text = raw_text or ""
    if len(raw_text) < 19500:
        biz_content = try_extract_from_existing_raw(raw_text)
        if biz_content:
            result["biz_content"] = biz_content
            result["source"] = "raw"
            return result

    # Step 2: ZIP 재다운로드
    zip_data = fetch_document_zip(api_key, rcept_no)
    if zip_data:
        biz_content = extract_biz_content(zip_data)
        if biz_content:
            result["biz_content"] = biz_content
            result["source"] = "zip"
        else:
            result["error"] = "섹션 없음"
    else:
        result["error"] = "ZIP 다운로드 실패"

    return result


def reparse_existing(db: sqlite3.Connection, api_key: str, type_key: str,
                     limit: int = 0, checkpoint: dict = None,
                     force: bool = False, workers: int = DEFAULT_WORKERS) -> dict:
    """
    기존 DB 보고서를 병렬로 재파싱:
    1) raw_text에서 biz_content 추출 시도
    2) 실패 시 ZIP 재다운로드해서 추출
    """
    type_info = REPORT_TYPES[type_key]
    db_type = type_info["db_type"]
    ck_key = f"reparse_{type_key}"

    if checkpoint is None:
        checkpoint = {}

    done_ids = set(checkpoint.get(ck_key, {}).get("done", []))

    # 처리 대상: biz_content 없거나 짧은 것
    if force:
        where = "WHERE report_type=?"
        params = (db_type,)
    else:
        where = "WHERE report_type=? AND (biz_content IS NULL OR LENGTH(biz_content)<?)"
        params = (db_type, MIN_BIZ_CONTENT)

    rows = db.execute(
        f"SELECT id, corp_name, rcept_no, raw_text FROM reports {where} ORDER BY rcept_dt",
        params
    ).fetchall()

    if limit:
        rows = rows[:limit]

    total = len(rows)
    log.info(f"\n[{type_key}] 재파싱 대상: {total}건 (워커: {workers}개)")

    stats = {"total": total, "from_raw": 0, "from_zip": 0, "failed": 0, "skipped": 0}

    # 미처리 행만 필터
    pending = [(r["id"], r["corp_name"], r["rcept_no"], r["raw_text"])
               for r in rows if str(r["id"]) not in done_ids]
    stats["skipped"] = total - len(pending)

    db_lock = threading.Lock()
    completed_count = [0]

    def write_result(res: dict):
        """DB 쓰기 (락 보호)"""
        with db_lock:
            db.execute(
                "UPDATE reports SET biz_content=?, updated_at=? WHERE id=?",
                (res["biz_content"], datetime.now().isoformat(), res["id"])
            )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_one_report, row_data, api_key): row_data
            for row_data in pending
        }

        for future in as_completed(futures):
            try:
                res = future.result()
            except Exception as e:
                row_data = futures[future]
                log.warning(f"워커 예외 {row_data[2]}: {e}")
                stats["failed"] += 1
                continue

            if res["source"] == "raw":
                stats["from_raw"] += 1
                log.debug(f"  raw: {res['corp_name']} ({len(res['biz_content']):,}자)")
            elif res["source"] == "zip":
                stats["from_zip"] += 1
                log.info(f"  zip: {res['corp_name']} ({len(res['biz_content']):,}자)")
            else:
                stats["failed"] += 1
                log.warning(f"  fail: {res['corp_name']} — {res['error']}")

            write_result(res)
            done_ids.add(str(res["id"]))
            completed_count[0] += 1

            # 주기적 commit + 체크포인트
            if completed_count[0] % 50 == 0:
                with db_lock:
                    db.commit()
                checkpoint[ck_key] = {"done": list(done_ids), "updated": datetime.now().isoformat()}
                save_checkpoint(checkpoint)
                log.info(f"  → 진행: {completed_count[0]}/{len(pending)} | raw:{stats['from_raw']} zip:{stats['from_zip']} fail:{stats['failed']}")

    with db_lock:
        db.commit()
    checkpoint[ck_key] = {"done": list(done_ids), "completed": datetime.now().isoformat()}
    save_checkpoint(checkpoint)
    return stats


# ── 회사별 Q1 수집 (per-company DART API) ────────────────────────────────────
def fetch_company_q1_filing(api_key: str, corp_code: str, corp_name: str,
                             bgn_de: str = "20250401", end_de: str = "20250630") -> dict | None:
    """
    특정 회사의 Q1 분기보고서 조회 (per-company DART API)
    Returns: {rcept_no, report_nm, rcept_dt, corp_code, corp_name} or None
    """
    url = (
        f"{DART_BASE_URL}/list.json"
        f"?crtfc_key={api_key}"
        f"&corp_code={corp_code}"
        f"&pblntf_ty=A"
        f"&bgn_de={bgn_de}&end_de={end_de}"
        f"&page_count=20"
    )
    try:
        rate_limit()
        req = urllib.request.Request(url, headers={"User-Agent": "FinanceScope/1.0"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        items = data.get("list", [])
        # Q1 분기보고서 = 분기보고서 AND NOT 반기 AND NOT 3분기
        for item in items:
            nm = item.get("report_nm", "")
            if "분기보고서" in nm and "반기" not in nm and "3분기" not in nm:
                return {
                    "rcept_no": item.get("rcept_no", ""),
                    "report_nm": nm,
                    "rcept_dt": item.get("rcept_dt", ""),
                    "corp_code": corp_code,
                    "corp_nm": corp_name,
                }
    except Exception as e:
        log.warning(f"[Q1 조회 실패] {corp_name} ({corp_code}): {e}")
    return None


def collect_q1_by_company(db: sqlite3.Connection, api_key: str,
                           limit: int = 0, checkpoint: dict = None,
                           workers: int = DEFAULT_WORKERS) -> dict:
    """
    회사별 Q1 2025 분기보고서 수집 (date-range list API 실패 대안)
    - 기존 DB의 corp_code 목록 활용
    - per-company DART API로 개별 조회 → ZIP 다운로드 → biz_content 추출
    """
    db_type = "2025_q1"
    ck_key = "collect_q1_company"
    if checkpoint is None:
        checkpoint = {}

    done_codes = set(checkpoint.get(ck_key, {}).get("done", []))

    # 기존 DB에서 corp_code 목록 추출 (annual + q3)
    rows = db.execute(
        """SELECT DISTINCT corp_code, corp_name FROM reports
           WHERE report_type IN ('2025_annual','2025_q3')
           AND corp_code IS NOT NULL AND corp_code != ''
           ORDER BY corp_code"""
    ).fetchall()

    if not rows:
        log.warning("[Q1] 기준 corp_code 없음 — annual/q3 데이터 필요")
        return {"total": 0, "new": 0, "updated": 0, "failed": 0, "skipped": 0}

    # 이미 완료된 corp_code 필터
    pending = [(r["corp_code"], r["corp_name"]) for r in rows
               if r["corp_code"] not in done_codes]
    skipped = len(rows) - len(pending)

    if limit:
        pending = pending[:limit]

    total = len(rows)
    log.info(f"\n[Q1] 기준 기업: {total}개, 처리 대상: {len(pending)}개 (워커: {workers}개)")

    stats = {"total": total, "new": 0, "updated": 0, "failed": 0,
             "no_filing": 0, "skipped": skipped}
    db_lock = threading.Lock()
    completed = [0]

    def process_company(corp_info: tuple) -> dict:
        corp_code, corp_name = corp_info
        filing = fetch_company_q1_filing(api_key, corp_code, corp_name)
        biz_content = None
        if filing and filing.get("rcept_no"):
            zip_data = fetch_document_zip(api_key, filing["rcept_no"])
            if zip_data:
                biz_content = extract_biz_content(zip_data)
        return {
            "corp_code": corp_code,
            "corp_name": corp_name,
            "filing": filing,
            "biz_content": biz_content,
        }

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_company, c): c for c in pending}

        for future in as_completed(futures):
            try:
                res = future.result()
            except Exception as e:
                stats["failed"] += 1
                log.warning(f"  워커 예외: {e}")
                continue

            corp_code = res["corp_code"]
            corp_name = res["corp_name"]
            filing = res.get("filing")
            biz_content = res.get("biz_content")

            completed[0] += 1
            done_codes.add(corp_code)

            if not filing:
                stats["no_filing"] += 1
                continue

            rcept_no = filing["rcept_no"]
            now = datetime.now().isoformat()

            if biz_content:
                log.info(f"  ✓ {corp_name} ({len(biz_content):,}자)")
            else:
                stats["failed"] += 1
                log.warning(f"  ✗ {corp_name} — 섹션 없음")

            with db_lock:
                existing = db.execute(
                    "SELECT id FROM reports WHERE rcept_no=?", (rcept_no,)
                ).fetchone()
                if existing:
                    db.execute(
                        "UPDATE reports SET biz_content=?, updated_at=? WHERE rcept_no=?",
                        (biz_content, now, rcept_no)
                    )
                    stats["updated"] += 1
                else:
                    db.execute(
                        """INSERT INTO reports
                           (corp_code, corp_name, report_type, report_name, rcept_no, rcept_dt, biz_content, updated_at)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (corp_code, corp_name, db_type,
                         filing.get("report_nm", "분기보고서"),
                         rcept_no, filing.get("rcept_dt", ""),
                         biz_content, now)
                    )
                    stats["new"] += 1

            if completed[0] % 100 == 0:
                with db_lock:
                    db.commit()
                checkpoint[ck_key] = {"done": list(done_codes), "updated": now}
                save_checkpoint(checkpoint)
                log.info(f"  → 진행: {completed[0]}/{len(pending)} | "
                         f"new:{stats['new']} no_filing:{stats['no_filing']} fail:{stats['failed']}")

    with db_lock:
        db.commit()
    checkpoint[ck_key] = {
        "done": list(done_codes),
        "completed": datetime.now().isoformat(),
        "total": total,
    }
    save_checkpoint(checkpoint)
    return stats


# ── 신규 보고서 수집 (h1 + q1) ───────────────────────────────────────────────
def collect_new_reports(db: sqlite3.Connection, api_key: str, type_key: str,
                        limit: int = 0, checkpoint: dict = None,
                        workers: int = DEFAULT_WORKERS) -> dict:
    """
    DART 목록에서 보고서 수집 → biz_content 추출 (병렬)
    H1(반기) + Q1(1분기)
    """
    type_info = REPORT_TYPES[type_key]
    dart_type = type_info["dart_type"]
    month_range = type_info["month_range"]
    db_type = type_info["db_type"]
    ck_key = f"collect_{type_key}"

    if checkpoint is None:
        checkpoint = {}

    done_rcepts = set(checkpoint.get(ck_key, {}).get("done", []))

    # DART 목록 전체 조회 (순차 - API rate limit)
    log.info(f"\n[{type_key}] DART 목록 조회 중 ({month_range[0]}~{month_range[1]})...")
    all_items = []
    page_no = 1
    while True:
        try:
            result = fetch_dart_list(
                api_key=api_key,
                pblntf_ty=dart_type,
                bgn_de=month_range[0] + "01",
                end_de=month_range[1] + "30",
                page_no=page_no,
                page_count=100
            )
        except Exception as e:
            log.error(f"목록 조회 오류 (page {page_no}): {e}")
            break

        if result.get("status") != "000":
            log.warning(f"목록 조회 상태 오류: {result.get('message')}")
            break

        raw_items = result.get("list", [])
        if not raw_items:
            break

        # DART API 전체 페이지 수 체크 (total_page 필드 활용)
        total_page = int(result.get("total_page", 0))
        total_count = int(result.get("total_count", 0))

        # 보고서 종류 필터링 (pblntf_ty=A = 정기공시 전체이므로 report_nm으로 구분)
        if type_key == "h1":
            # 반기보고서 (사업보고서·분기보고서 제외)
            items = [i for i in raw_items if "반기보고서" in i.get("report_nm", "")
                     and "사업보고서" not in i.get("report_nm", "")]
        elif type_key == "q1":
            items = [i for i in raw_items
                     if "분기보고서" in i.get("report_nm", "")
                     and "반기" not in i.get("report_nm", "")
                     and "3분기" not in i.get("report_nm", "")]
        else:
            items = raw_items

        all_items.extend(items)
        log.info(f"  페이지 {page_no}/{total_page if total_page else '?'}: {len(items)}건 "
                 f"(누적 {len(all_items)}/{total_count})")

        # total_page 기준으로 종료 (정확한 방법)
        if total_page and page_no >= total_page:
            break
        # fallback: 누적이 total_count 이상이거나 raw 페이지가 빈 경우
        if not raw_items or (total_count and len(all_items) >= total_count):
            break
        page_no += 1

    if limit:
        all_items = all_items[:limit]

    # 이미 완료된 것, DB에 있는 것 필터
    existing_rcepts = set(
        r[0] for r in db.execute(
            "SELECT rcept_no FROM reports WHERE report_type=? AND biz_content IS NOT NULL AND LENGTH(biz_content)>=?",
            (db_type, MIN_BIZ_CONTENT)
        ).fetchall()
    )
    pending_items = [it for it in all_items
                     if it.get("rcept_no") not in done_rcepts
                     and it.get("rcept_no") not in existing_rcepts]

    total = len(all_items)
    log.info(f"[{type_key}] 전체: {total}건, 처리 대상: {len(pending_items)}건 (워커: {workers}개)")

    stats = {"total": total, "new": 0, "updated": 0, "failed": 0, "skipped": total - len(pending_items)}

    def fetch_one(item: dict) -> dict:
        """단일 보고서 다운로드+추출"""
        rcept_no = item.get("rcept_no", "")
        zip_data = fetch_document_zip(api_key, rcept_no)
        biz_content = None
        if zip_data:
            biz_content = extract_biz_content(zip_data)
        return {
            "item": item,
            "biz_content": biz_content,
            "error": None if (zip_data and biz_content) else (
                "섹션 없음" if zip_data else "ZIP 실패"
            )
        }

    db_lock = threading.Lock()
    completed_count = [0]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, it): it for it in pending_items}

        for future in as_completed(futures):
            try:
                res = future.result()
            except Exception as e:
                stats["failed"] += 1
                log.warning(f"워커 예외: {e}")
                continue

            item = res["item"]
            rcept_no = item.get("rcept_no", "")
            corp_code = item.get("corp_code", "")
            corp_name = item.get("corp_nm", "")
            report_name = item.get("report_nm", "")
            rcept_dt = item.get("rcept_dt", "")
            biz_content = res["biz_content"]

            if biz_content:
                log.info(f"  ✓ {corp_name} ({len(biz_content):,}자)")
            else:
                stats["failed"] += 1
                log.warning(f"  ✗ {corp_name} — {res['error']}")

            now = datetime.now().isoformat()
            with db_lock:
                existing = db.execute(
                    "SELECT id FROM reports WHERE rcept_no=?", (rcept_no,)
                ).fetchone()
                if existing:
                    db.execute(
                        "UPDATE reports SET biz_content=?, report_name=?, updated_at=? WHERE rcept_no=?",
                        (biz_content, report_name, now, rcept_no)
                    )
                    stats["updated"] += 1
                else:
                    db.execute(
                        """INSERT INTO reports
                           (corp_code, corp_name, report_type, report_name, rcept_no, rcept_dt, biz_content, updated_at)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (corp_code, corp_name, db_type, report_name, rcept_no, rcept_dt, biz_content, now)
                    )
                    stats["new"] += 1

            done_rcepts.add(rcept_no)
            completed_count[0] += 1

            if completed_count[0] % 50 == 0:
                with db_lock:
                    db.commit()
                checkpoint[ck_key] = {"done": list(done_rcepts), "updated": now}
                save_checkpoint(checkpoint)
                log.info(f"  → 진행: {completed_count[0]}/{len(pending_items)} | new:{stats['new']} fail:{stats['failed']}")

    with db_lock:
        db.commit()
    checkpoint[ck_key] = {
        "done": list(done_rcepts),
        "completed": datetime.now().isoformat(),
        "total": total,
    }
    save_checkpoint(checkpoint)
    return stats


# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="DART 사업의 내용 섹션 수집")
    parser.add_argument(
        "--types", nargs="+", choices=["annual", "q3", "h1", "q1"],
        default=["annual", "q3", "h1", "q1"],
        help="처리할 보고서 종류 (기본: 전체)"
    )
    parser.add_argument("--limit", type=int, default=0, help="건수 제한 (0=무제한)")
    parser.add_argument("--check", action="store_true", help="현황 통계만 출력")
    parser.add_argument("--force", action="store_true", help="이미 추출된 것도 재처리")
    parser.add_argument("--reset-checkpoint", action="store_true", help="체크포인트 초기화")
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"병렬 워커 수 (기본: {DEFAULT_WORKERS}, 최대 권장: 8)"
    )
    parser.add_argument(
        "--year", type=int, default=2025,
        help="수집 연도 (기본: 2025) — 미래 확장용"
    )
    args = parser.parse_args()

    # API 키
    from pathlib import Path as _Path
    _env = _Path(".env").read_text(encoding="utf-8")
    _m = re.search(r'DART_API_KEY\s*=\s*(\S+)', _env)
    if not _m:
        log.error(".env에 DART_API_KEY 없음")
        sys.exit(1)
    api_key = _m.group(1)

    db = get_db()
    ensure_biz_column(db)
    ensure_report_type_table(db)

    if args.check:
        print_stats(db)
        db.close()
        return

    if args.reset_checkpoint:
        if CHECKPOINT_PATH.exists():
            CHECKPOINT_PATH.unlink()
        log.info("체크포인트 초기화 완료")

    checkpoint = load_checkpoint()

    all_stats = {}
    start_time = time.time()

    for type_key in args.types:
        type_info = REPORT_TYPES[type_key]
        log.info(f"\n{'─'*50}")
        log.info(f"  처리 시작: [{type_key}] {type_info['description']}")
        log.info(f"{'─'*50}")

        if type_key in ("annual", "q3"):
            stats = reparse_existing(
                db=db, api_key=api_key, type_key=type_key,
                limit=args.limit, checkpoint=checkpoint, force=args.force,
                workers=args.workers
            )
        elif type_key == "q1":
            # Q1은 per-company 방식으로 수집 (date-range 방식 DART API 미지원)
            stats = collect_q1_by_company(
                db=db, api_key=api_key,
                limit=args.limit, checkpoint=checkpoint, workers=args.workers
            )
        else:
            stats = collect_new_reports(
                db=db, api_key=api_key, type_key=type_key,
                limit=args.limit, checkpoint=checkpoint, workers=args.workers
            )

        all_stats[type_key] = stats
        log.info(f"\n[{type_key}] 완료: {stats}")

    elapsed = time.time() - start_time

    # 최종 통계
    print(f"\n{'='*60}")
    print(f"  작업 완료 ({elapsed/60:.1f}분)")
    print(f"{'='*60}")
    for type_key, stats in all_stats.items():
        print(f"\n  [{type_key}] {REPORT_TYPES[type_key]['description']}")
        for k, v in stats.items():
            print(f"    {k}: {v:,}")

    print()
    print_stats(db)
    db.close()


if __name__ == "__main__":
    main()
