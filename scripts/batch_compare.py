"""
scripts/batch_compare.py
────────────────────────
전체 기업 대상 Gemini AI 비교 분석 배치 실행기
biz_content(2. 사업의 내용)만 사용하여 두 기간 보고서를 비교

실행 예시:
  python scripts/batch_compare.py                          # 2025_annual vs 2025_q1, 1000개
  python scripts/batch_compare.py --limit 500              # 500개만
  python scripts/batch_compare.py --type-a 2025_annual --type-b 2025_h1
  python scripts/batch_compare.py --model flash            # flash | flash-lite | pro
  python scripts/batch_compare.py --all                    # 기존 결과 덮어쓰기
  python scripts/batch_compare.py --resume 1000            # 1000번째부터 재개
  python scripts/batch_compare.py --workers 2              # 병렬 2워커 (키 분리)

결과 저장:
  data/dart/dart_reports.db → ai_comparisons 테이블

무료 API 일일 한도 (Gemini 무료):
  Gemini 2.5 Flash      : 10 RPM / 500 RPD  → 딜레이 6초
  Gemini 2.5 Flash-Lite : 15 RPM / 1,000 RPD → 딜레이 4초  ← Flash-Lite 최고 RPD (2키 합산 2,000)
  Gemini 2.5 Pro        :  5 RPM / 100 RPD
  Gemini 3.1 Pro Preview: 10 RPM / 100 RPD  (기사 초안 전용)

전략:
  - flash 모드: Flash 우선 → 429 시 Flash-Lite 자동 전환 (합산 1,500 RPD)
  - flash-lite 모드: Flash-Lite 전용 (키당 1,000 RPD / 2키 합산 2,000 RPD)
  - 권장: --workers 2 --model flash-lite (약 100분 소요, 합산 2,000 RPD)
"""

import io
import json
import os
import re
import sqlite3
import sys
import time
import argparse
import threading
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# ── 환경변수 로드 ────────────────────────────────────────────────────────────
if ENV_PATH.exists():
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ── Gemini 모델 설정 ────────────────────────────────────────────────────────
# 무료 API 한도 (2026 기준)
GEMINI_LIMITS = {
    "flash":      {"rpm": 10, "rpd": 500,   "delay": 6.5},
    "flash-lite": {"rpm": 15, "rpd": 1000,  "delay": 4.5},
    "pro":        {"rpm":  5, "rpd": 100,   "delay": 12.5},
    "article":    {"rpm": 10, "rpd": 100,   "delay": 6.5},  # 3.1 Pro Preview (기사 초안)
}

# fallback 순서 (404 시 자동 시도)
GEMINI_FALLBACKS = {
    "flash":      ["gemini-2.5-flash-preview-05-20",
                   "gemini-2.5-flash",
                   "gemini-2.0-flash"],
    "flash-lite": ["gemini-2.5-flash-lite-preview-06-17",
                   "gemini-2.5-flash-lite",
                   "gemini-2.5-flash-8b",
                   "gemini-2.0-flash-lite"],
    "pro":        ["gemini-2.5-pro-preview-05-06",
                   "gemini-2.5-pro",
                   "gemini-2.0-pro-exp"],
    "article":    ["gemini-3.1-pro-preview",
                   "gemini-2.5-pro-preview-05-06",
                   "gemini-2.5-pro"],
}

# flash → quota 초과 시 자동 전환할 모델
FALLBACK_ON_QUOTA = {
    "flash": "flash-lite",
}

# ── 보고서 유형 표시명 ────────────────────────────────────────────────────────
TYPE_LABELS = {
    "2025_annual": "2025 사업보고서",
    "2025_q3":     "2025 3분기보고서",
    "2025_h1":     "2025 반기보고서",
    "2025_q1":     "2025 1분기보고서",
}


# ─── 스레드 안전 락 ──────────────────────────────────────────────────────────
_db_lock      = threading.Lock()   # DB 쓰기 락
_print_lock   = threading.Lock()   # print 락
_counter_lock = threading.Lock()   # 진행 카운터 락

# 워커별 마지막 호출 시각 (per-key rate limit)
_last_call_time: dict[int, float] = {}  # key_index → timestamp


def _tprint(*args, **kwargs):
    """스레드 안전 print"""
    with _print_lock:
        print(*args, **kwargs)


# ─── DB ─────────────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def ensure_table(db: sqlite3.Connection):
    """ai_comparisons 테이블 생성 (없으면)"""
    db.execute("""
        CREATE TABLE IF NOT EXISTS ai_comparisons (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            corp_code     TEXT NOT NULL,
            corp_name     TEXT,
            report_id_a   INTEGER,
            report_id_b   INTEGER,
            report_type_a TEXT,
            report_type_b TEXT,
            model         TEXT DEFAULT 'flash',
            result        TEXT,
            char_count_a  INTEGER DEFAULT 0,
            char_count_b  INTEGER DEFAULT 0,
            status        TEXT DEFAULT 'ok',
            error_msg     TEXT,
            analyzed_at   TEXT,
            UNIQUE(corp_code, report_type_a, report_type_b, model)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_ac_corp    ON ai_comparisons(corp_code)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_ac_status  ON ai_comparisons(status)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_ac_types   ON ai_comparisons(report_type_a, report_type_b)")
    db.commit()


def already_done(db: sqlite3.Connection, corp_code: str,
                 type_a: str, type_b: str, model: str) -> bool:
    row = db.execute("""
        SELECT 1 FROM ai_comparisons
        WHERE corp_code=? AND report_type_a=? AND report_type_b=? AND model=?
          AND status='ok'
        LIMIT 1
    """, [corp_code, type_a, type_b, model]).fetchone()
    return bool(row)


def _db_execute_with_retry(db: sqlite3.Connection, sql: str, params: list, retries: int = 5):
    """DB 잠금 시 최대 retries 회 재시도 (busy_timeout과 이중 보호)"""
    for attempt in range(retries):
        try:
            db.execute(sql, params)
            db.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < retries - 1:
                wait = 2 ** attempt  # 1, 2, 4, 8, 16초
                _tprint(f"  ⚠ DB 잠금 — {wait}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(wait)
            else:
                raise


def save_result(db: sqlite3.Connection, corp_code: str, corp_name: str,
                rid_a: int, rid_b: int, type_a: str, type_b: str,
                model: str, result: str, char_a: int, char_b: int):
    _db_execute_with_retry(db, """
        INSERT INTO ai_comparisons
          (corp_code, corp_name, report_id_a, report_id_b,
           report_type_a, report_type_b, model, result,
           char_count_a, char_count_b, status, analyzed_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,'ok',?)
        ON CONFLICT(corp_code, report_type_a, report_type_b, model)
        DO UPDATE SET
          result=excluded.result, char_count_a=excluded.char_count_a,
          char_count_b=excluded.char_count_b, status='ok',
          error_msg=NULL, analyzed_at=excluded.analyzed_at
    """, [corp_code, corp_name, rid_a, rid_b, type_a, type_b,
          model, result, char_a, char_b,
          datetime.now().strftime("%Y-%m-%d %H:%M:%S")])


def save_error(db: sqlite3.Connection, corp_code: str, corp_name: str,
               rid_a: int, rid_b: int, type_a: str, type_b: str,
               model: str, error_msg: str):
    _db_execute_with_retry(db, """
        INSERT INTO ai_comparisons
          (corp_code, corp_name, report_id_a, report_id_b,
           report_type_a, report_type_b, model, status, error_msg, analyzed_at)
        VALUES (?,?,?,?,?,?,?,'error',?,?)
        ON CONFLICT(corp_code, report_type_a, report_type_b, model)
        DO UPDATE SET status='error', error_msg=excluded.error_msg,
          analyzed_at=excluded.analyzed_at
    """, [corp_code, corp_name, rid_a, rid_b, type_a, type_b, model,
          error_msg[:500], datetime.now().strftime("%Y-%m-%d %H:%M:%S")])


# ─── 듀얼 API 키 관리 ────────────────────────────────────────────────────────
# 파싱1 + 파싱2 라운드로빈으로 합산 RPD 2배 활용
_key_index = 0  # 전역 키 인덱스 (라운드로빈, 단일워커 전용)

def _get_api_keys() -> list:
    """사용 가능한 Gemini API 키 목록 반환 (파싱1, 파싱2)"""
    keys = []
    k1 = os.environ.get("GEMINI_API_KEY", "").strip()
    k2 = os.environ.get("GEMINI_API_KEY_2", "").strip()
    if k1: keys.append(k1)
    if k2: keys.append(k2)
    if not keys:
        raise ValueError("GEMINI_API_KEY 또는 GEMINI_API_KEY_2 가 .env에 없습니다")
    return keys

def _next_api_key(quota_exhausted_key: str | None = None) -> str:
    """라운드로빈으로 다음 API 키 반환. quota_exhausted_key 는 건너뜀. (단일워커용)"""
    global _key_index
    keys = _get_api_keys()
    if len(keys) == 1:
        return keys[0]
    # quota 소진된 키 제외
    available = [k for k in keys if k != quota_exhausted_key]
    if not available:
        available = keys  # 모두 소진 시 그냥 시도
    key = available[_key_index % len(available)]
    _key_index += 1
    return key


# ─── 워커 전용 API 호출 (병렬 모드) ────────────────────────────────────────
def call_gemini_with_key(model_type: str, prompt: str, api_key: str,
                         key_label: str, timeout: int = 120):
    """지정된 API 키로 Gemini 호출. (result_text, used_model_name, used_model_type) 반환."""
    types_to_try = [model_type]
    if model_type in FALLBACK_ON_QUOTA:
        types_to_try.append(FALLBACK_ON_QUOTA[model_type])

    for mtype in types_to_try:
        candidates = GEMINI_FALLBACKS.get(mtype, GEMINI_FALLBACKS["flash"])
        last_err = None

        for model_name in candidates:
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model_name}:generateContent?key={api_key}"
            )
            payload = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 8192, "temperature": 0.3},
            }).encode("utf-8")

            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                return data["candidates"][0]["content"]["parts"][0]["text"], model_name, mtype
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                if e.code == 404:
                    last_err = f"모델 없음: {model_name}"
                    continue
                if e.code == 429:
                    _tprint(f"  ⚠ [{key_label}] 429 — {mtype}→{FALLBACK_ON_QUOTA.get(mtype,'없음')} 전환 시도")
                    last_err = f"할당량 초과({mtype})"
                    break  # 다음 mtype 시도
                raise RuntimeError(f"Gemini API 오류 {e.code}: {body[:300]}")
            except urllib.error.URLError as e:
                raise RuntimeError(f"네트워크 오류: {e.reason}")

    raise RuntimeError(f"사용 가능한 Gemini 모델 없음. 마지막 오류: {last_err}")


# ─── AI 호출 (단일워커/라운드로빈 모드) ────────────────────────────────────
def call_gemini(model_type: str, prompt: str, timeout: int = 120):
    """Gemini API 호출. (result_text, used_model_name, used_model_type) 반환.
    파싱1/파싱2 라운드로빈 + 429 시 다른 키 자동 전환 + fallback 모델 타입 전환."""
    keys = _get_api_keys()
    key_labels = {keys[0]: "파싱1"}
    if len(keys) > 1: key_labels[keys[1]] = "파싱2"

    types_to_try = [model_type]
    if model_type in FALLBACK_ON_QUOTA:
        types_to_try.append(FALLBACK_ON_QUOTA[model_type])

    exhausted_key = None

    for mtype in types_to_try:
        candidates = GEMINI_FALLBACKS.get(mtype, GEMINI_FALLBACKS["flash"])
        last_err = None

        for model_name in candidates:
            # 라운드로빈으로 키 선택 (소진 키 제외)
            api_key = _next_api_key(exhausted_key)
            label   = key_labels.get(api_key, "키")

            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model_name}:generateContent?key={api_key}"
            )
            payload = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 8192, "temperature": 0.3},
            }).encode("utf-8")

            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                return data["candidates"][0]["content"]["parts"][0]["text"], model_name, mtype
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                if e.code == 404:
                    last_err = f"모델 없음: {model_name}"
                    continue
                if e.code == 429:
                    # 현재 키 할당량 초과 → 다른 키로 전환 시도
                    other_keys = [k for k in keys if k != api_key]
                    if other_keys:
                        print(f"  ⚠ {label} 429 → 다른 키로 전환")
                        exhausted_key = api_key
                        # 다른 키로 같은 모델 즉시 재시도
                        alt_key = other_keys[0]
                        alt_label = key_labels.get(alt_key, "키")
                        alt_url = (
                            f"https://generativelanguage.googleapis.com/v1beta/models/"
                            f"{model_name}:generateContent?key={alt_key}"
                        )
                        alt_req = urllib.request.Request(
                            alt_url, data=payload,
                            headers={"Content-Type": "application/json"},
                            method="POST",
                        )
                        try:
                            with urllib.request.urlopen(alt_req, timeout=timeout) as resp2:
                                data2 = json.loads(resp2.read().decode("utf-8"))
                            print(f"    ✓ {alt_label}로 성공")
                            return data2["candidates"][0]["content"]["parts"][0]["text"], model_name, mtype
                        except urllib.error.HTTPError as e2:
                            if e2.code == 429:
                                print(f"  ⚠ {alt_label}도 429 → 모델 타입 전환")
                                last_err = f"두 키 모두 할당량 초과({mtype})"
                                break
                            raise RuntimeError(f"Gemini API 오류 {e2.code} ({alt_label})")
                        except Exception:
                            pass
                    else:
                        print(f"  ⚠ {label} 429 → {FALLBACK_ON_QUOTA.get(mtype,'없음')}으로 전환")
                    last_err = f"할당량 초과({mtype})"
                    break  # inner loop 탈출 → 다음 mtype 시도
                raise RuntimeError(f"Gemini API 오류 {e.code}: {body[:300]}")
            except urllib.error.URLError as e:
                raise RuntimeError(f"네트워크 오류: {e.reason}")

    raise RuntimeError(f"사용 가능한 Gemini 모델 없음. 마지막 오류: {last_err}")


def build_prompt(reports: list) -> str:
    """분석 프롬프트 생성 (biz_content 전용)"""
    sep = "─" * 60
    parts = []
    for r in reports:
        parts.append(f"[{r['label']}]\n{r['content']}")
    combined = f"\n{sep}\n".join(parts)

    return f"""아래는 동일 기업의 DART 공시 사업보고서 중 **'2. 사업의 내용'** 섹션 {len(reports)}개입니다.
각 보고서의 사업 내용만을 기반으로 비교 분석해주세요.

{sep}
{combined}
{sep}

## 분석 요청 항목

1. **핵심 변화 요약**
   - 기간별로 사업 내용에서 달라진 핵심 사항을 표로 정리

2. **사업 전략 변화**
   - 신규 사업 진출, 기존 사업 확장·축소·철수
   - 주력 제품·서비스의 변화

3. **시장 및 경쟁환경 변화**
   - 주요 고객, 시장 점유율, 경쟁사 관련 서술 변화

4. **리스크 요인 변화**
   - 새롭게 등장하거나 해소된 리스크 (원재료, 규제, 기술, 시장)

5. **수치·실적 언급 변화**
   - 사업 내용에 등장하는 매출·물량·점유율 등 구체 수치 비교

6. **투자자 관점 인사이트**
   - 사업 내용에서 포착할 수 있는 중요 시그널 (긍정/부정)

분석은 한국어로 작성하고, 각 항목마다 보고서 원문의 구체적인 표현·수치를 근거로 제시해주세요."""


# ─── 단일 태스크 처리 (병렬 워커용) ───────────────────────────────────────
def process_task(task: dict) -> dict:
    """단일 기업 비교 분석 태스크. 병렬 워커에서 호출됨.

    task 키:
      corp_code, corp_name, rid_a, rid_b,
      type_a, type_b, model, key_idx, key_label, delay,
      overwrite, total_limit
    """
    corp_code  = task["corp_code"]
    corp_name  = task["corp_name"]
    rid_a      = task["rid_a"]
    rid_b      = task["rid_b"]
    type_a     = task["type_a"]
    type_b     = task["type_b"]
    model      = task["model"]
    key_idx    = task["key_idx"]
    key_label  = task["key_label"]
    delay      = task["delay"]
    task_num   = task["task_num"]
    total      = task["total"]

    # 워커 전용 DB 연결 (스레드 로컬)
    db = get_db()

    # 이미 완료된 기업 건너뜀
    if not task["overwrite"] and already_done(db, corp_code, type_a, type_b, model):
        return {"status": "skipped", "corp_name": corp_name, "task_num": task_num}

    # biz_content 로드
    rr_a = db.execute(
        "SELECT biz_content, report_name, rcept_dt FROM reports WHERE id=?", [rid_a]
    ).fetchone()
    rr_b = db.execute(
        "SELECT biz_content, report_name, rcept_dt FROM reports WHERE id=?", [rid_b]
    ).fetchone()

    if not rr_a or not rr_b:
        return {"status": "no_report", "corp_name": corp_name, "task_num": task_num}

    biz_a = (rr_a["biz_content"] or "").strip()[:60000]
    biz_b = (rr_b["biz_content"] or "").strip()[:60000]

    def fmt_dt(dt):
        if dt and len(dt) == 8:
            return f"{dt[:4]}.{dt[4:6]}.{dt[6:]}"
        return dt or ""

    reports = [
        {"label": f"{corp_name} / {rr_a['report_name']} (접수:{fmt_dt(rr_a['rcept_dt'])})",
         "content": biz_a},
        {"label": f"{corp_name} / {rr_b['report_name']} (접수:{fmt_dt(rr_b['rcept_dt'])})",
         "content": biz_b},
    ]
    prompt = build_prompt(reports)

    # 워커별 딜레이 (per-key rate limit)
    now = time.time()
    with _counter_lock:
        last = _last_call_time.get(key_idx, 0)
        wait = delay - (now - last)
        if wait > 0:
            _last_call_time[key_idx] = now + wait
        else:
            _last_call_time[key_idx] = now
    if wait > 0:
        time.sleep(wait)

    # API 호출 (워커 전용 키)
    keys = _get_api_keys()
    api_key = keys[key_idx % len(keys)]

    try:
        result_text, used_model, used_type = call_gemini_with_key(
            model, prompt, api_key, key_label, timeout=120
        )
        with _db_lock:
            save_result(db, corp_code, corp_name, rid_a, rid_b,
                        type_a, type_b, model,
                        result_text, len(biz_a), len(biz_b))
        return {
            "status": "ok",
            "corp_name": corp_name,
            "task_num": task_num,
            "total": total,
            "biz_a": len(biz_a),
            "biz_b": len(biz_b),
            "used_type": used_type,
            "model": model,
            "key_label": key_label,
            "key_idx": key_idx,
        }
    except RuntimeError as e:
        err_msg = str(e)
        with _db_lock:
            save_error(db, corp_code, corp_name, rid_a, rid_b,
                       type_a, type_b, model, err_msg)
        return {
            "status": "error",
            "corp_name": corp_name,
            "task_num": task_num,
            "error": err_msg,
            "key_idx": key_idx,
            "key_label": key_label,
        }


# ─── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="배치 AI 비교 분석")
    parser.add_argument("--type-a",  default="2025_annual",
                        choices=["2025_annual","2025_q3","2025_h1","2025_q1"],
                        help="비교 기준 보고서 유형 (기본: 2025_annual)")
    parser.add_argument("--type-b",  default="2025_q1",
                        choices=["2025_annual","2025_q3","2025_h1","2025_q1"],
                        help="비교 대상 보고서 유형 (기본: 2025_q1)")
    parser.add_argument("--model",   default="flash",
                        choices=["flash", "flash-lite", "pro"],
                        help="AI 모델: flash(기본,500RPD) | flash-lite(최고RPD,1000/키,2키=2000) | pro(100RPD)")
    parser.add_argument("--limit",   type=int, default=1000,
                        help="처리할 최대 기업 수 (기본: 1000)")
    parser.add_argument("--resume",  type=int, default=0,
                        help="N번째 기업부터 재개 (기본: 0 = 처음부터)")
    parser.add_argument("--delay",   type=float, default=0,
                        help="요청 간격(초) — 0=모델 기본값 자동 사용 (기본: 0)")
    parser.add_argument("--all",     action="store_true",
                        help="이미 분석된 기업도 재분석(덮어쓰기)")
    parser.add_argument("--dry-run", action="store_true",
                        help="실제 API 호출 없이 대상 기업 목록만 출력")
    parser.add_argument("--workers", type=int, default=1,
                        help="병렬 워커 수 (기본: 1=순차, 최대: 2). 워커 0→키1, 워커 1→키2 고정 할당.")
    args = parser.parse_args()

    # workers 범위 클램핑 (최대 2)
    args.workers = max(1, min(2, args.workers))

    if args.type_a == args.type_b:
        print("오류: --type-a 와 --type-b 가 같습니다")
        sys.exit(1)

    # API 키 확인
    if not os.environ.get("GEMINI_API_KEY"):
        print("오류: GEMINI_API_KEY가 .env에 없습니다")
        sys.exit(1)

    keys = _get_api_keys()
    num_keys = len(keys)
    key_labels = [f"파싱{i+1}" for i in range(num_keys)]

    # workers > num_keys 이면 경고
    effective_workers = min(args.workers, num_keys)
    if args.workers > num_keys:
        print(f"⚠ --workers {args.workers} 요청, 사용 가능한 키 {num_keys}개 → {effective_workers}워커로 조정")
        args.workers = effective_workers

    db = get_db()
    ensure_table(db)

    label_a = TYPE_LABELS.get(args.type_a, args.type_a)
    label_b = TYPE_LABELS.get(args.type_b, args.type_b)

    # 딜레이: 명시 지정 없으면 모델 기본값
    limit_info = GEMINI_LIMITS.get(args.model, GEMINI_LIMITS["flash"])
    delay = args.delay if args.delay > 0 else limit_info["delay"]
    rpd   = limit_info["rpd"]
    # flash 모드는 flash-lite로 자동 전환되므로 합산 RPD 표시
    if args.model == "flash":
        fallback_rpd = GEMINI_LIMITS.get("flash-lite", {}).get("rpd", 0)
        rpd_display = f"{rpd}+{fallback_rpd}(lite)={rpd+fallback_rpd}"
    else:
        rpd_display = str(rpd)

    # 병렬 모드에서는 합산 RPD 계산
    total_rpd_display = rpd_display
    if args.workers > 1:
        total_rpd_display = f"{rpd_display} × {args.workers}키 = (합산)"

    print(f"\n{'='*60}")
    print(f"배치 AI 비교 분석 시작")
    print(f"  비교 조합 : {label_a}  vs  {label_b}")
    print(f"  모델      : Gemini {args.model.upper()}")
    print(f"  워커      : {args.workers}개 ({', '.join(key_labels[:args.workers])} 고정)")
    print(f"  일일 한도 : {total_rpd_display} RPD")
    print(f"  요청 간격 : {delay}초 (워커별 독립 타이머)")
    print(f"  처리 한도 : {args.limit}개")
    est = args.limit * delay / 60 / args.workers
    print(f"  예상 시간 : {est:.0f}분 ({args.workers}워커 병렬)")
    if args.resume:
        print(f"  재개위치  : {args.resume}번째부터")
    if args.all:
        print(f"  모드      : 덮어쓰기 (--all)")
    print(f"{'='*60}\n")

    # 두 보고서 유형 모두 있는 기업 조회
    rows = db.execute("""
        SELECT
            a.corp_code,
            MAX(a.corp_name) as corp_name,
            MAX(CASE WHEN a.report_type=? THEN a.id END) as id_a,
            MAX(CASE WHEN a.report_type=? THEN a.id END) as id_b
        FROM reports a
        WHERE a.biz_content IS NOT NULL
          AND LENGTH(a.biz_content) >= 500
          AND a.report_type IN (?, ?)
        GROUP BY a.corp_code
        HAVING
            MAX(CASE WHEN a.report_type=? THEN 1 ELSE 0 END) = 1
            AND MAX(CASE WHEN a.report_type=? THEN 1 ELSE 0 END) = 1
        ORDER BY a.corp_code
    """, [args.type_a, args.type_b,
          args.type_a, args.type_b,
          args.type_a, args.type_b]).fetchall()

    total_eligible = len(rows)
    print(f"분석 가능 기업: {total_eligible}개 (두 기간 모두 biz_content 보유)")

    # resume 적용
    if args.resume > 0:
        rows = rows[args.resume:]
        print(f"→ {args.resume}번째부터 재개: 남은 {len(rows)}개")

    if args.dry_run:
        print("\n[DRY RUN] 실제 API 호출 없음. 처음 10개 기업:")
        for i, row in enumerate(rows[:10], 1):
            print(f"  {i:3d}. {row['corp_name']} ({row['corp_code']})")
        print(f"\n전체 {len(rows)}개 기업 처리 예정")
        estimated_min = len(rows[:args.limit]) * delay / 60 / args.workers
        print(f"예상 소요 시간: {estimated_min:.0f}분 ({estimated_min/60:.1f}시간)")
        return

    # 이미 완료된 기업 수 확인
    done_count = db.execute("""
        SELECT COUNT(*) FROM ai_comparisons
        WHERE report_type_a=? AND report_type_b=? AND model=? AND status='ok'
    """, [args.type_a, args.type_b, args.model]).fetchone()[0]
    print(f"기완료 기업: {done_count}개")

    # limit 적용
    rows = rows[:args.limit]

    # 진행 카운터 (스레드 안전)
    counters = {"processed": 0, "success": 0, "errors": 0, "skipped": 0}
    start_time = time.time()
    quota_exhausted = threading.Event()

    # ── 단일 워커 모드 (--workers 1) ────────────────────────────────────────
    if args.workers == 1:
        processed = 0
        success   = 0
        skipped   = 0
        errors    = 0

        for row in rows:
            if processed >= args.limit:
                print(f"\n한도 {args.limit}개 도달. 오늘 분량 완료.")
                break

            corp_code = row["corp_code"]
            corp_name = row["corp_name"] or corp_code
            rid_a     = row["id_a"]
            rid_b     = row["id_b"]

            # 이미 완료된 기업 건너뜀
            if not args.all and already_done(db, corp_code, args.type_a, args.type_b, args.model):
                skipped += 1
                continue

            processed += 1

            rr_a = db.execute(
                "SELECT biz_content, report_name, rcept_dt FROM reports WHERE id=?", [rid_a]
            ).fetchone()
            rr_b = db.execute(
                "SELECT biz_content, report_name, rcept_dt FROM reports WHERE id=?", [rid_b]
            ).fetchone()

            if not rr_a or not rr_b:
                print(f"  [{processed:4d}] {corp_name}: 보고서 로드 실패 — 건너뜀")
                skipped += 1
                processed -= 1
                continue

            biz_a = (rr_a["biz_content"] or "").strip()[:60000]
            biz_b = (rr_b["biz_content"] or "").strip()[:60000]

            def fmt_dt(dt):
                if dt and len(dt) == 8:
                    return f"{dt[:4]}.{dt[4:6]}.{dt[6:]}"
                return dt or ""

            reports_data = [
                {"label": f"{corp_name} / {rr_a['report_name']} (접수:{fmt_dt(rr_a['rcept_dt'])})",
                 "content": biz_a},
                {"label": f"{corp_name} / {rr_b['report_name']} (접수:{fmt_dt(rr_b['rcept_dt'])})",
                 "content": biz_b},
            ]
            prompt = build_prompt(reports_data)

            try:
                result_text, used_model, used_type = call_gemini(args.model, prompt, timeout=120)
                save_result(db, corp_code, corp_name, rid_a, rid_b,
                            args.type_a, args.type_b, args.model,
                            result_text, len(biz_a), len(biz_b))
                success += 1

                elapsed = time.time() - start_time
                avg_sec = elapsed / processed
                remain  = (min(args.limit, total_eligible) - processed) * avg_sec
                eta_min = remain / 60
                type_tag = f"[{used_type}]" if used_type != args.model else ""
                print(
                    f"  [{processed:4d}/{min(args.limit, total_eligible):4d}] "
                    f"{corp_name[:18]:<18} ✓  "
                    f"({len(biz_a)//1000}K+{len(biz_b)//1000}K자)  "
                    f"{type_tag}  남은≈{eta_min:.0f}분"
                )

            except RuntimeError as e:
                err_msg = str(e)
                save_error(db, corp_code, corp_name, rid_a, rid_b,
                           args.type_a, args.type_b, args.model, err_msg)
                errors += 1
                print(f"  [{processed:4d}] {corp_name[:18]:<18} ✗  {err_msg[:80]}")

                if "할당량 초과" in err_msg and "flash-lite" in err_msg:
                    print("\n⚠ Flash + Flash-Lite 모두 할당량 초과! 내일 재개하세요.")
                    print(f"  재개: python scripts/batch_compare.py --resume {args.resume + processed}")
                    break

            if processed < min(args.limit, total_eligible):
                time.sleep(delay)

        # 최종 요약
        elapsed_total = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"배치 완료!")
        print(f"  처리: {processed}개  성공: {success}개  오류: {errors}개  건너뜀: {skipped}개")
        print(f"  소요 시간: {elapsed_total/60:.1f}분")
        total_done = db.execute("""
            SELECT COUNT(*) FROM ai_comparisons
            WHERE report_type_a=? AND report_type_b=? AND model=? AND status='ok'
        """, [args.type_a, args.type_b, args.model]).fetchone()[0]
        print(f"  DB 누적 완료: {total_done}개 / {total_eligible}개")
        remain_total = total_eligible - total_done
        if remain_total > 0:
            print(f"  남은 기업: {remain_total}개 → 내일 실행 권장")
            print(f"  재개 명령어: python scripts/batch_compare.py --resume {args.resume + processed}")
        else:
            print(f"  전체 완료!")
        print(f"{'='*60}\n")
        return

    # ── 병렬 워커 모드 (--workers 2) ─────────────────────────────────────────
    # 태스크 목록 생성 (인터리빙: task_i → key_idx = i % num_workers)
    tasks = []
    for i, row in enumerate(rows):
        key_idx = i % args.workers
        tasks.append({
            "corp_code":  row["corp_code"],
            "corp_name":  row["corp_name"] or row["corp_code"],
            "rid_a":      row["id_a"],
            "rid_b":      row["id_b"],
            "type_a":     args.type_a,
            "type_b":     args.type_b,
            "model":      args.model,
            "key_idx":    key_idx,
            "key_label":  key_labels[key_idx] if key_idx < len(key_labels) else f"키{key_idx}",
            "delay":      delay,
            "overwrite":  args.all,
            "task_num":   i + 1,
            "total":      len(rows),
        })

    processed = 0
    success   = 0
    errors    = 0
    skipped   = 0

    print(f"병렬 처리 시작: {len(tasks)}개 태스크, {args.workers}워커")
    print(f"  워커 0 → {key_labels[0]} (GEMINI_API_KEY)")
    if args.workers > 1 and num_keys > 1:
        print(f"  워커 1 → {key_labels[1]} (GEMINI_API_KEY_2)")
    print()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_task = {executor.submit(process_task, t): t for t in tasks}

        for future in as_completed(future_to_task):
            result = future.result()
            status     = result["status"]
            corp_name  = result["corp_name"]
            task_num   = result["task_num"]
            key_label  = result.get("key_label", "")
            wid        = result.get("key_idx", 0)

            if status == "skipped":
                skipped += 1
                continue
            elif status == "no_report":
                skipped += 1
                _tprint(f"  [W{wid}] [{task_num:4d}/{len(tasks):4d}] {corp_name[:18]:<18} - 보고서 없음")
                continue
            elif status == "ok":
                processed += 1
                success   += 1
                elapsed = time.time() - start_time
                biz_a = result.get("biz_a", 0)
                biz_b = result.get("biz_b", 0)
                used_type = result.get("used_type", args.model)
                type_tag = f"[{used_type}]" if used_type != args.model else ""
                avg_sec = elapsed / max(processed, 1)
                remain  = (len(tasks) - processed - skipped) * avg_sec / args.workers
                eta_min = remain / 60
                _tprint(
                    f"  [W{wid}/{key_label}] [{task_num:4d}/{len(tasks):4d}] "
                    f"{corp_name[:16]:<16} ✓  "
                    f"({biz_a//1000}K+{biz_b//1000}K자) "
                    f"{type_tag}  남은≈{eta_min:.0f}분"
                )
            elif status == "error":
                processed += 1
                errors    += 1
                err_msg = result.get("error", "알 수 없는 오류")
                _tprint(f"  [W{wid}/{key_label}] [{task_num:4d}/{len(tasks):4d}] {corp_name[:16]:<16} ✗  {err_msg[:70]}")

                if "할당량 초과" in err_msg:
                    _tprint(f"\n⚠ [{key_label}] 할당량 초과 감지. 내일 재개하세요.")

    # 최종 요약
    elapsed_total = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"병렬 배치 완료!")
    print(f"  처리: {processed}개  성공: {success}개  오류: {errors}개  건너뜀: {skipped}개")
    print(f"  소요 시간: {elapsed_total/60:.1f}분")
    total_done = db.execute("""
        SELECT COUNT(*) FROM ai_comparisons
        WHERE report_type_a=? AND report_type_b=? AND model=? AND status='ok'
    """, [args.type_a, args.type_b, args.model]).fetchone()[0]
    print(f"  DB 누적 완료: {total_done}개 / {total_eligible}개")
    remain_total = total_eligible - total_done
    if remain_total > 0:
        print(f"  남은 기업: {remain_total}개 → 내일 실행 권장")
        print(f"  재개 명령어: python scripts/batch_compare.py --resume {args.resume + processed} --workers {args.workers}")
    else:
        print(f"  전체 완료!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
