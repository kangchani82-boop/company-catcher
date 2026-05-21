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
# Gemini 모델 정책은 config/gemini_models.py 한 곳에서 관리 (SSOT)
sys.path.insert(0, str(ROOT))
from config.gemini_models import (
    GEMINI_LIMITS as _BASE_LIMITS,
    GEMINI_FALLBACKS,
    GEMINI_DELAYS,
    FALLBACK_ON_QUOTA,
)

# 본 스크립트는 limits dict에 'delay'도 함께 보관 (호출 간격 계산용)
GEMINI_LIMITS = {
    k: {**v, "delay": GEMINI_DELAYS.get(k, 6.5)}
    for k, v in _BASE_LIMITS.items()
}

# ── 보고서 유형 표시명 (연도+종류 명시 — 키워드 명확) ────────────────────
TYPE_LABELS = {
    "2025_annual": "2025년 전체 사업보고서",
    "2025_q3":     "2025년 3분기 보고서",
    "2025_h1":     "2025년 반기 보고서",
    "2025_q1":     "2025년 1분기 보고서",
    "2026_annual": "2026년 전체 사업보고서",
    "2026_q3":     "2026년 3분기 보고서",
    "2026_h1":     "2026년 반기 보고서",
    "2026_q1":     "2026년 1분기 보고서",
}

# 보고서 발간 시간 순서 (작을수록 과거) — 시간 흐름순 표시용
TYPE_TIME_ORDER = {
    "2025_q1": 1, "2025_h1": 2, "2025_q3": 3, "2025_annual": 4,
    "2026_q1": 5, "2026_h1": 6, "2026_q3": 7, "2026_annual": 8,
}


# ─── 스레드 안전 락 ──────────────────────────────────────────────────────────
_db_lock      = threading.Lock()   # DB 쓰기 락
_print_lock   = threading.Lock()   # print 락
_counter_lock = threading.Lock()   # 진행 카운터 락

# 워커별 마지막 호출 시각 (per-key rate limit)
_last_call_time: dict[int, float] = {}  # key_index → timestamp

# 키별 소진 플래그 (모든 워커 공유 — 소진된 키는 더 이상 시도 안 함)
_exhausted_keys: set = set()
# (key_idx, model_name) 페어별 소진 — 죽은 조합 재시도 방지
_exhausted_pairs: set = set()
_exhausted_lock = threading.Lock()


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
    """사용 가능한 Gemini API 키 목록 반환 (최대 3개)"""
    keys = []
    for var in ["GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"]:
        k = os.environ.get(var, "").strip()
        if k:
            keys.append(k)
    if not keys:
        raise ValueError("GEMINI_API_KEY 가 .env에 없습니다")
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
                         key_label: str, timeout: int = 120,
                         key_idx: int = None):
    """지정된 API 키로 Gemini 호출. (result_text, used_model_name, used_model_type) 반환.

    key_idx 가 주어지면 (key_idx, model_name) 페어별 소진 마킹으로 dead 조합 재시도 방지.
    """
    types_to_try = [model_type]
    if model_type in FALLBACK_ON_QUOTA:
        types_to_try.append(FALLBACK_ON_QUOTA[model_type])

    for mtype in types_to_try:
        candidates = GEMINI_FALLBACKS.get(mtype, GEMINI_FALLBACKS["flash"])
        last_err = None

        for model_name in candidates:
            # 이미 소진 마킹된 (키, 모델) 페어는 스킵
            if key_idx is not None:
                with _exhausted_lock:
                    if (key_idx, model_name) in _exhausted_pairs:
                        continue
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model_name}:generateContent?key={api_key}"
            )
            payload = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 8192, "temperature": 0.1},
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
                    # 페어 소진 마킹 (앞으로 이 키x모델은 시도 안 함)
                    if key_idx is not None:
                        with _exhausted_lock:
                            _exhausted_pairs.add((key_idx, model_name))
                    _tprint(f"  ⚠ [{key_label}] {model_name} 429 → 페어 소진 마킹")
                    last_err = f"할당량 초과({model_name})"
                    continue  # 같은 타입 내 다음 모델 시도
                if e.code in (500, 503):
                    last_err = f"{model_name} {e.code} (서버 오류) — 다음 모델 시도"
                    continue
                raise RuntimeError(f"Gemini API 오류 {e.code}: {body[:300]}")
            except urllib.error.URLError as e:
                raise RuntimeError(f"네트워크 오류: {e.reason}")
            except (TimeoutError, OSError) as e:
                last_err = f"{model_name} 타임아웃 — 다음 모델 시도"
                continue

    raise RuntimeError(f"사용 가능한 Gemini 모델 없음. 마지막 오류: {last_err}")


# ─── AI 호출 (단일워커/라운드로빈 모드) ────────────────────────────────────
def call_gemini(model_type: str, prompt: str, timeout: int = 120):
    """Gemini API 호출. (result_text, used_model_name, used_model_type) 반환.
    파싱1/파싱2 라운드로빈 + 429 시 다른 키 자동 전환 + fallback 모델 타입 전환."""
    keys = _get_api_keys()
    label_names = ["파싱1", "파싱2", "파싱3"]
    key_labels = {k: label_names[i] for i, k in enumerate(keys)}

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
                "generationConfig": {"maxOutputTokens": 8192, "temperature": 0.1},
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
                if e.code in (500, 503):
                    last_err = f"{model_name} {e.code} (서버 오류) — 다음 모델 시도"
                    continue  # 다음 fallback 모델로
                raise RuntimeError(f"Gemini API 오류 {e.code}: {body[:300]}")
            except urllib.error.URLError as e:
                raise RuntimeError(f"네트워크 오류: {e.reason}")
            except (TimeoutError, OSError) as e:
                last_err = f"{model_name} 타임아웃 — 다음 모델 시도"
                continue

    raise RuntimeError(f"사용 가능한 Gemini 모델 없음. 마지막 오류: {last_err}")


def build_prompt(reports: list) -> str:
    """분석 프롬프트 생성 (biz_content 전용) — v3 최신 기준 회고

    ★ 핵심 원칙: 가장 최신 보고서를 기준으로 과거 보고서 대비 무엇이 달라졌는가
       - reports[0] = 사업보고서 (★ 최신·기준)
       - reports[1] = 분기보고서 (과거·비교 대상)
       관점: "최신 사업보고서가 과거 분기보고서 대비 어떻게 달라졌나"
    """
    sep = "─" * 60
    assert len(reports) == 2
    latest_r  = reports[0]  # 사업보고서 — ★ 최신·기준
    past_r    = reports[1]  # 분기보고서 — 과거·비교

    return f"""[필수 준수 — 답변 시작 전 확인]
1) 답변은 반드시 한국어로만 작성. 영어 단어/문장 사용 절대 금지.
2) 분석 관점: **최신 사업보고서가 과거 분기보고서 대비 어떻게 달라졌나** (시간 회고형).
3) 모든 변화는 5가지 라벨 중 하나로 분류: NEW / REMOVED / EXPANDED / SHRUNK / CHANGED.

═══════════════════════════════════════════════════════════
[데이터]

▶ 과거 보고서: {past_r['label']} (먼저 발간)
{past_r['content']}

═══════════════════════════════════════════════════════════
▶ ★ 최신 보고서: {latest_r['label']} (가장 최근 발간 — 분석 기준점)
{latest_r['content']}
═══════════════════════════════════════════════════════════

[분석 원칙 — 한국어로만 답변]
시간 흐름: 과거(분기보고서, {past_r['label']}) → 최신(사업보고서, {latest_r['label']})
관점: "최신 사업보고서를 기준으로 보면, 과거 분기보고서 대비 ○○가 ○○로 달라졌다"
시점 차이 인지: 분기보고서는 분기 누적, 사업보고서는 연간 누적. 수치 단순 비교 X.

[변화 분류 룰 — 모든 변화 항목에 반드시 라벨]
- NEW       : 최신 사업보고서에 새로 등장 (과거 분기엔 없었음)
- REMOVED   : 최신 사업보고서에서 사라짐 (과거 분기엔 있었음)
- EXPANDED  : 과거에도 있었으나 최신에서 확대·강조됨
- SHRUNK    : 과거에 있었으나 최신에서 축소·약화됨
- CHANGED   : 과거에 있었으나 내용이 변경됨

[답변 형식 예시 — 이 한국어 패턴으로 작성]
"최신 사업보고서에 따르면 ○○ 사업이 신규 등장했다 [NEW]. 과거 분기보고서에는 해당 사업에 대한 언급이 없었다."
"최신 사업보고서에서 ○○ 자회사 관련 기술이 사라졌다 [REMOVED]. 과거 분기보고서에는 ○○로 명시되어 있었다."
"최신 사업보고서에서 신재생에너지 사업 비중이 확대됐다 [EXPANDED]. 과거 분기보고서에는 단순 언급에 그쳤으나 최신 보고서에서는 핵심 전략으로 부각됐다."

═══════════════════════════════════════════════════════════
[작성 항목 — 모두 한국어]

## 1. 핵심 변화 요약
한국어 표로 정리.

| 항목 | 과거 분기보고서 | 최신 사업보고서 | 변화 |
|------|----------------|------------------|------|
| (예) 신재생에너지 사업 | 언급 없음 | "신재생에너지 분야 진출" 명시 | NEW |
| (예) 모빌리티 자회사 | 자회사 X 운영 | 매각 완료 | REMOVED |

## 2. 최신 사업보고서에 새로 등장한 사업·전략 [NEW]
- 과거 분기보고서엔 없거나 축약돼 있다가 최신 사업보고서에 새로 등장한 항목
- 사업·정관·자회사·제품·시장 진출 등 (한국어로만)

## 3. 최신 사업보고서에서 사라진 내용 [REMOVED]
- 과거 분기보고서엔 있었으나 최신 사업보고서에 빠진 항목
- 사업 철수·자회사 청산·부문 통합 등

## 4. 시장 및 경쟁환경 서술 변화
- 주요 고객·점유율·경쟁사 서술이 최신 사업보고서에서 어떻게 달라졌는가
- 추가/삭제된 거래처 명단

## 5. 리스크 요인 변화
- 최신에 새로 등장한 리스크 [NEW]
- 최신에서 사라진 리스크 [REMOVED]

## 6. 수치·실적 변화 (시점 차이 명시)
- 형식: "과거 분기보고서(분기 누적 기준) X → 최신 사업보고서(연간 누적 기준) Y"

## 7. 취재 가치 판단 (1~3가지)
- 우선순위: NEW > REMOVED > EXPANDED > SHRUNK > CHANGED 순

═══════════════════════════════════════════════════════════
[★ 출력 직전 자체 점검]
- 답변에 영어 문장이 있는가? → 모두 한국어로 변환
- "Annual / Quarterly / Reference / Comparison" 같은 영어 단어가 있는가? → 삭제
- 모든 변화에 NEW/REMOVED/EXPANDED/SHRUNK/CHANGED 라벨이 붙었는가?
- 관점이 "최신이 과거 대비 어떻게 다른가"로 되어있는가?

지금 분석을 시작하세요. **한국어로만 답변.**"""


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

    # type_a = 사업보고서(연간 기준)가 reports[0], type_b = 분기보고서가 reports[1]
    # rcept_dt 기준 역순 정렬: 최신(사업보고서) → 오래된(분기보고서) 순
    reports = sorted([
        {"label": f"{corp_name} / {rr_a['report_name']} (접수:{fmt_dt(rr_a['rcept_dt'])})",
         "content": biz_a,
         "rcept_dt": rr_a['rcept_dt'] or ''},
        {"label": f"{corp_name} / {rr_b['report_name']} (접수:{fmt_dt(rr_b['rcept_dt'])})",
         "content": biz_b,
         "rcept_dt": rr_b['rcept_dt'] or ''},
    ], key=lambda x: x['rcept_dt'], reverse=True)  # 최신(사업보고서) 먼저 → 기준으로
    # build_prompt에서 rcept_dt 키는 불필요하므로 제거
    for r in reports:
        r.pop('rcept_dt', None)
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

    # API 호출 (워커 전용 키 — 단, 소진된 키는 다른 키로 우회)
    keys = _get_api_keys()
    # 워커가 자기 키만 고집하지 않고, 소진되지 않은 키 중 round-robin으로 선택
    with _exhausted_lock:
        own_idx = key_idx % len(keys)
        if own_idx in _exhausted_keys:
            # 자기 키가 소진됐으면 살아있는 다른 키로 전환
            alive = [i for i in range(len(keys)) if i not in _exhausted_keys]
            if not alive:
                # 모든 키 소진 → 에러로 즉시 반환
                return {
                    "status": "error", "corp_name": corp_name, "task_num": task_num,
                    "total": total, "error": "모든 키 소진 — 내일 재개",
                    "key_label": key_label, "key_idx": key_idx,
                }
            own_idx = alive[task_num % len(alive)]
            key_label = f"파싱{own_idx+1}"
    api_key = keys[own_idx]

    # 영어 응답 자동 검출 + 재시도 (max 3회: 같은 모델 → 다른 모델 → 마지막 강한 prefix)
    def _is_english_response(text: str) -> bool:
        """응답이 영어 위주인지 검출"""
        if not text or len(text) < 100:
            return False
        en_chars = len(re.findall(r"[a-zA-Z]", text))
        kr_chars = len(re.findall(r"[가-힣]", text))
        if kr_chars < 100 and en_chars > 200:
            return True
        if en_chars > kr_chars * 1.5:
            return True
        return False

    try:
        result_text = None
        used_type = None
        used_model = None

        # 시도 1: 기본 모델 + 자기 키. 429 시 다른 키로 자동 우회
        def _try_with_key_fallback(this_idx, this_label, this_key):
            """주어진 키로 시도. 할당량 초과 시 살아있는 다른 키로 자동 전환."""
            try:
                return call_gemini_with_key(model, prompt, this_key, this_label, timeout=120, key_idx=this_idx), this_idx, this_label, this_key
            except RuntimeError as ex:
                if "할당량 초과" not in str(ex):
                    raise
                # 이 키 소진 마킹
                with _exhausted_lock:
                    _exhausted_keys.add(this_idx)
                    alive = [i for i in range(len(keys)) if i not in _exhausted_keys]
                _tprint(f"  ⚠ [{this_label}] 키 소진 마킹 — 살아있는 키 {len(alive)}개 남음")
                # 살아있는 다른 키 시도
                for alt_idx in alive:
                    alt_key = keys[alt_idx]
                    alt_label = f"파싱{alt_idx+1}"
                    try:
                        _tprint(f"  ↻ [{this_label}→{alt_label}] {corp_name} 다른 키로 재시도")
                        return call_gemini_with_key(model, prompt, alt_key, alt_label, timeout=120, key_idx=alt_idx), alt_idx, alt_label, alt_key
                    except RuntimeError as ex2:
                        if "할당량 초과" in str(ex2):
                            with _exhausted_lock:
                                _exhausted_keys.add(alt_idx)
                        continue
                raise RuntimeError("모든 키 소진 — 내일 재개")

        (result_text, used_model, used_type), key_idx, key_label, api_key = _try_with_key_fallback(
            own_idx, key_label, api_key
        )

        # 영어 검출 시 재시도 — 모델 변경 + 강력한 한국어 prefix
        if _is_english_response(result_text):
            _tprint(f"  ⚠ [{key_label}] {corp_name} 영어 응답 검출 (시도 1) — 다른 모델로 재시도")
            time.sleep(2)
            ko_prompt = (
                "[★ 절대 명령 — 한국어로만 답변 ★]\n"
                "당신은 한국 경제 기자입니다. 영어 사용 절대 금지.\n"
                "답변은 반드시 한국어 문장으로 시작합니다. 모든 분석은 한국어로만 작성합니다.\n"
                "예시 시작 문장: '최신 사업보고서에 따르면 ...'\n\n"
                + prompt
            )
            # 다른 모델 타입 사용 (flash → flash-lite)
            alt_model = "flash-lite" if model == "flash" else "flash"
            try:
                result_text, used_model, used_type = call_gemini_with_key(
                    alt_model, ko_prompt, api_key, key_label, timeout=120, key_idx=key_idx
                )
            except Exception:
                pass

        # 시도 3: 다른 키 + 더 강한 prefix
        if _is_english_response(result_text):
            _tprint(f"  ⚠ [{key_label}] {corp_name} 영어 응답 검출 (시도 2) — 키·prefix 강화")
            time.sleep(2)
            other_keys = [(i, k) for i, k in enumerate(keys) if k != api_key]
            try_idx, try_key = other_keys[0] if other_keys else (key_idx, api_key)
            stronger_prompt = (
                "한국어 답변 필수. 영어로 답변하면 무효 처리됩니다.\n"
                "답변 첫 단어는 반드시 한글입니다.\n\n"
                "다음 분석을 한국어로만 작성하세요:\n\n"
                + prompt
            )
            try:
                result_text, used_model, used_type = call_gemini_with_key(
                    "flash-lite", stronger_prompt, try_key, "재시도", timeout=120, key_idx=try_idx
                )
            except Exception:
                pass

        # 마지막에도 영어면 폐기 처리 (status=error로 저장 — 재처리 추적용)
        if _is_english_response(result_text):
            _tprint(f"  ✗ [{key_label}] {corp_name} 영어 응답 3회 모두 실패 — 폐기")
            with _db_lock:
                save_error(db, corp_code, corp_name, rid_a, rid_b,
                           type_a, type_b, model,
                           "영어 응답 — 한국어 답변 3회 강제 실패 (재처리 대상)")
            return {
                "status": "error",
                "corp_name": corp_name,
                "task_num": task_num,
                "total": total,
                "error": "영어 응답 폐기",
                "key_label": key_label,
                "key_idx": key_idx,
            }

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
                        choices=["2025_annual","2025_q3","2025_h1","2025_q1",
                                 "2026_annual","2026_q3","2026_h1","2026_q1"],
                        help="비교 기준 보고서 유형 (기본: 2025_annual)")
    parser.add_argument("--type-b",  default="2025_q1",
                        choices=["2025_annual","2025_q3","2025_h1","2025_q1",
                                 "2026_annual","2026_q3","2026_h1","2026_q1"],
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
    parser.add_argument("--market",  type=str, default="KOSPI,KOSDAQ",
                        help="비교 대상 시장 (콤마 구분, 기본: KOSPI,KOSDAQ). KONEX/ALL 등 가능.")
    args = parser.parse_args()

    # workers 범위 클램핑 (최대 3)
    args.workers = max(1, min(3, args.workers))

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
    # 시장 필터: 기본 KOSPI+KOSDAQ만 (--market 으로 변경 가능)
    market_list = [m.strip().upper() for m in (args.market or "KOSPI,KOSDAQ").split(",") if m.strip()]
    market_placeholders = ",".join(["?"] * len(market_list))
    rows = db.execute(f"""
        SELECT
            a.corp_code,
            MAX(a.corp_name) as corp_name,
            MAX(CASE WHEN a.report_type=? THEN a.id END) as id_a,
            MAX(CASE WHEN a.report_type=? THEN a.id END) as id_b
        FROM reports a
        JOIN companies c ON c.corp_code = a.corp_code
                        AND c.market IN ({market_placeholders})
        WHERE a.biz_content IS NOT NULL
          AND LENGTH(a.biz_content) >= 500
          AND a.report_type IN (?, ?)
        GROUP BY a.corp_code
        HAVING
            MAX(CASE WHEN a.report_type=? THEN 1 ELSE 0 END) = 1
            AND MAX(CASE WHEN a.report_type=? THEN 1 ELSE 0 END) = 1
        ORDER BY a.corp_code
    """, [args.type_a, args.type_b,
          *market_list,
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

    # 진행 카운터 (스레드 안전)
    counters = {"processed": 0, "success": 0, "errors": 0, "skipped": 0}
    start_time = time.time()
    quota_exhausted = threading.Event()

    # ── 단일 워커 모드 (--workers 1) ────────────────────────────────────────
    if args.workers == 1:
        # 단일 모드: limit 적용 후 순차 처리 (already_done은 루프 내에서 skip)
        rows = rows[:args.limit]
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

            # 영어 응답 검출 함수
            def _is_en(t):
                if not t or len(t) < 100: return False
                en = len(re.findall(r"[a-zA-Z]", t))
                kr = len(re.findall(r"[가-힣]", t))
                return (kr < 100 and en > 200) or (en > kr * 1.5)

            try:
                # 1차 호출
                result_text, used_model, used_type = call_gemini(args.model, prompt, timeout=120)

                # 영어 응답이면 재시도 (다른 모델 + 강한 prefix)
                if _is_en(result_text):
                    print(f"        ⚠ 영어 응답 검출 — 다른 모델로 재시도")
                    time.sleep(2)
                    ko_prefix = (
                        "[★ 절대 명령 — 한국어로만 답변 ★]\n"
                        "당신은 한국 경제 기자입니다. 영어 사용 절대 금지.\n"
                        "답변 첫 단어는 반드시 한글입니다.\n"
                        "예시 시작: '최신 사업보고서에 따르면...'\n\n"
                    )
                    alt_model = "flash-lite" if args.model == "flash" else "flash"
                    try:
                        result_text, used_model, used_type = call_gemini(
                            alt_model, ko_prefix + prompt, timeout=120
                        )
                    except Exception:
                        pass

                # 두 번째도 영어면 한 번 더
                if _is_en(result_text):
                    print(f"        ⚠ 영어 응답 재발 — 마지막 시도")
                    time.sleep(2)
                    stronger = (
                        "한국어 답변만 유효. 영어 답변은 무효 처리.\n"
                        "반드시 '최신 사업보고서에 따르면'으로 시작하세요.\n\n"
                    )
                    try:
                        result_text, used_model, used_type = call_gemini(
                            "flash-lite", stronger + prompt, timeout=120
                        )
                    except Exception:
                        pass

                # 그래도 영어면 폐기 (status=error로 저장)
                if _is_en(result_text):
                    print(f"        ✗ 영어 응답 3회 — 폐기")
                    save_error(db, corp_code, corp_name, rid_a, rid_b,
                               args.type_a, args.type_b, args.model,
                               "영어 응답 — 한국어 강제 3회 실패")
                    errors += 1
                    continue

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
    # 태스크 목록 생성: 미완료 기업만 limit 개까지 선별
    pending = []
    for row in rows:
        if not args.all and already_done(db, row["corp_code"], args.type_a, args.type_b, args.model):
            continue
        pending.append(row)
        if len(pending) >= args.limit:
            break

    tasks = []
    for i, row in enumerate(pending):
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
            "total":      len(pending),
        })

    processed = 0
    success   = 0
    errors    = 0
    skipped   = 0

    key_var_names = ["GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"]
    print(f"병렬 처리 시작: {len(tasks)}개 태스크, {args.workers}워커")
    for wi in range(min(args.workers, num_keys)):
        vname = key_var_names[wi] if wi < len(key_var_names) else f"KEY_{wi+1}"
        print(f"  워커 {wi} → {key_labels[wi]} ({vname})")
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
