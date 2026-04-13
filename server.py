"""
server.py — Company Catcher DART 뷰어 서버
─────────────────────────────────────────
DART 공시 데이터베이스 조회 및 웹 뷰어 제공

API 엔드포인트:
  GET  /api/dart/stats          — 수집 현황 통계
  GET  /api/dart/companies      — 기업 목록 (검색/필터/페이지)
  GET  /api/dart/reports        — 특정 기업 보고서 목록
  GET  /api/dart/report         — 단일 보고서 전문 (raw_text + biz_content)
  GET  /api/dart/supply_chain   — 공급망 관계 조회
  GET  /api/dart/latest_type    — 가장 최근 수집된 보고서 유형
  GET  /api/ai/usage            — AI 모델 일일 사용량 조회
  GET  /api/session             — 세션 토큰 (로컬호스트 전용)
  POST /api/ai/analyze          — AI 비교 분석 (X-Api-Key 필요)

사용법:
  python server.py              → http://localhost:8888
  python server.py --port 9000
"""

import argparse
import json
import io
import logging
import os
import re
import secrets
import sqlite3
import sys
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import socketserver

# Windows cp949 콘솔 인코딩 오류 방지
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── 경로 설정 ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
WEB_ROOT = ROOT / "web"
AI_USAGE_PATH = ROOT / "data" / "dart" / "ai_usage.json"

# ── 로깅 ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "logs" / "server.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("dart-server")

# ── 환경 변수 로드 ──────────────────────────────────────────────────────────
_env_path = ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def _ensure_api_secret_key():
    """API_SECRET_KEY 가 없으면 자동 생성하여 .env 에 저장"""
    if os.environ.get("API_SECRET_KEY"):
        return
    key = secrets.token_urlsafe(32)
    os.environ["API_SECRET_KEY"] = key
    # .env 파일에 추가
    existing = _env_path.read_text(encoding="utf-8") if _env_path.exists() else ""
    if "API_SECRET_KEY=" in existing:
        lines = []
        for ln in existing.splitlines():
            if ln.strip().startswith("API_SECRET_KEY=") or ln.strip().startswith("# API_SECRET_KEY="):
                lines.append(f"API_SECRET_KEY={key}")
            else:
                lines.append(ln)
        _env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    else:
        with open(_env_path, "a", encoding="utf-8") as f:
            f.write(f"\nAPI_SECRET_KEY={key}\n")
    log.info(f"API_SECRET_KEY 자동 생성됨")


# ── DB 연결 (thread-local) ─────────────────────────────────────────────────
_local = threading.local()
_db_lock = threading.Lock()


def get_db() -> sqlite3.Connection:
    """스레드 로컬 DB 연결 반환"""
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _local.conn = conn
    return _local.conn


# ── AI 사용량 관리 ─────────────────────────────────────────────────────────
_usage_lock = threading.Lock()


def load_ai_usage() -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    if AI_USAGE_PATH.exists():
        try:
            data = json.loads(AI_USAGE_PATH.read_text(encoding="utf-8"))
            if data.get("date") == today:
                return data
        except Exception:
            pass
    return {"date": today, "flash_used": 0, "pro_used": 0, "sonnet_used": 0}


def save_ai_usage(data: dict):
    AI_USAGE_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def increment_usage(model: str):
    with _usage_lock:
        usage = load_ai_usage()
        key = f"{model}_used"
        usage[key] = usage.get(key, 0) + 1
        save_ai_usage(usage)


# ── AI 호출 함수 ────────────────────────────────────────────────────────────
# 무료 API 일일 한도 (2026 기준)
GEMINI_LIMITS = {
    "flash":      {"rpm": 10, "rpd": 500},
    "flash-lite": {"rpm": 15, "rpd": 1000},
    "pro":        {"rpm":  5, "rpd": 100},
    "article":    {"rpm": 10, "rpd": 100},   # Gemini 3.1 Pro Preview (기사 초안 전용)
}

# 모델별 fallback 순서 (404 시 자동 시도)
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


def call_gemini(model_type: str, prompt: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError("GEMINI_API_KEY가 .env에 설정되지 않았습니다")

    candidates = GEMINI_FALLBACKS.get(model_type, GEMINI_FALLBACKS["flash"])
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
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            log.info(f"Gemini 사용 모델: {model_name}")
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 404:
                log.warning(f"Gemini 모델 없음({model_name}), 다음 시도...")
                last_err = f"모델 없음: {model_name}"
                continue
            raise RuntimeError(f"Gemini API 오류 {e.code}: {body[:400]}")

    raise RuntimeError(f"사용 가능한 Gemini 모델 없음. 마지막 오류: {last_err}")


def call_claude(prompt: str) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY가 .env에 설정되지 않았습니다")

    url = "https://api.anthropic.com/v1/messages"
    payload = json.dumps({
        "model": "claude-sonnet-4-6",
        "max_tokens": 8192,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    req = urllib.request.Request(
        url, data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data["content"][0]["text"]
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Claude API 오류 {e.code}: {body[:400]}")


def build_compare_prompt(reports: list, focus: str = "") -> str:
    parts = []
    for i, r in enumerate(reports, 1):
        parts.append(f"[{r['label']}]\n{r['content']}")

    focus_line = f"\n특별히 '{focus}' 관련 내용을 중점 분석해주세요.\n" if focus else ""

    sep = "─" * 60
    combined = f"\n{sep}\n".join(parts)

    return f"""아래는 동일 기업의 DART 공시 사업보고서 중 **'2. 사업의 내용'** 섹션 {len(reports)}개입니다.
각 보고서의 사업 내용만을 기반으로 비교 분석해주세요.{focus_line}

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


# ── MIME 타입 ──────────────────────────────────────────────────────────────
MIME = {
    ".html": "text/html; charset=utf-8",
    ".css":  "text/css",
    ".js":   "application/javascript",
    ".json": "application/json",
    ".svg":  "image/svg+xml",
    ".png":  "image/png",
    ".ico":  "image/x-icon",
}

# ── 기사 초안 생성 헬퍼 (인라인) ──────────────────────────────────────────────
_ARTICLE_MODELS = [
    "gemini-3.1-pro-preview",
    "gemini-2.5-pro-preview-05-06",
    "gemini-2.5-pro-preview-06-05",
    "gemini-2.5-pro",
]
_LEAD_TYPE_KO = {
    "strategy_change": "경영전략 변화",
    "market_shift":    "시장 변화",
    "risk_alert":      "리스크 경보",
    "numeric_change":  "수치 급변",
    "supply_chain":    "공급망 변화",
}
_ARTICLE_STYLE_GUIDE = """[기사 작성 지침]
- 매체: 파이낸스코프 (finscope.co.kr) / 기자: 고종민
- 문체: 연합뉴스·중앙일보 경제면 스타일 (간결, 객관, 사실 중심)
- 제목: 30자 이내, 핵심 사실 중심 (과장 금지)
- 부제: 15자 이내
- 본문: 400~600자, 역피라미드 구조 (리드→구체내용→의미→전망)
- 주의: DART 공시 내용만 근거, 추측 시 '~것으로 분석된다' 표현"""


def _build_article_prompt(lead, ai_result: str) -> str:
    corp_name = lead["corp_name"] or "해당 기업"
    type_ko   = _LEAD_TYPE_KO.get(lead["lead_type"], lead["lead_type"])
    try:
        kw_list = ", ".join(json.loads(lead["keywords"] or "[]"))
    except Exception:
        kw_list = lead["keywords"] or ""
    return f"""당신은 파이낸스코프(finscope.co.kr)의 경제 전문 기자 고종민입니다.
아래 DART 공시 분석 데이터를 바탕으로 한국어 경제 기사 초안을 작성하세요.

{_ARTICLE_STYLE_GUIDE}

═══════════════════════════════
[취재 단서]
기업명: {corp_name}
단서 유형: {type_ko} (severity {lead['severity']}/5)
감지 키워드: {kw_list}
단서 제목: {lead['title'] or ''}
핵심 요약: {lead['summary'] or ''}
근거 문장: {lead['evidence'] or ''}

[AI 분석 참고 (최대 2500자)]
{(ai_result or '')[:2500]}
═══════════════════════════════

반드시 아래 JSON 형식으로만 출력하세요:
{{
  "headline": "기사 제목 (30자 이내)",
  "subheadline": "부제 (15자 이내)",
  "body": "본문 (400~600자, 단락 사이 빈 줄)",
  "keywords": ["키워드1", "키워드2", "키워드3"],
  "news_value": "취재 가치 한 줄 설명",
  "caution": "확인 필요 사항 (없으면 빈 문자열)"
}}"""


def _call_gemini_article(prompt: str, timeout: int = 120) -> tuple:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError("GEMINI_API_KEY 없음")
    last_err = None
    for model_name in _ARTICLE_MODELS:
        url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
               f"{model_name}:generateContent?key={api_key}")
        payload = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"maxOutputTokens": 4096, "temperature": 0.7},
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data["candidates"][0]["content"]["parts"][0]["text"], model_name
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 404:
                last_err = f"모델 없음: {model_name}"; continue
            if e.code == 429:
                raise RuntimeError(f"API 할당량 초과(429) — 내일 다시 시도하세요.")
            raise RuntimeError(f"Gemini API 오류 {e.code}: {body[:300]}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"네트워크 오류: {e.reason}")
    raise RuntimeError(f"사용 가능한 모델 없음. 마지막 오류: {last_err}")


def _parse_article_json(text: str) -> dict:
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m: text = m.group(1)
    else:
        m2 = re.search(r"(\{.*\})", text, re.DOTALL)
        if m2: text = m2.group(1)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        result = {}
        for field in ["headline", "subheadline", "body", "news_value", "caution"]:
            fm = re.search(rf'"{field}"\s*:\s*"(.*?)"(?=\s*[,\}}])', text, re.DOTALL)
            if fm: result[field] = fm.group(1).replace("\\n", "\n").strip()
        km = re.search(r'"keywords"\s*:\s*\[(.*?)\]', text, re.DOTALL)
        if km: result["keywords"] = re.findall(r'"([^"]+)"', km.group(1))
        return result


def _save_article_draft(conn, lead_id: int, lead, article: dict, model_name: str) -> int:
    headline    = (article.get("headline") or "")[:200]
    subheadline = (article.get("subheadline") or "")[:200]
    body        = article.get("body") or ""
    news_value  = article.get("news_value") or ""
    caution     = article.get("caution") or ""
    editor_note = ""
    if news_value: editor_note += f"[취재 가치] {news_value}\n"
    if caution:    editor_note += f"[주의] {caution}"
    cur = conn.execute("""
        INSERT INTO article_drafts
            (lead_id, corp_code, corp_name,
             headline, subheadline, content,
             style, model, word_count, char_count,
             status, editor_note, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,'draft',?,datetime('now','localtime'))
    """, (lead_id, lead["corp_code"], lead["corp_name"],
          headline, subheadline, body,
          "news", model_name,
          len(body.split()), len(body), editor_note.strip()))
    conn.commit()
    return cur.lastrowid


# ── HTTP 핸들러 ────────────────────────────────────────────────────────────
class DartHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass  # BaseHTTP 기본 로그 억제

    def _send(self, code: int, body: bytes, ct: str = "application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, data, code: int = 200):
        body = json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        self._send(code, body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Api-Key")
        self.end_headers()

    def _check_api_key(self) -> bool:
        expected = os.environ.get("API_SECRET_KEY", "")
        provided = self.headers.get("X-Api-Key", "")
        return not expected or secrets.compare_digest(expected, provided)

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        qs     = parse_qs(parsed.query)

        # ── /api/session (로컬호스트 전용 — API 키 반환) ─────────────────
        if path == "/api/session":
            client_ip = self.client_address[0]
            if client_ip not in ("127.0.0.1", "::1"):
                self._json({"error": "로컬에서만 접근 가능"}, 403)
                return
            self._json({
                "api_key": os.environ.get("API_SECRET_KEY", ""),
                "has_gemini": bool(os.environ.get("GEMINI_API_KEY")),
                "has_claude": bool(os.environ.get("ANTHROPIC_API_KEY")),
            })
            return

        # ── /api/dart/stats ───────────────────────────────────────────────
        if path == "/api/dart/stats":
            try:
                db = get_db()
                total       = db.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
                with_content = db.execute(
                    "SELECT COUNT(*) FROM reports WHERE "
                    "(raw_text IS NOT NULL AND raw_text != '') OR "
                    "(biz_content IS NOT NULL AND biz_content != '')"
                ).fetchone()[0]
                companies   = db.execute(
                    "SELECT COUNT(DISTINCT corp_code) FROM reports WHERE rcept_no != '' AND "
                    "((raw_text IS NOT NULL AND raw_text != '') OR "
                    " (biz_content IS NOT NULL AND biz_content != ''))"
                ).fetchone()[0]
                supply_chain = db.execute("SELECT COUNT(*) FROM supply_chain").fetchone()[0]
                annual  = db.execute("SELECT COUNT(*) FROM reports WHERE report_type='2025_annual'").fetchone()[0]
                q3      = db.execute("SELECT COUNT(*) FROM reports WHERE report_type='2025_q3'").fetchone()[0]
                h1      = db.execute("SELECT COUNT(*) FROM reports WHERE report_type='2025_h1'").fetchone()[0]
                q1      = db.execute("SELECT COUNT(*) FROM reports WHERE report_type='2025_q1'").fetchone()[0]
                old_a   = db.execute("SELECT COUNT(*) FROM reports WHERE report_type='A'").fetchone()[0]
                self._json({
                    "total": total,
                    "with_text": with_content,
                    "companies": companies,
                    "supply_chain": supply_chain,
                    "annual_2025": annual,
                    "q3_2025": q3,
                    "h1_2025": h1,
                    "q1_2025": q1,
                    "legacy_A": old_a,
                })
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/report_types ─────────────────────────────────────────────
        # 소프트코딩: DB의 report_type_meta + 실제 수집 현황 합산 반환
        if path == "/api/report_types":
            try:
                db = get_db()
                # report_type_meta 등록 유형
                meta_rows = db.execute("""
                    SELECT * FROM report_type_meta ORDER BY sort_order
                """).fetchall()
                # 실제 DB에 수집된 유형별 건수
                counts = {r["report_type"]: r["cnt"] for r in db.execute("""
                    SELECT report_type,
                           COUNT(*) as cnt
                    FROM reports
                    WHERE biz_content IS NOT NULL AND LENGTH(biz_content) > 100
                    GROUP BY report_type
                """).fetchall()}
                # 합산
                result = []
                for m in meta_rows:
                    tc = m["type_code"]
                    result.append({
                        **dict(m),
                        "count": counts.get(tc, 0),
                        "has_data": counts.get(tc, 0) > 0,
                    })
                # 메타에 없지만 실제 수집된 유형 추가 (레거시 등)
                meta_codes = {m["type_code"] for m in meta_rows}
                for tc, cnt in counts.items():
                    if tc not in meta_codes:
                        result.append({
                            "type_code": tc, "label": tc, "short_label": tc,
                            "year": None, "quarter": None,
                            "count": cnt, "has_data": True, "is_active": 1,
                        })
                self._json({"report_types": result})
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/dart/latest_type ─────────────────────────────────────────
        if path == "/api/dart/latest_type":
            try:
                db = get_db()
                # 가장 최근 rcept_dt를 기준으로 보고서 유형 결정 (동적)
                row = db.execute("""
                    SELECT report_type, MAX(rcept_dt) as latest_dt
                    FROM reports
                    WHERE rcept_no != ''
                      AND biz_content IS NOT NULL AND LENGTH(biz_content) > 100
                    GROUP BY report_type
                    ORDER BY latest_dt DESC
                    LIMIT 1
                """).fetchone()
                if row:
                    self._json({"report_type": row["report_type"], "rcept_dt": row["latest_dt"]})
                else:
                    self._json({"report_type": None})
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/ai/usage ─────────────────────────────────────────────────
        if path == "/api/ai/usage":
            try:
                usage = load_ai_usage()
                self._json({
                    "date":   usage["date"],
                    "flash":      {"used": usage.get("flash_used",      0), "limit": 500},
                    "flash-lite": {"used": usage.get("flash_lite_used", 0), "limit": 1000},
                    "pro":        {"used": usage.get("pro_used",        0), "limit": 100},
                    "sonnet":     {"used": usage.get("sonnet_used",     0), "limit": None},
                })
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/dart/companies ───────────────────────────────────────────
        if path == "/api/dart/companies":
            try:
                db = get_db()
                q      = (qs.get("q",    [None])[0] or "").strip()
                rtype  = (qs.get("type", [None])[0] or "").strip()
                limit  = int(qs.get("limit",  [100])[0])
                offset = int(qs.get("offset", [0])[0])

                where = [
                    "rcept_no != ''",
                    "((raw_text IS NOT NULL AND raw_text != '') OR "
                    " (biz_content IS NOT NULL AND biz_content != ''))",
                ]
                params = []

                if q:
                    where.append("(corp_name LIKE ? OR corp_code LIKE ?)")
                    params += [f"%{q}%", f"%{q}%"]

                if rtype:
                    where.append("report_type = ?")
                    params.append(rtype)

                where_sql = " AND ".join(where)

                count_sql = f"SELECT COUNT(DISTINCT corp_code) FROM reports WHERE {where_sql}"
                total = db.execute(count_sql, params).fetchone()[0]

                sql = f"""
                    SELECT
                        corp_code,
                        MAX(corp_name) as corp_name,
                        COUNT(*) as report_cnt,
                        SUM(CASE WHEN report_type='2025_annual' THEN 1 ELSE 0 END) as annual_cnt,
                        SUM(CASE WHEN report_type='2025_q3'     THEN 1 ELSE 0 END) as q3_cnt,
                        SUM(CASE WHEN report_type='2025_h1'     THEN 1 ELSE 0 END) as h1_cnt,
                        SUM(CASE WHEN report_type='2025_q1'     THEN 1 ELSE 0 END) as q1_cnt,
                        SUM(CASE WHEN report_type='A'           THEN 1 ELSE 0 END) as legacy_cnt,
                        MAX(rcept_dt) as last_rcept_dt
                    FROM reports
                    WHERE {where_sql}
                    GROUP BY corp_code
                    ORDER BY
                        CASE WHEN MAX(corp_name) IS NULL OR MAX(corp_name) = '' THEN 1 ELSE 0 END,
                        MAX(corp_name) COLLATE NOCASE
                    LIMIT ? OFFSET ?
                """
                rows = db.execute(sql, params + [limit, offset]).fetchall()
                companies = [dict(r) for r in rows]
                self._json({"companies": companies, "total": total, "offset": offset, "limit": limit})
            except Exception as e:
                log.exception("companies error")
                self._json({"error": str(e)}, 500)
            return

        # ── /api/dart/reports ─────────────────────────────────────────────
        if path == "/api/dart/reports":
            try:
                db = get_db()
                corp_code = (qs.get("corp_code", [None])[0] or "").strip()
                rtype     = (qs.get("type",      [None])[0] or "").strip()
                limit     = int(qs.get("limit",  [50])[0])
                offset    = int(qs.get("offset", [0])[0])

                if not corp_code:
                    self._json({"error": "corp_code 필수"}, 400)
                    return

                has_content = (
                    "((raw_text IS NOT NULL AND raw_text != '') OR "
                    " (biz_content IS NOT NULL AND biz_content != ''))"
                )
                where = [f"corp_code = ?", has_content]
                params = [corp_code]
                if rtype:
                    where.append("report_type = ?")
                    params.append(rtype)

                where_sql = " AND ".join(where)
                rows = db.execute(f"""
                    SELECT
                        id, corp_code, corp_name, report_type, report_name,
                        rcept_no, rcept_dt, flr_nm,
                        LENGTH(raw_text)    as text_len,
                        LENGTH(biz_content) as biz_len,
                        CASE WHEN biz_content IS NOT NULL AND LENGTH(biz_content) >= 300
                             THEN 1 ELSE 0 END as has_biz,
                        SUBSTR(COALESCE(biz_content, raw_text, ''), 1, 300) as preview
                    FROM reports
                    WHERE {where_sql}
                    ORDER BY rcept_dt DESC
                    LIMIT ? OFFSET ?
                """, params + [limit, offset]).fetchall()

                reports = [dict(r) for r in rows]
                self._json({"reports": reports, "corp_code": corp_code})
            except Exception as e:
                log.exception("reports error")
                self._json({"error": str(e)}, 500)
            return

        # ── /api/dart/report ──────────────────────────────────────────────
        if path == "/api/dart/report":
            try:
                db = get_db()
                rcept_no = (qs.get("rcept_no", [None])[0] or "").strip()
                rec_id   = (qs.get("id",       [None])[0] or "").strip()

                if rcept_no:
                    row = db.execute(
                        "SELECT * FROM reports WHERE rcept_no = ?", [rcept_no]
                    ).fetchone()
                elif rec_id:
                    row = db.execute(
                        "SELECT * FROM reports WHERE id = ?", [int(rec_id)]
                    ).fetchone()
                else:
                    self._json({"error": "rcept_no 또는 id 필수"}, 400)
                    return

                if not row:
                    self._json({"error": "보고서 없음"}, 404)
                    return
                self._json(dict(row))
            except Exception as e:
                log.exception("report error")
                self._json({"error": str(e)}, 500)
            return

        # ── /api/settings/status ─────────────────────────────────────────
        if path == "/api/settings/status":
            client_ip = self.client_address[0]
            if client_ip not in ("127.0.0.1", "::1"):
                self._json({"error": "로컬에서만 접근 가능"}, 403)
                return
            gemini_key  = os.environ.get("GEMINI_API_KEY",    "")
            claude_key  = os.environ.get("ANTHROPIC_API_KEY", "")
            self._json({
                "has_gemini":  bool(gemini_key),
                "has_claude":  bool(claude_key),
                "gemini_hint":  (gemini_key[:8] + "…")  if gemini_key  else "",
                "claude_hint":  (claude_key[:10] + "…") if claude_key  else "",
            })
            return

        # ── /api/search ───────────────────────────────────────────────────
        # FTS5 전문 검색: 기업명 + 사업내용 키워드
        if path == "/api/search":
            try:
                db = get_db()
                q       = (qs.get("q",      [None])[0] or "").strip()
                rtype   = (qs.get("type",   [None])[0] or "").strip()
                sector  = (qs.get("sector", [None])[0] or "").strip()
                limit   = int(qs.get("limit",  [20])[0])
                offset  = int(qs.get("offset", [0])[0])

                if not q:
                    self._json({"error": "q(검색어) 필수"}, 400)
                    return

                # FTS5 쿼리 — 기업명 OR 사업내용
                fts_q = q.replace('"', '""')
                where_extra = ""
                params_extra = []
                if rtype:
                    where_extra += " AND r.report_type = ?"
                    params_extra.append(rtype)

                rows = db.execute(f"""
                    SELECT r.id, r.corp_code, r.corp_name, r.report_type,
                           r.report_name, r.rcept_dt,
                           LENGTH(r.biz_content) as biz_len,
                           snippet(reports_fts, 2, '<mark>', '</mark>', '...', 32) as snippet
                    FROM reports_fts f
                    JOIN reports r ON r.id = f.rowid
                    WHERE reports_fts MATCH ?
                      {where_extra}
                    ORDER BY rank
                    LIMIT ? OFFSET ?
                """, [f'"{fts_q}"'] + params_extra + [limit, offset]).fetchall()

                # 전체 수 (근사)
                total_row = db.execute(f"""
                    SELECT COUNT(*) FROM reports_fts f
                    JOIN reports r ON r.id = f.rowid
                    WHERE reports_fts MATCH ?
                    {where_extra}
                """, [f'"{fts_q}"'] + params_extra).fetchone()
                total = total_row[0] if total_row else 0

                self._json({
                    "results": [dict(r) for r in rows],
                    "total": total,
                    "query": q,
                    "offset": offset,
                    "limit": limit,
                })
            except Exception as e:
                log.exception("search error")
                self._json({"error": str(e)}, 500)
            return

        # ── /api/alerts ────────────────────────────────────────────────────
        # 변화 알림 목록 (심각도순)
        if path == "/api/alerts":
            try:
                db = get_db()
                min_sev  = int(qs.get("min_severity", [3])[0])
                status   = (qs.get("status", ["new"])[0] or "new").strip()
                lead_type= (qs.get("lead_type",[None])[0] or "").strip()
                limit    = int(qs.get("limit",  [50])[0])
                offset   = int(qs.get("offset", [0])[0])

                where = ["severity >= ?", "status = ?"]
                params = [min_sev, status]
                if lead_type:
                    where.append("lead_type = ?")
                    params.append(lead_type)

                where_sql = " AND ".join(where)
                try:
                    total = db.execute(
                        f"SELECT COUNT(*) FROM story_leads WHERE {where_sql}", params
                    ).fetchone()[0]
                    rows = db.execute(
                        f"SELECT * FROM story_leads WHERE {where_sql} "
                        f"ORDER BY severity DESC, created_at DESC LIMIT ? OFFSET ?",
                        params + [limit, offset]
                    ).fetchall()
                    items = [dict(r) for r in rows]
                except Exception:
                    total, items = 0, []

                # 심각도별 통계
                try:
                    stats = db.execute("""
                        SELECT severity, lead_type, status, COUNT(*) as cnt
                        FROM story_leads
                        GROUP BY severity, lead_type, status
                        ORDER BY severity DESC
                    """).fetchall()
                    stats_list = [dict(r) for r in stats]
                except Exception:
                    stats_list = []

                self._json({
                    "alerts": items,
                    "total": total,
                    "stats": stats_list,
                    "offset": offset,
                    "limit": limit,
                })
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/leads ─────────────────────────────────────────────────────
        if path == "/api/leads":
            try:
                db = get_db()
                corp_code = (qs.get("corp_code",[None])[0] or "").strip()
                status    = (qs.get("status",  [None])[0] or "").strip()
                lead_type = (qs.get("type",    [None])[0] or "").strip()
                min_sev   = int(qs.get("min_severity",[1])[0])
                limit     = int(qs.get("limit", [30])[0])
                offset    = int(qs.get("offset",[0])[0])

                where = ["severity >= ?"]
                params = [min_sev]
                if corp_code:
                    where.append("corp_code = ?"); params.append(corp_code)
                if status:
                    where.append("status = ?"); params.append(status)
                if lead_type:
                    where.append("lead_type = ?"); params.append(lead_type)

                where_sql = " AND ".join(where)
                try:
                    total = db.execute(
                        f"SELECT COUNT(*) FROM story_leads WHERE {where_sql}", params
                    ).fetchone()[0]
                    rows = db.execute(
                        f"SELECT * FROM story_leads WHERE {where_sql} "
                        f"ORDER BY severity DESC, created_at DESC LIMIT ? OFFSET ?",
                        params + [limit, offset]
                    ).fetchall()
                    self._json({"leads": [dict(r) for r in rows], "total": total})
                except Exception:
                    self._json({"leads": [], "total": 0})
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/articles ──────────────────────────────────────────────────
        if path == "/api/articles":
            try:
                db = get_db()
                corp_code = (qs.get("corp_code",[None])[0] or "").strip()
                status    = (qs.get("status",  [None])[0] or "").strip()
                limit     = int(qs.get("limit", [20])[0])
                offset    = int(qs.get("offset",[0])[0])

                where = []
                params = []
                if corp_code:
                    where.append("corp_code = ?"); params.append(corp_code)
                if status:
                    where.append("status = ?"); params.append(status)

                where_sql = ("WHERE " + " AND ".join(where)) if where else ""
                try:
                    total = db.execute(
                        f"SELECT COUNT(*) FROM article_drafts {where_sql}", params
                    ).fetchone()[0]
                    rows = db.execute(
                        f"SELECT id, lead_id, corp_code, corp_name, headline, subheadline, "
                        f"style, model, word_count, status, created_at, "
                        f"SUBSTR(content, 1, 300) as content_preview "
                        f"FROM article_drafts {where_sql} "
                        f"ORDER BY created_at DESC LIMIT ? OFFSET ?",
                        params + [limit, offset]
                    ).fetchall()
                    self._json({"articles": [dict(r) for r in rows], "total": total})
                except Exception:
                    self._json({"articles": [], "total": 0})
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/stats/dashboard ───────────────────────────────────────────
        if path == "/api/stats/dashboard":
            try:
                db = get_db()
                def safe(sql, params=[], default=0):
                    try: return db.execute(sql, params).fetchone()[0]
                    except: return default

                self._json({
                    "reports":       safe("SELECT COUNT(*) FROM reports WHERE biz_content IS NOT NULL"),
                    "companies":     safe("SELECT COUNT(DISTINCT corp_code) FROM reports WHERE biz_content IS NOT NULL"),
                    "comparisons":   safe("SELECT COUNT(*) FROM ai_comparisons WHERE status='ok'"),
                    "supply_chain":  safe("SELECT COUNT(*) FROM supply_chain"),
                    "story_leads":   safe("SELECT COUNT(*) FROM story_leads"),
                    "leads_new":     safe("SELECT COUNT(*) FROM story_leads WHERE status='new'"),
                    "leads_high":    safe("SELECT COUNT(*) FROM story_leads WHERE severity >= 4"),
                    "articles":      safe("SELECT COUNT(*) FROM article_drafts"),
                    "report_types":  [dict(r) for r in db.execute(
                        "SELECT type_code, label, short_label, is_active FROM report_type_meta ORDER BY sort_order"
                    ).fetchall()],
                })
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/alert_rules ──────────────────────────────────────────────
        if path == "/api/alert_rules":
            try:
                db = get_db()
                rows = db.execute(
                    "SELECT * FROM alert_rules ORDER BY severity DESC, rule_code"
                ).fetchall()
                self._json({"rules": [dict(r) for r in rows]})
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── /api/ai/comparisons ──────────────────────────────────────────
        if path == "/api/ai/comparisons":
            try:
                db = get_db()
                corp_code = (qs.get("corp_code", [None])[0] or "").strip()
                type_a    = (qs.get("type_a",    [None])[0] or "").strip()
                type_b    = (qs.get("type_b",    [None])[0] or "").strip()
                model     = (qs.get("model",     [None])[0] or "").strip()
                status    = (qs.get("status",    ["ok"])[0] or "ok").strip()
                limit     = int(qs.get("limit",  [20])[0])
                offset    = int(qs.get("offset", [0])[0])

                where = ["status = ?"]
                params = [status]
                if corp_code:
                    where.append("corp_code = ?")
                    params.append(corp_code)
                if type_a:
                    where.append("report_type_a = ?")
                    params.append(type_a)
                if type_b:
                    where.append("report_type_b = ?")
                    params.append(type_b)
                if model:
                    where.append("model = ?")
                    params.append(model)

                where_sql = "WHERE " + " AND ".join(where)

                # ai_comparisons 테이블이 없으면 빈 결과 반환
                try:
                    total = db.execute(
                        f"SELECT COUNT(*) FROM ai_comparisons {where_sql}", params
                    ).fetchone()[0]
                    rows = db.execute(
                        f"SELECT id, corp_code, corp_name, report_type_a, report_type_b, "
                        f"model, char_count_a, char_count_b, status, analyzed_at, "
                        f"SUBSTR(result, 1, 500) as result_preview "
                        f"FROM ai_comparisons {where_sql} "
                        f"ORDER BY analyzed_at DESC LIMIT ? OFFSET ?",
                        params + [limit, offset]
                    ).fetchall()
                    items = [dict(r) for r in rows]
                except sqlite3.OperationalError:
                    total = 0
                    items = []

                # 전체 통계
                try:
                    stats = db.execute("""
                        SELECT report_type_a, report_type_b, model,
                               COUNT(*) as total,
                               SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) as ok_cnt,
                               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) as err_cnt
                        FROM ai_comparisons
                        GROUP BY report_type_a, report_type_b, model
                        ORDER BY report_type_a, report_type_b, model
                    """).fetchall()
                    stats_list = [dict(r) for r in stats]
                except sqlite3.OperationalError:
                    stats_list = []

                self._json({
                    "comparisons": items,
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                    "stats": stats_list,
                })
            except Exception as e:
                log.exception("comparisons error")
                self._json({"error": str(e)}, 500)
            return

        # ── /api/ai/comparison_detail ─────────────────────────────────────
        if path == "/api/ai/comparison_detail":
            try:
                db = get_db()
                comp_id   = (qs.get("id",        [None])[0] or "").strip()
                corp_code = (qs.get("corp_code",  [None])[0] or "").strip()
                type_a    = (qs.get("type_a",     [None])[0] or "").strip()
                type_b    = (qs.get("type_b",     [None])[0] or "").strip()
                model     = (qs.get("model",      ["flash"])[0] or "flash").strip()

                try:
                    if comp_id:
                        row = db.execute(
                            "SELECT * FROM ai_comparisons WHERE id=?", [int(comp_id)]
                        ).fetchone()
                    elif corp_code and type_a and type_b:
                        row = db.execute("""
                            SELECT * FROM ai_comparisons
                            WHERE corp_code=? AND report_type_a=? AND report_type_b=? AND model=?
                            ORDER BY analyzed_at DESC LIMIT 1
                        """, [corp_code, type_a, type_b, model]).fetchone()
                    else:
                        self._json({"error": "id 또는 (corp_code+type_a+type_b) 필수"}, 400)
                        return

                    if not row:
                        self._json({"error": "분석 결과 없음"}, 404)
                        return
                    self._json(dict(row))
                except sqlite3.OperationalError:
                    self._json({"error": "ai_comparisons 테이블 없음 (배치 분석을 먼저 실행하세요)"}, 404)
            except Exception as e:
                log.exception("comparison_detail error")
                self._json({"error": str(e)}, 500)
            return

        # ── /api/dart/supply_chain ────────────────────────────────────────
        if path == "/api/dart/supply_chain":
            try:
                db = get_db()
                corp_code  = (qs.get("corp_code",    [None])[0] or "").strip()
                partner    = (qs.get("partner",      [None])[0] or "").strip()
                rel_type   = (qs.get("relation",     [None])[0] or "").strip()
                limit      = int(qs.get("limit",  [100])[0])
                offset     = int(qs.get("offset", [0])[0])

                where = []
                params = []
                if corp_code:
                    where.append("corp_code = ?")
                    params.append(corp_code)
                if partner:
                    where.append("partner_name LIKE ?")
                    params.append(f"%{partner}%")
                if rel_type:
                    where.append("relation_type = ?")
                    params.append(rel_type)

                where_sql = ("WHERE " + " AND ".join(where)) if where else ""
                rows = db.execute(
                    f"SELECT * FROM supply_chain {where_sql} "
                    f"ORDER BY corp_name, relation_type LIMIT ? OFFSET ?",
                    params + [limit, offset]
                ).fetchall()

                total = db.execute(
                    f"SELECT COUNT(*) FROM supply_chain {where_sql}", params
                ).fetchone()[0]

                self._json({
                    "supply_chain": [dict(r) for r in rows],
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                })
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── 정적 파일 서빙 ────────────────────────────────────────────────
        if path == "/" or path == "":
            path = "/dart_viewer.html"

        file_path = WEB_ROOT / path.lstrip("/")

        try:
            file_path.resolve().relative_to(WEB_ROOT.resolve())
        except ValueError:
            self._send(403, b"Forbidden", "text/plain")
            return

        if file_path.is_file():
            ext = file_path.suffix.lower()
            ct  = MIME.get(ext, "application/octet-stream")
            self._send(200, file_path.read_bytes(), ct)
        else:
            self._send(404, b"Not Found", "text/plain")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # ── POST /api/ai/analyze ──────────────────────────────────────────
        if path == "/api/ai/analyze":
            # API 키 인증
            if not self._check_api_key():
                self._json({"error": "인증 실패 — X-Api-Key 헤더를 확인하세요"}, 403)
                return

            try:
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                data = json.loads(body.decode("utf-8"))
            except Exception:
                self._json({"error": "요청 파싱 오류"}, 400)
                return

            model      = data.get("model", "flash")      # flash | pro | sonnet
            report_ids = data.get("report_ids", [])
            focus      = data.get("focus", "").strip()   # 선택적 분석 포커스

            if not report_ids:
                self._json({"error": "report_ids 필수"}, 400)
                return
            if len(report_ids) > 8:
                self._json({"error": "한 번에 최대 8개 보고서만 비교 가능"}, 400)
                return

            # 보고서 내용 로드 — biz_content(사업의 내용) 섹션만 사용
            db = get_db()
            reports = []
            skipped_no_biz = []
            for rid in report_ids:
                try:
                    row = db.execute(
                        "SELECT corp_name, report_name, report_type, rcept_dt, biz_content "
                        "FROM reports WHERE id = ?",
                        [int(rid)]
                    ).fetchone()
                except Exception:
                    continue
                if not row:
                    continue

                biz = (row["biz_content"] or "").strip()
                if not biz:
                    skipped_no_biz.append(row["report_name"] or str(rid))
                    continue

                # 접수일 포맷
                dt = row["rcept_dt"] or ""
                if len(dt) == 8:
                    dt = f"{dt[:4]}.{dt[4:6]}.{dt[6:]}"

                label = f"{row['corp_name']} / {row['report_name']} (접수: {dt})"
                # biz_content 최대 60,000자 (사업 내용만 사용하므로 한도 상향)
                reports.append({"label": label, "content": biz[:60000]})

            if not reports:
                msg = "사업의 내용(biz_content) 데이터가 없습니다"
                if skipped_no_biz:
                    msg += f": {', '.join(skipped_no_biz)}"
                self._json({"error": msg}, 404)
                return

            if skipped_no_biz:
                log.warning(f"사업내용 없어 제외된 보고서: {skipped_no_biz}")

            prompt = build_compare_prompt(reports, focus)

            log.info(f"AI 분석 요청: model={model}, 보고서 {len(reports)}개, 포커스='{focus}'")

            try:
                if model in ("flash", "pro"):
                    result = call_gemini(model, prompt)
                elif model == "sonnet":
                    result = call_claude(prompt)
                else:
                    self._json({"error": f"알 수 없는 모델: {model}"}, 400)
                    return

                increment_usage(model)
                usage = load_ai_usage()

                self._json({
                    "result": result,
                    "model": model,
                    "report_count": len(reports),
                    "usage": {
                        "flash":  {"used": usage.get("flash_used",  0), "limit": 1500},
                        "pro":    {"used": usage.get("pro_used",    0), "limit": 25},
                        "sonnet": {"used": usage.get("sonnet_used", 0), "limit": None},
                    }
                })
            except Exception as e:
                log.exception("AI 분석 오류")
                self._json({"error": str(e)}, 500)
            return

        # ── POST /api/leads/{id}/draft ─────────────────────────────────────
        m_draft = re.match(r"^/api/leads/(\d+)/draft$", path)
        if m_draft:
            lead_id = int(m_draft.group(1))
            try:
                db = get_db()
                lead = db.execute("""
                    SELECT sl.*, ac.result as ai_result
                    FROM story_leads sl
                    LEFT JOIN ai_comparisons ac ON sl.comparison_id = ac.id
                    WHERE sl.id = ?
                """, [lead_id]).fetchone()

                if not lead:
                    self._json({"error": f"lead_id={lead_id} 없음"}, 404)
                    return

                # 기사 초안 생성 (인라인)
                prompt  = _build_article_prompt(lead, lead["ai_result"] or "")
                text, model_name = _call_gemini_article(prompt)
                article = _parse_article_json(text)

                if not article.get("headline"):
                    article = {
                        "headline":    lead["title"] or f"{lead['corp_name']} 관련 기사",
                        "subheadline": "",
                        "body":        text[:2000],
                        "keywords":    [],
                        "news_value":  "",
                        "caution":     "자동 파싱 실패 — 편집 필요",
                    }

                draft_id = _save_article_draft(db, lead_id, lead, article, model_name)
                draft = db.execute(
                    "SELECT * FROM article_drafts WHERE id = ?", [draft_id]
                ).fetchone()

                self._json({"ok": True, "draft": dict(draft), "model": model_name})
            except Exception as e:
                log.exception("기사 초안 생성 오류")
                self._json({"error": str(e)}, 500)
            return

        # ── POST /api/leads/{id}/status ────────────────────────────────────
        m_status = re.match(r"^/api/leads/(\d+)/status$", path)
        if m_status:
            lead_id = int(m_status.group(1))
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                data = json.loads(body.decode("utf-8"))
                new_status = data.get("status", "").strip()
                valid = {"new", "reviewing", "drafted", "published", "archived"}
                if new_status not in valid:
                    self._json({"error": f"유효한 status: {valid}"}, 400)
                    return
                db = get_db()
                db.execute(
                    "UPDATE story_leads SET status=?, updated_at=datetime('now','localtime') WHERE id=?",
                    [new_status, lead_id]
                )
                db.commit()
                self._json({"ok": True, "lead_id": lead_id, "status": new_status})
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── POST /api/articles/{id}/status ─────────────────────────────────
        m_art_status = re.match(r"^/api/articles/(\d+)/status$", path)
        if m_art_status:
            art_id = int(m_art_status.group(1))
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                data = json.loads(body.decode("utf-8"))
                new_status = data.get("status", "").strip()
                valid = {"draft", "editing", "ready", "published", "rejected"}
                if new_status not in valid:
                    self._json({"error": f"유효한 status: {valid}"}, 400)
                    return
                db = get_db()
                db.execute(
                    "UPDATE article_drafts SET status=?, updated_at=datetime('now','localtime') WHERE id=?",
                    [new_status, art_id]
                )
                db.commit()
                self._json({"ok": True, "article_id": art_id, "status": new_status})
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        # ── POST /api/settings ────────────────────────────────────────────
        if path == "/api/settings":
            client_ip = self.client_address[0]
            if client_ip not in ("127.0.0.1", "::1"):
                self._json({"error": "로컬에서만 접근 가능"}, 403)
                return
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                data = json.loads(body.decode("utf-8"))
            except Exception:
                self._json({"error": "요청 파싱 오류"}, 400)
                return

            gemini_key   = data.get("gemini_key",   "").strip()
            anthropic_key = data.get("anthropic_key","").strip()
            updated = []

            def _update_env_key(file_key: str, new_val: str, env_key: str):
                """key=value 또는 # key=... 줄을 교체"""
                if not _env_path.exists():
                    return
                lines = _env_path.read_text(encoding="utf-8").splitlines()
                found = False
                new_lines = []
                for ln in lines:
                    stripped = ln.strip()
                    if (stripped.startswith(file_key + "=") or
                            stripped.startswith("# " + file_key + "=") or
                            stripped.startswith("#" + file_key + "=")):
                        new_lines.append(f"{file_key}={new_val}" if new_val else f"# {file_key}=")
                        found = True
                    else:
                        new_lines.append(ln)
                if not found and new_val:
                    new_lines.append(f"{file_key}={new_val}")
                _env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
                if new_val:
                    os.environ[env_key] = new_val
                elif env_key in os.environ:
                    del os.environ[env_key]

            if gemini_key is not None:
                _update_env_key("GEMINI_API_KEY", gemini_key, "GEMINI_API_KEY")
                updated.append("gemini")
                log.info(f"GEMINI_API_KEY {'설정됨' if gemini_key else '삭제됨'}")

            if anthropic_key is not None:
                _update_env_key("ANTHROPIC_API_KEY", anthropic_key, "ANTHROPIC_API_KEY")
                updated.append("anthropic")
                log.info(f"ANTHROPIC_API_KEY {'설정됨' if anthropic_key else '삭제됨'}")

            self._json({
                "ok": True,
                "updated": updated,
                "has_gemini": bool(os.environ.get("GEMINI_API_KEY")),
                "has_claude": bool(os.environ.get("ANTHROPIC_API_KEY")),
            })
            return

        self._json({"error": "Not found"}, 404)


# ── 서버 실행 ──────────────────────────────────────────────────────────────
class ThreadingServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True


def main():
    parser = argparse.ArgumentParser(description="Company Catcher DART 뷰어 서버")
    parser.add_argument("--port", type=int, default=8888)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    if not DB_PATH.exists():
        log.error(f"DB 없음: {DB_PATH}")
        sys.exit(1)

    (ROOT / "logs").mkdir(exist_ok=True)
    _ensure_api_secret_key()

    log.info(f"Company Catcher DART 서버 시작 >> http://localhost:{args.port}")
    log.info(f"DB: {DB_PATH} ({DB_PATH.stat().st_size // 1024 // 1024}MB)")
    log.info(f"Gemini API: {'설정됨' if os.environ.get('GEMINI_API_KEY') else '미설정 (.env에 GEMINI_API_KEY 추가 필요)'}")
    log.info(f"Claude API: {'설정됨' if os.environ.get('ANTHROPIC_API_KEY') else '미설정 (.env에 ANTHROPIC_API_KEY 추가 필요)'}")

    server = ThreadingServer((args.host, args.port), DartHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("서버 종료")
        server.shutdown()


if __name__ == "__main__":
    main()
