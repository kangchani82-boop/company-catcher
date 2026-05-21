"""
scripts/quality_review_articles.py
──────────────────────────────────
기사 초안 + 질문지 페어를 Gemini로 2차 검토 (품질 평가 + 미래가치 관점 보강).

입력 (회사당):
  - article_drafts.headline + content
  - ir_questionnaires.questions (JSON)
  - external_sources 최근 60일 뉴스 8건 (T1/T2)
  - financials 최근 4년 (매출/영익/부채비율/ROE/영업CF)
  - ai_comparisons.result (요약)

출력 (DB article_quality_reviews):
  - score 1~5
  - 재무 건전성 / 미래 가치 평가
  - 질문 개선 제안 (JSON)
  - 헤드라인 앵글 재정의
  - verdict (approve/revise/escalate/discard)

실행:
  python scripts/quality_review_articles.py --sample 5         # 샘플 5건만
  python scripts/quality_review_articles.py --status pending   # pending 전체
  python scripts/quality_review_articles.py --article-id 285   # 특정 1건
  python scripts/quality_review_articles.py --stats            # 진행률만
  python scripts/quality_review_articles.py --workers 2        # 병렬 (기본 2)

호출량: 313건 × Flash-Lite (1,000 RPD/키, 2키 = 2,000 RPD) → 안전.
"""
import argparse
import io
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# .env 로드
if ENV_PATH.exists():
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# Gemini 키 (2개) — batch_compare 와 동일 환경변수 사용
GEMINI_KEYS = []
for env_name in ["GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"]:
    v = os.environ.get(env_name, "").strip()
    if v:
        GEMINI_KEYS.append(v)
if not GEMINI_KEYS:
    print("[오류] GEMINI_API_KEY 미설정")
    sys.exit(1)

# 사용 모델 — Flash-Lite 우선, 429 시 폴백
MODEL_CHAIN = [
    "gemini-2.5-flash-lite",
    "gemini-3-flash-preview",
    "gemini-3.1-flash-lite",
    "gemini-2.5-flash",
]
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
RATE_LIMIT_DELAY = 4.5  # per worker

# 스키마 생성
SCHEMA = """
CREATE TABLE IF NOT EXISTS article_quality_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER NOT NULL UNIQUE,
    questionnaire_id INTEGER,
    corp_code TEXT,
    corp_name TEXT,
    score INTEGER,
    financial_health TEXT,
    future_value TEXT,
    news_summary TEXT,
    suggested_questions TEXT,   -- JSON: [{"q":"...","reason":"..."}]
    suggested_angle TEXT,
    verdict TEXT,               -- approve/revise/escalate/discard
    rationale TEXT,             -- 1~2줄 종합 평가
    news_count INTEGER DEFAULT 0,
    financials_used INTEGER DEFAULT 0,
    model TEXT,
    raw_response TEXT,
    error TEXT,
    reviewed_at TEXT,
    applied INTEGER DEFAULT 0,
    applied_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_qr_article ON article_quality_reviews(article_id);
CREATE INDEX IF NOT EXISTS idx_qr_corp    ON article_quality_reviews(corp_code);
CREATE INDEX IF NOT EXISTS idx_qr_score   ON article_quality_reviews(score);
CREATE INDEX IF NOT EXISTS idx_qr_verdict ON article_quality_reviews(verdict);
"""


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def ensure_schema(db):
    db.executescript(SCHEMA)
    db.commit()


# ── 회사 컨텍스트 수집 ────────────────────────────────────────────────────
def fetch_context(db, art_row) -> dict:
    """기사 1건의 검토용 컨텍스트 — 질문지, 뉴스, 재무, 비교 원본."""
    article_id = art_row["id"]
    corp_code = art_row["corp_code"]
    lead_id = art_row["lead_id"]

    ctx = {
        "article_id": article_id,
        "corp_code": corp_code,
        "corp_name": art_row["corp_name"],
        "headline": art_row["headline"] or "",
        "content": (art_row["content"] or "")[:4000],   # 컨텍스트 절약
    }

    # 질문지
    qst = db.execute(
        "SELECT id, questions FROM ir_questionnaires WHERE article_id=? LIMIT 1",
        [article_id]
    ).fetchone()
    if qst:
        ctx["questionnaire_id"] = qst["id"]
        try:
            qs = json.loads(qst["questions"] or "[]")
            ctx["questions"] = [
                (q.get("q") if isinstance(q, dict) else str(q))
                for q in qs
            ]
        except Exception:
            ctx["questions"] = []
    else:
        ctx["questionnaire_id"] = None
        ctx["questions"] = []

    # 뉴스 (60일, 최대 8건)
    news = db.execute("""
        SELECT outlet_name, outlet_tier, title, summary, published_at
        FROM external_sources
        WHERE source_type='news' AND related_corp_code=?
          AND (published_at IS NULL OR published_at >= date('now','-60 days'))
        ORDER BY COALESCE(published_at, fetched_at) DESC LIMIT 8
    """, [corp_code]).fetchall()
    ctx["news"] = [dict(n) for n in news]

    # 재무 (최근 4 fiscal_year)
    fins = db.execute("""
        SELECT fiscal_year, revenue, operating_income, net_income,
               debt_ratio, operating_margin, roe, operating_cf
        FROM financials WHERE corp_code=?
        ORDER BY fiscal_year DESC LIMIT 4
    """, [corp_code]).fetchall()
    ctx["financials"] = [dict(f) for f in fins]

    # AI 비교 결과 (관련된 가장 최근 1건)
    if lead_id:
        cmp_row = db.execute("""
            SELECT c.result FROM story_leads sl
            JOIN ai_comparisons c ON c.id = sl.comparison_id
            WHERE sl.id=? LIMIT 1
        """, [lead_id]).fetchone()
        ctx["comparison_excerpt"] = (cmp_row["result"][:2500] if cmp_row and cmp_row["result"] else "")
    else:
        ctx["comparison_excerpt"] = ""

    return ctx


# ── 프롬프트 빌드 ─────────────────────────────────────────────────────────
def build_prompt(ctx: dict) -> str:
    qstr = "\n".join(f"  Q{i+1}. {q}" for i, q in enumerate(ctx.get("questions", [])))
    news_str = "\n".join(
        f"  - [{n['outlet_tier']}] {n['outlet_name']} ({n['published_at'] or '?'}): "
        f"{n['title']}{(' — ' + n['summary'][:80]) if n.get('summary') else ''}"
        for n in ctx.get("news", [])
    ) or "  (최근 60일 매칭 뉴스 없음)"

    if ctx["financials"]:
        fin_str = "\n".join(
            f"  {f['fiscal_year']}: 매출 {fmt_won(f['revenue'])} / 영익 {fmt_won(f['operating_income'])} / "
            f"순익 {fmt_won(f['net_income'])} / 부채비율 {fmt_pct(f['debt_ratio'])} / "
            f"영업CF {fmt_won(f['operating_cf'])} / ROE {fmt_pct(f['roe'])}"
            for f in ctx["financials"]
        )
    else:
        fin_str = "  (재무 데이터 없음)"

    cmp_str = ctx["comparison_excerpt"] or "(비교 원본 없음)"

    prompt = f"""당신은 한국 증권부 데스크장입니다. 후배 기자가 작성한 기사 초안과 IR 담당자에게 보낼 질문지를 검토해, 기사 가치와 미래 가치 관점에서 평가하세요.

[회사]
  {ctx['corp_name']} (corp_code: {ctx['corp_code']})

[기사 초안]
  헤드라인: {ctx['headline']}
  본문 (요약):
{textwrap_indent(ctx['content'], '    ')}

[질문지]
{qstr or '  (질문 없음)'}

[회사 최근 60일 뉴스 (T1/T2)]
{news_str}

[재무 스냅샷 (최근 4년)]
{fin_str}

[AI가 잡은 보고서 변화 (발췌)]
{cmp_str[:1500]}

[검토 작업]
다음 6가지를 평가하고 반드시 JSON 형식으로만 답변하세요.

1️⃣ score (1~5 정수)
   5: 즉시 출고 가능 — 시의성·중요도·재무 임팩트 모두 강함
   4: 양호 — 약간의 보강으로 출고 가능
   3: 평범 — 추가 취재 필요
   2: 일반론 — 폐기 권장
   1: 가치 없음 — 기재정정·중복·무의미

2️⃣ financial_health (3줄 이내, 한국어)
   - 현금흐름, 부채, 수익성 단기 위험 신호
   - 뉴스에 신용평가 부정·구조조정·법정관리 signal 있는지

3️⃣ future_value (3줄 이내, 한국어)
   - 투자 계획·신사업·M&A·설비투자 등 미래 성장 signal
   - 뉴스에서 발견한 forward-looking statement

4️⃣ suggested_questions (최대 3개, 한국어)
   각 항목: {{"q": "질문 본문", "reason": "왜 필요한가 1줄"}}
   미래 가치·재무 건전성·투자 회수 관점 우선

5️⃣ suggested_angle (1줄, 한국어)
   현재 헤드라인보다 더 임팩트 있는 각도

6️⃣ verdict
   approve  — 그대로 발송 가능
   revise   — 제안 반영 후 발송 (대부분 이 케이스)
   escalate — 사람이 재검토 필요 (애매)
   discard  — 폐기 권장

7️⃣ rationale (1~2줄)
   종합 평가 요약

[필수 JSON 출력 형식 — 다른 텍스트 금지]
{{"score": 3, "financial_health": "...", "future_value": "...",
  "suggested_questions": [{{"q":"...","reason":"..."}}, ...],
  "suggested_angle": "...", "verdict": "revise", "rationale": "..."}}"""
    return prompt


def fmt_won(v):
    if v is None: return "-"
    try:
        v = float(v)
        if abs(v) >= 1e8:  return f"{v/1e8:.1f}억"
        if abs(v) >= 1e4:  return f"{v/1e4:.1f}만"
        return f"{v:,.0f}"
    except: return str(v)


def fmt_pct(v):
    if v is None: return "-"
    try: return f"{float(v):.1f}%"
    except: return str(v)


def textwrap_indent(text, prefix):
    return "\n".join(prefix + line for line in (text or "").splitlines())


# ── Gemini 호출 ─────────────────────────────────────────────────────────
def call_gemini(prompt: str, key: str, model: str, timeout: int = 90) -> dict:
    url = GEMINI_URL.format(model=model, key=key)
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.3,
            "topP": 0.9,
            "maxOutputTokens": 2048,
            "responseMimeType": "application/json",
        },
    }
    req = urllib.request.Request(
        url, data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        cands = data.get("candidates", [])
        if not cands:
            return {"ok": False, "error": "no_candidates", "raw": data}
        parts = cands[0].get("content", {}).get("parts", [])
        text = "".join(p.get("text", "") for p in parts)
        return {"ok": True, "text": text, "model": model}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {e.code}", "code": e.code, "body": body}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def parse_review(text: str) -> dict | None:
    """Gemini 응답 → dict. JSON mode 라 보통 바로 파싱됨."""
    if not text:
        return None
    # 코드펜스 제거 (혹시 모를)
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.M)
    try:
        return json.loads(text)
    except Exception:
        # 가장 큰 JSON 블록 추출
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try: return json.loads(m.group(0))
            except: return None
    return None


# ── 워커 (per-key) ────────────────────────────────────────────────────────
_locks = {"db": threading.Lock(), "print": threading.Lock()}
_last_call = {}
_dead_models = {0: set(), 1: set(), 2: set()}


def safe_sleep(worker_idx: int):
    """워커별 rate limit (4.5s)."""
    last = _last_call.get(worker_idx, 0)
    wait = RATE_LIMIT_DELAY - (time.time() - last)
    if wait > 0: time.sleep(wait)
    _last_call[worker_idx] = time.time()


def review_one(ctx: dict, worker_idx: int) -> dict:
    """1건 검토 — 모델 chain 시도."""
    key = GEMINI_KEYS[worker_idx % len(GEMINI_KEYS)]
    prompt = build_prompt(ctx)

    for model in MODEL_CHAIN:
        if model in _dead_models.get(worker_idx, set()):
            continue
        safe_sleep(worker_idx)
        res = call_gemini(prompt, key, model)
        if res["ok"]:
            review = parse_review(res["text"])
            if review:
                return {
                    "ok": True, "model": model, "raw": res["text"],
                    "review": review,
                }
            else:
                return {"ok": False, "error": "parse_fail",
                        "raw": res["text"], "model": model}
        if res.get("code") == 429:
            with _locks["print"]:
                print(f"  ⚠ [W{worker_idx}] {model} 429 — 다음 모델")
            _dead_models[worker_idx].add(model)
            continue
        if res.get("code") in (400, 404):
            with _locks["print"]:
                print(f"  ⚠ [W{worker_idx}] {model} {res['error']} — 다음 모델")
            continue
        # 기타 오류 → 즉시 실패
        return {"ok": False, "error": res.get("error", "unknown"), "model": model}

    return {"ok": False, "error": "all_models_exhausted"}


def save_review(db, ctx: dict, result: dict):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if result["ok"]:
        r = result["review"]
        suggested_q = json.dumps(r.get("suggested_questions", []), ensure_ascii=False)
        with _locks["db"]:
            db.execute("""
                INSERT INTO article_quality_reviews
                  (article_id, questionnaire_id, corp_code, corp_name,
                   score, financial_health, future_value, news_summary,
                   suggested_questions, suggested_angle, verdict, rationale,
                   news_count, financials_used, model, raw_response,
                   reviewed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(article_id) DO UPDATE SET
                  score=excluded.score,
                  financial_health=excluded.financial_health,
                  future_value=excluded.future_value,
                  suggested_questions=excluded.suggested_questions,
                  suggested_angle=excluded.suggested_angle,
                  verdict=excluded.verdict,
                  rationale=excluded.rationale,
                  model=excluded.model,
                  raw_response=excluded.raw_response,
                  reviewed_at=excluded.reviewed_at,
                  error=NULL
            """, [
                ctx["article_id"], ctx["questionnaire_id"],
                ctx["corp_code"], ctx["corp_name"],
                int(r.get("score", 3) or 3),
                r.get("financial_health", ""),
                r.get("future_value", ""),
                r.get("news_summary", ""),
                suggested_q,
                r.get("suggested_angle", ""),
                r.get("verdict", "escalate"),
                r.get("rationale", ""),
                len(ctx["news"]),
                1 if ctx["financials"] else 0,
                result["model"], result["raw"], now,
            ])
            db.commit()
    else:
        with _locks["db"]:
            db.execute("""
                INSERT OR REPLACE INTO article_quality_reviews
                  (article_id, questionnaire_id, corp_code, corp_name,
                   error, model, reviewed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [ctx["article_id"], ctx["questionnaire_id"],
                  ctx["corp_code"], ctx["corp_name"],
                  result.get("error", "?"), result.get("model"), now])
            db.commit()


# ── 메인 ─────────────────────────────────────────────────────────────────
def fetch_targets(db, status_filter: str | None, article_id: int | None,
                  redo: bool, limit: int) -> list:
    where = []
    params = []
    if article_id:
        where.append("a.id=?")
        params.append(article_id)
    else:
        if status_filter:
            where.append("q.status=?")
            params.append(status_filter)
        if not redo:
            where.append("NOT EXISTS (SELECT 1 FROM article_quality_reviews qr WHERE qr.article_id=a.id AND qr.error IS NULL)")

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    sql = f"""
        SELECT a.id, a.lead_id, a.corp_code, a.corp_name,
               a.headline, a.content, q.status
        FROM article_drafts a
        LEFT JOIN ir_questionnaires q ON q.article_id = a.id
        {where_sql}
        ORDER BY a.id DESC
        LIMIT ?
    """
    params.append(limit)
    return db.execute(sql, params).fetchall()


def print_stats(db):
    print("=" * 70)
    print("  품질 재검토 진행률")
    print("=" * 70)
    total = db.execute("SELECT COUNT(*) FROM article_quality_reviews").fetchone()[0]
    ok = db.execute("SELECT COUNT(*) FROM article_quality_reviews WHERE error IS NULL").fetchone()[0]
    err = total - ok
    print(f"  검토 누적: {total} (성공 {ok} / 오류 {err})")

    print("\n  [score 분포]")
    for r in db.execute("""SELECT score, COUNT(*) c FROM article_quality_reviews
                           WHERE error IS NULL GROUP BY score ORDER BY score DESC"""):
        print(f"    score {r['score']}: {r['c']}건")

    print("\n  [verdict 분포]")
    for r in db.execute("""SELECT verdict, COUNT(*) c FROM article_quality_reviews
                           WHERE error IS NULL GROUP BY verdict"""):
        print(f"    {r['verdict']:<10}: {r['c']}건")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", default="pending",
                    help="질문지 status 필터 (기본 pending)")
    ap.add_argument("--article-id", type=int, help="단일 article_id")
    ap.add_argument("--sample", type=int, help="샘플 N건만 실행")
    ap.add_argument("--limit", type=int, default=1000, help="최대 처리")
    ap.add_argument("--workers", type=int, default=2, help="병렬 워커 (키 수)")
    ap.add_argument("--redo", action="store_true", help="이미 검토된 것도 재검토")
    ap.add_argument("--stats", action="store_true", help="진행률만 출력")
    args = ap.parse_args()

    db = get_db()
    ensure_schema(db)

    if args.stats:
        print_stats(db); return

    limit = args.sample if args.sample else args.limit
    targets = fetch_targets(db, args.status, args.article_id, args.redo, limit)
    if not targets:
        print("[info] 처리할 대상 없음")
        print_stats(db); return

    print(f"[info] 대상 {len(targets)}건, 워커 {args.workers}, 키 {len(GEMINI_KEYS)}개")
    print(f"[info] 예상 시간: {len(targets) * RATE_LIMIT_DELAY / args.workers / 60:.1f}분\n")

    stats = {"ok": 0, "err": 0}
    stats_lock = threading.Lock()

    def worker(idx_target):
        idx, art = idx_target
        # 워커별 독립 DB 커넥션 (SQLite 스레드 안전)
        tdb = get_db()
        try:
            ctx = fetch_context(tdb, art)
            result = review_one(ctx, idx % args.workers)
            save_review(tdb, ctx, result)
        finally:
            tdb.close()
        with stats_lock:
            if result["ok"]:
                stats["ok"] += 1
                with _locks["print"]:
                    r = result["review"]
                    print(f"  [{stats['ok']+stats['err']:>3}/{len(targets)}] "
                          f"#{ctx['article_id']} {ctx['corp_name'][:18]:<18} "
                          f"score={r.get('score','?')} verdict={r.get('verdict','?'):<8} "
                          f"({result['model']})")
            else:
                stats["err"] += 1
                with _locks["print"]:
                    print(f"  [{stats['ok']+stats['err']:>3}/{len(targets)}] "
                          f"✗ #{ctx['article_id']} {ctx['corp_name'][:18]:<18} "
                          f"{result.get('error','?')}")

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        list(ex.map(worker, enumerate(targets)))

    print(f"\n[완료] 성공 {stats['ok']} / 오류 {stats['err']}\n")
    print_stats(db)


if __name__ == "__main__":
    main()
