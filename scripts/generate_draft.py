"""
scripts/generate_draft.py
─────────────────────────
story_leads 취재 단서를 기반으로 Gemini 3.1 Pro Preview 로 기사 초안 자동 생성

실행 예시:
  python scripts/generate_draft.py --stats              # 현재 초안 통계
  python scripts/generate_draft.py --lead-id 5          # 특정 lead만 생성
  python scripts/generate_draft.py --limit 10           # 최대 10건 생성
  python scripts/generate_draft.py                      # 새 단서 전체 생성

출력 형식: 연합뉴스·중앙일보 스타일 한국어 기사
           파이낸스코프 고종민 기자 바이라인
모델: Gemini 3.1 Pro Preview (100 RPD, 10 RPM) → fallback: 2.5 Pro
"""

import io
import json
import os
import re
import sqlite3
import sys
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# ── 환경변수 로드 ────────────────────────────────────────────────────────────
def load_env(path: Path):
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


# ── 모델 설정 ────────────────────────────────────────────────────────────────
# 기사 초안 전용 모델 fallback 체인
ARTICLE_MODELS = [
    "gemini-3.1-pro-preview",
    "gemini-2.5-pro-preview-05-06",
    "gemini-2.5-pro-preview-06-05",
    "gemini-2.5-pro",
]
ARTICLE_RPD   = 100
ARTICLE_DELAY = 7.0  # 10 RPM → 6초 + 여유


# ── DB 연결 ──────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ── Gemini API 호출 (듀얼 키 지원) ───────────────────────────────────────────
def _get_draft_keys() -> list:
    """파싱1 + 파싱2 키 목록"""
    keys = []
    k1 = os.environ.get("GEMINI_API_KEY", "").strip()
    k2 = os.environ.get("GEMINI_API_KEY_2", "").strip()
    if k1: keys.append(k1)
    if k2: keys.append(k2)
    return keys

_draft_key_idx = 0

def call_gemini_article(prompt: str, timeout: int = 120) -> tuple[str, str]:
    """기사 초안 생성. (text, model_name) 반환. 파싱1/파싱2 라운드로빈."""
    global _draft_key_idx
    keys = _get_draft_keys()
    if not keys:
        raise ValueError("GEMINI_API_KEY 없음 — .env 확인")

    last_err = None
    exhausted = set()

    for model_name in ARTICLE_MODELS:
        # 소진되지 않은 키 선택
        available = [k for k in keys if k not in exhausted]
        if not available:
            available = keys
        api_key = available[_draft_key_idx % len(available)]
        _draft_key_idx += 1

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{model_name}:generateContent?key={api_key}"
        )
        payload = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": 4096,
                "temperature": 0.7,
            },
        }).encode("utf-8")

        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data["candidates"][0]["content"]["parts"][0]["text"], model_name
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 404:
                last_err = f"모델 없음: {model_name}"
                continue
            if e.code == 429:
                exhausted.add(api_key)
                # 다른 키로 즉시 재시도
                alt_keys = [k for k in keys if k not in exhausted]
                if alt_keys:
                    alt_url = url.replace(api_key, alt_keys[0])
                    alt_req = urllib.request.Request(alt_url, data=payload,
                        headers={"Content-Type": "application/json"}, method="POST")
                    try:
                        with urllib.request.urlopen(alt_req, timeout=timeout) as r2:
                            d2 = json.loads(r2.read().decode("utf-8"))
                        return d2["candidates"][0]["content"]["parts"][0]["text"], model_name
                    except Exception:
                        pass
                raise RuntimeError(f"API 할당량 초과(429) — 두 키 모두 소진. 내일 다시 시도하세요.")
            raise RuntimeError(f"Gemini API 오류 {e.code}: {body[:300]}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"네트워크 오류: {e.reason}")

    raise RuntimeError(f"사용 가능한 모델 없음. 마지막 오류: {last_err}")


# ── 프롬프트 생성 ─────────────────────────────────────────────────────────────
LEAD_TYPE_KO = {
    "strategy_change": "경영전략 변화",
    "market_shift":    "시장 변화",
    "risk_alert":      "리스크 경보",
    "numeric_change":  "수치 급변",
    "supply_chain":    "공급망 변화",
}

STYLE_GUIDE = """
[기사 작성 지침]
- 매체: 파이낸스코프 (finscope.co.kr)
- 기자: 고종민 기자
- 문체: 연합뉴스·중앙일보 경제면 스타일 (간결, 객관, 사실 중심)
- 제목: 30자 이내, 핵심 사실 중심 (과장 금지)
- 부제: 15자 이내, 제목 보완
- 본문: 400~600자 (단락 구분, 역피라미드 구조)
  * 1단락(리드): 누가·무엇을·언제 (핵심 사실 1~2문장)
  * 2단락: 구체적 내용 (수치, 사업 변화 등)
  * 3단락: 의미·배경·전망
  * 4단락(마지막): 회사 측 입장 또는 업계 시각 (추론 시 '~로 분석된다' 표현)
- 주의: 추측 없이 DART 공시 내용만 근거로 작성, 미확인 사항은 '~것으로 나타났다' 표현
- 날짜: 오늘 날짜를 기준으로 작성 (정확한 날짜 모르면 '최근' 표현)
""".strip()


def build_article_prompt(lead: sqlite3.Row, ai_result: str | None) -> str:
    corp_name  = lead["corp_name"] or "해당 기업"
    lead_type  = lead["lead_type"]
    type_ko    = LEAD_TYPE_KO.get(lead_type, lead_type)
    severity   = lead["severity"]
    title_hint = lead["title"] or ""
    summary    = lead["summary"] or ""
    evidence   = lead["evidence"] or ""
    keywords   = lead["keywords"] or "[]"
    try:
        kw_list = ", ".join(json.loads(keywords))
    except Exception:
        kw_list = keywords

    # AI 비교 분석 전체 결과 (최대 3000자)
    full_context = (ai_result or "")[:3000]

    return f"""당신은 파이낸스코프(finscope.co.kr)의 경제 전문 기자 고종민입니다.
아래 DART 공시 분석 데이터를 바탕으로 한국어 경제 기사 초안을 작성하세요.

{STYLE_GUIDE}

═══════════════════════════════════════════
[취재 단서 정보]
기업명: {corp_name}
단서 유형: {type_ko} (severity {severity}/5)
감지 키워드: {kw_list}
단서 제목: {title_hint}

[핵심 요약]
{summary}

[근거 문장 (DART AI 분석 발췌)]
{evidence}

[AI 분석 원문 (참고용, 최대 3000자)]
{full_context}
═══════════════════════════════════════════

위 정보를 바탕으로 아래 형식으로 기사 초안을 작성하세요.
반드시 JSON 형식으로 출력하세요.

{{
  "headline": "기사 제목 (30자 이내)",
  "subheadline": "부제 (15자 이내)",
  "body": "본문 (400~600자, 단락 사이 빈 줄 1개)",
  "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3"],
  "news_value": "이 기사의 취재 가치 한 줄 설명",
  "caution": "확인 필요 사항 또는 주의점 (없으면 빈 문자열)"
}}"""


# ── 기사 파싱 ─────────────────────────────────────────────────────────────────
def parse_article_json(text: str) -> dict:
    """Gemini 출력에서 JSON 파싱. 실패 시 텍스트 기반 파싱."""
    # JSON 블록 추출
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        text = m.group(1)
    else:
        m2 = re.search(r"(\{.*\})", text, re.DOTALL)
        if m2:
            text = m2.group(1)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # fallback: 정규식으로 파싱
        result = {}
        for field in ["headline", "subheadline", "body", "news_value", "caution"]:
            m = re.search(rf'"{field}"\s*:\s*"(.*?)"(?=\s*[,\}}])', text, re.DOTALL)
            if m:
                result[field] = m.group(1).replace("\\n", "\n").strip()
        if "keywords" not in result:
            km = re.search(r'"keywords"\s*:\s*\[(.*?)\]', text, re.DOTALL)
            if km:
                result["keywords"] = re.findall(r'"([^"]+)"', km.group(1))
        return result


# ── 초안 저장 ─────────────────────────────────────────────────────────────────
def save_draft(conn: sqlite3.Connection, lead_id: int, lead: sqlite3.Row,
               article: dict, model_name: str) -> int:
    headline    = article.get("headline", "")[:200]
    subheadline = article.get("subheadline", "")[:200]
    body        = article.get("body", "")
    kw_list     = article.get("keywords", [])
    news_value  = article.get("news_value", "")
    caution     = article.get("caution", "")

    # body에 keywords 주석 추가
    editor_note = ""
    if news_value:
        editor_note += f"[취재 가치] {news_value}\n"
    if caution:
        editor_note += f"[주의] {caution}"

    word_count = len(body.split())
    char_count = len(body)

    cur = conn.execute("""
        INSERT INTO article_drafts
            (lead_id, corp_code, corp_name,
             headline, subheadline, content,
             style, model, word_count, char_count,
             status, editor_note, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,'draft',?,datetime('now','localtime'))
    """, (
        lead_id,
        lead["corp_code"],
        lead["corp_name"],
        headline,
        subheadline,
        body,
        "news",
        model_name,
        word_count,
        char_count,
        editor_note.strip(),
    ))
    conn.commit()
    return cur.lastrowid


# ── 통계 출력 ─────────────────────────────────────────────────────────────────
def print_stats(conn: sqlite3.Connection):
    print("=" * 60)
    print("  article_drafts 현재 통계")
    print("=" * 60)
    total = conn.execute("SELECT COUNT(*) FROM article_drafts").fetchone()[0]
    print(f"  전체 기사 초안: {total}건")
    if total == 0:
        print("  (아직 초안이 없습니다)")
        print("=" * 60)
        return

    print()
    print("  [상태별]")
    for r in conn.execute("SELECT status, COUNT(*) c FROM article_drafts GROUP BY status ORDER BY c DESC"):
        print(f"    {r['status']:<15} {r['c']}건")

    print()
    print("  [최근 10건]")
    for r in conn.execute("""
        SELECT corp_name, headline, char_count, model, created_at
        FROM article_drafts ORDER BY created_at DESC LIMIT 10
    """):
        print(f"    {r['corp_name']} | {(r['headline'] or '')[:35]} | {r['char_count']}자 | {r['model']}")

    print()
    leads_total   = conn.execute("SELECT COUNT(*) FROM story_leads").fetchone()[0]
    drafted       = conn.execute("SELECT COUNT(DISTINCT lead_id) FROM article_drafts").fetchone()[0]
    print(f"  story_leads 커버리지: {drafted}/{leads_total}건")
    print("=" * 60)


# ── 메인 처리 ─────────────────────────────────────────────────────────────────
def process_leads(conn: sqlite3.Connection, limit: int | None, lead_id: int | None,
                  severity_min: int, force: bool) -> dict:
    """
    story_leads → article_drafts 생성
    반환: {"processed": int, "success": int, "errors": int}
    """
    if lead_id is not None:
        leads = conn.execute("""
            SELECT sl.*, ac.result as ai_result
            FROM story_leads sl
            LEFT JOIN ai_comparisons ac ON sl.comparison_id = ac.id
            WHERE sl.id = ?
        """, (lead_id,)).fetchall()
    else:
        # 아직 초안이 없는 단서 (severity 기준 내림차순)
        already_clause = "AND NOT EXISTS (SELECT 1 FROM article_drafts ad WHERE ad.lead_id = sl.id)"
        if force:
            already_clause = ""

        query = f"""
            SELECT sl.*, ac.result as ai_result
            FROM story_leads sl
            LEFT JOIN ai_comparisons ac ON sl.comparison_id = ac.id
            WHERE sl.status = 'new'
              AND sl.severity >= ?
              {already_clause}
            ORDER BY sl.severity DESC, sl.created_at ASC
        """
        if limit:
            query += f" LIMIT {int(limit)}"
        leads = conn.execute(query, (severity_min,)).fetchall()

    print(f"[info] 처리 대상 취재 단서: {len(leads)}건")
    if not leads:
        print("[info] 새로 생성할 기사 초안이 없습니다.")
        return {"processed": 0, "success": 0, "errors": 0}

    processed = 0
    success   = 0
    errors    = 0

    for lead in leads:
        lead_id_cur = lead["id"]
        corp_name   = lead["corp_name"] or "해당 기업"
        ai_result   = lead["ai_result"] or ""

        processed += 1
        print(f"  [{processed:3d}] {corp_name} | {lead['lead_type']} | severity={lead['severity']}")

        try:
            prompt = build_article_prompt(lead, ai_result)
            text, model_name = call_gemini_article(prompt)
            article = parse_article_json(text)

            if not article.get("headline"):
                print(f"        ⚠ JSON 파싱 실패 — 원문 저장")
                article = {
                    "headline":    lead["title"] or f"{corp_name} 관련 기사",
                    "subheadline": "",
                    "body":        text[:2000],
                    "keywords":    [],
                    "news_value":  "",
                    "caution":     "자동 파싱 실패 — 편집 필요",
                }

            draft_id = save_draft(conn, lead_id_cur, lead, article, model_name)
            print(f"        ✓ [{draft_id}] {article['headline'][:40]} ({model_name})")
            success += 1

        except RuntimeError as e:
            print(f"        ✗ 오류: {e}")
            errors += 1
            if "할당량 초과" in str(e):
                print("[중단] API 일일 한도 초과. 내일 다시 실행하세요.")
                break

        # RPM 제한 준수 딜레이
        if processed < len(leads):
            time.sleep(ARTICLE_DELAY)

    print(f"\n[완료] {processed}건 처리, {success}건 생성, {errors}건 오류")
    return {"processed": processed, "success": success, "errors": errors}


# ── 진입점 ────────────────────────────────────────────────────────────────────
def main():
    load_env(ENV_PATH)

    parser = argparse.ArgumentParser(description="취재 단서 기반 기사 초안 자동 생성")
    parser.add_argument("--stats",      action="store_true", help="현재 통계 출력 후 종료")
    parser.add_argument("--lead-id",    type=int, default=None, metavar="N", help="특정 lead ID만 처리")
    parser.add_argument("--limit",      type=int, default=None, metavar="N", help="최대 처리 건수")
    parser.add_argument("--severity",   type=int, default=4, metavar="N",
                        help="최소 심각도 (기본: 4, 범위: 1~5)")
    parser.add_argument("--force",      action="store_true", help="이미 초안이 있는 단서도 재생성")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"[error] DB 없음: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = get_conn()
    try:
        if args.stats:
            print_stats(conn)
            return

        stats = process_leads(
            conn,
            limit=args.limit,
            lead_id=args.lead_id,
            severity_min=args.severity,
            force=args.force,
        )

        print()
        print_stats(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
