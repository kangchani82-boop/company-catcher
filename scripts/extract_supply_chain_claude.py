"""
scripts/extract_supply_chain_claude.py
───────────────────────────────────────
Claude Haiku API를 사용해 biz_content에서 공급망 관계 추출
정규식으로 못 잡은 나머지 기업 대상

실행:
  python scripts/extract_supply_chain_claude.py           # 미처리 기업만
  python scripts/extract_supply_chain_claude.py --limit 100
  python scripts/extract_supply_chain_claude.py --all     # 기존 데이터 포함 덮어쓰기
"""

import io
import json
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

# ── 환경 변수 로드 ────────────────────────────────────────────────────────
import os
_env_path = ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            key, val = k.strip(), v.strip()
            if val and not os.environ.get(key):
                os.environ[key] = val

MODEL = "claude-haiku-4-5"
API_URL = "https://api.anthropic.com/v1/messages"
DELAY = 1.5   # 초 (450K TPM 한도 고려: 6K tokens × ~60 RPM = 360K TPM)

# Gemini 폴백 모델 (Claude 크레딧 소진 시)
GEMINI_MODELS = [
    "gemma-3-27b-it",           # JSON 출력 정확, 주력 모델
    "gemma-3-12b-it",           # 폴백
    "gemini-2.5-flash",         # 쿼터 리셋 후 자동 활성
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemma-4-31b-it",           # JSON 출력 불안정, 최후 수단
]

SYSTEM_PROMPT = """Extract supply chain relationships from the Korean corporate disclosure text below.
Output ONLY a valid JSON array. No explanation, no markdown, no extra text.

Each item must have exactly these keys:
- "relation_type": one of "customer", "supplier", "partner", "competitor"
- "partner_name": company name in standard Korean form (string). Normalize: remove (주)/㈜/주식회사, keep core name. For foreign companies keep English name.
- "context": key sentence from the text describing the relationship (max 60 Korean chars)
- "confidence": "explicit" if directly stated with company name, "implicit" if inferred from context
- "revenue_share_pct": numeric percentage if mentioned (e.g. 30.5 for "매출의 30.5%"), otherwise null

Rules:
- customer: major buyers / sales channels of this company
- supplier: major raw material / parts / component suppliers to this company
- partner: strategic alliance / technology partner / joint venture
- competitor: competing companies in the same industry
- Exclude vague/generic: whole industry segments, unnamed parties, government agencies unless specifically named
- Exclude noise: percentages only, table headers, section titles
- If no specific relationships found, output exactly: []
- Entity normalization: "삼성전자(주)" → "삼성전자", "현대자동차㈜" → "현대자동차", "LG Electronics" → "LG전자" if Korean name known

Output format example:
[
  {"relation_type":"supplier","partner_name":"삼성전자","context":"주요 반도체 부품 공급사로 전체 매입의 30% 차지","confidence":"explicit","revenue_share_pct":30.0},
  {"relation_type":"customer","partner_name":"현대자동차","context":"완성차 납품 주요 고객","confidence":"explicit","revenue_share_pct":null},
  {"relation_type":"competitor","partner_name":"LG화학","context":"동일 배터리 시장 경쟁사","confidence":"implicit","revenue_share_pct":null}
]"""


def call_gemini(corp_name: str, biz_content: str) -> list[dict]:
    """Gemini API로 공급망 관계 추출."""
    key1 = os.environ.get("GEMINI_API_KEY", "").strip()
    key2 = os.environ.get("GEMINI_API_KEY_2", "").strip()
    keys = [k for k in [key1, key2] if k]
    if not keys:
        raise ValueError("GEMINI_API_KEY 없음")

    text = biz_content[:40000]
    prompt = f"{SYSTEM_PROMPT}\n\nCompany name: {corp_name}\n\nText:\n{text}\n\nJSON array:"
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.1},
    }).encode("utf-8")

    last_error = None
    for model in GEMINI_MODELS:
        for key in keys:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=120) as resp:
                    body = resp.read().decode("utf-8")
                if not body.strip():
                    last_error = f"{model}: 빈 응답"
                    continue
                data = json.loads(body)
                # candidates 없거나 빈 경우 (safety block 등)
                candidates = data.get("candidates", [])
                if not candidates:
                    last_error = f"{model}: candidates 없음"
                    continue
                parts = candidates[0].get("content", {}).get("parts", [])
                if not parts:
                    last_error = f"{model}: parts 없음"
                    continue
                raw = parts[0].get("text", "").strip()
                raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
                if not raw:
                    return []
                try:
                    relations = json.loads(raw)
                    return relations if isinstance(relations, list) else []
                except json.JSONDecodeError:
                    # JSON 파싱 실패 시 빈 배열 반환 (비-JSON 텍스트 응답)
                    last_error = f"{model}: JSON 파싱 실패 (raw={raw[:80]})"
                    return []
            except urllib.error.HTTPError as e:
                err_body = ""
                try:
                    err_body = e.read().decode("utf-8")[:100]
                except Exception:
                    pass
                if e.code in (429, 503):
                    last_error = f"{model}: HTTP {e.code}"
                    continue  # 다음 키/모델 시도
                if e.code == 404:
                    last_error = f"{model}: 404 모델 없음"
                    break  # 이 모델은 건너뜀
                raise RuntimeError(f"HTTP {e.code}: {err_body}")
            except Exception as e:
                last_error = f"{model}: {e}"
                continue  # 타임아웃 등 → 다음 키/모델
    raise RuntimeError(f"Gemini 모든 키/모델 소진. 마지막 오류: {last_error}")


def call_claude(corp_name: str, biz_content: str) -> list[dict]:
    """Claude Haiku로 공급망 관계 추출. 결과 리스트 반환."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY 없음")

    # 토큰 절약: 60K자 제한
    text = biz_content[:60000]

    payload = json.dumps({
        "model": MODEL,
        "max_tokens": 1024,
        "system": SYSTEM_PROMPT,
        "messages": [{
            "role": "user",
            "content": f"기업명: {corp_name}\n\n{text}"
        }]
    }).encode("utf-8")

    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    raw = data["content"][0]["text"].strip()

    # JSON 파싱 (마크다운 코드블록 처리)
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    if not raw:
        return []
    relations = json.loads(raw)
    if not isinstance(relations, list):
        return []
    return relations


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="처리 최대 기업 수 (0=전체)")
    parser.add_argument("--all",   action="store_true", help="이미 처리된 기업도 덮어쓰기")
    args = parser.parse_args()

    db = get_db()
    now = datetime.now().isoformat(timespec="seconds")

    # 대상: 공급망 데이터가 없는 기업의 2025_annual biz_content
    # 금융사/지주사/스팩 등 공급망 관계 없는 업종 제외
    query = """
        SELECT r.corp_code, r.corp_name, r.biz_content, r.report_name
        FROM reports r
        WHERE r.report_type = '2025_annual'
          AND r.biz_content IS NOT NULL
          AND LENGTH(r.biz_content) >= 500
          AND r.corp_name NOT LIKE '%보험%'
          AND r.corp_name NOT LIKE '%스팩%'
          AND r.corp_name NOT LIKE '%기업인수목적%'
          AND r.corp_name NOT LIKE '%증권%'
          AND r.corp_name NOT LIKE '%은행%'
          AND r.corp_name NOT LIKE '%캐피탈%'
          AND r.corp_name NOT LIKE '%지주%'
          AND r.corp_name NOT LIKE '%홀딩스%'
          AND r.corp_name NOT LIKE '%카드%'
          AND r.corp_name NOT LIKE '%리츠%'
          AND r.corp_name NOT LIKE '%자산운용%'
          AND r.corp_name NOT LIKE '%투자신탁%'
          AND r.corp_name NOT LIKE '%투자금융%'
          AND r.corp_name NOT LIKE '%인베스트먼트%'
          AND r.corp_name NOT LIKE '%벤처스%'
          AND r.corp_name NOT LIKE '%자산신탁%'
          AND r.corp_name NOT LIKE '%신탁%'
          AND r.corp_name NOT IN ('국도화학')
          AND SUBSTR(r.biz_content, 1, 500) NOT LIKE '%회계감사인%'
          AND SUBSTR(r.biz_content, 1, 500) NOT LIKE '%외부감사%'
    """
    if not args.all:
        query += " AND NOT EXISTS (SELECT 1 FROM supply_chain s WHERE s.corp_code = r.corp_code)"
    query += " ORDER BY r.corp_code"

    rows = db.execute(query).fetchall()
    total = len(rows)
    if args.limit:
        rows = rows[:args.limit]

    print(f"처리 대상: {len(rows)}개 기업 (전체 미처리: {total}개)")
    print(f"모델: {MODEL} | 예상 비용: ~${len(rows)*6102/1_000_000*0.8:.2f} USD\n")

    processed = skipped = inserted = errors = 0

    for i, row in enumerate(rows, 1):
        corp_code = row["corp_code"]
        corp_name = row["corp_name"] or corp_code
        biz_content = row["biz_content"]
        src = row["report_name"] or "2025_annual"

        # Gemini로 직접 처리
        relations = None
        used_api = "gemini"
        try:
            relations = call_gemini(corp_name, biz_content)
        except Exception as e:
            print(f"  [{i:4d}] {corp_name}: 오류 — {e}")
            errors += 1
            time.sleep(3)
            continue

        if relations is None:
            errors += 1
            continue

        if args.all:
            db.execute("DELETE FROM supply_chain WHERE corp_code=?", [corp_code])

        count = 0
        for rel in relations:
            rtype      = rel.get("relation_type", "").strip()
            partner    = rel.get("partner_name", "").strip()
            context    = rel.get("context", "").strip()[:200]
            confidence = rel.get("confidence", "implicit")
            rev_share  = rel.get("revenue_share_pct")
            if rtype not in ("customer", "supplier", "partner", "competitor"):
                continue
            if not partner or len(partner) < 2:
                continue
            if confidence not in ("explicit", "implicit"):
                confidence = "implicit"
            if not isinstance(rev_share, (int, float)):
                rev_share = None
            db.execute("""
                INSERT OR IGNORE INTO supply_chain
                  (corp_code, corp_name, relation_type, partner_name, context,
                   confidence, revenue_share_pct, source_report, analyzed_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, [corp_code, corp_name, rtype, partner, context,
                  confidence, rev_share, src, now])
            count += 1

        db.commit()
        inserted += count
        processed += 1

        if count > 0:
            print(f"  [{i:4d}/{len(rows)}] {corp_name:<20} → {count}개 관계 추출")
        elif i % 50 == 0:
            print(f"  [{i:4d}/{len(rows)}] 진행 중... (누적 {inserted}개 추출)")

        time.sleep(DELAY)

    print(f"\n=== 완료 ===")
    print(f"처리: {processed}개 | 오류: {errors}개 | 추출: {inserted}개 관계")

    # 최종 현황
    total_sc = db.execute("SELECT COUNT(*) FROM supply_chain").fetchone()[0]
    corps_sc = db.execute("SELECT COUNT(DISTINCT corp_code) FROM supply_chain").fetchone()[0]
    by_type  = db.execute(
        "SELECT relation_type, COUNT(*) FROM supply_chain GROUP BY relation_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    print(f"\nsupply_chain 전체: {total_sc:,}건 ({corps_sc:,}개 기업)")
    for r in by_type:
        print(f"  {r[0]}: {r[1]:,}건")
    db.close()


if __name__ == "__main__":
    main()
