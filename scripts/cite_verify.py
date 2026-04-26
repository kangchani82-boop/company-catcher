"""
scripts/cite_verify.py
──────────────────────
외부 미디어 인용 후보 → 2차 검증 → external_citations 테이블 기록

처리 절차:
  [1] 매체 식별: URL/도메인 → media_whitelist.yaml에서 tier 조회
  [2] 사실 추출: 인용할 본문에서 수치·인물·사건 추출
  [3] 1차 자료 대조:
       - 수치는 financials 테이블과 ±5% 비교
       - 사실은 reports.biz_content / ai_comparisons.result 본문 검색
  [4] 종합 판정: PASS / PARTIAL / FAIL
  [5] DB 기록: external_citations 테이블에 저장

사용:
  python scripts/cite_verify.py --init                    # DB 스키마 생성
  python scripts/cite_verify.py --check \\
       --article-id 233 \\
       --url "https://www.mk.co.kr/article/12345" \\
       --outlet "매일경제" \\
       --date 2025-04-15 \\
       --text "포스코이앤씨 영업이익 618억 원 기록"

  python scripts/cite_verify.py --stats
  python scripts/cite_verify.py --report PASS|FAIL|PARTIAL
"""

import io
import os
import re
import sys
import json
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

# stdout/stderr UTF-8 래핑은 CLI 실행 시에만 (import 시 부작용 방지)
def _ensure_utf8_io():
    try:
        if hasattr(sys.stdout, "buffer"):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "buffer"):
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
WHITELIST_YAML = ROOT / "rules" / "media_whitelist.yaml"


# ══════════════════════════════════════════════════════════════════════════════
# DB 스키마
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS external_citations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER NOT NULL,
    source_outlet TEXT NOT NULL,
    source_url TEXT,
    source_date TEXT,
    source_tier TEXT,                    -- T1/T2/T3/BLOCKED/UNKNOWN
    cited_text TEXT NOT NULL,
    extracted_numbers TEXT,              -- JSON list
    extracted_facts TEXT,                -- JSON list
    verified_against TEXT,               -- e.g. "financials,biz_content"
    verification_status TEXT NOT NULL,   -- PASS/PARTIAL/FAIL
    verification_score INTEGER NOT NULL, -- 0-100
    details TEXT,                        -- JSON
    inserted_at TEXT NOT NULL,
    FOREIGN KEY (article_id) REFERENCES article_drafts(id)
);
CREATE INDEX IF NOT EXISTS idx_ec_article ON external_citations(article_id);
CREATE INDEX IF NOT EXISTS idx_ec_status  ON external_citations(verification_status);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# 1. 매체 식별 (Tier 조회)
# ══════════════════════════════════════════════════════════════════════════════

def load_whitelist() -> dict[str, dict]:
    """domain → {name, tier, category, note}."""
    try:
        import yaml
    except ImportError:
        return {}
    if not WHITELIST_YAML.exists():
        return {}
    with open(WHITELIST_YAML, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    out = {}
    for m in data.get("media", []):
        d = m["domain"].lower().lstrip(".")
        out[d] = {
            "name": m.get("name", d),
            "tier": m.get("tier", "UNKNOWN"),
            "category": m.get("category", ""),
            "note": m.get("note", ""),
        }
    return out


def identify_outlet(url: str, whitelist: dict) -> dict:
    """URL → 매체 식별. 부분 일치 (서브도메인 허용)."""
    if not url:
        return {"name": "unknown", "tier": "UNKNOWN", "domain": ""}
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return {"name": "unknown", "tier": "UNKNOWN", "domain": ""}

    # 정확 일치
    if host in whitelist:
        info = whitelist[host].copy()
        info["domain"] = host
        return info

    # 도메인 끝 일치 (서브도메인)
    for d, info in whitelist.items():
        if host.endswith("." + d) or host == d:
            r = info.copy()
            r["domain"] = d
            return r

    return {"name": "unknown", "tier": "UNKNOWN", "domain": host}


# ══════════════════════════════════════════════════════════════════════════════
# 2. 사실 추출
# ══════════════════════════════════════════════════════════════════════════════

NUMBER_RX = re.compile(r"([0-9][0-9,]*\.?[0-9]*)\s*(조|억|천만|백만|만|%|%p)")


def extract_numbers(text: str) -> list[dict]:
    """본문에서 (값, 단위) 추출."""
    out = []
    for m in NUMBER_RX.finditer(text):
        try:
            v = float(m.group(1).replace(",", ""))
        except ValueError:
            continue
        out.append({"raw": m.group(0).strip(), "value": v, "unit": m.group(2)})
    return out


def to_won(value: float, unit: str) -> float | None:
    """단위 → 원 단위 환산."""
    units = {"조": 1e12, "억": 1e8, "천만": 1e7, "백만": 1e6, "만": 1e4}
    return value * units[unit] if unit in units else None


# ══════════════════════════════════════════════════════════════════════════════
# 3. 1차 자료 대조
# ══════════════════════════════════════════════════════════════════════════════

def fetch_truth(conn: sqlite3.Connection, corp_code: str) -> dict:
    """대조 진실 풀: financials + biz_content + ai_comparisons."""
    truth = {"financial_values": [], "biz_text": "", "ai_text": ""}

    rows = conn.execute("""
        SELECT revenue, operating_income, net_income, total_assets,
               total_liabilities, total_equity, cash, inventory,
               debt_ratio, current_ratio, operating_margin, net_margin,
               roe, roa
        FROM financials WHERE corp_code=? ORDER BY fiscal_year DESC LIMIT 5
    """, [corp_code]).fetchall()
    for r in rows:
        for k in ("revenue", "operating_income", "net_income",
                  "total_assets", "total_liabilities", "total_equity",
                  "cash", "inventory"):
            v = r[k]
            if v is not None and v != 0:
                truth["financial_values"].append(("abs", float(v)))
        for k in ("debt_ratio", "current_ratio", "operating_margin",
                  "net_margin", "roe", "roa"):
            v = r[k]
            if v is not None:
                truth["financial_values"].append(("pct", float(v)))

    biz = conn.execute("""
        SELECT biz_content FROM reports
        WHERE corp_code=? AND biz_content IS NOT NULL
        ORDER BY rcept_dt DESC LIMIT 1
    """, [corp_code]).fetchone()
    truth["biz_text"] = (biz["biz_content"] if biz else "") or ""

    ai = conn.execute("""
        SELECT result FROM ai_comparisons
        WHERE corp_code=? AND status='ok'
        ORDER BY analyzed_at DESC LIMIT 1
    """, [corp_code]).fetchone()
    truth["ai_text"] = (ai["result"] if ai else "") or ""

    return truth


def cross_check(cited_text: str, truth: dict) -> dict:
    """인용 텍스트의 사실을 1차 자료와 대조."""
    nums = extract_numbers(cited_text)
    pct_values = [v for kind, v in truth["financial_values"] if kind == "pct"]
    abs_values = [v for kind, v in truth["financial_values"] if kind == "abs"]

    matched_nums = []
    unmatched_nums = []
    for n in nums:
        is_pct = n["unit"] in ("%", "%p")
        if is_pct:
            ok = any(abs(tv - n["value"]) <= 2.0 for tv in pct_values)
        else:
            won = to_won(n["value"], n["unit"])
            ok = False
            if won:
                for tv in abs_values:
                    if tv > 1e6 and abs(tv - won) / max(tv, won) <= 0.05:
                        ok = True
                        break
        (matched_nums if ok else unmatched_nums).append(n["raw"])

    # 텍스트 사실 — 핵심어 substring 매칭
    common_text = (truth["biz_text"][:30000] + " " + truth["ai_text"]).lower()
    cited_lower = cited_text.lower()
    # 명사 후보
    nouns = set(re.findall(r"[가-힣]{3,}", cited_text)) | set(
        w.lower() for w in re.findall(r"[A-Za-z]{4,}", cited_text)
    )
    found_facts = sum(1 for n in nouns if n in common_text)
    fact_coverage = found_facts / len(nouns) * 100 if nouns else 100

    # 종합 점수
    num_score = 100
    if nums:
        num_score = int(len(matched_nums) / len(nums) * 100)
    score = int(num_score * 0.6 + fact_coverage * 0.4)

    return {
        "score": score,
        "numbers": {
            "tested": len(nums),
            "matched": len(matched_nums),
            "unmatched": unmatched_nums[:5],
        },
        "facts": {
            "noun_count": len(nouns),
            "found": found_facts,
            "coverage_pct": round(fact_coverage, 1),
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# 4. 종합 판정
# ══════════════════════════════════════════════════════════════════════════════

def decide(tier: str, score: int) -> str:
    if tier == "BLOCKED":
        return "FAIL"
    if tier == "UNKNOWN":
        # 낯선 매체 + 점수 80+면 PARTIAL, 그 외 FAIL
        return "PARTIAL" if score >= 80 else "FAIL"
    if tier == "T1":
        return "PASS" if score >= 70 else ("PARTIAL" if score >= 50 else "FAIL")
    if tier == "T2":
        return "PASS" if score >= 80 else ("PARTIAL" if score >= 60 else "FAIL")
    if tier == "T3":
        return "PARTIAL" if score >= 80 else "FAIL"
    return "FAIL"


# ══════════════════════════════════════════════════════════════════════════════
# 5. CLI 진입점
# ══════════════════════════════════════════════════════════════════════════════

def cmd_check(args, conn) -> None:
    article = conn.execute("SELECT * FROM article_drafts WHERE id=?",
                           [args.article_id]).fetchone()
    if not article:
        print(f"기사 없음: {args.article_id}")
        sys.exit(1)

    whitelist = load_whitelist()
    outlet_info = identify_outlet(args.url or "", whitelist)
    if args.outlet:
        outlet_info["name"] = args.outlet

    truth = fetch_truth(conn, article["corp_code"] or "")
    check = cross_check(args.text, truth)

    status = decide(outlet_info["tier"], check["score"])

    # DB 저장
    nums = extract_numbers(args.text)
    nouns = list(set(re.findall(r"[가-힣]{3,}", args.text)))[:20]
    conn.execute("""
        INSERT INTO external_citations
            (article_id, source_outlet, source_url, source_date, source_tier,
             cited_text, extracted_numbers, extracted_facts,
             verified_against, verification_status, verification_score,
             details, inserted_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
    """, [
        args.article_id,
        outlet_info["name"],
        args.url,
        args.date,
        outlet_info["tier"],
        args.text,
        json.dumps([n["raw"] for n in nums], ensure_ascii=False),
        json.dumps(nouns, ensure_ascii=False),
        "financials,biz_content,ai_comparisons",
        status,
        check["score"],
        json.dumps(check, ensure_ascii=False),
    ])
    conn.commit()

    icon = {"PASS":"🟢","PARTIAL":"🟡","FAIL":"🔴"}.get(status, "⚪")
    print(f"\n{icon} 결과: {status} ({check['score']}점)")
    print(f"   매체: {outlet_info['name']} (tier {outlet_info['tier']})")
    print(f"   수치 매칭: {check['numbers']['matched']}/{check['numbers']['tested']}")
    print(f"   사실 커버: {check['facts']['coverage_pct']}%")
    if check["numbers"]["unmatched"]:
        print(f"   ⚠ 매칭 안된 수치: {check['numbers']['unmatched']}")


def cmd_stats(conn) -> None:
    print("=" * 60)
    print("  외부 인용 검증 분포")
    print("=" * 60)
    for r in conn.execute("""
        SELECT verification_status, source_tier, COUNT(*) cnt,
               AVG(verification_score) avg_score
        FROM external_citations
        GROUP BY verification_status, source_tier
        ORDER BY verification_status, source_tier
    """):
        icon = {"PASS":"🟢","PARTIAL":"🟡","FAIL":"🔴"}.get(r["verification_status"],"⚪")
        print(f"  {icon} {r['verification_status']:<7} | {r['source_tier']:<8} | "
              f"{r['cnt']:>4}건 | 평균 {r['avg_score']:.0f}점")
    total = conn.execute("SELECT COUNT(*) FROM external_citations").fetchone()[0]
    print(f"\n  전체: {total}건")


def cmd_report(conn, status: str) -> None:
    rows = conn.execute("""
        SELECT ec.*, ad.corp_name, ad.headline
        FROM external_citations ec
        JOIN article_drafts ad ON ec.article_id = ad.id
        WHERE ec.verification_status = ?
        ORDER BY ec.verification_score DESC LIMIT 30
    """, [status]).fetchall()
    print(f"\n=== {status} 인용 목록 ===")
    for r in rows:
        print(f"\n  기사 #{r['article_id']} ({r['corp_name']})")
        print(f"    매체: {r['source_outlet']} (tier {r['source_tier']}, {r['source_date']})")
        print(f"    점수: {r['verification_score']}")
        print(f"    인용: {r['cited_text'][:100]}…")


def main():
    p = argparse.ArgumentParser(description="외부 미디어 인용 2차 검증")
    p.add_argument("--init", action="store_true", help="DB 스키마 생성")
    p.add_argument("--check", action="store_true", help="인용 검증 실행")
    p.add_argument("--stats", action="store_true", help="통계 출력")
    p.add_argument("--report", choices=["PASS","PARTIAL","FAIL"], help="상세 리포트")
    p.add_argument("--article-id", type=int, help="대상 기사 id")
    p.add_argument("--url", type=str, help="외부 매체 URL")
    p.add_argument("--outlet", type=str, help="매체명 (URL이 부족할 때)")
    p.add_argument("--date", type=str, help="보도일 YYYY-MM-DD")
    p.add_argument("--text", type=str, help="인용할 본문")
    args = p.parse_args()

    conn = get_conn()
    ensure_schema(conn)

    if args.init:
        print("✓ external_citations 스키마 생성 완료")
        return
    if args.stats:
        cmd_stats(conn); return
    if args.report:
        cmd_report(conn, args.report); return
    if args.check:
        if not (args.article_id and args.text):
            print("usage: --check --article-id <id> --text <citation> [--url URL --outlet 매체명 --date YYYY-MM-DD]")
            sys.exit(1)
        cmd_check(args, conn); return

    # 기본 도움말
    print(__doc__)


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
