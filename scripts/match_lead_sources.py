"""
scripts/match_lead_sources.py
─────────────────────────────
단서(story_leads) ↔ 외부 자료(external_sources) 매칭 + 사실 검증

매칭 점수 계산:
  - corp_code 일치 (필수)
  - 키워드 매칭 비율 (lead.keywords ∩ source.title+summary)
  - 매체 weight × 시간 근접도
  - 최종: keyword_score × 0.5 + outlet_weight × 0.3 + recency × 0.2

사실 검증:
  - cite_verify의 cross_check 활용
  - PASS / PARTIAL / FAIL 판정
  - PASS만 프롬프트 주입 후보

사용:
  python scripts/match_lead_sources.py --all                # 미매칭 단서 전체
  python scripts/match_lead_sources.py --lead-id 123
  python scripts/match_lead_sources.py --severity 4 --limit 50
  python scripts/match_lead_sources.py --stats
"""

import io
import re
import sys
import json
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
sys.path.insert(0, str(ROOT / "scripts"))

# cite_verify는 lazy import (stdout wrap 충돌 방지)
cross_check = None
fetch_truth = None
cite_decide = None


def _lazy_import_cite():
    global cross_check, fetch_truth, cite_decide
    if cross_check is not None:
        return
    try:
        import cite_verify as cv
        cross_check = cv.cross_check
        fetch_truth = cv.fetch_truth
        cite_decide = cv.decide
    except Exception as e:
        print(f"[경고] cite_verify import 실패: {e}")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# 매칭 점수 계산
# ══════════════════════════════════════════════════════════════════════════════

STOP_TOKENS = {
    "있다", "있음", "있는", "회사", "당사", "기업", "사업",
    "기록", "보였다", "발표", "공시", "이날", "지난해", "올해",
}


def korean_tokens(text: str, min_len: int = 2) -> set[str]:
    if not text:
        return set()
    kr = set(re.findall(rf"[가-힣]{{{min_len},}}", text))
    en = set(w.lower() for w in re.findall(r"[A-Za-z]{3,}", text))
    return (kr | en) - STOP_TOKENS


def keyword_match_score(lead_kw: list[str], source_text: str) -> tuple[float, list[str]]:
    """단서 키워드와 자료 텍스트 매칭."""
    if not lead_kw:
        return 0.5, []
    src_tokens = korean_tokens(source_text)
    src_lower = source_text.lower()
    matched = []
    for kw in lead_kw:
        if not kw:
            continue
        # substring 매칭 (조사 변형 대비)
        if kw in source_text or kw.lower() in src_lower:
            matched.append(kw)
            continue
        # 토큰 매칭
        kw_tokens = korean_tokens(kw)
        if kw_tokens & src_tokens:
            matched.append(kw)
    score = len(matched) / max(1, len(lead_kw))
    return score, matched


def recency_score(published_at: str | None, lead_created_at: str | None = None,
                  max_days: int = 180) -> float:
    """
    Recency 점수 v2: today(기사 작성일) 기준 가까울수록 높음.

      30일 이내   → 1.0  (매우 최신)
      30~60일     → 0.85 (최신)
      60~90일     → 0.7  (최근)
      90~180일    → 0.5  (참고)
      180일+      → 0.3  (역사적)
      불명         → 0.3
    """
    if not published_at:
        return 0.3
    try:
        pd = datetime.strptime(published_at[:10], "%Y-%m-%d")
    except Exception:
        return 0.3
    today = datetime.now()
    diff = abs((today - pd).days)
    if diff <= 30:
        return 1.0
    if diff <= 60:
        return 0.85
    if diff <= 90:
        return 0.7
    if diff <= 180:
        return 0.5
    return 0.3


# ══════════════════════════════════════════════════════════════════════════════
# 매칭 + 검증
# ══════════════════════════════════════════════════════════════════════════════

def match_one_lead(conn: sqlite3.Connection, lead: sqlite3.Row,
                   verify_facts: bool = True, verbose: bool = False) -> dict:
    """단일 단서에 대해 모든 관련 자료 매칭 + 점수화."""
    corp_code = lead["corp_code"] or ""
    if not corp_code:
        return {"matched": 0, "verified": 0}

    # 키워드 파싱
    try:
        kw_list = json.loads(lead["keywords"] or "[]")
    except Exception:
        kw_list = []

    # 후보 외부 자료 (corp_code 일치)
    sources = conn.execute("""
        SELECT * FROM external_sources
        WHERE related_corp_code = ?
          AND outlet_tier IN ('T1', 'T2', 'RESEARCH')
    """, [corp_code]).fetchall()

    if not sources:
        return {"matched": 0, "verified": 0}

    # 진실 풀 (사실 검증용) — cite_verify 재활용
    truth = fetch_truth(conn, corp_code) if (verify_facts and fetch_truth) else None

    matched = 0
    verified = 0

    for s in sources:
        text_blob = f"{s['title']} {s['summary'] or ''}"
        kw_score, matched_kw = keyword_match_score(kw_list, text_blob)
        rec_score = recency_score(s["published_at"], lead["created_at"])
        outlet_w = float(s["outlet_weight"] or 0.5)

        # 종합 매칭 점수
        match_score = kw_score * 0.5 + outlet_w * 0.3 + rec_score * 0.2

        # 사실 검증
        fact_status = "NOT_CHECKED"
        fact_score = None
        if verify_facts and cross_check and truth:
            try:
                cc = cross_check(text_blob, truth)
                fact_score = cc["score"]
                # tier에 따른 결정
                fact_status = cite_decide(s["outlet_tier"], cc["score"])
                if fact_status in ("PASS", "PARTIAL"):
                    verified += 1
            except Exception as e:
                if verbose:
                    print(f"    cross_check 실패: {e}")

        # 매칭 저장
        conn.execute("""
            INSERT OR REPLACE INTO lead_external_match
                (lead_id, source_id, match_score, keyword_match,
                 fact_check_status, fact_check_score, matched_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        """, [
            lead["id"], s["id"], round(match_score, 3),
            json.dumps(matched_kw, ensure_ascii=False),
            fact_status, fact_score,
        ])
        matched += 1

        if verbose:
            print(f"    {s['outlet_name']:<15} | match {match_score:.2f} | "
                  f"kw {kw_score:.2f} | fact {fact_status}({fact_score})")

    conn.commit()
    return {"matched": matched, "verified": verified}


# ══════════════════════════════════════════════════════════════════════════════
# 통계
# ══════════════════════════════════════════════════════════════════════════════

def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 65)
    print("  단서-자료 매칭 현황")
    print("=" * 65)

    total = conn.execute("SELECT COUNT(*) FROM lead_external_match").fetchone()[0]
    print(f"\n  전체 매칭: {total:,}건")

    # 매칭된 단서 수
    leads_matched = conn.execute(
        "SELECT COUNT(DISTINCT lead_id) FROM lead_external_match"
    ).fetchone()[0]
    leads_total = conn.execute("SELECT COUNT(*) FROM story_leads").fetchone()[0]
    print(f"  매칭된 단서: {leads_matched:,} / {leads_total:,}")

    print("\n  [사실 검증 분포]")
    for r in conn.execute("""
        SELECT fact_check_status, COUNT(*) cnt,
               AVG(fact_check_score) avg_score
        FROM lead_external_match
        GROUP BY fact_check_status ORDER BY cnt DESC
    """):
        avg = f"{r['avg_score']:.0f}점" if r["avg_score"] else "-"
        icon = {"PASS":"🟢","PARTIAL":"🟡","FAIL":"🔴"}.get(r["fact_check_status"],"⚪")
        print(f"    {icon} {r['fact_check_status']:<12} {r['cnt']:>4}건 | 평균 {avg}")

    print("\n  [매칭 점수 분포]")
    for label, lo, hi in [("🟢 0.7+", 0.7, 1.01), ("🟠 0.5~0.7", 0.5, 0.7),
                          ("🔴 0.5미만", 0.0, 0.5)]:
        cnt = conn.execute("""
            SELECT COUNT(*) FROM lead_external_match
            WHERE match_score >= ? AND match_score < ?
        """, [lo, hi]).fetchone()[0]
        print(f"    {label:<14}: {cnt:>4}건")

    print("\n  [PASS 인용 가능 자료 — Top 매체]")
    for r in conn.execute("""
        SELECT es.outlet_name, COUNT(*) cnt, AVG(lem.match_score) avg
        FROM lead_external_match lem
        JOIN external_sources es ON lem.source_id = es.id
        WHERE lem.fact_check_status IN ('PASS', 'PARTIAL')
        GROUP BY es.outlet_name ORDER BY cnt DESC LIMIT 10
    """):
        print(f"    {r['outlet_name']:<20}: {r['cnt']:>3}건 (평균 {r['avg']:.2f})")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="단서-외부자료 매칭 + 사실 검증")
    p.add_argument("--all",      action="store_true", help="미매칭 단서 전체")
    p.add_argument("--lead-id",  type=int)
    p.add_argument("--severity", type=int, help="severity N 이상")
    p.add_argument("--limit",    type=int, default=50)
    p.add_argument("--no-verify", action="store_true", help="사실 검증 스킵 (속도 우선)")
    p.add_argument("--verbose",  action="store_true")
    p.add_argument("--stats",    action="store_true")
    args = p.parse_args()

    conn = get_conn()

    if args.stats:
        print_stats(conn); return

    if not args.no_verify:
        _lazy_import_cite()

    # 대상 단서
    if args.lead_id:
        leads = conn.execute("SELECT * FROM story_leads WHERE id=?",
                             [args.lead_id]).fetchall()
    elif args.severity:
        leads = conn.execute("""
            SELECT * FROM story_leads
            WHERE severity >= ? ORDER BY severity DESC, id DESC LIMIT ?
        """, [args.severity, args.limit]).fetchall()
    else:
        # 외부 자료가 있는 단서만 (corp_code 매칭) + 미매칭
        leads = conn.execute("""
            SELECT sl.* FROM story_leads sl
            WHERE EXISTS (
                SELECT 1 FROM external_sources es
                WHERE es.related_corp_code = sl.corp_code
            )
            AND NOT EXISTS (
                SELECT 1 FROM lead_external_match lem WHERE lem.lead_id = sl.id
            )
            ORDER BY sl.severity DESC LIMIT ?
        """, [args.limit]).fetchall()

    if not leads:
        print("[info] 처리할 단서 없음 — collect_kr_news.py로 먼저 수집하세요")
        print_stats(conn); return

    print(f"[info] 처리 대상 단서: {len(leads)}건")
    print(f"       사실 검증: {'OFF' if args.no_verify else 'ON'}\n")

    total_matched = 0
    total_verified = 0
    for i, lead in enumerate(leads, 1):
        r = match_one_lead(conn, lead,
                           verify_facts=not args.no_verify,
                           verbose=args.verbose)
        total_matched += r["matched"]
        total_verified += r["verified"]
        if r["matched"]:
            print(f"  [{i:3d}/{len(leads)}] #{lead['id']} {lead['corp_name']:<20} "
                  f"매칭 {r['matched']:>2} / 검증통과 {r['verified']:>2}")

    print(f"\n=== 완료 ===")
    print(f"  매칭 총 {total_matched}건 / 사실검증 통과 {total_verified}건")
    print()
    print_stats(conn)


if __name__ == "__main__":
    main()
