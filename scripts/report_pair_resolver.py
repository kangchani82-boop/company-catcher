"""
scripts/report_pair_resolver.py
───────────────────────────────
Phase N — 비교 페어링 자동화

원칙: "항상 가장 최신 보고서 + 직전 보고서"
  - 2026-04 (오늘): 2025_annual vs 2025_q1 (가장 최근 분기)
  - 2026-05 이후 (2026_q1 발행): 2026_q1 vs 2025_annual
  - 2026-08 이후 (2026_h1):     2026_h1 vs 2026_q1
  - 2026-11 이후 (2026_q3):     2026_q3 vs 2026_h1
  - 2027-03 이후 (2026_annual): 2026_annual vs 2026_q3

실행:
  python scripts/report_pair_resolver.py                       # 현재 권장 페어 출력
  python scripts/report_pair_resolver.py --corp-code 00100814  # 특정 회사
  python scripts/report_pair_resolver.py --year 2026 --month 8 # 가상 시점
"""

import io
import sys
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


def _ensure_utf8_io():
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass


REPORT_TYPES_ORDER = [
    # (report_type, expected_publish_month_after_period_end)
    "_annual",   # 연간 (사업보고서) — 다음해 3월까지 공시
    "_q1",       # 1분기 — 5월까지
    "_h1",       # 반기 — 8월까지
    "_q3",       # 3분기 — 11월까지
]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# 핵심: 시점 → 페어 결정
# ══════════════════════════════════════════════════════════════════════════════

def resolve_pair_by_date(today: datetime = None) -> tuple[str, str]:
    """
    today 시점에서 사용 가능한 가장 최신 보고서 페어를 룰 기반으로 결정.
    Returns: (latest, prev)
    """
    today = today or datetime.now()
    Y = today.year
    M = today.month

    # 룰: 보고서 발행 월
    #   _annual : 다음해 1~3월 (안전: 4월부터 직전년도 사업보고서)
    #   _q1     : 4~5월
    #   _h1     : 7~8월
    #   _q3     : 10~11월

    # 4월 이전 (1~3월): 직전직전 연도 q3 vs 직전직전 h1
    #   ex) 2026-02: 2025_q3 vs 2025_h1
    # 4월: 직전 연도 annual vs 직전 연도 q3
    #   ex) 2026-04: 2025_annual vs 2025_q1 (실제 데이터 정책에 맞춤)
    # 5~6월: 당해 q1 vs 직전 연도 annual
    #   ex) 2026-05: 2026_q1 vs 2025_annual
    # 7월: 같은 페어 유지 (h1은 8월에야)
    # 8~9월: 당해 h1 vs 당해 q1
    #   ex) 2026-08: 2026_h1 vs 2026_q1
    # 10월: 같은 페어 유지
    # 11~12월: 당해 q3 vs 당해 h1

    if M >= 11:
        return f"{Y}_q3", f"{Y}_h1"
    if M >= 8:
        return f"{Y}_h1", f"{Y}_q1"
    if M >= 5:
        return f"{Y}_q1", f"{Y-1}_annual"
    if M >= 4:
        # 4월: 사업보고서 발행 직후. 분기 비교 정책 따라
        return f"{Y-1}_annual", f"{Y-1}_q1"
    # 1~3월: 직전 분기 q3 vs h1
    return f"{Y-1}_q3", f"{Y-1}_h1"


def resolve_pair_for_corp(conn: sqlite3.Connection, corp_code: str,
                          today: datetime = None) -> dict:
    """
    실제 DB에 보고서가 있는지 확인 후 페어 결정.
    DB에 없으면 한 단계 이전 페어로 폴백.
    """
    today = today or datetime.now()
    latest, prev = resolve_pair_by_date(today)

    # DB에서 해당 보고서 존재 확인
    rows = conn.execute("""
        SELECT report_type, id, rcept_dt
        FROM reports WHERE corp_code=?
          AND biz_content IS NOT NULL
          AND LENGTH(biz_content) >= 500
    """, [corp_code]).fetchall()
    available = {r["report_type"]: r for r in rows}

    if latest in available and prev in available:
        return {
            "latest": latest, "prev": prev,
            "latest_id": available[latest]["id"],
            "prev_id":   available[prev]["id"],
            "fallback": False,
        }

    # 폴백: 한 단계씩 후퇴
    fallback_chains = [
        (f"{today.year-1}_annual", f"{today.year-1}_q3"),
        (f"{today.year-1}_q3",     f"{today.year-1}_h1"),
        (f"{today.year-1}_h1",     f"{today.year-1}_q1"),
        (f"{today.year-2}_annual", f"{today.year-2}_q3"),
    ]
    for L, P in fallback_chains:
        if L in available and P in available:
            return {
                "latest": L, "prev": P,
                "latest_id": available[L]["id"],
                "prev_id":   available[P]["id"],
                "fallback": True,
            }

    return {"latest": None, "prev": None, "fallback": True, "available": list(available.keys())}


def main():
    p = argparse.ArgumentParser(description="비교 페어링 자동화 (Phase N)")
    p.add_argument("--corp-code", type=str)
    p.add_argument("--year",  type=int)
    p.add_argument("--month", type=int)
    args = p.parse_args()

    today = datetime.now()
    if args.year and args.month:
        today = datetime(args.year, args.month, 1)

    print(f"[기준 시점] {today.strftime('%Y-%m-%d')}")
    latest, prev = resolve_pair_by_date(today)
    print(f"\n[권장 비교 페어]")
    print(f"  최신:    {latest}")
    print(f"  비교대상: {prev}")
    print(f"\n[해석]")
    print(f"  {latest}와(과) {prev}를 비교해 변화를 추출합니다.")

    if args.corp_code:
        conn = get_conn()
        result = resolve_pair_for_corp(conn, args.corp_code, today)
        print(f"\n[기업 {args.corp_code} 실제 적용]")
        if result["latest"]:
            fb = " (폴백)" if result["fallback"] else ""
            print(f"  최신:    {result['latest']} (id={result['latest_id']}){fb}")
            print(f"  비교대상: {result['prev']} (id={result['prev_id']})")
        else:
            print(f"  ❌ 페어 구성 불가 — 가용 보고서: {result.get('available')}")
        conn.close()


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
