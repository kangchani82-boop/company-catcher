"""
scripts/expand_supply_chain.py
──────────────────────────────
공급망 네트워크 2촌 확장 — Stage 1

현재 supply_chain 테이블에 파트너사로 등록된 상장 기업 중
아직 자체 공급망 분석이 되지 않은 기업을 찾아
extract_supply_chain_claude.py 로 분석 확장.

실행:
  python scripts/expand_supply_chain.py --stats          # 확장 대상 현황만 확인
  python scripts/expand_supply_chain.py --limit 20       # 20개사 확장 분석
  python scripts/expand_supply_chain.py --dry-run        # 실제 실행 없이 대상 목록만 출력
  python scripts/expand_supply_chain.py --limit 20 --priority customer
                                                         # 고객사 파트너 우선 분석
"""

import io
import json
import sqlite3
import subprocess
import sys
import argparse
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ── 확장 대상 조회 ─────────────────────────────────────────────────────────────
def get_expansion_candidates(conn: sqlite3.Connection,
                             priority_type: str | None = None,
                             limit: int = 50) -> list[dict]:
    """
    partner_mapping에 매핑된 상장사 중 supply_chain에 허브로 없는 기업.
    = 남의 공급망에는 파트너로 등장하지만, 자신의 공급망은 미분석.

    priority_type: 특정 relation_type 파트너 우선 (예: 'customer')
    """
    priority_clause = ""
    params = []
    if priority_type:
        priority_clause = f"AND sc.relation_type = ?"
        params.append(priority_type)

    query = f"""
        SELECT
            pm.corp_code,
            pm.corp_name,
            COUNT(DISTINCT sc.corp_code) AS hub_link_cnt,
            GROUP_CONCAT(DISTINCT sc.relation_type) AS relation_types,
            GROUP_CONCAT(sc.corp_name, ', ') AS linked_from
        FROM partner_mapping pm
        JOIN supply_chain sc ON sc.partner_name = pm.partner_name
            {priority_clause}
        WHERE pm.is_listed = 1
          AND pm.corp_code IS NOT NULL
          AND pm.corp_code != ''
          -- 아직 자체 supply_chain 분석 없음
          AND NOT EXISTS (
              SELECT 1 FROM supply_chain s2
              WHERE s2.corp_code = pm.corp_code
          )
          -- DART biz_content 있는 기업만
          AND EXISTS (
              SELECT 1 FROM reports r
              WHERE r.corp_code = pm.corp_code
                AND r.biz_content IS NOT NULL
                AND LENGTH(r.biz_content) >= 500
          )
        GROUP BY pm.corp_code, pm.corp_name
        ORDER BY hub_link_cnt DESC, pm.corp_name
        LIMIT ?
    """
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


# ── 통계 출력 ─────────────────────────────────────────────────────────────────
def print_stats(conn: sqlite3.Connection):
    print("=" * 65)
    print("  공급망 2촌 확장 현황")
    print("=" * 65)

    # 현재 허브 수
    hubs = conn.execute("SELECT COUNT(DISTINCT corp_code) FROM supply_chain").fetchone()[0]
    links = conn.execute("SELECT COUNT(*) FROM supply_chain").fetchone()[0]
    print(f"  현재 허브: {hubs:,}개사 / 총 관계: {links:,}건")

    # partner_mapping 현황
    pm_total = conn.execute("SELECT COUNT(*) FROM partner_mapping").fetchone()[0]
    pm_listed = conn.execute("SELECT COUNT(*) FROM partner_mapping WHERE is_listed=1").fetchone()[0]
    pm_mapped = conn.execute("SELECT COUNT(*) FROM partner_mapping WHERE corp_code IS NOT NULL AND corp_code != ''").fetchone()[0]
    print(f"  파트너 매핑: {pm_total:,}건 (상장 {pm_listed:,}건 / corp_code 있음 {pm_mapped:,}건)")

    # Stage 1 확장 후보
    candidates = get_expansion_candidates(conn, limit=1000)
    print(f"\n  ─── Stage 1 확장 후보 ─────────────────────────────")
    print(f"  대상: {len(candidates)}개사 (상장+biz_content있음+자체분석없음)")

    if candidates:
        print(f"\n  상위 20개사 (연결 허브 수 기준):")
        for i, c in enumerate(candidates[:20], 1):
            rtypes = c.get("relation_types") or ""
            from_  = (c.get("linked_from") or "")[:40]
            print(f"  {i:3d}. {c['corp_name']:<20} | 연결 {c['hub_link_cnt']}개 허브 | {rtypes} | ← {from_}")

    # 관계 유형별 분포
    print(f"\n  supply_chain 관계 유형별:")
    for r in conn.execute("""
        SELECT relation_type, COUNT(*) cnt,
               COUNT(DISTINCT corp_code) corp_cnt
        FROM supply_chain GROUP BY relation_type ORDER BY cnt DESC
    """):
        print(f"    {r['relation_type']:<12}: {r['cnt']:>6,}건 ({r['corp_cnt']:,}개 허브)")

    # confidence 분포
    has_conf = conn.execute("SELECT COUNT(*) FROM supply_chain WHERE confidence IS NOT NULL").fetchone()[0]
    if has_conf:
        print(f"\n  신뢰도(confidence) 분포:")
        for r in conn.execute("""
            SELECT confidence, COUNT(*) cnt FROM supply_chain
            WHERE confidence IS NOT NULL
            GROUP BY confidence ORDER BY cnt DESC
        """):
            print(f"    {r['confidence']:<10}: {r['cnt']:,}건")

    # revenue_share 있는 항목
    rev_cnt = conn.execute("SELECT COUNT(*) FROM supply_chain WHERE revenue_share_pct IS NOT NULL").fetchone()[0]
    if rev_cnt:
        print(f"\n  매출비중(revenue_share_pct) 있는 항목: {rev_cnt:,}건")

    print("=" * 65)


# ── Stage 1 확장 실행 ──────────────────────────────────────────────────────────
def run_expansion(conn: sqlite3.Connection, candidates: list[dict],
                  dry_run: bool = False) -> dict:
    """
    확장 후보 기업들에 대해 extract_supply_chain_claude.py 개별 실행.
    """
    stats = {"attempted": 0, "success": 0, "failed": 0, "skipped": 0}

    extract_script = ROOT / "scripts" / "extract_supply_chain_claude.py"
    if not extract_script.exists():
        print(f"[오류] 스크립트 없음: {extract_script}")
        return stats

    for i, cand in enumerate(candidates, 1):
        corp_code = cand["corp_code"]
        corp_name = cand["corp_name"]

        print(f"  [{i:3d}/{len(candidates)}] {corp_name:<20} (연결 {cand['hub_link_cnt']}개 허브)", end=" ")

        if dry_run:
            print("→ [dry-run 건너뜀]")
            stats["skipped"] += 1
            continue

        # extract_supply_chain_claude.py는 corp_code 직접 지정 옵션이 없으므로
        # 임시로 DB에서 해당 기업의 biz_content를 직접 호출 방식으로 처리
        # → corp_code 기반 단일 처리 스크립트 방식
        stats["attempted"] += 1

        try:
            result = subprocess.run(
                [sys.executable, str(extract_script), f"--corp-code={corp_code}", "--limit=1"],
                capture_output=True, text=True, encoding="utf-8", timeout=120,
                cwd=str(ROOT)
            )
            if result.returncode == 0:
                # 결과에서 추출된 관계 수 파악
                out = result.stdout
                import re
                m = re.search(r"(\d+)개 관계 추출", out)
                cnt = int(m.group(1)) if m else "?"
                print(f"→ ✅ {cnt}개 관계")
                stats["success"] += 1
            else:
                print(f"→ ⚠️  리턴코드={result.returncode}")
                stats["failed"] += 1
        except subprocess.TimeoutExpired:
            print("→ ⏱️  타임아웃")
            stats["failed"] += 1
        except Exception as e:
            print(f"→ ❌ {e}")
            stats["failed"] += 1

    return stats


# ── 진입점 ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="공급망 2촌 확장 — Stage 1")
    parser.add_argument("--stats",     action="store_true", help="현황 통계만 출력")
    parser.add_argument("--dry-run",   action="store_true", help="대상 목록만 출력 (실행 없음)")
    parser.add_argument("--limit",     type=int, default=20, metavar="N", help="처리 최대 기업 수 (기본 20)")
    parser.add_argument("--priority",  type=str, default=None,
                        choices=["customer", "supplier", "partner", "competitor"],
                        help="특정 관계 유형 파트너 우선 (기본: 전체)")
    args = parser.parse_args()

    conn = get_conn()
    try:
        if args.stats or (not args.dry_run and args.limit == 20 and not args.priority):
            print_stats(conn)
            if args.stats:
                return

        candidates = get_expansion_candidates(conn, priority_type=args.priority, limit=args.limit)

        if not candidates:
            print("\n확장 후보 없음 — 모든 상장 파트너사의 공급망이 이미 분석됐습니다.")
            return

        print(f"\n확장 대상: {len(candidates)}개사")
        if args.priority:
            print(f"우선 유형: {args.priority}")

        if args.dry_run:
            print("\n[dry-run] 실제 실행 없이 대상 목록:")
            for i, c in enumerate(candidates, 1):
                print(f"  {i:3d}. {c['corp_name']:<20} ({c['corp_code']}) 연결 {c['hub_link_cnt']}개 허브")
            return

        print("\n실행 중...\n")
        stats = run_expansion(conn, candidates, dry_run=False)
        print(f"\n완료: 시도 {stats['attempted']}건 / 성공 {stats['success']}건 / 실패 {stats['failed']}건")

        # 완료 후 통계
        print()
        print_stats(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
