"""
scripts/detect_leads.py
────────────────────────
AI 비교 결과(ai_comparisons)에서 취재 단서(story_leads)를 자동 추출

실행 예시:
  python scripts/detect_leads.py --stats               # 현재 통계만 출력
  python scripts/detect_leads.py --limit 100           # 100개 비교 결과만 처리
  python scripts/detect_leads.py --comparison-id 42   # 특정 비교 결과만 처리
  python scripts/detect_leads.py                       # 전체 처리

알림 규칙(alert_rules)에서 키워드를 읽어 ai_comparisons.result 텍스트를 검색하고
매칭되면 story_leads 레코드를 삽입합니다.
"""

import io
import json
import os
import re
import sqlite3
import sys
import argparse
from pathlib import Path
from datetime import datetime

# UTF-8 출력 강제
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"


# ── .env 로드 ────────────────────────────────────────────────────────────────
def load_env(path: Path):
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key, val)


# ── DB 연결 ───────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


# ── story_leads UNIQUE 제약 보장 ──────────────────────────────────────────────
def ensure_unique_constraint(conn: sqlite3.Connection):
    """UNIQUE(corp_code, lead_type, comparison_id) 인덱스가 없으면 추가"""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='story_leads'"
    )
    existing = {row[0] for row in cur.fetchall()}
    if "uq_leads_corp_type_cmp" not in existing:
        try:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_leads_corp_type_cmp "
                "ON story_leads(corp_code, lead_type, comparison_id)"
            )
            conn.commit()
            print("[init] UNIQUE 인덱스 uq_leads_corp_type_cmp 생성됨")
        except sqlite3.OperationalError as e:
            print(f"[warn] UNIQUE 인덱스 생성 실패 (이미 존재하거나 충돌): {e}")


# ── 텍스트 추출 헬퍼 ──────────────────────────────────────────────────────────
def extract_evidence(text: str, keyword: str, max_chars: int = 500) -> str:
    """키워드가 포함된 문장(들)을 찾아 최대 max_chars 만큼 반환"""
    lower_text = text.lower()
    lower_kw = keyword.lower()
    pos = lower_text.find(lower_kw)
    if pos == -1:
        return ""

    # 문장 분리: ., \n, !, ? 기준
    sentences = re.split(r"(?<=[.!?\n])\s*", text)
    matched = []
    total = 0
    for sent in sentences:
        if lower_kw in sent.lower():
            s = sent.strip()
            if s:
                matched.append(s)
                total += len(s)
                if total >= max_chars:
                    break

    evidence = " ".join(matched)
    return evidence[:max_chars]


def extract_summary(text: str, keyword: str, window: int = 150) -> str:
    """키워드 주변 window 글자를 summary로 반환 (100-200자 목표)"""
    lower_text = text.lower()
    lower_kw = keyword.lower()
    pos = lower_text.find(lower_kw)
    if pos == -1:
        return ""
    start = max(0, pos - window // 2)
    end = min(len(text), pos + len(keyword) + window // 2)
    snippet = text[start:end].strip()
    # 앞뒤 잘린 경우 ... 처리
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet = snippet + "..."
    return snippet[:300]


# ── 통계 출력 ─────────────────────────────────────────────────────────────────
def print_stats(conn: sqlite3.Connection):
    print("=" * 60)
    print("  story_leads 현재 통계")
    print("=" * 60)

    cur = conn.execute("SELECT COUNT(*) FROM story_leads")
    total = cur.fetchone()[0]
    print(f"  전체 취재 단서: {total}건")

    if total == 0:
        print("  (아직 취재 단서가 없습니다)")
        print("=" * 60)
        return

    print()
    print("  [상태별]")
    for row in conn.execute(
        "SELECT status, COUNT(*) c FROM story_leads GROUP BY status ORDER BY c DESC"
    ):
        print(f"    {row['status']:<15} {row['c']}건")

    print()
    print("  [유형별]")
    for row in conn.execute(
        "SELECT lead_type, COUNT(*) c FROM story_leads GROUP BY lead_type ORDER BY c DESC"
    ):
        print(f"    {row['lead_type']:<20} {row['c']}건")

    print()
    print("  [심각도별]")
    for row in conn.execute(
        "SELECT severity, COUNT(*) c FROM story_leads GROUP BY severity ORDER BY severity DESC"
    ):
        print(f"    severity={row['severity']}  {row['c']}건")

    print()
    print("  [규칙별 (lead_type + severity)]")
    for row in conn.execute(
        "SELECT lead_type, severity, COUNT(*) c FROM story_leads "
        "GROUP BY lead_type, severity ORDER BY c DESC LIMIT 20"
    ):
        print(f"    {row['lead_type']:<20} severity={row['severity']}  {row['c']}건")

    print()
    print("  [최근 10건]")
    for row in conn.execute(
        "SELECT corp_name, lead_type, severity, title, created_at "
        "FROM story_leads ORDER BY created_at DESC LIMIT 10"
    ):
        print(f"    [{row['severity']}] {row['corp_name']} | {row['lead_type']} | {row['title'][:40]}")

    print()
    # ai_comparisons 처리 현황
    cur = conn.execute("SELECT COUNT(*) FROM ai_comparisons WHERE status='ok'")
    total_ok = cur.fetchone()[0]
    cur = conn.execute(
        "SELECT COUNT(DISTINCT comparison_id) FROM story_leads WHERE comparison_id IS NOT NULL"
    )
    processed = cur.fetchone()[0]
    print(f"  ai_comparisons 처리 현황: {processed}/{total_ok}건 처리됨")
    print("=" * 60)


# ── 메인 처리 ─────────────────────────────────────────────────────────────────
def process_comparisons(
    conn: sqlite3.Connection,
    limit: int | None = None,
    comparison_id: int | None = None,
) -> dict:
    """
    ai_comparisons를 순회하며 alert_rules 키워드로 story_leads를 생성.
    반환값: {"processed": int, "leads_found": int, "leads_by_rule": dict}
    """
    # 1) 활성 alert_rules 로드
    rules = conn.execute(
        "SELECT * FROM alert_rules WHERE is_active=1"
    ).fetchall()
    if not rules:
        print("[warn] 활성화된 alert_rules가 없습니다.")
        return {"processed": 0, "leads_found": 0, "leads_by_rule": {}}

    print(f"[info] 활성 알림 규칙: {len(rules)}개")
    for r in rules:
        kws = json.loads(r["keywords"]) if r["keywords"] else []
        print(f"  - [{r['rule_code']}] {r['lead_type']} severity={r['severity']} 키워드={kws[:3]}...")

    print()

    # 2) 처리할 ai_comparisons 쿼리
    #    미처리 = story_leads에 comparison_id가 없는 것
    if comparison_id is not None:
        query = """
            SELECT ac.id, ac.corp_code, ac.corp_name, ac.report_type_a, ac.report_type_b,
                   ac.model, ac.result
            FROM ai_comparisons ac
            WHERE ac.status = 'ok'
              AND ac.id = ?
              AND ac.result IS NOT NULL
        """
        params: tuple = (comparison_id,)
    else:
        query = """
            SELECT ac.id, ac.corp_code, ac.corp_name, ac.report_type_a, ac.report_type_b,
                   ac.model, ac.result
            FROM ai_comparisons ac
            WHERE ac.status = 'ok'
              AND ac.result IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM story_leads sl WHERE sl.comparison_id = ac.id
              )
            ORDER BY ac.id ASC
        """
        params = ()
        if limit is not None:
            query += f" LIMIT {int(limit)}"

    comparisons = conn.execute(query, params).fetchall()
    print(f"[info] 처리 대상 비교 결과: {len(comparisons)}건")

    if not comparisons:
        print("[info] 처리할 새 비교 결과가 없습니다.")
        return {"processed": 0, "leads_found": 0, "leads_by_rule": {}}

    # 규칙별 매칭 카운트
    leads_by_rule: dict[str, int] = {r["rule_code"]: 0 for r in rules}
    total_leads = 0
    processed = 0

    for cmp in comparisons:
        cmp_id = cmp["id"]
        corp_code = cmp["corp_code"]
        corp_name = cmp["corp_name"] or ""
        report_type_a = cmp["report_type_a"]
        report_type_b = cmp["report_type_b"]
        result_text = cmp["result"] or ""

        if not result_text.strip():
            processed += 1
            continue

        result_lower = result_text.lower()

        for rule in rules:
            rule_code = rule["rule_code"]
            lead_type = rule["lead_type"]
            severity = rule["severity"]
            title_tmpl = rule["title_tmpl"] or ""
            keywords: list[str] = json.loads(rule["keywords"]) if rule["keywords"] else []
            exclude_kw: list[str] = json.loads(rule["exclude_kw"]) if rule["exclude_kw"] else []
            require_context: list[str] = json.loads(rule["require_context"]) if rule["require_context"] else []
            min_evidence_len: int = rule["min_evidence_len"] or 0

            if not keywords:
                continue

            # 키워드 매칭 (case-insensitive)
            matched_kws = [kw for kw in keywords if kw.lower() in result_lower]
            if not matched_kws:
                continue

            # 첫 번째 매칭 키워드 기준으로 evidence/summary 추출
            first_kw = matched_kws[0]
            evidence = extract_evidence(result_text, first_kw, max_chars=1000)
            summary = extract_summary(result_text, first_kw, window=150)

            # 제외 키워드 체크 — evidence 범위(키워드 포함 문장)에서만 검사
            # 전체 result 텍스트가 아닌 매칭 문장에서 exclude_kw 확인
            if exclude_kw:
                check_text = evidence.lower() if evidence else result_lower
                excluded = any(ex.lower() in check_text for ex in exclude_kw)
                if excluded:
                    continue

            # 최소 evidence 길이 체크 (빈 섹션 헤더 제거)
            if min_evidence_len > 0 and len(evidence.strip()) < min_evidence_len:
                continue

            # require_context: evidence 내에 하나 이상의 '실제 변화' 단어 필요
            if require_context:
                ev_lower = evidence.lower()
                if not any(rc.lower() in ev_lower for rc in require_context):
                    continue

            # evidence를 500자로 다시 자름
            evidence = evidence[:500]

            # title 생성
            title = title_tmpl.replace("{corp_name}", corp_name)

            # story_leads 삽입 (중복 시 무시)
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO story_leads
                        (corp_code, corp_name, lead_type, severity, title,
                         summary, evidence, keywords, comparison_id,
                         report_type_a, report_type_b, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')
                    """,
                    (
                        corp_code,
                        corp_name,
                        lead_type,
                        severity,
                        title,
                        summary,
                        evidence,
                        json.dumps(matched_kws, ensure_ascii=False),
                        cmp_id,
                        report_type_a,
                        report_type_b,
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    leads_by_rule[rule_code] = leads_by_rule.get(rule_code, 0) + 1
                    total_leads += 1

            except sqlite3.IntegrityError:
                # UNIQUE 충돌 무시
                pass

        processed += 1
        if processed % 50 == 0:
            conn.commit()
            print(f"  진행: {processed}/{len(comparisons)} 처리, {total_leads}건 단서 발견")

    conn.commit()
    print(f"\n[완료] {processed}건 처리, {total_leads}건 취재 단서 생성")

    return {
        "processed": processed,
        "leads_found": total_leads,
        "leads_by_rule": leads_by_rule,
    }


# ── 결과 요약 출력 ────────────────────────────────────────────────────────────
def print_results(stats: dict, rules_map: dict[str, str]):
    print()
    print("=" * 60)
    print("  처리 결과 요약")
    print("=" * 60)
    print(f"  처리된 비교 결과: {stats['processed']}건")
    print(f"  생성된 취재 단서: {stats['leads_found']}건")
    print()
    print("  [규칙별 단서 수]")
    for rule_code, count in sorted(
        stats["leads_by_rule"].items(), key=lambda x: -x[1]
    ):
        lead_type = rules_map.get(rule_code, rule_code)
        print(f"    {rule_code:<20} ({lead_type:<20}) : {count}건")
    print("=" * 60)


# ── 진입점 ────────────────────────────────────────────────────────────────────
def main():
    load_env(ENV_PATH)

    parser = argparse.ArgumentParser(
        description="AI 비교 결과에서 취재 단서(story_leads)를 자동 추출합니다."
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="현재 story_leads 통계만 출력하고 종료",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="처리할 비교 결과 최대 개수 (기본: 전체)",
    )
    parser.add_argument(
        "--comparison-id",
        type=int,
        default=None,
        metavar="N",
        help="특정 comparison ID만 처리",
    )
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"[error] DB 파일을 찾을 수 없습니다: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = get_conn()

    try:
        if args.stats:
            print_stats(conn)
            return

        # UNIQUE 제약 확인/추가
        ensure_unique_constraint(conn)

        # 처리 실행
        stats = process_comparisons(
            conn,
            limit=args.limit,
            comparison_id=args.comparison_id,
        )

        # 규칙 코드 → lead_type 맵
        rules_map = {
            r["rule_code"]: r["lead_type"]
            for r in conn.execute("SELECT rule_code, lead_type FROM alert_rules").fetchall()
        }
        print_results(stats, rules_map)

        # 처리 후 통계
        print()
        print_stats(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
