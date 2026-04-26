"""
scripts/novelty_filter.py
─────────────────────────
Phase O — 새로움 검증 (Novelty Filter)

핵심: "이미 과거 보고서에서 등장한 내용이면 기사화 가치 없음"
사용자 원칙: "없다가 새로 생긴 것 / 변화한 것 / 없어진 것 / 축소된 것" 만 가치 있음.

N-Score 계산 (0~100):
  A. 키워드 과거 등장 (40%) — 단서 키워드가 같은 회사의 과거 보고서 본문에 등장했나?
  B. AI 분석 명시 (30%)    — ai_comparisons.result에 "신규/처음/도입/추가" 표현?
  C. Evidence 차이 (30%)   — evidence 문장이 과거 biz_content에 substring 매칭?

추가: 시계열 추세 (Phase O 강화)
  - 키워드 등장 빈도가 시간에 따라 어떻게 변화했나
  - "신규 등장" / "확장 중" / "이미 핵심 키워드" 자동 분류

등급:
  🟢 N-Score 70+   : 진짜 새로움 (출고 우선)
  🟡 N-Score 50-69 : 보통 (검토 후 출고)
  🔴 N-Score < 50  : 이미 알려짐 (기사화 패스)

실행:
  python scripts/novelty_filter.py --init
  python scripts/novelty_filter.py --build              # 모든 단서 점수 계산
  python scripts/novelty_filter.py --lead-id 12345
  python scripts/novelty_filter.py --severity 4
  python scripts/novelty_filter.py --stats
"""

import io
import re
import sys
import json
import sqlite3
import argparse
from pathlib import Path

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


def _ensure_utf8_io():
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS lead_novelty (
    lead_id INTEGER PRIMARY KEY,
    score_total INTEGER NOT NULL,
    score_kw_novelty INTEGER,        -- A 키워드 새로움 (40%)
    score_ai_novelty INTEGER,        -- B AI 명시 (30%)
    score_evidence_unique INTEGER,   -- C Evidence 차이 (30%)
    grade TEXT,                      -- A/B (70+) / C (50-69) / D (<50)
    pattern TEXT,                    -- NEW / EXPANDING / EXISTING / CHANGED / REMOVED
    keyword_history TEXT,            -- JSON: {keyword: [count_2023, count_2024, count_2025_q1, count_2025_annual]}
    similar_in_past TEXT,            -- JSON: 과거 보고서 매칭 발췌
    updated_at TEXT NOT NULL,
    FOREIGN KEY (lead_id) REFERENCES story_leads(id)
);
CREATE INDEX IF NOT EXISTS idx_ln_grade  ON lead_novelty(grade);
CREATE INDEX IF NOT EXISTS idx_ln_score  ON lead_novelty(score_total);
CREATE INDEX IF NOT EXISTS idx_ln_pattern ON lead_novelty(pattern);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# 과거 보고서 텍스트 풀 조회
# ══════════════════════════════════════════════════════════════════════════════

def get_past_reports(conn: sqlite3.Connection, corp_code: str,
                     exclude_ids: list[int] = None) -> list[dict]:
    """
    같은 회사의 과거 보고서 본문 (현재 비교 대상이 아닌 것들).
    report_type 우선순위: 오래된 것부터.
    """
    exclude = exclude_ids or []
    placeholders = ",".join("?" * len(exclude)) if exclude else ""
    where = "corp_code=? AND biz_content IS NOT NULL"
    if placeholders:
        where += f" AND id NOT IN ({placeholders})"

    rows = conn.execute(f"""
        SELECT id, report_type, rcept_dt, biz_content
        FROM reports
        WHERE {where}
        ORDER BY rcept_dt ASC
    """, [corp_code] + exclude).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# A. 키워드 과거 등장 빈도 추적
# ══════════════════════════════════════════════════════════════════════════════

def keyword_history(conn: sqlite3.Connection, corp_code: str,
                    keywords: list[str]) -> dict:
    """
    각 키워드의 시간순 등장 빈도.
    Returns: {keyword: [(report_type, rcept_dt, count), ...]}
    """
    reports = get_past_reports(conn, corp_code)
    out = {}
    for kw in keywords:
        if not kw or len(kw) < 2:
            continue
        timeline = []
        for r in reports:
            cnt = r["biz_content"].count(kw)
            timeline.append({
                "report": r["report_type"],
                "date": r["rcept_dt"],
                "count": cnt,
            })
        out[kw] = timeline
    return out


def calc_kw_novelty(history: dict) -> tuple[int, str]:
    """
    키워드 시계열에서 새로움 점수 + 패턴 분류.
    Returns: (score 0-100, pattern)
    """
    if not history:
        return 50, "UNKNOWN"

    patterns = []
    scores = []

    for kw, timeline in history.items():
        if not timeline:
            continue
        counts = [t["count"] for t in timeline]
        if not any(counts):
            # 어느 보고서에도 등장 안 함 = 진짜 신규
            scores.append(100)
            patterns.append("NEW")
        elif counts[-1] > 0 and all(c == 0 for c in counts[:-1]):
            # 가장 최근에만 등장 = 신규 진입
            scores.append(95)
            patterns.append("NEW")
        elif counts[-1] > sum(counts[:-1]):
            # 최근 등장이 과거 합보다 많음 = 확장 중
            scores.append(70)
            patterns.append("EXPANDING")
        elif counts[-1] > 0 and any(counts[:-1]):
            # 꾸준히 등장 = 이미 핵심 키워드
            scores.append(30)
            patterns.append("EXISTING")
        elif counts[-1] == 0 and any(counts[:-1]):
            # 이전엔 있었으나 최근 사라짐 = 제거
            scores.append(85)
            patterns.append("REMOVED")
        else:
            scores.append(50)
            patterns.append("UNKNOWN")

    if not scores:
        return 50, "UNKNOWN"

    avg = int(sum(scores) / len(scores))

    # 가장 새로움이 큰 패턴 우선
    priority = ["NEW", "REMOVED", "EXPANDING", "CHANGED", "EXISTING", "UNKNOWN"]
    pattern = sorted(patterns, key=lambda p: priority.index(p) if p in priority else 99)[0]
    return avg, pattern


# ══════════════════════════════════════════════════════════════════════════════
# B. AI 분석 명시 (신규/처음/도입 등)
# ══════════════════════════════════════════════════════════════════════════════

NEW_PATTERNS = [
    r"신규\s*(?:사업|진출|추가|등장|편입|출시|도입)",
    r"새롭게\s*(?:추가|등장|시작|진출)",
    r"새로운\s*(?:사업|영역|분야)",
    r"처음(?:으로)?\s*(?:등장|진출|시작|언급|편입|추가|발표)",
    r"본격(?:화|적|적인)?\s*(?:진출|확대|착수)",
    r"착수했",
    r"진출(?:한다|했다|을\s*공식화)",
    r"추가(?:됐|되었|한\s*것)",
    r"도입(?:했|한\s*것|을\s*결정)",
    r"기존엔?\s*없었(?:으나|던)",
    r"이번에\s*처음",
]

REMOVED_PATTERNS = [
    r"폐지(?:했|한\s*것)",
    r"청산(?:했|한\s*것)",
    r"중단(?:했|한\s*것)",
    r"매각(?:했|한\s*것)",
    r"양도(?:했|한\s*것)",
    r"제외(?:됐|되었)",
    r"삭제(?:됐|되었)",
    r"기존(?:에는?)?\s*있었(?:으나|던)",
]


def calc_ai_novelty(ai_result: str) -> tuple[int, list[str]]:
    """
    ai_comparisons.result에서 "신규" 표현 강도 측정.
    Returns: (score 0-100, matched expressions)
    """
    if not ai_result:
        return 50, []

    matched = []
    new_hits = 0
    removed_hits = 0

    for pat in NEW_PATTERNS:
        for m in re.finditer(pat, ai_result):
            matched.append(("NEW", m.group(0)))
            new_hits += 1

    for pat in REMOVED_PATTERNS:
        for m in re.finditer(pat, ai_result):
            matched.append(("REMOVED", m.group(0)))
            removed_hits += 1

    total = new_hits + removed_hits

    if total >= 5:
        score = 95
    elif total >= 3:
        score = 80
    elif total >= 1:
        score = 65
    else:
        score = 30  # "신규" 표현 전혀 없음 = 새로움 약함

    return score, [m[1] for m in matched[:10]]


# ══════════════════════════════════════════════════════════════════════════════
# C. Evidence 문장이 과거 보고서에 있는가
# ══════════════════════════════════════════════════════════════════════════════

def calc_evidence_unique(conn: sqlite3.Connection, corp_code: str,
                         evidence: str, comparison_id: int | None) -> tuple[int, list[str]]:
    """
    단서 evidence의 핵심 명사구가 과거 보고서에 이미 등장했는가.
    등장 안 했을수록 새로움 ↑
    """
    if not evidence:
        return 50, []

    # 비교 대상이었던 보고서 ID 제외
    exclude_ids = []
    if comparison_id:
        cmp = conn.execute(
            "SELECT report_id_a, report_id_b FROM ai_comparisons WHERE id=?",
            [comparison_id]
        ).fetchone()
        if cmp:
            exclude_ids = [x for x in [cmp["report_id_a"], cmp["report_id_b"]] if x]

    # evidence에서 핵심 명사구 추출 (3글자+ 한글 + 영문 5자+)
    nouns = set(re.findall(r"[가-힣]{3,}", evidence))
    en = set(re.findall(r"[A-Za-z]{4,}", evidence))
    candidates = (nouns | en) - {
        "회사", "당사", "기업", "사업", "변화", "있다", "있는",
        "보고서", "분기", "기준", "분석", "활동", "수행",
    }
    candidates = {c for c in candidates if len(c) >= 3}
    if not candidates:
        return 60, []

    # 상위 10개 후보만 (성능)
    candidates = sorted(candidates, key=lambda x: -len(x))[:10]

    past_reports = get_past_reports(conn, corp_code, exclude_ids)
    if not past_reports:
        return 70, []  # 과거 자료 없음 = 검증 불가, 약간 가산

    past_text = " ".join(r["biz_content"][:30000] for r in past_reports)

    found_in_past = [c for c in candidates if c in past_text]
    not_found = [c for c in candidates if c not in past_text]

    # 못 찾을수록 새로움 ↑
    coverage = len(not_found) / max(1, len(candidates))
    score = int(coverage * 100)

    return score, not_found[:8]


# ══════════════════════════════════════════════════════════════════════════════
# 종합 점수
# ══════════════════════════════════════════════════════════════════════════════

def grade_from_score(s: int) -> tuple[str, str]:
    """등급 + AB 세분화 (사용자 Q3=B 변형)."""
    if s >= 85:  return "🟢A", "NEW_STRONG"
    if s >= 70:  return "🟢B", "NEW"
    if s >= 60:  return "🟡A", "MAYBE"
    if s >= 50:  return "🟡B", "WEAK"
    return "🔴", "EXISTING"


def calc_lead_novelty(conn: sqlite3.Connection, lead: sqlite3.Row) -> dict:
    corp_code = lead["corp_code"] or ""

    # keywords 파싱
    try:
        kw_list = json.loads(lead["keywords"] or "[]")
    except Exception:
        kw_list = []

    # ai_result
    ai_row = conn.execute(
        "SELECT result FROM ai_comparisons WHERE id=?",
        [lead["comparison_id"]]
    ).fetchone()
    ai_result = (ai_row["result"] if ai_row else "") or ""

    # A. 키워드 시계열
    kh = keyword_history(conn, corp_code, kw_list)
    s_kw, pattern = calc_kw_novelty(kh)

    # B. AI 명시
    s_ai, ai_matches = calc_ai_novelty(ai_result)

    # C. Evidence 차이
    s_ev, missing = calc_evidence_unique(
        conn, corp_code, lead["evidence"] or "",
        lead["comparison_id"]
    )

    total = int(s_kw * 0.4 + s_ai * 0.3 + s_ev * 0.3)
    grade, _ = grade_from_score(total)

    return {
        "lead_id":             lead["id"],
        "score_total":         total,
        "score_kw_novelty":    s_kw,
        "score_ai_novelty":    s_ai,
        "score_evidence_unique": s_ev,
        "grade":               grade,
        "pattern":             pattern,
        "keyword_history":     json.dumps({k: v for k, v in list(kh.items())[:5]},
                                          ensure_ascii=False),
        "similar_in_past":     json.dumps({
                                   "ai_matches": ai_matches,
                                   "evidence_missing": missing,
                               }, ensure_ascii=False),
    }


def save_novelty(conn: sqlite3.Connection, score: dict) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO lead_novelty
            (lead_id, score_total, score_kw_novelty, score_ai_novelty,
             score_evidence_unique, grade, pattern,
             keyword_history, similar_in_past, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
    """, [
        score["lead_id"], score["score_total"],
        score["score_kw_novelty"], score["score_ai_novelty"],
        score["score_evidence_unique"],
        score["grade"], score["pattern"],
        score["keyword_history"], score["similar_in_past"],
    ])


def build_all(conn: sqlite3.Connection, severity_min: int = 3,
              limit: int | None = None, force: bool = False) -> dict:
    where = "severity >= ?"
    params = [severity_min]
    if not force:
        where += " AND NOT EXISTS (SELECT 1 FROM lead_novelty ln WHERE ln.lead_id = sl.id)"

    q = f"SELECT * FROM story_leads sl WHERE {where} ORDER BY severity DESC, id DESC"
    if limit:
        q += f" LIMIT {limit}"

    rows = conn.execute(q, params).fetchall()
    print(f"[info] {len(rows)}개 단서 새로움 검증 중...")

    grade_count: dict[str, int] = {}
    pattern_count: dict[str, int] = {}
    for i, lead in enumerate(rows, 1):
        try:
            score = calc_lead_novelty(conn, lead)
            save_novelty(conn, score)
            grade_count[score["grade"]] = grade_count.get(score["grade"], 0) + 1
            pattern_count[score["pattern"]] = pattern_count.get(score["pattern"], 0) + 1
        except Exception as e:
            print(f"  ⚠ lead#{lead['id']}: {e}")
        if i % 100 == 0:
            print(f"  {i}/{len(rows)} 처리...")
            conn.commit()

    conn.commit()
    return {"grade": grade_count, "pattern": pattern_count}


def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 60)
    print("  새로움 검증 분포 (Phase O)")
    print("=" * 60)
    total = conn.execute("SELECT COUNT(*) FROM lead_novelty").fetchone()[0]
    if total == 0:
        print("  (아직 빌드 안 됨 — --build 실행)")
        return
    print(f"\n  전체 단서: {total:,}건\n  [등급]")
    for r in conn.execute("""
        SELECT grade, COUNT(*) cnt, AVG(score_total) avg_s
        FROM lead_novelty GROUP BY grade
        ORDER BY MIN(score_total) DESC
    """):
        print(f"    {r['grade']:<5} {r['cnt']:>4}건 (평균 {r['avg_s']:.0f}점)")

    print("\n  [패턴]")
    for r in conn.execute("""
        SELECT pattern, COUNT(*) cnt FROM lead_novelty
        GROUP BY pattern ORDER BY cnt DESC
    """):
        print(f"    {r['pattern']:<12} {r['cnt']:>4}건")

    print("\n  [상위 새로움 Top 15 — 출고 우선]")
    for r in conn.execute("""
        SELECT ln.score_total, ln.grade, ln.pattern, sl.corp_name, sl.lead_type, sl.severity, sl.title
        FROM lead_novelty ln JOIN story_leads sl ON ln.lead_id = sl.id
        ORDER BY ln.score_total DESC LIMIT 15
    """):
        print(f"  {r['grade']} {r['score_total']}점 | {r['pattern']:<10} | {r['corp_name'][:18]:<18} "
              f"| sev{r['severity']} | {(r['title'] or '')[:35]}")


def main():
    p = argparse.ArgumentParser(description="새로움 검증 (Phase O)")
    p.add_argument("--init",  action="store_true")
    p.add_argument("--build", action="store_true")
    p.add_argument("--lead-id", type=int)
    p.add_argument("--severity", type=int, default=3)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--force", action="store_true")
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()

    conn = get_conn()
    ensure_schema(conn)

    if args.init:
        print("✓ lead_novelty 스키마 생성 완료"); return
    if args.stats:
        print_stats(conn); return

    if args.lead_id:
        lead = conn.execute("SELECT * FROM story_leads WHERE id=?",
                            [args.lead_id]).fetchone()
        if not lead:
            print("단서 없음"); return
        score = calc_lead_novelty(conn, lead)
        save_novelty(conn, score)
        conn.commit()
        print(f"\n  {score['grade']} {score['score_total']}점 / {score['pattern']}")
        print(f"  └ 키워드 새로움: {score['score_kw_novelty']}")
        print(f"  └ AI 명시:       {score['score_ai_novelty']}")
        print(f"  └ Evidence 차이: {score['score_evidence_unique']}")
        return

    if args.build:
        gc = build_all(conn, args.severity, args.limit, args.force)
        print(f"\n[완료]")
        print(f"  등급: {gc['grade']}")
        print(f"  패턴: {gc['pattern']}")
        print()
        print_stats(conn)
        return

    print(__doc__)


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
