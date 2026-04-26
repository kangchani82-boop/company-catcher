"""
scripts/classify_change_type.py
───────────────────────────────
Phase Q — 변화 유형 분류 + 산업 vs 개별 구분

각 단서/기사의 변화 유형을 자동 라벨링:
  NEW       — 없다가 새로 등장
  CHANGED   — 변경됨
  REMOVED   — 사라짐
  SHRUNK    — 축소
  EXPANDED  — 확대

산업 vs 개별 구분:
  - 같은 산업(sector)의 다른 회사들에서 같은 시기에 같은 패턴 변화 발생?
    YES → "산업 트렌드"
    NO  → "개별 기업 행보"

실행:
  python scripts/classify_change_type.py --init
  python scripts/classify_change_type.py --build              # 모든 단서 분류
  python scripts/classify_change_type.py --lead-id 12345
  python scripts/classify_change_type.py --stats
"""

import io
import re
import sys
import sqlite3
import argparse
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


def _ensure_utf8_io():
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS lead_change_class (
    lead_id INTEGER PRIMARY KEY,
    change_type TEXT NOT NULL,           -- NEW/CHANGED/REMOVED/SHRUNK/EXPANDED
    industry_or_individual TEXT,         -- INDUSTRY/INDIVIDUAL/UNKNOWN
    industry_match_count INTEGER,        -- 같은 패턴 회사 수
    matched_keywords TEXT,               -- JSON
    classified_at TEXT NOT NULL,
    FOREIGN KEY (lead_id) REFERENCES story_leads(id)
);
CREATE INDEX IF NOT EXISTS idx_lcc_type    ON lead_change_class(change_type);
CREATE INDEX IF NOT EXISTS idx_lcc_indiv   ON lead_change_class(industry_or_individual);
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
# 1. 변화 유형 분류 (텍스트 패턴 매칭)
# ══════════════════════════════════════════════════════════════════════════════

PATTERNS = {
    "NEW": [
        r"신규\s*(?:사업|진출|추가|등장|편입|출시|도입|투자|법인)",
        r"새롭게\s*(?:추가|등장|시작|진출)",
        r"새로운\s*(?:사업|영역|분야|법인)",
        r"처음(?:으로)?\s*(?:등장|진출|시작|편입|추가|언급)",
        r"본격(?:적|화)?\s*(?:진출|착수)",
        r"신설(?:했|됐|된)",
        r"기존엔?\s*없었(?:으나|던)",
        r"이번에\s*처음",
    ],
    "REMOVED": [
        r"폐지(?:했|됐|한\s*것)",
        r"청산(?:했|됐|한\s*것)",
        r"중단(?:했|됐|한\s*것)",
        r"매각(?:했|됐|한\s*것)",
        r"양도(?:했|됐|한\s*것)",
        r"제외(?:됐|되었|한\s*것)",
        r"삭제(?:됐|되었|한\s*것)",
        r"기존(?:에는?)?\s*있었(?:으나|던)",
        r"사업\s*철수",
        r"법인\s*해산",
    ],
    "EXPANDED": [
        r"확대(?:했|됐)",
        r"확장(?:했|됐)",
        r"증설(?:했|됐)",
        r"비중\s*(?:증가|상승|확대)",
        r"점유율\s*(?:상승|증가)",
        r"매출\s*비중.*(?:증가|상승|확대)",
    ],
    "SHRUNK": [
        r"축소(?:했|됐)",
        r"감축(?:했|됐)",
        r"비중\s*(?:감소|하락|축소)",
        r"점유율\s*(?:하락|감소)",
        r"인력\s*(?:감축|구조조정)",
    ],
    "CHANGED": [
        r"변경(?:했|됐)",
        r"전환(?:했|됐)",
        r"개편(?:했|됐)",
        r"통합(?:했|됐)",
        r"분리(?:했|됐)",
        r"조정(?:했|됐)",
    ],
}


def classify_change(text: str) -> tuple[str, list[str]]:
    """텍스트에서 변화 유형 분류. (type, matched_phrases)."""
    if not text:
        return "UNKNOWN", []

    type_hits = {t: 0 for t in PATTERNS}
    matched_phrases = []

    for ctype, pats in PATTERNS.items():
        for pat in pats:
            for m in re.finditer(pat, text):
                type_hits[ctype] += 1
                matched_phrases.append((ctype, m.group(0)))

    # 가장 많은 hit 유형
    best_type = max(type_hits, key=lambda k: type_hits[k])
    if type_hits[best_type] == 0:
        return "UNKNOWN", []

    return best_type, [p[1] for p in matched_phrases[:8]]


# ══════════════════════════════════════════════════════════════════════════════
# 2. 산업 vs 개별 구분
# ══════════════════════════════════════════════════════════════════════════════

def industry_vs_individual(conn: sqlite3.Connection, lead: sqlite3.Row,
                           change_type: str, keywords: list[str]) -> tuple[str, int]:
    """
    같은 sector의 다른 회사들이 같은 변화 유형을 가졌는지 확인.
    Returns: (label, match_count)
    """
    corp_code = lead["corp_code"] or ""

    # 같은 회사 sector 조회
    corp_row = conn.execute(
        "SELECT sector FROM companies WHERE corp_code=?", [corp_code]
    ).fetchone()
    if not corp_row or not corp_row["sector"]:
        return "UNKNOWN", 0
    sector = corp_row["sector"]

    # 같은 sector + 같은 change_type 단서 수
    try:
        rows = conn.execute("""
            SELECT lcc.lead_id FROM lead_change_class lcc
            JOIN story_leads sl ON lcc.lead_id = sl.id
            JOIN companies c    ON sl.corp_code = c.corp_code
            WHERE c.sector = ? AND lcc.change_type = ?
              AND sl.id != ?
        """, [sector, change_type, lead["id"]]).fetchall()
    except sqlite3.OperationalError:
        return "UNKNOWN", 0

    match_count = len(rows)
    if match_count >= 5:
        return "INDUSTRY", match_count   # 5개사 이상 같은 변화 = 산업 트렌드
    if match_count >= 2:
        return "MIXED", match_count
    return "INDIVIDUAL", match_count


# ══════════════════════════════════════════════════════════════════════════════
# 3. 단일 단서 분류
# ══════════════════════════════════════════════════════════════════════════════

def classify_lead(conn: sqlite3.Connection, lead: sqlite3.Row) -> dict:
    # ai_comparisons.result + evidence 합쳐서 분류
    ai_row = conn.execute(
        "SELECT result FROM ai_comparisons WHERE id=?",
        [lead["comparison_id"]]
    ).fetchone()
    ai_text = (ai_row["result"] if ai_row else "") or ""
    ev_text = lead["evidence"] or ""

    combined = f"{ev_text}\n\n{ai_text[:5000]}"
    change_type, matched = classify_change(combined)

    # 산업 vs 개별
    import json as _json
    try:
        kw_list = _json.loads(lead["keywords"] or "[]")
    except Exception:
        kw_list = []

    indiv_label, match_count = industry_vs_individual(
        conn, lead, change_type, kw_list
    )

    return {
        "lead_id":                lead["id"],
        "change_type":            change_type,
        "industry_or_individual": indiv_label,
        "industry_match_count":   match_count,
        "matched_keywords":       _json.dumps(matched, ensure_ascii=False),
    }


def save_classification(conn: sqlite3.Connection, c: dict) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO lead_change_class
            (lead_id, change_type, industry_or_individual,
             industry_match_count, matched_keywords, classified_at)
        VALUES (?,?,?,?,?,datetime('now','localtime'))
    """, [
        c["lead_id"], c["change_type"], c["industry_or_individual"],
        c["industry_match_count"], c["matched_keywords"],
    ])


# ══════════════════════════════════════════════════════════════════════════════
# 일괄 빌드
# ══════════════════════════════════════════════════════════════════════════════

def build_all(conn: sqlite3.Connection, severity_min: int = 3,
              limit: int | None = None, force: bool = False) -> dict:
    where = "severity >= ?"
    if not force:
        where += " AND NOT EXISTS (SELECT 1 FROM lead_change_class lcc WHERE lcc.lead_id = sl.id)"

    q = f"SELECT * FROM story_leads sl WHERE {where} ORDER BY severity DESC, id DESC"
    if limit:
        q += f" LIMIT {limit}"

    rows = conn.execute(q, [severity_min]).fetchall()
    print(f"[info] {len(rows)}개 단서 분류 중...")

    counts = Counter()
    indiv_counts = Counter()
    for i, lead in enumerate(rows, 1):
        try:
            c = classify_lead(conn, lead)
            save_classification(conn, c)
            counts[c["change_type"]] += 1
            indiv_counts[c["industry_or_individual"]] += 1
        except Exception as e:
            print(f"  ⚠ lead#{lead['id']}: {e}")
        if i % 100 == 0:
            print(f"  {i}/{len(rows)}")
            conn.commit()

    conn.commit()
    return {"types": dict(counts), "indiv": dict(indiv_counts)}


def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 60)
    print("  변화 유형 분류 (Phase Q)")
    print("=" * 60)
    total = conn.execute("SELECT COUNT(*) FROM lead_change_class").fetchone()[0]
    if total == 0:
        print("  (아직 빌드 안 됨)"); return
    print(f"\n  전체: {total:,}건\n  [변화 유형]")
    for r in conn.execute("""
        SELECT change_type, COUNT(*) cnt FROM lead_change_class
        GROUP BY change_type ORDER BY cnt DESC
    """):
        icons = {"NEW":"🆕","CHANGED":"🔄","REMOVED":"🗑","SHRUNK":"📉",
                 "EXPANDED":"📈","UNKNOWN":"❓"}
        print(f"    {icons.get(r['change_type'],'⚪')} {r['change_type']:<10} {r['cnt']:>4}건")

    print("\n  [산업 vs 개별]")
    for r in conn.execute("""
        SELECT industry_or_individual, COUNT(*) cnt FROM lead_change_class
        GROUP BY industry_or_individual ORDER BY cnt DESC
    """):
        print(f"    {r['industry_or_individual']:<12} {r['cnt']:>4}건")

    print("\n  [산업 트렌드 발견 — 5개사+ 동일 변화]")
    for r in conn.execute("""
        SELECT lcc.change_type, c.sector, COUNT(*) cnt
        FROM lead_change_class lcc
        JOIN story_leads sl ON lcc.lead_id = sl.id
        JOIN companies c    ON sl.corp_code = c.corp_code
        WHERE lcc.industry_or_individual = 'INDUSTRY'
        GROUP BY lcc.change_type, c.sector
        HAVING COUNT(*) >= 3
        ORDER BY cnt DESC LIMIT 10
    """):
        print(f"    {r['change_type']:<10} | {(r['sector'] or 'N/A')[:25]:<25} | {r['cnt']}개사")


def main():
    p = argparse.ArgumentParser(description="변화 유형 분류 (Phase Q)")
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
        print("✓ lead_change_class 스키마 생성"); return
    if args.stats:
        print_stats(conn); return

    if args.lead_id:
        lead = conn.execute("SELECT * FROM story_leads WHERE id=?",
                            [args.lead_id]).fetchone()
        if not lead:
            print("단서 없음"); return
        c = classify_lead(conn, lead)
        save_classification(conn, c)
        conn.commit()
        print(f"  lead #{lead['id']} {lead['corp_name']}")
        print(f"  변화 유형: {c['change_type']}")
        print(f"  산업/개별: {c['industry_or_individual']} (매치 {c['industry_match_count']}개)")
        return

    if args.build:
        r = build_all(conn, args.severity, args.limit, args.force)
        print(f"\n[완료]")
        print(f"  변화 유형: {r['types']}")
        print(f"  산업/개별: {r['indiv']}")
        print()
        print_stats(conn)
        return

    print(__doc__)


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
