"""
scripts/info_gap_score.py
─────────────────────────
Phase T — 정보 격차 점수 시스템

사명: "상장사는 많은데 정보 격차가 크다. 그 격차를 줄인다."
       → 잘 안 알려진 회사를 자동 발굴해 우선순위 ↑

점수 0~100 (높을수록 정보 격차 큼 = 기사 우선순위 높음)

  factor                         weight  계산 방식
  ──────────────────────────────  ─────  ─────────────────────────────
  A. 보도량 부족                    40%   external_sources 보도 수 (적을수록 점수↑)
  B. 시가총액 작음                  20%   financials.total_equity (작을수록 점수↑)
  C. 기존 기사 없음                 20%   article_drafts 그 회사 기사 수
  D. 공급망 허브 아님               20%   supply_chain partner_name 등장 수

총점 = A*0.4 + B*0.2 + C*0.2 + D*0.2

등급:
  🚨 90+ : 매우 소외 (최우선 출고)
  ⭐ 70-89: 소외 (우선 출고)
  📰 50-69: 보통
  🌟 30-49: 잘 알려짐
  📺 0-29: 매우 잘 알려짐 (대형 종목, 후순위)

실행:
  python scripts/info_gap_score.py --init             # 스키마만
  python scripts/info_gap_score.py --build            # 전체 점수 계산
  python scripts/info_gap_score.py --corp-code 00100814   # 단일 조회
  python scripts/info_gap_score.py --top 30           # 정보격차 큰 회사 Top
  python scripts/info_gap_score.py --stats
"""

import io
import sys
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
CREATE TABLE IF NOT EXISTS company_info_gap (
    corp_code TEXT PRIMARY KEY,
    corp_name TEXT,
    score_total INTEGER NOT NULL,
    score_news_lack INTEGER,         -- A 보도량 부족
    score_size_small INTEGER,        -- B 시가총액 작음
    score_article_lack INTEGER,      -- C 기존 기사 없음
    score_not_hub INTEGER,           -- D 공급망 허브 아님
    grade TEXT,                      -- 🚨 / ⭐ / 📰 / 🌟 / 📺
    news_count INTEGER,
    article_count INTEGER,
    sc_appearance INTEGER,           -- 공급망 등장 수
    total_equity REAL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cig_grade ON company_info_gap(grade);
CREATE INDEX IF NOT EXISTS idx_cig_score ON company_info_gap(score_total DESC);
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
# 점수 계산 (4개 factor)
# ══════════════════════════════════════════════════════════════════════════════

def grade_from_score(s: int) -> str:
    if s >= 90: return "🚨"
    if s >= 70: return "⭐"
    if s >= 50: return "📰"
    if s >= 30: return "🌟"
    return "📺"


def _bucket_score(value: float, thresholds: list) -> int:
    """
    value가 작을수록 점수 ↑ (정보 격차 큼).
    thresholds = [t1, t2, t3, t4]  (오름차순)
      value <= t1 → 100
      value <= t2 →  80
      value <= t3 →  60
      value <= t4 →  40
      else        →  20
    """
    for i, t in enumerate(thresholds):
        if value <= t:
            return [100, 80, 60, 40][i]
    return 20


def calc_news_lack_score(news_count: int) -> int:
    # 적을수록 점수 ↑
    # 0건=100 / 1-2건=85 / 3-5건=65 / 6-10건=45 / 11-20건=25 / 20+=10
    if news_count == 0:  return 100
    if news_count <= 2:  return 85
    if news_count <= 5:  return 65
    if news_count <= 10: return 45
    if news_count <= 20: return 25
    return 10


def calc_size_score(total_equity: float | None) -> int:
    # 작을수록 점수 ↑ (소형주일수록 정보 격차 큼)
    if total_equity is None or total_equity <= 0:
        return 60   # 데이터 없으면 중립
    # 단위: 원
    # < 500억 (5e10) = 100
    # < 1000억 = 85
    # < 5000억 = 65
    # < 1조 = 45
    # < 5조 = 25
    # 5조+ = 10
    if total_equity < 5e10:  return 100
    if total_equity < 1e11:  return 85
    if total_equity < 5e11:  return 65
    if total_equity < 1e12:  return 45
    if total_equity < 5e12:  return 25
    return 10


def calc_article_lack_score(article_count: int) -> int:
    # 적을수록 점수 ↑
    if article_count == 0:  return 100
    if article_count == 1:  return 80
    if article_count <= 3:  return 60
    if article_count <= 5:  return 40
    return 20


def calc_not_hub_score(sc_appearance: int) -> int:
    # 공급망에서 partner_name으로 적게 등장할수록 점수 ↑
    # (자주 등장 = 시장에서 알려진 허브 = 정보 격차 작음)
    if sc_appearance == 0:   return 100
    if sc_appearance <= 2:   return 80
    if sc_appearance <= 5:   return 60
    if sc_appearance <= 15:  return 40
    if sc_appearance <= 50:  return 20
    return 10


# ══════════════════════════════════════════════════════════════════════════════
# 단일 회사 점수 계산
# ══════════════════════════════════════════════════════════════════════════════

def calc_corp_score(conn: sqlite3.Connection, corp_code: str) -> dict | None:
    """단일 corp_code 점수 산출."""
    info = conn.execute(
        "SELECT corp_code, corp_name FROM companies WHERE corp_code=?",
        [corp_code]
    ).fetchone()
    if not info:
        # companies에 없으면 reports에서 추출
        info = conn.execute(
            "SELECT DISTINCT corp_code, corp_name FROM reports WHERE corp_code=? LIMIT 1",
            [corp_code]
        ).fetchone()
        if not info:
            return None

    corp_name = info["corp_name"]

    # A. 보도량
    news_count = conn.execute(
        "SELECT COUNT(*) FROM external_sources WHERE related_corp_code=?",
        [corp_code]
    ).fetchone()[0]

    # B. 시가총액 (financials.total_equity 최신값 사용)
    fin_row = conn.execute("""
        SELECT total_equity FROM financials
        WHERE corp_code=? AND total_equity IS NOT NULL
        ORDER BY fiscal_year DESC LIMIT 1
    """, [corp_code]).fetchone()
    total_equity = float(fin_row["total_equity"]) if fin_row else None

    # C. 기사 수
    article_count = conn.execute(
        "SELECT COUNT(*) FROM article_drafts WHERE corp_code=?",
        [corp_code]
    ).fetchone()[0]

    # D. 공급망 등장 (partner_name으로 — 거래처로 자주 언급되는가)
    sc_appearance = conn.execute(
        "SELECT COUNT(*) FROM supply_chain WHERE partner_name LIKE ?",
        [f"%{corp_name}%"]
    ).fetchone()[0]

    # 점수 계산
    s_news = calc_news_lack_score(news_count)
    s_size = calc_size_score(total_equity)
    s_art  = calc_article_lack_score(article_count)
    s_hub  = calc_not_hub_score(sc_appearance)

    total = int(s_news * 0.4 + s_size * 0.2 + s_art * 0.2 + s_hub * 0.2)

    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "score_total": total,
        "score_news_lack": s_news,
        "score_size_small": s_size,
        "score_article_lack": s_art,
        "score_not_hub": s_hub,
        "grade": grade_from_score(total),
        "news_count": news_count,
        "article_count": article_count,
        "sc_appearance": sc_appearance,
        "total_equity": total_equity,
    }


def save_score(conn: sqlite3.Connection, score: dict) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO company_info_gap
            (corp_code, corp_name, score_total,
             score_news_lack, score_size_small,
             score_article_lack, score_not_hub,
             grade, news_count, article_count,
             sc_appearance, total_equity, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
    """, [
        score["corp_code"], score["corp_name"], score["score_total"],
        score["score_news_lack"], score["score_size_small"],
        score["score_article_lack"], score["score_not_hub"],
        score["grade"], score["news_count"], score["article_count"],
        score["sc_appearance"], score["total_equity"],
    ])


# ══════════════════════════════════════════════════════════════════════════════
# 일괄 빌드
# ══════════════════════════════════════════════════════════════════════════════

def build_all(conn: sqlite3.Connection, limit: int | None = None) -> dict:
    # 분석 가능한 모든 기업
    rows = conn.execute("""
        SELECT DISTINCT corp_code, MAX(corp_name) AS corp_name
        FROM reports WHERE biz_content IS NOT NULL
          AND LENGTH(biz_content) >= 500
        GROUP BY corp_code
        ORDER BY corp_code
    """).fetchall()
    if limit:
        rows = rows[:limit]

    print(f"[info] {len(rows)}개 기업 점수 산정 중...")

    grade_count: dict[str, int] = {}
    for i, r in enumerate(rows, 1):
        try:
            score = calc_corp_score(conn, r["corp_code"])
            if score:
                save_score(conn, score)
                grade_count[score["grade"]] = grade_count.get(score["grade"], 0) + 1
        except Exception as e:
            print(f"  ⚠ {r['corp_code']} 실패: {e}")
        if i % 200 == 0:
            print(f"  {i}/{len(rows)} 처리...")
            conn.commit()

    conn.commit()
    print(f"\n[완료] {len(rows)}개 처리")
    return grade_count


def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 60)
    print("  정보 격차 점수 분포 (Phase T)")
    print("=" * 60)
    total = conn.execute("SELECT COUNT(*) FROM company_info_gap").fetchone()[0]
    if total == 0:
        print("  (아직 빌드 안 됨 — --build 실행)")
        return
    print(f"\n  전체: {total:,}개사\n")
    for r in conn.execute("""
        SELECT grade, COUNT(*) cnt, AVG(score_total) avg_s
        FROM company_info_gap GROUP BY grade
        ORDER BY MIN(score_total) DESC
    """):
        print(f"  {r['grade']} {r['cnt']:>4}개 (평균 {r['avg_s']:.0f}점)")
    print()
    print("  [상위 정보 격차 큰 회사 Top 15]")
    for r in conn.execute("""
        SELECT corp_name, score_total, grade,
               news_count, article_count, sc_appearance
        FROM company_info_gap
        ORDER BY score_total DESC LIMIT 15
    """):
        print(f"  {r['grade']} {r['score_total']}점 | {r['corp_name'][:20]:<20} "
              f"| 보도 {r['news_count']:>3} / 기사 {r['article_count']:>2} / 공급망 {r['sc_appearance']:>2}")


def print_top(conn: sqlite3.Connection, limit: int = 30) -> None:
    print(f"\n=== 정보 격차 Top {limit} (소외 기업 우선) ===\n")
    rows = conn.execute("""
        SELECT * FROM company_info_gap
        ORDER BY score_total DESC LIMIT ?
    """, [limit]).fetchall()
    for r in rows:
        eq = (r["total_equity"] / 1e8) if r["total_equity"] else 0
        print(f"  {r['grade']} {r['score_total']:>3}점 | {r['corp_name'][:18]:<18} "
              f"| 자본 {eq:>7.0f}억 | 보도 {r['news_count']:>3} | 기사 {r['article_count']}")


def main():
    p = argparse.ArgumentParser(description="정보 격차 점수 시스템 (Phase T)")
    p.add_argument("--init",  action="store_true")
    p.add_argument("--build", action="store_true", help="전체 기업 점수 빌드")
    p.add_argument("--corp-code", type=str, help="단일 회사 점수 조회")
    p.add_argument("--top",   type=int, default=0, help="상위 N건")
    p.add_argument("--stats", action="store_true")
    p.add_argument("--limit", type=int, default=None, help="빌드 한도")
    args = p.parse_args()

    conn = get_conn()
    ensure_schema(conn)

    if args.init:
        print("✓ company_info_gap 스키마 생성 완료")
        return
    if args.stats:
        print_stats(conn); return
    if args.top:
        print_top(conn, args.top); return
    if args.corp_code:
        s = calc_corp_score(conn, args.corp_code)
        if s:
            save_score(conn, s)
            conn.commit()
            print(f"\n  {s['grade']} {s['corp_name']} ({s['corp_code']})")
            print(f"  총점: {s['score_total']}")
            print(f"  └ 보도부족: {s['score_news_lack']} (보도 {s['news_count']}건)")
            print(f"  └ 시총소형: {s['score_size_small']} (자본 {(s['total_equity'] or 0)/1e8:.0f}억)")
            print(f"  └ 기사부족: {s['score_article_lack']} (기사 {s['article_count']}건)")
            print(f"  └ 비허브:   {s['score_not_hub']} (공급망 {s['sc_appearance']}회)")
        else:
            print(f"  {args.corp_code}: 데이터 없음")
        return

    if args.build:
        gc = build_all(conn, args.limit)
        print()
        for g, c in sorted(gc.items()):
            print(f"  {g} : {c}개")
        print()
        print_stats(conn)
        return

    print(__doc__)


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
