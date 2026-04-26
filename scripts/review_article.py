"""
scripts/review_article.py
─────────────────────────
사람 검토용 기사 출력 도구.
기사 본문 + 검증 점수 + 외부 인용 + 단서 evidence를 한 화면에 정리.

사용:
  python scripts/review_article.py --id 295
  python scripts/review_article.py --ids 295,296,297,298,299
  python scripts/review_article.py --recent 10
  python scripts/review_article.py --safe                 # SAFE 등급만
  python scripts/review_article.py --has-external         # 외부 인용 있는 것만
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
        if hasattr(sys.stdout, "buffer"):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "buffer"):
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def render_article(conn: sqlite3.Connection, article_id: int) -> str:
    a = conn.execute("SELECT * FROM article_drafts WHERE id=?", [article_id]).fetchone()
    if not a:
        return f"#{article_id}: 기사 없음"

    # 단서 + ai_result
    lead = conn.execute("""
        SELECT sl.*, ac.result AS ai_result
        FROM story_leads sl
        LEFT JOIN ai_comparisons ac ON sl.comparison_id = ac.id
        WHERE sl.id = ?
    """, [a["lead_id"]]).fetchone()

    # 검증 점수
    v = conn.execute(
        "SELECT * FROM article_verification WHERE article_id=?",
        [article_id]
    ).fetchone()

    # 외부 인용
    refs = conn.execute("""
        SELECT es.outlet_name, es.outlet_tier, es.published_at,
               es.title, es.summary, es.url,
               aer.citation_text, aer.citation_position
        FROM article_external_refs aer
        JOIN external_sources es ON aer.source_id = es.id
        WHERE aer.article_id = ?
        ORDER BY aer.citation_position
    """, [article_id]).fetchall()

    out = []
    out.append("=" * 78)
    out.append(f"  기사 #{article_id} 검토 리포트")
    out.append("=" * 78)
    out.append("")
    out.append(f"  기업: {a['corp_name']} ({a['corp_code'] or '-'})")
    out.append(f"  유형: {lead['lead_type'] if lead else '?'} | severity: {lead['severity'] if lead else '?'}")
    out.append(f"  모델: {a['model']} | 자수: {a['char_count']}")
    out.append(f"  생성: {a['created_at']}")

    if v:
        flag_icon = {"SAFE":"🟢","LOW":"🟠","MEDIUM":"🟡","HIGH":"🔴"}.get(v["flag"], "⚪")
        out.append(f"  검증: {flag_icon} {v['flag']} {v['score_total']}점 "
                   f"(수치 {v['score_numeric']} / 방향 {v['score_direction']} / "
                   f"evidence {v['score_evidence']} / 그라운딩 {v['score_grounding']})")
    else:
        out.append(f"  검증: ⏳ 미검증")

    if refs:
        out.append(f"  📰 외부 인용: {len(refs)}건")
    out.append("")

    out.append("-" * 78)
    out.append(f"📰 헤드라인:  {a['headline']}")
    if a['subheadline']:
        out.append(f"📑 부제:      {a['subheadline']}")
    out.append("-" * 78)
    out.append("")
    out.append("[본문]")
    out.append("")

    body = a['content'] or ""
    paragraphs = body.split("\n\n")
    for i, p in enumerate(paragraphs, 1):
        out.append(f"  ▶ 단락 {i} ({len(p)}자)")
        # 줄바꿈 보존하면서 들여쓰기
        for line in p.split("\n"):
            out.append(f"    {line}")
        out.append("")

    out.append("-" * 78)
    out.append("[원본 단서 evidence]")
    out.append("")
    if lead and lead['evidence']:
        ev = lead['evidence']
        # HTML 태그 제거
        ev = re.sub(r"<[^>]+>", "", ev)
        for line in ev.split("\n")[:15]:
            out.append(f"  {line}")
    else:
        out.append("  (없음)")
    out.append("")

    if refs:
        out.append("-" * 78)
        out.append(f"[외부 인용 자료 — {len(refs)}건]")
        out.append("")
        for i, r in enumerate(refs, 1):
            tier = r["outlet_tier"]
            out.append(f"  {i}. [{tier}] {r['outlet_name']} ({r['published_at'] or '-'})")
            out.append(f"     제목: {(r['title'] or '')[:80]}")
            if r['summary']:
                out.append(f"     요약: {(r['summary'] or '').replace(chr(10),' ')[:120]}")
            out.append(f"     URL:  {r['url']}")
            if r['citation_text']:
                ctx = (r['citation_text'] or '').replace("\n", " ")[:120]
                out.append(f"     인용 위치: …{ctx}…")
            out.append("")

    if v:
        out.append("-" * 78)
        out.append("[검증 상세]")
        out.append("")
        for fld in ("numeric_details", "direction_details",
                    "evidence_details", "grounding_details"):
            try:
                d = json.loads(v[fld] or "{}")
                out.append(f"  ◾ {fld}")
                # 핵심 부분만 표시
                if "matched" in d and "tested" in d:
                    out.append(f"     매칭 {d['matched']}/{d['tested']}")
                if "coverage_pct" in d:
                    out.append(f"     커버 {d['coverage_pct']}%")
                if "missing_examples" in d and d['missing_examples']:
                    out.append(f"     누락 예: {d['missing_examples'][:5]}")
                if "actual_dir" in d:
                    out.append(f"     실제 방향: {d.get('actual_dir')} / 본문 방향: {d.get('body_dir')}")
                if "reason" in d:
                    out.append(f"     사유: {d['reason']}")
            except Exception:
                pass
        out.append("")

    out.append("=" * 78)
    return "\n".join(out)


def main():
    p = argparse.ArgumentParser(description="기사 사람 검토용 출력")
    p.add_argument("--id", type=int)
    p.add_argument("--ids", type=str, help="쉼표 구분 id 목록")
    p.add_argument("--recent", type=int, help="최근 N건")
    p.add_argument("--safe",   action="store_true", help="SAFE만")
    p.add_argument("--has-external", action="store_true", help="외부 인용 있는 것만")
    args = p.parse_args()

    conn = get_conn()

    ids = []
    if args.id:
        ids = [args.id]
    elif args.ids:
        ids = [int(x) for x in args.ids.split(",") if x.strip()]
    elif args.recent:
        rows = conn.execute(
            "SELECT id FROM article_drafts ORDER BY id DESC LIMIT ?",
            [args.recent]
        ).fetchall()
        ids = [r["id"] for r in rows]
    elif args.safe:
        rows = conn.execute("""
            SELECT ad.id FROM article_drafts ad
            JOIN article_verification av ON ad.id = av.article_id
            WHERE av.flag = 'SAFE' ORDER BY av.score_total DESC
        """).fetchall()
        ids = [r["id"] for r in rows]
    elif args.has_external:
        rows = conn.execute("""
            SELECT DISTINCT ad.id FROM article_drafts ad
            JOIN article_external_refs aer ON aer.article_id = ad.id
            ORDER BY ad.id DESC LIMIT 20
        """).fetchall()
        ids = [r["id"] for r in rows]
    else:
        print(__doc__)
        return

    for aid in ids:
        print(render_article(conn, aid))
        print()

    conn.close()


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
