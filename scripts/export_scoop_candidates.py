"""
scripts/export_scoop_candidates.py
───────────────────────────────────
N-1 검증 완료된 출고 후보(newsability_score >= 7)를 CSV로 export.

CSV는 UTF-8 BOM(엑셀 직접 열기 가능), 컬럼 한국어 헤더.

실행:
  python scripts/export_scoop_candidates.py                # 7점 이상 모두
  python scripts/export_scoop_candidates.py --min-score 8  # 8점 이상만
  python scripts/export_scoop_candidates.py --out scoop.csv
"""
import argparse
import csv
import io
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUT_DIR = ROOT / "output"

HEADERS = [
    ("score",            "점수"),
    ("urgency",          "긴급도"),
    ("corp_name",        "회사"),
    ("corp_code",        "회사코드"),
    ("headline_ko",      "헤드라인"),
    ("subheadline",      "부제(N1)"),
    ("valence",          "방향(GOOD/BAD/MIXED)"),
    ("publish_risk",     "출고리스크"),
    ("value_direction",  "가치방향"),
    ("industry_alignment","산업트렌드대비"),
    ("story_angle",      "스토리각도"),
    ("missing_verification","추가확인필요"),
    ("ir_question_hint", "IR질문후보"),
    ("cross_verified",   "교차검증"),
    ("news_status",      "보도여부"),
    ("actual_news_count","보도수"),
    ("lead_type",        "단서유형"),
    ("severity",         "severity"),
    ("info_gap_label",   "정보격차"),
    ("fact_match_label", "사실매칭"),
    ("report_type_a",    "비교페어_과거"),
    ("report_type_b",    "비교페어_최신"),
    ("comparison_id",    "비교id"),
    ("lead_id",          "단서id"),
    ("source_path",      "출처경로"),
    ("created_at",       "단서생성일"),
    ("n1_verified_at",   "N1검증일"),
]


def parse_value_meta(v_rationale_ai: str) -> dict:
    """value_rationale_ai JSON에서 story_type / urgency / ir_question_hint / subheadline 등 추출."""
    if not v_rationale_ai:
        return {}
    try:
        return json.loads(v_rationale_ai)
    except Exception:
        return {}


def main():
    ap = argparse.ArgumentParser(description="출고 후보 CSV export")
    ap.add_argument("--min-score", type=int, default=7,
                    help="newsability_score 최소값 (기본 7)")
    ap.add_argument("--out", default=None,
                    help="출력 파일 경로 (기본 output/scoop_candidates_YYYYMMDD.csv)")
    ap.add_argument("--since", default=None,
                    help="created_at >= YYYY-MM-DD 필터 (기본 없음)")
    args = ap.parse_args()

    OUT_DIR.mkdir(exist_ok=True)
    out_path = (Path(args.out) if args.out
                else OUT_DIR / f"scoop_candidates_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")

    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row

    where = ["sl.n1_verified_at IS NOT NULL",
             "sl.newsability_score >= ?"]
    params = [args.min_score]
    if args.since:
        where.append("sl.created_at >= ?")
        params.append(args.since)

    rows = db.execute(f"""
        SELECT sl.*, ac.report_type_a, ac.report_type_b
        FROM story_leads sl
        LEFT JOIN ai_comparisons ac ON ac.id = sl.comparison_id
        WHERE {' AND '.join(where)}
        ORDER BY sl.newsability_score DESC,
                 (sl.publish_risk='LOW') DESC,
                 (sl.publish_risk='MEDIUM') DESC,
                 sl.severity DESC,
                 sl.id DESC
    """, params).fetchall()

    print(f"[export_scoop_candidates] 후보: {len(rows)}건  (min_score={args.min_score})")

    # UTF-8 BOM (엑셀 직접 열기)
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow([h_ko for _, h_ko in HEADERS])

        for r in rows:
            meta = parse_value_meta(r["value_rationale_ai"])
            src_path = ""
            try:
                sp = json.loads(r["source_path"] or "{}")
                if isinstance(sp, dict):
                    src_path = " / ".join(
                        f"{k}:{v}" for k, v in sp.items() if v
                    )[:200]
            except Exception:
                src_path = (r["source_path"] or "")[:200]

            row_out = []
            for col_key, _ in HEADERS:
                if col_key == "subheadline":
                    val = meta.get("subheadline", "") or meta.get("story_angle_short", "")
                elif col_key == "urgency":
                    val = meta.get("urgency", "")
                elif col_key == "lead_id":
                    val = r["id"]
                elif col_key == "source_path":
                    val = src_path
                elif col_key == "report_type_a" or col_key == "report_type_b":
                    val = r[col_key] if col_key in r.keys() else ""
                else:
                    try:
                        val = r[col_key]
                    except Exception:
                        val = ""
                if val is None:
                    val = ""
                row_out.append(str(val))
            w.writerow(row_out)

    print(f"[완료] {out_path}")
    print(f"  파일 크기: {out_path.stat().st_size:,} bytes")

    # 분포 요약
    print("\n[점수 분포]")
    dist = {}
    for r in rows:
        dist[r["newsability_score"]] = dist.get(r["newsability_score"], 0) + 1
    for s in sorted(dist.keys(), reverse=True):
        bar = "█" * dist[s]
        print(f"  {s}점: {dist[s]:>3}건 {bar}")

    print("\n[valence 분포]")
    val_dist = {}
    for r in rows:
        v = r["valence"] or "?"
        val_dist[v] = val_dist.get(v, 0) + 1
    for v, c in sorted(val_dist.items(), key=lambda x: -x[1]):
        print(f"  {v:<6} {c}건")

    db.close()


if __name__ == "__main__":
    main()
