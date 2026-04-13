"""
scripts/export_for_gemini.py
─────────────────────────────
DB의 biz_content를 Gemini(또는 다른 AI)에서 읽을 수 있는 파일로 내보내기

출력 구조:
  exports/
    biz_content/
      삼성전자/
        삼성전자_2025_annual_20260313.txt
        삼성전자_2025_q3_20251114.txt
        삼성전자_2025_h1_20250814.txt
        삼성전자_2025_q1_20250514.txt
      SK하이닉스/
        ...
    index.json              — 전체 인덱스 (기업명/유형/경로)
    all_reports.jsonl       — 한 줄에 보고서 1개 (AI 배치 처리용)
    by_type/
      2025_annual.jsonl     — 사업보고서만
      2025_q3.jsonl         — 3분기만
      2025_h1.jsonl         — 반기만
      2025_q1.jsonl         — 1분기만

사용법:
  python scripts/export_for_gemini.py                      # 전체 내보내기
  python scripts/export_for_gemini.py --types annual q3    # 특정 유형만
  python scripts/export_for_gemini.py --corp 삼성전자       # 특정 기업만
  python scripts/export_for_gemini.py --format txt         # 텍스트 파일만
  python scripts/export_for_gemini.py --format jsonl       # JSONL만
  python scripts/export_for_gemini.py --check              # 현황 확인
"""

import argparse
import io
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Windows cp949 콘솔 인코딩 오류 방지
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT       = Path(__file__).parent.parent
DB_PATH    = ROOT / "data" / "dart" / "dart_reports.db"
EXPORT_DIR = ROOT / "exports" / "biz_content"
MIN_LEN    = 300

TYPE_LABELS = {
    "2025_annual": "2025 사업보고서",
    "2025_q3":     "2025 3분기보고서",
    "2025_h1":     "2025 반기보고서",
    "2025_q1":     "2025 1분기보고서",
}
TYPE_ORDER = ["2025_q1", "2025_h1", "2025_q3", "2025_annual"]


def get_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    return db


def safe_filename(name: str) -> str:
    """파일명에 사용 불가한 문자 제거"""
    name = re.sub(r'[\\/*?:"<>|]', '_', name)
    return name.strip().strip('.')


def format_report_txt(row: dict) -> str:
    """단일 보고서를 텍스트로 포맷팅 (Gemini 프롬프트용)"""
    label = TYPE_LABELS.get(row["report_type"], row["report_type"])
    dt = row.get("rcept_dt", "")
    if dt and len(dt) == 8:
        dt = f"{dt[:4]}.{dt[4:6]}.{dt[6:8]}"
    lines = [
        f"{'='*70}",
        f"  기업명:     {row['corp_name']}",
        f"  보고서:     {label}",
        f"  접수일:     {dt}",
        f"  보고서명:   {row.get('report_name', '')}",
        f"{'='*70}",
        "",
        row.get("biz_content", ""),
    ]
    return "\n".join(lines)


def export_txt_files(db: sqlite3.Connection, where: str, params: list,
                     corp_filter: str = "") -> tuple[int, int]:
    """기업별 폴더 + 보고서별 .txt 파일 생성"""
    rows = db.execute(f"""
        SELECT corp_code, corp_name, report_type, report_name, rcept_dt, biz_content
        FROM reports
        WHERE biz_content IS NOT NULL AND LENGTH(biz_content) >= {MIN_LEN}
          {where}
        ORDER BY corp_name COLLATE NOCASE, report_type
    """, params).fetchall()

    if corp_filter:
        rows = [r for r in rows if corp_filter in (r["corp_name"] or "")]

    total = len(rows)
    written = 0

    for row in rows:
        row = dict(row)
        corp_name = (row["corp_name"] or "unknown").strip()
        report_type = row["report_type"] or "unknown"
        rcept_dt = row["rcept_dt"] or "00000000"

        corp_dir = EXPORT_DIR / safe_filename(corp_name)
        corp_dir.mkdir(parents=True, exist_ok=True)

        fname = f"{safe_filename(corp_name)}_{report_type}_{rcept_dt}.txt"
        fpath = corp_dir / fname

        content = format_report_txt(row)
        fpath.write_text(content, encoding="utf-8")
        written += 1

        if written % 500 == 0:
            print(f"  txt: {written}/{total} 건 작성...")

    return total, written


def export_jsonl(db: sqlite3.Connection, where: str, params: list,
                 out_path: Path, corp_filter: str = "") -> int:
    """JSONL 파일 생성 (한 줄 = 보고서 1개)"""
    rows = db.execute(f"""
        SELECT corp_code, corp_name, report_type, report_name,
               rcept_no, rcept_dt, biz_content
        FROM reports
        WHERE biz_content IS NOT NULL AND LENGTH(biz_content) >= {MIN_LEN}
          {where}
        ORDER BY corp_name COLLATE NOCASE, report_type
    """, params).fetchall()

    if corp_filter:
        rows = [r for r in rows if corp_filter in (r["corp_name"] or "")]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rows:
            row = dict(row)
            record = {
                "corp_code":   row["corp_code"],
                "corp_name":   row["corp_name"],
                "report_type": row["report_type"],
                "report_label": TYPE_LABELS.get(row["report_type"], row["report_type"]),
                "report_name": row["report_name"],
                "rcept_no":    row["rcept_no"],
                "rcept_dt":    row["rcept_dt"],
                "biz_content": row["biz_content"],
                "char_count":  len(row["biz_content"] or ""),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


def export_index(db: sqlite3.Connection) -> dict:
    """index.json 생성"""
    rows = db.execute(f"""
        SELECT corp_code, corp_name, report_type, report_name, rcept_dt,
               LENGTH(biz_content) as char_count
        FROM reports
        WHERE biz_content IS NOT NULL AND LENGTH(biz_content) >= {MIN_LEN}
        ORDER BY corp_name COLLATE NOCASE, report_type
    """).fetchall()

    by_corp: dict[str, dict] = {}
    for row in rows:
        corp_code = row["corp_code"]
        corp_name = (row["corp_name"] or "").strip()
        if corp_code not in by_corp:
            by_corp[corp_code] = {
                "corp_code": corp_code,
                "corp_name": corp_name,
                "reports":   [],
            }
        by_corp[corp_code]["reports"].append({
            "report_type":  row["report_type"],
            "report_label": TYPE_LABELS.get(row["report_type"], row["report_type"]),
            "report_name":  row["report_name"],
            "rcept_dt":     row["rcept_dt"],
            "char_count":   row["char_count"],
            "txt_file": (
                f"{safe_filename(corp_name)}/"
                f"{safe_filename(corp_name)}_{row['report_type']}_{row['rcept_dt']}.txt"
            ),
        })

    index = {
        "generated_at": datetime.now().isoformat(),
        "total_companies": len(by_corp),
        "total_reports": len(rows),
        "companies": sorted(by_corp.values(), key=lambda x: x["corp_name"]),
    }

    idx_path = EXPORT_DIR.parent / "index.json"
    idx_path.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    return index


def print_stats(db: sqlite3.Connection):
    print(f"\n{'='*65}")
    print("  biz_content 수출 가능 현황")
    print(f"{'='*65}")
    for db_type, label in TYPE_LABELS.items():
        row = db.execute(
            f"SELECT COUNT(*) as cnt, SUM(LENGTH(biz_content)) as total_chars "
            f"FROM reports WHERE report_type=? AND biz_content IS NOT NULL AND LENGTH(biz_content)>={MIN_LEN}",
            (db_type,)
        ).fetchone()
        cnt   = row["cnt"] or 0
        total = row["total_chars"] or 0
        print(f"\n  [{db_type}] {label}")
        print(f"    기업 수:    {cnt:,}개")
        print(f"    총 텍스트:  약 {total//1024//1024}MB ({total//10000}만자)")

    # 4개 모두 있는 기업
    full_corp = db.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT corp_code FROM reports
            WHERE report_type IN ('2025_q1','2025_h1','2025_q3','2025_annual')
              AND biz_content IS NOT NULL AND LENGTH(biz_content)>={MIN_LEN}
            GROUP BY corp_code HAVING COUNT(DISTINCT report_type)=4
        )
    """).fetchone()[0]
    print(f"\n  4개 유형 모두 보유 기업: {full_corp:,}개")
    print(f"{'='*65}\n")


def main():
    parser = argparse.ArgumentParser(description="biz_content Gemini용 내보내기")
    parser.add_argument("--types", nargs="+",
                        choices=["annual", "q3", "h1", "q1"],
                        help="내보낼 유형 (기본: 전체)")
    parser.add_argument("--corp", type=str, default="",
                        help="특정 기업명 필터 (예: 삼성전자)")
    parser.add_argument("--format", choices=["all", "txt", "jsonl"],
                        default="all", help="출력 형식 (기본: 전체)")
    parser.add_argument("--check", action="store_true",
                        help="현황 확인만")
    parser.add_argument("--out", type=str, default="",
                        help="출력 디렉토리 지정 (기본: exports/biz_content)")
    args = parser.parse_args()

    global EXPORT_DIR
    if args.out:
        EXPORT_DIR = Path(args.out)

    db = get_db()

    if args.check:
        print_stats(db)
        db.close()
        return

    # 필터 조건
    type_map = {"annual": "2025_annual", "q3": "2025_q3", "h1": "2025_h1", "q1": "2025_q1"}
    if args.types:
        db_types = [type_map[t] for t in args.types]
        placeholders = ",".join("?" * len(db_types))
        where  = f"AND report_type IN ({placeholders})"
        params = db_types
    else:
        where  = ""
        params = []

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n내보내기 시작 → {EXPORT_DIR.resolve()}")
    print_stats(db)

    start = datetime.now()

    # ── TXT 파일 ──────────────────────────────────────────────────────────────
    if args.format in ("all", "txt"):
        print("[TXT] 기업별 .txt 파일 생성 중...")
        total, written = export_txt_files(db, where, params, args.corp)
        print(f"  완료: {written:,}건 ({total:,}건 중)")

    # ── JSONL 파일 ────────────────────────────────────────────────────────────
    if args.format in ("all", "jsonl"):
        print("\n[JSONL] all_reports.jsonl 생성 중...")
        all_path = EXPORT_DIR.parent / "all_reports.jsonl"
        cnt = export_jsonl(db, where, params, all_path, args.corp)
        print(f"  완료: {cnt:,}건 -> {all_path}")

        # 유형별 JSONL
        if not args.types and not args.corp:
            print("\n[JSONL] 유형별 .jsonl 생성 중...")
            by_type_dir = EXPORT_DIR.parent / "by_type"
            for db_type, label in TYPE_LABELS.items():
                out_path = by_type_dir / f"{db_type}.jsonl"
                cnt = export_jsonl(db, "AND report_type=?", [db_type], out_path)
                size_mb = out_path.stat().st_size / 1024 / 1024 if out_path.exists() else 0
                print(f"  {db_type}.jsonl -- {cnt:,}건 ({size_mb:.1f}MB)")

    # ── 인덱스 ────────────────────────────────────────────────────────────────
    if args.format == "all" and not args.corp:
        print("\n[INDEX] index.json 생성 중...")
        idx = export_index(db)
        print(f"  완료: {idx['total_companies']:,}개 기업, {idx['total_reports']:,}건")

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n완료 ({elapsed:.1f}초)")
    print(f"  경로: {EXPORT_DIR.parent.resolve()}")
    print()

    # 출력 파일 목록
    out_root = EXPORT_DIR.parent
    files = list(out_root.rglob("*.jsonl")) + list(out_root.glob("*.json"))
    if files:
        print("생성된 파일:")
        for f in sorted(files):
            size_mb = f.stat().st_size / 1024 / 1024
            print(f"  {f.relative_to(out_root)} ({size_mb:.1f}MB)")

    txt_count = len(list(EXPORT_DIR.rglob("*.txt")))
    if txt_count:
        print(f"\n  txt 파일: {txt_count:,}개 -> {EXPORT_DIR.resolve()}")

    db.close()


if __name__ == "__main__":
    main()
