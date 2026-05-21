"""
scripts/export_eng_names_md.py
──────────────────────────────
companies.corp_name_eng 데이터를 fas-news-searcher 용 MD 파일로 출력.

출력 형식:
  - 섹션: 알파벳 (A, B, ... Z)
  - 행: `한글명 | 영문명 | stock_code | corp_code`
  - 상장사(stock_code 있음) 우선

  data/exports/listed_corps_eng.md
"""
import io, sqlite3, sys
from collections import defaultdict
from pathlib import Path

try: sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except: pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUT_DIR = ROOT / "data" / "exports"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row

    # 상장사 우선, stock_code 있는 것
    rows = db.execute("""
        SELECT corp_code, corp_name, corp_name_eng, stock_code, sector
        FROM companies
        WHERE corp_name_eng IS NOT NULL AND corp_name_eng != ''
        ORDER BY UPPER(corp_name_eng), corp_name
    """).fetchall()

    listed = [r for r in rows if (r["stock_code"] or "").strip()]
    unlisted = [r for r in rows if not (r["stock_code"] or "").strip()]

    # 알파벳 그룹핑 (영문명 첫 문자 기준)
    groups = defaultdict(list)
    for r in listed:
        first = (r["corp_name_eng"] or "").strip()[:1].upper()
        key = first if first.isalpha() else "#"
        groups[key].append(r)

    out_md = OUT_DIR / "listed_corps_eng.md"
    out_csv = OUT_DIR / "listed_corps_eng.csv"
    out_json = OUT_DIR / "listed_corps_eng.json"

    # ── Markdown ─────────────────────────────────────────────────────────
    md = []
    md.append("# 🇰🇷 한국 상장사 영문명 매핑\n")
    md.append(f"> 자동 생성 — Company Catcher (`companies.corp_name_eng` from DART API)")
    md.append(f"> 총 **{len(listed):,}개 상장사** + 비상장 {len(unlisted):,}개")
    md.append("")
    md.append(f"## 📊 한눈에")
    md.append("")
    md.append(f"| 구분 | 회사 수 |")
    md.append(f"|---|---:|")
    md.append(f"| 상장사 (stock_code 보유) | {len(listed):,} |")
    md.append(f"| 비상장 | {len(unlisted):,} |")
    md.append(f"| **합계** | **{len(rows):,}** |")
    md.append("")

    md.append("## 🔤 알파벳별 상장사 영문명\n")
    for key in sorted(groups.keys()):
        items = groups[key]
        md.append(f"### {key} ({len(items)}개)\n")
        md.append("| 영문명 | 한글명 | 종목코드 | corp_code | 섹터 |")
        md.append("|---|---|---|---|---|")
        for r in items:
            eng = (r["corp_name_eng"] or "").replace("|", "\\|")
            ko = (r["corp_name"] or "").replace("|", "\\|")
            stk = r["stock_code"] or ""
            cc = r["corp_code"] or ""
            sec = (r["sector"] or "").replace("|", "\\|")
            md.append(f"| {eng} | {ko} | `{stk}` | `{cc}` | {sec} |")
        md.append("")

    if unlisted:
        md.append("\n## 📁 비상장 (참고)\n")
        md.append("<details><summary>펼치기 ({}건)</summary>\n".format(len(unlisted)))
        md.append("\n| 영문명 | 한글명 | corp_code |")
        md.append("|---|---|---|")
        for r in unlisted[:300]:   # 상위 300개만
            eng = (r["corp_name_eng"] or "").replace("|", "\\|")
            ko = (r["corp_name"] or "").replace("|", "\\|")
            cc = r["corp_code"] or ""
            md.append(f"| {eng} | {ko} | `{cc}` |")
        if len(unlisted) > 300:
            md.append(f"| ... | _{len(unlisted)-300}건 생략_ | |")
        md.append("\n</details>")

    out_md.write_text("\n".join(md), encoding="utf-8")
    print(f"✅ {out_md} ({len(listed):,} listed + {len(unlisted):,} unlisted)")

    # ── CSV (편집 가능) ─────────────────────────────────────────────────
    import csv
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["corp_code", "corp_name_kr", "corp_name_eng", "stock_code", "sector"])
        for r in rows:
            w.writerow([r["corp_code"], r["corp_name"], r["corp_name_eng"],
                        r["stock_code"] or "", r["sector"] or ""])
    print(f"✅ {out_csv}")

    # ── JSON (프로그래밍 사용) ──────────────────────────────────────────
    import json
    payload = {
        "generated_at": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
        "total": len(rows),
        "listed_count": len(listed),
        "unlisted_count": len(unlisted),
        "items": [
            {
                "corp_code": r["corp_code"],
                "corp_name": r["corp_name"],
                "corp_name_eng": r["corp_name_eng"],
                "stock_code": r["stock_code"] or None,
                "sector": r["sector"] or None,
                "is_listed": bool((r["stock_code"] or "").strip()),
            }
            for r in rows
        ],
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ {out_json}")


if __name__ == "__main__":
    main()
