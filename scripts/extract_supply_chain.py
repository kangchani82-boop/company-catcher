"""
scripts/extract_supply_chain.py
───────────────────────────────
biz_content 텍스트에서 공급망 관계(고객사·매입처·협력사·경쟁사)를 추출해
supply_chain 테이블에 저장

실행:
  python scripts/extract_supply_chain.py           # 전체 (미처리 기업만)
  python scripts/extract_supply_chain.py --all     # 기존 데이터 덮어쓰기 포함
  python scripts/extract_supply_chain.py --limit 100
"""

import io
import re
import sqlite3
import sys
import argparse
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# ─── 추출 패턴 ─────────────────────────────────────────────────────────────
# (relation_type, [패턴 목록])
# 각 패턴 그룹은 "섹션 제목" 이후 파트너명 목록을 찾음

PATTERNS = [
    # ── 고객사 ──────────────────────────────────────────────────────────────
    ("customer", [
        r"주요\s*(?:고객|거래처|납품처|판매처)\s*(?:현황|목록|내역|리스트)?[^\n]*\n((?:[^\n]+\n){1,30})",
        r"주요\s*(?:고객사|고객기업)\s*(?:현황|목록)?[^\n]*\n((?:[^\n]+\n){1,20})",
        r"(?:주요|핵심)\s*고객\s*:\s*([^\n]+)",
        r"고객\s*(?:현황|구성)[^\n]*\n((?:[^\n]+\n){1,15})",
    ]),
    # ── 매입처/공급사 ────────────────────────────────────────────────────────
    ("supplier", [
        r"주요\s*(?:매입처|원재료\s*매입처|구매처|공급처|공급사|협력업체)\s*(?:현황|목록|내역)?[^\n]*\n((?:[^\n]+\n){1,20})",
        r"주요\s*원재료[^\n]*\n((?:[^\n]+\n){1,20})",
        r"(?:원재료|부품)\s*(?:구매|매입)\s*(?:현황|내역)[^\n]*\n((?:[^\n]+\n){1,15})",
        r"(?:주요|핵심)\s*공급\s*(?:업체|사)\s*:\s*([^\n]+)",
    ]),
    # ── 협력사/파트너 ────────────────────────────────────────────────────────
    ("partner", [
        r"주요\s*협력\s*(?:업체|사|파트너)\s*(?:현황|목록)?[^\n]*\n((?:[^\n]+\n){1,15})",
        r"전략적\s*(?:파트너|제휴)\s*(?:업체|사)?\s*:\s*([^\n]+)",
        r"기술\s*협력\s*(?:업체|사|파트너)[^\n]*\n((?:[^\n]+\n){1,10})",
    ]),
    # ── 경쟁사 ──────────────────────────────────────────────────────────────
    ("competitor", [
        r"주요\s*경쟁\s*(?:업체|사|기업)\s*(?:현황|목록)?[^\n]*\n((?:[^\n]+\n){1,15})",
        r"국내외\s*경쟁\s*(?:업체|사)[^\n]*\n((?:[^\n]+\n){1,10})",
        r"(?:주요|핵심)\s*경쟁\s*(?:사|업체)\s*:\s*([^\n]+)",
    ]),
]

# 파트너명 노이즈 필터링 패턴
NOISE_PATTERNS = [
    r"^\s*$",                               # 빈 줄
    r"^[\d\s\.\-,]+$",                      # 숫자/구분자만
    r"^(?:위와|상기|이하|아래|해당|기타|등|및|또한|따라서|그러나|하지만)",
    r"^\s{4,}",                             # 들여쓰기 과도
    r"^[가-힣]{1,2}$",                      # 1~2글자 한글
    r"(?:단위|억원|백만원|천원|%|매출액|점유율|비중|구성비)",
    r"^[-=*─━]{2,}",                        # 구분선
    r"^\(?\d{4}\)?년",                      # 연도
    r"^표\s*\d",                            # 표 번호
]

# 괄호 안 설명 제거 / 정리
def clean_partner_name(name: str) -> str:
    name = re.sub(r"\s*\(주\)\s*|\s*주식회사\s*", "", name)
    name = re.sub(r"\([^)]{0,30}\)", "", name)  # 짧은 괄호 설명 제거
    name = re.sub(r"[①②③④⑤⑥⑦⑧⑨⑩]", "", name)
    name = re.sub(r"[\t]", " ", name)
    name = name.strip().strip(",.;:·•-")
    return name.strip()


def is_noise(name: str) -> bool:
    if len(name) < 2 or len(name) > 60:
        return True
    for pat in NOISE_PATTERNS:
        if re.search(pat, name):
            return True
    return False


def extract_names_from_block(block: str) -> list[str]:
    """블록 텍스트에서 회사명 후보 추출"""
    names = []
    for line in block.splitlines():
        line = line.strip()
        if not line:
            continue

        # 표 형식: 구분자로 나눠진 줄에서 첫 번째 셀
        parts = re.split(r'\s{2,}|\t|\|', line)
        for p in parts[:2]:  # 앞 2개 셀만
            cand = clean_partner_name(p)
            if cand and not is_noise(cand):
                names.append(cand)
            break  # 첫 셀만

        # 쉼표로 나열된 경우
        if "," in line and len(parts) == 1:
            for p in line.split(","):
                cand = clean_partner_name(p)
                if cand and not is_noise(cand):
                    names.append(cand)

    return names


def extract_from_text(text: str) -> list[tuple[str, str, str]]:
    """
    biz_content → [(relation_type, partner_name, context), ...]
    """
    results = []
    seen = set()

    for rel_type, pat_list in PATTERNS:
        for pat in pat_list:
            for m in re.finditer(pat, text, re.IGNORECASE | re.MULTILINE):
                block = m.group(1) if m.lastindex else m.group(0)
                context = text[max(0, m.start()-50): m.end()].replace("\n", " ")[:200]

                names = extract_names_from_block(block)
                for name in names:
                    key = (rel_type, name.lower())
                    if key not in seen:
                        seen.add(key)
                        results.append((rel_type, name, context))

    return results


# ─── DB 작업 ───────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def ensure_supply_chain_table(db: sqlite3.Connection):
    db.execute("""
        CREATE TABLE IF NOT EXISTS supply_chain (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            corp_code     TEXT NOT NULL,
            corp_name     TEXT,
            relation_type TEXT,
            partner_name  TEXT,
            context       TEXT,
            source_report TEXT,
            analyzed_at   TEXT
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_sc_corp ON supply_chain(corp_code)")
    db.commit()


def already_extracted(db: sqlite3.Connection, corp_code: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM supply_chain WHERE corp_code=? LIMIT 1", [corp_code]
    ).fetchone()
    return bool(row)


def delete_existing(db: sqlite3.Connection, corp_code: str):
    db.execute("DELETE FROM supply_chain WHERE corp_code=?", [corp_code])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all",   action="store_true", help="기존 데이터 덮어쓰기")
    parser.add_argument("--limit", type=int, default=0, help="처리 기업 수 제한 (0=전체)")
    args = parser.parse_args()

    db = get_db()
    ensure_supply_chain_table(db)

    # 처리 대상 기업 목록
    rows = db.execute("""
        SELECT corp_code, MAX(corp_name) as corp_name,
               MAX(id) as latest_id,
               MAX(report_name) as report_name
        FROM reports
        WHERE biz_content IS NOT NULL AND LENGTH(biz_content) >= 500
          AND report_type IN ('2025_annual','2025_q3','2025_h1','2025_q1')
        GROUP BY corp_code
        ORDER BY corp_code
    """).fetchall()

    total = len(rows)
    print(f"처리 대상: {total}개 기업")

    processed = 0
    inserted  = 0
    skipped   = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    for row in rows:
        corp_code = row["corp_code"]
        corp_name = row["corp_name"] or ""
        report_name = row["report_name"] or ""

        if not args.all and already_extracted(db, corp_code):
            skipped += 1
            continue

        # biz_content 가져오기 (사업보고서 우선, 없으면 최근 것)
        rr = db.execute("""
            SELECT biz_content, report_name FROM reports
            WHERE corp_code=?
              AND biz_content IS NOT NULL AND LENGTH(biz_content) >= 500
              AND report_type IN ('2025_annual','2025_q3','2025_h1','2025_q1')
            ORDER BY
              CASE report_type
                WHEN '2025_annual' THEN 1
                WHEN '2025_q3'     THEN 2
                WHEN '2025_h1'     THEN 3
                WHEN '2025_q1'     THEN 4
                ELSE 5
              END
            LIMIT 1
        """, [corp_code]).fetchone()

        if not rr:
            continue

        text = rr["biz_content"]
        src  = rr["report_name"]

        if args.all:
            delete_existing(db, corp_code)

        relations = extract_from_text(text)

        if relations:
            db.executemany("""
                INSERT INTO supply_chain
                  (corp_code, corp_name, relation_type, partner_name, context, source_report, analyzed_at)
                VALUES (?,?,?,?,?,?,?)
            """, [
                (corp_code, corp_name, rel, name, ctx, src, now)
                for rel, name, ctx in relations
            ])
            db.commit()
            inserted += len(relations)

        processed += 1
        if processed % 100 == 0:
            print(f"  진행: {processed}/{total - skipped} 기업 처리, {inserted}개 관계 추출")

        if args.limit and processed >= args.limit:
            break

    print(f"\n완료: {processed}개 기업 처리, {inserted}개 공급망 관계 추출 (건너뜀: {skipped}개)")

    # 최종 현황
    total_sc = db.execute("SELECT COUNT(*) FROM supply_chain").fetchone()[0]
    by_type = db.execute(
        "SELECT relation_type, COUNT(*) as cnt FROM supply_chain GROUP BY relation_type"
    ).fetchall()
    print(f"supply_chain 전체: {total_sc}건")
    for r in by_type:
        print(f"  {r['relation_type']}: {r['cnt']}건")


if __name__ == "__main__":
    main()
