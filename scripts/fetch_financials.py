"""
scripts/fetch_financials.py
───────────────────────────
dart-fss XBRL로 기업 재무제표 핵심 수치 수집
연간/분기 보고서에서 주요 계정과목 추출 후 financials 테이블에 저장

실행 예시:
  python scripts/fetch_financials.py                  # 비교분석 완료 기업 전체
  python scripts/fetch_financials.py --limit 50       # 50개만
  python scripts/fetch_financials.py --corp 00126380  # 특정 기업 1개
  python scripts/fetch_financials.py --year 2024      # 특정 연도

주요 추출 항목:
  매출액, 영업이익, 당기순이익, 총자산, 총부채, 자본총계, 부채비율, ROE
"""

import io
import os
import re
import sys
import time
import sqlite3
import argparse
import json
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# ── 환경변수 로드 ──────────────────────────────────────────────────────────────
if ENV_PATH.exists():
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ── 핵심 계정과목 매핑 (IFRS concept_id → 우리 필드명) ──────────────────────
# concept_id 일부는 회사마다 다를 수 있으므로 복수 후보 지정
CONCEPT_MAP = {
    "revenue": [
        "ifrs-full_Revenue",
        "ifrs-full_RevenueFromContractsWithCustomers",
        "dart_TotalRevenue",
    ],
    "operating_income": [
        "dart_OperatingIncomeLoss",
        "ifrs-full_ProfitLossFromOperatingActivities",
        "ifrs-full_OperatingProfit",
    ],
    "net_income": [
        "ifrs-full_ProfitLoss",
        "ifrs-full_ProfitLossAttributableToOwnersOfParent",
        "dart_NetIncomeLoss",
    ],
    "total_assets": [
        "ifrs-full_Assets",
        "ifrs-full_TotalAssets",
    ],
    "total_liabilities": [
        "ifrs-full_Liabilities",
        "ifrs-full_TotalLiabilities",
    ],
    "total_equity": [
        "ifrs-full_Equity",
        "ifrs-full_EquityAttributableToOwnersOfParent",
    ],
    "current_assets": [
        "ifrs-full_CurrentAssets",
    ],
    "current_liabilities": [
        "ifrs-full_CurrentLiabilities",
    ],
    "cash": [
        "ifrs-full_CashAndCashEquivalents",
    ],
}


def get_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    return db


def ensure_table(db: sqlite3.Connection):
    db.execute("""
        CREATE TABLE IF NOT EXISTS financials (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            corp_code       TEXT NOT NULL,
            corp_name       TEXT,
            stock_code      TEXT,
            fiscal_year     INTEGER,  -- 결산년도 (2024)
            fiscal_end      TEXT,     -- 결산월일 (1231)
            report_type     TEXT,     -- annual | q1 | h1 | q3
            consolidated    INTEGER DEFAULT 1,  -- 1=연결, 0=별도
            revenue         REAL,     -- 매출액
            operating_income REAL,    -- 영업이익
            net_income      REAL,     -- 당기순이익
            total_assets    REAL,     -- 총자산
            total_liabilities REAL,   -- 총부채
            total_equity    REAL,     -- 자본총계
            current_assets  REAL,     -- 유동자산
            current_liabilities REAL, -- 유동부채
            cash            REAL,     -- 현금및현금성자산
            debt_ratio      REAL,     -- 부채비율 (부채/자본)
            current_ratio   REAL,     -- 유동비율 (유동자산/유동부채)
            roe             REAL,     -- ROE (순이익/자본)
            operating_margin REAL,    -- 영업이익률 (영업이익/매출)
            net_margin      REAL,     -- 순이익률
            raw_json        TEXT,     -- 전체 원시 데이터 (JSON)
            fetched_at      TEXT,
            UNIQUE(corp_code, fiscal_year, fiscal_end, report_type, consolidated)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_fin_corp ON financials(corp_code)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_fin_year ON financials(fiscal_year)")
    db.commit()


def _parse_df(df):
    """MultiIndex DataFrame에서 (cid_col, date_cols) 반환.

    dart-fss가 반환하는 DataFrame 컬럼은 MultiIndex 형태:
      - 메타 컬럼: (테이블명_긴문자열, 'concept_id' | 'label_ko' | ...)
      - 데이터 컬럼: ('YYYYMMDD-YYYYMMDD', ('연결재무제표',)) 또는 ('YYYYMMDD', ('연결재무제표',))
    """
    if df is None or df.empty:
        return None, []
    try:
        # concept_id 컬럼 탐색 (level1 = 'concept_id')
        cid_col = None
        for col in df.columns:
            if isinstance(col, tuple):
                if 'concept_id' in str(col[1]).lower():
                    cid_col = col
                    break
            else:
                if 'concept_id' in str(col).lower():
                    cid_col = col
                    break
        if cid_col is None:
            return None, []

        # 날짜 컬럼 탐색:
        #   형태1: ('YYYYMMDD-YYYYMMDD', ('연결재무제표',))  — 기간
        #   형태2: ('YYYYMMDD', ('연결재무제표',))            — 시점(BS)
        #   형태3: 'YYYYMMDD' flat string
        date_cols = []
        for c in df.columns:
            if isinstance(c, tuple):
                s = str(c[0]).strip()
                # 'YYYYMMDD-YYYYMMDD' or 'YYYYMMDD'
                if re.match(r'^\d{8}(-\d{8})?$', s):
                    date_cols.append(c)
            else:
                s = str(c).strip()
                if re.match(r'^\d{8}(-\d{8})?$', s):
                    date_cols.append(c)

        return cid_col, date_cols
    except Exception:
        return None, []


def get_year_values(df, field_key: str) -> dict:
    """모든 연도별 값 반환 {year: value}"""
    if df is None or df.empty:
        return {}

    candidates = CONCEPT_MAP.get(field_key, [])
    result = {}

    try:
        cid_col, date_cols = _parse_df(df)
        if cid_col is None:
            return {}

        for concept_id in candidates:
            mask = df[cid_col] == concept_id
            rows = df[mask]
            if not rows.empty:
                for dcol in date_cols:
                    val = rows.iloc[0][dcol]
                    if val is not None and str(val) not in ('nan', '', 'None'):
                        try:
                            # 날짜 문자열 첫 4자리가 연도
                            raw_date = str(dcol[0] if isinstance(dcol, tuple) else dcol).strip()
                            year_key = int(raw_date[:4])
                            result[year_key] = float(str(val).replace(',', ''))
                        except (ValueError, TypeError):
                            pass
                break
    except Exception:
        pass
    return result


def extract_value(df, field_key: str):
    """가장 최신 연도 값 추출"""
    vals = get_year_values(df, field_key)
    if not vals:
        return None
    return vals[max(vals.keys())]


def fetch_corp_financials(corp_code: str, corp_name: str, bgn_de: str, dart_module) -> list:
    """단일 기업 재무데이터 수집. [{fiscal_year, ...}, ...] 반환"""
    try:
        corp_list = dart_module.get_corp_list()
        corp = corp_list.find_by_corp_code(corp_code)
        if not corp:
            print(f"  [{corp_name}] dart-fss 기업 없음")
            return []

        fs = corp.extract_fs(bgn_de=bgn_de)
        if fs is None:
            return []

        stmts = fs._statements if hasattr(fs, '_statements') else {}
        bs_df = stmts.get('bs')   # 재무상태표
        # 손익계산서: 'is' 없으면 포괄손익 'cis' 사용
        is_df = stmts.get('is')
        if is_df is None or (hasattr(is_df, 'empty') and is_df.empty):
            is_df = stmts.get('cis')

        # 연도별 수집
        revenue_by_year = get_year_values(is_df, 'revenue')
        op_income_by_year = get_year_values(is_df, 'operating_income')
        net_income_by_year = get_year_values(is_df, 'net_income')
        assets_by_year = get_year_values(bs_df, 'total_assets')
        liabilities_by_year = get_year_values(bs_df, 'total_liabilities')
        equity_by_year = get_year_values(bs_df, 'total_equity')
        cur_assets_by_year = get_year_values(bs_df, 'current_assets')
        cur_liab_by_year = get_year_values(bs_df, 'current_liabilities')
        cash_by_year = get_year_values(bs_df, 'cash')

        years = sorted(set(
            list(revenue_by_year.keys()) + list(assets_by_year.keys())
        ), reverse=True)

        records = []
        for year in years[:3]:  # 최근 3년
            rev  = revenue_by_year.get(year)
            op   = op_income_by_year.get(year)
            net  = net_income_by_year.get(year)
            ast  = assets_by_year.get(year)
            liab = liabilities_by_year.get(year)
            eq   = equity_by_year.get(year)
            ca   = cur_assets_by_year.get(year)
            cl   = cur_liab_by_year.get(year)
            cash = cash_by_year.get(year)

            # 파생 지표
            debt_ratio       = (liab / eq * 100)       if liab and eq and eq != 0 else None
            current_ratio    = (ca / cl * 100)          if ca and cl and cl != 0 else None
            roe              = (net / eq * 100)          if net and eq and eq != 0 else None
            op_margin        = (op / rev * 100)          if op and rev and rev != 0 else None
            net_margin       = (net / rev * 100)         if net and rev and rev != 0 else None

            raw = {
                "revenue": rev, "operating_income": op, "net_income": net,
                "total_assets": ast, "total_liabilities": liab, "total_equity": eq,
                "current_assets": ca, "current_liabilities": cl, "cash": cash,
            }

            records.append({
                "corp_code":          corp_code,
                "corp_name":          corp_name,
                "stock_code":         getattr(corp, 'stock_code', ''),
                "fiscal_year":        year,
                "fiscal_end":         "1231",
                "report_type":        "annual",
                "consolidated":       1,
                "revenue":            rev,
                "operating_income":   op,
                "net_income":         net,
                "total_assets":       ast,
                "total_liabilities":  liab,
                "total_equity":       eq,
                "current_assets":     ca,
                "current_liabilities": cl,
                "cash":               cash,
                "debt_ratio":         debt_ratio,
                "current_ratio":      current_ratio,
                "roe":                roe,
                "operating_margin":   op_margin,
                "net_margin":         net_margin,
                "raw_json":           json.dumps(raw, ensure_ascii=False),
                "fetched_at":         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

        return records

    except Exception as e:
        print(f"  [{corp_name}] 오류: {e}")
        return []


def save_records(db: sqlite3.Connection, records: list):
    for r in records:
        db.execute("""
            INSERT INTO financials
              (corp_code, corp_name, stock_code, fiscal_year, fiscal_end, report_type, consolidated,
               revenue, operating_income, net_income, total_assets, total_liabilities, total_equity,
               current_assets, current_liabilities, cash, debt_ratio, current_ratio, roe,
               operating_margin, net_margin, raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(corp_code, fiscal_year, fiscal_end, report_type, consolidated)
            DO UPDATE SET
              revenue=excluded.revenue, operating_income=excluded.operating_income,
              net_income=excluded.net_income, total_assets=excluded.total_assets,
              total_liabilities=excluded.total_liabilities, total_equity=excluded.total_equity,
              current_assets=excluded.current_assets, current_liabilities=excluded.current_liabilities,
              cash=excluded.cash, debt_ratio=excluded.debt_ratio, current_ratio=excluded.current_ratio,
              roe=excluded.roe, operating_margin=excluded.operating_margin,
              net_margin=excluded.net_margin, raw_json=excluded.raw_json, fetched_at=excluded.fetched_at
        """, [
            r["corp_code"], r["corp_name"], r.get("stock_code",""),
            r["fiscal_year"], r["fiscal_end"], r["report_type"], r["consolidated"],
            r.get("revenue"), r.get("operating_income"), r.get("net_income"),
            r.get("total_assets"), r.get("total_liabilities"), r.get("total_equity"),
            r.get("current_assets"), r.get("current_liabilities"), r.get("cash"),
            r.get("debt_ratio"), r.get("current_ratio"), r.get("roe"),
            r.get("operating_margin"), r.get("net_margin"),
            r.get("raw_json"), r.get("fetched_at"),
        ])
    db.commit()


def main():
    parser = argparse.ArgumentParser(description="XBRL 재무데이터 수집")
    parser.add_argument("--corp",  default="",   help="특정 corp_code (기본: AI분석 완료 기업)")
    parser.add_argument("--limit", type=int, default=100, help="처리 최대 기업 수 (기본: 100)")
    parser.add_argument("--year",  type=int, default=2023, help="수집 시작 연도 (기본: 2023)")
    parser.add_argument("--delay", type=float, default=3.0, help="기업간 대기 시간 초 (기본: 3)")
    parser.add_argument("--all",   action="store_true", help="이미 수집된 기업도 재수집")
    args = parser.parse_args()

    dart_key = os.environ.get("DART_API_KEY", "")
    if not dart_key:
        print("오류: DART_API_KEY가 없습니다")
        sys.exit(1)

    import dart_fss as dart
    dart.enable_spinner(False)
    dart.set_api_key(dart_key)

    db = get_db()
    ensure_table(db)

    bgn_de = f"{args.year}0101"

    # 수집 대상 기업 목록
    if args.corp:
        rows = db.execute(
            "SELECT DISTINCT corp_code, MAX(corp_name) as corp_name FROM ai_comparisons WHERE corp_code=? AND status='ok' GROUP BY corp_code",
            [args.corp]
        ).fetchall()
    else:
        rows = db.execute("""
            SELECT DISTINCT ac.corp_code, MAX(ac.corp_name) as corp_name
            FROM ai_comparisons ac
            WHERE ac.status='ok'
            GROUP BY ac.corp_code
            ORDER BY ac.corp_code
        """).fetchall()

    # 이미 수집된 기업 제외
    if not args.all:
        done_set = set(
            r[0] for r in db.execute(
                "SELECT DISTINCT corp_code FROM financials WHERE fiscal_year >= ?",
                [args.year]
            ).fetchall()
        )
        rows = [r for r in rows if r["corp_code"] not in done_set]

    rows = rows[:args.limit]
    total = len(rows)

    print(f"\n{'='*60}")
    print(f"XBRL 재무데이터 수집 시작")
    print(f"  대상 기업: {total}개")
    print(f"  수집 시작 연도: {args.year}년")
    print(f"  요청 간격: {args.delay}초")
    print(f"{'='*60}\n")

    success = 0
    errors  = 0
    start   = time.time()

    for i, row in enumerate(rows, 1):
        corp_code = row["corp_code"]
        corp_name = row["corp_name"] or corp_code

        print(f"  [{i:4d}/{total:4d}] {corp_name[:20]:<20}", end=" ", flush=True)
        records = fetch_corp_financials(corp_code, corp_name, bgn_de, dart)

        if records:
            save_records(db, records)
            yrs = [r["fiscal_year"] for r in records if r.get("revenue")]
            rev = records[0].get("revenue")
            rev_str = f"{rev/1e8:.0f}억" if rev else "매출없음"
            print(f"✓ {len(records)}년치 ({', '.join(map(str,yrs[:2]))}) 매출:{rev_str}")
            success += 1
        else:
            print("- 데이터 없음")
            errors += 1

        if i < total:
            time.sleep(args.delay)

    elapsed = time.time() - start
    total_saved = db.execute("SELECT COUNT(*) FROM financials").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"완료!")
    print(f"  성공: {success}개  실패: {errors}개")
    print(f"  소요 시간: {elapsed/60:.1f}분")
    print(f"  DB 누적 재무데이터: {total_saved}건")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
