"""
scripts/fetch_financials.py
────────────────────────────
DART fnlttSinglAcntAll REST API → financials 테이블 수집

단계:
  1. --update-stocks : corpCode.xml → companies.stock_code 업데이트
  2. (기본)           : 공급망 상장사 재무데이터 수집

수집 대상 : companies 테이블 + stock_code 있음 + 금융·보험/공공·기관 제외
수집 연도 : 2022 · 2023 · 2024 (연간 사업보고서)
수집 계정 : 매출액 · 영업이익 · 당기순이익 · 자산총계 · 부채총계 · 자본총계
            유동자산 · 유동부채 · 현금및현금성자산 · 재고자산 · 영업활동현금흐름
파생 지표 : 부채비율 · 자기자본비율 · 유동비율 · 현금비율 · 영업이익률
            순이익률 · ROE · ROA · 재고회전율

실행 예시:
  python scripts/fetch_financials.py --update-stocks   # stock_code 먼저 업데이트
  python scripts/fetch_financials.py                   # 전체 수집
  python scripts/fetch_financials.py --corp 00126380   # 특정 기업 1개
  python scripts/fetch_financials.py --force           # 기존 데이터 덮어쓰기
  python scripts/fetch_financials.py --stats           # 현황 통계
"""

import io, sys, os, json, time, sqlite3, zipfile, argparse
import urllib.request, urllib.error, urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT     = Path(__file__).parent.parent
DB_PATH  = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# ── 환경변수 ──────────────────────────────────────────────────────────────────
def _load_env():
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_load_env()
DART_KEY = os.environ.get("DART_API_KEY", "").strip()

# ── 설정 ──────────────────────────────────────────────────────────────────────
EXCLUDE_SECTORS = {"금융·보험", "공공·기관"}
COLLECT_YEARS   = [2022, 2023, 2024, 2025]
REPRT_CODE      = "11011"   # 사업보고서(연간)
API_DELAY       = 0.5       # 초
BASE_URL        = "https://opendart.fss.or.kr/api"

# ── 계정명 매핑 ───────────────────────────────────────────────────────────────
# 각 필드별로 DART에서 나올 수 있는 한글 계정명 변형 목록
ACCOUNT_MAP: dict[str, list[str]] = {
    "revenue": [
        "매출액", "수익(매출액)", "영업수익", "매출", "순매출액",
        "수익", "총매출액", "매출액(수익)",
    ],
    "operating_income": [
        "영업이익", "영업이익(손실)", "영업손익", "영업이익(영업손실)",
        "영업손실",
    ],
    "net_income": [
        "당기순이익", "당기순이익(손실)", "당기순손익",
        "연결당기순이익", "당기순이익(당기순손실)",
        "지배기업소유주지분당기순이익",
    ],
    "total_assets":       ["자산총계"],
    "total_liabilities":  ["부채총계"],
    "total_equity":       ["자본총계"],
    "current_assets":     ["유동자산"],
    "current_liabilities":["유동부채"],
    "cash": [
        "현금및현금성자산", "현금및현금성자산(연결)", "현금 및 현금성자산",
        "기말현금및현금성자산", "기말 현금및현금성자산", "현금및현금등가물",
    ],
    "inventory": [
        "재고자산", "재고",
    ],
    "operating_cf": [
        "영업활동으로인한현금흐름", "영업활동현금흐름",
        "영업활동으로 인한 현금흐름", "영업활동으로인한 현금흐름",
        "영업으로부터창출된현금",
    ],
}

# 역방향 맵: 공백제거 계정명 → 필드명
_LOOKUP: dict[str, str] = {}
for _f, _names in ACCOUNT_MAP.items():
    for _nm in _names:
        _LOOKUP[_nm.replace(" ", "")] = _f


# ── DB ────────────────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    return db


def ensure_schema(db: sqlite3.Connection):
    """financials 테이블 생성 및 신규 컬럼 추가 (기존 데이터 보존)."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS financials (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            corp_code           TEXT NOT NULL,
            corp_name           TEXT,
            stock_code          TEXT,
            fiscal_year         INTEGER,
            report_type         TEXT DEFAULT 'annual',
            fs_div              TEXT DEFAULT 'CFS',
            revenue             REAL,
            operating_income    REAL,
            net_income          REAL,
            total_assets        REAL,
            total_liabilities   REAL,
            total_equity        REAL,
            current_assets      REAL,
            current_liabilities REAL,
            cash                REAL,
            inventory           REAL,
            operating_cf        REAL,
            debt_ratio          REAL,
            equity_ratio        REAL,
            current_ratio       REAL,
            cash_ratio          REAL,
            operating_margin    REAL,
            net_margin          REAL,
            roe                 REAL,
            roa                 REAL,
            inventory_turnover  REAL,
            fetched_at          TEXT,
            UNIQUE(corp_code, fiscal_year, report_type, fs_div)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_fin_corp ON financials(corp_code)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_fin_year ON financials(fiscal_year)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_fin_sector ON financials(corp_code, fiscal_year)")

    # 기존 테이블에 없는 컬럼 추가 (마이그레이션)
    existing = {r[1] for r in db.execute("PRAGMA table_info(financials)").fetchall()}
    new_cols = [
        ("inventory",          "REAL"),
        ("operating_cf",       "REAL"),
        ("equity_ratio",       "REAL"),
        ("cash_ratio",         "REAL"),
        ("roa",                "REAL"),
        ("inventory_turnover", "REAL"),
        ("fs_div",             "TEXT DEFAULT 'CFS'"),
    ]
    for col, typ in new_cols:
        if col not in existing:
            db.execute(f"ALTER TABLE financials ADD COLUMN {col} {typ}")
            print(f"  [migrate] 컬럼 추가: {col}")

    db.commit()


# ── DART API 호출 ─────────────────────────────────────────────────────────────
def _dart_get(endpoint: str, params: dict, timeout: int = 30) -> dict:
    """DART REST API GET 호출 → dict 반환."""
    params["crtfc_key"] = DART_KEY
    url = f"{BASE_URL}/{endpoint}.json?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        raise RuntimeError(f"HTTP {e.code}: {body}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL 오류: {e.reason}")


def _parse_amount(val: str) -> float | None:
    """DART 금액 문자열 → float. 빈값/대시 → None."""
    if not val or val.strip() in ("", "-", "－"):
        return None
    val = val.strip().replace(",", "").replace(" ", "")
    # 음수: (1234567) 형태
    if val.startswith("(") and val.endswith(")"):
        val = "-" + val[1:-1]
    try:
        return float(val)
    except ValueError:
        return None


def _call_acnt_all(corp_code: str, year: int, fs_div: str) -> list[dict]:
    """fnlttSinglAcntAll 호출 → list of account dicts."""
    try:
        data = _dart_get("fnlttSinglAcntAll", {
            "corp_code":  corp_code,
            "bsns_year":  str(year),
            "reprt_code": REPRT_CODE,
            "fs_div":     fs_div,
        })
    except RuntimeError:
        return []

    if data.get("status") != "000":
        return []
    return data.get("list") or []


# ── 계정 파싱 ──────────────────────────────────────────────────────────────────
def parse_accounts(items: list[dict]) -> dict[str, float | None]:
    """API 응답 list → {필드명: 금액} 딕셔너리."""
    result: dict[str, float | None] = {f: None for f in ACCOUNT_MAP}

    for item in items:
        nm = (item.get("account_nm") or "").replace(" ", "")
        field = _LOOKUP.get(nm)
        if not field:
            continue
        if result[field] is not None:
            continue   # 이미 채워진 경우 첫 번째 값 우선
        amt = _parse_amount(item.get("thstrm_amount", ""))
        if amt is not None:
            result[field] = amt

    return result


# ── 파생 지표 계산 ─────────────────────────────────────────────────────────────
def _safe_div(a, b) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return a / b


def calc_derived(a: dict) -> dict:
    """원본 계정 → 파생 지표 계산."""
    rev  = a.get("revenue")
    op   = a.get("operating_income")
    net  = a.get("net_income")
    ast  = a.get("total_assets")
    liab = a.get("total_liabilities")
    eq   = a.get("total_equity")
    ca   = a.get("current_assets")
    cl   = a.get("current_liabilities")
    cash = a.get("cash")
    inv  = a.get("inventory")

    debt_ratio       = _safe_div(liab, eq)
    equity_ratio     = _safe_div(eq,   ast)
    current_ratio    = _safe_div(ca,   cl)
    cash_ratio       = _safe_div(cash, cl)
    op_margin        = _safe_div(op,   rev)
    net_margin       = _safe_div(net,  rev)
    roe              = _safe_div(net,  eq)
    roa              = _safe_div(net,  ast)
    inv_turnover     = _safe_div(rev,  inv)

    def pct(v): return round(v * 100, 4) if v is not None else None
    def x(v):   return round(v, 4)       if v is not None else None

    return {
        "debt_ratio":        pct(debt_ratio),
        "equity_ratio":      pct(equity_ratio),
        "current_ratio":     pct(current_ratio),
        "cash_ratio":        pct(cash_ratio),
        "operating_margin":  pct(op_margin),
        "net_margin":        pct(net_margin),
        "roe":               pct(roe),
        "roa":               pct(roa),
        "inventory_turnover":x(inv_turnover),
    }


# ── 단일 기업 수집 ─────────────────────────────────────────────────────────────
def fetch_one(corp_code: str, corp_name: str, stock_code: str,
              years: list[int]) -> list[dict]:
    """기업 1개, 여러 연도 수집 → record list."""
    records = []

    for year in years:
        # CFS 우선, 없으면 OFS
        for fs_div in ("CFS", "OFS"):
            items = _call_acnt_all(corp_code, year, fs_div)
            if items:
                break
        else:
            continue   # 둘 다 없음

        acnts = parse_accounts(items)

        # 최소 데이터 없으면 스킵 (매출액 or 자산총계 중 하나라도 있어야)
        if acnts.get("revenue") is None and acnts.get("total_assets") is None:
            continue

        derived = calc_derived(acnts)

        records.append({
            "corp_code":            corp_code,
            "corp_name":            corp_name,
            "stock_code":           stock_code or "",
            "fiscal_year":          year,
            "report_type":          "annual",
            "fs_div":               fs_div,
            **acnts,
            **derived,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

        time.sleep(API_DELAY)

    return records


# ── DB 저장 ───────────────────────────────────────────────────────────────────
def upsert_records(db: sqlite3.Connection, records: list[dict]):
    for r in records:
        db.execute("""
            INSERT INTO financials
              (corp_code, corp_name, stock_code, fiscal_year, report_type, fs_div,
               revenue, operating_income, net_income,
               total_assets, total_liabilities, total_equity,
               current_assets, current_liabilities, cash, inventory, operating_cf,
               debt_ratio, equity_ratio, current_ratio, cash_ratio,
               operating_margin, net_margin, roe, roa, inventory_turnover,
               fetched_at)
            VALUES
              (:corp_code, :corp_name, :stock_code, :fiscal_year, :report_type, :fs_div,
               :revenue, :operating_income, :net_income,
               :total_assets, :total_liabilities, :total_equity,
               :current_assets, :current_liabilities, :cash, :inventory, :operating_cf,
               :debt_ratio, :equity_ratio, :current_ratio, :cash_ratio,
               :operating_margin, :net_margin, :roe, :roa, :inventory_turnover,
               :fetched_at)
            ON CONFLICT(corp_code, fiscal_year, report_type, fs_div) DO UPDATE SET
              corp_name=excluded.corp_name, stock_code=excluded.stock_code,
              revenue=excluded.revenue, operating_income=excluded.operating_income,
              net_income=excluded.net_income, total_assets=excluded.total_assets,
              total_liabilities=excluded.total_liabilities, total_equity=excluded.total_equity,
              current_assets=excluded.current_assets, current_liabilities=excluded.current_liabilities,
              cash=excluded.cash, inventory=excluded.inventory, operating_cf=excluded.operating_cf,
              debt_ratio=excluded.debt_ratio, equity_ratio=excluded.equity_ratio,
              current_ratio=excluded.current_ratio, cash_ratio=excluded.cash_ratio,
              operating_margin=excluded.operating_margin, net_margin=excluded.net_margin,
              roe=excluded.roe, roa=excluded.roa, inventory_turnover=excluded.inventory_turnover,
              fetched_at=excluded.fetched_at
        """, r)
    db.commit()


# ── corpCode.xml → stock_code 업데이트 ────────────────────────────────────────
def update_stock_codes(db: sqlite3.Connection):
    """DART corpCode.xml 다운로드 → companies.stock_code 일괄 업데이트."""
    if not DART_KEY:
        print("[오류] DART_API_KEY 없음")
        return

    print("[1/3] corpCode.xml 다운로드 중...")
    url = f"{BASE_URL}/corpCode.xml?crtfc_key={DART_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            zip_data = resp.read()
    except Exception as e:
        print(f"[오류] 다운로드 실패: {e}")
        return

    print("[2/3] ZIP 압축 해제 및 XML 파싱 중...")
    try:
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            xml_name = [n for n in zf.namelist() if n.lower().endswith(".xml")][0]
            xml_data = zf.read(xml_name)
    except Exception as e:
        print(f"[오류] ZIP 파싱 실패: {e}")
        return

    root = ET.fromstring(xml_data)
    corp_map: dict[str, str] = {}   # corp_code → stock_code
    for item in root.findall(".//list"):
        cc   = (item.findtext("corp_code") or "").strip()
        sc   = (item.findtext("stock_code") or "").strip()
        if cc and sc:   # stock_code가 공백이면 비상장
            corp_map[cc] = sc

    print(f"[3/3] 상장사 {len(corp_map)}개 확인. DB 업데이트 중...")
    updated = 0
    for cc, sc in corp_map.items():
        cur = db.execute(
            "UPDATE companies SET stock_code=? WHERE corp_code=? AND (stock_code IS NULL OR stock_code='')",
            [sc, cc]
        )
        updated += cur.rowcount
    db.commit()

    total_listed = db.execute(
        "SELECT COUNT(*) FROM companies WHERE stock_code IS NOT NULL AND stock_code != ''"
    ).fetchone()[0]
    print(f"  ✓ {updated}개 업데이트 완료 (DB 내 상장사 총 {total_listed}개)")


# ── 통계 출력 ──────────────────────────────────────────────────────────────────
def print_stats(db: sqlite3.Connection):
    total = db.execute("SELECT COUNT(*) FROM financials").fetchone()[0]
    corps = db.execute("SELECT COUNT(DISTINCT corp_code) FROM financials").fetchone()[0]
    print(f"\n{'='*55}")
    print(f"  financials 현황")
    print(f"{'='*55}")
    print(f"  전체 레코드: {total:,}건  /  기업 수: {corps:,}개")

    print(f"\n  [연도별]")
    for r in db.execute(
        "SELECT fiscal_year, COUNT(*) c, COUNT(DISTINCT corp_code) nc "
        "FROM financials GROUP BY fiscal_year ORDER BY fiscal_year DESC"
    ).fetchall():
        print(f"    {r['fiscal_year']}  {r['c']:5}건  ({r['nc']}개사)")

    print(f"\n  [필드 커버리지 — 2024년 기준]")
    base = db.execute(
        "SELECT COUNT(*) FROM financials WHERE fiscal_year=2024"
    ).fetchone()[0]
    if base:
        for col in ["revenue","operating_income","net_income","total_assets",
                    "cash","inventory","operating_cf","debt_ratio","inventory_turnover"]:
            cnt = db.execute(
                f"SELECT COUNT(*) FROM financials WHERE fiscal_year=2024 AND {col} IS NOT NULL"
            ).fetchone()[0]
            bar = "█" * int(cnt/base*20)
            print(f"    {col:<22} {cnt:4}/{base} {bar}")
    print(f"{'='*55}\n")


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="DART 재무데이터 수집")
    parser.add_argument("--update-stocks", action="store_true",
                        help="corpCode.xml → companies.stock_code 업데이트")
    parser.add_argument("--corp",  default="", help="특정 corp_code 1개만 처리")
    parser.add_argument("--force", action="store_true",
                        help="이미 수집된 기업도 재수집 (덮어쓰기)")
    parser.add_argument("--limit", type=int, default=0,
                        help="처리 최대 기업 수 (0=전체)")
    parser.add_argument("--stats", action="store_true", help="현황 통계만 출력")
    args = parser.parse_args()

    if not DART_KEY:
        print("[오류] DART_API_KEY가 .env에 없습니다.")
        sys.exit(1)

    db = get_db()
    ensure_schema(db)

    if args.stats:
        print_stats(db)
        db.close()
        return

    # ── Step 1: stock_code 업데이트 ──
    if args.update_stocks:
        update_stock_codes(db)
        listed = db.execute(
            "SELECT COUNT(*) FROM companies WHERE stock_code IS NOT NULL AND stock_code!=''"
        ).fetchone()[0]
        print(f"\n상장사 필터 적용 시 대상 기업 수: {listed}개")
        db.close()
        return

    # ── Step 2: 수집 대상 기업 선별 ──
    if args.corp:
        rows = db.execute(
            "SELECT corp_code, corp_name, stock_code FROM companies WHERE corp_code=?",
            [args.corp]
        ).fetchall()
    else:
        rows = db.execute("""
            SELECT corp_code, corp_name, stock_code
            FROM companies
            WHERE stock_code IS NOT NULL AND stock_code != ''
              AND (sector NOT IN ('금융·보험','공공·기관') OR sector IS NULL)
            ORDER BY corp_code
        """).fetchall()

    # 이미 수집된 기업 제외 (force 아닐 때)
    if not args.force and not args.corp:
        done = set(r[0] for r in db.execute(
            "SELECT DISTINCT corp_code FROM financials WHERE fiscal_year = ?",
            [max(COLLECT_YEARS)]
        ).fetchall())
        rows = [r for r in rows if r["corp_code"] not in done]

    if args.limit:
        rows = rows[:args.limit]

    total = len(rows)
    if total == 0:
        print("수집할 기업이 없습니다. (--update-stocks 먼저 실행하거나 --force 사용)")
        print_stats(db)
        db.close()
        return

    print(f"\n{'='*55}")
    print(f"  DART 재무데이터 수집 시작")
    print(f"  대상 기업: {total}개  /  연도: {COLLECT_YEARS}")
    print(f"  API 간격: {API_DELAY}초  /  예상 소요: {total*len(COLLECT_YEARS)*API_DELAY/60:.0f}분")
    print(f"{'='*55}\n")

    success = errors = skipped = 0
    t_start = time.time()

    for i, row in enumerate(rows, 1):
        corp_code  = row["corp_code"]
        corp_name  = row["corp_name"] or corp_code
        stock_code = row["stock_code"] or ""

        print(f"  [{i:4d}/{total}] {corp_name[:22]:<22}", end=" ", flush=True)

        try:
            records = fetch_one(corp_code, corp_name, stock_code, COLLECT_YEARS)
            if records:
                upsert_records(db, records)
                yrs = [r["fiscal_year"] for r in records]
                rev = records[-1].get("revenue")   # 최신연도 매출
                rev_str = f"{rev/1e8:,.0f}억" if rev else "매출없음"
                print(f"✓ {len(records)}년  매출:{rev_str}  ({','.join(map(str,yrs))})")
                success += 1
            else:
                print("- 데이터 없음 (비상장/무공시)")
                skipped += 1
        except Exception as e:
            print(f"✗ 오류: {e}")
            errors += 1

    elapsed = time.time() - t_start
    print(f"\n{'='*55}")
    print(f"  완료: {success}성공 / {skipped}무데이터 / {errors}오류")
    print(f"  소요: {elapsed/60:.1f}분")
    print(f"{'='*55}")
    print_stats(db)
    db.close()


if __name__ == "__main__":
    main()
