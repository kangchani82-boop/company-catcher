"""
scripts/fetch_corp_eng_names.py
───────────────────────────────
DART company.json API 호출해서 corp_name_eng 일괄 수집.

실행:
  python scripts/fetch_corp_eng_names.py             # 전체 (누락분만)
  python scripts/fetch_corp_eng_names.py --force     # 이미 있어도 재수집
  python scripts/fetch_corp_eng_names.py --workers 8 # 병렬 워커 수
"""
import argparse, io, os, sqlite3, sys, time, json, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# .env
if ENV_PATH.exists():
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

API_KEY = os.environ.get("DART_API_KEY", "")
if not API_KEY:
    print("[오류] DART_API_KEY 미설정"); sys.exit(1)


def ensure_column():
    db = sqlite3.connect(str(DB_PATH))
    cols = [r[1] for r in db.execute("PRAGMA table_info(companies)").fetchall()]
    if "corp_name_eng" not in cols:
        print("[info] companies.corp_name_eng 컬럼 추가")
        db.execute("ALTER TABLE companies ADD COLUMN corp_name_eng TEXT")
    if "corp_name_eng_fetched_at" not in cols:
        db.execute("ALTER TABLE companies ADD COLUMN corp_name_eng_fetched_at TEXT")
    db.commit(); db.close()


_FETCH_DELAY = 0.0

def fetch_one(corp_code: str) -> dict:
    if _FETCH_DELAY > 0:
        time.sleep(_FETCH_DELAY)
    url = f"https://opendart.fss.or.kr/api/company.json?crtfc_key={API_KEY}&corp_code={corp_code}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            d = json.loads(resp.read().decode("utf-8"))
        if d.get("status") == "000":
            return {"ok": True, "corp_code": corp_code,
                    "corp_name_eng": (d.get("corp_name_eng") or "").strip(),
                    "stock_code": (d.get("stock_code") or "").strip()}
        return {"ok": False, "corp_code": corp_code, "error": d.get("message", "unknown")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "corp_code": corp_code, "error": f"HTTP {e.code}"}
    except Exception as e:
        return {"ok": False, "corp_code": corp_code, "error": str(e)[:60]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--force",   action="store_true")
    ap.add_argument("--limit",   type=int, default=0)
    ap.add_argument("--delay",   type=float, default=0,
                    help="요청 간 sleep (초). 차단 회피용. 기본 0.")
    ap.add_argument("--listed-only", action="store_true",
                    help="stock_code 있는 상장사만 (잔여분 효율화)")
    args = ap.parse_args()

    global _FETCH_DELAY
    _FETCH_DELAY = args.delay

    ensure_column()
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row

    listed_filter = " AND stock_code IS NOT NULL AND stock_code != ''" if args.listed_only else ""
    if args.force:
        rows = db.execute(f"SELECT corp_code FROM companies WHERE corp_code IS NOT NULL{listed_filter}").fetchall()
    else:
        rows = db.execute(f"""
            SELECT corp_code FROM companies
            WHERE corp_code IS NOT NULL
              AND (corp_name_eng IS NULL OR corp_name_eng_fetched_at IS NULL OR corp_name_eng = '')
              {listed_filter}
        """).fetchall()

    codes = [r["corp_code"] for r in rows]
    if args.limit:
        codes = codes[: args.limit]
    total = len(codes)
    print(f"[info] 대상 {total}개 / 워커 {args.workers}")

    now = time.strftime("%Y-%m-%d %H:%M:%S")
    ok_cnt = err_cnt = with_eng = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(fetch_one, c): c for c in codes}
        for i, fut in enumerate(as_completed(futs), 1):
            r = fut.result()
            cc = r["corp_code"]
            if r["ok"]:
                eng = r["corp_name_eng"]
                if eng: with_eng += 1
                db.execute(
                    "UPDATE companies SET corp_name_eng=?, corp_name_eng_fetched_at=? WHERE corp_code=?",
                    [eng, now, cc]
                )
                ok_cnt += 1
            else:
                err_cnt += 1
            if i % 100 == 0:
                db.commit()
                eta = (time.time() - t0) / i * (total - i)
                print(f"  [{i:>4}/{total}] ok={ok_cnt} err={err_cnt} eng={with_eng} (남은 ~{eta:.0f}s)")
    db.commit()
    db.close()
    print(f"\n[완료] {ok_cnt}/{total} 성공, 영문명 있음 {with_eng}, 오류 {err_cnt}")
    print(f"       소요 {time.time()-t0:.1f}초")


if __name__ == "__main__":
    main()
