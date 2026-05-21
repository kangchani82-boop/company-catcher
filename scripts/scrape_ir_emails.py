"""
scripts/scrape_ir_emails.py
───────────────────────────
회사 홈페이지·IR 페이지에서 IR 담당 이메일 자동 발굴.

결과는 **ir_contacts_2** 테이블에만 저장 (기존 ir_contacts 와 분리).

탐색 우선순위:
  1) companies.ir_url (DART 제공 IR 페이지)
  2) hm_url/{ir, ir/contact, investor, investors, investor-relations}
  3) hm_url/{contact, contact-us, about/contact, contact-us/ir}
  4) hm_url (메인 — footer 의 IR 이메일 확률)

이메일 점수 (60+ 만 저장):
  +50  email 도메인 == 회사 도메인
  +25  prefix: ir@/invest@/disclosure@/pr@/irteam@
  +20  HTML 주변에 "IR", "투자자", "공시" 키워드
  +15  URL 경로 /ir/ 또는 /investor/
   -30 prefix: info@/contact@/webmaster@/support@/admin@
   -40 외부 도메인 (gmail/naver/daum)

실행:
  python scripts/scrape_ir_emails.py --pending-only   # IR 이메일 없는 pending 회사만
  python scripts/scrape_ir_emails.py --listed-only    # 전체 상장사
  python scripts/scrape_ir_emails.py --corp-code 00126380
  python scripts/scrape_ir_emails.py --stats
"""
import argparse
import io
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

UA = "Mozilla/5.0 (CompanyCatcher IR-Discovery; +contact: kjm@finance-scope.com)"

# ─── 이메일 패턴 ─────────────────────────────────────────────────────────
EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    re.MULTILINE,
)
# 난독화 형태 (예: ir [at] samsung [dot] com)
OBFUSCATED_RE = re.compile(
    r"([A-Za-z0-9._%+-]+)\s*[\[\(]\s*(?:at|@|골뱅이)\s*[\]\)]\s*([A-Za-z0-9.-]+)\s*[\[\(]\s*(?:dot|\.|점)\s*[\]\)]\s*([A-Za-z]{2,})",
    re.IGNORECASE,
)

IR_KEYWORDS = ["ir", "investor", "공시", "investor relations", "투자자", "투자정보",
               "IR담당", "기업설명", "주주", "disclosure"]

# priority/bad prefix를 정규식 기반으로 유연하게 매칭 (lem_ir@, invest.kr@ 같은 변형 포함)
PRIORITY_PREFIX_RE = re.compile(
    r"(?:^|[_.\-])(?:ir|invest|disclosure|shareholders?|pr|kicvis)(?:[_.\-]|$)",
    re.IGNORECASE,
)
BAD_PREFIX_EXACT = {"info", "contact", "webmaster", "admin", "support",
                    "help", "service", "cs", "mail", "office", "test",
                    "noreply", "no-reply", "newsletter", "marketing", "sales",
                    "hr", "career", "careers", "jobs", "recruit", "privacy",
                    "legal", "press", "webadmin"}

# 일반 무료 메일 도메인 (회사 이메일이 아님)
EXTERNAL_DOMAINS = {"gmail.com", "naver.com", "daum.net", "hanmail.net",
                    "nate.com", "kakao.com", "yahoo.com", "outlook.com",
                    "hotmail.com", "icloud.com", "yahoo.co.kr"}

# 한국 대기업 그룹 도메인 (계열사 자주 사용 — 외부 패널티 완화)
KR_GROUP_DOMAINS = {"samsung.com", "samsung.co.kr",
                    "lg.com", "lg.co.kr", "lge.com", "lguplus.co.kr",
                    "sk.com", "sk.co.kr", "skbroadband.com",
                    "hyundai.com", "hyundai-mobis.com", "hyundai-motor.com",
                    "kia.com", "lotte.net", "lotte.co.kr",
                    "hanwha.co.kr", "hanwha.com",
                    "doosan.com", "doosan.co.kr",
                    "hanjin.com", "kumhopetrochem.com",
                    "cj.net", "cjcheiljedang.com",
                    "shinhan.com", "kbfg.com", "hanafn.com",
                    "kt.com", "ktnet.co.kr",
                    "posco.co.kr", "poscoholdings.com", "poscoenc.com",
                    "kakaocorp.com"}

# URL 경로 후보 — 메인 페이지 먼저, 그 다음 IR 추측 경로
PATH_CANDIDATES = [
    "",  # 메인 페이지 (footer 이메일 자주 있음)
    "/ir", "/ir/", "/investor", "/investor/", "/investors",
    "/contact", "/contact/", "/contact-us",
    "/about/contact", "/company/contact",
    "/kor/ir/", "/ko/ir/", "/ko/contact",
]

# 링크 텍스트/URL에 이 키워드 있으면 IR 후보 링크로 추출
LINK_HINTS = [
    "ir", "investor", "investors", "investor-relations",
    "공시", "투자정보", "투자자", "IR자료", "기업설명",
    "contact", "문의", "연락처", "고객문의", "기업문의", "관계자",
]
LINK_RE = re.compile(r'<a\s+[^>]*href\s*=\s*["\']([^"\']+)["\'][^>]*>(.*?)</a>',
                     re.IGNORECASE | re.DOTALL)

# ─── 유틸 ───────────────────────────────────────────────────────────────
def normalize_url(url: str) -> str:
    """www.x.com → https://www.x.com 같은 형태로 정규화."""
    if not url:
        return ""
    url = url.strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def domain_of(url: str) -> str:
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        host = host.split(":")[0]  # 포트 제거
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def fetch_html(url: str, timeout: int = 8) -> str | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ko,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ctype = resp.headers.get("Content-Type", "")
            if "html" not in ctype.lower() and "xml" not in ctype.lower():
                return None
            raw = resp.read(500_000)   # 500KB 제한
        # encoding 감지
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            try: return raw.decode("euc-kr", errors="ignore")
            except: return raw.decode("latin-1", errors="ignore")
    except Exception:
        return None


def extract_emails(html: str) -> set:
    """HTML 에서 모든 이메일 추출 (난독화 포함)."""
    if not html:
        return set()
    emails = set(m.lower() for m in EMAIL_RE.findall(html))
    # 난독화 복원
    for m in OBFUSCATED_RE.finditer(html):
        emails.add(f"{m.group(1)}@{m.group(2)}.{m.group(3)}".lower())
    return emails


def find_text_around(html: str, email: str, window: int = 200) -> str:
    """이메일 주변 ±window 문자."""
    if not html: return ""
    idx = html.lower().find(email.lower())
    if idx < 0: return ""
    return html[max(0, idx - window): idx + len(email) + window]


def score_email(email: str, html: str, url: str, corp_domain: str) -> int:
    score = 0
    prefix = email.split("@", 1)[0].lower()
    domain = email.split("@", 1)[1].lower()

    is_priority_prefix = bool(PRIORITY_PREFIX_RE.search(prefix))
    is_bad_prefix = prefix in BAD_PREFIX_EXACT

    # ── 도메인 매칭 ──────────────────────────────────────────────────────
    domain_matched = False
    if corp_domain and (
        domain == corp_domain
        or domain.endswith("." + corp_domain)
        or corp_domain.endswith("." + domain)
    ):
        score += 50
        domain_matched = True
    else:
        cd_root = corp_domain.split(".")[0] if corp_domain else ""
        dom_root = domain.split(".")[0]
        if cd_root and len(cd_root) >= 4 and (cd_root in domain or dom_root in corp_domain):
            score += 30
            domain_matched = True

    if not domain_matched:
        if domain in EXTERNAL_DOMAINS:
            # 무료 메일은 큰 페널티 (회사 메일 X)
            score -= 50
        elif domain in KR_GROUP_DOMAINS:
            # 그룹사 도메인 — priority prefix면 사실상 회사 IR 메일
            if is_priority_prefix:
                score += 20    # 거의 매칭으로 인정
            else:
                score -= 10
        else:
            # 기타 외부 도메인 — priority prefix면 페널티 완화
            if is_priority_prefix:
                score -= 5
            else:
                score -= 25

    # ── prefix 평가 ──────────────────────────────────────────────────────
    if is_priority_prefix:
        score += 35
    elif is_bad_prefix:
        score -= 35

    # ── URL 경로 ────────────────────────────────────────────────────────
    p = urllib.parse.urlparse(url).path.lower()
    if "/ir" in p or "/investor" in p or "/disclosure" in p:
        score += 15
    elif "/contact" in p or "/inquiry" in p or "/about" in p:
        score += 8

    # ── 주변 텍스트 (IR/투자자/공시 키워드) ──────────────────────────────
    ctx = find_text_around(html, email, window=300).lower()
    keyword_hits = sum(1 for k in IR_KEYWORDS if k.lower() in ctx)
    if keyword_hits >= 3:
        score += 25
    elif keyword_hits >= 2:
        score += 15
    elif keyword_hits == 1:
        score += 8

    return score


def candidate_urls_for(hm_url: str, ir_url: str) -> list:
    """탐색할 URL 후보 리스트 (우선순위 순)."""
    urls = []
    if ir_url:
        u = normalize_url(ir_url)
        if u: urls.append(u)
    if hm_url:
        base = normalize_url(hm_url).rstrip("/")
        if base:
            for path in PATH_CANDIDATES:
                u = base + path if path else base
                urls.append(u)
    # 중복 제거 (순서 유지)
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out


def find_ir_links_in_html(html: str, base_url: str) -> list:
    """HTML 안에서 IR/contact 키워드 들어간 링크 추출."""
    if not html: return []
    base_parsed = urllib.parse.urlparse(base_url)
    base_root = f"{base_parsed.scheme}://{base_parsed.netloc}"

    candidates = []
    for m in LINK_RE.finditer(html):
        href, text = m.group(1).strip(), re.sub(r"<[^>]+>", "", m.group(2)).strip().lower()
        href_l = href.lower()
        # 키워드 매칭 (URL 또는 anchor text)
        if not any(k in href_l or k in text for k in LINK_HINTS):
            continue
        # mailto / javascript / # 제외
        if href_l.startswith(("mailto:", "javascript:", "#", "tel:")):
            continue
        # 절대화
        if href.startswith("//"):
            href = base_parsed.scheme + ":" + href
        elif href.startswith("/"):
            href = base_root + href
        elif not href.startswith("http"):
            href = base_root + "/" + href.lstrip("/")
        # 같은 도메인만
        try:
            target_host = urllib.parse.urlparse(href).netloc.lower()
            if not target_host: continue
            if base_parsed.netloc.lower() not in target_host and target_host not in base_parsed.netloc.lower():
                # 서브도메인이면 통과
                base_root_dom = base_parsed.netloc.split(":")[0]
                if base_root_dom.split(".")[-2:] != target_host.split(".")[-2:]:
                    continue
        except Exception:
            continue
        if href not in candidates:
            candidates.append(href)
    return candidates[:8]   # 최대 8개


# ─── 도메인별 rate limit ───────────────────────────────────────────────
_domain_lock = {}
_domain_last = {}
_global_lock = threading.Lock()


def domain_throttle(host: str, min_gap: float = 1.0):
    with _global_lock:
        lock = _domain_lock.setdefault(host, threading.Lock())
    with lock:
        last = _domain_last.get(host, 0)
        wait = min_gap - (time.time() - last)
        if wait > 0:
            time.sleep(wait)
        _domain_last[host] = time.time()


def scrape_one_corp(corp_code: str, corp_name: str, hm_url: str, ir_url: str,
                    threshold: int = 40, max_links: int = 6) -> dict:
    """
    탐색 전략:
      Phase A: 추측 경로 (ir_url + /ir, /contact 등) — 빠르게
      Phase B: 메인 페이지 HTML에서 IR/contact 링크 추출 → 추가 방문
    """
    urls = candidate_urls_for(hm_url, ir_url)
    if not urls:
        return {"ok": False, "reason": "no_url"}

    corp_domain = domain_of(normalize_url(hm_url or ir_url))
    best = None
    visited = set()

    def process_url(u: str, also_extract_links: bool = False) -> tuple:
        """returns (best_updated, html_for_link_extraction)"""
        nonlocal best
        if u in visited: return False, None
        visited.add(u)
        host = domain_of(u)
        if not host: return False, None
        domain_throttle(host, min_gap=1.0)
        html = fetch_html(u, timeout=8)
        if not html: return False, None
        emails = extract_emails(html)
        updated = False
        for em in emails:
            sc = score_email(em, html, u, corp_domain)
            if best is None or sc > best["score"]:
                best = {"email": em, "score": sc, "url": u}
                updated = True
        return updated, (html if also_extract_links else None)

    # Phase A: 추측 경로 시도
    main_html = None
    for u in urls[:10]:   # 최대 10개 후보
        updated, html_for_links = process_url(u, also_extract_links=(u in urls[:2]))
        if html_for_links and main_html is None:
            main_html = (u, html_for_links)
        if best and best["score"] >= 80:
            return {"ok": True, "best": best, "found_at": best["url"]}

    # Phase B: 메인 페이지의 IR/contact 링크 따라가기
    if main_html:
        base_u, main = main_html
        ir_links = find_ir_links_in_html(main, base_u)
        for link in ir_links[:max_links]:
            process_url(link)
            if best and best["score"] >= 80:
                return {"ok": True, "best": best, "found_at": best["url"]}

    if best and best["score"] >= threshold:
        return {"ok": True, "best": best, "found_at": best["url"]}
    return {"ok": False, "reason": f"low_score (best_sc={best['score'] if best else 'none'})",
            "best_seen": best}


# ─── DB 저장 ─────────────────────────────────────────────────────────
_db_lock = threading.Lock()

def save_ir_contact(corp_code: str, corp_name: str, hm_url: str,
                    email: str, source_url: str, score: int):
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if   score >= 80: confidence = "A_complete"
    elif score >= 60: confidence = "B_scraped"
    else:             confidence = "C_likely"   # 40~59
    try:
        with _db_lock:
            db.execute("""
                INSERT INTO ir_contacts_2
                  (corp_code, corp_name, ir_email, homepage, source, source_url,
                   confidence, is_active, created_at, updated_at, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(corp_code, ir_email) DO UPDATE SET
                  source_url=excluded.source_url,
                  confidence=excluded.confidence,
                  updated_at=excluded.updated_at,
                  notes=excluded.notes
            """, [
                corp_code, corp_name, email, hm_url or "",
                "WEBSITE_SCRAPED", source_url,
                confidence, now, now,
                f"score={score}",
            ])
            db.commit()
    finally:
        db.close()


def get_targets(pending_only: bool, listed_only: bool, corp_code: str | None,
                limit: int) -> list:
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    if corp_code:
        rows = db.execute(
            "SELECT corp_code, corp_name, hm_url, ir_url FROM companies WHERE corp_code=?",
            [corp_code]
        ).fetchall()
    elif pending_only:
        # 1) pending 질문지 회사 중
        # 2) ir_contacts (구) 또는 ir_contacts_2 (신) 에 발송가능 이메일 없는 corp
        # 3) hm_url 또는 ir_url 있는 회사만
        rows = db.execute("""
            SELECT DISTINCT c.corp_code, c.corp_name, c.hm_url, c.ir_url
            FROM companies c
            WHERE c.corp_code IN (SELECT DISTINCT corp_code FROM ir_questionnaires WHERE status='pending')
              AND (
                COALESCE(c.hm_url,'') != '' OR COALESCE(c.ir_url,'') != ''
              )
              AND NOT EXISTS (
                SELECT 1 FROM ir_contacts ic
                WHERE ic.corp_code = c.corp_code
                  AND ic.ir_email IS NOT NULL AND ic.ir_email != ''
                  AND substr(ic.ir_email,1,1) != '_'
                  AND ic.is_active = 1
                  AND COALESCE(ic.bounced_count,0) < 3
              )
              AND NOT EXISTS (
                SELECT 1 FROM ir_contacts_2 ic2
                WHERE ic2.corp_code = c.corp_code AND ic2.is_active = 1
              )
        """).fetchall()
    elif listed_only:
        rows = db.execute("""
            SELECT corp_code, corp_name, hm_url, ir_url FROM companies
            WHERE stock_code IS NOT NULL AND stock_code != ''
              AND (COALESCE(hm_url,'') != '' OR COALESCE(ir_url,'') != '')
              AND NOT EXISTS (SELECT 1 FROM ir_contacts_2 ic WHERE ic.corp_code=companies.corp_code)
        """).fetchall()
    else:
        rows = db.execute("""
            SELECT corp_code, corp_name, hm_url, ir_url FROM companies
            WHERE (COALESCE(hm_url,'') != '' OR COALESCE(ir_url,'') != '')
              AND NOT EXISTS (SELECT 1 FROM ir_contacts_2 ic WHERE ic.corp_code=companies.corp_code)
        """).fetchall()

    if limit:
        rows = rows[:limit]
    return rows


def print_stats():
    db = sqlite3.connect(str(DB_PATH))
    total = db.execute("SELECT COUNT(*) FROM ir_contacts_2").fetchone()[0]
    distinct = db.execute("SELECT COUNT(DISTINCT corp_code) FROM ir_contacts_2").fetchone()[0]
    print(f"ir_contacts_2: 총 {total}건 / {distinct}개 회사")
    print()
    print("[confidence 분포]")
    for r in db.execute("SELECT confidence, COUNT(*) c FROM ir_contacts_2 GROUP BY confidence"):
        print(f"  {r[0]:<15} {r[1]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--pending-only", action="store_true")
    ap.add_argument("--listed-only",  action="store_true")
    ap.add_argument("--corp-code",    type=str)
    ap.add_argument("--limit",        type=int, default=0)
    ap.add_argument("--stats",        action="store_true")
    args = ap.parse_args()

    if args.stats:
        print_stats(); return

    targets = get_targets(args.pending_only, args.listed_only, args.corp_code, args.limit)
    if not targets:
        print("[info] 처리 대상 없음")
        print_stats(); return

    print(f"[info] 대상 {len(targets)}개 회사 / 워커 {args.workers}")
    t0 = time.time()

    stats = {"ok": 0, "no_url": 0, "low_score": 0, "err": 0}
    found_list = []

    def worker(row):
        try:
            res = scrape_one_corp(row["corp_code"], row["corp_name"],
                                  row["hm_url"], row["ir_url"])
            if res["ok"]:
                b = res["best"]
                save_ir_contact(row["corp_code"], row["corp_name"], row["hm_url"],
                                b["email"], res["found_at"], b["score"])
                return ("ok", row, b)
            else:
                return ("fail", row, res.get("reason", "?"))
        except Exception as e:
            return ("err", row, str(e)[:80])

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(worker, dict(r)) for r in targets]
        for i, fut in enumerate(as_completed(futs), 1):
            kind, row, info = fut.result()
            if kind == "ok":
                stats["ok"] += 1
                found_list.append((row["corp_name"], info["email"], info["score"]))
                print(f"  [{i:>3}/{len(targets)}] ✓ {row['corp_name'][:20]:<20} → {info['email']} (sc={info['score']})")
            elif kind == "fail":
                if "no_url" in info: stats["no_url"] += 1
                else: stats["low_score"] += 1
                if i % 20 == 0:
                    print(f"  [{i:>3}/{len(targets)}] (ok={stats['ok']} / low={stats['low_score']} / no_url={stats['no_url']} / err={stats['err']})")
            else:
                stats["err"] += 1

    print(f"\n[완료] 소요 {time.time()-t0:.1f}s")
    print(f"  발견: {stats['ok']} / 낮은점수: {stats['low_score']} / URL없음: {stats['no_url']} / 오류: {stats['err']}")
    print()
    print_stats()


if __name__ == "__main__":
    main()
