"""
scripts/collect_ir_contacts.py
──────────────────────────────
IR 담당자 이메일 자동 수집

전략 (메일은 절대 발송하지 않음 — DNS 조회만):
  1) DART company.json → hm_url 수집
  2) 회사 홈페이지에서 mailto: 링크 + 이메일 텍스트 추출
  3) IR 정형 경로 추가 크롤링 (/ir, /investor, /contact 등)
  4) 못 찾으면 도메인 추정 (ir@<domain>, pr@<domain> 등) + MX 레코드 검증

실행:
  python scripts/collect_ir_contacts.py --limit 50          # 50개 회사 시도
  python scripts/collect_ir_contacts.py --corp-code 00126380 # 특정 회사
  python scripts/collect_ir_contacts.py --priority leads    # story_leads 있는 회사 우선
  python scripts/collect_ir_contacts.py --stats             # 현재 통계만
"""

import io
import os
import re
import sys
import time
import socket
import sqlite3
import argparse
import requests
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urljoin

# UTF-8 강제
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"


def load_env():
    if not ENV_PATH.exists():
        return
    for line in open(ENV_PATH, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()
DART_KEY = os.environ.get("DART_API_KEY", "")


def get_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    return db


# ── 이메일 정규식 ────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"[\w\._\-]+@[\w\._\-]+\.[a-zA-Z]{2,}")
# 제외 패턴 (이미지 파일명, 일반 시스템 메일 등)
EXCLUDE_PATTERNS = [
    r"@[0-9]x\.png$", r"@[0-9]x\.jpg$",          # 이미지 파일명
    r"^noreply", r"^no-reply", r"^donotreply",
    r"^example", r"^sample", r"^test@",
    r"@example\.", r"@sample\.", r"@test\.",
    r"^webmaster", r"^admin@",
    r"^privacy", r"^kakao\.",
]


def is_valid_email(email: str) -> bool:
    """일반적이지 않거나 시스템 이메일 제외"""
    em = email.lower()
    if not re.match(r"^[\w\._\-]+@[\w\._\-]+\.[a-zA-Z]{2,}$", em):
        return False
    for pat in EXCLUDE_PATTERNS:
        if re.search(pat, em):
            return False
    return True


# ── MX 레코드 검증 (DNS만, 메일 발송 X) ─────────────────────────────────────
def check_mx(domain: str, timeout: float = 3.0) -> bool:
    """도메인 MX 레코드 존재 여부 (이메일 받을 수 있는 서버 있는지)"""
    try:
        # nslookup 대안 — Python 표준 라이브러리 socket으로는 MX 못 가져옴
        # dnspython 라이브러리 사용
        import dns.resolver
        r = dns.resolver.Resolver()
        r.timeout = timeout
        r.lifetime = timeout
        answers = r.resolve(domain, "MX")
        return len(list(answers)) > 0
    except ImportError:
        # dnspython 없으면 socket으로 도메인만 체크 (MX 직접은 못 봄)
        try:
            socket.gethostbyname(domain)
            return True  # A 레코드 있으면 일단 살아있는 도메인
        except Exception:
            return False
    except Exception:
        return False


# ── DART에서 회사 홈페이지 URL 수집 ─────────────────────────────────────────
def fetch_company_info(corp_code: str) -> dict:
    """DART company.json — hm_url + 대표 정보"""
    if not DART_KEY:
        return {}
    try:
        url = "https://opendart.fss.or.kr/api/company.json"
        r = requests.get(url, params={"crtfc_key": DART_KEY, "corp_code": corp_code}, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        if data.get("status") != "000":
            return {}
        return {
            "hm_url": data.get("hm_url", "").strip(),
            "phn_no": data.get("phn_no", "").strip(),
            "fax_no": data.get("fax_no", "").strip(),
        }
    except Exception:
        return {}


def normalize_url(url: str) -> str:
    """URL 정규화 — http(s) 추가, trailing slash 등"""
    if not url:
        return ""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    # trailing slash 제거
    if url.endswith("/"):
        url = url[:-1]
    return url


def domain_from_url(url: str) -> str:
    """URL에서 도메인만 추출"""
    try:
        p = urlparse(normalize_url(url))
        return p.netloc.lower().replace("www.", "")
    except Exception:
        return ""


# ── 홈페이지 크롤링 → 이메일 추출 ───────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.5",
}

# IR 정형 경로 (확장 v2 — 더 많은 패턴)
IR_PATH_CANDIDATES = [
    # 기본 IR 경로
    "/ir", "/ir/", "/ir/contact", "/ir/contact.html", "/ir/inquiry",
    "/ir/officer", "/ir/team", "/ir/staff", "/ir/people",
    "/ir/contact_us", "/ir/contact-us", "/ir/IR_contact",
    # Investor
    "/investor", "/investors", "/investor/contact",
    "/investor-relations", "/investor-relations/contact",
    # Contact
    "/contact", "/contact-us", "/contactus", "/contact_us",
    "/contact-officer",
    # About / Company
    "/about/contact", "/about/ir", "/about-us/contact",
    "/company/contact", "/company/ir",
    # 한·영 다국어
    "/kr/ir", "/kr/ir/contact", "/kr/contact",
    "/ko/ir", "/ko/ir/contact", "/ko/contact", "/ko/about/contact",
    "/eng/ir", "/eng/contact", "/en/ir", "/en/contact",
    # Support / Customer
    "/support/contact", "/customer/contact", "/customer/ir",
    "/center/ir", "/help/contact",
    # 기타
    "/IR", "/IR_contact", "/main/ir",
]


def fetch_emails_from_url(url: str, timeout: int = 8) -> list:
    """URL에서 이메일 추출"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return []
        # mailto: 링크 우선
        mailtos = re.findall(r'mailto:([^"\'>\s?]+)', r.text)
        # 일반 이메일 텍스트
        text_emails = EMAIL_RE.findall(r.text)
        all_emails = list(set(mailtos + text_emails))
        return [e for e in all_emails if is_valid_email(e)]
    except Exception:
        return []


def crawl_homepage_for_emails(homepage: str, max_paths: int = 15) -> list:
    """회사 홈페이지에서 IR 관련 페이지 크롤링하여 이메일 수집"""
    if not homepage:
        return []
    base = normalize_url(homepage)
    found_emails = set()
    sources = {}

    # 1) 홈페이지 자체
    emails = fetch_emails_from_url(base)
    for e in emails:
        if e not in found_emails:
            found_emails.add(e)
            sources[e] = base

    # 2) IR 정형 경로
    for path in IR_PATH_CANDIDATES[:max_paths]:
        if found_emails and len(found_emails) >= 3:  # 충분히 수집되면 중단
            break
        url = base.rstrip("/") + path
        emails = fetch_emails_from_url(url, timeout=5)
        for e in emails:
            if e not in found_emails:
                found_emails.add(e)
                sources[e] = url
        time.sleep(0.5)  # 부하 분산

    return [(e, sources[e]) for e in found_emails]


# ── IR 도메인 추정 + MX 검증 ────────────────────────────────────────────────
IR_GUESS_LOCAL_PARTS = ["ir", "pr", "contact", "info", "media", "investor", "investors"]


def guess_ir_emails(homepage: str) -> list:
    """홈페이지 도메인에서 IR 추정 이메일 + MX 검증"""
    domain = domain_from_url(homepage)
    if not domain or "." not in domain:
        return []
    results = []
    has_mx = check_mx(domain)
    if not has_mx:
        return []  # MX 없으면 어차피 메일 못 받음
    for local in IR_GUESS_LOCAL_PARTS:
        results.append((f"{local}@{domain}", "GUESS"))
    return results


# ── 이메일 우선순위 (IR 가능성 순) ──────────────────────────────────────────
def email_priority(email: str) -> int:
    """이메일 주소의 IR 적합성 점수 (낮을수록 우선)"""
    em = email.lower()
    local = em.split("@")[0]
    # 우선순위 (낮을수록 좋음)
    priorities = [
        ("ir", 1), ("investor", 1),
        ("pr", 2), ("public", 2),
        ("media", 3), ("press", 3),
        ("contact", 4), ("info", 5),
        ("support", 8), ("help", 8),
        ("sales", 9), ("marketing", 9),
    ]
    for prefix, score in priorities:
        if local.startswith(prefix):
            return score
    return 6  # 기타


# ── DB 저장 ──────────────────────────────────────────────────────────────────
def save_contact(db, corp_code, corp_name, email, source, source_url, homepage,
                 phone="", confidence="guessed", mx_verified=False):
    """ir_contacts 에 저장 (UPSERT)"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db.execute("""
        INSERT INTO ir_contacts
            (corp_code, corp_name, ir_email, ir_phone, homepage,
             source, source_url, confidence, mx_verified, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(corp_code, ir_email) DO UPDATE SET
            confidence = MAX(ir_contacts.confidence, excluded.confidence),
            mx_verified = MAX(ir_contacts.mx_verified, excluded.mx_verified),
            source = excluded.source,
            source_url = excluded.source_url,
            homepage = COALESCE(excluded.homepage, ir_contacts.homepage)
    """, [corp_code, corp_name, email, phone, homepage, source, source_url,
          confidence, 1 if mx_verified else 0, now])
    db.commit()


# ── 메인 처리 ────────────────────────────────────────────────────────────────
def process_company(db, corp_code, corp_name) -> dict:
    """회사 1개 처리. 결과 dict 반환"""
    # 이미 user_verified 된 게 있으면 SKIP
    existing = db.execute("""
        SELECT COUNT(*) FROM ir_contacts
        WHERE corp_code=? AND user_verified=1
    """, [corp_code]).fetchone()[0]
    if existing > 0:
        return {"status": "skipped_verified", "found": existing}

    # 1) DART에서 hm_url 등 가져오기
    info = fetch_company_info(corp_code)
    homepage = info.get("hm_url", "")
    phone = info.get("phn_no", "")

    if not homepage:
        return {"status": "no_homepage"}

    # 2) 홈페이지 크롤링 (mailto + 이메일 텍스트)
    crawled = crawl_homepage_for_emails(homepage)
    found_real = len(crawled) > 0

    if found_real:
        # 우선순위 정렬
        crawled.sort(key=lambda x: email_priority(x[0]))
        for email, source_url in crawled[:3]:  # 상위 3개만 저장
            save_contact(db, corp_code, corp_name, email,
                         source="HOMEPAGE", source_url=source_url,
                         homepage=homepage, phone=phone,
                         confidence="found", mx_verified=True)
        return {"status": "found_real", "count": len(crawled), "emails": [e[0] for e in crawled[:3]]}

    # 3) 못 찾으면 도메인 추정 (MX 검증 후)
    guesses = guess_ir_emails(homepage)
    if not guesses:
        # 최소한 홈페이지 정보는 저장
        save_contact(db, corp_code, corp_name, "",
                     source="DART_HOMEPAGE_ONLY", source_url=homepage,
                     homepage=homepage, phone=phone, confidence="none")
        return {"status": "no_email", "homepage": homepage}

    # MX 통과한 추정 이메일 저장
    for email, _ in guesses[:3]:
        save_contact(db, corp_code, corp_name, email,
                     source="DOMAIN_GUESS", source_url=homepage,
                     homepage=homepage, phone=phone,
                     confidence="guessed", mx_verified=True)
    return {"status": "guessed", "count": len(guesses[:3]),
            "emails": [g[0] for g in guesses[:3]]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--corp-code", type=str, default=None)
    parser.add_argument("--priority", choices=["leads", "all"], default="leads")
    parser.add_argument("--stats", action="store_true", help="현재 통계만 출력")
    args = parser.parse_args()

    db = get_db()

    if args.stats:
        print_stats(db)
        return

    # 처리 대상 선정
    if args.corp_code:
        rows = [{"corp_code": args.corp_code, "corp_name": ""}]
    elif args.priority == "leads":
        # story_leads 있는 회사 우선 + 미처리
        rows = db.execute("""
            SELECT DISTINCT s.corp_code, s.corp_name
            FROM story_leads s
            LEFT JOIN ir_contacts c ON s.corp_code = c.corp_code
            WHERE c.id IS NULL
            LIMIT ?
        """, [args.limit]).fetchall()
    else:
        rows = db.execute("""
            SELECT DISTINCT c.corp_code, c.corp_name
            FROM companies c
            LEFT JOIN ir_contacts ic ON c.corp_code = ic.corp_code
            WHERE ic.id IS NULL
            LIMIT ?
        """, [args.limit]).fetchall()

    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"IR 담당자 수집 시작 — 대상 {len(rows)}개사")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

    stats = {"found_real": 0, "guessed": 0, "no_email": 0, "no_homepage": 0,
             "skipped_verified": 0}
    for i, row in enumerate(rows, 1):
        cc = row["corp_code"]
        cn = row["corp_name"] or "(이름 없음)"

        try:
            r = process_company(db, cc, cn)
            stats[r["status"]] = stats.get(r["status"], 0) + 1

            if r["status"] == "found_real":
                print(f"  [{i:3d}/{len(rows):3d}] ✅ {cn[:20]:<20} 실제 발견 {r['count']}개")
                for em in r["emails"]:
                    print(f"           → {em}")
            elif r["status"] == "guessed":
                print(f"  [{i:3d}/{len(rows):3d}] 🟡 {cn[:20]:<20} 추정 {r['count']}개 (MX 통과)")
                for em in r["emails"]:
                    print(f"           ~ {em}")
            elif r["status"] == "no_email":
                print(f"  [{i:3d}/{len(rows):3d}] ⚪ {cn[:20]:<20} 도메인 무효 (홈페이지: {r.get('homepage','')[:40]})")
            elif r["status"] == "no_homepage":
                print(f"  [{i:3d}/{len(rows):3d}] ❌ {cn[:20]:<20} DART에 홈페이지 정보 없음")
            elif r["status"] == "skipped_verified":
                print(f"  [{i:3d}/{len(rows):3d}] ⏭ {cn[:20]:<20} 검증된 정보 있음 (skip)")
        except Exception as e:
            print(f"  [{i:3d}/{len(rows):3d}] ⚠ {cn[:20]:<20} ERR: {str(e)[:60]}")

        time.sleep(0.4)

    # 결과 요약
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"수집 완료")
    print(f"  ✅ 실제 발견 (HOMEPAGE)   : {stats.get('found_real',0):>4}개사")
    print(f"  🟡 도메인 추정 (GUESS+MX) : {stats.get('guessed',0):>4}개사")
    print(f"  ⚪ 도메인 무효            : {stats.get('no_email',0):>4}개사")
    print(f"  ❌ 홈페이지 정보 없음     : {stats.get('no_homepage',0):>4}개사")
    print(f"  ⏭ 이미 검증됨            : {stats.get('skipped_verified',0):>4}개사")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

    print_stats(db)


def print_stats(db):
    """전체 통계"""
    print("\n━━━━━━━━━━ 전체 통계 ━━━━━━━━━━")
    rows = db.execute("""
        SELECT confidence, COUNT(*) c FROM ir_contacts
        WHERE ir_email != '' GROUP BY confidence
    """).fetchall()
    for r in rows:
        print(f"  {r['confidence']:12s}: {r['c']:>4}건")
    n_companies = db.execute("SELECT COUNT(DISTINCT corp_code) FROM ir_contacts WHERE ir_email != ''").fetchone()[0]
    n_verified = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE user_verified=1").fetchone()[0]
    n_mx = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE mx_verified=1 AND ir_email != ''").fetchone()[0]
    print(f"\n  이메일 수집된 회사 수    : {n_companies}")
    print(f"  사용자 검증 완료         : {n_verified}건")
    print(f"  MX 레코드 통과           : {n_mx}건")


if __name__ == "__main__":
    main()
