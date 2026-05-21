"""Debug single corp: show all URLs tried, status, found emails, scores"""
import sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent))

from scrape_ir_emails import (
    candidate_urls_for, fetch_html, extract_emails,
    score_email, domain_of, normalize_url,
    find_ir_links_in_html, scrape_one_corp,
)

# 5개 회사 디버그
samples = [
    ("00126788", "(주)사조대림", "dr.sajo.co.kr", ""),
    ("롯데에너지머티리얼즈", "롯데에너지머티리얼즈", "www.lotteenergymaterials.com", ""),
    ("동성제약", "동성제약", "dongsung-pharm.co.kr", ""),
    ("(주)BYC", "(주)BYC", "home.byc.co.kr", ""),
    ("삼보산업", "삼보산업", "www.samboind.com", ""),
]

for cc, name, hm, ir in samples:
    print(f"\n{'='*70}\n{name} (hm={hm})")
    r = scrape_one_corp(cc, name, hm, ir, threshold=50)
    if r.get("ok"):
        b = r["best"]
        print(f"  ✅ FOUND sc={b['score']} email={b['email']}")
        print(f"     URL: {b['url']}")
    else:
        print(f"  ❌ {r.get('reason','?')}")
        if r.get("best_seen"):
            b = r["best_seen"]
            print(f"     (best_seen: sc={b['score']} email={b['email']} @ {b['url']})")
