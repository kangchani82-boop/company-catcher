"""
scripts/collect_news_for_questionnaires.py
──────────────────────────────────────────
Track A — 질문지 검토 보조용 뉴스 수집기.

목적:
  /questionnaire_review 페이지에서 각 질문지 옆에 "최근 뉴스" 사이드바를
  보여주기 위해, 미검토(pending/hold) 질문지의 회사들에 대해
  최근 30일 한국 뉴스를 수집해 external_sources 테이블에 적재한다.

사용자 워크플로:
  1) 본 스크립트 실행 → 회사별 뉴스 5~10건씩 DB에 누적
  2) /questionnaire_review 카드 옆에 사이드바로 노출
  3) 사용자가 뉴스 보고 → 이미 답이 있는 질문은 삭제, 새 질문 추가, 승인

특징:
  - story_leads 의존 X (기존 collect_kr_news.py 와 분리)
  - 회사명 단독 + (회사명 + 헤드라인 키워드 1개) 검색
  - T1/T2 매체만 저장 (collect_kr_news.py 의 화이트리스트 재사용)
  - 같은 corp_code 24h 내 재수집 스킵 (중복 호출 절약)

실행:
  python scripts/collect_news_for_questionnaires.py
  python scripts/collect_news_for_questionnaires.py --days 30 --limit 50
  python scripts/collect_news_for_questionnaires.py --status pending,hold
  python scripts/collect_news_for_questionnaires.py --corp-code 00126380
  python scripts/collect_news_for_questionnaires.py --force        # 24h 캐시 무시
"""
import os, re, sys, json, time, sqlite3, argparse
from pathlib import Path
from datetime import datetime, timedelta

# stdout/stderr UTF-8 wrap은 collect_kr_news 가 import 시점에 처리.
# 여기서 중복으로 감싸면 ValueError('I/O operation on closed file') 발생.
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

# 기존 모듈 재사용
from collect_kr_news import (
    DB_PATH, SCHEMA, get_conn, ensure_schema,
    load_whitelist, identify_outlet,
    search_naver, strip_html, parse_pubdate,
    NAVER_CLIENT_ID, NAVER_CLIENT_SECRET,
)


# 헤드라인 → 핵심 키워드 추출
# 한글 토큰화 후 끝에 붙는 조사/어미를 제거하면 의미 단위로 잘 추출됨.
STOPWORDS = {
    # 일반 명사 (의미 약함)
    "기자", "기업", "회사", "올해", "지난", "전년", "관련", "한편", "이번",
    "이후", "이상", "최근", "현재", "당기", "전기",
    # 동사·부사
    "통해", "위해", "대비", "대해", "있는", "없는", "있다", "없다",
    "기록", "발표", "공시",
    # 보고서 일반어
    "보고서", "분기보고서", "사업보고서", "재무제표",
    # 조사 단독으로 토큰화 안 되지만 어근 분리 후 들어올 수 있음
    "에서", "으로", "부터", "까지", "에게", "한테",
    # 짧은 의미어
    "대상", "관계", "비율", "수치", "수준",
    # 의존명사·접속어 (단독으로 토큰될 때 정보 없음)
    "대로", "처럼", "같이", "보다", "정도", "동안", "동시", "사이",
    "초과", "이하", "이상", "미만", "이내", "이후", "이전",
}

# 한국어 조사/어미 (어근 뒤에 붙는 것) — 토큰 끝에서 제거
KOR_PARTICLES = (
    # 길이 긴 것 우선 (그렇지 않으면 "에"가 "에서"보다 먼저 매칭됨)
    "으로부터", "으로서", "으로써", "에게서", "에서는", "에서도",
    "한테서", "에서", "에게", "한테", "보다", "처럼", "마저", "조차",
    "까지", "부터", "라고", "이라", "으로", "에는", "에도", "이나",
    "라도", "이야", "이고",
    "은", "는", "이", "가", "을", "를", "의", "도", "만", "와", "과",
    "로", "에", "야", "여",
)

def strip_particle(token: str) -> str:
    """토큰 끝에 붙은 조사 1회 제거.
    - 2자 이상 조사만 제거 (1자 조사는 회사명/단어 끝 글자와 충돌 → 부작용 큼).
    - 어근이 2자 이상 남아야 함.
    - 조사 목록은 길이 내림차순으로 매칭 (가장 긴 것 먼저).
    """
    for p in KOR_PARTICLES:
        if len(p) < 2:
            continue
        if token.endswith(p) and len(token) - len(p) >= 2:
            return token[: -len(p)]
    return token

def extract_keywords(headline: str, max_n: int = 3) -> list[str]:
    if not headline:
        return []
    # 한글/영문 토큰 (한글 2자+ / 영문 3자+)
    tokens = re.findall(r"[가-힣]{2,}|[A-Za-z]{3,}", headline)
    seen = []
    for t in tokens:
        if re.fullmatch(r"[가-힣]+", t):
            stripped = strip_particle(t)
            if stripped == t:
                # 어근 1자 + 2자 조사 ("억으로", "건에서", "장으로") 같은 경우
                # 어근이 너무 짧아 strip 거부됨 → 의미 약하므로 키워드 후보에서 제외
                if any(t.endswith(p) for p in KOR_PARTICLES if len(p) >= 2):
                    continue
            else:
                t = stripped
            if len(t) < 2:
                continue
        if t in STOPWORDS or t in seen:
            continue
        seen.append(t)
        if len(seen) >= max_n:
            break
    return seen


def already_collected_recently(conn, corp_code: str, hours: int = 24) -> bool:
    """24h 이내 fetched_at 이 있으면 스킵."""
    if not corp_code:
        return False
    cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    row = conn.execute("""
        SELECT 1 FROM external_sources
        WHERE related_corp_code=? AND fetched_at >= ?
        LIMIT 1
    """, [corp_code, cutoff]).fetchone()
    return bool(row)


def collect_for_qst(conn, qst, whitelist, days_window: int, verbose: bool) -> dict:
    """단일 질문지의 회사에 대해 뉴스 수집."""
    corp_name = (qst["corp_name"] or "").strip()
    corp_code = qst["corp_code"] or ""
    if not corp_name:
        return {"saved": 0, "skip": "no corp"}

    cn = corp_name.replace("(주)", "").replace("주식회사", "").strip()
    headline = qst["headline"] or ""
    kws = extract_keywords(headline, max_n=3)
    # 회사명/약어가 헤드라인 키워드로 다시 들어가는 것 제거
    kws = [k for k in kws if k != cn and k not in cn and cn not in k][:2]

    # 짧은 회사명(한글 2자 이하 또는 일반어 가능성)은 false-positive 위험 높음
    # → 회사명 단독 검색 제거, 반드시 키워드 동반 검색만 수행
    short_name = (
        len(re.findall(r"[가-힣]", cn)) <= 2 and
        not re.search(r"[A-Za-z]", cn)  # 영문이 섞여 있으면 식별성 ↑
    )
    queries = []
    if not short_name:
        queries.append(cn)
    for k in kws:
        queries.append(f"{cn} {k}")
    if not queries and cn:
        # 키워드도 없는 경우엔 어쩔 수 없이 회사명 단독
        queries.append(cn)
    queries = list(dict.fromkeys(queries))[:3]
    require_keyword_in_text = short_name  # 결과 검증 강화 플래그

    earliest = datetime.now() - timedelta(days=days_window)
    saved = 0
    seen_urls = set()

    for q in queries:
        if verbose:
            print(f"    🔎 '{q}'")
        items = search_naver(q, display=20, sort="date")
        time.sleep(0.15)

        for it in items:
            url = it.get("originallink") or it.get("link") or ""
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            outlet = identify_outlet(url, whitelist)
            if outlet["tier"] not in ("T1", "T2"):
                continue

            pub = parse_pubdate(it.get("pubDate", ""))
            if pub:
                try:
                    if datetime.strptime(pub, "%Y-%m-%d") < earliest:
                        continue
                except Exception:
                    pass

            title = strip_html(it.get("title", ""))
            summary = strip_html(it.get("description", ""))
            text = title + " " + summary
            # 본문/요약에 회사명 포함 검증
            if cn not in text:
                continue
            # 짧은 회사명: 헤드라인 키워드 중 1개 이상이 본문에 함께 등장해야 매칭
            if require_keyword_in_text and kws:
                if not any(k in text for k in kws):
                    continue

            conn.execute("""
                INSERT OR IGNORE INTO external_sources
                    (source_type, outlet_name, outlet_tier, outlet_weight,
                     title, summary, url, published_at,
                     related_corp_code, related_corp_name, related_keywords,
                     raw_meta, fetched_at)
                VALUES ('news', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            """, [
                outlet["name"], outlet["tier"], outlet.get("weight", 0.7),
                title, summary, url, pub,
                corp_code, corp_name,
                json.dumps(kws, ensure_ascii=False),
                json.dumps({"qst_id": qst["id"], "naver_pubdate": it.get("pubDate")},
                           ensure_ascii=False),
            ])
            if conn.total_changes:
                saved += 1
    conn.commit()
    return {"saved": saved}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="최근 N일 뉴스 (기본 30)")
    ap.add_argument("--limit", type=int, default=300, help="처리 질문지 수 한도")
    ap.add_argument("--status", type=str, default="pending,hold,approved",
                    help="대상 상태 (콤마구분, 기본 pending,hold,approved)")
    ap.add_argument("--corp-code", type=str, help="단일 corp_code 만 처리")
    ap.add_argument("--force", action="store_true", help="24h 캐시 무시 강제 재수집")
    args = ap.parse_args()

    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SECRET):
        print("[오류] .env 의 NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정")
        sys.exit(1)

    conn = get_conn()
    ensure_schema(conn)
    whitelist = load_whitelist()
    print(f"[info] 매체 화이트리스트: {len(whitelist)}개")

    # 대상 질문지 (corp_code 단위 dedup — 같은 회사 여러 질문지 → 1번만)
    statuses = [s.strip() for s in args.status.split(",") if s.strip()]
    placeholders = ",".join("?" * len(statuses))
    if args.corp_code:
        rows = conn.execute(f"""
            SELECT q.id, q.corp_code, q.corp_name, a.headline
            FROM ir_questionnaires q
            LEFT JOIN article_drafts a ON a.id = q.article_id
            WHERE q.corp_code=? AND q.status IN ({placeholders})
              AND q.id = (
                SELECT MAX(q2.id) FROM ir_questionnaires q2
                WHERE q2.corp_code = q.corp_code
                  AND q2.status IN ({placeholders})
              )
        """, [args.corp_code, *statuses, *statuses]).fetchall()
    else:
        # corp_code 별 1건 — 가장 최근 질문지 기준
        rows = conn.execute(f"""
            SELECT q.id, q.corp_code, q.corp_name, a.headline
            FROM ir_questionnaires q
            LEFT JOIN article_drafts a ON a.id = q.article_id
            WHERE q.status IN ({placeholders})
              AND q.corp_code IS NOT NULL AND q.corp_code != ''
              AND q.id = (
                SELECT MAX(q2.id) FROM ir_questionnaires q2
                WHERE q2.corp_code = q.corp_code
                  AND q2.status IN ({placeholders})
              )
            ORDER BY q.created_at DESC
            LIMIT ?
        """, [*statuses, *statuses, args.limit]).fetchall()

    if not rows:
        print("[info] 대상 질문지 없음")
        return

    print(f"[info] 대상 회사 {len(rows)}개 (status={','.join(statuses)})\n")

    total_saved = 0
    skipped = 0
    for i, qst in enumerate(rows, 1):
        cn = qst["corp_name"] or "(미상)"
        cc = qst["corp_code"] or ""
        print(f"[{i:3d}/{len(rows)}] {cn} ({cc})")

        if not args.force and already_collected_recently(conn, cc, hours=24):
            print(f"    ⏭ 24h 내 수집됨 — 스킵")
            skipped += 1
            continue

        try:
            r = collect_for_qst(conn, qst, whitelist,
                                days_window=args.days, verbose=True)
            print(f"    💾 저장 {r['saved']}건")
            total_saved += r["saved"]
        except Exception as e:
            print(f"    ⚠ 오류: {e}")
        time.sleep(0.3)

    print(f"\n=== 완료 ===")
    print(f"  처리 회사: {len(rows)} (스킵 {skipped})")
    print(f"  저장 뉴스: 총 {total_saved}건")


if __name__ == "__main__":
    main()
