"""
scripts/verify_articles.py
──────────────────────────
기사 초안 할루시네이션 검증 — 4차원 점수 산정

검증 차원:
  D1. 수치 정확성 (Numeric Accuracy)
      - 본문 모든 수치를 financials 테이블 실제 값과 대조
      - 억/조 단위 변환, ±5% 오차 허용
      - 대조 불가 / 오차 큼 / 일치 비율 산정

  D2. 방향 일치 (Direction Match)
      - "증가/감소/흑자/적자/급증/급감" 등 방향 키워드 검출
      - financials YoY 변화와 부합 여부 확인

  D3. Evidence 앵커링 (Evidence Anchoring)
      - 단서 evidence의 핵심 명사/구문을 본문이 다루는지
      - 단순 매칭(부분 문자열) + 토큰 자카드 유사도

  D4. 출처 그라운딩 (Source Grounding)
      - 본문의 고유명사·기업명·제품명을 ai_comparisons.result나 biz_content에서 검색
      - 미발견 명사 = 할루시네이션 후보

총점: 0~100점 (100=완벽)
플래그: HIGH / MEDIUM / LOW / SAFE

실행:
  python scripts/verify_articles.py --stats          # 검증 결과 분포만
  python scripts/verify_articles.py --limit 20       # 20건 검증
  python scripts/verify_articles.py --all            # 전체
  python scripts/verify_articles.py --id 230         # 특정 기사
  python scripts/verify_articles.py --report HIGH    # 위험 기사 리포트
"""

import io
import re
import sys
import sqlite3
import argparse
from pathlib import Path
from collections import Counter

# stdout wrap은 CLI 시에만 (import 시 부작용 방지)
def _ensure_utf8_io():
    try:
        if hasattr(sys.stdout, "buffer"):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "buffer"):
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


# ══════════════════════════════════════════════════════════════════════════════
# DB 스키마 — verification_results 테이블 (없으면 생성)
# ══════════════════════════════════════════════════════════════════════════════

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS article_verification (
            article_id          INTEGER PRIMARY KEY,
            score_total         INTEGER NOT NULL,
            score_numeric       INTEGER NOT NULL,
            score_direction     INTEGER NOT NULL,
            score_evidence      INTEGER NOT NULL,
            score_grounding     INTEGER NOT NULL,
            flag                TEXT NOT NULL,         -- HIGH / MEDIUM / LOW / SAFE
            numeric_details     TEXT,                  -- JSON
            direction_details   TEXT,
            evidence_details    TEXT,
            grounding_details   TEXT,
            verified_at         TEXT NOT NULL,
            FOREIGN KEY (article_id) REFERENCES article_drafts(id)
        );
        CREATE INDEX IF NOT EXISTS idx_av_flag ON article_verification(flag);
        CREATE INDEX IF NOT EXISTS idx_av_score ON article_verification(score_total);
    """)
    conn.commit()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# 유틸
# ══════════════════════════════════════════════════════════════════════════════

def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "")


# 한국어 숫자 단위 변환
UNITS = {
    "조": 1_000_000_000_000,
    "억": 100_000_000,
    "천만": 10_000_000,
    "백만": 1_000_000,
    "만": 10_000,
}


def parse_number(text_with_unit: str) -> float | None:
    """'1,234억', '5.6조', '2억 3,000만' 등을 원 단위로 변환."""
    s = text_with_unit.strip()
    s = s.replace(",", "").replace(" ", "")
    # 예: 1.5조2,000억 같은 복합 패턴은 패스 (단순 매칭만)
    m = re.match(r"([0-9.]+)(조|억|천만|백만|만)", s)
    if m:
        try:
            num = float(m.group(1))
            return num * UNITS[m.group(2)]
        except ValueError:
            return None
    # 단위 없는 순수 숫자
    if re.match(r"^[0-9.]+$", s):
        try:
            return float(s)
        except ValueError:
            return None
    return None


def extract_numbers_with_unit(text: str) -> list[tuple[str, float]]:
    """본문에서 (원본문자열, 숫자값) 튜플 리스트 추출.
    예: '2,509억 원', '6.5%', '13.8% 증가' 등
    """
    results = []
    # 한국어 단위 (조/억/만 등)
    pattern_kr = r"([0-9][0-9,]*\.?[0-9]*\s*(?:조|억|천만|백만|만))"
    for m in re.finditer(pattern_kr, text):
        raw = m.group(1).strip()
        v = parse_number(raw)
        if v is not None:
            results.append((raw, v))
    # 퍼센트
    pattern_pct = r"([+-]?[0-9.,]+)\s*%"
    for m in re.finditer(pattern_pct, text):
        try:
            v = float(m.group(1).replace(",", ""))
            results.append((m.group(0).strip(), v))
        except ValueError:
            continue
    return results


def korean_tokens(text: str) -> set[str]:
    """한국어 명사 후보 토큰 추출 (2글자 이상 연속 한글 + 영문 단어)."""
    text = text or ""
    kr = set(re.findall(r"[가-힣]{2,}", text))
    en = set(w.lower() for w in re.findall(r"[A-Za-z]{3,}", text))
    return kr | en


# ══════════════════════════════════════════════════════════════════════════════
# D1. 수치 정확성
# ══════════════════════════════════════════════════════════════════════════════

def verify_numeric(article_body: str, corp_code: str,
                   conn: sqlite3.Connection) -> tuple[int, dict]:
    """기사 본문의 수치를 financials 테이블 값과 대조.
    Returns: (score 0-100, details dict)
    """
    # 해당 기업의 financials (최근 3년)
    rows = conn.execute("""
        SELECT fiscal_year, revenue, operating_income, net_income, total_assets,
               total_liabilities, total_equity, debt_ratio, current_ratio,
               operating_margin, net_margin, roe, roa, cash, inventory
        FROM financials WHERE corp_code = ?
        ORDER BY fiscal_year DESC LIMIT 5
    """, [corp_code]).fetchall()

    if not rows:
        return 50, {"reason": "financials 데이터 없음 — 검증 불가",
                    "tested": 0, "matched": 0, "unmatched": []}

    # 실제 값 풀 (절대값과 비율 분리)
    truth_pool: list[float] = []
    for r in rows:
        for k in ("revenue", "operating_income", "net_income",
                  "total_assets", "total_liabilities", "total_equity",
                  "cash", "inventory"):
            v = r[k]
            if v is not None and v != 0:
                truth_pool.append(float(v))
        # 비율도 풀에 추가 (퍼센트는 별도 처리)
        for k in ("debt_ratio", "current_ratio", "operating_margin",
                  "net_margin", "roe", "roa"):
            v = r[k]
            if v is not None:
                truth_pool.append(float(v))

    # 본문 숫자 추출
    body_nums = extract_numbers_with_unit(article_body)
    if not body_nums:
        return 70, {"reason": "본문에 검증 가능한 수치 없음",
                    "tested": 0, "matched": 0, "unmatched": []}

    matched = []
    unmatched = []
    for raw, val in body_nums:
        # 단위 있는 큰 수: 절대값과 5% 오차 허용 매칭
        is_pct = "%" in raw
        candidates = []
        for tv in truth_pool:
            if is_pct:
                # 비율은 ±2%p 허용
                if abs(tv - val) <= 2.0:
                    candidates.append(tv)
            else:
                # 절대값은 ±5% 허용 (큰 수일 때만 의미 있음)
                if val > 1_000_000 and tv > 1_000_000:
                    if abs(tv - val) / max(tv, val) <= 0.05:
                        candidates.append(tv)
                elif val == tv:
                    candidates.append(tv)
        if candidates:
            matched.append({"text": raw, "val": val, "matched_to": candidates[0]})
        else:
            unmatched.append({"text": raw, "val": val})

    total = len(body_nums)
    matched_cnt = len(matched)
    score = int((matched_cnt / total) * 100) if total else 70

    return score, {
        "tested": total,
        "matched": matched_cnt,
        "unmatched": unmatched[:10],  # 상위 10개만
    }


# ══════════════════════════════════════════════════════════════════════════════
# D2. 방향 일치
# ══════════════════════════════════════════════════════════════════════════════

DIRECTION_UP = ["증가", "급증", "상승", "확대", "흑자", "개선", "성장", "호조", "회복"]
DIRECTION_DOWN = ["감소", "급감", "하락", "축소", "적자", "악화", "둔화", "부진", "추락", "곤두박질"]


def verify_direction(article_body: str, corp_code: str,
                     conn: sqlite3.Connection) -> tuple[int, dict]:
    """본문의 방향성과 financials YoY 변화 일치 여부."""
    rows = conn.execute("""
        SELECT fiscal_year, revenue, operating_income, net_income
        FROM financials WHERE corp_code=? ORDER BY fiscal_year DESC LIMIT 2
    """, [corp_code]).fetchall()

    if len(rows) < 2:
        return 70, {"reason": "YoY 비교용 데이터 부족"}

    cur, prev = rows[0], rows[1]
    actual = {}
    for k in ("revenue", "operating_income", "net_income"):
        if cur[k] is not None and prev[k] is not None:
            if prev[k] != 0:
                yoy = (cur[k] - prev[k]) / abs(prev[k])
                actual[k] = "up" if yoy > 0.02 else ("down" if yoy < -0.02 else "flat")
            else:
                actual[k] = "up" if cur[k] > 0 else ("down" if cur[k] < 0 else "flat")

    # 본문 방향 키워드 카운트
    up_count = sum(article_body.count(w) for w in DIRECTION_UP)
    down_count = sum(article_body.count(w) for w in DIRECTION_DOWN)
    body_dir = "up" if up_count > down_count * 1.3 else (
               "down" if down_count > up_count * 1.3 else "mixed")

    # 영업이익 방향이 가장 중요 → 우선 매칭
    main_actual = actual.get("operating_income") or actual.get("net_income") or actual.get("revenue")
    if main_actual is None:
        return 70, {"reason": "actual 방향 미확정", "body_dir": body_dir}

    if body_dir == "mixed":
        score = 75   # 양면 보도 — 중립적으로 좋음
    elif body_dir == main_actual:
        score = 100
    else:
        score = 30   # 방향 불일치 — 위험

    return score, {
        "body_dir": body_dir,
        "actual_dir": main_actual,
        "actual_yoy": actual,
        "up_keywords": up_count,
        "down_keywords": down_count,
    }


# ══════════════════════════════════════════════════════════════════════════════
# D3. Evidence 앵커링
# ══════════════════════════════════════════════════════════════════════════════

def verify_evidence(article_body: str, evidence: str) -> tuple[int, dict]:
    """기사가 단서 evidence의 핵심 키워드를 다루는지."""
    if not evidence or len(evidence) < 30:
        return 70, {"reason": "evidence 너무 짧음"}

    ev_tokens = korean_tokens(strip_html(evidence))
    body_tokens = korean_tokens(article_body)

    # evidence에서 일반 단어 제외 (stop-words 류)
    stops = {"있다", "있음", "있는", "있어", "있으며",
             "회사", "당사", "기업", "사업", "보고서", "공시",
             "기준", "비교", "변화", "분석", "내용", "관련"}
    ev_unique = ev_tokens - stops
    if not ev_unique:
        return 70, {"reason": "evidence 핵심 토큰 없음"}

    overlap = ev_unique & body_tokens
    coverage = len(overlap) / len(ev_unique)
    score = int(min(coverage * 200, 100))   # 50% 이상이면 100점

    return score, {
        "evidence_tokens": len(ev_unique),
        "matched_tokens": len(overlap),
        "coverage_pct": round(coverage * 100, 1),
        "missing_examples": list(ev_unique - body_tokens)[:10],
    }


# ══════════════════════════════════════════════════════════════════════════════
# D4. 출처 그라운딩
# ══════════════════════════════════════════════════════════════════════════════

def verify_grounding(article_body: str, ai_result: str,
                     biz_content: str) -> tuple[int, dict]:
    """본문 고유 명사가 AI 분석/원문에서 검증되는지."""
    body_tokens = korean_tokens(article_body)
    # 너무 일반적인 단어 / 공통 단어 제외
    stops = {"있다", "있음", "있는", "있어", "있으며", "있으나",
             "회사", "당사", "기업", "사업", "보고서", "공시",
             "기준", "비교", "변화", "분석", "내용", "관련",
             "전년", "올해", "지난해", "2024", "2025", "2023",
             "개선", "감소", "증가", "확대", "축소", "기록",
             "보였다", "나타냈다", "보인다", "이어졌다",
             "하락", "상승", "성장", "부진", "악화", "호조",
             "주목", "분석", "전망", "예상", "추정", "추산",
             "수익성", "재무", "매출", "영업", "이익", "손실"}

    candidates = body_tokens - stops
    # 영문/3글자+ 한글만 의미 있음
    candidates = {t for t in candidates if len(t) >= 3}

    # 고유명사 후보 (대문자 영문, 4글자+ 한글)
    proper_nouns = {t for t in candidates if (re.match(r"[A-Z]", t) or len(t) >= 4)}

    if not proper_nouns:
        return 80, {"reason": "검증 대상 고유명사 없음"}

    source_text = (ai_result or "") + " " + (biz_content or "")[:30000]
    source_tokens = korean_tokens(source_text)

    found = proper_nouns & source_tokens
    missing = proper_nouns - source_tokens

    coverage = len(found) / len(proper_nouns)
    score = int(min(coverage * 120, 100))   # 80%만 발견되어도 100점

    return score, {
        "candidate_count": len(proper_nouns),
        "found": len(found),
        "missing_count": len(missing),
        "missing_examples": sorted(missing)[:15],
    }


# ══════════════════════════════════════════════════════════════════════════════
# 종합 점수
# ══════════════════════════════════════════════════════════════════════════════

def flag_from_score(score: int) -> str:
    if score >= 80:
        return "SAFE"
    if score >= 65:
        return "LOW"
    if score >= 50:
        return "MEDIUM"
    return "HIGH"


def verify_one(article: sqlite3.Row, conn: sqlite3.Connection) -> dict:
    """단일 기사 검증 → score dict."""
    body = strip_html(article["content"] or "")
    corp_code = article["corp_code"] or ""

    # 단서 정보
    lead = conn.execute("""
        SELECT sl.evidence, sl.summary, ac.result as ai_result
        FROM story_leads sl
        LEFT JOIN ai_comparisons ac ON sl.comparison_id = ac.id
        WHERE sl.id = ?
    """, [article["lead_id"]]).fetchone()

    evidence = strip_html((lead["evidence"] if lead else "") or "")
    ai_result = strip_html((lead["ai_result"] if lead else "") or "")

    # biz_content (해당 기업 최신 보고서)
    biz_row = conn.execute("""
        SELECT biz_content FROM reports
        WHERE corp_code=? AND biz_content IS NOT NULL
        ORDER BY rcept_dt DESC LIMIT 1
    """, [corp_code]).fetchone()
    biz_content = (biz_row["biz_content"] if biz_row else "") or ""

    # 4차원 검증
    s1, d1 = verify_numeric(body, corp_code, conn)
    s2, d2 = verify_direction(body, corp_code, conn)
    s3, d3 = verify_evidence(body, evidence)
    s4, d4 = verify_grounding(body, ai_result, biz_content)

    # 가중 평균: 수치 30% / 방향 25% / evidence 25% / 그라운딩 20%
    total = int(s1 * 0.30 + s2 * 0.25 + s3 * 0.25 + s4 * 0.20)
    return {
        "article_id": article["id"],
        "score_total": total,
        "score_numeric": s1,
        "score_direction": s2,
        "score_evidence": s3,
        "score_grounding": s4,
        "flag": flag_from_score(total),
        "numeric_details": d1,
        "direction_details": d2,
        "evidence_details": d3,
        "grounding_details": d4,
    }


def save_verification(conn: sqlite3.Connection, result: dict) -> None:
    import json
    conn.execute("""
        INSERT OR REPLACE INTO article_verification
        (article_id, score_total, score_numeric, score_direction,
         score_evidence, score_grounding, flag,
         numeric_details, direction_details, evidence_details,
         grounding_details, verified_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
    """, [
        result["article_id"], result["score_total"],
        result["score_numeric"], result["score_direction"],
        result["score_evidence"], result["score_grounding"],
        result["flag"],
        json.dumps(result["numeric_details"], ensure_ascii=False),
        json.dumps(result["direction_details"], ensure_ascii=False),
        json.dumps(result["evidence_details"], ensure_ascii=False),
        json.dumps(result["grounding_details"], ensure_ascii=False),
    ])
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# 통계 & 리포트
# ══════════════════════════════════════════════════════════════════════════════

def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 65)
    print("  기사 검증 결과 분포")
    print("=" * 65)
    rows = conn.execute("""
        SELECT flag, COUNT(*) cnt, AVG(score_total) avg_total,
               AVG(score_numeric) sn, AVG(score_direction) sd,
               AVG(score_evidence) se, AVG(score_grounding) sg
        FROM article_verification GROUP BY flag
        ORDER BY CASE flag WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1
                            WHEN 'LOW' THEN 2 ELSE 3 END
    """).fetchall()
    if not rows:
        print("  (검증 결과 없음 — --all 또는 --limit으로 실행하세요)")
        return
    for r in rows:
        icon = {"HIGH":"🔴","MEDIUM":"🟡","LOW":"🟠","SAFE":"🟢"}.get(r["flag"],"⚪")
        print(f"\n  {icon} {r['flag']:<7} {r['cnt']:>4}건 | 평균 {r['avg_total']:.0f}점")
        print(f"     수치 {r['sn']:.0f} / 방향 {r['sd']:.0f} / "
              f"evidence {r['se']:.0f} / 그라운딩 {r['sg']:.0f}")

    total_cnt = conn.execute("SELECT COUNT(*) FROM article_verification").fetchone()[0]
    total_articles = conn.execute("SELECT COUNT(*) FROM article_drafts").fetchone()[0]
    print(f"\n  검증 완료: {total_cnt}/{total_articles}건")


def print_report(conn: sqlite3.Connection, flag: str) -> None:
    """특정 flag 기사 상세 리포트."""
    print(f"\n=== {flag} 등급 기사 상세 ===\n")
    rows = conn.execute("""
        SELECT av.*, ad.corp_name, ad.headline, ad.char_count
        FROM article_verification av
        JOIN article_drafts ad ON av.article_id = ad.id
        WHERE av.flag = ?
        ORDER BY av.score_total ASC LIMIT 30
    """, [flag]).fetchall()
    import json
    for r in rows:
        print(f"[#{r['article_id']:>3}] {r['corp_name']:<20} | 총점 {r['score_total']:>3} | {r['headline'][:35]}")
        print(f"     수치 {r['score_numeric']:>3} / 방향 {r['score_direction']:>3} / "
              f"evidence {r['score_evidence']:>3} / 그라운딩 {r['score_grounding']:>3}")
        # 가장 낮은 차원의 details 표시
        scores = [
            ("수치", r["score_numeric"], r["numeric_details"]),
            ("방향", r["score_direction"], r["direction_details"]),
            ("evidence", r["score_evidence"], r["evidence_details"]),
            ("그라운딩", r["score_grounding"], r["grounding_details"]),
        ]
        worst = min(scores, key=lambda x: x[1])
        try:
            d = json.loads(worst[2])
            print(f"     ↓ 가장 약한 차원: {worst[0]} ({worst[1]}점) - {str(d)[:120]}")
        except Exception:
            pass
        print()


# ══════════════════════════════════════════════════════════════════════════════
# 진입점
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="기사 할루시네이션 검증")
    p.add_argument("--stats", action="store_true", help="결과 분포만 출력")
    p.add_argument("--all", action="store_true", help="모든 기사 검증")
    p.add_argument("--limit", type=int, default=20, help="검증 최대 건수")
    p.add_argument("--id", type=int, help="특정 article_id만")
    p.add_argument("--report", choices=["HIGH", "MEDIUM", "LOW", "SAFE"],
                   help="특정 flag 상세 리포트")
    p.add_argument("--force", action="store_true", help="이미 검증된 것도 재검증")
    args = p.parse_args()

    conn = get_conn()
    ensure_schema(conn)

    if args.stats and not args.all and not args.id and not args.report:
        print_stats(conn)
        return

    if args.report:
        print_report(conn, args.report)
        return

    # 검증 대상 선정
    if args.id:
        articles = conn.execute(
            "SELECT * FROM article_drafts WHERE id=?", [args.id]
        ).fetchall()
    else:
        if args.force:
            q = "SELECT * FROM article_drafts WHERE char_count >= 100 ORDER BY id"
        else:
            q = """SELECT ad.* FROM article_drafts ad
                   WHERE ad.char_count >= 100
                     AND NOT EXISTS (SELECT 1 FROM article_verification av WHERE av.article_id = ad.id)
                   ORDER BY ad.id"""
        if args.all:
            articles = conn.execute(q).fetchall()
        else:
            articles = conn.execute(q + f" LIMIT {args.limit}").fetchall()

    if not articles:
        print("[info] 검증할 기사 없음.")
        print_stats(conn)
        return

    print(f"[info] 검증 대상: {len(articles)}건")
    print()

    flag_counter = Counter()
    for i, a in enumerate(articles, 1):
        result = verify_one(a, conn)
        save_verification(conn, result)
        flag_counter[result["flag"]] += 1
        icon = {"HIGH":"🔴","MEDIUM":"🟡","LOW":"🟠","SAFE":"🟢"}[result["flag"]]
        print(f"  [{i:3d}/{len(articles)}] #{a['id']:>3} {a['corp_name'][:18]:<18} "
              f"→ {icon} {result['flag']} ({result['score_total']}점) "
              f"[수{result['score_numeric']}/방{result['score_direction']}"
              f"/단{result['score_evidence']}/그{result['score_grounding']}]")

    print(f"\n[완료] {len(articles)}건 검증")
    print(f"  🔴 HIGH:   {flag_counter['HIGH']}건")
    print(f"  🟡 MEDIUM: {flag_counter['MEDIUM']}건")
    print(f"  🟠 LOW:    {flag_counter['LOW']}건")
    print(f"  🟢 SAFE:   {flag_counter['SAFE']}건")
    print()
    print_stats(conn)

    conn.close()


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
