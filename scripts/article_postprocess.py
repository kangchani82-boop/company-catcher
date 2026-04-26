"""
scripts/article_postprocess.py
───────────────────────────────
기사 후처리 + Self-Check 모듈 (v2 프롬프트 후크)

기능:
  1. headline_lead_consistency_check() — 헤드라인-리드 합일치 검증
  2. detect_unsourced_speculation() — 출처 없는 추측 표현 검출
  3. enforce_citation_format() — 인용 표기 정규화
  4. flag_external_knowledge() — 입력 자료 외 명사 탐지

generate_draft.py에서 save_draft 직전에 호출.

표준 검증 인터페이스:
  result = postprocess_article(article_dict, lead, fin_block, ai_result, conn)
  result['warnings']: list[str]
  result['rewritten']: dict (수정된 article)
  result['quality_score']: int (0-100)
"""

import io
import re
import sys
from typing import Optional

def _ensure_utf8_io():
    try:
        if hasattr(sys.stdout, "buffer"):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "buffer"):
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# 1. 헤드라인-리드 합일치
# ══════════════════════════════════════════════════════════════════════════════

# 의미 없는 토큰 (검증 시 제외)
STOP_TOKENS = {
    # 동사·서술어
    "있다", "있음", "있는", "있어", "있으며", "있으나",
    "기록", "보였다", "나타냈다", "보인다", "이뤘다",
    # 일반 명사
    "회사", "당사", "기업", "사업", "보고서", "공시", "기준",
    "변화", "분석", "내용", "관련", "전년", "올해", "지난해",
    "주목", "주식회사", "전망", "예상", "추정",
    # 일반 경제 용어
    "매출", "매출액", "영업이익", "순이익", "당기순이익",
    "총자산", "총부채", "자본총계", "영업이익률", "순이익률",
    "부채비율", "유동비율", "현금성자산", "수익성", "성장성",
    "안정성", "효율성", "건전성", "단기적", "장기적", "중장기",
    "단기", "중기", "전년동기", "전기대비", "전년대비",
    "동기", "당기", "전기", "추가", "감소", "증가",
    "확대", "축소", "개선", "악화", "심화", "둔화", "호조",
    "부진", "회복", "성장", "감안", "고려", "포함", "비중",
    "기록", "유지", "달성", "확보", "기여", "강화", "우려",
    # 분석 용어
    "ROE", "roe", "ROA", "roa", "EPS", "eps", "PER", "per",
    "PBR", "pbr", "EBITDA", "ebitda", "OPM", "opm",
    "YoY", "yoy", "QoQ", "qoq",
    # 일반 개념
    "포트폴리오", "다각화", "리스크", "포지션", "구조",
    "전략", "방향성", "지속", "기여", "확장", "전환",
    "필요성", "가능성", "효과",
    # 기업/시장 일반
    "한국", "중국", "일본", "미국", "유럽", "아시아",
    "글로벌", "국내", "해외", "국내외", "세계", "전세계",
    "수출", "내수", "시장",
    # 보고서 명칭
    "사업보고서", "분기보고서", "반기보고서", "연결재무제표",
    "별도재무제표", "DART",
}


def korean_tokens(text: str, min_len: int = 2) -> set[str]:
    """한글 명사·영문 단어 토큰 집합 (substring 매칭 가능하도록 원본도 보존)."""
    if not text:
        return set()
    kr = set(re.findall(rf"[가-힣]{{{min_len},}}", text))
    en = set(w.lower() for w in re.findall(r"[A-Za-z]{3,}", text))
    return (kr | en) - STOP_TOKENS


def _substring_match(needle: str, haystack: str) -> bool:
    """needle이 haystack 안에 부분 문자열로 등장하는지."""
    return needle in haystack


def headline_lead_consistency(headline: str, body: str) -> dict:
    """
    헤드라인 핵심 토큰이 본문 1~2단락에 등장하는지 검증.
    부분 문자열(substring) 매칭으로 '온실가스' ⊂ '온실가스배출권' 같이 처리.
    """
    if not body:
        return {"match_score": 0, "missing": [], "pass": False}
    paragraphs = body.split("\n\n")
    lead = "\n\n".join(paragraphs[:2])
    full_body = body  # 전체 본문도 검색
    h_tokens = korean_tokens(headline, min_len=2)
    if not h_tokens:
        return {"match_score": 70, "missing": [], "pass": True,
                "reason": "헤드라인 토큰 없음"}

    matched = set()
    missing = set()
    for tok in h_tokens:
        # 1차: 리드(1~2단락)에서 부분 문자열 매칭
        if _substring_match(tok, lead):
            matched.add(tok)
            continue
        # 2차: 본문 전체에서 매칭 (리드엔 없지만 본문엔 있음 = 부분 통과)
        if _substring_match(tok, full_body):
            matched.add(tok + "(본문)")
            continue
        missing.add(tok)

    score = int(len(matched) / len(h_tokens) * 100)
    return {
        "match_score": score,
        "matched": sorted(matched),
        "missing": sorted(missing),
        "pass": score >= 60,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 2. 추측 표현 검출
# ══════════════════════════════════════════════════════════════════════════════

# 추측 표현 패턴 (출처 없으면 위험)
SPECULATION_PATTERNS = [
    r"(전망된다|예상된다|추정된다|예측된다|기대된다)",
    r"(우려된다|우려가 (있|커))",
    r"(전망이다|예상이다|추정이다)",
    r"(가능성이 (있|크|높))",
    r"(보일 것으로|것으로 보인다|것으로 분석)",
    r"(이뤄질 것|이루어질 것|될 것으로)",
]

# 출처 인용 패턴 — 광범위 인식 (괄호 안에 출처 키워드 포함)
CITATION_NEARBY = re.compile(
    r"\([^)]*(?:"
    # 1차 자료 (DART)
    r"DART|dart|financials|YoY|yoy|"
    r"AI\s*비교분석|AI\s*분석|비교분석|발췌|인용|기준|"
    # 보고서 명칭
    r"사업보고서|분기보고서|반기보고서|정기공시|"
    r"보도자료|IR\s*자료|IR자료|공시자료|공시|"
    # 신용평가사
    r"한국기업평가|NICE신용평가|한국신용평가|"
    r"S&P|Moody'?s?|Fitch|신용평가|신용등급|"
    # 정부/규제기관
    r"통계청|한국은행|금감원|금융감독원|"
    r"산업부|기재부|관세청|공정위|국토부|"
    # 글로벌 기관
    r"OECD|IMF|IEA|World\s*Bank|UN|WHO|"
    # 리서치
    r"Gartner|McKinsey|Forrester|IDC|"
    # 협회·조합
    r"[가-힣]+협회|[가-힣]+조합|[가-힣]+학회|"
    # 날짜 형식
    r"\d{4}\s*사업|\d{4}\s*분기|\d{4}\s*반기|"
    r"\d{4}[-\s]Q[1-4]|\d{4}[-\s]\d{2}[-\s]?\d{0,2}|\d{4}년"
    r")[^)]*\)"
)

# 출처 인용 추가 패턴 — 본문 인라인 (괄호 없이 등장)
INLINE_CITATION = re.compile(
    r"(?:"
    r"\d{4}년?\s*(?:사업|분기|반기)보고서(?:에\s*따르면|기준)?|"
    r"DART\s*(?:공시|기준)|"
    r"(?:한국기업평가|NICE신용평가|S&P|Moody'?s?)\s*(?:평가|등급|분석)|"
    r"(?:통계청|한국은행|금감원|금융감독원|산업부|OECD|IMF|IEA)\s*(?:발표|통계|보고서|기준)?|"
    r"신용등급|신용평가"
    r")"
)


def _has_citation_near(text: str, position: int, window: int = 100) -> bool:
    """주어진 위치에서 ±window 범위 안에 출처 인용이 있는지."""
    start = max(0, position - window)
    end = min(len(text), position + window)
    context = text[start:end]
    return bool(CITATION_NEARBY.search(context) or INLINE_CITATION.search(context))


def detect_unsourced_speculation(body: str) -> list[dict]:
    """
    출처 없는 추측 표현 검출.
    각 추측 표현 주변 100자 안에 인용 표기가 없으면 경고.
    """
    warnings = []
    for pat in SPECULATION_PATTERNS:
        for m in re.finditer(pat, body):
            if not _has_citation_near(body, m.start(), window=100):
                warnings.append({
                    "phrase": m.group(0),
                    "position": m.start(),
                    "context": body[max(0,m.start()-50):min(len(body),m.end()+50)].strip(),
                })
    return warnings


# ══════════════════════════════════════════════════════════════════════════════
# 3. 인용 표기 정규화 / 누락 검출
# ══════════════════════════════════════════════════════════════════════════════

# 수치 패턴 (단위 포함)
NUMBER_PATTERN = re.compile(
    r"[0-9][0-9,]*\.?[0-9]*\s*(?:조|억|천만|백만|만|%|%p)"
)


def detect_unsourced_numbers(body: str) -> list[dict]:
    """
    수치 인근 100자 안에 인용 표기 없으면 경고.
    """
    warnings = []
    for m in NUMBER_PATTERN.finditer(body):
        if not _has_citation_near(body, m.start(), window=100):
            warnings.append({
                "number": m.group(0),
                "position": m.start(),
                "context": body[max(0,m.start()-50):min(len(body),m.end()+50)].strip(),
            })
    return warnings


# ══════════════════════════════════════════════════════════════════════════════
# 4. 외부 지식(입력 자료에 없는 명사) 검출
# ══════════════════════════════════════════════════════════════════════════════

def detect_external_knowledge(body: str, sources: dict[str, str]) -> dict:
    """
    body의 고유명사 후보가 sources(입력 자료 합본) 안에 부분 문자열로
    등장하는지 확인. 단순 토큰 매칭이 아닌 substring 검색.
    """
    body_tokens = korean_tokens(body, min_len=4)
    en_proper = set(re.findall(r"\b[A-Z][a-zA-Z]{2,}\b", body))
    candidates = body_tokens | {w.lower() for w in en_proper}

    # 일반 경제·시간·동작 단어 추가 제외 (substring 검사로도 못 찾을 수 있음)
    extra_exclude = {
        "확대", "감소세", "증가세", "유지", "유지하고", "확보하기",
        "기여하고", "분석된다", "전망된다", "해석된다", "면치", "달성했다",
        "기록했다", "안정화", "강화하기", "다각화", "확장",
        "장기적인", "단기적인", "중장기적", "긍정적인", "부정적인",
        "공식화했다", "공식화했으며", "실시했다", "공고화", "통합했다",
        "지속적인", "전반적인", "구조적인",
    }
    candidates -= extra_exclude

    # 검증 출처 (전체 합본)
    source_text = " ".join(v for v in sources.values() if v)

    # 부분 문자열(substring) 매칭으로 검사
    found = set()
    missing = set()
    for tok in candidates:
        if not tok or len(tok) < 3:
            continue
        if tok in source_text or tok.lower() in source_text.lower():
            found.add(tok)
        else:
            missing.add(tok)

    if not candidates:
        coverage = 100
    else:
        coverage = int(len(found) / max(1, len(found) + len(missing)) * 100)

    return {
        "candidate_count": len(found) + len(missing),
        "found_count": len(found),
        "missing_count": len(missing),
        "coverage_pct": coverage,
        "missing_examples": sorted(missing)[:15],
    }


# ══════════════════════════════════════════════════════════════════════════════
# 5. 종합 후처리
# ══════════════════════════════════════════════════════════════════════════════

def postprocess_article(article: dict, lead: dict | None,
                        fin_block: str, ai_result: str,
                        biz_content: str = "",
                        sc_context: str = "") -> dict:
    """
    기사 dict를 받아 검증·후처리.
    Returns: {
        "warnings": [{type, ...}],
        "quality_score": 0-100,
        "headline_check": {...},
        "speculation": [...],
        "unsourced_numbers": [...],
        "external_knowledge": {...},
        "rewritten": dict (현재는 원본 그대로 반환, 향후 자동수정 추가)
    }
    """
    headline = (article.get("headline") or "").strip()
    body = (article.get("body") or "").strip()

    if not body:
        return {
            "warnings": [{"type": "EMPTY_BODY", "msg": "본문 없음"}],
            "quality_score": 0,
            "rewritten": article,
        }

    sources = {
        "evidence": (lead.get("evidence") if lead else "") or "",
        "summary":  (lead.get("summary")  if lead else "") or "",
        "ai_result": ai_result or "",
        "fin_block": fin_block or "",
        "biz_content": biz_content or "",
        "sc_context": sc_context or "",
    }

    warnings = []

    # 1) 헤드라인-리드 합일치
    h_check = headline_lead_consistency(headline, body)
    if not h_check["pass"]:
        warnings.append({
            "type": "HEADLINE_LEAD_MISMATCH",
            "msg": f"헤드라인-리드 일치율 {h_check['match_score']}% — 본문에 없는 키워드: {h_check['missing'][:5]}",
        })

    # 2) 출처 없는 추측
    spec = detect_unsourced_speculation(body)
    if spec:
        warnings.append({
            "type": "UNSOURCED_SPECULATION",
            "msg": f"근거 없는 추측 {len(spec)}건",
            "samples": [s["phrase"] for s in spec[:3]],
        })

    # 3) 출처 없는 수치
    nums = detect_unsourced_numbers(body)
    # 수치는 흔히 누락되므로 5건 이상일 때만 경고
    if len(nums) >= 5:
        warnings.append({
            "type": "UNSOURCED_NUMBERS",
            "msg": f"출처 없는 수치 {len(nums)}건 (5건 이상)",
            "samples": [n["number"] for n in nums[:5]],
        })

    # 4) 외부 지식 검출
    ek = detect_external_knowledge(body, sources)
    if ek["coverage_pct"] < 50 and ek["candidate_count"] >= 5:
        warnings.append({
            "type": "EXTERNAL_KNOWLEDGE",
            "msg": f"입력 자료 외 명사 {ek['missing_count']}건 (커버 {ek['coverage_pct']}%)",
            "samples": ek["missing_examples"][:5],
        })

    # 종합 점수 (감점 방식)
    score = 100
    score -= max(0, 100 - h_check["match_score"]) // 2   # 헤드라인 감점
    score -= len(spec) * 8                                # 추측 표현 감점
    if len(nums) >= 5:
        score -= (len(nums) - 4) * 3                      # 수치 출처 감점
    score -= max(0, (60 - ek["coverage_pct"]) // 2)       # 외부 지식 감점
    score = max(0, min(100, score))

    return {
        "warnings": warnings,
        "quality_score": score,
        "headline_check": h_check,
        "speculation": spec[:5],
        "unsourced_numbers": nums[:5],
        "external_knowledge": ek,
        "rewritten": article,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CLI 테스트
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    _ensure_utf8_io()
    import argparse, sqlite3, json
    from pathlib import Path

    ROOT = Path(__file__).parent.parent
    DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

    p = argparse.ArgumentParser(description="기사 후처리·검증 (CLI)")
    p.add_argument("--id", type=int, help="article_drafts.id")
    args = p.parse_args()

    if not args.id:
        print("usage: python scripts/article_postprocess.py --id <article_id>")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    a = conn.execute("SELECT * FROM article_drafts WHERE id=?", [args.id]).fetchone()
    if not a:
        print("article 없음")
        sys.exit(1)

    lead = conn.execute("""
        SELECT sl.*, ac.result AS ai_result
        FROM story_leads sl
        LEFT JOIN ai_comparisons ac ON sl.comparison_id = ac.id
        WHERE sl.id = ?
    """, [a["lead_id"]]).fetchone()

    biz_row = conn.execute("""
        SELECT biz_content FROM reports
        WHERE corp_code=? AND biz_content IS NOT NULL
        ORDER BY rcept_dt DESC LIMIT 1
    """, [a["corp_code"]]).fetchone()

    article_dict = {"headline": a["headline"], "body": a["content"]}
    lead_dict = dict(lead) if lead else None
    result = postprocess_article(
        article_dict, lead_dict,
        fin_block="",   # generate_draft가 만든 fin_block은 별도 보관 필요
        ai_result=(lead["ai_result"] if lead else "") or "",
        biz_content=(biz_row["biz_content"] if biz_row else "") or "",
    )
    print(f"기사 #{a['id']}: {a['corp_name']}")
    print(f"헤드라인: {a['headline']}")
    print()
    print(f"품질 점수: {result['quality_score']}점")
    print(f"헤드라인 합일치: {result['headline_check']['match_score']}%")
    print(f"외부 지식 커버: {result['external_knowledge']['coverage_pct']}%")
    print()
    if result["warnings"]:
        print("[경고]")
        for w in result["warnings"]:
            print(f"  • {w['type']}: {w['msg']}")
            if w.get("samples"):
                print(f"    예: {w['samples']}")
    else:
        print("[경고 없음]")

    conn.close()
