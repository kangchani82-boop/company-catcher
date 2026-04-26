"""
scripts/clean_supply_chain.py
─────────────────────────────
공급망 데이터 품질 검증 및 노이즈 제거 (1차 + 2차 통합)

실행:
  python scripts/clean_supply_chain.py --dry-run   # 삭제 예상 건수만 확인
  python scripts/clean_supply_chain.py              # 실제 정리 실행
"""

import io
import sqlite3
import sys
import argparse
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


# ══════════════════════════════════════════════════════════════════
#  1차 규칙: corp_name 기준 (공급망 없는 업종)
# ══════════════════════════════════════════════════════════════════

FINANCE_KEYWORDS = [
    '%보험%', '%스팩%', '%기업인수목적%', '%증권%', '%은행%', '%캐피탈%',
    '%지주%', '%홀딩스%', '%카드%', '%리츠%', '%자산운용%', '%투자신탁%',
    '%투자금융%', '%인베스트먼트%', '%벤처스%', '%신탁%',
]


# ══════════════════════════════════════════════════════════════════
#  2차 규칙: partner_name 기준 (비기업 노이즈)
# ══════════════════════════════════════════════════════════════════

# ── A. 목차/섹션 제목 시작 패턴 (가나다 + 숫자 목차) ─────────────
# 예: "가. 주요 원재료 등의 현황", "1) 생산능력", "나. 생산 및 설비"
TOC_PREFIX_LIKE = [
    "가. %", "나. %", "다. %", "라. %", "마. %", "바. %", "사. %",
    "1) %", "2) %", "3) %", "4) %", "5) %", "6) %", "7) %",
    "1. %", "2. %", "3. %", "4. %", "5. %", "6. %",
    "■%", "○%", "●%", "▶%", "※%", "☞%", "→%",
    "- %",
]

# ── B. 섹션 제목 핵심 키워드 (partner_name 내 포함) ───────────────
# 공시 보고서 목차 구조에서 자주 나타나는 섹션 이름들
SECTION_TITLE_LIKE = [
    # 생산·설비 관련
    "%생산능력%", "%생산실적%", "%생산설비%",
    "%생산 및 설비%", "%설비에 관한 사항%", "%설비의 현황%",
    "%가동률%",
    # 원재료 관련
    "%주요 원재료%", "%원재료 가격%", "%가격변동추이%", "%가격변동 추이%",
    "%원재료 현황%", "%매입 현황%", "%매입현황%", "%주요 매입처%",
    "%원재료 매입%",
    # 사업 구조 관련
    "%영업개황%", "%사업부문의 구분%", "%공시대상 사업%", "%사업부문 구분%",
    "%신규사업 등의 내용%", "%신규 사업에 관한 사항%",
    "%회사의 현황%", "%수주상황%", "%수주 현황%",
    "% 사업부문", "%사업부문)", "%사업부문의%", "%사업부문은%",
    # 시장·경쟁 분석 섹션
    "%경기변동의 특성%", "%산업의 특성%", "%산업의 성장%",
    "%경쟁판도%", "%시장위험%", "%시장여건%",
    "%시장의 특성%", "%비교우위%", "%가격변동원인%",
    # 표 항목명
    "%특수관계여부%", "%특수관계 여부%",
    "%판매전략%",
    "%비 고%",
    "%영업의 현황%", "%사업의 분류%",
    "%금융위험관리%", "%금융위험 관리%",
    # 공급 구조 설명
    "%공급의 안정성%",
]

# ── C. 비특정 업태명 (정확히 일치 - 기업명이 아닌 업종/채널명) ────
GENERIC_NAMES_EXACT = [
    # 유통 채널
    "공공기관", "관공서", "정부", "지방자치단체", "지자체",
    "백화점", "마트", "대형마트", "슈퍼마켓", "편의점",
    "홈쇼핑", "온라인쇼핑몰", "이커머스",
    "대리점", "도매상", "소매상", "유통업체", "유통사", "총판",
    # 의료 채널
    "병원", "약국", "의원", "클리닉",
    # 금융 채널
    "금융기관", "시중은행",
    # 건설·산업 일반
    "건설사", "시공사", "발주처", "원청사",
    # 협력 관계 일반명사
    "협력사", "협력업체", "파트너사", "하청업체", "외주업체",
    "고객사", "거래처", "납품처", "수주처", "발주사",
    # 기업 규모 분류
    "대기업", "중견기업", "중소기업", "스타트업", "벤처기업",
    # 기술 일반명사 (기업명이 아님)
    "AI", "반도체", "전자", "플랫폼", "소프트웨어",
    "자동차", "완성차",
    # 업종 일반명사 (기업명 아님)
    "제약사", "보험사", "방송사", "건설업체", "제조업체", "IT업체",
    "스트리밍 서비스", "SI업체",
    # 기타
    "학교", "대학교", "연구소",
    "2차전지 제조사", "2차전지 제조업체",
    "완성차 업체", "완성차업체",
    # 단독 키워드 (기업명 아님)
    "원재료", "매입액", "조직도", "매출처", "판매처",
    "공급의 안정성", "주요 가격변동원인", "시장의 특성",
    "비교우위 사항", "시장 특성",
    "외주 업체", "임가공업체", "도매업체",
    "클라우드 사업자", "주요 목표시장",
    "일본 업체", "국내 업체", "해외 업체",
    # 익명 기업 단독 표기
    "A업체", "B업체", "C업체", "A 업체",
]

# ── D. 비특정 업태명 (LIKE 패턴 - 변형 포함) ──────────────────────
GENERIC_NAMES_LIKE = [
    "%국내 소비자%", "%국내 전력%", "%국내 발전사%", "%국내 기업체%",
    "%국내 전력 소비자%", "%국내외 소비자%",
    "%동종업계%", "%경쟁업체%",
    "%동종업%",
    # 회계법인
    "%회계법인%", "%공인회계사%",
    # 불특정
    "%(다수)%", "%(미상)%", "%(미특정)%", "%미기재%",
    "%불특정%", "%추정%",
    # 업체 일반 패턴
    "%다수 업체%", "%여러 업체%",
]

# ── E. 문장 형태 어미 패턴 (한국어 서술형 → 기업명 아님) ──────────
SENTENCE_LIKE = [
    "%있습니다%",
    "%됩니다%",
    "%있으며%",
    "%하고 있%",
    "%하였습니다%",
    "%예상됩니다%",
    "%판단됩니다%",
    "%전망입니다%",
    "%추진합니다%",
    "%추진하고%",
    "%높아지고 있%",
    "%증가하고%",
    "%변동됩니다%",
    "%나갈 것으%",
    "%이루어지고 있%",
    "%노출되어 있%",
    "%마련했습니다%",
    "%계획입니다%",
    "%갖추고 있%",
    "%합니다. %",
    "%입니다. %",
    "%습니다. %",
]

# ── F. 표 값·숫자 형태 (금액·통계값이 기업명으로 오인된 것) ─────────
# 예: "제13기", "153억입니다. 공시대상 사업부문의 구분"
NUMBER_SECTION_LIKE = [
    "제%기",          # 제13기, 제29기 등
    "%억 원%",        # 금액 표현
    "%억원%",
    "%만원%",
    "%만 원%",
    "%조 원%",
]

# ── G. 길이 기준: 50자 초과이고 한국어 서술 포함 ─────────────────
# 외국 기업명은 50자를 거의 초과하지 않음 (예외: 아주 긴 외국 법인명)
# 단, 순수 영문/숫자만으로 구성된 것은 제외 → 한국어 포함 조건 추가
# LENGTH > 45 AND 한국어 서술 어미·조사 포함 → 삭제
LONG_SENTENCE_LIKE = [
    # 길이 45자 초과하면서 명백히 문장인 것
    # → Python 로직에서 별도 처리 (SQL LIKE로 처리 어려움)
]


def build_where(patterns: list[str], col: str = "partner_name") -> str:
    return "(" + " OR ".join(f"{col} LIKE '{p}'" for p in patterns) + ")"


def build_exact_where(names: list[str], col: str = "partner_name") -> str:
    escaped = ", ".join(f"'{n}'" for n in names)
    return f"{col} IN ({escaped})"


def build_prefix_where(prefixes: list[str], col: str = "partner_name") -> str:
    return "(" + " OR ".join(f"{col} LIKE '{p}'" for p in prefixes) + ")"


def analyze(db: sqlite3.Connection) -> dict:
    stats = {}
    total = db.execute("SELECT COUNT(*) FROM supply_chain").fetchone()[0]
    stats["total_before"] = total
    stats["corps_before"] = db.execute("SELECT COUNT(DISTINCT corp_code) FROM supply_chain").fetchone()[0]

    # 1차: 금융사 corp_name
    finance_w = "(" + " OR ".join(f"corp_name LIKE '{k}'" for k in FINANCE_KEYWORDS) + ")"
    stats["finance"] = db.execute(f"SELECT COUNT(*) FROM supply_chain WHERE {finance_w}").fetchone()[0]

    # 2차-A: 목차 시작
    stats["toc_prefix"] = db.execute(
        f"SELECT COUNT(*) FROM supply_chain WHERE {build_prefix_where(TOC_PREFIX_LIKE)}"
    ).fetchone()[0]

    # 2차-B: 섹션 제목 키워드
    stats["section_title"] = db.execute(
        f"SELECT COUNT(*) FROM supply_chain WHERE {build_where(SECTION_TITLE_LIKE)}"
    ).fetchone()[0]

    # 2차-C: 비특정 업태명 (exact)
    stats["generic_exact"] = db.execute(
        f"SELECT COUNT(*) FROM supply_chain WHERE {build_exact_where(GENERIC_NAMES_EXACT)}"
    ).fetchone()[0]

    # 2차-D: 비특정 업태명 (LIKE)
    stats["generic_like"] = db.execute(
        f"SELECT COUNT(*) FROM supply_chain WHERE {build_where(GENERIC_NAMES_LIKE)}"
    ).fetchone()[0]

    # 2차-E: 문장 형태
    stats["sentence"] = db.execute(
        f"SELECT COUNT(*) FROM supply_chain WHERE {build_where(SENTENCE_LIKE)}"
    ).fetchone()[0]

    # 2차-F: 숫자/기 패턴
    stats["number_section"] = db.execute(
        f"SELECT COUNT(*) FROM supply_chain WHERE {build_where(NUMBER_SECTION_LIKE)}"
    ).fetchone()[0]

    # 2차-G: 길이 45자 초과 + 한국어 문장 어미
    long_sentence_where = """
        LENGTH(partner_name) > 45
        AND (
            partner_name LIKE '%습니다%'
            OR partner_name LIKE '%있으며%'
            OR partner_name LIKE '%하고 있%'
            OR partner_name LIKE '%나갈 것%'
            OR partner_name LIKE '%추진하%'
            OR partner_name LIKE '%계획입니다%'
            OR partner_name LIKE '%합니다.%'
            OR partner_name LIKE '%입니다.%'
        )
    """
    stats["long_sentence"] = db.execute(
        f"SELECT COUNT(*) FROM supply_chain WHERE {long_sentence_where}"
    ).fetchone()[0]

    # 기존 1차 규칙 유지
    stats["noise_generic"] = db.execute(
        "SELECT COUNT(*) FROM supply_chain WHERE partner_name IN ('%(다수)%','%(미상)%')"
    ).fetchone()[0]
    stats["self_relation"] = db.execute(
        "SELECT COUNT(*) FROM supply_chain WHERE corp_name = partner_name"
    ).fetchone()[0]
    stats["too_short"] = db.execute(
        "SELECT COUNT(*) FROM supply_chain WHERE LENGTH(partner_name) < 2"
    ).fetchone()[0]

    return stats


def clean(db: sqlite3.Connection, dry_run: bool = False) -> dict:
    stats = analyze(db)

    print(f"\n{'='*60}")
    print(f"  supply_chain 품질 정리 (1차 + 2차 통합)")
    print(f"{'='*60}")
    print(f"  현재: {stats['total_before']:,}건 / {stats['corps_before']:,}개 기업\n")
    print(f"  ── 1차 규칙 (corp_name 기준) ──")
    print(f"    금융사/공급망없는업종:          {stats['finance']:,}건")
    print(f"    자기 자신 관계:                 {stats['self_relation']:,}건")
    print(f"    1자 이하:                       {stats['too_short']:,}건")
    print(f"\n  ── 2차 규칙 (partner_name 기준) ──")
    print(f"    목차/섹션 제목 시작 (가.나.1)): {stats['toc_prefix']:,}건")
    print(f"    섹션 제목 키워드 포함:           {stats['section_title']:,}건")
    print(f"    비특정 업태명 (정확히일치):      {stats['generic_exact']:,}건")
    print(f"    비특정 업태명 (패턴):            {stats['generic_like']:,}건")
    print(f"    문장 형태 어미:                  {stats['sentence']:,}건")
    print(f"    금액/제N기 패턴:                 {stats['number_section']:,}건")
    print(f"    45자초과+한국어문장:             {stats['long_sentence']:,}건")
    print(f"  ※ 범주 간 중복 있으므로 실제 제거는 순차 실행 후 확인")

    if dry_run:
        print(f"\n  [DRY-RUN] 실제 삭제 없음. --dry-run 없이 실행하면 정리됩니다.")
        return stats

    print(f"\n  정리 시작...")
    removed = {}

    finance_w = "(" + " OR ".join(f"corp_name LIKE '{k}'" for k in FINANCE_KEYWORDS) + ")"

    # 1차-1: 금융사
    db.execute(f"DELETE FROM supply_chain WHERE {finance_w}")
    removed["금융사_corp"] = db.execute("SELECT changes()").fetchone()[0]

    # 1차-2: 자기 자신
    db.execute("DELETE FROM supply_chain WHERE corp_name = partner_name")
    removed["자기자신"] = db.execute("SELECT changes()").fetchone()[0]

    # 1차-3: 너무 짧음
    db.execute("DELETE FROM supply_chain WHERE LENGTH(partner_name) < 2")
    removed["1자이하"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-A: 목차 시작
    db.execute(f"DELETE FROM supply_chain WHERE {build_prefix_where(TOC_PREFIX_LIKE)}")
    removed["목차_시작"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-B: 섹션 제목 키워드
    db.execute(f"DELETE FROM supply_chain WHERE {build_where(SECTION_TITLE_LIKE)}")
    removed["섹션_제목"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-C: 비특정 업태명 (exact)
    db.execute(f"DELETE FROM supply_chain WHERE {build_exact_where(GENERIC_NAMES_EXACT)}")
    removed["비특정_정확"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-D: 비특정 업태명 (LIKE)
    db.execute(f"DELETE FROM supply_chain WHERE {build_where(GENERIC_NAMES_LIKE)}")
    removed["비특정_패턴"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-E: 문장 형태 어미
    db.execute(f"DELETE FROM supply_chain WHERE {build_where(SENTENCE_LIKE)}")
    removed["문장_어미"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-F: 금액/기 패턴
    db.execute(f"DELETE FROM supply_chain WHERE {build_where(NUMBER_SECTION_LIKE)}")
    removed["금액_기패턴"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-G: 45자 초과 + 한국어 문장 어미
    long_sentence_where = """
        LENGTH(partner_name) > 45
        AND (
            partner_name LIKE '%습니다%'
            OR partner_name LIKE '%있으며%'
            OR partner_name LIKE '%하고 있%'
            OR partner_name LIKE '%나갈 것%'
            OR partner_name LIKE '%추진하%'
            OR partner_name LIKE '%계획입니다%'
            OR partner_name LIKE '%합니다.%'
            OR partner_name LIKE '%입니다.%'
        )
    """
    db.execute(f"DELETE FROM supply_chain WHERE {long_sentence_where}")
    removed["장문_문장"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-H: 남은 문장 형태 보완 (40자 이상 + 추가 어미)
    remaining_sentence_where = """
        LENGTH(partner_name) >= 40
        AND (
            partner_name LIKE '%강점입니다%'
            OR partner_name LIKE '%다음과 같습니다%'
            OR partner_name LIKE '%추진 중입니다%'
            OR partner_name LIKE '%확대하고%'
            OR partner_name LIKE '%하였으며%'
            OR partner_name LIKE '%발생합니다%'
            OR partner_name LIKE '%됩니다.%'
            OR partner_name LIKE '%이루어지고%'
            OR partner_name LIKE '%있습니다.%'
            OR partner_name LIKE '%이 있어%'
            OR partner_name LIKE '%있으므로%'
            OR partner_name LIKE '%위해서%'
        )
    """
    db.execute(f"DELETE FROM supply_chain WHERE {remaining_sentence_where}")
    removed["장문_보완"] = db.execute("SELECT changes()").fetchone()[0]

    # 2차-I: 익명 기업 표기 (A사~Z사 단일 영문자 + 사)
    # 예: A사, B사, C사, D사, E사, F사, G사, H사, I사, J사, K사, N사, S사, T사
    # 단, 실제 기업명 포함하는 것 제외 (글로브 패턴으로 단독 처리)
    anon_corps = [f"{c}사" for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"]
    anon_where = "partner_name IN (" + ",".join(f"'{n}'" for n in anon_corps) + ")"
    db.execute(f"DELETE FROM supply_chain WHERE {anon_where}")
    removed["익명_기업"] = db.execute("SELECT changes()").fetchone()[0]

    db.commit()

    # ── 3차: 표기 중복 정규화 (UPDATE) ────────────────────────────
    # 중복 표기를 하나의 대표명으로 통일 (DELETE IGNORE 방식: 이미 있으면 중복행 삭제)
    NORMALIZE_MAP = {
        # 원래 표기   →  대표 표기
        "삼성전자(주)":   "삼성전자",
        "Samsung Electronics (삼성전자)": "삼성전자",
        "삼성전자 (Samsung Electronics)": "삼성전자",
        "현대자동차(주)": "현대자동차",
        "현대자동차 (Hyundai Motor Company)": "현대자동차",
        "기아자동차":     "기아",
        "LGU+":          "LG유플러스",
        "LG U+":         "LG유플러스",
        "LG U +":        "LG유플러스",
        "POSCO":         "포스코",
        "SK Hynix":      "SK하이닉스",
        "제이와이피엔터테인먼트 (JYP Entertainment)": "JYP엔터테인먼트",
        "JYP Entertainment": "JYP엔터테인먼트",
        "BMS (Bristol-Myers Squibb)": "Bristol-Myers Squibb",
        "ST Micro (STMicroelectronics)": "STMicroelectronics",
    }
    normalized = 0
    for old_name, new_name in NORMALIZE_MAP.items():
        # 먼저 UPDATE 후, UNIQUE 충돌분은 DELETE로 처리
        db.execute("""
            DELETE FROM supply_chain
            WHERE partner_name = ?
              AND EXISTS (
                SELECT 1 FROM supply_chain s2
                WHERE s2.corp_code = supply_chain.corp_code
                  AND s2.relation_type = supply_chain.relation_type
                  AND s2.partner_name = ?
              )
        """, [old_name, new_name])
        n = db.execute("SELECT changes()").fetchone()[0]
        db.execute("UPDATE supply_chain SET partner_name = ? WHERE partner_name = ?", [new_name, old_name])
        normalized += db.execute("SELECT changes()").fetchone()[0]
    db.commit()
    removed["표기_정규화"] = normalized

    after = db.execute("SELECT COUNT(*), COUNT(DISTINCT corp_code) FROM supply_chain").fetchone()
    stats["total_after"] = after[0]
    stats["corps_after"] = after[1]
    stats["actually_removed"] = stats["total_before"] - after[0]
    stats["removed_detail"] = removed

    by_type = db.execute(
        "SELECT relation_type, COUNT(*) FROM supply_chain GROUP BY relation_type ORDER BY COUNT(*) DESC"
    ).fetchall()

    print(f"\n  ✅ 정리 완료!")
    print(f"  실제 제거: {stats['actually_removed']:,}건")
    for k, v in removed.items():
        if v:
            print(f"    {k}: {v:,}건")
    print(f"\n  최종: {stats['total_after']:,}건 / {stats['corps_after']:,}개 기업")
    for r in by_type:
        print(f"    {r[0]}: {r[1]:,}건")

    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="삭제 예상 건수만 확인")
    args = parser.parse_args()

    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    clean(db, dry_run=args.dry_run)
    db.close()


if __name__ == "__main__":
    main()
