"""
scripts/validate_supply_chain.py
──────────────────────────────────
공급망 데이터 검증 · 정제 스크립트

Phase 1: 규칙 기반 자동 제거 (API 불필요)
  - 문장 조각, 카테고리명, 제품명 등 명백한 노이즈 제거

Phase 2: Gemini 배치 검증 (업종별 20개 기업 묶음)
  - biz_content 없이 partner 목록만 전송 → 토큰 최소화
  - Gemini가 relation_type 오류, 비기업명 등 검출
  - 삭제 / relation_type 수정 적용

실행:
  python scripts/validate_supply_chain.py --phase1            # 규칙 기반만
  python scripts/validate_supply_chain.py --phase2            # API 검증만
  python scripts/validate_supply_chain.py --phase2 --limit 5  # 배치 5개만 (테스트)
  python scripts/validate_supply_chain.py                     # 전체 (phase1 + phase2)
  python scripts/validate_supply_chain.py --dry-run           # 삭제 예정 목록만 출력
"""

import argparse
import io
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# ── 환경 변수 로드 ────────────────────────────────────────────────────
_env_path = ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            key, val = k.strip(), v.strip()
            if val and not os.environ.get(key):
                os.environ[key] = val

# ── Gemini 모델 우선순위 ─────────────────────────────────────────────
GEMINI_MODELS = [
    "gemma-3-27b-it",
    "gemini-2.0-flash",
    "gemma-3-12b-it",
    "gemini-2.5-flash",
    "gemini-2.0-flash-lite",
]
BATCH_SIZE  = 20    # 한 번 API 호출당 기업 수
DELAY       = 1.5   # API 호출 간격 (초)


# ══════════════════════════════════════════════════════════════════════
#  Phase 1: 규칙 기반 노이즈 탐지
# ══════════════════════════════════════════════════════════════════════

_NOISE_ENDINGS = [
    "에 따라", "하여", "있고", "이며", "정도", "위험", "대상",
    "상황", "추이", "요소", "특성", "특징", "수준", "경우",
    "이상의", "이하의", "으로서", "으로부터", "있습니다", "합니다",
    "됩니다", "있으며", "있음", "바람직", "추세", "환경",
]
_CATEGORY_KEYWORDS = [
    "제조사", "공급사", "공급업체", "납품업체", "생산업체",
    "제조업체", "유통업체", "판매처", "구매처", "거래처",
    "대행사", "업체들", "사업자", "공급자", "구매자", "소비자",
    "이동통신사", "통신사", "카드사", "발전사", "발전소",
    "조선소", "국내 건설사", "파운드리 업체", "제약사",
    "주요 경쟁사", "경쟁업체", "협력사들",
]
_PRODUCT_KEYWORDS = [
    "리튬", "배터리 셀", "반도체 칩", "가전제품",
    "합성수지", "타이어", "철근", "레미콘", "알루미늄 잉곳",
    "원부재료", "항공기 부품",
]
_EXPLICIT_NOISE = {
    "회사명", "경쟁요소", "주요고객", "기타", "동종업체",
    "주요거래처", "경쟁상황", "경쟁 상황", "가격경쟁력",
    "공급시장의 독과점 정도", "공급시장의독과점 정도",
    "공급의안정성", "독과점 정도", "독과점정도",
    "신용위험", "환위험", "유동성위험", "수주물량",
    "자원조달의 특성", "자원조달상의 특성",
    "특수한 관계여부", "주주관계", "합작관계",
    "가격 변동 추이", "전방산업", "후방산업",
    "ESS", "LCD", "OLED", "PCB", "PLATE", "RESIN", "PP",
    "IC", "LTD", "AL", "Wire Type", "Package",
    "더불어", "병·의원", "병원·의원", "데이터센터",
    "직영점", "지하철", "아울렛",
    "베트남", "동남아", "동남아시아", "싱가포르", "브라질",
    "인도네시아", "중국", "미국", "일본", "유럽", "국내외",
    "전력거래소", "L社", "S社", "국내 대기업",
    "INC", "SM", "VAN", "LM", "LED", "PTA", "TDI", "TUBE",
    "IoT", "Power", "LENS", "CHIP", "CPU", "FPCB", "IT",
    "Distributor", "BOX소요메이커", "GIS MAP 시스템",
    "AI단말", "Display",
}

# 회사명으로 볼 수 있는 숫자 시작 패턴 (false positive 방지)
_DIGIT_OK_PATTERNS = re.compile(
    r'^(3M|11번가|1588|2050|24시간|365|3GPP|5G|4G)\b', re.IGNORECASE
)


def is_noise(name: str) -> str | None:
    """
    노이즈로 판단되면 이유 문자열 반환, 정상이면 None.
    Phase 1용 — Phase 2 API보다 보수적으로 적용.
    """
    n = name.strip()
    if not n or len(n) < 2:
        return "TOO_SHORT"
    if n.lower() in {x.lower() for x in _EXPLICIT_NOISE}:
        return "EXPLICIT_NOISE"
    # 50자 초과 → 거의 확실히 문장
    if len(n) > 50:
        return "TOO_LONG"
    # 종결어미로 끝남
    for e in _NOISE_ENDINGS:
        if n.endswith(e):
            return f"NOISE_ENDING({e})"
    # 카테고리 단어 포함 (단독으로)
    for kw in _CATEGORY_KEYWORDS:
        if kw in n and len(n) <= len(kw) + 12:
            return f"CATEGORY({kw})"
    # 제품명 포함
    for kw in _PRODUCT_KEYWORDS:
        if kw in n and len(n) <= len(kw) + 8:
            return f"PRODUCT({kw})"
    # "A社", "B社" 형태
    if re.search(r'[A-Z가-힣]社$', n):
        return "ANON_COMPANY"
    # 숫자·특수문자만
    if re.match(r'^[\d\s\.\-\(\)\[\]\/\%]+$', n):
        return "NUMERIC"
    # 숫자 시작 (단, 알려진 유효 패턴 제외)
    if n[0].isdigit() and not _DIGIT_OK_PATTERNS.match(n):
        return "DIGIT_START"
    # 괄호형 설명 포함
    if re.search(r'\([가-힣]{10,}\)', n):
        return "LONG_PAREN"
    return None


# ══════════════════════════════════════════════════════════════════════
#  업종 추정 (companies 테이블 없을 경우 corp_name 키워드 기반)
# ══════════════════════════════════════════════════════════════════════

def guess_sector(corp_name: str) -> str:
    n = corp_name
    if re.search(r'바이오|제약|헬스|의료|병원|신약|백신|진단|의생명', n): return '바이오·제약'
    if re.search(r'반도체|디스플레이|LED|PCB|전자부품|팹', n):          return '반도체·전자'
    if re.search(r'삼성전자|SK하이닉스|LG전자|LG디스플레이', n):         return '반도체·전자'
    if re.search(r'자동차|타이어|모비스|만도|한온|현대위아|기아', n):     return '자동차·부품'
    if re.search(r'화학|소재|섬유|고무|플라스틱|수지|도료|잉크', n):      return '화학·소재'
    if re.search(r'건설|건축|토목|인프라|플랜트|시공', n):              return '건설·인프라'
    if re.search(r'게임|엔터|엔씨|넷마블|크래프톤|카카오게임', n):        return 'IT·게임'
    if re.search(r'IT|소프트|시스템|솔루션|플랫폼|클라우드', n):         return 'IT·소프트웨어'
    if re.search(r'에너지|전력|가스|석유|발전|태양광|수소', n):          return '에너지'
    if re.search(r'철강|포스코|현대제철|동국제강|고로|제철', n):          return '철강'
    if re.search(r'유통|백화점|마트|쇼핑|물류|택배|항공|해운', n):       return '유통·물류'
    if re.search(r'금융|은행|보험|증권|투자|캐피탈', n):               return '금융'
    if re.search(r'식품|음료|제과|제빵|주류|농업|축산', n):             return '식품·소비재'
    if re.search(r'방산|방위|무기|탄약|군수', n):                     return '방산'
    return '기타'


# ══════════════════════════════════════════════════════════════════════
#  Phase 2: Gemini 배치 검증
# ══════════════════════════════════════════════════════════════════════

VALIDATION_PROMPT = """You are validating supply chain data for Korean companies (extracted from DART disclosures).

For each company, review its listed supply chain relationships.

REMOVE a relationship if:
1. partner_name is NOT a real company name (sentence fragment, product/material name, vague category like "제조사", "공급업체", location name, number)
2. relation_type is clearly wrong (e.g., a well-known competitor listed as "supplier", or a government agency listed as "partner")

KEEP a relationship if the partner seems like a plausible real company, even if unfamiliar.

Output ONLY valid JSON. No explanation.
Format:
{
  "CORP_CODE": {
    "remove": ["partner_name_1", "partner_name_2"],
    "fix_type": [{"partner": "name", "new_type": "correct_type"}]
  },
  ...
}

If nothing to remove/fix for a company, omit it from output entirely.
Output empty object {} if all data is clean.

Companies to validate:
"""


def call_gemini_validate(batch: list[dict]) -> dict:
    """
    batch: [{"corp_code": ..., "corp_name": ..., "sector": ...,
              "relations": [{"partner": ..., "type": ...}, ...]}]
    returns: {corp_code: {"remove": [...], "fix_type": [...]}}
    """
    key1 = os.environ.get("GEMINI_API_KEY", "").strip()
    key2 = os.environ.get("GEMINI_API_KEY_2", "").strip()
    keys  = [k for k in [key1, key2] if k]
    if not keys:
        raise ValueError("GEMINI_API_KEY 없음")

    prompt = VALIDATION_PROMPT + json.dumps(batch, ensure_ascii=False, indent=2)
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": 2048, "temperature": 0.0},
    }).encode("utf-8")

    last_err = None
    for model in GEMINI_MODELS:
        for key in keys:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
            req = urllib.request.Request(
                url, data=payload, headers={"Content-Type": "application/json"}
            )
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    body = resp.read().decode("utf-8")
                data = json.loads(body)
                candidates = data.get("candidates", [])
                if not candidates:
                    last_err = f"{model}: candidates 없음"
                    continue
                raw = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
                raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
                if not raw:
                    return {}
                result = json.loads(raw)
                return result if isinstance(result, dict) else {}
            except urllib.error.HTTPError as e:
                if e.code in (429, 503):
                    last_err = f"{model}: HTTP {e.code} 쿼터 초과"
                    time.sleep(5)
                    continue
                if e.code == 404:
                    last_err = f"{model}: 404 모델 없음"
                    break
                raise
            except json.JSONDecodeError as e:
                last_err = f"{model}: JSON 파싱 실패"
                return {}
            except Exception as e:
                last_err = f"{model}: {e}"
                continue
    raise RuntimeError(f"Gemini 모든 키/모델 소진. 마지막: {last_err}")


# ══════════════════════════════════════════════════════════════════════
#  DB 유틸
# ══════════════════════════════════════════════════════════════════════

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


# ══════════════════════════════════════════════════════════════════════
#  메인
# ══════════════════════════════════════════════════════════════════════

def phase1(db, dry_run=False) -> int:
    """규칙 기반 노이즈 제거. 삭제 레코드 수 반환."""
    rows = db.execute(
        "SELECT DISTINCT partner_name FROM supply_chain"
    ).fetchall()

    to_delete = []
    reasons   = {}
    for r in rows:
        pn = r["partner_name"]
        reason = is_noise(pn)
        if reason:
            to_delete.append(pn)
            reasons[pn] = reason

    print(f"\n[Phase 1] 노이즈 탐지: {len(to_delete)}개 고유 파트너명")
    for pn in to_delete[:30]:
        print(f"  DELETE [{pn[:55]}] → {reasons[pn]}")
    if len(to_delete) > 30:
        print(f"  ... 외 {len(to_delete)-30}개")

    if dry_run:
        cnt = db.execute(
            f"SELECT COUNT(*) FROM supply_chain WHERE partner_name IN ({','.join(['?']*len(to_delete))})",
            to_delete
        ).fetchone()[0] if to_delete else 0
        print(f"\n  [DRY-RUN] 삭제 예정 레코드: {cnt}건")
        return cnt

    if not to_delete:
        print("  삭제 대상 없음")
        return 0

    # 배치 삭제
    CHUNK = 500
    total_deleted = 0
    for i in range(0, len(to_delete), CHUNK):
        chunk = to_delete[i:i+CHUNK]
        cur = db.execute(
            f"DELETE FROM supply_chain WHERE partner_name IN ({','.join(['?']*len(chunk))})",
            chunk
        )
        total_deleted += cur.rowcount
    db.commit()
    print(f"\n  ✅ 삭제 완료: {total_deleted}건 레코드")
    return total_deleted


def phase2(db, limit_batches=0, dry_run=False) -> tuple[int, int]:
    """Gemini 배치 검증. (삭제 수, 수정 수) 반환."""
    # 전체 supply_chain 로드 (corp별 그룹화)
    rows = db.execute("""
        SELECT corp_code, corp_name, relation_type, partner_name
        FROM supply_chain
        ORDER BY corp_code, relation_type, partner_name
    """).fetchall()

    # corp별 그룹화
    corp_data: dict[str, dict] = {}
    for r in rows:
        cc = r["corp_code"]
        if cc not in corp_data:
            corp_data[cc] = {
                "corp_code": cc,
                "corp_name": r["corp_name"],
                "sector":    guess_sector(r["corp_name"]),
                "relations": [],
            }
        corp_data[cc]["relations"].append({
            "partner": r["partner_name"],
            "type":    r["relation_type"],
        })

    # 업종별로 정렬 → 같은 업종끼리 묶음
    corp_list = sorted(corp_data.values(), key=lambda x: x["sector"])
    total_corps = len(corp_list)
    batches = [corp_list[i:i+BATCH_SIZE] for i in range(0, total_corps, BATCH_SIZE)]

    if limit_batches:
        batches = batches[:limit_batches]

    print(f"\n[Phase 2] 검증 대상: {total_corps}개 기업 → {len(batches)}개 배치 (배치당 {BATCH_SIZE}개)")
    if limit_batches:
        print(f"  (--limit {limit_batches} 적용)")

    total_removed = total_fixed = 0

    for bi, batch in enumerate(batches, 1):
        sector = batch[0]["sector"]
        corp_names_preview = ", ".join(c["corp_name"] for c in batch[:3])
        print(f"\n  배치 {bi}/{len(batches)} [{sector}] {corp_names_preview}...")

        try:
            result = call_gemini_validate(batch)
        except Exception as e:
            print(f"    오류: {e}")
            time.sleep(5)
            continue

        if not result:
            print(f"    → 이상 없음 (빈 결과)")
            time.sleep(DELAY)
            continue

        for corp in batch:
            cc = corp["corp_code"]
            corp_result = result.get(cc, {})
            if not corp_result:
                continue

            removes   = corp_result.get("remove", [])
            fix_types = corp_result.get("fix_type", [])

            if removes:
                print(f"    [{corp['corp_name']}] 제거: {removes[:5]}")
                if not dry_run:
                    for pn in removes:
                        db.execute(
                            "DELETE FROM supply_chain WHERE corp_code=? AND partner_name=?",
                            [cc, pn]
                        )
                    total_removed += len(removes)

            if fix_types:
                print(f"    [{corp['corp_name']}] 타입수정: {fix_types[:3]}")
                if not dry_run:
                    for ft in fix_types:
                        new_type = ft.get("new_type")
                        partner  = ft.get("partner")
                        if new_type not in ("customer", "supplier", "partner", "competitor"):
                            continue
                        # 대표 행 가져오기 (context/source_report 보존)
                        old_row = db.execute(
                            "SELECT context, source_report, analyzed_at FROM supply_chain"
                            " WHERE corp_code=? AND partner_name=? LIMIT 1",
                            [cc, partner]
                        ).fetchone()
                        if not old_row:
                            continue
                        # 해당 파트너의 모든 행 삭제 후 새 타입으로 재삽입
                        db.execute(
                            "DELETE FROM supply_chain WHERE corp_code=? AND partner_name=?",
                            [cc, partner]
                        )
                        db.execute(
                            "INSERT OR IGNORE INTO supply_chain"
                            " (corp_code, corp_name, relation_type, partner_name,"
                            "  context, source_report, analyzed_at)"
                            " VALUES (?,?,?,?,?,?,?)",
                            [cc, corp["corp_name"], new_type, partner,
                             old_row[0], old_row[1], old_row[2]]
                        )
                    total_fixed += len(fix_types)

        if not dry_run:
            db.commit()
        time.sleep(DELAY)

    return total_removed, total_fixed


def main():
    parser = argparse.ArgumentParser(description="supply_chain 데이터 검증·정제")
    parser.add_argument("--phase1",   action="store_true", help="규칙 기반 정제만 실행")
    parser.add_argument("--phase2",   action="store_true", help="Gemini 배치 검증만 실행")
    parser.add_argument("--limit",    type=int, default=0, help="Phase 2 배치 수 제한 (테스트용)")
    parser.add_argument("--dry-run",  action="store_true", help="실제 삭제 없이 예정 목록만 출력")
    args = parser.parse_args()

    # 기본값: 둘 다 실행
    run_p1 = args.phase1 or (not args.phase1 and not args.phase2)
    run_p2 = args.phase2 or (not args.phase1 and not args.phase2)

    db = get_db()

    # 시작 현황
    before = db.execute("SELECT COUNT(*) as n, COUNT(DISTINCT corp_code) as c FROM supply_chain").fetchone()
    print(f"=== supply_chain 검증 시작 ===")
    print(f"현재: {before['n']:,}건 / {before['c']:,}개 기업")
    if args.dry_run:
        print("⚠️  DRY-RUN 모드 (실제 변경 없음)")

    p1_del = p2_del = p2_fix = 0

    if run_p1:
        p1_del = phase1(db, dry_run=args.dry_run)

    if run_p2:
        p2_del, p2_fix = phase2(db, limit_batches=args.limit, dry_run=args.dry_run)

    # 최종 현황
    after = db.execute("SELECT COUNT(*) as n, COUNT(DISTINCT corp_code) as c FROM supply_chain").fetchone()
    print(f"\n=== 완료 ===")
    print(f"Phase 1 삭제: {p1_del:,}건")
    print(f"Phase 2 삭제: {p2_del:,}건 / 타입수정: {p2_fix:,}건")
    print(f"결과: {after['n']:,}건 / {after['c']:,}개 기업")
    print(f"순감: {before['n'] - after['n']:,}건")
    db.close()


if __name__ == "__main__":
    main()
