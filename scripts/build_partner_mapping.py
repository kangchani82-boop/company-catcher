"""
scripts/build_partner_mapping.py
─────────────────────────────────
partner_name → corp_code 매핑 테이블 구축

단계:
  1. 노이즈 감지 (패턴 + 명시 목록)
  2. 수동 사전 매칭 (표기 불일치 / 사명변경 / 비상장)
  3. 비상장/외국기업 마킹
  4. 정확 매칭 (corp_name = partner_name)
  5. 정규화 매칭 (법인 접두/접미 제거 후)
  6. 결과 리포트

실행:
  python scripts/build_partner_mapping.py
  python scripts/build_partner_mapping.py --top 1000  # 상위 N개
  python scripts/build_partner_mapping.py --reset      # 초기화 후 재실행
"""

import argparse
import re
import sqlite3
import sys
import io
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB   = ROOT / "data" / "dart" / "dart_reports.db"


# ──────────────────────────────────────────────────────────────────
#  법인명 정규화
# ──────────────────────────────────────────────────────────────────
_LEGAL_PREFIXES = ["주식회사 ", "유한회사 ", "합자회사 ", "합명회사 ", "주식회사", "㈜ ", "(주) "]
_LEGAL_SUFFIXES = [" 주식회사", " (주)", " ㈜", "(주)", "㈜", " 주식회사.", "주식회사",
                   " 유한회사", " 유한책임회사"]
_LEGAL_EMBEDDED = ["㈜", "(주)"]
_ORG_SUFFIXES   = [" 주식회사", " 공사", " 공단", " 재단", " 연구원",
                   " 연구소", " 학교", " 대학교", " 대학", " 병원"]

def normalize(name: str) -> str:
    n = name.strip()
    for pat in _LEGAL_PREFIXES:
        if n.startswith(pat):
            n = n[len(pat):].strip()
    for pat in _LEGAL_SUFFIXES:
        if n.endswith(pat):
            n = n[:-len(pat)].strip()
    for pat in _LEGAL_EMBEDDED:
        n = n.replace(pat, "").strip()
    for pat in _ORG_SUFFIXES:
        if n.endswith(pat):
            n = n[:-len(pat)].strip()
            break
    # 영문 괄호 제거: "LG화학 (LG Chem)" → "LG화학" (영문+숫자만인 경우)
    n = re.sub(r'\s*\([A-Za-z0-9 .,&\-]+\)\s*$', '', n).strip()
    return n


# ──────────────────────────────────────────────────────────────────
#  패턴 기반 노이즈 탐지
#  (명시 목록에 없어도 문장형/카테고리형이면 자동 노이즈 처리)
# ──────────────────────────────────────────────────────────────────
_NOISE_ENDINGS = [
    "에 따라", "하여", "있고", "이며", "정도", "위험", "대상",
    "상황", "추이", "요소", "특성", "특징", "수준", "경우",
    "이상의", "이하의", "으로서", "으로부터",
]
_CATEGORY_PATTERNS = [
    "제조사", "공급사", "공급업체", "납품업체", "생산업체",
    "제조업체", "제조사들", "생산업체들",
    "유통업체", "판매처", "구매처", "거래처", "협력사",
    "대행사", "경쟁사", "업체들", "사업자", "공급자",
    "구매자", "수요자", "소비자",
]

def is_noise_pattern(name: str) -> bool:
    """문장형/카테고리형/비기업명 패턴 → True 반환"""
    # 38자 초과는 거의 문장 (안전마진 2자 낮춤)
    if len(name) > 38:
        return True
    # "A) 구매품", "B) 가공품" 형태 (품목/공정 카테고리)
    if re.match(r'^[A-Za-z]\)', name):
        return True
    # "A社", "B社" 형태 (익명 플레이스홀더)
    if re.search(r'[A-Z가-힣]社$', name):
        return True
    # 특정 종결어미로 끝나는 경우
    for e in _NOISE_ENDINGS:
        if name.endswith(e):
            return True
    # 카테고리 서술어 포함
    for p in _CATEGORY_PATTERNS:
        if p in name:
            return True
    # 숫자 또는 특수문자 시작
    if name and (name[0].isdigit() or name[0] in ("'",)):
        return True
    return False


# ──────────────────────────────────────────────────────────────────
#  수동 매칭 사전
#  key  : partner_name (DB에 저장된 표기)
#  value: corp_code (DART 코드) or None (비상장/미수록)
# ──────────────────────────────────────────────────────────────────
MANUAL_EXACT: dict[str, str | None] = {
    # ── KT 계열 (00190321 = 우리 DB 확인) ──
    "KT":              "00190321",
    "케이티":          "00190321",
    "KT(주)":          "00190321",
    # ── 카카오 (00258801 = 우리 DB 확인) ──
    "카카오":          "00258801",
    "Kakao":           "00258801",
    # ── SK텔레콤 (00159023 = 우리 DB 확인) ──
    "SKT":             "00159023",
    "SK텔레콤":        "00159023",
    # ── 현대자동차 (00164742 = 우리 DB 확인) ──
    "현대차":          "00164742",
    "현대자동차":      "00164742",
    # ── 현대중공업 → HD현대중공업 (01390344 = 우리 DB 확인) ──
    "현대중공업":      "01390344",
    "HD현대중공업":    "01390344",
    # ── 두산중공업 → 두산에너빌리티 (00159616 = 우리 DB 확인) ──
    "두산중공업":      "00159616",
    "두산에너빌리티":  "00159616",
    # ── 현대오일뱅크 → 에이치디현대오일뱅크 (00105174 = 우리 DB 확인) ──
    "현대오일뱅크":    "00105174",
    "HD현대오일뱅크":  "00105174",
    # ── 현대산업개발 → IPARK현대산업개발 (01310269 = 우리 DB 확인) ──
    "현대산업개발":    "01310269",
    "HDC현대산업개발": "01310269",
    # ── JYP엔터테인먼트 (00258689 = 우리 DB 확인) ──
    "JYP엔터테인먼트": "00258689",
    "JYP":             "00258689",
    # ── JTBC (00922702 = 우리 DB 확인) ──
    "JTBC":            "00922702",
    "제이티비씨":      "00922702",
    # ── 삼성SDS (00126186 = 우리 DB 확인) ──
    "삼성SDS":         "00126186",
    "삼성에스디에스":  "00126186",
    # ── LG디스플레이 (00105873 = 우리 DB 확인) ──
    "LGD":             "00105873",
    "엘지디스플레이":  "00105873",
    # ── 코오롱인더스트리 (00795135 = 우리 DB 확인: 코오롱인더) ──
    "코오롱인더스트리": "00795135",
    # ── 한국전력공사 (00159193 = 우리 DB 확인) ──
    "한전":            "00159193",
    "한국전력":        "00159193",
    # ── 한국수력원자력 (00382001 = 우리 DB 확인) ──
    "한수원":          "00382001",
    "한국수력원자력":  "00382001",
    # ── KT&G (00244455 = 우리 DB 확인) ──
    "KT&G":            "00244455",
    "케이티앤지":      "00244455",
    # ── S-OIL (00104117 = 우리 DB 확인) ──
    "S-OIL":           "00104117",
    "에쓰오일":        "00104117",

    # ═══════ None: 비상장 / 공공기관 / 우리 DB 미수록 ═══════

    # ── KG모빌리티 (구 쌍용자동차) ──
    "KG모빌리티":      None,
    "쌍용자동차":      None,

    # ── 삼성 계열 비상장/미수록 ──
    "삼성디스플레이":  None,
    "삼성바이오에피스": None,
    "삼성증권":        None,
    "삼성화재":        None,
    "삼성카드":        None,
    "에버랜드":        None,
    "삼성그룹":        None,
    "삼성":            None,       # 너무 모호
    "삼성엔지니어링":  None,       # 우리 DB 미수록
    "삼성생명":        None,

    # ── SK 계열 비상장/미수록 ──
    "SK온":            None,
    "SK브로드밴드":    None,
    "SKB":             None,
    "SK에너지":        None,
    "SK매직":          None,
    "SK지오센트릭":    None,
    "SK에코플랜트":    None,
    "SKON":            None,

    # ── 네이버 (우리 DB 미수록) ──
    "네이버":          None,
    "NAVER":           None,
    "Naver":           None,
    "네이버웹툰":      None,

    # ── 쿠팡 (DART 미등록) ──
    "쿠팡":            None,

    # ── 은행권 (금융감독원 신고 → DART 미수록) ──
    "신한은행":        None,
    "(주)신한은행":    None,
    "하나은행":        None,
    "우리은행":        None,
    "국민은행":        None,
    "KB국민은행":      None,
    "한국산업은행":    None,
    "KDB산업은행":     None,
    "NH농협은행":      None,
    "농협은행":        None,
    "기업은행":        None,
    "IBK기업은행":     None,
    "중소기업은행":    None,
    "KEB하나은행":     None,
    "하나금융지주":    None,

    # ── 증권사 (우리 DB 미수록) ──
    "NH투자증권":      None,
    "한국투자증권":    None,
    "한화투자증권":    None,
    "현대차증권":      None,
    "키움증권":        None,
    "신한펀드파트너스": None,

    # ── 정부/공공기관 ──
    "방위사업청":      None,
    "조달청":          None,
    "한국토지주택공사": None,
    "한국토지주택공사(LH)": None,
    "LH":              None,
    "한국수자원공사":  None,
    "한국도로공사":    None,
    "한국철도공사":    None,
    "코레일":          None,
    "인천국제공항공사": None,
    "주택도시보증공사": None,
    "한국거래소":      None,
    "한국전력거래소":  None,
    "한국동서발전":    None,
    "한국동서발전(주)": None,
    "남부발전":        None,
    "한국수출입은행":  None,
    "국가철도공단":    None,
    "서울보증보험":    None,
    "과학기술정보통신부": None,
    "산업통상자원부":  None,
    "국토교통부":      None,
    "중소벤처기업부":  None,
    "방위산업체":      None,
    "민간기업":        None,
    "공군":            None,
    "해군":            None,
    "육군":            None,
    "서울시":          None,

    # ── 연구기관 ──
    "한국생명공학연구원": None,
    "한국생산기술연구원": None,
    "한국전자통신연구원": None,
    "한국전자통신연구원(ETRI)": None,
    "한국과학기술연구원": None,
    "한국기계연구원":  None,
    "한국항공우주연구원": None,
    "한국화학연구원":  None,
    "한국건설기술연구원": None,
    "국방과학연구소":  None,
    "KAIST":           None,
    "한국지능정보사회진흥원": None,

    # ── 신용평가사 ──
    "한국신용평가":    None,
    "한국신용평가㈜":  None,
    "NICE신용평가":    None,
    "Moody's":         None,
    "S&P":             None,

    # ── 정부기관 ──
    "국방부":          None,
    "질병관리청":      None,
    "건강보험공단":    None,

    # ── 대학교 ──
    "서울대학교":      None,
    "서울대학교 산학협력단": None,
    "서울시립대학교":  None,
    "고려대학교":      None,
    "고려대학교산학협력단": None,
    "연세대학교":      None,
    "성균관대학교":    None,

    # ── 농협 계열 ──
    "농협":            None,
    "농협중앙회":      None,

    # ── 비상장 유통/소비재 ──
    "올리브영":        None,
    "CJ올리브영":      None,
    "스타벅스":        None,
    "홈플러스":        None,
    "G마켓":           None,
    "마켓컬리":        None,
    "무신사":          None,
    "코스트코":        None,
    "아워홈":          None,
    "하이마트":        None,
    "롯데하이마트":    None,
    "전자랜드":        None,

    # ── 롯데 계열 ──
    "롯데":            None,
    "롯데마트":        None,
    "롯데건설":        None,
    "롯데백화점":      None,
    "롯데월드":        None,
    "롯데케미칼":      None,

    # ── 현대 금융/유통 ──
    "현대카드":        None,
    "현대캐피탈":      None,
    "현대홈쇼핑":      None,
    "현대기아차":      None,   # 두 회사 묶음 (모호)

    # ── GS 계열 비상장 ──
    "GS칼텍스":        None,
    "GS Caltex":       None,
    "GS리테일":        None,

    # ── 한화 계열 비상장 ──
    "한화토탈":        None,
    "한화토탈에너지스": None,

    # ── 포스코 계열 비상장 ──
    "포스코건설":      None,
    "포스코이앤씨":    None,

    # ── 기타 비상장 ──
    "SK넥실리스":      None,
    "LX하우시스":      None,
    "11번가":          None,
    "아시아나항공":    None,
    "에어서울":        None,
    "한국GM":          None,
    "한국지엠":        None,
    "르노코리아":      None,
    "르노삼성":        None,
    "OCI":             None,
    "서울반도체":      None,
    "넥센타이어":      None,
    "청호나이스":      None,
    "코스맥스":        None,
    "코스맥스(주)":    None,
    "효성":            None,   # 복수 계열사 (모호)
    "삼양사":          None,
    "도레이첨단소재":  None,
    "한국토지신탁":    None,
    "와이케이스틸":    None,
    "전주페이퍼":      None,
    "녹십자":          None,
    "하림":            None,
    "한화투자증권":    None,
    "한화투자증권":    None,

    # ── 방송사 (우리 DB 미수록) ──
    "MBC":             None,
    "KBS":             None,

    # ── 병원 (비영리법인) ──
    "서울대학교병원":  None,
    "서울대병원":      None,
    "삼성서울병원":    None,
    "서울아산병원":    None,
    "아산병원":        None,
    "세브란스병원":    None,

    # ── 외국기업 (비상장 또는 DART 미등록) ──
    "신일본제철":      None,

    # ══════ 추가 검증분 (v2) ══════
    # ── 영문 표기 ──
    "LG Display":      "00105873",   # LG디스플레이
    "엘지전자":        "00401731",   # LG전자
    "KCC":             "00105271",   # (주)케이씨씨

    # ── 비상장/미수록 추가 ──
    "카카오모빌리티":  None,         # 카카오 비상장 자회사
    "현대차그룹":      None,         # 그룹명 (모호)
    "LS산전":          None,         # LS ELECTRIC 사명변경, 우리 DB 미수록
    "KB손해보험":      None,         # 금융사 (우리 DB 미수록)
    "녹십자의료재단":  None,         # 비영리 재단
    "대림산업":        None,         # DL이앤씨 사명변경, 우리 DB 미수록
    "대우":            None,         # 모호 (대우건설/대우조선 등 여러 회사)
    "(주)하이플":      None,
    "HD현대사이트솔루션": None,
    "(주)에스엠엔터테인먼트": None,  # SM엔터테인먼트 (우리 DB 미수록)
    "케이씨글라스":    None,
    "클라크":          None,         # Clark Equipment (외국기업)
    "(유)써모피셔사이언티픽솔루션스": None,  # Thermo Fisher 한국법인
    "(주)MDS모빌리티": None,
    "(주)문경레저타운": None,
    "(주)에비오시스 테크놀러지스 (구 타아스)": None,
    "(주)참프레":      None,
    "(주)한신정공":    None,
    "셀투바이오":      None,
    "아주산업":        None,
    "대한주정판매(주)": None,
    "비피코리아(주)":  None,
    "한화토탈에너지스": None,        # 중복 방지

    # ══════ 추가 검증분 (v3 — none 345 정리) ══════
    # ── 상장사 영문 표기 (corp_code 확인) ──
    "LGES":                "01515323",   # LG에너지솔루션
    "Samsung Electronics": "00126380",   # 삼성전자
    "S-OIL주":             "00138279",   # S-Oil (주 표기 variant)
    "SK이노베이션(주)":    "00631518",   # SK이노베이션
    "LX인터내셔날":        "00120076",   # LX인터내셔널 (오타 variant)

    # ── HD현대 계열 (우리 DB 미수록 → None) ──
    "HD현대로보틱스":      None,   # HD현대 계열 로봇 자회사
    "HD현대마린솔루션":    None,   # HD현대 해양 서비스
    "HD현대에너지솔루션":  None,   # HD현대 태양광
    "HD현대건설기계":      None,   # HD현대 건설장비

    # ── HK·HM·HE 계열 약자 ──
    "HK이노엔":            None,   # HK그룹 제약 (비상장)
    "HKMC":                None,   # Hyundai Kia Motors Corp (내부 지칭)
    "HMMA":                None,   # Hyundai Motor Manufacturing Alabama (해외법인)
    "HEC":                 None,   # Hyundai Engineering 등 (모호)
    "HKC":                 None,   # 홍콩 패널 제조사 (외국기업)

    # ── SK 계열 미수록 ──
    "SK쉴더스":            None,   # 구 ADT캡스 (비상장)
    "SK파워":              None,   # SK E&S 계열 (비상장)
    "SK그룹":              None,   # 그룹명 (모호)
    "SK C&C":              None,   # SK(주) C&C 사업부
    "SK그린":              None,   # SK 그린사업 (비상장)
    "SK플래닛":            None,   # 비상장

    # ── LG 계열 미수록 ──
    "LG CNS":              None,   # 비상장 IT서비스
    "LGCNS":               None,   # 표기 변형
    "LG AI 연구원":        None,   # 연구조직
    "LG그룹":              None,   # 그룹명 (모호)

    # ── KT 계열 미수록 ──
    "KT클라우드":          None,   # KT 자회사 (비상장)
    "KTDS":                None,   # KT DS (비상장)

    # ── KB 계열 미수록 ──
    "KB증권":              None,   # KB금융 증권사 (우리 DB 미수록)
    "KB캐피탈":            None,   # KB금융 계열

    # ── 방송·미디어 미수록 ──
    "tvN":                 None,   # CJ ENM 채널
    "TVH":                 None,   # Belgian parts distributor (외국기업)
    "TV홈쇼핑":            None,   # 복수 지칭 (모호)
    "MBN":                 None,   # 매일방송
    "KBS2":                None,   # KBS 채널 (법인 아님)
    "KDDI":                None,   # 일본 통신사 (외국기업)
    "NHN클라우드":         None,   # NHN 자회사 (비상장)

    # ── 기타 미수록 ──
    "SDC":                 None,   # 삼성디스플레이 약자
    "S&P (Standard & Poor's)": None,
    "NICE신용평가(주)":    None,   # variant
    "LH공사":              None,   # 한국토지주택공사 variant
    "HPE":                 None,   # Hewlett Packard Enterprise (외국기업)
    "HPI":                 None,   # HP Inc. variant
    "KEPCO KPS PHILIPPINES CORP.": None,  # 한전KPS 해외법인
    "SAPEON Inc.":         None,   # NPU 팹리스 스타트업
    "SBVA":                None,   # 소프트뱅크 벤처스 아시아
    "SBI Nevada, Inc.":    None,   # 외국 금융법인
    "IDIS AMERICAS, Inc.": None,   # 아이디스 미주법인
    "ECOPRO HN HUNGARY Kft.": None,  # 에코프로 헝가리 법인
    "PT. HLI Green Power": None,   # 인도네시아 법인
    "TURBODEN":            None,   # 이탈리아 ORC 발전 기업
    "ORMAT":               None,   # 미국 지열 발전 (외국기업)
    "PGU Turkestan LLP":   None,   # 카자흐스탄 합작법인
    "KINLOCH ANDERSON":    None,   # 스코틀랜드 섬유 (외국기업)
    "Rivian":              None,   # 미국 EV (DART 미수록)
    "Walmart":             None,   # 미국 유통 (외국기업)
    "Verizon":             None,   # 미국 통신 (외국기업)
    "Janssen (얀센)":      None,   # J&J 자회사 (외국기업)
    "Repsol Petroleo":     None,   # 스페인 정유 (외국기업)
    "MITSUI & CO.,LTD":   None,   # 일본 미쓰이물산 (외국기업)
    "Nippon Dynawave Packaging co": None,
    "IDT Biologika":       None,   # 독일 바이오 CDMO
    "LivaNova":            None,   # 영국 의료기기 (외국기업)
    "Olink Proteomics":    None,   # 스웨덴 단백질 분석 (외국기업)
    "Pharma Two B":        None,   # 이스라엘 제약 (외국기업)
    "Ignis Therapeutics":  None,   # 외국 제약 스타트업
    "Iksuda":              None,   # 외국 바이오텍
    "KeyGen BioTECH":      None,   # 외국 바이오텍
    "Clarifai":            None,   # 미국 AI 스타트업
    "Artiabio":            None,   # 외국 바이오텍
    "Agent Vi":            None,   # 이스라엘 AI 영상분석
    "Genius":              None,   # 외국 기업 (모호)
    "EXERGY":              None,   # 외국 에너지 기업
    "INFINEUM":            None,   # Shell/ExxonMobil JV 윤활유 (외국기업)
    "Heraeus":             None,   # 독일 귀금속 (외국기업)
    "TAICA":               None,   # 일본 전자 소재 (외국기업)
    "Taconic":             None,   # 미국 PTFE 소재 (외국기업)
    "Sensirion":           None,   # 스위스 센서 (외국기업)
    "Wildwood Natural Foods": None,  # 미국 식품 (외국기업)
    "Bangladesh Bondhu Foundation": None,  # 방글라데시 NGO
    "Sinopharm Zhijun Pharmaceutical": None,  # 중국 제약 (외국기업)
    "Sanofi (Genzyme Corporation)": None,
    "Cristalia (크리스탈리아)": None,  # 브라질 제약 (외국기업)
    "SHENZHEN QINGLANG CLOTHES CO.,LTD": None,  # 중국 의류 (외국기업)
    "TIANMA":              None,   # 중국 패널 (이미 Tianma 있음)
    "Tianma":              None,   # variant
    "NOVATEK":             None,   # 러시아 LNG (외국기업)
    "LUK OIL":             None,   # 러시아 석유 (외국기업)
    "UMC":                 None,   # 대만 파운드리 (외국기업)
    "SanDisk":             None,   # WD 자회사 (외국기업)
    "MediaTek":            None,   # 대만 팹리스 (외국기업)
    "Largan":              None,   # 대만 렌즈 (외국기업)
    "Lenovo":              None,   # 중국 PC (외국기업)
    "MACOM":               None,   # 미국 반도체 (외국기업)
    "LAM Research":        None,   # 미국 반도체 장비 (외국기업)
    "STMicroelectronics":  None,   # 유럽 반도체 (외국기업)
    "Infineon":            None,   # 독일 반도체 (외국기업)
    "Toshiba":             None,   # 일본 전자 (외국기업)
    "Mitsumi":             None,   # 일본 전자부품 (외국기업)
    "TORAY":               None,   # 일본 화섬 (외국기업)
    "Thales":              None,   # 프랑스 방산 (외국기업)
    "VOLVO":               None,   # 스웨덴 차량 (외국기업)
    "Renault":             None,   # 프랑스 자동차 (외국기업)
    "Yamaha":              None,   # 일본 악기/오토바이 (외국기업)
    "P&W (Pratt & Whitney)": None, # 미국 항공엔진 (외국기업)
    "TOTAL LUBRICANTS":    None,   # 프랑스 TotalEnergies 자회사
    "TRINSEO":             None,   # 미국 화학 (외국기업)
    "Unity Technologies":  None,   # 미국 게임엔진 (외국기업)
    "Unity Ad":            None,   # Unity 광고 플랫폼
    "Siemens PLM Software": None,  # 지멘스 소프트웨어
    "Ruckus Networks":     None,   # 미국 네트워크 (외국기업)
    "Matrox":              None,   # 캐나다 영상처리 (외국기업)
    "Microsoft Azure":     None,   # MS 클라우드
    "SATO":                None,   # 일본 바코드 (외국기업)
    "Volkswagen (VW)":     None,   # 이미 Volkswagen 있음
    "CBLAB : 법무법인(유한)유앤아이(": None,  # 법무법인

    # ── 복합 표기 (여러 회사 묶음) ──
    "KT, LG U+":           None,   # 복수 기업 (모호)
    "SKT, KT, LGU+":       None,   # 복수 기업 (모호)
    "SKON 및 삼성SDI":      None,   # 복수 기업
}


# ──────────────────────────────────────────────────────────────────
#  비상장/외국기업 명단
#  (corp_code=None, is_listed=0)
# ──────────────────────────────────────────────────────────────────
UNLISTED_FOREIGN: set[str] = {
    # 미국 빅테크
    "Apple", "애플", "Apple (애플)", "Apple Inc",
    "Google", "구글", "Google LLC", "Google (구글)",
    "Microsoft", "마이크로소프트", "Microsoft Corporation",
    "Amazon", "아마존", "AWS",
    "Meta", "Facebook", "메타",
    "Tesla", "테슬라",
    "Netflix", "넷플릭스",
    "YouTube", "유튜브",
    "Alphabet",
    "Cisco", "시스코",
    "IBM",
    "VMware",
    "Broadcom",
    "Qualcomm", "퀄컴",
    "Intel", "인텔",
    "NVIDIA", "엔비디아",
    "AMD",
    "Micron",
    "NXP",
    # 유럽 기업
    "BASF",
    "Dow",
    "3M",
    "Siemens", "SIEMENS", "지멘스",
    "ABB",
    "Bosch", "보쉬",
    "Continental",
    "LVMH",
    "Gucci", "구찌",
    "Zara",
    "Shell",
    "Glencore",
    "Wartsila",
    "Lonza",
    # 독일 자동차
    "BMW",
    "Mercedes-Benz", "메르세데스",
    "Volkswagen", "폭스바겐",
    "Stellantis",
    # 미국 자동차
    "Ford", "FORD",
    "GM", "General Motors",
    "GM (General Motors)",
    "Lucid",
    # 일본 기업
    "Toyota", "토요타",
    "Honda", "혼다",
    "Sony", "소니", "SONY",
    "Panasonic", "파나소닉",
    "Sharp",
    "Mitsubishi", "MITSUBISHI",
    "SUMITOMO",
    "RENESAS",
    "Alps",
    "라쿠텐",
    # 중국 기업
    "Huawei", "화웨이",
    "Xiaomi", "샤오미",
    "BOE",
    "CSOT",
    "Visionox",
    "CATL",
    "BYD",
    # 기타 아시아
    "TSMC",
    "AUO",
    "Innolux",
    "Saudi Aramco",
    # 제약/의료
    "Roche", "로슈",
    "Pfizer", "화이자",
    "Medtronic",
    "AstraZeneca", "아스트라제네카",
    "Merck", "머크",
    "MSD",         # Merck Sharp & Dohme
    "GSK",
    "Novavax",
    "Boston Scientific",
    "Illumina", "일루미나",
    # 항공/방위
    "Boeing",
    "Airbus",
    # 기타
    "Juniper Networks",
    "Honeywell",
    "GE",
    "HP",
    "듀폰",
    "로레알",
    # 미국 기타 기업
    "Abbott",
    "Adobe",
    "Aercap",
    "Amphenol",
    "Arista Networks",
    "ARM",
    "Chevron",
    "Cisco Systems",
    "Cognex",
    "Costco",
    "Dell", "Dell Technologies",
    "DSM",
    "Daiichi Sankyo",   # 다이이치산쿄 (일본 제약)
    "Daimler",
    "Eli Lilly",
    "EMC",
    "Fitch",
    "GE",
    "Ghost Robotics",
    "Gorilla Technology",
    "HP Inc",
    "Honeywell",
    "Extreme Networks",
    "Juniper Networks",
    "Lonza",
    "NetApp",
    "Oracle",
    "오라클",
    "Qualys",
    "Raytheon",
    "ServiceNow",
    "Splunk",
    "Synopsys",
    "Thermo Fisher Scientific",
    "써모피셔",
    "Veeva",
    "Zebra Technologies",
    # 유럽 기타
    "Agfa Graphics",
    "Benteler",
    "Bio-Works Technologies AB",
    "Boehringer Ingelheim",
    "Bristol-Myers Squibb",
    "CSPC",
    "Cristalia",
    "DSM",
    "Dream S.A.S",
    "Eurofarma",
    "Fresenius",
    "Novartis",
    "노바티스",
    "Sanofi",
    "사노피",
    "UCB",
    # 일본 기타
    "Canon Tokki",      # 캐논토키
    "CSTI(일본)",
    "Daiichi Sankyo",
    # 중국/아시아 기타
    "ADNOC",            # UAE 국영 석유
    "AES",              # 미국 에너지
    "Aramco Trading Singapore Pte. Ltd.",
    "Ecogas Asia Ltd",
    "Global Fund",
    "DJI",              # 중국 드론
    # 기타 글로벌
    "Adobe",
    "AUDI",
    "Disney+",
    "General Motors (GM)",
    "Google AdMob",
    "Google Cloud Platform",
    "Google Cloud",
    # 국내 비상장 (MANUAL_EXACT 외 추가분)
    "삼성파운드리",
    "SK넥실리스",
    "현대오비스",
    "현대케피코",
    "에이치엔에스",
    "BC카드",
    "DB손해보험",
    "GS홈쇼핑",
    "CJ홈쇼핑",
    "롯데카드",
    "KB손해보험",
    "한화손해보험",
    "현대해상",
    "삼성화재해상",
    "LIG넥스원",       # 실제 상장사이지만 우리 DB 미수록
}


# ──────────────────────────────────────────────────────────────────
#  명시적 노이즈 목록
#  (기업명이 아닌 것이 확실한 항목)
# ──────────────────────────────────────────────────────────────────
KNOWN_NOISE: set[str] = {
    # ── 범용 사업 용어 ──
    "회사명", "경쟁요소", "주요고객", "기타", "국내외",
    "동종업체", "경쟁업체", "주요거래처", "수주처",
    "광고주", "광고대행사", "의료기관", "종합병원", "신용카드사",
    "자동차 제조사", "경쟁상황", "경쟁 상황",
    "관련법령 또는 정부의 규제 등", "국내 주요 건설사",
    "전방산업", "후방산업", "주요 경쟁사", "글로벌 기업",
    "가격경쟁력", "공급시장의 독과점 정도", "공급시장의독과점 정도",
    "공급의안정성", "독과점 정도", "독과점정도",
    "신용위험", "환위험", "유동성위험",
    "수주물량", "주요매입처", "판매경로",
    "자원조달의 특성", "자원조달상의 특성",
    "특수한 관계여부", "주주관계", "합작관계",
    "회사의 경쟁우위 요소", "기술력", "구체적 용도", "구체적용도",
    "가격 변동 추이", "당기말",

    # ── 비기업 범주 표현 ──
    "이동통신사", "통신사", "카드사", "은행", "발전사", "발전소",
    "조선소", "대형 건설사", "대형유통업체", "파운드리 업체",
    "스마트폰 제조사", "스마트폰 제조업체", "자동차 제조업체",
    "디스플레이 제조사", "국내 원재료 공급업체",
    "레미콘 공급업체", "외주 생산업체", "철근 공급업체",
    "OTT 플랫폼", "국내외 제약사", "국내 B사",
    "은행, 카드사 등 금융기관",

    # ── 제품/소재명 ──
    "ESS", "LCD", "OLED", "PC", "PCB", "PLATE", "RESIN", "PP",
    "노트북", "컴퓨터", "타이어", "알루미늄", "배터리",
    "가전제품", "화장품", "바인더", "합성수지",
    "합성사 및 화학 사업", "디스플레이", "스마트폰",
    "IC", "LTD", "AL", "Wire Type", "Package",
    "원부재료", "설비 등", "항공기", "에너지",

    # ── 일반어/문장 잔여 ──
    "더불어", "현대",  # "현대"만으로는 매우 모호
    "병·의원", "병원·의원", "국내 병원", "대학병원",
    "데이터센터", "사물인터넷", "소형화", "디자인", "마케팅",
    "직영점", "지하철", "아울렛",

    # ── 지역/국가명 ──
    "베트남", "동남아", "동남아시아", "싱가포르", "브라질",
    "인도네시아", "중국", "미국", "일본", "유럽",
    "국 내", "국내외",

    # ── 기타 ──
    "전력거래소",   # 공공기관이지만 파트너로 부적절
    "반도체 제조사", "제약회사", "글로벌 제약사",
    "아울렛", "외주가공업체",
    "산출기준",
    "매출채권은 다수의 거래처로 구성되어 있고",
    "L社", "S社",  # 익명 플레이스홀더
    "국내 대기업",
    "글로벌 스마트폰 제조사의 역량 및 스마트폰 판매 수량에 따라 이에 종속되는 공급사",

    # ── 추가 노이즈 (v2) ──
    "레미콘",           # 건설재료명
    "위생도기",         # 위생도기류 (제품명)
    "비금속 재료를 잘 투과하여 고해상도로 투시가 가능하기 때문에 비파괴",  # 문장
    "시스템 분석 및 설계",   # IT서비스 유형 설명
    "시스템 통합 및 구축",   # IT서비스 유형 설명

    # ── 추가 노이즈 (v3: 501-1000 범위) ──
    "AI단말", "CHIP", "CPU", "FPCB", "Display",  # 부품/기술명
    "Distributor",    # 유통 채널 일반명
    "BOX소요메이커",   # 부품 카테고리
    "ETRI",           # 이미 MANUAL_EXACT에 None으로 있지만 혹시 모를 중복 방지
    "GIS MAP 시스템",  # 솔루션명
    "Disney+",        # OTT (외국기업이지만 파트너로 부적절)

    # ── 추가 노이즈 (v4: none 345 정리) ──
    "IT", "INC", "SM", "VAN", "LM", "LED", "PTA", "TDI",  # 부품/소재명
    "IoT", "LPG 탱크 업체 별도", "Large Body", "LENS", "Power",  # 비기업
    "SBR 및 관련화학원료를 생산하는 광양 Cell",  # 공정 설명
    "USD)",  # 통화 단위
    "PE계열 합",  # 화학소재 카테고리
    "PHBA",  # 화학 약자
    "TUBE",  # 제품명
    "Sensor계",  # 센서 카테고리
    "Sole계",    # 소재 카테고리
    "CP그룹 회원사 APV",  # 내부 그룹 개념
    "MV) 고려아연 인근지역 생활고 해소를 위해 관련사 임직원과 함께 봉사활동을 수행하였으며",  # 문장
    "SERVE 및 관련내용은 대부분 MAKER(고객사",  # 문장
    "GS칼텍스주",  # 표기 오류 (GS칼텍스 → MANUAL_EXACT None)
}


# ──────────────────────────────────────────────────────────────────
#  메인 빌드
# ──────────────────────────────────────────────────────────────────
def build(top_n: int = 500, reset: bool = False):
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row

    if reset:
        db.execute("DELETE FROM partner_mapping")
        db.commit()
        print("partner_mapping 초기화 완료")

    # 보고 기업 corp_name → corp_code 역방향 인덱스
    corp_rows = db.execute(
        "SELECT DISTINCT corp_name, corp_code FROM supply_chain"
    ).fetchall()
    corp_to_code: dict[str, str] = {r["corp_name"]: r["corp_code"] for r in corp_rows}
    norm_to_code: dict[str, str] = {}
    for cn, cc in corp_to_code.items():
        n = normalize(cn)
        if n and n not in norm_to_code:
            norm_to_code[n] = cc

    # 대소문자 무관 비교용 집합
    _known_noise_lower  = {n.lower() for n in KNOWN_NOISE}
    _unlisted_lower     = {n.lower() for n in UNLISTED_FOREIGN}

    # 상위 N개 파트너 로드
    top_partners = db.execute(f"""
        SELECT partner_name, COUNT(*) as cnt, COUNT(DISTINCT corp_code) as reporters
        FROM supply_chain GROUP BY partner_name ORDER BY cnt DESC LIMIT {top_n}
    """).fetchall()

    stats = {"exact": 0, "norm": 0, "manual": 0, "partial": 0,
             "unlisted": 0, "noise": 0, "skip": 0, "none": 0}
    upserted = 0

    for row in top_partners:
        p = row["partner_name"]

        # 이미 매핑된 경우 스킵
        existing = db.execute(
            "SELECT 1 FROM partner_mapping WHERE partner_name = ?", [p]
        ).fetchone()
        if existing:
            stats["skip"] += 1
            continue

        corp_code = None
        corp_name = None
        match_type = "none"
        is_listed = 0
        note = None

        # 1. 명시적 노이즈 (대소문자 무관)
        if p in KNOWN_NOISE or p.lower() in _known_noise_lower:
            match_type = "noise"
            note = "정제 필요"

        # 2. 패턴 기반 노이즈 (자동 탐지)
        elif is_noise_pattern(p):
            match_type = "noise"
            note = "패턴 노이즈"

        # 3. 수동 사전
        elif p in MANUAL_EXACT:
            corp_code = MANUAL_EXACT[p]
            if corp_code is None:
                match_type = "unlisted"
                note = "비상장/외국기업/정부기관"
            else:
                match_type = "manual"
                is_listed = 1
                matched_names = [cn for cn, cc in corp_to_code.items() if cc == corp_code]
                corp_name = matched_names[0] if matched_names else None

        # 4. 비상장/외국기업 사전 (대소문자 무관)
        elif p in UNLISTED_FOREIGN or p.lower() in _unlisted_lower:
            match_type = "unlisted"
            note = "외국기업 또는 국내 비상장"

        # 5. 정확 매칭
        elif p in corp_to_code:
            corp_code = corp_to_code[p]
            corp_name = p
            match_type = "exact"
            is_listed = 1

        # 6. 정규화 매칭
        else:
            p_norm = normalize(p)
            if p_norm and p_norm in norm_to_code:
                corp_code = norm_to_code[p_norm]
                match_type = "norm"
                is_listed = 1
                corp_name = next((cn for cn, cc in corp_to_code.items() if cc == corp_code), None)
            else:
                # 부분 매칭 비활성화
                # 한국 기업명은 모회사 prefix를 자회사가 사용하는 경우가 많아
                # "포스코건설→포스코", "카카오모빌리티→카카오" 같은 오매핑 발생
                match_type = "none"

        db.execute("""
            INSERT OR IGNORE INTO partner_mapping
                (partner_name, corp_code, corp_name, match_type, is_listed, note)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [p, corp_code, corp_name, match_type, is_listed, note])
        stats[match_type if match_type in stats else "none"] += 1
        upserted += 1

    db.commit()
    db.close()

    total = sum(v for k, v in stats.items() if k != "skip")
    mapped = stats["exact"] + stats["norm"] + stats["manual"] + stats.get("partial", 0)
    print(f"\n=== partner_mapping 빌드 결과 ===")
    print(f"처리 대상 (상위 {top_n}개): {total + stats['skip']}개")
    print(f"신규 삽입:                {upserted}개")
    print(f"기존 스킵:                {stats['skip']}개")
    print()
    print(f"  정확 매칭:   {stats['exact']:4d}개")
    print(f"  정규화 매칭: {stats['norm']:4d}개")
    print(f"  수동 매칭:   {stats['manual']:4d}개")
    print(f"  비상장/외국: {stats['unlisted']:4d}개")
    print(f"  노이즈:      {stats['noise']:4d}개")
    print(f"  미매핑:      {stats['none']:4d}개")
    print()
    if total:
        print(f"  corp_code 매핑률: {mapped/total*100:.1f}%  ({mapped}/{total})")
        print(f"  분류 완료율:      {(total - stats['none'])/total*100:.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top",   type=int, default=500)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    build(top_n=args.top, reset=args.reset)
