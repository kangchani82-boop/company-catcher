"""
config/gemini_models.py
────────────────────────
Gemini 모델 fallback 체인 및 무료 API 한도 — 단일 진실 소스(SSOT).

정책 변경 시 이 파일 한 곳만 수정하면 server.py / scripts/*가 모두 반영됨.

2026-05 기준 정책:
  - 1순위: gemini-flash-latest / gemini-flash-lite-latest (latest alias)
           → Google이 알아서 최신 GA로 라우팅. 다음 정책 변경에도 강함.
  - 2~3순위: RPD 큰 모델 (대량 배치 대응)
  - 4~5순위: GA 안정 모델
  - 최후: pro (한도 작지만 품질 최고)

각 모델의 무료 티어 한도 (2026-05 기준):
  Gemini Flash-Latest    : 30 RPM / 1,000 RPD  ← alias
  Gemini 2.5 Flash-Lite  : 30 RPM / 1,000 RPD  ← 가장 빠름
  Gemini 3-Flash-Preview : 15 RPM / 1,500 RPD  ← 가장 큰 RPD
  Gemini 3.1 Flash-Lite  : 15 RPM / 1,000 RPD
  Gemini 2.5 Flash       : 10 RPM /   250 RPD
  Gemini 2.5 Pro         :  2 RPM /    25 RPD
"""

# ─── 무료 API 일일 한도 ──────────────────────────────────────────────────────
GEMINI_LIMITS = {
    "flash":      {"rpm": 10, "rpd": 500},
    "flash-lite": {"rpm": 15, "rpd": 1000},
    "pro":        {"rpm":  5, "rpd": 100},
    "article":    {"rpm": 10, "rpd": 100},
}

# ─── 모델 카테고리별 fallback 체인 (404/429 자동 시도) ──────────────────
# 우선순위: latest alias → RPD 큰 모델 → 안정 GA → pro 최후
GEMINI_FALLBACKS = {
    "flash": [
        "gemini-flash-latest",       # 1순위: Google 관리 alias
        "gemini-2.5-flash-lite",     # 2순위: 30 RPM, 1,000 RPD
        "gemini-3-flash-preview",    # 3순위: 1,500 RPD (가장 큼)
        "gemini-3.1-flash-lite",     # 4순위: 1,000 RPD
        "gemini-2.5-flash",          # 5순위: 250 RPD (한도 작음)
        "gemini-2.5-pro",            # 최후
    ],
    "flash-lite": [
        "gemini-flash-lite-latest",
        "gemini-2.5-flash-lite",
        "gemini-3.1-flash-lite",
        "gemini-3-flash-preview",
        "gemini-2.5-flash",
        "gemini-2.5-pro",
    ],
    "pro": [
        "gemini-2.5-pro",            # 1순위: 안정 GA
        "gemini-3-pro-preview",
        "gemini-3.1-pro-preview",
        "gemini-flash-latest",       # 쿼터 초과 시 우회
        "gemini-2.5-flash",
    ],
    "article": [
        "gemini-2.5-pro",            # 1순위: 기사 품질
        "gemini-flash-latest",
        "gemini-3-flash-preview",
        "gemini-2.5-flash-lite",
        "gemini-2.5-flash",
    ],
}

# ─── 호출 간 권장 딜레이 (초) — RPM 기반 ───────────────────────────────────
GEMINI_DELAYS = {
    "flash":      6.5,
    "flash-lite": 4.5,
    "pro":        12.5,
    "article":    6.5,
}

# ─── 쿼터 초과 시 카테고리 우회 ───────────────────────────────────────────
FALLBACK_ON_QUOTA = {
    "flash": "flash-lite",
}

# ─── 공급망 관계 추출 (Gemma 폴백 포함) ──────────────────────────────────
SUPPLY_CHAIN_EXTRACT_MODELS = [
    "gemma-3-27b-it",
    "gemma-3-12b-it",
    "gemini-flash-latest",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
]

# ─── 공급망 노이즈 검증 (배치) ────────────────────────────────────────────
SUPPLY_CHAIN_VALIDATE_MODELS = [
    "gemma-3-27b-it",
    "gemini-flash-latest",
    "gemini-2.5-flash",
    "gemma-3-12b-it",
    "gemini-2.5-flash-lite",
]
