# 🔄 HANDOFF — Company Catcher 작업 인수인계

> **새 채팅에서 이 파일을 먼저 읽고 즉시 이어서 작업하기 위한 종합 인수인계서**
>
> 작성일: 2026-04-26 (자정~새벽)
> 이전 채팅 컨텍스트 사용량: 87% — 새 채팅 전환 필요

---

# 🎯 사명 (절대 잊지 말 것)

> **"한국 상장사 정보 격차를 줄인다.**
> **변화 인지 → 진짜 팩트 기반 양질 기사로 → 가능한 많은 기업 노출"**

---

# ⚠️ 절대 원칙 — 비교 분석 방향

```
★ 가장 최신 보고서가 기준점이다.
★ "최신 사업보고서가 과거 분기보고서 대비 어떻게 달라졌나" 회고형.
★ NEW (최신에 새로 등장) / REMOVED (최신에서 사라짐) /
   EXPANDED / SHRUNK / CHANGED 라벨 5종 강제.
★ 답변은 무조건 한국어. 영어 답변은 자동 폐기.
```

**보고서 페어링 룰 (시점에 따라 자동 변경):**
- 2026-04 (현재): 2025_annual(최신) vs 2025_q1(과거)
- 2026-05+ (2026_q1 발행): 2026_q1(최신) vs 2025_annual(과거)
- 2026-08+ (2026_h1): 2026_h1(최신) vs 2026_q1(과거)
- 2027-03+ (2026_annual): 2026_annual(최신) vs 2026_q3(과거)

---

# 🚨 현재 진행 상태 (2026-04-30 야간 자동 작업 완료)

## ✅ 완료된 Phase

### Phase 1: ai_comparisons 재처리 — **완료** (99.96%)
- ✅ 2,699 / 2,700 (잔여 1건 무시 가능)

### Phase 2: 단서 추출 — **완료**
- ✅ story_leads 812건 / lead_novelty 812건 / lead_change_class 812건
- ✅ external_sources 1,242건 / lead_external_match 250건
- ✅ company_info_gap 3,242건

### Phase 3: 기사 초안 — **진행 중** (17.9%)
- ✅ article_drafts: **145건** / 812단서
- ✅ article_fact_cards: 23건 (v3 REPORTER)
- ✅ reporter_briefs: 23건

### Phase 4-1: cross_signals — **완료**
- ✅ 56건

### Phase 5: 대시보드 UX — **완료**
- ✅ 섹션 재배치, nav 추가, 교차신호 클릭, 라벨 명확화

---

## 🆕 IR 자동화 시스템 — 야간 작업 완료

### Phase B (IR 담당자 수집) — **거의 완료**
- ✅ `companies` 테이블: 1,546 → **4,091개** (DART corpCode.xml 보강)
- ✅ `ir_contacts` 테이블: **2,572건 / 992개 회사**
- ✅ Gmail OAuth 인증 + 보도자료 자동 추출 v5
- ✅ **story_leads 회사 709개 중 620개 (87.4%)에서 IR 이메일 확보**

| 출처 | contact | 회사 |
|---|---|---|
| PRESS_RELEASE (Gmail v5) | 840 | 370 |
| DOMAIN_GUESS (추정) | 1,422 | 474 |
| HOMEPAGE (실제) | 234 | 143 |
| DART_HOMEPAGE_ONLY | 76 | 76 |

**즉시 발송 가능: 449개 회사** (검증된 이메일 보유)

### Phase A, C, D, E — 아직 미시작
- A. 질문지 자동 생성 (`generate_questionnaire.py`)
- C. Gmail 발송 (`gmail_send.py`)
- D. 인박스 통합 (`gmail_sync.py`)
- E. 답변 기반 최종 기사

---

## 🎯 사용자 결정 필요 — 다음 우선순위

| 옵션 | 내용 | 소요 |
|---|---|---|
| **1** | 사이트 내 ir_contacts.html UI (1,422 추정 검증) | 1일 |
| **2** | Phase A 질문지 생성 (Gemini 활용) | 1일 |
| **3** | Phase 3 기사 초안 추가 확장 (145 → 300+) | API 여력 |
| **4** | Gmail 발송 모듈 (실제 IR에 발송) | 0.5일 |
| **5** | Phase 4-2 supply_chain_news --build-all | API 여력 |

**상세 리포트**: `IR_EXTRACTION_LOG.md` 참고

---

# 🗄 DB 상태 (작업 시작 직전 스냅샷)

## ✅ 보존 (영향 없음)
| 테이블 | 행 수 | 비고 |
|---|---:|---|
| reports | 12,825 | DART 원문 |
| companies | 1,546 | |
| financials | 5,598 | |
| supply_chain | 11,264 | 별도 추출 — 영향 없음 |
| partner_mapping | 1,053 | |
| external_sources | 826 | Naver 보도 |
| event_disclosures | 8,510 | DART 수시공시 |
| event_leads | 4,320 | |
| company_info_gap | 3,242 | 정보 격차 점수 |
| alert_rules | 16 | YAML 규칙 |

## 🗑 폐기됨 (재생성 필요)
| 테이블 | 이전 → 현재 | 재생성 시점 |
|---|---|---|
| ai_comparisons | 1,932 → 27 (작업 중) | Phase 1 진행 중 |
| story_leads | 505 → 0 | Phase 2 |
| article_drafts | 324 → 0 | Phase 3 |
| lead_novelty | 505 → 0 | Phase 2 |
| lead_change_class | 505 → 0 | Phase 2 |
| lead_external_match | 643 → 0 | Phase 2 |
| article_verification | 292 → 0 | Phase 3 |
| article_fact_cards | 20 → 0 | Phase 3 |
| reporter_briefs | 20 → 0 | Phase 3 |
| article_external_refs | 54 → 0 | Phase 3 |
| cross_signals | 44 → 0 | Phase 3 후 |
| supply_chain_leads | 228 → 0 | Phase 3 후 |

## 📦 백업 위치
- `data/backups/dart_reports_PRE_RESET_20260426_1935.db` (1.07GB) — **작업 완료 후 삭제 예정**
- GitHub 최신 커밋: `69840b3` (https://github.com/kangchani82-boop/company-catcher.git)

---

# 📅 진행 단계 (순차 — 동시 진행 금지)

```
[Phase 1] ai_comparisons 1,932건 재처리 ★ 현재 진행 중
    ↓ (완료까지 대기)
[Phase 2] 단서 재추출
    - detect_leads.py
    - novelty_filter.py --build
    - classify_change_type.py --build
    - collect_kr_news.py + match_lead_sources.py
    ↓
[Phase 3] 기사 재생성
    - generate_draft.py (v2.2 STRAIGHT + v3 REPORTER)
    - 검증 (verify_articles, article_postprocess)
    ↓
[Phase 4] 후속 시스템 재빌드
    - supply_chain_news.py --build-all
    - cross_signals.py --build-all
    ↓
[Phase 5] 대시보드 UX 보완
    - home_v2.html 재배치 (KPI → 콘셉 → 단서 → 교차신호 맨 아래)
    - 교차신호 카드 클릭 링크
    - nav에 종합 대시보드 돌아오는 링크
```

---

# 📂 핵심 파일 위치

## 프롬프트
- `scripts/batch_compare.py` (line 392) — `build_prompt()` 시간 회고형 v3
- `scripts/generate_draft.py` — `_build_prompt_default/change/risk` (v2.2 STRAIGHT) + `_build_prompt_reporter_v3` (v3 REPORTER)

## 핵심 스크립트
| 파일 | 역할 |
|---|---|
| `scripts/batch_compare.py` | 보고서 비교 (Gemini Flash) |
| `scripts/detect_leads.py` | 단서 자동 추출 |
| `scripts/novelty_filter.py` | 새로움 검증 (NEW/REMOVED 등) |
| `scripts/classify_change_type.py` | 변화 유형 + 산업 vs 개별 |
| `scripts/info_gap_score.py` | 정보 격차 점수 (Phase T) |
| `scripts/collect_kr_news.py` | Naver 외부 보도 수집 |
| `scripts/match_lead_sources.py` | 단서-외부자료 매칭 |
| `scripts/generate_draft.py` | 기사 초안 생성 (v2.2 + v3) |
| `scripts/verify_articles.py` | 4차원 검증 |
| `scripts/article_postprocess.py` | 헤드라인 합일치 + 추측어 검출 |
| `scripts/cite_verify.py` | 외부 미디어 인용 사실 대조 |
| `scripts/supply_chain_news.py` | 공급망 8 시나리오 |
| `scripts/cross_signals.py` | 교차 신호 8 패턴 |
| `scripts/collect_event_disclosures.py` | DART 수시공시 |
| `scripts/report_pair_resolver.py` | 비교 페어링 자동화 |

## 웹
| 파일 | 경로 |
|---|---|
| `web/home_v2.html` | 종합 대시보드 (`/home_v2`) |
| `web/articles.html` | 기사 목록 (`/articles`) |
| `web/leads.html` | 단서 목록 (`/leads`) |
| `web/supply_chain_leads.html` | 공급망 단서 |
| `web/dart_viewer.html` | 기존 DART 뷰어 (보존) |

## 서버
- `server.py` 핵심 API:
  - `/api/dashboard` — 홈 통합
  - `/api/articles?sort=verify_high|recent|char_count`
  - `/api/articles/{id}/verification`
  - `/api/articles/{id}/external_refs`
  - `/api/articles/{id}/fact_card` (v3)
  - `/api/articles/{id}/reporter_brief` (v3)
  - `/api/leads/{id}/external_pool`
  - `/api/supply-chain-leads`
  - `/api/cross-signals`

---

# 🚧 사용자가 강조한 사항

## 비교 분석 (★ 가장 중요)
> "어떤 정도든 데이터 발간일이 가장 최신일이 되며 그것을 기준으로 과거 대비 변화한 것을 찾는 것"
> "무조건 최신 보고서가 과거랑 뭐가 달라졌나임"
> "모든 뉴스 정보는 최신을 기준으로 하는거야"

## 작업 순서
> "비교 분석 우선해서 끝내고, 다 끝났다 그러면 취재 단서 다음 작업.
> 그다음 기사 초안 작업. 각각 동일한 작업 마치고 나서 그 다음 다른 걸로"

## 한국어
> "다 한글로 되야지 그것도 다 바로 잡자"

## 진행 방식
> "다시 플래닝 짜서 실행하지 말고 허락 받고 해"

## 무료 API 우선
> "최대한 토큰을 많이 쓰지 않고 무료 API를 활용하는 방안 유지"
> "유료 API 꼭 사용해야하는 부분은 상의"

---

# 🔧 v2.2 STRAIGHT vs v3 REPORTER

## v2.2 STRAIGHT (단신·즉시 출고)
- 5단락 / 1,000~1,200자
- 회사 통화 X — 공시만으로 출고 가능
- 평가어 절대 금지 ('긍정적', '우려', '주목', '기대' 등)
- 외부 미디어 인용 원칙 금지 (예외: 내용 부족 시 T1 매체 1건)
- 종결: '~확인됐다', '~기록했다', '~밝혔다'

## v3 REPORTER (심층 기획)
- 7단락 + Fact Card + Reporter Brief
- 1,800~2,400자
- STEP 6 (핵심 질문) — 회사 IR에 물어볼 것
- STEP 7 (다음 체크포인트) — 후속 취재 시점
- IR 통화 질문 5개 자동 생성
- CLI: `--reporter-mode` 플래그

---

# 🌐 시스템 구축 완료 영역 (재빌드 시 다 활용 가능)

## Phase T (정보 격차 점수)
- 3,242개사 점수 산정
- 보도량(40%) + 시총(20%) + 기사(20%) + 공급망허브(20%)
- 등급: 🚨(90+) / ⭐(70-89) / 📰(50-69) / 🌟(30-49) / 📺(~29)

## Phase O (새로움 검증)
- 키워드 시계열 등장 추적
- AI 분석 NEW 표현 검출
- Evidence 과거 보고서 매칭

## Phase P (v2.2 STRAIGHT 재정의)
- 5단락 단신 / 평가어 차단

## Phase R (과거 보고서 컨텍스트)
- generate_draft에서 자동 주입

## Phase Q (변화 유형 + 산업 vs 개별)
- NEW/REMOVED/EXPANDED 라벨
- 산업 5개사+ 동시 변화 → INDUSTRY 트렌드

## Phase N (비교 페어링 자동화)
- `scripts/report_pair_resolver.py`
- 시기 자동 전환 룰

## 공급망 시나리오 (8가지)
- HUB / DEPENDENCE / CLUSTER / VERTICAL / GLOBAL / INDUSTRY / NEW_ENTRANT / IMPACT

## 교차 신호 (Phase Cross — 8가지)
- CONCENTRATION_SHIFT / SUPPLY_CHAIN_RIPPLE / NEW_BUSINESS_FUNDING /
  INDUSTRY_WAVE / HIDDEN_CHANGE / GLOBAL_RISK_TRANSFER /
  GROUP_RESTRUCTURING / OWNERSHIP_DECISION_SIGNAL

---

# 🐛 알려진 이슈 (다음 작업 시 주의)

## 1. 영어 응답 변덕
- Gemini Flash가 가끔 한국어 → 영어로 변덕
- **자동 폐기 로직 추가됨** (3회 재시도, 다른 모델·키)
- 그래도 폐기 안 된 건은 status='error'로 마킹

## 2. 대시보드 UX 미반영 항목 (Phase 5)
- 교차신호 카드 클릭 시 액션 없음 → 링크 추가 필요
- 메뉴에서 종합 대시보드(/home_v2)로 돌아오는 링크 없음
- 교차신호 위치 — 사용자 요청: 맨 아래로 이동
- KPI → 콘셉별 빠른 진입 → 단서 → 교차신호 순으로 재배치

## 3. 콘셉별 카드 링크 일부 끊김
- 일부 카드가 잘못된 페이지로 이동 — 점검 필요

## 4. event_leads 공시 단서
- 사용자 지시: "수시공시 자체로는 기사화 가치 없음. 여러 정보 조합으로 새로운 해석 낳을 때 가치"
- → cross_signals 시스템에서 활용. 단독 기사화 X.

---

# 🔑 환경

## API 키 (.env)
- `GEMINI_API_KEY`, `GEMINI_API_KEY_2` — Flash 무료
- `ANTHROPIC_API_KEY` — Claude (사용자 허락 필수)
- `DART_API_KEY` — DART 수시공시
- `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET` — Naver News

## 서버 PID 추적
- 새 채팅에서 `Get-Process python | Where-Object {...}` 로 확인

---

# 📋 새 채팅 즉시 액션 가이드

## 1️⃣ 먼저 확인할 것
```bash
# 백그라운드 task 진척
tail -30 [백그라운드 출력 파일 경로]

# DB 현재 상태
python -c "
import sqlite3
conn = sqlite3.connect('data/dart/dart_reports.db')
n_ok = conn.execute('SELECT COUNT(*) FROM ai_comparisons WHERE status=\"ok\"').fetchone()[0]
n_err = conn.execute('SELECT COUNT(*) FROM ai_comparisons WHERE status=\"error\"').fetchone()[0]
print(f'ai_comparisons: ok {n_ok} / error {n_err} / total 2,700 분석 가능')
"
```

## 2️⃣ 질문할 것 (사용자에게)
- "Phase 1 (재처리) 진척 ___% 입니다. 계속 모니터링할까요, 아니면 다른 작업 시작할까요?"

## 3️⃣ Phase 1 완료 후 자동 진행
```bash
# 1) 영어 폐기된 건 재처리
python scripts/batch_compare.py --all  # status='error' 자동 재시도

# 2) 단서 재추출 (Phase 2)
python scripts/detect_leads.py
python scripts/novelty_filter.py --build
python scripts/classify_change_type.py --build
python scripts/info_gap_score.py --build  # 정보격차 재계산
python scripts/collect_kr_news.py --severity 4 --limit 90
python scripts/match_lead_sources.py --severity 4

# 3) 기사 재생성 (Phase 3) — 사용자 확인 후
python scripts/generate_draft.py --severity 4 --limit 50          # v2.2 STRAIGHT
python scripts/generate_draft.py --severity 5 --reporter-mode --limit 30  # v3 REPORTER

# 4) 교차 신호 재빌드 (Phase 4)
python scripts/supply_chain_news.py --build-all
python scripts/cross_signals.py --build-all

# 5) 대시보드 UX 수정 (Phase 5)
# - home_v2.html 섹션 순서: KPI → 콘셉 → 단서 → 교차신호 맨 아래
# - nav에 /home_v2 링크 추가
# - 교차신호 카드 클릭 → 단서/기사 상세로
```

---

# 💬 마지막 메시지 (사용자)

> "그렇게 할건데 지금 이번 채팅에서 컨텍스트 87% 가량 써서 처리가 늦어지는게 아닌가 싶은데. 맞아??
> 만약에 그렇다면 방안을 마련해줘. 새채팅으로 가서 현재 작업 이어서 한다 던지 MD 파일 적용 등"

→ **이 HANDOFF.md가 그 답.**

새 채팅에서:
1. 이 파일 먼저 읽음
2. 백그라운드 task 진척 확인
3. 사용자 질문에 따라 Phase 2~5 순차 진행

---

**🤖 인수인계 완료. 새 채팅에서 이 파일 읽고 즉시 이어가세요.**
