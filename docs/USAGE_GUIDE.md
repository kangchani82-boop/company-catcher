# 📖 Company Catcher 사용 설명서

> **미션**: 한국 상장사 정보 격차를 줄인다.
> **수단**: DART 보고서 변화 → AI 분석 → 취재 단서 → 기사 → IR 질문지 → 발송 → 답장 학습

이 문서는 **"하나의 기사를 만들기 위한 최적의 경로"** 를 단계별로 설명합니다.
관리자 1명이 매일 30~60분 투자로 마감 시즌 100건+ 기사를 만들어내는 것이 목표입니다.

---

## 🗺 한눈에 보는 데이터 흐름

```
[DART 공시 발표]
   │  fetch_biz_content.py (사업의 내용 섹션 추출)
   ▼
[reports 테이블]
   │  batch_compare.py (Gemini로 두 기간 보고서 비교)
   ▼
[ai_comparisons]
   │  detect_leads.py (severity ≥3 변화점 추출)
   ▼
[story_leads]
   │  generate_draft.py (Gemini로 기사 초안 생성)
   ▼
[article_drafts]
   │  generate_questionnaire.py (질문 5~7개 생성)
   ▼
[ir_questionnaires]
   │  /questionnaire_review 에서 검수 → 승인
   │  📤 IR 메일 발송 (Gmail API)
   ▼
[ir_emails (direction='out')]
   │  IR 담당자 답장
   │  gmail_sync_inbox.py (자동 매칭)
   ▼
[ir_emails (direction='in')]
   │  learn_from_replies.py (자동)
   ▼
[ir_contacts 갱신 + corp_response_stats]
```

---

## 🚀 일일 운영 (5분)

매일 출근 직후 한 줄 실행:

```bash
python scripts/run_daily_q1_cycle.py
```

이 스크립트가 자동으로:
1. **신규 DART 공시 수집** (Q1 2026 분기보고서)
2. **사업의 내용 추출** (HTML/ZIP 파싱)
3. **vs 2025 연간 AI 비교** (Gemini Flash-Lite, 2워커 병렬)
4. **취재 단서 추출** (severity 3~5 변화)
5. **기사 초안 생성** (단서당 1건, Gemini)
6. **IR 질문지 생성** (기사당 질문 5~7개)
7. **회사별 뉴스 수집** (Naver News API, T1/T2 매체만)

소요 시간:
- 처음 실행: 15~20분 (전체 누적)
- 일일 증분: 3~10분 (신규분만 처리)

옵션:
- `--dry-run` : 무엇을 처리할지만 출력
- `--only-stats` : 현재 진척만 보기
- `--skip-news` : 뉴스 단계 스킵 (Naver API 한도 절약)
- `--skip-fetch` : DART fetch 스킵 (오늘 이미 받은 경우)

---

## 📋 페이지별 사용 가이드

### 1️⃣ 종합 대시보드 (`/`)

마감 시즌(2026 Q1)에는 상단에 **D-day 카운트다운 스트립**이 자동 노출됩니다:
- D-3 같은 카운트다운
- 오늘 vs 어제 (신규 공시·비교·단서·기사·질문지·발송·답장) 증감
- **검토 대기 / 승인 / 연락처 없음** 클릭하면 검토 페이지로

색상 의미:
- 🟣 보라색 = 평상시 (D-5+)
- 🟠 주황색 = D-3~5 (경고)
- 🔴 빨간색 = D-2 이내 (긴급)
- 🟢 초록색 = D-Day 이후

### 2️⃣ 비교분석 페이지 (`/comparisons`)

AI가 분석한 두 보고서의 변화 결과 목록.
- 카드 클릭 → 상세 분석
- **📄 작업** 버튼 → 이 비교가 시작된 보고서의 작업 흐름 (단서+기사+질문지+답장 한 화면)
- **🚫 제외** 버튼 → 기사 가치 없는 비교 숨김

### 3️⃣ 취재단서 페이지 (`/leads`)

severity로 정렬된 단서 목록.

**severity 의미:**
- `5` 🚨 사업 철수·중단, 대규모 손실 같은 critical event
- `4` ⚠ 시장 점유율 변화, 신규 진출, 리스크 강화
- `3` 📌 매출/이익 급변, 환경 규제 대처, 일반 수치 변화

단서 상세에서:
- **📄 작업 흐름** → 이 단서가 나온 회사의 보고서 타임라인
- **뉴스화** → 즉시 기사 초안 생성 (Gemini)

### 4️⃣ 기사초안 페이지 (`/articles`)

자동 생성된 기사 초안 목록.
- 카드 클릭 → 본문 + 외부 인용 검증 + IR 질문지
- **📄 흐름** → 이 회사 보고서 타임라인
- 사이드바: 검증 점수, 외부 인용, 기자 브리프

### 5️⃣ ⭐ 질문지 검토 (`/questionnaire_review`) — **가장 중요한 페이지**

**여기서 하루 30분 작업**:

#### 페이지 구조
- 좌측: 질문 6~7개 (인라인 편집 가능)
- 우측: 회사의 최근 30일 뉴스 사이드바 (자동 로딩)

#### 시각적 단서
| 표시 | 의미 |
|---|---|
| 🟢 초록 줄 | ✓승인 |
| 🟡 노란 줄 | ⏸보류 |
| 🔴 빨간 줄 (반투명) | ✕폐기 |
| 🔵 파란 줄 | 📤 발송됨 |
| 빗금 배경 + ⚠ IR연락처 없음 | 발송 불가 카드 |
| ↻ 24h내 발송 배지 | 같은 회사 24h 내 발송 이력 있음 |

#### 키보드 단축키 (편집창 외부에서)
| 키 | 동작 |
|---|---|
| `J` / `↓` | 다음 카드로 포커스 이동 |
| `K` / `↑` | 이전 카드 |
| `A` | 현재 카드 ✓승인 |
| `H` | 현재 카드 ⏸보류 |
| `D` | 현재 카드 ✕폐기 |
| `N` | 현재 카드 뉴스 사이드바로 스크롤 |
| `?` | 도움말 토글 |

#### 1-클릭 액션
- 사이드바 뉴스 아래 `Q1✗ Q2✗` 버튼 → "이 뉴스가 답함" 클릭 시 해당 질문 즉시 삭제
- 질문 옆 🗑 → 개별 질문 삭제
- `+ 질문 추가` → 새 질문 라인 추가 후 인라인 입력

#### 발송 (안전망 4중)
1. 검토 페이지 우상단 `📤 승인된 것 IR 메일 발송` 클릭
2. **확인 다이얼로그**: 회사명 5건 미리보기 + 일일 한도 + 자동 제외된 건수
3. 발송 진행 패널: 카드별 성공/실패 실시간 표시
4. **자동 보호**:
   - 일일 한도 (기본 400/일) 초과 시 즉시 중단
   - 24h 내 같은 회사 재발송 차단
   - IR연락처 없는 카드 자동 제외
   - Gmail rate limit (초당 1건 미만)

### 6️⃣ IR 인박스 (`/inbox`)

답장 모니터링 + IR 담당자 학습.

#### 워크플로
1. `🔄 새 메일 가져오기 + 학습` 클릭 → Gmail에서 최근 7일 INBOX 가져옴
2. 우리가 발송한 thread에 대한 답장만 매칭 (자동)
3. 매칭된 답장 → `ir_questionnaires.status='replied'` 자동 갱신
4. 답장 본문에서 **이름·직책·부서·전화** 자동 추출 (한국어 이름 패턴 + KOREAN_SURNAMES)
5. `ir_contacts` 테이블 신규 등록 또는 confidence 승급
6. 다음 발송 시 학습된 담당자 우선 사용

#### `📚 답장 재학습` 버튼
이미 받아둔 모든 답장에서 IR 담당자 정보 다시 학습 (idempotent).
- 신규 IR담당자 N건, 갱신 N건, 중복스킵 N건 결과 표시

#### 📊 학습 통계 (펼쳐보기)
- 발송한 메일 / 받은 답장 / 질문지 매칭
- 학습된 IR담당자 / 사용자 검증
- 회사별 응답통계 / 응답률 (%)

### 7️⃣ 📄 보고서 작업 카드 (`/report_work?id=N`)

**한 보고서 = 한 페이지**. 비교·단서·기사·질문지·답장 전체 흐름.

진입점:
- 비교 페이지의 📄 작업 버튼
- 회사 타임라인의 보고서 카드 클릭

사용 케이스:
- "이 회사 Q1 보고서 어떻게 처리됐는지 확인"
- "단서 → 기사 → 질문지 → 답장까지 한 화면에서 추적"

### 8️⃣ 🏢 회사 타임라인 (`/corp_work?corp_code=...`)

회사 하나가 매 분기/연간 어떻게 다뤄졌는지.
- 발간일 시간순 (최신 위)
- 각 보고서 카드: 비교·단서·기사·질문지 카운트
- 좌측 그라데이션 라인 + 활성 보고서는 글로우

진입점:
- 기사 카드 📄 흐름 / 기사 상세 사이드바
- 단서 상세 / 비교 상세 툴바
- 보고서 작업 카드 상단

---

## 🎯 기사를 만드는 최적 경로

> 다음은 **하루 30분으로 5~10건의 양질의 기사 초안을 발송 가능 상태로 만드는** 권장 경로입니다.

### Step 1 · 데이터 갱신 (자동)
```bash
python scripts/run_daily_q1_cycle.py
```
출근 후 한 번. 끝.

### Step 2 · 대시보드 확인 (1분)
- `/` 페이지
- 오늘 신규 질문지 N건 / 검토 대기 N건 / IR연락처 없음 N건 파악
- D-day 색상으로 긴급도 체감

### Step 3 · 검토 (대부분의 작업 시간 — 25분)
- `/questionnaire_review` 진입
- `pending` 필터 (기본값)
- 키보드 `J/K` 로 카드 이동
- 각 카드:
  1. 좌측 질문 6~7개 빠르게 읽기 (8초)
  2. 우측 사이드바 뉴스 헤드라인 스캔 (5초)
  3. 뉴스가 답해주는 질문은 `Q번✗` 클릭 (3초)
  4. 어색한 질문은 inline 클릭해서 편집 (15초)
  5. `A` 누르면 승인 (즉시)
- 폐기 기준 (`D` 키):
  - 헤드라인이 너무 추상적 ("일반 수치 변동" 같은 보일러플레이트)
  - 본문이 회사 sector와 무관한 기재 정정 (예: "사업보고서 기재정정 — 내용 변경 없음")
  - 회사가 너무 작아 답장 기대 어려움 (SPAC, 페이퍼컴퍼니)
- 보류 기준 (`H` 키): 흥미롭지만 사실 확인 필요한 케이스

### Step 4 · 발송 (2분)
- 검토 페이지 상단 `📤 승인된 것 IR 메일 발송` 클릭
- 다이얼로그 검토 (회사명 미리보기 + 한도 확인) → 확인
- 발송 패널 모니터링 (자동)
- 우상단 📮 quota pill 확인 — 일일 한도 대비 사용량

### Step 5 · 답장 확인 (시간 띄어서 5분 × 2~3회)
- `/inbox` 진입
- `🔄 새 메일 가져오기 + 학습` 클릭
- 매칭 N건, 학습 N건 표시
- 답장 본문 클릭 → 보고서 작업 카드로 이동 (해당 회사 전체 컨텍스트)

### Step 6 · 기사 출고 (답장 받은 회사부터)
- `/articles` 페이지 → status로 정렬
- 답장 내용을 본문에 인용 추가
- 외부 인용 검증 ([검증 점수] 배지 확인)
- 회사 사이드바에서 **📄 흐름** 클릭하면 분기별 시계열 데이터 한눈에

---

## 🛠 자주 쓰는 CLI

```bash
# 진척 확인만 (실행 없음)
python scripts/run_daily_q1_cycle.py --only-stats

# 신규 공시만 받고 멈춤
python scripts/run_daily_q1_cycle.py --skip-news

# 답장 통계
python scripts/learn_from_replies.py --stats

# 뉴스 재수집 (특정 회사)
python scripts/collect_news_for_questionnaires.py --corp-code 00257732 --force

# 발송 한도 변경 (Workspace 계정 등)
export GMAIL_DAILY_LIMIT=1500
python -m server  # 서버 재시작
```

---

## ⚠️ 안전 규칙

### 절대 하지 말 것
1. ❌ `/api/ir/send` 를 코드에서 직접 루프로 호출 (안전망 우회됨)
2. ❌ `ir_questionnaires.status='approved'` 를 SQL로 직접 일괄 변경 (검수 의미 상실)
3. ❌ IR 담당자 메일에 같은 내용 24h 내 재발송 (block 우회 시도 X)
4. ❌ 공휴일/주말 새벽에 발송 (한국 IR 업무 시간 9~18시 권장)

### 권장 규칙
1. ✅ 검토 → 승인 → 발송은 **반드시 검토 페이지에서**
2. ✅ 하루 발송은 50~100건 이내 권장 (답장 처리 capacity)
3. ✅ Gmail 발송 한도는 보수적으로 설정 (`GMAIL_DAILY_LIMIT=400`)
4. ✅ 답장 받으면 그 회사 기사 우선 출고 (정보의 시의성)

---

## 📊 DB 스키마 핵심 테이블

| 테이블 | 의미 | 핵심 컬럼 |
|---|---|---|
| `reports` | DART 보고서 (사업의 내용 포함) | corp_code, report_type, rcept_dt, biz_content |
| `ai_comparisons` | Gemini 비교 결과 | report_id_a, report_id_b, result, status |
| `story_leads` | severity ≥3 변화점 | lead_type, severity, comparison_id |
| `article_drafts` | 기사 초안 | headline, content, lead_id |
| `ir_questionnaires` | 발송용 질문지 | article_id, questions(JSON), status, sent_at |
| `ir_contacts` | IR 담당자 | corp_code, ir_email, confidence, source |
| `ir_emails` | 메일 송수신 이력 | direction, gmail_thread_id, questionnaire_id |
| `external_sources` | 회사별 뉴스 (사이드바) | related_corp_code, outlet_tier, published_at |
| `corp_response_stats` | 회사별 응답성 | sent_count, replied_count, avg_response_days |

---

## 🆘 트러블슈팅

### "발송 가능한 IR 담당자 없음" 에러
- `/ir_contacts` 페이지에서 해당 회사 검색
- 수동으로 IR 이메일 등록 + `user_verified=1` 체크

### Gemini 429 (한도 초과)
- 다음날 자동 복구
- 또는 `--model flash-lite` 로 변경 (1,000 RPD/key, 2키 합산 2,000 RPD)

### Naver News 검색 결과 0건
- 회사명이 너무 짧으면 strict 필터로 제외됨 (정상)
- `--force` 로 재수집 가능

### 답장이 매칭 안 됨
- `gmail_thread_id` 가 보존되어야 매칭. 답장이 새 thread로 오면 매칭 X
- 발신 시 `In-Reply-To` 헤더가 유지되는지 확인

---

## 🔄 마감 시즌 D-7 ~ D-Day 추천 루틴

| D-day | 작업 |
|---|---|
| D-7 | `run_daily_q1_cycle.py` 매일 아침 / 검토 30분 / 발송 20건 |
| D-5 | 위와 동일 + IR 담당자 비등록 회사 보강 (`/ir_contacts`) |
| D-3 | 사이클 + 검토 + 발송 (50건+) |
| D-1 | **마감 직전 폭주** — 사이클 2회 (오전·오후) + 일괄 발송 |
| D-Day | 답장 우선 처리. 추가 발송 자제. |
| D+1~ | 답장 학습 + 기사 출고 집중 |

---

## 💡 핵심 가치 한 문장

> "Gemini가 보고서 변화를 잡아내고 — 사용자가 30초로 질문을 검토하면 — Gmail이 IR에 정확히 보내고 — 답장이 다음 발송을 더 똑똑하게 만든다."

이 사이클이 매일 반복되는 것이 시스템의 핵심입니다.

---

**문서 버전**: 1.0 (2026-05-11)
**문의/이슈**: kangchani82@gmail.com
