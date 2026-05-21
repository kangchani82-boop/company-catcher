# 📋 IR 자동화 시스템 — 구현 플랜 (v2)

> **목표**: 비교 분석에서 도출된 변화 → 자동 질문지 생성 → IR 담당자에게 이메일 발송 → 답장 수신 → 답변 기반 최종 기사화. 사이트 안에서 모두 처리.
>
> 작성일: 2026-04-29 (v2 업데이트: 샘플 검증 + 코스닥협회 파서 + 수동 보강 정책)
> 발신 이메일: `kjm@finance-scope.com`
> Gmail 수집 메일함: `fin@finance-scope.com` (4,758건 메시지)

---

## 🚦 현재 진척 (2026-04-29)

| 항목 | 상태 |
|---|---|
| Google Cloud 프로젝트 | ✅ `company-catcher-mail` 생성 |
| Gmail API 활성화 | ✅ |
| OAuth 클라이언트 ID 발급 | ✅ Desktop 타입 |
| `secrets/credentials.json` | ✅ 저장됨 |
| `secrets/token.json` | ✅ OAuth 인증 완료 (fin@finance-scope.com) |
| `gmail_setup.py` | ✅ 검증 (4,758 메시지 / 4,257 스레드 확인) |
| `ir_contacts` 테이블 | ✅ 생성됨 |
| `collect_ir_contacts.py` (홈페이지) | ✅ 구현·검증 (50개사 샘플) |
| `extract_ir_from_gmail.py` v1 | ✅ 구현 (50건 샘플 → 매칭률 14%) |
| **v2 개선 작업** | 🟡 진행 중 (이 문서 기준) |

**v1 → v2 발견된 개선점:**
- 발신자 이름이 사람 이름인 경우 매칭 실패 → **도메인 우선 매칭으로 변경**
- IR/PR 대행사 메일 다수 발견 → **화이트리스트 + 본문 추출**
- 코스닥협회 정형 보도자료 → **별도 파서 신규**
- 이름·직책 추출 오작동 ("대표 이사", "아무런 책임") → **한국 성씨 필터**
- 이메일 없는 contact 폐기 → **이름+전화만으로도 등록 허용**

---

## 🎯 전체 워크플로

```
[기존 완료]                            [신규 추가]
공시 보고서 분석                          ↓
  ↓                                  ┌─ A. 질문지 자동 생성
비교 분석 (NEW/REMOVED/...)            │   - 변화 라벨별 템플릿
  ↓                                  │   - 회사가 답해야 할 것 5~10개
취재 단서 (story_leads)                │
  ↓                                  ├─ B. IR 담당자 이메일 수집
1차 기사 (article_drafts)             │   - KRX KIND, DART, 회사 홈페이지
  ↓                                  │
  ──────────────────────────────────┼─ C. 이메일 발송 (Gmail API)
                                     │   - 사이트에서 1클릭 발송
                                     │   - kjm@finance-scope.com 발신
                                     │
                                     ├─ D. 답장 수신 + 사이트 인박스
                                     │   - Gmail API 폴링
                                     │   - thread_id 자동 매칭
                                     │
                                     └─ E. 답변 기반 최종 기사
                                         - 답변 추출 → 기사 업데이트
                                         - 최종 기사 자동 재생성
```

---

## 🗄 신규 DB 스키마

```sql
-- 1. IR 담당자 정보 (한 회사에 여러 명 가능)
CREATE TABLE ir_contacts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    corp_code           TEXT NOT NULL,           -- 'UNKNOWN' 허용 (매칭 미완)
    corp_name           TEXT,
    ir_email            TEXT,                    -- NULL/빈 문자열 허용 (이름+전화만으로도 OK)
    ir_email_secondary  TEXT,                    -- 보조 이메일 (예: 부서 공용)
    ir_phone            TEXT,                    -- 일반전화
    ir_mobile           TEXT,                    -- 휴대폰
    ir_name             TEXT,
    ir_dept             TEXT,                    -- 'IR팀', '홍보팀' 등
    ir_title            TEXT,                    -- '부장', '팀장' 등
    homepage            TEXT,
    source              TEXT,                    -- 'PRESS_RELEASE' / 'KOSDAQ_PRESS' / 'HOMEPAGE' / 'DOMAIN_GUESS' / 'MANUAL'
    source_url          TEXT,                    -- gmail msg_id, 홈페이지 URL 등
    confidence          TEXT,                    -- 'A_verified' / 'A_complete' / 'A_email_only' / 'B_extracted' / 'B_phone_only' / 'C_homepage' / 'C_guessed' / 'D_manual'
    mx_verified         INTEGER DEFAULT 0,       -- 도메인 MX 검증
    user_verified       INTEGER DEFAULT 0,       -- 사용자 수동 검증
    bounced_count       INTEGER DEFAULT 0,       -- 반송 누적 — 3 이상이면 자동 비활성
    is_active           INTEGER DEFAULT 1,       -- 활성/비활성
    notes               TEXT,
    created_at          TEXT,
    updated_at          TEXT,
    last_used_at        TEXT,
    UNIQUE(corp_code, ir_email)                  -- 같은 회사+이메일 중복 방지 (이메일 NULL이면 다중 허용)
);
CREATE INDEX idx_ir_contacts_corp ON ir_contacts(corp_code);
CREATE INDEX idx_ir_contacts_email ON ir_contacts(ir_email);
CREATE INDEX idx_ir_contacts_active ON ir_contacts(is_active, user_verified);
CREATE INDEX idx_ir_contacts_conf ON ir_contacts(confidence);

-- 2. 질문지
CREATE TABLE ir_questionnaires (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id    INTEGER REFERENCES article_drafts(id),
    lead_id       INTEGER REFERENCES story_leads(id),
    corp_code     TEXT,
    corp_name     TEXT,
    questions     TEXT,           -- JSON: [{q, why, expected_format}]
    cover_letter  TEXT,           -- 인사말·기자 소개
    status        TEXT DEFAULT 'pending',  -- pending/sent/replied/archived
    created_at    TEXT,
    sent_at       TEXT,
    replied_at    TEXT
);
CREATE INDEX idx_qst_article ON ir_questionnaires(article_id);
CREATE INDEX idx_qst_status  ON ir_questionnaires(status);

-- 3. 이메일 송수신 이력
CREATE TABLE ir_emails (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    questionnaire_id INTEGER REFERENCES ir_questionnaires(id),
    direction       TEXT,        -- 'out' / 'in'
    gmail_msg_id    TEXT UNIQUE,
    gmail_thread_id TEXT,
    from_addr       TEXT,
    to_addr         TEXT,
    cc              TEXT,
    subject         TEXT,
    body_text       TEXT,
    body_html       TEXT,
    attachments     TEXT,        -- JSON: [{filename, gmail_attach_id, size}]
    sent_at         TEXT,
    received_at     TEXT,
    is_read         INTEGER DEFAULT 0
);
CREATE INDEX idx_email_thread ON ir_emails(gmail_thread_id);
CREATE INDEX idx_email_qst    ON ir_emails(questionnaire_id);

-- 4. 답변 → 기사 매칭
CREATE TABLE ir_responses (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id            INTEGER REFERENCES ir_emails(id),
    questionnaire_id    INTEGER REFERENCES ir_questionnaires(id),
    parsed_answers      TEXT,    -- JSON: 질문별 답변 추출 결과
    used_in_article_id  INTEGER REFERENCES article_drafts(id),
    final_article_id    INTEGER REFERENCES article_drafts(id),
    processed_at        TEXT
);
```

---

## 📦 Phase 별 상세 플랜

### Phase A — 질문지 자동 생성 (1일)

**목표**: 모든 article_draft에 5~10개 IR 질문 자동 생성.

**산출물:**
- `scripts/generate_questionnaire.py`
- `web/articles.html` — "질문지 보기" 버튼 추가
- `web/questionnaire_detail.html` (신규) — 질문지 미리보기

**로직:**
1. 입력: `article_draft.content` + `story_lead.evidence` + `ai_comparison.result`
2. 변화 라벨별 템플릿 적용:
   - **NEW (신규 사업)**: "왜 진출? / 투자 규모? / 매출 목표 시점? / 핵심 인력? / 경쟁사 차별점?"
   - **REMOVED (사업 철수)**: "왜 철수? / 매각·청산 형태? / 손실 규모? / 인력 재배치? / 잔여 자산?"
   - **EXPANDED**: "확장 배경? / 추가 투자? / 점유율 목표? / 리스크?"
   - **SHRUNK**: "축소 원인? / 회복 계획? / 매출 영향?"
   - **CHANGED**: "변경 사유? / 영향 범위? / 시행 시점?"
   - **재무 변화** (numeric_change): "원인? / 정상화 시점? / 추가 자본 조달 계획?"
3. Gemini로 템플릿 + 컨텍스트 → 자연스러운 한국어 질문 5개 생성
4. 커버 레터 자동 생성 (기자 인사 + 마감 안내)

**API:**
- `GET /api/articles/{id}/questionnaire` — 질문지 조회
- `POST /api/articles/{id}/questionnaire/generate` — 신규 생성

**UI:**
- 기사 상세 페이지 → "📨 IR 질문지" 탭 추가
- 질문 항목별 편집 가능 (체크박스로 선택, 수정·추가)

**검증:**
- 65개 기존 article_drafts 모두에 질문지 자동 생성 시도
- 22개 v3 REPORTER 모드의 기존 IR 질문 5개와 비교·합치기

---

### Phase B — IR 담당자 정보 수집 (v2 — 2일)

**목표**: 1,546개 회사의 IR/PR 담당자 정보를 다중 소스에서 수집. **이메일이 메인, 전화/이름이 서브**. **수동 보강 가능**.

**핵심 원칙:**
> ⚡ 메일 발송은 **0건** — 모든 수집은 DNS / 크롤링 / Gmail 읽기로만.
> ⚡ 이메일 없어도 **이름+전화만으로도 등록 가능** — 추후 사용자가 채움.
> ⚡ 모든 필드는 **사이트에서 인라인 수동 편집 가능** (소프트 코딩).

**산출물:**
- `scripts/collect_ir_contacts.py` (홈페이지 크롤링 + 도메인 추정)
- `scripts/extract_ir_from_gmail.py` (Gmail 보도자료 파서)
- `scripts/parse_kosdaq_press.py` (코스닥협회 정형 파서) ★ 신규
- `web/ir_contacts.html` (관리·편집 UI)
- `server.py` — IR contacts CRUD API

---

#### B-1. Gmail 보도자료 파서 (가장 강력한 소스)

**검증된 사실 (2026-04-29 샘플):**
- `fin@finance-scope.com` 메일함에 **4,758건** 메시지
- 50건 샘플에서 72개 contact 추출 (메일당 평균 1.4개)
- 회사 매칭률 14% — **v2에서 60~70% 목표**

**v2 매칭 우선순위:**

| 순위 | 방법 | 신뢰도 라벨 |
|---|---|---|
| 1 | 발신 도메인 ↔ `companies.hm_url` 도메인 일치 | `A_verified` |
| 2 | 코스닥협회 정형 블록 (회사명 명시) | `A_verified` |
| 3 | 본문 첫 단락 회사명 추출 (`[회사명]`, `○○가 발표`) | `B_extracted` |
| 4 | 본문 서명 블록 회사명 | `B_extracted` |
| 5 | 회사 홈페이지 크롤링 | `C_homepage` |
| 6 | 도메인 추정 + MX | `C_guessed` |
| 7 | 사용자 수동 입력 | `D_manual` |

**IR/PR 대행사 화이트리스트 (본문 의존 — 발신자=대행사):**
```python
PR_AGENCY_DOMAINS = {
    'irkudos.co.kr', 'irmed.co.kr', 'irup.co.kr',
    'their.co.kr', 'upcom.co.kr', 'kggroup.co.kr',
    'edelman.com', 'fleishmanhillard.com',
    'kosdaqca.or.kr', 'klca.or.kr',  # 협회 (별도 파서)
}
```
→ 이 도메인 발신 메일은 **본문에서 실제 회사명 추출**

**한국 보도자료 표준 패턴:**
- 첫 줄: `[회사명] 제목` 또는 `회사명, 제목`
- 본문 첫 단락: `○○○(대표 ○○○)는 ~ 발표했다`
- 끝 부분 키워드: `■ 문의처`, `※ 보도문의`, `# 담당자`, `Contact:`
- 그 이후 200~500자 = **contact 블록**

**이름·직책 추출 강화:**
- 한국 성씨 200개 리스트로 필터 (`[김이박최정]` + 성씨)
- 직책 리스트: 부사장/전무/상무/이사/본부장/실장/팀장/부장/차장/과장/대리/매니저
- 거짓 매칭 거르기: "대표 이사", "아무런 책임", "최고과학 책임" 같은 명사구 폐기

---

#### B-2. 코스닥협회 정형 보도자료 별도 파서 ★ 신규

**관찰된 패턴:**
코스닥협회 (`@kosdaqca.or.kr`) 발송 보도자료는 회원사 정보가 정형 블록으로 모여 있어 **한 메일에서 다중 회사 추출 가능**:

```
■ 회사명: ○○주식회사
   담당자: 홍길동 부장 (홍보팀)
   연락처: 010-1234-5678
   이메일: hong@example.com

■ 회사명: △△주식회사
   담당자: 김철수 팀장
   연락처: 02-1234-5678
   이메일: kim@example.com
```

**처리 방식:**
1. Gmail 검색 쿼리: `from:kosdaqca.or.kr OR from:klca.or.kr`
2. `■ 회사명:` 블록 단위로 분할
3. 각 블록 안에서 정규식 추출:
   - `회사명:\s*(.+)`
   - `담당자[:\s]*(.+?(?:부장|팀장|...))`
   - `연락처[:\s]*(\d{2,3}-\d{3,4}-\d{4})`
   - `이메일[:\s]*([\w@\.-]+)`
4. 회사명 → companies 매칭 → corp_code 부여
5. `source='KOSDAQ_PRESS'`, `confidence='A_verified'` 저장

**같은 패턴 적용 대상:**
- 한국상장사협의회 (KLCA)
- 한국IR협의회 (KIRS)
- 산업별 협회 (자동차산업협회 등)

---

#### B-3. 회사 홈페이지 크롤링 (이미 구현)

`scripts/collect_ir_contacts.py` 작동 검증됨 (50개 샘플):
- 1개 실제 발견 (10%) — 홈페이지 mailto: / 이메일 텍스트
- 8개 도메인 추정 + MX 통과 (80%)
- 1개 도메인 무효 (10%)

→ 보도자료 파서로 못 잡은 회사 보완용.

---

#### B-4. 등록 정책 (이메일 없어도 OK)

| 케이스 | 등록 여부 | confidence |
|---|---|---|
| 이메일 + 이름 + 전화 모두 | 등록 | `A_complete` |
| 이메일 + 일부 정보 | 등록 | `A_email_only` |
| **이메일 없음 + 이름 + 전화** | **등록** (사용자가 추후 보강) | `B_phone_only` |
| 이메일·전화·이름 모두 없음 | 폐기 | - |

**DB 스키마 보강:**
```sql
-- 기존 ir_contacts 에 추가
ALTER TABLE ir_contacts ADD COLUMN ir_email_secondary TEXT;  -- 보조 이메일
-- ir_email 은 NULL 허용 (이름·전화만으로도 등록)
```

**발송 우선순위:**
- 1순위: `ir_email` 있으면 Gmail API 발송
- 2순위: 이메일 없고 `ir_phone` 만 있으면 사이트에 표시 (사용자가 직접 전화)

---

#### B-5. 수동 보강 UI ★ 핵심 (소프트 코딩)

**`web/ir_contacts.html` 기능:**

| 기능 | 동작 |
|---|---|
| 회사별 contact 목록 표시 | 표 형태 + 검색 / 필터 / 정렬 |
| 빈 셀 (이메일·전화·이름) 인라인 편집 | 클릭 → 입력 → 자동 저장 |
| 신규 contact 추가 | "+ 추가" 버튼 → 모달 |
| 검증 토글 | ☐ `user_verified` 체크박스 |
| 출처 표시 | `PRESS_RELEASE` / `KOSDAQ_PRESS` / `HOMEPAGE` / `GUESS` / `MANUAL` 배지 |
| 신뢰도 정렬 | `A > B > C > D` 순 |
| 수동 추가 시 source | `MANUAL` 자동 설정 + `confidence='A_complete'` |
| 일괄 import | CSV 업로드 |
| 일괄 export | CSV 다운로드 |
| 잘못된 정보 삭제 | 휴지통 아이콘 |

**API:**
- `GET /api/ir_contacts?corp_code=...&q=...&page=...&confidence=...`
- `GET /api/ir_contacts/{id}` — 상세
- `POST /api/ir_contacts` — 신규 (`MANUAL` 자동)
- `PATCH /api/ir_contacts/{id}` — 필드 부분 업데이트 (인라인 편집용)
- `DELETE /api/ir_contacts/{id}` — 삭제
- `POST /api/ir_contacts/import` — CSV 일괄
- `GET /api/ir_contacts/export` — CSV 출력
- `POST /api/ir_contacts/refresh?corp_code=...` — 재수집 트리거

---

#### B-6. 중복 처리 + 통합

**같은 corp_code 다중 contact 허용** (회사마다 IR/PR 여러 명):
- DB UNIQUE 제약: `(corp_code, ir_email)` — 같은 회사+이메일만 중복 방지
- 한 회사에 여러 부서 (IR팀 / PR팀 / 홍보팀) 등록 가능

**충돌 시 우선순위 (UPSERT 룰):**
- `user_verified=1` 가 `0` 보다 우선
- `confidence` 등급 높은 게 우선
- 같은 등급이면 최신 `created_at` 우선

---

**예상 커버리지 (v2):**

| 단계 | 누적 회사 수 |
|---|---|
| Gmail 보도자료 추출 (~4,758건) | 500~1,000 |
| 코스닥협회 정형 파서 | +200~500 |
| 회사 홈페이지 크롤링 | +200~400 |
| 도메인 추정 + MX | 나머지 (이메일 추정 가능) |
| 수동 보강 | 빈 부분 채움 |
| **합계** | **70%+ 회사 커버리지 예상** |

---

### Phase C — Gmail API 발송 통합 (0.5일)

**목표**: 사이트에서 1클릭으로 IR에게 질문지 이메일 발송.

**산출물:**
- `scripts/gmail_setup.py` (OAuth 1회 설정)
- `scripts/gmail_send.py`
- `server.py` — `/api/ir/send` 엔드포인트
- `web/articles.html` — "📤 IR에 발송" 버튼 추가

**Google Cloud 사전 작업 (사용자):**
1. Google Cloud Console → 프로젝트 생성 (또는 기존 사용)
2. Gmail API 활성화
3. OAuth 2.0 클라이언트 ID 생성 (Desktop app 타입)
4. `credentials.json` 다운로드 → 프로젝트 `secrets/` 에 저장
5. 첫 인증 시 `kjm@finance-scope.com` 로그인

**필요한 OAuth Scope:**
```
https://www.googleapis.com/auth/gmail.send
https://www.googleapis.com/auth/gmail.readonly
https://www.googleapis.com/auth/gmail.modify  (선택 — 라벨링)
```

**발송 흐름:**
1. 사이트에서 "발송" 클릭 → `POST /api/ir/send`
2. 서버가 ir_questionnaires + ir_contacts 결합
3. MIME 메시지 생성 (HTML + plain text 동시)
4. Gmail API `users.messages.send` 호출
5. 응답의 `id`, `threadId` 를 `ir_emails` 에 저장
6. `ir_questionnaires.status = 'sent'` 갱신

**메시지 템플릿:**
```
제목: [Finance Scope] {corp_name} 사업 변화 관련 취재 문의 ({today})

안녕하세요. Finance Scope 강종민 기자입니다.

귀사의 2025년 전체 사업보고서에서 다음 변화를 확인하여
관련 취재 차원에서 문의드립니다.

[변화 요약]
{change_summary}

[질문]
1. {q1}
2. {q2}
...

번거로우시겠지만 검토 후 답변 부탁드립니다. 마감일은 {deadline}입니다.

감사합니다.
강종민 드림

Finance Scope
{kjm@finance-scope.com}
```

---

### Phase D — 답장 수신 + 사이트 인박스 (1일)

**목표**: Gmail 답장을 사이트에서 바로 확인 + 발송과 자동 매칭.

**산출물:**
- `scripts/gmail_sync.py` (백그라운드 워커)
- `server.py` — `/api/inbox`, `/api/inbox/{id}` 엔드포인트
- `web/inbox.html` (신규) — 인박스 UI
- nav에 "📬 인박스" 메뉴 추가

**동기화 흐름:**
1. 5분 주기로 Gmail API 폴링: `users.messages.list?q=in:inbox newer_than:1d`
2. 새 메시지의 `threadId` 가 `ir_emails.gmail_thread_id` 와 매칭되면:
   - `direction='in'`, `received_at`, `body_text/html`, 첨부 정보 저장
   - 매칭된 `ir_questionnaires.status = 'replied'`
3. 매칭 안 되면 일반 인박스로 표시

**중복 방지:**
- `gmail_msg_id` UNIQUE 제약
- 이미 저장된 메시지는 SKIP

**인박스 UI:**
```
┌────────────────────────────────────────────────────┐
│ 📬 IR 인박스                            🔍 검색    │
├────────────────────────────────────────────────────┤
│ 🔴 [신규] 삼성전자 IR <ir@samsung.com>             │
│    Re: [Finance Scope] 신사업 관련 취재 문의       │
│    안녕하세요. 강 기자님의 문의에 답변드립니다...   │
│    2026-04-29 11:23 · 첨부 1개                    │
├────────────────────────────────────────────────────┤
│ ✅ 디앤씨미디어 IR <ir@dnc.com>                    │
│    Re: [Finance Scope] 자회사 영업 중단 문의       │
│    2026-04-28 15:45                              │
└────────────────────────────────────────────────────┘
```

**상세 화면:**
- 답장 본문
- 매칭된 질문지 + 발송 메일 (대조 가능)
- "이 답변으로 최종 기사 생성" 버튼 → Phase E 트리거

---

### Phase E — 답변 기반 최종 기사 (0.5일)

**목표**: 답장 내용 추출 → 1차 기사 + 답변 = 최종 기사 자동 생성.

**산출물:**
- `scripts/finalize_article.py`
- `web/article_detail.html` — 최종 버전 표시 추가

**로직:**
1. 답장 본문 → Gemini로 질문별 답변 추출 (JSON 파싱)
2. 1차 article_draft.content + 답변 → 최종 기사 프롬프트:
   ```
   [1차 기사]
   {draft_content}

   [회사 답변]
   {ir_response_text}

   [지시]
   1차 기사를 답변 내용으로 보강하여 최종 기사로 재작성.
   - 회사 입장 인용 추가 ("회사 측은 ~라고 밝혔다")
   - 기존 문장 유지 + 확장
   - 답변에 없는 사실은 추측 금지
   ```
3. 새 article_draft 행 생성 (`status='ready'`, 부모 article 참조)

---

## 🔐 보안·안정성 고려

| 항목 | 대책 |
|---|---|
| OAuth 토큰 관리 | `secrets/token.json` 로컬 저장 + `.gitignore` 등록 |
| 발송 한도 (2,000/일) | 큐 시스템 + 일일 카운터로 자동 페이싱 |
| 반송·실패 처리 | `bounced_count` 누적 + 3 이상 시 자동 비활성 |
| 회사 응답 무시 | 7일 후 자동 리마인드 메일 (선택) |
| 잘못된 발송 방지 | UI에서 미리보기 + 재확인 모달 |
| 회사명 오기재 | 발송 전 corp_name + ir_contacts.corp_name 일치 체크 |

---

## 📊 데이터 흐름 다이어그램

```
[article_draft]    [story_lead]    [ai_comparison]
       \              |              /
        \             |             /
         ▼            ▼            ▼
        ┌─────────────────────────────┐
        │ Phase A: 질문지 생성        │
        │ (Gemini)                    │
        └────────────┬────────────────┘
                     ▼
        [ir_questionnaires]  +  [ir_contacts] (Phase B)
                     │              │
                     └──────┬───────┘
                            ▼
        ┌─────────────────────────────┐
        │ Phase C: 이메일 발송 (Gmail) │
        └────────────┬────────────────┘
                     ▼
                [ir_emails (out)]
                     │
                     ▼
        ┌─────────────────────────────┐
        │ Phase D: 답장 폴링 (5분)     │
        └────────────┬────────────────┘
                     ▼
                [ir_emails (in)]
                     │
                     ▼
        ┌─────────────────────────────┐
        │ Phase E: 답변 기반 최종 기사 │
        │ (Gemini)                    │
        └────────────┬────────────────┘
                     ▼
            [article_draft (final)]
```

---

## 📅 타임라인 제안

| 일자 | 작업 |
|---|---|
| Day 1 (오늘) | Phase A 시작 — DB 스키마, 질문지 생성 스크립트, UI 버튼 |
| Day 2 | Phase A 마무리 + Phase B 시작 (KRX KIND, DART 수집) |
| Day 3 | Phase B 마무리 + Phase C (Gmail OAuth + 발송) |
| Day 4 | Phase D (인박스 동기화 + UI) |
| Day 5 | Phase E + 통합 테스트 |

**총 4~5일.** 단계별 독립 동작 가능 — 중간에 멈춰도 직전 단계까지는 즉시 사용 가능.

---

## ✅ 성공 기준

| Phase | 검증 |
|---|---|
| A | 65개 article_drafts 모두에 질문지 5~10개 생성. 헤드라인과 일치. |
| B | 1,500개 회사 중 70%+ 의 IR 이메일 수집 |
| C | 테스트 발송 (자기 자신 → 자기 자신) 성공 + Gmail 보낸편지함 표시 |
| D | 테스트 답장이 5분 내 사이트 인박스에 반영 + thread_id 매칭 |
| E | 답장 → 최종 기사 자동 생성 + 인용·보강 정상 |

---

## ⚠️ 주의 사항

1. **첫 발송 전 반드시 1~2개 회사 수동 검증** — 자동화로 잘못된 정보 대량 발송 방지
2. **kjm@finance-scope.com 도메인 신뢰도** — SPF / DKIM / DMARC 설정 확인 (스팸 분류 방지)
3. **회신 답변 신뢰성** — IR 답변도 사실 확인 후 기사화. 무비판 인용 금지.
4. **법적 고지 명시** — 메일 본문에 "Finance Scope는 ㈜파이낸스스코프 매체이며..." 같은 발신 정체 명확
5. **답장 없는 기업** — 일주일 무응답 시 우선 재발송 — 그래도 무응답이면 1차 기사로 출고

---

## 🗂 파일 구조 (예상)

```
company catcher/
├── scripts/
│   ├── generate_questionnaire.py   ★ NEW (Phase A)
│   ├── collect_ir_contacts.py      ★ DONE (Phase B-3 — 홈페이지 크롤링)
│   ├── extract_ir_from_gmail.py    ★ DONE v1 (Phase B-1 — Gmail 보도자료)
│   ├── parse_kosdaq_press.py       ★ NEW v2 (Phase B-2 — 코스닥협회 정형 파서)
│   ├── gmail_setup.py              ★ DONE (OAuth 설정)
│   ├── gmail_send.py               ★ NEW (Phase C — 발송)
│   ├── gmail_sync.py               ★ NEW (Phase D — 답장 폴링)
│   └── finalize_article.py         ★ NEW (Phase E — 답변 기반 최종 기사)
├── secrets/                         ★ DONE (.gitignore 등록)
│   ├── credentials.json             — Google Cloud OAuth ✓
│   └── token.json                   — 발급된 토큰 ✓
├── server.py                        — API 엔드포인트 추가 (CRUD + 발송 + 인박스)
└── web/
    ├── ir_contacts.html             ★ NEW (Phase B-5 인라인 편집 UI)
    ├── questionnaire_detail.html    ★ NEW (Phase A)
    └── inbox.html                   ★ NEW (Phase D)
```

---

## 🚀 다음 액션

지금 바로 진행할 수 있는 것:
1. **Phase A 즉시 시작** — DB 스키마 추가 + `generate_questionnaire.py` 작성 + 기존 65개 article_drafts에 일괄 적용
2. **Google Cloud 프로젝트 준비** — Phase C 진입 전 필요 (사용자 작업)

사용자 결정 필요한 것:
1. ☐ Day 1에 Phase A 시작할지 / 다른 우선순위 있는지
2. ☐ Google Cloud 프로젝트는 기존 거 사용할지 / 새로 만들지
3. ☐ kjm@finance-scope.com이 Workspace 계정인지 일반 Gmail인지 (발송 한도)
4. ☐ 발송 메시지 템플릿 검토 (위에 초안)
5. ☐ 답장 수신 폴링 주기 (기본 5분 / 더 짧게/길게)

---

**🎯 핵심 가치**: 이 시스템 완성 시 **하루에 50~100개 기업** 분석 → 질문 발송 → 답변 수신 → 최종 기사 자동 생성. 한국 상장사 정보 격차를 정말 줄이는 도구가 됩니다.
