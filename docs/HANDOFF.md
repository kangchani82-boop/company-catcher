# 🤝 세션 핸드오프 — Company Catcher

> 이 문서를 새 Claude 세션 첫 메시지에 첨부하면 즉시 같은 컨텍스트로 이어집니다.
> 작성일: 2026-05-14 (D-1) — 분기보고서 마감 D-Day 직전

---

## 🎯 미션 한 줄

> **한국 상장사 정보 격차를 줄인다.**
> DART 보고서 변화 → AI 비교 → 취재 단서 → 기사 → IR 질문지 → 발송 → 답장 학습.

---

## 📊 현재 시스템 상태 (스냅샷)

| 지표 | 값 |
|---|---:|
| Q1 2026 보고서 | 408건 (분석 가능 374건) |
| AI 비교 (annual×Q1) | 203건 완료 |
| Q1 단서 | 50건 |
| **질문지 (pending)** | **337건** |
| 품질 재검토 완료 | 315/337 (점수 4:39, 3:261, 2:12, 1:3) |
| 뉴스 자료 | 2,727건 |
| 상장사 영문명 보유 | 3,916개 |
| 상장사 ir_url 보유 | 430개 (전체 3,919 중 11%) |
| ir_contacts (검증) | ~268 회사 |
| **ir_contacts_2 (스크래핑·신규)** | 진행 중 |

---

## 🔄 현재 진행 중인 백그라운드 작업

| Task ID | 작업 | 예상 완료 |
|---|---|---|
| `booicl0so` | **pending 41개 회사 IR 이메일 스크래핑** | ~15-25분 |

완료 후 결과 보려면:
```bash
tail -n 30 logs/scrape_pending.log
python scripts/scrape_ir_emails.py --stats
```

---

## 🆕 가장 최근 작업 (이번 세션) — **IR담당자2 시스템**

사용자가 명시한 요구사항: **기존 `ir_contacts`와 분리된 신규 IR 이메일 발굴 풀**.

### 생성된 자산

```
DB:
  ir_contacts_2 테이블 (25컬럼, ir_contacts와 동일 스키마)
  companies.ir_url 컬럼 (+ ir_url_fetched_at)

스크립트:
  scripts/fetch_corp_ir_urls.py         # DART ir_url 일괄 수집 (완료)
  scripts/scrape_ir_emails.py           # 웹 스크래퍼 (실행 중)
  scripts/_debug_scrape.py              # 디버그 도구
  scripts/_create_ir_contacts_2.py      # 스키마 생성기

API (server.py):
  GET    /api/ir_contacts_2              # 목록 + 통계
  POST   /api/ir_contacts_2/scrape       # 스크래핑 트리거
  POST   /api/ir_contacts_2/{id}/verify  # 사용자 검증 토글
  POST   /api/ir_contacts_2/{id}/promote # ir_contacts 본 테이블로 승급
  DELETE /api/ir_contacts_2/{id}         # 비활성화

UI:
  web/ir_contacts_2.html                # 신규 페이지
  네비게이션: 🌐 IR담당자2
```

### 스크래퍼 점수 알고리즘 (60+ 저장, threshold 40)
```
회사 도메인 정확 매칭        +50
회사 도메인 부분 매칭 (4자+)  +30
그룹사 도메인 + ir prefix    +20  (lotte.net, samsung.com 등)
그룹사 도메인 외            -10
무료 메일 (gmail, naver)    -50
기타 외부 도메인            -25 (priority prefix 면 -5)

priority prefix (ir@, lem_ir@, invest@ 등): +35
bad prefix (info@, contact@, webmaster@ 등): -35

URL /ir/ /investor/ /disclosure/ 경로: +15
URL /contact/ /inquiry/ /about/ 경로: +8

주변 텍스트에 IR/투자/공시 키워드 3개+: +25
                                 2개  : +15
                                 1개  : +8
```

### 신뢰도 3단
- **A_complete** (80+): 도메인 정확 매칭 + priority prefix
- **B_scraped** (60-79): 일반 매칭
- **C_likely** (40-59): 그룹사 도메인 — **사용자 검증 필수**

### 검증된 케이스
- **롯데에너지머티리얼즈**: `lem_ir@lotte.net` sc=81 (그룹사 도메인 + ir prefix)
- **사조대림/동성제약/BYC/삼보산업**: 페이지에 이메일 자체 미노출 (SPA 또는 폼만 제공)

---

## 📋 시스템 아키텍처 (페이지 + 핵심 스크립트)

### 페이지 (web/)
```
/                          home_v2.html       종합 대시보드 + D-day 스트립
/questionnaire_review      질문지 검토 (337건 pending) — A승인/H보류/D폐기 단축키
/quality_review            품질 재검토 (315건) — score≥4 일괄 적용 기능
/ir_contacts               (기존) IR 담당자
/ir_contacts_2             (신규) 웹 발굴 IR 이메일 — 검증 후 승급
/inbox                     IR 인박스 + 답장 학습
/report_work?id=N          보고서 단위 작업 카드
/corp_work?corp_code=C     회사 타임라인
/articles, /leads, /comparisons   목록 페이지들
```

### 핵심 스크립트 (scripts/)
```
run_daily_q1_cycle.py            매일 1회 — fetch→compare→leads→drafts→qst→news
fetch_biz_content.py             DART 보고서 + 사업의 내용 추출
batch_compare.py                 Gemini Flash-Lite로 두 보고서 비교
detect_leads.py                  비교 결과 → severity≥3 단서
generate_draft.py                단서 → 기사 초안
generate_questionnaire.py        기사 → IR 질문지
collect_news_for_questionnaires.py  Naver 뉴스 수집
quality_review_articles.py       313건 기사 2차 검토 (score+verdict)
gmail_send.py                    IR 메일 발송 (안전망 4중)
gmail_sync_inbox.py              답장 자동 매칭 + learn_from_replies 트리거
learn_from_replies.py            답장 → IR 담당자 학습
scrape_ir_emails.py              (신규) 웹 스크래핑으로 IR 이메일 발굴
fetch_corp_eng_names.py          상장사 영문명 수집 (3,916개)
fetch_corp_ir_urls.py            DART ir_url 수집 (430개)
export_eng_names_md.py           영문명 → MD/CSV/JSON
```

---

## 💼 일일 운영 한 줄

```bash
python scripts/run_daily_q1_cycle.py
```
출근 후 1회. 약 5-15분.

서버 가동 (별도 창):
```
start_server.bat 더블클릭
또는 python server.py
```

---

## 🛡 발송 안전망

1. ✅ 검수 게이트: `status='approved'` 만 발송
2. ✅ 일일 한도: `GMAIL_DAILY_LIMIT=400` (env로 변경 가능)
3. ✅ 24h 중복 발송 차단 (corp_code 또는 to_addr 동일)
4. ✅ 발송 가능 IR 담당자 없는 회사 자동 제외

---

## 🚧 알려진 한계 / 다음 단계 후보

### 한계
1. **정적 스크래핑 한계** — SPA·JS 렌더 페이지에서 이메일 못 잡음
   → 대안: Playwright headless (느림, 4-6시간)
2. **22건 quality_review 미완료** — Gemini 일일 한도 소진. 새벽 자동 회복
3. **이미지화된 이메일** (`info[at]samsung[dot]com`) 일부 미처리

### P0 (당장)
- pending 41개 스크래핑 결과 검토 + 승급 (~10건 예상)
- `/quality_review` 에서 score≥4 일괄 적용 → 발송 시작

### P1 (마감 후)
- D+1 답장 매칭 + 학습 사이클
- 답장 받은 회사 기사 우선 출고
- ir_contacts_2 전체 상장사 확대 스크래핑 (~6시간)
- 22건 Gemini retry

### P2 (장기)
- Playwright 동적 스크래퍼 (SPA 회사 대응)
- KRX 공식 상장사 영문명 매칭 (fas-news-searcher 연계)
- 일일 사이클 윈도우 작업 스케줄러 등록

---

## 🔑 핵심 환경변수 (`.env`)

```
DART_API_KEY=...
GEMINI_API_KEY=...      # 3개까지 가능 (GEMINI_API_KEY, _2, _3)
NAVER_CLIENT_ID=...
NAVER_CLIENT_SECRET=...
GMAIL_DAILY_LIMIT=400   # 옵션
```

---

## 📁 데이터 위치

```
data/dart/dart_reports.db        메인 DB (~1.1GB)
data/exports/                    fas-news-searcher용 export 파일들
  ├── listed_corps_eng.md         925→3,916 상장사 영문명 (315KB)
  ├── listed_corps_eng.csv
  └── listed_corps_eng.json
docs/
  ├── USAGE_GUIDE.md              사용 설명서 (사람용)
  └── HANDOFF.md                  이 문서
logs/                             모든 사이클·스크립트 로그
```

---

## 🗣 사용자 스타일 메모 (소통 방식)

- 한국어로 답변
- "ㅇㅇ 진행해줘" / "A부터 갑시다" → 자동 진행 OK
- "Claude API 사용 전 허락 필요" (다른 API는 자율)
- 단계별 확인 X — Phase 순서대로 바로 진행
- 응답은 간결하게. 표/이모지로 구조화
- 결과 보고 → 다음 단계 후보 제시
- 폐기/오류 케이스는 명확히 알려야 함

---

## 🆘 새 세션 시작 시 첫 작업

1. 이 문서 읽고 "현재 상태 파악 완료" 확인
2. 백그라운드 작업 (`booicl0so`) 완료됐는지 체크
3. `python scripts/run_daily_q1_cycle.py --only-stats` 로 시스템 현황 확인
4. `python scripts/scrape_ir_emails.py --stats` 로 ir_contacts_2 결과 확인
5. 사용자에게 "어디부터 이어갈까요?" 묻기

---

**이 문서는 작업 진행에 따라 갱신 필요.** 큰 변경 있을 때 한 번씩 업데이트.
