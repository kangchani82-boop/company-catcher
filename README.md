# Company Catcher

> **한국 상장사 정보 격차를 줄인다.**
> DART 공시 분석 → AI 비교 → 취재 단서 발굴 → 기사 초안 → IR 질문지 → 발송 → 답장 학습.

## 개요

KOSPI·KOSDAQ 상장사 약 2,200개의 DART 사업·분기보고서를 AI(Gemini)로 자동 비교하고, 변화 시그널을 룰 기반으로 탐지해 기자가 바로 활용 가능한 취재 단서·기사 초안·IR 질문지로 생성하는 통합 시스템.

## 핵심 기능

- **AI 비교 분석**: 두 보고서의 "사업의 내용" 섹션 비교 (Gemini Flash-Lite)
- **취재 단서 탐지**: 29개 룰(M&A, 매각, 기술이전, 고객 의존도, 비공시 지분 변동 등) 자동 매칭
- **기사 초안 생성**: 한국 경제지 스타일 (파이낸스코프 톤)
- **IR 질문지 생성**: 단서별 맞춤 질문 자동 작성
- **품질 재검토**: AI score 1-4 + verdict(approve/revise/discard)
- **공급망 그래프**: 11,000+ 관계 + 충격 전파 분석
- **IR 담당자 풀**: 6,900+ 검증 이메일 + 600+ 웹 스크래핑 발굴

## 데이터 규모 (2026-05 기준)

| 지표 | 값 |
|---|---:|
| DART 보고서 | 16,000+ |
| AI 비교 (2025_annual × 2026_Q1) | 2,164 (KOSPI+KOSDAQ 100%) |
| 취재 단서 (story_leads) | 1,600+ |
| 기사 초안 (article_drafts) | 1,200+ |
| IR 질문지 | 1,200+ |
| 공급망 관계 | 11,200+ |
| IR 담당자 (검증) | 6,900+ |

## 아키텍처

```
DART API → reports → biz_content 추출
                ↓
        ai_comparisons (Gemini 비교)
                ↓
        story_leads (29개 룰 매칭)
                ↓
        article_drafts (기사 초안)
                ↓
        ir_questionnaires (IR 질문지)
                ↓
        quality_review (score 4 = 발송 후보)
                ↓
        gmail_send → inbox → learn_from_replies
```

## 폴더 구조

```
company-catcher/
├── server.py                    # HTTP 서버 (75+ API)
├── config/
│   └── gemini_models.py         # Gemini 모델 SSOT
├── dart/                        # DART API · 공급망 분석
├── scripts/                     # 데이터 파이프라인 (60+)
├── web/                         # 웹 UI (21개 페이지)
├── data/dart/                   # SQLite DB (gitignore)
└── docs/                        # 운영 가이드 · 핸드오프
```

## 실행

```bash
# 환경 변수 (.env)
DART_API_KEY=...
GEMINI_API_KEY=...
NAVER_CLIENT_ID=...
NAVER_CLIENT_SECRET=...

# 의존성
pip install -r requirements.txt

# 서버
python server.py            # http://localhost:8888

# 일일 사이클
python scripts/run_daily_q1_cycle.py
```

## 일일 운영

| 명령 | 용도 |
|---|---|
| `python scripts/batch_compare.py --model flash-lite --workers 2` | AI 비교 분석 |
| `python scripts/detect_leads.py` | 취재 단서 탐지 |
| `python scripts/generate_draft.py` | 기사 초안 (sev≥4) |
| `python scripts/generate_questionnaire.py` | IR 질문지 |
| `python scripts/collect_news_for_questionnaires.py` | 뉴스 매칭 |
| `python scripts/quality_review_articles.py --workers 2` | 품질 재검토 |
| `python scripts/scrape_ir_emails.py --listed-only --workers 8` | IR 이메일 발굴 |

## 주요 페이지

| 경로 | 용도 |
|---|---|
| `/` | 종합 대시보드 |
| `/leads` | 취재 단서 목록 |
| `/articles` | 기사 초안 목록 |
| `/quality_review` | 품질 재검토 (score 4 검수) |
| `/ir_contacts_2` | 스크래핑 IR 풀 |
| `/inbox` | IR 답장 학습 |
| `/supply_chain` | 공급망 그래프 |

## 라이센스

Private. All rights reserved.
