# 인계 노트 — 배치 처리 개선 (2026-07-01)

## 목적
Gemini 무료 quota(호출 횟수 RPD 기준) 소진 문제 해결 — 여러 단서를 **한 호출에 묶는 배치 처리**로 호출 수 대폭 감소.

## 완료된 작업

### 1. 공통 배치 모듈 — `scripts/_gemini_batch.py` (신규)
- `process_in_batches(items, item_id, render_item, instruction, output_example, on_results, max_items, ...)`
- N건을 한 프롬프트로 묶어 "각 항목 독립 분석 → JSON 배열 응답" → id로 원본 매칭 → `on_results` 콜백에서 DB 업데이트
- **잘린 JSON 배열 복구**: maxOutputTokens 초과로 응답이 끊겨도 (1) 마지막 완전한 `}`까지 자르고 `]` 붙여 재파싱 (2) 개별 `{...}` 객체 스캔 → 앞부분 객체는 건짐
- chunk 파싱 전무 시 절반으로 재귀 재시도(depth<3), 그래도 실패면 skip (증분이라 다음 실행 재처리)
- `call_gemini`가 모든 키·모델 소진 시 RuntimeError → 상위에서 종료(quota)

### 2. 배치 전환 완료 (3종)
| 스크립트 | 배치 크기 | 상태 |
|---------|---------|------|
| `_l2_value_direction.py` | 10 | ✅ **검증 완료** — 26건→3호출 (10:1 효율, fail 0) |
| `_k3_cross_verify_flash.py` | 12 | ✅ 코드 완료, **실행 테스트 안 함** |
| `_n1_newsability_verify.py` | 6 | ✅ 코드 완료, **실행 테스트 안 함** |

- 셋 다 기존 증분(`xxx_at IS NULL`) 유지 + `stats`/통계 출력 유지.

### 3. 배치 안 하는 것
- `batch_compare.py` — 회사당 100~150K 토큰(사업보고서 2개 통째 비교)이라 배치 불가. 그대로.
- `_j1_fact_verify.py` — Gemini 미사용(룰 기반).

## ⚠️ 다음 세션 할 일

1. **_k3 / _n1 배치 실행 테스트** (quota 있을 때)
   ```
   python scripts/_k3_cross_verify_flash.py   # 12건/호출 확인
   python scripts/_n1_newsability_verify.py   # 6건/호출 확인
   ```
   - 검증 포인트: 로그의 "ok=N 호출=M" 비율이 배치 크기에 가까운지, fail 적은지, 결과 정확한지
   - _n1은 출력 필드가 많아(headline/story_angle/ir_hint 등) 6건에서도 잘림 가능 → 잘리면 BATCH_SIZE 4로 낮추기

2. **배치 정확성 교차검증** (선택): 같은 단서를 단건 vs 배치로 돌려 결과 일치율 확인

3. **Phase 2-1** (미착수): `external_sources.processed_at` 컬럼 추가 → 뉴스 재처리 방지 ([collect_kr_news.py](scripts/collect_kr_news.py))

4. **Phase 3** (후순위): 뉴스 jaccard dedup

## 참고
- 로드맵 전문: `C:\Users\kangc\.claude\plans\batch-incremental-roadmap.md`
- **stdout 버퍼링 주의**: 스크립트가 `io.TextIOWrapper` 재래핑 → 실행 중엔 로그 파일이 비어보이고 종료 시 flush됨. 진행 확인은 DB 쿼리로 (`value_classified_at`/`cross_verified`/`n1_verified_at` IS NOT NULL 카운트).
- **좀비 프로세스 주의**: `timeout` 래퍼로 감싸면 Windows에서 자식 python이 안 죽고 DB 락 유발. 백그라운드는 `run_in_background` 또는 nohup 직접 사용.
- 서버 재가동: `start_server.bat` (백그라운드 유지형) 또는 `nohup python -u -X utf8 server.py &`

## 현재 데이터 상태 (참고)
- GOLD 330 / ULTRA GOLD ~47 / 출고후보 8점 16·7점 80·6점 84
- _l2 처리 누적 ~330 (거의 완료)
