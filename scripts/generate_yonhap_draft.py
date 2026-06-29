"""
scripts/generate_yonhap_draft.py
─────────────────────────────────
연합뉴스 포맷 기사 초안 생성 (라벨 반전 수정 이후 신규 단서 대상)

핵심 로직:
  1. 후보 필터:
     - n1_verified_at IS NOT NULL (newsability 검증 완료)
     - newsability_score >= --min-score
     - created_at >= --since (라벨 반전 수정 이후만)
     - article_drafts에 style='yonhap' 미작성
  2. 변화 포인트 추출:
     - ai_comparisons.result 텍스트에서 [NEW]/[REMOVED]/[EXPANDED]/[SHRUNK]/[CHANGED] 라벨 문장 분류
     - NEW/REMOVED/EXPANDED를 우선 포인트로
  3. 이슈 컨텍스트:
     - Naver 뉴스 API로 회사명 + 핵심 키워드 최근 30일 헤드라인 5건 (선택)
  4. 연합뉴스 포맷 프롬프트:
     - 제목 (30자 이내, 핵심 사실)
     - 부제 (보조)
     - 리드: (서울=연합뉴스) 기자명 = 한 문장 핵심
     - 본문: 변화 사실 → 구체 수치 → 회사 측 설명/인용 → 업계 배경 → 전망
  5. Gemini 호출 → JSON 파싱 → article_drafts 저장 (style='yonhap')

실행:
  python scripts/generate_yonhap_draft.py --min-score 8 --limit 6     # 8점 SCOOP 6건
  python scripts/generate_yonhap_draft.py --min-score 7 --limit 38    # 7점 이상
  python scripts/generate_yonhap_draft.py --dry-run --limit 1         # 1건 미리보기
"""
import argparse
import io
import json
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# 고종민 기자 스타일 SSOT
sys.path.insert(0, str(ROOT))
try:
    from config.journalist_styles import build_style_block
except Exception:
    def build_style_block(*a, **k):
        return ""

# .env 로드
_env_path = ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

KEYS = [os.environ.get(k, "").strip() for k in
        ["GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"]]
KEYS = [k for k in KEYS if k]
MODELS = ["gemini-2.5-flash", "gemini-2.5-flash-lite"]

NAVER_CID = os.environ.get("NAVER_CLIENT_ID", "").strip()
NAVER_CSC = os.environ.get("NAVER_CLIENT_SECRET", "").strip()


# ─────────── Gemini 호출 (article 전용 — 긴 본문, temp 0.4) ───────────
def call_gemini_article(prompt: str, max_wait: int = 2):
    last_err = None
    for wait in range(max_wait + 1):
        for model in MODELS:
            for k in KEYS:
                url = (f"https://generativelanguage.googleapis.com/v1beta/"
                       f"models/{model}:generateContent?key={k}")
                payload = json.dumps({
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "maxOutputTokens": 6144,
                        "temperature": 0.4,
                        "responseMimeType": "application/json",
                    },
                }).encode("utf-8")
                req = urllib.request.Request(
                    url, data=payload,
                    headers={"Content-Type": "application/json"}, method="POST",
                )
                try:
                    with urllib.request.urlopen(req, timeout=120) as r:
                        d = json.loads(r.read().decode("utf-8"))
                    parts = d["candidates"][0]["content"]["parts"]
                    text = ""
                    for p in reversed(parts):
                        if not p.get("thought", False) and p.get("text", ""):
                            text = p["text"]
                            break
                    if not text:
                        text = parts[-1].get("text", "")
                    return text, model
                except urllib.error.HTTPError as e:
                    last_err = f"{model} HTTP {e.code}"
                    if e.code in (429, 503):
                        continue
                    continue
                except Exception as e:
                    last_err = f"{model} {e}"
                    continue
        if wait < max_wait:
            print(f"  (모든 키 429 — 60초 대기 {wait+1}/{max_wait})", flush=True)
            time.sleep(60)
    raise RuntimeError(f"Gemini 호출 실패 — 마지막: {last_err}")


def parse_json(txt: str):
    s = txt.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-z]*\n?", "", s)
        s = re.sub(r"\n?```$", "", s)
    # 1차: 그대로
    try:
        return json.loads(s)
    except Exception:
        pass
    # 2차: 첫 { ~ 마지막 } 블록
    m = re.search(r"\{.*\}", s, re.DOTALL)
    if m:
        block = m.group(0)
        try:
            return json.loads(block)
        except Exception:
            pass
        # 3차: 흔한 깨짐 보정 — trailing comma 제거, 제어문자 정리
        fixed = re.sub(r",\s*([}\]])", r"\1", block)
        fixed = fixed.replace("\n", "\\n").replace("\r", "")
        # 다시 escape된 \\n 이중처리 방지
        fixed = fixed.replace('\\\\n', '\\n')
        try:
            return json.loads(fixed)
        except Exception:
            pass
    # 4차: 필드별 정규식 추출 (body 누락돼도 부분 복구)
    out = {}
    for field in ["headline", "subheadline", "lead", "body"]:
        fm = re.search(rf'"{field}"\s*:\s*"((?:[^"\\]|\\.)*)"', s, re.DOTALL)
        if fm:
            out[field] = fm.group(1).replace('\\n', '\n').replace('\\"', '"').strip()
    # lead + body 합쳐 본문 구성 (body 없으면 lead라도)
    if out.get("body") or out.get("lead"):
        return out
    return None


# ─────────── 변화 포인트 추출 ───────────
CHANGE_LABELS = ["NEW", "REMOVED", "EXPANDED", "SHRUNK", "CHANGED"]


def extract_change_points(ai_result: str) -> dict:
    """ai_comparisons.result 텍스트에서 [NEW]/[REMOVED]/... 라벨 문장 분류.

    표 행과 본문 단락 모두 스캔.
    """
    points = {lbl: [] for lbl in CHANGE_LABELS}
    if not ai_result:
        return points

    # 단락/표행 split
    chunks = re.split(r"\n(?:\s*\n|\|)", ai_result)
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        for lbl in CHANGE_LABELS:
            patt = rf"\[{lbl}\]|\*\*\[{lbl}\]\*\*|\*\*{lbl}\*\*"
            if re.search(patt, chunk):
                # 클린업
                clean = re.sub(r"[\*\|]", "", chunk).strip()
                clean = re.sub(r"\s+", " ", clean)
                if len(clean) > 20 and clean not in points[lbl]:
                    points[lbl].append(clean[:500])
                break
    # 각 라벨 최대 5개로 제한 (프롬프트 크기 절감)
    for lbl in CHANGE_LABELS:
        points[lbl] = points[lbl][:5]
    return points


# ─────────── Naver 뉴스 이슈 컨텍스트 ───────────
def fetch_naver_context(corp_name: str, keywords: list, max_items: int = 5) -> list:
    """회사 + 핵심 키워드로 최근 30일 Naver 뉴스 검색 (선택)."""
    if not (NAVER_CID and NAVER_CSC):
        return []
    kw = (keywords[0] if keywords else "").strip()
    query = f"{corp_name} {kw}".strip()
    q = urllib.parse.quote(query)
    url = f"https://openapi.naver.com/v1/search/news.json?query={q}&display={max_items}&sort=date"
    try:
        req = urllib.request.Request(url, headers={
            "X-Naver-Client-Id": NAVER_CID,
            "X-Naver-Client-Secret": NAVER_CSC,
            "User-Agent": "Mozilla/5.0",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
    except Exception:
        return []

    cutoff = datetime.now() - timedelta(days=30)
    out = []
    for it in data.get("items", []):
        try:
            pd = datetime.strptime(it.get("pubDate", "")[:25],
                                   "%a, %d %b %Y %H:%M:%S")
            if pd >= cutoff:
                title = re.sub(r"<[^>]+>", "", it.get("title", ""))[:120]
                out.append(f"  - [{pd.strftime('%m-%d')}] {title}")
        except Exception:
            pass
    return out[:max_items]


# ─────────── 연합뉴스 프롬프트 ───────────
def build_yonhap_prompt(lead: sqlite3.Row, ai_result: str,
                        change_points: dict, naver_ctx: list) -> str:
    corp_name = lead["corp_name"]
    sector = ""
    try:
        ctx = json.loads(lead["company_context"] or "{}")
        sector = ctx.get("sector", "") or ctx.get("induty_name", "")
    except Exception:
        pass

    # 변화 포인트 블록
    cp_lines = []
    for lbl, items in change_points.items():
        if not items:
            continue
        lbl_ko = {"NEW": "신규 등장", "REMOVED": "사라짐",
                  "EXPANDED": "확대", "SHRUNK": "축소",
                  "CHANGED": "변경"}[lbl]
        cp_lines.append(f"▷ {lbl} ({lbl_ko})")
        for it in items:
            cp_lines.append(f"  • {it}")
    cp_block = "\n".join(cp_lines) if cp_lines else "(변화 포인트 추출 안됨)"

    # Naver 컨텍스트
    naver_block = ("\n".join(naver_ctx) if naver_ctx else
                   "(관련 최근 보도 없음 — 외부 컨텍스트 사용 금지)")

    # 정량 데이터
    num_facts = ""
    try:
        nf = json.loads(lead["numeric_facts"] or "[]")
        if nf:
            parts = []
            for f in nf[:5]:
                if f.get("type") == "change":
                    parts.append(f"{f.get('from')} → {f.get('to')} {f.get('unit','')}")
                elif f.get("type") == "money":
                    parts.append(f"{f.get('value')} {f.get('unit','')}")
            num_facts = " / ".join(parts)
    except Exception:
        pass

    # 단서 valence(GOOD/BAD/MIXED)에 맞는 고종민 스타일 예시 선택
    _style_block = build_style_block(include_fewshot=True, n_fewshot=3,
                                     valence=(lead["valence"] or None))

    return f"""{_style_block}

═══════════════════════════════════════════
이번 기사는 **사업보고서(과거: 2025) → 1분기보고서(최신: 2026) 변화**를 다룹니다.
**포맷은 연합뉴스 단신 형식**(제목 / (서울=연합뉴스) 리드 / 단락 본문)을 따르되,
**문체·어휘·구성은 위 고종민 기자 스타일을 그대로** 적용하세요.

[핵심 원칙]
1. **[입력 자료]에 없는 정보는 절대 사용 금지.** 근거 없는 추측·과장 금지.
2. 핵심은 **NEW / REMOVED / EXPANDED — 새로 생기거나 사라지거나 성장한 것**.
3. 수치는 반드시 "이전 → 현재" 병기 (고종민 스타일: 구체 수치·전망·비교 강조).
4. 회사 측 입장은 보고서 인용으로 처리 ("회사는 보고서에서 ~밝혔다").

═══════════════════════════════════════════
[입력 자료]

▶ 회사: {corp_name}
▶ 업종: {sector or '(미확인)'}
▶ 단서 유형: {lead['lead_type']} (severity {lead['severity']}/5)
▶ N-1 헤드라인 후보: {lead['headline_ko'] or '(미정)'}
▶ N-1 스토리 각도: {lead['story_angle'] or '(미정)'}
▶ valence: {lead['valence'] or 'MIXED'}  /  방향: {lead['value_direction'] or '(미분류)'}

▶ ★ 변화 포인트 (NEW/REMOVED/EXPANDED 우선) — **이게 기사 본문 핵심**
{cp_block}

▶ 정량 데이터
{num_facts or '(없음)'}

▶ 사업보고서 원문 발췌 (1500자)
{(lead['evidence_deep'] or lead['evidence'] or '')[:1500]}

▶ AI 비교분석 발췌 (참고용 1500자)
{ai_result[:1500]}

▶ 관련 최근 보도 (Naver 30일) — 업계 배경 보강
{naver_block}

═══════════════════════════════════════════
[연합뉴스 포맷 — 엄수]

1. **headline**: 30자 이내. 핵심 사실 한 줄. 회사명으로 시작.
   예: "TCC스틸, 연간 영업손실 전환…적자 쇼크"

2. **subheadline**: 30자 이내. 핵심 수치/배경.
   예: "원재료 가격 상승·판매 둔화 직격"

3. **lead** (리드 문장): 첫 줄은 반드시 `(서울=연합뉴스) 기자 = `로 시작.
   리드는 한 문장으로 5W1H 압축.
   예: "(서울=연합뉴스) 기자 = 1차 강관 제조업체 TCC스틸이 2025년 연간 기준 영업손실로 돌아서며 적자를 기록했다."

4. **body**: 본문 4~5단락, 각 단락 2~3문장. 800~1200자.
   - 1단락: 변화 사실 핵심 (NEW/REMOVED/EXPANDED 1개 골라 구체화)
   - 2단락: 구체 수치 (이전 → 현재, 출처 명시: "2025 사업보고서 기준")
   - 3단락: 추가 변화 포인트 (다른 NEW/REMOVED)
   - 4단락: 회사 측 보고서 인용 또는 배경 사실
   - 5단락(선택): 업계 컨텍스트 (Naver 보도 인용 가능, 출처 표기)

5. **key_points**: 본문 핵심 사실 3~5개 (NEW/REMOVED/EXPANDED 라벨과 함께)

6. **facts_cited**: 본문에서 인용한 외부 사실 (보고서 페이지/Naver 등) 목록

[중요]
- 본문 끝에 "(끝)" 표시는 넣지 마세요 (JSON 출력이므로).
- 한국어로만 작성. 영어 단어 금지.
- 표나 리스트 사용 금지 — 단락만.
- "예상된다", "전망이다" 같은 추측 표현 절대 금지.

═══════════════════════════════════════════
[출력 JSON]

{{
  "headline": "30자 이내",
  "subheadline": "30자 이내",
  "lead": "(서울=연합뉴스) 기자 = 한 문장",
  "body": "5단락 본문 800-1200자 (단락 사이 빈 줄)",
  "key_points": ["NEW: ...", "EXPANDED: ...", "REMOVED: ..."],
  "facts_cited": ["2025 사업보고서 X쪽", "Naver: ..."]
}}
"""


# ─────────── DB ───────────
def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def save_draft(db, lead, parsed, model):
    """article_drafts에 yonhap 스타일로 저장."""
    body_md = (parsed.get("lead", "") + "\n\n" +
               parsed.get("body", "")).strip()
    content = body_md
    word_count = len(content.split())
    char_count = len(content)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # editor_note에 yonhap 스타일 마킹 + key_points + facts_cited 메타 저장
    meta = {
        "style_kind": "yonhap",
        "key_points": parsed.get("key_points", []),
        "facts_cited": parsed.get("facts_cited", []),
    }
    editor_note = json.dumps(meta, ensure_ascii=False, indent=2)

    # style은 DB CHECK 제약상 ('news','analysis','brief','column'). yonhap은 'news' + meta.
    db.execute("""
        INSERT INTO article_drafts
          (lead_id, corp_code, corp_name, headline, subheadline,
           content, style, model, word_count, char_count,
           status, editor_note, created_at, updated_at)
        VALUES (?,?,?,?,?,?,'news',?,?,?,'draft',?,?,?)
    """, [lead["id"], lead["corp_code"], lead["corp_name"],
          parsed.get("headline", "")[:200],
          parsed.get("subheadline", "")[:200],
          content, model, word_count, char_count,
          editor_note, now, now])
    db.commit()


# ─────────── 메인 ───────────
def main():
    ap = argparse.ArgumentParser(description="연합뉴스 포맷 초안 생성")
    ap.add_argument("--min-score", type=int, default=8,
                    help="newsability_score 최소값 (기본 8 = SCOOP)")
    ap.add_argument("--limit", type=int, default=10, help="최대 생성 건수")
    ap.add_argument("--since", default="2026-06-06",
                    help="story_leads.created_at 시작일 (라벨 반전 수정 이후)")
    ap.add_argument("--dry-run", action="store_true",
                    help="프롬프트만 출력, API 호출/저장 안 함")
    ap.add_argument("--no-naver", action="store_true",
                    help="Naver 컨텍스트 사용 안 함")
    ap.add_argument("--lead-id", type=int, default=None,
                    help="특정 lead_id 1건만 처리")
    args = ap.parse_args()

    if not KEYS:
        print("오류: GEMINI_API_KEY 없음")
        sys.exit(1)

    db = get_db()

    # 후보 조회
    if args.lead_id:
        where = "sl.id = ?"
        params = [args.lead_id]
    else:
        where = """sl.n1_verified_at IS NOT NULL
              AND sl.newsability_score >= ?
              AND sl.created_at >= ?
              AND NOT EXISTS (
                  SELECT 1 FROM article_drafts ad
                   WHERE ad.lead_id = sl.id
                     AND ad.status='draft'
                     AND (ad.editor_note LIKE '%"style_kind": "yonhap"%' OR ad.editor_note LIKE '%style_kind":"yonhap"%')
              )"""
        params = [args.min_score, args.since]

    leads = db.execute(f"""
        SELECT sl.*, ac.result AS ai_result
        FROM story_leads sl
        LEFT JOIN ai_comparisons ac ON ac.id = sl.comparison_id
        WHERE {where}
        ORDER BY sl.newsability_score DESC, sl.severity DESC, sl.id DESC
        LIMIT ?
    """, params + [args.limit]).fetchall()

    print(f"\n연합뉴스 초안 작성 대상: {len(leads)}건")
    print(f"  --min-score={args.min_score}  --since={args.since}  --limit={args.limit}")
    if args.dry_run:
        print("  ★ DRY RUN — 프롬프트만 출력, API 호출/저장 안 함")
    print()

    ok = 0
    err = 0
    for i, lead in enumerate(leads, 1):
        try:
            cp = extract_change_points(lead["ai_result"] or "")
            keywords = []
            try:
                keywords = json.loads(lead["keywords"] or "[]")
            except Exception:
                pass
            naver_ctx = []
            if not args.no_naver:
                naver_ctx = fetch_naver_context(
                    lead["corp_name"], keywords, max_items=5
                )
            prompt = build_yonhap_prompt(lead, lead["ai_result"] or "",
                                          cp, naver_ctx)

            if args.dry_run:
                print(f"=== [{i}/{len(leads)}] {lead['corp_name']} (score {lead['newsability_score']}) ===")
                print(prompt[:3500])
                print(f"...(생략, 총 {len(prompt)}자)\n")
                continue

            print(f"  [{i:>2}/{len(leads)}] {lead['corp_name']} (score {lead['newsability_score']}) — 생성 중...",
                  flush=True)
            text, model = call_gemini_article(prompt)
            parsed = parse_json(text)
            # body 누락 시 lead로 최소 복구 (완전 실패만 skip)
            if parsed and not parsed.get("body") and parsed.get("lead"):
                parsed["body"] = parsed["lead"]
            if not parsed or not (parsed.get("body") or parsed.get("lead")):
                err += 1
                print(f"    ✗ JSON 파싱 실패 또는 본문 누락 (resp {len(text)}자)")
                continue
            save_draft(db, lead, parsed, model)
            ok += 1
            print(f"    ✓ 저장 ({model})  헤드: {parsed.get('headline','')[:50]}")
            time.sleep(3)
        except RuntimeError as e:
            err += 1
            print(f"    ✗ {lead['corp_name']}: {str(e)[:100]}")
            break
        except Exception as e:
            err += 1
            print(f"    ✗ {lead['corp_name']}: {str(e)[:100]}")

    print(f"\n[완료] ok={ok}  err={err}")
    if not args.dry_run and ok > 0:
        print("\n[생성된 초안 샘플]")
        for r in db.execute("""SELECT id, corp_name, headline, char_count, model
                               FROM article_drafts
                               WHERE style='yonhap'
                               ORDER BY id DESC LIMIT ?""", [ok]):
            print(f"  [#{r[0]}] {r[1]:<22} {r[2]:<35}  {r[3]}자  {r[4]}")


if __name__ == "__main__":
    main()
