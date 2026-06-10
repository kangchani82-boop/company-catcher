"""
N-1: 뉴스화 가능성 최종 검증
- 입력: HIGH_CONFIRMED + EXACT/STRONG (골드 102건)
- 평가: 뉴스가치·헤드라인·악재/호재 명확성·기사화 리스크·추가확인사항
- "기자 관점에서 이 단서로 실제로 기사를 쓸 수 있는가?"
- 출력: newsability_score, headline_ko, story_angle, valence, publish_risk, missing_verification
"""
import sqlite3, json, os, sys, time, io, re
import urllib.request, urllib.error
from pathlib import Path
from datetime import datetime

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except: pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

_env = ROOT / ".env"
if _env.exists():
    for line in _env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 컬럼 추가
cols = [c[1] for c in db.execute('PRAGMA table_info(story_leads)')]
NEW_COLS = {
    'newsability_score': 'INTEGER',    # 1~10
    'headline_ko':       'TEXT',        # 기사 헤드라인 후보
    'story_angle':       'TEXT',        # 취재 각도
    'valence':           'TEXT',        # GOOD|BAD|MIXED
    'publish_risk':      'TEXT',        # HIGH|MEDIUM|LOW
    'missing_verification': 'TEXT',    # 추가 확인 필요 사항
    'already_public':    'TEXT',        # YES|LIKELY_NO|UNKNOWN
    'n1_model':          'TEXT',
    'n1_verified_at':    'TEXT',
}
for col, dtype in NEW_COLS.items():
    if col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {col} {dtype}')
        print(f'  ✓ {col} 컬럼 추가')

KEYS = [os.environ.get(k, "").strip() for k in ["GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"]]
KEYS = [k for k in KEYS if k]
# 2.5 계열만 사용 — flash-latest는 추론 텍스트로 JSON 파싱 실패
MODELS = ["gemini-2.5-flash-lite", "gemini-2.5-flash"]

def call_gemini(prompt, max_wait=3):
    """429 시 최대 max_wait번 60초 대기 후 재시도"""
    for wait in range(max_wait + 1):
        for model in MODELS:
            for k in KEYS:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={k}"
                payload = json.dumps({
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "maxOutputTokens": 1536,
                        "temperature": 0.2,
                        "responseMimeType": "application/json"
                    },
                }).encode("utf-8")
                req = urllib.request.Request(url, data=payload,
                                             headers={"Content-Type": "application/json"}, method="POST")
                try:
                    with urllib.request.urlopen(req, timeout=60) as r:
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
                    if e.code in (429, 503):
                        continue
                    continue
                except Exception:
                    continue
        if wait < max_wait:
            print(f"  (모든 키 429 — 60초 대기 {wait+1}/{max_wait})")
            time.sleep(60)
    raise RuntimeError("모든 모델·키 소진")

def parse_json(txt):
    s = txt.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-z]*\n?", "", s)
        s = re.sub(r"\n?```$", "", s)
    try:
        return json.loads(s)
    except:
        m = re.search(r'\{.*\}', s, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except:
                return None
    return None

# 대상: 골드 102건 (n1_verified_at=NULL인 것)
rows = db.execute("""
    SELECT sl.id, sl.corp_name, sl.title, sl.lead_type, sl.severity,
           sl.evidence, sl.evidence_deep, sl.numeric_facts,
           sl.company_context, sl.supply_chain_impact,
           sl.value_direction, sl.industry_alignment, sl.impact_magnitude,
           sl.short_term, sl.medium_term, sl.long_term,
           sl.cross_verified, sl.freshness_label,
           sl.fact_match_label, sl.fact_match_ratio
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH_CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND sl.n1_verified_at IS NULL
""").fetchall()
print(f'\nN-1 뉴스화 검증 대상: {len(rows)}건')

PROMPT = """당신은 경제 전문지 데스크 에디터이자 취재 기자입니다.
아래 취재 단서를 분석해 **실제 기사화 가능성을 검증**하세요.
악재(부정적 정보)도 기사 가치가 있습니다. 숨어 있던 리스크, 구조적 문제도 훌륭한 단독입니다.

═══════════════════════════════════
[회사] {corp_name}  /  [분류] {lead_type}
[단서 제목] {title}
[미래 가치 방향] {value_direction}  /  [산업 트렌드 대비] {industry_alignment}
[원문 매칭 수준] {fact_match}  /  [공시 최신성] {freshness}
[K-3 교차검증] {cross_verified}

[핵심 증거]
{evidence}

[사업보고서 원문 발췌]
{evidence_deep}

[정량 데이터]
{numeric_facts}

[공급망 영향]
{supply_chain}

[단기 영향] {short_term}
[중기 영향] {medium_term}
[장기 영향] {long_term}
═══════════════════════════════════

다음 JSON으로만 응답:
{{
  "newsability_score": 1~10,
  "headline_ko": "기사 헤드라인 후보 (30자 이내, 구체적 팩트 포함)",
  "story_angle": "취재 각도 — 독자에게 어떤 가치가 있나 (2문장)",
  "valence": "GOOD|BAD|MIXED",
  "valence_reason": "호재/악재 판단 근거 (1문장)",
  "publish_risk": "HIGH|MEDIUM|LOW",
  "publish_risk_reason": "기사화 리스크 설명 (예: '아직 계획 단계', '법적 리스크')",
  "missing_verification": "기사 쓰기 전 반드시 확인해야 할 사항 (없으면 NONE)",
  "already_public": "YES|LIKELY_NO|UNKNOWN",
  "story_type": "SCOOP|ANALYSIS|TREND|WARNING|PROFILE",
  "urgency": "IMMEDIATE|THIS_WEEK|THIS_MONTH",
  "ir_question_hint": "IR 담당자에게 꼭 물어볼 핵심 질문 1개"
}}

newsability_score 기준:
- 9~10: 즉시 출고 (SCOOP급, 시장에 알려지지 않은 중요 팩트)
- 7~8:  이번 주 출고 (명확한 방향성, 충분한 근거)
- 5~6:  추가 취재 후 출고 (방향은 맞지만 보강 필요)
- 3~4:  보류 (불확실성 높음, 추가 검증 필수)
- 1~2:  폐기 (증거 불충분, 기사 가치 없음)

story_type:
- SCOOP:   아직 언론에 보도 안 된 단독
- ANALYSIS: 데이터 기반 심층 분석
- TREND:   산업 트렌드 흐름 짚기
- WARNING: 잠재적 위험 경고
- PROFILE: 기업 변화 인물/전략 스토리
"""

scores = []
ok = err = 0
t0 = time.time()

for i, r in enumerate(rows, 1):
    # 정량 데이터 파싱
    num_summary = ''
    try:
        nf = json.loads(r['numeric_facts'] or '[]')
        if nf:
            parts = []
            for f in nf[:3]:
                if f.get('type') == 'change':
                    parts.append(f"{f.get('from')} → {f.get('to')} {f.get('unit','')}")
                elif f.get('type') == 'money':
                    parts.append(f"{f.get('value')} {f.get('unit','')}")
            num_summary = ' / '.join(parts)
    except:
        pass

    # 공급망 요약
    sc_summary = ''
    try:
        sc = json.loads(r['supply_chain_impact'] or '{}')
        as_sup = sc.get('as_supplier_to', [])
        if as_sup:
            sc_summary = f"거래처: {', '.join(x.get('name','') for x in as_sup[:3])}"
    except:
        pass

    prompt = PROMPT.format(
        corp_name=r['corp_name'],
        lead_type=r['lead_type'],
        title=r['title'] or '',
        value_direction=r['value_direction'] or '미분류',
        industry_alignment=r['industry_alignment'] or '미분류',
        fact_match=f"{r['fact_match_label']} ({r['fact_match_ratio']})",
        freshness=r['freshness_label'] or '-',
        cross_verified=r['cross_verified'] or '미실행',
        evidence=(r['evidence'] or '')[:500],
        evidence_deep=(r['evidence_deep'] or '')[:1200],
        numeric_facts=num_summary or '없음',
        supply_chain=sc_summary or '없음',
        short_term=r['short_term'] or '-',
        medium_term=r['medium_term'] or '-',
        long_term=r['long_term'] or '-',
    )

    try:
        resp, model = call_gemini(prompt)
        parsed = parse_json(resp)
        if not parsed:
            err += 1
            print(f'  [{i}] PARSE_FAIL: {r["corp_name"]}')
            continue

        score = parsed.get('newsability_score', 0)
        scores.append(score)
        db.execute("""UPDATE story_leads SET
                        newsability_score=?,
                        headline_ko=?,
                        story_angle=?,
                        valence=?,
                        publish_risk=?,
                        missing_verification=?,
                        already_public=?,
                        n1_model=?,
                        n1_verified_at=?,
                        value_rationale_ai=COALESCE(
                            json_patch(COALESCE(value_rationale_ai,'{}'),
                            json_object(
                              'story_type', ?,
                              'urgency', ?,
                              'ir_question_hint', ?,
                              'valence_reason', ?,
                              'publish_risk_reason', ?
                            )),
                            value_rationale_ai
                        )
                      WHERE id=?""",
                   [score,
                    (parsed.get('headline_ko', '') or '')[:100],
                    (parsed.get('story_angle', '') or '')[:400],
                    parsed.get('valence', 'MIXED'),
                    parsed.get('publish_risk', 'MEDIUM'),
                    (parsed.get('missing_verification', '') or '')[:300],
                    parsed.get('already_public', 'UNKNOWN'),
                    model, now,
                    parsed.get('story_type', ''),
                    parsed.get('urgency', ''),
                    (parsed.get('ir_question_hint', '') or '')[:200],
                    (parsed.get('valence_reason', '') or '')[:200],
                    (parsed.get('publish_risk_reason', '') or '')[:200],
                    r['id']])
        db.commit()
        ok += 1
        time.sleep(5)  # RPM 제한 대응
        if i % 10 == 0:
            avg = sum(scores) / len(scores) if scores else 0
            elapsed = (time.time() - t0) / 60
            print(f'  [{i:>3}/{len(rows)}] ok={ok} err={err} avg_score={avg:.1f} ({elapsed:.1f}분)')
    except RuntimeError:
        print(f'\n  [{i}] 한도 소진 — 중단')
        break
    except Exception as e:
        err += 1
        print(f'  [{i}] 오류: {r["corp_name"]} — {str(e)[:60]}')

elapsed = (time.time() - t0) / 60
avg_score = sum(scores) / len(scores) if scores else 0
print(f'\n[N-1 완료] {elapsed:.1f}분  ok={ok} err={err}  평균 newsability={avg_score:.1f}')

# 결과 분포
print('\n[Newsability 분포]')
for row in db.execute("""
    SELECT newsability_score, COUNT(*) c FROM story_leads
    WHERE n1_verified_at IS NOT NULL
    GROUP BY newsability_score ORDER BY newsability_score DESC
"""):
    bar = '█' * row[1]
    print(f'  {row[0]:>2}점: {row[1]:>3}건 {bar}')

print('\n[출고 우선순위]')
for row in db.execute("""
    SELECT newsability_score, valence, publish_risk, corp_name, headline_ko
    FROM story_leads
    WHERE n1_verified_at IS NOT NULL
      AND newsability_score >= 7
    ORDER BY newsability_score DESC, publish_risk ASC
    LIMIT 20
"""):
    risk_icon = '🟢' if row[2]=='LOW' else ('🟡' if row[2]=='MEDIUM' else '🔴')
    val_icon = '📈' if row[1]=='GOOD' else ('📉' if row[1]=='BAD' else '🔄')
    print(f'  {row[0]}점 {val_icon}{risk_icon} {row[3]} | {(row[4] or "")[:35]}')

print('\n[valence 분포]')
for row in db.execute("""SELECT valence, COUNT(*) c FROM story_leads
    WHERE n1_verified_at IS NOT NULL GROUP BY valence ORDER BY c DESC"""):
    print(f'  {row[0]}: {row[1]}')

print('\n[즉시 출고 SCOOP 후보 (score>=8, risk LOW/MEDIUM)]')
for row in db.execute("""
    SELECT sl.corp_name, sl.headline_ko, sl.newsability_score, sl.valence,
           sl.value_direction, sl.missing_verification
    FROM story_leads sl
    WHERE sl.n1_verified_at IS NOT NULL
      AND sl.newsability_score >= 8
      AND sl.publish_risk IN ('LOW','MEDIUM')
    ORDER BY sl.newsability_score DESC
"""):
    print(f'\n  [{row[2]}점/{row[3]}] {row[0]}')
    print(f'  헤드라인: {row[1] or "-"}')
    print(f'  방향: {row[4]}')
    print(f'  추가확인: {(row[5] or "NONE")[:80]}')
