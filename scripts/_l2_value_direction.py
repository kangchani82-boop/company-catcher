"""
L-2: Gemini 정밀 미래 가치 분류
- 입력: 골드 102건 단서 + 재무 + sector
- 출력: 산업 트렌드 대비 방향 + 단/중/장기 + 시총 영향
"""
import sqlite3, json, os, sys, time, io, re
import urllib.request, urllib.error
from pathlib import Path
from datetime import datetime

try: sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except: pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
_env_path = ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line=line.strip()
        if line and not line.startswith("#") and "=" in line:
            k,v=line.split("=",1); os.environ.setdefault(k.strip(), v.strip())

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 컬럼 추가
cols = [c[1] for c in db.execute('PRAGMA table_info(story_leads)')]
NEW_COLS = ['industry_alignment','short_term','medium_term','long_term',
            'impact_magnitude','value_confidence','value_rationale_ai']
for col in NEW_COLS:
    if col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {col} TEXT')

KEYS = [os.environ.get(k,"").strip() for k in ["GEMINI_API_KEY","GEMINI_API_KEY_2","GEMINI_API_KEY_3"]]
KEYS = [k for k in KEYS if k]
MODELS = ["gemini-flash-latest","gemini-2.5-flash-lite","gemini-2.5-flash","gemini-3-flash-preview","gemini-3.1-flash-lite"]

def call_gemini(prompt):
    for model in MODELS:
        for k in KEYS:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={k}"
            payload = json.dumps({
                "contents":[{"parts":[{"text":prompt}]}],
                "generationConfig":{"maxOutputTokens":1024,"temperature":0.2}
            }).encode("utf-8")
            req = urllib.request.Request(url, data=payload, headers={"Content-Type":"application/json"}, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    d = json.loads(r.read().decode("utf-8"))
                return d["candidates"][0]["content"]["parts"][0]["text"], model
            except urllib.error.HTTPError as e:
                if e.code in (429,503): continue
                continue
            except Exception: continue
    raise RuntimeError("모든 모델·키 소진")

def parse_json(txt):
    s = txt.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-z]*\n?","",s); s = re.sub(r"\n?```$","",s)
    try: return json.loads(s)
    except:
        m = re.search(r'\{.*\}', s, re.DOTALL)
        if m:
            try: return json.loads(m.group(0))
            except: return None
    return None

PROMPT = """당신은 증권사 산업 분석가입니다. 아래 회사의 사업보고서 변화 시그널을 평가해주세요.

[회사] {corp_name}  /  [업종] {sector}  /  [시장] {market}
[최근 재무] 매출 {revenue}억, 영업이익 {oi}억, 영업이익률 {om}%

[단서 제목] {title}
[Evidence]
{evidence}

[원문 발췌]
{evidence_deep}

──────────────────────────────────
다음 JSON으로만 응답:

{{
  "value_direction": "POSITIVE_GROWTH|NEGATIVE_DECLINE|TRANSFORMATION|RISK_WARNING|UNCERTAIN",
  "confidence": 1~5,
  "industry_alignment": "ALIGNED|CONTRARIAN|NEUTRAL",
  "industry_trend": "이 업종의 최근 일반 트렌드 (1문장)",
  "short_term": "단기 (1-3개월) 영향 한 문장",
  "medium_term": "중기 (6-12개월) 영향 한 문장",
  "long_term": "장기 (1년+) 영향 한 문장",
  "impact_magnitude": "STRONG|MEDIUM|WEAK",
  "rationale": "종합 판단 근거 (2-3 문장, 산업 트렌드 대비)"
}}

평가 기준:
- value_direction: 회사 가치에 명확히 긍정/부정/전환/리스크인지
- industry_alignment: 산업 트렌드와 같은 방향(ALIGNED) / 역행(CONTRARIAN) / 무관(NEUTRAL)
- impact_magnitude: 시총 5%+ 영향(STRONG) / 1-5%(MEDIUM) / <1%(WEAK)
- 단기/중기/장기 영향은 구체적으로 (예: "신제품 인지도 확산", "경쟁사 진입으로 점유율 압박")
"""

# 골드 102건 (cross_verified=null 인 것만)
rows = db.execute("""
    SELECT sl.id, sl.corp_name, sl.title, sl.severity, sl.evidence, sl.evidence_deep,
           sl.company_context
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH_CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND sl.industry_alignment IS NULL
""").fetchall()
print(f'대상: {len(rows)}건')

stats = {}
ok = err = 0
t0 = time.time()
for i, r in enumerate(rows, 1):
    # 컨텍스트 추출
    ctx = json.loads(r['company_context'] or '{}') if r['company_context'] else {}
    fin = ctx.get('recent_financials') or {}
    rev = fin.get('revenue_billion_krw','-')
    oi = fin.get('operating_income_billion_krw','-')
    om = fin.get('operating_margin','-')

    prompt = PROMPT.format(
        corp_name=r['corp_name'], sector=ctx.get('sector','-'), market=ctx.get('market','-'),
        revenue=rev, oi=oi, om=om,
        title=r['title'] or '', evidence=(r['evidence'] or '')[:400],
        evidence_deep=(r['evidence_deep'] or '')[:1200])
    try:
        resp, model = call_gemini(prompt)
        parsed = parse_json(resp)
        if not parsed:
            err += 1
            continue
        vd = parsed.get('value_direction','UNCERTAIN')
        stats[vd] = stats.get(vd,0) + 1
        db.execute("""UPDATE story_leads SET
                        value_direction=?,
                        industry_alignment=?,
                        short_term=?, medium_term=?, long_term=?,
                        impact_magnitude=?, value_confidence=?,
                        value_rationale_ai=?,
                        value_classified_at=?
                      WHERE id=?""",
                   [vd,
                    parsed.get('industry_alignment','NEUTRAL'),
                    (parsed.get('short_term','') or '')[:300],
                    (parsed.get('medium_term','') or '')[:300],
                    (parsed.get('long_term','') or '')[:300],
                    parsed.get('impact_magnitude','MEDIUM'),
                    str(parsed.get('confidence',0)),
                    json.dumps({
                      'trend': parsed.get('industry_trend',''),
                      'rationale': parsed.get('rationale','')
                    }, ensure_ascii=False),
                    now, r['id']])
        db.commit()
        ok += 1
        if i % 10 == 0:
            el = (time.time()-t0)/60
            print(f'  [{i:>3}/{len(rows)}] ok={ok} err={err} {el:.1f}분')
    except RuntimeError:
        print(f'  [{i}] 한도 소진 — 종료')
        break
    except Exception as e:
        err += 1

print(f'\n[완료] ok={ok} err={err}  소요 {(time.time()-t0)/60:.1f}분')
print('\n[방향 분포]')
for k,v in sorted(stats.items(), key=lambda x:-x[1]):
    print(f'  {k:<22} {v}')

# alignment 분포
print('\n[산업 트렌드 대비]')
for r in db.execute("""SELECT industry_alignment, COUNT(*) c
                       FROM story_leads
                       WHERE info_gap_label='HIGH_CONFIRMED'
                         AND fact_match_label IN ('EXACT','STRONG')
                         AND industry_alignment IS NOT NULL
                       GROUP BY industry_alignment ORDER BY c DESC"""):
    print(f'  {r[0]:<14} {r[1]}')

# impact magnitude
print('\n[시총 영향]')
for r in db.execute("""SELECT impact_magnitude, COUNT(*) c
                       FROM story_leads
                       WHERE info_gap_label='HIGH_CONFIRMED'
                         AND fact_match_label IN ('EXACT','STRONG')
                         AND impact_magnitude IS NOT NULL
                       GROUP BY impact_magnitude ORDER BY c DESC"""):
    print(f'  {r[0]:<14} {r[1]}')
