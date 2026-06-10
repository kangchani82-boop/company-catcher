"""
L-2b: UNCERTAIN 43건 + industry_alignment NULL 재분류
- 더 강한 프롬프트: UNCERTAIN 최대한 억제, 명확한 방향 강제
- 악재/호재 모두 포함 (기사화 가치 판단)
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
                        "maxOutputTokens": 1024,
                        "temperature": 0.3,
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

# UNCERTAIN 43건 + industry_alignment NULL 전체 (중복 제거)
rows = db.execute("""
    SELECT sl.id, sl.corp_name, sl.title, sl.lead_type, sl.severity,
           sl.evidence, sl.evidence_deep, sl.company_context, sl.value_direction
    FROM story_leads sl
    WHERE sl.info_gap_label='HIGH_CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND (sl.value_direction='UNCERTAIN' OR sl.industry_alignment IS NULL)
""").fetchall()
print(f'재분류 대상: {len(rows)}건 (UNCERTAIN + alignment NULL)')

# 더 강한 분류 프롬프트
PROMPT = """당신은 한국 경제 전문 기자 겸 증권 분석가입니다.
아래 기업의 사업보고서 변화 단서를 평가하고 **반드시 명확한 방향**을 제시해주세요.

[회사] {corp_name}  /  [분류] {lead_type}
[단서 제목] {title}
[현재 분류] {current_dir}

[Evidence]
{evidence}

[사업보고서 원문]
{evidence_deep}

══════════════════════════════════
⚡ 중요: 악재도 기사 가치가 있습니다. 긍정/부정/전환 모두 명확하게 판단하세요.
⚡ UNCERTAIN은 정말 양방향 해석이 동등할 때만 사용. 증거가 약해도 방향성이 보이면 명확히 선택.

다음 JSON으로만 응답:
{{
  "value_direction": "POSITIVE_GROWTH|NEGATIVE_DECLINE|TRANSFORMATION|RISK_WARNING|UNCERTAIN",
  "direction_reason": "이 방향을 선택한 핵심 이유 (1문장)",
  "industry_alignment": "ALIGNED|CONTRARIAN|NEUTRAL",
  "alignment_reason": "업종 트렌드 대비 설명 (1문장)",
  "short_term": "단기 1-3개월 영향",
  "medium_term": "중기 6-12개월 영향",
  "long_term": "장기 1년+ 영향",
  "impact_magnitude": "STRONG|MEDIUM|WEAK",
  "news_value": "기사 가치 판단: HIGH|MEDIUM|LOW",
  "valence": "GOOD|BAD|MIXED",
  "confidence": 1~5
}}

value_direction 선택 기준:
- POSITIVE_GROWTH: 매출·이익·시장 성장 기대 (신규 고객, 투자, 특허 확보)
- NEGATIVE_DECLINE: 실적 악화, 시장 축소, 경쟁 심화, 수익성 저하
- TRANSFORMATION: 사업 구조 전환 (업종 변경, M&A, 핵심 사업 교체)
- RISK_WARNING: 잠재적 리스크 (규제, 소송, 부채, 유동성)
- UNCERTAIN: 정말 어느 방향인지 판단 불가 (최후 수단)
"""

stats = {}
ok = err = 0
t0 = time.time()

for i, r in enumerate(rows, 1):
    ctx = {}
    try:
        ctx = json.loads(r['company_context'] or '{}') if r['company_context'] else {}
    except:
        pass

    prompt = PROMPT.format(
        corp_name=r['corp_name'],
        lead_type=r['lead_type'],
        title=r['title'] or '',
        current_dir=r['value_direction'] or 'NULL',
        evidence=(r['evidence'] or '')[:500],
        evidence_deep=(r['evidence_deep'] or '')[:1500]
    )
    try:
        resp, model = call_gemini(prompt)
        parsed = parse_json(resp)
        if not parsed:
            err += 1
            print(f'  [{i}] PARSE_FAIL: {r["corp_name"]}')
            continue
        vd = parsed.get('value_direction', 'UNCERTAIN')
        stats[vd] = stats.get(vd, 0) + 1
        db.execute("""UPDATE story_leads SET
                        value_direction=?,
                        industry_alignment=?,
                        short_term=?, medium_term=?, long_term=?,
                        impact_magnitude=?, value_confidence=?,
                        value_rationale_ai=?,
                        value_classified_at=?
                      WHERE id=?""",
                   [vd,
                    parsed.get('industry_alignment', 'NEUTRAL'),
                    (parsed.get('short_term', '') or '')[:300],
                    (parsed.get('medium_term', '') or '')[:300],
                    (parsed.get('long_term', '') or '')[:300],
                    parsed.get('impact_magnitude', 'MEDIUM'),
                    str(parsed.get('confidence', 0)),
                    json.dumps({
                        'direction_reason': parsed.get('direction_reason', ''),
                        'alignment_reason': parsed.get('alignment_reason', ''),
                        'news_value': parsed.get('news_value', ''),
                        'valence': parsed.get('valence', ''),
                    }, ensure_ascii=False),
                    now, r['id']])
        db.commit()
        ok += 1
        time.sleep(5)  # RPM 제한 대응
        if i % 10 == 0:
            elapsed = (time.time() - t0) / 60
            still_unc = stats.get('UNCERTAIN', 0)
            print(f'  [{i:>3}/{len(rows)}] ok={ok} err={err} UNCERTAIN={still_unc} ({elapsed:.1f}분)')
    except RuntimeError:
        print(f'\n  [{i}] 한도 소진 — 중단')
        break
    except Exception as e:
        err += 1
        print(f'  [{i}] 오류: {str(e)[:60]}')

elapsed = (time.time() - t0) / 60
print(f'\n[L-2b 완료] {elapsed:.1f}분  ok={ok} err={err}')
print('\n[방향 분포]')
for k, v in sorted(stats.items(), key=lambda x: -x[1]):
    print(f'  {k:<22} {v}')

# 최종 현황
print('\n[최종 현황]')
for row in db.execute("""SELECT value_direction, industry_alignment, COUNT(*) c
    FROM story_leads WHERE info_gap_label='HIGH_CONFIRMED' AND fact_match_label IN ('EXACT','STRONG')
    GROUP BY value_direction, industry_alignment ORDER BY c DESC"""):
    print(f'  {row[0] or "NULL":<22} / {row[1] or "NULL":<12} : {row[2]}')
