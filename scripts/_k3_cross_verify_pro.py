"""
K-3: 골드 단서 102건 3차 cross-verify (다른 모델)
- 입력: HIGH_CONFIRMED + fact_match_label IN ('EXACT','STRONG')
- 모델: gemini-2.5-pro (또는 다른 모델)
- 프롬프트: 보수적 평가 — "이 단서가 진짜 회사 가치에 영향 있는가?"
- 출력: cross_verified Y/N/UNCERTAIN
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
for col in ['cross_verified','cross_verify_model','cross_verify_at','cross_evidence']:
    if col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {col} TEXT')

# Gemini 호출 (pro 우선, fallback flash)
KEYS = [os.environ.get(k,"").strip() for k in ["GEMINI_API_KEY","GEMINI_API_KEY_2","GEMINI_API_KEY_3"]]
KEYS = [k for k in KEYS if k]
MODELS = ["gemini-2.5-pro","gemini-flash-latest","gemini-2.5-flash"]

def call_gemini(prompt):
    for model in MODELS:
        for k in KEYS:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={k}"
            payload = json.dumps({
                "contents":[{"parts":[{"text":prompt}]}],
                "generationConfig":{"maxOutputTokens":1024,"temperature":0.1}
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

# 골드 단서 추출
golds = db.execute("""
    SELECT id, corp_name, title, lead_type, severity, evidence, evidence_deep
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED'
      AND fact_match_label IN ('EXACT','STRONG')
      AND cross_verified IS NULL
""").fetchall()
print(f'K-3 cross-verify 대상: {len(golds)}건')

PROMPT = """당신은 한국 증권부 시니어 에디터입니다. 아래 취재 단서를 **보수적 관점**에서 평가하세요.

[회사] {corp_name}
[단서 제목] {title}
[분류] {lead_type}
[심각도] {severity}/5

[Evidence 요약]
{evidence}

[보고서 원문 발췌 (강화)]
{evidence_deep}

══════════════════════════════════
다음 JSON으로만 출력:
{{
  "verdict": "CONFIRMED|NEED_MORE|REJECT",
  "confidence": 1~5,
  "core_fact": "팩트 1문장 (50자 이내)",
  "missing_evidence": "추가로 확인해야 할 자료",
  "risk_note": "발송 전 주의점 (없으면 'NONE')"
}}

평가 기준:
- CONFIRMED: 원문에 명확한 변화·근거 + 회사 가치 영향 분명
- NEED_MORE: 변화 시그널은 있으나 추가 외부 자료 (재무·뉴스·IR) 필요
- REJECT: 단순 서술·계획 단계·증거 부족
- confidence 5 = 즉시 출고 가능 / 3 = 후속 확인 / 1 = 폐기 권장
"""

ok = need = rej = err = 0
t0 = time.time()

for i, r in enumerate(golds, 1):
    prompt = PROMPT.format(
        corp_name=r['corp_name'], title=r['title'] or '',
        lead_type=r['lead_type'], severity=r['severity'],
        evidence=(r['evidence'] or '')[:500],
        evidence_deep=(r['evidence_deep'] or '')[:1500])
    try:
        resp, model = call_gemini(prompt)
        parsed = parse_json(resp)
        if not parsed:
            db.execute("""UPDATE story_leads SET cross_verified='PARSE_FAIL',
                          cross_verify_model=?, cross_verify_at=? WHERE id=?""",
                       [model, now, r['id']])
            err += 1
        else:
            verdict = parsed.get("verdict","NEED_MORE")
            if verdict=="CONFIRMED": ok += 1
            elif verdict=="REJECT": rej += 1
            else: need += 1
            ev_summary = json.dumps({
                "core_fact": parsed.get("core_fact",""),
                "missing": parsed.get("missing_evidence",""),
                "risk": parsed.get("risk_note","NONE"),
                "confidence": parsed.get("confidence",0),
            }, ensure_ascii=False)
            db.execute("""UPDATE story_leads SET
                            cross_verified=?, cross_verify_model=?, cross_verify_at=?,
                            cross_evidence=?
                          WHERE id=?""",
                       [verdict, model, now, ev_summary, r['id']])
        db.commit()
        if i % 10 == 0:
            print(f'  [{i:>3}/{len(golds)}] CONFIRMED={ok} NEED_MORE={need} REJECT={rej} err={err} ({(time.time()-t0)/60:.1f}분)')
    except RuntimeError as e:
        print(f'  [{i}] 한도 소진 — 종료')
        break
    except Exception as e:
        err += 1

print(f'\n[K-3 완료]')
print(f'  CONFIRMED: {ok} (즉시 출고)')
print(f'  NEED_MORE: {need} (추가 자료 필요)')
print(f'  REJECT:    {rej} (폐기 권장)')
print(f'  PARSE_FAIL: {err}')

# 최종 ULTRA_GOLD: CONFIRMED + EXACT + IR 이메일
ultra = db.execute("""
    SELECT COUNT(*) FROM story_leads sl
    WHERE sl.cross_verified='CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND EXISTS (SELECT 1 FROM ir_contacts ic
                  WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
                    AND ic.ir_email IS NOT NULL AND ic.ir_email<>''
                    AND substr(ic.ir_email,1,1)<>'_')
""").fetchone()[0]
print(f'\n🏆🏆 ULTRA GOLD (CONFIRMED + EXACT + IR 이메일): {ultra}건')
