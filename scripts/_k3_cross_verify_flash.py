"""
K-3b: 골드 102건 cross-verify (Flash 모델 — Pro 한도 소진 대체)
- CONFIRMED / NEED_MORE / REJECT 분류
- gemini-2.5-flash 우선, flash-latest fallback
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
for col in ['cross_verified', 'cross_verify_model', 'cross_verify_at', 'cross_evidence']:
    if col not in cols:
        db.execute(f'ALTER TABLE story_leads ADD COLUMN {col} TEXT')

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
                body = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "maxOutputTokens": 2048,
                        "temperature": 0.1,
                        "responseMimeType": "application/json",
                        # 2.5 계열 thinking 비활성화 — thinking이 토큰 예산을 소진해
                        # JSON 본문이 잘려 PARSE_FAIL 발생하던 문제 수정
                        "thinkingConfig": {"thinkingBudget": 0}
                    }
                }
                payload = json.dumps(body).encode("utf-8")
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
    if not txt:
        return None
    s = txt.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-z]*\n?", "", s)
        s = re.sub(r"\n?```$", "", s)
    # 1) 정상 파싱
    try:
        return json.loads(s)
    except Exception:
        pass
    # 2) 첫 '{' ~ 마지막 '}' 구간 추출
    m = re.search(r'\{.*\}', s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    # 3) 잘린 JSON 복구 — 미닫힌 문자열/중괄호 보정 후 재시도
    frag = s[s.find("{"):] if "{" in s else s
    if frag:
        cand = frag
        # 미종료 문자열 닫기 (홀수 개의 " 이면 하나 추가)
        if cand.count('"') % 2 == 1:
            cand += '"'
        # 중괄호 균형 맞추기
        opens = cand.count("{") - cand.count("}")
        if opens > 0:
            cand += "}" * opens
        try:
            return json.loads(cand)
        except Exception:
            pass
    # 4) 키 단위 정규식 fallback (verdict만이라도 살림)
    vm = re.search(r'"verdict"\s*:\s*"(CONFIRMED|NEED_MORE|REJECT)"', s)
    if vm:
        out = {"verdict": vm.group(1)}
        for key in ("core_fact", "missing_evidence", "risk_note"):
            km = re.search(rf'"{key}"\s*:\s*"([^"]*)"', s)
            if km:
                out[key] = km.group(1)
        cm = re.search(r'"confidence"\s*:\s*([0-9.]+)', s)
        if cm:
            try: out["confidence"] = float(cm.group(1))
            except Exception: pass
        return out
    return None

# 대상: cross_verified=NULL인 골드 단서
golds = db.execute("""
    SELECT id, corp_name, title, lead_type, severity, evidence, evidence_deep,
           value_direction, industry_alignment
    FROM story_leads
    WHERE info_gap_label='HIGH_CONFIRMED'
      AND fact_match_label IN ('EXACT','STRONG')
      AND cross_verified IS NULL
""").fetchall()
print(f'K-3 cross-verify 대상: {len(golds)}건 (Flash 모델)')

PROMPT = """당신은 한국 증권부 시니어 에디터입니다. 아래 취재 단서를 **보수적 관점**으로 평가하세요.

[회사] {corp_name}
[단서 제목] {title}
[분류] {lead_type}  /  [심각도] {severity}/5
[미래 가치 방향] {value_direction}  /  [산업 트렌드 대비] {industry_alignment}

[Evidence]
{evidence}

[사업보고서 원문 발췌]
{evidence_deep}

══════════════════════════════════
다음 JSON으로만 응답:
{{
  "verdict": "CONFIRMED|NEED_MORE|REJECT",
  "confidence": 1~5,
  "core_fact": "팩트 1문장 (50자 이내)",
  "missing_evidence": "추가로 확인해야 할 자료 (없으면 NONE)",
  "risk_note": "발송 전 주의점 (없으면 NONE)"
}}

판단 기준:
- CONFIRMED: 원문에 명확한 변화+근거, 회사 가치 영향 분명
- NEED_MORE: 변화 시그널 있으나 추가 확인 (재무·뉴스·IR) 필요
- REJECT: 단순 서술·계획 단계·증거 부족
- confidence 5=즉시 출고 / 3=후속 확인 / 1=폐기 권장
"""

ok = need = rej = err = 0
t0 = time.time()

for i, r in enumerate(golds, 1):
    prompt = PROMPT.format(
        corp_name=r['corp_name'],
        title=r['title'] or '',
        lead_type=r['lead_type'],
        severity=r['severity'],
        value_direction=r['value_direction'] or '-',
        industry_alignment=r['industry_alignment'] or '-',
        evidence=(r['evidence'] or '')[:500],
        evidence_deep=(r['evidence_deep'] or '')[:1500]
    )
    try:
        resp, model = call_gemini(prompt)
        parsed = parse_json(resp)
        if not parsed:
            db.execute("""UPDATE story_leads SET cross_verified='PARSE_FAIL',
                          cross_verify_model=?, cross_verify_at=? WHERE id=?""",
                       [model, now, r['id']])
            err += 1
        else:
            verdict = parsed.get("verdict", "NEED_MORE")
            if verdict == "CONFIRMED":   ok += 1
            elif verdict == "REJECT":    rej += 1
            else:                        need += 1
            ev = json.dumps({
                "core_fact":  parsed.get("core_fact", ""),
                "missing":    parsed.get("missing_evidence", ""),
                "risk":       parsed.get("risk_note", "NONE"),
                "confidence": parsed.get("confidence", 0),
            }, ensure_ascii=False)
            db.execute("""UPDATE story_leads SET
                            cross_verified=?, cross_verify_model=?, cross_verify_at=?,
                            cross_evidence=?
                          WHERE id=?""",
                       [verdict, model, now, ev, r['id']])
        db.commit()
        time.sleep(5)  # RPM 제한 대응 (12 req/min 유지)
        if i % 10 == 0:
            elapsed = (time.time() - t0) / 60
            print(f'  [{i:>3}/{len(golds)}] CONFIRMED={ok} NEED_MORE={need} REJECT={rej} err={err} ({elapsed:.1f}분)')
    except RuntimeError:
        print(f'\n  [{i}] 한도 소진 — 중단')
        break
    except Exception as e:
        err += 1
        print(f'  [{i}] 오류: {str(e)[:60]}')

elapsed = (time.time() - t0) / 60
print(f'\n[K-3 완료] {elapsed:.1f}분')
print(f'  CONFIRMED:  {ok}')
print(f'  NEED_MORE:  {need}')
print(f'  REJECT:     {rej}')
print(f'  PARSE_FAIL: {err}')

# ULTRA GOLD
ultra = db.execute("""
    SELECT COUNT(*) FROM story_leads sl
    WHERE sl.cross_verified='CONFIRMED'
      AND sl.fact_match_label IN ('EXACT','STRONG')
      AND EXISTS (SELECT 1 FROM ir_contacts ic
                  WHERE ic.corp_code=sl.corp_code AND ic.is_active=1
                    AND ic.ir_email IS NOT NULL AND ic.ir_email<>''
                    AND substr(ic.ir_email,1,1)<>'_')
""").fetchone()[0]
print(f'\n🏆 ULTRA GOLD (CONFIRMED + IR 이메일): {ultra}건')
