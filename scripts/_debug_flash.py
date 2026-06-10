"""gemini-2.5-flash responseMimeType 테스트"""
import os, json, urllib.request, urllib.error, sys, re
sys.stdout.reconfigure(encoding="utf-8")
from pathlib import Path

ROOT = Path(__file__).parent.parent
env = ROOT / ".env"
if env.exists():
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

KEY = os.environ.get("GEMINI_API_KEY", "").strip()
MODEL = "gemini-2.5-flash-lite"

prompt = """당신은 한국 경제 전문 기자입니다.
[회사] 케이씨씨건설 / [분류] EARNINGS_RISK
[단서 제목] 분기 매출 감소 지속
[현재 분류] UNCERTAIN
[Evidence] 3분기 매출 20% 감소, 수주 잔고 감소 추세
[사업보고서 원문] 건설 경기 침체 영향으로 신규 수주 어려움.

다음 JSON으로만 응답:
{
  "value_direction": "POSITIVE_GROWTH|NEGATIVE_DECLINE|TRANSFORMATION|RISK_WARNING|UNCERTAIN",
  "direction_reason": "이 방향을 선택한 핵심 이유 (1문장)",
  "industry_alignment": "ALIGNED|CONTRARIAN|NEUTRAL",
  "short_term": "단기 영향 설명",
  "medium_term": "중기 영향 설명",
  "long_term": "장기 영향 설명",
  "impact_magnitude": "STRONG|MEDIUM|WEAK",
  "news_value": "HIGH|MEDIUM|LOW",
  "valence": "GOOD|BAD|MIXED",
  "confidence": 3
}"""

print(f"=== {MODEL} WITHOUT responseMimeType ===")
url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent?key={KEY}"
body = {"contents":[{"parts":[{"text":prompt}]}],"generationConfig":{"maxOutputTokens":1024,"temperature":0.1}}
payload = json.dumps(body).encode()
req = urllib.request.Request(url, data=payload, headers={"Content-Type":"application/json"}, method="POST")
try:
    with urllib.request.urlopen(req, timeout=15) as r:
        d = json.loads(r.read().decode())
    text = d["candidates"][0]["content"]["parts"][0].get("text","")
    print(f"finish_reason: {d['candidates'][0].get('finishReason','?')}")
    print(f"응답: {repr(text[:300])}")
    try:
        print(f"JSON 파싱: OK → {list(json.loads(text).keys())}")
    except:
        print("JSON 파싱: FAIL")
except urllib.error.HTTPError as e:
    print(f"HTTP {e.code}: {e.read().decode()[:200]}")

print(f"\n=== {MODEL} WITH responseMimeType=application/json ===")
body2 = {"contents":[{"parts":[{"text":prompt}]}],"generationConfig":{"maxOutputTokens":1024,"temperature":0.1,"responseMimeType":"application/json"}}
payload2 = json.dumps(body2).encode()
req2 = urllib.request.Request(url, data=payload2, headers={"Content-Type":"application/json"}, method="POST")
try:
    with urllib.request.urlopen(req2, timeout=15) as r:
        d = json.loads(r.read().decode())
    text = d["candidates"][0]["content"]["parts"][0].get("text","")
    print(f"finish_reason: {d['candidates'][0].get('finishReason','?')}")
    print(f"응답: {repr(text[:400])}")
    try:
        print(f"JSON 파싱: OK → {list(json.loads(text).keys())}")
    except Exception as e:
        print(f"JSON 파싱: FAIL - {e}")
except urllib.error.HTTPError as e:
    print(f"HTTP {e.code}: {e.read().decode()[:200]}")
