"""
scripts/verify_sev5_leads.py
─────────────────────────────
sev=5 (severity=5) story_leads 단서 진위·보도여부·취재 포인트를 Gemini로 평가.
결과 저장: story_leads 테이블에 컬럼 4개 추가
  gemini_verified  (Y/N/UNCERTAIN)
  gemini_news_likely (Y/N/UNKNOWN)
  gemini_angles    (JSON array — 추가 조사 포인트)
  gemini_score     (1-5 진위 강도)

실행:
  python scripts/verify_sev5_leads.py --stats
  python scripts/verify_sev5_leads.py --limit 50
  python scripts/verify_sev5_leads.py                    # 전체 (미검증 sev=5만)
  python scripts/verify_sev5_leads.py --include-sev4     # sev=4도 포함
"""
import os, sys, json, sqlite3, argparse, time, io
import urllib.request, urllib.error
from pathlib import Path
from datetime import datetime

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# .env 로드
_env_path = ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, str(ROOT))
from config.gemini_models import GEMINI_FALLBACKS


def _keys():
    out = []
    for k in ["GEMINI_API_KEY","GEMINI_API_KEY_2","GEMINI_API_KEY_3"]:
        v = os.environ.get(k,"").strip()
        if v: out.append(v)
    return out


def ensure_columns(db):
    """story_leads에 Gemini 평가 컬럼 추가 (없으면)"""
    cols = [c[1] for c in db.execute("PRAGMA table_info(story_leads)")]
    for col, ddl in [
        ("gemini_verified",   "TEXT"),
        ("gemini_news_likely","TEXT"),
        ("gemini_angles",     "TEXT"),
        ("gemini_score",      "INTEGER DEFAULT 0"),
        ("gemini_verified_at","TEXT"),
        ("gemini_model",      "TEXT"),
        ("gemini_error",      "TEXT"),
    ]:
        if col not in cols:
            db.execute(f"ALTER TABLE story_leads ADD COLUMN {col} {ddl}")
    db.commit()


def build_prompt(lead):
    return f"""당신은 한국 증권부 기자입니다. 아래 DART 사업보고서 분석에서 추출된
"취재 단서"의 진위를 평가하세요.

[회사]: {lead['corp_name']}
[단서 유형]: {lead['lead_type']} (severity {lead['severity']}/5)
[제목]: {lead['title']}
[요약]: {(lead['summary'] or '')[:300]}
[근거 문장 (보고서 발췌)]:
{(lead['evidence'] or '')[:1500]}

평가 기준:
- 진위 (verified): 보고서 본문에서 실제로 발생/결정/완료된 변화가 있는가?
- 보도 여부 (news_likely): 이 변화가 언론에 이미 보도됐을 가능성이 높은가? (대형사·공시의무·증자 → Y / 본문 디테일 → N)
- 진위 강도 (score 1-5): 5=확실한 큰 변화, 3=가능성 있음, 1=단순 언급

──────────────────────────────────────
JSON 형식만 출력 (마크다운·설명 없이):

{{
  "verified": "Y|N|UNCERTAIN",
  "news_likely": "Y|N|UNKNOWN",
  "score": 1~5,
  "angles": ["추가 조사 포인트 3개"]
}}
"""


def call_gemini(prompt, keys):
    for model in GEMINI_FALLBACKS["flash"]:
        for api_key in keys:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
            payload = json.dumps({
                "contents":[{"parts":[{"text":prompt}]}],
                "generationConfig":{"maxOutputTokens":1024,"temperature":0.2}
            }).encode("utf-8")
            req = urllib.request.Request(url, data=payload,
                headers={"Content-Type":"application/json"}, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=90) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                txt = data["candidates"][0]["content"]["parts"][0]["text"]
                return txt, model
            except urllib.error.HTTPError as e:
                if e.code in (429, 503): continue
                if e.code == 404: break
                continue
            except Exception:
                continue
    raise RuntimeError("모든 모델·키 소진")


def parse_json(txt):
    import re
    s = txt.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-z]*\n?", "", s)
        s = re.sub(r"\n?```$", "", s)
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r'\{.*\}', s, re.DOTALL)
        if m:
            try: return json.loads(m.group(0))
            except: pass
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--stats", action="store_true")
    ap.add_argument("--include-sev4", action="store_true")
    args = ap.parse_args()

    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    ensure_columns(db)

    if args.stats:
        for sev in (5,4):
            tot = db.execute("SELECT COUNT(*) FROM story_leads WHERE severity=?", [sev]).fetchone()[0]
            ver = db.execute("SELECT COUNT(*) FROM story_leads WHERE severity=? AND gemini_verified IS NOT NULL", [sev]).fetchone()[0]
            print(f"sev={sev}: {ver} / {tot} 검증")
        return

    keys = _keys()
    if not keys:
        print("GEMINI_API_KEY 없음"); sys.exit(1)

    sev_filter = "(severity=5 OR severity=4)" if args.include_sev4 else "severity=5"
    q = f"""
        SELECT id, corp_code, corp_name, lead_type, severity, title, summary, evidence
        FROM story_leads
        WHERE {sev_filter}
          AND gemini_verified IS NULL
        ORDER BY severity DESC, id ASC
    """
    if args.limit:
        q += f" LIMIT {int(args.limit)}"
    cands = db.execute(q).fetchall()
    print(f"[info] 검증 대상: {len(cands)}개 ({'sev=5+4' if args.include_sev4 else 'sev=5만'})")

    ok = err = 0
    t0 = time.time()
    for i, lead in enumerate(cands, 1):
        try:
            resp, model = call_gemini(build_prompt(dict(lead)), keys)
            p = parse_json(resp)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not p:
                db.execute("""UPDATE story_leads SET gemini_verified='PARSE_FAIL',
                              gemini_model=?, gemini_verified_at=?, gemini_error='json parse'
                              WHERE id=?""", [model, now, lead['id']])
                err += 1
            else:
                db.execute("""UPDATE story_leads
                              SET gemini_verified=?, gemini_news_likely=?,
                                  gemini_score=?, gemini_angles=?,
                                  gemini_model=?, gemini_verified_at=?
                              WHERE id=?""",
                    [p.get("verified","UNCERTAIN"),
                     p.get("news_likely","UNKNOWN"),
                     int(p.get("score",0) or 0),
                     json.dumps(p.get("angles",[]), ensure_ascii=False),
                     model, now, lead['id']])
                ok += 1
            db.commit()
            if i % 10 == 0:
                el = (time.time()-t0)/60
                print(f"  [{i:>4}/{len(cands)}] ok={ok} err={err} ({el:.1f}분)")
        except RuntimeError as e:
            print(f"  [{i}] 한도 소진 — 종료: {e}")
            break
        except Exception as e:
            err += 1
            print(f"  [{i}] 오류: {str(e)[:100]}")

    print(f"\n[완료] ok={ok} err={err} / {len(cands)} 시도")
    print(f"소요: {(time.time()-t0)/60:.1f}분")

if __name__ == "__main__":
    main()
