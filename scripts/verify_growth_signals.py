"""
scripts/verify_growth_signals.py
─────────────────────────────────
사용자가 정의한 "회사 성장 변화 14 카테고리" 키워드를 ai_comparisons.result에서 발견
하고, Gemini로 진위·중요도를 재평가해 `growth_signals` 테이블에 저장.

목적: 단순 룰 매칭으로 잡히지 않는 "정보 격차 큰" 변화 시그널을 정밀하게 추출.

실행:
  python scripts/verify_growth_signals.py --stats        # 현재 통계
  python scripts/verify_growth_signals.py --limit 100    # 100건만
  python scripts/verify_growth_signals.py                # 전체 진행
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

# 환경 변수
_env_path = ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# SSOT
sys.path.insert(0, str(ROOT))
from config.gemini_models import GEMINI_FALLBACKS

# 사용자 정의 14개 카테고리
GROWTH_CATEGORIES = {
    "facility_investment": ["시설투자", "공장 신설", "공장 증설", "생산 시설"],
    "corp_acquire":        ["법인 인수", "기업 인수", "타법인 인수", "회사 인수"],
    "corp_setup":          ["법인 설립", "자회사 신설", "합작법인", "JV 설립"],
    "corp_close_sell":     ["법인 폐쇄", "법인 매각", "자회사 매각", "사업부 매각"],
    "customer_change":     ["고객사 추가", "신규 고객", "주요 거래처 추가", "고객 변화"],
    "new_product":         ["신규 제품", "신제품 출시", "신규 라인업"],
    "new_project":         ["신규 과제", "신규 프로젝트", "신규 사업 과제"],
    "gov_grant":           ["정부 과제", "국책 과제", "정부 출연금"],
    "new_business":        ["신규사업", "신규 사업 진출", "사업 다각화"],
    "exit_business":       ["사업 철수", "사업 종료", "사업 중단"],
    "new_patent":          ["신규 특허", "특허 등록", "특허 출원"],
    "equity_invest":       ["타법인 출자", "주식 양수도", "지분 취득 결정"],
    "global_customer":     ["글로벌 고객", "해외 거래처", "수출 거래처"],
    "regulator_risk":      ["공정위 조사", "검찰 수사", "조세 소송", "세무조사"],
}
ALL_KEYWORDS = [kw for kws in GROWTH_CATEGORIES.values() for kw in kws]

# Gemini 키
def _keys():
    out = []
    for k in ["GEMINI_API_KEY","GEMINI_API_KEY_2","GEMINI_API_KEY_3"]:
        v = os.environ.get(k,"").strip()
        if v: out.append(v)
    return out

# ─────────── 테이블 생성 ───────────
def ensure_table(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS growth_signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            comparison_id   INTEGER UNIQUE,
            corp_code       TEXT NOT NULL,
            corp_name       TEXT,
            matched_keywords TEXT,
            has_change      INTEGER DEFAULT 0,
            change_categories TEXT,
            importance      INTEGER DEFAULT 0,
            evidence        TEXT,
            news_likely     TEXT,
            angles          TEXT,
            model           TEXT,
            status          TEXT DEFAULT 'ok',
            error_msg       TEXT,
            raw_response    TEXT,
            created_at      TEXT
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_growth_corp ON growth_signals(corp_code)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_growth_imp  ON growth_signals(importance DESC)")
    db.commit()

# ─────────── 키워드 매칭 비교 추출 ───────────
def find_candidates(db, limit=None, blind=False):
    """ai_comparisons 미처리분 추출.
    - blind=False: 키워드 매칭된 비교만 (디폴트)
    - blind=True : 키워드 매칭 X 비교 (false negative 검증용)
    """
    # KOSPI+KOSDAQ 한정
    sql = """
        SELECT ac.id, ac.corp_code, ac.corp_name, ac.result
        FROM ai_comparisons ac
        JOIN companies c ON c.corp_code=ac.corp_code AND c.market IN ('KOSPI','KOSDAQ')
        WHERE ac.status='ok' AND ac.result IS NOT NULL
          AND ac.report_type_a='2025_annual' AND ac.report_type_b='2026_q1'
          AND NOT EXISTS (SELECT 1 FROM growth_signals gs WHERE gs.comparison_id=ac.id)
        ORDER BY ac.id ASC
    """
    rows = db.execute(sql).fetchall()

    out = []
    kw_lower = [kw.lower() for kw in ALL_KEYWORDS]
    for r in rows:
        rid, ccode, cname, txt = r
        txt_l = (txt or "").lower()
        matched = [kw for kw, kwl in zip(ALL_KEYWORDS, kw_lower) if kwl in txt_l]
        is_match = bool(matched)
        if blind and is_match: continue       # blind 모드: 매칭 X 만
        if not blind and not is_match: continue  # 일반 모드: 매칭만
        out.append((rid, ccode, cname, txt, matched))
        if limit and len(out) >= limit:
            break
    return out

# ─────────── Gemini 호출 ───────────
def build_prompt(corp_name, result_text, matched_keywords):
    return f"""당신은 한국 증권부 기자입니다. 아래 DART 사업보고서 비교 분석 결과에서
**{corp_name}**의 회사 가치에 영향을 주는 "성장 변화 시그널"을 정밀하게 평가하세요.

[성장 변화 14 카테고리]
{', '.join(GROWTH_CATEGORIES.keys())}

[키워드 매칭 결과]
{', '.join(matched_keywords[:10])}

[비교 분석 텍스트 (최대 4000자)]
{result_text[:4000]}

──────────────────────────────────────
다음 JSON 형식으로만 출력 (마크다운·설명 없이 JSON만):

{{
  "has_change": true|false,
  "change_categories": ["facility_investment","new_product",...],
  "importance": 1~5,
  "evidence": "보고서에서 실제 변화를 드러내는 핵심 문장 (200자 이내)",
  "news_likely": "Y|N|UNKNOWN (이 변화가 이미 보도됐을 것 같은가?)",
  "angles": ["추가 조사 포인트 3가지"]
}}

평가 기준:
- has_change=true 만족 조건: "결정·완료·체결·시작·출시" 등 실제 발생 표현 + 구체 근거
- "검토 중", "계획 중", "예정", "가능성" 등 의도 단계는 false
- importance 5 = 시총 영향 큰 변화 / 3 = 사업 방향 변화 / 1 = 미미한 언급
- news_likely=N → 정보 격차 큰 시그널 (사이트 목적상 우선순위 높음)
"""

def call_gemini(prompt, keys):
    for model in GEMINI_FALLBACKS["flash"]:
        for api_key in keys:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
            payload = json.dumps({
                "contents":[{"parts":[{"text":prompt}]}],
                "generationConfig":{"maxOutputTokens":2048,"temperature":0.2}
            }).encode("utf-8")
            req = urllib.request.Request(url, data=payload, headers={"Content-Type":"application/json"}, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                txt = data["candidates"][0]["content"]["parts"][0]["text"]
                return txt, model
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                if e.code in (429, 503): continue
                if e.code == 404: break
                continue
            except Exception:
                continue
    raise RuntimeError("모든 모델·키 소진")

def parse_response(txt):
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

# ─────────── 메인 ───────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--stats", action="store_true")
    ap.add_argument("--blind", action="store_true",
                    help="키워드 매칭 X 비교를 검증 (false negative 발굴)")
    args = ap.parse_args()

    db = sqlite3.connect(str(DB_PATH), timeout=30)
    ensure_table(db)

    if args.stats:
        n = db.execute("SELECT COUNT(*) FROM growth_signals").fetchone()[0]
        hi = db.execute("SELECT COUNT(*) FROM growth_signals WHERE importance>=4").fetchone()[0]
        gap = db.execute("SELECT COUNT(*) FROM growth_signals WHERE news_likely='N'").fetchone()[0]
        print(f"growth_signals: {n}건  (importance≥4: {hi}, 정보격차 큼: {gap})")
        return

    keys = _keys()
    if not keys:
        print("GEMINI_API_KEY 없음"); sys.exit(1)

    candidates = find_candidates(db,
                                  limit=args.limit if args.limit else None,
                                  blind=args.blind)
    mode = "blind (매칭 X)" if args.blind else "키워드 매칭"
    print(f"[info] {mode} 후보: {len(candidates)}개")

    ok = err = 0
    t0 = time.time()
    for i, (cid, ccode, cname, txt, matched) in enumerate(candidates, 1):
        prompt = build_prompt(cname or "(이름 없음)", txt, matched)
        try:
            resp, model = call_gemini(prompt, keys)
            parsed = parse_response(resp)
            if not parsed:
                db.execute("""INSERT OR IGNORE INTO growth_signals
                    (comparison_id, corp_code, corp_name, matched_keywords, status, error_msg,
                     raw_response, model, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    [cid, ccode, cname, json.dumps(matched, ensure_ascii=False),
                     'parse_fail', 'json parse', resp[:2000], model,
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
                err += 1
            else:
                db.execute("""INSERT OR IGNORE INTO growth_signals
                    (comparison_id, corp_code, corp_name, matched_keywords,
                     has_change, change_categories, importance, evidence,
                     news_likely, angles, model, status, raw_response, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [cid, ccode, cname, json.dumps(matched, ensure_ascii=False),
                     1 if parsed.get("has_change") else 0,
                     json.dumps(parsed.get("change_categories",[]), ensure_ascii=False),
                     int(parsed.get("importance",0) or 0),
                     (parsed.get("evidence","") or "")[:500],
                     parsed.get("news_likely","UNKNOWN"),
                     json.dumps(parsed.get("angles",[]), ensure_ascii=False),
                     model, 'ok', resp[:2000],
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
                ok += 1
            db.commit()
            if i % 10 == 0:
                el = (time.time()-t0)/60
                print(f"  [{i:>4}/{len(candidates)}] ok={ok} err={err} ({el:.1f}분 경과)")
        except RuntimeError as e:
            print(f"  [{i}] 한도 소진 — 종료: {e}")
            break
        except Exception as e:
            err += 1
            print(f"  [{i}] 오류: {str(e)[:100]}")

    print(f"\n[완료] ok={ok} err={err} / {len(candidates)} 시도")
    print(f"소요: {(time.time()-t0)/60:.1f}분")

if __name__ == "__main__":
    main()
