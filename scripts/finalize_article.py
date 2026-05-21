"""
scripts/finalize_article.py
────────────────────────────
IR 답장을 기반으로 1차 기사 → 최종 기사 자동 재작성.

흐름:
  1) ir_emails (direction='in') 답장에서 본문 추출
  2) 답장 본문에서 답변 정보 추출 (질문 vs 답변 매칭)
  3) 1차 article_draft + 답변 → Gemini로 최종 기사 재작성
  4) 새 article_draft 행 생성 (status='ready', parent_article_id 참조)

실행:
  python scripts/finalize_article.py --email-id 5    # 특정 답장 처리
  python scripts/finalize_article.py                  # 미처리 답장 전체
  python scripts/finalize_article.py --stats          # 통계만
"""
import io, os, re, sys, json, sqlite3, argparse, requests, time
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# .env 로드
if ENV_PATH.exists():
    for line in open(ENV_PATH, encoding='utf-8'):
        if '=' in line and not line.startswith('#'):
            k, v = line.strip().split('=', 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


# 2026-05 정책 반영, 2.0 Legacy 제거
GEMINI_MODELS = [
    'gemini-2.5-flash-lite',          # 30 RPM / 1,000 RPD
    'gemini-3-flash-preview',         # 1,500 RPD
    'gemini-3.1-flash-lite',  # 1,000 RPD
    'gemini-2.5-flash',               # 250 RPD
    'gemini-2.5-pro',                 # 25 RPD (최후)
]


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def call_gemini(prompt, key, model='gemini-2.5-flash'):
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}'
    r = requests.post(url, json={
        'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {'maxOutputTokens': 8192, 'temperature': 0.3}
    }, timeout=60)
    if r.status_code == 200:
        try:
            return r.json()['candidates'][0]['content']['parts'][0]['text']
        except Exception:
            return None
    return None


def call_gemini_with_fallback(prompt):
    """여러 키/모델 시도"""
    keys = [os.environ.get(k) for k in ['GEMINI_API_KEY', 'GEMINI_API_KEY_2', 'GEMINI_API_KEY_3']]
    keys = [k for k in keys if k]
    for model in GEMINI_MODELS:
        for key in keys:
            r = call_gemini(prompt, key, model)
            if r:
                return r, model
            time.sleep(0.3)
    return None, None


def build_finalize_prompt(article, qst, answers_text):
    """최종 기사 재작성 프롬프트"""
    return f'''당신은 한국 경제 기자입니다. 다음 정보를 종합해 최종 기사를 한국어로 작성하세요.

[1차 기사 헤드라인]
{article.get('headline','')}

[1차 기사 본문]
{article.get('content','')}

[관련 질문지]
{qst.get('cover_letter','')}

[회사 IR 답변]
{answers_text}

═══════════════════════════════════════════════════════════
[작성 지시]
1. 1차 기사를 답변으로 보강하여 최종 기사로 재작성하세요.
2. 회사 측 입장은 명확히 인용 ("회사 측은 ~라고 밝혔다", "○○ 부장은 ~라고 답했다").
3. 답변에 없는 사실 추측 금지.
4. 1,500~2,000자.
5. 한국어로만 작성. 영어 금지.
6. 답변에 핵심 정보 있으면 헤드라인도 업데이트 가능.

[출력 형식]
HEADLINE: [한 줄 헤드라인]

[본문 5~7단락]

(끝)
'''


def parse_response(text):
    """Gemini 응답에서 헤드라인·본문 분리"""
    if not text:
        return None, None
    # HEADLINE: 패턴
    m = re.search(r'HEADLINE\s*[:：]\s*(.+?)(?:\n|$)', text)
    if m:
        headline = m.group(1).strip()
        body = text[m.end():].strip()
        # 빈 줄 제거
        body = re.sub(r'^\s*\n+', '', body)
        return headline, body
    # 첫 줄을 헤드라인으로
    lines = text.strip().split('\n', 1)
    return lines[0].strip(), lines[1].strip() if len(lines) > 1 else ''


def finalize_one(db, email_row):
    """답장 1건 → 최종 기사 생성"""
    qst = db.execute(
        "SELECT q.*, a.headline, a.content, a.subheadline FROM ir_questionnaires q "
        "JOIN article_drafts a ON q.article_id = a.id WHERE q.id=?",
        [email_row['questionnaire_id']]
    ).fetchone()
    if not qst:
        return {"ok": False, "error": "질문지/기사 없음"}
    qst = dict(qst)
    article = {
        'headline': qst.get('headline'),
        'content': qst.get('content'),
        'subheadline': qst.get('subheadline'),
    }

    # 답변 본문 — 단순 첨부 (질문-답변 매칭은 향후 개선)
    answers_text = email_row.get('body_text') or ''
    if not answers_text or len(answers_text) < 50:
        return {"ok": False, "error": "답변 본문 부족"}

    # Gemini 호출
    prompt = build_finalize_prompt(article, qst, answers_text)
    resp, model = call_gemini_with_fallback(prompt)
    if not resp:
        return {"ok": False, "error": "Gemini 호출 실패"}

    headline, body = parse_response(resp)
    if not body:
        return {"ok": False, "error": "응답 파싱 실패"}

    # 새 article_draft 생성 (status='ready', parent 참조)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur = db.execute("""
        INSERT INTO article_drafts
            (lead_id, corp_code, corp_name, headline, content, status,
             generation_model, parent_article_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?)
    """, [None, qst.get('corp_code'), qst.get('corp_name'),
          headline, body, model, qst.get('article_id'), now, now])
    new_id = cur.lastrowid
    db.commit()
    return {"ok": True, "article_id": new_id, "model": model, "headline": headline}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--email-id', type=int)
    ap.add_argument('--stats', action='store_true')
    args = ap.parse_args()

    db = get_db()
    # parent_article_id 컬럼 추가 (없으면)
    try:
        db.execute("ALTER TABLE article_drafts ADD COLUMN parent_article_id INTEGER")
        db.commit()
    except: pass

    if args.stats:
        n_in = db.execute("SELECT COUNT(*) FROM ir_emails WHERE direction='in'").fetchone()[0]
        n_processed = db.execute("SELECT COUNT(*) FROM article_drafts WHERE parent_article_id IS NOT NULL").fetchone()[0]
        print(f"답장 받음: {n_in}건")
        print(f"최종 기사로 변환: {n_processed}건")
        print(f"미처리: {n_in - n_processed}건")
        return

    # 처리 대상 — 답장 중 아직 최종 기사 없는 것
    if args.email_id:
        rows = db.execute("SELECT * FROM ir_emails WHERE id=? AND direction='in'", [args.email_id]).fetchall()
    else:
        rows = db.execute("""
            SELECT e.* FROM ir_emails e
            WHERE e.direction='in' AND e.questionnaire_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM article_drafts a
                JOIN ir_questionnaires q ON a.parent_article_id = q.article_id
                WHERE q.id = e.questionnaire_id
              )
        """).fetchall()

    print(f"처리 대상: {len(rows)}건")
    for r in rows:
        result = finalize_one(db, dict(r))
        print(f"  email #{r['id']}: {json.dumps(result, ensure_ascii=False)}")


if __name__ == '__main__':
    main()
