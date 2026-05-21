"""
scripts/generate_questionnaire.py
──────────────────────────────────
article_drafts 기사들에 IR 질문지 자동 생성.

Gemini를 사용하지 않고 변화 라벨(NEW/REMOVED/CHANGED 등) + 키워드 기반 템플릿.
이미 reporter_briefs (v3 REPORTER) 가 있는 기사는 그것 활용.

생성된 질문지 → ir_questionnaires 테이블 저장.

실행:
  python scripts/generate_questionnaire.py              # 전체 article_drafts
  python scripts/generate_questionnaire.py --limit 50   # 50건만
  python scripts/generate_questionnaire.py --reset      # 기존 삭제 후 재생성
  python scripts/generate_questionnaire.py --stats      # 통계만
"""
import io, os, re, sys, json, sqlite3, argparse
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


# 변화 유형별 질문 템플릿
TEMPLATES = {
    'NEW': [
        "신규 진출(신사업)을 결정하신 핵심 동기는?",
        "초기 투자 규모는 어느 정도이며, 자금 조달 방식은?",
        "매출 가시화 목표 시점은?",
        "기존 사업과 시너지·자기잠식 우려는?",
        "핵심 인력은 신규 채용/내부 이전?",
    ],
    'REMOVED': [
        "사업 철수의 핵심 사유는?",
        "철수에 따른 일회성 손실 규모는?",
        "잔여 자산·인력 재배치 계획은?",
        "철수 발표 시 주가/투자자 반응 대응 방안?",
        "유사 사업으로 재진출 가능성은?",
    ],
    'EXPANDED': [
        "확장의 배경(시장 신호 vs 자체 전략)은?",
        "추가 투자 규모와 시기는?",
        "목표 점유율 및 회수 기간은?",
        "확장에 따른 리스크 요인은?",
        "경쟁사 대비 차별화 포인트는?",
    ],
    'SHRUNK': [
        "축소 결정의 직접적 원인은?",
        "정상화 시점·전략은?",
        "매출/이익 영향 정량은?",
        "관련 인력·자산 영향은?",
        "회복 후 사업 운영 방향은?",
    ],
    'CHANGED': [
        "변경의 직접 사유는?",
        "변경의 영향 범위는?",
        "시행 시점과 단계별 일정은?",
        "이해관계자 대응 방안은?",
        "변경 후 성과 측정 지표는?",
    ],
    'numeric_change': [
        "수치 변동의 핵심 원인은?",
        "정상화 시점은?",
        "추가 자본 조달 계획이 있는지?",
        "차후 분기 가이던스는?",
        "재발 방지 대책은?",
    ],
    'risk_alert': [
        "이번 리스크에 대한 회사의 공식 입장은?",
        "재발 가능성과 사전 대응 체계는?",
        "이번 리스크가 실적에 미치는 영향 추정은?",
        "법적·규제적 절차는 어느 단계인지?",
        "투자자 우려 해소 방안은?",
    ],
    'market_shift': [
        "시장 변화 인식 시점·근거는?",
        "변화에 대한 회사의 대응 전략은?",
        "경쟁 구도 재편 영향은?",
        "단기/중기 가이던스 변경 가능성은?",
        "관련 의사결정 일정은?",
    ],
    'strategy_change': [
        "전략 변경의 핵심 동인은?",
        "기존 전략 대비 핵심 차이는?",
        "실행을 위한 조직·인력 변화는?",
        "재무적 영향(투자/회수)은?",
        "시장이 인지할 첫 가시화 시점은?",
    ],
    'supply_chain': [
        "공급망 변화의 직접 원인은?",
        "대체 공급처/판매처 확보 현황은?",
        "원가/마진 영향 추정은?",
        "재고 운영 정책 변경 사항은?",
        "장기 파트너십 재구성 방향은?",
    ],
    'default': [
        "이 변화의 핵심 배경은?",
        "재무적 영향 추정은?",
        "후속 일정·계획은?",
        "투자자에게 강조하고 싶은 메시지는?",
        "다음 분기까지 변화의 가시화 가능성은?",
    ],
}


def detect_change_type(content, headline):
    """기사 본문/헤드라인에서 변화 유형 추론"""
    text = (headline + ' ' + content).lower()
    # 라벨 키워드
    if 'new' in text or '신규' in content or '신사업' in content or '진출' in content:
        return 'NEW'
    if 'removed' in text or '철수' in content or '청산' in content or '매각' in content:
        return 'REMOVED'
    if 'expanded' in text or '확대' in content or '확장' in content or '증설' in content:
        return 'EXPANDED'
    if 'shrunk' in text or '축소' in content:
        return 'SHRUNK'
    if 'changed' in text or '변경' in content or '교체' in content:
        return 'CHANGED'
    return None


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def gen_questions(article, lead, comparison):
    """기사 + 단서 + 비교분석에서 질문 5~7개 생성"""
    headline = article['headline'] or ''
    content = article['content'] or ''
    lead_type = lead.get('lead_type','') if lead else ''

    # 1. 변화 라벨 기반 (가장 정확)
    change_type = detect_change_type(content, headline)
    base_qs = TEMPLATES.get(change_type) if change_type else None

    # 2. 단서 유형 기반
    if not base_qs and lead_type:
        base_qs = TEMPLATES.get(lead_type) or TEMPLATES['default']

    # 3. fallback
    if not base_qs:
        base_qs = TEMPLATES['default']

    # 4. 헤드라인을 첫 질문에 활용 (구체적)
    custom_q = None
    if headline:
        # 헤드라인에 수치/이름 명시되어 있으면 그것을 추궁
        m_pct = re.search(r'(\d+(?:\.\d+)?)\s*[%％]', headline)
        m_won = re.search(r'(\d+(?:\.\d+)?)\s*억(?:원)?', headline)
        if m_pct:
            custom_q = f"{headline.split(',')[0]}와 관련, {m_pct.group(0)} 수치의 측정 시점·기준은?"
        elif m_won:
            custom_q = f"{headline.split(',')[0]}와 관련, {m_won.group(0)} 규모의 산정 근거는?"

    questions = []
    if custom_q:
        questions.append({"q": custom_q, "type": "specific"})
    for q in base_qs[:5 if custom_q else 5]:
        questions.append({"q": q, "type": "general"})

    return questions[:6]  # 최대 6개


def gen_cover_letter(corp_name, headline, deadline_days=3):
    """커버 레터 생성"""
    today = datetime.now().strftime('%Y년 %m월 %d일')
    return (
        f"안녕하세요. Finance Scope 강종민 기자입니다.\n\n"
        f"{corp_name}의 정기보고서 변화를 분석하던 중 다음 사안 관련 취재를 진행 중입니다.\n\n"
        f"[관련 사안] {headline}\n\n"
        f"아래 질문을 검토해주시면 감사하겠습니다. "
        f"답변은 {deadline_days}영업일 내 회신 부탁드립니다. 답변이 어려운 항목은 \"비공개\"로만 표기해주셔도 됩니다.\n\n"
        f"[질문]\n"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--reset', action='store_true')
    ap.add_argument('--stats', action='store_true')
    args = ap.parse_args()

    db = get_db()

    if args.stats:
        n = db.execute("SELECT COUNT(*) FROM ir_questionnaires").fetchone()[0]
        n_pending = db.execute("SELECT COUNT(*) FROM ir_questionnaires WHERE status='pending'").fetchone()[0]
        n_sent = db.execute("SELECT COUNT(*) FROM ir_questionnaires WHERE status='sent'").fetchone()[0]
        n_arts = db.execute("SELECT COUNT(*) FROM article_drafts").fetchone()[0]
        print(f"이미 생성된 질문지: {n}건 (pending={n_pending} / sent={n_sent})")
        print(f"전체 기사: {n_arts}건")
        print(f"미생성: {n_arts - n}건")
        return

    if args.reset:
        db.execute("DELETE FROM ir_questionnaires")
        db.commit()
        print('기존 질문지 모두 삭제\n')

    # 미생성 article_drafts 가져오기
    rows = db.execute("""
        SELECT a.id AS aid, a.lead_id, a.corp_code, a.corp_name,
               a.headline, a.subheadline, a.content
        FROM article_drafts a
        LEFT JOIN ir_questionnaires q ON a.id = q.article_id
        WHERE q.id IS NULL
        ORDER BY a.id DESC
    """).fetchall()
    if args.limit > 0:
        rows = rows[:args.limit]

    print(f'질문지 생성 대상: {len(rows)}건\n')

    stats = {'created': 0, 'errors': 0}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for r in rows:
        try:
            # lead 정보
            lead = None
            if r['lead_id']:
                lr = db.execute("SELECT * FROM story_leads WHERE id=?", [r['lead_id']]).fetchone()
                lead = dict(lr) if lr else None

            article = {
                'headline': r['headline'],
                'content': r['content'],
            }
            questions = gen_questions(article, lead, None)
            cover = gen_cover_letter(r['corp_name'] or '귀사', r['headline'] or '')

            db.execute("""
                INSERT INTO ir_questionnaires
                    (article_id, lead_id, corp_code, corp_name,
                     questions, cover_letter, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
            """, [r['aid'], r['lead_id'], r['corp_code'], r['corp_name'],
                  json.dumps(questions, ensure_ascii=False),
                  cover, now])
            stats['created'] += 1
        except Exception as e:
            stats['errors'] += 1
            if stats['errors'] <= 3:
                print(f"  ERR article #{r['aid']}: {str(e)[:80]}")

    db.commit()
    print(f"\n✅ 생성: {stats['created']}건 / 에러: {stats['errors']}건")
    n_total = db.execute("SELECT COUNT(*) FROM ir_questionnaires").fetchone()[0]
    print(f"누적 질문지: {n_total}건")


if __name__ == '__main__':
    main()
