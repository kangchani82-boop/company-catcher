"""
scripts/learn_from_replies.py
──────────────────────────────
답장(ir_emails direction='in')에서 진짜 IR 담당자 정보 학습.

학습 대상:
  1. 답장 발신자 이메일 → ir_contacts 등록/갱신
  2. 발신자 서명 본문에서 이름·직책·부서·전화 추출
  3. 회사별 응답성 통계 (응답률·평균 응답일·품질점수)

trigger: gmail_sync_inbox.py 에서 답장 매칭 후 자동 호출 가능

실행:
  python scripts/learn_from_replies.py            # 미학습 답장 모두
  python scripts/learn_from_replies.py --stats    # 통계만
"""
import io, os, re, sys, json, sqlite3, argparse
from pathlib import Path
from datetime import datetime
from email.utils import parseaddr

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

KOREAN_SURNAMES = set('김이박최정강조윤장임한오서신권황안송류전홍고문양손배백허유남심노하곽성차주우구민나진지엄채원천방공현함변염여추도소석선설마길연위표명기반왕금옥육인맹제모탁국어은편복단연봉경사부')

TITLES = ['부사장','전무','상무','이사','본부장','실장','팀장','부장','차장',
          '과장','대리','사원','주임','매니저','선임','책임','수석',
          'Director','Manager','VP','CFO','CEO','CTO','CMO']
TITLE_RE = re.compile(r'([가-힣]{2,4})\s*(' + '|'.join(re.escape(t) for t in TITLES) + r')')
DEPT_RE = re.compile(r'(IR|PR|홍보|투자홍보|커뮤니케이션|투자자|재무|기획|경영지원|대외협력)\s*(팀|실|부|본부|그룹)?')
EMAIL_RE = re.compile(r"[\w\._\-+]+@[\w\._\-]+\.[a-zA-Z]{2,}")
MOBILE_RE = re.compile(r'(?:\+?82[-\s]?|0)?(010|011|016|017|018|019)[-\s\.\)]?(\d{3,4})[-\s\.](\d{4})')
PHONE_RE = re.compile(r'(?:\+?82[-\s]?|0)?(\d{2,3})[-\s\.\)](\d{3,4})[-\s\.](\d{4})')


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def is_valid_korean_name(name):
    if not name or len(name) < 2 or len(name) > 4:
        return False
    if not re.match(r'^[가-힣]+$', name):
        return False
    return name[0] in KOREAN_SURNAMES


def extract_signature_block(body):
    """본문 끝 1500자 = 서명 영역"""
    if not body: return ''
    return body[-1500:]


def extract_contact_from_signature(body):
    """본문에서 연락처 정보 추출"""
    sig = extract_signature_block(body)
    out = {}
    # 이름·직책
    for nm, ti in TITLE_RE.findall(sig):
        if is_valid_korean_name(nm):
            out['name'], out['title'] = nm, ti
            break
    # 부서
    m = DEPT_RE.search(sig)
    if m: out['dept'] = m.group(1) + (m.group(2) or '')
    # 휴대폰
    mob = MOBILE_RE.search(sig)
    if mob: out['mobile'] = f'0{mob.group(1)}-{mob.group(2)}-{mob.group(3)}'
    # 일반 전화 (휴대폰 다음 candidate)
    phones = PHONE_RE.findall(sig)
    for p in phones:
        prefix = p[0] if p[0].startswith('0') else '0' + p[0]
        cand = f'{prefix}-{p[1]}-{p[2]}'
        if cand != out.get('mobile'):
            out['phone'] = cand; break
    return out


def learn_one_reply(db, email_row):
    """답장 1건에서 학습"""
    qid = email_row['questionnaire_id']
    if not qid:
        return {'ok': False, 'reason': 'no questionnaire'}

    qst = db.execute("SELECT * FROM ir_questionnaires WHERE id=?", [qid]).fetchone()
    if not qst:
        return {'ok': False, 'reason': 'no qst'}
    qst = dict(qst)
    corp_code = qst.get('corp_code')
    corp_name = qst.get('corp_name')

    sender_email = (email_row['from_addr'] or '').strip()
    if '@' not in sender_email:
        # parseaddr 다시 시도
        _, sender_email = parseaddr(email_row['from_addr'] or '')
    if not sender_email or '@' not in sender_email:
        return {'ok': False, 'reason': 'invalid sender'}

    body = email_row['body_text'] or ''
    info = extract_contact_from_signature(body)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 1) ir_contacts 갱신/등록
    existing = db.execute(
        "SELECT * FROM ir_contacts WHERE corp_code=? AND ir_email=?",
        [corp_code, sender_email]
    ).fetchone()

    if existing:
        # 기존 contact 업데이트
        db.execute("""
            UPDATE ir_contacts SET
                ir_name = COALESCE(NULLIF(ir_name,''), ?),
                ir_title = COALESCE(NULLIF(ir_title,''), ?),
                ir_dept = COALESCE(NULLIF(ir_dept,''), ?),
                ir_mobile = COALESCE(NULLIF(ir_mobile,''), ?),
                ir_phone = COALESCE(NULLIF(ir_phone,''), ?),
                user_verified = 2,
                confidence = 'A_complete',
                reply_count = COALESCE(reply_count,0) + 1,
                learned_at = ?,
                updated_at = ?
            WHERE id=?
        """, [info.get('name',''), info.get('title',''), info.get('dept',''),
              info.get('mobile',''), info.get('phone',''),
              now, now, existing['id']])
        action = 'updated'
        contact_id = existing['id']
    else:
        # 신규 등록
        try:
            cur = db.execute("""
                INSERT INTO ir_contacts
                    (corp_code, corp_name, ir_email, ir_phone, ir_mobile,
                     ir_name, ir_dept, ir_title,
                     source, confidence, user_verified,
                     reply_count, learned_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'REPLY_LEARNED', 'A_complete', 2,
                        1, ?, ?, ?)
            """, [corp_code, corp_name, sender_email, info.get('phone',''),
                  info.get('mobile',''), info.get('name',''),
                  info.get('dept',''), info.get('title',''),
                  now, now, now])
            action = 'inserted'
            contact_id = cur.lastrowid
        except sqlite3.IntegrityError:
            action = 'duplicate_skip'
            contact_id = None

    # 2) 응답성 통계 갱신
    sent_at = None
    sent_row = db.execute("""
        SELECT sent_at FROM ir_emails
        WHERE questionnaire_id=? AND direction='out'
        ORDER BY sent_at LIMIT 1
    """, [qid]).fetchone()
    if sent_row:
        sent_at = sent_row['sent_at']

    reply_days = None
    if sent_at and email_row.get('received_at'):
        try:
            d_sent = datetime.strptime(sent_at[:19], '%Y-%m-%d %H:%M:%S')
            d_recv = datetime.strptime(email_row['received_at'][:19], '%Y-%m-%d %H:%M:%S')
            reply_days = (d_recv - d_sent).total_seconds() / 86400.0
        except: pass

    # 답변 품질 (단순 휴리스틱: 본문 길이 + "비공개" 등 회피 키워드 카운트)
    quality = min(100, len(body) // 10)
    if any(k in body for k in ['비공개', '공개 자료 참조', '답변 어렵', '확인 어렵']):
        quality = max(0, quality - 30)

    # corp_response_stats UPSERT
    db.execute("""
        INSERT INTO corp_response_stats
            (corp_code, corp_name, sent_count, replied_count,
             avg_reply_days, last_sent_at, last_reply_at, quality_score, updated_at)
        VALUES (?, ?, 1, 1, ?, ?, ?, ?, ?)
        ON CONFLICT(corp_code) DO UPDATE SET
            replied_count = corp_response_stats.replied_count + 1,
            avg_reply_days = CASE
                WHEN corp_response_stats.avg_reply_days IS NULL THEN ?
                ELSE (corp_response_stats.avg_reply_days * corp_response_stats.replied_count + ?)
                     / (corp_response_stats.replied_count + 1)
            END,
            last_reply_at = ?,
            quality_score = CASE
                WHEN corp_response_stats.quality_score IS NULL THEN ?
                ELSE (corp_response_stats.quality_score + ?) / 2
            END,
            updated_at = ?
    """, [corp_code, corp_name, reply_days, sent_at,
          email_row.get('received_at'), quality, now,
          reply_days, reply_days,
          email_row.get('received_at'),
          quality, quality, now])

    db.commit()
    return {'ok': True, 'action': action, 'contact_id': contact_id,
            'reply_days': reply_days, 'quality': quality, 'extracted': info}


def update_sent_stats(db):
    """발송 통계 갱신 (replied_count 따로 + sent_count 별도)"""
    # 회사별 발송 횟수 카운트
    rows = db.execute("""
        SELECT q.corp_code, q.corp_name, COUNT(*) c, MAX(e.sent_at) last_sent
        FROM ir_emails e JOIN ir_questionnaires q ON e.questionnaire_id = q.id
        WHERE e.direction='out'
        GROUP BY q.corp_code
    """).fetchall()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for r in rows:
        db.execute("""
            INSERT INTO corp_response_stats (corp_code, corp_name, sent_count, last_sent_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(corp_code) DO UPDATE SET
                sent_count = excluded.sent_count,
                last_sent_at = excluded.last_sent_at,
                updated_at = excluded.updated_at
        """, [r['corp_code'], r['corp_name'], r['c'], r['last_sent'], now])
    db.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--stats', action='store_true')
    args = ap.parse_args()

    db = get_db()

    if args.stats:
        n_st = db.execute("SELECT COUNT(*) FROM corp_response_stats").fetchone()[0]
        n_lc = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE source='REPLY_LEARNED'").fetchone()[0]
        n_uv = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE user_verified >= 1").fetchone()[0]
        print(f"학습된 contact (REPLY_LEARNED) : {n_lc}")
        print(f"사용자 검증 contact            : {n_uv}")
        print(f"응답성 통계 보유 회사           : {n_st}")
        return

    # 미학습 답장 (in 메일 중 reply_count 0인 것의 발신자)
    rows = db.execute("""
        SELECT * FROM ir_emails
        WHERE direction='in' AND questionnaire_id IS NOT NULL
    """).fetchall()
    print(f'학습 대상: {len(rows)}건')

    update_sent_stats(db)
    stats = {'updated':0,'inserted':0,'duplicate_skip':0,'skipped':0}
    for r in rows:
        result = learn_one_reply(db, dict(r))
        if result.get('ok'):
            stats[result.get('action','skipped')] = stats.get(result.get('action','skipped'),0) + 1
        else:
            stats['skipped'] += 1

    print(f"\n학습 결과: {stats}")


if __name__ == '__main__':
    main()
