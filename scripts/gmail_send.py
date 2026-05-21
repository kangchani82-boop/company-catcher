"""
scripts/gmail_send.py
─────────────────────
Gmail API로 IR 담당자에게 질문지 메일 발송.

⚠️ 자동 발송 모드 비활성화 — 모든 발송은 사용자 수동 검증 후 사이트에서 1건씩 진행.
스케줄러는 발송 단계 제외. 큐만 만들고 사용자가 검토.

실행:
  # 단독 테스트 (article_id 기반) — 사용자 명시적 호출
  python scripts/gmail_send.py --article-id 1 --to test@example.com --dry-run
  python scripts/gmail_send.py --article-id 1 --to ir@samsung.com   # 실제 발송 (확인 후)
  python scripts/gmail_send.py --article-id 1                       # 자동 contact 선택

사이트 사용 (권장):
  /article_detail.html?id=N → "📤 IR에 발송" 버튼 → 미리보기 + 확인 + 발송
"""
import io, os, re, sys, json, base64, sqlite3, argparse
from pathlib import Path
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
SECRETS = ROOT / "secrets"
TOKEN_FILE = SECRETS / "token.json"

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]

SENDER = "kjm@finance-scope.com"
SENDER_NAME = "Finance Scope 강종민 기자"

# 발송 안전 한도 — Gmail 무료 계정 500/일, Workspace 2,000/일
# 5/15 마감 폭주 + bounce 마진 고려해 보수적으로 400으로 설정
DAILY_SEND_LIMIT = int(os.environ.get("GMAIL_DAILY_LIMIT", "400"))

# 같은 corp_code 또는 to_addr 에 N시간 내 재발송 차단
DUPLICATE_WINDOW_HOURS = 24


def count_sent_today(db) -> int:
    """오늘(00:00~) 발송 건수 — ir_emails 기준."""
    return db.execute("""
        SELECT COUNT(*) FROM ir_emails
        WHERE direction='out'
          AND date(sent_at) = date('now', 'localtime')
    """).fetchone()[0]


def find_recent_send(db, corp_code: str, to_addr: str, hours: int = DUPLICATE_WINDOW_HOURS):
    """N시간 내 같은 회사 OR 같은 수신자에게 발송 이력이 있으면 행 반환."""
    return db.execute("""
        SELECT e.id, e.to_addr, e.subject, e.sent_at, q.corp_code, q.corp_name
        FROM ir_emails e
        LEFT JOIN ir_questionnaires q ON q.id = e.questionnaire_id
        WHERE e.direction='out'
          AND e.sent_at >= datetime('now', ?, 'localtime')
          AND (e.to_addr = ? OR q.corp_code = ?)
        ORDER BY e.sent_at DESC LIMIT 1
    """, [f"-{hours} hours", to_addr or "", corp_code or ""]).fetchone()


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    return db


def get_gmail_service():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


def build_email(to_addr, to_name, corp_name, headline, cover_letter, questions):
    """질문지 → 이메일 메시지 작성"""
    subject = f"[Finance Scope] {corp_name} 취재 문의 — {headline[:40]}"

    # plain text
    text = cover_letter + '\n'
    for i, q in enumerate(questions, 1):
        qtext = q.get('q') if isinstance(q, dict) else str(q)
        text += f"{i}. {qtext}\n"
    text += '\n감사합니다.\n'
    text += f'{SENDER_NAME}\n{SENDER}\n'

    # HTML 버전
    html = f'''
<html><body style="font-family:'Malgun Gothic',sans-serif;line-height:1.6;color:#333">
<p>{cover_letter.replace(chr(10), '<br>')}</p>
<ol>
'''
    for q in questions:
        qtext = q.get('q') if isinstance(q, dict) else str(q)
        html += f'<li style="margin:8px 0">{qtext}</li>'
    html += f'''
</ol>
<p>감사합니다.<br>
<b>{SENDER_NAME}</b><br>
<a href="mailto:{SENDER}">{SENDER}</a></p>
</body></html>'''

    msg = MIMEMultipart('alternative')
    msg['From'] = formataddr((SENDER_NAME, SENDER))
    msg['To'] = formataddr((to_name, to_addr)) if to_name else to_addr
    msg['Subject'] = subject
    msg.attach(MIMEText(text, 'plain', 'utf-8'))
    msg.attach(MIMEText(html, 'html', 'utf-8'))
    return msg, subject


def send_email(article_id, to_addr=None, dry_run=False):
    """질문지 발송"""
    db = get_db()
    qst = db.execute("""
        SELECT q.*, a.headline FROM ir_questionnaires q
        JOIN article_drafts a ON q.article_id = a.id
        WHERE q.article_id=?
    """, [article_id]).fetchone()
    if not qst:
        return {"ok": False, "error": "질문지 없음"}

    qst = dict(qst)
    questions = json.loads(qst['questions']) if qst.get('questions') else []
    if not questions:
        return {"ok": False, "error": "질문 없음"}
    # 검수 게이트: 'approved' 또는 'sent'만 발송 가능
    if qst.get('status') not in ('approved', 'sent', 'replied'):
        return {"ok": False, "error": f"검수 미완료 (status={qst.get('status')}). /questionnaire_review 에서 승인 필요"}

    # 안전망 ①: ir_emails 테이블 사전 보장 (없으면 생성)
    db.execute('''
        CREATE TABLE IF NOT EXISTS ir_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            questionnaire_id INTEGER,
            direction TEXT,
            gmail_msg_id TEXT UNIQUE,
            gmail_thread_id TEXT,
            from_addr TEXT, to_addr TEXT,
            subject TEXT, body_text TEXT,
            sent_at TEXT, received_at TEXT,
            is_read INTEGER DEFAULT 0
        )
    ''')

    # 안전망 ②: 일일 발송 한도 (실제 발송 시에만 적용 — dry_run 제외)
    if not dry_run:
        sent_today = count_sent_today(db)
        if sent_today >= DAILY_SEND_LIMIT:
            return {"ok": False, "error": f"오늘 발송 한도 도달 ({sent_today}/{DAILY_SEND_LIMIT}). 내일 재시도하거나 GMAIL_DAILY_LIMIT 환경변수 조정.",
                    "limit_hit": True, "sent_today": sent_today}

    # 자동 contact 선택
    contact = None
    if not to_addr:
        c = db.execute("""
            SELECT * FROM ir_contacts
            WHERE corp_code=?
              AND ir_email IS NOT NULL AND ir_email != ''
              AND substr(ir_email,1,1) != '_'
              AND is_active=1
              AND bounced_count < 3
            ORDER BY user_verified DESC,
                     CASE confidence
                       WHEN 'A_complete' THEN 1
                       WHEN 'A_verified' THEN 2
                       WHEN 'A_email_only' THEN 3
                       WHEN 'found' THEN 4
                       ELSE 9 END
            LIMIT 1
        """, [qst['corp_code']]).fetchone()
        if not c:
            return {"ok": False, "error": "발송 가능한 IR 담당자 없음",
                    "no_contact": True}
        contact = dict(c)
        to_addr = contact['ir_email']

    # 안전망 ③: 24h 중복 발송 방지 (dry_run 제외)
    if not dry_run:
        dup = find_recent_send(db, qst.get('corp_code'), to_addr,
                               hours=DUPLICATE_WINDOW_HOURS)
        if dup:
            return {"ok": False,
                    "error": (f"24h 내 중복 발송 차단 — {dup['sent_at']} 에 "
                              f"{dup['corp_name'] or dup['to_addr']} 로 이미 발송됨"),
                    "duplicate": True,
                    "prev_sent_at": dup['sent_at'],
                    "prev_to": dup['to_addr']}

    # 이메일 작성
    to_name = contact.get('ir_name') if contact else ''
    msg, subject = build_email(
        to_addr, to_name, qst['corp_name'], qst['headline'],
        qst['cover_letter'], questions
    )

    if dry_run:
        print(f'[DRY-RUN]')
        print(f'  To: {to_addr}')
        print(f'  Subject: {subject}')
        print(f'  Questions: {len(questions)}개')
        return {"ok": True, "dry_run": True, "to": to_addr, "subject": subject}

    # Gmail API 발송
    service = get_gmail_service()
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    try:
        result = service.users().messages().send(
            userId='me', body={'raw': raw}
        ).execute()
        msg_id = result.get('id')
        thread_id = result.get('threadId')

        # ir_emails 테이블에 저장 + ir_questionnaires 상태 업데이트
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # ir_emails 테이블 생성 (없으면)
        db.execute('''
            CREATE TABLE IF NOT EXISTS ir_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                questionnaire_id INTEGER,
                direction TEXT,
                gmail_msg_id TEXT UNIQUE,
                gmail_thread_id TEXT,
                from_addr TEXT, to_addr TEXT,
                subject TEXT, body_text TEXT,
                sent_at TEXT, received_at TEXT,
                is_read INTEGER DEFAULT 0
            )
        ''')
        db.execute('CREATE INDEX IF NOT EXISTS idx_email_thread ON ir_emails(gmail_thread_id)')
        db.execute('CREATE INDEX IF NOT EXISTS idx_email_qst ON ir_emails(questionnaire_id)')
        db.execute('''
            INSERT OR IGNORE INTO ir_emails
                (questionnaire_id, direction, gmail_msg_id, gmail_thread_id,
                 from_addr, to_addr, subject, sent_at)
            VALUES (?, 'out', ?, ?, ?, ?, ?, ?)
        ''', [qst['id'], msg_id, thread_id, SENDER, to_addr, subject, now])
        db.execute("""
            UPDATE ir_questionnaires SET status='sent', sent_at=? WHERE id=?
        """, [now, qst['id']])
        if contact:
            db.execute("UPDATE ir_contacts SET last_used_at=? WHERE id=?", [now, contact['id']])
        db.commit()

        return {"ok": True, "msg_id": msg_id, "thread_id": thread_id,
                "to": to_addr, "subject": subject}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--article-id', type=int, required=True)
    ap.add_argument('--to', type=str, help='수신 이메일 (없으면 자동 선택)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    result = send_email(args.article_id, args.to, args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
