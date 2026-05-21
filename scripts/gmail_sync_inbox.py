"""
scripts/gmail_sync_inbox.py
────────────────────────────
Gmail 받은편지함을 폴링하여 답장(우리가 보낸 메일에 대한)을 자동 매칭.

실행:
  python scripts/gmail_sync_inbox.py                    # 1회 실행
  python scripts/gmail_sync_inbox.py --watch --interval 300  # 5분 폴링 (백그라운드 워커)
"""
import io, os, re, sys, json, base64, sqlite3, time, argparse
from pathlib import Path
from datetime import datetime
from email.utils import parseaddr
from email.header import decode_header

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
SECRETS = ROOT / "secrets"
TOKEN_FILE = SECRETS / "token.json"

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]


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


def decode_mime_header(value):
    if not value: return ""
    try:
        parts = decode_header(value)
        out = []
        for txt, enc in parts:
            if isinstance(txt, bytes):
                out.append(txt.decode(enc or "utf-8", errors="replace"))
            else:
                out.append(txt)
        return "".join(out)
    except: return value


def decode_body(payload):
    body = ""
    def walk(part):
        nonlocal body
        mime = part.get("mimeType", "")
        if mime == "text/plain" and "data" in part.get("body", {}):
            try:
                body += base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace") + "\n"
            except: pass
        elif mime == "text/html" and "data" in part.get("body", {}) and not body:
            try:
                txt = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
                txt = re.sub(r"<[^>]+>", " ", txt)
                body += txt + "\n"
            except: pass
        for child in part.get("parts", []):
            walk(child)
    walk(payload)
    return body.strip()


def sync_once():
    """1회 폴링 — 새 메시지 가져와서 답장 매칭"""
    db = get_db()
    db.execute('''
        CREATE TABLE IF NOT EXISTS ir_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            questionnaire_id INTEGER, direction TEXT,
            gmail_msg_id TEXT UNIQUE, gmail_thread_id TEXT,
            from_addr TEXT, to_addr TEXT, subject TEXT, body_text TEXT,
            sent_at TEXT, received_at TEXT, is_read INTEGER DEFAULT 0
        )
    ''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_email_thread ON ir_emails(gmail_thread_id)')

    # 우리가 발송한 thread_id 목록 (답장 매칭용)
    our_threads = set(r[0] for r in db.execute(
        "SELECT DISTINCT gmail_thread_id FROM ir_emails WHERE direction='out' AND gmail_thread_id IS NOT NULL"
    ).fetchall())
    if not our_threads:
        return {"new": 0, "matched": 0, "msg": "발송 이력 없음"}

    # 이미 처리한 received msg_id
    processed = set(r[0] for r in db.execute(
        "SELECT gmail_msg_id FROM ir_emails WHERE direction='in'"
    ).fetchall())

    service = get_gmail_service()
    # INBOX 최근 1일 (효율)
    resp = service.users().messages().list(
        userId='me', q='in:inbox newer_than:7d', maxResults=200
    ).execute()
    msg_ids = [m['id'] for m in resp.get('messages', [])]

    stats = {'fetched': 0, 'matched': 0, 'skipped': 0, 'new': 0}
    for mid in msg_ids:
        if mid in processed:
            stats['skipped'] += 1
            continue
        try:
            msg = service.users().messages().get(userId='me', id=mid, format='full').execute()
            stats['fetched'] += 1
            tid = msg.get('threadId')
            if tid not in our_threads:
                continue  # 우리 발송에 대한 답장 아님
            payload = msg.get('payload', {})
            headers = {h['name']: h['value'] for h in payload.get('headers', [])}
            from_raw = decode_mime_header(headers.get('From',''))
            subject = decode_mime_header(headers.get('Subject',''))
            sender_name, sender_email = parseaddr(from_raw)
            body = decode_body(payload)

            # 매칭된 questionnaire 찾기
            qst_row = db.execute("""
                SELECT questionnaire_id FROM ir_emails
                WHERE gmail_thread_id=? AND direction='out'
                LIMIT 1
            """, [tid]).fetchone()
            qst_id = qst_row[0] if qst_row else None

            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            db.execute('''
                INSERT OR IGNORE INTO ir_emails
                    (questionnaire_id, direction, gmail_msg_id, gmail_thread_id,
                     from_addr, to_addr, subject, body_text, received_at)
                VALUES (?, 'in', ?, ?, ?, ?, ?, ?, ?)
            ''', [qst_id, mid, tid, sender_email, headers.get('To',''), subject, body, now])
            if qst_id:
                db.execute("""
                    UPDATE ir_questionnaires SET status='replied', replied_at=? WHERE id=?
                """, [now, qst_id])
                stats['matched'] += 1
            stats['new'] += 1
        except Exception as e:
            print(f'  ERR msg {mid}: {str(e)[:80]}')

    db.commit()

    # 새 답장 매칭됐으면 자동으로 학습 트리거
    if stats['matched'] > 0:
        try:
            sys.path.insert(0, str(Path(__file__).parent))
            from learn_from_replies import learn_one_reply, update_sent_stats
            update_sent_stats(db)
            new_replies = db.execute("""
                SELECT * FROM ir_emails
                WHERE direction='in' AND questionnaire_id IS NOT NULL
                ORDER BY received_at DESC LIMIT ?
            """, [stats['matched']]).fetchall()
            learned = 0
            for r in new_replies:
                if learn_one_reply(db, dict(r)).get('ok'):
                    learned += 1
            stats['learned'] = learned
        except Exception as e:
            stats['learn_err'] = str(e)[:50]

    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--watch', action='store_true')
    ap.add_argument('--interval', type=int, default=300)
    args = ap.parse_args()

    if args.watch:
        print(f'폴링 모드 — {args.interval}초 간격')
        while True:
            try:
                s = sync_once()
                print(f'[{datetime.now().strftime("%H:%M:%S")}] {s}')
            except Exception as e:
                print(f'ERR: {e}')
            time.sleep(args.interval)
    else:
        s = sync_once()
        print(json.dumps(s, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
