"""Create ir_contacts_2 (same schema as ir_contacts) + companies.ir_url"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "dart" / "dart_reports.db"
db = sqlite3.connect(str(DB_PATH))

db.executescript("""
CREATE TABLE IF NOT EXISTS ir_contacts_2 (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    corp_code     TEXT NOT NULL,
    corp_name     TEXT,
    ir_email      TEXT,
    ir_phone      TEXT,
    ir_name       TEXT,
    ir_dept       TEXT,
    homepage      TEXT,
    source        TEXT,
    source_url    TEXT,
    confidence    TEXT DEFAULT 'guessed',
    mx_verified   INTEGER DEFAULT 0,
    user_verified INTEGER DEFAULT 0,
    bounced_count INTEGER DEFAULT 0,
    is_active     INTEGER DEFAULT 1,
    notes         TEXT,
    created_at    TEXT,
    last_used_at  TEXT,
    ir_email_secondary TEXT,
    ir_mobile     TEXT,
    ir_title      TEXT,
    updated_at    TEXT,
    reply_count   INTEGER DEFAULT 0,
    learned_at    TEXT,
    avg_reply_days REAL,
    UNIQUE(corp_code, ir_email)
);
CREATE INDEX IF NOT EXISTS idx_irc2_corp ON ir_contacts_2(corp_code);
CREATE INDEX IF NOT EXISTS idx_irc2_email ON ir_contacts_2(ir_email);
CREATE INDEX IF NOT EXISTS idx_irc2_active ON ir_contacts_2(is_active, user_verified);
""")

cols = [r[1] for r in db.execute("PRAGMA table_info(companies)").fetchall()]
if "ir_url" not in cols:
    db.execute("ALTER TABLE companies ADD COLUMN ir_url TEXT")
    print("+ companies.ir_url 컬럼 추가")
else:
    print("• companies.ir_url 이미 존재")

db.commit()
n_cols = len(db.execute("PRAGMA table_info(ir_contacts_2)").fetchall())
print(f"OK ir_contacts_2 ({n_cols} columns)")
