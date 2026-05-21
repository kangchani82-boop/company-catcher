import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
conn = sqlite3.connect('data/dart/dart_reports.db')
print(f"article_drafts : {conn.execute('SELECT COUNT(*) FROM article_drafts').fetchone()[0]}")
print(f"questionnaires : {conn.execute('SELECT COUNT(*) FROM ir_questionnaires').fetchone()[0]}")
print(f"ir_contacts    : {conn.execute('SELECT COUNT(*) FROM ir_contacts').fetchone()[0]}")
for src in ['HOMEPAGE','PRESS_RELEASE','DOMAIN_GUESS','CSV_IMPORT','MANUAL']:
    n = conn.execute(f"SELECT COUNT(*) FROM ir_contacts WHERE source='{src}'").fetchone()[0]
    print(f"  {src:14s}: {n}")
