import sqlite3
import io
import sys
from datetime import date
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH = r"C:\Users\kangc\Desktop\claude_projects\company catcher\data\dart\dart_reports.db"
OUTPUT_PATH = r"C:\Users\kangc\Desktop\claude_projects\company catcher\data\supply_chain_keywords.md"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# --- Stats ---
cur.execute("SELECT COUNT(*) as cnt FROM supply_chain")
total_records = cur.fetchone()['cnt']

cur.execute("SELECT COUNT(DISTINCT corp_code) as cnt FROM supply_chain")
total_companies = cur.fetchone()['cnt']

cur.execute("SELECT relation_type, COUNT(*) as cnt FROM supply_chain GROUP BY relation_type")
type_counts = {row['relation_type']: row['cnt'] for row in cur.fetchall()}

# --- Partner counts per relation_type ---
def get_partner_counts(relation_type):
    cur.execute("""
        SELECT partner_name, COUNT(DISTINCT corp_code) as company_count
        FROM supply_chain
        WHERE relation_type = ?
        GROUP BY partner_name
        ORDER BY partner_name
    """, (relation_type,))
    return cur.fetchall()

customer_partners = get_partner_counts('customer')
supplier_partners = get_partner_counts('supplier')
partner_partners = get_partner_counts('partner')
competitor_partners = get_partner_counts('competitor')

# --- Hub companies (3+ mentions) ---
cur.execute("""
    SELECT partner_name, relation_type, COUNT(DISTINCT corp_code) as company_count
    FROM supply_chain
    GROUP BY partner_name, relation_type
    HAVING company_count >= 3
    ORDER BY relation_type, company_count DESC, partner_name
""")
hub_rows = cur.fetchall()
hub_by_type = defaultdict(list)
for row in hub_rows:
    hub_by_type[row['relation_type']].append((row['partner_name'], row['company_count']))

# --- Per-company supply chain summary (limit 500) ---
cur.execute("""
    SELECT DISTINCT corp_code, corp_name FROM supply_chain
    ORDER BY corp_code
    LIMIT 500
""")
companies = cur.fetchall()

company_data = {}
for company in companies:
    corp_code = company['corp_code']
    corp_name = company['corp_name']
    cur.execute("""
        SELECT relation_type, partner_name
        FROM supply_chain
        WHERE corp_code = ?
        ORDER BY relation_type, partner_name
    """, (corp_code,))
    rows = cur.fetchall()
    by_type = defaultdict(list)
    for row in rows:
        by_type[row['relation_type']].append(row['partner_name'])
    company_data[(corp_code, corp_name)] = by_type

conn.close()

# --- Build Markdown ---
today = date.today().strftime('%Y-%m-%d')

relation_label = {
    'customer': '고객사',
    'supplier': '공급사',
    'partner': '파트너',
    'competitor': '경쟁사',
}

lines = []

lines.append("# 공급망 키워드 데이터베이스")
lines.append("")
lines.append(f"**생성일:** {today}")
lines.append("")
lines.append("## 통계 요약")
lines.append("")
lines.append(f"- **총 레코드 수:** {total_records:,}건")
lines.append(f"- **총 기업 수:** {total_companies:,}개사")
lines.append(f"- **고객사 관계:** {type_counts.get('customer', 0):,}건")
lines.append(f"- **공급사 관계:** {type_counts.get('supplier', 0):,}건")
lines.append(f"- **파트너 관계:** {type_counts.get('partner', 0):,}건")
lines.append(f"- **경쟁사 관계:** {type_counts.get('competitor', 0):,}건")
lines.append("")

# Section helper
def write_partner_section(lines, title, partners):
    lines.append(f"## {title}")
    lines.append("")
    lines.append(f"총 **{len(partners):,}**개 고유 파트너명")
    lines.append("")
    for row in partners:
        lines.append(f"- **{row['partner_name']}** ({row['company_count']}개 기업)")
    lines.append("")

write_partner_section(lines, "고객사 (Customer)", customer_partners)
write_partner_section(lines, "공급사 (Supplier)", supplier_partners)
write_partner_section(lines, "파트너 (Partner)", partner_partners)
write_partner_section(lines, "경쟁사 (Competitor)", competitor_partners)

# Hub companies section
lines.append("## 주요 허브 기업 (3개 이상 기업에서 언급)")
lines.append("")
for rtype in ['customer', 'supplier', 'partner', 'competitor']:
    entries = hub_by_type.get(rtype, [])
    if not entries:
        continue
    label = relation_label[rtype]
    lines.append(f"### {label}")
    lines.append("")
    for partner_name, count in entries:
        lines.append(f"- **{partner_name}** ({count}개 기업)")
    lines.append("")

# Per-company supply chain summary
lines.append("## 기업별 공급망 요약")
lines.append("")
lines.append(f"*(상위 500개 기업 표시)*")
lines.append("")

type_order = ['customer', 'supplier', 'partner', 'competitor']

for (corp_code, corp_name), by_type in company_data.items():
    lines.append(f"### {corp_name} ({corp_code})")
    lines.append("")
    for rtype in type_order:
        partners = by_type.get(rtype, [])
        if partners:
            label = relation_label[rtype]
            lines.append(f"**{label}:** {', '.join(partners)}")
            lines.append("")
    lines.append("")

content = '\n'.join(lines)

with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Done. Written to: {OUTPUT_PATH}")
print(f"Lines: {len(lines)}")
print(f"Chars: {len(content)}")
