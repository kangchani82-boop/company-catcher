"""
H-1: growth_signals importance=5 + has_change=1 + news_likely=N 인데
     story_leads에 sev>=4가 없는 회사들의 단서를 정식 story_leads에 추가 발굴.

원인: detect_leads의 룰이 못 잡았지만 Gemini가 명확한 변화로 인식한 케이스.
"""
import sqlite3, json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row

now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 카테고리 → lead_type 매핑
CAT_TO_LT = {
    'new_product':'strategy_change',
    'new_business':'strategy_change',
    'facility_investment':'strategy_change',
    'customer_change':'supply_chain',
    'new_project':'strategy_change',
    'exit_business':'strategy_change',
    'global_customer':'supply_chain',
    'corp_acquire':'strategy_change',
    'new_patent':'strategy_change',
    'corp_setup':'strategy_change',
    'regulator_risk':'risk_alert',
    'corp_close_sell':'strategy_change',
    'gov_grant':'strategy_change',
    'equity_invest':'strategy_change',
}
CAT_KO = {
    'new_product':'신규 제품 출시','new_business':'신규 사업 진출',
    'facility_investment':'시설투자','customer_change':'고객사 변화',
    'new_project':'신규 과제','exit_business':'사업철수',
    'global_customer':'글로벌 고객 확보','corp_acquire':'기업·법인 인수',
    'new_patent':'신규 특허','corp_setup':'법인 설립',
    'regulator_risk':'규제 리스크','corp_close_sell':'법인 매각',
    'gov_grant':'정부 과제','equity_invest':'타법인 출자',
}

# 후보 추출: importance=5 + has_change=1 + news_likely=N + story_leads에 sev>=4 없음
candidates = db.execute('''
    SELECT gs.id, gs.comparison_id, gs.corp_code, gs.corp_name,
           gs.change_categories, gs.evidence, gs.importance, gs.news_likely,
           ac.report_type_a, ac.report_type_b
    FROM growth_signals gs
    JOIN ai_comparisons ac ON ac.id=gs.comparison_id
    WHERE gs.importance=5 AND gs.has_change=1 AND gs.news_likely='N'
      AND NOT EXISTS (
        SELECT 1 FROM story_leads sl
        WHERE sl.corp_code=gs.corp_code AND sl.severity>=4
          AND sl.comparison_id=gs.comparison_id
      )
''').fetchall()

print(f'추가 발굴 후보: {len(candidates)}건')

added = 0; skipped = 0
for r in candidates:
    try:
        cats = json.loads(r['change_categories'] or '[]')
        if not cats: continue
        # 첫 카테고리 기준
        cat = cats[0]
        lt = CAT_TO_LT.get(cat, 'strategy_change')
        title = f"{r['corp_name']} {CAT_KO.get(cat, cat)}"
        sev = 4  # importance=5는 sev=4로 (sev=5는 룰 기반 보수적)
        kws = [CAT_KO.get(c,c) for c in cats[:5]]

        # UNIQUE: corp_code + lead_type + comparison_id
        try:
            db.execute('''
                INSERT INTO story_leads
                  (corp_code, corp_name, lead_type, severity, title, summary, evidence,
                   keywords, comparison_id, report_type_a, report_type_b, status,
                   created_at, updated_at, info_gap_label, gemini_verified,
                   gemini_news_likely, gemini_score, gemini_verified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?,
                        'HIGH', 'Y', 'N', 5, ?)
            ''', [r['corp_code'], r['corp_name'], lt, sev, title,
                  (r['evidence'] or '')[:200],
                  (r['evidence'] or '')[:500],
                  json.dumps(kws, ensure_ascii=False),
                  r['comparison_id'], r['report_type_a'], r['report_type_b'],
                  now, now, now])
            added += 1
        except sqlite3.IntegrityError:
            skipped += 1
            continue
    except Exception as e:
        print(f'  ✗ corp={r["corp_code"]}: {str(e)[:80]}')

db.commit()
print(f'\n✓ 추가 발굴: {added}건  /  중복 스킵: {skipped}건')
print(f'  → 모두 info_gap_label=HIGH, gemini_verified=Y, news_likely=N 으로 표시')

# 검증
total = db.execute("SELECT COUNT(*) FROM story_leads WHERE info_gap_label='HIGH'").fetchone()[0]
conf = db.execute("SELECT COUNT(*) FROM story_leads WHERE info_gap_label='HIGH_CONFIRMED'").fetchone()[0]
print(f'\n[전체 누적]')
print(f'  HIGH 단서: {total}')
print(f'  HIGH_CONFIRMED: {conf}')
