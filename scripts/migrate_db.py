"""
scripts/migrate_db.py
─────────────────────
DB 스키마 마이그레이션 — Company Catcher v2.0

추가 테이블:
  report_type_meta  — 보고서 유형 소프트코딩 (연도·분기·공시기간)
  story_leads       — 취재 단서 (AI 변화 감지)
  article_drafts    — 기사 초안
  alert_rules       — 변화 감지 규칙 (소프트코딩)

추가 인덱스:
  ai_comparisons    — corp_code, status 복합
  reports           — FTS5 전문 검색

실행:
  python scripts/migrate_db.py
  python scripts/migrate_db.py --seed   # 기초 데이터 자동 입력
"""

import io
import sqlite3
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


def get_db():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db


# ── 마이그레이션 정의 ──────────────────────────────────────────────────────────
MIGRATIONS = []

def migration(fn):
    MIGRATIONS.append(fn)
    return fn


@migration
def m01_migration_log(db):
    """마이그레이션 이력 테이블"""
    db.execute("""
        CREATE TABLE IF NOT EXISTS _migrations (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT UNIQUE,
            applied_at TEXT
        )
    """)
    db.commit()


@migration
def m02_report_type_meta(db):
    """보고서 유형 메타 — 소프트코딩 핵심 테이블"""
    db.execute("""
        CREATE TABLE IF NOT EXISTS report_type_meta (
            type_code    TEXT PRIMARY KEY,
            label        TEXT NOT NULL,
            short_label  TEXT NOT NULL,
            year         INTEGER NOT NULL,
            quarter      TEXT NOT NULL CHECK(quarter IN ('annual','q1','h1','q3')),
            filing_start TEXT,          -- 공시 기간 시작 MM-DD
            filing_end   TEXT,          -- 공시 기간 마감 MM-DD
            collect_start TEXT,         -- 자동수집 시작 MM-DD (기간보다 며칠 앞서)
            collect_end   TEXT,         -- 자동수집 종료 MM-DD
            is_active    INTEGER DEFAULT 1,
            sort_order   INTEGER DEFAULT 0,
            created_at   TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    db.commit()


@migration
def m03_story_leads(db):
    """취재 단서 테이블"""
    db.execute("""
        CREATE TABLE IF NOT EXISTS story_leads (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            corp_code      TEXT NOT NULL,
            corp_name      TEXT,
            sector         TEXT,
            lead_type      TEXT NOT NULL,
            severity       INTEGER NOT NULL DEFAULT 3
                           CHECK(severity BETWEEN 1 AND 5),
            title          TEXT NOT NULL,
            summary        TEXT,
            evidence       TEXT,
            keywords       TEXT,        -- JSON 배열: ["신규사업","AI","해외진출"]
            comparison_id  INTEGER REFERENCES ai_comparisons(id),
            report_type_a  TEXT,
            report_type_b  TEXT,
            status         TEXT NOT NULL DEFAULT 'new'
                           CHECK(status IN ('new','reviewing','drafted','published','dismissed')),
            assigned_to    TEXT,        -- 담당 기자 (확장용)
            note           TEXT,        -- 편집자 메모
            created_at     TEXT DEFAULT (datetime('now','localtime')),
            updated_at     TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_leads_severity   ON story_leads(severity DESC, created_at DESC)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_leads_status     ON story_leads(status, lead_type)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_leads_corp       ON story_leads(corp_code)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_leads_type       ON story_leads(lead_type, severity DESC)")
    db.commit()


@migration
def m04_article_drafts(db):
    """기사 초안 테이블"""
    db.execute("""
        CREATE TABLE IF NOT EXISTS article_drafts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id      INTEGER REFERENCES story_leads(id),
            corp_code    TEXT,
            corp_name    TEXT,
            headline     TEXT NOT NULL,
            subheadline  TEXT,
            content      TEXT NOT NULL,
            style        TEXT NOT NULL DEFAULT 'news'
                         CHECK(style IN ('news','analysis','brief','column')),
            model        TEXT,
            word_count   INTEGER DEFAULT 0,
            char_count   INTEGER DEFAULT 0,
            status       TEXT NOT NULL DEFAULT 'draft'
                         CHECK(status IN ('draft','edited','approved','published','rejected')),
            editor_note  TEXT,
            published_url TEXT,
            created_at   TEXT DEFAULT (datetime('now','localtime')),
            updated_at   TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_drafts_status  ON article_drafts(status, created_at DESC)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_drafts_corp    ON article_drafts(corp_code)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_drafts_lead    ON article_drafts(lead_id)")
    db.commit()


@migration
def m05_alert_rules(db):
    """변화 감지 규칙 — 소프트코딩 (관리자가 수정 가능)"""
    db.execute("""
        CREATE TABLE IF NOT EXISTS alert_rules (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_code   TEXT UNIQUE NOT NULL,
            lead_type   TEXT NOT NULL,
            severity    INTEGER NOT NULL DEFAULT 3,
            title_tmpl  TEXT NOT NULL,
            keywords    TEXT NOT NULL,  -- JSON 배열: 감지할 키워드
            exclude_kw  TEXT,           -- JSON 배열: 제외 키워드
            description TEXT,
            is_active   INTEGER DEFAULT 1,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    db.commit()


@migration
def m06_fts5_index(db):
    """FTS5 전문 검색 인덱스 — 기업명 + 사업내용 키워드 검색"""
    # 기존 FTS 테이블 있으면 재생성
    db.execute("DROP TABLE IF EXISTS reports_fts")
    db.execute("""
        CREATE VIRTUAL TABLE reports_fts USING fts5(
            corp_code  UNINDEXED,
            corp_name,
            biz_content,
            report_type UNINDEXED,
            content    = 'reports',
            content_rowid = 'id',
            tokenize   = 'unicode61 remove_diacritics 2'
        )
    """)
    # 기존 데이터 색인
    print("  FTS5 인덱스 구축 중 (biz_content 있는 보고서 대상)...")
    db.execute("""
        INSERT INTO reports_fts(rowid, corp_name, biz_content)
        SELECT id, corp_name, COALESCE(biz_content,'')
        FROM reports
        WHERE biz_content IS NOT NULL AND LENGTH(biz_content) > 100
    """)
    db.commit()


@migration
def m07_additional_indexes(db):
    """ai_comparisons 추가 인덱스"""
    db.execute("CREATE INDEX IF NOT EXISTS idx_ac_corp_status ON ai_comparisons(corp_code, status)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_ac_analyzed    ON ai_comparisons(analyzed_at DESC)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_ac_model       ON ai_comparisons(model, status)")
    # reports 추가 인덱스
    db.execute("CREATE INDEX IF NOT EXISTS idx_rpt_type_biz  ON reports(report_type) WHERE biz_content IS NOT NULL")
    db.commit()


@migration
def m08_fts5_triggers(db):
    """FTS5 자동 동기화 트리거"""
    db.execute("""
        CREATE TRIGGER IF NOT EXISTS reports_fts_insert
        AFTER INSERT ON reports BEGIN
            INSERT INTO reports_fts(rowid, corp_name, biz_content)
            VALUES (new.id, new.corp_name, COALESCE(new.biz_content,''));
        END
    """)
    db.execute("""
        CREATE TRIGGER IF NOT EXISTS reports_fts_update
        AFTER UPDATE ON reports BEGIN
            UPDATE reports_fts
            SET corp_name=new.corp_name, biz_content=COALESCE(new.biz_content,'')
            WHERE rowid=new.id;
        END
    """)
    db.execute("""
        CREATE TRIGGER IF NOT EXISTS reports_fts_delete
        AFTER DELETE ON reports BEGIN
            DELETE FROM reports_fts WHERE rowid=old.id;
        END
    """)
    db.commit()


# ── 시드 데이터 ────────────────────────────────────────────────────────────────
SEED_REPORT_TYPES = [
    # (type_code, label, short_label, year, quarter, filing_start, filing_end, collect_start, collect_end, sort_order)
    ("2024_q1",    "2024년 1분기보고서", "24·1Q",  2024, "q1",     "05-01","05-15","04-25","05-16", 10),
    ("2024_h1",    "2024년 반기보고서",  "24·반기", 2024, "h1",     "08-01","08-14","07-25","08-15", 20),
    ("2024_q3",    "2024년 3분기보고서", "24·3Q",  2024, "q3",     "11-01","11-14","10-25","11-15", 30),
    ("2024_annual","2024년 사업보고서",  "24·사업", 2024, "annual", "01-01","04-30","12-20","05-01", 40),
    ("2025_q1",    "2025년 1분기보고서", "25·1Q",  2025, "q1",     "05-01","05-15","04-25","05-16", 50),
    ("2025_h1",    "2025년 반기보고서",  "25·반기", 2025, "h1",     "08-01","08-14","07-25","08-15", 60),
    ("2025_q3",    "2025년 3분기보고서", "25·3Q",  2025, "q3",     "11-01","11-14","10-25","11-15", 70),
    ("2025_annual","2025년 사업보고서",  "25·사업", 2025, "annual", "01-01","04-30","12-20","05-01", 80),
    ("2026_q1",    "2026년 1분기보고서", "26·1Q",  2026, "q1",     "05-01","05-15","04-25","05-16", 90),
    ("2026_h1",    "2026년 반기보고서",  "26·반기", 2026, "h1",     "08-01","08-14","07-25","08-15",100),
    ("2026_q3",    "2026년 3분기보고서", "26·3Q",  2026, "q3",     "11-01","11-14","10-25","11-15",110),
    ("2026_annual","2026년 사업보고서",  "26·사업", 2026, "annual", "01-01","04-30","12-20","05-01",120),
]

SEED_ALERT_RULES = [
    # severity 5 — 즉시 취재
    ("biz_exit",       "strategy_change", 5, "{corp_name} 사업 철수·중단 감지",
     ["사업 철수","영업 중단","폐업","청산","사업 포기","사업 중지"],
     [], "기존 사업 철수 또는 영업 중단 관련 서술 등장"),

    ("biz_new_mega",   "strategy_change", 5, "{corp_name} 대규모 신사업 진출",
     ["신규 사업","새로운 사업","사업 다각화","신사업"],
     ["검토 중","계획 중"], "신규 사업 진출 + 수백억 이상 투자"),

    ("market_share_surge","market_shift", 5, "{corp_name} 시장점유율 급변",
     ["시장 점유율","점유율","마켓쉐어"],
     [], "시장 점유율 관련 수치 변동"),

    # severity 4 — 당일 검토
    ("new_competitor",  "market_shift",   4, "{corp_name} 신규 경쟁사 등장",
     ["경쟁사","경쟁업체","경쟁 심화","시장 경쟁"],
     [], "경쟁사 관련 서술 신규 등장 또는 변화"),

    ("overseas_entry",  "strategy_change",4, "{corp_name} 해외 시장 진출",
     ["해외 진출","글로벌","수출 확대","해외 시장","현지화"],
     [], "해외 시장 진출 또는 수출 확대 관련 서술"),

    ("supply_risk",     "risk_alert",     4, "{corp_name} 공급망 리스크 발생",
     ["공급 리스크","원재료 부족","공급 불안","조달 차질","공급망 위기"],
     [], "원재료·부품 공급 리스크 신규 등장"),

    ("restructure",     "risk_alert",     4, "{corp_name} 구조조정 감지",
     ["구조조정","인력 감축","희망퇴직","조직 개편","사업 재편"],
     [], "구조조정 또는 인력 감축 관련 서술"),

    # severity 3 — 주간 검토
    ("product_shift",   "strategy_change",3, "{corp_name} 주력 제품 변화",
     ["주력 제품","핵심 제품","신제품","제품 라인업","포트폴리오"],
     [], "주력 제품·서비스 변화 감지"),

    ("regulation_risk", "risk_alert",     3, "{corp_name} 규제 리스크 변화",
     ["규제","법규","인허가","인증","정책 변화","법 개정"],
     [], "규제·인허가 관련 리스크 변화"),

    ("partner_change",  "supply_chain",   3, "{corp_name} 공급망 파트너 변화",
     ["협력사","파트너","주요 공급사","매입처","거래처"],
     [], "주요 공급망 파트너 변경"),

    ("numeric_surge",   "numeric_change", 3, "{corp_name} 주요 수치 급변",
     ["전년 대비","증가","감소","성장","하락","급등","급락"],
     [], "주요 실적 수치의 급격한 변동"),

    ("ai_tech",         "strategy_change",3, "{corp_name} AI·첨단기술 도입",
     ["인공지능","AI","머신러닝","딥러닝","빅데이터","자동화","디지털 전환"],
     [], "AI·첨단기술 관련 신규 사업 또는 도입"),
]


def seed_data(db):
    print("\n시드 데이터 입력 중...")

    # report_type_meta
    for row in SEED_REPORT_TYPES:
        db.execute("""
            INSERT OR IGNORE INTO report_type_meta
              (type_code, label, short_label, year, quarter,
               filing_start, filing_end, collect_start, collect_end, sort_order)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, row)
    print(f"  report_type_meta: {len(SEED_REPORT_TYPES)}개 유형 등록")

    # alert_rules
    for r in SEED_ALERT_RULES:
        db.execute("""
            INSERT OR IGNORE INTO alert_rules
              (rule_code, lead_type, severity, title_tmpl, keywords, exclude_kw, description)
            VALUES (?,?,?,?,?,?,?)
        """, (r[0], r[1], r[2], r[3],
              json.dumps(r[4], ensure_ascii=False),
              json.dumps(r[5], ensure_ascii=False),
              r[6]))
    print(f"  alert_rules: {len(SEED_ALERT_RULES)}개 규칙 등록")

    db.commit()


# ── 실행 ───────────────────────────────────────────────────────────────────────
def run_migration(db, fn):
    name = fn.__name__
    exists = db.execute(
        "SELECT 1 FROM _migrations WHERE name=?", [name]
    ).fetchone()
    if exists:
        print(f"  [{name}] 이미 적용됨 — 건너뜀")
        return False
    print(f"  [{name}] 적용 중...", end=" ")
    fn(db)
    db.execute(
        "INSERT OR IGNORE INTO _migrations(name, applied_at) VALUES(?,?)",
        [name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    )
    db.commit()
    print("완료")
    return True


def main():
    parser = argparse.ArgumentParser(description="DB 스키마 마이그레이션")
    parser.add_argument("--seed", action="store_true", help="기초 데이터 자동 입력")
    parser.add_argument("--fts-rebuild", action="store_true", help="FTS5 인덱스 재구축")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"Company Catcher DB 마이그레이션 v2.0")
    print(f"DB: {DB_PATH}")
    print(f"{'='*55}\n")

    db = get_db()

    # m01은 항상 먼저 (이력 테이블 생성)
    m01_migration_log(db)

    applied = 0
    for fn in MIGRATIONS[1:]:  # m01 제외
        if fn.__name__ == "m06_fts5_index" and not args.fts_rebuild:
            # FTS는 --fts-rebuild 또는 최초 실행 시만
            exists = db.execute(
                "SELECT 1 FROM _migrations WHERE name=?", [fn.__name__]
            ).fetchone()
            if exists:
                print(f"  [{fn.__name__}] 이미 적용됨 — 건너뜀 (재구축: --fts-rebuild)")
                continue
        if run_migration(db, fn):
            applied += 1

    print(f"\n마이그레이션 완료: {applied}개 적용")

    if args.seed:
        seed_data(db)

    # 최종 상태 출력
    print(f"\n{'─'*55}")
    print("현재 DB 테이블 현황:")
    tables = db.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """).fetchall()
    for t in tables:
        try:
            cnt = db.execute(f"SELECT COUNT(*) FROM [{t['name']}]").fetchone()[0]
            print(f"  {t['name']:<35} {cnt:>8,}건")
        except Exception:
            print(f"  {t['name']}")

    print(f"\n등록된 alert_rules:")
    try:
        rules = db.execute(
            "SELECT severity, rule_code, lead_type FROM alert_rules ORDER BY severity DESC, rule_code"
        ).fetchall()
        for r in rules:
            star = "⭐" * r["severity"]
            print(f"  [{r['severity']}] {r['rule_code']:<22} ({r['lead_type']})")
    except Exception:
        pass
    print(f"{'─'*55}\n")


if __name__ == "__main__":
    main()
