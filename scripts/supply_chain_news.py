"""
scripts/supply_chain_news.py
────────────────────────────
공급망 기사화 통합 시스템 — 9가지 시나리오 자동 발굴

시나리오 (사용자 정의 우선순위):
  1. 거래처 신규/삭제 (NEW/REMOVED) — 시간 변화
  2. 매출 의존도 집중 (DEPENDENCE) — 단일 거래처 ≥30%
  3. 공급망 허브 분석 (HUB) — 핵심 거래처 분석
  4. 충격 전파 (IMPACT) — A사 위기 시 거래처 영향
  5. 클러스터 (CLUSTER) — 같은 거래처 공유 회사
  6. 수직계열 (VERTICAL) — 그룹 내부 거래
  7. 글로벌 거래처 (GLOBAL) — 해외 거래처 변동
  8. 신규 진출 (NEW_ENTRANT) — 공급망 첫 등장 회사
  9. 산업별 종합 (INDUSTRY) — 업종별 공급망 변화

각 시나리오마다 lead_id를 새로 만들거나, 별도 supply_chain_leads 테이블에 저장.
이 단서들도 generate_draft가 처리할 수 있도록 표준 형식.

실행:
  python scripts/supply_chain_news.py --init
  python scripts/supply_chain_news.py --build-all
  python scripts/supply_chain_news.py --scenario hub
  python scripts/supply_chain_news.py --scenario dependence
  python scripts/supply_chain_news.py --stats
"""

import io
import re
import sys
import json
import sqlite3
import argparse
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# .env 로드 (Gemini API 키)
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass


def _ensure_utf8_io():
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS supply_chain_leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario TEXT NOT NULL,           -- HUB/DEPENDENCE/CLUSTER/VERTICAL/GLOBAL/NEW_ENTRANT/INDUSTRY
    title TEXT NOT NULL,
    summary TEXT,
    primary_corp_code TEXT,           -- 주요 회사 (있는 경우)
    primary_corp_name TEXT,
    related_corp_codes TEXT,          -- JSON list
    related_partners TEXT,            -- JSON list
    severity INTEGER DEFAULT 3,
    metadata TEXT,                    -- JSON: 시나리오별 상세 데이터
    article_id INTEGER,               -- 생성된 기사 id (있는 경우)
    status TEXT DEFAULT 'new',
    generated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_scl_scenario ON supply_chain_leads(scenario);
CREATE INDEX IF NOT EXISTS idx_scl_severity ON supply_chain_leads(severity);
CREATE INDEX IF NOT EXISTS idx_scl_corp     ON supply_chain_leads(primary_corp_code);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def save_lead(conn: sqlite3.Connection, lead: dict) -> int:
    cur = conn.execute("""
        INSERT INTO supply_chain_leads
            (scenario, title, summary, primary_corp_code, primary_corp_name,
             related_corp_codes, related_partners, severity, metadata,
             generated_at)
        VALUES (?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
    """, [
        lead["scenario"], lead["title"], lead.get("summary", ""),
        lead.get("primary_corp_code"), lead.get("primary_corp_name"),
        json.dumps(lead.get("related_corp_codes", []), ensure_ascii=False),
        json.dumps(lead.get("related_partners", []), ensure_ascii=False),
        lead.get("severity", 3),
        json.dumps(lead.get("metadata", {}), ensure_ascii=False),
    ])
    return cur.lastrowid


# ══════════════════════════════════════════════════════════════════════════════
# 노이즈 파트너 필터 (이미 우리가 만든 export_crawl_keywords와 동일)
# ══════════════════════════════════════════════════════════════════════════════

NOISE_PARTNER = {
    "기타", "미명시", "미공개", "비공개", "없음", "해당없음",
    "다수", "다수의 고객사", "고객사", "공급사", "파트너사", "경쟁사",
}


def is_valid_partner(name: str) -> bool:
    if not name or len(name.strip()) < 2:
        return False
    s = name.strip()
    if s in NOISE_PARTNER:
        return False
    # "고객사 (미명시)" 같은 잡음
    if re.search(r"미명시|미공개|미상", s):
        return False
    if len(s) > 60:
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# 시나리오 3: 공급망 허브 분석 (HUB)
# ══════════════════════════════════════════════════════════════════════════════

def build_hub_leads(conn: sqlite3.Connection,
                    min_companies: int = 5) -> list[dict]:
    """파트너로 5개사 이상에 등장하는 회사 = 허브."""
    print(f"\n[시나리오 3: HUB] 5개사+ 거래 받는 허브 발굴 중...")

    rows = conn.execute("""
        SELECT partner_name, relation_type, COUNT(DISTINCT corp_code) cnt
        FROM supply_chain
        WHERE partner_name IS NOT NULL
        GROUP BY partner_name, relation_type
        HAVING cnt >= ?
        ORDER BY cnt DESC
    """, [min_companies]).fetchall()

    leads = []
    seen = set()  # 같은 파트너 중복 방지
    for r in rows:
        name = r["partner_name"]
        if not is_valid_partner(name):
            continue
        if name in seen:
            continue
        seen.add(name)

        # 어느 회사들이 거래?
        partners_of = conn.execute("""
            SELECT DISTINCT corp_code, corp_name, relation_type
            FROM supply_chain WHERE partner_name = ?
        """, [name]).fetchall()
        if not partners_of:
            continue

        # 관계 유형별 분류
        by_rel = defaultdict(list)
        for p in partners_of:
            by_rel[p["relation_type"]].append((p["corp_code"], p["corp_name"]))

        rel_summary = ", ".join(f"{rt}({len(lst)})" for rt, lst in by_rel.items())
        related_corps = [p["corp_code"] for p in partners_of if p["corp_code"]]
        related_names = [p["corp_name"] for p in partners_of[:10]]

        # severity: 거래 회사 많을수록 ↑
        sev = 5 if len(partners_of) >= 20 else (4 if len(partners_of) >= 10 else 3)

        leads.append({
            "scenario": "HUB",
            "title": f"{name} 거래처 {len(partners_of)}곳 분석",
            "summary": f"{name}을(를) 거래처로 둔 상장사 {len(partners_of)}곳 추적. 관계: {rel_summary}",
            "primary_corp_name": name,
            "related_corp_codes": related_corps[:30],
            "related_partners": [name],
            "severity": sev,
            "metadata": {
                "company_count": len(partners_of),
                "relations": {rt: [n for _, n in lst[:10]] for rt, lst in by_rel.items()},
                "rel_summary": rel_summary,
            },
        })

    return leads


# ══════════════════════════════════════════════════════════════════════════════
# 시나리오 5: 클러스터 (CLUSTER) — 같은 거래처 공유
# ══════════════════════════════════════════════════════════════════════════════

def build_cluster_leads(conn: sqlite3.Connection,
                        min_shared: int = 3) -> list[dict]:
    """같은 거래처를 3곳+ 공유하는 회사 그룹 = 운명 공동체."""
    print(f"\n[시나리오 5: CLUSTER] 같은 거래처 {min_shared}+ 공유 회사군...")

    # 회사 페어별 공유 거래처 수
    rows = conn.execute("""
        SELECT a.corp_code AS code1, a.corp_name AS name1,
               b.corp_code AS code2, b.corp_name AS name2,
               COUNT(*) AS shared_n,
               GROUP_CONCAT(DISTINCT a.partner_name) AS partners
        FROM supply_chain a
        JOIN supply_chain b ON a.partner_name = b.partner_name
                            AND a.corp_code < b.corp_code
                            AND a.relation_type = b.relation_type
        WHERE a.partner_name IS NOT NULL
        GROUP BY a.corp_code, b.corp_code
        HAVING shared_n >= ?
        ORDER BY shared_n DESC LIMIT 30
    """, [min_shared]).fetchall()

    leads = []
    for r in rows:
        partners = (r["partners"] or "").split(",")[:5]
        partners = [p for p in partners if is_valid_partner(p)]
        if not partners:
            continue

        sev = 5 if r["shared_n"] >= 10 else (4 if r["shared_n"] >= 5 else 3)
        leads.append({
            "scenario": "CLUSTER",
            "title": f"{r['name1']}·{r['name2']} 거래처 {r['shared_n']}곳 공유",
            "summary": (f"{r['name1']}와(과) {r['name2']}이(가) 동일 거래처 "
                        f"{r['shared_n']}곳을 공유. 동일 산업·동일 운명 공동체 가능성."),
            "primary_corp_code": r["code1"],
            "primary_corp_name": r["name1"],
            "related_corp_codes": [r["code1"], r["code2"]],
            "related_partners": partners,
            "severity": sev,
            "metadata": {
                "shared_count": r["shared_n"],
                "shared_partners": partners,
                "company_a": {"code": r["code1"], "name": r["name1"]},
                "company_b": {"code": r["code2"], "name": r["name2"]},
            },
        })
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# 시나리오 7: 글로벌 거래처 (GLOBAL)
# ══════════════════════════════════════════════════════════════════════════════

# 글로벌 거래처 화이트리스트 (대표 대형사)
GLOBAL_PARTNERS = [
    "Apple", "Google", "Amazon", "Microsoft", "Meta", "TSMC",
    "Intel", "Nvidia", "AMD", "Tesla", "Boeing", "Airbus",
    "Toyota", "Volkswagen", "BMW", "Mercedes", "Ford", "Stellantis",
    "GM", "Honda", "Hyundai", "Kia",
    "Samsung Electronics", "LG", "Sony", "Panasonic",
    "Walmart", "Costco", "Target", "Carrefour",
    "BASF", "Dow", "DuPont", "Bayer",
    "Pfizer", "Merck", "Roche", "Novartis",
    "BOE", "CSOT", "AUO", "JOLED",
    "Foxconn", "Pegatron",
    "ARAMCO", "ExxonMobil", "Shell", "BP",
    "ASML", "AMAT", "Lam Research", "KLA",
]


def build_global_leads(conn: sqlite3.Connection) -> list[dict]:
    """글로벌 대형사가 거래처로 등장한 한국 상장사."""
    print(f"\n[시나리오 7: GLOBAL] 글로벌 대형사 거래처 보유 한국 상장사...")

    leads = []
    for gp in GLOBAL_PARTNERS:
        # 대소문자 무관 매칭
        rows = conn.execute("""
            SELECT corp_code, corp_name, partner_name, relation_type, context
            FROM supply_chain
            WHERE partner_name LIKE ? COLLATE NOCASE
            ORDER BY corp_name
        """, [f"%{gp}%"]).fetchall()

        if len(rows) < 2:   # 1개사만 있으면 패스
            continue

        # 한국 회사들의 GP 거래 정리
        related_corps = [(r["corp_code"], r["corp_name"], r["relation_type"])
                         for r in rows]
        sev = 5 if len(rows) >= 10 else (4 if len(rows) >= 5 else 3)

        leads.append({
            "scenario": "GLOBAL",
            "title": f"{gp} 거래 한국 상장사 {len(rows)}곳",
            "summary": (f"글로벌 기업 {gp}을(를) 거래처(고객/공급/협력)로 둔 "
                        f"한국 상장사 {len(rows)}곳. 글로벌 가치사슬 노출도 분석 가치."),
            "primary_corp_name": gp,
            "related_corp_codes": [c[0] for c in related_corps if c[0]][:30],
            "related_partners": [gp],
            "severity": sev,
            "metadata": {
                "global_partner": gp,
                "korean_corps": [{"code": c[0], "name": c[1], "rel": c[2]} for c in related_corps[:20]],
            },
        })
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# 시나리오 6: 수직계열 (VERTICAL) — 같은 그룹 내 거래
# ══════════════════════════════════════════════════════════════════════════════

GROUP_KEYWORDS = {
    "삼성":     ["삼성", "Samsung"],
    "현대차":   ["현대", "기아", "Hyundai", "Kia", "Mobis"],
    "SK":      ["SK", "에스케이"],
    "LG":      ["LG", "엘지"],
    "롯데":    ["롯데", "Lotte"],
    "포스코":   ["POSCO", "포스코"],
    "한화":    ["한화", "Hanwha"],
    "GS":      ["GS"],
    "두산":    ["두산", "Doosan"],
    "CJ":      ["CJ"],
}


def build_vertical_leads(conn: sqlite3.Connection) -> list[dict]:
    """그룹 내 거래 빈도가 높은 회사 발굴."""
    print(f"\n[시나리오 6: VERTICAL] 그룹 내부거래 분석...")

    leads = []
    for group, keys in GROUP_KEYWORDS.items():
        # 그룹 회사 corp_code 모음
        clauses = " OR ".join(["corp_name LIKE ?"] * len(keys))
        params = [f"%{k}%" for k in keys]
        group_corps = conn.execute(f"""
            SELECT DISTINCT corp_code, corp_name FROM supply_chain
            WHERE {clauses}
        """, params).fetchall()

        if len(group_corps) < 2:
            continue

        # 그룹 내 거래 (corp_name이 같은 그룹의 다른 회사를 partner로)
        partner_clauses = " OR ".join(["partner_name LIKE ?"] * len(keys))
        rows = conn.execute(f"""
            SELECT corp_code, corp_name, partner_name, relation_type
            FROM supply_chain
            WHERE ({clauses})
              AND ({partner_clauses})
        """, params + params).fetchall()

        if len(rows) < 3:
            continue

        sev = 5 if len(rows) >= 15 else (4 if len(rows) >= 8 else 3)
        leads.append({
            "scenario": "VERTICAL",
            "title": f"{group}그룹 내부거래 {len(rows)}건",
            "summary": (f"{group}그룹 계열사 {len(group_corps)}곳 사이 내부거래 "
                        f"{len(rows)}건 추적. 계열 의존도 및 그룹 경쟁력 분석 가치."),
            "primary_corp_name": f"{group}그룹",
            "related_corp_codes": [g["corp_code"] for g in group_corps[:30] if g["corp_code"]],
            "related_partners": list({r["partner_name"] for r in rows[:15]}),
            "severity": sev,
            "metadata": {
                "group": group,
                "group_corps": [{"code": g["corp_code"], "name": g["corp_name"]} for g in group_corps[:30]],
                "internal_links": len(rows),
            },
        })
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# 시나리오 2: 매출 의존도 집중 (DEPENDENCE) — context 텍스트 분석
# ══════════════════════════════════════════════════════════════════════════════

DEPENDENCE_PATTERNS = [
    r"매출\s*비중\s*([0-9.]+)\s*%",
    r"비중\s*([0-9.]+)\s*%",
    r"([0-9.]+)\s*%(?:를?)\s*(?:차지|점유|비중)",
    r"전체\s*매출\s*[가-힣]*\s*([0-9.]+)\s*%",
]


def build_dependence_leads(conn: sqlite3.Connection,
                           threshold_pct: float = 30.0) -> list[dict]:
    """context 텍스트에서 30%+ 매출 의존 패턴 추출."""
    print(f"\n[시나리오 2: DEPENDENCE] {threshold_pct}%+ 매출 의존도 추출...")

    rows = conn.execute("""
        SELECT corp_code, corp_name, partner_name, relation_type, context
        FROM supply_chain
        WHERE relation_type = 'customer'
          AND context IS NOT NULL
          AND LENGTH(context) >= 20
    """).fetchall()

    leads = []
    seen = set()
    for r in rows:
        if not r["context"]:
            continue
        ctx = r["context"]

        # 비중 추출
        max_pct = 0.0
        for pat in DEPENDENCE_PATTERNS:
            for m in re.finditer(pat, ctx):
                try:
                    v = float(m.group(1))
                    if 5 < v < 100:   # 합리 범위
                        max_pct = max(max_pct, v)
                except ValueError:
                    continue

        if max_pct < threshold_pct:
            continue
        if not is_valid_partner(r["partner_name"]):
            continue

        key = (r["corp_code"], r["partner_name"])
        if key in seen:
            continue
        seen.add(key)

        sev = 5 if max_pct >= 50 else (4 if max_pct >= 40 else 3)
        leads.append({
            "scenario": "DEPENDENCE",
            "title": f"{r['corp_name']}, {r['partner_name']} 매출 의존도 {max_pct:.0f}%",
            "summary": (f"{r['corp_name']}의 매출이 {r['partner_name']} 한 거래처에 "
                        f"{max_pct:.0f}% 집중. 단일 거래처 리스크 부각."),
            "primary_corp_code": r["corp_code"],
            "primary_corp_name": r["corp_name"],
            "related_corp_codes": [r["corp_code"]],
            "related_partners": [r["partner_name"]],
            "severity": sev,
            "metadata": {
                "dependence_pct": max_pct,
                "dependent_partner": r["partner_name"],
                "context_excerpt": ctx[:300],
            },
        })
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# 시나리오 9: 산업별 종합 (INDUSTRY)
# ══════════════════════════════════════════════════════════════════════════════

def build_industry_leads(conn: sqlite3.Connection,
                         min_corps: int = 5) -> list[dict]:
    """sector별 공급망 패턴 분석."""
    print(f"\n[시나리오 9: INDUSTRY] 산업별 공급망 종합 분석...")

    rows = conn.execute("""
        SELECT c.sector, COUNT(DISTINCT sc.corp_code) corp_n,
               COUNT(DISTINCT sc.partner_name) partner_n,
               COUNT(*) total_links
        FROM supply_chain sc
        JOIN companies c ON sc.corp_code = c.corp_code
        WHERE c.sector IS NOT NULL
        GROUP BY c.sector
        HAVING corp_n >= ?
        ORDER BY corp_n DESC
    """, [min_corps]).fetchall()

    leads = []
    for r in rows:
        sector = r["sector"]
        if not sector:
            continue

        # 해당 산업 Top 거래처
        top_partners = conn.execute("""
            SELECT sc.partner_name, COUNT(DISTINCT sc.corp_code) cnt
            FROM supply_chain sc
            JOIN companies c ON sc.corp_code = c.corp_code
            WHERE c.sector = ?
            GROUP BY sc.partner_name
            HAVING cnt >= 2
            ORDER BY cnt DESC LIMIT 10
        """, [sector]).fetchall()
        top_partners = [(t["partner_name"], t["cnt"])
                        for t in top_partners
                        if is_valid_partner(t["partner_name"])][:8]

        if not top_partners:
            continue

        sev = 5 if r["corp_n"] >= 30 else (4 if r["corp_n"] >= 15 else 3)
        leads.append({
            "scenario": "INDUSTRY",
            "title": f"{sector} 공급망 — {r['corp_n']}개사 분석",
            "summary": (f"{sector} 업종 상장사 {r['corp_n']}곳의 공급망 종합. "
                        f"고유 거래처 {r['partner_n']}곳, 총 거래 {r['total_links']}건."),
            "primary_corp_name": sector,
            "related_partners": [p for p, _ in top_partners],
            "severity": sev,
            "metadata": {
                "sector": sector,
                "corp_count": r["corp_n"],
                "partner_count": r["partner_n"],
                "total_links": r["total_links"],
                "top_partners": [{"name": p, "company_count": c} for p, c in top_partners],
            },
        })
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# 시나리오 4: 충격 전파 (IMPACT) — 위기 회사 → 거래처 영향
# ══════════════════════════════════════════════════════════════════════════════

def build_impact_leads(conn: sqlite3.Connection) -> list[dict]:
    """severity 5 risk_alert 단서 발생 회사의 거래처 영향 분석."""
    print(f"\n[시나리오 4: IMPACT] 위기 단서 회사의 거래처 충격 분석...")

    risk_corps = conn.execute("""
        SELECT DISTINCT sl.corp_code, sl.corp_name, sl.title
        FROM story_leads sl
        WHERE sl.lead_type = 'risk_alert' AND sl.severity = 5
    """).fetchall()

    leads = []
    for rc in risk_corps:
        if not rc["corp_code"]:
            continue
        # 그 회사의 고객사·공급사 (영향 받는 거래처)
        partners = conn.execute("""
            SELECT partner_name, relation_type, context
            FROM supply_chain
            WHERE corp_code = ?
        """, [rc["corp_code"]]).fetchall()
        if len(partners) < 3:
            continue

        valid = [p for p in partners if is_valid_partner(p["partner_name"])]
        if not valid:
            continue

        leads.append({
            "scenario": "IMPACT",
            "title": f"{rc['corp_name']} 위기 시 거래처 {len(valid)}곳 영향권",
            "summary": (f"{rc['corp_name']}의 risk_alert 단서 발생. 영향받을 수 있는 "
                        f"거래처 {len(valid)}곳 추적. 공급망 충격 전파 분석 가치."),
            "primary_corp_code": rc["corp_code"],
            "primary_corp_name": rc["corp_name"],
            "related_partners": [p["partner_name"] for p in valid[:15]],
            "severity": 5,
            "metadata": {
                "risk_origin": rc["title"],
                "affected_partners": [
                    {"name": p["partner_name"], "rel": p["relation_type"]}
                    for p in valid[:20]
                ],
            },
        })
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# 시나리오 8: 신규 진출 (NEW_ENTRANT) — 공급망 첫 등장
# ══════════════════════════════════════════════════════════════════════════════

def build_new_entrant_leads(conn: sqlite3.Connection) -> list[dict]:
    """
    supply_chain에 partner_name으로 처음 등장한 회사들 발굴.
    이 회사들은 시장에서 인지도 낮은 새 진입자일 수 있음.
    """
    print(f"\n[시나리오 8: NEW_ENTRANT] 공급망 첫 등장 회사들...")

    # 등장 횟수 1~2회인 partner들 (희소 진입자)
    rows = conn.execute("""
        SELECT partner_name, COUNT(*) cnt,
               GROUP_CONCAT(corp_name) hub_names,
               GROUP_CONCAT(relation_type) rels
        FROM supply_chain
        WHERE partner_name IS NOT NULL
        GROUP BY partner_name
        HAVING cnt = 1   -- 정확히 1회만 등장
        ORDER BY partner_name LIMIT 200
    """).fetchall()

    # 정보 격차 점수 매칭 (이 회사가 별도 상장사인지)
    leads = []
    for r in rows:
        name = r["partner_name"]
        if not is_valid_partner(name):
            continue

        # company_info_gap에 등록된 상장사인가?
        ig = conn.execute("""
            SELECT cig.* FROM company_info_gap cig
            WHERE cig.corp_name LIKE ? OR cig.corp_name = ?
            LIMIT 1
        """, [f"%{name}%", name]).fetchone()

        # 비상장 거래처는 패스 (DART 분석 못함)
        if not ig:
            continue
        if ig["score_total"] < 70:   # 너무 알려진 회사면 패스
            continue

        leads.append({
            "scenario": "NEW_ENTRANT",
            "title": f"{name} 공급망 첫 등장 — 정보격차 {ig['score_total']}점",
            "summary": (f"{name}이(가) 상장사 거래처로 처음 등장. "
                        f"{r['hub_names'][:50]}와(과) 거래 시작. "
                        f"정보 격차 {ig['score_total']}점."),
            "primary_corp_code": ig["corp_code"],
            "primary_corp_name": name,
            "related_partners": [name],
            "severity": 4 if ig["score_total"] >= 90 else 3,
            "metadata": {
                "info_gap_score": ig["score_total"],
                "hub_names": r["hub_names"],
                "first_relation": r["rels"],
            },
        })

    # 너무 많으면 상위 50개만
    return leads[:50]


# ══════════════════════════════════════════════════════════════════════════════
# 통합 빌드
# ══════════════════════════════════════════════════════════════════════════════

SCENARIO_BUILDERS = {
    "hub":         build_hub_leads,
    "cluster":     build_cluster_leads,
    "global":      build_global_leads,
    "vertical":    build_vertical_leads,
    "dependence":  build_dependence_leads,
    "industry":    build_industry_leads,
    "impact":      build_impact_leads,
    "new_entrant": build_new_entrant_leads,
}


def build_all(conn: sqlite3.Connection, only: str = None) -> dict:
    # 기존 supply_chain_leads 모두 삭제 후 재빌드 (중복 방지)
    if not only:
        conn.execute("DELETE FROM supply_chain_leads")
        conn.commit()

    counts = {}
    for name, builder in SCENARIO_BUILDERS.items():
        if only and name != only:
            continue
        try:
            leads = builder(conn)
            for lead in leads:
                save_lead(conn, lead)
            counts[name] = len(leads)
            print(f"  ✓ {name.upper():<12} {len(leads):>4}건")
        except Exception as e:
            print(f"  ⚠ {name}: {e}")
        conn.commit()
    return counts


def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 70)
    print("  공급망 기사화 시나리오별 단서 현황")
    print("=" * 70)
    total = conn.execute("SELECT COUNT(*) FROM supply_chain_leads").fetchone()[0]
    if total == 0:
        print("  (아직 빌드 안 됨 — --build-all 실행)")
        return
    print(f"\n  전체: {total:,}건\n  [시나리오별]")
    for r in conn.execute("""
        SELECT scenario, COUNT(*) cnt, AVG(severity) avg_s
        FROM supply_chain_leads GROUP BY scenario ORDER BY cnt DESC
    """):
        icon = {"HUB":"🌐","DEPENDENCE":"⚠️","CLUSTER":"🔗","VERTICAL":"🏢",
                "GLOBAL":"🌍","INDUSTRY":"📊","IMPACT":"💥","NEW_ENTRANT":"🆕"}
        print(f"    {icon.get(r['scenario'],'⚪')} {r['scenario']:<12} {r['cnt']:>4}건 (sev 평균 {r['avg_s']:.1f})")

    print("\n  [severity 5 — 즉시 출고 우선]")
    for r in conn.execute("""
        SELECT scenario, title FROM supply_chain_leads
        WHERE severity = 5 ORDER BY id DESC LIMIT 15
    """):
        print(f"    [{r['scenario']:<12}] {(r['title'] or '')[:55]}")


# ══════════════════════════════════════════════════════════════════════════════
# 공급망 단서 → 기사 생성
# ══════════════════════════════════════════════════════════════════════════════

def generate_articles(conn: sqlite3.Connection,
                      scenario: str | None = None,
                      severity_min: int = 4,
                      limit: int = 20) -> dict:
    """supply_chain_leads → article_drafts (전용 프롬프트)."""
    sys.path.insert(0, str(ROOT / "scripts"))
    from generate_draft import (
        _build_prompt_supply_chain, call_gemini_article,
        parse_article_json, save_draft
    )

    where = "severity >= ? AND article_id IS NULL"
    params = [severity_min]
    if scenario:
        where += " AND scenario = ?"
        params.append(scenario.upper())

    rows = conn.execute(f"""
        SELECT * FROM supply_chain_leads
        WHERE {where} ORDER BY severity DESC, id ASC LIMIT ?
    """, params + [limit]).fetchall()

    if not rows:
        return {"processed": 0, "success": 0}

    print(f"\n[info] 공급망 단서 {len(rows)}건 → 기사화\n")
    success = errors = 0

    for i, lead in enumerate(rows, 1):
        ld = dict(lead)
        try:
            ld["metadata"] = json.loads(ld.get("metadata") or "{}")
        except Exception:
            ld["metadata"] = {}

        print(f"  [{i:3d}/{len(rows)}] {ld['scenario']:<12} sev{ld['severity']} | {ld['title'][:50]}")
        try:
            prompt = _build_prompt_supply_chain(ld, conn)
            text, model_name = call_gemini_article(prompt)
            article = parse_article_json(text)
            if not article.get("headline"):
                print(f"        ⚠ 파싱 실패")
                errors += 1
                continue

            # supply_chain_lead의 primary_corp을 article_drafts에 매핑
            # story_lead가 없는 경우 가짜 lead Row 생성
            class FakeLead:
                def __init__(self, d):
                    self._d = d
                def __getitem__(self, k): return self._d.get(k)

            fake_lead = FakeLead({
                "id": -ld["id"],   # 음수로 supply_chain_lead 표시
                "corp_code": ld.get("primary_corp_code") or "",
                "corp_name": ld.get("primary_corp_name") or "",
                "lead_type": f"sc_{ld['scenario'].lower()}",
                "evidence":  ld.get("summary") or "",
                "summary":   ld.get("summary") or "",
                "comparison_id": None,
            })

            draft_id = save_draft(conn, lead_id=-ld["id"],
                                  lead=fake_lead, article=article,
                                  model_name=model_name, ai_result=ld.get("summary",""),
                                  fin_block="", sc_context="")
            # supply_chain_leads에 article_id 연결
            conn.execute(
                "UPDATE supply_chain_leads SET article_id=?, status='drafted' WHERE id=?",
                [draft_id, ld["id"]]
            )
            conn.commit()
            print(f"        ✓ #{draft_id} {article['headline'][:40]} ({model_name})")
            success += 1
        except RuntimeError as e:
            print(f"        ✗ {e}")
            errors += 1
            if "할당량 초과" in str(e):
                print("[중단] API 한도 초과")
                break
        except Exception as e:
            print(f"        ✗ {type(e).__name__}: {e}")
            errors += 1

    return {"processed": len(rows), "success": success, "errors": errors}


def main():
    p = argparse.ArgumentParser(description="공급망 기사화 시스템")
    p.add_argument("--init", action="store_true")
    p.add_argument("--build-all", action="store_true", help="9가지 시나리오 모두 빌드")
    p.add_argument("--scenario", choices=list(SCENARIO_BUILDERS.keys()),
                   help="특정 시나리오만 빌드")
    p.add_argument("--generate", action="store_true",
                   help="공급망 단서 → 기사 생성")
    p.add_argument("--severity", type=int, default=4)
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--gen-scenario", choices=[s.upper() for s in SCENARIO_BUILDERS.keys()],
                   help="특정 시나리오 단서만 기사화")
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()

    conn = get_conn()
    ensure_schema(conn)

    if args.init:
        print("✓ supply_chain_leads 스키마 생성"); return
    if args.stats:
        print_stats(conn); return
    if args.generate:
        result = generate_articles(conn,
                                   scenario=args.gen_scenario,
                                   severity_min=args.severity,
                                   limit=args.limit)
        print(f"\n[완료] {result}")
        return
    if args.build_all or args.scenario:
        counts = build_all(conn, only=args.scenario)
        print(f"\n[완료] 시나리오별 단서: {counts}")
        print()
        print_stats(conn)
        return

    print(__doc__)


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
