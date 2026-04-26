"""
scripts/cross_signals.py
────────────────────────
Phase Cross — 교차 신호 시스템

핵심 사명: "수시공시·단서·공급망 자체는 가치 없음.
            여러 신호가 조합되어 새로운 해석을 낳을 때 진짜 가치."

8가지 교차 패턴 자동 발굴:

  1. CONCENTRATION_SHIFT  매출 의존도 큰 회사 + 그 거래처의 수시공시
     → "Apple 변화가 LG이노텍 매출 81%에 직접 영향"

  2. SUPPLY_CHAIN_RIPPLE  공급망 허브 회사 + 위기 단서 발생
     → "삼성전자 위기 시 거래처 107곳 충격 전파"

  3. NEW_BUSINESS_FUNDING 신사업 진출 단서 + 같은 시기 자본 조달
     → "신사업 발표 + 유상증자 = 본격 추진 의지"

  4. INDUSTRY_WAVE        같은 산업 다수 회사 + 동시 변화/공시
     → "건설업 9곳 동시 신규사업 + 수시공시 = 산업 트렌드"

  5. HIDDEN_CHANGE        정보격차 큰 회사 + 활발한 공시 활동
     → "잘 모르던 코스닥 종목인데 갑자기 공시 늘어남 — 주목 시점"

  6. GLOBAL_RISK_TRANSFER 글로벌 거래처 회사 + 거시 변화
     → "Apple 거래 25곳 동시 영향 가능"

  7. GROUP_RESTRUCTURING  같은 그룹 회사들이 동시에 M&A/분할 공시
     → "그룹 차원 재편 진행"

  8. OWNERSHIP_DECISION_SIGNAL  사업 변화 단서 + 임원·주주 변동
     → "대주주 지분 변경이 신사업 결정 동기 추정"

각 패턴 발견 시 cross_signals 테이블에 저장.
점수 ★★★ ~ ★ 차등.

실행:
  python scripts/cross_signals.py --init
  python scripts/cross_signals.py --build-all
  python scripts/cross_signals.py --pattern concentration_shift
  python scripts/cross_signals.py --stats
"""

import io
import re
import sys
import json
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter, defaultdict

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


def _ensure_utf8_io():
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS cross_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern TEXT NOT NULL,           -- CONCENTRATION_SHIFT/SUPPLY_CHAIN_RIPPLE/...
    title TEXT NOT NULL,
    interpretation TEXT,             -- 새로운 해석 (이게 핵심)
    primary_corp_code TEXT,
    primary_corp_name TEXT,
    related_corp_codes TEXT,         -- JSON list
    signal_count INTEGER,            -- 결합된 신호 개수
    severity INTEGER DEFAULT 4,
    metadata TEXT,                   -- JSON 상세
    article_id INTEGER,              -- 기사화된 경우
    status TEXT DEFAULT 'new',
    detected_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cs_pattern  ON cross_signals(pattern);
CREATE INDEX IF NOT EXISTS idx_cs_severity ON cross_signals(severity);
CREATE INDEX IF NOT EXISTS idx_cs_corp     ON cross_signals(primary_corp_code);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def save_signal(conn: sqlite3.Connection, sig: dict) -> int:
    cur = conn.execute("""
        INSERT INTO cross_signals
            (pattern, title, interpretation,
             primary_corp_code, primary_corp_name,
             related_corp_codes, signal_count, severity,
             metadata, detected_at)
        VALUES (?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
    """, [
        sig["pattern"], sig["title"], sig.get("interpretation", ""),
        sig.get("primary_corp_code"), sig.get("primary_corp_name"),
        json.dumps(sig.get("related_corp_codes", []), ensure_ascii=False),
        sig.get("signal_count", 2), sig.get("severity", 4),
        json.dumps(sig.get("metadata", {}), ensure_ascii=False),
    ])
    return cur.lastrowid


# ══════════════════════════════════════════════════════════════════════════════
# 패턴 1: CONCENTRATION_SHIFT
# 매출 의존도 큰 회사(공급망_DEPENDENCE) + 그 거래처의 수시공시
# ══════════════════════════════════════════════════════════════════════════════

def detect_concentration_shift(conn: sqlite3.Connection) -> list[dict]:
    """매출 의존도 큰 회사의 거래처가 최근 큰 공시 발표 시 강력 신호."""
    print("\n[패턴 1: CONCENTRATION_SHIFT] 매출 의존도 + 거래처 공시 교차...")

    # supply_chain_leads 중 DEPENDENCE 단서 (매출 의존도 30%+)
    dep_rows = conn.execute("""
        SELECT * FROM supply_chain_leads WHERE scenario='DEPENDENCE'
    """).fetchall()

    signals = []
    for d in dep_rows:
        try:
            meta = json.loads(d["metadata"] or "{}")
        except Exception:
            meta = {}
        dep_partner = meta.get("dependent_partner", "")
        dep_pct = meta.get("dependence_pct", 0)
        if not dep_partner:
            continue

        # 의존하는 거래처(파트너)와 이름 일치하는 회사의 수시공시 검색
        # partner_name ↔ event_disclosures.corp_name 매칭
        related_disclosures = conn.execute("""
            SELECT * FROM event_disclosures
            WHERE corp_name LIKE ? AND report_type IN ('MA','CAPITAL','GUIDANCE')
            ORDER BY rcept_dt DESC LIMIT 5
        """, [f"%{dep_partner}%"]).fetchall()

        if not related_disclosures:
            continue

        # 강력한 교차 신호 발견
        sev = 5 if dep_pct >= 50 else 4
        signal_count = 1 + len(related_disclosures)

        disc_summary = ", ".join(
            f"{r['report_type']}({(r['rcept_dt'] or '')[:8]})"
            for r in related_disclosures[:3]
        )

        signals.append({
            "pattern": "CONCENTRATION_SHIFT",
            "title": f"{d['primary_corp_name']} 매출 {dep_pct:.0f}% 의존 거래처 {dep_partner}, 최근 큰 공시 발표",
            "interpretation": (
                f"{d['primary_corp_name']}이(가) {dep_partner}에 매출 {dep_pct:.0f}% 집중된 상태에서, "
                f"{dep_partner}이(가) 최근 {disc_summary} 등 {len(related_disclosures)}건 공시. "
                f"거래처 변동이 {d['primary_corp_name']}의 매출 구조에 직접 영향 가능성."
            ),
            "primary_corp_code": d["primary_corp_code"],
            "primary_corp_name": d["primary_corp_name"],
            "related_corp_codes": [d["primary_corp_code"]] if d["primary_corp_code"] else [],
            "signal_count": signal_count,
            "severity": sev,
            "metadata": {
                "dependence_pct": dep_pct,
                "dependent_partner": dep_partner,
                "disclosures": [
                    {"type": r["report_type"], "title": r["report_nm"],
                     "date": r["rcept_dt"], "url": r["raw_url"]}
                    for r in related_disclosures
                ],
            },
        })

    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 패턴 2: SUPPLY_CHAIN_RIPPLE
# 공급망 허브 회사 + 위기 단서
# ══════════════════════════════════════════════════════════════════════════════

def detect_supply_chain_ripple(conn: sqlite3.Connection) -> list[dict]:
    """허브 회사가 위기 단서 발생 시 거래처 충격 전파 경고."""
    print("\n[패턴 2: SUPPLY_CHAIN_RIPPLE] 공급망 허브 + 위기 단서...")

    # supply_chain_leads HUB (큰 회사들) 와 story_leads risk_alert 매칭
    risk_corps = conn.execute("""
        SELECT corp_code, corp_name, COUNT(*) risk_n
        FROM story_leads
        WHERE lead_type IN ('risk_alert','numeric_change')
          AND severity >= 4
        GROUP BY corp_code
    """).fetchall()

    signals = []
    for rc in risk_corps:
        if not rc["corp_code"]:
            continue
        # 이 회사가 다른 상장사의 거래처로 등장하는지
        partners = conn.execute("""
            SELECT DISTINCT corp_code, corp_name, relation_type
            FROM supply_chain
            WHERE partner_name LIKE ? AND corp_code != ?
        """, [f"%{rc['corp_name']}%", rc["corp_code"]]).fetchall()

        if len(partners) < 5:   # 5개사+ 거래처가 있을 때만 의미
            continue

        cust_n = sum(1 for p in partners if p["relation_type"] == "customer")
        sev = 5 if len(partners) >= 20 else 4

        signals.append({
            "pattern": "SUPPLY_CHAIN_RIPPLE",
            "title": f"{rc['corp_name']} 위기 시 거래처 {len(partners)}곳 충격 전파권",
            "interpretation": (
                f"{rc['corp_name']}에서 위기·수치급변 단서 {rc['risk_n']}건 발생. "
                f"동 회사를 거래처로 둔 상장사 {len(partners)}곳(고객사 {cust_n}곳 포함) 영향권. "
                f"공급망 충격 전파 모니터링 필요."
            ),
            "primary_corp_code": rc["corp_code"],
            "primary_corp_name": rc["corp_name"],
            "related_corp_codes": [p["corp_code"] for p in partners[:30] if p["corp_code"]],
            "signal_count": 1 + len(partners),
            "severity": sev,
            "metadata": {
                "risk_origin_count": rc["risk_n"],
                "affected_partners": [
                    {"name": p["corp_name"], "rel": p["relation_type"]}
                    for p in partners[:30]
                ],
                "customer_partners": cust_n,
            },
        })
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 패턴 3: NEW_BUSINESS_FUNDING
# 신사업 진출 단서 + 같은 시기 자본 조달
# ══════════════════════════════════════════════════════════════════════════════

def detect_new_business_funding(conn: sqlite3.Connection) -> list[dict]:
    """신사업 단서가 있는 회사 + 같은 시기 유상증자/사채 발행."""
    print("\n[패턴 3: NEW_BUSINESS_FUNDING] 신사업 + 자본 조달...")

    # NEW 패턴 단서 (lead_novelty)
    new_leads = conn.execute("""
        SELECT sl.id, sl.corp_code, sl.corp_name, sl.title, sl.lead_type
        FROM story_leads sl
        JOIN lead_novelty ln ON sl.id = ln.lead_id
        WHERE ln.pattern = 'NEW'
          AND sl.lead_type IN ('strategy_change','numeric_change')
          AND sl.severity >= 4
    """).fetchall()

    signals = []
    for lead in new_leads:
        if not lead["corp_code"]:
            continue
        # 같은 회사의 CAPITAL 공시 (유상증자, 사채 등)
        capital_disc = conn.execute("""
            SELECT * FROM event_disclosures
            WHERE corp_code = ? AND report_type = 'CAPITAL'
            ORDER BY rcept_dt DESC LIMIT 5
        """, [lead["corp_code"]]).fetchall()

        if not capital_disc:
            continue

        sev = 5 if len(capital_disc) >= 2 else 4
        signals.append({
            "pattern": "NEW_BUSINESS_FUNDING",
            "title": f"{lead['corp_name']} 신사업 진출 + 자본 조달 {len(capital_disc)}건 동시 발생",
            "interpretation": (
                f"{lead['corp_name']}의 신규 사업 단서('{lead['title']}')와 함께 "
                f"같은 시기 자본 변동 공시 {len(capital_disc)}건 확인. "
                f"신사업 본격 추진을 위한 자금 조달 가능성. 결정 진정성 신호."
            ),
            "primary_corp_code": lead["corp_code"],
            "primary_corp_name": lead["corp_name"],
            "related_corp_codes": [lead["corp_code"]],
            "signal_count": 1 + len(capital_disc),
            "severity": sev,
            "metadata": {
                "lead_id": lead["id"],
                "lead_title": lead["title"],
                "lead_type": lead["lead_type"],
                "capital_disclosures": [
                    {"title": r["report_nm"], "date": r["rcept_dt"], "url": r["raw_url"]}
                    for r in capital_disc
                ],
            },
        })
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 패턴 4: INDUSTRY_WAVE
# 같은 산업 다수 회사 + 동시 변화/공시
# ══════════════════════════════════════════════════════════════════════════════

def detect_industry_wave(conn: sqlite3.Connection) -> list[dict]:
    """같은 산업 5개사+가 동시에 같은 변화 + 수시공시 빈도 증가."""
    print("\n[패턴 4: INDUSTRY_WAVE] 산업 트렌드 + 동시 공시...")

    # lead_change_class에서 같은 sector + 같은 change_type 5개+ 모음
    rows = conn.execute("""
        SELECT lcc.change_type, c.sector, COUNT(*) corp_n,
               GROUP_CONCAT(DISTINCT sl.corp_code) corp_codes,
               GROUP_CONCAT(DISTINCT sl.corp_name) corp_names
        FROM lead_change_class lcc
        JOIN story_leads sl ON lcc.lead_id = sl.id
        JOIN companies c    ON sl.corp_code = c.corp_code
        WHERE c.sector IS NOT NULL
          AND lcc.change_type != 'UNKNOWN'
          AND lcc.industry_or_individual = 'INDUSTRY'
        GROUP BY lcc.change_type, c.sector
        HAVING corp_n >= 4
        ORDER BY corp_n DESC LIMIT 30
    """).fetchall()

    signals = []
    for r in rows:
        sector = r["sector"]
        codes = (r["corp_codes"] or "").split(",")
        # 그 산업 회사들의 같은 시기 수시공시 (M&A·CAPITAL)
        if not codes:
            continue
        placeholders = ",".join("?" * len(codes))
        disc_count = conn.execute(f"""
            SELECT COUNT(*) FROM event_disclosures
            WHERE corp_code IN ({placeholders})
              AND report_type IN ('MA','CAPITAL')
              AND rcept_dt >= ?
        """, codes + [(datetime.now() - timedelta(days=90)).strftime("%Y%m%d")]).fetchone()[0]

        if disc_count < 2:   # 산업 차원 공시 활발해야 의미
            continue

        names = (r["corp_names"] or "").split(",")[:8]
        sev = 5 if r["corp_n"] >= 8 else 4

        signals.append({
            "pattern": "INDUSTRY_WAVE",
            "title": f"{sector} 업계 {r['corp_n']}곳 동시 {r['change_type']} + 공시 {disc_count}건",
            "interpretation": (
                f"{sector} 업종 상장사 {r['corp_n']}곳이 동시에 '{r['change_type']}' 패턴 변화. "
                f"같은 시기 자본·M&A 공시 {disc_count}건 발생. 산업 차원 트렌드 가능성. "
                f"개별 기업 행보가 아닌 업계 공통 흐름으로 분석 가치."
            ),
            "primary_corp_name": sector,
            "related_corp_codes": codes[:30],
            "signal_count": r["corp_n"] + disc_count,
            "severity": sev,
            "metadata": {
                "sector": sector,
                "change_type": r["change_type"],
                "company_count": r["corp_n"],
                "company_names": names,
                "disclosure_count_90d": disc_count,
            },
        })
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 패턴 5: HIDDEN_CHANGE
# 정보격차 큰 회사 + 활발한 공시 (잘 안 알려진 곳에 변화 시작)
# ══════════════════════════════════════════════════════════════════════════════

def detect_hidden_change(conn: sqlite3.Connection) -> list[dict]:
    """정보격차 🚨 등급(90+) 회사가 최근 활발한 공시 → '주목 시점' 신호."""
    print("\n[패턴 5: HIDDEN_CHANGE] 소외 기업 + 활발한 공시...")

    rows = conn.execute("""
        SELECT cig.corp_code, cig.corp_name, cig.score_total as info_gap,
               COUNT(ed.rcept_no) disc_n,
               GROUP_CONCAT(DISTINCT ed.report_type) types
        FROM company_info_gap cig
        JOIN event_disclosures ed ON cig.corp_code = ed.corp_code
        WHERE cig.score_total >= 90
          AND ed.rcept_dt >= ?
        GROUP BY cig.corp_code
        HAVING disc_n >= 3
        ORDER BY disc_n DESC LIMIT 50
    """, [(datetime.now() - timedelta(days=60)).strftime("%Y%m%d")]).fetchall()

    signals = []
    for r in rows:
        types = (r["types"] or "").split(",")
        # 의미 있는 유형이 있어야 (M&A, CAPITAL은 강한 신호)
        has_strong = any(t in types for t in ["MA", "CAPITAL", "GUIDANCE"])
        if not has_strong:
            continue

        sev = 5 if "MA" in types else 4

        signals.append({
            "pattern": "HIDDEN_CHANGE",
            "title": f"{r['corp_name']} 알려지지 않은 회사인데 최근 {r['disc_n']}건 공시",
            "interpretation": (
                f"{r['corp_name']}은(는) 보도·기사 거의 없는 정보격차 큰(🚨 {r['info_gap']}점) 회사. "
                f"그러나 최근 60일 내 수시공시 {r['disc_n']}건 발생. "
                f"유형: {', '.join(set(types))}. "
                f"시장에서 아직 주목받지 않은 변화 시작 — 이제 주목할 시점."
            ),
            "primary_corp_code": r["corp_code"],
            "primary_corp_name": r["corp_name"],
            "related_corp_codes": [r["corp_code"]],
            "signal_count": r["disc_n"],
            "severity": sev,
            "metadata": {
                "info_gap_score": r["info_gap"],
                "disclosure_count_60d": r["disc_n"],
                "disclosure_types": list(set(types)),
            },
        })
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 패턴 6: GLOBAL_RISK_TRANSFER
# 글로벌 거래처 회사들 동시 변화
# ══════════════════════════════════════════════════════════════════════════════

GLOBAL_KEY_PARTNERS = ["Apple", "TSMC", "Bloomberg", "Boeing", "Tesla",
                      "Toyota", "ARAMCO", "Samsung Electronics",
                      "Intel", "Nvidia", "Foxconn"]


def detect_global_risk_transfer(conn: sqlite3.Connection) -> list[dict]:
    """글로벌 거래처 회사들에 동시 위기/변화 발생."""
    print("\n[패턴 6: GLOBAL_RISK_TRANSFER] 글로벌 거래처 충격 전파...")

    signals = []
    for gp in GLOBAL_KEY_PARTNERS:
        # gp 거래 한국 상장사
        kr_corps = conn.execute("""
            SELECT DISTINCT corp_code, corp_name
            FROM supply_chain WHERE partner_name LIKE ?
        """, [f"%{gp}%"]).fetchall()
        if len(kr_corps) < 5:
            continue

        codes = [c["corp_code"] for c in kr_corps if c["corp_code"]]
        if not codes:
            continue

        # 이 회사들 중 risk_alert 단서 또는 numeric_change severity 4+ 발생
        placeholders = ",".join("?" * len(codes))
        risk_n = conn.execute(f"""
            SELECT COUNT(DISTINCT corp_code) FROM story_leads
            WHERE corp_code IN ({placeholders})
              AND lead_type IN ('risk_alert','numeric_change','market_shift')
              AND severity >= 4
        """, codes).fetchone()[0]

        if risk_n < 2:   # 2개사+ 동시 변화여야 의미
            continue

        sev = 5 if risk_n >= 4 else 4
        signals.append({
            "pattern": "GLOBAL_RISK_TRANSFER",
            "title": f"{gp} 거래 한국 {len(kr_corps)}곳 중 {risk_n}곳 동시 변화 신호",
            "interpretation": (
                f"글로벌 기업 {gp}을(를) 거래처로 둔 한국 상장사 {len(kr_corps)}곳 중 "
                f"{risk_n}곳에서 위기·시장변화 단서가 동시 발견됨. "
                f"{gp} 차원의 글로벌 변화가 한국 가치사슬에 전이된 가능성. "
                f"환율·관세·수요 변화 종합 점검 필요."
            ),
            "primary_corp_name": gp,
            "related_corp_codes": codes[:30],
            "signal_count": risk_n,
            "severity": sev,
            "metadata": {
                "global_partner": gp,
                "korean_count": len(kr_corps),
                "with_risk_signal": risk_n,
            },
        })
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 패턴 7: GROUP_RESTRUCTURING
# 같은 그룹 회사들의 동시 M&A/분할 공시
# ══════════════════════════════════════════════════════════════════════════════

GROUP_KEYWORDS = {
    "삼성":   ["삼성"],
    "현대차": ["현대", "기아", "Hyundai", "Kia"],
    "SK":    ["SK", "에스케이"],
    "LG":    ["LG", "엘지"],
    "롯데":   ["롯데"],
    "POSCO": ["POSCO", "포스코"],
    "한화":   ["한화"],
    "GS":    ["GS"],
}


def detect_group_restructuring(conn: sqlite3.Connection) -> list[dict]:
    """그룹 계열사들이 같은 시기 M&A/분할 공시."""
    print("\n[패턴 7: GROUP_RESTRUCTURING] 그룹 동시 재편...")

    signals = []
    for group, keys in GROUP_KEYWORDS.items():
        clauses = " OR ".join(["corp_name LIKE ?"] * len(keys))
        params = [f"{k}%" for k in keys]   # 시작 매칭 (오탐 줄임)

        # 그룹 회사들의 M&A/분할 공시
        ma_disc = conn.execute(f"""
            SELECT corp_code, corp_name, report_nm, rcept_dt
            FROM event_disclosures
            WHERE ({clauses})
              AND report_type = 'MA'
              AND rcept_dt >= ?
        """, params + [(datetime.now() - timedelta(days=180)).strftime("%Y%m%d")]).fetchall()

        if len(ma_disc) < 2:
            continue

        unique_corps = {r["corp_code"] for r in ma_disc}
        if len(unique_corps) < 2:
            continue

        sev = 5 if len(unique_corps) >= 3 else 4
        signals.append({
            "pattern": "GROUP_RESTRUCTURING",
            "title": f"{group}그룹 계열사 {len(unique_corps)}곳 동시 M&A·분할",
            "interpretation": (
                f"{group}그룹 계열사 {len(unique_corps)}곳에서 최근 6개월 내 "
                f"M&A·분할 공시 {len(ma_disc)}건 동시 발생. "
                f"그룹 차원의 사업 재편이 진행 중일 가능성. "
                f"개별 공시가 아닌 그룹 전략 변화 신호로 해석 가치."
            ),
            "primary_corp_name": f"{group}그룹",
            "related_corp_codes": list(unique_corps),
            "signal_count": len(ma_disc),
            "severity": sev,
            "metadata": {
                "group": group,
                "ma_disclosures": [
                    {"name": r["corp_name"], "title": r["report_nm"],
                     "date": r["rcept_dt"]}
                    for r in ma_disc[:10]
                ],
            },
        })
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 패턴 8: OWNERSHIP_DECISION_SIGNAL
# 사업 변화 단서 + 임원·주주 변동
# ══════════════════════════════════════════════════════════════════════════════

def detect_ownership_decision(conn: sqlite3.Connection) -> list[dict]:
    """전략 변화 단서 + 같은 시기 임원·주주 변동 = 결정 동기 추정."""
    print("\n[패턴 8: OWNERSHIP_DECISION_SIGNAL] 사업 변화 + 지분 변동...")

    # strategy_change 또는 NEW 단서
    leads = conn.execute("""
        SELECT sl.id, sl.corp_code, sl.corp_name, sl.title
        FROM story_leads sl
        JOIN lead_novelty ln ON sl.id = ln.lead_id
        WHERE sl.lead_type = 'strategy_change'
          AND sl.severity >= 4
          AND ln.pattern IN ('NEW','REMOVED','EXPANDING')
    """).fetchall()

    signals = []
    for lead in leads:
        if not lead["corp_code"]:
            continue
        # 같은 회사의 INSIDER 공시
        ins_disc = conn.execute("""
            SELECT * FROM event_disclosures
            WHERE corp_code = ? AND report_type = 'INSIDER'
            ORDER BY rcept_dt DESC LIMIT 5
        """, [lead["corp_code"]]).fetchall()

        if len(ins_disc) < 2:
            continue

        signals.append({
            "pattern": "OWNERSHIP_DECISION_SIGNAL",
            "title": f"{lead['corp_name']} 사업 변화 + 임원·주주 변동 {len(ins_disc)}건 동시",
            "interpretation": (
                f"{lead['corp_name']} 전략 변화 단서('{lead['title']}')와 같은 시기 "
                f"임원·주주 관련 공시 {len(ins_disc)}건. "
                f"지배구조 변화가 사업 전략 결정 동기일 가능성. "
                f"누가 결정했는가(WHO) 추적 가치 있음."
            ),
            "primary_corp_code": lead["corp_code"],
            "primary_corp_name": lead["corp_name"],
            "related_corp_codes": [lead["corp_code"]],
            "signal_count": 1 + len(ins_disc),
            "severity": 4,
            "metadata": {
                "lead_id": lead["id"],
                "lead_title": lead["title"],
                "insider_count": len(ins_disc),
                "insider_recent": [
                    {"title": r["report_nm"], "date": r["rcept_dt"]}
                    for r in ins_disc[:5]
                ],
            },
        })
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 통합
# ══════════════════════════════════════════════════════════════════════════════

PATTERN_BUILDERS = {
    "concentration_shift":      detect_concentration_shift,
    "supply_chain_ripple":      detect_supply_chain_ripple,
    "new_business_funding":     detect_new_business_funding,
    "industry_wave":            detect_industry_wave,
    "hidden_change":            detect_hidden_change,
    "global_risk_transfer":     detect_global_risk_transfer,
    "group_restructuring":      detect_group_restructuring,
    "ownership_decision":       detect_ownership_decision,
}


def build_all(conn: sqlite3.Connection, only: str = None) -> dict:
    if not only:
        conn.execute("DELETE FROM cross_signals")
        conn.commit()

    counts = {}
    for name, builder in PATTERN_BUILDERS.items():
        if only and name != only:
            continue
        try:
            sigs = builder(conn)
            for sig in sigs:
                save_signal(conn, sig)
            counts[name] = len(sigs)
            print(f"  ✓ {name.upper():<25} {len(sigs):>3}건")
            conn.commit()
        except Exception as e:
            print(f"  ⚠ {name}: {type(e).__name__}: {e}")
    return counts


def print_stats(conn: sqlite3.Connection) -> None:
    print("=" * 70)
    print("  교차 신호 시스템 (Phase Cross)")
    print("=" * 70)
    total = conn.execute("SELECT COUNT(*) FROM cross_signals").fetchone()[0]
    if total == 0:
        print("  (아직 빌드 안 됨)"); return
    print(f"\n  전체: {total}건\n  [패턴별]")
    for r in conn.execute("""
        SELECT pattern, COUNT(*) cnt, AVG(severity) avg_s
        FROM cross_signals GROUP BY pattern ORDER BY cnt DESC
    """):
        icons = {
            "CONCENTRATION_SHIFT":  "🎯",
            "SUPPLY_CHAIN_RIPPLE":  "💥",
            "NEW_BUSINESS_FUNDING": "💰",
            "INDUSTRY_WAVE":        "🌊",
            "HIDDEN_CHANGE":        "🔍",
            "GLOBAL_RISK_TRANSFER": "🌍",
            "GROUP_RESTRUCTURING":  "🏢",
            "OWNERSHIP_DECISION_SIGNAL": "👤",
        }
        print(f"    {icons.get(r['pattern'],'⚪')} {r['pattern']:<28} {r['cnt']:>3}건 (sev {r['avg_s']:.1f})")

    print("\n  [severity 5 — 최우선 출고]")
    for r in conn.execute("""
        SELECT pattern, title, interpretation FROM cross_signals
        WHERE severity = 5 ORDER BY id DESC LIMIT 12
    """):
        print(f"\n  [{r['pattern']}]")
        print(f"    {r['title']}")
        if r['interpretation']:
            print(f"    → {r['interpretation'][:120]}")


def main():
    p = argparse.ArgumentParser(description="교차 신호 시스템 (Phase Cross)")
    p.add_argument("--init", action="store_true")
    p.add_argument("--build-all", action="store_true")
    p.add_argument("--pattern", choices=list(PATTERN_BUILDERS.keys()))
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()

    conn = get_conn()
    ensure_schema(conn)

    if args.init:
        print("✓ cross_signals 스키마 생성"); return
    if args.stats:
        print_stats(conn); return
    if args.build_all or args.pattern:
        counts = build_all(conn, only=args.pattern)
        print(f"\n[완료] {counts}")
        print()
        print_stats(conn)
        return

    print(__doc__)


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
