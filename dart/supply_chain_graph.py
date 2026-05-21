"""
dart/supply_chain_graph.py
──────────────────────────
공급망 그래프 분석 엔진

기능:
  - 기업 중심 그래프 (1·2단계 탐색)
  - 허브 역방향 조회 (파트너명 → 언급 기업들)
  - 충격 전파 분석 (upstream / downstream)
  - 공급망 변화 감지 (보고서 기간별 비교)
  - 연결 경로 탐색 (BFS)
  - 섹터별 허브 순위
  - 교집합 종목 발굴
"""

import re
import sqlite3
from collections import deque, defaultdict
from pathlib import Path

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"


def _get_db() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


# ══════════════════════════════════════════════════════════════════
#  내부 유틸
# ══════════════════════════════════════════════════════════════════

def _corp_name_to_code(db: sqlite3.Connection, corp_name: str) -> str | None:
    """partner_name 문자열 → corp_code 매핑 (정확 or 유사)"""
    row = db.execute(
        "SELECT corp_code FROM supply_chain WHERE corp_name = ? LIMIT 1",
        [corp_name]
    ).fetchone()
    if row:
        return row["corp_code"]
    # 부분 일치 시도
    row = db.execute(
        "SELECT corp_code FROM supply_chain WHERE corp_name LIKE ? LIMIT 1",
        [f"%{corp_name}%"]
    ).fetchone()
    return row["corp_code"] if row else None


def _corp_code_to_name(db: sqlite3.Connection, corp_code: str) -> str:
    row = db.execute(
        "SELECT corp_name FROM supply_chain WHERE corp_code = ? LIMIT 1",
        [corp_code]
    ).fetchone()
    return row["corp_name"] if row else corp_code


def _partner_to_corp_code(db: sqlite3.Connection, partner_name: str) -> str | None:
    """
    partner_name → corp_code 역방향 매핑.

    우선순위:
      1. partner_mapping 테이블 (정확한 매핑 사전)
      2. supply_chain.corp_name 정확 일치
      3. supply_chain.corp_name 접두/접미 부분 일치
    """
    # 1. partner_mapping 테이블 우선 조회
    pm = db.execute(
        "SELECT corp_code FROM partner_mapping WHERE partner_name = ?",
        [partner_name]
    ).fetchone()
    if pm is not None:
        return pm["corp_code"]  # None이면 비상장 — 그대로 None 반환

    # 2. supply_chain.corp_name 정확 일치
    row = db.execute(
        "SELECT corp_code FROM supply_chain WHERE corp_name = ? LIMIT 1",
        [partner_name]
    ).fetchone()
    if row:
        return row["corp_code"]

    # 3. 표기 차이 허용 (접두/접미)
    row = db.execute(
        """SELECT corp_code FROM supply_chain
           WHERE corp_name LIKE ? OR corp_name LIKE ?
           LIMIT 1""",
        [f"{partner_name}%", f"%{partner_name}"]
    ).fetchone()
    return row["corp_code"] if row else None


# ══════════════════════════════════════════════════════════════════
#  1. 기업 중심 그래프
# ══════════════════════════════════════════════════════════════════

def get_company_graph(
    corp_code: str,
    depth: int = 1,
    direction: str = "both",   # upstream | downstream | both
    relation: str = "all",     # all | customer | supplier | partner | competitor
) -> dict:
    """
    기업 중심 공급망 그래프.

    depth=1: 직접 연결 기업
    depth=2: 직접 연결 기업의 연결 기업까지 (단, DB에 등록된 기업만)

    Returns:
        { center, nodes, edges, summary }
    """
    db = _get_db()

    # 중심 기업 확인
    center_row = db.execute(
        "SELECT DISTINCT corp_code, corp_name FROM supply_chain WHERE corp_code = ? LIMIT 1",
        [corp_code]
    ).fetchone()
    if not center_row:
        db.close()
        return {"error": f"corp_code '{corp_code}' 의 공급망 데이터 없음"}

    center_name = center_row["corp_name"]

    # 관계 필터
    rel_filter = "" if relation == "all" else f" AND relation_type = '{relation}'"

    # 방향 필터 → relation_type으로 구분
    # upstream(공급 방향):   이 기업의 supplier
    # downstream(고객 방향): 이 기업의 customer
    if direction == "upstream":
        dir_filter = " AND relation_type = 'supplier'"
    elif direction == "downstream":
        dir_filter = " AND relation_type = 'customer'"
    else:
        dir_filter = ""

    filter_sql = rel_filter + dir_filter

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    visited_codes: set[str] = {corp_code}

    # ── 중심 노드 ──
    nodes[corp_code] = {
        "id": corp_code,
        "name": center_name,
        "type": "center",
        "depth": 0,
    }

    # ── 1단계 탐색 ──
    rows_1 = db.execute(
        f"""SELECT relation_type, partner_name, context, COUNT(*) as weight
            FROM supply_chain
            WHERE corp_code = ? {filter_sql}
            GROUP BY relation_type, partner_name
            ORDER BY weight DESC""",
        [corp_code]
    ).fetchall()

    partner_queue: list[tuple[str, str]] = []  # (partner_name, depth)

    for row in rows_1:
        p_name = row["partner_name"]
        p_code = _partner_to_corp_code(db, p_name)
        node_id = p_code if p_code else f"p:{p_name}"

        if node_id not in nodes:
            nodes[node_id] = {
                "id": node_id,
                "name": p_name,
                "type": row["relation_type"],
                "corp_code": p_code,
                "is_listed": p_code is not None,
                "depth": 1,
            }

        edges.append({
            "source": corp_code,
            "target": node_id,
            "relation": row["relation_type"],
            "context": row["context"],
            "weight": row["weight"],
            "depth": 1,
        })

        if p_code and p_code not in visited_codes:
            visited_codes.add(p_code)
            partner_queue.append((p_name, p_code))

    # ── 2단계 탐색 (DB에 등록된 기업만) ──
    if depth >= 2:
        for p_name, p_code in partner_queue:
            rows_2 = db.execute(
                f"""SELECT relation_type, partner_name, context, COUNT(*) as weight
                    FROM supply_chain
                    WHERE corp_code = ? {filter_sql}
                    GROUP BY relation_type, partner_name
                    ORDER BY weight DESC
                    LIMIT 20""",  # 2단계는 주요 관계만 (과부하 방지)
                [p_code]
            ).fetchall()

            for row in rows_2:
                p2_name = row["partner_name"]
                p2_code = _partner_to_corp_code(db, p2_name)
                node_id = p2_code if p2_code else f"p:{p2_name}"

                if node_id not in nodes:
                    nodes[node_id] = {
                        "id": node_id,
                        "name": p2_name,
                        "type": row["relation_type"],
                        "corp_code": p2_code,
                        "is_listed": p2_code is not None,
                        "depth": 2,
                    }

                edges.append({
                    "source": p_code,
                    "target": node_id,
                    "relation": row["relation_type"],
                    "context": row["context"],
                    "weight": row["weight"],
                    "depth": 2,
                })

    # ── 요약 ──
    depth1_edges = [e for e in edges if e["depth"] == 1]
    summary = {
        "total_nodes":  len(nodes),
        "total_edges":  len(edges),
        "depth1_count": len(depth1_edges),
        "suppliers":    sum(1 for e in depth1_edges if e["relation"] == "supplier"),
        "customers":    sum(1 for e in depth1_edges if e["relation"] == "customer"),
        "partners":     sum(1 for e in depth1_edges if e["relation"] == "partner"),
        "competitors":  sum(1 for e in depth1_edges if e["relation"] == "competitor"),
        "listed_partners": sum(1 for n in nodes.values() if n.get("is_listed") and n["type"] != "center"),
    }

    db.close()
    return {
        "center": {"corp_code": corp_code, "corp_name": center_name},
        "nodes": list(nodes.values()),
        "edges": edges,
        "summary": summary,
    }


# ══════════════════════════════════════════════════════════════════
#  2. 허브 역방향 조회 (파트너명 기준)
# ══════════════════════════════════════════════════════════════════

def get_hub_detail(partner_name: str, relation: str = "all") -> dict:
    """
    특정 파트너명을 언급한 기업들 전체 조회.
    비상장 기업도 포함 (partner_name 문자열 그대로).

    Returns:
        { partner_name, corp_code, total, by_relation, reporters }
    """
    db = _get_db()

    rel_filter = "" if relation == "all" else f" AND relation_type = '{relation}'"

    rows = db.execute(
        f"""SELECT sc.corp_code, sc.corp_name, sc.relation_type,
                   sc.context, sc.source_report, sc.analyzed_at
            FROM supply_chain sc
            WHERE sc.partner_name = ? {rel_filter}
            ORDER BY sc.relation_type, sc.corp_name""",
        [partner_name]
    ).fetchall()

    if not rows:
        # 부분 일치 재시도
        rows = db.execute(
            f"""SELECT sc.corp_code, sc.corp_name, sc.relation_type,
                       sc.context, sc.source_report, sc.analyzed_at
                FROM supply_chain sc
                WHERE sc.partner_name LIKE ? {rel_filter}
                ORDER BY sc.relation_type, sc.corp_name""",
            [f"%{partner_name}%"]
        ).fetchall()

    by_relation: dict[str, list] = defaultdict(list)
    for row in rows:
        by_relation[row["relation_type"]].append({
            "corp_code": row["corp_code"],
            "corp_name": row["corp_name"],
            "context":   row["context"],
            "report":    row["source_report"],
        })

    # 파트너가 우리 DB에 보고 기업으로도 있는지 확인
    own_code = _partner_to_corp_code(db, partner_name)

    db.close()
    return {
        "partner_name": partner_name,
        "own_corp_code": own_code,   # DB 내 자체 corp_code (있으면 상장사)
        "total": len(rows),
        "by_relation": dict(by_relation),
        "summary": {
            "as_customer": len(by_relation.get("customer", [])),
            "as_supplier": len(by_relation.get("supplier", [])),
            "as_partner":  len(by_relation.get("partner",  [])),
            "as_competitor": len(by_relation.get("competitor", [])),
        },
    }


# ══════════════════════════════════════════════════════════════════
#  3. 공급망 충격 전파 분석
# ══════════════════════════════════════════════════════════════════

def get_impact_analysis(corp_code: str) -> dict:
    """
    특정 기업이 이슈(실적 쇼크, 공급 중단 등) 발생 시
    영향을 받을 수 있는 기업들을 분류.

    upstream_impact:
      이 기업의 고객사들 → "이 기업 제품을 사는 곳들이 타격"
    downstream_impact:
      이 기업의 공급사들 → "이 기업에 납품하는 곳들이 타격"
    """
    db = _get_db()

    name_row = db.execute(
        "SELECT DISTINCT corp_name FROM supply_chain WHERE corp_code = ? LIMIT 1",
        [corp_code]
    ).fetchone()
    if not name_row:
        db.close()
        return {"error": f"corp_code '{corp_code}' 없음"}

    corp_name = name_row["corp_name"]

    # 이 기업의 직접 관계
    direct = db.execute(
        """SELECT relation_type, partner_name, context
           FROM supply_chain WHERE corp_code = ?
           ORDER BY relation_type""",
        [corp_code]
    ).fetchall()

    customers  = [r for r in direct if r["relation_type"] == "customer"]
    suppliers  = [r for r in direct if r["relation_type"] == "supplier"]
    partners   = [r for r in direct if r["relation_type"] == "partner"]
    competitors = [r for r in direct if r["relation_type"] == "competitor"]

    # 역방향: 이 기업을 파트너로 언급하는 기업들
    reverse = db.execute(
        """SELECT corp_code, corp_name, relation_type, context
           FROM supply_chain WHERE partner_name = ?
           ORDER BY relation_type""",
        [corp_name]
    ).fetchall()

    # 이 기업을 공급사로 의존하는 기업들 (가장 직접적 타격)
    depends_on_this = [r for r in reverse if r["relation_type"] == "supplier"]
    # 이 기업을 고객으로 가진 기업들 (매출 타격)
    sells_to_this   = [r for r in reverse if r["relation_type"] == "customer"]

    def _fmt(rows):
        return [{"partner_name": r["partner_name"] if "partner_name" in r.keys()
                                 else r["corp_name"],
                 "corp_code": r["corp_code"] if "corp_code" in r.keys() else None,
                 "context": r["context"]} for r in rows]

    db.close()
    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        # 이 기업 이슈 발생 시:
        "impact": {
            # 매출 타격: 이 기업이 팔던 고객들
            "customer_impact": {
                "desc": f"{corp_name}의 제품·서비스를 구매하던 기업들 → 조달 차질 가능",
                "count": len(customers),
                "companies": _fmt(customers),
            },
            # 원재료 타격: 이 기업에 납품하던 공급사들
            "supplier_impact": {
                "desc": f"{corp_name}에 납품하던 공급사들 → 매출 감소 가능",
                "count": len(suppliers),
                "companies": _fmt(suppliers),
            },
            # 이 기업을 공급사로 의존하던 기업들 (가장 직접적)
            "dependent_companies": {
                "desc": f"다른 기업들이 {corp_name}을 supplier로 의존 → 가장 직접적 충격",
                "count": len(depends_on_this),
                "companies": [{"corp_code": r["corp_code"], "corp_name": r["corp_name"],
                               "context": r["context"]} for r in depends_on_this],
            },
            # 이 기업을 고객으로 의존하던 기업들
            "revenue_exposed": {
                "desc": f"{corp_name}를 주요 고객으로 보유한 기업들 → 매출 충격",
                "count": len(sells_to_this),
                "companies": [{"corp_code": r["corp_code"], "corp_name": r["corp_name"],
                               "context": r["context"]} for r in sells_to_this],
            },
        },
        "partners": _fmt(partners),
        "competitors": _fmt(competitors),
    }


# ══════════════════════════════════════════════════════════════════
#  4. 두 기업 간 연결 경로 탐색 (BFS)
# ══════════════════════════════════════════════════════════════════

def find_path(from_code: str, to_code: str, max_depth: int = 3) -> dict:
    """
    두 기업 간 공급망 연결 경로를 BFS로 탐색.
    partner_name ↔ corp_name 매핑으로 간선 연결.

    Returns:
        { found, path, hops, message }
    """
    db = _get_db()

    def _neighbors(code: str) -> list[tuple[str, str, str, str]]:
        """(neighbor_code, neighbor_name, relation_type, context) 반환"""
        rows = db.execute(
            """SELECT partner_name, relation_type, context
               FROM supply_chain WHERE corp_code = ?""",
            [code]
        ).fetchall()
        result = []
        for r in rows:
            neighbor_code = _partner_to_corp_code(db, r["partner_name"])
            if neighbor_code:
                result.append((neighbor_code, r["partner_name"], r["relation_type"], r["context"]))
        return result

    from_name = _corp_code_to_name(db, from_code)
    to_name   = _corp_code_to_name(db, to_code)

    if from_code == to_code:
        db.close()
        return {"found": False, "message": "출발지와 목적지가 동일"}

    # BFS
    queue   = deque([(from_code, [{"code": from_code, "name": from_name, "via_relation": None, "context": None}])])
    visited = {from_code}

    while queue:
        current_code, path = queue.popleft()
        if len(path) > max_depth + 1:
            break

        for n_code, n_name, rel, ctx in _neighbors(current_code):
            new_path = path + [{"code": n_code, "name": n_name, "via_relation": rel, "context": ctx}]
            if n_code == to_code:
                db.close()
                return {
                    "found": True,
                    "hops": len(new_path) - 1,
                    "path": new_path,
                    "message": f"{from_name} → {to_name} {len(new_path)-1}단계 연결",
                }
            if n_code not in visited:
                visited.add(n_code)
                queue.append((n_code, new_path))

    db.close()
    return {
        "found": False,
        "hops": None,
        "path": [],
        "message": f"{max_depth}단계 이내 연결 없음",
    }


# ══════════════════════════════════════════════════════════════════
#  5. 섹터별 허브 순위
# ══════════════════════════════════════════════════════════════════

SECTORS = {
    "반도체·전자": ["반도체", "전자", "디스플레이", "LED", "PCB", "웨이퍼"],
    "자동차·부품": ["자동차", "모비스", "모터", "타이어", "차량"],
    "바이오·제약": ["바이오", "제약", "의약", "헬스케어", "의료기기", "진단"],
    "IT·소프트웨어": ["소프트웨어", "시스템", "솔루션", "정보기술", "데이터", "플랫폼"],
    "통신": ["통신", "텔레콤", "네트워크", "방송"],
    "철강·소재": ["철강", "스틸", "소재", "금속", "알루미늄"],
    "화학·에너지": ["화학", "에너지", "석유", "배터리", "이차전지", "태양광"],
    "건설·엔지니어링": ["건설", "건축", "시공", "엔지니어링"],
    "유통·물류": ["유통", "물류", "마트", "쇼핑", "편의점"],
    "식품·음료": ["식품", "음료", "제과", "농업", "축산"],
    "게임·엔터": ["게임", "엔터테인먼트", "미디어", "콘텐츠"],
}


def get_hub_rankings(sector: str = "all", limit: int = 20) -> dict:
    """
    가장 많이 언급된 파트너(허브) 순위.
    sector 필터 가능.
    """
    db = _get_db()

    if sector != "all" and sector in SECTORS:
        kws = SECTORS[sector]
        corp_cond = " OR ".join([f"corp_name LIKE '%{kw}%'" for kw in kws])
        where = f"WHERE ({corp_cond})"
    else:
        where = ""
        sector = "전체"

    rows = db.execute(
        f"""SELECT partner_name,
                   COUNT(*) as total_mentions,
                   COUNT(DISTINCT corp_code) as reporter_count,
                   SUM(CASE WHEN relation_type='customer'   THEN 1 ELSE 0 END) as as_customer,
                   SUM(CASE WHEN relation_type='supplier'   THEN 1 ELSE 0 END) as as_supplier,
                   SUM(CASE WHEN relation_type='partner'    THEN 1 ELSE 0 END) as as_partner,
                   SUM(CASE WHEN relation_type='competitor' THEN 1 ELSE 0 END) as as_competitor
            FROM supply_chain
            {where}
            GROUP BY partner_name
            ORDER BY total_mentions DESC
            LIMIT ?""",
        [limit]
    ).fetchall()

    db.close()
    return {
        "sector": sector,
        "limit": limit,
        "hubs": [dict(r) for r in rows],
    }


# ══════════════════════════════════════════════════════════════════
#  6. 공급망 변화 감지 (보고서 기간별 비교)
# ══════════════════════════════════════════════════════════════════

_RCEPT_RE = re.compile(r'^\d{14}$')   # DART rcept_no 형식 감지

def _rcept_sort_key(rcept_no: str) -> str:
    """rcept_no를 날짜순 정렬 키로 변환. 숫자 14자리 → 그대로. 아니면 맨 뒤."""
    return rcept_no if _RCEPT_RE.match(rcept_no) else "9999" + rcept_no

def _rcept_to_label(rcept_no: str) -> str:
    """rcept_no(YYYYMMDDXXXXXX) → 사람이 읽기 쉬운 분기 레이블."""
    if not _RCEPT_RE.match(rcept_no):
        return rcept_no  # human-readable 그대로
    y, m = rcept_no[:4], int(rcept_no[4:6])
    if m <= 3:   period = f"{int(y)-1}년 연간 (사업보고)"
    elif m <= 5: period = f"{y}년 1분기"
    elif m <= 8: period = f"{y}년 반기 (2분기)"
    else:        period = f"{y}년 3분기"
    return period

def _significance_score(new_cnt: int, lost_cnt: int, changed_cnt: int,
                         has_supplier_change: bool) -> int:
    """변화의 취재 중요도 점수 (0–100)."""
    score = (new_cnt * 2) + (lost_cnt * 3) + (changed_cnt * 2)
    if has_supplier_change:
        score += 15   # 공급자 변화는 특히 중요
    return min(score, 100)

def _enrich_partner(db: sqlite3.Connection, partner_name: str) -> dict:
    """partner_mapping 테이블로 파트너 보강 정보 반환."""
    pm = db.execute(
        "SELECT corp_code, corp_name, match_type, is_listed FROM partner_mapping WHERE partner_name = ?",
        [partner_name]
    ).fetchone()
    if pm:
        return {
            "corp_code":  pm["corp_code"],
            "corp_name":  pm["corp_name"],
            "match_type": pm["match_type"],
            "is_listed":  bool(pm["is_listed"]),
        }
    return {"corp_code": None, "corp_name": None, "match_type": None, "is_listed": False}


def get_supply_chain_changes(corp_code: str,
                              from_report: str | None = None,
                              to_report:   str | None = None) -> dict:
    """
    동일 기업의 시기별 공급망 변화 감지.

    - rcept_no(YYYYMMDDXXXXXX) 기준 날짜순 정렬 (기존 analyzed_at 정렬 버그 수정)
    - partner_mapping 연동으로 파트너 corp_code/상장여부 보강
    - 변화 중요도 점수(significance) 제공
    - from_report / to_report 지정 가능 (미지정 시 인접 2개 비교)

    반환:
      periods   : 보유 보고서 전체 목록 (타임라인)
      compared  : {from, to, from_label, to_label}
      changes   : {new_partners, lost_partners, relation_changed, unchanged_count}
      summary   : {total_from, total_to, new, lost, changed, significance}
    """
    db = _get_db()

    name_row = db.execute(
        "SELECT DISTINCT corp_name FROM supply_chain WHERE corp_code = ? LIMIT 1",
        [corp_code]
    ).fetchone()
    if not name_row:
        db.close()
        return {"error": f"corp_code '{corp_code}' 없음"}

    corp_name = name_row["corp_name"]

    # ── 보고서 목록: rcept_no 기준 날짜순 정렬 ──
    raw_reports = db.execute(
        "SELECT DISTINCT source_report FROM supply_chain WHERE corp_code = ?",
        [corp_code]
    ).fetchall()
    periods_sorted = sorted(
        [r["source_report"] for r in raw_reports],
        key=_rcept_sort_key
    )

    periods_info = [
        {"rcept_no": r, "label": _rcept_to_label(r)}
        for r in periods_sorted
    ]

    if len(periods_sorted) < 2:
        db.close()
        return {
            "corp_code": corp_code,
            "corp_name": corp_name,
            "message": "비교 가능한 기간이 1개뿐 (변화 감지 불가)",
            "periods": periods_info,
            "changes": None,
        }

    # ── 비교 대상 결정 ──
    if from_report and to_report:
        rpt_from, rpt_to = from_report, to_report
    else:
        # 기본: 가장 오래된 vs 가장 최신
        rpt_from = periods_sorted[0]
        rpt_to   = periods_sorted[-1]

    def _get_partners(rtype: str) -> dict[str, dict]:
        rows = db.execute(
            "SELECT relation_type, partner_name, context FROM supply_chain WHERE corp_code = ? AND source_report = ?",
            [corp_code, rtype]
        ).fetchall()
        return {r["partner_name"]: {"relation": r["relation_type"], "context": r["context"]} for r in rows}

    from_partners = _get_partners(rpt_from)
    to_partners   = _get_partners(rpt_to)

    from_set = set(from_partners.keys())
    to_set   = set(to_partners.keys())

    new_set   = to_set   - from_set   # 새로 등장
    lost_set  = from_set - to_set     # 사라진 것
    kept_set  = from_set & to_set     # 유지

    # 관계 타입 변경
    relation_changed = [
        p for p in kept_set
        if from_partners[p]["relation"] != to_partners[p]["relation"]
    ]

    # supplier 관련 변화 여부 (취재 중요도 가중)
    has_supplier_change = any(
        from_partners.get(p, {}).get("relation") == "supplier" or
        to_partners.get(p, {}).get("relation") == "supplier"
        for p in (new_set | lost_set | set(relation_changed))
    )

    significance = _significance_score(
        len(new_set), len(lost_set), len(relation_changed), has_supplier_change
    )

    def _make_entry(p: str, partners_dict: dict, enrichment: bool = True) -> dict:
        e = {
            "partner_name":  p,
            "relation_type": partners_dict[p]["relation"],
            "context":       partners_dict[p]["context"],
        }
        if enrichment:
            e.update(_enrich_partner(db, p))
        return e

    # 관계 변경 항목 보강
    rel_changed_list = []
    for p in relation_changed:
        entry = {
            "partner_name": p,
            "old_relation": from_partners[p]["relation"],
            "new_relation": to_partners[p]["relation"],
        }
        entry.update(_enrich_partner(db, p))
        rel_changed_list.append(entry)

    # 파트너 목록 빌드 (db 닫기 전에 완성)
    new_list  = [_make_entry(p, to_partners)   for p in sorted(new_set)]
    lost_list = [_make_entry(p, from_partners) for p in sorted(lost_set)]

    db.close()
    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "periods":   periods_info,
        "compared": {
            "from":       rpt_from,
            "to":         rpt_to,
            "from_label": _rcept_to_label(rpt_from),
            "to_label":   _rcept_to_label(rpt_to),
        },
        "changes": {
            "new_partners":     new_list,
            "lost_partners":    lost_list,
            "relation_changed": rel_changed_list,
            "unchanged_count":  len(kept_set) - len(relation_changed),
        },
        "summary": {
            "total_from":  len(from_set),
            "total_to":    len(to_set),
            "new":         len(new_set),
            "lost":        len(lost_set),
            "changed":     len(relation_changed),
            "significance": significance,
        },
    }


def get_change_alerts(limit: int = 20) -> dict:
    """
    변화 감지 알림: 최근 공급망 변화가 있는 기업 목록 (취재 대시보드용).

    복수 보고서를 보유한 기업을 대상으로 변화 유무·중요도를 스코어링해 반환.
    """
    db = _get_db()

    # 복수 보고서 보유 기업 목록
    multi_report_corps = db.execute("""
        SELECT corp_code, corp_name, COUNT(DISTINCT source_report) as report_cnt
        FROM supply_chain
        WHERE source_report GLOB '2[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*'
        GROUP BY corp_code
        HAVING report_cnt >= 2
        ORDER BY report_cnt DESC, corp_code
    """).fetchall()

    alerts = []
    for row in multi_report_corps:
        cc   = row["corp_code"]
        name = row["corp_name"]

        # 보고서 기간 목록 (날짜순)
        rpts = db.execute(
            "SELECT DISTINCT source_report FROM supply_chain WHERE corp_code = ? AND source_report GLOB '2[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*'",
            [cc]
        ).fetchall()
        periods = sorted([r["source_report"] for r in rpts], key=_rcept_sort_key)

        if len(periods) < 2:
            continue

        rpt_from = periods[-2]   # 직전 기간
        rpt_to   = periods[-1]   # 최신 기간

        def _get_set(rpt: str) -> dict[str, str]:
            rows = db.execute(
                "SELECT partner_name, relation_type FROM supply_chain WHERE corp_code = ? AND source_report = ?",
                [cc, rpt]
            ).fetchall()
            return {r["partner_name"]: r["relation_type"] for r in rows}

        from_d = _get_set(rpt_from)
        to_d   = _get_set(rpt_to)
        from_s = set(from_d)
        to_s   = set(to_d)

        new_cnt  = len(to_s - from_s)
        lost_cnt = len(from_s - to_s)
        kept     = from_s & to_s
        changed_cnt = sum(1 for p in kept if from_d[p] != to_d[p])

        has_supplier = any(
            from_d.get(p) == "supplier" or to_d.get(p) == "supplier"
            for p in (to_s - from_s) | (from_s - to_s)
        )
        sig = _significance_score(new_cnt, lost_cnt, changed_cnt, has_supplier)

        if new_cnt + lost_cnt + changed_cnt == 0:
            continue   # 변화 없으면 건너뜀

        alerts.append({
            "corp_code":   cc,
            "corp_name":   name,
            "report_cnt":  len(periods),
            "from_period": _rcept_to_label(rpt_from),
            "to_period":   _rcept_to_label(rpt_to),
            "from_rcept":  rpt_from,
            "to_rcept":    rpt_to,
            "new":         new_cnt,
            "lost":        lost_cnt,
            "changed":     changed_cnt,
            "significance": sig,
        })

    db.close()
    alerts.sort(key=lambda x: x["significance"], reverse=True)
    return {
        "total_monitored": len(multi_report_corps),
        "total_changed":   len(alerts),
        "alerts":          alerts[:limit],
    }


# ══════════════════════════════════════════════════════════════════
#  7. 교집합 종목 발굴
# ══════════════════════════════════════════════════════════════════

def get_common_suppliers(partner_names: list[str], relation: str = "customer") -> dict:
    """
    여러 대기업의 공통 공급사(또는 고객사) 찾기.

    예: 삼성전자 + 현대자동차 + LG에너지솔루션 의 공통 supplier
    → 멀티섹터 수혜 부품사 발굴

    relation: 'customer' → 해당 파트너들의 고객인 기업 찾기
              'supplier' → 해당 파트너들에게 납품하는 기업 찾기
    """
    if not partner_names:
        return {"error": "partner_names 필요"}

    db = _get_db()

    # 각 파트너를 customer로 가진 기업들의 corp_code 교집합
    sets: list[set[str]] = []
    partner_reporters: dict[str, list[dict]] = {}

    for partner in partner_names:
        rows = db.execute(
            """SELECT corp_code, corp_name, context
               FROM supply_chain
               WHERE partner_name = ? AND relation_type = ?""",
            [partner, relation]
        ).fetchall()
        codes = {r["corp_code"] for r in rows}
        sets.append(codes)
        partner_reporters[partner] = [dict(r) for r in rows]

    if not sets:
        db.close()
        return {"error": "결과 없음"}

    common_codes = sets[0]
    for s in sets[1:]:
        common_codes &= s

    # 공통 기업 상세 정보
    common_companies = []
    for code in common_codes:
        name_row = db.execute(
            "SELECT DISTINCT corp_name FROM supply_chain WHERE corp_code = ? LIMIT 1",
            [code]
        ).fetchone()
        if name_row:
            common_companies.append({
                "corp_code": code,
                "corp_name": name_row["corp_name"],
            })

    db.close()
    return {
        "query": {
            "partners": partner_names,
            "relation": relation,
            "desc": f"{', '.join(partner_names)} 모두에 해당하는 기업 ({relation})",
        },
        "common_count": len(common_companies),
        "common_companies": sorted(common_companies, key=lambda x: x["corp_name"]),
        "per_partner_count": {p: len(s) for p, s in zip(partner_names, sets)},
    }


# ══════════════════════════════════════════════════════════════════
#  8. 집중도 리스크 스코어  (Part 3/4)
# ══════════════════════════════════════════════════════════════════

def get_concentration_risk(corp_code: str) -> dict:
    """
    기업 공급망 집중도 리스크 분석.
    HHI(허핀달-허쉬만) 방식 + 섹터 다양성 지수.

    Returns:
        { corp_code, corp_name, risk_score(0~100), breakdown, alerts }
    """
    db = _get_db()

    name_row = db.execute(
        "SELECT DISTINCT corp_name FROM supply_chain WHERE corp_code = ? LIMIT 1",
        [corp_code]
    ).fetchone()
    if not name_row:
        db.close()
        return {"error": f"corp_code '{corp_code}' 없음"}
    corp_name = name_row["corp_name"]

    # ── 고객 집중도 (customer) ──
    customer_rows = db.execute(
        """SELECT partner_name, COUNT(*) as w FROM supply_chain
           WHERE corp_code = ? AND relation_type = 'customer'
           GROUP BY partner_name ORDER BY w DESC""",
        [corp_code]
    ).fetchall()

    # ── 공급사 집중도 (supplier) ──
    supplier_rows = db.execute(
        """SELECT partner_name, COUNT(*) as w FROM supply_chain
           WHERE corp_code = ? AND relation_type = 'supplier'
           GROUP BY partner_name ORDER BY w DESC""",
        [corp_code]
    ).fetchall()

    # ── 이 기업을 의존하는 기업 수 (역방향 exposure) ──
    exposure_row = db.execute(
        """SELECT COUNT(DISTINCT corp_code) as cnt FROM supply_chain
           WHERE partner_name = ? AND relation_type = 'supplier'""",
        [corp_name]
    ).fetchone()
    exposure_count = exposure_row["cnt"] if exposure_row else 0

    def _hhi(rows) -> float:
        """HHI: 0(완전분산) ~ 10000(독점). 단순화 버전."""
        total = sum(r["w"] for r in rows)
        if not total:
            return 0
        return sum((r["w"] / total * 100) ** 2 for r in rows)

    def _risk_level(hhi: float) -> str:
        if hhi >= 2500: return "HIGH"
        if hhi >= 1500: return "MED"
        return "LOW"

    cust_hhi  = _hhi(customer_rows)
    supp_hhi  = _hhi(supplier_rows)

    # 고객 집중도: 고객이 적을수록 리스크 높음
    cust_count  = len(customer_rows)
    supp_count  = len(supplier_rows)

    # 종합 리스크 스코어 (0~100)
    # 고객 집중도(40%) + 공급사 집중도(30%) + 의존도(20%) + 파트너 다양성(10%)
    cust_score  = min(40, (cust_hhi / 10000) * 40)
    supp_score  = min(30, (supp_hhi / 10000) * 30)
    exp_score   = min(20, (1 / (1 + exposure_count * 0.1)) * 20)  # 의존하는 기업 많을수록 낮아짐
    div_score   = min(10, (1 / (1 + (cust_count + supp_count) * 0.05)) * 10)

    risk_score = round(cust_score + supp_score + exp_score + div_score, 1)

    # 경고 생성
    alerts = []
    if cust_count <= 3 and cust_count > 0:
        top = customer_rows[0]["partner_name"] if customer_rows else ""
        alerts.append({"type": "customer_concentration",
                        "severity": "HIGH",
                        "msg": f"고객사 {cust_count}곳에 집중 — {top} 이탈 시 매출 타격 우려"})
    if supp_count <= 3 and supp_count > 0:
        top = supplier_rows[0]["partner_name"] if supplier_rows else ""
        alerts.append({"type": "supplier_concentration",
                        "severity": "HIGH",
                        "msg": f"공급사 {supp_count}곳 집중 — {top} 공급 차질 시 생산 위험"})
    if exposure_count >= 20:
        alerts.append({"type": "systemic_risk",
                        "severity": "HIGH",
                        "msg": f"{exposure_count}개 기업이 {corp_name}를 공급사로 의존 — 시스템 리스크 높음"})
    elif exposure_count == 0:
        alerts.append({"type": "low_exposure",
                        "severity": "LOW",
                        "msg": "다른 기업의 의존도가 낮아 시스템 리스크는 제한적"})

    db.close()
    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "risk_score": risk_score,
        "risk_level": "HIGH" if risk_score >= 60 else "MED" if risk_score >= 35 else "LOW",
        "breakdown": {
            "customer_concentration": {
                "hhi": round(cust_hhi, 1),
                "level": _risk_level(cust_hhi),
                "count": cust_count,
                "top3": [r["partner_name"] for r in customer_rows[:3]],
            },
            "supplier_concentration": {
                "hhi": round(supp_hhi, 1),
                "level": _risk_level(supp_hhi),
                "count": supp_count,
                "top3": [r["partner_name"] for r in supplier_rows[:3]],
            },
            "systemic_exposure": {
                "dependent_count": exposure_count,
                "desc": f"{exposure_count}개 기업이 {corp_name}를 핵심 공급사로 의존",
            },
        },
        "alerts": alerts,
    }


# ══════════════════════════════════════════════════════════════════
#  8-2. 집중도 리스크 순위  (배치 계산)
# ══════════════════════════════════════════════════════════════════

def get_concentration_risk_ranking(
    limit: int = 50,
    risk_level: str | None = None,
    sort_by: str = "risk",        # "risk" | "systemic"
    min_partners: int = 3,        # 파트너 최소 수 (1~2개만 있는 소규모 기업 필터)
) -> dict:
    """
    전체 기업 집중도 리스크 순위 (배치 계산).

    단일 쿼리로 18,000여 레코드를 메모리에 올린 뒤
    HHI·exposure 계산 → 정렬.

    Args:
        sort_by:      "risk" = HHI 기반 집중도 리스크 내림차순
                      "systemic" = 의존 기업 수(exposure_count) 내림차순
        min_partners: 이 수 이상의 파트너를 가진 기업만 포함 (기본값 3)

    Returns:
        { total, stats: {HIGH, MED, LOW}, ranking: [...] }
    """
    db = _get_db()

    # ── 1. 모든 관계 레코드 집계 ─────────────────────────────────────
    rows = db.execute("""
        SELECT corp_code, corp_name, relation_type, partner_name, COUNT(*) as w
        FROM supply_chain
        GROUP BY corp_code, corp_name, relation_type, partner_name
    """).fetchall()

    # ── 2. 역방향 exposure: 기업명 → 의존 기업 수 ──────────────────
    exp_rows = db.execute("""
        SELECT partner_name, COUNT(DISTINCT corp_code) as dep_count
        FROM supply_chain
        WHERE relation_type = 'supplier'
        GROUP BY partner_name
    """).fetchall()
    exposure_map = {r["partner_name"]: r["dep_count"] for r in exp_rows}

    db.close()

    # ── 3. 기업별 분류 ────────────────────────────────────────────────
    company_names: dict[str, str] = {}
    cust_data: dict[str, list] = defaultdict(list)
    supp_data: dict[str, list] = defaultdict(list)

    for r in rows:
        company_names[r["corp_code"]] = r["corp_name"]
        if r["relation_type"] == "customer":
            cust_data[r["corp_code"]].append((r["partner_name"], r["w"]))
        elif r["relation_type"] == "supplier":
            supp_data[r["corp_code"]].append((r["partner_name"], r["w"]))

    def _hhi(items: list) -> float:
        total = sum(w for _, w in items)
        if not total:
            return 0.0
        return sum((w / total * 100) ** 2 for _, w in items)

    # ── 4. 스코어 계산 ────────────────────────────────────────────────
    results = []
    for corp_code, corp_name in company_names.items():
        custs = sorted(cust_data.get(corp_code, []), key=lambda x: -x[1])
        supps = sorted(supp_data.get(corp_code, []), key=lambda x: -x[1])

        cust_count = len(custs)
        supp_count = len(supps)
        total_partners = cust_count + supp_count

        # 파트너 최소 수 필터 (1~2개만 있는 소규모 기업 제외)
        if total_partners < min_partners:
            continue

        cust_hhi = _hhi(custs)
        supp_hhi = _hhi(supps)
        exposure_count = exposure_map.get(corp_name, 0)

        cust_score = min(40, (cust_hhi / 10000) * 40)
        supp_score = min(30, (supp_hhi / 10000) * 30)
        exp_score  = min(20, (1 / (1 + exposure_count * 0.1)) * 20)
        div_score  = min(10, (1 / (1 + total_partners * 0.05)) * 10)
        risk_score = round(cust_score + supp_score + exp_score + div_score, 1)

        # 시스템 리스크 점수 (이 기업에 의존하는 기업이 많을수록 높음)
        systemic_score = round(min(100, exposure_count * 8), 1)

        rl = "HIGH" if risk_score >= 60 else "MED" if risk_score >= 35 else "LOW"

        if risk_level and rl != risk_level:
            continue

        results.append({
            "corp_code":       corp_code,
            "corp_name":       corp_name,
            "sector":          _sector_of(corp_name, corp_code=corp_code),
            "risk_score":      risk_score,
            "risk_level":      rl,
            "systemic_score":  systemic_score,
            "cust_hhi":        round(cust_hhi),
            "supp_hhi":        round(supp_hhi),
            "cust_count":      cust_count,
            "supp_count":      supp_count,
            "exposure_count":  exposure_count,
            "top_customer":    custs[0][0] if custs else None,
            "top_supplier":    supps[0][0] if supps else None,
        })

    # 정렬
    if sort_by == "systemic":
        results.sort(key=lambda x: (-x["exposure_count"], -x["risk_score"]))
    else:
        results.sort(key=lambda x: (-x["risk_score"], -x["exposure_count"]))

    high = sum(1 for r in results if r["risk_level"] == "HIGH")
    med  = sum(1 for r in results if r["risk_level"] == "MED")
    low  = sum(1 for r in results if r["risk_level"] == "LOW")

    return {
        "total":   len(results),
        "stats":   {"HIGH": high, "MED": med, "LOW": low},
        "ranking": results[:limit],
    }


# ══════════════════════════════════════════════════════════════════
#  9. 테마 기사 데이터  (Part 3 — FinanceScope)
# ══════════════════════════════════════════════════════════════════

def get_theme_article_data(corp_code: str, article_type: str = "supply_top10") -> dict:
    """
    팩트 기반 테마 기사 자동 데이터 생성.
    AI 없이 DB 데이터만으로 기사 뼈대(팩트) 제공.

    article_type:
      - supply_top10   : 이 기업에 납품하는 TOP10 기업
      - customer_top10 : 이 기업의 주요 고객사 TOP10
      - hub_ecosystem  : 이 기업 중심 생태계 전체 조망
      - common_play    : 멀티 섹터 공통 수혜주 발굴

    Returns:
        { article_type, headline_hint, key_facts, entities, data_table }
    """
    db = _get_db()

    name_row = db.execute(
        "SELECT DISTINCT corp_name FROM supply_chain WHERE corp_code = ? LIMIT 1",
        [corp_code]
    ).fetchone()
    if not name_row:
        db.close()
        return {"error": f"corp_code '{corp_code}' 없음"}
    corp_name = name_row["corp_name"]

    result = {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "article_type": article_type,
    }

    if article_type in ("supply_top10", "customer_top10"):
        rel = "supplier" if article_type == "supply_top10" else "customer"
        rel_ko = "납품사(공급사)" if article_type == "supply_top10" else "고객사"

        rows = db.execute(
            """SELECT partner_name,
                      COUNT(*) as mention_cnt,
                      GROUP_CONCAT(DISTINCT source_report) as reports
               FROM supply_chain
               WHERE corp_code = ? AND relation_type = ?
               GROUP BY partner_name
               ORDER BY mention_cnt DESC
               LIMIT 10""",
            [corp_code, rel]
        ).fetchall()

        entities = []
        for i, r in enumerate(rows, 1):
            p_code = _partner_to_corp_code(db, r["partner_name"])
            entities.append({
                "rank": i,
                "partner_name": r["partner_name"],
                "corp_code": p_code,
                "is_listed": p_code is not None,
                "mention_cnt": r["mention_cnt"],
                "reports": r["reports"],
            })

        listed_count = sum(1 for e in entities if e["is_listed"])
        top3_names   = "·".join(e["partner_name"] for e in entities[:3])

        result.update({
            "headline_hint": f"{corp_name} {rel_ko} 상위 10선 — {top3_names} 등 주목",
            "key_facts": [
                f"{corp_name}의 공시 기반 {rel_ko} {len(entities)}곳 확인",
                f"이 중 상장사 {listed_count}곳, 비상장 {len(entities)-listed_count}곳",
                f"보고서 복수 언급: {sum(1 for e in entities if e['mention_cnt'] > 1)}곳",
            ],
            "entities": entities,
            "data_table_hint": f"DART 공시 데이터 기반, 보고 횟수 = 관계 신뢰도 지표",
        })

    elif article_type == "hub_ecosystem":
        # 전체 관계 조망
        all_rows = db.execute(
            """SELECT relation_type, COUNT(DISTINCT partner_name) as cnt
               FROM supply_chain WHERE corp_code = ?
               GROUP BY relation_type""",
            [corp_code]
        ).fetchall()

        top_by_type = {}
        for rel in ("customer", "supplier", "partner", "competitor"):
            rows = db.execute(
                """SELECT partner_name, COUNT(*) as w FROM supply_chain
                   WHERE corp_code = ? AND relation_type = ?
                   GROUP BY partner_name ORDER BY w DESC LIMIT 5""",
                [corp_code, rel]
            ).fetchall()
            top_by_type[rel] = [r["partner_name"] for r in rows]

        # 이 기업을 언급하는 타 기업 수
        reverse_cnt = db.execute(
            "SELECT COUNT(DISTINCT corp_code) FROM supply_chain WHERE partner_name = ?",
            [corp_name]
        ).fetchone()[0]

        rel_counts = {r["relation_type"]: r["cnt"] for r in all_rows}
        result.update({
            "headline_hint": f"{corp_name} 공급망 생태계 총정리 — {sum(rel_counts.values())}개사와 연결",
            "key_facts": [
                f"고객사 {rel_counts.get('customer', 0)}곳, 공급사 {rel_counts.get('supplier', 0)}곳",
                f"파트너 {rel_counts.get('partner', 0)}곳, 경쟁사 {rel_counts.get('competitor', 0)}곳",
                f"타 기업 공시에서 {corp_name} 언급 {reverse_cnt}건",
            ],
            "entities": top_by_type,
            "reverse_mention_count": reverse_cnt,
        })

    db.close()
    return result


# ══════════════════════════════════════════════════════════════════
#  10. 리스크 시나리오 시뮬레이션  (Part 3 — Palantir 스타일)
# ══════════════════════════════════════════════════════════════════

def get_risk_scenario(corp_code: str, scenario: str = "disruption") -> dict:
    """
    리스크 시나리오 파급 효과 시뮬레이션.

    scenario:
      - disruption : 공급 중단 (생산 차질, 공급사 이슈)
      - demand_drop: 수요 급감 (주요 고객 이탈/파산)
      - bankruptcy : 기업 파산 (전방·후방 동시 타격)

    Returns:
        { scenario, corp_name, wave1(직접), wave2(간접), total_exposed, impact_map }
    """
    db = _get_db()

    name_row = db.execute(
        "SELECT DISTINCT corp_name FROM supply_chain WHERE corp_code = ? LIMIT 1",
        [corp_code]
    ).fetchone()
    if not name_row:
        db.close()
        return {"error": f"corp_code '{corp_code}' 없음"}
    corp_name = name_row["corp_name"]

    def _get_related(code: str, rel_type: str) -> list[dict]:
        rows = db.execute(
            """SELECT DISTINCT partner_name, context FROM supply_chain
               WHERE corp_code = ? AND relation_type = ?
               ORDER BY partner_name""",
            [code, rel_type]
        ).fetchall()
        result = []
        for r in rows:
            p_code = _partner_to_corp_code(db, r["partner_name"])
            result.append({
                "name": r["partner_name"],
                "corp_code": p_code,
                "is_listed": p_code is not None,
                "context": (r["context"] or "")[:80],
            })
        return result

    def _get_reverse(name: str, rel_type: str) -> list[dict]:
        """이 기업을 rel_type으로 언급하는 기업들"""
        rows = db.execute(
            """SELECT corp_code, corp_name, context FROM supply_chain
               WHERE partner_name = ? AND relation_type = ?
               ORDER BY corp_name""",
            [name, rel_type]
        ).fetchall()
        return [{"name": r["corp_name"], "corp_code": r["corp_code"],
                 "is_listed": True, "context": (r["context"] or "")[:80]} for r in rows]

    # ── 시나리오별 wave1 결정 ──
    if scenario == "disruption":
        # 공급 중단: 이 기업에서 구매하던 고객들이 직격
        wave1_label = "직접 타격 (고객사 — 조달 차질)"
        wave1 = _get_related(corp_code, "customer")   # 이 기업의 고객들
        wave1 += _get_reverse(corp_name, "supplier")  # 이 기업을 supplier로 의존
        scenario_ko = f"{corp_name} 공급 중단 시나리오"
        scenario_desc = "핵심 부품·원자재 공급이 끊길 경우 고객사 생산 라인 차질 분석"

    elif scenario == "demand_drop":
        # 수요 급감: 이 기업의 공급사들이 주문 감소 타격
        wave1_label = "직접 타격 (공급사 — 주문 감소)"
        wave1 = _get_related(corp_code, "supplier")   # 이 기업의 공급사들
        wave1 += _get_reverse(corp_name, "customer")  # 이 기업을 customer로 가진 기업
        scenario_ko = f"{corp_name} 수요 급감 시나리오"
        scenario_desc = "주력 고객 이탈 또는 시장 수요 급락 시 납품 기업 연쇄 타격 분석"

    else:  # bankruptcy
        # 파산: 전방·후방 동시 타격
        wave1_label = "직접 타격 (전방·후방 동시)"
        wave1_customers = _get_related(corp_code, "customer")
        wave1_suppliers = _get_related(corp_code, "supplier")
        wave1 = wave1_customers + wave1_suppliers
        scenario_ko = f"{corp_name} 파산·폐업 시나리오"
        scenario_desc = "기업 파산 가정 시 공급망 전체 충격 전파 경로 분석"

    # 중복 제거
    seen_wave1 = set()
    wave1_uniq = []
    for w in wave1:
        if w["name"] not in seen_wave1:
            seen_wave1.add(w["name"])
            wave1_uniq.append(w)

    # ── wave2: wave1 기업들의 공급망 파급 (corp_code 있는 기업만) ──
    wave2 = []
    seen_wave2 = seen_wave1.copy()
    for w in wave1_uniq:
        if not w["corp_code"]:
            continue
        # wave1 기업의 공급사들 (원자재 조달 차질 전파)
        sub_rows = db.execute(
            """SELECT DISTINCT partner_name FROM supply_chain
               WHERE corp_code = ? AND relation_type IN ('supplier', 'customer')
               LIMIT 10""",
            [w["corp_code"]]
        ).fetchall()
        for sr in sub_rows:
            if sr["partner_name"] not in seen_wave2:
                seen_wave2.add(sr["partner_name"])
                p2_code = _partner_to_corp_code(db, sr["partner_name"])
                wave2.append({
                    "name": sr["partner_name"],
                    "corp_code": p2_code,
                    "is_listed": p2_code is not None,
                    "via": w["name"],   # 어느 wave1 기업을 통해 전파됐는지
                })

    listed_w1 = sum(1 for w in wave1_uniq if w["is_listed"])
    listed_w2 = sum(1 for w in wave2 if w["is_listed"])

    db.close()
    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "scenario": scenario,
        "scenario_ko": scenario_ko,
        "scenario_desc": scenario_desc,
        "wave1": {
            "label": wave1_label,
            "companies": wave1_uniq[:30],
            "total": len(wave1_uniq),
            "listed": listed_w1,
        },
        "wave2": {
            "label": "2단계 전파 (간접 노출)",
            "companies": wave2[:20],
            "total": len(wave2),
            "listed": listed_w2,
        },
        "summary": {
            "total_exposed": len(wave1_uniq) + len(wave2),
            "listed_exposed": listed_w1 + listed_w2,
            "propagation_depth": 2 if wave2 else 1,
        },
    }


# ══════════════════════════════════════════════════════════════════
#  11. 공급망 전체 통계
# ══════════════════════════════════════════════════════════════════

def get_supply_chain_stats() -> dict:
    """전체 공급망 DB 통계"""
    db = _get_db()

    total     = db.execute("SELECT COUNT(*) FROM supply_chain").fetchone()[0]
    corps     = db.execute("SELECT COUNT(DISTINCT corp_code) FROM supply_chain").fetchone()[0]
    partners  = db.execute("SELECT COUNT(DISTINCT partner_name) FROM supply_chain").fetchone()[0]

    by_type   = db.execute(
        """SELECT relation_type, COUNT(*), COUNT(DISTINCT corp_code)
           FROM supply_chain GROUP BY relation_type ORDER BY COUNT(*) DESC"""
    ).fetchall()

    top_hubs  = db.execute(
        """SELECT partner_name, COUNT(*) as cnt, COUNT(DISTINCT corp_code) as reporters
           FROM supply_chain GROUP BY partner_name ORDER BY cnt DESC LIMIT 10"""
    ).fetchall()

    by_report = db.execute(
        """SELECT source_report, COUNT(*) as cnt, COUNT(DISTINCT corp_code) as corps
           FROM supply_chain GROUP BY source_report ORDER BY cnt DESC"""
    ).fetchall()

    dist = db.execute(
        """SELECT cnt_range, COUNT(*) as corps FROM (
             SELECT CASE WHEN cnt >= 20 THEN '20건이상'
                         WHEN cnt >= 10 THEN '10-19건'
                         WHEN cnt >= 5  THEN '5-9건'
                         WHEN cnt >= 2  THEN '2-4건'
                         ELSE '1건'
                    END as cnt_range, cnt
             FROM (SELECT corp_code, COUNT(*) as cnt FROM supply_chain GROUP BY corp_code)
           ) GROUP BY cnt_range ORDER BY MIN(cnt) DESC"""
    ).fetchall()

    db.close()
    return {
        "total_relations": total,
        "reporting_corps": corps,
        "unique_partners": partners,
        "by_relation_type": [dict(r) for r in by_type],
        "top_hubs": [dict(r) for r in top_hubs],
        "by_report_period": [dict(r) for r in by_report],
        "corps_distribution": [dict(r) for r in dist],
    }


# ══════════════════════════════════════════════════════════════════
#  산업별 공급망 지도
# ══════════════════════════════════════════════════════════════════

_SECTOR_KEYWORDS = [
    # ── 공공·기관 (가장 먼저 — 오분류 방지) ──────────────────────────────
    (r"조달청|한국도로공사|국가철도공단|인천국제공항|김포공항|한국공항공사|한국수자원|질병관리청"
     r"|한국토지주택|LH공사|한국전력공사|한국가스공사|한국석유공사|한국광물자원"
     r"|코레일|SR주식회사|지하철|도시철도|한국철도"
     r"|정부|공단|공사(?!부품)|공기업|국가|국립|국방부|국토부|환경부|산업부"
     r"|한국거래소|금융감독원|금융위원회|한국은행|산업은행|기업은행|수출입은행"
     r"|한국신용평가|한국기업평가|NICE신용평가|나이스신용평가|한국평가정보"
     r"|건강보험|국민연금|근로복지공단|한국산업안전|KOTRA|대한무역투자진흥"
     r"|서울시|경기도|부산시|인천시|대구시|광주시|대전시|울산시"
     r"|대학교|연구원|연구소|과학기술원|KAIST|POSTECH|서울대|연세대|고려대", "공공·기관"),

    # ── 바이오·제약·의료 ───────────────────────────────────────────────
    (r"바이오|제약|의료|헬스|병원|메디|약품|케어|테라퓨틱|진단|백신|의약|클리닉|덴탈|치과"
     r"|파마|팜|임상|세포|유전|항암|항체|보건|수의|동물약|생명과학|라이프사이언스"
     r"|Roche|로슈|Pfizer|화이자|Novartis|노바티스|AstraZeneca|아스트라제네카"
     r"|GSK|MSD|Merck|머크|Johnson|존슨|Abbott|애보트|Medtronic|메드트로닉"
     r"|Illumina|일루미나|Thermo|써모피셔|Bio-Rad|바이오래드|Danaher|다나허"
     r"|녹십자|유한양행|종근당|한미약품|동아ST|보령|대웅|일동|광동|JW중외"
     r"|셀트리온|삼성바이오|한올바이오파마|휴젤|메디톡스|오스템|클래시스", "바이오·제약"),

    # ── 반도체·전자 (글로벌 빅테크 + 소재/장비 포함) ────────────────────
    (r"반도체|디스플레이|OLED|LED|PCB|팹|칩셋|HBM|메모리|웨이퍼|파운드리|반도체공정"
     r"|전자(?!레인지|상거래|정부|공시|신고|공고|금융)|전기전자|마이크로|나노|센서|DRAM|NAND|SSD|GPU|CPU|NPU"
     r"|하이닉스|이노텍|SDI|삼성전기|SK실트론|DB하이텍|키옥시아"
     r"|삼성전자|SK하이닉스|LG이노텍|LG전자|LG디스플레이"
     r"|TSMC|인텔|Intel|NVIDIA|엔비디아|Qualcomm|퀄컴|AMD|Broadcom|브로드컴"
     r"|Apple|애플|Samsung|BOE|CSOT|AUO|이노룩스|Innolux"
     r"|ASML|어플라이드머티리얼즈|Applied Materials|램리서치|Lam Research"
     r"|마이크론|Micron|웨스턴디지털|Western Digital|시게이트|Seagate"
     r"|텍사스인스트루먼트|Texas Instruments|TI(?!\s*증권)|ST마이크로|STMicro|르네사스|Renesas"
     r"|Sony|소니|Panasonic|파나소닉|Sharp|샤프|Murata|무라타|TDK|Alps|알프스"
     r"|Nitto|닛토|Sumitomo|스미토모|Kyocera|교세라|Corning|코닝|Hoya|호야|JSR|신에츠|Shin-Etsu"
     r"|파워로직스|옵트론텍|에스에프에이|원익IPS|피에스케이|테스|유진테크|리노공업", "반도체·전자"),

    # ── 자동차·배터리 ──────────────────────────────────────────────────
    (r"자동차|모비스|타이어|전장|이차전지|전기차|EV배터리|자동차부품|자동차OEM"
     r"|현대차|기아(?!\s*타이거)|현대모비스|한국GM|한국지엠|르노코리아|르노삼성|차체|엔진부품|구동|변속|트랜스미션"
     r"|LG에너지솔루션|SK온|에코프로|포스코퓨처엠|배터리셀|배터리모듈|배터리팩"
     r"|현대트랜시스|만도|한온시스템|HL만도|성우하이텍|명신산업|서연이화|화신|SL|평화정공"
     r"|KG모빌리티|쌍용자동차|GM|General Motors|Ford|폭스바겐|Volkswagen|VW"
     r"|BMW|벤츠|Mercedes|아우디|Audi|토요타|Toyota|혼다|Honda|닛산|Nissan"
     r"|스텔란티스|Stellantis|볼보|Volvo|지리|BYD|테슬라|Tesla"
     r"|Bosch|보쉬|컨티넨탈|Continental|덴소|Denso|아이신|Aisin|ZF|마그나|Magna", "자동차·배터리"),

    # ── IT·소프트웨어·플랫폼 (글로벌 포함) ────────────────────────────
    (r"소프트웨어|솔루션|시스템통합|플랫폼|클라우드|빅데이터|핀테크|ERP|SaaS|SW개발|IT서비스"
     r"|카카오|네이버|라인|삼성SDS|LG CNS|SK C&C|NHN|티몬|야놀자|배달의민족|토스"
     r"|Google|구글|Microsoft|마이크로소프트|AWS|아마존웹서비스|Amazon|아마존"
     r"|Meta|메타|Facebook|페이스북|Oracle|오라클|SAP|세일즈포스|Salesforce"
     r"|Cisco|시스코|IBM|HP|Dell|EMC|VMware|Palo Alto|팔로알토"
     r"|Kakao|Naver|쿠팡IT|배민|직방|당근마켓|리멤버|뤼이드|크래프톤(?=.*엔진)", "IT·소프트웨어"),

    # ── 미디어·엔터·게임 ───────────────────────────────────────────────
    (r"게임|엔터테인먼트|미디어|방송|콘텐츠|영화|음악|드라마|광고|출판|웹툰|OTT|스튜디오"
     r"|하이브|SM엔터|JYP|YG|크래프톤|넥슨|엔씨소프트|넷마블|펄어비스|카카오게임"
     r"|CJ ENM|CJENM|JTBC|MBC|KBS|SBS|tvN|웨이브|왓챠|시즌"
     r"|Netflix|넷플릭스|Disney|디즈니|Warner|워너|Universal|유니버설"
     r"|Sony Music|소니뮤직|Spotify|스포티파이|YouTube|유튜브", "미디어·엔터"),

    # ── 화학·소재 ──────────────────────────────────────────────────────
    (r"화학|소재|섬유|수지|플라스틱|페인트|접착|정밀화학|신소재|첨단소재|필름|코팅|폴리"
     r"|케미칼|케미|롯데케미칼|LG화학|금호석유|한화솔루션|SK이노(?!베이션)|SKC|효성|코오롱"
     r"|카본|그래핀|에폭시|폴리이미드|아크릴|레진|엘라스토머|고무|타일|유리|단열"
     r"|BASF|바스프|3M|듀폰|DuPont|Dow Chemical|다우케미칼|SABIC|헌츠만|Huntsman"
     r"|Evonik|에보닉|Lanxess|란세스|Arkema|아르케마|Solvay|솔베이"
     r"|Glencore|글렌코어|Umicore|유미코아|Albemarle|알베마를|Livent"
     r"|동우화인켐|동성화학|한화케미칼|태광산업|효성화학|롯데정밀화학"
     r"|OCI|오씨아이|KCC|케이씨씨|LX하우시스|한국유리|벽산|이건산업|KAC"
     r"|삼양사(?=.*화학)|아주산업|유진기업|한솔제지|삼성엔지니어링(?=.*화학)|일신방직", "화학·소재"),

    # ── 건설·인프라 ────────────────────────────────────────────────────
    (r"건설|시멘트|레미콘|부동산|주택|토목|플랜트|건자재|건축|리모델링|분양|시행|엔지니어링"
     r"|현대건설|대우건설|GS건설|포스코건설|롯데건설|SK에코플랜트|태영|현대엔지니어링"
     r"|DL이앤씨|호반건설|중흥건설|HDC현대산업|제일건설|서희건설|금호건설"
     r"|삼성엔지니어링|현대산업개발|GS이앤씨|한화건설|두산건설|한미글로벌|희림|무림SP", "건설·인프라"),

    # ── 금융·보험 ──────────────────────────────────────────────────────
    (r"금융|은행|보험|증권|투자|캐피탈|카드|리스|저축|신탁|자산운용|인베스트|펀드|벤처캐피탈"
     r"|KB금융|신한금융|하나금융|우리금융|NH농협금융|메리츠|삼성생명|한화생명|교보생명"
     r"|삼성화재|현대해상|DB손해보험|KB손해보험|흥국생명|동양생명"
     r"|미래에셋|한국투자|키움|NH투자|대신|신영|유안타|하이투자|IBK"
     r"|농협|농협중앙회|수협|신협|새마을금고|저축은행"
     r"|Goldman|골드만|Morgan Stanley|JP Morgan|BlackRock|블랙록|Fidelity|피델리티"
     r"|Berkshire|버크셔|Vanguard|뱅가드|State Street|씨티|Citi|HSBC", "금융·보험"),

    # ── 유통·물류·항공 ─────────────────────────────────────────────────
    (r"유통|마트|백화점|쇼핑|홈쇼핑|편의점|이커머스|리테일|물류|배송|택배|항만|선사|해운"
     r"|이마트|롯데쇼핑|GS리테일|CJ대한통운|한진|쿠팡|현대백화점|신세계|SSG|무신사"
     r"|올리브영|화해|지그재그|29CM|오늘의집|11번가|G마켓|옥션|인터파크"
     r"|Costco|코스트코|Walmart|월마트|Amazon(?!.*클라우드)|ZARA|H&M|나이키|Nike|아디다스"
     r"|대한항공|아시아나|제주항공|진에어|티웨이|에어부산|이스타|에어서울"
     r"|K-LINE|MOL|NYK|HMM|현대상선|SM상선|팬스타|장금상선|고려해운"
     r"|DHL|FedEx|UPS|CJ로지스틱스|한진택배|롯데글로벌로지스", "유통·물류"),

    # ── 식품·소비재·뷰티 ───────────────────────────────────────────────
    (r"식품|음료|제과|제빵|주류|양조|농업|수산|축산|식자재|카페|치킨|외식|라면|소주|맥주"
     r"|뷰티|화장품|향수|코스메틱|스킨케어|헤어|이미용"
     r"|CJ제일제당|오리온|롯데제과|농심|빙그레|해태|오뚜기|동원|사조|하이트진로"
     r"|아모레퍼시픽|LG생활건강|코스맥스|한국콜마|잇츠스킨|클리오|에이블씨엔씨"
     r"|스타벅스|맥도날드|KFC|롯데리아|BBQ|교촌|bhc|파리바게뜨|뚜레쥬르"
     r"|삼양사|대상|CJ푸드빌|풀무원|매일유업|남양유업|동서식품|롯데칠성", "식품·소비재"),

    # ── 에너지·자원 ────────────────────────────────────────────────────
    (r"에너지(?!솔루션배터리)|발전|원자력|석유|정유|가스공사|전력|LNG|원유|수소에너지"
     r"|태양광|풍력|한국전력|KEPCO|한국가스|포스코에너지|GS에너지|SK이노베이션|S-OIL"
     r"|남동발전|서부발전|중부발전|동서발전|남부발전|한수원|두산에너빌리티(?=.*원자력)"
     r"|GS칼텍스|에쓰오일|현대오일뱅크|SK에너지"
     r"|Shell|쉘|BP|ExxonMobil|엑슨모빌|Chevron|쉐브론|Total|토탈|ConocoPhillips"
     r"|Rio Tinto|리오틴토|BHP|Vale|발레|Freeport|앵글로아메리칸", "에너지·자원"),

    # ── 철강·금속 ──────────────────────────────────────────────────────
    (r"철강|금속|비철|제철|주조|단조|알루미늄|구리|아연|스테인리스|코일|봉형강|철근|선재"
     r"|POSCO|포스코|현대제철|동국제강|세아|고려아연|풍산|LS전선|대한전선|효성중공업"
     r"|Nippon Steel|신일본제철|JFE|아르셀로미탈|ArcelorMittal|바오우강철|POSCO홀딩스"
     r"|비엠메탈|영풍|서원|LS니코동제련|노벨리스|Novelis|하이드로|Hydro", "철강·금속"),

    # ── 조선·방산·항공우주 ─────────────────────────────────────────────
    (r"조선|선박|방산|방위|무기|군수|항공우주|우주|에어로스페이스|항공기|함정|잠수함|드론방산|해양플랜트"
     r"|한화에어로스페이스|현대중공업|삼성중공업|대우조선해양|한화오션|HJ중공업|한국항공우주|LIG넥스원"
     r"|한화시스템|한화방산|현대로템|풍산(?=.*방산)|빅텍|오르비텍|한국화이바|오리온이엔씨"
     r"|Lockheed|록히드|Boeing|보잉|Northrop|노스롭|Raytheon|레이시온|BAE Systems"
     r"|Airbus|에어버스|GE Aviation|Rolls-Royce|롤스로이스|Safran|사프란", "조선·방산"),

    # ── 통신 ───────────────────────────────────────────────────────────
    (r"통신|SK텔레콤|KT(?!\s*금융|\s*렌탈)|LG유플러스|LG U\+|5G|네트워크통신|인터넷서비스|ISP"
     r"|SK브로드밴드|MVNO|통신망|위성통신|해저케이블"
     r"|Ericsson|에릭슨|Nokia|노키아|Huawei|화웨이|ZTE|삼성네트웍스"
     r"|Qualcomm(?=.*통신)|Intel(?=.*통신)|MediaTek(?=.*통신)", "통신"),

    # ── 중공업·기계·장비 ───────────────────────────────────────────────
    (r"중공업|기계(?!부품)|엔진|펌프|압축기|터빈|발전설비|산업기계|공작기계|로봇|자동화"
     r"|두산에너빌리티|두산밥캣|두산로보틱스|HD현대|현대두산|현대위아|STX|일진다이아"
     r"|LS일렉트릭|효성중공업|대한전기|현대일렉트릭|한국중공업"
     r"|GE|제너럴일렉트릭|Siemens|지멘스|ABB|Emerson|에머슨|Honeywell|하니웰"
     r"|Caterpillar|캐터필러|Komatsu|코마츠|Hitachi|히타치|Mitsubishi|미쓰비시"
     r"|Fanuc|파낙|KUKA|쿠카|Yaskawa|야스카와", "중공업·기계"),
]

_SECTOR_RE = [(re.compile(p, re.IGNORECASE), s) for p, s in _SECTOR_KEYWORDS]

# ── 패턴으로 잡기 어려운 기업 명시적 매핑 ────────────────────────────────────
_SECTOR_EXACT: dict[str, str] = {
    # 대기업 지주·상사
    "삼성물산": "유통·물류", "삼성C&T": "유통·물류",
    "삼성엔지니어링": "건설·인프라", "삼성중공업": "조선·방산",
    "삼성화재": "금융·보험", "삼성생명": "금융·보험",
    "현대": "자동차·배터리", "현대차": "자동차·배터리",
    "현대로템": "조선·방산", "현대위아": "자동차·배터리",
    "현대산업개발": "건설·인프라", "현대백화점": "유통·물류",
    "SK": "에너지·자원", "SK매직": "식품·소비재",
    "SK지오센트릭": "화학·소재", "SK아이이테크놀로지": "반도체·전자",
    "SK실트론": "반도체·전자", "SK머티리얼즈": "화학·소재",
    "CJ": "식품·소비재", "CJ제일제당": "식품·소비재",
    "CJ대한통운": "유통·물류", "CJ ENM": "미디어·엔터",
    "CJ올리브네트웍스": "IT·소프트웨어", "CJ푸드빌": "식품·소비재",
    "롯데": "유통·물류", "롯데케미칼": "화학·소재",
    "롯데쇼핑": "유통·물류", "롯데칠성": "식품·소비재",
    "롯데제과": "식품·소비재", "롯데건설": "건설·인프라",
    "GS": "에너지·자원", "GS칼텍스": "에너지·자원",
    "GS Caltex": "에너지·자원", "GS리테일": "유통·물류",
    "GS건설": "건설·인프라", "GS에너지": "에너지·자원",
    "한화": "조선·방산", "한화오션": "조선·방산",
    "한화에어로스페이스": "조선·방산", "한화시스템": "조선·방산",
    "한화솔루션": "화학·소재", "한화생명": "금융·보험",
    "LG": "반도체·전자", "LG전자": "반도체·전자",
    "LG화학": "화학·소재", "LG에너지솔루션": "자동차·배터리",
    "LG디스플레이": "반도체·전자", "LG이노텍": "반도체·전자",
    "LG생활건강": "식품·소비재", "LG CNS": "IT·소프트웨어",
    "LX하우시스": "화학·소재", "LX세미콘": "반도체·전자",
    "포스코": "철강·금속", "POSCO": "철강·금속",
    "포스코홀딩스": "철강·금속", "포스코인터내셔널": "유통·물류",
    "두산": "중공업·기계", "두산에너빌리티": "중공업·기계",
    "두산밥캣": "중공업·기계", "두산퓨얼셀": "에너지·자원",
    "OCI": "화학·소재", "KCC": "화학·소재",
    "삼양사": "식품·소비재", "아주산업": "화학·소재",
    "유진기업": "건설·인프라", "한솔제지": "화학·소재",
    "아모레퍼시픽": "식품·소비재", "올리브영": "유통·물류",
    "무신사": "유통·물류", "11번가": "유통·물류",
    "질병관리청": "공공·기관", "조달청": "공공·기관",
    "한국거래소": "공공·기관", "한국기업평가": "공공·기관",
    "한국신용평가": "공공·기관", "NICE신용평가": "공공·기관",
    "농협": "금융·보험", "농협중앙회": "금융·보험",
    "대한항공": "유통·물류", "아시아나항공": "유통·물류",
    "HMM": "유통·물류", "현대상선": "유통·물류",
    "유한양행": "바이오·제약", "동아에스티": "바이오·제약",
    "동아ST": "바이오·제약",
    "서울대학교": "공공·기관", "KAIST": "공공·기관",
    "Juniper Networks": "IT·소프트웨어",
    "Google": "IT·소프트웨어", "Google (구글)": "IT·소프트웨어",
    "Microsoft": "IT·소프트웨어", "AWS": "IT·소프트웨어",
    "MediaTek": "반도체·전자", "Realtek": "반도체·전자",
    "INTEL": "반도체·전자", "Intel": "반도체·전자",
    "HKC": "반도체·전자",
    "Glencore": "화학·소재",
    # 빈도 상위 추가 기업
    "대림산업": "건설·인프라", "대림": "건설·인프라",
    "홈플러스": "유통·물류", "마켓컬리": "유통·물류", "SSG": "유통·물류",
    "하림": "식품·소비재", "아워홈": "식품·소비재", "로레알": "식품·소비재",
    "한전": "에너지·자원", "비피코리아": "에너지·자원", "BP Korea": "에너지·자원",
    "와이케이스틸": "철강·금속", "한국특강": "철강·금속",
    "에이디테크놀로지": "반도체·전자", "케이씨글라스": "화학·소재",
    "전주페이퍼": "화학·소재", "코스메카코리아": "식품·소비재",
    "휴온스": "바이오·제약",
    "삼성그룹": "반도체·전자",  # 그룹명 → 대표 섹터
}

# ── companies 테이블에서 biz_content 기반 섹터 캐시 (lazy load) ────────────────
_BIZ_SECTOR_CACHE: dict[str, str] | None = None  # corp_code → sector


def _load_biz_sector_cache() -> dict[str, str]:
    """companies 테이블에서 섹터 정보를 한 번만 로드."""
    global _BIZ_SECTOR_CACHE
    if _BIZ_SECTOR_CACHE is not None:
        return _BIZ_SECTOR_CACHE
    try:
        db = _get_db()
        rows = db.execute(
            "SELECT corp_code, sector FROM companies WHERE sector IS NOT NULL AND sector != '기타'"
        ).fetchall()
        db.close()
        _BIZ_SECTOR_CACHE = {r["corp_code"]: r["sector"] for r in rows}
    except Exception:
        _BIZ_SECTOR_CACHE = {}
    return _BIZ_SECTOR_CACHE


def _sector_of(name: str, corp_code: str | None = None) -> str:
    """기업명에서 업종 추론. 명시적 매핑 우선, 없으면 biz_content 캐시, 없으면 패턴 매칭."""
    if not name:
        return "기타"
    # 1) 완전 일치 (정확 매핑)
    if name in _SECTOR_EXACT:
        return _SECTOR_EXACT[name]
    # 2) 정규식 패턴 (오분류 방지를 위해 substring 매핑 사용 안 함)
    for pattern, sector in _SECTOR_RE:
        if pattern.search(name):
            return sector
    # 3) 명시적 딕셔너리 키가 name의 시작부분과 일치하는 경우만 (3글자 이상)
    #    예: "삼성전자" → "반도체·전자" 매핑에서 "삼성전자주식회사" 처리
    for key, sec in _SECTOR_EXACT.items():
        if len(key) >= 4 and name.startswith(key):
            return sec
    # 4) biz_content 기반 섹터 캐시 (corp_code 있을 때)
    if corp_code:
        cache = _load_biz_sector_cache()
        if corp_code in cache:
            return cache[corp_code]
    return "기타"


def get_supply_chain_profile(corp_code: str) -> dict:
    """
    기업 공급망 프로파일 — 기자 브리핑용.

    Returns:
      corp_name, sector, position, concentration, systemic, changes, story_angles
    """
    db = _get_db()

    # 기본 정보
    row = db.execute(
        "SELECT corp_name FROM supply_chain WHERE corp_code=? LIMIT 1", [corp_code]
    ).fetchone()
    if not row:
        db.close()
        return {"error": "해당 기업의 공급망 데이터가 없습니다."}

    corp_name = row["corp_name"]

    # 전체 관계
    rels = db.execute(
        "SELECT relation_type, partner_name, context FROM supply_chain WHERE corp_code=?",
        [corp_code]
    ).fetchall()

    customers  = [r for r in rels if r["relation_type"] == "customer"]
    suppliers  = [r for r in rels if r["relation_type"] == "supplier"]
    partners   = [r for r in rels if r["relation_type"] == "partner"]
    competitors= [r for r in rels if r["relation_type"] == "competitor"]

    # HHI 계산 (1/n 균등 가중)
    def _hhi(lst):
        n = len(lst)
        if n == 0: return 0
        w = 100 / n
        return int(sum(w**2 for _ in lst))

    cust_hhi = _hhi(customers)
    supp_hhi = _hhi(suppliers)

    # 리스크 레벨
    def _risk_level(hhi, count):
        if hhi >= 5000 or count == 0: return "HIGH"
        if hhi >= 2500: return "MED"
        return "LOW"

    cust_risk = _risk_level(cust_hhi, len(customers))
    supp_risk = _risk_level(supp_hhi, len(suppliers))

    # 시스템 중요도: 이 기업을 파트너명으로 언급한 기업 수
    exposure = db.execute(
        "SELECT COUNT(DISTINCT corp_code) FROM supply_chain WHERE partner_name LIKE ?",
        [f"%{corp_name}%"]
    ).fetchone()[0]

    # 업종
    sector = _sector_of(corp_name, corp_code=corp_code)

    # 공급망 포지션 분석
    total = len(rels)
    if len(customers) > len(suppliers) * 1.5 and len(customers) >= 3:
        position = "제조·공급 중심"
        pos_desc = f"{len(customers)}개 고객사에 제품·서비스 공급"
    elif len(suppliers) > len(customers) * 1.5 and len(suppliers) >= 3:
        position = "구매·유통 중심"
        pos_desc = f"{len(suppliers)}개 공급사에서 원자재·부품 조달"
    elif len(partners) >= 3:
        position = "파트너십 허브"
        pos_desc = f"{len(partners)}개 파트너사와 전략적 협력"
    else:
        position = "복합형"
        pos_desc = f"고객 {len(customers)}개 · 공급 {len(suppliers)}개 · 파트너 {len(partners)}개"

    # 취재 포인트 생성
    story_angles = []

    if cust_hhi >= 5000:
        top_c = customers[0]["partner_name"] if customers else "?"
        story_angles.append({
            "type": "위험",
            "icon": "🔴",
            "title": "고객 집중도 과다",
            "body": f"고객사가 {len(customers)}개에 불과하며, 특히 {top_c} 의존도 극심. 고객 상실 시 매출 직격탄 우려.",
        })
    elif cust_hhi >= 2500:
        story_angles.append({
            "type": "주의",
            "icon": "🟡",
            "title": "고객 편중 주의",
            "body": f"상위 고객 편중(HHI {cust_hhi:,}). 특정 고객 이탈 시 실적 변동성 높음.",
        })

    if supp_hhi >= 5000:
        top_s = suppliers[0]["partner_name"] if suppliers else "?"
        story_angles.append({
            "type": "위험",
            "icon": "🔴",
            "title": "공급망 단일화 위험",
            "body": f"핵심 원자재·부품을 {top_s} 등 소수 공급사에 의존. 공급 차질 시 생산 중단 가능.",
        })

    if exposure >= 5:
        story_angles.append({
            "type": "시스템",
            "icon": "⚡",
            "title": "공급망 핵심 허브",
            "body": f"{exposure}개 기업이 이 회사에 의존. 이 회사의 위기는 산업 전반으로 전파 가능.",
        })

    if len(competitors) >= 3:
        story_angles.append({
            "type": "경쟁",
            "icon": "⚔️",
            "title": "치열한 경쟁 구도",
            "body": f"{', '.join(c['partner_name'] for c in competitors[:3])} 등과 직접 경쟁. 시장 점유율 각축 중.",
        })

    if total >= 15 and exposure >= 3:
        story_angles.append({
            "type": "분석",
            "icon": "🌐",
            "title": "복잡한 생태계 중심",
            "body": f"공급망 파트너 {total}개, {exposure}개 기업 의존. 업계 생태계 분석 기사 적합.",
        })

    # 최근 보고서 정보
    recent_report = db.execute(
        "SELECT source_report, analyzed_at FROM supply_chain WHERE corp_code=? "
        "ORDER BY source_report DESC LIMIT 1", [corp_code]
    ).fetchone()
    report_period = recent_report["source_report"][:8] if recent_report else "미상"

    # 취재 리드 (story_leads) 연동
    leads_rows = db.execute(
        """SELECT id, lead_type, severity, title, summary, evidence, keywords
           FROM story_leads
           WHERE corp_code = ? AND status != 'archived'
           ORDER BY severity DESC, id DESC
           LIMIT 5""",
        [corp_code]
    ).fetchall()

    story_leads_out = []
    for lr in leads_rows:
        type_icon = {
            "strategy_change": "🔵",
            "market_shift":    "📊",
            "risk_alert":      "🔴",
            "numeric_change":  "🔢",
        }.get(lr["lead_type"], "📌")
        # Short summary (first 120 chars of summary)
        short_summary = (lr["summary"] or "").strip()[:200].replace("\n", " ")
        story_leads_out.append({
            "id":         lr["id"],
            "type":       lr["lead_type"],
            "type_icon":  type_icon,
            "severity":   lr["severity"],
            "title":      lr["title"],
            "summary":    short_summary,
            "evidence":   (lr["evidence"] or "")[:200],
        })

    db.close()

    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "sector": sector,
        "position": position,
        "position_desc": pos_desc,
        "total_relations": total,
        "customers": [{"name": r["partner_name"], "context": r["context"]} for r in customers[:8]],
        "suppliers": [{"name": r["partner_name"], "context": r["context"]} for r in suppliers[:8]],
        "partners":  [{"name": r["partner_name"], "context": r["context"]} for r in partners[:5]],
        "competitors":[{"name": r["partner_name"], "context": r["context"]} for r in competitors[:5]],
        "cust_hhi": cust_hhi,
        "supp_hhi": supp_hhi,
        "cust_risk": cust_risk,
        "supp_risk": supp_risk,
        "exposure_count": exposure,
        "story_angles": story_angles,
        "story_leads":  story_leads_out,
        "report_period": report_period,
    }


def get_industry_map(relation_type: str | None = None) -> dict:
    """
    산업별 공급망 흐름 집계.

    Returns:
      nodes: [{sector, corp_count, total_links}]
      links: [{source, target, relation_type, count, sample_corps}]
      summary: {total_corps, total_links}
    """
    db = _get_db()

    # 1. 모든 공급망 레코드 로드
    rows = db.execute(
        "SELECT corp_code, corp_name, relation_type, partner_name FROM supply_chain"
    ).fetchall()

    # 2. corp_name → sector 매핑 (정규화)
    corp_sector: dict[str, str] = {}
    for r in rows:
        cc = r["corp_code"]
        if cc not in corp_sector:
            corp_sector[cc] = _sector_of(r["corp_name"], corp_code=cc)

    # 3. partner_name → corp_code 역매핑 (supply_chain의 corp_name과 exact 매칭)
    name_to_code: dict[str, str] = {}
    for r in rows:
        name_to_code.setdefault(r["corp_name"], r["corp_code"])

    # 4. 업종 간 흐름 집계
    from collections import defaultdict
    flow_count: dict[tuple, int] = defaultdict(int)
    flow_samples: dict[tuple, list] = defaultdict(list)
    corp_links: dict[str, int] = defaultdict(int)

    for r in rows:
        rtype = r["relation_type"]
        if relation_type and rtype != relation_type:
            continue

        src_sector = corp_sector.get(r["corp_code"], "기타")

        # partner 섹터 결정
        partner_code = name_to_code.get(r["partner_name"])
        if partner_code and partner_code in corp_sector:
            tgt_sector = corp_sector[partner_code]
        else:
            tgt_sector = _sector_of(r["partner_name"])

        # customer: 이 기업 → 고객업종 (공급 방향: 이 기업이 공급)
        # supplier: 이 기업 ← 공급업종 (구매 방향: 이 기업이 구매)
        # partner/competitor: 동업종 관계
        if rtype == "customer":
            key = (src_sector, tgt_sector, "customer")
        elif rtype == "supplier":
            key = (tgt_sector, src_sector, "supplier")   # 공급업종 → 이 기업
        else:
            key = (src_sector, tgt_sector, rtype)

        flow_count[key] += 1
        corp_name = r["corp_name"]
        if len(flow_samples[key]) < 3 and corp_name not in flow_samples[key]:
            flow_samples[key].append(corp_name)
        corp_links[src_sector] += 1

    db.close()

    # 5. 노드 집계
    sector_corps: dict[str, set] = defaultdict(set)
    for r in rows:
        sector_corps[corp_sector.get(r["corp_code"], "기타")].add(r["corp_code"])

    nodes = sorted(
        [{"sector": s, "corp_count": len(corps), "total_links": corp_links.get(s, 0)}
         for s, corps in sector_corps.items()],
        key=lambda x: -x["corp_count"]
    )

    # 6. 링크 목록 (최소 3건 이상)
    links = [
        {
            "source": k[0],
            "target": k[1],
            "relation_type": k[2],
            "count": v,
            "sample_corps": flow_samples[k][:3],
        }
        for k, v in sorted(flow_count.items(), key=lambda x: -x[1])
        if v >= 3
    ]

    return {
        "nodes": nodes,
        "links": links,
        "summary": {
            "total_corps": len(sector_corps),
            "total_links": sum(flow_count.values()),
            "sectors": len(sector_corps),
        },
    }


# ══════════════════════════════════════════════════════════════════
#  일일 취재 브리핑
# ══════════════════════════════════════════════════════════════════

def get_daily_briefing(limit: int = 15) -> dict:
    """
    오늘의 취재 추천 브리핑 — 공급망 분석 기자용.

    3가지 시그널을 종합:
      A) 공급망 변화 알림 (change_alerts 상위)
      B) 취재 리드 + 공급망 연동 기업 (story_leads × supply_chain)
      C) 시스템 중요 허브 기업 (exposure_count 상위)

    Returns:
      { generated_at, total_signals, items: [{...}] }
    """
    db = _get_db()
    items = []

    # ── A. 공급망 변화 알림 ──────────────────────────────────────
    multi_corps = db.execute("""
        SELECT corp_code, corp_name, COUNT(DISTINCT source_report) as rcnt
        FROM supply_chain
        WHERE source_report GLOB '2[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*'
        GROUP BY corp_code HAVING rcnt >= 2
    """).fetchall()

    for row in multi_corps:
        cc, name = row["corp_code"], row["corp_name"]
        rpts = sorted([
            r["source_report"] for r in db.execute(
                "SELECT DISTINCT source_report FROM supply_chain WHERE corp_code=? "
                "AND source_report GLOB '2[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*'", [cc]
            ).fetchall()
        ], key=_rcept_sort_key)
        if len(rpts) < 2:
            continue
        from_r, to_r = rpts[-2], rpts[-1]

        def _ps(rpt):
            return {r["partner_name"]: r["relation_type"] for r in db.execute(
                "SELECT partner_name, relation_type FROM supply_chain WHERE corp_code=? AND source_report=?",
                [cc, rpt]
            ).fetchall()}

        fd, td = _ps(from_r), _ps(to_r)
        fs, ts = set(fd), set(td)
        new_c, lost_c = len(ts - fs), len(fs - ts)
        changed_c = sum(1 for p in fs & ts if fd[p] != td[p])
        sig = _significance_score(new_c, lost_c, changed_c,
                                   any(fd.get(p) == "supplier" or td.get(p) == "supplier"
                                       for p in (ts - fs) | (fs - ts)))
        if sig < 10:
            continue

        new_names  = [p for p in (ts - fs)][:3]
        lost_names = [p for p in (fs - ts)][:3]
        angle = (f"{name}: {_rcept_to_label(from_r)}→{_rcept_to_label(to_r)} "
                 f"신규 {new_c}개사{('·'+','.join(new_names)) if new_names else ''}, "
                 f"이탈 {lost_c}개사{('·'+','.join(lost_names)) if lost_names else ''}.")

        items.append({
            "signal":      "change",
            "signal_icon": "🔄",
            "corp_code":   cc,
            "corp_name":   name,
            "sector":      _sector_of(name, corp_code=cc),
            "score":       sig,
            "title":       f"{name} 공급망 {new_c+lost_c}건 변화",
            "summary":     f"{_rcept_to_label(from_r)} → {_rcept_to_label(to_r)}: 신규 {new_c}개, 이탈 {lost_c}개",
            "angle":       angle,
            "from_rcept":  from_r,
            "to_rcept":    to_r,
        })

    # ── B. 취재 리드 × 공급망 교집합 ─────────────────────────────
    lead_corps = db.execute("""
        SELECT sl.corp_code, sl.corp_name,
               MAX(sl.severity) as max_sev,
               COUNT(*) as lead_cnt,
               GROUP_CONCAT(sl.title, '||') as titles
        FROM story_leads sl
        JOIN supply_chain sc ON sl.corp_code = sc.corp_code
        WHERE sl.status != 'archived'
        GROUP BY sl.corp_code
        HAVING max_sev >= 4
        ORDER BY max_sev DESC, lead_cnt DESC
        LIMIT 30
    """).fetchall()

    existing_codes = {it["corp_code"] for it in items}
    for row in lead_corps:
        cc = row["corp_code"]
        if cc in existing_codes:
            continue
        name = row["corp_name"]
        first_title = (row["titles"] or "").split("||")[0][:60]
        # partner count
        p_cnt = db.execute(
            "SELECT COUNT(DISTINCT partner_name) FROM supply_chain WHERE corp_code=?", [cc]
        ).fetchone()[0]
        score = row["max_sev"] * 15 + row["lead_cnt"] * 3 + min(p_cnt, 10)
        angle = (f"{name}: {first_title}. "
                 f"공급망 {p_cnt}개사 연결 — 공시 이후 공급망 영향 추적 필요.")
        items.append({
            "signal":      "lead",
            "signal_icon": "📋",
            "corp_code":   cc,
            "corp_name":   name,
            "sector":      _sector_of(name, corp_code=cc),
            "score":       score,
            "title":       first_title or f"{name} 취재 리드",
            "summary":     f"리드 {row['lead_cnt']}건 (최고 위험도 {row['max_sev']}), 파트너 {p_cnt}개사",
            "angle":       angle,
        })
        existing_codes.add(cc)

    # ── C. 시스템 허브 기업 ──────────────────────────────────────
    # 타 기업이 가장 많이 언급한 파트너 (= 공급망 핵심 노드)
    hub_rows = db.execute("""
        SELECT partner_name, COUNT(DISTINCT corp_code) as dep_cnt
        FROM supply_chain
        GROUP BY partner_name
        HAVING dep_cnt >= 10
        ORDER BY dep_cnt DESC
        LIMIT 20
    """).fetchall()

    for hrow in hub_rows:
        pname = hrow["partner_name"]
        dep   = hrow["dep_cnt"]
        # Try to find corp_code
        cc_row = db.execute(
            "SELECT corp_code FROM supply_chain WHERE corp_name = ? LIMIT 1", [pname]
        ).fetchone()
        if not cc_row:
            cc_row = db.execute(
                "SELECT corp_code FROM partner_mapping WHERE partner_name = ? AND corp_code IS NOT NULL LIMIT 1",
                [pname]
            ).fetchone()
        cc = cc_row["corp_code"] if cc_row else None
        if cc and cc in existing_codes:
            continue
        score = dep * 4
        angle = (f"{pname}: {dep}개 기업이 공급망에서 의존. "
                 f"이 기업의 위기는 업계 전반으로 파급 — 시스템 리스크 기사 적합.")
        items.append({
            "signal":      "hub",
            "signal_icon": "⚡",
            "corp_code":   cc,
            "corp_name":   pname,
            "sector":      _sector_of(pname, corp_code=cc),
            "score":       score,
            "title":       f"{pname} — 공급망 허브 ({dep}개사 의존)",
            "summary":     f"{dep}개 기업이 이 파트너에 의존",
            "angle":       angle,
        })
        if cc:
            existing_codes.add(cc)

    db.close()

    # 점수 정렬 + 상위 limit 반환
    items.sort(key=lambda x: x["score"], reverse=True)
    from datetime import datetime
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_signals": len(items),
        "items": items[:limit],
    }
