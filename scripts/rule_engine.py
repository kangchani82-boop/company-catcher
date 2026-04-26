"""
scripts/rule_engine.py
──────────────────────
YAML 규칙 파일(rules/financial_alerts.yaml)을 alert_rules DB 테이블과 동기화.
엔티티 별칭(rules/entity_aliases.yaml)을 partner_mapping에 반영.

실행:
  python scripts/rule_engine.py --stats          # 현재 규칙 현황
  python scripts/rule_engine.py --sync           # YAML → DB 동기화
  python scripts/rule_engine.py --sync-aliases   # 엔티티 별칭 → partner_mapping 동기화
  python scripts/rule_engine.py --diff           # YAML vs DB 차이점 확인
"""

import io
import json
import sqlite3
import sys
import argparse
from pathlib import Path
from datetime import datetime

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT       = Path(__file__).parent.parent
DB_PATH    = ROOT / "data" / "dart" / "dart_reports.db"
RULES_YAML = ROOT / "rules" / "financial_alerts.yaml"
ALIAS_YAML = ROOT / "rules" / "entity_aliases.yaml"


# ── YAML 로더 (yaml 없으면 간이 파서) ─────────────────────────────────────────
def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"파일 없음: {path}")
    if _HAS_YAML:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    # yaml 미설치 시 pip install pyyaml 안내
    raise ImportError("PyYAML이 필요합니다: pip install pyyaml")


# ── DB 연결 ────────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# YAML → alert_rules 동기화
# ══════════════════════════════════════════════════════════════════════════════
def sync_rules(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """
    YAML 규칙을 alert_rules 테이블에 UPSERT.
    새 규칙 추가 / 기존 규칙 업데이트 / YAML에 없는 규칙은 비활성화.
    반환: {"added": int, "updated": int, "deactivated": int}
    """
    data = load_yaml(RULES_YAML)
    rules = data.get("rules", [])
    defaults = data.get("defaults", {})

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = {"added": 0, "updated": 0, "deactivated": 0, "skipped": 0}

    yaml_codes = set()

    for rule in rules:
        code      = rule["rule_code"]
        lead_type = rule["lead_type"]
        severity  = rule["severity"]
        title_tmpl = rule.get("title_tmpl", f"{{corp_name}} {lead_type}")
        keywords  = json.dumps(rule.get("keywords", []), ensure_ascii=False)
        exclude_kw = json.dumps(rule.get("exclude_keywords", []), ensure_ascii=False)
        req_ctx   = rule.get("require_context")
        req_ctx_j = json.dumps(req_ctx, ensure_ascii=False) if req_ctx else None
        min_ev    = rule.get("min_evidence_len", defaults.get("min_evidence_len", 0))
        is_active = 1 if rule.get("is_active", defaults.get("is_active", True)) else 0
        desc      = rule.get("description", "")

        yaml_codes.add(code)

        existing = conn.execute(
            "SELECT * FROM alert_rules WHERE rule_code=?", [code]
        ).fetchone()

        if not existing:
            if not dry_run:
                conn.execute("""
                    INSERT INTO alert_rules
                      (rule_code, lead_type, severity, title_tmpl, keywords,
                       exclude_kw, require_context, min_evidence_len,
                       is_active, description, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, [code, lead_type, severity, title_tmpl, keywords,
                      exclude_kw, req_ctx_j, min_ev, is_active, desc, now])
            stats["added"] += 1
            print(f"  ➕ [{code}] 추가: {lead_type} severity={severity}")
        else:
            # 변경 여부 체크
            changed = (
                existing["lead_type"] != lead_type or
                existing["severity"] != severity or
                existing["title_tmpl"] != title_tmpl or
                existing["keywords"] != keywords or
                existing["is_active"] != is_active or
                (existing["min_evidence_len"] or 0) != min_ev
            )
            if changed:
                if not dry_run:
                    conn.execute("""
                        UPDATE alert_rules
                        SET lead_type=?, severity=?, title_tmpl=?, keywords=?,
                            exclude_kw=?, require_context=?, min_evidence_len=?,
                            is_active=?, description=?
                        WHERE rule_code=?
                    """, [lead_type, severity, title_tmpl, keywords,
                          exclude_kw, req_ctx_j, min_ev, is_active, desc, code])
                stats["updated"] += 1
                print(f"  ✏️  [{code}] 업데이트")
            else:
                stats["skipped"] += 1

    # YAML에 없는 규칙 비활성화
    db_codes = {r["rule_code"] for r in conn.execute("SELECT rule_code FROM alert_rules WHERE is_active=1")}
    orphans = db_codes - yaml_codes
    for code in orphans:
        if not dry_run:
            conn.execute("UPDATE alert_rules SET is_active=0 WHERE rule_code=?", [code])
        stats["deactivated"] += 1
        print(f"  ⏸️  [{code}] 비활성화 (YAML에 없음)")

    if not dry_run:
        conn.commit()

    return stats


# ══════════════════════════════════════════════════════════════════════════════
# 엔티티 별칭 → partner_mapping 동기화
# ══════════════════════════════════════════════════════════════════════════════
def sync_aliases(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """
    entity_aliases.yaml의 별칭 목록을 partner_mapping 테이블에 UPSERT.
    canonical 이름 + 각 alias를 partner_name으로 등록.
    """
    data = load_yaml(ALIAS_YAML)
    alias_list = data.get("aliases", [])
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = {"added": 0, "updated": 0, "skipped": 0}

    for entry in alias_list:
        canonical  = entry["canonical"]
        corp_code  = entry.get("corp_code")
        corp_name  = canonical
        is_listed  = 1 if entry.get("is_listed") else 0
        note       = entry.get("note", "alias_sync")
        all_names  = [canonical] + entry.get("aliases", [])

        for name in all_names:
            existing = conn.execute(
                "SELECT * FROM partner_mapping WHERE partner_name=?", [name]
            ).fetchone()

            if not existing:
                if not dry_run and corp_code:
                    conn.execute("""
                        INSERT OR IGNORE INTO partner_mapping
                          (partner_name, corp_code, corp_name, match_type, is_listed, note, created_at)
                        VALUES (?,?,?,?,?,?,?)
                    """, [name, corp_code, corp_name, "alias", is_listed, note, now])
                stats["added"] += 1
            else:
                # corp_code 없으면 업데이트
                if corp_code and not existing["corp_code"]:
                    if not dry_run:
                        conn.execute(
                            "UPDATE partner_mapping SET corp_code=?, corp_name=?, match_type='alias' WHERE partner_name=?",
                            [corp_code, corp_name, name]
                        )
                    stats["updated"] += 1
                else:
                    stats["skipped"] += 1

    if not dry_run:
        conn.commit()

    return stats


# ══════════════════════════════════════════════════════════════════════════════
# 통계 출력
# ══════════════════════════════════════════════════════════════════════════════
def print_stats(conn: sqlite3.Connection):
    print("=" * 60)
    print("  alert_rules 현황")
    print("=" * 60)

    rows = conn.execute("""
        SELECT rule_code, lead_type, severity, is_active,
               json_array_length(keywords) as kw_cnt
        FROM alert_rules
        ORDER BY is_active DESC, severity DESC, lead_type
    """).fetchall()

    type_groups: dict[str, list] = {}
    for r in rows:
        lt = r["lead_type"]
        type_groups.setdefault(lt, []).append(r)

    for lt, group in type_groups.items():
        active = sum(1 for r in group if r["is_active"])
        print(f"\n  [{lt}] — {active}개 활성 / {len(group)}개 전체")
        for r in group:
            status = "✅" if r["is_active"] else "⏸"
            print(f"    {status} [{r['rule_code']}] severity={r['severity']} 키워드={r['kw_cnt']}개")

    total   = len(rows)
    active  = sum(1 for r in rows if r["is_active"])
    print(f"\n  합계: {active}개 활성 / {total}개 전체")

    # YAML과 비교
    if RULES_YAML.exists() and _HAS_YAML:
        data = load_yaml(RULES_YAML)
        yaml_cnt = len(data.get("rules", []))
        yaml_codes = {r["rule_code"] for r in data["rules"]}
        db_codes = {r["rule_code"] for r in rows}
        only_yaml = yaml_codes - db_codes
        only_db = db_codes - yaml_codes
        print(f"\n  YAML 규칙 수: {yaml_cnt}개")
        if only_yaml:
            print(f"  ⚠️  YAML에만 있음 (미동기화): {only_yaml}")
        if only_db:
            print(f"  ⚠️  DB에만 있음 (YAML 제거됨): {only_db}")
        if not only_yaml and not only_db:
            print("  ✅ YAML ↔ DB 동기화 완료")

    print("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# 진입점
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="규칙 엔진 — YAML ↔ DB 동기화")
    parser.add_argument("--stats",         action="store_true", help="현재 규칙 현황 출력")
    parser.add_argument("--sync",          action="store_true", help="YAML → DB 동기화")
    parser.add_argument("--sync-aliases",  action="store_true", help="엔티티 별칭 → partner_mapping 동기화")
    parser.add_argument("--diff",          action="store_true", help="YAML vs DB 차이점만 확인 (dry-run)")
    args = parser.parse_args()

    if not _HAS_YAML:
        print("PyYAML 미설치. 설치 후 재실행: pip install pyyaml")
        sys.exit(1)

    conn = get_conn()
    try:
        if args.stats or not any([args.sync, args.sync_aliases, args.diff]):
            print_stats(conn)

        if args.diff:
            print("\n[diff] YAML vs DB 차이 확인 (dry-run)")
            stats = sync_rules(conn, dry_run=True)
            print(f"\n결과: 추가 예정 {stats['added']}개 / 업데이트 {stats['updated']}개 / 비활성화 {stats['deactivated']}개")

        if args.sync:
            print("\n[sync] YAML → alert_rules 동기화 중...")
            stats = sync_rules(conn)
            print(f"\n완료: 추가 {stats['added']}개 / 업데이트 {stats['updated']}개 / 비활성화 {stats['deactivated']}개 / 변경없음 {stats['skipped']}개")
            print_stats(conn)

        if args.sync_aliases:
            print("\n[sync-aliases] entity_aliases → partner_mapping 동기화 중...")
            stats = sync_aliases(conn)
            print(f"\n완료: 추가 {stats['added']}개 / 업데이트 {stats['updated']}개 / 변경없음 {stats['skipped']}개")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
