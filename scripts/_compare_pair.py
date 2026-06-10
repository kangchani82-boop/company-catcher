"""
scripts/_compare_pair.py
────────────────────────
보고서 페어 자동 도출 + 보고서 종류 한국어 라벨 SSOT.

[페어 의미]
- type_a = 출발점 (과거 기준 보고서)
- type_b = 종착점 (최신 비교 대상)
- 시간 흐름: type_a → type_b (과거 → 최신)
- 분석 관점: "type_b(최신)가 type_a(과거) 대비 무엇이 NEW/REMOVED/EXPANDED/SHRUNK/CHANGED인가"

[새 보고서가 들어올 때마다 자동 갱신]
- get_latest_compare_pair(db) 는 DB의 reports 테이블을 보고
  현재 시점에서 (직전, 최신) 페어를 자동 산출한다.
- 새 분기 보고서가 임계 회사 수(min_corp_count, 기본 1000) 이상 들어오면
  자동으로 페어가 다음 단계로 전환된다.

[유지보수]
- 새 보고서 종류 (예: 2027_q1) 추가 시 TYPE_TIME_ORDER / TYPE_KIND_LABEL 양쪽에 한 줄씩만 추가.
"""

from typing import Optional, Tuple


# 보고서 발간 시간 순서 (작을수록 과거)
TYPE_TIME_ORDER = {
    "2025_q1":  1, "2025_h1":  2, "2025_q3":  3, "2025_annual":  4,
    "2026_q1":  5, "2026_h1":  6, "2026_q3":  7, "2026_annual":  8,
    "2027_q1":  9, "2027_h1": 10, "2027_q3": 11, "2027_annual": 12,
    "2028_q1": 13, "2028_h1": 14, "2028_q3": 15, "2028_annual": 16,
}

# AI 프롬프트에 쓰는 한국어 라벨 (사업/분기/반기/3분기)
TYPE_KIND_LABEL = {
    "2025_annual": "사업보고서", "2026_annual": "사업보고서",
    "2027_annual": "사업보고서", "2028_annual": "사업보고서",
    "2025_q3":     "3분기보고서", "2026_q3":    "3분기보고서",
    "2027_q3":     "3분기보고서", "2028_q3":    "3분기보고서",
    "2025_h1":     "반기보고서",  "2026_h1":    "반기보고서",
    "2027_h1":     "반기보고서",  "2028_h1":    "반기보고서",
    "2025_q1":     "1분기보고서", "2026_q1":    "1분기보고서",
    "2027_q1":     "1분기보고서", "2028_q1":    "1분기보고서",
}


def get_latest_compare_pair(db, min_corp_count: int = 1000) -> Optional[Tuple[str, str]]:
    """현재 시점의 (과거=type_a, 최신=type_b) 페어 자동 도출.

    동작:
      DB의 reports 테이블에서 biz_content 보유 회사 수가 min_corp_count 이상인
      report_type 중 가장 최근에 발간된 두 type을 (과거, 최신) 순서로 반환.

    예 (현재 시점):
      ('2025_annual', '2026_q1')  ← 2025년 사업보고서 → 2026년 1분기

    예 (2026_h1이 1000사 이상 들어온 후):
      ('2026_q1', '2026_h1')

    임계 min_corp_count=1000:
      KOSPI+KOSDAQ 약 2700사 중 1000사 이상이 새 보고서를 제출했을 때만 전환.
      발간 초기 며칠 동안 페어가 흔들리지 않게 보호.

    반환:
      (past_type, latest_type)  — 페어가 충분히 확보된 경우
      None                       — 후보가 2개 미만이거나 시간 순서 안전성 실패
    """
    type_keys = list(TYPE_TIME_ORDER.keys())
    placeholders = ",".join("?" * len(type_keys))
    rows = db.execute(
        f"""
        SELECT report_type,
               COUNT(DISTINCT corp_code) AS cnt,
               MAX(rcept_dt)              AS latest
          FROM reports
         WHERE report_type IN ({placeholders})
           AND biz_content IS NOT NULL
           AND LENGTH(biz_content) >= 500
         GROUP BY report_type
         HAVING cnt >= ?
         ORDER BY latest DESC
         LIMIT 2
        """,
        type_keys + [min_corp_count],
    ).fetchall()

    if len(rows) < 2:
        return None

    # sqlite3.Row 또는 tuple 양쪽 지원
    def _t(r):
        try:
            return r["report_type"]
        except (TypeError, IndexError):
            return r[0]

    latest_type = _t(rows[0])
    past_type   = _t(rows[1])

    # 시간 순서 안전장치 — past가 latest보다 늦으면 None
    if TYPE_TIME_ORDER.get(past_type, 0) >= TYPE_TIME_ORDER.get(latest_type, 0):
        return None

    return past_type, latest_type  # (type_a=과거, type_b=최신)


def assert_pair_order(type_a: str, type_b: str) -> None:
    """type_a(과거) < type_b(최신) 시간 순서 강제. 위반 시 ValueError."""
    if type_a not in TYPE_TIME_ORDER:
        raise ValueError(f"알 수 없는 report_type: type_a={type_a}")
    if type_b not in TYPE_TIME_ORDER:
        raise ValueError(f"알 수 없는 report_type: type_b={type_b}")
    if TYPE_TIME_ORDER[type_a] >= TYPE_TIME_ORDER[type_b]:
        raise ValueError(
            f"페어 순서 오류: type_a({type_a})는 과거, type_b({type_b})는 최신이어야 합니다. "
            f"현재: type_a={TYPE_TIME_ORDER[type_a]} >= type_b={TYPE_TIME_ORDER[type_b]}"
        )


if __name__ == "__main__":
    # 단독 실행 시 현재 페어 출력 (디버깅용)
    import sqlite3
    import sys
    from pathlib import Path

    sys.stdout.reconfigure(encoding="utf-8")
    db_path = Path(__file__).parent.parent / "data" / "dart" / "dart_reports.db"
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    pair = get_latest_compare_pair(db)
    if pair is None:
        print("페어 산출 실패 — 후보 부족 또는 임계 미달")
    else:
        a, b = pair
        print(f"현재 페어: type_a={a} ({TYPE_KIND_LABEL[a]}) → type_b={b} ({TYPE_KIND_LABEL[b]})")
    db.close()
