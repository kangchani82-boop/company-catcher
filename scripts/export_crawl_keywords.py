"""
scripts/export_crawl_keywords.py
────────────────────────────────
공급망 DB → 크롤러 친화적 MD 파일 생성.

외부 크롤러가 이 MD를 파싱해 뉴스/공시 수집 대상을 확장하기 위한 키워드 사전.

주요 처리:
  1. 쉼표·세미콜론으로 연결된 복수 기업을 개별 키워드로 분리
  2. 서술형 문장·파싱 오류·placeholder 제거
  3. "(구 xxx)", "외 N개사", " 등" 등 수식어 제거
  4. 영문·한글 별칭 정규화
  5. 관계별 / 참조수별 / 기계파싱용 세 가지 형태로 중복 출력

출력: data/crawl_keywords.md

실행:
  python scripts/export_crawl_keywords.py
  python scripts/export_crawl_keywords.py --min-refs 2
  python scripts/export_crawl_keywords.py --output data/my.md
"""

import io
import re
import sys
import sqlite3
import argparse
from pathlib import Path
from datetime import date

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
OUTPUT_PATH = ROOT / "data" / "crawl_keywords.md"


# ══════════════════════════════════════════════════════════════════════════════
# 1. 노이즈 필터
# ══════════════════════════════════════════════════════════════════════════════

# 거절 키워드 — 이런 단어 포함하면 바로 탈락 (서술형/placeholder)
BLOCK_SUBSTRINGS = [
    "습니다", "합니다", "됩니다", "하였", "되었", "있으며", "있음", "있어",
    "있으나", "없음", "해당없음", "미명시", "미공개", "비공개", "해당사항",
    "기타", "포함한", "포함하여", "같은", "위한", "대한", "따라서", "그러나",
    "또한", "하지만", "이러한", "이와 같은", "당사는", "당사의", "회사는",
    "구축하", "구축된", "판단됩", "전망", "예상", "추정",
    "등의 ", " 등을 ", "등과 ", "등에서", "등으로",
    " 외 ", " 및 ", " 또는 ",
    "대형할인점", "대상으로", "경우에 ",
]

# 아예 이 문자로 시작하거나 끝나면 탈락
BAD_START = (")", "]", ".", ",", "-", "*", "ㅇ", "●", "•", "※", "•", "-",
             "고성능", "고내열", "고객사의", "이마트 등", "경쟁", "주요 고객", "주요 공급")
BAD_END = ("을", "를", "이", "가", "은", "는", "의", "에", "에서", "에게",
           "으로", "로", "와", "과", "및", "등", "하고", "된다", "한다",
           ":", ";", "-", ".", ",")

# 완전 일치 블랙리스트
EXACT_BLACKLIST = {
    "기타", "미명시", "미공개", "비공개", "없음", "해당없음", "N/A", "n/a",
    "불명", "미상", "미정", "-", "기타 고객사", "기타 공급사", "기타 파트너",
    "기타 경쟁사", "미상 기업", "고객사", "공급사", "파트너사", "경쟁사",
    "다수", "다수의 고객사", "다수 고객사", "내수", "수출", "국내", "해외",
    "국내외", "전 세계", "전세계",
}

# 너무 일반적인 단어들 (단독으로는 가치 없음)
GENERIC_ONLY = {
    "은행", "증권", "보험", "카드", "건설", "화학", "제약", "바이오",
    "전자", "반도체", "통신", "에너지", "자동차", "철강", "조선",
}


def _strip_suffix(s: str) -> str:
    """'(구 xxx)', '외 N개사', ' 등' 등 수식어 제거."""
    # "(구 xxx)" → 제거
    s = re.sub(r"\s*\(\s*구\s+[^)]+\)\s*", "", s)
    # "(英문명)" → 제거 (한글과 영문 병기)
    s = re.sub(r"\s*\(\s*[A-Za-z][^)]*\)\s*$", "", s)
    # "외 N개사", "외 N사" → 제거
    s = re.sub(r"\s+외\s*\d+\s*개?사?\s*$", "", s)
    s = re.sub(r"\s+외\s*$", "", s)
    # 끝의 " 등", " 등등" → 제거
    s = re.sub(r"\s+등+\s*$", "", s)
    # 끝의 " / 설명" → "/" 앞만
    s = re.sub(r"\s*/\s*.+$", "", s)
    # 끝의 "(미명시)" → 제거
    s = re.sub(r"\s*\([^)]*미명시[^)]*\)\s*$", "", s)
    # 끝의 ":xxx" 라벨 → 제거
    s = re.sub(r"\s*:\s*[^:]+$", "", s)
    return s.strip()


def _normalize(s: str) -> str:
    """앞뒤 공백·특수문자 정리."""
    s = s.strip()
    s = re.sub(r'^["\'"\s()\[\]*•●※ㅇ>\-]+', "", s)
    s = re.sub(r'["\'"\s()\[\]*•●※>\-]+$', "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _is_valid(s: str) -> bool:
    """정제 후 키워드가 유효한지 판정."""
    if not s or len(s) < 2 or len(s) > 60:
        return False

    if s in EXACT_BLACKLIST:
        return False

    # 전부 특수문자/공백
    if re.match(r"^[\s\W_]+$", s):
        return False

    # 숫자/기호만
    if re.match(r"^[\d\s\.\,\-%]+$", s):
        return False

    # 차단 부분 문자열 포함
    for blk in BLOCK_SUBSTRINGS:
        if blk in s:
            return False

    # 나쁜 시작/끝
    for bs in BAD_START:
        if s.startswith(bs):
            return False
    for be in BAD_END:
        if s.endswith(be) and len(s) > len(be) + 1:
            # 단 "LG" 같은 2글자는 허용 필요 없고, 3글자 이상에서 체크
            if len(s) >= 4:
                return False

    # 공백이 너무 많음 (4개 이상 = 5단어 이상 = 문장 가능성)
    if s.count(" ") >= 4:
        return False

    # 한국어 서술형 끝 (ex: "~했다", "~되었다")
    if re.search(r"[가-힣](다|음|임|됨|함)$", s):
        return False

    # 괄호 짝 안 맞음
    if s.count("(") != s.count(")"):
        return False

    # 너무 일반적인 단어
    if s in GENERIC_ONLY:
        return False

    # 영문 대문자+공백+산발적 소문자 (단어 나열 가능성)
    if re.match(r"^[A-Z]+\s+[a-z]", s) and s.count(" ") >= 2:
        # "AWS Amazon Web" 같은 건 OK, 그러나 문장형 제외는 위에서 처리
        pass

    return True


def _split_multi(raw: str) -> list[str]:
    """쉼표·세미콜론·슬래시로 연결된 복수 기업 분리."""
    # 괄호 안 쉼표는 보존하기 위해 일시 치환
    protected = re.sub(r"\(([^)]*)\)",
                       lambda m: "(" + m.group(1).replace(",", "§") + ")",
                       raw)
    # 쉼표·세미콜론·슬래시(공백 낀 경우만)로 분리
    parts = re.split(r"[,;]|\s/\s", protected)
    # 복원
    return [p.replace("§", ",") for p in parts]


def clean_keywords(raw: str) -> list[str]:
    """
    원본 partner_name에서 0개 이상의 유효 키워드를 추출.
    - 쉼표 나열 분리
    - 수식어 제거
    - 정규화
    - 유효성 검사
    """
    if not raw:
        return []
    candidates = _split_multi(raw)
    out: list[str] = []
    for c in candidates:
        c = _strip_suffix(c)
        c = _normalize(c)
        if _is_valid(c):
            out.append(c)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 2. 키워드 집계
# ══════════════════════════════════════════════════════════════════════════════

def collect(conn: sqlite3.Connection, min_refs: int) -> tuple[dict, dict]:
    """
    Returns:
      by_relation: {rtype: [{name, ref_count, corp_code, is_listed, sample_hubs}]}
      master:      [{name, total_refs, relations, corp_code, is_listed}]
    """
    rows = conn.execute("""
        SELECT sc.relation_type, sc.partner_name, sc.corp_name AS hub_name,
               pm.corp_code AS pm_code, pm.is_listed AS pm_listed
        FROM supply_chain sc
        LEFT JOIN partner_mapping pm ON pm.partner_name = sc.partner_name
    """).fetchall()

    # (rtype, name) → {ref_hubs: set, corp_code, is_listed}
    grouped: dict[tuple, dict] = {}
    for r in rows:
        for name in clean_keywords(r["partner_name"]):
            key = (r["relation_type"], name)
            g = grouped.setdefault(key, {
                "ref_hubs": set(),
                "corp_code": None,
                "is_listed": 0,
            })
            g["ref_hubs"].add(r["hub_name"] or "")
            if r["pm_code"]:
                g["corp_code"] = r["pm_code"]
            if r["pm_listed"]:
                g["is_listed"] = 1

    by_relation: dict[str, list[dict]] = {
        "customer": [], "supplier": [], "partner": [], "competitor": []
    }
    for (rtype, name), g in grouped.items():
        if rtype not in by_relation:
            continue
        rc = len(g["ref_hubs"])
        if rc < min_refs:
            continue
        by_relation[rtype].append({
            "name": name,
            "ref_count": rc,
            "corp_code": g["corp_code"] or "",
            "is_listed": g["is_listed"],
            "sample_hubs": sorted(g["ref_hubs"])[:3],
        })

    for k in by_relation:
        by_relation[k].sort(key=lambda x: (-x["ref_count"], x["name"]))

    # 마스터 인덱스 (모든 관계 합산)
    master_map: dict[str, dict] = {}
    for rtype, items in by_relation.items():
        for it in items:
            m = master_map.setdefault(it["name"], {
                "name": it["name"],
                "total_refs": 0,
                "relations": {},
                "corp_code": it["corp_code"],
                "is_listed": it["is_listed"],
            })
            m["total_refs"] += it["ref_count"]
            m["relations"][rtype] = it["ref_count"]
            if it["corp_code"] and not m["corp_code"]:
                m["corp_code"] = it["corp_code"]
            if it["is_listed"]:
                m["is_listed"] = 1

    master = sorted(master_map.values(),
                    key=lambda x: (-x["total_refs"], x["name"]))

    return by_relation, master


# ══════════════════════════════════════════════════════════════════════════════
# 3. 엔티티 별칭 로드
# ══════════════════════════════════════════════════════════════════════════════

def load_aliases() -> dict[str, list[str]]:
    path = ROOT / "rules" / "entity_aliases.yaml"
    if not path.exists():
        return {}
    try:
        import yaml
    except ImportError:
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {e["canonical"]: e.get("aliases", []) for e in data.get("aliases", [])}


# ══════════════════════════════════════════════════════════════════════════════
# 4. MD 렌더링 (크롤러 친화적)
# ══════════════════════════════════════════════════════════════════════════════

def render_md(by_relation: dict, master: list[dict],
              aliases: dict, min_refs: int) -> str:
    today = date.today().isoformat()
    L: list[str] = []

    # ── 헤더 ────────────────────────────────────────────────────────────────
    L += [
        "# 공급망 크롤링 키워드 사전",
        "",
        f"> **생성일:** {today}  ",
        f"> **소스:** DART 사업보고서 AI 추출 (supply_chain 테이블)  ",
        "> **용도:** 외부 크롤러가 뉴스·공시 수집 대상을 이 키워드로 확장",
        "",
        "---",
        "",
        "## 📖 크롤러 파싱 가이드",
        "",
        "이 문서는 세 가지 방식으로 파싱 가능합니다:",
        "",
        "1. **TSV 블록** (섹션별 ` ```tsv ... ``` `) — 가장 기계 친화적",
        "2. **JSON 블록** (`## JSON_DATA` 섹션) — 전체 구조 일괄 로드",
        "3. **마크다운 표** — 사람 열람용",
        "",
        "### 필드 설명",
        "",
        "| 필드 | 설명 |",
        "|------|------|",
        "| `name` | 키워드 (회사명/기관명) |",
        "| `ref_count` | 이 키워드를 언급한 상장사 수 (많을수록 허브) |",
        "| `relations` | 관계 유형 (customer/supplier/partner/competitor) |",
        "| `is_listed` | DART 상장 여부 (1/0) |",
        "| `corp_code` | DART 8자리 공시코드 (있는 경우만) |",
        "",
        "### 추천 크롤링 우선순위",
        "",
        "1. `ref_count ≥ 5` AND `is_listed = 1` — 시장 영향력 큰 상장 허브",
        "2. `corp_code` 있는 키워드 — DART 공시 직접 매칭 가능",
        "3. `customer`/`supplier` 관계 — 수주·수출 뉴스 가치 큼",
        "4. `competitor` — 풍문 리스크 고려해 보완 키워드로만",
        "",
        "---",
        "",
    ]

    # ── 통계 ────────────────────────────────────────────────────────────────
    totals = {k: len(v) for k, v in by_relation.items()}
    L += [
        "## 📊 통계",
        "",
        f"- **유니크 키워드(전체):** {len(master):,}개",
        f"- **고객사(customer):** {totals.get('customer', 0):,}개",
        f"- **공급사(supplier):** {totals.get('supplier', 0):,}개",
        f"- **파트너(partner):** {totals.get('partner', 0):,}개",
        f"- **경쟁사(competitor):** {totals.get('competitor', 0):,}개",
        f"- **필터:** 최소 참조 {min_refs}회",
        "",
        "---",
        "",
    ]

    # ── 1. TSV 평문 블록 (기계 파싱 최우선) ──────────────────────────────────
    L += [
        "## 📋 TSV 평문 블록 (기계 파싱용)",
        "",
        "각 섹션을 탭(`\\t`) 구분자로 파싱하세요. 주석은 `#`로 시작합니다.",
        "",
    ]

    for rtype in ["customer", "supplier", "partner", "competitor"]:
        items = by_relation.get(rtype, [])
        if not items:
            continue
        L += [
            f"### ▶ {rtype.upper()} — {len(items):,}개",
            "",
            f"```tsv",
            f"# name\tref_count\tis_listed\tcorp_code",
        ]
        for it in items:
            L.append(f"{it['name']}\t{it['ref_count']}\t{it['is_listed']}\t{it['corp_code']}")
        L += ["```", ""]

    L += ["---", ""]

    # ── 2. 핵심 허브 표 (사람 열람용) ────────────────────────────────────────
    L += [
        "## 🎯 핵심 허브 키워드 (총 참조 3+)",
        "",
        "여러 기업/여러 관계에서 반복 등장하는 허브 엔티티.",
        "",
        "| 키워드 | 총 참조 | C / S / P / Cmp | 상장 | corp_code |",
        "|--------|--------:|----------------:|:----:|:----------|",
    ]
    top = [m for m in master if m["total_refs"] >= 3]
    for m in top:
        r = m["relations"]
        rel_str = f"{r.get('customer',0)} / {r.get('supplier',0)} / {r.get('partner',0)} / {r.get('competitor',0)}"
        listed = "✅" if m["is_listed"] else ""
        code = m["corp_code"] or ""
        L.append(f"| {m['name']} | {m['total_refs']} | {rel_str} | {listed} | {code} |")
    L += ["", f"> Total: {len(top):,}개 (C=Customer, S=Supplier, P=Partner, Cmp=Competitor)", ""]
    L += ["---", ""]

    # ── 3. 관계별 다중참조 상세 ─────────────────────────────────────────────
    rel_titles = {
        "customer":   "🛒 고객사 (Customer)",
        "supplier":   "🏭 공급사 (Supplier)",
        "partner":    "🤝 파트너 (Partner)",
        "competitor": "⚔️ 경쟁사 (Competitor)",
    }
    for rtype in ["customer", "supplier", "partner", "competitor"]:
        items = by_relation.get(rtype, [])
        if not items:
            continue
        multi = [x for x in items if x["ref_count"] >= 2]
        single = [x for x in items if x["ref_count"] == 1]

        L += [f"## {rel_titles[rtype]}", "",
              f"- 총 **{len(items):,}**개 | 다중참조 {len(multi):,}개 | 단일참조 {len(single):,}개",
              ""]

        if multi:
            L += ["### 다중 참조", "",
                  "| 키워드 | 참조 | 상장 | corp_code | 예시 허브 |",
                  "|--------|-----:|:----:|:----------|:----------|"]
            for it in multi:
                listed = "✅" if it["is_listed"] else ""
                code = it["corp_code"] or ""
                samples = ", ".join(it["sample_hubs"])
                L.append(f"| {it['name']} | {it['ref_count']} | {listed} | {code} | {samples[:40]} |")
            L += ["", ""]

        if single:
            L += [f"### 단일 참조 ({len(single):,}개)", "",
                  "<details><summary>펼쳐서 보기</summary>", "", "```", ]
            for it in single:
                L.append(it["name"])
            L += ["```", "", "</details>", "", ""]

        L += ["---", ""]

    # ── 4. 엔티티 별칭 ─────────────────────────────────────────────────────
    if aliases:
        L += ["## 🔀 엔티티 별칭 (동의어 매칭)", "",
              "크롤러는 본문에서 아래 별칭을 만나면 `canonical`로 정규화하세요.",
              "",
              "```tsv",
              "# canonical\taliases (comma-separated)"]
        for canon in sorted(aliases.keys()):
            alist = aliases[canon]
            if alist:
                L.append(f"{canon}\t{','.join(alist)}")
        L += ["```", "", "---", ""]

    # ── 5. JSON 전체 구조 ──────────────────────────────────────────────────
    import json
    json_obj = {
        "generated_at": today,
        "min_refs": min_refs,
        "totals": totals,
        "master": [
            {
                "name": m["name"],
                "total_refs": m["total_refs"],
                "relations": m["relations"],
                "is_listed": m["is_listed"],
                "corp_code": m["corp_code"],
            }
            for m in master
        ],
        "aliases": aliases,
    }
    L += ["## 💾 JSON_DATA (일괄 로드용)", "",
          "크롤러는 이 블록만 추출해 파싱해도 전체 데이터 확보 가능.",
          "",
          "```json",
          json.dumps(json_obj, ensure_ascii=False, indent=2),
          "```",
          ""]

    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════════════════
# 5. 진입점
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="공급망 → 크롤링 키워드 MD 추출")
    p.add_argument("--min-refs", type=int, default=1,
                   help="최소 참조 수 (기본 1)")
    p.add_argument("--output", type=str, default=str(OUTPUT_PATH),
                   help="출력 MD 경로")
    args = p.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    print(f"[1/3] 공급망 DB 로드 및 키워드 정제 중...")
    by_relation, master = collect(conn, min_refs=args.min_refs)
    counts = {k: len(v) for k, v in by_relation.items()}
    print(f"       C={counts['customer']:,}  S={counts['supplier']:,}  "
          f"P={counts['partner']:,}  Cmp={counts['competitor']:,}  "
          f"(유니크 {len(master):,})")

    print(f"[2/3] 엔티티 별칭 로드...")
    aliases = load_aliases()
    print(f"       {len(aliases):,}개 그룹")

    print(f"[3/3] MD 렌더링 및 저장...")
    md = render_md(by_relation, master, aliases, args.min_refs)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")

    print(f"\n완료: {out}")
    print(f"  라인 수: {md.count(chr(10)):,}")
    print(f"  문자 수: {len(md):,}")

    conn.close()


if __name__ == "__main__":
    main()
