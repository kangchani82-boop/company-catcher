"""
dart/article_draft.py
─────────────────────
공급망 데이터 → 투자 기사 초안 자동 생성 (API 호출 없음)

스타일 기준:
  - 헤드라인: "[주체] [수치/동향]…[영향]" 대비·긴장 구조
  - 리드문: 역피라미드, 핵심 팩트 + 수치 1~2문장
  - 본문: 섹션별 역순 (최신→배경), 공시 수치 명시
  - 톤: 평문체, 능동태 우선, 기관 귀속("DART 공시에 따르면")
  - 출처: DART 공시 기반 명시

draft_type:
  - supply_change  : 공급망 변화 감지 기사 (변화 규모·방향 중심)
  - hub_risk       : 허브 기업 시스템 리스크 기사
  - partner_map    : 공급사·고객사 지도 기사 (집중도 분석)
  - news_related   : 뉴스 키워드 → 공급망 관련주 탐색 (향후)
"""

from dart.supply_chain_graph import (
    _get_db, _sector_of, _rcept_sort_key, _rcept_to_label
)


def generate_article_draft(corp_code: str, draft_type: str = "partner_map") -> dict:
    """
    공급망 데이터 → 투자 기사 초안 자동 생성.

    Returns:
        { draft_type, corp_code, corp_name, sector,
          headline, subheadline, lead,
          body_sections, data_points, angle_tips,
          publish_readiness, byline, source_note }
    """
    db = _get_db()

    name_row = db.execute(
        "SELECT DISTINCT corp_name FROM supply_chain WHERE corp_code=? LIMIT 1",
        [corp_code]
    ).fetchone()
    if not name_row:
        db.close()
        return {"error": f"corp_code '{corp_code}' 없음"}

    corp_name = name_row["corp_name"]
    sector = _sector_of(corp_name, corp_code=corp_code)

    if draft_type == "supply_change":
        result = _draft_supply_change(db, corp_code, corp_name, sector)
    elif draft_type == "hub_risk":
        result = _draft_hub_risk(db, corp_code, corp_name, sector)
    elif draft_type == "partner_map":
        result = _draft_partner_map(db, corp_code, corp_name, sector)
    else:
        db.close()
        return {"error": f"지원하지 않는 draft_type: {draft_type}"}

    db.close()
    result.update({
        "draft_type": draft_type,
        "corp_code": corp_code,
        "corp_name": corp_name,
        "sector": sector,
        "byline": "파이낸스코프 고종민 기자",
        "source_note": "※ DART 전자공시 기반 자동 분석. 공시 원문 확인 및 기업 취재 병행 필요.",
    })
    return result


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _hhi_label(hhi: int) -> str:
    if hhi >= 7500: return f"초고집중({hhi:,})"
    if hhi >= 5000: return f"고집중({hhi:,})"
    if hhi >= 2500: return f"중집중({hhi:,})"
    return f"분산({hhi:,})"


def _risk_word(hhi: int) -> str:
    if hhi >= 5000: return "높다"
    if hhi >= 2500: return "중간 수준이다"
    return "낮은 편이다"


# ── 유형별 초안 생성 ──────────────────────────────────────────────────────────

def _draft_supply_change(db, corp_code, corp_name, sector):
    """
    공급망 변화 감지 기사
    스타일: 매일경제·한국경제 산업부 스타일
    - 헤드라인: 변화 방향 + 핵심 업체명 명시
    - 리드: 기간·건수 팩트 우선
    - 본문: 신규/이탈 분리 섹션
    """
    rpts = sorted(set(
        r["source_report"] for r in db.execute(
            """SELECT DISTINCT source_report FROM supply_chain
               WHERE corp_code=?
               AND source_report GLOB '2[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*'""",
            [corp_code]
        ).fetchall()
    ), key=_rcept_sort_key)

    if len(rpts) < 2:
        return {
            "error": "비교 가능한 보고서 2건 이상 없음",
            "tip": "분기·연도별 공시가 2건 이상 축적돼야 변화 감지가 가능합니다.",
        }

    from_r, to_r = rpts[-2], rpts[-1]

    def _ps(rpt):
        return {r["partner_name"]: r["relation_type"] for r in db.execute(
            "SELECT partner_name, relation_type FROM supply_chain WHERE corp_code=? AND source_report=?",
            [corp_code, rpt]
        ).fetchall()}

    fd, td = _ps(from_r), _ps(to_r)
    fs, ts = set(fd), set(td)
    new_p   = list(ts - fs)
    lost_p  = list(fs - ts)
    changed = [p for p in (fs & ts) if fd[p] != td[p]]

    new_supp  = [p for p in new_p  if td.get(p) == "supplier"]
    new_cust  = [p for p in new_p  if td.get(p) == "customer"]
    lost_supp = [p for p in lost_p if fd.get(p) == "supplier"]
    lost_cust = [p for p in lost_p if fd.get(p) == "customer"]

    fr_label = _rcept_to_label(from_r)
    to_label = _rcept_to_label(to_r)
    total_change = len(new_p) + len(lost_p)

    # ── 헤드라인 (매경·한경 스타일: 대비 구조 + 업체명 명시) ──
    if new_supp and lost_supp:
        headline = (f"{corp_name}, 공급망 대대적 재편… "
                    f"신규 공급사 {len(new_supp)}곳 교체")
        subheadline = (f"{new_supp[0]} 등 신규 진입·{lost_supp[0]} 이탈 — "
                       f"원가·공급 안정성 변화 주목")
    elif new_supp:
        headline = (f"{corp_name}, 신규 공급사 {len(new_supp)}곳 전격 편입… "
                    f"공급망 다각화 신호")
        subheadline = f"{', '.join(new_supp[:2])} 등 — {fr_label}→{to_label} 변화"
    elif lost_supp:
        headline = (f"{corp_name}, 핵심 공급사 {len(lost_supp)}곳 이탈 — "
                    f"공급망 리스크 부각")
        subheadline = f"{', '.join(lost_supp[:2])} 등 탈락 — 대체 조달처 확보 여부 관건"
    elif new_cust:
        headline = (f"{corp_name}, 고객사 {len(new_cust)}곳 신규 확보… "
                    f"매출 다변화 기대감")
        subheadline = f"{', '.join(new_cust[:2])} 등 — 수주 잔고 증가 전망"
    elif lost_cust:
        headline = (f"{corp_name}, 주요 고객사 {len(lost_cust)}곳 이탈 — "
                    f"매출 공백 우려")
        subheadline = f"{', '.join(lost_cust[:2])} 탈락 — 하반기 실적 영향 불가피"
    else:
        headline = (f"{corp_name}, {fr_label}→{to_label} 공급망 {total_change}건 변동… "
                    f"구조 재편 진행")
        subheadline = f"신규 {len(new_p)}건·이탈 {len(lost_p)}건·관계변경 {len(changed)}건"

    # ── 리드문 (역피라미드: 핵심 팩트 + 기간 명시) ──
    lead = (
        f"{corp_name}({sector})의 {to_label} DART 공시에서 공급망 대규모 변화가 포착됐다. "
        f"{fr_label} 대비 신규 파트너 {len(new_p)}곳이 진입하고 기존 파트너 {len(lost_p)}곳이 이탈하는 등 "
        f"총 {total_change}건의 공급망 구조 변동이 확인됐다."
    )

    # ── 본문 섹션 ──
    body_sections = []

    if new_supp:
        ns_list = "·".join(new_supp[:4]) + (f" 등 {len(new_supp)}개사" if len(new_supp) > 4 else "")
        body_sections.append({
            "title": "신규 공급사 진입 현황",
            "content": (
                f"DART 공시에 따르면 {corp_name}의 신규 공급사로 {ns_list}가 확인됐다. "
                f"이는 기존 공급망의 다각화 또는 원가 절감 전략 차원으로 해석된다. "
                f"특히 {new_supp[0]}의 경우 {corp_name}의 핵심 소재·부품 조달처로 "
                f"부상할 가능성이 있어 주목된다."
            ),
            "data": new_supp[:5],
        })

    if lost_supp:
        ls_list = "·".join(lost_supp[:3]) + (f" 등 {len(lost_supp)}개사" if len(lost_supp) > 3 else "")
        body_sections.append({
            "title": "이탈 공급사 — 대체 조달 확보 여부 관건",
            "content": (
                f"반면 {ls_list}가 공급망에서 제외됐다. "
                f"이들의 이탈 원인(단가 협상 결렬·납품 불량·전략적 공급처 교체 등)과 "
                f"대체 조달처 확보 여부가 {corp_name}의 단기 원가 경쟁력을 좌우할 전망이다. "
                f"공급 공백이 클수록 생산 차질 리스크로 이어질 수 있다."
            ),
            "data": lost_supp[:5],
        })

    if new_cust:
        nc_list = "·".join(new_cust[:3]) + (f" 등 {len(new_cust)}개사" if len(new_cust) > 3 else "")
        body_sections.append({
            "title": "신규 고객사 — 매출 다변화 긍정 신호",
            "content": (
                f"매출처 측면에서는 {nc_list}가 신규 고객으로 확인됐다. "
                f"고객사 다변화는 특정 거래처 의존도를 낮춰 매출 안정성을 높이는 요인이다. "
                f"신규 고객 매출 비중이 얼마나 빠르게 확대되느냐가 향후 실적의 변수다."
            ),
            "data": new_cust[:3],
        })

    if lost_cust:
        lc_list = "·".join(lost_cust[:3]) + (f" 등 {len(lost_cust)}개사" if len(lost_cust) > 3 else "")
        body_sections.append({
            "title": "이탈 고객사 — 하반기 실적 영향 점검 필요",
            "content": (
                f"반면 {lc_list}가 고객 명단에서 사라졌다. "
                f"이탈 고객사의 과거 발주 규모를 고려할 때 단기 매출 공백이 발생할 수 있다. "
                f"기업 측 해명 취재(계약 만료·경쟁사 교체·수요 감소 여부)를 병행할 필요가 있다."
            ),
            "data": lost_cust[:3],
        })

    if changed:
        body_sections.append({
            "title": "관계 유형 변경 — 전략적 포지셔닝 변화",
            "content": (
                f"기존 파트너 중 {len(changed)}곳의 관계 유형(공급사↔고객사·파트너 등)이 변경됐다. "
                f"공급사에서 고객사로의 전환 등 관계 역전은 사업 구조 변화의 신호일 수 있다."
            ),
            "data": changed[:3],
        })

    # ── 시장 맥락 섹션 (공통 포함) ──
    body_sections.append({
        "title": "시장 맥락 및 투자 시사점",
        "content": (
            f"{sector} 업종의 공급망 재편이 이어지는 가운데, "
            f"{corp_name}의 이번 공급망 변화는 원가 구조·수익성에 직접 영향을 미칠 수 있다. "
            f"공시 데이터 기반 초기 분석인 만큼, 기업 IR 확인 및 추가 취재를 통한 검증이 필요하다."
        ),
        "data": [],
    })

    angle_tips = [t for t in [
        f"신규 공급사 {new_supp[0]}의 {corp_name} 매출 비중·계약 규모 확인" if new_supp else None,
        f"이탈 공급사 {lost_supp[0]}의 주가·실적 영향 추적, 대체 공급처 특정" if lost_supp else None,
        f"신규 고객사 {new_cust[0]}의 발주 규모 및 계약 기간 취재" if new_cust else None,
        f"동종 {sector} 기업들의 동기간 공급망 변화 비교 분석",
        f"{corp_name}의 최근 실적 발표 자료에서 공급망 관련 코멘트 확인",
        "공급망 변화 직후 주가·거래량 변동 패턴 데이터 추출",
    ] if t]

    readiness = "HIGH" if len(body_sections) >= 3 else "MED" if body_sections else "LOW"

    return {
        "headline": headline,
        "subheadline": subheadline,
        "lead": lead,
        "body_sections": body_sections,
        "data_points": {
            "기간": f"{fr_label} → {to_label}",
            "총변화": total_change,
            "신규_파트너": len(new_p),
            "이탈_파트너": len(lost_p),
            "관계_변경": len(changed),
            "신규_공급사": new_supp[:5],
            "이탈_공급사": lost_supp[:5],
            "신규_고객사": new_cust[:3],
            "이탈_고객사": lost_cust[:3],
        },
        "angle_tips": angle_tips,
        "publish_readiness": readiness,
    }


def _draft_hub_risk(db, corp_code, corp_name, sector):
    """
    허브 기업 시스템 리스크 기사
    스타일: 연합뉴스·파이낸셜뉴스 리스크 분석 스타일
    - 헤드라인: 의존 기업 수 + "직격" 표현
    - 리드: 수치 근거 + 파급 경로 명시
    """
    dep_rows = db.execute(
        """SELECT corp_code, corp_name FROM supply_chain
           WHERE partner_name LIKE ?""",
        [f"%{corp_name}%"]
    ).fetchall()

    dep_corps = {r["corp_code"]: r["corp_name"] for r in dep_rows}
    dep_cnt = len(dep_corps)

    dep_sectors = {}
    for cc, cname in dep_corps.items():
        s = _sector_of(cname, corp_code=cc)
        dep_sectors[s] = dep_sectors.get(s, 0) + 1
    top_sectors = sorted(dep_sectors.items(), key=lambda x: -x[1])[:5]

    # 의존 유형 분류
    type_rows = db.execute(
        """SELECT relation_type, COUNT(DISTINCT corp_code) as cnt
           FROM supply_chain WHERE partner_name LIKE ?
           GROUP BY relation_type""",
        [f"%{corp_name}%"]
    ).fetchall()
    type_map = {r["relation_type"]: r["cnt"] for r in type_rows}
    as_supplier = type_map.get("supplier", 0)  # 공급사로 의존
    as_customer = type_map.get("customer", 0)  # 고객사로 의존

    # ── 헤드라인 ──
    headline = (f"[리스크 분석] {corp_name} 공급 차질 시 국내 {dep_cnt}개사 직격 — "
                f"시스템 리스크 현실화 우려")
    subheadline = (f"DART 공시 분석…공급사 의존 {as_supplier}개사·"
                   f"고객사 의존 {as_customer}개사 — "
                   f"{top_sectors[0][0] if top_sectors else '전업종'} 집중")

    # ── 리드문 ──
    lead = (
        f"{corp_name}({sector})이 국내 공급망 생태계에서 {dep_cnt}개사가 의존하는 "
        f"'슈퍼 허브' 역할을 담당하고 있는 것으로 나타났다. "
        f"DART 공시 전수 분석 결과, 이 기업의 생산 차질·재무 이슈 발생 시 "
        f"광범위한 업황 파급이 불가피한 구조다."
    )

    # ── 본문 ──
    sec_desc = "·".join(f"{s}({c}개사)" for s, c in top_sectors[:3])
    top1 = top_sectors[0] if top_sectors else ("해당 업종", dep_cnt)

    body_sections = [
        {
            "title": "의존 기업 현황 — 규모·섹터 분포",
            "content": (
                f"DART 공시 기준 {dep_cnt}개사가 {corp_name}을 공급망 내 핵심 파트너로 명시했다. "
                f"섹터별로는 {sec_desc} 순으로 집중도가 높다. "
                f"특히 {top1[0]} 업종의 경우 {top1[1]}개사가 동시에 의존 중이어서 "
                f"해당 업종 전반의 동반 타격이 우려된다."
            ),
            "data": [{"sector": s, "count": c} for s, c in top_sectors],
        },
        {
            "title": "리스크 시나리오 — 공급 중단·수요 급감 양방향",
            "content": (
                f"{corp_name}이 공급처로 기능하는 {as_supplier}개사의 경우, "
                f"생산 차질 발생 시 원자재·부품 조달이 즉각 타격을 받는다. "
                f"또한 이 기업을 고객사로 두는 {as_customer}개사는 "
                f"수주 급감이라는 반대 방향의 리스크에 노출된다. "
                f"전방(공급)·후방(수요) 양방향 충격이 동시에 발생할 수 있는 구조다."
            ),
            "data": [],
        },
        {
            "title": "과거 유사 사례 — 취재 체크포인트",
            "content": (
                f"{corp_name}의 과거 생산 차질(파업·화재·천재지변) 이력과 "
                f"당시 의존 기업들의 주가·실적 변화를 추적할 필요가 있다. "
                f"대체 공급처 구축 여부, 재고 확보 수준 등이 "
                f"충격 완충 여부를 가르는 핵심 변수다."
            ),
            "data": [],
        },
    ]

    angle_tips = [
        f"{corp_name}의 공급 가능 대체재·경쟁사 현황 파악",
        f"주요 의존사({list(dep_corps.values())[:2]}등) {corp_name} 매출 비중 공시 확인",
        f"과거 {corp_name} 생산 차질 사례와 해당 시점 의존 기업 주가 반응 비교",
        f"{top1[0]} 업종 내 '{corp_name} 없는 공급망' 확보 기업 vs 미확보 기업 투자 시사점",
        f"{corp_name} 신용등급·재무 안정성 최신 리포트 확인",
    ]

    return {
        "headline": headline,
        "subheadline": subheadline,
        "lead": lead,
        "body_sections": body_sections,
        "data_points": {
            "의존_기업수": dep_cnt,
            "공급사_의존": as_supplier,
            "고객사_의존": as_customer,
            "섹터_분포": dict(top_sectors),
            "기업_섹터": sector,
        },
        "angle_tips": angle_tips,
        "publish_readiness": "HIGH" if dep_cnt >= 10 else "MED",
    }


def _draft_partner_map(db, corp_code, corp_name, sector):
    """
    공급사·고객사 지도 기사
    스타일: 한국경제 기업 심층분석 스타일
    - 헤드라인: 숫자 + 집중도(HHI) 기반 리스크 판정
    - 리드: 공급사·고객사 수 + HHI 기반 리스크 등급 명시
    """
    suppliers = db.execute(
        """SELECT partner_name, COUNT(*) as w FROM supply_chain
           WHERE corp_code=? AND relation_type='supplier'
           GROUP BY partner_name ORDER BY w DESC LIMIT 10""",
        [corp_code]
    ).fetchall()
    customers = db.execute(
        """SELECT partner_name, COUNT(*) as w FROM supply_chain
           WHERE corp_code=? AND relation_type='customer'
           GROUP BY partner_name ORDER BY w DESC LIMIT 10""",
        [corp_code]
    ).fetchall()

    supp_names = [r["partner_name"] for r in suppliers]
    cust_names = [r["partner_name"] for r in customers]

    # HHI 계산
    def _calc_hhi(rows):
        total = sum(r["w"] for r in rows)
        if not total: return 0
        return int(sum((r["w"] / total * 100) ** 2 for r in rows))

    cust_hhi = _calc_hhi(customers)
    supp_hhi = _calc_hhi(suppliers)

    # 공급 섹터 분포
    supp_sectors = {}
    for r in suppliers:
        s = _sector_of(r["partner_name"])
        supp_sectors[s] = supp_sectors.get(s, 0) + 1

    cust_risk_word = _risk_word(cust_hhi)
    supp_risk_word = _risk_word(supp_hhi)

    # ── 헤드라인 ──
    if cust_hhi >= 5000:
        headline = (f"{corp_name} 고객 집중도 위험 수위… "
                    f"1개사 의존 구조에 '매출 쏠림' 경고")
        subheadline = (f"고객 HHI {cust_hhi:,} — 상위 거래처 이탈 시 실적 직격탄 우려")
    elif supp_hhi >= 5000:
        headline = (f"{corp_name} 공급사 집중 리스크 포착 — "
                    f"조달 다변화 시급")
        subheadline = (f"공급 HHI {supp_hhi:,} — 단일 공급처 비중 과다, 협상력 열위")
    else:
        headline = (f"{corp_name} 공급망 해부 — "
                    f"조달처 {len(supp_names)}곳·판매처 {len(cust_names)}곳 총정리")
        subheadline = (f"공급 HHI {supp_hhi:,}·고객 HHI {cust_hhi:,} — "
                       f"분산형 안정 구조")

    # ── 리드문 ──
    lead = (
        f"{corp_name}({sector})의 DART 공시 기반 공급망을 전수 분석한 결과, "
        f"원재료·부품 조달처 {len(supp_names)}곳·고객사 {len(cust_names)}곳이 확인됐다. "
        f"고객 집중도(HHI) {cust_hhi:,}으로 리스크가 {cust_risk_word}."
    )

    # ── 본문 ──
    body_sections = []

    if supp_names:
        sec_desc = "·".join(f"{s}({c}곳)" for s, c in
                            sorted(supp_sectors.items(), key=lambda x: -x[1])[:3])
        body_sections.append({
            "title": f"공급처 현황 — 조달 안정성 HHI {_hhi_label(supp_hhi)}",
            "content": (
                f"{corp_name}의 주요 원재료·부품 공급사로 {supp_names[0]}을 필두로 "
                f"{'·'.join(supp_names[1:4])} 등 {len(supp_names)}곳이 확인됐다. "
                f"공급 섹터는 {sec_desc} 순이다. "
                f"공급 집중도(HHI) {supp_hhi:,}으로 조달 리스크가 {supp_risk_word}."
            ),
            "data": supp_names[:10],
        })

    if cust_names:
        top_cust = cust_names[0]
        conc_comment = (
            f"최대 고객 {top_cust} 편중도가 높아 이탈 시 매출 충격이 클 수 있다."
            if cust_hhi >= 5000 else
            f"고객이 {len(cust_names)}개사로 분산돼 있어 단일 거래처 리스크는 낮다."
        )
        body_sections.append({
            "title": f"고객사 현황 — 매출 안정성 HHI {_hhi_label(cust_hhi)}",
            "content": (
                f"매출처로는 {top_cust}을 최대 고객으로 "
                f"{'·'.join(cust_names[1:4])} 등 {len(cust_names)}곳이 파악됐다. "
                f"{conc_comment} "
                f"고객 집중도(HHI) {cust_hhi:,}."
            ),
            "data": cust_names[:10],
        })

    # 투자 시사점
    supp_verdict = "조달 리스크 관리 우수" if supp_hhi < 2500 else "조달 집중 리스크 존재"
    cust_verdict = "고객 다변화 안정" if cust_hhi < 2500 else "고객 집중 취약 구조"
    body_sections.append({
        "title": "투자 시사점 — 공급망 안정성 종합 평가",
        "content": (
            f"{corp_name}의 공급망 구조를 종합하면 "
            f"공급 측면({supp_verdict})·수요 측면({cust_verdict})으로 평가된다. "
            f"특정 거래처 의존도가 높을수록 거래 조건 변경·이탈 시 실적 변동성이 커진다. "
            f"향후 분기 보고서에서 공급망 변화 추이를 지속 모니터링해야 한다."
        ),
        "data": [],
    })

    angle_tips = [t for t in [
        f"최대 공급사 {supp_names[0]}의 단가·납기 변화 추적" if supp_names else None,
        f"최대 고객사 {cust_names[0]}의 {corp_name} 의존 매출 비중 확인" if cust_names else None,
        f"동종 {sector} 경쟁사 대비 HHI 비교 (공급·고객 집중도 벤치마크)",
        "최근 3개 분기 공급망 데이터 추이 분석 (다각화 진행 중인지 확인)",
        "공급사 중 비상장사의 재무 안정성 리스크 점검",
    ] if t]

    return {
        "headline": headline,
        "subheadline": subheadline,
        "lead": lead,
        "body_sections": body_sections,
        "data_points": {
            "공급사_수": len(supp_names),
            "고객사_수": len(cust_names),
            "공급_HHI": supp_hhi,
            "고객_HHI": cust_hhi,
            "공급_섹터": supp_sectors,
            "주요_공급사": supp_names[:5],
            "주요_고객사": cust_names[:5],
        },
        "angle_tips": angle_tips,
        "publish_readiness": "HIGH" if supp_names and cust_names else "MED",
    }
