"""
scripts/generate_draft.py
─────────────────────────
story_leads 취재 단서를 기반으로 Gemini 3.1 Pro Preview 로 기사 초안 자동 생성

실행 예시:
  python scripts/generate_draft.py --stats              # 현재 초안 통계
  python scripts/generate_draft.py --lead-id 5          # 특정 lead만 생성
  python scripts/generate_draft.py --limit 10           # 최대 10건 생성
  python scripts/generate_draft.py                      # 새 단서 전체 생성

출력 형식: 연합뉴스·중앙일보 스타일 한국어 기사
           파이낸스코프 고종민 기자 바이라인
모델: Gemini 3.1 Pro Preview (100 RPD, 10 RPM) → fallback: 2.5 Pro
"""

import io
import json
import os
import re
import sqlite3
import sys
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

def _ensure_utf8_io():
    try:
        if hasattr(sys.stdout, "buffer"):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "buffer"):
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
ENV_PATH = ROOT / ".env"

# ── 환경변수 로드 ────────────────────────────────────────────────────────────
def load_env(path: Path):
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


# ── 모델 설정 ────────────────────────────────────────────────────────────────
# 기사 초안 전용 모델 fallback 체인
ARTICLE_MODELS = [
    "gemini-flash-latest",            # 1순위: 최신 alias (자동 최신 버전)
    "gemini-2.5-flash",               # 2순위: Flash 안정
    "gemini-2.5-flash-lite",          # 3순위: 경량·고속
    "gemini-2.0-flash",               # 최종 폴백
]
ARTICLE_RPD   = 100
ARTICLE_DELAY = 7.0  # 10 RPM → 6초 + 여유


# ── DB 연결 ──────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ── Gemini API 호출 (듀얼 키 지원) ───────────────────────────────────────────
def _get_draft_keys() -> list:
    """파싱1 + 파싱2 키 목록"""
    keys = []
    k1 = os.environ.get("GEMINI_API_KEY", "").strip()
    k2 = os.environ.get("GEMINI_API_KEY_2", "").strip()
    if k1: keys.append(k1)
    if k2: keys.append(k2)
    return keys

_draft_key_idx = 0

def call_gemini_article(prompt: str, timeout: int = 120) -> tuple[str, str]:
    """기사 초안 생성. (text, model_name) 반환. 파싱1/파싱2 라운드로빈."""
    global _draft_key_idx
    keys = _get_draft_keys()
    if not keys:
        raise ValueError("GEMINI_API_KEY 없음 — .env 확인")

    last_err = None
    # 모델별로 429 여부 추적 (키 단위가 아닌 모델 단위로 스킵)
    model_exhausted = set()

    for model_name in ARTICLE_MODELS:
        if model_name in model_exhausted:
            continue

        for ki, api_key in enumerate(keys):
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model_name}:generateContent?key={api_key}"
            )
            payload = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": 8192,
                    "temperature": 0.5,
                },
            }).encode("utf-8")

            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                _draft_key_idx = ki
                return data["candidates"][0]["content"]["parts"][0]["text"], model_name
            except urllib.error.HTTPError as e:
                body_err = e.read().decode("utf-8", errors="replace")
                if e.code == 404:
                    last_err = f"모델 없음: {model_name}"
                    model_exhausted.add(model_name)
                    break  # 이 모델은 없음 → 다음 모델로
                if e.code == 429:
                    last_err = f"{model_name} 429 (키{ki+1})"
                    if ki == len(keys) - 1:
                        model_exhausted.add(model_name)
                    continue  # 다음 키 시도
                if e.code in (500, 503):
                    # 서버 일시 오류 → 이 모델 스킵, 다음 모델로
                    last_err = f"{model_name} {e.code} (서버 오류)"
                    model_exhausted.add(model_name)
                    break
                raise RuntimeError(f"Gemini API 오류 {e.code}: {body_err[:300]}")
            except urllib.error.URLError as e:
                raise RuntimeError(f"네트워크 오류: {e.reason}")

    # 모든 모델 소진 — 429인지 서버 오류인지 구분
    if last_err and "429" in last_err:
        raise RuntimeError(f"API 할당량 초과(429) — 모든 모델/키 소진. 마지막 오류: {last_err}")
    raise RuntimeError(f"사용 가능한 모델 없음 — 마지막 오류: {last_err}")


# ══════════════════════════════════════════════════════════════════════════════
# 재무 데이터 분석 및 프롬프트 주입
# ══════════════════════════════════════════════════════════════════════════════

def _get_sector_avg(sector: str, year: int, conn: sqlite3.Connection) -> dict | None:
    """
    업종 평균 지표 반환.
    매출 있는 기업만 포함, 이상치(±300%) 제거.
    """
    if not sector:
        return None
    rows = conn.execute("""
        SELECT f.operating_margin, f.net_margin, f.roe, f.roa,
               f.debt_ratio, f.current_ratio, f.inventory_turnover
        FROM financials f
        JOIN companies c ON c.corp_code = f.corp_code
        WHERE c.sector = ? AND f.fiscal_year = ?
          AND f.report_type = 'annual'
          AND f.revenue IS NOT NULL AND f.revenue > 0
    """, [sector, year]).fetchall()

    if len(rows) < 3:   # 최소 3개사 이상이어야 의미 있음
        return None

    def _trimmed_avg(vals, lo=-300, hi=300):
        clipped = [v for v in vals if v is not None and lo <= v <= hi]
        return sum(clipped) / len(clipped) if clipped else None

    return {
        "corp_count":    len(rows),
        "op_margin":     _trimmed_avg([r["operating_margin"] for r in rows]),
        "net_margin":    _trimmed_avg([r["net_margin"] for r in rows]),
        "roe":           _trimmed_avg([r["roe"] for r in rows], -200, 200),
        "roa":           _trimmed_avg([r["roa"] for r in rows], -100, 100),
        "debt_ratio":    _trimmed_avg([r["debt_ratio"] for r in rows], 0, 2000),
        "current_ratio": _trimmed_avg([r["current_ratio"] for r in rows], 0, 2000),
        "inv_turnover":  _trimmed_avg([r["inventory_turnover"] for r in rows], 0, 500),
    }


def _sector_compare_block(corp_code: str, sector: str, year: int,
                           corp_data: dict, conn: sqlite3.Connection) -> str:
    """업종 평균 대비 해당 기업 위치 분석 블록 생성."""
    avg = _get_sector_avg(sector, year, conn)
    if not avg:
        return ""

    lines = [f"[업종 비교 — {sector} · {year}년 · {avg['corp_count']}개사 평균]"]

    # 바이오·제약은 매출 없는 초기 기업이 많아 마진/ROE 평균이 왜곡됨 → 제외
    MARGIN_SKIP_SECTORS = {"바이오·제약", "바이오", "제약·바이오"}
    skip_margin = any(s in (sector or "") for s in MARGIN_SKIP_SECTORS)

    def _vs(label, corp_val, avg_val, higher_better=True, unit="%"):
        if corp_val is None or avg_val is None:
            return
        diff = corp_val - avg_val
        if higher_better:
            icon = "▲" if diff > 0 else "▼"
            level = "상위" if diff > 0 else "하위"
        else:
            icon = "▲" if diff < 0 else "▼"
            level = "양호" if diff < 0 else "부담"
        lines.append(
            f"  {label:<10}: {corp_val:.1f}{unit} vs 업종평균 {avg_val:.1f}{unit} "
            f"({icon}{abs(diff):.1f}{unit} {level})"
        )

    if not skip_margin:
        _vs("영업이익률", corp_data.get("operating_margin"), avg["op_margin"])
        _vs("ROE",       corp_data.get("roe"),              avg["roe"])
    else:
        lines.append("  * 바이오·제약 업종 특성상 마진·ROE 업종 비교 생략 (초기 기업 왜곡)")
    _vs("부채비율",   corp_data.get("debt_ratio"),       avg["debt_ratio"], higher_better=False)
    _vs("유동비율",   corp_data.get("current_ratio"),    avg["current_ratio"])
    if avg["inv_turnover"] and corp_data.get("inventory_turnover"):
        _vs("재고회전율", corp_data.get("inventory_turnover"), avg["inv_turnover"], unit="회")

    return "\n".join(lines)


def _get_financials(corp_code: str, conn: sqlite3.Connection) -> list[dict]:
    """financials 테이블에서 최근 3년 연간 데이터 로드 (최신연도 우선)."""
    rows = conn.execute("""
        SELECT fiscal_year, fs_div,
               revenue, operating_income, net_income,
               total_assets, total_liabilities, total_equity,
               current_assets, current_liabilities,
               cash, inventory, operating_cf,
               debt_ratio, equity_ratio, current_ratio, cash_ratio,
               operating_margin, net_margin, roe, roa, inventory_turnover
        FROM financials
        WHERE corp_code = ? AND report_type = 'annual'
        ORDER BY fiscal_year DESC, fs_div ASC
        LIMIT 3
    """, [corp_code]).fetchall()
    return [dict(r) for r in rows]


def _yoy(curr, prev) -> float | None:
    """YoY 변화율(%) 계산."""
    if curr is None or prev is None or prev == 0:
        return None
    return (curr - prev) / abs(prev) * 100


def _fmt_amt(val, unit="억") -> str:
    """금액 포맷. 원 단위 → 억원."""
    if val is None:
        return "N/A"
    if unit == "억":
        v = val / 1e8
        if abs(v) >= 10000:
            return f"{v/10000:,.1f}조"
        return f"{v:,.0f}억"
    return f"{val:,.0f}"


def _fmt_pct(val) -> str:
    if val is None:
        return "N/A"
    return f"{val:+.1f}%" if val != 0 else "0.0%"


def _fmt_ratio(val) -> str:
    if val is None:
        return "N/A"
    return f"{val:.1f}%"


def _inventory_signal(rows: list[dict]) -> str:
    """
    재고자산 신호 분석 — 사용자 관점 + 시장 표준 지표 통합.

    [사용자 관점]
    - 매출 정체/감소 + 재고 급증 → 전방 수요 악화, 경영 리스크
    - 매출 성장 + 재고 증가    → 전방 호황 대비, 성장 가속 신호

    [시장 표준 추가]
    - DIO(재고회전일수): 365/재고회전율. 급증 = 재고 소화 둔화
      · 업종 평균 대비 +50% 이상: 강경고
      · 반도체·화학: DIO 120일+ 진입 시 재고평가손실 위험
    - 재고 소진 반등 사이클: 재고 감소 전환 = 다음 분기 매출 회복 선행지표
      (반도체·디스플레이 사이클에서 가장 강력한 바닥 신호)
    - 재고평가손실 리스크: 재고 급증 + 판가 하락 업종(반도체·화학·철강)
    """
    if len(rows) < 2:
        return ""
    curr, prev = rows[0], rows[1]
    inv_curr = curr.get("inventory")
    inv_prev = prev.get("inventory")
    rev_curr = curr.get("revenue")
    rev_prev = prev.get("revenue")
    it_curr  = curr.get("inventory_turnover")   # 매출/재고
    it_prev  = prev.get("inventory_turnover")

    if inv_curr is None or inv_prev is None:
        return ""

    inv_yoy = _yoy(inv_curr, inv_prev)
    rev_yoy = _yoy(rev_curr, rev_prev)
    if inv_yoy is None:
        return ""

    signals = []

    # ── DIO(재고회전일수) 분석 ──
    dio_curr = (365 / it_curr) if it_curr and it_curr > 0 else None
    dio_prev = (365 / it_prev) if it_prev and it_prev > 0 else None
    if dio_curr and dio_prev:
        dio_chg = dio_curr - dio_prev
        if dio_curr > 120 and dio_chg > 30:
            signals.append(
                f"🚨 [DIO 위험] 재고회전일수 {dio_curr:.0f}일 (전년比 {dio_chg:+.0f}일 악화) → "
                f"재고 소화에 {dio_curr:.0f}일 소요. 반도체·화학 등 판가 변동성 큰 업종은 "
                f"재고자산평가손실 직접 타격 가능"
            )
        elif dio_curr > 90 and dio_chg > 20:
            signals.append(
                f"⚠️ [DIO 주의] 재고회전일수 {dio_curr:.0f}일 (전년比 {dio_chg:+.0f}일 증가) → "
                f"재고 소화 속도 둔화. 운전자본 부담 증가"
            )
        elif dio_chg < -20 and dio_curr < 60:
            signals.append(
                f"✅ [DIO 개선] 재고회전일수 {dio_curr:.0f}일 (전년比 {dio_chg:+.0f}일 단축) → "
                f"재고 효율화. 운전자본 절감, 현금전환주기 개선"
            )

    # ── 재고 소진 반등 사이클 신호 ──
    # 직전 연도 재고가 피크 후 이번 연도 감소 시작 → 사이클 바닥 신호
    if len(rows) >= 3:
        oldest = rows[2]
        inv_oldest = oldest.get("inventory")
        if inv_oldest and inv_prev and inv_curr:
            if inv_prev > inv_oldest and inv_curr < inv_prev:
                pct_peak = _yoy(inv_curr, inv_prev)
                signals.append(
                    f"📊 [재고 소진 사이클] 재고 피크({_fmt_amt(inv_prev)}) → "
                    f"소진 전환({pct_peak:+.1f}%) → 반도체·화학 등 사이클 업종의 경우 "
                    f"다음 분기 매출 회복 선행지표. 바닥 통과 여부 주목"
                )

    # ── 방향성 복합 신호 (사용자 관점) ──
    if inv_yoy >= 20 and (rev_yoy is None or rev_yoy < 5):
        signals.append(
            f"⚠️ [재고 경고] 재고자산 {inv_yoy:+.1f}% 급증 vs 매출 {_fmt_pct(rev_yoy)} → "
            f"전방 수요 둔화 가능성. 재고 누적→원가 압박→손상 리스크 경로"
        )
    elif inv_yoy >= 10 and rev_yoy is not None and rev_yoy >= 10:
        signals.append(
            f"✅ [재고 긍정] 재고자산 {inv_yoy:+.1f}% 확대 + 매출 {rev_yoy:+.1f}% 동반 성장 → "
            f"전방 업황 호조 기대, 선제적 재고 확충. 성장 가속 국면"
        )
    elif inv_yoy <= -15 and rev_yoy is not None and rev_yoy >= 5:
        signals.append(
            f"✅ [재고 효율] 재고 {inv_yoy:+.1f}% 감소 + 매출 {rev_yoy:+.1f}% 성장 → "
            f"재고회전율 상승, 운전자본 효율화. FCF 개선 기대"
        )
    elif inv_yoy <= -10 and rev_yoy is not None and rev_yoy < 0:
        signals.append(
            f"⚠️ [수요 위축] 재고 {inv_yoy:+.1f}% + 매출 {rev_yoy:+.1f}% 동반 감소 → "
            f"전방 수요 실질 위축. 생산 축소·감산 대응 가능성"
        )

    return "\n".join(signals)


def _cash_health_signal(rows: list[dict]) -> str:
    """
    현금성자산·부채비율 건전성 신호 분석 — 사용자 관점 + 시장 표준 통합.

    [사용자 관점]
    - 현금 감소 + 부채비율 상승 → 신용/자산 건전성 악화
    - 현금 변동 추이가 핵심 지표

    [시장 표준 추가]
    - OCF 품질(이익의 질): 영업CF / 순이익
      · > 1.0: 비현금비용 고려 시 현금창출력 우수 (감가상각 효과)
      · 0.5~1.0: 보통
      · < 0.5: 이익 대비 현금 부족, 미수금·재고 증가 의심
      · 순이익 양수 + OCF 음수: 매우 위험 (이익의 질 최저)
    - FCF(잉여현금흐름) 프록시: 영업CF (CAPEX 데이터 없으므로 근사)
      · OCF 지속 음수: 자생적 현금 창출 불가, 외부 자금 의존
    - 현금 런웨이: OCF 음수 기업 → 현금 / |분기 소진| 개월 수
      · < 4분기 런웨이: 자금 조달 위험
    - 순현금 판단: 현금성자산 vs 부채총계 비교
      · 현금 > 부채×30%: 방어적 현금 충분
      · 현금 < 유동부채×10%: 단기 유동성 매우 취약
    """
    if len(rows) < 1:
        return ""
    curr = rows[0]
    prev = rows[1] if len(rows) >= 2 else None

    cash_curr = curr.get("cash")
    cash_prev = prev.get("cash") if prev else None
    dr_curr   = curr.get("debt_ratio")
    dr_prev   = prev.get("debt_ratio") if prev else None
    cr_curr   = curr.get("cash_ratio")       # 현금/유동부채 %
    ocf_curr  = curr.get("operating_cf")
    net_curr  = curr.get("net_income")
    liab_curr = curr.get("total_liabilities")
    cl_curr   = curr.get("current_liabilities")

    if cash_curr is None:
        return ""

    cash_yoy = _yoy(cash_curr, cash_prev) if cash_prev else None
    dr_chg   = (dr_curr - dr_prev) if (dr_curr and dr_prev) else None

    signals = []

    # ── OCF 품질 (이익의 질) ──
    if ocf_curr is not None and net_curr is not None and net_curr != 0:
        quality = ocf_curr / net_curr
        if net_curr > 0 and ocf_curr < 0:
            signals.append(
                f"🚨 [이익 품질 최악] 순이익 양수({_fmt_amt(net_curr)})이나 영업CF 음수({_fmt_amt(ocf_curr)}) → "
                f"이익의 상당 부분이 미수채권·재고 등 비현금. 실제 현금 창출력 없음"
            )
        elif quality < 0.5 and net_curr > 0:
            signals.append(
                f"⚠️ [이익 품질 저하] OCF/순이익={quality:.2f} → "
                f"순이익 대비 현금 창출력 부족. 운전자본 증가 또는 매출채권 누적 의심"
            )
        elif quality > 1.5:
            signals.append(
                f"✅ [이익 품질 우수] OCF/순이익={quality:.2f} → "
                f"비현금비용(감가상각) 고려 시 실제 현금 창출력이 이익보다 높음"
            )

    # ── 현금 런웨이 (OCF 음수 기업) ──
    if ocf_curr is not None and ocf_curr < 0 and cash_curr > 0:
        annual_burn = abs(ocf_curr)
        runway_months = (cash_curr / annual_burn) * 12
        if runway_months < 12:
            signals.append(
                f"🚨 [런웨이 위험] 현금 {_fmt_amt(cash_curr)} / 연간 소진액 {_fmt_amt(annual_burn)} → "
                f"현금 런웨이 {runway_months:.0f}개월. 12개월 이내 자금 조달 또는 비용 절감 필수"
            )
        elif runway_months < 24:
            signals.append(
                f"⚠️ [런웨이 주의] 현금 런웨이 약 {runway_months:.0f}개월 → "
                f"2년 이내 추가 자금 조달 필요 가능성"
            )

    # ── 순현금/순부채 판단 ──
    if liab_curr and cash_curr:
        cash_to_liab = cash_curr / liab_curr
        if cash_to_liab >= 0.5:
            signals.append(
                f"✅ [순현금 강건] 현금({_fmt_amt(cash_curr)})이 부채({_fmt_amt(liab_curr)})의 "
                f"{cash_to_liab*100:.0f}% → 실질 순현금 방어력 충분. M&A·배당·자사주 여력"
            )
        elif cl_curr and cash_curr < cl_curr * 0.1:
            signals.append(
                f"🚨 [단기 유동성 위험] 현금({_fmt_amt(cash_curr)})이 유동부채({_fmt_amt(cl_curr)})의 "
                f"{cash_curr/cl_curr*100:.0f}% → 단기 상환 압박 극도로 높음"
            )

    # ── 종합 현금·부채 추이 신호 (사용자 관점) ──
    if cash_yoy is not None and dr_chg is not None:
        if cash_yoy <= -20 and dr_chg >= 10:
            signals.append(
                f"🚨 [재무 위험] 현금 {cash_yoy:+.1f}% 급감 + 부채비율 {dr_chg:+.1f}%p 상승 → "
                f"자금 압박 심화. 단기차입 확대·유상증자 가능성 점검 필요"
            )
        elif cash_yoy >= 15 and dr_chg <= -5:
            signals.append(
                f"✅ [재무 건전화] 현금 {cash_yoy:+.1f}% 증가 + 부채비율 {dr_chg:+.1f}%p 개선 → "
                f"재무 체력 강화. 주주환원·설비투자 여력 확대 국면"
            )
        elif cash_yoy <= -15:
            signals.append(
                f"⚠️ [현금 감소] 현금성자산 {cash_yoy:+.1f}% → "
                f"설비투자 집중 or 영업CF 악화 원인 확인 필요"
            )
        elif dr_chg >= 20:
            signals.append(
                f"⚠️ [레버리지 확대] 부채비율 {dr_chg:+.1f}%p 상승 (현재 {_fmt_ratio(dr_curr)}) → "
                f"금융비용 증가. 금리 민감도 상승"
            )

    # ── 현금비율 절대 수준 ──
    if cr_curr is not None and cr_curr < 20 and dr_curr is not None and dr_curr > 150:
        signals.append(
            f"⚠️ [유동성 복합 위험] 현금비율 {_fmt_ratio(cr_curr)} + 부채비율 {_fmt_ratio(dr_curr)} → "
            f"단기 상환 능력 취약 + 재무 레버리지 과다. 복합 리스크"
        )

    return "\n".join(signals)


def _profitability_signal(rows: list[dict]) -> str:
    """수익성 추이 신호 — DuPont 분해 관점 포함."""
    if len(rows) < 2:
        return ""
    curr, prev = rows[0], rows[1]
    op_curr  = curr.get("operating_margin")
    op_prev  = prev.get("operating_margin")
    roe_curr = curr.get("roe")
    roe_prev = prev.get("roe")
    roa_curr = curr.get("roa")
    nm_curr  = curr.get("net_margin")
    eq_curr  = curr.get("equity_ratio")   # 자기자본비율 = 1/재무레버리지

    signals = []

    # 영업이익률 추이
    if op_curr is not None and op_prev is not None:
        op_chg = op_curr - op_prev
        if op_chg <= -5 and op_curr < 0:
            signals.append(
                f"🚨 [영업 적자] 영업이익률 {op_prev:.1f}% → {op_curr:.1f}% "
                f"({op_chg:+.1f}%p) → 본업 손실 구조. 고정비 부담 점검 필요"
            )
        elif op_chg <= -5:
            signals.append(
                f"⚠️ [수익성 악화] 영업이익률 {op_chg:+.1f}%p ({op_prev:.1f}%→{op_curr:.1f}%) → "
                f"원가 상승 or 가격 경쟁 심화"
            )
        elif op_chg >= 5:
            signals.append(
                f"✅ [수익성 개선] 영업이익률 {op_chg:+.1f}%p ({op_prev:.1f}%→{op_curr:.1f}%) → "
                f"원가 절감 or 믹스 개선 효과"
            )

    # DuPont 관점: ROE 분해 (순이익률 × 자산회전율 × 레버리지)
    # 자기자본비율 낮음(레버리지 高) + ROE 높음 → 레버리지 의존 성장 경고
    if roe_curr is not None and eq_curr is not None and nm_curr is not None:
        leverage = 100 / eq_curr if eq_curr > 0 else None
        if roe_curr >= 15 and leverage and leverage > 3:
            signals.append(
                f"⚠️ [레버리지 ROE] ROE {roe_curr:.1f}% 우수하나 재무레버리지 {leverage:.1f}배 → "
                f"부채 활용 성장. 금리 상승·경기 둔화 시 ROE 급락 위험"
            )
        elif roe_curr >= 15 and (leverage is None or leverage <= 2):
            signals.append(
                f"✅ [질적 고ROE] ROE {roe_curr:.1f}% + 레버리지 낮음 → "
                f"실제 수익 창출력 기반 고수익. 지속 가능한 성장"
            )
        elif roe_curr is not None and roe_curr < 0:
            signals.append(f"🚨 [자본 훼손] ROE {roe_curr:.1f}% → 순손실로 자기자본 감소 중")

    # ROE 개선 추이
    if roe_curr is not None and roe_prev is not None:
        roe_chg = roe_curr - roe_prev
        if roe_chg >= 5 and roe_curr > 0:
            signals.append(f"✅ [ROE 개선] {roe_prev:.1f}% → {roe_curr:.1f}% ({roe_chg:+.1f}%p)")
        elif roe_chg <= -5:
            signals.append(f"⚠️ [ROE 하락] {roe_prev:.1f}% → {roe_curr:.1f}% ({roe_chg:+.1f}%p)")

    return "\n".join(signals)


def build_financials_block(corp_code: str, conn: sqlite3.Connection) -> str:
    """
    재무 데이터 분석 블록 생성.
    기사 프롬프트에 삽입할 텍스트 반환.
    """
    rows = _get_financials(corp_code, conn)
    if not rows:
        return ""

    # 업종·기업명 조회
    meta = conn.execute(
        "SELECT corp_name, sector FROM companies WHERE corp_code=? LIMIT 1",
        [corp_code]
    ).fetchone()
    sector = meta["sector"] if meta else None

    lines = ["[재무 데이터 — DART 공시 기준]"]

    # ── 연도별 수치 테이블 ──
    lines.append("")
    header = f"{'':6} {'매출':>9} {'영업이익':>9} {'영업이익률':>9} {'순이익':>9} {'ROE':>7} {'부채비율':>8}"
    lines.append(header)
    lines.append("-" * 65)

    for r in reversed(rows):   # 오래된 연도부터 표시
        yr  = r["fiscal_year"]
        rev = _fmt_amt(r.get("revenue"))
        op  = _fmt_amt(r.get("operating_income"))
        opm = _fmt_ratio(r.get("operating_margin"))
        net = _fmt_amt(r.get("net_income"))
        roe = _fmt_ratio(r.get("roe"))
        dr  = _fmt_ratio(r.get("debt_ratio"))
        lines.append(f"{yr}년  {rev:>9} {op:>9} {opm:>9} {net:>9} {roe:>7} {dr:>8}")

    # ── 현금·재고 추이 ──
    lines.append("")
    lines.append(f"{'':6} {'현금성자산':>10} {'현금비율':>8} {'재고자산':>10} {'재고회전율':>9} {'영업CF':>9}")
    lines.append("-" * 55)
    for r in reversed(rows):
        yr   = r["fiscal_year"]
        cash = _fmt_amt(r.get("cash"))
        cr   = _fmt_ratio(r.get("cash_ratio"))
        inv  = _fmt_amt(r.get("inventory"))
        it   = f"{r['inventory_turnover']:.1f}회" if r.get("inventory_turnover") else "N/A"
        ocf  = _fmt_amt(r.get("operating_cf"))
        lines.append(f"{yr}년  {cash:>10} {cr:>8} {inv:>10} {it:>9} {ocf:>9}")

    # ── YoY 변화율 ──
    if len(rows) >= 2:
        curr, prev = rows[0], rows[1]
        yr_curr, yr_prev = curr["fiscal_year"], prev["fiscal_year"]
        lines.append("")
        lines.append(f"[YoY 변화 — {yr_prev}→{yr_curr}]")

        for label, key in [
            ("매출", "revenue"), ("영업이익", "operating_income"),
            ("순이익", "net_income"), ("현금성자산", "cash"), ("재고자산", "inventory"),
        ]:
            yoy = _yoy(curr.get(key), prev.get(key))
            if yoy is not None:
                arrow = "▲" if yoy > 0 else "▼"
                lines.append(f"  {label:<8}: {arrow} {abs(yoy):.1f}%")

    # ── 자동 해석 신호 ──
    inv_sig  = _inventory_signal(rows)
    cash_sig = _cash_health_signal(rows)
    prof_sig = _profitability_signal(rows)
    all_sigs = "\n".join(s for s in [inv_sig, cash_sig, prof_sig] if s)

    if all_sigs:
        lines.append("")
        lines.append("[자동 해석 신호 — 기사 작성 참고]")
        lines.append(all_sigs)

    # ── 업종 평균 비교 ──
    if sector and rows:
        latest = rows[0]
        latest_year = latest["fiscal_year"]
        cmp_block = _sector_compare_block(corp_code, sector, latest_year, latest, conn)
        if cmp_block:
            lines.append("")
            lines.append(cmp_block)

    lines.append("")
    lines.append("※ 위 수치는 DART 공시 기준 연결재무제표(없을 시 별도). 원 단위.")

    return "\n".join(lines)


# ── 프롬프트 생성 ─────────────────────────────────────────────────────────────
LEAD_TYPE_KO = {
    "strategy_change": "경영전략 변화",
    "market_shift":    "시장 변화",
    "risk_alert":      "리스크 경보",
    "numeric_change":  "수치 급변",
    "supply_chain":    "공급망 변화",
}

# ══════════════════════════════════════════════════════════════════════════════
# 공통 지침 (모든 유형 공유)
# ══════════════════════════════════════════════════════════════════════════════
_COMMON_RULES = """
[공통 규칙 — v2.1 (팩트 그라운딩 + 헤드라인 강화)]
- 매체: 파이낸스코프 (finscope.co.kr) / 기자: 고종민
- 문체: 연합뉴스·중앙일보 경제면 스타일 (간결, 객관, 사실 중심)

[★ 헤드라인 작성 규칙 — 매우 엄격]
- 30자 이내
- **본문 1단락 첫 문장의 핵심 명사·동사를 그대로 사용** (재구성 금지)
- 추상 명사 단독 사용 금지: "불안/심화/위기/혼란/우려" 등은 구체 수치와 결합 필요
  ✗ 나쁜 예: "○○ 신사업 진출에도 재무 불안 심화"
  ✓ 좋은 예: "○○ 부채비율 100→145%, 신사업 진출 부담 가중"
- 회사명은 정식 명칭의 핵심부를 사용 ("(주)" 제거 가능, 약칭은 본문에 등장한 표기 그대로)
- 헤드라인의 모든 핵심어가 본문 1~2단락에 같은 형태로 등장해야 함
- 동사는 사실 보고형: "추가했다", "증가했다", "감소했다", "전환됐다", "통합했다"
- 추측·평가 동사 금지: "우려된다", "전망된다", "분석된다", "보인다"

[부제 규칙]
- 15자 이내, 제목 보완 (구체 사실 1개 추가)
- 각 문단은 반드시 빈 줄(\\n\\n)로 구분

[★ 절대 규칙 — 위반 시 기사 거절]
1. **출처 필수**: 모든 수치·사실은 옆에 출처 태그 명시
   - "(DART 2025 사업보고서)", "(DART 2025 1분기보고서)", "(financials YoY)"
   - 출처 없는 수치는 절대 생성 금지
2. **외부 지식 금지**:
   - 제공된 [입력 자료]에 없는 정보 사용 금지
   - 일반 업계 상식·역사·과거 사건 임의 인용 금지
   - 단, **공신력 출처**(정부/규제기관/공공통계/리서치사) 인용은 허용
     예: "(통계청 2024 산업통계)", "(한국기업평가 2025-Q1)"
     단 인용은 반드시 해당 기업과 직접 연결되어야 함
3. **추측 표현 제한**:
   - "전망된다·예상된다·추정된다" 등은 반드시 [입력 자료]에 그 추정의 근거가 있을 때만 사용
   - 근거 없는 추측 한 문장 발견 시 그 단락 전체 재작성
4. **방향 일치**:
   - 본문 방향(증가/감소/흑자/적자)이 financials YoY와 일치해야 함
   - financials와 정반대 방향 서술 금지
5. **익명 인용 금지**:
   - "관계자에 따르면", "업계에서는", "전문가들은" 등 출처 불명 인용 금지
6. **헤드라인-리드 합일치**:
   - 헤드라인의 핵심 명사·동사가 본문 1~2단락에 반드시 등장해야 함
   - 헤드라인이 본문 핵심 사실과 무관하거나 정반대면 재작성
""".strip()

# ══════════════════════════════════════════════════════════════════════════════
# v2 7단계 작성 절차 (모든 유형 공통)
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# v2.2 STRAIGHT — 단신·속보형 (5단락 / 1,000~1,200자)
# 사명: "공시 보자마자 즉시 출고 가능, 회사 통화 X"
# ══════════════════════════════════════════════════════════════════════════════
FIVE_STEPS_STRAIGHT = """
[v2.2 STRAIGHT 작성 절차 — 5단락 단신, 회사 통화 없이 출고 가능]
[★ 평가어·해석·전망 표현 단 한 번도 사용 금지 — 위반 시 기사 폐기]

PRE-STEP. Fact Card (출력 안 함, 내부 정리)
  - 무엇이 / 어떻게 / 얼마나 변했나 정확한 사실 5~7개

STEP 1. 리드 (200~250자) — 변화의 실체
  - "[기업명], [지표/사업] [변화]" 패턴
  - 시점 명시: "최근 발표된 ○○보고서에서"
  - 허용 종결어미: "~확인됐다", "~나타났다", "~기록했다"
  - 예: "금호건설이 최근 사업보고서에서 물산업·신재생에너지·주택 리모델링 등 신규 사업 진출 계획을 처음 명시했다."

STEP 2. 변화 직접 발췌 (250~300자) — DART 인용
  - 사업보고서·분기보고서 발췌만
  - 형식: "사업보고서에 따르면 ~"
  - 분석·해석 단 한 문장도 금지

STEP 3. 수치 박스 (200~250자) — financials YoY
  - 출처 강제: "(2025 사업보고서 기준)"
  - 형식: "매출 X억(전년 대비 Y% 증가)"
  - 비교 시점 명확히
  - 단 한 줄도 평가 추가 금지 (수치만)

STEP 4. 회사 공식 입장 (200자) ★★★ 가장 자주 위반되는 단락
  - **반드시 "회사 측은 ○○라고 밝혔다" 형식**
  - 보고서·공시·IR에 명시된 회사 측 발표만 직접 인용
  - **이 단락에서 "예상", "기대", "분석", "전망" 단어가 등장하면 즉시 폐기**
  - 회사 측 입장이 보고서에 명시되어 있지 않으면 **이 단락 생략**
  - 외부 미디어 인용 금지 (원칙)
  - ※ 예외: 내용 부족 시 T1 매체 1건만 "○○일보 보도에 따르면" 형식으로

STEP 5. 변화 요약 (200~250자) ★ 가장 자주 위반되는 단락
  - **다음 표현 단 한 번도 사용 금지 (자동 폐기 사유):**
    - "긍정적", "부정적", "우려", "주목", "기대"
    - "예상된다", "전망된다", "분석된다", "보인다", "풀이된다"
    - "가능성", "여력", "잠재력", "효과"
    - "기여할", "강화할", "개선할", "성장할" + 미래형
  - 허용 표현:
    - "이번 변화로 ○○가 X%에서 Y%로 변동됐다"
    - "○○ 사업 비중이 줄었고 △△ 사업이 추가됐다"
    - "보고서에는 ○○에 대한 언급이 새로 추가됐다"
  - **STEP 5는 사실의 단순 요약·정리이며 미래·분석 금지**

[★ 자가 검증 (출력 직전 필수)]
  본문에 다음 단어가 **하나라도** 있으면 그 단락 다시 작성:
  - 평가어: 긍정적, 부정적, 우려, 주목, 기대, 획기적
  - 해석: 풀이된다, 분석된다, 보인다, 평가된다
  - 추측: 예상된다, 전망된다, 추정된다, 기대된다, 가능성, 여력, 잠재력
  - 미래형: ~할 것이다, ~기여할, ~강화할, ~제고할, ~개선할

[★ 자수: 1,000~1,200자 (5단락 합산). 미달 시 STEP 2·3 사실 보강]

[★ 문체 — 신문 기사 표준 반말체]
  - "~확인됐다", "~기록했다", "~밝혔다", "~발표했다", "~명시됐다"
  - 모든 수치에 출처 태그
""".strip()


SEVEN_STEPS_RULE = """
[작성 절차 — 7단계 강제, 위반 시 거절]

PRE-STEP. Fact Card 내부 작성 (출력 X, 모델 내부에서만)
  - [입력 자료]에서 검증 가능한 사실 5~10개를 카드로 정리
  - 각 사실에 [출처 태그] 부여: DART_연차, DART_분기, financials, ai_compare, supply_chain, 공신력
  - 카드에 없는 사실은 본문에 절대 등장 금지

STEP 1. 단서(Evidence) 직접 인용 — 1단락 (200~300자)
  - [근거 문장]을 그대로 또는 직접 해석으로 인용
  - 변화의 핵심 신호 한 줄 요약
  - 출처: "(DART 2025 1분기보고서 발췌)" 등 명시

STEP 2. 비교분석 핵심 변화 — 2단락 (300~400자)
  - [AI 비교분석 발췌]의 "핵심 변화 요약"을 기사 언어로
  - "사업보고서 → 분기보고서" 형식의 전후 대비 명확히
  - 4축 중 해당 축만 (사업/수치/리스크/전략)

STEP 3. 실측 수치 검증 — 3단락 (250~350자)
  - [재무 데이터]의 절대값 + YoY 변동률만 사용
  - 형식: "매출 1,234억 원 (전년 1,000억 원 → +23.4%, 2025 사업보고서)"
  - 추정/예상 표기 금지, 실제 financials 수치만

STEP 4. 기업가치 영향 4채널 평가 — 4단락 (400~500자) ★ 핵심
  ① 매출 영향: 사업 변화가 매출 채널에 미치는 영향
  ② 이익 영향: 마진/원가/판관비 영향
  ③ 주주가치: 자본효율성/배당 가능성/희석 가능성
  ④ 리스크: 부채/유동성/규제/공급망
  → 각 채널마다 최소 1개의 수치 인용 + 출처 표기

STEP 5. 시사점 — 5단락 (200~300자)
  - [AI 비교분석 발췌]에 명시된 시사점만 인용
  - 새로운 해석·전망 추가 금지
  - 공신력 출처 인용 가능 (단, 해당 기업과 직접 연결될 때만)

STEP 6. 헤드라인-리드 합일치 — 출력 직전 자체 검증
  - headline의 핵심 명사·동사가 STEP 1~3 본문에 등장하는가?
  - headline 방향(↑/↓)이 STEP 3 수치와 일치하는가?
  - 불일치 발견 시 headline 재작성

STEP 7. Self-Check — 출력 직전 자체 검증
  - 본문에 출처 미명시 수치/사실 있는가? → 삭제 또는 출처 추가
  - "전망/예상/추정/우려" 표현이 근거 없이 사용됐는가? → 인용형 변환
  - [입력 자료]에 없는 명사·기업명·제품명 있는가? → 삭제
""".strip()


# ══════════════════════════════════════════════════════════════════════════════
# v3 8단계 작성 절차 — REPORTER MODE (한국 증권부 에이스 기자 관점)
# ══════════════════════════════════════════════════════════════════════════════
EIGHT_STEPS_RULE_V3 = """
[v3 작성 절차 — 8단계 + 부속 취재 노트, 위반 시 거절]
[★ 이 기사는 한국 증권부 에이스 기자가 작성한다는 가정으로 — 단순 팩트 정리 X]

PRE-STEP. 기자 Fact Card (출력 안 함, 본문 작성 전 내부 정리)
  - WHO   : 누가 결정했나 (이사회/CEO/자회사)
  - WHAT  : 무엇이 바뀌었나 (한 줄 요약)
  - WHEN  : 언제 (보고서 발행일 + 시장 인지 시점 차이)
  - WHERE : 어느 사업·시장에 영향
  - WHY   : 왜 지금 (가설 1~3개, 자료 근거)
  - HOW   : 어떻게 (자금·조직·일정)
  - SO WHAT: 시장이 봐야 할 의미 (한 줄)

STEP 1. 리드 (300자) — 핵심 변화 + 시점성 ★ 반말체
  - 첫 문장: 무엇이 어떻게 바뀌었나 (능동형)
  - 둘째 문장: 시점 (today와의 차이) + 출처
  - 셋째 문장: 시장 맥락 (왜 이 변화가 의미 있는가)
  - 종결어미: "~로 확인됐다", "~한 것으로 나타났다", "~로 풀이된다"
  - 예: "한국기업평가가 ESG 사업 진출에 시동을 걸었다.
        지난 X월 발표된 분기보고서에서 처음 확인된 것으로,
        주력인 신용평가 시장 둔화에 대응한 행보로 풀이된다."

STEP 2. 변화의 실체 (400자) — 사업보고서 vs 분기보고서
  - 무엇이 신규로 등장 / 무엇이 사라짐
  - 정관 변경·조직 개편·자회사 움직임
  - 출처: (DART 2025 사업보고서 vs 1분기보고서)

STEP 3. 수치로 보는 변화 (350자) — financials YoY ★ 반말체
  - 절대값 + 변동률 + 출처
  - 형식: "매출 1,234억 원 (전년 1,000억 원 대비 23.4% 증가, 2025 사업보고서 기준)"
  - 추정 금지, 실측치만

STEP 4. 기업가치 4채널 (500자) — 매출/이익/주주/리스크
  - 각 채널마다 최소 1개 수치 + 출처 표기
  - 종결어미: "~로 분석된다", "~할 수 있다", "~가 우려된다"

STEP 5. 외부 시각 (300자) — T1 미디어 + 리서치
  - today에 가까운 보도 우선 인용
  - 형식: "○○일보 (2026-04-XX) 보도에 따르면, ~"
  - 외부 자료 없으면 이 단락 짧게 마무리

STEP 6. 핵심 질문 (250자) ★★★ 신규 — 기자가 던지는 질문
  - "시장이 주목해야 할 포인트는 ○가지다." 식 도입
  - 미확인 팩트 / 추정 가능 / 회사 답변이 필요한 것 명시
  - 예: "시장이 주목해야 할 포인트는 세 가지다.
        첫째, ESG 서비스의 첫 매출 시점은 공시되지 않았다.
        둘째, 한국신용평가의 대응 카드도 미확정이다.
        셋째, 글로벌 평가사와의 협업 가능성은 IR에서 답변해야 한다."

STEP 7. 다음 체크포인트 (200자) ★★★ 신규 — 후속 모니터링
  - 구체적 시점·이벤트·지표 명시
  - 예: "다음 분기 보고서에서 자회사 매출 부문이 처음 공개될 전망이다.
        Y월 예정된 IR 데이에서 ESG 사업 로드맵 발표 여부를 주목할 필요가 있다."

STEP 8. 헤드라인 + Self-Check
  - 헤드라인은 STEP 1 첫 문장의 핵심어를 그대로
  - Self-Check 7대 룰 (v2 동일) 적용

═══════════════════════════════════════════
[부속] 취재 노트 (REPORTER_BRIEF) — JSON 별도 출력
  ① IR 통화 질문 5개 (회사 IR팀에 직접 물어볼 것)
  ② 추가 확인 필요 사실 (DART 외 출처 필요)
  ③ 경쟁사 비교 포인트 3개
  ④ 다음 분기 체크 지표 5개 (재무·사업)
  ⑤ 잠재적 후속 기사 주제 3개
═══════════════════════════════════════════

[★ 7가지 기자 관점 의무 체크리스트 — 미답변 시 재작성]
  1. WHAT  — STEP 1·2에 명확한가?
  2. WHO   — STEP 2 또는 6에 결정 주체 명시?
  3. WHEN  — STEP 1에 시점성 표기?
  4. WHERE — STEP 4에 영향 범위 명시?
  5. WHY ⭐ — STEP 6에 "왜 지금" 가설 제시?
  6. SO WHAT ⭐ — STEP 5·6에 시장 의미 명시?
  7. WHAT NEXT ⭐ — STEP 7에 구체 체크포인트 제시?

[★ 문체 규칙 — 신문 기사 표준 반말체]
  - "~합니다" ❌ → "~한다" ✅
  - "~보입니다" ❌ → "~로 분석된다" ✅
  - "~기록했습니다" ❌ → "~기록했다" ✅
  - "~줄어들었습니다" ❌ → "~감소했다" ✅
  - "~할 것으로 기대됩니다" ❌ → "~로 기대된다" ✅
  - 직접 인용은 존댓말 허용: "○○ 관계자는 'XX한다'라고 말했다."
""".strip()


# ── A. 기본형 (strategy_change · supply_chain) ──────────────────────
STYLE_GUIDE = """
[기사 작성 지침 — 기본형 (전략 변화/공급망)]
앵글: "전략·사업 구조의 변화" → "기업가치 4채널 영향" → "공신력 시사점"

본문 7단계 구조:
  1단락(STEP 1): 단서 evidence 직접 인용 (200~300자)
  2단락(STEP 2): 비교분석 핵심 변화 — 사업 영역/제품/조직 (300~400자)
  3단락(STEP 3): 실측 수치 (매출·자본·자산 등 변화) (250~350자)
  4단락(STEP 4): 기업가치 4채널 영향 평가 ★ 매출/이익/주주/리스크 (400~500자)
  5단락(STEP 5): 시사점 — AI 분석 인용 + 공신력 출처 보강 (200~300자)

총 1,500~2,000자 (5단락 합산), 단락 간 빈 줄 필수.

전략 변화 특화 포인트:
  - 사업 목적 추가/철회는 정관 변경 인용으로 시작 (출처: DART)
  - 자산 처분/취득 시 재무 영향 정량화 (financials 인용)
  - 신사업 진출 시 매출 비중 추정은 AI 분석에 있을 때만
""".strip()

# ── B. 변화 감지 (numeric_change · market_shift) ────────────────────
STYLE_GUIDE_CHANGE = """
[기사 작성 지침 — 변화 감지형 (수치/시장)]
앵글: "무엇이 얼마나 바뀌었나" → "변화의 구조적 의미" → "기업가치 영향"
숫자로 시작하고, 변화 전·후 값을 반드시 병기.

본문 7단계 구조:
  1단락(STEP 1): 단서 evidence 인용 — 어떤 변화가 감지됐나 (200~300자)
    예) "분기보고서에서 영업이익률 X%→Y%로 ○%p 변화 감지 (DART 2025 1분기보고서)"
  2단락(STEP 2): 비교분석 핵심 — 사업보고서 vs 분기보고서 (300~400자)
  3단락(STEP 3): 실측 수치 — financials YoY 변동 (250~350자)
    형식: "매출 1,234억 원 → 1,500억 원 (+21.6%, 2025 사업보고서 vs 1분기 환산)"
  4단락(STEP 4): 기업가치 4채널 영향 ★ (400~500자)
    - 매출 채널: 변화가 매출 동력에 어떤 영향?
    - 이익 채널: 마진 구조에 어떤 영향?
    - 주주가치: ROE/ROA/EPS에 어떤 영향?
    - 리스크: 부채비율/현금흐름/규제 노출에 어떤 영향?
  5단락(STEP 5): 시사점 — AI 분석 시사점 인용 + 업종 통계 보강 (200~300자)

총 1,500~2,000자.

수치 표기 규칙 (절대 준수):
  - 변화 전후 병기: "X억 원 → Y억 원" 또는 "X% → Y% (○○%p ↑/↓)"
  - YoY 표기: "(전년 대비 ○○% 증가/감소, 2025 사업보고서)"
  - 절대값과 비율 모두 financials 테이블 값과 일치 (±5% 이상 차이 시 거절)
""".strip()

# ── C. 리스크 경보 (risk_alert) ──────────────────────────────────────
STYLE_GUIDE_RISK = """
[기사 작성 지침 — 리스크 경보형]
앵글: "리스크 실체" → "기업가치 4채널 충격" → "공급망 파급" → "공신력 시사점"
경보 성격 — 리드에서 리스크의 실체와 규모를 명확히.

본문 7단계 구조:
  1단락(STEP 1): 단서 evidence 인용 — 어떤 리스크가 감지됐나 (200~300자)
  2단락(STEP 2): 리스크 실체 — 비교분석에서 발견된 변화 (300~400자)
  3단락(STEP 3): 직접 재무 영향 — financials 기반 수치 (250~350자)
    예: "부채비율 100% → 145% (+45%p, 2025 사업보고서 기준)"
  4단락(STEP 4): 기업가치 4채널 충격 평가 ★ (400~500자)
    - 매출 영향: 매출처 리스크가 기업 매출에 미치는 영향
    - 이익 영향: 마진 압박 / 비용 증가
    - 주주가치: 자본 잠식 가능성 / 배당 축소
    - 리스크: 유동성 / 부채 / 규제 / 공급망 (★ 거래처 정보 활용)
  5단락(STEP 5): 시사점 — 신용평가사·금융감독원 등 공신력 출처 (200~300자)
    예: "(한국기업평가 2025-Q1 신용 분석 기준 우려 등급)"

총 1,500~2,000자.

리스크 정량화 규칙:
  - 부채비율, 현금비율, 영업이익률 등 명확한 비율 인용
  - 유동성 위험은 "현금 소진 ○개월" 형태 (단 financials로 산정 가능할 때만)
  - 공급망 파급은 supply_chain 거래처 정보가 있을 때만 작성
  - 거래처 정보 없으면 해당 채널 생략 (절대 임의 생성 금지)
""".strip()


# ══════════════════════════════════════════════════════════════════════════════
# 과거 보고서 컨텍스트 빌더 (Phase R) — "이미 알려진 내용" 검증
# ══════════════════════════════════════════════════════════════════════════════
def build_past_reports_context(corp_code: str, comparison_id: int | None,
                               keywords: list[str],
                               conn: sqlite3.Connection) -> str:
    """
    같은 회사의 과거 보고서(현재 비교 대상이 아닌)에서
    핵심 키워드가 어떻게 언급됐는지 발췌해 프롬프트에 주입.
    목적: 모델이 "이번이 처음 등장"인지 "이미 있던 것"인지 판단 가능.
    """
    if not corp_code:
        return ""

    # 비교 대상이었던 보고서 ID 제외
    exclude_ids = []
    if comparison_id:
        cmp = conn.execute(
            "SELECT report_id_a, report_id_b FROM ai_comparisons WHERE id=?",
            [comparison_id]
        ).fetchone()
        if cmp:
            exclude_ids = [x for x in [cmp["report_id_a"], cmp["report_id_b"]] if x]

    placeholders = ",".join("?" * len(exclude_ids)) if exclude_ids else ""
    where = "corp_code=? AND biz_content IS NOT NULL"
    if placeholders:
        where += f" AND id NOT IN ({placeholders})"

    rows = conn.execute(f"""
        SELECT id, report_type, rcept_dt, biz_content
        FROM reports WHERE {where}
        ORDER BY rcept_dt ASC
    """, [corp_code] + exclude_ids).fetchall()

    if not rows:
        return ""

    if not keywords:
        return ""

    # 각 키워드의 시간순 등장 추적
    type_label = {
        "2024_annual": "2024 사업보고서",
        "2024_q3":     "2024 3분기보고서",
        "2024_h1":     "2024 반기보고서",
        "2024_q1":     "2024 1분기보고서",
        "2023_annual": "2023 사업보고서",
        "2023_q3":     "2023 3분기보고서",
        "2023_h1":     "2023 반기보고서",
        "2023_q1":     "2023 1분기보고서",
    }

    lines = ["[★ 과거 보고서 키워드 등장 이력 — 새로움 검증용]"]
    lines.append("이미 같은 키워드가 과거 보고서에 등장했다면 STEP 1에서 '이번이 처음', '신규 등장' 표현 사용 금지.")
    lines.append("")

    has_data = False
    for kw in keywords[:5]:   # 키워드 최대 5개
        if not kw or len(kw) < 2:
            continue
        history = []
        for r in rows:
            cnt = r["biz_content"].count(kw)
            if cnt > 0:
                rt = type_label.get(r["report_type"], r["report_type"])
                history.append(f"{rt}({cnt}회)")
        if history:
            has_data = True
            lines.append(f"  ▶ '{kw}' 등장 이력: {', '.join(history[:6])}")
        else:
            lines.append(f"  ▶ '{kw}' : 과거 보고서에 등장 없음 ★ 신규 진입 가능성 높음")

    if not has_data:
        lines.append("  → 모든 키워드가 과거 보고서에 처음 등장 (진짜 신규)")

    return "\n".join(lines) if has_data else "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# 외부 검증 자료 컨텍스트 빌더 (Phase E)
# ══════════════════════════════════════════════════════════════════════════════
def build_external_sources_context(lead_id: int, conn: sqlite3.Connection,
                                   max_per_tier: int = 5) -> tuple[str, list[int]]:
    """
    lead_external_match 테이블에서 사실검증 PASS/PARTIAL인 자료를 조회해
    프롬프트 STEP 5용 컨텍스트 블록 생성.
    Returns: (block_text, source_ids_used)
    """
    if not lead_id:
        return "", []
    try:
        rows = conn.execute("""
            SELECT es.id, es.outlet_name, es.outlet_tier, es.title, es.summary,
                   es.url, es.published_at, es.source_type,
                   lem.match_score, lem.fact_check_status, lem.fact_check_score
            FROM lead_external_match lem
            JOIN external_sources es ON lem.source_id = es.id
            WHERE lem.lead_id = ?
              AND lem.fact_check_status IN ('PASS', 'PARTIAL')
            ORDER BY
              CASE lem.fact_check_status WHEN 'PASS' THEN 0 ELSE 1 END,
              CASE es.outlet_tier WHEN 'T1' THEN 0 WHEN 'T2' THEN 1
                                  WHEN 'RESEARCH' THEN 2 ELSE 3 END,
              lem.match_score DESC
            LIMIT 30
        """, [lead_id]).fetchall()
    except Exception:
        return "", []

    if not rows:
        return "", []

    # tier별 그룹 + recency 라벨
    today = datetime.now()
    def _recency_label(pub: str | None) -> str:
        if not pub:
            return "(시점 미상)"
        try:
            pd = datetime.strptime(pub[:10], "%Y-%m-%d")
            d = (today - pd).days
            if d <= 30:   return f"⭐⭐⭐ {d}일 전"
            if d <= 60:   return f"⭐⭐ {d}일 전"
            if d <= 90:   return f"⭐ {d}일 전"
            if d <= 180:  return f"({d}일 전)"
            return f"({d}일 전·오래됨)"
        except Exception:
            return f"({pub})"

    buckets: dict[str, list] = {"T1": [], "T2": [], "RESEARCH": []}
    for r in rows:
        t = r["outlet_tier"] if r["outlet_tier"] in buckets else "T2"
        buckets[t].append(r)

    # 각 tier 내에서 recency 우선 정렬
    def _rec_key(r):
        pub = r["published_at"]
        if not pub:
            return 999
        try:
            return (today - datetime.strptime(pub[:10], "%Y-%m-%d")).days
        except Exception:
            return 999
    for t in buckets:
        buckets[t].sort(key=_rec_key)

    lines = ["[★ 외부 검증 자료 — STEP 5 시사점 단락에서만 인용 가능, 본문 STEP 1~4 인용 금지]"]
    lines.append(f"[기사 작성 시점: {today.strftime('%Y-%m-%d')} — today에 가까운 자료 우선 인용]")
    used_ids = []

    label = {
        "T1": "▶ T1 신뢰 매체 (직접 인용 가능, today에 가까운 순)",
        "T2": "▶ T2 산업·전문지 (보강 인용 가능)",
        "RESEARCH": "▶ 리서치 리포트 (메타데이터 인용)",
    }
    for tier in ("T1", "T2", "RESEARCH"):
        items = buckets.get(tier, [])[:max_per_tier]
        if not items:
            continue
        lines.append("")
        lines.append(label[tier])
        for it in items:
            used_ids.append(it["id"])
            pub = it["published_at"] or "날짜 미상"
            rec = _recency_label(it["published_at"])
            verify = it["fact_check_status"]
            verify_score = it["fact_check_score"] or 0
            title_short = (it["title"] or "")[:80]
            summary_short = (it["summary"] or "").replace("\n", " ")[:120]
            lines.append(
                f"  • {it['outlet_name']} ({pub}) {rec} — {title_short}"
            )
            if summary_short:
                lines.append(f"    요약: {summary_short}")
            lines.append(
                f"    [본지 검증: {verify} {verify_score}점, 매칭 {it['match_score']:.2f}]"
            )

    lines.append("")
    lines.append("[STEP 5 인용 규칙 — 매우 엄격, today 최신성 우선]")
    lines.append("- 위 자료는 STEP 5 시사점 단락에서만 인용 (헤드라인·STEP 1~4 절대 금지)")
    lines.append("- ⭐⭐⭐ (30일 이내) 자료가 있으면 무조건 우선 인용")
    lines.append("- 인용 형식: \"매일경제 2026-04-15 보도(본지 자체 검증)\"")
    lines.append("- T1 자료가 있으면 T1 우선, 없으면 T2 인용")
    lines.append("- PASS 자료가 핵심 사실 보강용, PARTIAL은 보조 인용용")
    lines.append("- 자료 내용을 자의적으로 확장 해석 금지 — 제목·요약에 명시된 사실만")
    lines.append("- 180일 초과 자료는 \"역사적 맥락\"으로만 활용, 핵심 인용 금지")

    return "\n".join(lines), used_ids


# ══════════════════════════════════════════════════════════════════════════════
# 공급망 컨텍스트 빌더 (리스크 기사 전용)
# ══════════════════════════════════════════════════════════════════════════════
def build_supply_chain_context(corp_code: str, conn: sqlite3.Connection) -> str:
    """
    supply_chain 테이블에서 해당 기업의 거래처 정보를 조회해
    리스크 기사 프롬프트용 컨텍스트 블록 생성.
    """
    if not corp_code:
        return ""
    try:
        rows = conn.execute("""
            SELECT relation_type, partner_name, context
            FROM supply_chain
            WHERE corp_code = ?
            ORDER BY relation_type, id
            LIMIT 30
        """, [corp_code]).fetchall()
    except Exception:
        return ""

    if not rows:
        return ""

    buckets: dict[str, list] = {"customer": [], "supplier": [], "competitor": [], "other": []}
    for r in rows:
        rtype = r["relation_type"] if r["relation_type"] in buckets else "other"
        buckets[rtype].append((r["partner_name"], r["context"] or ""))

    lines = ["[거래처 정보 — supply_chain DB 기준]"]
    label_map = {
        "customer":   "▶ 주요 고객사 (매출처)",
        "supplier":   "▶ 주요 공급사 (매입처)",
        "competitor": "▶ 주요 경쟁사",
        "other":      "▶ 기타 관계사",
    }
    for rtype, items in buckets.items():
        if not items:
            continue
        lines.append(label_map[rtype])
        for name, ctx in items[:8]:   # 최대 8개
            ctx_short = ctx[:80].replace("\n", " ") if ctx else ""
            lines.append(f"  - {name}" + (f" : {ctx_short}" if ctx_short else ""))

    lines.append(
        "\n※ 위 거래처가 재무 악화 중이거나 사업 축소 시 해당 기업 매출·원가에 "
        "직접 파급될 수 있음을 기사에 반영하세요."
    )
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# 유형별 프롬프트 빌더
# ══════════════════════════════════════════════════════════════════════════════

def _lead_base(lead: sqlite3.Row) -> dict:
    """공통 lead 필드 딕셔너리."""
    keywords = lead["keywords"] or "[]"
    try:
        kw_list = ", ".join(json.loads(keywords))
    except Exception:
        kw_list = keywords
    return {
        "corp_name":  lead["corp_name"] or "해당 기업",
        "corp_code":  lead["corp_code"] or "",
        "lead_type":  lead["lead_type"],
        "type_ko":    LEAD_TYPE_KO.get(lead["lead_type"], lead["lead_type"]),
        "severity":   lead["severity"],
        "title_hint": lead["title"] or "",
        "summary":    lead["summary"] or "",
        "evidence":   lead["evidence"] or "",
        "kw_list":    kw_list,
    }


def _json_output_block() -> str:
    return """\
위 정보를 바탕으로 아래 JSON 형식으로 기사 초안을 작성하세요.
반드시 JSON 형식으로만 출력하세요 (설명 텍스트 앞뒤 추가 금지).

★ 출력 전 자체 검증 필수 (STEP 6 + STEP 7):
  - headline 핵심어가 body의 1~2단락에 등장하는가?
  - body의 모든 수치가 [재무 데이터]에 있는 값인가?
  - body에 출처 미명시 사실이 있는가?
  - body의 방향(증가/감소)이 financials YoY와 일치하는가?
  - "전망/예상/추정" 표현이 근거 없이 사용됐는가?
  → 모두 통과해야 출력 (PRE-STEP fact card는 내부에서만 작성, 출력 X)

{
  "headline": "기사 제목 (30자 이내, 본문 1~2단락 핵심어 그대로 사용)",
  "subheadline": "부제 (15자 이내)",
  "body": "★ 5단락, 1,500~2,000자, 단락 간 빈 줄(\\n\\n) 필수. 모든 수치 옆에 (DART/financials/공신력) 출처 태그",
  "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3"],
  "news_value": "이 기사의 취재 가치 한 줄 설명",
  "caution": "확인 필요 사항 (없으면 빈 문자열)"
}"""


def _json_output_block_v3() -> str:
    """v3 REPORTER MODE — 8단락 본문 + Fact Card + Reporter Brief."""
    return """\
위 정보를 바탕으로 아래 JSON 형식으로 기사 초안을 작성하세요.
반드시 JSON 형식으로만 출력하세요. 모든 본문은 신문 기사 반말체로 작성.

★ 출력 전 의무 체크 (8가지):
  - WHAT/WHO/WHEN/WHERE/WHY/SO WHAT/WHAT NEXT 모두 답변됐는가?
  - body 모든 수치에 출처 태그가 있는가?
  - body가 반말체 종결어미("~한다", "~된다", "~확인됐다")를 사용하는가?
  - STEP 6 (핵심 질문)이 구체적 미확인 팩트를 제시하는가?
  - STEP 7 (다음 체크포인트)에 구체적 시점·지표가 있는가?
  - reporter_brief의 IR 질문 5개가 답변 가능한 형태인가?

{
  "headline": "30자 이내, 반말체, 핵심 변화 + 시점/규모",
  "subheadline": "15자 이내, 핵심 보강",
  "body": "★ 7단락(STEP 1~7), 단락 간 빈 줄, 전체 1,800~2,400자, 모든 수치에 출처 태그, 신문 기사 반말체",
  "fact_card": {
    "who": "결정 주체",
    "what": "변화 한 줄",
    "when": "시점",
    "where": "영향 범위",
    "why_hypothesis": ["가설1", "가설2"],
    "how": "방법",
    "so_what": "시장 의미 한 줄",
    "confidence": "HIGH|MEDIUM|LOW"
  },
  "reporter_brief": {
    "ir_questions": [
      "IR 통화 질문 1 (구체적, 답변 가능)",
      "IR 통화 질문 2",
      "IR 통화 질문 3",
      "IR 통화 질문 4",
      "IR 통화 질문 5"
    ],
    "unverified_facts": [
      "DART 외 추가 확인 필요 사실 1",
      "필요 사실 2",
      "필요 사실 3"
    ],
    "competitor_compare": [
      "경쟁사 비교 포인트 1",
      "포인트 2",
      "포인트 3"
    ],
    "next_quarter_indicators": [
      "다음 분기 체크 지표 1",
      "지표 2",
      "지표 3",
      "지표 4",
      "지표 5"
    ],
    "follow_up_stories": [
      "후속 기사 주제 1",
      "주제 2",
      "주제 3"
    ]
  },
  "keywords": ["키워드1", "키워드2", "키워드3"],
  "news_value": "이 기사의 시장 가치 한 줄",
  "caution": "확인 필요 사항"
}"""


# ── A. 기본형 프롬프트 (strategy_change · supply_chain) ──────────────────────
def _build_prompt_default(lead: sqlite3.Row, ai_result: str,
                          fin_block: str, ext_block: str = "") -> str:
    b = _lead_base(lead)
    fin_section = f"\n[재무 현황 (DART 공시 수치 — 반드시 기사에 활용)]\n{fin_block}\n" if fin_block else ""
    ext_section = f"\n{ext_block}\n" if ext_block else ""
    return f"""당신은 파이낸스코프(finscope.co.kr)의 한국 증권부 단신 기자입니다.
이 기사는 v2.2 STRAIGHT (단신·속보형) — 회사 통화 없이 즉시 출고 가능한 형태로 작성하세요.
**[입력 자료]에 없는 정보 절대 사용 금지** — 평가/해석/전망 표현 금지.

{_COMMON_RULES}

{FIVE_STEPS_STRAIGHT}

{STYLE_GUIDE}

═══════════════════════════════════════════
[입력 자료]

▶ 취재 단서
기업명: {b['corp_name']}
단서 유형: {b['type_ko']} (severity {b['severity']}/5)
감지 키워드: {b['kw_list']}

▶ 핵심 요약
{b['summary']}

▶ 근거 문장 (★ STEP 1에서 직접 인용 대상)
{b['evidence']}
{fin_section}
▶ AI 비교분석 발췌 (★ STEP 2·5에서 인용 대상, 최대 2000자)
{ai_result[:2000]}
{ext_section}═══════════════════════════════════════════

{_json_output_block()}"""


# ── B. 변화 감지형 프롬프트 (numeric_change · market_shift) ──────────────────
def _build_prompt_change(lead: sqlite3.Row, ai_result: str,
                         fin_block: str, ext_block: str = "") -> str:
    b = _lead_base(lead)
    fin_section = f"\n▶ 재무 데이터 (★ STEP 3 수치 인용 대상)\n{fin_block}\n" if fin_block else ""
    ext_section = f"\n{ext_block}\n" if ext_block else ""
    return f"""당신은 파이낸스코프(finscope.co.kr)의 한국 증권부 단신 기자입니다.
이 기사는 v2.2 STRAIGHT 【수치 변화】 유형 — 회사 통화 없이 즉시 출고 가능 형태.
**[입력 자료]에 없는 정보 절대 사용 금지** — 평가/해석/전망 표현 금지.

{_COMMON_RULES}

{FIVE_STEPS_STRAIGHT}

[추가 — 수치 변화 특화]
  - STEP 3 수치 박스를 가장 강조 (변화 전·후 병기 필수)
  - 형식: "X억 원 → Y억 원 (전년 대비 ○○% 증가/감소, 2025 사업보고서 기준)"

═══════════════════════════════════════════
[입력 자료]

▶ 취재 단서
기업명: {b['corp_name']}
단서 유형: {b['type_ko']} (severity {b['severity']}/5)
감지 키워드: {b['kw_list']}

▶ 핵심 요약 — 변화의 실체
{b['summary']}

▶ 근거 문장 (★ STEP 1에서 직접 인용 대상)
{b['evidence']}
{fin_section}
▶ AI 비교분석 발췌 (★ STEP 2·5에서 인용 대상, 최대 2000자)
{ai_result[:2000]}
{ext_section}═══════════════════════════════════════════

{_json_output_block()}"""


# ── C. 리스크 경보형 프롬프트 (risk_alert) ──────────────────────────────────
def _build_prompt_risk(lead: sqlite3.Row, ai_result: str,
                       fin_block: str, sc_context: str, ext_block: str = "") -> str:
    b = _lead_base(lead)
    fin_section = f"\n▶ 재무 데이터 (★ STEP 3 수치 인용 대상)\n{fin_block}\n" if fin_block else ""
    sc_section  = f"\n▶ 공급망 거래처 (★ STEP 5 보강 가능)\n{sc_context}\n" if sc_context else ""
    ext_section = f"\n{ext_block}\n" if ext_block else ""
    return f"""당신은 파이낸스코프(finscope.co.kr)의 한국 증권부 단신 기자입니다.
이 기사는 v2.2 STRAIGHT 【리스크 경보】 유형 — 회사 통화 없이 즉시 출고 가능 형태.
**[입력 자료]에 없는 정보 절대 사용 금지** — 평가/해석/전망 표현 금지.

{_COMMON_RULES}

{FIVE_STEPS_STRAIGHT}

[추가 — 리스크 경보 특화]
  - STEP 1 리드에 리스크 실체와 규모를 수치로 명시
  - "부채비율 100% → 145% (2025 사업보고서 기준)" 식
  - 공급망 거래처 정보가 있으면 STEP 5에 단순 사실로 인용 (분석 X)

═══════════════════════════════════════════
[입력 자료]

▶ 취재 단서
기업명: {b['corp_name']}
단서 유형: {b['type_ko']} (severity {b['severity']}/5)
감지 키워드: {b['kw_list']}

▶ 핵심 요약 — 리스크 실체
{b['summary']}

▶ 근거 문장 (★ STEP 1에서 직접 인용 대상)
{b['evidence']}
{fin_section}{sc_section}
▶ AI 비교분석 발췌 (★ STEP 2·5에서 인용 대상, 최대 2000자)
{ai_result[:2000]}
{ext_section}═══════════════════════════════════════════

{_json_output_block()}"""


# ══════════════════════════════════════════════════════════════════════════════
# 브리핑 프롬프트 빌더 (복수 단서 → 종합 기사)
# ══════════════════════════════════════════════════════════════════════════════
def build_briefing_prompt(leads: list, conn: sqlite3.Connection) -> str:
    """
    상위 N건 취재단서를 묶어 종합 브리핑 기사 프롬프트 생성.
    leads: story_leads Row 목록 (severity 내림차순 정렬 권장)
    """
    today = datetime.now().strftime("%Y년 %m월 %d일")

    items = []
    for i, lead in enumerate(leads, 1):
        corp  = lead["corp_name"] or "미상"
        ltype = LEAD_TYPE_KO.get(lead["lead_type"], lead["lead_type"])
        sev   = lead["severity"]
        title = lead["title"] or ""
        summ  = (lead["summary"] or "")[:200]
        nstat = lead["news_status"] or "unknown"
        nstat_ko = {"exclusive": "🔴독점", "partial": "🟡일부보도", "covered": "🟢기보도"}.get(nstat, "🔵미확인")
        items.append(
            f"[{i}] {corp} | {ltype} | severity {sev}/5 | {nstat_ko}\n"
            f"    제목: {title}\n"
            f"    요약: {summ}"
        )

    leads_block = "\n\n".join(items)

    return f"""당신은 파이낸스코프(finscope.co.kr)의 편집장입니다.
오늘({today}) 가장 주목할 기업 동향을 엮어 경제 브리핑 기사를 작성하세요.

{_COMMON_RULES}

[브리핑 기사 작성 지침]
- 제목: "{today} 기업동향 브리핑" 형태 (날짜 포함)
- 본문: 8~10 문단, 전체 1,500~2,000자
  * 1단락(오프닝): 오늘 기업 동향의 핵심 키워드 1~2개로 시작
    예) "오늘 국내 주요 기업들은 ○○과 ○○을 동시에 맞닥뜨렸다"
  * 2~4단락(주요 단서 3선): 가장 뉴스 가치 높은 단서 3건을 각 1단락으로 요약
    - 🔴독점(미보도) 단서를 최우선 배치
    - 각 단락은 기업명·핵심 변화·의미를 3~4문장으로
  * 5단락(섹터 동향): 공통 업종·테마 있으면 묶어서 분석, 없으면 생략
  * 6단락(리스크 모아보기): risk_alert 단서 중 경고 수위 높은 것 요약
  * 7단락(편집장 픽): 독자가 가장 주목해야 할 1건 + 이유
  * 8단락(내일 체크포인트): 다음에 확인해야 할 후속 지표·이벤트

[선정 기준]
- 🔴독점(미보도) 단서 우선
- severity 높은 순
- 같은 기업 중복 배제

[입력 단서 목록]
{leads_block}

반드시 아래 JSON 형식으로만 출력하세요.

{{
  "headline": "브리핑 제목 (날짜 포함, 30자 이내)",
  "subheadline": "부제 (15자 이내)",
  "body": "본문 (8~10 문단, 각 문단 사이 빈 줄, 1,500~2,000자)",
  "top_leads": [
    {{"corp_name": "기업명", "one_line": "한 줄 요약"}},
    {{"corp_name": "기업명", "one_line": "한 줄 요약"}},
    {{"corp_name": "기업명", "one_line": "한 줄 요약"}}
  ],
  "keywords": ["키워드1", "키워드2", "키워드3"],
  "news_value": "이 브리핑의 편집 가치 한 줄",
  "caution": "확인 필요 사항 (없으면 빈 문자열)"
}}"""


# ══════════════════════════════════════════════════════════════════════════════
# v3 REPORTER MODE — 통합 프롬프트 빌더 (8단계 + 취재 노트)
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# 공급망 시나리오별 기사 프롬프트 (8가지 시나리오 통합)
# ══════════════════════════════════════════════════════════════════════════════

SUPPLY_CHAIN_PROMPT_GUIDE = """
[공급망 기사 작성 지침 — 시나리오별]
앵글:
  HUB         : "○○사 거래처 N곳 종합 분석 — 누가 늘었고 누가 줄었나"
  DEPENDENCE  : "○○사 매출 X% 한 거래처 집중 — 단일 의존 리스크"
  CLUSTER     : "○○·○○ 같은 거래처 N곳 공유 — 운명 공동체"
  GLOBAL      : "Apple 거래 한국 N곳 — 글로벌 가치사슬 노출도"
  VERTICAL    : "○○그룹 내부거래 N건 — 계열 의존도와 그룹 경쟁력"
  INDUSTRY    : "○○ 업종 공급망 종합 N개사 — 핵심 거래처와 공통 패턴"
  NEW_ENTRANT : "○○ 공급망 첫 등장 — 정보 격차 X점, 알려지지 않은 신규 진입자"
  IMPACT      : "○○ 위기 시 거래처 N곳 영향권 — 충격 전파 분석"

본문 5단락 표준 (1,200~1,600자):
  STEP 1. 리드 (200~250자): 시나리오 핵심 사실 + 시점성
  STEP 2. 관련 기업·거래처 발췌 (300~400자): 누가 누구와 거래
  STEP 3. 수치·비중 (200~300자): 매출 비중·거래 횟수 등 정량 정보
  STEP 4. 의미·맥락 (300~400자): 시장 가치·산업 트렌드
  STEP 5. 후속 모니터링 (200자): 다음 분기 체크 포인트

문체: 신문 기사 반말체 (~다 종결), 평가어 최소화
출처: 공급망 DB / DART 사업보고서 / 지정된 거래 맥락만
""".strip()


def _build_prompt_supply_chain(lead: dict, conn: sqlite3.Connection) -> str:
    """공급망 단서 → 기사 프롬프트 생성."""
    scenario = lead.get("scenario", "HUB")
    title    = lead.get("title", "")
    summary  = lead.get("summary", "")
    metadata = lead.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}

    # 메타데이터를 텍스트 블록으로 펼치기
    meta_lines = ["▶ 공급망 단서 메타 데이터"]
    for k, v in metadata.items():
        if isinstance(v, list):
            meta_lines.append(f"  - {k}: {json.dumps(v, ensure_ascii=False)[:600]}")
        elif isinstance(v, dict):
            meta_lines.append(f"  - {k}:")
            for k2, v2 in list(v.items())[:8]:
                meta_lines.append(f"      • {k2}: {str(v2)[:200]}")
        else:
            meta_lines.append(f"  - {k}: {str(v)[:200]}")
    meta_block = "\n".join(meta_lines)

    # 시나리오별 강조 포인트
    scenario_focus = {
        "HUB":        "허브 회사가 어떤 산업에서 영향력을 가지는지, 거래처 회사들의 매출 의존도 분석",
        "DEPENDENCE": "단일 거래처 의존 리스크, 거래처 위기 시 충격 시뮬레이션",
        "CLUSTER":   "공유 거래처가 위기 시 두 회사가 동시에 영향받을 가능성",
        "GLOBAL":    "글로벌 가치사슬 노출도, 환율·관세 영향, 미국·중국 갈등 리스크",
        "VERTICAL":  "그룹 내부거래 비중과 계열 경쟁력, 외부 매출 비중 변화",
        "INDUSTRY":  "업종 공통 거래처와 산업 트렌드, 핵심 의존도 현황",
        "NEW_ENTRANT": "신규 진입자의 향후 성장성과 정보 격차 해소 가치",
        "IMPACT":    "충격 전파 시나리오, 거래처별 영향 강도 차등 분석",
    }
    focus = scenario_focus.get(scenario, "")

    return f"""당신은 파이낸스코프(finscope.co.kr)의 한국 증권부 공급망 전문 기자입니다.
공급망 데이터에서 발굴한 단서를 바탕으로 산업·증권 기사를 작성하세요.

{_COMMON_RULES}

{SUPPLY_CHAIN_PROMPT_GUIDE}

═══════════════════════════════════════════
[입력 자료 — 공급망 단서]

▶ 시나리오: {scenario}
▶ 단서 제목: {title}
▶ 요약: {summary}

{meta_block}

▶ 시나리오 강조 포인트
{focus}

═══════════════════════════════════════════

위 자료만 활용해 5단락 1,200~1,600자 기사를 JSON 형식으로 작성하세요.
출력 전 자체 검증:
  - 본문에 메타데이터에 없는 회사명·수치를 사용하지 않았는가?
  - 헤드라인이 본문 1단락 핵심어와 일치하는가?
  - 평가어("긍정적","우려","주목")를 남용하지 않았는가?

{{
  "headline": "30자 이내, 시나리오 핵심을 명확히",
  "subheadline": "15자 이내",
  "body": "★ 5단락, 단락 간 빈 줄, 1200~1600자, 반말체",
  "keywords": ["키워드1","키워드2","키워드3"],
  "news_value": "이 기사의 시장 가치 한 줄",
  "caution": "확인 필요 사항 (없으면 빈 문자열)"
}}"""


def _build_prompt_reporter_v3(lead: sqlite3.Row, ai_result: str,
                              fin_block: str, sc_context: str = "",
                              ext_block: str = "") -> str:
    """v3 — 한국 증권부 에이스 기자 관점 8단계 + 취재 노트."""
    b = _lead_base(lead)
    fin_section = f"\n▶ 재무 데이터 (★ STEP 3·4 인용 대상)\n{fin_block}\n" if fin_block else ""
    sc_section  = f"\n▶ 공급망 거래처 (★ STEP 4 인용 대상)\n{sc_context}\n" if sc_context else ""
    ext_section = f"\n{ext_block}\n" if ext_block else ""

    type_aux = {
        "numeric_change":  "【수치 급변】 — 변화 전·후 수치 대비 + 구조적 의미",
        "market_shift":    "【시장 전환】 — 점유율·경쟁 환경 변화 + 미래 시나리오",
        "risk_alert":      "【리스크 경보】 — 리스크 실체 + 공급망 파급 + 대응",
        "strategy_change": "【전략 변화】 — 사업 구조 변화 + 결정 배경 + 향후 전략",
        "supply_chain":    "【공급망 변화】 — 거래처 이동 + 매출 비중 영향",
    }
    type_label = type_aux.get(lead["lead_type"], "")

    return f"""당신은 파이낸스코프(finscope.co.kr)의 한국 증권부 에이스 기자입니다.
**팩트 정리가 아닌 "취재 관점"** 으로 8단계 절차로 기사를 작성하세요.
변화를 인지 → 질문을 던지 → 풀어내고 → 다음 취재 방향까지 제시.

이 기사는 {type_label} 유형입니다.

{_COMMON_RULES}

{EIGHT_STEPS_RULE_V3}

═══════════════════════════════════════════
[입력 자료]

▶ 취재 단서
기업명: {b['corp_name']}
단서 유형: {b['type_ko']} (severity {b['severity']}/5)
감지 키워드: {b['kw_list']}

▶ 핵심 요약
{b['summary']}

▶ 근거 문장 (★ STEP 1·2 인용 대상)
{b['evidence']}
{fin_section}{sc_section}
▶ AI 비교분석 발췌 (★ STEP 2·6 인용 대상, 최대 2000자)
{ai_result[:2000]}
{ext_section}═══════════════════════════════════════════

{_json_output_block_v3()}"""


# ══════════════════════════════════════════════════════════════════════════════
# 라우터: lead_type → 전용 프롬프트
# ══════════════════════════════════════════════════════════════════════════════
def build_article_prompt(lead: sqlite3.Row, ai_result: str | None,
                         conn: sqlite3.Connection | None = None,
                         reporter_mode: bool = False) -> str:
    """lead_type에 따라 최적 프롬프트 선택 후 반환."""
    ai_result = ai_result or ""
    corp_code  = lead["corp_code"] or ""
    lead_type  = lead["lead_type"]
    lead_id    = lead["id"]

    # ── 재무 데이터 블록 (공통) ──
    fin_block = ""
    if conn and corp_code:
        try:
            fin_block = build_financials_block(corp_code, conn)
        except Exception:
            fin_block = ""

    # ── 외부 자료 블록 (Phase E) ──
    ext_block = ""
    if conn and lead_id:
        try:
            ext_block, _ = build_external_sources_context(lead_id, conn)
        except Exception:
            ext_block = ""

    # ── 과거 보고서 컨텍스트 (Phase R) — 새로움 검증용 ──
    past_block = ""
    if conn and corp_code:
        try:
            kw_list = []
            try:
                kw_list = json.loads(lead["keywords"] or "[]")
            except Exception:
                pass
            past_block = build_past_reports_context(
                corp_code, lead["comparison_id"], kw_list, conn
            )
        except Exception:
            past_block = ""

    # 외부 자료에 과거 보고서 컨텍스트 추가
    if past_block:
        ext_block = past_block + "\n\n" + ext_block if ext_block else past_block

    # ── 공급망 컨텍스트 (리스크용) ──
    sc_context = ""
    if conn and corp_code and lead_type == "risk_alert":
        try:
            sc_context = build_supply_chain_context(corp_code, conn)
        except Exception:
            sc_context = ""

    # ── v3 REPORTER MODE ── 모든 lead_type 통합 처리
    if reporter_mode:
        return _build_prompt_reporter_v3(lead, ai_result, fin_block,
                                         sc_context, ext_block)

    # ── v2.2 기본 라우팅 (유형별 분기) ──
    if lead_type in ("numeric_change", "market_shift"):
        return _build_prompt_change(lead, ai_result, fin_block, ext_block)

    if lead_type == "risk_alert":
        return _build_prompt_risk(lead, ai_result, fin_block, sc_context, ext_block)

    # 기본: strategy_change, supply_chain, 기타
    return _build_prompt_default(lead, ai_result, fin_block, ext_block)


# ── 기사 파싱 ─────────────────────────────────────────────────────────────────
def _fix_json_newlines(text: str) -> str:
    """JSON 문자열 값 안의 literal 개행을 \\n으로 치환해 json.loads 가능하게 만든다."""
    result = []
    in_string = False
    escape_next = False
    for ch in text:
        if escape_next:
            result.append(ch)
            escape_next = False
            continue
        if ch == '\\':
            result.append(ch)
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            result.append(ch)
            continue
        if in_string and ch == '\n':
            result.append('\\n')
            continue
        if in_string and ch == '\r':
            result.append('\\r')
            continue
        result.append(ch)
    return "".join(result)


def _extract_body_raw(text: str) -> str:
    """JSON 파싱 없이 "body": "..." 값을 문자 단위 스캔으로 추출."""
    m = re.search(r'"body"\s*:\s*"', text)
    if not m:
        return ""
    pos = m.end()
    chars = []
    while pos < len(text):
        ch = text[pos]
        if ch == '\\' and pos + 1 < len(text):
            nch = text[pos + 1]
            if nch == 'n':
                chars.append('\n')
            elif nch == '"':
                chars.append('"')
            elif nch == '\\':
                chars.append('\\')
            else:
                chars.append(nch)
            pos += 2
            continue
        if ch == '"':
            break  # 문자열 종료
        chars.append(ch)
        pos += 1
    return "".join(chars).strip()


def parse_article_json(text: str) -> dict:
    """Gemini 출력에서 JSON 파싱. 5단계 전략으로 최대한 추출."""
    original_text = text

    # ── 전략 1: ```json ... ``` 블록 직접 파싱 (GREEDY)
    m = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    candidate = m.group(1) if m else None

    # ── 전략 2: 전체 텍스트에서 가장 큰 {} 블록 (greedy)
    if not candidate:
        m2 = re.search(r"(\{.*\})", text, re.DOTALL)
        candidate = m2.group(1) if m2 else None

    if candidate:
        # 전략 1/2 직접 파싱
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

        # 전략 3: literal 개행 수정 후 재파싱
        try:
            return json.loads(_fix_json_newlines(candidate))
        except json.JSONDecodeError:
            pass

    # ── 전략 4: JSONDecoder 위치 스캔
    decoder = json.JSONDecoder()
    for i, ch in enumerate(original_text):
        if ch == '{':
            try:
                obj, _ = decoder.raw_decode(original_text, i)
                if isinstance(obj, dict) and obj.get("headline"):
                    return obj
            except json.JSONDecodeError:
                continue

    # ── 전략 5: 필드별 개별 추출 (최후 수단)
    result: dict = {}
    src = candidate or original_text

    for field in ["headline", "subheadline", "news_value", "caution"]:
        fm = re.search(rf'"{field}"\s*:\s*"([^"\n]{{0,300}})"', src)
        if fm:
            result[field] = fm.group(1).strip()

    # body: 문자 단위 스캔으로 안전 추출
    body_raw = _extract_body_raw(src)
    if body_raw:
        result["body"] = body_raw

    # keywords 배열
    km = re.search(r'"keywords"\s*:\s*\[([\s\S]*?)\]', src)
    if km:
        result["keywords"] = re.findall(r'"([^"]+)"', km.group(1))

    return result


# ── 초안 저장 ─────────────────────────────────────────────────────────────────
def save_draft(conn: sqlite3.Connection, lead_id: int, lead: sqlite3.Row,
               article: dict, model_name: str,
               ai_result: str = "", fin_block: str = "",
               sc_context: str = "") -> int:
    headline    = article.get("headline", "")[:200]
    subheadline = article.get("subheadline", "")[:200]
    body        = article.get("body", "")
    kw_list     = article.get("keywords", [])
    news_value  = article.get("news_value", "")
    caution     = article.get("caution", "")

    # body에 keywords 주석 추가
    editor_note = ""
    if news_value:
        editor_note += f"[취재 가치] {news_value}\n"
    if caution:
        editor_note += f"[주의] {caution}"

    word_count = len(body.split())
    char_count = len(body)

    cur = conn.execute("""
        INSERT INTO article_drafts
            (lead_id, corp_code, corp_name,
             headline, subheadline, content,
             style, model, word_count, char_count,
             status, editor_note, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,'draft',?,datetime('now','localtime'))
    """, (
        lead_id,
        lead["corp_code"],
        lead["corp_name"],
        headline,
        subheadline,
        body,
        "news",
        model_name,
        word_count,
        char_count,
        editor_note.strip(),
    ))
    conn.commit()
    draft_id = cur.lastrowid

    # ── 자동 검증 후크 (P9): article_postprocess + article_verification ──
    try:
        _auto_verify_draft(conn, draft_id, article, lead,
                           ai_result, fin_block, sc_context)
    except Exception as e:
        print(f"        ⚠ 자동 검증 실패 (무시): {e}")

    # ── 외부 출처 사용 기록 (Phase E) ──
    try:
        _record_external_refs(conn, draft_id, lead["id"], body)
    except Exception as e:
        print(f"        ⚠ 외부 인용 기록 실패 (무시): {e}")

    # ── v3 REPORTER MODE: fact_card + reporter_brief 저장 ──
    try:
        _record_reporter_v3(conn, draft_id, article)
    except Exception as e:
        print(f"        ⚠ reporter_brief 기록 실패 (무시): {e}")

    return draft_id


def _ensure_reporter_schema(conn: sqlite3.Connection) -> None:
    """v3 신규 테이블 자동 생성."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS article_fact_cards (
            article_id INTEGER PRIMARY KEY,
            who TEXT, what TEXT, when_at TEXT, where_at TEXT,
            why_hypothesis TEXT,
            how_method TEXT, so_what TEXT,
            confidence_level TEXT,
            generated_at TEXT,
            FOREIGN KEY (article_id) REFERENCES article_drafts(id)
        );

        CREATE TABLE IF NOT EXISTS reporter_briefs (
            article_id INTEGER PRIMARY KEY,
            ir_questions TEXT,
            unverified_facts TEXT,
            competitor_compare TEXT,
            next_quarter_indicators TEXT,
            follow_up_stories TEXT,
            generated_at TEXT,
            FOREIGN KEY (article_id) REFERENCES article_drafts(id)
        );
    """)
    conn.commit()


def _record_reporter_v3(conn: sqlite3.Connection, article_id: int,
                        article: dict) -> None:
    """v3 출력의 fact_card + reporter_brief를 별도 테이블에 저장."""
    fc = article.get("fact_card") or {}
    rb = article.get("reporter_brief") or {}
    if not fc and not rb:
        return  # v2 기사는 스킵

    _ensure_reporter_schema(conn)

    if fc:
        conn.execute("""
            INSERT OR REPLACE INTO article_fact_cards
                (article_id, who, what, when_at, where_at,
                 why_hypothesis, how_method, so_what,
                 confidence_level, generated_at)
            VALUES (?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
        """, [
            article_id,
            fc.get("who", ""), fc.get("what", ""),
            fc.get("when", ""), fc.get("where", ""),
            json.dumps(fc.get("why_hypothesis") or [], ensure_ascii=False),
            fc.get("how", ""), fc.get("so_what", ""),
            fc.get("confidence", "MEDIUM"),
        ])

    if rb:
        conn.execute("""
            INSERT OR REPLACE INTO reporter_briefs
                (article_id, ir_questions, unverified_facts,
                 competitor_compare, next_quarter_indicators,
                 follow_up_stories, generated_at)
            VALUES (?,?,?,?,?,?,datetime('now','localtime'))
        """, [
            article_id,
            json.dumps(rb.get("ir_questions") or [], ensure_ascii=False),
            json.dumps(rb.get("unverified_facts") or [], ensure_ascii=False),
            json.dumps(rb.get("competitor_compare") or [], ensure_ascii=False),
            json.dumps(rb.get("next_quarter_indicators") or [], ensure_ascii=False),
            json.dumps(rb.get("follow_up_stories") or [], ensure_ascii=False),
        ])

    conn.commit()


def _record_external_refs(conn: sqlite3.Connection, article_id: int,
                          lead_id: int, body: str) -> None:
    """기사 본문에 매체명이 등장한 경우 article_external_refs에 기록."""
    if not body or not lead_id:
        return
    rows = conn.execute("""
        SELECT es.id, es.outlet_name FROM lead_external_match lem
        JOIN external_sources es ON lem.source_id = es.id
        WHERE lem.lead_id = ? AND lem.fact_check_status IN ('PASS','PARTIAL')
    """, [lead_id]).fetchall()
    for r in rows:
        outlet = r["outlet_name"]
        if outlet and outlet in body:
            pos = body.find(outlet)
            ctx = body[max(0,pos-30):pos+len(outlet)+50]
            conn.execute("""
                INSERT OR IGNORE INTO article_external_refs
                    (article_id, source_id, citation_text, citation_position, inserted_at)
                VALUES (?, ?, ?, ?, datetime('now','localtime'))
            """, [article_id, r["id"], ctx, pos])
    conn.commit()


def _auto_verify_draft(conn: sqlite3.Connection, draft_id: int,
                       article: dict, lead: sqlite3.Row,
                       ai_result: str, fin_block: str, sc_context: str) -> None:
    """save_draft 직후 자동 호출되는 검증 후크.
    article_postprocess + article_verification(별도 스크립트) 점수를 DB에 기록.
    """
    # 1) article_postprocess (헤드라인 합일치 + 추측 표현 + 외부 지식)
    try:
        from article_postprocess import postprocess_article
        biz_row = conn.execute("""
            SELECT biz_content FROM reports WHERE corp_code=?
              AND biz_content IS NOT NULL ORDER BY rcept_dt DESC LIMIT 1
        """, [lead["corp_code"]]).fetchone()
        biz_content = (biz_row["biz_content"] if biz_row else "") or ""
        lead_dict = {
            "evidence": lead["evidence"] or "",
            "summary": lead["summary"] or "",
        }
        pp_result = postprocess_article(
            article, lead_dict,
            fin_block=fin_block,
            ai_result=ai_result,
            biz_content=biz_content,
            sc_context=sc_context,
        )
        # editor_note에 검증 결과 요약 추가
        warnings_summary = "; ".join(w["type"] for w in pp_result.get("warnings", []))
        score_line = f"\n[자동검증] 품질 {pp_result['quality_score']}점 / 헤드라인 {pp_result['headline_check']['match_score']}%"
        if warnings_summary:
            score_line += f" / 경고: {warnings_summary}"
        conn.execute(
            "UPDATE article_drafts SET editor_note = COALESCE(editor_note,'') || ? WHERE id=?",
            [score_line, draft_id]
        )
    except Exception as e:
        print(f"        postprocess 실패: {e}")

    # 2) article_verification (4차원 검증) — verify_articles.py가 ensure_schema 후 INSERT
    try:
        from verify_articles import ensure_schema, verify_one, save_verification
        ensure_schema(conn)
        a = conn.execute("SELECT * FROM article_drafts WHERE id=?", [draft_id]).fetchone()
        if a:
            v_result = verify_one(a, conn)
            save_verification(conn, v_result)
    except Exception as e:
        print(f"        verify_articles 실패: {e}")

    conn.commit()


# ── 통계 출력 ─────────────────────────────────────────────────────────────────
def print_stats(conn: sqlite3.Connection):
    print("=" * 60)
    print("  article_drafts 현재 통계")
    print("=" * 60)
    total = conn.execute("SELECT COUNT(*) FROM article_drafts").fetchone()[0]
    print(f"  전체 기사 초안: {total}건")
    if total == 0:
        print("  (아직 초안이 없습니다)")
        print("=" * 60)
        return

    print()
    print("  [상태별]")
    for r in conn.execute("SELECT status, COUNT(*) c FROM article_drafts GROUP BY status ORDER BY c DESC"):
        print(f"    {r['status']:<15} {r['c']}건")

    print()
    print("  [최근 10건]")
    for r in conn.execute("""
        SELECT corp_name, headline, char_count, model, created_at
        FROM article_drafts ORDER BY created_at DESC LIMIT 10
    """):
        print(f"    {r['corp_name']} | {(r['headline'] or '')[:35]} | {r['char_count']}자 | {r['model']}")

    print()
    leads_total   = conn.execute("SELECT COUNT(*) FROM story_leads").fetchone()[0]
    drafted       = conn.execute("SELECT COUNT(DISTINCT lead_id) FROM article_drafts").fetchone()[0]
    print(f"  story_leads 커버리지: {drafted}/{leads_total}건")
    print("=" * 60)


# ── 메인 처리 ─────────────────────────────────────────────────────────────────
def process_leads(conn: sqlite3.Connection, limit: int | None, lead_id: int | None,
                  severity_min: int, force: bool,
                  reporter_mode: bool = False) -> dict:
    """
    story_leads → article_drafts 생성
    반환: {"processed": int, "success": int, "errors": int}
    """
    if lead_id is not None:
        leads = conn.execute("""
            SELECT sl.*, ac.result as ai_result
            FROM story_leads sl
            LEFT JOIN ai_comparisons ac ON sl.comparison_id = ac.id
            WHERE sl.id = ?
        """, (lead_id,)).fetchall()
    else:
        # 아직 초안이 없는 단서 (severity + 새로움 + 정보격차 종합 정렬)
        already_clause = "AND NOT EXISTS (SELECT 1 FROM article_drafts ad WHERE ad.lead_id = sl.id)"
        if force:
            already_clause = ""

        # T+O 통합 우선순위:
        #   1. 새로움 점수(novelty) ★ 최우선 — 진짜 새로운 변화만
        #   2. 정보 격차 점수(info_gap) ★ 사명 직결 — 소외 기업 우선
        #   3. severity
        #   4. created_at
        # 새로움 < 50 (🔴 EXISTING) 자동 제외 옵션
        novelty_filter = "AND COALESCE(ln.score_total, 60) >= 50"

        query = f"""
            SELECT sl.*, ac.result as ai_result,
                   ln.score_total as novelty_score,
                   ln.grade as novelty_grade,
                   ln.pattern as novelty_pattern,
                   cig.score_total as info_gap_score,
                   cig.grade as info_gap_grade
            FROM story_leads sl
            LEFT JOIN ai_comparisons ac    ON sl.comparison_id = ac.id
            LEFT JOIN lead_novelty ln      ON sl.id = ln.lead_id
            LEFT JOIN company_info_gap cig ON sl.corp_code = cig.corp_code
            WHERE sl.status = 'new'
              AND sl.severity >= ?
              {already_clause}
              {novelty_filter}
            ORDER BY
                COALESCE(ln.score_total, 60) DESC,
                COALESCE(cig.score_total, 50) DESC,
                sl.severity DESC,
                sl.created_at ASC
        """
        if limit:
            query += f" LIMIT {int(limit)}"
        try:
            leads = conn.execute(query, (severity_min,)).fetchall()
        except sqlite3.OperationalError:
            # lead_novelty / company_info_gap 테이블이 아직 없으면 폴백
            fallback = f"""
                SELECT sl.*, ac.result as ai_result
                FROM story_leads sl
                LEFT JOIN ai_comparisons ac ON sl.comparison_id = ac.id
                WHERE sl.status = 'new'
                  AND sl.severity >= ?
                  {already_clause}
                ORDER BY sl.severity DESC, sl.created_at ASC
            """
            if limit:
                fallback += f" LIMIT {int(limit)}"
            leads = conn.execute(fallback, (severity_min,)).fetchall()

    print(f"[info] 처리 대상 취재 단서: {len(leads)}건")
    if not leads:
        print("[info] 새로 생성할 기사 초안이 없습니다.")
        return {"processed": 0, "success": 0, "errors": 0}

    processed = 0
    success   = 0
    errors    = 0

    for lead in leads:
        lead_id_cur = lead["id"]
        corp_name   = lead["corp_name"] or "해당 기업"
        ai_result   = lead["ai_result"] or ""

        processed += 1
        # novelty / info_gap 정보 표시
        nov_info = ""
        ig_info = ""
        try:
            nov = lead["novelty_score"]
            if nov is not None:
                nov_info = f" | 새로움 {nov}({lead['novelty_pattern'] or '?'})"
            ig = lead["info_gap_score"]
            if ig is not None:
                ig_info = f" | 정보격차 {ig}{lead['info_gap_grade'] or ''}"
        except (KeyError, IndexError):
            pass
        print(f"  [{processed:3d}] {corp_name} | {lead['lead_type']} | severity={lead['severity']}{nov_info}{ig_info}")

        try:
            prompt = build_article_prompt(lead, ai_result, conn=conn,
                                          reporter_mode=reporter_mode)
            text, model_name = call_gemini_article(prompt)
            article = parse_article_json(text)

            if not article.get("headline"):
                print(f"        ⚠ JSON 파싱 실패 — 원문 저장")
                article = {
                    "headline":    lead["title"] or f"{corp_name} 관련 기사",
                    "subheadline": "",
                    "body":        text[:2000],
                    "keywords":    [],
                    "news_value":  "",
                    "caution":     "자동 파싱 실패 — 편집 필요",
                }

            # 자동 검증용 컨텍스트 재구성
            corp_code = lead["corp_code"] or ""
            fin_block_for_save = ""
            sc_context_for_save = ""
            if corp_code:
                try:
                    fin_block_for_save = build_financials_block(corp_code, conn)
                except Exception:
                    pass
                if lead["lead_type"] == "risk_alert":
                    try:
                        sc_context_for_save = build_supply_chain_context(corp_code, conn)
                    except Exception:
                        pass

            draft_id = save_draft(conn, lead_id_cur, lead, article, model_name,
                                  ai_result=ai_result,
                                  fin_block=fin_block_for_save,
                                  sc_context=sc_context_for_save)
            print(f"        ✓ [{draft_id}] {article['headline'][:40]} ({model_name})")
            success += 1

        except RuntimeError as e:
            print(f"        ✗ 오류: {e}")
            errors += 1
            if "할당량 초과" in str(e):
                print("[중단] API 일일 한도 초과. 내일 다시 실행하세요.")
                break

        # RPM 제한 준수 딜레이
        if processed < len(leads):
            time.sleep(ARTICLE_DELAY)

    print(f"\n[완료] {processed}건 처리, {success}건 생성, {errors}건 오류")
    return {"processed": processed, "success": success, "errors": errors}


# ── 브리핑 생성 ───────────────────────────────────────────────────────────────
def generate_briefing(conn: sqlite3.Connection, top_n: int = 8,
                      severity_min: int = 3) -> dict:
    """
    상위 N건 단서를 묶어 브리핑 기사 초안 1건 생성.
    독점(exclusive) 단서 우선, severity 내림차순.
    반환: {"draft_id": int, "headline": str, "char_count": int}
    """
    leads = conn.execute("""
        SELECT * FROM story_leads
        WHERE status != 'archived'
          AND severity >= ?
        ORDER BY
          CASE news_status WHEN 'exclusive' THEN 0
                           WHEN 'partial'   THEN 1
                           ELSE 2 END,
          severity DESC,
          created_at DESC
        LIMIT ?
    """, [severity_min, top_n]).fetchall()

    if not leads:
        print("[브리핑] 대상 단서 없음")
        return {}

    print(f"[브리핑] 대상 단서 {len(leads)}건으로 브리핑 기사 생성 중...")
    prompt = build_briefing_prompt(leads, conn)
    text, model_name = call_gemini_article(prompt)
    article = parse_article_json(text)

    if not article.get("headline"):
        article = {
            "headline":    f"{datetime.now().strftime('%m월 %d일')} 기업동향 브리핑",
            "subheadline": "파이낸스코프 편집부",
            "body":        text[:2500],
            "keywords":    [],
            "news_value":  "",
            "caution":     "자동 파싱 실패 — 편집 필요",
        }

    # 브리핑은 lead_id=None으로 저장 (corp_code/corp_name도 편집부로)
    top_leads_json = json.dumps(article.get("top_leads", []), ensure_ascii=False)
    editor_note = f"[브리핑] 포함 단서 {len(leads)}건\n"
    if article.get("news_value"):
        editor_note += f"[취재 가치] {article['news_value']}\n"
    if article.get("caution"):
        editor_note += f"[주의] {article['caution']}\n"
    editor_note += f"[top_leads] {top_leads_json}"

    body = article.get("body", "")
    cur = conn.execute("""
        INSERT INTO article_drafts
            (lead_id, corp_code, corp_name,
             headline, subheadline, content,
             style, model, word_count, char_count,
             status, editor_note, created_at)
        VALUES (NULL, NULL, '편집부',?,?,?,?,?,?,?,'draft',?,datetime('now','localtime'))
    """, (
        article.get("headline", "")[:200],
        article.get("subheadline", "")[:200],
        body,
        "briefing",
        model_name,
        len(body.split()),
        len(body),
        editor_note.strip(),
    ))
    conn.commit()
    draft_id = cur.lastrowid
    print(f"  ✓ 브리핑 저장 [id={draft_id}] {article['headline'][:40]} ({len(body)}자)")
    return {"draft_id": draft_id, "headline": article.get("headline", ""), "char_count": len(body)}


# ── 진입점 ────────────────────────────────────────────────────────────────────
def main():
    load_env(ENV_PATH)

    parser = argparse.ArgumentParser(description="취재 단서 기반 기사 초안 자동 생성")
    parser.add_argument("--stats",      action="store_true", help="현재 통계 출력 후 종료")
    parser.add_argument("--lead-id",    type=int, default=None, metavar="N", help="특정 lead ID만 처리")
    parser.add_argument("--limit",      type=int, default=None, metavar="N", help="최대 처리 건수")
    parser.add_argument("--severity",   type=int, default=4, metavar="N",
                        help="최소 심각도 (기본: 4, 범위: 1~5)")
    parser.add_argument("--force",      action="store_true", help="이미 초안이 있는 단서도 재생성")
    parser.add_argument("--briefing",   action="store_true",
                        help="브리핑 기사 1건 생성 (복수 단서 종합)")
    parser.add_argument("--briefing-top", type=int, default=8, metavar="N",
                        help="브리핑에 포함할 단서 수 (기본: 8)")
    parser.add_argument("--reporter-mode", action="store_true",
                        help="v3 REPORTER MODE — 한국 증권부 에이스 기자 관점 (8단계 + 취재 노트)")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"[error] DB 없음: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = get_conn()
    try:
        if args.stats:
            print_stats(conn)
            return

        if args.briefing:
            result = generate_briefing(conn, top_n=args.briefing_top,
                                       severity_min=args.severity)
            if result:
                print(f"\n브리핑 생성 완료: [{result['draft_id']}] {result['headline']} ({result['char_count']}자)")
            print()
            print_stats(conn)
            return

        stats = process_leads(
            conn,
            limit=args.limit,
            lead_id=args.lead_id,
            severity_min=args.severity,
            force=args.force,
            reporter_mode=args.reporter_mode,
        )

        print()
        print_stats(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    _ensure_utf8_io()
    main()
