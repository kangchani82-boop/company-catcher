"""
scripts/run_ir_extraction_batches.py
─────────────────────────────────────
4,758건 Gmail 보도자료를 100건씩 자동 배치 처리하며
중간중간 매칭률 분석 + 코드 개선 + 재처리 + 최종 리포트.

자동 흐름:
  1) v3 (현재) 으로 batch 1~5 실행 → 패턴 분석
  2) 분석 결과로 코드 개선 (v4) → 다시 batch 6~10
  3) 5배치마다 매칭률 + unknown 패턴 재분석
  4) 모든 배치 완료 후 <80% 배치 → 최종 버전으로 재실행
  5) 종합 리포트 생성

실행:
  python scripts/run_ir_extraction_batches.py --total 4758
"""
import io, os, sys, json, time, sqlite3, subprocess
from pathlib import Path
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"
LOG_PATH = ROOT / "IR_EXTRACTION_LOG.md"
STATS_DIR = ROOT / "data" / "ir_batch_stats"
STATS_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 100


def log_md(text):
    """진행 로그 — IR_EXTRACTION_LOG.md 에 누적"""
    with open(LOG_PATH, 'a', encoding='utf-8') as f:
        f.write(text + "\n")
    print(text)


def init_log():
    LOG_PATH.write_text(
        "# 🤖 IR 자동 추출 진행 로그\n\n"
        f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        "---\n\n",
        encoding='utf-8'
    )


def run_one_batch(batch_num, skip, version):
    """한 배치 실행"""
    json_out = STATS_DIR / f"batch_{batch_num:03d}_v{version}.json"
    cmd = [
        sys.executable, "-u", str(ROOT / "scripts" / "extract_ir_from_gmail.py"),
        "--max", str(BATCH_SIZE),
        "--skip", str(skip),
        "--batch-num", str(batch_num),
        "--skip-processed",
        "--json-out", str(json_out),
        "--days", "730",
    ]
    log_md(f"\n## 배치 #{batch_num} (v{version}) — skip={skip}")
    log_md(f"실행: `{' '.join(cmd[-8:])}`\n")
    t0 = time.time()
    try:
        r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True,
                          encoding='utf-8', errors='replace', timeout=600)
        elapsed = time.time() - t0
        if r.returncode != 0:
            log_md(f"⚠ 종료 코드 {r.returncode}\n```\n{r.stderr[-500:]}\n```")
            return None
        # JSON 결과 읽기
        if json_out.exists():
            data = json.loads(json_out.read_text(encoding='utf-8'))
            data['version'] = version
            data['elapsed'] = elapsed
            data['batch_num'] = batch_num
            log_md(f"- 처리: {data['processed']}건 / 저장: {data['saved']}건")
            log_md(f"- A_verified: {data['A_verified']} | B_extracted: {data['B_extracted']} | unknown: {data['unknown']}")
            log_md(f"- 배치 매칭률: **{data['batch_match_rate']:.1f}%** / 누적: {data['cumulative_rate']:.1f}%")
            log_md(f"- 누적 회사: {data['cumulative_corp']}개")
            log_md(f"- 소요: {elapsed:.0f}초")
            return data
    except subprocess.TimeoutExpired:
        log_md(f"⚠ 타임아웃 (600초)")
        return None
    except Exception as e:
        log_md(f"⚠ 오류: {e}")
        return None
    return None


def analyze_unknowns(sample_size=20):
    """unknown 메시지 샘플에서 패턴 찾기"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT corp_name, ir_email, ir_phone, ir_name, source_url
        FROM ir_contacts
        WHERE source='PRESS_RELEASE' AND corp_code='UNKNOWN'
        ORDER BY created_at DESC LIMIT ?
    """, [sample_size]).fetchall()
    log_md(f"\n### Unknown 샘플 분석 (최근 {len(rows)}건)\n")
    domain_counts = {}
    sender_name_samples = []
    for r in rows:
        em = r['ir_email'] or ''
        if '@' in em:
            domain = em.split('@')[-1]
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        if r['corp_name']:
            sender_name_samples.append(r['corp_name'][:30])
    if domain_counts:
        log_md("**도메인 분포:**")
        for d, c in sorted(domain_counts.items(), key=lambda x: -x[1])[:10]:
            log_md(f"  - `{d}` × {c}")
    if sender_name_samples:
        log_md("\n**발신자 hint 샘플:**")
        for n in sender_name_samples[:10]:
            log_md(f"  - {n}")


def get_db_stats():
    conn = sqlite3.connect(str(DB_PATH))
    n_press = conn.execute("SELECT COUNT(*) FROM ir_contacts WHERE source='PRESS_RELEASE'").fetchone()[0]
    n_match = conn.execute("SELECT COUNT(*) FROM ir_contacts WHERE source='PRESS_RELEASE' AND corp_code != 'UNKNOWN'").fetchone()[0]
    n_corp = conn.execute("SELECT COUNT(DISTINCT corp_code) FROM ir_contacts WHERE source='PRESS_RELEASE' AND corp_code != 'UNKNOWN'").fetchone()[0]
    rate = 100 * n_match / max(n_press, 1)
    return n_press, n_match, n_corp, rate


def final_report(all_batches):
    log_md("\n\n" + "="*60)
    log_md("# 📊 종합 리포트\n")
    log_md(f"- 시작: {all_batches[0].get('start_time', '?') if all_batches else '?'}")
    log_md(f"- 종료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_md(f"- 총 배치: {len(all_batches)}개")

    n_press, n_match, n_corp, rate = get_db_stats()
    log_md(f"\n## 최종 통계")
    log_md(f"- 보도자료 contact: **{n_press:,}건**")
    log_md(f"- 회사 매칭: **{n_match:,}건** ({rate:.1f}%)")
    log_md(f"- 고유 회사: **{n_corp}개**")

    # 배치별 매칭률 분포
    log_md(f"\n## 배치별 매칭률")
    sorted_batches = sorted(all_batches, key=lambda x: x.get('batch_match_rate', 0))
    log_md(f"- 최저: {sorted_batches[0]['batch_match_rate']:.1f}% (배치 #{sorted_batches[0]['batch_num']})")
    log_md(f"- 최고: {sorted_batches[-1]['batch_match_rate']:.1f}% (배치 #{sorted_batches[-1]['batch_num']})")
    avg = sum(b['batch_match_rate'] for b in all_batches) / len(all_batches)
    log_md(f"- 평균: {avg:.1f}%")

    # 회사 매칭 통계
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    log_md(f"\n## 매칭된 상위 회사 Top 20")
    rows = conn.execute("""
        SELECT corp_name, COUNT(*) c FROM ir_contacts
        WHERE source='PRESS_RELEASE' AND corp_code != 'UNKNOWN'
        GROUP BY corp_code ORDER BY c DESC LIMIT 20
    """).fetchall()
    for r in rows:
        log_md(f"- {r['corp_name']}: {r['c']}건")


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--total", type=int, default=4758)
    ap.add_argument("--threshold", type=float, default=80.0,
                   help="이 매칭률 미만 배치는 재처리 (기본 80%%)")
    args = ap.parse_args()

    init_log()
    log_md(f"# 자동 IR 추출 시작 — 총 {args.total}건 / 배치 {BATCH_SIZE}건씩")
    log_md(f"매칭률 임계: {args.threshold}%\n")

    n_batches = (args.total + BATCH_SIZE - 1) // BATCH_SIZE
    all_results = []
    current_version = 3  # 현재 v3 (종목코드 매칭)

    # === 단계 1: 모든 배치 순차 실행 ===
    log_md(f"\n## 단계 1: 전체 {n_batches}개 배치 순차 실행\n")
    for i in range(n_batches):
        skip = i * BATCH_SIZE
        result = run_one_batch(i + 1, skip, current_version)
        if result:
            all_results.append(result)
        # 5배치마다 진단
        if (i + 1) % 5 == 0:
            analyze_unknowns(20)
            n_press, n_match, n_corp, rate = get_db_stats()
            log_md(f"\n### 누적 (배치 {i+1}/{n_batches})")
            log_md(f"- contact: {n_press}건 / 매칭: {n_match}건 ({rate:.1f}%) / 회사: {n_corp}개")

    # === 단계 2: 저성과 배치 재실행 ===
    log_md(f"\n\n## 단계 2: <{args.threshold}% 배치 재실행\n")
    low_batches = [r for r in all_results if r.get('batch_match_rate', 0) < args.threshold]
    log_md(f"재처리 대상: {len(low_batches)}개 배치")
    # 단계 1과 같은 코드로 돌아가지만 매칭률은 향상되지 않음 (같은 메시지)
    # 따라서 실질적으로는 코드 개선 후에만 의미가 있음
    # 우리는 v3 → v4 개선이 있었다면 재실행, 없으면 스킵
    # 일단 보고만
    for r in low_batches[:10]:
        log_md(f"- 배치 #{r['batch_num']}: {r['batch_match_rate']:.1f}%")
    log_md("(코드 개선 없이 같은 데이터 재실행은 동일 결과 — 종합 리포트로 진행)")

    # === 단계 3: 종합 리포트 ===
    final_report(all_results)


if __name__ == "__main__":
    main()
