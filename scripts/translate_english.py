"""
scripts/translate_english.py
─────────────────────────────
ai_comparisons.result 중 영어로 생성된 건을 한국어로 번역.
deep-translator (Google Translate 무료) 사용.

실행:
  python scripts/translate_english.py --stats          # 현황 확인
  python scripts/translate_english.py --dry-run        # 번역 없이 대상 목록만
  python scripts/translate_english.py --limit 10       # 최대 10건 번역
  python scripts/translate_english.py                  # 전체 영어 건 번역
  python scripts/translate_english.py --leads          # story_leads summary도 번역
"""

import io
import re
import sqlite3
import sys
import time
import argparse
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

# Google Translate 한 번에 처리 가능한 최대 글자 수
CHUNK_SIZE  = 4500
DELAY       = 0.5   # 요청 간 딜레이 (초)


# ── 번역기 초기화 ─────────────────────────────────────────────────────────────
def get_translator():
    try:
        from deep_translator import GoogleTranslator
        return GoogleTranslator(source="auto", target="ko")
    except ImportError:
        print("[오류] deep-translator 미설치: pip install deep-translator")
        sys.exit(1)


# ── 텍스트를 청크로 분할해 번역 ───────────────────────────────────────────────
def translate_text(text: str, translator) -> str:
    """긴 텍스트를 단락 단위로 분할해 번역 후 합치기."""
    if not text or not text.strip():
        return text

    # 이미 한국어가 충분히 포함되어 있으면 번역 불필요
    korean_chars = len(re.findall(r"[가-힣]", text))
    if korean_chars > len(text) * 0.3:
        return text

    # 단락 단위 분할 (\n\n 기준)
    paragraphs = text.split("\n\n")
    translated_parts = []
    chunk = ""

    for para in paragraphs:
        if len(chunk) + len(para) + 2 <= CHUNK_SIZE:
            chunk += para + "\n\n"
        else:
            if chunk.strip():
                try:
                    translated_parts.append(translator.translate(chunk.strip()))
                    time.sleep(DELAY)
                except Exception as e:
                    translated_parts.append(chunk.strip())  # 실패 시 원문 유지
            chunk = para + "\n\n"

    # 마지막 청크
    if chunk.strip():
        try:
            translated_parts.append(translator.translate(chunk.strip()))
            time.sleep(DELAY)
        except Exception as e:
            translated_parts.append(chunk.strip())

    return "\n\n".join(translated_parts)


# ── DB 연결 ────────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ── 영어 판별 ─────────────────────────────────────────────────────────────────
def is_english_dominant(text: str) -> bool:
    """한국어 비율이 15% 미만이면 영어 지배적으로 판단."""
    if not text:
        return False
    korean = len(re.findall(r"[가-힣]", text))
    alpha  = len(re.findall(r"[a-zA-Z]", text))
    total  = len(text.strip())
    if total < 50:
        return False
    return (korean / total) < 0.15 and alpha > 20


# ── 통계 ─────────────────────────────────────────────────────────────────────
def print_stats(conn):
    print("=" * 60)
    print("  영어 콘텐츠 현황")
    print("=" * 60)

    # ai_comparisons
    total_cmp = conn.execute("SELECT COUNT(*) FROM ai_comparisons WHERE result IS NOT NULL").fetchone()[0]
    rows = conn.execute("SELECT result FROM ai_comparisons WHERE result IS NOT NULL").fetchall()
    en_cmp = sum(1 for r in rows if is_english_dominant(r["result"]))
    print(f"\n  ai_comparisons.result: 전체 {total_cmp}건")
    print(f"    영어 지배적: {en_cmp}건 → 번역 대상")
    print(f"    한국어:      {total_cmp - en_cmp}건 → 정상")

    # story_leads summary
    total_sl = conn.execute("SELECT COUNT(*) FROM story_leads WHERE summary IS NOT NULL").fetchone()[0]
    rows2 = conn.execute("SELECT summary FROM story_leads WHERE summary IS NOT NULL").fetchall()
    en_sl = sum(1 for r in rows2 if is_english_dominant(r["summary"] or ""))
    print(f"\n  story_leads.summary: 전체 {total_sl}건")
    print(f"    영어 지배적: {en_sl}건")

    print("=" * 60)


# ── ai_comparisons 번역 ────────────────────────────────────────────────────────
def translate_comparisons(conn, translator, limit: int | None, dry_run: bool) -> dict:
    rows = conn.execute("""
        SELECT id, corp_name, result FROM ai_comparisons
        WHERE result IS NOT NULL AND status = 'ok'
        ORDER BY id
    """).fetchall()

    targets = [r for r in rows if is_english_dominant(r["result"])]
    if limit:
        targets = targets[:limit]

    total = len(targets)
    print(f"\n번역 대상: {total}건 (ai_comparisons.result)")
    if dry_run:
        for r in targets:
            print(f"  [{r['id']}] {r['corp_name']} — {len(r['result'])}자")
        return {"total": total, "success": 0, "failed": 0}

    success = failed = 0
    for i, row in enumerate(targets, 1):
        corp  = row["corp_name"] or "?"
        rid   = row["id"]
        orig_len = len(row["result"])
        print(f"  [{i:3d}/{total}] {corp:<20} {orig_len}자 번역 중...", end=" ", flush=True)
        try:
            translated = translate_text(row["result"], translator)
            conn.execute(
                "UPDATE ai_comparisons SET result=? WHERE id=?",
                [translated, rid]
            )
            conn.commit()
            new_len = len(translated)
            print(f"→ ✅ {new_len}자")
            success += 1
        except Exception as e:
            print(f"→ ❌ {e}")
            failed += 1

    return {"total": total, "success": success, "failed": failed}


# ── story_leads summary 번역 ────────────────────────────────────────────────────
def translate_leads(conn, translator, limit: int | None, dry_run: bool) -> dict:
    rows = conn.execute("""
        SELECT id, corp_name, summary, evidence FROM story_leads
        WHERE summary IS NOT NULL
    """).fetchall()

    targets = [r for r in rows if is_english_dominant(r["summary"] or "")]
    if limit:
        targets = targets[:limit]

    total = len(targets)
    print(f"\n번역 대상: {total}건 (story_leads.summary)")
    if dry_run:
        for r in targets:
            print(f"  [{r['id']}] {r['corp_name']} — {len(r['summary'] or '')}자")
        return {"total": total, "success": 0, "failed": 0}

    success = failed = 0
    for i, row in enumerate(targets, 1):
        corp = row["corp_name"] or "?"
        rid  = row["id"]
        print(f"  [{i:3d}/{total}] {corp:<20} 번역 중...", end=" ", flush=True)
        try:
            new_summary  = translate_text(row["summary"] or "", translator)
            new_evidence = translate_text(row["evidence"] or "", translator) if is_english_dominant(row["evidence"] or "") else row["evidence"]
            conn.execute(
                "UPDATE story_leads SET summary=?, evidence=? WHERE id=?",
                [new_summary, new_evidence, rid]
            )
            conn.commit()
            print(f"→ ✅")
            success += 1
        except Exception as e:
            print(f"→ ❌ {e}")
            failed += 1

    return {"total": total, "success": success, "failed": failed}


# ── 진입점 ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="영어 AI 분석 결과 → 한국어 번역")
    parser.add_argument("--stats",    action="store_true", help="현황 통계만 출력")
    parser.add_argument("--dry-run",  action="store_true", help="번역 없이 대상 목록만")
    parser.add_argument("--limit",    type=int, default=None, metavar="N", help="최대 처리 건수")
    parser.add_argument("--leads",    action="store_true", help="story_leads.summary도 번역")
    args = parser.parse_args()

    conn = get_conn()
    try:
        print_stats(conn)
        if args.stats:
            return

        translator = get_translator()

        # ai_comparisons 번역
        stats1 = translate_comparisons(conn, translator, args.limit, args.dry_run)
        print(f"\nai_comparisons: 성공 {stats1['success']}건 / 실패 {stats1.get('failed',0)}건")

        # story_leads 번역 (--leads 옵션 시)
        if args.leads:
            stats2 = translate_leads(conn, translator, args.limit, args.dry_run)
            print(f"story_leads:    성공 {stats2['success']}건 / 실패 {stats2.get('failed',0)}건")

        if not args.dry_run:
            print()
            print_stats(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
