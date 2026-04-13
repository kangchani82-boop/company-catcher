"""
dart/analyzer.py
─────────────────
DART 사업보고서 공급망 분석기 (Claude Sonnet 4.6)

- dart_reports.db에서 보고서 텍스트 로드
- Claude Sonnet 4.6으로 공급사/고객사/경쟁사 추출
- 결과를 supply_chain 테이블에 저장
- config/reverse_supply_chain.py에서 _load_dart_supply_map()으로 자동 로드

비용: ~$0.09/보고서 (Sonnet 4.6 기준, ~$26/291건)

환경변수:
  ANTHROPIC_API_KEY — Anthropic API 키 (필수)

사용법:
  python dart/analyzer.py --dry-run --limit 3
  python dart/analyzer.py --analyze --limit 10
"""

import argparse
import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("dart.analyzer")

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

ANALYSIS_PROMPT = """다음은 한국 기업 '{company}'의 사업보고서 일부입니다.

이 보고서에서 다음 4가지 유형의 관계를 추출해주세요:
1. supplier (공급사): 이 기업에 원자재/부품/서비스를 공급하는 회사
2. customer (고객사): 이 기업의 제품/서비스를 구매하는 회사
3. partner (파트너): 합작/MOU/공동개발 등 파트너십 관계
4. competitor (경쟁사): 같은 시장에서 경쟁하는 회사

JSON 배열로 응답해주세요:
[
  {{"type": "supplier", "partner": "회사명 (영문 또는 한글)", "context": "관계 설명 1줄"}},
  ...
]

보고서 텍스트:
{text}
"""


class DARTAnalyzer:
    """DART 보고서 공급망 분석기"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._client = None
        if self.api_key:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                logger.warning("anthropic 패키지 미설치")
        else:
            logger.warning("ANTHROPIC_API_KEY 미설정")

    def analyze_report(self, company: str, text: str, max_text: int = 8000) -> list[dict]:
        """
        보고서 텍스트에서 공급망 관계 추출

        Returns:
            [{"type": "supplier", "partner": "Intel", "context": "반도체 원자재 공급"}]
        """
        if not self._client:
            logger.error("Claude API 클라이언트 없음")
            return []

        prompt = ANALYSIS_PROMPT.format(company=company, text=text[:max_text])

        try:
            response = self._client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2000,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}],
            )
            content = response.content[0].text.strip()

            # JSON 추출
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()

            relations = json.loads(content)
            if isinstance(relations, list):
                return relations
            return []

        except json.JSONDecodeError:
            logger.error(f"JSON 파싱 실패: {company}")
            return []
        except Exception as e:
            logger.error(f"분석 실패 ({company}): {e}")
            return []

    def analyze_from_db(self, limit: int = 10, dry_run: bool = False) -> dict:
        """
        DB에서 미분석 보고서를 가져와 분석

        Returns:
            {"analyzed": N, "relations_found": M, "errors": E}
        """
        if not DB_PATH.exists():
            logger.error(f"DART DB 없음: {DB_PATH}")
            return {"analyzed": 0, "relations_found": 0, "errors": 0}

        db = sqlite3.connect(str(DB_PATH))
        db.row_factory = sqlite3.Row

        # 아직 분석되지 않은 보고서 (raw_text가 있고, supply_chain에 없는 것)
        rows = db.execute("""
            SELECT r.* FROM reports r
            WHERE r.raw_text IS NOT NULL AND r.raw_text != ''
            AND r.rcept_no NOT IN (SELECT DISTINCT source_report FROM supply_chain)
            LIMIT ?
        """, (limit,)).fetchall()

        stats = {"analyzed": 0, "relations_found": 0, "errors": 0}

        for row in rows:
            company = row["corp_name"]
            rcept_no = row["rcept_no"]

            if dry_run:
                print(f"  [DRY-RUN] 분석 대상: {company} ({rcept_no})")
                stats["analyzed"] += 1
                continue

            logger.info(f"분석 중: {company} ({rcept_no})")
            relations = self.analyze_report(company, row["raw_text"])

            if relations:
                from dart.report_collector import DARTCollector
                collector = DARTCollector()
                collector.save_supply_chain(
                    row["corp_code"], company, relations, rcept_no
                )
                collector.close()
                stats["relations_found"] += len(relations)
            else:
                stats["errors"] += 1

            stats["analyzed"] += 1

        db.close()
        return stats


def main():
    parser = argparse.ArgumentParser(description="DART 보고서 공급망 분석기")
    parser.add_argument("--analyze", action="store_true", help="미분석 보고서 분석")
    parser.add_argument("--dry-run", action="store_true", help="분석 없이 대상만 확인")
    parser.add_argument("--limit", type=int, default=5, help="분석 건수 제한")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    analyzer = DARTAnalyzer()

    if args.analyze or args.dry_run:
        stats = analyzer.analyze_from_db(limit=args.limit, dry_run=args.dry_run)
        print(f"분석 결과: {stats}")
    else:
        print("사용법: python dart/analyzer.py --analyze --limit 5")
        print("        python dart/analyzer.py --dry-run --limit 10")


if __name__ == "__main__":
    main()
