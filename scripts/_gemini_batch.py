"""
scripts/_gemini_batch.py
─────────────────────────
Gemini 배치 호출 공통 모듈 — 여러 단서를 한 호출에 묶어 처리(호출 횟수 1/N).

무료 quota가 호출 횟수(RPD) 기준이므로, N건을 한 프롬프트로 묶으면 같은 한도로 N배 처리.

핵심:
  process_in_batches(items, item_id, render_item, instruction, output_example, on_results, ...)
    - items를 max_items씩 chunk → 각 chunk를 "각 항목 독립 분석, JSON 배열 응답" 프롬프트로 1호출
    - 응답 JSON 배열을 id로 원본 매칭 → on_results(matched) 콜백에서 DB 업데이트
    - chunk 파싱/매칭 실패 시 절반으로 쪼개 재귀 재시도 (최소 1건까지)
    - 일부 id 누락은 그냥 건너뜀 → 증분(IS NULL)이라 다음 실행에서 재처리
    - call_gemini가 모든 키·모델 소진하면 RuntimeError → 상위에서 종료(quota)

각 스크립트(_l2/_k3/_n1)는 render_item / instruction / output_example / on_results만 제공.
"""
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

ROOT = Path(__file__).parent.parent

DEFAULT_MODELS = [
    "gemini-flash-latest", "gemini-2.5-flash-lite", "gemini-2.5-flash",
    "gemini-3-flash-preview", "gemini-3.1-flash-lite",
]


def load_env():
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def load_keys() -> list:
    load_env()
    keys = [os.environ.get(k, "").strip()
            for k in ["GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"]]
    return [k for k in keys if k]


def call_gemini(prompt: str, max_output_tokens: int = 8192,
                temperature: float = 0.2, keys: list = None,
                models: list = None, timeout: int = 90) -> tuple:
    """모든 모델 × 모든 키 순회. 429/503/오류 시 다음 시도. 전부 실패하면 RuntimeError."""
    keys = keys or load_keys()
    models = models or DEFAULT_MODELS
    if not keys:
        raise RuntimeError("GEMINI_API_KEY 없음")
    for model in models:
        for k in keys:
            url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
                   f"{model}:generateContent?key={k}")
            payload = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": max_output_tokens,
                    "temperature": temperature,
                    "responseMimeType": "application/json",
                },
            }).encode("utf-8")
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"}, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    d = json.loads(r.read().decode("utf-8"))
                parts = d["candidates"][0]["content"]["parts"]
                text = ""
                for p in reversed(parts):
                    if not p.get("thought", False) and p.get("text", ""):
                        text = p["text"]
                        break
                if not text:
                    text = parts[-1].get("text", "")
                return text, model
            except urllib.error.HTTPError as e:
                if e.code in (429, 503):
                    continue
                continue
            except Exception:
                continue
    raise RuntimeError("모든 모델·키 소진")


def parse_json_array(txt: str):
    """응답에서 JSON 배열 추출. 실패 시 None."""
    if not txt:
        return None
    s = txt.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-z]*\n?", "", s)
        s = re.sub(r"\n?```$", "", s)
    # 1) 그대로
    try:
        v = json.loads(s)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            # {"items":[...]} 또는 단일 객체
            for key in ("items", "results", "data"):
                if isinstance(v.get(key), list):
                    return v[key]
            return [v]
    except Exception:
        pass
    # 2) 배열 블록 추출
    m = re.search(r"\[.*\]", s, re.DOTALL)
    if m:
        try:
            v = json.loads(m.group(0))
            if isinstance(v, list):
                return v
        except Exception:
            pass
    # 3) 잘린 배열 복구 — 마지막 완전한 '}' 까지 자르고 ']' 붙여 재파싱
    #    (maxOutputTokens 초과로 응답이 중간에 끊긴 경우 앞부분 객체는 건짐)
    start = s.find("[")
    last = s.rfind("}")
    if start != -1 and last > start:
        candidate = s[start:last + 1] + "]"
        try:
            v = json.loads(candidate)
            if isinstance(v, list) and v:
                return v
        except Exception:
            pass
    # 4) 개별 객체 스캔 (플랫 객체 가정 — 중첩 없는 {...})
    objs = []
    for mm in re.finditer(r"\{[^{}]*\}", s):
        try:
            objs.append(json.loads(mm.group(0)))
        except Exception:
            pass
    if objs:
        return objs
    return None


def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _build_batch_prompt(batch, item_id, render_item, instruction, output_example) -> str:
    blocks = []
    for it in batch:
        blocks.append(f"### id={item_id(it)}\n{render_item(it)}")
    items_text = "\n\n".join(blocks)
    n = len(batch)
    return f"""{instruction}

══════════════════════════════════════════
아래 {n}개 항목을 **각각 독립적으로** 분석하라.
반드시 **JSON 배열로만** 응답하라. 각 객체에 입력의 "id" 값을 그대로 넣어라.
배열 길이는 정확히 {n}개여야 한다. 설명·마크다운 금지, JSON 배열만 출력.

[항목 {n}개]
{items_text}

══════════════════════════════════════════
[출력: JSON 배열 — 각 객체 형식]
{output_example}

위 형식의 객체 {n}개를 담은 JSON 배열만 출력하라. 예: [{{"id": ...}}, {{"id": ...}}]
"""


def process_in_batches(items, item_id, render_item, instruction, output_example,
                       on_results, max_items=8, max_output_tokens=8192,
                       temperature=0.2, throttle=4.0, progress=None,
                       keys=None, models=None):
    """배치 처리 오케스트레이터.

    items       : 처리할 원본 항목 리스트 (sqlite3.Row 등)
    item_id     : item -> id (정수/문자)
    render_item : item -> 프롬프트에 넣을 텍스트 블록
    instruction : 평가 지시 + 기준 (공통 헤더)
    output_example : 각 객체 JSON 예시 (문자열)
    on_results  : matched [(item, obj), ...] -> 처리 건수(int). DB 업데이트 담당.
    반환: (total_ok, total_fail, total_calls)
    """
    keys = keys or load_keys()
    models = models or DEFAULT_MODELS
    total_ok = total_fail = total_calls = 0

    def _run_chunk(batch, depth=0):
        nonlocal total_ok, total_fail, total_calls
        prompt = _build_batch_prompt(batch, item_id, render_item, instruction, output_example)
        text, model = call_gemini(prompt, max_output_tokens, temperature,
                                  keys=keys, models=models)  # RuntimeError → 상위 전파
        total_calls += 1
        arr = parse_json_array(text)
        matched = []
        if arr:
            by_id = {}
            for o in arr:
                if isinstance(o, dict) and "id" in o:
                    by_id[str(o["id"])] = o
            for it in batch:
                o = by_id.get(str(item_id(it)))
                if o:
                    matched.append((it, o))
        if matched:
            done = on_results(matched)
            total_ok += (done if isinstance(done, int) else len(matched))
            # 매칭 안 된 항목은 다음 실행에서 재처리(증분) — fail로 세지 않음
            time.sleep(throttle)
            return
        # 파싱/매칭 전무 → 절반으로 재귀 (depth 제한)
        if len(batch) > 1 and depth < 3:
            mid = len(batch) // 2
            _run_chunk(batch[:mid], depth + 1)
            _run_chunk(batch[mid:], depth + 1)
        else:
            total_fail += len(batch)
            time.sleep(throttle)

    for batch in chunked(items, max_items):
        try:
            _run_chunk(batch)
        except RuntimeError:
            # quota 소진 — 남은 것은 다음 실행에서 (증분)
            if progress:
                progress(total_ok, total_fail, total_calls, exhausted=True)
            raise
        if progress:
            progress(total_ok, total_fail, total_calls, exhausted=False)
    return total_ok, total_fail, total_calls
