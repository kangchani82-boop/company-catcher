"""
scripts/extract_ir_from_gmail.py (v2)
──────────────────────────────────────
Gmail 보도자료에서 IR/PR 담당자 정보 자동 추출.

v2 개선:
  - 발신 도메인 우선 매칭 (companies.hm_url)
  - PR/IR 대행사 화이트리스트 → 본문 의존
  - 한국 성씨 200개 필터 (이름 추출 정확도)
  - 이메일 없어도 이름+전화만 있으면 등록
  - 신뢰도 등급 (A_verified, B_extracted, ...)

실행:
  python scripts/extract_ir_from_gmail.py --max 100
  python scripts/extract_ir_from_gmail.py                   # 전체
  python scripts/extract_ir_from_gmail.py --reset           # 기존 PRESS_RELEASE 데이터 삭제 후 재수집
"""

import io
import os
import re
import sys
import base64
import sqlite3
import argparse
import time
from pathlib import Path
from datetime import datetime
from email.utils import parseaddr
from email.header import decode_header
from urllib.parse import urlparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
SECRETS = ROOT / "secrets"
TOKEN_FILE = SECRETS / "token.json"
DB_PATH = ROOT / "data" / "dart" / "dart_reports.db"

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]

DEFAULT_QUERY = (
    'subject:(보도자료 OR 알려드립니다 OR 안내드립니다 OR 자료요청 '
    'OR 자료송부 OR 자료입니다 OR 보내드립니다 OR "press release")'
)

# ── PR/IR 대행사 도메인 ──────────────────────────────────────────────────────
PR_AGENCY_DOMAINS = {
    'irkudos.co.kr', 'irmed.co.kr', 'irup.co.kr', 'their.co.kr',
    'upcom.co.kr', 'kggroup.co.kr', 'edelman.com', 'fleishmanhillard.com',
    'kosdaqca.or.kr', 'klca.or.kr', 'prain.co.kr', 'fnsbiz.co.kr',
    'kpr.co.kr', 'kookjepr.com', 'communicaco.kr', 'wiscom.kr',
    'media2.co.kr', 'pr-one.co.kr', 'naver.com', 'gmail.com',
    'daum.net', 'hanmail.net', 'kakao.com', 'naver.net',
}

# 한국 주요 성씨 (200대) — 이름 검증용
KOREAN_SURNAMES = {
    '김','이','박','최','정','강','조','윤','장','임','한','오','서','신','권',
    '황','안','송','류','전','홍','고','문','양','손','배','조','백','허','유',
    '남','심','노','정','하','곽','성','차','주','우','구','신','임','전','민',
    '유','류','나','진','지','엄','채','원','천','방','공','강','현','함','변',
    '염','양','변','여','추','노','도','소','신','석','선','설','마','길','연',
    '위','표','명','기','반','왕','금','옥','육','인','맹','제','모','장','남',
    '탁','국','여','진','어','은','편','구','용','예','봉','한','경','사','부',
    '황보','남궁','선우','독고','동방','서문','제갈','사공','어금','복',
    '시','단','주','연','전','강','노','왕','대','모','담','두',
}

# 직책 (한국어 + 영문)
TITLES = ['부사장','전무','상무','이사','본부장','실장','팀장','부장','차장',
          '과장','대리','사원','주임','매니저','선임','책임','수석',
          'Director','Manager','VP','CFO','CEO','CTO','CMO','SVP','EVP',
          'Senior Manager','Lead','Head']
# 직책 정규식 (한국어 직책 우선)
TITLE_RE = re.compile(
    r'([가-힣]{2,4})\s*[\(\[]?\s*(' + '|'.join(re.escape(t) for t in TITLES) + r')[\)\]]?'
)

# 부서
DEPT_RE = re.compile(r'(IR|PR|홍보|커뮤니케이션|투자홍보|재무|기획|경영지원|대외협력|마케팅)\s*(팀|실|부|본부|그룹|팀장|실장)?')

# 연락처 정규식
EMAIL_RE = re.compile(r"[\w\._\-+]+@[\w\._\-]+\.[a-zA-Z]{2,}")
PHONE_RE = re.compile(r'(?:\+?82[-\s]?|0)?(\d{2,3})[-\s\.\)](\d{3,4})[-\s\.](\d{4})')
MOBILE_RE = re.compile(r'(?:\+?82[-\s]?|0)?(010|011|016|017|018|019)[-\s\.\)]?(\d{3,4})[-\s\.](\d{4})')

# 본문에서 contact 블록 시작 키워드
CONTACT_KEYWORDS_RE = re.compile(
    r'(■\s*문의\s*처?|■\s*담당|■\s*Contact|※\s*문의|\*\s*문의|문의\s*처?\s*[:：]|'
    r'담당\s*자?\s*[:：]|연락처\s*[:：]|취재\s*문의|보도\s*문의|Contact\s*[:：]|'
    r'#{2,}|={3,}|-{5,})'
)

# 거짓 이름 매칭 차단 (직책-only 또는 명사구)
FAKE_NAMES = {
    '대표', '책임', '담당', '문의', '여러', '각종', '각 위', '각자', '아무런',
    '최고', '최상', '저는', '관련', '주요', '특별', '신규', '기존',
    '오늘', '내일', '이상', '이하', '이내', '관계', '주식', '회사',
}


def get_db():
    db = sqlite3.connect(str(DB_PATH), timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    return db


def get_gmail_service():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


# ── 헤더 디코딩 ──────────────────────────────────────────────────────────────
def decode_mime_header(value):
    if not value:
        return ""
    try:
        parts = decode_header(value)
        out = []
        for txt, enc in parts:
            if isinstance(txt, bytes):
                out.append(txt.decode(enc or "utf-8", errors="replace"))
            else:
                out.append(txt)
        return "".join(out)
    except Exception:
        return value


# ── 본문 추출 ────────────────────────────────────────────────────────────────
def decode_body(payload):
    body = ""

    def walk(part):
        nonlocal body
        mime = part.get("mimeType", "")
        if mime == "text/plain" and "data" in part.get("body", {}):
            try:
                body += base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace") + "\n"
            except Exception:
                pass
        elif mime == "text/html" and "data" in part.get("body", {}) and not body:
            try:
                txt = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
                txt = re.sub(r"<style[^>]*>.*?</style>", "", txt, flags=re.DOTALL | re.IGNORECASE)
                txt = re.sub(r"<script[^>]*>.*?</script>", "", txt, flags=re.DOTALL | re.IGNORECASE)
                txt = re.sub(r"<br\s*/?>", "\n", txt, flags=re.IGNORECASE)
                txt = re.sub(r"</p>", "\n", txt, flags=re.IGNORECASE)
                txt = re.sub(r"</tr>", "\n", txt, flags=re.IGNORECASE)
                txt = re.sub(r"</td>", " | ", txt, flags=re.IGNORECASE)
                txt = re.sub(r"<[^>]+>", " ", txt)
                txt = re.sub(r"&nbsp;", " ", txt)
                txt = re.sub(r"&amp;", "&", txt)
                txt = re.sub(r"&[lg]t;", "", txt)
                body += txt + "\n"
            except Exception:
                pass
        for child in part.get("parts", []):
            walk(child)

    walk(payload)
    return body.strip()


# ── 회사 매칭 인덱스 ─────────────────────────────────────────────────────────
def normalize_corp_name(name):
    """회사명 정규화 — (주), 주식회사, ㈜, 공백 제거"""
    n = name.strip()
    n = re.sub(r'\(주\)|주식회사|㈜|（주）', '', n)
    n = n.replace(' ', '').replace('\t', '')
    return n.strip()


def domain_root(domain):
    """example.com → example, www.samsung.com → samsung"""
    d = (domain or '').lower().replace('www.', '').strip()
    parts = d.split('.')
    if len(parts) >= 2:
        return parts[0]
    return d


def build_corp_index(db):
    """매칭용 인덱스 — 정규화된 회사명 + 종목코드 + 부분 매칭"""
    rows = db.execute("SELECT corp_code, corp_name, stock_code FROM companies").fetchall()
    by_name = {}      # normalized_name → (corp_code, corp_name)
    by_partial = {}   # partial substring → list of (corp_code, full_name)
    by_stock = {}     # stock_code → (corp_code, corp_name)  ★ 신규
    for r in rows:
        cn = (r['corp_name'] or '').strip()
        sc = (r['stock_code'] or '').strip()
        if not cn:
            continue
        norm = normalize_corp_name(cn)
        if norm:
            by_name[norm] = (r['corp_code'], cn)
        if sc and sc != '0' and len(sc) == 6:
            by_stock[sc] = (r['corp_code'], cn)
        for w in re.split(r'[\s\(\)\[\]/_\-,]+', cn):
            w = normalize_corp_name(w)
            if len(w) >= 2 and w not in FAKE_NAMES:
                by_partial.setdefault(w, []).append((r['corp_code'], cn))
    return by_name, by_partial, by_stock


def build_domain_index(db):
    """{domain_root: corp_code} — DART 회사들의 hm_url에서 도메인 추출"""
    # ir_contacts 의 homepage 필드도 활용
    domain_map = {}
    rows = db.execute("""
        SELECT DISTINCT corp_code, corp_name, homepage FROM ir_contacts
        WHERE homepage IS NOT NULL AND homepage != ''
    """).fetchall()
    for r in rows:
        try:
            d = urlparse(r['homepage'] if r['homepage'].startswith('http') else 'http://' + r['homepage']).netloc
            root = domain_root(d)
            if root and len(root) >= 2 and root not in {'naver', 'daum', 'gmail', 'kakao'}:
                domain_map[root] = (r['corp_code'], r['corp_name'])
        except Exception:
            pass
    return domain_map


# ── contact 블록 추출 ───────────────────────────────────────────────────────
def find_contact_block(body):
    """본문에서 가장 정확한 contact 블록 찾기"""
    if not body:
        return ""
    # 1) "■ 문의처" 같은 키워드 발견 → 그 이후
    m = CONTACT_KEYWORDS_RE.search(body)
    if m:
        block = body[m.start():m.start() + 1500]
        return block
    # 2) 끝 부분 1500자
    return body[-1500:]


def is_valid_korean_name(name):
    """한국 이름 검증 — 성씨가 한국 성씨 리스트에 있어야 함"""
    if not name or len(name) < 2 or len(name) > 4:
        return False
    if name in FAKE_NAMES:
        return False
    # 한글이 아니면 일단 영문 이름 가능성
    if not re.match(r'^[가-힣]+$', name):
        return False
    # 첫 글자 = 성씨 검증
    first = name[0]
    if first in KOREAN_SURNAMES:
        return True
    # 2글자 성씨 (남궁, 황보 등)
    two = name[:2]
    if two in KOREAN_SURNAMES:
        return True
    return False


def extract_contact_info(sender_name, sender_email, body, subject):
    """이메일에서 contact 정보 추출"""
    sig = find_contact_block(body)
    contacts = []

    # 1) 이메일 추출
    skip_patterns = ['noreply', 'no-reply', 'donotreply', 'mailer', 'postmaster',
                     '@accounts.google', 'webmaster', 'admin@']
    emails_in_body = []
    for em in EMAIL_RE.findall(sig):
        em_lower = em.lower()
        if any(s in em_lower for s in skip_patterns):
            continue
        if em not in emails_in_body:
            emails_in_body.append(em)

    # 발신자 자체 추가 (대행사 도메인 아닌 경우)
    if sender_email and not any(s in sender_email.lower() for s in skip_patterns):
        sender_domain = sender_email.split('@')[-1].lower() if '@' in sender_email else ''
        if sender_email not in emails_in_body and sender_domain not in PR_AGENCY_DOMAINS:
            emails_in_body.insert(0, sender_email)

    # 2) 이름·직책 추출 (한국 성씨 필터링)
    name_titles = []
    for nm, ti in TITLE_RE.findall(sig):
        if is_valid_korean_name(nm):
            name_titles.append((nm, ti))

    # 3) 부서
    depts = DEPT_RE.findall(sig)
    dept_str = depts[0][0] if depts else ''

    # 4) 휴대폰·전화
    mobiles = MOBILE_RE.findall(sig)
    mobile_str = f"0{mobiles[0][0]}-{mobiles[0][1]}-{mobiles[0][2]}" if mobiles else ''
    phones = PHONE_RE.findall(sig)
    phone_candidates = []
    for p in phones:
        prefix = p[0]
        if not prefix.startswith('0'):
            prefix = '0' + prefix
        phone_candidates.append(f"{prefix}-{p[1]}-{p[2]}")
    # 휴대폰과 같은 건 제외
    phone_candidates = [p for p in phone_candidates if p != mobile_str]
    phone_str = phone_candidates[0] if phone_candidates else ''

    # 5) 메인 contact 만들기
    primary_email = emails_in_body[0] if emails_in_body else ''
    primary_name = name_titles[0][0] if name_titles else ''
    primary_title = name_titles[0][1] if name_titles else ''

    # 등록 조건: 이메일 OR (이름 + 전화) 있어야 함
    if primary_email:
        contacts.append({
            'name': primary_name,
            'title': primary_title,
            'dept': dept_str,
            'phone': phone_str,
            'mobile': mobile_str,
            'email': primary_email,
            'email_secondary': emails_in_body[1] if len(emails_in_body) > 1 else '',
        })
    elif primary_name and (mobile_str or phone_str):
        # 이메일 없어도 이름+전화 있으면 등록
        contacts.append({
            'name': primary_name,
            'title': primary_title,
            'dept': dept_str,
            'phone': phone_str,
            'mobile': mobile_str,
            'email': '',
            'email_secondary': '',
        })

    return contacts


# ── 회사 매칭 (도메인 우선) ──────────────────────────────────────────────────
def match_company(sender_email, sender_name, subject, body,
                  by_name, by_partial, by_stock, domain_map):
    """
    회사 매칭 v4 — 우선순위:
    1. 본문 종목코드 (6자리)
    2. 제목 [회사명_보도자료] / [회사명 보도자료] / [XX-보도자료] 회사명
    3. 코스닥협회 제목 패턴
    4. 본문 첫 줄 회사명, ~
    5. 발신 도메인
    6. 본문 발신자 자기소개 (○○○ 커뮤니케이션팀)
    7. (주)○○ 패턴
    8. 발신자 이름 회사명
    """
    def try_name(cand_raw, conf='B_extracted'):
        cand = normalize_corp_name(cand_raw)
        if cand in by_name:
            return by_name[cand] + (conf,)
        # 부분 매칭 (단어가 unique한 경우)
        if len(cand) >= 3 and cand in by_partial:
            cands = by_partial[cand]
            if len(cands) == 1:
                return cands[0][0], cands[0][1], conf
        return None

    # 1) ★ 종목코드 매칭
    if body:
        head = body[:2000]
        for m in re.finditer(r'\(\s*(\d{6})\s*[,\s\)]', head):
            sc = m.group(1)
            if sc in by_stock:
                cc, cn = by_stock[sc]
                return cc, cn, 'A_verified'
        for m in re.finditer(r'(?:^|[\s\(])(\d{6})(?:[\s,\)])', head):
            sc = m.group(1)
            if sc in by_stock:
                cc, cn = by_stock[sc]
                return cc, cn, 'A_verified'

    # 2) 제목 패턴 (확장)
    if subject:
        # `[회사명 보도자료]` (공백) / `[회사명_보도자료]` (언더바) / `[회사명-보도자료]` (하이픈)
        for pat in [
            r'\[([가-힣A-Za-z0-9]{2,20})[\s_\-]+보도자료\]',
            r'\[([가-힣A-Za-z0-9]{2,20})\][_\s\-]*[\s\(]',
        ]:
            m = re.search(pat, subject)
            if m:
                r = try_name(m.group(1), 'A_verified')
                if r: return r
        # `[XX-보도자료] 회사명, ~` (XX는 대행사 등)
        m = re.search(r'\[[^\]]*보도자료[^\]]*\]\s*([가-힣A-Za-z0-9\s\(\)\.\-]{2,25}?)[\,\s]', subject)
        if m:
            cand_raw = m.group(1).strip()
            r = try_name(cand_raw, 'A_verified')
            if r: return r
            for w in re.split(r'[\s\(\)\[\]/_\-,]+', cand_raw):
                if len(w) >= 2:
                    r = try_name(w, 'B_extracted')
                    if r: return r
        # 3) 코스닥협회 패턴
        m = re.search(r'\[코스닥협회[^\]]*\]\s*([가-힣A-Za-z0-9]{2,20})', subject)
        if m:
            r = try_name(m.group(1), 'A_verified')
            if r: return r

    # 4) 본문 첫 줄 — 코스닥협회 메일은 본문 시작이 `㈜회사명, ...` 형식
    if body:
        # CSS noise 제거 후 분석
        clean_body = re.sub(r'#dext_body[^}]*\}|[\.\#][\w\-]+\s*\{[^}]*\}', '', body)
        clean_body = re.sub(r'<[^>]+>', '', clean_body)
        head = clean_body[:800]
        # `[보도자료] 회사명, ~` 같은 본문 첫 줄
        m = re.search(r'\[보도자료\]\s*((?:\(주\)|㈜)?[가-힣A-Za-z0-9]{2,20})\s*[\,\、]', head)
        if m:
            cand_raw = m.group(1).strip()
            r = try_name(cand_raw, 'A_verified')
            if r: return r
        # `㈜회사명, 제목` / `회사명㈜, 제목`
        for pat in [
            r'(?:^|\n)\s*(?:\(주\)|㈜)\s*([가-힣A-Za-z0-9]{2,20})\s*[\,\、]',
            r'(?:^|\n)\s*([가-힣A-Za-z0-9]{2,20})\s*(?:\(주\)|㈜)\s*[\,\、]',
        ]:
            m = re.search(pat, head)
            if m:
                r = try_name(m.group(1), 'A_verified')
                if r: return r

    # 5) 발신 도메인 매칭
    sender_domain = sender_email.split('@')[-1].lower() if '@' in sender_email else ''
    sender_domain_root = domain_root(sender_domain)
    is_pr_agency = sender_domain in PR_AGENCY_DOMAINS

    if not is_pr_agency and sender_domain_root in domain_map:
        cc, cn = domain_map[sender_domain_root]
        return cc, cn, 'A_verified'

    # 6) 본문 자기소개 — `회사명 커뮤니케이션팀입니다` / `회사명 홍보실입니다`
    if body:
        head = body[:500]
        for pat in [
            r'([가-힣A-Za-z0-9]{2,20})\s*(?:커뮤니케이션|홍보|PR|IR)\s*(?:팀|실|부|본부|담당)',
            r'안녕하(?:세요|십니까)[\s\,\.]+(?:기자님[\s\,\.]+)?([가-힣A-Za-z0-9]{2,20})\s*(?:입니다|커뮤니케이션|홍보)',
        ]:
            m = re.search(pat, head)
            if m:
                cand = m.group(1)
                if cand in {'안녕', '기자', '저는', '오늘'}:
                    continue
                r = try_name(cand, 'B_extracted')
                if r: return r

    # 7) (주)○○ 본문 패턴
    if body:
        head = body[:1000]
        for pat in [
            r'(?:\(주\)|㈜|주식회사)\s*([가-힣A-Za-z0-9]{2,20})',
            r'([가-힣A-Za-z0-9]{2,20})\s*(?:\(주\)|㈜)',
        ]:
            m = re.search(pat, head)
            if m:
                r = try_name(m.group(1), 'B_extracted')
                if r: return r

    # 8) 발신자 표시 이름 (사람 이름 제외)
    name = sender_name or ''
    if name and not is_valid_korean_name(name):
        r = try_name(name, 'A_verified')
        if r: return r
        for word in re.split(r'[\s\(\)\[\]/_\-,]+', name):
            if len(word) >= 3:
                r = try_name(word, 'B_extracted')
                if r: return r

    return None, sender_name or sender_email or '', 'unknown'


# ── DB 저장 ──────────────────────────────────────────────────────────────────
def save_contact(db, corp_code, corp_name, contact, source_msg_id, confidence):
    """ir_contacts에 UPSERT"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    email = contact.get('email', '')
    email_2 = contact.get('email_secondary', '')

    # confidence 세부 결정
    if email and contact.get('name') and (contact.get('phone') or contact.get('mobile')):
        conf = confidence if confidence in {'A_verified'} else 'A_complete'
    elif email:
        conf = confidence if confidence in {'A_verified'} else 'A_email_only'
    elif contact.get('name') and (contact.get('phone') or contact.get('mobile')):
        conf = 'B_phone_only'
    else:
        return False

    name_part = contact.get('name', '')
    title_part = contact.get('title', '')
    dept = contact.get('dept', '')
    phone = contact.get('phone', '')
    mobile = contact.get('mobile', '')

    # corp_code NULL/UNKNOWN 처리
    cc = corp_code or 'UNKNOWN'

    # 이메일 NULL일 땐 임시 키 (이름+전화 조합)
    unique_email = email if email else f"_phone:{cc}:{mobile or phone}:{name_part}"

    try:
        db.execute("""
            INSERT INTO ir_contacts
                (corp_code, corp_name, ir_email, ir_email_secondary,
                 ir_phone, ir_mobile, ir_name, ir_dept, ir_title,
                 source, source_url, confidence, mx_verified,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(corp_code, ir_email) DO UPDATE SET
                ir_phone     = COALESCE(NULLIF(excluded.ir_phone,''),     ir_contacts.ir_phone),
                ir_mobile    = COALESCE(NULLIF(excluded.ir_mobile,''),    ir_contacts.ir_mobile),
                ir_name      = COALESCE(NULLIF(excluded.ir_name,''),      ir_contacts.ir_name),
                ir_dept      = COALESCE(NULLIF(excluded.ir_dept,''),      ir_contacts.ir_dept),
                ir_title     = COALESCE(NULLIF(excluded.ir_title,''),     ir_contacts.ir_title),
                source       = excluded.source,
                source_url   = excluded.source_url,
                confidence   = excluded.confidence,
                updated_at   = excluded.updated_at
        """, [cc, corp_name, unique_email if email else None,
              email_2 or None, phone, mobile, name_part, dept, title_part,
              'PRESS_RELEASE', f'gmail:{source_msg_id}',
              conf, 1 if email else 0, now, now])
        db.commit()
        return True
    except sqlite3.IntegrityError:
        # email NULL + 다른 이름·전화 조합 등록 시도
        if not email:
            try:
                db.execute("""
                    INSERT INTO ir_contacts
                        (corp_code, corp_name, ir_phone, ir_mobile, ir_name, ir_dept, ir_title,
                         source, source_url, confidence, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [cc, corp_name, phone, mobile, name_part, dept, title_part,
                      'PRESS_RELEASE', f'gmail:{source_msg_id}', conf, now, now])
                db.commit()
                return True
            except Exception:
                pass
        return False
    except Exception as e:
        print(f"   DB ERR: {e}")
        return False


# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=100)
    parser.add_argument("--query", type=str, default=DEFAULT_QUERY)
    parser.add_argument("--days", type=int, default=730)
    parser.add_argument("--reset", action="store_true", help="기존 PRESS_RELEASE 삭제 후 재수집")
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--skip", type=int, default=0, help="앞에서 N건 스킵 (배치 페이지네이션용)")
    parser.add_argument("--batch-num", type=int, default=0, help="배치 번호 (로그용)")
    parser.add_argument("--skip-processed", action="store_true",
                       help="이미 ir_emails에 있는 gmail msg_id는 스킵")
    parser.add_argument("--json-out", type=str, default="",
                       help="결과 JSON 저장 경로 (배치 통계용)")
    args = parser.parse_args()

    db = get_db()

    if args.stats:
        n = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE source='PRESS_RELEASE'").fetchone()[0]
        n_match = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE source='PRESS_RELEASE' AND corp_code != 'UNKNOWN'").fetchone()[0]
        n_corp = db.execute("SELECT COUNT(DISTINCT corp_code) FROM ir_contacts WHERE source='PRESS_RELEASE' AND corp_code != 'UNKNOWN'").fetchone()[0]
        print(f"PRESS_RELEASE: 총 {n}건 / 매칭 {n_match}건 / 고유 회사 {n_corp}개")
        return

    if args.reset:
        n = db.execute("DELETE FROM ir_contacts WHERE source='PRESS_RELEASE'").rowcount
        db.commit()
        print(f"기존 PRESS_RELEASE 데이터 {n}건 삭제\n")

    print("━" * 60)
    print("  Gmail IR 추출 v2")
    print("━" * 60)

    service = get_gmail_service()
    by_name, by_partial, by_stock = build_corp_index(db)
    domain_map = build_domain_index(db)
    print(f"\n회사 인덱스 — 정규화: {len(by_name)} / 부분: {len(by_partial)} / 종목코드: {len(by_stock)} / 도메인: {len(domain_map)}\n")

    query = f"{args.query} newer_than:{args.days}d"
    # Gmail API는 한 번에 최대 500개. skip + max 만큼 가져와서 자르기
    fetch_target = args.skip + args.max
    msg_ids = []
    page_token = None
    while True:
        resp = service.users().messages().list(
            userId="me", q=query, pageToken=page_token, maxResults=500
        ).execute()
        msg_ids.extend(m["id"] for m in resp.get("messages", []))
        if len(msg_ids) >= fetch_target:
            break
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    # skip 적용
    msg_ids = msg_ids[args.skip:args.skip + args.max]

    # 이미 처리된 메시지 ID 스킵
    if args.skip_processed:
        processed = set(r[0] for r in db.execute(
            "SELECT DISTINCT REPLACE(source_url, 'gmail:', '') FROM ir_contacts "
            "WHERE source='PRESS_RELEASE' AND source_url LIKE 'gmail:%'"
        ).fetchall())
        msg_ids = [m for m in msg_ids if m not in processed]
        print(f"이미 처리된 {len(processed)}건 중 {args.max - len(msg_ids)}건 스킵")
    print(f"검색 결과: {len(msg_ids):,}건 처리 시작 (skip={args.skip}, batch#{args.batch_num})\n")

    stats = {
        'processed': 0, 'saved': 0,
        'A_verified': 0, 'B_extracted': 0, 'unknown': 0,
        'no_email_no_phone': 0, 'errors': 0,
    }

    for i, mid in enumerate(msg_ids, 1):
        try:
            msg = service.users().messages().get(userId="me", id=mid, format="full").execute()
            payload = msg.get("payload", {})
            headers = {h["name"]: h["value"] for h in payload.get("headers", [])}

            from_raw = decode_mime_header(headers.get("From", ""))
            subject = decode_mime_header(headers.get("Subject", ""))
            sender_name, sender_email = parseaddr(from_raw)
            sender_name = decode_mime_header(sender_name)

            body = decode_body(payload)
            stats['processed'] += 1

            # 회사 매칭
            cc, cn, conf = match_company(sender_email, sender_name, subject, body,
                                          by_name, by_partial, by_stock, domain_map)
            stats[conf] = stats.get(conf, 0) + 1

            # contact 추출
            contacts = extract_contact_info(sender_name, sender_email, body, subject)
            if not contacts:
                stats['no_email_no_phone'] += 1
                continue

            for c in contacts:
                if save_contact(db, cc, cn or sender_name, c, mid, conf):
                    stats['saved'] += 1

            if i % 25 == 0:
                print(f"  [{i:4d}/{len(msg_ids):4d}] 저장 {stats['saved']:4d} | "
                      f"A_verified {stats['A_verified']:3d} | B_extracted {stats['B_extracted']:3d} | "
                      f"unknown {stats['unknown']:3d}")

        except Exception as e:
            stats['errors'] += 1
            if stats['errors'] <= 5:
                print(f"  [{i:4d}] ERR: {str(e)[:80]}")

        if i % 20 == 0:
            time.sleep(0.4)

    # 결과
    print("\n" + "━" * 60)
    print("  추출 v2 완료")
    print("━" * 60)
    print(f"  처리 메시지       : {stats['processed']:,}")
    print(f"  ✅ contact 저장   : {stats['saved']:,}")
    print(f"  매칭 등급별:")
    print(f"    A_verified  (도메인 일치)    : {stats['A_verified']:,}")
    print(f"    B_extracted (본문 추출)      : {stats['B_extracted']:,}")
    print(f"    unknown                      : {stats['unknown']:,}")
    print(f"  ⚪ 추출 실패     : {stats['no_email_no_phone']:,}")
    print(f"  ❌ 에러          : {stats['errors']:,}")

    # DB 통계
    n_press = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE source='PRESS_RELEASE'").fetchone()[0]
    n_match = db.execute("SELECT COUNT(*) FROM ir_contacts WHERE source='PRESS_RELEASE' AND corp_code != 'UNKNOWN'").fetchone()[0]
    n_corp = db.execute("SELECT COUNT(DISTINCT corp_code) FROM ir_contacts WHERE source='PRESS_RELEASE' AND corp_code != 'UNKNOWN'").fetchone()[0]
    rate = 100 * n_match / max(n_press, 1)
    batch_match_rate = 100 * stats['A_verified'] / max(stats['processed'], 1)
    print(f"\n[전체] PRESS_RELEASE: {n_press}건 / 매칭 {n_match}건 ({rate:.1f}%) / 고유 회사 {n_corp}개")

    # JSON 저장 (배치 통계용)
    if args.json_out:
        import json
        result = {
            'batch_num': args.batch_num,
            'skip': args.skip,
            'max': args.max,
            'processed': stats['processed'],
            'saved': stats['saved'],
            'A_verified': stats['A_verified'],
            'B_extracted': stats['B_extracted'],
            'unknown': stats['unknown'],
            'no_email_no_phone': stats['no_email_no_phone'],
            'errors': stats['errors'],
            'batch_match_rate': batch_match_rate,
            'cumulative_press': n_press,
            'cumulative_match': n_match,
            'cumulative_corp': n_corp,
            'cumulative_rate': rate,
        }
        with open(args.json_out, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
