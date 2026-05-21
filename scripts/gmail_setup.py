"""
scripts/gmail_setup.py
──────────────────────
Gmail OAuth 첫 인증 — 1회만 실행.

실행 방법:
  python scripts/gmail_setup.py

동작:
  1) secrets/credentials.json 읽기
  2) 브라우저 자동 열기 → fin@finance-scope.com 로그인 → 권한 승인
  3) secrets/token.json 자동 저장
  4) 인증 후 메일함 1건 미리보기로 검증

이후 다른 스크립트는 token.json 으로 자동 인증 (refresh 자동).
"""

import io
import sys
import json
from pathlib import Path

# UTF-8 출력
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
SECRETS = ROOT / "secrets"
CRED_FILE = SECRETS / "credentials.json"
TOKEN_FILE = SECRETS / "token.json"

# OAuth 스코프 — 보내기 + 읽기 + 라벨링
SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]


def main():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

    print("━" * 60)
    print("  Gmail OAuth 첫 인증 시작")
    print("━" * 60)

    if not CRED_FILE.exists():
        print(f"\n❌ {CRED_FILE} 파일이 없습니다.")
        print(f"   Google Cloud Console에서 OAuth 클라이언트 ID 발급 후")
        print(f"   credentials.json 으로 저장해주세요.")
        sys.exit(1)

    creds = None
    if TOKEN_FILE.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
            print("✓ 기존 토큰 발견")
        except Exception as e:
            print(f"⚠ 기존 토큰 로드 실패: {e}")

    # 토큰 만료/없음 → 갱신 또는 신규 발급
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("→ 토큰 만료 — refresh 시도...")
            creds.refresh(Request())
            print("✓ 토큰 자동 갱신 완료")
        else:
            print("→ 새 OAuth 인증 시작")
            print("  브라우저가 열립니다. fin@finance-scope.com 으로 로그인 후 권한 승인하세요.")
            print("  (권한: Gmail 읽기 + 보내기 + 라벨)")
            print()
            flow = InstalledAppFlow.from_client_secrets_file(str(CRED_FILE), SCOPES)
            # 로컬 서버에서 콜백 받기 (자동)
            creds = flow.run_local_server(port=0)
            print("✓ OAuth 인증 성공")

        # 토큰 저장
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        print(f"✓ 토큰 저장: {TOKEN_FILE}")

    # 검증 — 사용자 정보 + 메일함 1건 가져오기
    print("\n→ Gmail 연결 검증...")
    try:
        service = build("gmail", "v1", credentials=creds)
        # 1) 프로필
        profile = service.users().getProfile(userId="me").execute()
        print(f"  ✓ 메일주소     : {profile.get('emailAddress')}")
        print(f"  ✓ 총 메시지     : {profile.get('messagesTotal'):,}건")
        print(f"  ✓ 총 스레드     : {profile.get('threadsTotal'):,}건")

        # 2) 라벨 목록 (보도자료 라벨 있는지)
        labels = service.users().labels().list(userId="me").execute().get("labels", [])
        custom_labels = [l for l in labels if l.get("type") == "user"]
        print(f"  ✓ 사용자 라벨   : {len(custom_labels)}개")
        if custom_labels:
            sample = [l["name"] for l in custom_labels[:8]]
            print(f"     샘플: {sample}")

        # 3) 받은편지함 최근 1건 미리보기
        msgs = service.users().messages().list(
            userId="me", labelIds=["INBOX"], maxResults=1
        ).execute().get("messages", [])
        if msgs:
            mid = msgs[0]["id"]
            msg = service.users().messages().get(
                userId="me", id=mid, format="metadata",
                metadataHeaders=["From", "Subject", "Date"]
            ).execute()
            headers = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
            print(f"\n  최근 메시지 미리보기:")
            print(f"    From   : {headers.get('From','?')[:80]}")
            print(f"    Subject: {headers.get('Subject','?')[:80]}")
            print(f"    Date   : {headers.get('Date','?')}")
        print("\n" + "━" * 60)
        print("  ✅ Gmail 인증 완료. 이제 보도자료 수집 가능합니다.")
        print("━" * 60)
    except HttpError as e:
        print(f"\n❌ Gmail API 호출 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
