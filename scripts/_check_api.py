import os, requests, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

for line in open('.env', encoding='utf-8'):
    if '=' in line and not line.startswith('#'):
        k, v = line.strip().split('=', 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

keys = {'K1(83So)': os.getenv('GEMINI_API_KEY'),
        'K2(yw9I)': os.getenv('GEMINI_API_KEY_2'),
        'K3(lgk4)': os.getenv('GEMINI_API_KEY_3')}
models = ['gemini-2.5-flash','gemini-3-flash-preview','gemini-2.5-flash-lite',
          'gemini-3.1-flash-lite','gemini-2.0-flash','gemini-2.0-flash-lite',
          'gemini-3-pro-preview','gemini-2.5-pro']

print('=== Gemini API 상태 (모델 × 키) ===')
print(f'{"":36s}', end='')
for kn in keys: print(f'{kn:>10s}', end='')
print()
total_ok, total_bad = 0, 0
for m in models:
    print(f'{m:36s}', end='')
    for kn, key in keys.items():
        if not key:
            print(f'{"-":>10s}', end=''); continue
        url = f'https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={key}'
        try:
            r = requests.post(url, json={'contents':[{'parts':[{'text':'hi'}]}]}, timeout=10)
            if r.status_code == 200:
                total_ok += 1
                mark = 'OK'
            else:
                total_bad += 1
                mark = str(r.status_code)
            print(f'{mark:>10s}', end='')
        except:
            print(f'{"ERR":>10s}', end='')
        time.sleep(0.4)
    print()
print(f'\n살아있는 조합: {total_ok} / {total_ok+total_bad}')

# Gmail
print('\n=== Gmail API (fin@finance-scope.com) ===')
try:
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    creds = Credentials.from_authorized_user_file('secrets/token.json',
        ["https://www.googleapis.com/auth/gmail.readonly"])
    svc = build('gmail','v1', credentials=creds)
    profile = svc.users().getProfile(userId='me').execute()
    print(f'  메일주소        : {profile.get("emailAddress")}')
    print(f'  총 메시지        : {profile.get("messagesTotal"):,}')
    print(f'  스레드          : {profile.get("threadsTotal"):,}')
    print(f'  Gmail API quota : 1,000,000,000 units/일 (실질 무제한)')
    print(f'  메일 발송 한도   : 일반 Gmail 500/일, Workspace 2,000/일')
except Exception as e:
    print(f'  체크 실패: {e}')
