@echo off
REM ============================================================
REM  Company Catcher 서버 시작 (백그라운드 유지형)
REM  더블클릭하면 서버가 뜨고, 이 창을 닫아도 서버는 계속 실행됩니다.
REM ============================================================
cd /d "%~dp0"
chcp 65001 >nul

REM 이미 8888 포트가 LISTENING이면 중복 실행 방지
netstat -ano | findstr ":8888" | findstr "LISTENING" >nul 2>&1
if %errorlevel%==0 (
    echo [알림] 서버가 이미 실행 중입니다  ^>^>  http://localhost:8888
    start "" http://localhost:8888
    timeout /t 3 >nul
    exit /b 0
)

echo [시작] Company Catcher 서버를 백그라운드로 가동합니다...

REM pythonw = 콘솔 없는 파이썬. 창 닫아도 서버 유지.
start "" /b pythonw -X utf8 server.py --port 8888

timeout /t 4 >nul
netstat -ano | findstr ":8888" | findstr "LISTENING" >nul 2>&1
if %errorlevel%==0 (
    echo [성공] 서버 가동 완료  ^>^>  http://localhost:8888
    start "" http://localhost:8888
) else (
    echo [실패] 기동 확인 실패. 직접 실행: python -X utf8 server.py
)
timeout /t 3 >nul
