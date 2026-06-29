@echo off
REM ============================================================
REM  Company Catcher 서버 종료
REM  8888 포트를 점유한 프로세스만 정확히 종료합니다.
REM ============================================================
chcp 65001 >nul
echo [종료] 8888 포트 서버를 찾는 중...

set "FOUND="
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":8888" ^| findstr "LISTENING"') do (
    set "FOUND=1"
    echo   PID %%P 종료
    taskkill /F /PID %%P >nul 2>&1
)

if defined FOUND (
    echo [완료] 서버를 종료했습니다.
) else (
    echo [알림] 실행 중인 서버가 없습니다.
)
timeout /t 2 >nul
