@echo off
cd /d %~dp0
echo Company Catcher DART 서버 시작 중...
echo URL: http://localhost:8888
echo 종료: Ctrl+C
python server.py --port 8888
pause
