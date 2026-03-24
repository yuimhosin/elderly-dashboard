@echo off
REM 飞书 RAG 机器人启动脚本
cd /d "%~dp0"
if exist .env (
    echo 使用 .env 配置
) else (
    echo 请先配置 .env 文件（参考 env_example.txt）
    pause
    exit /b 1
)
python run_bot.py --port 9000
pause
