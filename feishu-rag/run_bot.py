# -*- coding: utf-8 -*-
"""飞书 RAG 机器人启动入口。从 feishu-rag 目录运行：python run_bot.py"""
import sys
from pathlib import Path

# 将当前目录加入 path，以便导入 config、feishu_api_client 等
_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

# 导入并运行
from feishu_bot_server import main

if __name__ == "__main__":
    main()
