# -*- coding: utf-8 -*-
"""Agentic RAG 本地测试：Agent 自动选择 RAG 检索或统计分析"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from agentic_rag import query

if __name__ == "__main__":
    print("Agentic RAG 测试（Agent 自动选择工具）\n")
    while True:
        try:
            q = input("你的问题: ").strip()
            if not q:
                break
            ans = query(q)
            print(f"回答: {ans}\n")
        except KeyboardInterrupt:
            break
    print("测试结束")
