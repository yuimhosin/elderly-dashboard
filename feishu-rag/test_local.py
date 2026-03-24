# -*- coding: utf-8 -*-
"""本地测试 RAG 问答，无需启动服务"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from rag_engine import RAGEngine

if __name__ == "__main__":
    print("加载 RAG 引擎...")
    rag = RAGEngine()
    print("文档已索引，可以提问。输入空行退出。\n")

    while True:
        try:
            q = input("你的问题: ").strip()
            if not q:
                break
            ans = rag.query(q)
            print(f"回答: {ans}\n")
        except KeyboardInterrupt:
            break
    print("测试结束")
