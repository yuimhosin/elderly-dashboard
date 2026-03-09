# -*- coding: utf-8 -*-
"""诊断飞书文档同步问题"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import FEISHU_DOC_IDS, FEISHU_APP_ID
from feishu_api_client import get_tenant_access_token, get_doc_raw_content, list_wiki_space_docs

def main():
    print("=== 飞书 RAG 同步诊断 ===\n")
    print(f"App ID: {FEISHU_APP_ID[:20]}..." if FEISHU_APP_ID else "App ID: 未配置")
    token = get_tenant_access_token()
    print(f"Token: {'OK' if token else 'FAIL'}\n")

    if not FEISHU_DOC_IDS:
        print("FEISHU_DOC_IDS 未配置")
        return

    for source, doc_id in FEISHU_DOC_IDS:
        print(f"--- {source}: {doc_id[:40]}... ---")
        if source == "wiki":
            content, rev = get_doc_raw_content(doc_id, source="wiki")
            if content is not None:
                print(f"  [单节点] 成功, len={len(content)}")
            else:
                docs = list_wiki_space_docs(doc_id)
                if docs:
                    print(f"  [知识库空间] 找到 {len(docs)} 个文档")
                    for nt, ot, title in docs[:3]:
                        c, _ = get_doc_raw_content(ot, source="doc")
                        print(f"    - {title[:30]}: {'OK' if c else 'FAIL'}")
                else:
                    print("  [失败] 单节点和知识库空间均无法获取")
                    print("""
  【可能原因】
  1. 知识库权限：需将应用添加为知识库成员
     - 打开知识库 → 设置 → 成员与权限 → 添加成员 → 选择你的应用
  2. 或改用普通文档链接（更简单）：
     - 在知识库中打开某个文档 → 分享 → 复制链接
     - 链接格式如 https://xxx.feishu.cn/docx/xxxxx
     - 将 FEISHU_DOC_IDS 改为该文档链接
""")
        else:
            content, rev = get_doc_raw_content(doc_id, source=source)
            print(f"  {'成功' if content else '失败'}, len={len(content) if content else 0}")
        print()

if __name__ == "__main__":
    main()
