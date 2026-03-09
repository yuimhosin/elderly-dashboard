# -*- coding: utf-8 -*-
"""
飞书 RAG 可视化界面 - Streamlit
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import streamlit as st

st.set_page_config(
    page_title="飞书知识库问答",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 自定义样式
st.markdown("""
<style>
    .stApp { max-width: 900px; margin: 0 auto; }
    .chat-user { 
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
        color: white; 
        padding: 12px 16px; 
        border-radius: 12px; 
        margin: 8px 0;
        margin-left: 20%;
    }
    .chat-bot { 
        background: #f0f2f6; 
        padding: 12px 16px; 
        border-radius: 12px; 
        margin: 8px 0;
        margin-right: 20%;
        border-left: 4px solid #667eea;
    }
    .quick-btn { margin: 4px; }
</style>
""", unsafe_allow_html=True)


def get_answer(question: str, use_agentic: bool) -> str:
    """根据模式获取回答"""
    if use_agentic:
        from agentic_rag import query
        return query(question)
    else:
        from rag_engine import RAGEngine
        rag = RAGEngine()
        return rag.query(question)


def main():
    st.title("📚 飞书知识库问答")
    st.caption("基于多维表格的 RAG 检索与 Agentic 智能问答")

    # 侧边栏
    with st.sidebar:
        st.header("⚙️ 设置")
        use_agentic = st.radio(
            "模式",
            ["Agentic RAG", "经典 RAG"],
            index=0,
            help="Agentic 可自动选择检索/统计/时间等工具；经典仅向量检索",
        )
        use_agentic = use_agentic == "Agentic RAG"

        st.divider()
        st.markdown("**快捷问题**")
        quick_questions = [
            "现在几点了？",
            "哪个机构上报最积极？",
            "查询 2026/01 的所有事件",
            "按月份统计上报数量",
        ]
        for q in quick_questions:
            if st.button(q, key=q, use_container_width=True):
                st.session_state.quick_q = q
                st.rerun()  # 触发处理快捷问题

    # 初始化对话历史
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 快捷问题
    if "quick_q" in st.session_state:
        q = st.session_state.quick_q
        del st.session_state.quick_q
        if q:
            with st.spinner("思考中..."):
                ans = get_answer(q, use_agentic)
            st.session_state.messages.append({"role": "user", "content": q})
            st.session_state.messages.append({"role": "assistant", "content": ans})
            st.rerun()  # 刷新显示新对话

    # 显示对话历史
    for msg in st.session_state.messages:
        role = msg["role"]
        content = msg["content"]
        with st.chat_message(role):
            st.markdown(content)

    # 输入框
    if prompt := st.chat_input("输入你的问题..."):
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("assistant"):
            with st.spinner("思考中..."):
                answer = get_answer(prompt, use_agentic)
            st.markdown(answer)

        st.session_state.messages.append({"role": "assistant", "content": answer})

    # 清空对话
    if st.session_state.messages:
        st.sidebar.divider()
        if st.sidebar.button("🗑️ 清空对话"):
            st.session_state.messages = []
            st.rerun()


if __name__ == "__main__":
    main()
