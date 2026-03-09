# -*- coding: utf-8 -*-
"""
Agentic RAG: 智能体驱动的检索增强生成
- Agent 根据问题自动选择工具：RAG 检索 / 统计分析
- 支持多轮决策、查询重写（可选）
"""
import sys
from pathlib import Path
from typing import Literal

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import LLM_API_KEY, LLM_API_BASE, LLM_MODEL, TOP_K, TOP_K_LIST


def _get_rag_engine():
    from rag_engine import RAGEngine
    return RAGEngine()


def _rag_search(query: str) -> str:
    """向量检索：从知识库中搜索与问题相关的文档片段。适用于：具体事件查询、某时间/机构/类型的事件列表。"""
    rag = _get_rag_engine()
    k = TOP_K_LIST if any(kw in query for kw in ("所有", "全部", "有哪些", "多少", "列出")) else TOP_K
    chunks = rag.search(query, top_k=k)
    if not chunks:
        return "未找到相关文档内容。"
    return "\n\n---\n\n".join([c["content"] for c in chunks])


def _stats_analysis(question: str) -> str:
    """统计分析：直接从多维表格拉取全量数据做聚合统计。适用于：机构排名、上报数量、按时间/分类统计。"""
    from stats_analysis import get_records, format_stats_report
    recs = get_records()
    return format_stats_report(recs, question)


def _get_current_time(timezone: str = "Asia/Shanghai") -> str:
    """联网获取当前时间，默认东八区（中国）"""
    import urllib.request
    import json
    try:
        url = f"https://worldtimeapi.org/api/timezone/{timezone}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        dt = data.get("datetime", "")
        # 格式如 2026-03-10T12:34:56.123456+08:00
        if dt:
            # 简化为 2026-03-10 12:34:56
            return dt[:19].replace("T", " ")
        return ""
    except Exception as e:
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" (本地，联网失败: {e})"


def _create_tools():
    """创建 Agent 可调用的工具"""
    from langchain_core.tools import tool

    @tool
    def rag_search(query: str) -> str:
        """向量检索：从知识库搜索相关文档。用于查询具体事件、某时间/机构的事件列表等。"""
        return _rag_search(query)

    @tool
    def stats_analysis(question: str) -> str:
        """统计分析：对多维表格做聚合统计。用于机构排名、上报数量、按月份/分类统计等。"""
        return _stats_analysis(question)

    @tool
    def get_current_time(timezone: str = "Asia/Shanghai") -> str:
        """联网获取当前时间。用于：现在几点了、今天日期、当前时间等。timezone 如 Asia/Shanghai（中国）、America/New_York 等。"""
        return _get_current_time(timezone)

    return [rag_search, stats_analysis, get_current_time]


def _get_llm():
    """DeepSeek（OpenAI 兼容）"""
    from langchain_openai import ChatOpenAI
    return ChatOpenAI(
        model=LLM_MODEL,
        api_key=LLM_API_KEY,
        base_url=LLM_API_BASE,
        temperature=0.2,
    )


def _build_agent():
    """构建 Agentic RAG 图"""
    from langgraph.graph import StateGraph, MessagesState, START, END
    from langgraph.prebuilt import ToolNode
    from langchain_core.messages import SystemMessage

    tools = _create_tools()
    llm = _get_llm().bind_tools(tools)
    tool_node = ToolNode(tools)

    SYSTEM = """你是企业知识库助手。根据用户问题选择工具：
- 现在几点/今天日期/当前时间 → 用 get_current_time
- 统计/分析/机构排名/上报数量/哪个最积极 → 用 stats_analysis
- 查询具体事件、某时间/机构的事件列表、细节 → 用 rag_search
- 若不确定，优先用 rag_search。"""

    def agent(state):
        msgs = [SystemMessage(content=SYSTEM)] + list(state["messages"])
        response = llm.invoke(msgs)
        return {"messages": [response]}

    def should_continue(state) -> Literal["tools", "__end__"]:
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return "__end__"

    workflow = StateGraph(MessagesState)
    workflow.add_node("agent", agent)
    workflow.add_node("tools", tool_node)

    workflow.add_edge(START, "agent")
    workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", "__end__": END})
    workflow.add_edge("tools", "agent")

    return workflow.compile()


def query(question: str) -> str:
    """
    Agentic RAG 入口：Agent 自动选择工具并生成回答。
    """
    if not LLM_API_KEY:
        return "LLM 未配置，无法使用 Agentic RAG。"

    from langchain_core.messages import HumanMessage

    graph = _build_agent()
    result = graph.invoke({"messages": [HumanMessage(content=question)]})

    last_msg = result["messages"][-1]
    if hasattr(last_msg, "content") and last_msg.content:
        return (last_msg.content or "").strip()
    return "未能生成回答，请重试。"


if __name__ == "__main__":
    q = sys.argv[1] if len(sys.argv) > 1 else "哪个机构上报最积极？"
    print("问题:", q)
    print("回答:", query(q))
