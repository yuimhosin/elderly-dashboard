# -*- coding: utf-8 -*-
"""
多维表格统计分析：按机构、时间等维度统计
"""
import sys
from pathlib import Path
from collections import Counter
from datetime import datetime

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import FEISHU_DOC_IDS
from feishu_api_client import get_bitable_records


def _ts_to_date(ts) -> str:
    """毫秒时间戳转 YYYY/MM"""
    try:
        t = int(ts)
        if t < 1e10:
            t *= 1000
        dt = datetime.fromtimestamp(t / 1000)
        return dt.strftime("%Y/%m")
    except (ValueError, TypeError):
        return ""


def get_records() -> list[dict]:
    """从配置的 bitable 获取所有记录"""
    records = []
    for item in FEISHU_DOC_IDS:
        if not item or len(item) != 2:
            continue
        source, doc_id = item
        if source == "bitable":
            app_token, table_id = doc_id if isinstance(doc_id, tuple) else ("", "")
            records.extend(get_bitable_records(app_token, table_id))
    return records


def stats_by_org(records: list[dict], org_field: str = "上报机构") -> list[tuple]:
    """按机构统计上报数量，返回 [(机构, 数量), ...] 按数量降序"""
    orgs = [r.get(org_field, "").strip() or "未填写" for r in records]
    cnt = Counter(orgs)
    return cnt.most_common()


def stats_by_month(records: list[dict], time_field: str = "上报时间") -> list[tuple]:
    """按月份统计，返回 [(YYYY/MM, 数量), ...]"""
    months = []
    for r in records:
        v = r.get(time_field, "")
        m = _ts_to_date(v) if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()) else v[:7] if v else ""
        if m:
            months.append(m)
    return Counter(months).most_common()


def stats_by_event_type(records: list[dict], field: str = "事件分类") -> list[tuple]:
    """按事件分类统计"""
    vals = [r.get(field, "").strip() or "未分类" for r in records]
    return Counter(vals).most_common()


def format_stats_report(records: list[dict], question: str = "") -> str:
    """
    根据问题生成统计报告。
    支持：机构上报、上报积极、统计、分析 等关键词
    """
    if not records:
        return "暂无数据，请确认多维表格已配置且可访问。"

    q = (question or "").lower()
    lines = [f"共 {len(records)} 条记录。\n"]

    if "机构" in q or "积极" in q or "上报" in q:
        by_org = stats_by_org(records)
        lines.append("【按上报机构统计】")
        for org, n in by_org[:15]:
            lines.append(f"  {org}: {n} 条")
        if by_org:
            lines.append(f"\n上报最积极: {by_org[0][0]} ({by_org[0][1]} 条)")

    if "月" in q or "时间" in q or "趋势" in q:
        by_month = stats_by_month(records)
        if by_month:
            lines.append("\n【按月份统计】")
            for m, n in sorted(by_month)[-12:]:
                lines.append(f"  {m}: {n} 条")

    if "分类" in q or "类型" in q:
        by_type = stats_by_event_type(records)
        if by_type:
            lines.append("\n【按事件分类统计】")
            for t, n in by_type[:10]:
                lines.append(f"  {t}: {n} 条")

    if len(lines) == 1:
        by_org = stats_by_org(records)
        lines.append("【按上报机构统计】")
        for org, n in by_org[:15]:
            lines.append(f"  {org}: {n} 条")
        if by_org:
            lines.append(f"\n上报最积极: {by_org[0][0]} ({by_org[0][1]} 条)")

    return "\n".join(lines)


if __name__ == "__main__":
    recs = get_records()
    print(format_stats_report(recs, "哪个机构上报最积极"))
