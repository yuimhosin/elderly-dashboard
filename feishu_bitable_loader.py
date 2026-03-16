# -*- coding: utf-8 -*-
"""飞书多维表格数据加载：从飞书 Bitable 读取养老社区进度表数据。"""
import os
import re
import json
import time
import urllib.request
import urllib.error
from typing import Optional

import pandas as pd

FEISHU_API_BASE = "https://open.feishu.cn/open-apis"
_token_cache = {"token": None, "expires_at": 0}


def _get_tenant_access_token() -> Optional[str]:
    """获取 tenant_access_token，带缓存。"""
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
    if not app_id or not app_secret:
        return None

    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 300:
        return _token_cache["token"]

    url = f"{FEISHU_API_BASE}/auth/v3/tenant_access_token/internal"
    body = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == 0:
                token = data.get("tenant_access_token")
                expire = data.get("expire", 7200)
                _token_cache["token"] = token
                _token_cache["expires_at"] = now + expire
                return token
    except Exception:
        pass
    return None


def _parse_bitable_url(url_or_id: str) -> tuple[str, str, bool]:
    """
    从飞书多维表格 URL 解析 app_token 和 table_id。
    支持：
    - base 格式：https://xxx.feishu.cn/base/AppToken 或 ?table=TableId
    - wiki 格式：https://xxx.feishu.cn/wiki/NodeToken?table=TableId
    返回 (app_token, table_id, is_wiki)，table_id 可能为空；is_wiki 表示需通过 wiki API 解析 app_token。
    """
    s = (url_or_id or "").strip()
    table_m = re.search(r"[?&]table=([A-Za-z0-9]+)", s)
    table_id = table_m.group(1) if table_m else ""

    m_base = re.search(r"base/([A-Za-z0-9]+)", s)
    if m_base:
        return m_base.group(1), table_id, False

    m_wiki = re.search(r"wiki/([A-Za-z0-9]+)", s)
    if m_wiki and table_id:
        return m_wiki.group(1), table_id, True

    return "", "", False


def _get_app_token_from_wiki_node(node_token: str) -> Optional[str]:
    """通过 wiki get_node API 获取 bitable 的 app_token。"""
    token = _get_tenant_access_token()
    if not token:
        return None
    url = f"{FEISHU_API_BASE}/wiki/v2/spaces/get_node?token={node_token}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") != 0:
                return None
            d = data.get("data", {}).get("node", {})
            if d.get("obj_type") == "bitable":
                return d.get("obj_token")
    except Exception:
        pass
    return None


def _get_first_table_id(app_token: str) -> Optional[str]:
    """获取多维表格的第一个数据表 ID。"""
    token = _get_tenant_access_token()
    if not token:
        return None
    url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") != 0:
                return None
            items = data.get("data", {}).get("items", [])
            if items:
                return items[0].get("table_id")
    except Exception:
        pass
    return None


def _flatten_field_value(v) -> str:
    """将飞书字段值转为字符串。"""
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, list):
        parts = []
        for item in v:
            if isinstance(item, dict):
                parts.append(item.get("name") or item.get("text") or str(item))
            else:
                parts.append(str(item))
        return "; ".join(parts) if parts else ""
    if isinstance(v, dict):
        return v.get("text") or v.get("name") or str(v)
    return str(v)


def load_from_bitable(url_or_id: str) -> pd.DataFrame:
    """
    从飞书多维表格加载数据为 DataFrame。
    url_or_id: 飞书多维表格链接，支持：
    - base 格式：https://xxx.feishu.cn/base/AppToken 或含 ?table=TableId
    - wiki 格式：https://xxx.feishu.cn/wiki/NodeToken?table=TableId
    需配置环境变量 FEISHU_APP_ID、FEISHU_APP_SECRET。
    """
    token = _get_tenant_access_token()
    if not token:
        return pd.DataFrame()

    parsed = _parse_bitable_url(url_or_id)
    app_token, table_id, is_wiki = parsed[0], parsed[1], parsed[2]
    if is_wiki:
        app_token = _get_app_token_from_wiki_node(app_token)
        if not app_token:
            return pd.DataFrame()
    elif not app_token:
        return pd.DataFrame()

    if not table_id:
        table_id = _get_first_table_id(app_token)
        if not table_id:
            return pd.DataFrame()

    all_records = []
    page_token = None

    while True:
        params = ["page_size=500"]
        if page_token:
            params.append(f"page_token={page_token}")
        url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records?{'&'.join(params)}"

        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                if data.get("code") != 0:
                    return pd.DataFrame()
                d = data.get("data", {})
                items = d.get("items", [])
                for rec in items:
                    fields = rec.get("fields", {})
                    flat = {}
                    for k, v in fields.items():
                        if k in ("file_token", "tmp_url", "avatar_url"):
                            continue
                        flat[k] = _flatten_field_value(v)
                    all_records.append(flat)
                page_token = d.get("page_token")
                if not d.get("has_more", False) or not page_token:
                    break
        except Exception:
            return pd.DataFrame()

    if not all_records:
        return pd.DataFrame()

    return pd.DataFrame(all_records)
