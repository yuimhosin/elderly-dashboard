# -*- coding: utf-8 -*-
"""
清洗 doc_contents.json：
1. 时间戳转为可读格式 YYYY/MM/DD HH:MM
2. 删除图片/文件等冗余数据（file_token, tmp_url, avatar_url 等）
"""
import json
import re
from datetime import datetime
from pathlib import Path

VECTOR_DB = Path(__file__).resolve().parent / "vector_db"
DOC_CONTENTS = VECTOR_DB / "doc_contents.json"

# 需要删除的 JSON 对象中的特征键（含这些键的对象整块删除）
_REMOVE_KEYS = ("file_token", "tmp_url", "avatar_url")


def _remove_file_avatar_objects(content: str) -> str:
    """
    删除 content 中包含 file_token、tmp_url、avatar_url 的 JSON 对象，
    替换为 [已省略]
    """
    result = []
    i = 0
    while i < len(content):
        if content[i] == "{":
            start = i
            depth = 1
            i += 1
            while i < len(content) and depth > 0:
                if content[i] == "{":
                    depth += 1
                elif content[i] == "}":
                    depth -= 1
                i += 1
            chunk = content[start:i]
            if any(f'"{k}"' in chunk for k in _REMOVE_KEYS):
                result.append("[已省略]")
            else:
                result.append(chunk)
        elif content[i] == "[":
            # 处理数组：若内部有需删除的对象，整段替换
            start = i
            depth = 1
            i += 1
            while i < len(content) and depth > 0:
                if content[i] == "[":
                    depth += 1
                elif content[i] == "]":
                    depth -= 1
                i += 1
            chunk = content[start:i]
            if any(f'"{k}"' in chunk for k in _REMOVE_KEYS):
                result.append("[已省略]")
            else:
                result.append(chunk)
        else:
            result.append(content[i])
            i += 1
    return "".join(result)


def ts_to_readable(ts_str: str) -> str:
    """将毫秒时间戳转为 YYYY/MM/DD HH:MM"""
    try:
        ts = int(ts_str)
        if ts < 1e10:  # 可能是秒级
            ts = ts * 1000
        elif ts > 1e15:  # 超出合理范围
            return ts_str
        dt = datetime.fromtimestamp(ts / 1000)
        return dt.strftime("%Y/%m/%d %H:%M")
    except (ValueError, OSError):
        return ts_str


def clean_content(content: str) -> str:
    """
    1. 清洗时间戳：字段名含「时间」且值为 12-13 位数字时，转为可读格式
    2. 删除图片/文件等冗余数据（file_token, tmp_url, avatar_url）
    3. 删除分隔符和多余换行（\n\n---\n\n、连续 \n）
    """
    # 先删除 file/avatar 对象
    content = _remove_file_avatar_objects(content)

    # 删除 \n\n---\n\n 分隔符
    content = content.replace("\n\n---\n\n", "\n")
    # 将连续多个换行压缩为单个
    content = re.sub(r"\n{2,}", "\n", content)

    # 再清洗时间戳
    def repl(m):
        field, num = m.group(1), m.group(2)
        if "时间" in field and num.isdigit() and 12 <= len(num) <= 13:
            return f"{field}: {ts_to_readable(num)}"
        return m.group(0)

    pattern = r"([^\n:]+): (\d{12,13})(?=\s|$|\n)"
    return re.sub(pattern, repl, content)


def main():
    with open(DOC_CONTENTS, "r", encoding="utf-8") as f:
        data = json.load(f)

    for doc_id, item in data.items():
        content = item.get("content", "")
        if content:
            item["content"] = clean_content(content)

    with open(DOC_CONTENTS, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # 删除旧向量索引，下次 RAG 查询时会自动重建
    faiss_index = VECTOR_DB / "faiss_index"
    if faiss_index.exists():
        import shutil
        shutil.rmtree(faiss_index)
        print("已删除旧向量索引，RAG 将使用清洗后内容重建")

    print("清洗完成（时间戳 + 图片/文件/头像数据），已保存到", DOC_CONTENTS)


if __name__ == "__main__":
    main()
