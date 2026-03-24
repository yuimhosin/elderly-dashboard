# -*- coding: utf-8 -*-
"""
将 安装位置-楼栋、单元、楼层、空间类型、空间名称 合并为新列「安装位置」
格式：1号楼>1单元>1层>1号楼1层西走道
"""
import re
import pandas as pd

INPUT_CSV = "瓯园副本 - 总表.csv"
OUTPUT_CSV = "瓯园副本 - 总表_带安装位置.csv"


def _floor_to_ceng(floor) -> str:
    """1F -> 1层, 2F -> 2层, B1 -> B1层"""
    if pd.isna(floor) or not str(floor).strip():
        return ""
    s = str(floor).strip().upper()
    if s.startswith("B") and re.match(r"B\d+", s):
        return f"{s}层" if not s.endswith("层") else s
    m = re.search(r"(\d+)F?", s)
    if m:
        return f"{m.group(1)}层"
    return s


def build_location(row: pd.Series) -> str:
    def _v(x):
        v = row.get(x, "")
        if pd.isna(v) or str(v).strip() in ("", "nan"):
            return ""
        return str(v).strip()

    lou = _v("安装位置-楼栋")
    dan = _v("安装位置-单元") or "1单元"  # 空则默认 1单元
    ceng = _floor_to_ceng(row.get("安装位置-楼层"))
    space_name = _v("空间名称")

    parts = [p for p in [lou, dan, ceng, space_name] if p]
    return ">".join(parts)


def main():
    df = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")
    # 去重列名，保留首次出现；若已有安装位置则先删除
    df = df.loc[:, ~df.columns.duplicated(keep="first")]
    # 安装位置-单元 为空时填充 1单元
    if "安装位置-单元" in df.columns:
        mask = df["安装位置-单元"].isna() | (df["安装位置-单元"].astype(str).str.strip() == "")
        df.loc[mask, "安装位置-单元"] = "1单元"
    if "安装位置" in df.columns:
        df = df.drop(columns=["安装位置"])
    df["安装位置"] = df.apply(build_location, axis=1)
    # 插入到「空间名称」之后
    cols = list(df.columns)
    idx = cols.index("空间名称") + 1 if "空间名称" in cols else len(cols)
    cols.insert(idx, "安装位置")
    # 去重且保持顺序
    seen = set()
    cols_uniq = [c for c in cols if c in df.columns and not (c in seen or seen.add(c))]
    df = df[cols_uniq]
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"已生成: {OUTPUT_CSV}")
    print("示例:")
    for i in range(min(5, len(df))):
        print(f"  {df['安装位置'].iloc[i]}", flush=True)


if __name__ == "__main__":
    main()
