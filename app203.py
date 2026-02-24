# -*- coding: utf-8 -*-
"""
养老社区改良改造进度管理 - Streamlit 交互看板
审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部运行保障协调招采/施工 → 总部督促验收
"""
import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import io
import base64
from datetime import datetime
from data_loader import load_single_csv, load_from_directory, load_uploaded, get_稳定需求_mask
from location_config import 园区_TO_城市, 园区_TO_区域, 城市_COORDS

# PDF导出相关导入
try:
    from reportlab.lib.pagesizes import A4, letter
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# 图表配色：饼图用 20+ 种不重复颜色，避免多分类时颜色重复
CHART_COLORS_PIE = [
    "#5470c6", "#91cc75", "#fac858", "#ee6666", "#73c0de",
    "#3ba272", "#fc8452", "#9a60b4", "#ea7ccc", "#5ad8a6",
    "#6dc8ec", "#945fb9", "#ff9845", "#1e9bb5", "#ffbf00",
    "#c23531", "#2f4554", "#61a0a8", "#d48265", "#749f83",
    "#ca8622", "#bda29a", "#6e7074", "#546570", "#c4ccd3",
]

st.set_page_config(
    page_title="养老社区改良改造进度管理",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 默认数据目录与默认单文件路径
# 这里将项目根目录下的 CSV 表作为默认文件，方便打包后的 exe 直接使用
DEFAULT_DATA_DIR = str(Path(__file__).resolve().parent)
DEFAULT_SINGLE_FILE = str(
    Path(__file__).resolve().parent / "改良改造报表-V4.csv"
)

# 专业 9 大类（与 CSV 中「专业」列对应，用于分类统计）
专业大类 = [
    "土建设施", "供配电系统", "暖通/供冷系统", "弱电系统", "供排水系统",
    "电梯系统", "其它系统", "消防系统", "安防系统"
]


@st.cache_data(ttl=300)
def load_data(source_type: str, path_or_dir: str, pattern: str = "*.csv") -> pd.DataFrame:
    """根据数据源类型加载数据。"""
    if source_type == "单文件":
        return load_single_csv(path_or_dir)
    return load_from_directory(path_or_dir, pattern)


def render_审核流程说明():
    """审核流程说明区块。"""
    st.markdown("### 📋 需求审核与实施流程说明")
    steps = [
        ("1. 社区提出", "一线园区提出改造需求。"),
        ("2. 紧急程度分级", "按一级（最高级）、二级、三级划分。"),
        ("3. 专业分类", "按 9 大类专业划分：土建、供配电、暖通/供冷、弱电、供排水、电梯、其它、消防、安防等。"),
        ("4. 财务预算拆分", "按预算系统进行金额拆分与汇总。"),
        ("5. 一线立项时间", "一线填写需求并提出立项时间。"),
        ("6. 项目部施工", "项目部根据已确定的需求立项组织施工。"),
        ("7. 总部运行保障部", "督促一线需求稳定，协调总部相关部门把控需求，输出给不动产进行招采、施工。"),
        ("8. 施工验收", "总部运行保障部督促一线园区进行最终施工验收。"),
    ]
    for title, desc in steps:
        st.markdown(f"- **{title}**：{desc}")
    st.divider()


def render_园区分级分类(df: pd.DataFrame, 园区选择: list):
    """各园区分级、专业分类统计与明细。"""
    st.subheader("各园区分级分类统计")
    # 处理园区选择：如果为空或None，显示所有数据；否则按选择筛选
    if 园区选择 and len(园区选择) > 0:
        # 过滤掉 None 值
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df[df["园区"].isin(valid_parks)]
        else:
            sub = df[df["园区"].notna()]
    else:
        sub = df[df["园区"].notna()]  # 只显示有园区信息的行

    # 检查是否有专业分包列
    has_prof_subcontract = "专业分包" in sub.columns or "专业细分" in sub.columns
    prof_subcontract_col = "专业分包" if "专业分包" in sub.columns else ("专业细分" if "专业细分" in sub.columns else None)
    
    if has_prof_subcontract:
        c1, c2, c3, c4 = st.columns(4)
    else:
        c1, c2, c3 = st.columns(3)
    
    with c1:
        by_level = sub.groupby("项目分级", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        st.markdown("**按紧急程度（分级）**")
        st.dataframe(by_level, use_container_width=True, hide_index=True)
    with c2:
        by_prof = sub.groupby("专业", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        # 过滤掉"其它系统"分类
        by_prof = by_prof[~by_prof["专业"].isin(["其它系统", "其他系统"])]
        st.markdown("**按专业分类**")
        st.dataframe(by_prof, use_container_width=True, hide_index=True)
    with c3:
        by_park = sub.groupby("园区", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        st.markdown("**按园区**")
        st.dataframe(by_park, use_container_width=True, hide_index=True)
    
    if has_prof_subcontract:
        with c4:
            by_prof_subcontract = sub.groupby(prof_subcontract_col, dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index()
            st.markdown("**按专业分包**")
            st.dataframe(by_prof_subcontract, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("**全部项目清单（可筛选）**")
    col1, col2, col3 = st.columns(3)
    with col1:
        level_filter = st.multiselect("按分级筛选", options=sub["项目分级"].dropna().unique().tolist(), default=None)
    with col2:
        prof_filter = st.multiselect("按专业筛选", options=sub["专业"].dropna().unique().tolist(), default=None)
    with col3:
        if has_prof_subcontract:
            prof_subcontract_filter = st.multiselect("按专业分包筛选", options=sub[prof_subcontract_col].dropna().unique().tolist(), default=None)
        else:
            prof_subcontract_filter = None
    
    detail = sub.copy()
    if level_filter:
        detail = detail[detail["项目分级"].isin(level_filter)]
    if prof_filter:
        detail = detail[detail["专业"].isin(prof_filter)]
    if prof_subcontract_filter and has_prof_subcontract:
        detail = detail[detail[prof_subcontract_col].isin(prof_subcontract_filter)]
    st.caption(f"共 {len(detail)} 条项目")
    # 为避免不同表头字段缺失导致 KeyError，这里按实际存在的列进行展示
    base_cols = ["园区", "序号", "项目分级", "项目分类", "专业", "项目名称", "拟定金额"]
    optional_cols = ["拟定承建组织", "需求立项", "验收", "验收(社区需求完成交付)"]
    cols_to_show = [c for c in base_cols + optional_cols if c in detail.columns]
    if not cols_to_show:
        st.dataframe(detail, use_container_width=True, hide_index=True)
    else:
        df_show = detail[cols_to_show].copy()
        # 统一验收列名称，优先使用「验收(社区需求完成交付)」
        if "验收" in df_show.columns and "验收(社区需求完成交付)" not in df_show.columns:
            df_show = df_show.rename(columns={"验收": "验收(社区需求完成交付)"})
        st.dataframe(df_show, use_container_width=True, hide_index=True)


def render_项目统计分析(df: pd.DataFrame, 园区选择: list):
    """项目统计分析：数量费用统计、预算差值、确定/未确定项目分析、按月份统计立项。"""
    st.subheader("项目统计分析")
    # 处理园区选择：如果为空或None，显示所有有园区信息的数据
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df[df["园区"].isin(valid_parks)]
        else:
            sub = df[df["园区"].notna()]
    else:
        sub = df[df["园区"].notna()]  # 只显示有园区信息的行
    
    # 过滤掉汇总行（序号为空或为"合计"等）
    if "序号" in sub.columns:
        sub = sub[sub["序号"].notna()]
        # 过滤掉合计行
        sub = sub[~sub["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        # 确保序号是数字（过滤掉非数字的序号）
        sub = sub[pd.to_numeric(sub["序号"], errors='coerce').notna()]
    else:
        st.warning("数据中未找到'序号'列，无法进行统计分析。")
        return
    
    # 1. 按数量和费用统计项目，计算与预算差值
    st.markdown("### 📊 项目数量与费用统计")
    total_count = len(sub)
    total_amount = sub["拟定金额"].sum() if "拟定金额" in sub.columns else 0
    
    # 尝试从原始数据中提取预算系统合计（如果有汇总行）
    budget_total = 0
    # 方法1：从序号为空的汇总行中查找
    if "序号" in df.columns:
        budget_rows = df[df["序号"].isna() | (df["序号"].astype(str).str.strip() == "预算系统合计")]
        if not budget_rows.empty:
            for _, row in budget_rows.iterrows():
                if "预算系统合计" in str(row.values):
                    for col in ["拟定金额", "金额", "预算"]:
                        if col in row.index:
                            try:
                                val = row[col]
                                if pd.notna(val):
                                    budget_total = float(val)
                                    break
                            except:
                                continue
                    if budget_total > 0:
                        break
        
        # 方法2：从园区列包含"预算系统合计"的行中查找
        if budget_total == 0 and "园区" in df.columns:
            budget_rows = df[df["园区"].astype(str).str.contains("预算系统合计", na=False)]
            if not budget_rows.empty:
                for col in ["拟定金额", "金额", "预算"]:
                    if col in budget_rows.columns:
                        try:
                            val = budget_rows.iloc[0][col]
                            if pd.notna(val):
                                budget_total = float(val)
                                break
                        except:
                            continue
    
    diff = total_amount - budget_total
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("项目总数", f"{total_count:,}")
    with col2:
        st.metric("总金额（万元）", f"{total_amount:,.0f}")
    with col3:
        st.metric("预算系统合计（万元）", f"{budget_total:,.0f}" if budget_total > 0 else "未找到")
    with col4:
        st.metric("差值（万元）", f"{diff:,.0f}", delta=f"{diff:,.0f}" if diff != 0 else None)
    
    # 按园区统计
    st.markdown("#### 按园区统计")
    park_stats = sub.groupby("园区", dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    park_stats["金额合计"] = park_stats["金额合计"].round(2)
    st.dataframe(park_stats, use_container_width=True, hide_index=True)
    
    # 按区域统计（如果存在所属区域列）
    if "所属区域" in sub.columns:
        st.markdown("#### 按所属区域统计")
        region_stats = sub.groupby("所属区域", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
            园区数=("园区", "nunique"),
        ).reset_index()
        region_stats = region_stats[region_stats["所属区域"] != "其他"].sort_values("项目数", ascending=False)
        region_stats["金额合计"] = region_stats["金额合计"].round(2)
        st.dataframe(region_stats, use_container_width=True, hide_index=True)
        
        # 区域下各园区明细
        st.markdown("##### 各区域下园区明细")
        for region in region_stats["所属区域"].unique():
            region_df = sub[sub["所属区域"] == region]
            parks_in_region = region_df.groupby("园区", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index().sort_values("项目数", ascending=False)
            parks_in_region["金额合计"] = parks_in_region["金额合计"].round(2)
            
            with st.expander(f"📌 {region}（{len(parks_in_region)}个园区，{int(parks_in_region['项目数'].sum())}个项目，{parks_in_region['金额合计'].sum():,.0f}万元）"):
                st.dataframe(parks_in_region, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # 按专业分包统计（如果存在该列）
    if "专业分包" in sub.columns or "专业细分" in sub.columns:
        prof_subcontract_col = "专业分包" if "专业分包" in sub.columns else "专业细分"
        st.markdown("### 📦 按专业分包统计")
        by_prof_subcontract = sub.groupby(prof_subcontract_col, dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index().sort_values("金额合计", ascending=False)
        by_prof_subcontract["金额合计"] = by_prof_subcontract["金额合计"].round(2)
        by_prof_subcontract["项目数占比"] = (by_prof_subcontract["项目数"] / by_prof_subcontract["项目数"].sum() * 100).round(2)
        by_prof_subcontract["金额占比"] = (by_prof_subcontract["金额合计"] / by_prof_subcontract["金额合计"].sum() * 100).round(2)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 专业分包项目数统计")
            st.dataframe(by_prof_subcontract[["专业分包" if prof_subcontract_col == "专业分包" else "专业细分", "项目数", "项目数占比"]], use_container_width=True, hide_index=True)
        with col2:
            st.markdown("#### 专业分包金额统计")
            st.dataframe(by_prof_subcontract[["专业分包" if prof_subcontract_col == "专业分包" else "专业细分", "金额合计", "金额占比"]], use_container_width=True, hide_index=True)
        
        # 显示图表
        try:
            import plotly.express as px
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(
                    by_prof_subcontract, 
                    values="项目数", 
                    names=prof_subcontract_col,
                    title="专业分包项目数占比",
                    color_discrete_sequence=CHART_COLORS_PIE[:len(by_prof_subcontract)]
                )
                fig.update_traces(textposition="outside", textinfo="label+percent+value")
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            with col2:
                fig = px.pie(
                    by_prof_subcontract, 
                    values="金额合计", 
                    names=prof_subcontract_col,
                    title="专业分包金额占比",
                    color_discrete_sequence=CHART_COLORS_PIE[:len(by_prof_subcontract)]
                )
                fig.update_traces(textposition="outside", textinfo="label+percent+value")
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            
            # 专业与专业分包的交叉统计
            st.markdown("#### 专业与专业分包交叉统计")
            cross_stats = sub.groupby(["专业", prof_subcontract_col], dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index().sort_values("金额合计", ascending=False)
            # 过滤掉"其它系统"分类
            cross_stats = cross_stats[~cross_stats["专业"].isin(["其它系统", "其他系统"])]
            cross_stats["金额合计"] = cross_stats["金额合计"].round(2)
            st.dataframe(cross_stats, use_container_width=True, hide_index=True)
        except ImportError:
            pass
    
    st.markdown("---")
    
    # 2. 一类、二类、三类项目占比统计
    st.markdown("### 📈 项目分级占比统计")
    if "项目分级" in sub.columns:
        # 映射：一级->一类，二级->二类，三级->三类
        level_mapping = {"一级": "一类", "二级": "二类", "三级": "三类"}
        sub_copy = sub.copy()
        sub_copy["项目类别"] = sub_copy["项目分级"].map(level_mapping).fillna(sub_copy["项目分级"])
        
        level_stats = sub_copy.groupby("项目类别", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        
        total_projects = level_stats["项目数"].sum()
        total_amount_level = level_stats["金额合计"].sum()
        
        if total_projects > 0:
            level_stats["项目数占比"] = (level_stats["项目数"] / total_projects * 100).round(2)
            level_stats["金额占比"] = (level_stats["金额合计"] / total_amount_level * 100).round(2) if total_amount_level > 0 else 0
            level_stats["金额合计"] = level_stats["金额合计"].round(2)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 项目数量占比")
                st.dataframe(level_stats[["项目类别", "项目数", "项目数占比"]], use_container_width=True, hide_index=True)
            with col2:
                st.markdown("#### 项目金额占比")
                st.dataframe(level_stats[["项目类别", "金额合计", "金额占比"]], use_container_width=True, hide_index=True)
            
            # 显示饼图
            try:
                import plotly.express as px
                col1, col2 = st.columns(2)
                with col1:
                    fig = px.pie(
                        level_stats, values="项目数", names="项目类别",
                        title="项目数量占比",
                        color_discrete_sequence=["#FF6B6B", "#4ECDC4", "#45B7D1"]
                    )
                    fig.update_traces(textposition="outside", textinfo="label+percent+value")
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                with col2:
                    fig = px.pie(
                        level_stats, values="金额合计", names="项目类别",
                        title="项目金额占比",
                        color_discrete_sequence=["#FF6B6B", "#4ECDC4", "#45B7D1"]
                    )
                    fig.update_traces(textposition="outside", textinfo="label+percent+value")
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            except ImportError:
                pass
        else:
            st.info("暂无项目分级数据。")
    else:
        st.info("未找到项目分级列，无法进行分级占比统计。")
    
    st.markdown("---")
    
    # 3. 是否已实施判断
    st.markdown("### 🔧 项目实施状态分析")
    impl_col = None
    for col in sub.columns:
        col_str = str(col).strip()
        if "实施" in col_str and "时间" not in col_str.lower():
            impl_col = col
            break
    
    if impl_col:
        # 解析实施日期
        def parse_impl_date(series):
            """解析实施日期，支持Excel日期序列号、datetime、字符串格式"""
            result = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns]')
            
            if pd.api.types.is_datetime64_any_dtype(series):
                result = pd.to_datetime(series, errors='coerce')
                result = result.mask(result.dt.year == 1900, pd.NaT)
                return result
            
            numeric = pd.to_numeric(series, errors='coerce')
            excel_mask = pd.Series(False, index=series.index)
            if numeric.notna().any():
                excel_mask = (numeric >= 1) & (numeric <= 100000) & numeric.notna()
                if excel_mask.any():
                    result.loc[excel_mask] = pd.to_datetime(
                        numeric[excel_mask].astype(int),
                        unit='D',
                        origin='1899-12-30'
                    )
                    result = result.mask(result.dt.year == 1900, pd.NaT)
            
            str_mask = ~excel_mask & result.isna()
            if str_mask.any():
                str_series = series[str_mask].astype(str).str.strip()
                str_series = str_series.replace(['', 'nan', 'None', 'NaT'], pd.NA)
                str_mask2 = ~str_series.str.startswith('1900', na=False)
                str_parse = pd.to_datetime(str_series[str_mask2], format='mixed', errors='coerce')
                result.loc[str_mask] = str_parse
            
            return result
        
        sub_copy = sub.copy()
        sub_copy["_实施日期_parsed"] = parse_impl_date(sub_copy[impl_col])
        
        # 获取当前时间
        current_time = datetime.now()
        sub_copy["已实施"] = sub_copy["_实施日期_parsed"].notna() & (sub_copy["_实施日期_parsed"] <= pd.Timestamp(current_time))
        
        已实施项目 = sub_copy[sub_copy["已实施"]]
        未实施项目 = sub_copy[~sub_copy["已实施"]]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("已实施项目数", len(已实施项目))
            st.metric("已实施金额（万元）", f"{已实施项目['拟定金额'].sum():,.0f}" if len(已实施项目) > 0 else "0")
        with col2:
            st.metric("未实施项目数", len(未实施项目))
            st.metric("未实施金额（万元）", f"{未实施项目['拟定金额'].sum():,.0f}" if len(未实施项目) > 0 else "0")
        with col3:
            total_impl = len(sub_copy)
            if total_impl > 0:
                impl_rate = len(已实施项目) / total_impl * 100
                st.metric("实施率", f"{impl_rate:.1f}%")
            else:
                st.metric("实施率", "0%")
        
        # 按园区统计实施情况
        st.markdown("#### 各园区实施情况统计")
        park_impl_list = []
        for park in sub_copy["园区"].dropna().unique():
            park_df = sub_copy[sub_copy["园区"] == park]
            总项目数 = len(park_df)
            已实施数 = park_df["已实施"].sum()
            总金额 = park_df["拟定金额"].sum()
            已实施金额 = park_df[park_df["已实施"]]["拟定金额"].sum()
            park_impl_list.append({
                "园区": park,
                "总项目数": 总项目数,
                "已实施数": int(已实施数),
                "未实施数": int(总项目数 - 已实施数),
                "总金额": round(总金额, 2),
                "已实施金额": round(已实施金额, 2),
                "实施率": round(已实施数 / 总项目数 * 100, 1) if 总项目数 > 0 else 0,
            })
        park_impl_stats = pd.DataFrame(park_impl_list).sort_values("总金额", ascending=False)
        st.dataframe(park_impl_stats, use_container_width=True, hide_index=True)
    else:
        st.info("未找到实施日期列，无法进行实施状态分析。")
    
    st.markdown("---")
    
    # 4. 各园区的分类统计：一级项目、总部项目、重大改造项目（200万以上）
    st.markdown("### 🏢 各园区分类项目统计")
    
    # 准备数据
    park_analysis = []
    for park in sub["园区"].dropna().unique():
        park_df = sub[sub["园区"] == park]
        
        # 一级项目金额（支持多种格式：一级、1级、一级项目、1等）
        if "项目分级" in park_df.columns:
            # 将项目分级转换为字符串并去除空格，然后匹配包含"一级"、"1级"或数字"1"的值
            # 先尝试字符串匹配
            一级项目_str = park_df[
                park_df["项目分级"].astype(str).str.strip().str.contains("一级|1级", na=False, regex=True)
            ]
            # 再尝试数字匹配（如果是数字1）
            try:
                一级项目_num = park_df[pd.to_numeric(park_df["项目分级"], errors='coerce') == 1]
            except:
                一级项目_num = pd.DataFrame()
            # 合并两种匹配结果
            一级项目 = pd.concat([一级项目_str, 一级项目_num]).drop_duplicates()
        else:
            一级项目 = pd.DataFrame()
        一级项目金额 = 一级项目["拟定金额"].sum() if len(一级项目) > 0 else 0
        
        # 总部项目金额（总部重点关注项目列为"是"的项目）
        if "总部重点关注项目" in park_df.columns:
            总部项目 = park_df[
                park_df["总部重点关注项目"].astype(str).str.strip().str.contains("是", na=False, case=False)
            ]
        else:
            总部项目 = pd.DataFrame()
        总部项目金额 = 总部项目["拟定金额"].sum() if len(总部项目) > 0 else 0
        
        # 重大改造项目（单个200万以上）
        重大改造项目 = park_df[park_df["拟定金额"] >= 200]
        重大改造项目金额 = 重大改造项目["拟定金额"].sum() if len(重大改造项目) > 0 else 0
        重大改造项目数 = len(重大改造项目)
        
        # 总金额
        总金额 = park_df["拟定金额"].sum()
        
        park_analysis.append({
            "园区": park,
            "一级项目金额": round(一级项目金额, 2),
            "一级项目占比": round(一级项目金额 / 总金额 * 100, 2) if 总金额 > 0 else 0,
            "总部项目金额": round(总部项目金额, 2),
            "总部项目占比": round(总部项目金额 / 总金额 * 100, 2) if 总金额 > 0 else 0,
            "重大改造项目数": 重大改造项目数,
            "重大改造项目金额": round(重大改造项目金额, 2),
            "重大改造项目占比": round(重大改造项目金额 / 总金额 * 100, 2) if 总金额 > 0 else 0,
            "总金额": round(总金额, 2),
        })
    
    park_analysis_df = pd.DataFrame(park_analysis)
    park_analysis_df = park_analysis_df.sort_values("总金额", ascending=False)
    
    st.dataframe(park_analysis_df, use_container_width=True, hide_index=True)
    
    # 显示金额汇总信息
    total_level1 = park_analysis_df["一级项目金额"].sum()
    total_hq = park_analysis_df["总部项目金额"].sum()
    total_major = park_analysis_df["重大改造项目金额"].sum()
    total_all = park_analysis_df["总金额"].sum()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("一级项目总金额", f"{total_level1:,.0f} 万元")
    with col2:
        st.metric("总部项目总金额", f"{total_hq:,.0f} 万元")
    with col3:
        st.metric("重大改造项目总金额", f"{total_major:,.0f} 万元")
    with col4:
        st.metric("所有项目总金额", f"{total_all:,.0f} 万元")
    
    st.markdown("---")
    
    # 显示整合图表（合并到同一个坐标轴下，优化版）
    try:
        import plotly.graph_objects as go
        
        # 按总金额排序，确保图表顺序一致
        park_analysis_df_sorted = park_analysis_df.sort_values("总金额", ascending=False)
        
        # 创建单一图表，使用三Y轴（左Y轴：金额，中Y轴：项目数，右Y轴：占比）
        fig = go.Figure()
        
        # 左Y轴：金额（柱状图）
        # 1. 一级项目金额
        fig.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["一级项目金额"],
                name="一级项目金额（万元）",
                marker=dict(
                    color="#5470c6",
                    line=dict(color="#3a5a9c", width=1)
                ),
                text=park_analysis_df_sorted["一级项目金额"].apply(lambda x: f"{int(x)}万" if x > 0 else None),
                textposition="outside",
                textfont=dict(size=12, color="#5470c6"),
                hovertemplate="<b>%{x}</b><br>一级项目金额: %{y:,.0f} 万元<extra></extra>",
                yaxis="y",
                cliponaxis=False
            )
        )
        
        # 2. 总部项目金额
        fig.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["总部项目金额"],
                name="总部项目金额（万元）",
                marker=dict(
                    color="#91cc75",
                    line=dict(color="#6fa85a", width=1)
                ),
                text=park_analysis_df_sorted["总部项目金额"].apply(lambda x: f"{int(x)}万" if x > 0 else None),
                textposition="outside",
                textfont=dict(size=12, color="#91cc75"),
                hovertemplate="<b>%{x}</b><br>总部项目金额: %{y:,.0f} 万元<extra></extra>",
                yaxis="y",
                cliponaxis=False
            )
        )
        
        # 3. 重大改造项目金额
        fig.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["重大改造项目金额"],
                name="重大改造项目金额（万元）",
                marker=dict(
                    color="#fac858",
                    line=dict(color="#d4a84a", width=1)
                ),
                text=park_analysis_df_sorted["重大改造项目金额"].apply(lambda x: f"{int(x)}万" if x > 0 else None),
                textposition="outside",
                textfont=dict(size=12, color="#d4a84a"),
                hovertemplate="<b>%{x}</b><br>重大改造项目金额: %{y:,.0f} 万元<extra></extra>",
                yaxis="y",
                cliponaxis=False
            )
        )
        
        # 中Y轴：项目数量（使用独立的Y轴，避免缩放）
        max_amount = max(
            park_analysis_df_sorted["一级项目金额"].max(),
            park_analysis_df_sorted["总部项目金额"].max(),
            park_analysis_df_sorted["重大改造项目金额"].max()
        )
        max_count = park_analysis_df_sorted["重大改造项目数"].max()
        # 计算缩放因子，使项目数在视觉上与金额协调
        if max_count > 0 and max_amount > 0:
            scale_factor = max_amount / (max_count * 50)  # 调整缩放比例
        else:
            scale_factor = 1
        scaled_count = park_analysis_df_sorted["重大改造项目数"] * scale_factor
        
        # 4. 重大改造项目数量
        fig.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=scaled_count,
                name="重大改造项目数（个）",
                marker=dict(
                    color="#73c0de",
                    line=dict(color="#4a9bc0", width=1.5)
                ),
                text=park_analysis_df_sorted["重大改造项目数"].apply(lambda x: f"{int(x)}个" if x > 0 else None),
                textposition="inside",
                textfont=dict(size=11, color="#ffffff"),
                customdata=list(zip(
                    park_analysis_df_sorted["重大改造项目数"],
                    park_analysis_df_sorted["重大改造项目金额"]
                )),
                hovertemplate="<b>%{x}</b><br>重大改造项目数: %{customdata[0]:.0f} 个<br>重大改造项目金额: %{customdata[1]:,.0f} 万元<extra></extra>",
                yaxis="y",
                opacity=0.85,
                cliponaxis=False
            )
        )
        
        # 右Y轴：占比（折线图）
        # 5. 一级项目占比
        fig.add_trace(
            go.Scatter(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["一级项目占比"],
                name="一级项目占比（%）",
                mode="lines+markers",
                marker=dict(
                    color="#ee6666",
                    size=10,
                    line=dict(width=2, color="white"),
                    symbol="circle"
                ),
                line=dict(color="#ee6666", width=3),
                text=park_analysis_df_sorted["一级项目占比"].apply(lambda x: f"{x:.0f}%" if x > 0 else None),
                textposition="top center",
                textfont=dict(size=11, color="#ee6666"),
                customdata=park_analysis_df_sorted["一级项目金额"],
                hovertemplate="<b>%{x}</b><br>一级项目占比: %{y:.1f}%<br>一级项目金额: %{customdata:,.0f} 万元<extra></extra>",
                yaxis="y2",
                cliponaxis=False
            )
        )
        
        # 6. 总部项目占比
        fig.add_trace(
            go.Scatter(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["总部项目占比"],
                name="总部项目占比（%）",
                mode="lines+markers",
                marker=dict(
                    color="#ff9800",
                    size=10,
                    line=dict(width=2, color="white"),
                    symbol="square"
                ),
                line=dict(color="#ff9800", width=3, dash="dash"),
                text=park_analysis_df_sorted["总部项目占比"].apply(lambda x: f"{x:.0f}%" if x > 0 else None),
                textposition="top center",
                textfont=dict(size=11, color="#ff9800"),
                customdata=park_analysis_df_sorted["总部项目金额"],
                hovertemplate="<b>%{x}</b><br>总部项目占比: %{y:.1f}%<br>总部项目金额: %{customdata:,.0f} 万元<extra></extra>",
                yaxis="y2",
                cliponaxis=False
            )
        )
        
        # 7. 重大改造项目占比
        fig.add_trace(
            go.Scatter(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["重大改造项目占比"],
                name="重大改造项目占比（%）",
                mode="lines+markers",
                marker=dict(
                    color="#9c27b0",
                    size=10,
                    line=dict(width=2, color="white"),
                    symbol="diamond"
                ),
                line=dict(color="#9c27b0", width=3, dash="dot"),
                text=park_analysis_df_sorted["重大改造项目占比"].apply(lambda x: f"{x:.0f}%" if x > 0 else None),
                textposition="top center",
                textfont=dict(size=11, color="#9c27b0"),
                customdata=park_analysis_df_sorted["重大改造项目金额"],
                hovertemplate="<b>%{x}</b><br>重大改造项目占比: %{y:.1f}%<br>重大改造项目金额: %{customdata:,.0f} 万元<extra></extra>",
                yaxis="y2",
                cliponaxis=False
            )
        )
        
        # 更新X轴
        fig.update_xaxes(
            tickangle=-45,
            tickfont=dict(size=11),
            title_text="园区",
            title_font=dict(size=13, color="#333"),
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)",
            gridwidth=1,
            showline=True,
            linecolor="#ccc",
            linewidth=1
        )
        
        # 更新左Y轴（金额）
        fig.update_yaxes(
            title_text="金额（万元）",
            title_font=dict(size=13, color="#333"),
            tickfont=dict(size=11),
            side="left",
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)",
            gridwidth=1,
            showline=True,
            linecolor="#5470c6",
            linewidth=2,
            zeroline=True,
            zerolinecolor="rgba(200,200,200,0.5)",
            zerolinewidth=1
        )
        
        # 更新右Y轴（占比）
        fig.update_yaxes(
            title_text="占比（%）",
            title_font=dict(size=13, color="#333"),
            tickfont=dict(size=11),
            side="right",
            overlaying="y",
            range=[0, 105],
            showgrid=False,
            showline=True,
            linecolor="#ee6666",
            linewidth=2
        )
        
        # 更新整体布局
        fig.update_layout(
            height=700,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.18,
                xanchor="center",
                x=0.5,
                font=dict(size=11),
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="rgba(0,0,0,0.3)",
                borderwidth=1,
                itemwidth=30
            ),
            title=dict(
                text="<b>各园区分类项目统计（金额、项目数与占比）</b>",
                x=0.5,
                xanchor="center",
                y=0.97,
                yanchor="top",
                font=dict(size=18, family="Arial, sans-serif", color="#1f4788")
            ),
            margin=dict(t=100, b=160, l=90, r=90),
            plot_bgcolor="rgba(255,255,255,1)",
            paper_bgcolor="white",
            barmode="group",
            bargap=0.15,
            bargroupgap=0.1,
            hovermode="x unified",
            hoverlabel=dict(
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="#333",
                font_size=12,
                font_family="Arial"
            )
        )
        
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        
        # 添加一个新的金额统计图表：分组柱状图，使用对数刻度以保证小金额园区的可见性
        st.markdown("#### 📊 各园区分类项目金额统计（对数刻度）")
        fig_amount = go.Figure()
        
        # 分组柱状图：一级项目金额、总部项目金额、重大改造项目金额
        fig_amount.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["一级项目金额"],
                name="一级项目金额（万元）",
                marker=dict(color="#5470c6", line=dict(color="#3a5a9c", width=1)),
                text=park_analysis_df_sorted["一级项目金额"].apply(lambda x: f"{int(x)}" if x > 0 else ""),
                textposition="outside",
                textfont=dict(size=9, color="#5470c6"),
                hovertemplate="<b>%{x}</b><br>一级项目金额: %{y:,.0f} 万元<extra></extra>"
            )
        )
        
        fig_amount.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["总部项目金额"],
                name="总部项目金额（万元）",
                marker=dict(color="#91cc75", line=dict(color="#6fa85a", width=1)),
                text=park_analysis_df_sorted["总部项目金额"].apply(lambda x: f"{int(x)}" if x > 0 else ""),
                textposition="outside",
                textfont=dict(size=9, color="#91cc75"),
                hovertemplate="<b>%{x}</b><br>总部项目金额: %{y:,.0f} 万元<extra></extra>"
            )
        )
        
        fig_amount.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["重大改造项目金额"],
                name="重大改造项目金额（万元）",
                marker=dict(color="#fac858", line=dict(color="#d4a84a", width=1)),
                text=park_analysis_df_sorted["重大改造项目金额"].apply(lambda x: f"{int(x)}" if x > 0 else ""),
                textposition="outside",
                textfont=dict(size=9, color="#d4a84a"),
                hovertemplate="<b>%{x}</b><br>重大改造项目金额: %{y:,.0f} 万元<extra></extra>"
            )
        )
        
        fig_amount.update_xaxes(
            tickangle=-45,
            tickfont=dict(size=11),
            title_text="园区",
            title_font=dict(size=13, color="#333"),
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)"
        )
        
        # 计算Y轴范围，使用对数刻度以保证小金额园区的可见性
        import math
        max_amount = park_analysis_df_sorted["总金额"].max()
        min_amount = park_analysis_df_sorted[park_analysis_df_sorted["总金额"] > 0]["总金额"].min()
        
        # 生成对数刻度的不均匀标签
        if max_amount > 0 and min_amount > 0 and not math.isnan(min_amount) and max_amount > min_amount * 2:
            # 计算对数范围
            log_min = math.log10(max(1, min_amount))  # 确保最小值至少为1
            log_max = math.log10(max_amount)
            
            # 生成不均匀的刻度值（对数间隔）
            tick_vals = []
            tick_texts = []
            
            # 生成主要刻度：1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000等
            for exp in range(int(math.floor(log_min)), int(math.ceil(log_max)) + 1):
                for multiplier in [1, 2, 5]:
                    val = multiplier * (10 ** exp)
                    if val >= max(1, min_amount * 0.5) and val <= max_amount * 1.5:
                        tick_vals.append(val)
                        if val >= 1000:
                            tick_texts.append(f"{val/1000:.1f}千")
                        elif val >= 100:
                            tick_texts.append(f"{int(val)}")
                        else:
                            tick_texts.append(f"{val}")
            
            # 去重并排序
            tick_pairs = sorted(set(zip(tick_vals, tick_texts)), key=lambda x: x[0])
            tick_vals = [v for v, _ in tick_pairs]
            tick_texts = [t for _, t in tick_pairs]
        else:
            tick_vals = None
            tick_texts = None
        
        fig_amount.update_yaxes(
            title_text="金额（万元，对数刻度）",
            title_font=dict(size=13, color="#333"),
            tickfont=dict(size=10),
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)",
            type="log",  # 使用对数刻度
            tickvals=tick_vals if tick_vals else None,
            ticktext=tick_texts if tick_texts else None,
            dtick=1  # 对数刻度的步长
        )
        
        fig_amount.update_layout(
            height=600,
            barmode="group",  # 使用分组模式，支持对数刻度
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.15,
                xanchor="center",
                x=0.5,
                font=dict(size=11)
            ),
            title=dict(
                text="<b>各园区分类项目金额统计（对数刻度，保证小金额园区可见性）</b>",
                x=0.5,
                xanchor="center",
                y=0.97,
                yanchor="top",
                font=dict(size=16, color="#1f4788")
            ),
            margin=dict(t=80, b=140, l=80, r=40),
            plot_bgcolor="rgba(255,255,255,1)",
            paper_bgcolor="white",
            hovermode="x unified"
        )
        
        st.plotly_chart(fig_amount, use_container_width=True, config={"displayModeBar": False})
        
    except ImportError:
        # 如果plotly不可用，回退到简单的表格显示
        st.info("图表库不可用，仅显示数据表格。")
    except Exception as e:
        st.warning(f"图表生成出错：{str(e)}")
    
    st.markdown("---")
    
    # 2. 确定项目（有立项日期）和未确定项目（无立项日期）分析
    st.markdown("### ✅ 项目确定状态分析")
    
    # 查找立项日期列（支持多种可能的列名）
    立项_col = None
    for col in sub.columns:
        col_str = str(col).strip()
        # 支持多种列名格式
        if any(keyword in col_str for keyword in ["需求立项", "项目立项", "立项日期", "立项"]):
            # 排除包含"审核"、"决策"等其他时间节点的列
            if "审核" not in col_str and "决策" not in col_str and "成本" not in col_str:
                立项_col = col
                break
    
    if 立项_col:
        # 创建数据副本，避免修改原始数据
        sub = sub.copy()
        
        # 解析日期列：支持Excel日期序列号、datetime对象、字符串等多种格式
        def parse_date_series(series):
            """解析日期序列，支持Excel日期序列号、datetime、字符串格式"""
            result = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns]')
            
            # 1. 如果已经是datetime类型，直接使用
            if pd.api.types.is_datetime64_any_dtype(series):
                result = pd.to_datetime(series, errors='coerce')
                # 过滤1900年的日期（Excel占位符）
                result = result.mask(result.dt.year == 1900, pd.NaT)
                return result
            
            # 2. 尝试解析为数值（Excel日期序列号）
            numeric = pd.to_numeric(series, errors='coerce')
            excel_mask = pd.Series(False, index=series.index)
            if numeric.notna().any():
                # Excel日期序列号范围：1-100000（约1900-2100年）
                excel_mask = (numeric >= 1) & (numeric <= 100000) & numeric.notna()
                if excel_mask.any():
                    # Excel基准日期：1899-12-30
                    result.loc[excel_mask] = pd.to_datetime(
                        numeric[excel_mask].astype(int), 
                        unit='D', 
                        origin='1899-12-30'
                    )
                    # 过滤1900年的日期
                    result = result.mask(result.dt.year == 1900, pd.NaT)
            
            # 3. 解析字符串格式的日期（仅对未成功解析为Excel序列号的部分）
            str_mask = ~excel_mask & result.isna()
            if str_mask.any():
                str_series = series[str_mask].astype(str).str.strip()
                str_series = str_series.replace(['', 'nan', 'None', 'NaT'], pd.NA)
                # 过滤以1900开头的字符串
                str_mask2 = ~str_series.str.startswith('1900', na=False)
                str_parse = pd.to_datetime(str_series[str_mask2], format='mixed', errors='coerce')
                result.loc[str_mask] = str_parse
            
            return result
        
        # 处理合并单元格：按园区向下填充空值
        sub[立项_col] = sub[立项_col].replace('', pd.NA)
        # 按园区和序号排序
        sorted_idx = sub.sort_values(['园区', '序号']).index
        # 按园区分组向下填充
        sub.loc[sorted_idx, 立项_col] = sub.loc[sorted_idx].groupby('园区', sort=False)[立项_col].ffill()
        
        # 解析日期
        sub["_立项日期_parsed"] = parse_date_series(sub[立项_col])
        
        # 判断是否有有效立项日期
        sub["有立项日期"] = sub["_立项日期_parsed"].notna()
        
        确定项目 = sub[sub["有立项日期"]]
        未确定项目 = sub[~sub["有立项日期"]]
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**已确定项目（有立项日期）**")
            st.metric("项目数", len(确定项目))
            st.metric("金额合计（万元）", f"{确定项目['拟定金额'].sum():,.0f}")
            if not 确定项目.empty:
                # 准备显示用的数据框
                display_df = 确定项目[["园区", "序号", "项目名称", "拟定金额"]].copy()
                # 添加格式化后的立项日期
                if "_立项日期_parsed" in 确定项目.columns:
                    display_df["立项日期"] = 确定项目["_立项日期_parsed"].dt.strftime("%Y-%m-%d")
                else:
                    display_df["立项日期"] = 确定项目[立项_col].astype(str)
                st.dataframe(
                    display_df.head(20),
                    use_container_width=True,
                    hide_index=True
                )
        
        with col2:
            st.markdown("**未确定项目（无立项日期）**")
            st.metric("项目数", len(未确定项目))
            st.metric("金额合计（万元）", f"{未确定项目['拟定金额'].sum():,.0f}")
            if not 未确定项目.empty:
                st.dataframe(
                    未确定项目[["园区", "序号", "项目名称", "拟定金额"]].head(20),
                    use_container_width=True,
                    hide_index=True
                )
        
        # 确定率统计
        st.markdown("#### 确定率统计")
        park_determination = sub.groupby("园区", dropna=False).agg(
            总项目数=("序号", "count"),
            已确定数=("有立项日期", "sum"),
        ).reset_index()
        park_determination["未确定数"] = park_determination["总项目数"] - park_determination["已确定数"]
        park_determination["确定率"] = (park_determination["已确定数"] / park_determination["总项目数"] * 100).round(1)
        st.dataframe(park_determination, use_container_width=True, hide_index=True)
    else:
        st.info("未找到立项日期列，无法进行确定/未确定项目分析。")
    
    st.markdown("---")
    
    # 3. 按月份统计立项
    st.markdown("### 📅 按月份统计立项")
    if 立项_col and "_立项日期_parsed" in sub.columns:
        # 从已解析的日期列提取月份
        sub["立项月份"] = sub["_立项日期_parsed"].dt.to_period('M').astype(str)
        有月份的项目 = sub[sub["立项月份"].notna()]
        
        if not 有月份的项目.empty:
            monthly_stats = 有月份的项目.groupby("立项月份", dropna=False).agg(
                立项项目数=("序号", "count"),
                立项金额=("拟定金额", "sum"),
            ).reset_index().sort_values("立项月份")
            monthly_stats["立项金额"] = monthly_stats["立项金额"].round(2)
            
            # 显示表格
            st.dataframe(monthly_stats, use_container_width=True, hide_index=True)
            
            # 显示图表
            try:
                import plotly.express as px
                col1, col2 = st.columns(2)
                with col1:
                    fig = px.bar(
                        monthly_stats, x="立项月份", y="立项项目数",
                        title="每月立项项目数",
                        text_auto=".0f"
                    )
                    fig.update_layout(xaxis_tickangle=-45, showlegend=False, height=350)
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                
                with col2:
                    fig = px.bar(
                        monthly_stats, x="立项月份", y="立项金额",
                        title="每月立项金额（万元）",
                        text_auto=".0f"
                    )
                    fig.update_layout(xaxis_tickangle=-45, showlegend=False, height=350)
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            except ImportError:
                st.bar_chart(monthly_stats.set_index("立项月份"))
        else:
            st.info("暂无有效的立项日期数据。")
    else:
        st.info("未找到立项日期列，无法进行月份统计。")


def render_总部视图(df: pd.DataFrame, 园区选择: list):
    """总部视图：各园区稳定需求数量与金额、施工进展、验收时间预告。"""
    st.subheader("总部视图：稳定需求与施工验收")
    # 处理园区选择：如果为空或None，显示所有有园区信息的数据
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df[df["园区"].isin(valid_parks)]
        else:
            sub = df[df["园区"].notna()]
    else:
        sub = df[df["园区"].notna()]  # 只显示有园区信息的行

    stable_mask = get_稳定需求_mask(sub)
    stable = sub[stable_mask]

    st.markdown("#### 各园区已确定稳定需求数量与金额")
    summary = stable.groupby("园区", dropna=False).agg(
        稳定需求数量=("序号", "count"),
        稳定需求金额=("拟定金额", "sum"),
    ).reset_index()
    col1, col2 = st.columns(2)
    with col1:
        st.metric("稳定需求项目数", int(stable["序号"].count()))
    with col2:
        st.metric("稳定需求金额合计（万元）", f"{stable['拟定金额'].sum():.0f}")
    st.dataframe(summary, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("#### 施工进展与验收时间预告")
    # 验收列
    accept_col = "验收"
    if accept_col not in sub.columns:
        for c in sub.columns:
            if "验收" in str(c):
                accept_col = c
                break
    impl_col = "实施"
    if impl_col not in sub.columns:
        impl_col = [c for c in sub.columns if "实施" in str(c)]
        impl_col = impl_col[0] if impl_col else None

    preview = sub[["园区", "序号", "项目名称", "拟定金额", "拟定承建组织"]].copy()
    preview["实施时间"] = sub[impl_col] if impl_col and impl_col in sub.columns else ""
    preview["验收时间"] = sub[accept_col] if accept_col in sub.columns else ""
    # 过滤无效日期
    def valid_date(s):
        if pd.isna(s): return False
        t = str(s).strip()
        if not t or t.startswith("-") or "1900" in t: return False
        return True
    preview["验收有效"] = preview["验收时间"].map(valid_date)
    st.dataframe(preview, use_container_width=True, hide_index=True)

    st.markdown("**验收时间预告（仅含有效日期）**")
    accept_preview = preview[preview["验收有效"]].copy()
    if accept_preview.empty:
        st.info("暂无有效验收日期，请在一线填报「验收(社区需求完成交付)」节点。")
    else:
        accept_preview = accept_preview.sort_values("验收时间").drop(columns=["验收有效"])
        st.dataframe(accept_preview, use_container_width=True, hide_index=True)


def _add_城市列(df: pd.DataFrame) -> pd.DataFrame:
    """为 df 增加「城市」列（根据园区映射），不修改原表。"""
    out = df.copy()
    out["城市"] = out["园区"].map(园区_TO_城市).fillna("其他")
    return out


def _add_区域列(df: pd.DataFrame) -> pd.DataFrame:
    """为 df 增加「所属区域」列（根据园区映射），不修改原表。"""
    out = df.copy()
    out["所属区域"] = out["园区"].map(园区_TO_区域).fillna("其他")
    return out


def _add_城市和区域列(df: pd.DataFrame) -> pd.DataFrame:
    """为 df 同时增加「城市」和「所属区域」列，不修改原表。"""
    out = df.copy()
    out["城市"] = out["园区"].map(园区_TO_城市).fillna("其他")
    out["所属区域"] = out["园区"].map(园区_TO_区域).fillna("其他")
    return out


def _build_城市_园区明细(df: pd.DataFrame) -> dict:
    """按城市汇总，每个城市下为各园区的：园区名称、项目总数、总预算。供地图 tooltip 使用。"""
    sub = df[df["城市"].notna() & (df["城市"] != "其他")]
    if sub.empty:
        return {}
    by_city_park = sub.groupby(["城市", "园区"], dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    out = {}
    for city in by_city_park["城市"].unique():
        rows = by_city_park[by_city_park["城市"] == city]
        parks = []
        total_n = 0
        total_a = 0
        for _, r in rows.iterrows():
            n = int(r["项目数"])
            a = int(r["金额合计"])
            parks.append({"园区名称": str(r["园区"]), "项目数": n, "预算万元": int(round(a))})
            total_n += n
            total_a += a
        out[str(city)] = {"项目总数": total_n, "总预算万元": int(round(total_a)), "园区列表": parks}
    return out


def _render_中国地图(df: pd.DataFrame, city_tooltip_data: dict):
    """中国地图：悬浮显示城市下各园区详情；点击城市后通过 URL 参数筛选并跳转下方详情。"""
    try:
        from pyecharts.charts import Geo
        from pyecharts import options as opts
        from pyecharts.commons.utils import JsCode
    except ImportError:
        st.warning("请安装 pyecharts：pip install pyecharts")
        st.info("如果已安装，请尝试：pip install pyecharts -U")
        st.info("地图显示还需要安装地图数据包：pip install echarts-china-provinces-pypkg echarts-china-cities-pypkg")
        return
    
    # 检查数据是否为空
    if df.empty:
        st.warning("数据为空，无法显示地图。")
        return
    
    # 检查是否有城市列
    if "城市" not in df.columns:
        st.warning("数据中缺少'城市'列，无法显示地图。")
        return
    
    by_city = df.groupby("城市", dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    
    data = []
    for _, row in by_city.iterrows():
        city = row["城市"]
        if city in 城市_COORDS and city != "其他":
            data.append((city, int(row["项目数"])))
    
    if not data:
        st.info("当前数据中暂无已配置区位的城市，或请先在侧边栏选择园区。")
        st.info(f"数据中的城市列表：{by_city['城市'].unique().tolist()}")
        st.info(f"已配置区位的城市：{list(城市_COORDS.keys())[:10]}...")
        return
    
    # 准备园区地点数据：收集所有园区的位置信息（在创建图表之前）
    park_locations = []
    for park in df["园区"].dropna().unique():
        if park in 园区_TO_城市:
            city = 园区_TO_城市[park]
            if city in 城市_COORDS:
                lon, lat = 城市_COORDS[city]
                # 统计该园区的项目数
                park_count = len(df[df["园区"] == park])
                park_locations.append((park, lon, lat, park_count))
    
    # 悬浮详情：园区名称、园区上报项目总数、园区总预算；城市级汇总（JS 中用 [] 访问中文键）
    tooltip_js = JsCode(
        """
        function(params) {
            var name = params.name;
            var value = params.value;
            var n = (value && value[2]) != null ? value[2] : (value || 0);
            var info = typeof window.MAP_TOOLTIP_DATA !== 'undefined' && window.MAP_TOOLTIP_DATA[name];
            if (info) {
                var s = '<div style="text-align:left; min-width:200px;">';
                s += '<b>' + name + '</b><br/>';
                s += '项目总数：' + (info['项目总数'] || n) + ' 项<br/>';
                s += '总预算：' + (info['总预算万元'] || 0) + ' 万元<br/>';
                s += '<hr style="margin:6px 0;"/>';
                s += '各园区：<br/>';
                var list = info['园区列表'] || [];
                for (var i = 0; i < list.length; i++) {
                    var p = list[i];
                    s += '· ' + (p['园区名称'] || '') + '｜' + (p['项目数'] || 0) + ' 项｜' + (p['预算万元'] || 0) + ' 万<br/>';
                }
                s += '</div>';
                return s;
            }
            return name + '<br/>项目数：' + n + ' 项';
        }
        """
    )
    
    # 使用Geo图表（支持同时显示地图和园区位置散点）
    geo = Geo(init_opts=opts.InitOpts(width="100%", height="500px", theme="light", renderer="canvas"))
    geo.add_schema(maptype="china", is_roam=True)
    # 添加所有城市坐标
    for city, (lon, lat) in 城市_COORDS.items():
        geo.add_coordinate(city, lon, lat)
    # 添加城市项目数散点图（使用effectScatter效果更明显）
    geo.add(
        "项目数",
        data,
        type_="effectScatter",
        symbol_size=14,
        effect_opts=opts.EffectOpts(scale=4, brush_type="stroke"),
        label_opts=opts.LabelOpts(is_show=True, formatter="{b}", font_size=11),
    )
    # 添加园区地点标记（红色散点）
    if park_locations:
        # 为每个园区添加坐标
        for park_name, lon, lat, park_count in park_locations:
            geo.add_coordinate(park_name, lon, lat)
        # 添加园区散点图
        park_data = [(park_name, park_count) for park_name, lon, lat, park_count in park_locations]
        geo.add(
            "园区位置",
            park_data,
            type_="scatter",
            symbol_size=10,
            itemstyle_opts=opts.ItemStyleOpts(color="#ff6b6b"),
            label_opts=opts.LabelOpts(is_show=True, formatter="{b}", font_size=9, position="right"),
        )
    # 设置全局选项
    geo.set_global_opts(
        title_opts=opts.TitleOpts(title="各地市项目分布（点击城市可筛选下方详情）", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="item", formatter=tooltip_js),
        visualmap_opts=opts.VisualMapOpts(
            min_=min(d[1] for d in data),
            max_=max(d[1] for d in data),
            is_piecewise=False,
            pos_left="left",
            range_color=["#e0f3f8", "#0868ac"],
        ),
    )
    
    # 如果数据为空，显示备用信息
    if not data:
        st.warning("暂无地图数据可显示")
        # 显示城市列表作为备用
        if not by_city.empty:
            st.dataframe(by_city[["城市", "项目数", "金额合计"]], use_container_width=True, hide_index=True)
        return
    
    # 尝试使用pyecharts
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False, encoding="utf-8") as f:
            geo.render(f.name)
            html_path = f.name
        
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        
        # 检查HTML是否生成成功
        if not html or len(html) < 100:
            st.error("地图HTML生成失败，请检查pyecharts安装是否正确。")
            st.info("提示：可能需要安装地图数据包：pip install echarts-china-provinces-pypkg echarts-china-cities-pypkg")
            # 显示备用表格
            if not by_city.empty:
                st.dataframe(by_city[["城市", "项目数", "金额合计"]], use_container_width=True, hide_index=True)
            return
        
        # 检查HTML中是否包含echarts相关代码
        if "echarts" not in html.lower() and "echart" not in html.lower():
            st.warning("生成的HTML中未找到echarts相关代码，地图可能无法正常显示。")
            st.info("提示：可能需要安装地图数据包：pip install echarts-china-provinces-pypkg echarts-china-cities-pypkg")
            if not by_city.empty:
                st.dataframe(by_city[["城市", "项目数", "金额合计"]], use_container_width=True, hide_index=True)
            return
        
        # 注入 tooltip 数据与点击跳转：悬浮用 MAP_TOOLTIP_DATA，点击后带 ?selected_city= 刷新并定位下方
        import json
        tooltip_json = json.dumps(city_tooltip_data, ensure_ascii=False)
        inject = (
            "<script>\n"
            "window.MAP_TOOLTIP_DATA = " + tooltip_json + ";\n"
            "function attachMapClick() {\n"
            "  var dom = document.querySelector('[id^=\"_\"]');\n"
            "  if (dom && window.echarts) {\n"
            "    var inst = window.echarts.getInstanceByDom(dom);\n"
            "    if (inst && !inst._mapClickAttached) {\n"
            "      inst._mapClickAttached = true;\n"
            "      inst.on('click', function(params) {\n"
            "        if (params && params.name) {\n"
            "          var u = window.top.location.pathname || '/';\n"
            "          var q = 'selected_city=' + encodeURIComponent(params.name);\n"
            "          window.top.location.href = u + (u.indexOf('?')>=0 ? '&' : '?') + q;\n"
            "        }\n"
            "      });\n"
            "    }\n"
            "  }\n"
            "}\n"
            "if (document.readyState === 'complete') { setTimeout(attachMapClick, 300); }\n"
            "else { document.addEventListener('DOMContentLoaded', function() { setTimeout(attachMapClick, 300); }); }\n"
            "</script>\n"
        )
        # pyecharts 渲染的图表在 div 内，在 body 末尾插入 script
        if "</body>" in html:
            html = html.replace("</body>", inject + "</body>")
        else:
            html = html + inject
        
        # 显示地图
        st.info("使用pyecharts地图显示")
        st.components.v1.html(html, height=450, scrolling=False)
        
    except Exception as e:
        error_msg = str(e)
        st.error(f"pyecharts地图渲染出错：{error_msg}")
        st.info("已在上方显示Streamlit原生地图作为备用方案")
        st.info("如需使用pyecharts地图，请检查：")
        st.info("1) pyecharts是否正确安装：pip install pyecharts")
        st.info("2) 是否安装了地图数据包：pip install echarts-china-provinces-pypkg echarts-china-cities-pypkg")
        st.info("3) 数据是否包含城市信息")
        
        # 显示详细错误信息（仅在开发模式下）
        if st.checkbox("显示详细错误信息（调试用）", value=False):
            import traceback
            st.code(traceback.format_exc())
        
        # 显示数据表格作为备用
        if not by_city.empty:
            st.markdown("### 城市项目统计（表格视图）")
            st.dataframe(by_city[["城市", "项目数", "金额合计"]].sort_values("项目数", ascending=False), 
                        use_container_width=True, hide_index=True)
    finally:
        try:
            if 'html_path' in locals():
                Path(html_path).unlink(missing_ok=True)
        except Exception:
            pass


def _render_图表_简易(sub: pd.DataFrame):
    """无 plotly 时的简易柱状图回退。"""
    c1, c2 = st.columns(2)
    with c1:
        by_prof = sub.groupby("专业", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_prof.empty:
            st.bar_chart(by_prof.set_index("专业")["项目数"])
    with c2:
        by_level = sub.groupby("项目分级", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_level.empty:
            st.bar_chart(by_level.set_index("项目分级")["项目数"])
    by_park = sub.groupby("园区", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False).head(20)
    if not by_park.empty:
        st.bar_chart(by_park.set_index("园区")["项目数"])
    by_prof_m = sub.groupby("专业", dropna=False).agg(金额=("拟定金额", "sum")).reset_index().sort_values("金额", ascending=False)
    if not by_prof_m.empty:
        st.bar_chart(by_prof_m.set_index("专业")["金额"])


def render_地区分析(df: pd.DataFrame, 园区选择: list):
    """地区分析模块：按所属区域进行详细统计分析。"""
    df_with_location = _add_城市和区域列(df)
    # 处理园区选择：如果为空或None，显示所有有园区信息的数据
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df_with_location[df_with_location["园区"].isin(valid_parks)]
        else:
            sub = df_with_location[df_with_location["园区"].notna()]
    else:
        sub = df_with_location[df_with_location["园区"].notna()]  # 只显示有园区信息的行
    
    # 过滤掉汇总行
    if "序号" in sub.columns:
        sub = sub[sub["序号"].notna()]
        sub = sub[~sub["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        sub = sub[pd.to_numeric(sub["序号"], errors='coerce').notna()]
    
    st.subheader("地区分析：按所属区域统计")
    
    if "所属区域" not in sub.columns:
        st.warning("数据中未找到'所属区域'列，无法进行地区分析。")
        return
    
    # 过滤掉"其他"区域
    sub_region = sub[sub["所属区域"] != "其他"].copy()
    
    if sub_region.empty:
        st.info("当前筛选条件下暂无有效的区域数据。")
        return
    
    # 1. 区域总览统计
    st.markdown("### 📊 区域总览")
    region_summary = sub_region.groupby("所属区域", dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
        园区数=("园区", "nunique"),
        城市数=("城市", "nunique"),
    ).reset_index().sort_values("项目数", ascending=False)
    region_summary["金额合计"] = region_summary["金额合计"].round(2)
    region_summary["平均项目金额"] = (region_summary["金额合计"] / region_summary["项目数"]).round(2)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("总区域数", len(region_summary))
    with col2:
        st.metric("总项目数", int(region_summary["项目数"].sum()))
    with col3:
        st.metric("总金额（万元）", f"{region_summary['金额合计'].sum():,.0f}")
    with col4:
        st.metric("总园区数", int(region_summary["园区数"].sum()))
    with col5:
        st.metric("总城市数", int(region_summary["城市数"].sum()))
    
    st.markdown("#### 各区域统计汇总")
    st.dataframe(region_summary, use_container_width=True, hide_index=True)
    
    # 2. 区域对比图表
    st.markdown("---")
    st.markdown("### 📈 区域对比分析")
    
    try:
        import plotly.express as px
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go
        
        st.markdown("**区域对比分析：项目数与金额**")
        # 创建组合图表：使用 subplots 创建包含柱状图和饼图的组合
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("各区域项目数对比", "各区域金额对比（万元）", "各区域金额分布（万元）", "各区域项目数分布"),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "pie"}, {"type": "pie"}]],
            vertical_spacing=0.2,
            horizontal_spacing=0.15
        )
        
        colors_region = ["#FF6B6B", "#4ECDC4", "#45B7D1", "#FFA07A", "#98D8C8"]
        colors = colors_region[:len(region_summary)]
        
        # 第一个子图：项目数柱状图
        for i, (idx, row) in enumerate(region_summary.iterrows()):
            fig.add_trace(
                go.Bar(
                    x=[row["所属区域"]],
                    y=[row["项目数"]],
                    name=row["所属区域"],
                    marker_color=colors[i],
                    text=[int(row["项目数"])],
                    textposition="outside",
                    showlegend=False
                ),
                row=1, col=1
            )
        
        # 第二个子图：金额柱状图
        for i, (idx, row) in enumerate(region_summary.iterrows()):
            fig.add_trace(
                go.Bar(
                    x=[row["所属区域"]],
                    y=[row["金额合计"]],
                    name=row["所属区域"],
                    marker_color=colors[i],
                    text=[f"{row['金额合计']:.0f}"],
                    textposition="outside",
                    showlegend=False
                ),
                row=1, col=2
            )
        
        # 第三个子图：金额分布饼图
        fig.add_trace(
            go.Pie(
                labels=region_summary["所属区域"],
                values=region_summary["金额合计"],
                name="金额分布",
                marker_colors=colors,
                hole=0.4,
                textinfo="label+percent+value",
                texttemplate="%{label}<br>%{percent}<br>%{value:,.0f}万元",
                showlegend=False
            ),
            row=2, col=1
        )
        
        # 第四个子图：项目数分布饼图
        fig.add_trace(
            go.Pie(
                labels=region_summary["所属区域"],
                values=region_summary["项目数"],
                name="项目数分布",
                marker_colors=colors,
                hole=0.4,
                textinfo="label+percent+value",
                texttemplate="%{label}<br>%{percent}<br>%{value}项",
                showlegend=False
            ),
            row=2, col=2
        )
        
        # 更新布局
        fig.update_xaxes(title_text="所属区域", row=1, col=1, tickangle=0)
        fig.update_yaxes(title_text="项目数", row=1, col=1)
        fig.update_xaxes(title_text="所属区域", row=1, col=2, tickangle=0)
        fig.update_yaxes(title_text="金额（万元）", row=1, col=2)
        
        fig.update_layout(
            height=800,
            showlegend=False,
            margin=dict(t=80, b=50, l=50, r=50),
            title_text="区域对比分析：项目数与金额统计",
            title_x=0.5,
            title_font_size=16
        )
        
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        
    except ImportError:
        st.warning("请安装 plotly 以使用美化图表：pip install plotly")
        st.bar_chart(region_summary.set_index("所属区域")[["项目数", "金额合计"]])
    
    # 3. 各区域详细分析
    st.markdown("---")
    st.markdown("### 🔍 各区域详细分析")
    
    # 按区域分组展示
    for region in sorted(region_summary["所属区域"].unique()):
        region_df = sub_region[sub_region["所属区域"] == region]
        
        # 区域基本信息
        region_info = region_summary[region_summary["所属区域"] == region].iloc[0]
        parks_in_region = region_df["园区"].dropna().unique().tolist()
        cities_in_region = region_df["城市"].dropna().unique().tolist()
        
        with st.expander(
            f"📌 {region} - {len(parks_in_region)}个园区，{int(region_info['项目数'])}个项目，{region_info['金额合计']:,.0f}万元"
        ):
            # 区域概览指标
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("项目数", int(region_info["项目数"]))
            with col2:
                st.metric("金额合计（万元）", f"{region_info['金额合计']:,.0f}")
            with col3:
                st.metric("园区数", int(region_info["园区数"]))
            with col4:
                st.metric("城市数", int(region_info["城市数"]))
            
            # 该区域下各园区统计
            st.markdown("#### 各园区统计")
            parks_stats = region_df.groupby("园区", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
                城市=("城市", "first"),
            ).reset_index().sort_values("项目数", ascending=False)
            parks_stats["金额合计"] = parks_stats["金额合计"].round(2)
            st.dataframe(parks_stats, use_container_width=True, hide_index=True)
            
            # 该区域下各城市统计
            st.markdown("#### 各城市统计")
            cities_stats = region_df.groupby("城市", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
                园区数=("园区", "nunique"),
            ).reset_index().sort_values("项目数", ascending=False)
            cities_stats["金额合计"] = cities_stats["金额合计"].round(2)
            st.dataframe(cities_stats, use_container_width=True, hide_index=True)
            
            # 该区域按专业分类统计
            st.markdown("#### 按专业分类统计")
            prof_stats = region_df.groupby("专业", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index().sort_values("项目数", ascending=False)
            # 过滤掉"其它系统"分类
            prof_stats = prof_stats[~prof_stats["专业"].isin(["其它系统", "其他系统"])]
            prof_stats["金额合计"] = prof_stats["金额合计"].round(2)
            st.dataframe(prof_stats, use_container_width=True, hide_index=True)
            
            # 该区域按项目分级统计
            st.markdown("#### 按项目分级统计")
            level_stats = region_df.groupby("项目分级", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index().sort_values("项目数", ascending=False)
            level_stats["金额合计"] = level_stats["金额合计"].round(2)
            st.dataframe(level_stats, use_container_width=True, hide_index=True)
            
            # 该区域项目明细（可选，显示前20条）
            st.markdown("#### 项目明细（前20条）")
            detail_cols = ["园区", "城市", "序号", "项目分级", "专业", "项目名称", "拟定金额"]
            detail_cols = [c for c in detail_cols if c in region_df.columns]
            st.dataframe(
                region_df[detail_cols].head(20),
                use_container_width=True,
                hide_index=True
            )
            if len(region_df) > 20:
                st.caption(f"共 {len(region_df)} 条项目，仅显示前20条。可在「全部项目」Tab 中查看完整列表。")


def generate_pdf_report_html(df: pd.DataFrame, 园区选择: list, output_path: str = None):
    """生成PDF报告，使用HTML转PDF方式，完整保留网页所有内容"""
    if output_path is None:
        output_path = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf').name
    
    # 处理园区选择
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df[df["园区"].isin(valid_parks)]
        else:
            sub = df[df["园区"].notna()]
    else:
        sub = df[df["园区"].notna()]
    
    # 过滤汇总行
    if "序号" in sub.columns:
        sub = sub[sub["序号"].notna()]
        sub = sub[~sub["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        sub = sub[pd.to_numeric(sub["序号"], errors='coerce').notna()]
    
    # 添加城市和区域列
    df_with_location = _add_城市和区域列(df)
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub_location = df_with_location[df_with_location["园区"].isin(valid_parks)]
        else:
            sub_location = df_with_location[df_with_location["园区"].notna()]
    else:
        sub_location = df_with_location[df_with_location["园区"].notna()]
    
    if "序号" in sub_location.columns:
        sub_location = sub_location[sub_location["序号"].notna()]
        sub_location = sub_location[~sub_location["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        sub_location = sub_location[pd.to_numeric(sub_location["序号"], errors='coerce').notna()]
    
    # 生成HTML内容
    html_content = generate_html_report(df, sub, sub_location, 园区选择)
    
    # 尝试使用weasyprint转换为PDF
    try:
        from weasyprint import HTML, CSS
        from weasyprint.text.fonts import FontConfiguration
        
        font_config = FontConfiguration()
        HTML(string=html_content).write_pdf(
            output_path,
            stylesheets=[CSS(string='''
                @page {
                    size: A4;
                    margin: 2cm;
                }
                body {
                    font-family: "Microsoft YaHei", "SimSun", "SimHei", sans-serif;
                    font-size: 12px;
                    line-height: 1.6;
                }
                h1 { font-size: 24px; color: #1f4788; margin-top: 20px; margin-bottom: 15px; }
                h2 { font-size: 20px; color: #2c5aa0; margin-top: 18px; margin-bottom: 12px; }
                h3 { font-size: 16px; color: #4a7bc8; margin-top: 15px; margin-bottom: 10px; }
                h4 { font-size: 14px; margin-top: 12px; margin-bottom: 8px; }
                table { border-collapse: collapse; width: 100%; margin: 10px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #4a7bc8; color: white; font-weight: bold; }
                tr:nth-child(even) { background-color: #f2f2f2; }
                .chart-container { margin: 20px 0; text-align: center; }
                .section { page-break-inside: avoid; margin-bottom: 30px; }
            ''')],
            font_config=font_config
        )
        return output_path
    except (ImportError, Exception) as e:
        # 如果weasyprint不可用或失败，使用subprocess调用独立的playwright脚本
        weasyprint_error = str(e)
        try:
            import subprocess
            import os
            import sys
            
            # 创建临时HTML文件
            html_file = tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8')
            html_file.write(html_content)
            html_file.close()
            html_path = html_file.name
            
            # 转换文件路径为file:// URL格式
            html_abs_path = os.path.abspath(html_path)
            file_url = f"file:///{html_abs_path.replace(os.sep, '/')}"
            
            # 创建独立的playwright脚本文件
            script_content = f'''# -*- coding: utf-8 -*-
import asyncio
import sys
from playwright.async_api import async_playwright

async def main():
    html_url = "{file_url}"
    pdf_path = r"{output_path}"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(html_url, wait_until="networkidle", timeout=30000)
        await page.pdf(
            path=pdf_path,
            format="A4",
            print_background=True,
            margin={{"top": "2cm", "right": "2cm", "bottom": "2cm", "left": "2cm"}}
        )
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
'''
            
            script_file = tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8')
            script_file.write(script_content)
            script_file.close()
            script_path = script_file.name
            
            # 使用subprocess运行脚本（在新进程中，避免asyncio冲突）
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=os.path.dirname(script_path)
            )
            
            # 清理临时文件
            Path(script_path).unlink(missing_ok=True)
            Path(html_path).unlink(missing_ok=True)
            
            if result.returncode != 0:
                error_msg = result.stderr if result.stderr else result.stdout
                raise Exception(f"Playwright脚本执行失败 (返回码: {result.returncode}): {error_msg}")
            
            if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                raise Exception("PDF文件未生成或文件为空")
            
            return output_path
        except ImportError as e2:
            raise ImportError(f"PDF生成失败。weasyprint错误: {weasyprint_error}。playwright未安装，请运行: pip install playwright && playwright install chromium")
        except subprocess.TimeoutExpired:
            raise Exception(f"PDF生成超时。请检查playwright是否正确安装: playwright install chromium")
        except Exception as e2:
            # 删除临时文件（如果存在）
            try:
                if 'html_path' in locals():
                    Path(html_path).unlink(missing_ok=True)
                if 'script_path' in locals():
                    Path(script_path).unlink(missing_ok=True)
            except:
                pass
            raise Exception(f"PDF生成失败。weasyprint错误: {weasyprint_error}。playwright错误: {str(e2)}。请检查playwright是否正确安装: playwright install chromium")


def generate_interactive_html(df: pd.DataFrame, 园区选择: list) -> str:
    """生成完全交互式的HTML文件，包含所有数据和交互功能，效果与运行程序一致"""
    import json
    
    # 准备数据：将DataFrame转换为JSON格式
    # 过滤汇总行
    df_clean = df.copy()
    if "序号" in df_clean.columns:
        df_clean = df_clean[df_clean["序号"].notna()]
        df_clean = df_clean[~df_clean["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        df_clean = df_clean[pd.to_numeric(df_clean["序号"], errors='coerce').notna()]
    
    # 添加城市和区域列
    df_with_location = _add_城市和区域列(df_clean)
    
    # 转换为JSON（处理NaN值）
    def convert_to_json_serializable(obj):
        if pd.isna(obj):
            return None
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.strftime('%Y-%m-%d')
        if isinstance(obj, (int, float)):
            return float(obj) if not pd.isna(obj) else None
        return str(obj)
    
    data_records = []
    for _, row in df_with_location.iterrows():
        record = {}
        for col in df_with_location.columns:
            val = row[col]
            record[col] = convert_to_json_serializable(val)
        data_records.append(record)
    
    # 获取所有园区列表
    parks_list = sorted([p for p in df_with_location["园区"].dropna().unique().tolist() 
                        if p and str(p).strip() and str(p) != "未知园区"])
    
    # 默认选中的园区
    default_parks = 园区选择 if 园区选择 and len(园区选择) > 0 else parks_list
    
    # 序列化JSON数据
    # 将JSON对象序列化为字符串，然后再次转义以便在JavaScript中作为字符串字面量使用
    data_json_raw = json.dumps(data_records, ensure_ascii=False)
    parks_json_raw = json.dumps(parks_list, ensure_ascii=False)
    
    # 将JSON字符串转换为JavaScript字符串字面量（转义引号、反斜杠等特殊字符）
    # 使用json.dumps再次转义，确保在JavaScript中可以安全使用
    data_json = json.dumps(data_json_raw)
    parks_json = json.dumps(parks_json_raw)
    
    # 生成HTML
    html_content = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>养老社区改良改造进度管理看板</title>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: "Microsoft YaHei", "SimSun", "SimHei", Arial, sans-serif;
            font-size: 14px;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background-color: white;
            min-height: 100vh;
        }}
        .header {{
            background: linear-gradient(135deg, #1f4788 0%, #4a7bc8 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            font-size: 28px;
            margin-bottom: 10px;
        }}
        .header .caption {{
            font-size: 14px;
            opacity: 0.9;
        }}
        .sidebar {{
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border: 1px solid #e0e0e0;
        }}
        .sidebar h3 {{
            color: #1f4788;
            margin-bottom: 15px;
            font-size: 18px;
        }}
        .multiselect {{
            width: 100%;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
            background-color: white;
            max-height: 200px;
            overflow-y: auto;
        }}
        .multiselect option {{
            padding: 5px;
        }}
        .multiselect option:checked {{
            background-color: #4a7bc8;
            color: white;
        }}
        .tabs {{
            display: flex;
            border-bottom: 2px solid #e0e0e0;
            margin-bottom: 20px;
            overflow-x: auto;
        }}
        .tab-button {{
            padding: 12px 24px;
            background-color: #f8f9fa;
            border: none;
            border-bottom: 3px solid transparent;
            cursor: pointer;
            font-size: 15px;
            color: #666;
            transition: all 0.3s;
            white-space: nowrap;
        }}
        .tab-button:hover {{
            background-color: #e9ecef;
            color: #1f4788;
        }}
        .tab-button.active {{
            background-color: white;
            color: #1f4788;
            border-bottom-color: #4a7bc8;
            font-weight: bold;
        }}
        .tab-content {{
            display: none;
            padding: 20px 0;
        }}
        .tab-content.active {{
            display: block;
        }}
        .section {{
            margin-bottom: 30px;
        }}
        .section h2 {{
            font-size: 22px;
            color: #1f4788;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #4a7bc8;
        }}
        .section h3 {{
            font-size: 18px;
            color: #2c5aa0;
            margin: 20px 0 10px 0;
        }}
        .section h4 {{
            font-size: 16px;
            color: #4a7bc8;
            margin: 15px 0 8px 0;
        }}
        .metrics {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .metric {{
            background: linear-gradient(135deg, #f0f8ff 0%, #e6f3ff 100%);
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #4a7bc8;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .metric-label {{
            font-size: 13px;
            color: #666;
            margin-bottom: 8px;
        }}
        .metric-value {{
            font-size: 24px;
            font-weight: bold;
            color: #1f4788;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 13px;
            background-color: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        th {{
            background-color: #4a7bc8;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #e0e0e0;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        tr:nth-child(even) {{
            background-color: #fafafa;
        }}
        .chart-container {{
            margin: 20px 0;
            background-color: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .info-box {{
            background-color: #e7f3ff;
            border-left: 4px solid #4a7bc8;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }}
        .warning-box {{
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }}
        .expander {{
            margin: 10px 0;
        }}
        .expander-header {{
            background-color: #f8f9fa;
            padding: 12px;
            cursor: pointer;
            border-radius: 4px;
            border: 1px solid #e0e0e0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .expander-header:hover {{
            background-color: #e9ecef;
        }}
        .expander-content {{
            display: none;
            padding: 15px;
            border: 1px solid #e0e0e0;
            border-top: none;
            border-radius: 0 0 4px 4px;
        }}
        .expander-content.active {{
            display: block;
        }}
        .expander-icon {{
            transition: transform 0.3s;
        }}
        .expander-header.active .expander-icon {{
            transform: rotate(90deg);
        }}
        ul {{
            padding-left: 25px;
            margin: 10px 0;
        }}
        li {{
            margin: 8px 0;
        }}
        .data-table-container {{
            overflow-x: auto;
            margin: 15px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏠 养老社区改良改造进度管理看板</h1>
            <div class="caption">需求审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部协调招采/施工 → 督促验收</div>
        </div>
        
        <div class="sidebar">
            <h3>📊 数据筛选</h3>
            <label for="park-select" style="display: block; margin-bottom: 8px; font-weight: bold;">筛选园区：</label>
            <select id="park-select" class="multiselect" multiple size="8">
                {''.join([f'<option value="{p}" {"selected" if p in default_parks else ""}>{p}</option>' for p in parks_list])}
            </select>
            <div style="margin-top: 10px; font-size: 12px; color: #666;">
                💡 提示：按住 Ctrl (Windows) 或 Cmd (Mac) 键可多选
            </div>
            
            <div style="margin-top: 20px;">
                <div class="expander">
                    <div class="expander-header" onclick="toggleExpander(this)">
                        <span><strong>📋 需求审核与实施流程说明</strong></span>
                        <span class="expander-icon">▶</span>
                    </div>
                    <div class="expander-content">
                        <ul style="margin: 10px 0; padding-left: 20px;">
                            <li><strong>1. 社区提出：</strong>一线园区提出改造需求。</li>
                            <li><strong>2. 紧急程度分级：</strong>按一级（最高级）、二级、三级划分。</li>
                            <li><strong>3. 专业分类：</strong>按 9 大类专业划分：土建、供配电、暖通/供冷、弱电、供排水、电梯、其它、消防、安防等。</li>
                            <li><strong>4. 财务预算拆分：</strong>按预算系统进行金额拆分与汇总。</li>
                            <li><strong>5. 一线立项时间：</strong>一线填写需求并提出立项时间。</li>
                            <li><strong>6. 项目部施工：</strong>项目部根据已确定的需求立项组织施工。</li>
                            <li><strong>7. 总部运行保障部：</strong>督促一线需求稳定，协调总部相关部门把控需求，输出给不动产进行招采、施工。</li>
                            <li><strong>8. 施工验收：</strong>总部运行保障部督促一线园区进行最终施工验收。</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="tabs">
            <button class="tab-button active" onclick="switchTab(0)">项目统计分析</button>
            <button class="tab-button" onclick="switchTab(1)">统计</button>
            <button class="tab-button" onclick="switchTab(2)">地区分析</button>
            <button class="tab-button" onclick="switchTab(3)">各园区分级分类</button>
            <button class="tab-button" onclick="switchTab(4)">总部视图</button>
            <button class="tab-button" onclick="switchTab(5)">全部项目</button>
        </div>
        
        <div id="tab-0" class="tab-content active"></div>
        <div id="tab-1" class="tab-content"></div>
        <div id="tab-2" class="tab-content"></div>
        <div id="tab-3" class="tab-content"></div>
        <div id="tab-4" class="tab-content"></div>
        <div id="tab-5" class="tab-content"></div>
    </div>
    
    <script>
        // 数据存储
        const allData = JSON.parse({data_json});
        const parksList = JSON.parse({parks_json});
        let filteredData = [...allData];
        let currentTab = 0;
        
        // 园区筛选
        document.getElementById('park-select').addEventListener('change', function() {{
            const selectedParks = Array.from(this.selectedOptions).map(opt => opt.value);
            if (selectedParks.length === 0) {{
                filteredData = allData.filter(d => d.园区 && d.园区 !== null && d.园区 !== '');
            }} else {{
                filteredData = allData.filter(d => selectedParks.includes(d.园区));
            }}
            renderAllTabs();
        }});
        
        // 标签页切换
        function switchTab(index) {{
            currentTab = index;
            document.querySelectorAll('.tab-button').forEach((btn, i) => {{
                btn.classList.toggle('active', i === index);
            }});
            document.querySelectorAll('.tab-content').forEach((content, i) => {{
                content.classList.toggle('active', i === index);
            }});
        }}
        
        // 工具函数
        function formatNumber(num) {{
            if (num === null || num === undefined || isNaN(num)) return '0';
            return parseFloat(num).toLocaleString('zh-CN', {{maximumFractionDigits: 2}});
        }}
        
        function formatCurrency(num) {{
            if (num === null || num === undefined || isNaN(num)) return '0';
            return parseFloat(num).toLocaleString('zh-CN', {{maximumFractionDigits: 0}});
        }}
        
        function getValue(row, col) {{
            return row[col] !== null && row[col] !== undefined ? row[col] : '';
        }}
        
        function isValidNumber(val) {{
            return val !== null && val !== undefined && !isNaN(val) && val !== '';
        }}
        
        // 过滤有效项目（有序号且为数字）
        function getValidProjects(data) {{
            return data.filter(d => {{
                const seq = d.序号;
                if (!seq || seq === null || seq === '') return false;
                const seqStr = String(seq).trim();
                if (['合计', '预算系统合计', '差', '差额', '小计'].includes(seqStr)) return false;
                return !isNaN(parseFloat(seq));
            }});
        }}
        
        // 渲染所有标签页
        function renderAllTabs() {{
            renderTab0(); // 项目统计分析
            renderTab1(); // 统计
            renderTab2(); // 地区分析
            renderTab3(); // 各园区分级分类
            renderTab4(); // 总部视图
            renderTab5(); // 全部项目
        }}
        
        // 日期解析工具函数
        function parseDate(dateStr) {{
            if (!dateStr || dateStr === null || dateStr === undefined || dateStr === '') return null;
            const str = String(dateStr).trim();
            if (str === '' || str === 'nan' || str === 'None' || str.startsWith('1900')) return null;
            
            // 尝试解析为日期
            const date = new Date(str);
            if (!isNaN(date.getTime()) && date.getFullYear() >= 2000) {{
                return date;
            }}
            
            // 尝试解析Excel日期序列号
            const num = parseFloat(str);
            if (!isNaN(num) && num >= 1 && num <= 100000) {{
                const excelDate = new Date(1899, 11, 30);
                excelDate.setDate(excelDate.getDate() + num);
                if (excelDate.getFullYear() >= 2000) {{
                    return excelDate;
                }}
            }}
            
            return null;
        }}
        
        // 稳定需求判断：需求已立项（需求立项日期有效）且非无效日期
        function isStableRequirement(d) {{
            // 查找需求立项列
            let 立项Col = null;
            for (let key in d) {{
                if (key.includes('需求立项')) {{
                    立项Col = key;
                    break;
                }}
            }}
            if (!立项Col) return false;
            
            const date = parseDate(d[立项Col]);
            return date !== null && date.getFullYear() >= 2000;
        }}
        
        // 标签页0: 项目统计分析
        function renderTab0() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-0');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 计算统计数据
            const totalCount = validData.length;
            const totalAmount = validData.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0);
            
            // 尝试提取预算系统合计（从原始数据中查找汇总行）
            let budgetTotal = 0;
            const allDataForBudget = getValidProjects(allData);
            for (let d of allDataForBudget) {{
                const seq = String(d.序号 || '').trim();
                if (seq === '预算系统合计' || seq === '合计') {{
                    const amt = parseFloat(d.拟定金额) || parseFloat(d.金额) || parseFloat(d.预算) || 0;
                    if (amt > 0) {{
                        budgetTotal = amt;
                        break;
                    }}
                }}
            }}
            const diff = totalAmount - budgetTotal;
            
            // 按园区统计
            const parkStats = {{}};
            validData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!parkStats[park]) {{
                    parkStats[park] = {{count: 0, amount: 0}};
                }}
                parkStats[park].count++;
                parkStats[park].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按所属区域统计
            const regionStats = {{}};
            validData.forEach(d => {{
                const region = d.所属区域 || '其他';
                if (region !== '其他') {{
                    if (!regionStats[region]) {{
                        regionStats[region] = {{count: 0, amount: 0, parks: new Set()}};
                    }}
                    regionStats[region].count++;
                    regionStats[region].amount += parseFloat(d.拟定金额) || 0;
                    if (d.园区) regionStats[region].parks.add(d.园区);
                }}
            }});
            
            // 按项目分级统计
            const levelStats = {{}};
            validData.forEach(d => {{
                const level = d.项目分级 || '未分类';
                if (!levelStats[level]) {{
                    levelStats[level] = {{count: 0, amount: 0}};
                }}
                levelStats[level].count++;
                levelStats[level].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 映射：一级->一类，二级->二类，三级->三类
            const levelMapping = {{'一级': '一类', '二级': '二类', '三级': '三类'}};
            const levelStatsMapped = {{}};
            Object.keys(levelStats).forEach(level => {{
                const mappedLevel = levelMapping[level] || level;
                if (!levelStatsMapped[mappedLevel]) {{
                    levelStatsMapped[mappedLevel] = {{count: 0, amount: 0}};
                }}
                levelStatsMapped[mappedLevel].count += levelStats[level].count;
                levelStatsMapped[mappedLevel].amount += levelStats[level].amount;
            }});
            
            // 项目实施状态分析
            let implCol = null;
            for (let key in validData[0]) {{
                if (key.includes('实施') && !key.toLowerCase().includes('时间')) {{
                    implCol = key;
                    break;
                }}
            }}
            
            let 已实施项目 = [];
            let 未实施项目 = [];
            let parkImplStats = {{}};
            
            if (implCol) {{
                const now = new Date();
                validData.forEach(d => {{
                    const implDate = parseDate(d[implCol]);
                    const isImplemented = implDate !== null && implDate <= now;
                    
                    if (isImplemented) {{
                        已实施项目.push(d);
                    }} else {{
                        未实施项目.push(d);
                    }}
                    
                    const park = d.园区 || '未知';
                    if (!parkImplStats[park]) {{
                        parkImplStats[park] = {{total: 0, implemented: 0, amount: 0, implAmount: 0}};
                    }}
                    parkImplStats[park].total++;
                    parkImplStats[park].amount += parseFloat(d.拟定金额) || 0;
                    if (isImplemented) {{
                        parkImplStats[park].implemented++;
                        parkImplStats[park].implAmount += parseFloat(d.拟定金额) || 0;
                    }}
                }});
            }}
            
            // 项目确定状态分析（有立项日期）
            let 立项Col = null;
            for (let key in validData[0]) {{
                if ((key.includes('需求立项') || key.includes('项目立项') || key.includes('立项日期') || key.includes('立项')) &&
                    !key.includes('审核') && !key.includes('决策') && !key.includes('成本')) {{
                    立项Col = key;
                    break;
                }}
            }}
            
            let 确定项目 = [];
            let 未确定项目 = [];
            let parkDeterminationStats = {{}};
            let monthlyStats = {{}};
            
            if (立项Col) {{
                // 按园区分组，向下填充空值（模拟合并单元格）
                const parkGroups = {{}};
                validData.forEach(d => {{
                    const park = d.园区 || '未知';
                    if (!parkGroups[park]) parkGroups[park] = [];
                    parkGroups[park].push(d);
                }});
                
                Object.keys(parkGroups).forEach(park => {{
                    let lastDate = null;
                    parkGroups[park].forEach(d => {{
                        const dateVal = d[立项Col];
                        if (dateVal && dateVal !== null && dateVal !== '') {{
                            lastDate = dateVal;
                        }} else if (lastDate) {{
                            d[立项Col + '_filled'] = lastDate;
                        }} else {{
                            d[立项Col + '_filled'] = dateVal;
                        }}
                    }});
                }});
                
                validData.forEach(d => {{
                    const dateVal = d[立项Col + '_filled'] || d[立项Col];
                    const hasDate = parseDate(dateVal) !== null;
                    
                    if (hasDate) {{
                        确定项目.push(d);
                        
                        // 按月统计
                        const date = parseDate(dateVal);
                        if (date) {{
                            const month = date.getFullYear() + '-' + String(date.getMonth() + 1).padStart(2, '0');
                            if (!monthlyStats[month]) {{
                                monthlyStats[month] = {{count: 0, amount: 0}};
                            }}
                            monthlyStats[month].count++;
                            monthlyStats[month].amount += parseFloat(d.拟定金额) || 0;
                        }}
                    }} else {{
                        未确定项目.push(d);
                    }}
                    
                    const park = d.园区 || '未知';
                    if (!parkDeterminationStats[park]) {{
                        parkDeterminationStats[park] = {{total: 0, determined: 0}};
                    }}
                    parkDeterminationStats[park].total++;
                    if (hasDate) parkDeterminationStats[park].determined++;
                }});
            }}
            
            // 各园区分类项目统计
            const parkAnalysis = {{}};
            validData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!parkAnalysis[park]) {{
                    parkAnalysis[park] = {{
                        total: 0,
                        level1: 0,
                        hq: 0,
                        major: 0,
                        majorCount: 0
                    }};
                }}
                const amount = parseFloat(d.拟定金额) || 0;
                parkAnalysis[park].total += amount;
                
                // 一级项目识别：支持多种格式（一级、1级、一级项目、1等）
                const levelStr = String(d.项目分级 || '').trim();
                let isLevel1 = false;
                // 字符串匹配：包含"一级"或"1级"
                if (levelStr && (levelStr.includes('一级') || levelStr.includes('1级'))) {{
                    isLevel1 = true;
                }}
                // 数字匹配：如果是数字1
                if (!isLevel1) {{
                    const levelNum = parseFloat(levelStr);
                    if (!isNaN(levelNum) && levelNum === 1) {{
                        isLevel1 = true;
                    }}
                }}
                if (isLevel1) {{
                    parkAnalysis[park].level1 += amount;
                }}
                
                const hqFocus = String(d.总部重点关注项目 || '').trim();
                if (hqFocus === '是' || hqFocus.toLowerCase() === 'yes') {{
                    parkAnalysis[park].hq += amount;
                }}
                
                if (amount >= 200) {{
                    parkAnalysis[park].major += amount;
                    parkAnalysis[park].majorCount++;
                }}
            }});
            
            let html = `
                <div class="section">
                    <h2>📊 项目数量与费用统计</h2>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">项目总数</div>
                            <div class="metric-value">${{formatNumber(totalCount)}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(totalAmount)}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">预算系统合计（万元）</div>
                            <div class="metric-value">${{budgetTotal > 0 ? formatCurrency(budgetTotal) : '未找到'}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">差值（万元）</div>
                            <div class="metric-value" style="color: ${{diff !== 0 ? (diff > 0 ? '#d32f2f' : '#388e3c') : '#666'}};">${{formatCurrency(diff)}}</div>
                        </div>
                    </div>
                    
                    <h3>按园区统计</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkStats).sort((a, b) => parkStats[b].amount - parkStats[a].amount).map(park => `
                                    <tr>
                                        <td>${{park}}</td>
                                        <td>${{parkStats[park].count}}</td>
                                        <td>${{formatCurrency(parkStats[park].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    ${{Object.keys(regionStats).length > 0 ? `
                    <h3>按所属区域统计</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>所属区域</th><th>项目数</th><th>金额合计（万元）</th><th>园区数</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count).map(region => `
                                    <tr>
                                        <td>${{region}}</td>
                                        <td>${{regionStats[region].count}}</td>
                                        <td>${{formatCurrency(regionStats[region].amount)}}</td>
                                        <td>${{regionStats[region].parks.size}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h4>各区域下园区明细</h4>
                    ${{Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count).map(region => {{
                        const regionData = validData.filter(d => d.所属区域 === region);
                        const parkStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const park = d.园区 || '未知';
                            if (!parkStatsInRegion[park]) {{
                                parkStatsInRegion[park] = {{count: 0, amount: 0}};
                            }}
                            parkStatsInRegion[park].count++;
                            parkStatsInRegion[park].amount += parseFloat(d.拟定金额) || 0;
                        }});
                        return `
                            <div class="expander">
                                <div class="expander-header" onclick="toggleExpander(this)">
                                    <span><strong>${{region}}</strong>（${{Object.keys(parkStatsInRegion).length}}个园区，${{regionStats[region].count}}个项目，${{formatCurrency(regionStats[region].amount)}}万元）</span>
                                    <span class="expander-icon">▶</span>
                                </div>
                                <div class="expander-content">
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(parkStatsInRegion).sort((a, b) => parkStatsInRegion[b].amount - parkStatsInRegion[a].amount).map(park => `
                                                    <tr>
                                                        <td>${{park}}</td>
                                                        <td>${{parkStatsInRegion[park].count}}</td>
                                                        <td>${{formatCurrency(parkStatsInRegion[park].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        `;
                    }}).join('')}}
                    ` : ''}}
                    
                    <h3>📈 项目分级占比统计</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>项目类别</th><th>项目数</th><th>项目数占比(%)</th><th>金额合计（万元）</th><th>金额占比(%)</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(levelStatsMapped).map(level => {{
                                    const count = levelStatsMapped[level].count;
                                    const amount = levelStatsMapped[level].amount;
                                    const countPercent = totalCount > 0 ? (count / totalCount * 100).toFixed(2) : 0;
                                    const amountPercent = totalAmount > 0 ? (amount / totalAmount * 100).toFixed(2) : 0;
                                    return `
                                        <tr>
                                            <td>${{level}}</td>
                                            <td>${{count}}</td>
                                            <td>${{countPercent}}%</td>
                                            <td>${{formatCurrency(amount)}}</td>
                                            <td>${{amountPercent}}%</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="chart-container">
                        <div id="chart-level-count"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-level-amount"></div>
                    </div>
                    
                    ${{(validData[0] && (validData[0].专业分包 || validData[0].专业细分)) ? `
                    <h3>📦 按专业分包统计</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>专业分包</th><th>项目数</th><th>项目数占比(%)</th><th>金额合计（万元）</th><th>金额占比(%)</th></tr>
                            </thead>
                            <tbody>
                                ${{(() => {{
                                    const profSubcontractCol = validData[0].专业分包 ? '专业分包' : '专业细分';
                                    const profSubcontractStats = {{}};
                                    validData.forEach(d => {{
                                        const val = d[profSubcontractCol] || '未分类';
                                        if (!profSubcontractStats[val]) {{
                                            profSubcontractStats[val] = {{count: 0, amount: 0}};
                                        }}
                                        profSubcontractStats[val].count++;
                                        profSubcontractStats[val].amount += parseFloat(d.拟定金额) || 0;
                                    }});
                                    const totalCount = validData.length;
                                    const totalAmount = validData.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0);
                                    return Object.keys(profSubcontractStats).sort((a, b) => profSubcontractStats[b].amount - profSubcontractStats[a].amount).map(key => {{
                                        const stats = profSubcontractStats[key];
                                        const countPercent = totalCount > 0 ? (stats.count / totalCount * 100).toFixed(2) : 0;
                                        const amountPercent = totalAmount > 0 ? (stats.amount / totalAmount * 100).toFixed(2) : 0;
                                        return `
                                            <tr>
                                                <td>${{key || '未分类'}}</td>
                                                <td>${{stats.count}}</td>
                                                <td>${{countPercent}}%</td>
                                                <td>${{formatCurrency(stats.amount)}}</td>
                                                <td>${{amountPercent}}%</td>
                                            </tr>
                                        `;
                                    }}).join('');
                                }})()}}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="chart-container">
                        <div id="chart-prof-subcontract-count"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-prof-subcontract-amount"></div>
                    </div>
                    
                    <h4>专业与专业分包交叉统计</h4>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>专业</th><th>专业分包</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{(() => {{
                                    const profSubcontractCol = validData[0].专业分包 ? '专业分包' : '专业细分';
                                    const crossStats = {{}};
                                    validData.forEach(d => {{
                                        const prof = d.专业 || '未分类';
                                        const subcontract = d[profSubcontractCol] || '未分类';
                                        // 过滤掉"其它系统"分类
                                        if (prof === '其它系统' || prof === '其他系统') return;
                                        const key = prof + '|' + subcontract;
                                        if (!crossStats[key]) {{
                                            crossStats[key] = {{prof: prof, subcontract: subcontract, count: 0, amount: 0}};
                                        }}
                                        crossStats[key].count++;
                                        crossStats[key].amount += parseFloat(d.拟定金额) || 0;
                                    }});
                                    return Object.keys(crossStats).sort((a, b) => crossStats[b].amount - crossStats[a].amount).map(key => {{
                                        const stats = crossStats[key];
                                        return `
                                            <tr>
                                                <td>${{stats.prof || '未分类'}}</td>
                                                <td>${{stats.subcontract || '未分类'}}</td>
                                                <td>${{stats.count}}</td>
                                                <td>${{formatCurrency(stats.amount)}}</td>
                                            </tr>
                                        `;
                                    }}).join('');
                                }})()}}
                            </tbody>
                        </table>
                    </div>
                    ` : ''}}
                    
                    ${{implCol ? `
                    <h3>🔧 项目实施状态分析</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">已实施项目数</div>
                            <div class="metric-value">${{已实施项目.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">已实施金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(已实施项目.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">未实施项目数</div>
                            <div class="metric-value">${{未实施项目.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">未实施金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(未实施项目.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">实施率</div>
                            <div class="metric-value">${{validData.length > 0 ? (已实施项目.length / validData.length * 100).toFixed(1) : 0}}%</div>
                        </div>
                    </div>
                    
                    <h4>各园区实施情况统计</h4>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>总项目数</th><th>已实施数</th><th>未实施数</th><th>总金额（万元）</th><th>已实施金额（万元）</th><th>实施率(%)</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkImplStats).sort((a, b) => parkImplStats[b].amount - parkImplStats[a].amount).map(park => {{
                                    const stats = parkImplStats[park];
                                    const rate = stats.total > 0 ? (stats.implemented / stats.total * 100).toFixed(1) : 0;
                                    return `
                                        <tr>
                                            <td>${{park}}</td>
                                            <td>${{stats.total}}</td>
                                            <td>${{stats.implemented}}</td>
                                            <td>${{stats.total - stats.implemented}}</td>
                                            <td>${{formatCurrency(stats.amount)}}</td>
                                            <td>${{formatCurrency(stats.implAmount)}}</td>
                                            <td>${{rate}}%</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    ` : '<div class="info-box">未找到实施日期列，无法进行实施状态分析。</div>'
                    }}
                    
                    <h3>🏢 各园区分类项目统计</h3>
                    
                    ${{(() => {{
                        const totalLevel1 = Object.values(parkAnalysis).reduce((sum, s) => sum + s.level1, 0);
                        const totalHq = Object.values(parkAnalysis).reduce((sum, s) => sum + s.hq, 0);
                        const totalMajor = Object.values(parkAnalysis).reduce((sum, s) => sum + s.major, 0);
                        const totalAll = Object.values(parkAnalysis).reduce((sum, s) => sum + s.total, 0);
                        return `
                            <div class="metrics">
                                <div class="metric">
                                    <div class="metric-label">一级项目总金额</div>
                                    <div class="metric-value">${{formatCurrency(totalLevel1)}} 万元</div>
                                </div>
                                <div class="metric">
                                    <div class="metric-label">总部项目总金额</div>
                                    <div class="metric-value">${{formatCurrency(totalHq)}} 万元</div>
                                </div>
                                <div class="metric">
                                    <div class="metric-label">重大改造项目总金额</div>
                                    <div class="metric-value">${{formatCurrency(totalMajor)}} 万元</div>
                                </div>
                                <div class="metric">
                                    <div class="metric-label">所有项目总金额</div>
                                    <div class="metric-value">${{formatCurrency(totalAll)}} 万元</div>
                                </div>
                            </div>
                        `;
                    }})()}}
                    
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>一级项目金额（万元）</th><th>一级项目占比(%)</th><th>总部项目金额（万元）</th><th>总部项目占比(%)</th><th>重大改造项目数</th><th>重大改造项目金额（万元）</th><th>重大改造项目占比(%)</th><th>总金额（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkAnalysis).sort((a, b) => parkAnalysis[b].total - parkAnalysis[a].total).map(park => {{
                                    const stats = parkAnalysis[park];
                                    const level1Percent = stats.total > 0 ? (stats.level1 / stats.total * 100).toFixed(2) : 0;
                                    const hqPercent = stats.total > 0 ? (stats.hq / stats.total * 100).toFixed(2) : 0;
                                    const majorPercent = stats.total > 0 ? (stats.major / stats.total * 100).toFixed(2) : 0;
                                    return `
                                        <tr>
                                            <td>${{park}}</td>
                                            <td>${{formatCurrency(stats.level1)}}</td>
                                            <td>${{level1Percent}}%</td>
                                            <td>${{formatCurrency(stats.hq)}}</td>
                                            <td>${{hqPercent}}%</td>
                                            <td>${{stats.majorCount}}</td>
                                            <td>${{formatCurrency(stats.major)}}</td>
                                            <td>${{majorPercent}}%</td>
                                            <td>${{formatCurrency(stats.total)}}</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h4>各园区分类项目统计（金额、项目数与占比）</h4>
                    <div class="chart-container">
                        <div id="chart-park-combined"></div>
                    </div>
                    
                    <h4>各园区分类项目金额统计（对数刻度）</h4>
                    <div class="chart-container">
                        <div id="chart-park-log-scale"></div>
                    </div>
                    
                    <h4>各园区分类项目统计（单独图表）</h4>
                    <div class="chart-container">
                        <div id="chart-park-level1"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-park-hq"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-park-major-amount"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-park-major-count"></div>
                    </div>
                    
                    ${{立项Col ? `
                    <h3>✅ 项目确定状态分析</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">已确定项目数（有立项日期）</div>
                            <div class="metric-value">${{确定项目.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">已确定金额合计（万元）</div>
                            <div class="metric-value">${{formatCurrency(确定项目.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">未确定项目数（无立项日期）</div>
                            <div class="metric-value">${{未确定项目.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">未确定金额合计（万元）</div>
                            <div class="metric-value">${{formatCurrency(未确定项目.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                    </div>
                    
                    <h4>确定率统计</h4>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>总项目数</th><th>已确定数</th><th>未确定数</th><th>确定率(%)</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkDeterminationStats).map(park => {{
                                    const stats = parkDeterminationStats[park];
                                    const rate = stats.total > 0 ? (stats.determined / stats.total * 100).toFixed(1) : 0;
                                    return `
                                        <tr>
                                            <td>${{park}}</td>
                                            <td>${{stats.total}}</td>
                                            <td>${{stats.determined}}</td>
                                            <td>${{stats.total - stats.determined}}</td>
                                            <td>${{rate}}%</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    ${{Object.keys(monthlyStats).length > 0 ? `
                    <h3>📅 按月份统计立项</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>立项月份</th><th>立项项目数</th><th>立项金额（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(monthlyStats).sort().map(month => `
                                    <tr>
                                        <td>${{month}}</td>
                                        <td>${{monthlyStats[month].count}}</td>
                                        <td>${{formatCurrency(monthlyStats[month].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="chart-container">
                        <div id="chart-monthly-count"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-monthly-amount"></div>
                    </div>
                    ` : ''
                    }}
                    ` : '<div class="info-box">未找到立项日期列，无法进行确定/未确定项目分析。</div>'
                    }}
                </div>
            `;
            
            container.innerHTML = html;
            
            // 渲染图表
            setTimeout(() => {{
                const levelLabels = Object.keys(levelStatsMapped);
                const levelCounts = levelLabels.map(l => levelStatsMapped[l].count);
                const levelAmounts = levelLabels.map(l => levelStatsMapped[l].amount);
                
                Plotly.newPlot('chart-level-count', [{{
                    values: levelCounts,
                    labels: levelLabels,
                    type: 'pie',
                    textinfo: 'label+percent+value',
                    textposition: 'outside',
                    marker: {{colors: ['#FF6B6B', '#4ECDC4', '#45B7D1']}}
                }}], {{
                    title: '项目数量占比',
                    showlegend: true
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-level-amount', [{{
                    values: levelAmounts,
                    labels: levelLabels,
                    type: 'pie',
                    textinfo: 'label+percent+value',
                    textposition: 'outside',
                    marker: {{colors: ['#FF6B6B', '#4ECDC4', '#45B7D1']}}
                }}], {{
                    title: '项目金额占比',
                    showlegend: true
                }}, {{displayModeBar: false}});
                
                // 各园区分类项目图表
                const parkLabels = Object.keys(parkAnalysis).sort((a, b) => parkAnalysis[b].total - parkAnalysis[a].total);
                const level1Amounts = parkLabels.map(p => parkAnalysis[p].level1);
                const hqAmounts = parkLabels.map(p => parkAnalysis[p].hq);
                const majorAmounts = parkLabels.map(p => parkAnalysis[p].major);
                const majorCounts = parkLabels.map(p => parkAnalysis[p].majorCount);
                const level1Percents = parkLabels.map(p => parkAnalysis[p].total > 0 ? (parkAnalysis[p].level1 / parkAnalysis[p].total * 100).toFixed(2) : 0);
                const hqPercents = parkLabels.map(p => parkAnalysis[p].total > 0 ? (parkAnalysis[p].hq / parkAnalysis[p].total * 100).toFixed(2) : 0);
                const majorPercents = parkLabels.map(p => parkAnalysis[p].total > 0 ? (parkAnalysis[p].major / parkAnalysis[p].total * 100).toFixed(2) : 0);
                
                // 复杂整合图表（多Y轴）
                const maxAmount = Math.max(...level1Amounts, ...hqAmounts, ...majorAmounts);
                const maxCount = Math.max(...majorCounts);
                const scaleFactor = maxCount > 0 && maxAmount > 0 ? maxAmount / (maxCount * 50) : 1;
                const scaledCounts = majorCounts.map(c => c * scaleFactor);
                
                Plotly.newPlot('chart-park-combined', [
                    // 一级项目金额
                    {{
                        x: parkLabels,
                        y: level1Amounts,
                        type: 'bar',
                        name: '一级项目金额（万元）',
                        marker: {{color: '#5470c6', line: {{color: '#3a5a9c', width: 1}}}},
                        text: level1Amounts.map(a => a > 0 ? formatCurrency(a) + '万' : ''),
                        textposition: 'outside',
                        yaxis: 'y'
                    }},
                    // 总部项目金额
                    {{
                        x: parkLabels,
                        y: hqAmounts,
                        type: 'bar',
                        name: '总部项目金额（万元）',
                        marker: {{color: '#91cc75', line: {{color: '#6fa85a', width: 1}}}},
                        text: hqAmounts.map(a => a > 0 ? formatCurrency(a) + '万' : ''),
                        textposition: 'outside',
                        yaxis: 'y'
                    }},
                    // 重大改造项目金额
                    {{
                        x: parkLabels,
                        y: majorAmounts,
                        type: 'bar',
                        name: '重大改造项目金额（万元）',
                        marker: {{color: '#fac858', line: {{color: '#d4a84a', width: 1}}}},
                        text: majorAmounts.map(a => a > 0 ? formatCurrency(a) + '万' : ''),
                        textposition: 'outside',
                        yaxis: 'y'
                    }},
                    // 重大改造项目数量（缩放后）
                    {{
                        x: parkLabels,
                        y: scaledCounts,
                        type: 'bar',
                        name: '重大改造项目数（个）',
                        marker: {{color: '#73c0de', line: {{color: '#4a9bc0', width: 1.5}}}},
                        text: majorCounts.map(c => c > 0 ? c + '个' : ''),
                        textposition: 'inside',
                        opacity: 0.85,
                        yaxis: 'y'
                    }},
                    // 一级项目占比
                    {{
                        x: parkLabels,
                        y: level1Percents,
                        type: 'scatter',
                        mode: 'lines+markers',
                        name: '一级项目占比（%）',
                        marker: {{color: '#ee6666', size: 10, line: {{width: 2, color: 'white'}}, symbol: 'circle'}},
                        line: {{color: '#ee6666', width: 3}},
                        yaxis: 'y2'
                    }},
                    // 总部项目占比
                    {{
                        x: parkLabels,
                        y: hqPercents,
                        type: 'scatter',
                        mode: 'lines+markers',
                        name: '总部项目占比（%）',
                        marker: {{color: '#ff9800', size: 10, line: {{width: 2, color: 'white'}}, symbol: 'square'}},
                        line: {{color: '#ff9800', width: 3, dash: 'dash'}},
                        yaxis: 'y2'
                    }},
                    // 重大改造项目占比
                    {{
                        x: parkLabels,
                        y: majorPercents,
                        type: 'scatter',
                        mode: 'lines+markers',
                        name: '重大改造项目占比（%）',
                        marker: {{color: '#9c27b0', size: 10, line: {{width: 2, color: 'white'}}, symbol: 'diamond'}},
                        line: {{color: '#9c27b0', width: 3, dash: 'dot'}},
                        yaxis: 'y2'
                    }}
                ], {{
                    title: '各园区分类项目统计（金额、项目数与占比）',
                    xaxis: {{tickangle: -45, title: '园区'}},
                    yaxis: {{title: '金额（万元）', side: 'left'}},
                    yaxis2: {{title: '占比（%）', side: 'right', overlaying: 'y', range: [0, 105]}},
                    barmode: 'group',
                    height: 700,
                    showlegend: true,
                    legend: {{orientation: 'h', yanchor: 'bottom', y: -0.18, xanchor: 'center', x: 0.5}}
                }}, {{displayModeBar: false}});
                
                // 对数刻度图表
                const maxTotal = Math.max(...parkLabels.map(p => parkAnalysis[p].total));
                const minTotal = Math.min(...parkLabels.filter(p => parkAnalysis[p].total > 0).map(p => parkAnalysis[p].total));
                let tickVals = null;
                let tickTexts = null;
                if (maxTotal > 0 && minTotal > 0 && maxTotal > minTotal * 2) {{
                    const logMin = Math.log10(Math.max(1, minTotal));
                    const logMax = Math.log10(maxTotal);
                    tickVals = [];
                    tickTexts = [];
                    for (let exp = Math.floor(logMin); exp <= Math.ceil(logMax); exp++) {{
                        for (let mult of [1, 2, 5]) {{
                            const val = mult * Math.pow(10, exp);
                            if (val >= Math.max(1, minTotal * 0.5) && val <= maxTotal * 1.5) {{
                                tickVals.push(val);
                                if (val >= 1000) {{
                                    tickTexts.push((val / 1000).toFixed(1) + '千');
                                }} else if (val >= 100) {{
                                    tickTexts.push(Math.floor(val).toString());
                                }} else {{
                                    tickTexts.push(val.toString());
                                }}
                            }}
                        }}
                    }}
                    // 去重并排序
                    const pairs = Array.from(new Set(tickVals.map((v, i) => [v, tickTexts[i]]))).sort((a, b) => a[0] - b[0]);
                    tickVals = pairs.map(p => p[0]);
                    tickTexts = pairs.map(p => p[1]);
                }}
                
                Plotly.newPlot('chart-park-log-scale', [
                    {{
                        x: parkLabels,
                        y: level1Amounts,
                        type: 'bar',
                        name: '一级项目金额（万元）',
                        marker: {{color: '#5470c6', line: {{color: '#3a5a9c', width: 1}}}},
                        text: level1Amounts.map(a => a > 0 ? formatCurrency(a) : ''),
                        textposition: 'outside'
                    }},
                    {{
                        x: parkLabels,
                        y: hqAmounts,
                        type: 'bar',
                        name: '总部项目金额（万元）',
                        marker: {{color: '#91cc75', line: {{color: '#6fa85a', width: 1}}}},
                        text: hqAmounts.map(a => a > 0 ? formatCurrency(a) : ''),
                        textposition: 'outside'
                    }},
                    {{
                        x: parkLabels,
                        y: majorAmounts,
                        type: 'bar',
                        name: '重大改造项目金额（万元）',
                        marker: {{color: '#fac858', line: {{color: '#d4a84a', width: 1}}}},
                        text: majorAmounts.map(a => a > 0 ? formatCurrency(a) : ''),
                        textposition: 'outside'
                    }}
                ], {{
                    title: '各园区分类项目金额统计（对数刻度，保证小金额园区可见性）',
                    xaxis: {{tickangle: -45, title: '园区'}},
                    yaxis: {{
                        title: '金额（万元，对数刻度）',
                        type: 'log',
                        tickvals: tickVals,
                        ticktext: tickTexts
                    }},
                    barmode: 'group',
                    height: 600,
                    showlegend: true,
                    legend: {{orientation: 'h', yanchor: 'bottom', y: -0.15, xanchor: 'center', x: 0.5}}
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-park-level1', [{{
                    x: parkLabels,
                    y: level1Amounts,
                    type: 'bar',
                    text: level1Amounts.map(a => formatCurrency(a)),
                    textposition: 'outside',
                    marker: {{color: '#FF6B6B'}}
                }}], {{
                    title: '各园区一级项目金额（万元）',
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-park-hq', [{{
                    x: parkLabels,
                    y: hqAmounts,
                    type: 'bar',
                    text: hqAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside',
                    marker: {{color: '#4ECDC4'}}
                }}], {{
                    title: '各园区总部项目金额（万元）',
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-park-major-amount', [{{
                    x: parkLabels,
                    y: majorAmounts,
                    type: 'bar',
                    text: majorAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside',
                    marker: {{color: '#45B7D1'}}
                }}], {{
                    title: '各园区重大改造项目金额（万元，≥200万）',
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-park-major-count', [{{
                    x: parkLabels,
                    y: majorCounts,
                    type: 'bar',
                    text: majorCounts,
                    textposition: 'outside',
                    marker: {{color: '#9a60b4'}}
                }}], {{
                    title: '各园区重大改造项目数量（≥200万）',
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '项目数'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                // 按月份统计图表
                if (Object.keys(monthlyStats).length > 0) {{
                    const months = Object.keys(monthlyStats).sort();
                    const monthlyCounts = months.map(m => monthlyStats[m].count);
                    const monthlyAmounts = months.map(m => monthlyStats[m].amount);
                    
                    Plotly.newPlot('chart-monthly-count', [{{
                        x: months,
                        y: monthlyCounts,
                        type: 'bar',
                        text: monthlyCounts,
                        textposition: 'outside',
                        marker: {{color: '#5470c6'}}
                    }}], {{
                        title: '每月立项项目数',
                        xaxis: {{tickangle: -45}},
                        yaxis: {{title: '项目数'}},
                        showlegend: false,
                        height: 350
                    }}, {{displayModeBar: false}});
                    
                    Plotly.newPlot('chart-monthly-amount', [{{
                        x: months,
                        y: monthlyAmounts,
                        type: 'bar',
                        text: monthlyAmounts.map(a => formatCurrency(a)),
                        textposition: 'outside',
                        marker: {{color: '#91cc75'}}
                    }}], {{
                        title: '每月立项金额（万元）',
                        xaxis: {{tickangle: -45}},
                        yaxis: {{title: '金额（万元）'}},
                        showlegend: false,
                        height: 350
                    }}, {{displayModeBar: false}});
                }}
                
                // 专业分包统计图表
                if (validData[0] && (validData[0].专业分包 || validData[0].专业细分)) {{
                    const profSubcontractCol = validData[0].专业分包 ? '专业分包' : '专业细分';
                    const profSubcontractStats = {{}};
                    validData.forEach(d => {{
                        const val = d[profSubcontractCol] || '未分类';
                        if (!profSubcontractStats[val]) {{
                            profSubcontractStats[val] = {{count: 0, amount: 0}};
                        }}
                        profSubcontractStats[val].count++;
                        profSubcontractStats[val].amount += parseFloat(d.拟定金额) || 0;
                    }});
                    
                    const profSubcontractLabels = Object.keys(profSubcontractStats).sort((a, b) => profSubcontractStats[b].amount - profSubcontractStats[a].amount);
                    const profSubcontractCounts = profSubcontractLabels.map(l => profSubcontractStats[l].count);
                    const profSubcontractAmounts = profSubcontractLabels.map(l => profSubcontractStats[l].amount);
                    
                    const colors = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4'];
                    
                    Plotly.newPlot('chart-prof-subcontract-count', [{{
                        values: profSubcontractCounts,
                        labels: profSubcontractLabels,
                        type: 'pie',
                        textinfo: 'label+percent+value',
                        textposition: 'outside',
                        marker: {{colors: colors.slice(0, profSubcontractLabels.length)}}
                    }}], {{
                        title: '专业分包项目数占比',
                        showlegend: true
                    }}, {{displayModeBar: false}});
                    
                    Plotly.newPlot('chart-prof-subcontract-amount', [{{
                        values: profSubcontractAmounts,
                        labels: profSubcontractLabels,
                        type: 'pie',
                        textinfo: 'label+percent+value',
                        textposition: 'outside',
                        marker: {{colors: colors.slice(0, profSubcontractLabels.length)}}
                    }}], {{
                        title: '专业分包金额占比',
                        showlegend: true
                    }}, {{displayModeBar: false}});
                }}
            }}, 100);
        }}
        
        // 标签页1: 统计
        function renderTab1() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-1');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 按专业统计（过滤掉"其它系统"）
            const profStats = {{}};
            validData.forEach(d => {{
                const prof = d.专业 || '未分类';
                // 过滤掉"其它系统"分类
                if (prof === '其它系统' || prof === '其他系统') return;
                if (!profStats[prof]) {{
                    profStats[prof] = {{count: 0, amount: 0}};
                }}
                profStats[prof].count++;
                profStats[prof].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按项目分级统计金额
            const levelAmountStats = {{}};
            validData.forEach(d => {{
                const level = d.项目分级 || '未分类';
                if (!levelAmountStats[level]) {{
                    levelAmountStats[level] = 0;
                }}
                levelAmountStats[level] += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按园区统计金额
            const parkAmountStats = {{}};
            validData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!parkAmountStats[park]) {{
                    parkAmountStats[park] = 0;
                }}
                parkAmountStats[park] += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按城市统计金额
            const cityAmountStats = {{}};
            validData.forEach(d => {{
                const city = d.城市 || '其他';
                if (city !== '其他') {{
                    if (!cityAmountStats[city]) {{
                        cityAmountStats[city] = 0;
                    }}
                    cityAmountStats[city] += parseFloat(d.拟定金额) || 0;
                }}
            }});
            
            // 按区域统计金额
            const regionAmountStats = {{}};
            validData.forEach(d => {{
                const region = d.所属区域 || '其他';
                if (region !== '其他') {{
                    if (!regionAmountStats[region]) {{
                        regionAmountStats[region] = 0;
                    }}
                    regionAmountStats[region] += parseFloat(d.拟定金额) || 0;
                }}
            }});
            
            // 按专业分包统计（如果存在）
            const hasProfSubcontract = validData[0] && (validData[0].专业分包 || validData[0].专业细分);
            const profSubcontractCol = hasProfSubcontract ? (validData[0].专业分包 ? '专业分包' : '专业细分') : null;
            const profSubcontractStats = {{}};
            if (hasProfSubcontract) {{
                validData.forEach(d => {{
                    const val = d[profSubcontractCol] || '未分类';
                    if (!profSubcontractStats[val]) {{
                        profSubcontractStats[val] = {{count: 0, amount: 0}};
                    }}
                    profSubcontractStats[val].count++;
                    profSubcontractStats[val].amount += parseFloat(d.拟定金额) || 0;
                }});
            }}
            
            // 按区域统计（详细统计，包含项目数、金额、园区数）
            const regionDetailedStats = {{}};
            validData.forEach(d => {{
                const region = d.所属区域 || '其他';
                if (region !== '其他') {{
                    if (!regionDetailedStats[region]) {{
                        regionDetailedStats[region] = {{count: 0, amount: 0, parks: new Set()}};
                    }}
                    regionDetailedStats[region].count++;
                    regionDetailedStats[region].amount += parseFloat(d.拟定金额) || 0;
                    if (d.园区) regionDetailedStats[region].parks.add(d.园区);
                }}
            }});
            
            // 按区域下各园区统计
            const regionParkDetails = {{}};
            Object.keys(regionDetailedStats).forEach(region => {{
                const regionData = validData.filter(d => d.所属区域 === region);
                const parkStatsInRegion = {{}};
                regionData.forEach(d => {{
                    const park = d.园区 || '未知';
                    if (!parkStatsInRegion[park]) {{
                        parkStatsInRegion[park] = {{count: 0, amount: 0}};
                    }}
                    parkStatsInRegion[park].count++;
                    parkStatsInRegion[park].amount += parseFloat(d.拟定金额) || 0;
                }});
                regionParkDetails[region] = parkStatsInRegion;
            }});
            
            let html = `
                <div class="section">
                    ${{Object.keys(regionDetailedStats).length > 0 ? `
                    <h2>📊 按区域统计分析</h2>
                    
                    <h3>各区域项目统计</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">总区域数</div>
                            <div class="metric-value">${{Object.keys(regionDetailedStats).length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总项目数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionDetailedStats).reduce((sum, r) => sum + r.count, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(Object.values(regionDetailedStats).reduce((sum, r) => sum + r.amount, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总园区数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionDetailedStats).reduce((sum, r) => sum + r.parks.size, 0))}}</div>
                        </div>
                    </div>
                    
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>所属区域</th><th>项目数</th><th>金额合计（万元）</th><th>园区数</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(regionDetailedStats).sort((a, b) => regionDetailedStats[b].count - regionDetailedStats[a].count).map(region => {{
                                    const stats = regionDetailedStats[region];
                                    return `
                                        <tr>
                                            <td>${{region}}</td>
                                            <td>${{stats.count}}</td>
                                            <td>${{formatCurrency(stats.amount)}}</td>
                                            <td>${{stats.parks.size}}</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>各区域下园区明细</h3>
                    ${{Object.keys(regionDetailedStats).sort((a, b) => regionDetailedStats[b].count - regionDetailedStats[a].count).map(region => {{
                        const stats = regionDetailedStats[region];
                        const parkDetails = regionParkDetails[region];
                        return `
                            <div class="expander">
                                <div class="expander-header" onclick="toggleExpander(this)">
                                    <span><strong>${{region}}</strong>（${{Object.keys(parkDetails).length}}个园区，${{stats.count}}个项目，${{formatCurrency(stats.amount)}}万元）</span>
                                    <span class="expander-icon">▶</span>
                                </div>
                                <div class="expander-content">
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(parkDetails).sort((a, b) => parkDetails[b].amount - parkDetails[a].amount).map(park => `
                                                    <tr>
                                                        <td>${{park}}</td>
                                                        <td>${{parkDetails[park].count}}</td>
                                                        <td>${{formatCurrency(parkDetails[park].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        `;
                    }}).join('')}}
                    
                    <hr style="margin: 30px 0;"/>
                    ` : ''}}
                    
                    <h2>📊 图表统计</h2>
                    
                    <h3>按专业 · 项目数</h3>
                    <div class="chart-container">
                        <div id="chart-prof-count"></div>
                    </div>
                    
                    <h3>按项目分级 · 金额占比</h3>
                    <div class="chart-container">
                        <div id="chart-level-amount-pie"></div>
                    </div>
                    
                    <h3>按园区 · 金额（万元）</h3>
                    <div class="chart-container">
                        <div id="chart-park-amount"></div>
                    </div>
                    
                    <h3>按城市 · 金额（万元）</h3>
                    <div class="chart-container">
                        <div id="chart-city-amount"></div>
                    </div>
                    
                    <h3>按所属区域 · 金额分布（万元）</h3>
                    <div class="chart-container">
                        <div id="chart-region-amount"></div>
                    </div>
                    
                    <h3>按专业 · 金额合计（万元）</h3>
                    <div class="chart-container">
                        <div id="chart-prof-amount"></div>
                    </div>
                    
                    ${{hasProfSubcontract ? `
                    <h3>按专业分包 · 项目数</h3>
                    <div class="chart-container">
                        <div id="chart-prof-subcontract-count-tab1"></div>
                    </div>
                    
                    <h3>按专业分包 · 金额占比</h3>
                    <div class="chart-container">
                        <div id="chart-prof-subcontract-amount-tab1"></div>
                    </div>
                    ` : ''}}
                </div>
            `;
            
            container.innerHTML = html;
            
            // 渲染图表
            setTimeout(() => {{
                // 按专业项目数
                const profLabels = Object.keys(profStats).sort((a, b) => profStats[b].count - profStats[a].count);
                const profCounts = profLabels.map(p => profStats[p].count);
                Plotly.newPlot('chart-prof-count', [{{
                    x: profLabels,
                    y: profCounts,
                    type: 'bar',
                    marker: {{color: profCounts, colorscale: 'Blues'}},
                    text: profCounts,
                    textposition: 'outside'
                }}], {{
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '项目数'}},
                    showlegend: false,
                    margin: {{t: 20, b: 80}}
                }}, {{displayModeBar: false}});
                
                // 按项目分级金额占比
                const levelLabels = Object.keys(levelAmountStats);
                const levelAmounts = levelLabels.map(l => levelAmountStats[l]);
                Plotly.newPlot('chart-level-amount-pie', [{{
                    values: levelAmounts,
                    labels: levelLabels,
                    type: 'pie',
                    hole: 0.35,
                    textinfo: 'label+percent+value',
                    textposition: 'outside',
                    texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value:,.0f}}万元'
                }}], {{
                    showlegend: true,
                    legend: {{orientation: 'h', yanchor: 'bottom', y: -0.2}}
                }}, {{displayModeBar: false}});
                
                // 按园区金额
                const parkLabels = Object.keys(parkAmountStats).sort((a, b) => parkAmountStats[b] - parkAmountStats[a]).slice(0, 20);
                const parkAmounts = parkLabels.map(p => parkAmountStats[p]);
                Plotly.newPlot('chart-park-amount', [{{
                    x: parkLabels,
                    y: parkAmounts,
                    type: 'bar',
                    marker: {{color: parkAmounts, colorscale: 'Blues'}},
                    text: parkAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside'
                }}], {{
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    margin: {{t: 20, b: 80}}
                }}, {{displayModeBar: false}});
                
                // 按城市金额
                const cityLabels = Object.keys(cityAmountStats).sort((a, b) => cityAmountStats[b] - cityAmountStats[a]);
                const cityAmounts = cityLabels.map(c => cityAmountStats[c]);
                if (cityLabels.length > 0) {{
                    Plotly.newPlot('chart-city-amount', [{{
                        x: cityLabels,
                        y: cityAmounts,
                        type: 'bar',
                        marker: {{color: cityAmounts, colorscale: 'Teal'}},
                        text: cityAmounts.map(a => formatCurrency(a)),
                        textposition: 'outside'
                    }}], {{
                        xaxis: {{tickangle: -45}},
                        yaxis: {{title: '金额（万元）'}},
                        showlegend: false,
                        margin: {{t: 20, b: 80}}
                    }}, {{displayModeBar: false}});
                }}
                
                // 按区域金额
                const regionLabels = Object.keys(regionAmountStats).sort((a, b) => regionAmountStats[b] - regionAmountStats[a]);
                const regionAmounts = regionLabels.map(r => regionAmountStats[r]);
                if (regionLabels.length > 0) {{
                    Plotly.newPlot('chart-region-amount', [{{
                        values: regionAmounts,
                        labels: regionLabels,
                        type: 'pie',
                        hole: 0.4,
                        textinfo: 'label+percent+value',
                        textposition: 'outside',
                        texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value:,.0f}}万元',
                        marker: {{colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8']}}
                    }}], {{
                        showlegend: true,
                        legend: {{orientation: 'h', yanchor: 'bottom', y: -0.15}}
                    }}, {{displayModeBar: false}});
                }}
                
                // 按专业金额
                const profAmountLabels = Object.keys(profStats).sort((a, b) => profStats[b].amount - profStats[a].amount);
                const profAmounts = profAmountLabels.map(p => profStats[p].amount);
                Plotly.newPlot('chart-prof-amount', [{{
                    x: profAmountLabels,
                    y: profAmounts,
                    type: 'bar',
                    marker: {{color: profAmounts, colorscale: 'Viridis'}},
                    text: profAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside'
                }}], {{
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    margin: {{t: 20, b: 80}}
                }}, {{displayModeBar: false}});
                
                // 按专业分包统计图表
                if (hasProfSubcontract) {{
                    const profSubcontractLabels = Object.keys(profSubcontractStats).sort((a, b) => profSubcontractStats[b].amount - profSubcontractStats[a].amount);
                    const profSubcontractCounts = profSubcontractLabels.map(l => profSubcontractStats[l].count);
                    const profSubcontractAmounts = profSubcontractLabels.map(l => profSubcontractStats[l].amount);
                    const colors = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4'];
                    
                    Plotly.newPlot('chart-prof-subcontract-count-tab1', [{{
                        x: profSubcontractLabels,
                        y: profSubcontractCounts,
                        type: 'bar',
                        marker: {{color: profSubcontractCounts, colorscale: 'Blues'}},
                        text: profSubcontractCounts,
                        textposition: 'outside'
                    }}], {{
                        title: '按专业分包 · 项目数',
                        xaxis: {{tickangle: -45}},
                        yaxis: {{title: '项目数'}},
                        showlegend: false,
                        margin: {{t: 20, b: 80}},
                        height: 400
                    }}, {{displayModeBar: false}});
                    
                    Plotly.newPlot('chart-prof-subcontract-amount-tab1', [{{
                        values: profSubcontractAmounts,
                        labels: profSubcontractLabels,
                        type: 'pie',
                        hole: 0.35,
                        textinfo: 'label+percent+value',
                        textposition: 'outside',
                        texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value:,.0f}}万元',
                        marker: {{colors: colors.slice(0, profSubcontractLabels.length)}}
                    }}], {{
                        title: '按专业分包 · 金额占比',
                        showlegend: true,
                        legend: {{orientation: 'h', yanchor: 'bottom', y: -0.2}}
                    }}, {{displayModeBar: false}});
                }}
            }}, 100);
        }}
        
        // 标签页2: 地区分析
        function renderTab2() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-2');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 按区域统计
            const regionStats = {{}};
            validData.forEach(d => {{
                const region = d.所属区域 || '其他';
                if (region !== '其他') {{
                    if (!regionStats[region]) {{
                        regionStats[region] = {{
                            count: 0,
                            amount: 0,
                            parks: new Set(),
                            cities: new Set()
                        }};
                    }}
                    regionStats[region].count++;
                    regionStats[region].amount += parseFloat(d.拟定金额) || 0;
                    if (d.园区) regionStats[region].parks.add(d.园区);
                    if (d.城市) regionStats[region].cities.add(d.城市);
                }}
            }});
            
            let html = `
                <div class="section">
                    <h2>🌍 地区分析：按所属区域统计</h2>
                    
                    <h3>📊 区域总览</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">总区域数</div>
                            <div class="metric-value">${{Object.keys(regionStats).length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总项目数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionStats).reduce((sum, r) => sum + r.count, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(Object.values(regionStats).reduce((sum, r) => sum + r.amount, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总园区数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionStats).reduce((sum, r) => sum + r.parks.size, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总城市数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionStats).reduce((sum, r) => sum + r.cities.size, 0))}}</div>
                        </div>
                    </div>
                    
                    <h4>各区域统计汇总</h4>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>所属区域</th><th>项目数</th><th>金额合计（万元）</th><th>平均项目金额（万元）</th><th>园区数</th><th>城市数</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count).map(region => {{
                                    const stats = regionStats[region];
                                    const avgAmount = stats.count > 0 ? (stats.amount / stats.count).toFixed(2) : 0;
                                    return `
                                        <tr>
                                            <td>${{region}}</td>
                                            <td>${{stats.count}}</td>
                                            <td>${{formatCurrency(stats.amount)}}</td>
                                            <td>${{avgAmount}}</td>
                                            <td>${{stats.parks.size}}</td>
                                            <td>${{stats.cities.size}}</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>📈 区域对比分析</h3>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0;">
                        <div class="chart-container">
                            <div id="chart-region-count-bar"></div>
                        </div>
                        <div class="chart-container">
                            <div id="chart-region-amount-bar"></div>
                        </div>
                        <div class="chart-container">
                            <div id="chart-region-amount-pie"></div>
                        </div>
                        <div class="chart-container">
                            <div id="chart-region-count-pie"></div>
                        </div>
                    </div>
                    
                    <h3>🔍 各区域详细分析</h3>
                    ${{Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count).map(region => {{
                        const stats = regionStats[region];
                        const regionData = validData.filter(d => d.所属区域 === region);
                        
                        // 按园区统计
                        const parkStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const park = d.园区 || '未知';
                            if (!parkStatsInRegion[park]) {{
                                parkStatsInRegion[park] = {{count: 0, amount: 0}};
                            }}
                            parkStatsInRegion[park].count++;
                            parkStatsInRegion[park].amount += parseFloat(d.拟定金额) || 0;
                        }});
                        
                        // 按专业统计（过滤掉"其它系统"）
                        const profStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const prof = d.专业 || '未分类';
                            // 过滤掉"其它系统"分类
                            if (prof === '其它系统' || prof === '其他系统') return;
                            if (!profStatsInRegion[prof]) {{
                                profStatsInRegion[prof] = {{count: 0, amount: 0}};
                            }}
                            profStatsInRegion[prof].count++;
                            profStatsInRegion[prof].amount += parseFloat(d.拟定金额) || 0;
                        }});
                        
                        // 按城市统计
                        const cityStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const city = d.城市 || '未知';
                            if (!cityStatsInRegion[city]) {{
                                cityStatsInRegion[city] = {{count: 0, amount: 0, parks: new Set()}};
                            }}
                            cityStatsInRegion[city].count++;
                            cityStatsInRegion[city].amount += parseFloat(d.拟定金额) || 0;
                            if (d.园区) cityStatsInRegion[city].parks.add(d.园区);
                        }});
                        
                        // 按项目分级统计
                        const levelStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const level = d.项目分级 || '未分类';
                            if (!levelStatsInRegion[level]) {{
                                levelStatsInRegion[level] = {{count: 0, amount: 0}};
                            }}
                            levelStatsInRegion[level].count++;
                            levelStatsInRegion[level].amount += parseFloat(d.拟定金额) || 0;
                        }});
                        
                        return `
                            <div class="expander">
                                <div class="expander-header" onclick="toggleExpander(this)">
                                    <span><strong>${{region}}</strong> - ${{stats.parks.size}}个园区，${{stats.count}}个项目，${{formatCurrency(stats.amount)}}万元</span>
                                    <span class="expander-icon">▶</span>
                                </div>
                                <div class="expander-content">
                                    <div class="metrics">
                                        <div class="metric">
                                            <div class="metric-label">项目数</div>
                                            <div class="metric-value">${{stats.count}}</div>
                                        </div>
                                        <div class="metric">
                                            <div class="metric-label">金额合计（万元）</div>
                                            <div class="metric-value">${{formatCurrency(stats.amount)}}</div>
                                        </div>
                                        <div class="metric">
                                            <div class="metric-label">园区数</div>
                                            <div class="metric-value">${{stats.parks.size}}</div>
                                        </div>
                                        <div class="metric">
                                            <div class="metric-label">城市数</div>
                                            <div class="metric-value">${{stats.cities.size}}</div>
                                        </div>
                                    </div>
                                    
                                    <h4>各园区统计</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(parkStatsInRegion).sort((a, b) => parkStatsInRegion[b].amount - parkStatsInRegion[a].amount).map(park => `
                                                    <tr>
                                                        <td>${{park}}</td>
                                                        <td>${{parkStatsInRegion[park].count}}</td>
                                                        <td>${{formatCurrency(parkStatsInRegion[park].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <h4>各城市统计</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>城市</th><th>项目数</th><th>金额合计（万元）</th><th>园区数</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(cityStatsInRegion).sort((a, b) => cityStatsInRegion[b].count - cityStatsInRegion[a].count).map(city => `
                                                    <tr>
                                                        <td>${{city}}</td>
                                                        <td>${{cityStatsInRegion[city].count}}</td>
                                                        <td>${{formatCurrency(cityStatsInRegion[city].amount)}}</td>
                                                        <td>${{cityStatsInRegion[city].parks.size}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <h4>按专业分类统计</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>专业</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(profStatsInRegion).sort((a, b) => profStatsInRegion[b].amount - profStatsInRegion[a].amount).map(prof => `
                                                    <tr>
                                                        <td>${{prof}}</td>
                                                        <td>${{profStatsInRegion[prof].count}}</td>
                                                        <td>${{formatCurrency(profStatsInRegion[prof].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <h4>按项目分级统计</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>项目分级</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(levelStatsInRegion).sort((a, b) => levelStatsInRegion[b].count - levelStatsInRegion[a].count).map(level => `
                                                    <tr>
                                                        <td>${{level || '未分类'}}</td>
                                                        <td>${{levelStatsInRegion[level].count}}</td>
                                                        <td>${{formatCurrency(levelStatsInRegion[level].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <h4>项目明细（前20条）</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>园区</th><th>城市</th><th>序号</th><th>项目分级</th>${{regionData[0] && regionData[0].项目分类 ? '<th>项目分类</th>' : ''}}<th>专业</th><th>项目名称</th><th>拟定金额</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{regionData.slice(0, 20).map(d => `
                                                    <tr>
                                                        <td>${{getValue(d, '园区')}}</td>
                                                        <td>${{getValue(d, '城市')}}</td>
                                                        <td>${{getValue(d, '序号')}}</td>
                                                        <td>${{getValue(d, '项目分级')}}</td>
                                                        ${{d.项目分类 ? `<td>${{getValue(d, '项目分类')}}</td>` : ''}}
                                                        <td>${{getValue(d, '专业')}}</td>
                                                        <td>${{getValue(d, '项目名称')}}</td>
                                                        <td>${{formatCurrency(getValue(d, '拟定金额'))}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    ${{regionData.length > 20 ? `<p style="color: #666; font-size: 12px; margin-top: 10px;">共 ${{regionData.length}} 条项目，仅显示前20条。可在「全部项目」Tab 中查看完整列表。</p>` : ''}}
                                </div>
                            </div>
                        `;
                    }}).join('')}}
                </div>
            `;
            
            container.innerHTML = html;
            
            // 渲染区域对比图表
            setTimeout(() => {{
                const regionLabels = Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count);
                const regionCounts = regionLabels.map(r => regionStats[r].count);
                const regionAmounts = regionLabels.map(r => regionStats[r].amount);
                const colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8'];
                
                // 第一个子图：项目数柱状图
                Plotly.newPlot('chart-region-count-bar', [{{
                    x: regionLabels,
                    y: regionCounts,
                    type: 'bar',
                    marker: {{color: colors.slice(0, regionLabels.length)}},
                    text: regionCounts,
                    textposition: 'outside'
                }}], {{
                    title: '各区域项目数对比',
                    xaxis: {{title: '所属区域', tickangle: 0}},
                    yaxis: {{title: '项目数'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                // 第二个子图：金额柱状图
                Plotly.newPlot('chart-region-amount-bar', [{{
                    x: regionLabels,
                    y: regionAmounts,
                    type: 'bar',
                    marker: {{color: colors.slice(0, regionLabels.length)}},
                    text: regionAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside'
                }}], {{
                    title: '各区域金额对比（万元）',
                    xaxis: {{title: '所属区域', tickangle: 0}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                // 第三个子图：金额分布饼图
                Plotly.newPlot('chart-region-amount-pie', [{{
                    values: regionAmounts,
                    labels: regionLabels,
                    type: 'pie',
                    hole: 0.4,
                    textinfo: 'label+percent+value',
                    texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value:,.0f}}万元',
                    marker: {{colors: colors.slice(0, regionLabels.length)}}
                }}], {{
                    title: '各区域金额分布（万元）',
                    showlegend: true,
                    height: 350
                }}, {{displayModeBar: false}});
                
                // 第四个子图：项目数分布饼图
                Plotly.newPlot('chart-region-count-pie', [{{
                    values: regionCounts,
                    labels: regionLabels,
                    type: 'pie',
                    hole: 0.4,
                    textinfo: 'label+percent+value',
                    texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value}}项',
                    marker: {{colors: colors.slice(0, regionLabels.length)}}
                }}], {{
                    title: '各区域项目数分布',
                    showlegend: true,
                    height: 350
                }}, {{displayModeBar: false}});
            }}, 100);
        }}
        
        // 标签页3: 各园区分级分类
        function renderTab3() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-3');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 按分级统计
            const levelStats = {{}};
            validData.forEach(d => {{
                const level = d.项目分级 || '未分类';
                if (!levelStats[level]) {{
                    levelStats[level] = {{count: 0, amount: 0}};
                }}
                levelStats[level].count++;
                levelStats[level].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按专业统计（过滤掉"其它系统"）
            const profStats = {{}};
            validData.forEach(d => {{
                const prof = d.专业 || '未分类';
                // 过滤掉"其它系统"分类
                if (prof === '其它系统' || prof === '其他系统') return;
                if (!profStats[prof]) {{
                    profStats[prof] = {{count: 0, amount: 0}};
                }}
                profStats[prof].count++;
                profStats[prof].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按园区统计
            const parkStats = {{}};
            validData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!parkStats[park]) {{
                    parkStats[park] = {{count: 0, amount: 0}};
                }}
                parkStats[park].count++;
                parkStats[park].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按专业分包统计（如果存在）
            const hasProfSubcontract = validData[0] && (validData[0].专业分包 || validData[0].专业细分);
            const profSubcontractCol = hasProfSubcontract ? (validData[0].专业分包 ? '专业分包' : '专业细分') : null;
            const profSubcontractStats = {{}};
            if (hasProfSubcontract) {{
                validData.forEach(d => {{
                    const val = d[profSubcontractCol] || '未分类';
                    if (!profSubcontractStats[val]) {{
                        profSubcontractStats[val] = {{count: 0, amount: 0}};
                    }}
                    profSubcontractStats[val].count++;
                    profSubcontractStats[val].amount += parseFloat(d.拟定金额) || 0;
                }});
            }}
            
            let html = `
                <div class="section">
                    <h2>📋 各园区分级分类统计</h2>
                    
                    <h3>按紧急程度（分级）</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>项目分级</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(levelStats).map(level => `
                                    <tr>
                                        <td>${{level || '未分类'}}</td>
                                        <td>${{levelStats[level].count}}</td>
                                        <td>${{formatCurrency(levelStats[level].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>按专业分类</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>专业</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(profStats).map(prof => `
                                    <tr>
                                        <td>${{prof || '未分类'}}</td>
                                        <td>${{profStats[prof].count}}</td>
                                        <td>${{formatCurrency(profStats[prof].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    ${{hasProfSubcontract ? `
                    <h3>按专业分包</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>专业分包</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(profSubcontractStats).sort((a, b) => profSubcontractStats[b].amount - profSubcontractStats[a].amount).map(key => `
                                    <tr>
                                        <td>${{key || '未分类'}}</td>
                                        <td>${{profSubcontractStats[key].count}}</td>
                                        <td>${{formatCurrency(profSubcontractStats[key].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    ` : ''}}
                    
                    <h3>按园区</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkStats).sort((a, b) => parkStats[b].amount - parkStats[a].amount).map(park => `
                                    <tr>
                                        <td>${{park}}</td>
                                        <td>${{parkStats[park].count}}</td>
                                        <td>${{formatCurrency(parkStats[park].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>全部项目清单（可筛选）</h3>
                    <div style="margin: 15px 0;">
                        <label style="display: inline-block; margin-right: 15px;">
                            <strong>按分级筛选：</strong>
                            <select id="level-filter-tab3" multiple style="padding: 5px; min-width: 150px;" onchange="filterTab3()">
                                <option value="">全部</option>
                                ${{Object.keys(levelStats).map(level => `
                                    <option value="${{level}}">${{level || '未分类'}}</option>
                                `).join('')}}
                            </select>
                        </label>
                        <label style="display: inline-block; margin-right: 15px;">
                            <strong>按专业筛选：</strong>
                            <select id="prof-filter-tab3" multiple style="padding: 5px; min-width: 150px;" onchange="filterTab3()">
                                <option value="">全部</option>
                                ${{Object.keys(profStats).map(prof => `
                                    <option value="${{prof}}">${{prof || '未分类'}}</option>
                                `).join('')}}
                            </select>
                        </label>
                        ${{hasProfSubcontract ? `
                        <label style="display: inline-block;">
                            <strong>按专业分包筛选：</strong>
                            <select id="prof-subcontract-filter-tab3" multiple style="padding: 5px; min-width: 150px;" onchange="filterTab3()">
                                <option value="">全部</option>
                                ${{Object.keys(profSubcontractStats).map(key => `
                                    <option value="${{key}}">${{key || '未分类'}}</option>
                                `).join('')}}
                            </select>
                        </label>
                        ` : ''}}
                    </div>
                    <div class="info-box">
                        <p id="filter-count-tab3">共 ${{validData.length}} 条项目</p>
                    </div>
                    <div class="data-table-container">
                        <table id="detail-table-tab3">
                            <thead>
                                <tr>
                                    <th>园区</th>
                                    <th>序号</th>
                                    <th>项目分级</th>
                                    ${{validData[0] && validData[0].项目分类 ? '<th>项目分类</th>' : ''}}
                                    <th>专业</th>
                                    ${{hasProfSubcontract ? '<th>专业分包</th>' : ''}}
                                    <th>项目名称</th>
                                    <th>拟定金额</th>
                                    ${{validData[0] && validData[0].拟定承建组织 ? '<th>拟定承建组织</th>' : ''}}
                                    ${{validData[0] && validData[0].需求立项 ? '<th>需求立项</th>' : ''}}
                                    ${{validData[0] && (validData[0].验收 || validData[0]['验收(社区需求完成交付)']) ? '<th>验收</th>' : ''}}
                                </tr>
                            </thead>
                            <tbody>
                                ${{validData.map(d => {{
                                    const profSubcontractVal = hasProfSubcontract ? (getValue(d, profSubcontractCol) || '') : '';
                                    return `
                                    <tr data-level="${{getValue(d, '项目分级')}}" data-prof="${{getValue(d, '专业')}}" ${{hasProfSubcontract ? `data-prof-subcontract="${{profSubcontractVal}}"` : ''}}>
                                        <td>${{getValue(d, '园区')}}</td>
                                        <td>${{getValue(d, '序号')}}</td>
                                        <td>${{getValue(d, '项目分级')}}</td>
                                        ${{d.项目分类 ? `<td>${{getValue(d, '项目分类')}}</td>` : ''}}
                                        <td>${{getValue(d, '专业')}}</td>
                                        ${{hasProfSubcontract ? `<td>${{profSubcontractVal || '未分类'}}</td>` : ''}}
                                        <td>${{getValue(d, '项目名称')}}</td>
                                        <td>${{formatCurrency(getValue(d, '拟定金额'))}}</td>
                                        ${{d.拟定承建组织 ? `<td>${{getValue(d, '拟定承建组织')}}</td>` : ''}}
                                        ${{d.需求立项 ? `<td>${{getValue(d, '需求立项')}}</td>` : ''}}
                                        ${{(d.验收 || d['验收(社区需求完成交付)']) ? `<td>${{getValue(d, '验收(社区需求完成交付)') || getValue(d, '验收')}}</td>` : ''}}
                                    </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
            
            container.innerHTML = html;
        }}
        
        // 标签页3的筛选功能
        function filterTab3() {{
            const levelFilter = Array.from(document.getElementById('level-filter-tab3').selectedOptions).map(opt => opt.value).filter(v => v);
            const profFilter = Array.from(document.getElementById('prof-filter-tab3').selectedOptions).map(opt => opt.value).filter(v => v);
            const profSubcontractFilterEl = document.getElementById('prof-subcontract-filter-tab3');
            const profSubcontractFilter = profSubcontractFilterEl ? Array.from(profSubcontractFilterEl.selectedOptions).map(opt => opt.value).filter(v => v) : [];
            
            const rows = document.querySelectorAll('#detail-table-tab3 tbody tr');
            let visibleCount = 0;
            
            rows.forEach(row => {{
                const level = row.getAttribute('data-level') || '';
                const prof = row.getAttribute('data-prof') || '';
                const profSubcontract = row.getAttribute('data-prof-subcontract') || '';
                
                const levelMatch = levelFilter.length === 0 || levelFilter.includes(level);
                const profMatch = profFilter.length === 0 || profFilter.includes(prof);
                const profSubcontractMatch = profSubcontractFilter.length === 0 || profSubcontractFilter.includes(profSubcontract);
                
                if (levelMatch && profMatch && profSubcontractMatch) {{
                    row.style.display = '';
                    visibleCount++;
                }} else {{
                    row.style.display = 'none';
                }}
            }});
            
            document.getElementById('filter-count-tab3').textContent = `共 ${{visibleCount}} 条项目`;
        }}
        
        // 标签页4: 总部视图
        function renderTab4() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-4');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 稳定需求判断：需求已立项（需求立项日期有效）且非无效日期
            const stableData = validData.filter(d => isStableRequirement(d));
            
            // 按园区统计稳定需求
            const stableParkStats = {{}};
            stableData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!stableParkStats[park]) {{
                    stableParkStats[park] = {{count: 0, amount: 0}};
                }}
                stableParkStats[park].count++;
                stableParkStats[park].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 查找验收列和实施列
            let acceptCol = null;
            let implCol = null;
            for (let key in validData[0]) {{
                if (!acceptCol && (key.includes('验收') || key === '验收(社区需求完成交付)')) {{
                    acceptCol = key;
                }}
                if (!implCol && key.includes('实施') && !key.toLowerCase().includes('时间')) {{
                    implCol = key;
                }}
            }}
            
            // 施工进展与验收时间预告
            const previewData = validData.map(d => {{
                const preview = {{
                    园区: d.园区 || '',
                    序号: d.序号 || '',
                    项目名称: d.项目名称 || '',
                    拟定金额: parseFloat(d.拟定金额) || 0,
                    拟定承建组织: d.拟定承建组织 || '',
                    实施时间: implCol ? (d[implCol] || '') : '',
                    验收时间: acceptCol ? (d[acceptCol] || '') : ''
                }};
                
                // 判断验收日期是否有效
                const acceptDateStr = preview.验收时间;
                preview.验收有效 = false;
                if (acceptDateStr && acceptDateStr !== null && acceptDateStr !== '') {{
                    const str = String(acceptDateStr).trim();
                    if (str && !str.startsWith('-') && !str.includes('1900')) {{
                        preview.验收有效 = true;
                    }}
                }}
                
                return preview;
            }});
            
            const acceptPreview = previewData.filter(d => d.验收有效).sort((a, b) => {{
                const dateA = parseDate(a.验收时间);
                const dateB = parseDate(b.验收时间);
                if (!dateA) return 1;
                if (!dateB) return -1;
                return dateA - dateB;
            }});
            
            let html = `
                <div class="section">
                    <h2>🏢 总部视图：稳定需求与施工验收</h2>
                    
                    <h3>各园区已确定稳定需求数量与金额</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">稳定需求项目数</div>
                            <div class="metric-value">${{stableData.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">稳定需求金额合计（万元）</div>
                            <div class="metric-value">${{formatCurrency(stableData.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                    </div>
                    
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>稳定需求数量</th><th>稳定需求金额（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(stableParkStats).sort((a, b) => stableParkStats[b].amount - stableParkStats[a].amount).map(park => `
                                    <tr>
                                        <td>${{park}}</td>
                                        <td>${{stableParkStats[park].count}}</td>
                                        <td>${{formatCurrency(stableParkStats[park].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>施工进展与验收时间预告</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>园区</th>
                                    <th>序号</th>
                                    <th>项目名称</th>
                                    <th>拟定金额（万元）</th>
                                    <th>拟定承建组织</th>
                                    <th>实施时间</th>
                                    <th>验收时间</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${{previewData.map(d => `
                                    <tr style="background-color: ${{d.验收有效 ? '#e8f5e9' : ''}}">
                                        <td>${{d.园区}}</td>
                                        <td>${{d.序号}}</td>
                                        <td>${{d.项目名称}}</td>
                                        <td>${{formatCurrency(d.拟定金额)}}</td>
                                        <td>${{d.拟定承建组织}}</td>
                                        <td>${{d.实施时间}}</td>
                                        <td>${{d.验收时间}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h4>验收时间预告（仅含有效日期）</h4>
                    ${{acceptPreview.length > 0 ? `
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>园区</th>
                                    <th>序号</th>
                                    <th>项目名称</th>
                                    <th>拟定金额（万元）</th>
                                    <th>拟定承建组织</th>
                                    <th>实施时间</th>
                                    <th>验收时间</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${{acceptPreview.map(d => `
                                    <tr>
                                        <td>${{d.园区}}</td>
                                        <td>${{d.序号}}</td>
                                        <td>${{d.项目名称}}</td>
                                        <td>${{formatCurrency(d.拟定金额)}}</td>
                                        <td>${{d.拟定承建组织}}</td>
                                        <td>${{d.实施时间}}</td>
                                        <td>${{d.验收时间}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    ` : '<div class="info-box">暂无有效验收日期，请在一线填报「验收(社区需求完成交付)」节点。</div>'
                    }}
                </div>
            `;
            
            container.innerHTML = html;
        }}
        
        // 标签页5: 全部项目
        function renderTab5() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-5');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 获取所有列
            const columns = new Set();
            validData.forEach(d => {{
                Object.keys(d).forEach(k => columns.add(k));
            }});
            const columnList = ['园区', '所属区域', '城市', ...Array.from(columns).filter(c => !['园区', '所属区域', '城市'].includes(c))];
            
            let html = `
                <div class="section">
                    <h2>📑 全部项目清单</h2>
                    <div class="info-box">
                        <p>共 ${{validData.length}} 条项目，以下列出所有项目明细。</p>
                    </div>
                    <div class="data-table-container" style="overflow-x: auto;">
                        <table style="font-size: 11px;">
                            <thead>
                                <tr>
                                    ${{columnList.map(col => `<th>${{col}}</th>`).join('')}}
                                </tr>
                            </thead>
                            <tbody>
                                ${{validData.map(d => `
                                    <tr>
                                        ${{columnList.map(col => {{
                                            const val = getValue(d, col);
                                            if (isValidNumber(val) && (col.includes('金额') || col.includes('金额'))) {{
                                                return `<td>${{formatCurrency(val)}}</td>`;
                                            }} else if (isValidNumber(val)) {{
                                                return `<td>${{formatNumber(val)}}</td>`;
                                            }} else {{
                                                return `<td>${{String(val).substring(0, 50)}}</td>`;
                                            }}
                                        }}).join('')}}
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
            
            container.innerHTML = html;
        }}
        
        // 展开/收起功能
        function toggleExpander(header) {{
            header.classList.toggle('active');
            const content = header.nextElementSibling;
            content.classList.toggle('active');
        }}
        
        // 初始化渲染
        renderAllTabs();
    </script>
</body>
</html>'''
    
    return html_content


def generate_html_report(df: pd.DataFrame, sub: pd.DataFrame, sub_location: pd.DataFrame, 园区选择: list) -> str:
    """生成交互式HTML报告（新版本，完全交互式）"""
    return generate_interactive_html(df, 园区选择)
    
    # HTML头部
    html_parts.append('''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>养老社区改良改造进度管理报告</title>
        <style>
            body {
                font-family: "Microsoft YaHei", "SimSun", "SimHei", Arial, sans-serif;
                font-size: 12px;
                line-height: 1.6;
                color: #333;
                padding: 20px;
                max-width: 1400px;
                margin: 0 auto;
            }
            h1 { font-size: 24px; color: #1f4788; margin-top: 20px; margin-bottom: 15px; text-align: center; }
            h2 { font-size: 20px; color: #2c5aa0; margin-top: 30px; margin-bottom: 15px; border-bottom: 2px solid #4a7bc8; padding-bottom: 5px; }
            h3 { font-size: 16px; color: #4a7bc8; margin-top: 20px; margin-bottom: 10px; }
            h4 { font-size: 14px; margin-top: 15px; margin-bottom: 8px; font-weight: bold; }
            table { border-collapse: collapse; width: 100%; margin: 15px 0; font-size: 11px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #4a7bc8; color: white; font-weight: bold; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            tr:hover { background-color: #f5f5f5; }
            .metric { display: inline-block; margin: 10px 20px; padding: 15px; background-color: #f0f8ff; border-left: 4px solid #4a7bc8; }
            .metric-label { font-size: 12px; color: #666; }
            .metric-value { font-size: 20px; font-weight: bold; color: #1f4788; }
            .section { margin-bottom: 40px; page-break-inside: avoid; }
            .chart-container { margin: 20px 0; text-align: center; width: 100%; }
            .chart-container > div { margin: 20px auto; }
            .dataframe { width: 100%; }
            ul { padding-left: 20px; }
            li { margin: 5px 0; }
            .tab-section { margin-top: 40px; border-top: 3px solid #4a7bc8; padding-top: 20px; }
        </style>
    </head>
    <body>
    ''')
    
    # 封面
    html_parts.append(f'''
        <div class="section">
            <h1>养老社区改良改造进度管理报告</h1>
            <div style="text-align: center; margin-top: 40px;">
                <p style="font-size: 14px;">生成时间：{datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}</p>
                <p style="font-size: 14px;">数据统计：共 {len(sub)} 个项目</p>
            </div>
        </div>
        <div style="page-break-after: always;"></div>
    ''')
    
    # 1. 审核流程说明
    html_parts.append('''
        <div class="section">
            <h2>一、需求审核与实施流程说明</h2>
            <ul>
                <li><strong>1. 社区提出：</strong>一线园区提出改造需求。</li>
                <li><strong>2. 紧急程度分级：</strong>按一级（最高级）、二级、三级划分。</li>
                <li><strong>3. 专业分类：</strong>按 9 大类专业划分：土建、供配电、暖通/供冷、弱电、供排水、电梯、其它、消防、安防等。</li>
                <li><strong>4. 财务预算拆分：</strong>按预算系统进行金额拆分与汇总。</li>
                <li><strong>5. 一线立项时间：</strong>一线填写需求并提出立项时间。</li>
                <li><strong>6. 项目部施工：</strong>项目部根据已确定的需求立项组织施工。</li>
                <li><strong>7. 总部运行保障部：</strong>督促一线需求稳定，协调总部相关部门把控需求，输出给不动产进行招采、施工。</li>
                <li><strong>8. 施工验收：</strong>总部运行保障部督促一线园区进行最终施工验收。</li>
            </ul>
        </div>
        <div style="page-break-after: always;"></div>
    ''')
    
    # 2. 项目统计分析 - 使用实际渲染函数生成内容
    html_parts.append('<div class="section"><h2>二、项目统计分析</h2>')
    
    # 2.1 项目数量与费用统计
    total_count = len(sub)
    total_amount = sub["拟定金额"].sum() if "拟定金额" in sub.columns else 0
    
    html_parts.append(f'''
        <h3>2.1 项目数量与费用统计</h3>
        <div class="metric">
            <div class="metric-label">项目总数</div>
            <div class="metric-value">{total_count:,}</div>
        </div>
        <div class="metric">
            <div class="metric-label">总金额（万元）</div>
            <div class="metric-value">{total_amount:,.0f}</div>
        </div>
    ''')
    
    # 2.2 按园区统计表格
    park_stats = sub.groupby("园区", dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    park_stats["金额合计"] = park_stats["金额合计"].round(2)
    
    html_parts.append('<h3>2.2 按园区统计</h3>')
    html_parts.append('<table><thead><tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr></thead><tbody>')
    for _, row in park_stats.iterrows():
        html_parts.append(f'<tr><td>{row["园区"]}</td><td>{int(row["项目数"])}</td><td>{row["金额合计"]:,.2f}</td></tr>')
    html_parts.append('</tbody></table>')
    
    # 2.3 项目分级占比统计
    if "项目分级" in sub.columns:
        html_parts.append('<h3>2.3 项目分级占比统计</h3>')
        level_mapping = {"一级": "一类", "二级": "二类", "三级": "三类"}
        sub_copy = sub.copy()
        sub_copy["项目类别"] = sub_copy["项目分级"].map(level_mapping).fillna(sub_copy["项目分级"])
        
        level_stats = sub_copy.groupby("项目类别", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        
        total_projects = level_stats["项目数"].sum()
        total_amount_level = level_stats["金额合计"].sum()
        
        if total_projects > 0:
            level_stats["项目数占比"] = (level_stats["项目数"] / total_projects * 100).round(2)
            level_stats["金额占比"] = (level_stats["金额合计"] / total_amount_level * 100).round(2) if total_amount_level > 0 else 0
            level_stats["金额合计"] = level_stats["金额合计"].round(2)
            
            html_parts.append('<table><thead><tr><th>项目类别</th><th>项目数</th><th>项目数占比(%)</th><th>金额合计（万元）</th><th>金额占比(%)</th></tr></thead><tbody>')
            for _, row in level_stats.iterrows():
                html_parts.append(f'<tr><td>{row["项目类别"]}</td><td>{int(row["项目数"])}</td><td>{row["项目数占比"]:.2f}</td><td>{row["金额合计"]:,.2f}</td><td>{row["金额占比"]:.2f}</td></tr>')
            html_parts.append('</tbody></table>')
            
            # 添加图表（使用plotly生成HTML）
            try:
                import plotly.express as px
                import plotly.graph_objects as go
                from plotly.utils import PlotlyJSONEncoder
                import json
                
                fig1 = px.pie(
                    level_stats, values="项目数", names="项目类别",
                    title="项目数量占比",
                    color_discrete_sequence=["#FF6B6B", "#4ECDC4", "#45B7D1"]
                )
                fig1.update_traces(textposition="outside", textinfo="label+percent+value")
                
                fig2 = px.pie(
                    level_stats, values="金额合计", names="项目类别",
                    title="项目金额占比",
                    color_discrete_sequence=["#FF6B6B", "#4ECDC4", "#45B7D1"]
                )
                fig2.update_traces(textposition="outside", textinfo="label+percent+value")
                
                # 将图表转换为HTML（使用内嵌plotlyjs，确保离线可用）
                # 第一个图表包含plotlyjs库，后续图表不需要重复包含
                chart1_html = fig1.to_html(include_plotlyjs=True, div_id="chart1")
                chart2_html = fig2.to_html(include_plotlyjs=False, div_id="chart2")
                
                html_parts.append('<div class="chart-container">')
                html_parts.append(chart1_html)
                html_parts.append(chart2_html)
                html_parts.append('</div>')
            except ImportError:
                pass
    
    html_parts.append('</div><div style="page-break-after: always;"></div>')
    
    # 3. 各园区分级分类统计
    html_parts.append('<div class="section"><h2>三、各园区分级分类统计</h2>')
    
    # 3.1 按分级统计
    by_level = sub.groupby("项目分级", dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    
    html_parts.append('<h3>3.1 按紧急程度（分级）统计</h3>')
    html_parts.append('<table><thead><tr><th>项目分级</th><th>项目数</th><th>金额合计（万元）</th></tr></thead><tbody>')
    for _, row in by_level.iterrows():
        level_name = str(row["项目分级"]) if pd.notna(row["项目分级"]) else "未分类"
        html_parts.append(f'<tr><td>{level_name}</td><td>{int(row["项目数"])}</td><td>{row["金额合计"]:,.2f}</td></tr>')
    html_parts.append('</tbody></table>')
    
    # 3.2 按专业分类统计
    by_prof = sub.groupby("专业", dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    # 过滤掉"其它系统"分类
    by_prof = by_prof[~by_prof["专业"].isin(["其它系统", "其他系统"])]
    
    html_parts.append('<h3>3.2 按专业分类统计</h3>')
    html_parts.append('<table><thead><tr><th>专业</th><th>项目数</th><th>金额合计（万元）</th></tr></thead><tbody>')
    for _, row in by_prof.iterrows():
        prof_name = str(row["专业"]) if pd.notna(row["专业"]) else "未分类"
        html_parts.append(f'<tr><td>{prof_name}</td><td>{int(row["项目数"])}</td><td>{row["金额合计"]:,.2f}</td></tr>')
    html_parts.append('</tbody></table>')
    
    html_parts.append('</div><div style="page-break-after: always;"></div>')
    
    # 4. 项目明细 - 显示所有数据
    html_parts.append('<div class="section"><h2>四、项目明细</h2>')
    html_parts.append(f'<p>共 {len(sub)} 条项目，以下显示所有项目明细：</p>')
    
    detail_cols = ["园区", "序号", "项目分级", "专业", "项目名称", "拟定金额"]
    detail_cols = [c for c in detail_cols if c in sub.columns]
    detail_df = sub[detail_cols]
    
    html_parts.append('<table><thead><tr>')
    for col in detail_cols:
        html_parts.append(f'<th>{col}</th>')
    html_parts.append('</tr></thead><tbody>')
    
    for _, row in detail_df.iterrows():
        html_parts.append('<tr>')
        for col in detail_cols:
            val = row[col]
            if pd.isna(val):
                html_parts.append('<td></td>')
            elif isinstance(val, (int, float)):
                if col == "拟定金额":
                    html_parts.append(f'<td>{val:,.2f}</td>')
                else:
                    html_parts.append(f'<td>{int(val)}</td>')
            else:
                html_parts.append(f'<td>{str(val)}</td>')
        html_parts.append('</tr>')
    
    html_parts.append('</tbody></table></div>')
    
    # ========== 标签页2: 统计 ==========
    html_parts.append('<div class="tab-section"><h2>📊 标签页2：统计</h2>')
    
    # 按专业统计图表
    try:
        import plotly.express as px
        by_prof_chart = sub_location.groupby("专业", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        # 过滤掉"其它系统"分类
        by_prof_chart = by_prof_chart[~by_prof_chart["专业"].isin(["其它系统", "其他系统"])]
        if not by_prof_chart.empty:
            fig = px.bar(by_prof_chart, x="专业", y="项目数", color="项目数", color_continuous_scale="Blues", text_auto=".0f")
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, height=400, xaxis_title="专业", yaxis_title="项目数")
            chart_html = fig.to_html(include_plotlyjs=not plotly_js_included, div_id="chart_prof")
            if not plotly_js_included:
                plotly_js_included = True
            html_parts.append('<h3>按专业 · 项目数</h3>')
            html_parts.append(f'<div class="chart-container">{chart_html}</div>')
    except:
        pass
    
    # 按项目分级金额占比
    try:
        by_level_chart = sub_location.groupby("项目分级", dropna=False).agg(金额合计=("拟定金额", "sum")).reset_index().sort_values("金额合计", ascending=False)
        if not by_level_chart.empty:
            fig = px.pie(by_level_chart, values="金额合计", names="项目分级", title="按项目分级 · 金额占比", hole=0.35)
            fig.update_traces(textposition="outside", textinfo="label+percent+value", texttemplate="%{label}<br>%{percent}<br>%{value:,.0f}万元")
            chart_html = fig.to_html(include_plotlyjs=False, div_id="chart_level")
            html_parts.append('<h3>按项目分级 · 金额占比</h3>')
            html_parts.append(f'<div class="chart-container">{chart_html}</div>')
    except:
        pass
    
    # 按园区金额统计
    try:
        by_park_chart = sub_location.groupby("园区", dropna=False).agg(金额合计=("拟定金额", "sum")).reset_index().sort_values("金额合计", ascending=False)
        if not by_park_chart.empty:
            by_park_chart["金额合计"] = by_park_chart["金额合计"].round(2)
            fig = px.bar(by_park_chart.head(20), x="园区", y="金额合计", color="金额合计", color_continuous_scale="Blues", text_auto=".0f")
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, height=400, xaxis_title="园区", yaxis_title="金额（万元）")
            chart_html = fig.to_html(include_plotlyjs=False, div_id="chart_park")
            html_parts.append('<h3>按园区 · 金额（万元）</h3>')
            html_parts.append(f'<div class="chart-container">{chart_html}</div>')
    except:
        pass
    
    html_parts.append('</div>')
    
    # ========== 标签页3: 地区分析 ==========
    if "所属区域" in sub_location.columns:
        html_parts.append('<div class="tab-section"><h2>🌍 标签页3：地区分析</h2>')
        sub_region = sub_location[sub_location["所属区域"] != "其他"].copy()
        
        if not sub_region.empty:
            region_summary = sub_region.groupby("所属区域", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
                园区数=("园区", "nunique"),
                城市数=("城市", "nunique"),
            ).reset_index().sort_values("项目数", ascending=False)
            region_summary["金额合计"] = region_summary["金额合计"].round(2)
            
            html_parts.append('<h3>按所属区域统计</h3>')
            html_parts.append('<table><thead><tr><th>所属区域</th><th>项目数</th><th>金额合计（万元）</th><th>园区数</th><th>城市数</th></tr></thead><tbody>')
            for _, row in region_summary.iterrows():
                html_parts.append(f'<tr><td>{row["所属区域"]}</td><td>{int(row["项目数"])}</td><td>{row["金额合计"]:,.2f}</td><td>{int(row["园区数"])}</td><td>{int(row["城市数"])}</td></tr>')
            html_parts.append('</tbody></table>')
            
            # 区域对比图表
            try:
                fig = px.bar(region_summary, x="所属区域", y="金额合计", color="金额合计", color_continuous_scale="Viridis", text_auto=".0f")
                fig.update_layout(xaxis_tickangle=-45, showlegend=False, height=400, xaxis_title="所属区域", yaxis_title="金额（万元）")
                chart_html = fig.to_html(include_plotlyjs=False, div_id="chart_region")
                html_parts.append('<h3>各区域金额对比</h3>')
                html_parts.append(f'<div class="chart-container">{chart_html}</div>')
            except:
                pass
        
        html_parts.append('</div>')
    
    # ========== 标签页4: 各园区分级分类 ==========
    html_parts.append('<div class="tab-section"><h2>📋 标签页4：各园区分级分类</h2>')
    
    # 按分级、专业、园区统计表格
    html_parts.append('<h3>按紧急程度（分级）统计</h3>')
    by_level_tab4 = sub.groupby("项目分级", dropna=False).agg(项目数=("序号", "count"), 金额合计=("拟定金额", "sum")).reset_index()
    html_parts.append('<table><thead><tr><th>项目分级</th><th>项目数</th><th>金额合计（万元）</th></tr></thead><tbody>')
    for _, row in by_level_tab4.iterrows():
        level_name = str(row["项目分级"]) if pd.notna(row["项目分级"]) else "未分类"
        html_parts.append(f'<tr><td>{level_name}</td><td>{int(row["项目数"])}</td><td>{row["金额合计"]:,.2f}</td></tr>')
    html_parts.append('</tbody></table>')
    
    html_parts.append('<h3>按专业分类统计</h3>')
    by_prof_tab4 = sub.groupby("专业", dropna=False).agg(项目数=("序号", "count"), 金额合计=("拟定金额", "sum")).reset_index()
    # 过滤掉"其它系统"分类
    by_prof_tab4 = by_prof_tab4[~by_prof_tab4["专业"].isin(["其它系统", "其他系统"])]
    html_parts.append('<table><thead><tr><th>专业</th><th>项目数</th><th>金额合计（万元）</th></tr></thead><tbody>')
    for _, row in by_prof_tab4.iterrows():
        prof_name = str(row["专业"]) if pd.notna(row["专业"]) else "未分类"
        html_parts.append(f'<tr><td>{prof_name}</td><td>{int(row["项目数"])}</td><td>{row["金额合计"]:,.2f}</td></tr>')
    html_parts.append('</tbody></table>')
    
    # 按专业分包统计（如果存在该列）
    if "专业分包" in sub.columns or "专业细分" in sub.columns:
        prof_subcontract_col = "专业分包" if "专业分包" in sub.columns else "专业细分"
        html_parts.append('<h3>按专业分包统计</h3>')
        by_prof_subcontract_tab4 = sub.groupby(prof_subcontract_col, dropna=False).agg(项目数=("序号", "count"), 金额合计=("拟定金额", "sum")).reset_index().sort_values("金额合计", ascending=False)
        html_parts.append('<table><thead><tr><th>专业分包</th><th>项目数</th><th>金额合计（万元）</th></tr></thead><tbody>')
        for _, row in by_prof_subcontract_tab4.iterrows():
            subcontract_name = str(row[prof_subcontract_col]) if pd.notna(row[prof_subcontract_col]) else "未分类"
            html_parts.append(f'<tr><td>{subcontract_name}</td><td>{int(row["项目数"])}</td><td>{row["金额合计"]:,.2f}</td></tr>')
    html_parts.append('</tbody></table>')
    
    html_parts.append('<h3>按园区统计</h3>')
    by_park_tab4 = sub.groupby("园区", dropna=False).agg(项目数=("序号", "count"), 金额合计=("拟定金额", "sum")).reset_index()
    html_parts.append('<table><thead><tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr></thead><tbody>')
    for _, row in by_park_tab4.iterrows():
        html_parts.append(f'<tr><td>{row["园区"]}</td><td>{int(row["项目数"])}</td><td>{row["金额合计"]:,.2f}</td></tr>')
    html_parts.append('</tbody></table>')
    
    html_parts.append('</div>')
    
    # ========== 标签页5: 总部视图 ==========
    html_parts.append('<div class="tab-section"><h2>🏢 标签页5：总部视图</h2>')
    
    try:
        from data_loader import get_稳定需求_mask
        stable_mask = get_稳定需求_mask(sub)
        stable = sub[stable_mask]
        
        html_parts.append('<h3>各园区已确定稳定需求数量与金额</h3>')
        summary = stable.groupby("园区", dropna=False).agg(稳定需求数量=("序号", "count"), 稳定需求金额=("拟定金额", "sum")).reset_index()
        html_parts.append('<table><thead><tr><th>园区</th><th>稳定需求数量</th><th>稳定需求金额（万元）</th></tr></thead><tbody>')
        for _, row in summary.iterrows():
            html_parts.append(f'<tr><td>{row["园区"]}</td><td>{int(row["稳定需求数量"])}</td><td>{row["稳定需求金额"]:,.2f}</td></tr>')
        html_parts.append('</tbody></table>')
    except:
        html_parts.append('<p>稳定需求数据暂不可用</p>')
    
    html_parts.append('</div>')
    
    # ========== 标签页6: 全部项目 ==========
    html_parts.append('<div class="tab-section"><h2>📑 标签页6：全部项目清单</h2>')
    html_parts.append(f'<p>共 {len(df)} 条项目，以下列出所有项目明细：</p>')
    
    # 显示所有列
    display_cols = ["园区", "所属区域", "城市"] + [c for c in df.columns if c not in ["园区", "所属区域", "城市"]]
    display_cols = [c for c in display_cols if c in df.columns]
    
    html_parts.append('<table style="font-size: 10px;"><thead><tr>')
    for col in display_cols:
        html_parts.append(f'<th>{col}</th>')
    html_parts.append('</tr></thead><tbody>')
    
    for _, row in df.iterrows():
        html_parts.append('<tr>')
        for col in display_cols:
            val = row[col]
            if pd.isna(val):
                html_parts.append('<td></td>')
            elif isinstance(val, (int, float)):
                if "金额" in str(col) or "金额" in str(col):
                    html_parts.append(f'<td>{val:,.2f}</td>')
                else:
                    html_parts.append(f'<td>{int(val)}</td>')
            else:
                html_parts.append(f'<td>{str(val)[:50]}</td>')  # 限制长度避免表格过宽
        html_parts.append('</tr>')
    
    html_parts.append('</tbody></table></div>')
    
    # HTML尾部
    html_parts.append('''
    </body>
    </html>
    ''')
    
    return ''.join(html_parts)


def generate_pdf_report(df: pd.DataFrame, 园区选择: list, output_path: str = None):
    """生成PDF报告，包含所有页面内容（使用HTML转PDF方式）"""
    return generate_pdf_report_html(df, 园区选择, output_path)


def render_地图与统计(df: pd.DataFrame, 园区选择: list):
    """地图与统计 Tab：中国地图 + 按专业/分级/园区/区域图表。"""
    df_with_location = _add_城市和区域列(df)
    # 处理园区选择：如果为空或None，显示所有有园区信息的数据
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df_with_location[df_with_location["园区"].isin(valid_parks)]
        else:
            sub = df_with_location[df_with_location["园区"].notna()]
    else:
        sub = df_with_location[df_with_location["园区"].notna()]  # 只显示有园区信息的行

    st.subheader("中国地图 · 各地市项目分布")
    # 为地图构造城市-园区明细，用于 tooltip 展示
    city_tooltip_data = _build_城市_园区明细(sub)
    _render_中国地图(sub, city_tooltip_data)
    
    st.markdown("---")
    st.subheader("数据统计")
    st.markdown("### 📊 按区域统计分析")
    
    # 区域统计表格
    if "所属区域" in sub.columns:
        st.markdown("#### 各区域项目统计")
        by_region = sub.groupby("所属区域", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
            园区数=("园区", "nunique"),
        ).reset_index()
        by_region = by_region[by_region["所属区域"] != "其他"].sort_values("项目数", ascending=False)
        by_region["金额合计"] = by_region["金额合计"].round(2)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("总区域数", len(by_region))
        with col2:
            st.metric("总项目数", int(by_region["项目数"].sum()))
        with col3:
            st.metric("总金额（万元）", f"{by_region['金额合计'].sum():,.0f}")
        with col4:
            st.metric("总园区数", int(by_region["园区数"].sum()))
        
        st.dataframe(by_region, use_container_width=True, hide_index=True)
        
        # 区域下各园区明细
        st.markdown("#### 各区域下园区明细")
        for region in by_region["所属区域"].unique():
            region_df = sub[sub["所属区域"] == region]
            parks_in_region = region_df.groupby("园区", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index().sort_values("项目数", ascending=False)
            parks_in_region["金额合计"] = parks_in_region["金额合计"].round(2)
            
            with st.expander(f"📌 {region}（{len(parks_in_region)}个园区，{int(parks_in_region['项目数'].sum())}个项目，{parks_in_region['金额合计'].sum():,.0f}万元）"):
                st.dataframe(parks_in_region, use_container_width=True, hide_index=True)
        
        st.markdown("---")
    
    st.markdown("### 图表统计")

    try:
        import plotly.express as px
    except ImportError:
        st.warning("请安装 plotly 以使用美化图表：pip install plotly")
        _render_图表_简易(sub)
        return

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**按专业 · 项目数**")
        by_prof = sub.groupby("专业", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        # 过滤掉"其它系统"分类
        by_prof = by_prof[~by_prof["专业"].isin(["其它系统", "其他系统"])]
        if not by_prof.empty:
            fig = px.bar(
                by_prof, x="专业", y="项目数", color="项目数",
                color_continuous_scale="Blues", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="项目数")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c2:
        st.markdown("**按项目分级 · 金额占比**")
        by_level = sub.groupby("项目分级", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum")
        ).reset_index().sort_values("金额合计", ascending=False)
        if not by_level.empty:
            colors = (CHART_COLORS_PIE * (1 + len(by_level) // len(CHART_COLORS_PIE)))[: len(by_level)]
            fig = px.pie(
                by_level, values="金额合计", names="项目分级", title="",
                color_discrete_sequence=colors, hole=0.35,
            )
            fig.update_traces(
                textposition="outside",
                textinfo="label+percent+value",
                texttemplate="%{label}<br>%{percent}<br>%{value:,.0f}万元",
                textfont_size=12,
                pull=[0.02] * len(by_level),
            )
            fig.update_layout(
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.2),
                margin=dict(t=20, b=60, l=20, r=20),
                height=380,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("**按园区 · 金额（万元）**")
        by_park = sub.groupby("园区", dropna=False).agg(金额合计=("拟定金额", "sum")).reset_index().sort_values("金额合计", ascending=False)
        if not by_park.empty:
            by_park["金额合计"] = by_park["金额合计"].round(2)
            fig = px.bar(
                by_park, x="园区", y="金额合计", color="金额合计",
                color_continuous_scale="Blues", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="金额（万元）")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c4:
        st.markdown("**按城市 · 金额（万元）**")
        by_city = sub.groupby("城市", dropna=False).agg(金额合计=("拟定金额", "sum")).reset_index()
        by_city = by_city[by_city["城市"] != "其他"].sort_values("金额合计", ascending=False)
        if not by_city.empty:
            by_city["金额合计"] = by_city["金额合计"].round(2)
            fig = px.bar(
                by_city, x="城市", y="金额合计", color="金额合计",
                color_continuous_scale="Teal", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="金额（万元）")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    
    # 添加按区域统计的图表（已合并到区域对比分析中，此处删除重复图表）
        
        # 区域饼图
        st.markdown("**按所属区域 · 金额分布（万元）**")
        by_region_pie = sub.groupby("所属区域", dropna=False).agg(金额合计=("拟定金额", "sum")).reset_index()
        by_region_pie = by_region_pie[by_region_pie["所属区域"] != "其他"].sort_values("金额合计", ascending=False)
        by_region_pie["金额合计"] = by_region_pie["金额合计"].round(2)
        if not by_region_pie.empty:
            colors_region = ["#FF6B6B", "#4ECDC4", "#45B7D1", "#FFA07A", "#98D8C8"]
            fig = px.pie(
                by_region_pie, values="金额合计", names="所属区域", title="",
                color_discrete_sequence=colors_region, hole=0.4,
            )
            fig.update_traces(
                textposition="outside",
                textinfo="label+percent+value",
                texttemplate="%{label}<br>%{percent}<br>%{value:,.0f}万元",
                textfont_size=12,
                pull=[0.05] * len(by_region_pie),
            )
            fig.update_layout(
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.15),
                margin=dict(t=20, b=80, l=20, r=20),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("**按专业 · 金额合计（万元）**")
    by_prof_m = sub.groupby("专业", dropna=False).agg(金额=("拟定金额", "sum")).reset_index().sort_values("金额", ascending=False)
    # 过滤掉"其它系统"分类
    by_prof_m = by_prof_m[~by_prof_m["专业"].isin(["其它系统", "其他系统"])]
    if not by_prof_m.empty:
        fig = px.bar(
            by_prof_m, x="专业", y="金额", color="金额",
            color_continuous_scale="Viridis", text_auto=".0f",
        )
        fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=360, xaxis_title="", yaxis_title="金额（万元）")
        fig.update_traces(textfont_size=10)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def main():
    st.title("养老社区改良改造进度管理看板")
    st.caption("需求审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部协调招采/施工 → 督促验收")

    # 侧边栏：数据源
    with st.sidebar:
        st.header("数据源")
        source = st.radio("数据来源", ["单文件", "目录下全部 CSV"], index=0)
        df = pd.DataFrame()
        manual_df = pd.DataFrame()
        if source == "单文件":
            update_backend = st.checkbox(
                "上传CSV后同时更新后台默认数据文件",
                value=False,
                help="勾选后，上传的CSV会覆盖当前目录下的默认数据文件（改良改造报表-V4.csv），用于后续所有访问。",
            )
            uploaded = st.file_uploader(
                "上传 CSV 或 Excel 文件",
                type=["csv", "xlsx", "xls"],
                help="支持 .csv 或 .xlsx。xlsx 会按分表自动识别进度表并合并（表头两行、含序号/项目分级/专业/拟定金额）。",
            )
            if uploaded is not None:
                import tempfile
                suffix = Path(uploaded.name).suffix.lower() or ".csv"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(uploaded.getvalue())
                    tmp_path = tmp.name
                try:
                    name = uploaded.name
                    园区名 = "燕园" if "燕园" in name else ("蜀园" if "蜀园" in name else None)
                    df = load_uploaded(tmp_path, filename=name, 园区名=园区名)
                    if df.empty:
                        st.warning("文件已解析但未找到有效数据行。请确认：表头为两行，且含「序号」「项目分级」「专业」「拟定金额」等列。")
                    else:
                        st.success(f"已加载：{name}，共 {len(df)} 条记录")
                        # 可选：将上传的 CSV 持久化为默认数据源，便于下次直接使用（需勾选开关）
                        if suffix == ".csv" and update_backend:
                            try:
                                target_path = Path(DEFAULT_SINGLE_FILE)
                                target_path.write_bytes(uploaded.getvalue())
                                st.info(f"已将上传的CSV保存为默认数据源：{target_path.name}，后端数据已更新。")
                            except Exception as e_save:
                                st.warning(f"上传文件已加载，但保存到服务器失败：{e_save}")
                        elif suffix != ".csv":
                            st.info("当前上传的是 Excel 文件，仅在本次会话中使用，未覆盖默认CSV文件。")
                except Exception as e:
                    st.error(f"解析失败：{e}")
                    import traceback
                    st.code(traceback.format_exc(), language=None)
            if df.empty:
                single_path = st.text_input("或填写本地文件路径（.csv / .xlsx）", value=DEFAULT_SINGLE_FILE)
                if single_path and Path(single_path).exists():
                    try:
                        df = load_uploaded(single_path, filename=Path(single_path).name)
                        st.success(f"已从路径加载，共 {len(df)} 条记录")
                    except Exception as e:
                        st.error(f"加载失败：{e}")
                else:
                    st.info("请在上方上传 CSV/Excel，或填写有效的本地文件路径。")
        else:
            dir_path = st.text_input("数据目录路径", value=DEFAULT_DATA_DIR)
            pattern = st.text_input("文件名匹配", value="*养老*进度*.csv")
            if dir_path and Path(dir_path).is_dir():
                try:
                    df = load_from_directory(dir_path, pattern)
                except Exception as e:
                    st.error(f"加载失败：{e}")
            else:
                st.warning("请填写有效目录路径")

        # 侧边栏：手动录入数据（按固定字段顺序，无需列名）
        st.markdown("----")
        manual_help = (
            "一行一条记录，用英文逗号分隔字段，不需要列名。\n\n"
            "字段顺序严格为：\n"
            "1) 社区（园区，例如：燕园）\n"
            "2) 项目名称（例如：燕园1号楼电梯更换）\n"
            "3) 项目分级（例如：一级/二级/三级）\n"
            "4) 专业（例如：电梯系统/供配电系统等）\n"
            "5) 拟定金额（万元，数字，如：120 或 120.5）\n"
            "6) 拟定承建组织（可留空）\n"
            "7) 需求立项日期（可留空，如：2025-01-15）\n"
            "8) 验收日期（可留空，如：2025-03-20）\n\n"
            "示例：\n"
            "燕园, 燕园1号楼电梯更换, 一级, 电梯系统, 120, XX工程公司, 2025-01-15, 2025-03-20\n"
            "蜀园, 蜀园供配电系统改造, 二级, 供配电系统, 80, , 2025-02-01, \n\n"
            "说明：\n"
            "- 序号字段系统会自动生成，无需填写；\n"
            "- 所属区域和城市将根据社区名称自动识别；\n"
            "- 多条记录请分别写在多行。"
        )
        manual_text = st.text_area(
            "手动输入数据（按固定字段顺序）",
            value=st.session_state.get("manual_text", ""),
            help=manual_help,
        )
        st.session_state["manual_text"] = manual_text
        if manual_text.strip():
            try:
                lines = [ln.strip() for ln in manual_text.strip().splitlines() if ln.strip()]
                rows = []
                # 计算当前数据中已有的最大序号，用于续编号
                current_max_seq = 0
                if not df.empty and "序号" in df.columns:
                    try:
                        current_max_seq = int(pd.to_numeric(df["序号"], errors="coerce").max() or 0)
                    except Exception:
                        current_max_seq = 0
                seq = current_max_seq
                value_cols = ["园区", "项目名称", "项目分级", "专业", "拟定金额", "拟定承建组织", "需求立项", "验收(社区需求完成交付)"]
                for line in lines:
                    # 支持中英文逗号
                    parts = [p.strip() for p in line.replace("，", ",").split(",")]
                    if not any(parts):
                        continue
                    # 补齐或截断到固定列数
                    if len(parts) < len(value_cols):
                        parts += [""] * (len(value_cols) - len(parts))
                    else:
                        parts = parts[: len(value_cols)]
                    seq += 1
                    row = {"序号": seq}
                    for col, val in zip(value_cols, parts):
                        if col == "拟定金额":
                            try:
                                row[col] = float(val) if val not in ("", None) else 0
                            except Exception:
                                row[col] = 0
                        else:
                            row[col] = val
                    rows.append(row)
                if rows:
                    manual_df = pd.DataFrame(rows)
                    st.success(f"已从手动输入加载 {len(manual_df)} 条记录（序号已自动生成），这些记录会参与下方所有统计。")
                else:
                    manual_df = pd.DataFrame()
            except Exception as e:
                manual_df = pd.DataFrame()
                st.error("手动输入数据解析失败，请检查每行是否为 8 个用逗号分隔的字段，且至少包含社区和项目名称。")

        # 侧边栏：自定义数据输入框（文本）
        st.markdown("---")
        custom_note = st.text_area(
            "补充说明 / 备注（可选）",
            value=st.session_state.get("custom_note", ""),
            help="可在此输入本次数据的备注说明、使用范围等信息，仅作为展示，不参与计算。",
        )
        st.session_state["custom_note"] = custom_note

        # 合并文件/目录数据与手动输入数据
        if not manual_df.empty:
            if not df.empty:
                df = pd.concat([df, manual_df], ignore_index=True)
            else:
                df = manual_df

        if not df.empty:
            # 过滤掉 None 和空值，但保留其他值
            parks = df["园区"].dropna().unique().tolist()
            # 过滤掉"未知园区"和无效值
            parks = [p for p in parks if p and str(p).strip() and str(p) != "未知园区"]
            if parks:
                园区选择 = st.multiselect("筛选园区", options=parks, default=parks)
            else:
                st.warning("数据中未找到有效的园区信息，请检查数据文件。")
                园区选择 = []
        else:
            园区选择 = []

    if df.empty:
        st.warning("请先在侧边栏选择或上传数据源。")
        render_审核流程说明()
        return

    # 自动添加城市和区域列
    df = _add_城市和区域列(df)

    # 导出按钮
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("📄 导出PDF报告", type="primary", use_container_width=True):
            try:
                with st.spinner("正在生成PDF报告，请稍候..."):
                    pdf_path = generate_pdf_report(df, 园区选择)
                    with open(pdf_path, "rb") as pdf_file:
                        pdf_bytes = pdf_file.read()
                    st.success("PDF报告生成成功！")
                    st.download_button(
                        label="⬇️ 下载PDF报告",
                        data=pdf_bytes,
                        file_name=f"养老社区改良改造进度管理报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
            except ImportError as e:
                st.error(f"PDF导出功能需要安装依赖库：{e}")
                st.info("请运行：pip install weasyprint 或 pip install playwright && playwright install chromium")
            except Exception as e:
                st.error(f"生成PDF报告失败：{e}")
                import traceback
                st.code(traceback.format_exc(), language=None)
    with col2:
        if st.button("💾 保存HTML文件", use_container_width=True):
            try:
                with st.spinner("正在生成HTML文件，请稍候..."):
                    # 处理园区选择
                    if 园区选择 and len(园区选择) > 0:
                        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
                        if valid_parks:
                            sub = df[df["园区"].isin(valid_parks)]
                        else:
                            sub = df[df["园区"].notna()]
                    else:
                        sub = df[df["园区"].notna()]
                    
                    # 过滤汇总行
                    if "序号" in sub.columns:
                        sub = sub[sub["序号"].notna()]
                        sub = sub[~sub["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
                        sub = sub[pd.to_numeric(sub["序号"], errors='coerce').notna()]
                    
                    # 添加城市和区域列
                    df_with_location = _add_城市和区域列(df)
                    if 园区选择 and len(园区选择) > 0:
                        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
                        if valid_parks:
                            sub_location = df_with_location[df_with_location["园区"].isin(valid_parks)]
                        else:
                            sub_location = df_with_location[df_with_location["园区"].notna()]
                    else:
                        sub_location = df_with_location[df_with_location["园区"].notna()]
                    
                    if "序号" in sub_location.columns:
                        sub_location = sub_location[sub_location["序号"].notna()]
                        sub_location = sub_location[~sub_location["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
                        sub_location = sub_location[pd.to_numeric(sub_location["序号"], errors='coerce').notna()]
                    
                    # 生成HTML内容
                    html_content = generate_html_report(df, sub, sub_location, 园区选择)
                    
                    st.success("HTML文件生成成功！")
                    st.download_button(
                        label="⬇️ 下载HTML文件",
                        data=html_content.encode('utf-8'),
                        file_name=f"养老社区改良改造进度管理报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                        mime="text/html",
                        use_container_width=True
                    )
                    st.info("💡 提示：HTML文件包含所有图表和数据，可以在任何设备上离线打开查看。")
            except Exception as e:
                st.error(f"生成HTML文件失败：{e}")
                import traceback
                st.code(traceback.format_exc(), language=None)
    
    render_审核流程说明()

    tab1, tab2, tab3 = st.tabs(["项目统计分析", "地图与统计", "全部项目"])
    with tab1:
        render_项目统计分析(df, 园区选择)
    with tab2:
        render_地图与统计(df, 园区选择)
    with tab3:
        st.subheader("全部项目清单")
        st.caption(f"共 {len(df)} 条项目，以下列出所有项目明细。")
        # 显示时包含城市和区域列
        display_cols = ["园区", "所属区域", "城市"] + [c for c in df.columns if c not in ["园区", "所属区域", "城市"]]
        display_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[display_cols], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
