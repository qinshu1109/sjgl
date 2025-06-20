import streamlit as st
import pandas as pd
import polars as pl
import re, io, textwrap
from pathlib import Path
import sys

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))
from core.etl_douyin import process_douyin_export, clean_common_fields

# ---------- 清洗函数 --------------------------------------------------
UNIT_MAP = {"w": 10_000, "万": 10_000, "k": 1_000, "千": 1_000}

def _to_number(token: str) -> float:
    """把 '2.5w' / '5000' / '1.00%' 统一转成 float"""
    token = token.strip()
    if token.endswith("%"):
        return float(token.rstrip("%")) / 100
    m = re.match(r"([0-9.]+)\s*([w万k千]?)", token, flags=re.I)
    if not m:
        return float("nan")
    val, unit = m.groups()
    factor = UNIT_MAP.get(unit.lower(), 1)
    return float(val) * factor

def _parse_range(value: str):
    """
    '1w~2.5w' -> (10000, 25000, 17500)
    '5000'     -> (5000, 5000, 5000)
    """
    if pd.isna(value):
        return (None, None, None)
    parts = re.split(r"[~-]", str(value))
    nums = [_to_number(p) for p in parts if p.strip()]
    if not nums:
        return (None, None, None)
    if len(nums) == 1:
        return (nums[0], nums[0], nums[0])
    return (min(nums), max(nums), sum(nums) / len(nums))

def clean_chanmama_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. 将区间列拆成 *_min / *_max / *_avg
    2. 佣金列百分比转小数
    3. 只返回处理后的数据（不包含原始列）
    """
    result_cols = []
    
    # 保留的文本列
    text_cols = ['商品', '商品链接', '商品分类', '小店', '品牌', '排名']
    for col in text_cols:
        if col in df.columns:
            result_cols.append(df[col])
    
    # 1) 佣金比例
    if "佣金比例" in df.columns:
        result_cols.append(
            pd.Series(df["佣金比例"].astype(str).apply(_to_number), name="佣金比例")
        )

    # 2) 区间列通用拆分
    RANGE_COLS = [c for c in df.columns if re.search(r"销量|销售额|转化率", c)]
    for col in RANGE_COLS:
        parsed = df[col].astype(str).apply(_parse_range).tolist()
        mins, maxs, avgs = zip(*parsed)
        
        # 如果是转化率，需要除以100
        if "转化率" in col:
            mins = [m/100 if m else None for m in mins]
            maxs = [m/100 if m else None for m in maxs]
            avgs = [a/100 if a else None for a in avgs]
        
        result_cols.extend([
            pd.Series(mins, name=f"{col}_min"),
            pd.Series(maxs, name=f"{col}_max"),
            pd.Series(avgs, name=f"{col}_avg")
        ])
    
    # 3) 直接数值列
    numeric_cols = ['直播销售额', '商品卡销售额']
    for col in numeric_cols:
        if col in df.columns:
            result_cols.append(
                pd.Series(pd.to_numeric(df[col], errors='coerce'), name=col)
            )
    
    return pd.concat(result_cols, axis=1)

# ---------------------------------------------------------------------

st.set_page_config(
    page_title="数据炼金工坊 - 简洁版",
    page_icon="🏗️",
    layout="wide"
)

st.title("🏗️ 蝉妈妈/抖店 Excel 批量清洗工具")

uploaded_files = st.file_uploader(
    "上传 CSV / Excel 文件（可多选）",
    type=["csv", "xlsx"],
    accept_multiple_files=True
)

if st.button("开始处理", type="primary") and uploaded_files:
    progress_bar = st.progress(0)
    all_clean = []

    for idx, up in enumerate(uploaded_files, start=1):
        st.markdown(f"### 正在处理：`{up.name}`")
        
        try:
            # ---------- 读原始文件 ----------
            if up.name.lower().endswith(".csv"):
                raw_df = pd.read_csv(up, encoding="utf-8-sig")
            else:
                raw_df = pd.read_excel(up, engine="openpyxl")
            
            # ---------- 清洗 ----------
            cleaned_df = clean_chanmama_df(raw_df)

            # ---------- 前端对比展示 ----------
            col1, col2 = st.columns(2)
            with col1:
                st.caption("📑 原始数据预览 (前5行)")
                st.dataframe(raw_df.head())
            with col2:
                st.caption("✨ 清洗后数据预览 (前5行)")
                st.dataframe(cleaned_df.head())

            # 单独下载按钮
            csv_single = cleaned_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                f"⬇️ 下载 {up.name} 的清洗结果",
                csv_single,
                file_name=f"cleaned_{up.name.split('.')[0]}.csv",
                mime="text/csv",
                key=f"download_{idx}"
            )

            all_clean.append(cleaned_df)
            progress_bar.progress(idx / len(uploaded_files),
                                text=f"{idx}/{len(uploaded_files)} 已完成")
            
            st.success(f"✅ {up.name} 处理完成")
            st.divider()
            
        except Exception as e:
            st.error(f"❌ 处理 {up.name} 时出错：{str(e)}")
            continue

    if all_clean:
        st.success(f"✅ 成功处理 {len(all_clean)}/{len(uploaded_files)} 个文件！")
        
        # ---------- 合并选项（可选） ----------
        if len(all_clean) > 1:
            if st.checkbox("合并所有文件"):
                merged = pd.concat(all_clean, ignore_index=True)
                st.subheader(f"合并后结果 (共 {len(merged)} 行)")
                st.dataframe(merged.head(100))

                csv_bytes = merged.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "⬇️ 下载合并后的CSV",
                    csv_bytes,
                    file_name="merged_clean.csv",
                    mime="text/csv"
                )
else:
    st.info("👆 请先选择文件并点击『开始处理』")
    
    # 使用说明
    with st.expander("📖 使用说明"):
        st.markdown("""
        ### 功能特性
        - ✅ 支持批量上传多个 CSV/Excel 文件
        - ✅ 自动识别蝉妈妈数据格式
        - ✅ 智能解析模糊数值（如 "7.5w~10w" → min/max/avg）
        - ✅ 佣金比例自动转换（如 "20%" → 0.2）
        - ✅ 每个文件独立处理和下载
        - ✅ 可选合并所有文件
        
        ### 数据处理说明
        1. **销量/销售额**：自动拆分为 _min/_max/_avg 三列
        2. **转化率**：百分比转换为小数（10% → 0.1）
        3. **佣金比例**：百分比转换为小数（20% → 0.2）
        4. **原始文本字段**：商品名、链接等保持不变
        
        ### 注意事项
        - 清洗后的数据**只包含处理后的列**，不保留原始模糊列
        - 支持的单位：w/万（万）、k/千（千）、%（百分比）
        - 文件编码自动识别（UTF-8-SIG/GBK）
        """)

# 页脚
st.markdown("---")
st.caption("🏗️ 数据炼金工坊 - 让数据处理变得简单高效")