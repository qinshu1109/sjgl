"""
🎨 数据炼金工坊 - Streamlit Web界面
极简优雅的抖音电商数据处理界面

作者: 界面设计师
设计理念: 用户不需要知道什么是Polars和DuckDB，他们只需要一个上传按钮和一个下载按钮
"""

import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import tempfile
import time
from datetime import datetime
import io
import traceback
import os
import zipfile

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from core.etl_douyin import process_douyin_export, get_data_quality_report

# 页面配置
st.set_page_config(
    page_title="数据炼金工坊",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

def init_page_style():
    """初始化页面样式"""
    st.markdown("""
    <style>
    /* 主题色彩 */
    :root {
        --primary-color: #FF6B6B;
        --secondary-color: #4ECDC4;
        --accent-color: #45B7D1;
        --success-color: #96CEB4;
        --warning-color: #FFEAA7;
        --error-color: #DDA0DD;
    }
    
    /* 隐藏默认菜单 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* 自定义标题样式 */
    .main-title {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1rem;
    }
    
    .subtitle {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
        font-style: italic;
    }
    
    /* 卡片样式 */
    .upload-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        color: white;
    }
    
    .result-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 2rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        color: white;
    }
    
    .feature-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid var(--primary-color);
        margin: 0.5rem 0;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    
    /* 进度条样式 */
    .stProgress > div > div > div > div {
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
    }
    
    /* 按钮样式 */
    .stDownloadButton > button {
        background: linear-gradient(45deg, #4ECDC4, #45B7D1);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 0.5rem 2rem;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .stDownloadButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    </style>
    """, unsafe_allow_html=True)

def render_header():
    """渲染页面头部"""
    st.markdown('<h1 class="main-title">🏗️ 数据炼金工坊</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">"将原始数据炼成纯净的数据黄金"</p>', unsafe_allow_html=True)
    
    # 功能介绍卡片
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="feature-card">
            <h4>🚀 智能解析</h4>
            <p>自动识别蝉妈妈多表CSV结构</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="feature-card">
            <h4>🔧 模糊处理</h4>
            <p>将"7.5w~10w"转换为精确数值</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="feature-card">
            <h4>⚡ 极速性能</h4>
            <p>基于Polars的极致性能优化</p>
        </div>
        """, unsafe_allow_html=True)

def render_file_upload():
    """渲染文件上传区域 - 支持批量上传"""
    st.markdown("""
    <div class="upload-card">
        <h3>📁 上传您的数据文件</h3>
        <p>支持蝉妈妈导出的CSV/Excel文件，可批量处理多个文件</p>
    </div>
    """, unsafe_allow_html=True)
    
    # 选择上传模式
    upload_mode = st.radio(
        "选择上传模式",
        ["单文件上传", "批量上传"],
        horizontal=True,
        help="单文件：处理一个文件；批量：同时处理多个文件并合并结果"
    )
    
    if upload_mode == "单文件上传":
        uploaded_files = st.file_uploader(
            "选择文件",
            type=['csv', 'xlsx', 'xls'],
            help="上传蝉妈妈导出的数据文件，系统将自动识别并处理多表数据",
            label_visibility="collapsed"
        )
        return [uploaded_files] if uploaded_files else []
    else:
        uploaded_files = st.file_uploader(
            "选择多个文件",
            type=['csv', 'xlsx', 'xls'],
            accept_multiple_files=True,
            help="一次上传多个文件，系统将批量处理并合并结果",
            label_visibility="collapsed"
        )
        return uploaded_files if uploaded_files else []

def render_processing_animation():
    """渲染处理动画"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    steps = [
        "🔍 智能识别表格结构...",
        "📊 解析多表数据...",
        "🧹 清洗模糊数值范围...", 
        "⚙️ 标准化数据格式...",
        "🎯 生成数据质量报告...",
        "✨ 数据炼金完成！"
    ]
    
    for i, step in enumerate(steps):
        progress_bar.progress((i + 1) / len(steps))
        status_text.text(step)
        time.sleep(0.5)
    
    status_text.text("🎉 处理完成！")
    return progress_bar, status_text

def render_data_overview(df: pl.DataFrame, quality_report: dict):
    """渲染数据概览"""
    st.markdown("""
    <div class="result-card">
        <h3>📊 数据处理结果</h3>
        <p>您的数据已成功炼制完成，可以开始分析了！</p>
    </div>
    """, unsafe_allow_html=True)
    
    # 基础统计信息
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("总行数", f"{quality_report['total_rows']:,}", delta=None)
    
    with col2:
        st.metric("总列数", f"{quality_report['total_columns']}", delta=None)
    
    with col3:
        # 计算数值列数量
        numeric_cols = len([col for col in df.columns if df[col].dtype in [pl.Float64, pl.Int64]])
        st.metric("数值列", f"{numeric_cols}", delta=None)
    
    with col4:
        # 计算生成的范围列数量
        range_cols = len([col for col in df.columns if '_avg' in col])
        st.metric("生成范围列", f"{range_cols}", delta=None)

def render_data_quality_chart(quality_report: dict):
    """渲染数据质量图表"""
    st.subheader("📈 数据质量分析")
    
    # 空值分析
    null_data = quality_report['null_counts']
    if any(count > 0 for count in null_data.values()):
        null_df = pd.DataFrame([
            {"列名": col, "空值数量": count, "空值比例": count / quality_report['total_rows']}
            for col, count in null_data.items()
            if count > 0
        ])
        
        fig = px.bar(
            null_df, 
            x="列名", 
            y="空值数量",
            title="空值分布情况",
            color="空值比例",
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.success("🎉 数据质量优秀！没有发现空值。")
    
    # 数据类型分析
    type_data = quality_report['data_types']
    type_counts = {}
    for dtype in type_data.values():
        type_counts[dtype] = type_counts.get(dtype, 0) + 1
    
    fig_pie = px.pie(
        values=list(type_counts.values()),
        names=list(type_counts.keys()),
        title="数据类型分布"
    )
    st.plotly_chart(fig_pie, use_container_width=True)

def render_data_preview(df: pl.DataFrame):
    """渲染数据预览"""
    st.subheader("👀 数据预览")
    
    # 转换为pandas以便在streamlit中显示
    df_pandas = df.to_pandas()
    
    # 显示前几行
    st.write("**前5行数据:**")
    st.dataframe(df_pandas.head(), use_container_width=True)
    
    # 如果有范围列，展示转换效果
    range_columns = [col for col in df.columns if col.endswith('_avg')]
    if range_columns:
        st.write("**模糊数值范围处理效果:**")
        
        # 找到原始列和对应的范围列
        for avg_col in range_columns[:3]:  # 只显示前3个
            base_col = avg_col.replace('_avg', '')
            if base_col in df.columns:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write(f"**原始**: {base_col}")
                    st.write(df_pandas[base_col].iloc[0] if len(df_pandas) > 0 else "无数据")
                with col2:
                    st.write(f"**最小值**: {avg_col.replace('_avg', '_min')}")
                    min_col = avg_col.replace('_avg', '_min')
                    if min_col in df.columns:
                        st.write(f"{df_pandas[min_col].iloc[0]:,.0f}" if len(df_pandas) > 0 and pd.notna(df_pandas[min_col].iloc[0]) else "N/A")
                with col3:
                    st.write(f"**最大值**: {avg_col.replace('_avg', '_max')}")
                    max_col = avg_col.replace('_avg', '_max')
                    if max_col in df.columns:
                        st.write(f"{df_pandas[max_col].iloc[0]:,.0f}" if len(df_pandas) > 0 and pd.notna(df_pandas[max_col].iloc[0]) else "N/A")
                with col4:
                    st.write(f"**平均值**: {avg_col}")
                    st.write(f"{df_pandas[avg_col].iloc[0]:,.0f}" if len(df_pandas) > 0 and pd.notna(df_pandas[avg_col].iloc[0]) else "N/A")
                st.divider()

def create_download_file(df: pl.DataFrame, format="csv") -> bytes:
    """创建下载文件 - 修复版：默认CSV格式，兼容Excel"""
    if format.lower() == "csv":
        # 🔧 核心修复：使用utf-8-sig编码，确保Excel兼容性
        csv_string = df.write_csv()
        csv_bytes_with_bom = csv_string.encode('utf-8-sig')
        return csv_bytes_with_bom
    else:
        # Parquet格式（可选）
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        return buffer.getvalue()

def main():
    """主函数"""
    # 初始化样式
    init_page_style()
    
    # 渲染头部
    render_header()
    
    # 侧边栏信息
    with st.sidebar:
        st.markdown("### 🎯 使用指南")
        st.markdown("""
        1. **选择模式**: 单文件或批量上传
        2. **上传文件**: 支持CSV/Excel格式
        3. **自动处理**: 系统智能识别和清洗
        4. **下载结果**: CSV格式(兼容Excel)
        
        ---
        
        ### 🔧 技术特性
        - 🚀 **智能引擎**: pandas+polars混合
        - 📁 **批量处理**: 支持多文件同时处理
        - 🧠 **智能解析**: 多表自动识别
        - 🔍 **模糊处理**: 范围数值精确化
        - 📊 **质量报告**: 完整数据分析
        - 🌍 **Excel兼容**: UTF-8-BOM编码
        
        ---
        
        ### 🆕 最新更新
        - ✅ **CSV默认导出**: 兼容Excel
        - ✅ **批量上传**: 多文件处理
        - ✅ **智能合并**: 自动数据合并
        - ✅ **进度显示**: 实时处理状态
        
        ---
        
        ### 📞 技术支持
        如需帮助，请联系开发团队：
        - 🏗️ 管道架构师
        - 🎨 界面设计师
        """)
    
    # 主要内容区域
    uploaded_files = render_file_upload()
    
    if uploaded_files:
        # 显示文件信息
        if len(uploaded_files) == 1:
            st.success(f"✅ 文件上传成功: {uploaded_files[0].name}")
            st.info(f"📁 文件大小: {len(uploaded_files[0].getvalue()) / 1024:.1f} KB")
        else:
            st.success(f"✅ 批量上传成功: {len(uploaded_files)} 个文件")
            total_size = sum(len(f.getvalue()) for f in uploaded_files) / 1024
            st.info(f"📁 总文件大小: {total_size:.1f} KB")
            
            # 显示文件列表
            with st.expander("📋 查看文件列表"):
                for i, f in enumerate(uploaded_files, 1):
                    st.write(f"{i}. {f.name} ({len(f.getvalue()) / 1024:.1f} KB)")
        
        # 处理按钮 - 修复版本：分而治之，不合并数据
        if st.button("🚀 开始数据炼金", type="primary", use_container_width=True):
            
            st.info("🔧 数据炼金开始！正在准备处理环境...")
            
            # 独立处理每个文件，不再合并
            for index, up_file in enumerate(uploaded_files):
                st.markdown(f"---\n### 正在处理文件 {index + 1}: `{up_file.name}`")
                
                try:
                    # 1. 保存到临时文件
                    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(up_file.name)[1]) as tmp_file:
                        tmp_file.write(up_file.getvalue())
                        tmp_file_path = tmp_file.name
                    
                    # 2. 调用核心引擎处理
                    cleaned_df = process_douyin_export(tmp_file_path)
                    
                    if cleaned_df is None:
                        st.error(f"❌ 无法解析文件 `{up_file.name}` 中的有效数据")
                        continue
                    
                    # 3. 获取原始数据用于对比（重新读取以显示原始状态）
                    try:
                        if up_file.name.endswith('.csv'):
                            original_df = pl.read_csv(tmp_file_path, encoding='utf-8-sig', separator='\t')
                        else:
                            original_df = pl.read_excel(tmp_file_path)
                        original_df = original_df.head(5)  # 只显示前5行
                    except:
                        original_df = cleaned_df.head(5)  # 如果读取失败，使用清洗后的数据
                    
                    # 4. 立即展示处理结果
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("#### 📋 原始数据预览 (前5行)")
                        st.dataframe(original_df.to_pandas(), use_container_width=True)
                    
                    with col2:
                        st.write("#### ✨ 清洗后数据预览 (前5行)")
                        st.dataframe(cleaned_df.head(5).to_pandas(), use_container_width=True)
                    
                    # 5. 数据统计信息
                    st.write("**处理统计:**")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("总行数", f"{len(cleaned_df):,}")
                    with col2:
                        st.metric("总列数", len(cleaned_df.columns))
                    with col3:
                        range_cols = [col for col in cleaned_df.columns if col.endswith('_avg')]
                        st.metric("范围列", len(range_cols))
                    with col4:
                        st.metric("文件大小", f"{len(up_file.getvalue()) / 1024:.1f} KB")
                    
                    # 6. 提供独立的下载按钮
                    csv_string = cleaned_df.write_csv()
                    csv_bytes_with_bom = csv_string.encode('utf-8-sig')
                    
                    st.download_button(
                        label=f"📥 下载清洗后的数据: {up_file.name}",
                        data=csv_bytes_with_bom,
                        file_name=f"cleaned_{up_file.name}",
                        mime="text/csv",
                        key=f"download-btn-{index}",  # 每个按钮必须有唯一的key
                        help="CSV格式，已使用UTF-8-BOM编码，可直接在Excel中打开"
                    )
                    
                    st.success(f"✅ `{up_file.name}` 处理完成！")
                    
                except Exception as e:
                    st.error(f"❌ 处理 `{up_file.name}` 时发生错误: {e}")
                    # 打印详细的Traceback，方便调试
                    st.code(traceback.format_exc())
                    
                finally:
                    # 确保临时文件被删除
                    if 'tmp_file_path' in locals() and os.path.exists(tmp_file_path):
                        os.remove(tmp_file_path)
            
            # 处理完成后的总结
            st.markdown("---")
            st.header("🎉 处理完成！")
            st.info("所有文件已独立处理完成，请在上方查看每个文件的处理结果并下载。")


def process_single_file(uploaded_file) -> pl.DataFrame:
    """处理单个文件"""
    try:
        # 保存上传的文件到临时目录
        file_extension = os.path.splitext(uploaded_file.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name
        
        # 显示处理动画
        with st.spinner("正在炼制您的数据..."):
            progress_bar, status_text = render_processing_animation()
        
        try:
            # 调用核心处理函数
            result_df = process_douyin_export(tmp_file_path)
            return result_df
            
        finally:
            # 清理临时文件
            os.remove(tmp_file_path)
    
    except Exception as e:
        st.error(f"❌ 处理文件 {uploaded_file.name} 失败: {str(e)}")
        st.exception(e)
        return None


def process_multiple_files(uploaded_files) -> pl.DataFrame:
    """批量处理多个文件"""
    processed_dfs = []
    failed_files = []
    
    # 创建进度条
    progress_bar = st.progress(0)
    status_container = st.container()
    
    for i, uploaded_file in enumerate(uploaded_files):
        try:
            with status_container:
                st.write(f"🔄 正在处理文件 {i+1}/{len(uploaded_files)}: {uploaded_file.name}")
            
            # 更新进度条
            progress_bar.progress((i + 1) / len(uploaded_files))
            
            # 保存文件到临时目录
            file_extension = os.path.splitext(uploaded_file.name)[1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_file_path = tmp_file.name
            
            try:
                # 处理文件
                result_df = process_douyin_export(tmp_file_path)
                
                # 添加文件来源列
                result_df = result_df.with_columns([
                    pl.lit(uploaded_file.name).alias("文件来源")
                ])
                
                processed_dfs.append(result_df)
                
                with status_container:
                    st.write(f"✅ {uploaded_file.name} 处理完成: {len(result_df)} 行数据")
                
            finally:
                # 清理临时文件
                os.remove(tmp_file_path)
                
        except Exception as e:
            failed_files.append(uploaded_file.name)
            with status_container:
                st.write(f"❌ {uploaded_file.name} 处理失败: {str(e)}")
    
    # 合并所有处理成功的DataFrame
    if processed_dfs:
        try:
            # 使用Polars的concat函数合并
            merged_df = pl.concat(processed_dfs, how="diagonal")
            
            success_count = len(processed_dfs)
            total_rows = len(merged_df)
            
            st.success(f"🎉 批量处理完成！成功处理 {success_count}/{len(uploaded_files)} 个文件，共 {total_rows} 行数据")
            
            if failed_files:
                st.warning(f"⚠️ 以下文件处理失败: {', '.join(failed_files)}")
            
            return merged_df
            
        except Exception as e:
            st.error(f"❌ 合并数据失败: {str(e)}")
            return None
    else:
        st.error("❌ 没有任何文件处理成功")
        return None
    
    # 显示处理结果
    if 'processed_df' in st.session_state:
        df = st.session_state.processed_df
        quality_report = st.session_state.quality_report
        original_filenames = st.session_state.get('original_filenames', ['unknown_file'])
        
        # 渲染结果概览
        render_data_overview(df, quality_report)
        
        # 下载区域 - 按照用户要求的【最终正确版本】
        st.subheader("下载清洗后的数据")

        # --- 核心修复点 ---
        # 1. 调用 Polars 的 write_csv 方法，将 DataFrame 转换为 CSV 格式的字符串。
        csv_string = df.write_csv()

        # 2. 将该字符串编码为 'utf-8-sig'。
        #    这个'sig' (Signature, 即BOM) 是关键，它会告诉 Excel 等软件
        #    "这是一个UTF-8编码的文件"，从而正确显示中文字符。
        csv_bytes_with_bom = csv_string.encode('utf-8-sig')

        # 3. 创建下载按钮，并提供正确的数据和 MIME 类型。
        st.download_button(
            label="⬇️ 下载清洗结果 (兼容Excel的CSV格式)",
            data=csv_bytes_with_bom,
            file_name=f"cleaned_data_{time.strftime('%Y%m%d_%H%M%S')}.csv", # 加上时间戳避免重名
            mime="text/csv"
        )
        
        # 可选的额外下载格式
        with st.expander("🔧 其他格式下载 (可选)"):
            col1, col2 = st.columns(2)
            
            with col1:
                # Parquet格式（高性能）
                parquet_data = create_download_file(df, format="parquet")
                parquet_filename = f"cleaned_data_{time.strftime('%Y%m%d_%H%M%S')}.parquet"
                
                st.download_button(
                    label="📊 下载Parquet格式 (高性能)",
                    data=parquet_data,
                    file_name=parquet_filename,
                    mime="application/octet-stream",
                    help="Parquet格式，体积更小，处理速度更快，适合数据分析"
                )
            
            with col2:
                # Excel格式（如果需要）
                st.info("💡 主要下载按钮已使用CSV格式，可直接在Excel中打开")
        
        # 详细分析
        st.divider()
        
        # 数据质量图表
        render_data_quality_chart(quality_report)
        
        # 数据预览
        render_data_preview(df)
        
        # 技术细节（可折叠）
        with st.expander("🔍 查看技术细节"):
            st.write("**处理统计信息:**")
            col1, col2 = st.columns(2)
            
            with col1:
                st.json({
                    "原始文件": ", ".join(original_filenames) if len(original_filenames) <= 3 else f"{', '.join(original_filenames[:3])} 等{len(original_filenames)}个文件",
                    "处理时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "数据维度": f"{quality_report['total_rows']} 行 × {quality_report['total_columns']} 列",
                    "处理引擎": "pandas+polars 混合引擎",
                    "文件数量": len(original_filenames)
                })
            
            with col2:
                st.write("**列名列表:**")
                for i, col in enumerate(df.columns, 1):
                    st.write(f"{i}. {col}")

if __name__ == "__main__":
    main()