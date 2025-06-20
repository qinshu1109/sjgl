#!/bin/bash

# 🏗️ 数据炼金工坊启动脚本
# 作者: 界面设计师

echo "🏗️ 数据炼金工坊 - Web界面启动"
echo "================================="
echo ""

# 检查虚拟环境
if [ ! -d "venv" ]; then
    echo "❌ 虚拟环境未找到，请先运行: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# 激活虚拟环境
echo "🔧 激活虚拟环境..."
source venv/bin/activate

# 检查依赖
echo "📦 检查依赖..."
python -c "import streamlit, polars" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ 依赖未安装，正在安装..."
    pip install streamlit plotly polars-lts-cpu
fi

echo "✅ 环境检查完成"
echo ""

# 启动应用
echo "🚀 启动Streamlit应用..."
echo "📍 访问地址: http://localhost:8501"
echo "⏹️  按 Ctrl+C 停止应用"
echo ""

python run_app.py