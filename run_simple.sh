#!/bin/bash

# 简洁版Web界面启动脚本

echo "--- 启动简洁版数据炼金工坊 ---"

# 激活虚拟环境
if [ -d "venv" ]; then
    echo "正在激活虚拟环境..."
    source venv/bin/activate
else
    echo "错误：未找到虚拟环境，请先运行 python -m venv venv"
    exit 1
fi

# 设置端口
PORT=8503

# 检查端口是否被占用
echo "正在检查端口 $PORT 是否被占用..."
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "端口 $PORT 被占用，正在清理..."
    kill $(lsof -Pi :$PORT -sTCP:LISTEN -t) 2>/dev/null
    sleep 2
fi

# 启动Streamlit
echo "正在启动简洁版界面..."
streamlit run app/ui/streamlit_app_simple.py \
    --server.port=$PORT \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --theme.primaryColor="#FF6B6B" \
    --theme.backgroundColor="#FFFFFF" \
    --theme.secondaryBackgroundColor="#F0F2F6" \
    --theme.textColor="#262730"