#!/bin/bash

# 简洁版后台启动脚本

echo "--- 启动简洁版数据炼金工坊 ---"

# 进入项目目录
cd "$(dirname "$0")"

# 激活虚拟环境
source venv/bin/activate

# 设置端口
PORT=8503

# 清理可能存在的进程
echo "正在清理端口 $PORT..."
pkill -f "streamlit.*$PORT" 2>/dev/null
sleep 2

# 后台启动
echo "正在后台启动简洁版界面..."
nohup streamlit run app/ui/streamlit_app_simple.py \
    --server.port=$PORT \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    > simple.log 2>&1 &

PID=$!
echo "Streamlit 进程ID: $PID"

# 等待启动
echo "等待服务启动..."
sleep 5

# 检查是否成功启动
if ps -p $PID > /dev/null; then
    echo "✅ 服务已成功在后台启动！"
    echo "🌐 请在浏览器中访问: http://localhost:$PORT"
    echo "📝 查看日志: tail -f simple.log"
    echo "🛑 停止服务: kill $PID"
else
    echo "❌ 服务启动失败，请查看日志: cat simple.log"
    exit 1
fi