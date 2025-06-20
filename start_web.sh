#!/bin/bash
# 数据炼金工坊Web界面启动脚本

# 激活虚拟环境
source venv/bin/activate

# 杀死之前的进程
pkill -f streamlit

# 等待一下
sleep 2

# 设置环境变量
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# 启动Streamlit应用
echo "🚀 正在启动数据炼金工坊..."
echo "📍 访问地址: http://localhost:8501"

# 使用nohup在后台运行，输出到日志文件
nohup streamlit run app/ui/streamlit_app.py \
    --server.port=8501 \
    --server.address=localhost \
    --server.headless=true \
    --browser.serverAddress=localhost \
    --browser.serverPort=8501 \
    > streamlit_app.log 2>&1 &

# 获取进程ID
PID=$!
echo "✅ 应用已启动，进程ID: $PID"

# 等待几秒让应用完全启动
sleep 3

# 检查进程是否还在运行
if ps -p $PID > /dev/null; then
    echo "✅ 应用运行正常！"
    echo "🌐 请在浏览器中访问: http://localhost:8501"
    echo "📋 查看日志: tail -f streamlit_app.log"
    echo "🛑 停止应用: kill $PID"
else
    echo "❌ 应用启动失败，请查看日志文件 streamlit_app.log"
    tail -20 streamlit_app.log
fi