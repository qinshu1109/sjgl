#!/bin/bash

# run_web.sh - 数据炼金工坊 Web 界面启动脚本

echo "--- 准备启动数据炼金工坊 Web 界面 ---"

# 1. 设置项目根目录 (确保脚本可以在任何位置运行)
# 这行代码会自动获取脚本所在的目录
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$DIR"

# 2. 检查并激活虚拟环境
if [ ! -d "venv" ]; then
    echo "错误：找不到虚拟环境 'venv'。请先运行 'python3 -m venv venv' 和 'pip install -r requirements.txt'"
    exit 1
fi
echo "正在激活虚拟环境..."
source venv/bin/activate

# 3. 检查并终止任何可能在运行的旧 Streamlit 进程
# 我们将使用一个特定的端口，并只杀死占用该端口的进程
PORT=8502
echo "正在检查端口 $PORT 是否被占用..."
# lsof -t -i:$PORT 会返回占用该端口的进程ID
# xargs -r kill -9 会在有进程ID时才执行 kill 命令
lsof -t -i:$PORT | xargs -r kill -9 2>/dev/null
echo "端口 $PORT 已清理。"

# 4. 以最稳健的方式启动 Streamlit
# --server.headless=true 对于在后台或容器中运行非常重要
# 将日志输出到固定文件，而不是 Claude 的临时日志
echo "正在启动 Streamlit 服务，日志将输出到 'streamlit.log'..."
streamlit run app/ui/streamlit_app.py \
    --server.port $PORT \
    --server.headless=true \
    --server.address 0.0.0.0 > streamlit.log 2>&1 &
    
# 获取进程ID
PID=$!
echo "Streamlit 进程ID: $PID"
    
# 等待几秒钟让服务启动
echo "等待服务启动..."
sleep 5

# 5. 验证服务是否成功运行
if pgrep -f "streamlit run app/ui/streamlit_app.py" > /dev/null; then
    echo "✅ 服务已成功在后台启动！"
    echo "🌐 请在浏览器中访问: http://localhost:$PORT"
    echo "📝 查看日志: tail -f streamlit.log"
    echo "🛑 停止服务: kill $PID"
else
    echo "❌ 服务启动失败！请检查 streamlit.log 文件获取详细错误信息。"
    echo "--- 最后20行日志 ---"
    tail -20 streamlit.log
fi

# 退出虚拟环境 (可选)
# deactivate