# 🐳 数据炼金工坊 - Docker配置
# 作者: 整合与部署工程师

FROM python:3.12-slim

LABEL maintainer="整合与部署工程师 <integration-engineer@datacleaner.ai>"
LABEL description="🏗️ 数据炼金工坊 - 抖音电商数据清洗工具"
LABEL version="1.0.0"

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY app/ ./app/
COPY data/ ./data/
COPY run_app.py .
COPY datacleaner.py .
COPY .streamlit/ ./.streamlit/

# 创建非root用户
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

# 暴露端口
EXPOSE 8501

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# 默认启动Web界面
CMD ["python", "run_app.py"]

# 可选的CLI模式
# CMD ["python", "datacleaner.py", "--help"]