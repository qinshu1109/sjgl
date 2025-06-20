# 🔧 数据炼金工坊 - 自动化构建脚本
# 作者: 整合与部署工程师

.PHONY: help install install-dev test test-cov lint format clean run-web run-cli build docker-build docker-run

# 默认目标
help:
	@echo "🏗️ 数据炼金工坊 - 自动化命令"
	@echo "================================="
	@echo ""
	@echo "📦 环境管理:"
	@echo "  install     - 安装生产依赖"
	@echo "  install-dev - 安装开发依赖"
	@echo "  clean       - 清理缓存和临时文件"
	@echo ""
	@echo "🧪 测试与质量:"
	@echo "  test        - 运行测试套件"
	@echo "  test-cov    - 运行测试并生成覆盖率报告"
	@echo "  lint        - 代码质量检查"
	@echo "  format      - 格式化代码"
	@echo ""
	@echo "🚀 运行应用:"
	@echo "  run-web     - 启动Web界面"
	@echo "  run-cli     - 显示CLI帮助"
	@echo "  demo        - 运行演示案例"
	@echo ""
	@echo "📦 构建部署:"
	@echo "  build       - 构建Python包"
	@echo "  docker-build - 构建Docker镜像"
	@echo "  docker-run  - 运行Docker容器"

# 环境管理
install:
	@echo "📦 安装生产依赖..."
	python -m pip install --upgrade pip
	pip install -r requirements.txt

install-dev: install
	@echo "🛠️ 安装开发依赖..."
	pip install pytest pytest-cov black isort flake8 mypy

clean:
	@echo "🧹 清理缓存和临时文件..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf dist/
	rm -rf build/
	rm -f test_output.*

# 测试与质量
test:
	@echo "🧪 运行测试套件..."
	python -m pytest tests/ -v

test-cov:
	@echo "📊 运行测试并生成覆盖率报告..."
	python -m pytest tests/ --cov=app --cov-report=html --cov-report=term-missing

lint:
	@echo "🔍 代码质量检查..."
	flake8 app/ tests/ --max-line-length=88 --extend-ignore=E203,W503
	mypy app/ --ignore-missing-imports

format:
	@echo "✨ 格式化代码..."
	black app/ tests/
	isort app/ tests/

# 运行应用
run-web:
	@echo "🌐 启动Web界面..."
	python run_app.py

run-cli:
	@echo "💻 CLI帮助信息:"
	python datacleaner.py --help

demo:
	@echo "🎬 运行演示案例..."
	python datacleaner.py clean data/test_sample.csv -o demo_output.parquet -v
	@echo "✅ 演示完成，输出文件: demo_output.parquet"

# 构建部署
build:
	@echo "📦 构建Python包..."
	python -m pip install --upgrade build
	python -m build

docker-build:
	@echo "🐳 构建Docker镜像..."
	docker build -t dy-ec-cleaner:latest .

docker-run:
	@echo "🐳 运行Docker容器..."
	docker run -p 8501:8501 dy-ec-cleaner:latest

# 快速开发命令
dev: install-dev format lint test
	@echo "🎉 开发环境准备完成！"

ci: lint test
	@echo "🤖 CI检查完成！"

all: clean install-dev format lint test build
	@echo "🏗️ 完整构建流程完成！"