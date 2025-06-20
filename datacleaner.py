#!/usr/bin/env python3
"""
🔧 数据炼金工坊 - 独立CLI启动器
可直接执行的命令行工具

作者: 整合与部署工程师
使用: python datacleaner.py clean data.csv
"""

from app.cli.main import app

if __name__ == "__main__":
    app()