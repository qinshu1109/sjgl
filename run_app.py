#!/usr/bin/env python3
"""
🚀 数据炼金工坊启动器
一键启动Streamlit应用

作者: 界面设计师
"""

import subprocess
import sys
from pathlib import Path

def main():
    """启动Streamlit应用"""
    app_path = Path(__file__).parent / "app" / "ui" / "streamlit_app.py"
    
    if not app_path.exists():
        print(f"❌ 应用文件不存在: {app_path}")
        sys.exit(1)
    
    print("🏗️ 启动数据炼金工坊...")
    print("🌐 应用将在浏览器中自动打开")
    print("📍 默认地址: http://localhost:8501")
    print("⏹️  按 Ctrl+C 停止应用")
    print("-" * 50)
    
    try:
        # 启动Streamlit应用
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(app_path),
            "--server.port=8501",
            "--server.address=localhost",
            "--browser.gatherUsageStats=false",
            "--theme.primaryColor=#FF6B6B",
            "--theme.backgroundColor=#FFFFFF",
            "--theme.secondaryBackgroundColor=#F0F2F6",
            "--theme.textColor=#262730"
        ])
    except KeyboardInterrupt:
        print("\n🛑 应用已停止")
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()