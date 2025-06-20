"""
🔧 数据炼金工坊 - 命令行主入口
高效的批处理数据清洗工具

作者: 整合与部署工程师
理念: 让架构师和设计师的才华能在一个命令下稳定运行
"""

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.prompt import Confirm
from pathlib import Path
import sys
from typing import Optional, List
import time

# 添加项目路径以便导入核心模块
sys.path.append(str(Path(__file__).parent.parent))

from core.etl_douyin import process_douyin_export, get_data_quality_report

# 创建Typer应用和Rich控制台
app = typer.Typer(
    name="datacleaner",
    help="🏗️ 数据炼金工坊 - 抖音电商数据批处理工具",
    add_completion=False,
    rich_markup_mode="rich"
)
console = Console()

# 版本信息
__version__ = "1.0.0"


def print_banner():
    """打印应用横幅"""
    banner = """
🏗️ 数据炼金工坊 CLI v{version}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ 高性能数据处理 | 🧠 智能多表解析 | 🔧 模糊数值处理 | 📊 质量报告生成
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""".format(version=__version__)
    
    console.print(Panel(banner, style="bold blue"))


@app.command()
def version():
    """显示版本信息"""
    console.print(f"🏗️ 数据炼金工坊 CLI v{__version__}", style="bold green")
    console.print("作者: 整合与部署工程师", style="dim")
    console.print("核心引擎: Polars (LTS-CPU)", style="dim")


@app.command()
def clean(
    input_file: Path = typer.Argument(
        ..., 
        help="输入CSV文件路径",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True
    ),
    output: Optional[Path] = typer.Option(
        None, 
        "-o", "--output",
        help="输出文件路径（默认: 输入文件名_cleaned.parquet）"
    ),
    output_format: str = typer.Option(
        "parquet",
        "-f", "--format",
        help="输出格式: parquet, csv, json",
        case_sensitive=False
    ),
    verbose: bool = typer.Option(
        False,
        "-v", "--verbose",
        help="显示详细处理信息"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="强制覆盖已存在的输出文件"
    ),
    quality_report: bool = typer.Option(
        True,
        "--quality-report/--no-quality-report",
        help="生成数据质量报告"
    )
):
    """
    🧹 清洗抖音电商数据文件
    
    将蝉妈妈导出的CSV文件进行智能清洗和格式转换。
    
    示例:
        datacleaner clean data.csv                           # 基础清洗
        datacleaner clean data.csv -o cleaned_data.parquet  # 指定输出
        datacleaner clean data.csv -f csv -v                # CSV格式+详细信息
    """
    print_banner()
    
    # 验证输入文件
    if not input_file.exists():
        console.print(f"❌ 输入文件不存在: {input_file}", style="bold red")
        raise typer.Exit(1)
    
    # 确定输出文件路径
    if output is None:
        output_stem = input_file.stem + "_cleaned"
        output_suffix = ".parquet" if output_format.lower() == "parquet" else f".{output_format.lower()}"
        output = input_file.parent / (output_stem + output_suffix)
    
    # 检查输出文件是否存在
    if output.exists() and not force:
        if not Confirm.ask(f"输出文件 {output} 已存在，是否覆盖？"):
            console.print("❌ 操作已取消", style="yellow")
            raise typer.Exit(0)
    
    # 显示处理信息
    info_table = Table(title="📋 处理配置", show_header=False, box=None)
    info_table.add_row("📁 输入文件", str(input_file))
    info_table.add_row("📤 输出文件", str(output))
    info_table.add_row("📊 输出格式", output_format.upper())
    info_table.add_row("📈 质量报告", "✅ 启用" if quality_report else "❌ 禁用")
    info_table.add_row("📝 详细模式", "✅ 启用" if verbose else "❌ 禁用")
    console.print(info_table)
    console.print()
    
    # 开始处理
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            
            # 主要处理任务
            main_task = progress.add_task("🏗️ 数据炼金处理中...", total=100)
            
            # 步骤1: 文件解析
            progress.update(main_task, description="🔍 智能解析多表CSV结构...", completed=15)
            if verbose:
                console.print("📊 正在识别表格结构...", style="dim")
            time.sleep(0.3)
            
            # 步骤2: 数据处理
            progress.update(main_task, description="🧹 清洗模糊数值范围...", completed=40)
            if verbose:
                console.print("🔢 正在处理模糊数值范围...", style="dim")
            
            # 调用核心引擎
            result_df = process_douyin_export(input_file)
            
            progress.update(main_task, description="⚙️ 标准化数据格式...", completed=70)
            if verbose:
                console.print("📏 正在标准化数据类型...", style="dim")
            time.sleep(0.2)
            
            # 步骤3: 质量检查
            if quality_report:
                progress.update(main_task, description="📊 生成质量报告...", completed=85)
                quality_data = get_data_quality_report(result_df)
                if verbose:
                    console.print("📈 正在分析数据质量...", style="dim")
            
            # 步骤4: 文件输出
            progress.update(main_task, description="💾 写入输出文件...", completed=95)
            
            # 根据格式保存文件
            if output_format.lower() == "parquet":
                result_df.write_parquet(output)
            elif output_format.lower() == "csv":
                result_df.write_csv(output)
            elif output_format.lower() == "json":
                result_df.write_json(output)
            else:
                console.print(f"❌ 不支持的输出格式: {output_format}", style="bold red")
                raise typer.Exit(1)
            
            progress.update(main_task, description="✅ 处理完成!", completed=100)
            time.sleep(0.2)
        
        # 显示处理结果
        console.print()
        success_panel = Panel(
            f"🎉 数据炼金完成！\n\n"
            f"📁 输出文件: {output}\n"
            f"📊 数据维度: {len(result_df)} 行 × {len(result_df.columns)} 列\n"
            f"💾 文件大小: {output.stat().st_size / 1024:.1f} KB",
            title="✅ 处理成功",
            style="bold green"
        )
        console.print(success_panel)
        
        # 显示质量报告
        if quality_report:
            display_quality_report(quality_data, verbose)
        
        # 显示数据预览
        if verbose:
            display_data_preview(result_df)
            
    except Exception as e:
        console.print(f"❌ 处理失败: {str(e)}", style="bold red")
        if verbose:
            console.print_exception()
        raise typer.Exit(1)


@app.command()
def batch(
    input_dir: Path = typer.Argument(
        ...,
        help="输入目录路径",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "-o", "--output-dir", 
        help="输出目录路径（默认: 输入目录/cleaned/）"
    ),
    pattern: str = typer.Option(
        "*.csv",
        "-p", "--pattern",
        help="文件匹配模式（默认: *.csv）"
    ),
    output_format: str = typer.Option(
        "parquet",
        "-f", "--format",
        help="输出格式: parquet, csv, json"
    ),
    workers: int = typer.Option(
        1,
        "-w", "--workers",
        help="并行处理数量（默认: 1）",
        min=1,
        max=8
    ),
    verbose: bool = typer.Option(
        False,
        "-v", "--verbose",
        help="显示详细处理信息"
    )
):
    """
    📦 批量处理目录中的所有数据文件
    
    扫描指定目录中的所有CSV文件并进行批量清洗处理。
    
    示例:
        datacleaner batch ./data/                           # 批量处理
        datacleaner batch ./data/ -o ./cleaned/ -w 4       # 并行处理
        datacleaner batch ./data/ -p "sales_*.csv" -v      # 模式匹配
    """
    print_banner()
    
    # 设置输出目录
    if output_dir is None:
        output_dir = input_dir / "cleaned"
    
    # 创建输出目录
    output_dir.mkdir(exist_ok=True)
    
    # 查找匹配的文件
    input_files = list(input_dir.glob(pattern))
    
    if not input_files:
        console.print(f"❌ 在目录 {input_dir} 中未找到匹配 {pattern} 的文件", style="bold red")
        raise typer.Exit(1)
    
    console.print(f"📁 找到 {len(input_files)} 个文件需要处理", style="bold blue")
    
    # 显示文件列表
    if verbose:
        file_table = Table(title="📋 待处理文件列表")
        file_table.add_column("序号", style="cyan", no_wrap=True)
        file_table.add_column("文件名", style="magenta")
        file_table.add_column("大小", style="green")
        
        for i, file_path in enumerate(input_files, 1):
            file_size = file_path.stat().st_size / 1024
            file_table.add_row(str(i), file_path.name, f"{file_size:.1f} KB")
        
        console.print(file_table)
        console.print()
    
    # 批量处理
    success_count = 0
    failed_files = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        
        batch_task = progress.add_task("📦 批量处理中...", total=len(input_files))
        
        for i, input_file in enumerate(input_files):
            try:
                progress.update(
                    batch_task, 
                    description=f"🏗️ 处理文件: {input_file.name}",
                    completed=i
                )
                
                # 确定输出文件
                output_stem = input_file.stem + "_cleaned"
                output_suffix = ".parquet" if output_format.lower() == "parquet" else f".{output_format.lower()}"
                output_file = output_dir / (output_stem + output_suffix)
                
                # 处理单个文件
                result_df = process_douyin_export(input_file)
                
                # 保存文件
                if output_format.lower() == "parquet":
                    result_df.write_parquet(output_file)
                elif output_format.lower() == "csv":
                    result_df.write_csv(output_file)
                elif output_format.lower() == "json":
                    result_df.write_json(output_file)
                
                success_count += 1
                
                if verbose:
                    console.print(f"✅ {input_file.name} → {output_file.name}", style="green")
                    
            except Exception as e:
                failed_files.append((input_file.name, str(e)))
                if verbose:
                    console.print(f"❌ {input_file.name}: {str(e)}", style="red")
        
        progress.update(batch_task, completed=len(input_files))
    
    # 显示批处理结果
    console.print()
    result_panel = Panel(
        f"📦 批量处理完成！\n\n"
        f"✅ 成功处理: {success_count} 个文件\n"
        f"❌ 处理失败: {len(failed_files)} 个文件\n"
        f"📁 输出目录: {output_dir}",
        title="📊 批处理结果",
        style="bold blue"
    )
    console.print(result_panel)
    
    # 显示失败文件详情
    if failed_files and verbose:
        error_table = Table(title="❌ 失败文件详情")
        error_table.add_column("文件名", style="red")
        error_table.add_column("错误信息", style="yellow")
        
        for filename, error in failed_files:
            error_table.add_row(filename, error[:50] + "..." if len(error) > 50 else error)
        
        console.print(error_table)


@app.command()
def info(
    file_path: Path = typer.Argument(
        ...,
        help="CSV文件路径",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True
    )
):
    """
    🔍 分析CSV文件结构和内容概览
    
    快速查看文件的基本信息，不进行数据清洗。
    
    示例:
        datacleaner info data.csv                           # 查看文件信息
    """
    console.print("🔍 正在分析文件结构...", style="bold blue")
    
    try:
        # 导入并解析文件
        from core.etl_douyin import parse_multi_table_csv
        
        tables = parse_multi_table_csv(file_path)
        
        # 文件基础信息
        file_size = file_path.stat().st_size / 1024
        info_table = Table(title="📁 文件基础信息", show_header=False)
        info_table.add_row("📄 文件名", file_path.name)
        info_table.add_row("📏 文件大小", f"{file_size:.1f} KB")
        info_table.add_row("🗂️ 表格数量", str(len(tables)))
        console.print(info_table)
        console.print()
        
        # 表格详细信息
        for table_name, df in tables.items():
            table_info = Table(title=f"📊 表格: {table_name}")
            table_info.add_column("属性", style="cyan")
            table_info.add_column("值", style="magenta")
            
            table_info.add_row("行数", f"{len(df):,}")
            table_info.add_row("列数", f"{len(df.columns)}")
            table_info.add_row("列名", ", ".join(df.columns[:5]) + ("..." if len(df.columns) > 5 else ""))
            
            console.print(table_info)
            console.print()
        
    except Exception as e:
        console.print(f"❌ 分析失败: {str(e)}", style="bold red")
        raise typer.Exit(1)


def display_quality_report(quality_data: dict, verbose: bool = False):
    """显示数据质量报告"""
    console.print("📊 数据质量报告", style="bold blue")
    
    # 基础统计
    stats_table = Table(show_header=False, box=None)
    stats_table.add_row("📏 总行数", f"{quality_data['total_rows']:,}")
    stats_table.add_row("📐 总列数", f"{quality_data['total_columns']}")
    
    # 统计空值
    null_count = sum(quality_data['null_counts'].values())
    stats_table.add_row("❌ 空值总数", f"{null_count:,}")
    
    # 统计数值列
    numeric_types = ['Float64', 'Int64', 'Float32', 'Int32']
    numeric_cols = sum(1 for dtype in quality_data['data_types'].values() if dtype in numeric_types)
    stats_table.add_row("🔢 数值列数", f"{numeric_cols}")
    
    console.print(stats_table)
    
    if verbose and null_count > 0:
        # 显示空值详情
        null_table = Table(title="❌ 空值分布")
        null_table.add_column("列名", style="red")
        null_table.add_column("空值数", style="yellow") 
        null_table.add_column("比例", style="cyan")
        
        for col, count in quality_data['null_counts'].items():
            if count > 0:
                ratio = count / quality_data['total_rows'] * 100
                null_table.add_row(col, str(count), f"{ratio:.1f}%")
        
        console.print(null_table)


def display_data_preview(df, limit: int = 5):
    """显示数据预览"""
    console.print(f"👀 数据预览 (前{limit}行)", style="bold blue")
    
    # 转换为pandas以便显示
    df_pandas = df.head(limit).to_pandas()
    
    preview_table = Table()
    
    # 添加列
    for col in df_pandas.columns[:8]:  # 最多显示8列
        preview_table.add_column(col[:15] + "..." if len(col) > 15 else col, style="cyan")
    
    # 添加行
    for _, row in df_pandas.iterrows():
        row_data = []
        for col in df_pandas.columns[:8]:
            value = str(row[col])
            if len(value) > 20:
                value = value[:17] + "..."
            row_data.append(value)
        preview_table.add_row(*row_data)
    
    console.print(preview_table)


@app.callback()
def main(
    version_flag: bool = typer.Option(
        False, 
        "--version", 
        help="显示版本信息",
        is_flag=True
    )
):
    """
    🏗️ 数据炼金工坊 - 抖音电商数据批处理工具
    
    让架构师和设计师的才华能在一个命令下稳定运行。
    """
    if version_flag:
        console.print(f"🏗️ 数据炼金工坊 CLI v{__version__}", style="bold green")
        raise typer.Exit()


if __name__ == "__main__":
    app()