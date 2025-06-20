"""
数据炼金工坊 - 抖音电商数据ETL引擎
基于Polars的高性能数据处理核心

作者: 管道架构师
理念: 任何Pandas能做的，Polars都能做得更快、更省内存
"""

import polars as pl
import re
from pathlib import Path
from typing import Dict, Optional, Union, List
import logging
import chardet
import openpyxl
import pandas as pd
import yaml

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def detect_encoding(file_path: Union[str, Path]) -> str:
    """
    智能检测文件编码
    
    解决编码问题：蝉妈妈导出文件可能使用GBK、GB2312或UTF-8-SIG编码
    """
    file_path = Path(file_path)
    
    # 读取文件的前几KB进行编码检测
    with open(file_path, 'rb') as f:
        raw_data = f.read(10240)  # 读取前10KB
    
    # 使用chardet检测编码
    result = chardet.detect(raw_data)
    detected_encoding = result['encoding']
    confidence = result['confidence']
    
    logger.info(f"检测到文件编码: {detected_encoding} (置信度: {confidence:.2f})")
    
    # 编码优先级列表：优先尝试常见的中文编码
    encoding_candidates = [
        'utf-8-sig',  # 带BOM的UTF-8（Excel导出常用）
        'utf-8',      # 标准UTF-8
        'gbk',        # 简体中文GBK
        'gb2312',     # 简体中文GB2312
        'gb18030',    # 扩展的中文编码
        detected_encoding  # chardet检测的编码
    ]
    
    # 去重并过滤None
    encoding_candidates = list(dict.fromkeys(filter(None, encoding_candidates)))
    
    # 测试每种编码
    for encoding in encoding_candidates:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                # 尝试读取前几行验证编码是否正确
                for _ in range(10):
                    line = f.readline()
                    if not line:
                        break
                    # 检查是否包含常见的中文关键词
                    if any(keyword in line for keyword in ['商品', '销量', '榜', '库', '抖音']):
                        logger.info(f"使用编码: {encoding}")
                        return encoding
                
                # 即使没有关键词，如果能正常读取也认为编码正确
                logger.info(f"使用编码: {encoding}")
                return encoding
                
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    # 如果所有编码都失败，默认使用utf-8
    logger.warning("无法检测文件编码，使用默认的utf-8")
    return 'utf-8'


def parse_messy_file(file_path: Union[str, Path]) -> Dict[str, pl.DataFrame]:
    """
    使用pandas+polars智能解析混乱的文件 - 重构版
    
    🏗️ 管道架构师核心重构：
    1. 使用pandas强大的文件读取能力自动处理编码和格式
    2. 智能多表检测和表头识别
    3. 结合配置文件的字段映射
    4. 最终转换为高性能Polars DataFrame
    
    Args:
        file_path: 文件路径（CSV/Excel）
        
    Returns:
        Dict[str, pl.DataFrame]: 表名到DataFrame的映射
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    logger.info(f"🏗️ 智能解析引擎启动 - 解析文件: {file_path}")
    
    tables = {}
    
    try:
        # Step 1: 智能检测文件类型和读取策略
        file_type = detect_file_type(file_path)
        
        if file_type == 'excel':
            tables = _parse_excel_with_pandas(file_path)
        else:  # csv
            tables = _parse_csv_with_pandas(file_path)
            
    except Exception as e:
        logger.error(f"智能解析失败: {e}")
        # 降级到原始解析方法
        logger.info("降级使用原始CSV解析方法")
        return _fallback_csv_parse(file_path)
    
    logger.info(f"🎯 智能解析完成，共发现 {len(tables)} 个表")
    
    if not tables:
        logger.warning("智能解析未找到表格，尝试降级解析")
        return _fallback_csv_parse(file_path)
    
    return tables


def _parse_excel_with_pandas(file_path: Path) -> Dict[str, pl.DataFrame]:
    """
    使用pandas解析Excel文件，支持多工作表和智能表格检测
    """
    logger.info(f"使用pandas解析Excel文件: {file_path}")
    tables = {}
    
    try:
        # 读取所有工作表
        excel_file = pd.ExcelFile(file_path)
        logger.info(f"发现 {len(excel_file.sheet_names)} 个工作表: {excel_file.sheet_names}")
        
        for sheet_name in excel_file.sheet_names:
            try:
                # 读取工作表数据，确保字符串类型以便后续strip处理
                df_pandas = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=str)
                
                if df_pandas.empty:
                    logger.warning(f"工作表 '{sheet_name}' 为空")
                    continue
                
                # 智能检测表格结构
                detected_tables = _detect_tables_in_sheet(df_pandas, sheet_name)
                
                # 转换为Polars DataFrame
                for table_name, table_df in detected_tables.items():
                    try:
                        # 转换为Polars
                        polars_df = pl.from_pandas(table_df)
                        tables[table_name] = polars_df
                        logger.info(f"✅ 成功解析表 '{table_name}': {len(polars_df)} 行 x {len(polars_df.columns)} 列")
                    except Exception as e:
                        logger.error(f"转换表 '{table_name}' 到Polars失败: {e}")
                        
            except Exception as e:
                logger.warning(f"读取工作表 '{sheet_name}' 失败: {e}")
                continue
                
    except Exception as e:
        logger.error(f"解析Excel文件失败: {e}")
        raise
    
    return tables


def _parse_csv_with_pandas(file_path: Path) -> Dict[str, pl.DataFrame]:
    """
    使用pandas解析CSV文件，支持智能编码检测和多表结构
    """
    logger.info(f"使用pandas解析CSV文件: {file_path}")
    
    # Step 1: 智能编码检测
    encoding = detect_encoding(file_path)
    
    try:
        # Step 2: 使用手动逐行解析处理多表格式
        # 如果检测到的编码不是utf-8-sig，但文件可能有BOM，优先使用utf-8-sig
        if encoding in ['utf-8', 'UTF-8']:
            encoding = 'utf-8-sig'
        
        logger.info("使用逐行解析方式处理多表格式文件")
        
        # 手动读取文件行并智能分隔符检测
        with open(file_path, 'r', encoding=encoding) as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        
        # 检测分隔符
        separator = _detect_separator_from_lines(lines)
        logger.info(f"检测到分隔符: '{repr(separator)}'")
        
        # 解析为DataFrame格式的数据
        df_pandas = _parse_multiformat_lines(lines, separator)
        
        logger.info(f"手动解析成功: {len(df_pandas)} 行 x {len(df_pandas.columns)} 列")
        
        # Step 3: 智能检测多表结构
        tables = _detect_tables_in_sheet(df_pandas, "main")
        
        # Step 4: 转换为Polars DataFrame
        polars_tables = {}
        for table_name, table_df in tables.items():
            try:
                polars_df = pl.from_pandas(table_df)
                polars_tables[table_name] = polars_df
                logger.info(f"✅ 成功解析表 '{table_name}': {len(polars_df)} 行 x {len(polars_df.columns)} 列")
            except Exception as e:
                logger.error(f"转换表 '{table_name}' 到Polars失败: {e}")
        
        return polars_tables
        
    except Exception as e:
        logger.error(f"pandas读取CSV失败: {e}")
        raise


def _detect_separator_from_lines(lines: List[str]) -> str:
    """
    从文本行中智能检测分隔符
    """
    separators = ['\t', ',', ';', '|']
    separator_scores = {}
    
    # 分析前几行来确定最可能的分隔符
    for sep in separators:
        scores = []
        for line in lines[:10]:  # 只检查前10行
            if sep in line:
                parts = line.split(sep)
                # 评分：列数 * 非空列数占比
                non_empty_parts = len([p for p in parts if p.strip()])
                if len(parts) > 1:
                    score = len(parts) * (non_empty_parts / len(parts))
                    scores.append(score)
        
        if scores:
            separator_scores[sep] = sum(scores) / len(scores)
    
    if not separator_scores:
        return '\t'  # 默认制表符
    
    # 返回得分最高的分隔符
    best_separator = max(separator_scores.keys(), key=lambda x: separator_scores[x])
    return best_separator


def _parse_multiformat_lines(lines: List[str], separator: str) -> pd.DataFrame:
    """
    解析多表格式的文本行为统一的DataFrame
    """
    # 找到最大列数
    max_columns = 0
    for line in lines:
        if separator in line:
            parts = line.split(separator)
            max_columns = max(max_columns, len(parts))
    
    # 解析所有行，统一列数
    parsed_rows = []
    for line in lines:
        if separator in line:
            parts = line.split(separator)
            # 补齐列数
            while len(parts) < max_columns:
                parts.append('')
            parsed_rows.append([p.strip() for p in parts[:max_columns]])
        else:
            # 单列行（通常是表标题）
            row = [line.strip()]
            while len(row) < max_columns:
                row.append('')
            parsed_rows.append(row)
    
    # 创建DataFrame
    if parsed_rows:
        columns = [f"col_{i}" for i in range(max_columns)]
        df = pd.DataFrame(parsed_rows, columns=columns)
        return df
    else:
        return pd.DataFrame()


def _detect_tables_in_sheet(df_pandas: pd.DataFrame, sheet_name: str) -> Dict[str, pd.DataFrame]:
    """
    在pandas DataFrame中智能检测多个表格 - 重构版
    使用更可靠的表头识别算法
    
    Args:
        df_pandas: pandas DataFrame（原始数据）
        sheet_name: 工作表名称
        
    Returns:
        Dict[str, pd.DataFrame]: 检测到的表格字典
    """
    logger.info(f"开始重构版表格结构检测 - 工作表: {sheet_name}")
    tables = {}
    
    # 定义可能的表头列关键词，越多越好
    HEADER_KEYWORDS = {
        '商品', '销量', '销售额', '佣金', '转化率', '链接', '分类',
        '商品标题', '商品链接', '商品价格', '店铺', '品牌', '类目',
        '佣金比例', '直播销售额', '商品卡销售额', '近30天销量',
        '周销量', '近1年销量', '30天转化率', '上架时间', '达人昵称'
    }
    
    possible_header_indices = []
    
    # 1. 预扫描，找到所有可能是表头的行的索引
    for idx, row in df_pandas.iterrows():
        # 确保所有值都strip处理
        row_values = {str(cell).strip() for cell in row.values if pd.notna(cell) and str(cell).strip()}
        
        # 如果某一行包含足够多的关键词，就认为它可能是表头
        keyword_matches = len(HEADER_KEYWORDS.intersection(row_values))
        non_empty_cells = len(row_values)
        
        # 动态条件判断：
        # 条件1：匹配关键词数 >= 2 (降低要求)
        # 条件2：非空单元格数量 >= 4 (支持小表格)
        # 条件3：非空单元格占比 > 40% (更宽松)
        # 条件4：特殊情况：如果包含"排名"+"商品"+"佣金比例"等核心组合，降低要求
        
        core_combo = {'排名', '商品', '佣金比例'}.intersection(row_values)
        is_core_combo = len(core_combo) >= 2
        
        if ((keyword_matches >= 2 and non_empty_cells >= 4) or 
            (keyword_matches >= 1 and non_empty_cells >= 5) or
            (is_core_combo and non_empty_cells >= 3)):
            cell_ratio = non_empty_cells / len(row.values)
            if cell_ratio > 0.4:
                possible_header_indices.append(idx)
                logger.debug(f"发现可能的表头行 {idx}: 匹配{keyword_matches}个关键词，{non_empty_cells}个非空单元格，核心组合:{is_core_combo}")
    
    if not possible_header_indices:
        logger.error("在工作表中未能识别出任何有效的表头行！")
        logger.warning("以下是工作表的前10行内容，请检查其格式：")
        logger.warning(df_pandas.head(10).to_string())
        
        # 降级处理：尝试寻找任何包含关键词的行
        for idx, row in df_pandas.iterrows():
            row_values = {str(cell).strip() for cell in row.values if pd.notna(cell) and str(cell).strip()}
            if len(HEADER_KEYWORDS.intersection(row_values)) >= 1 and len(row_values) >= 3:
                possible_header_indices.append(idx)
        
        if not possible_header_indices:
            return {}

    logger.info(f"识别到 {len(possible_header_indices)} 个可能的表头，位于行: {possible_header_indices}")

    # 2. 根据找到的表头行，切分数据块
    for i, header_idx in enumerate(possible_header_indices):
        # 确定数据块的起止行
        start_data_idx = header_idx + 1
        end_data_idx = possible_header_indices[i+1] if i + 1 < len(possible_header_indices) else len(df_pandas)
        
        # 提取数据块
        table_df = df_pandas.iloc[start_data_idx:end_data_idx].copy()
        
        # 清理掉完全是空值的行
        table_df.dropna(how='all', inplace=True)

        if table_df.empty:
            continue

        # 设置正确的表头（确保strip处理）
        header = [str(col).strip() if pd.notna(col) and str(col).strip() else f"col_{j}" 
                 for j, col in enumerate(df_pandas.iloc[header_idx].values)]
        
        # 确保列数匹配
        header = header[:len(table_df.columns)]
        
        # 确保列名唯一性
        unique_header = []
        col_counts = {}
        for col in header:
            if col in col_counts:
                col_counts[col] += 1
                unique_col = f"{col}_{col_counts[col]}"
            else:
                col_counts[col] = 0
                unique_col = col
            unique_header.append(unique_col)
        
        table_df.columns = unique_header
        
        # 确定表名（查找表头前面的标题行）
        table_name = f"数据表_{i+1}"
        if header_idx > 0:
            # 检查前面几行是否有表名
            for j in range(max(0, header_idx-3), header_idx):
                title_candidate = ' '.join([str(cell).strip() for cell in df_pandas.iloc[j].values 
                                          if pd.notna(cell) and str(cell).strip()])
                if title_candidate:
                    # 扩展表名关键词检查
                    table_keywords = ['销量榜', '商品库', 'SKU', '抖音', '直播', '热推榜', '潜力爆品榜', '持续好货榜', '历史同期榜']
                    if any(keyword in title_candidate for keyword in table_keywords):
                        table_name = title_candidate.strip()
                        break
                    # 如果包含"榜"或"库"字样，也认为是表名
                    elif any(keyword in title_candidate for keyword in ['榜', '库']):
                        table_name = title_candidate.strip()
                        break

        tables[table_name] = table_df
        logger.info(f"成功提取表格 '{table_name}'，共 {len(table_df)} 行数据，{len(table_df.columns)} 列")
        logger.debug(f"表格 '{table_name}' 的列名: {list(table_df.columns)[:10]}...")  # 只显示前10个列名
    
    # 3. 如果没有找到任何表格，尝试降级处理
    if not tables and not df_pandas.empty:
        logger.warning("使用降级方案：将整个数据作为单表处理")
        # 寻找第一个有足够非空单元格的行作为表头
        for idx, row in df_pandas.iterrows():
            non_empty_cells = [str(cell).strip() for cell in row.values if pd.notna(cell) and str(cell).strip()]
            if len(non_empty_cells) >= 3:
                table_df = df_pandas.iloc[idx:].copy()
                header = [str(col).strip() if pd.notna(col) and str(col).strip() else f"col_{j}" 
                         for j, col in enumerate(table_df.iloc[0].values)]
                header = header[:len(table_df.columns)]
                table_df.columns = header
                table_df = table_df.iloc[1:].reset_index(drop=True)
                table_df.dropna(how='all', inplace=True)
                
                if not table_df.empty:
                    tables[f"{sheet_name}_数据表"] = table_df
                    logger.info(f"降级处理: 创建表 '{sheet_name}_数据表', {len(table_df)} 行")
                break
        
    return tables


def _extract_table_data(df_pandas: pd.DataFrame, table_start: int, header_row: int, table_end: int) -> pd.DataFrame:
    """
    从pandas DataFrame中提取单个表格的数据
    """
    try:
        # 提取表头
        header_data = df_pandas.iloc[header_row]
        headers = [str(cell).strip() if pd.notna(cell) and str(cell).strip() else f"col_{i}" 
                  for i, cell in enumerate(header_data)]
        
        # 提取数据行（表头之后到表结束）
        data_start = header_row + 1
        if data_start >= table_end:
            return None
        
        table_data = df_pandas.iloc[data_start:table_end].copy()
        
        # 过滤掉完全空行
        table_data = table_data.dropna(how='all')
        
        if table_data.empty:
            return None
        
        # 设置列名
        table_data.columns = headers[:len(table_data.columns)]
        
        # 重置索引
        table_data = table_data.reset_index(drop=True)
        
        return table_data
        
    except Exception as e:
        logger.error(f"提取表格数据失败: {e}")
        return None




def _extract_table_name(text: str) -> str:
    """
    从包含表名的文本中提取表名
    """
    # 提取关键词
    keywords = ['SKU商品库', '抖音销量榜', '直播销量榜', '商品卡销量榜']
    
    for keyword in keywords:
        if keyword in text:
            return keyword
    
    # 如果没有精确匹配，提取包含关键词的部分
    if '商品库' in text:
        return 'SKU商品库'
    elif '销量榜' in text:
        if '抖音' in text:
            return '抖音销量榜'
        elif '直播' in text:
            return '直播销量榜'
        elif '商品卡' in text:
            return '商品卡销量榜'
        else:
            return '销量榜'
    
    # 默认返回清理后的文本前20个字符
    clean_text = re.sub(r'[^\w\u4e00-\u9fff\-_]', '', text)[:20]
    return clean_text if clean_text else '未知表格'


def _fallback_csv_parse(file_path: Path) -> Dict[str, pl.DataFrame]:
    """
    降级的CSV解析方法（保留原有逻辑作为备用）
    """
    logger.info("使用降级CSV解析方法")
    
    # 这里保留原有的parse_multi_table_csv逻辑的核心部分
    encoding = detect_encoding(file_path)
    
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    
    # 简化的表格识别逻辑
    tables = {}
    current_table = None
    current_data = []
    
    for line in lines:
        line = line.strip().replace('\ufeff', '')
        if not line or not line.replace(',', '').strip():
            continue
        
        # 简单的表头检测
        if any(keyword in line for keyword in ['商品', '销量', '榜', '库', 'SKU']):
            if ',' not in line or line.count(',') <= 3:
                # 可能是表头
                if current_table and current_data:
                    try:
                        tables[current_table] = _parse_table_data(current_data)
                    except:
                        pass
                
                current_table = line.split(',')[0].strip()[:20]
                current_data = []
                continue
        
        if current_table and ',' in line:
            current_data.append(line)
    
    # 保存最后一个表
    if current_table and current_data:
        try:
            tables[current_table] = _parse_table_data(current_data)
        except:
            pass
    
    return tables


def _parse_table_data(data_lines: List[str]) -> pl.DataFrame:
    """
    解析单个表的数据
    
    Args:
        data_lines: 表数据行列表
        
    Returns:
        pl.DataFrame: 解析后的DataFrame
    """
    if len(data_lines) < 2:
        return pl.DataFrame()
    
    # 第一行是列名
    header = data_lines[0].split(',')
    
    # 清理列名（移除空白和特殊字符）
    header = [col.strip() for col in header if col.strip()]
    
    # 数据行
    rows = []
    for line in data_lines[1:]:
        row = line.split(',')
        # 确保行长度与列数匹配
        while len(row) < len(header):
            row.append('')
        rows.append(row[:len(header)])
    
    if not rows:
        return pl.DataFrame(schema=header)
    
    # 创建DataFrame
    df = pl.DataFrame(rows, schema=header)
    
    return df


def parse_fuzzy_numeric_range(series: pl.Series, unit_multiplier: int = 1) -> Dict[str, pl.Series]:
    """
    解析模糊数值范围
    
    核心挑战：将"7.5w~10w"这样的范围转换为min/max/avg数值
    
    Args:
        series: 包含模糊范围的Series
        unit_multiplier: 单位乘数（w=10000）
        
    Returns:
        Dict包含min/max/avg的Series
    """
    min_values = []
    max_values = []
    avg_values = []
    
    # 遍历每个值进行处理
    for value in series.to_list():
        try:
            if value is None or value == '':
                min_values.append(None)
                max_values.append(None)
                avg_values.append(None)
                continue
            
            # 转换为字符串并清理
            str_val = str(value).strip()
            str_val = str_val.replace('万', 'w').replace('W', 'w').replace('%', '')
            
            # 检查是否包含范围符号
            if '~' in str_val:
                # 解析范围
                parts = str_val.split('~')
                if len(parts) == 2:
                    min_part = parts[0].strip()
                    max_part = parts[1].strip()
                    
                    # 解析最小值
                    if 'w' in min_part:
                        min_val = float(min_part.replace('w', '')) * 10000
                    else:
                        min_val = float(min_part)
                    
                    # 解析最大值
                    if 'w' in max_part:
                        max_val = float(max_part.replace('w', '')) * 10000
                    else:
                        max_val = float(max_part)
                    
                    min_values.append(min_val)
                    max_values.append(max_val)
                    avg_values.append((min_val + max_val) / 2)
                else:
                    min_values.append(None)
                    max_values.append(None)
                    avg_values.append(None)
            else:
                # 单一数值
                if 'w' in str_val:
                    val = float(str_val.replace('w', '')) * 10000
                else:
                    # 检查是否为纯数字
                    if str_val.replace('.', '').replace('-', '').isdigit():
                        val = float(str_val)
                    else:
                        val = None
                
                if val is not None:
                    min_values.append(val)
                    max_values.append(val)
                    avg_values.append(val)
                else:
                    min_values.append(None)
                    max_values.append(None)
                    avg_values.append(None)
                    
        except Exception as e:
            logger.warning(f"解析数值失败: {value}, 错误: {e}")
            min_values.append(None)
            max_values.append(None)
            avg_values.append(None)
    
    return {
        'min': pl.Series(min_values, dtype=pl.Float64),
        'max': pl.Series(max_values, dtype=pl.Float64),
        'avg': pl.Series(avg_values, dtype=pl.Float64)
    }


def clean_common_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    通用字段清洗函数 - 焦土重构版
    
    核心功能：
    1. 佣金比例百分比转换
    2. 模糊数值范围处理
    3. 精确数值类型转换
    
    重要：只返回处理后的数据，不包含原始列
    
    Args:
        df: 原始DataFrame
        
    Returns:
        pl.DataFrame: 只包含清洗后数据的DataFrame
    """
    logger.info("开始通用字段清洗 - 焦土重构版")
    
    # 创建一个新的列收集器，只收集处理后的列
    processed_columns = []
    
    # 1. 处理佣金比例
    if '佣金比例' in df.columns:
        processed_columns.append(
            pl.col('佣金比例')
            .str.replace('%', '')
            .str.replace('百分比', '')
            .cast(pl.Float64, strict=False)
            .truediv(100)
            .alias('佣金比例')
        )
    
    # 2. 处理模糊数值范围字段
    range_fields = ['近30天销量', '周销量', '近1年销量', '销售额', '近30天销售额', '近1年销售额', 
                   '昨日销量', '近90天销量', '同期销量', '周带货达人', '关联达人']
    
    for field in range_fields:
        if field in df.columns:
            logger.info(f"处理模糊数值字段: {field}")
            
            try:
                # 确保数据为字符串类型后再解析
                field_series = df[field].cast(pl.Utf8)
                
                # 解析范围
                range_data = parse_fuzzy_numeric_range(field_series)
                
                # 只添加处理后的新列到收集器
                processed_columns.extend([
                    range_data['min'].alias(f'{field}_min'),
                    range_data['max'].alias(f'{field}_max'),
                    range_data['avg'].alias(f'{field}_avg')
                ])
                
                logger.info(f"成功处理模糊数值字段: {field}")
                
            except Exception as e:
                logger.error(f"处理模糊数值字段 {field} 失败: {e}")
                logger.warning(f"跳过字段 {field} 的范围解析")
                continue
    
    # 3. 处理精确数值字段
    numeric_fields = ['直播销售额', '商品卡销售额']
    
    for field in numeric_fields:
        if field in df.columns:
            processed_columns.append(
                pl.col(field)
                .str.replace('%', '')
                .str.replace('，', '')
                .cast(pl.Float64, strict=False)
                .alias(field)
            )
    
    # 4. 处理转化率范围
    rate_fields = ['转化率', '30天转化率']
    
    for field in rate_fields:
        if field in df.columns:
            try:
                rate_series = df[field].cast(pl.Utf8)
                rate_data = parse_fuzzy_numeric_range(rate_series)
                
                # 转化率需要除以100
                processed_columns.extend([
                    (rate_data['min'] / 100).alias(f'{field}_min'),
                    (rate_data['max'] / 100).alias(f'{field}_max'),
                    (rate_data['avg'] / 100).alias(f'{field}_avg')
                ])
                
                logger.info(f"成功处理转化率字段: {field}")
            except Exception as e:
                logger.error(f"处理转化率字段 {field} 失败: {e}")
    
    # 5. 处理需要保留的原始文本字段
    text_fields = ['商品', '商品链接', '商品分类', '商品头图链接', '蝉妈妈商品链接', 
                   '抖音商品链接', '小店', '品牌', '蝉妈妈链接']
    
    for field in text_fields:
        if field in df.columns:
            processed_columns.append(pl.col(field))
    
    # 6. 处理需要保留的原始数值字段（如排名）
    keep_as_is_fields = ['排名']
    
    for field in keep_as_is_fields:
        if field in df.columns:
            processed_columns.append(
                pl.col(field).cast(pl.Int64, strict=False).alias(field)
            )
    
    # 7. 构建最终的DataFrame - 只包含处理后的列
    if not processed_columns:
        logger.error("没有找到任何可处理的列！")
        return pl.DataFrame()  # 返回空DataFrame
    
    # 创建只包含处理后数据的新DataFrame
    cleaned_df = df.select(processed_columns)
    
    # 输出最终的列信息
    logger.info(f"最终DataFrame包含 {len(cleaned_df.columns)} 列")
    logger.info(f"最终列名: {cleaned_df.columns}")
    
    # 验证数据
    logger.info(f"数据行数: {len(cleaned_df)}")
    
    # 显示数据类型分布
    type_counts = {}
    for col in cleaned_df.columns:
        dtype = str(cleaned_df[col].dtype)
        type_counts[dtype] = type_counts.get(dtype, 0) + 1
    logger.info(f"数据类型分布: {type_counts}")
    
    logger.info("通用字段清洗完成 - 焦土重构版")
    return cleaned_df


def parse_excel_file(file_path: Union[str, Path]) -> Dict[str, pl.DataFrame]:
    """
    解析Excel文件
    
    支持多个工作表，每个工作表作为一个表格
    
    Args:
        file_path: Excel文件路径
        
    Returns:
        Dict[str, pl.DataFrame]: 工作表名到DataFrame的映射
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    logger.info(f"开始解析Excel文件: {file_path}")
    
    tables = {}
    
    try:
        # 使用openpyxl读取Excel文件
        workbook = openpyxl.load_workbook(file_path, data_only=True)
        
        for sheet_name in workbook.sheetnames:
            logger.info(f"处理工作表: {sheet_name}")
            
            # 使用Polars读取Excel工作表
            try:
                df = pl.read_excel(file_path, sheet_name=sheet_name)
                
                if len(df) > 0:
                    tables[sheet_name] = df
                    logger.info(f"成功读取工作表 '{sheet_name}'，{len(df)} 行 x {len(df.columns)} 列")
                else:
                    logger.warning(f"工作表 '{sheet_name}' 为空")
                    
            except Exception as e:
                logger.warning(f"读取工作表 '{sheet_name}' 失败: {e}")
                continue
        
        workbook.close()
        
    except Exception as e:
        logger.error(f"读取Excel文件失败: {e}")
        raise ValueError(f"无法读取Excel文件: {e}")
    
    logger.info(f"Excel解析完成，共发现 {len(tables)} 个工作表")
    return tables


def detect_file_type(file_path: Union[str, Path]) -> str:
    """
    检测文件类型
    
    Returns:
        'csv' 或 'excel'
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()
    
    if suffix in ['.xlsx', '.xls']:
        return 'excel'
    elif suffix in ['.csv', '.txt']:
        return 'csv'
    else:
        # 默认按CSV处理
        logger.warning(f"未知文件类型 {suffix}，按CSV处理")
        return 'csv'


def process_douyin_export(file_path: Union[str, Path]) -> Optional[pl.DataFrame]:
    """
    抖音导出数据主处理流程 - 增强版
    
    数据炼金工坊的核心函数 - 将原始数据炼成纯金
    
    新特性：
    1. 支持CSV和Excel文件格式
    2. 智能编码检测
    3. 增强的错误处理和调试信息
    
    Args:
        file_path: 原始文件路径（CSV或Excel）
        
    Returns:
        pl.DataFrame: 清洗后的主DataFrame
    """
    logger.info(f"--- [引擎入口] 开始处理文件: {file_path} ---")
    logger.info("🏗️ 数据炼金工坊启动 - 开始数据处理")
    
    file_path = Path(file_path)
    
    # Step 1: 检测文件类型并解析
    file_type = detect_file_type(file_path)
    logger.info(f"检测到文件类型: {file_type.upper()}")
    
    try:
        # 使用messytables统一处理CSV和Excel文件
        tables = parse_messy_file(file_path)
    except Exception as e:
        logger.error(f"文件解析失败: {e}")
        raise ValueError(f"文件解析失败: {e}")
    
    if not tables:
        raise ValueError("未能解析出任何表格数据")
    
    logger.info(f"成功解析出 {len(tables)} 个表格: {list(tables.keys())}")
    
    # Step 2: 选择主表（优先级：抖音销量榜 > SKU商品库 > 包含关键词的表 > 第一个表）
    main_table_name = None
    
    # 优先级1：精确匹配
    priority_tables = ['抖音销量榜', 'SKU商品库', '直播销量榜', '商品卡销量榜']
    for priority_name in priority_tables:
        if priority_name in tables and len(tables[priority_name]) > 0:
            main_table_name = priority_name
            break
    
    # 优先级2：模糊匹配包含关键词的表
    if not main_table_name:
        keywords = ['销量', '商品', '榜', '库']
        for name, df in tables.items():
            if len(df) > 0:
                if any(keyword in name for keyword in keywords):
                    main_table_name = name
                    break
    
    # 优先级3：选择第一个非空表
    if not main_table_name:
        for name, df in tables.items():
            if len(df) > 0:
                main_table_name = name
                break
    
    if not main_table_name:
        raise ValueError("未找到有效的主表数据")
    
    logger.info(f"选择主表: '{main_table_name}' (数据行数: {len(tables[main_table_name])})")
    main_df = tables[main_table_name]
    
    # 输出主表基本信息
    logger.info(f"主表列名: {main_df.columns}")
    
    # Step 3: 清洗主表数据
    logger.info("开始数据清洗处理")
    try:
        cleaned_df = clean_common_fields(main_df)
    except Exception as e:
        logger.error(f"数据清洗失败: {e}")
        # 如果清洗失败，返回原始数据
        logger.warning("数据清洗失败，返回原始数据")
        cleaned_df = main_df
    
    # Step 4: 数据质量检查
    logger.info("执行数据质量检查")
    
    # 检查数据完整性
    total_rows = len(cleaned_df)
    if total_rows == 0:
        logger.warning("警告：处理后的数据为空")
    else:
        logger.info(f"数据处理完成：共 {total_rows} 行数据")
        
        # 输出列信息
        logger.info(f"列数：{len(cleaned_df.columns)}")
        logger.info(f"最终列名：{cleaned_df.columns}")
        
        # 数据类型分析
        numeric_cols = [col for col in cleaned_df.columns if cleaned_df[col].dtype in [pl.Float64, pl.Int64]]
        logger.info(f"数值列数量: {len(numeric_cols)}")
        
        # 检查是否有生成的范围列
        range_cols = [col for col in cleaned_df.columns if '_avg' in col]
        if range_cols:
            logger.info(f"成功生成 {len(range_cols)} 个范围平均值列")
    
    # Step 5: 应用字段映射配置
    try:
        mapped_df = apply_field_mapping(cleaned_df)
        logger.info("字段映射应用成功")
        cleaned_df = mapped_df
    except Exception as e:
        logger.warning(f"字段映射失败: {e}，使用原始字段名")
    
    logger.info("🎯 数据炼金工坊完成 - 数据已炼成纯金！")
    
    # 在函数最终返回之前进行验证
    if cleaned_df is not None and not cleaned_df.is_empty():
        logger.info(f"--- [引擎出口] 成功处理文件，返回 {cleaned_df.height} 行数据 ---")
        return cleaned_df
    else:
        logger.error(f"--- [引擎出口] 处理失败，未能生成有效数据，返回 None ---")
        return None


def load_field_mapping_config() -> Dict:
    """
    加载字段映射配置文件
    
    Returns:
        Dict: 配置字典
    """
    config_path = Path(__file__).parent.parent / "config" / "field_map.yml"
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.debug(f"成功加载配置文件: {config_path}")
        return config
    except Exception as e:
        logger.warning(f"加载配置文件失败: {e}")
        return {}


def apply_field_mapping(df: pl.DataFrame) -> pl.DataFrame:
    """
    应用字段映射配置
    
    将中文列名映射为英文标准列名
    
    Args:
        df: 原始DataFrame
        
    Returns:
        pl.DataFrame: 映射后的DataFrame
    """
    config = load_field_mapping_config()
    field_mapping = config.get('field_mapping', {})
    
    if not field_mapping:
        logger.warning("未找到字段映射配置，保持原始字段名")
        return df
    
    # 构建重命名映射
    rename_mapping = {}
    for col in df.columns:
        if col in field_mapping:
            new_name = field_mapping[col]
            rename_mapping[col] = new_name
            logger.debug(f"字段映射: '{col}' -> '{new_name}'")
    
    if rename_mapping:
        df = df.rename(rename_mapping)
        logger.info(f"应用了 {len(rename_mapping)} 个字段映射")
    else:
        logger.info("未找到匹配的字段映射")
    
    return df


def get_data_quality_report(df: pl.DataFrame) -> Dict:
    """
    生成数据质量报告
    
    Args:
        df: 数据DataFrame
        
    Returns:
        Dict: 质量报告
    """
    report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns': df.columns,
        'null_counts': {},
        'data_types': {}
    }
    
    for col in df.columns:
        report['null_counts'][col] = df[col].null_count()
        report['data_types'][col] = str(df[col].dtype)
    
    return report


if __name__ == "__main__":
    # 测试代码
    test_file = Path(__file__).parent.parent.parent / "data" / "test_sample.csv"
    
    if test_file.exists():
        result_df = process_douyin_export(test_file)
        print(f"处理结果：{len(result_df)} 行 x {len(result_df.columns)} 列")
        print(f"列名：{result_df.columns}")
        
        # 显示前几行
        print("\n前5行数据预览：")
        print(result_df.head())
        
        # 数据质量报告
        quality_report = get_data_quality_report(result_df)
        print(f"\n数据质量报告：{quality_report}")
    else:
        print(f"测试文件不存在：{test_file}")