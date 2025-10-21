# 下载、修复、拆分数据所需要用到的函数
import sys
from pathlib import Path
# 将项目根目录加入 sys.path（以便导入 tools/）
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import glob
import zipfile
from pathlib import Path
import matplotlib.pyplot as plt
import yaml
# 导入日志工具
from tools.log_utils import setup_logger

def load_config(config_path="config.yaml"):
    """加载配置文件"""
    config_path = Path(config_path)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_data_dirs(symbol, interval, config):
    """根据配置获取数据目录"""
    paths = config["paths"]
    return {
        "raw": PROJECT_ROOT / paths["raw_dir"] / symbol / interval,
        "processed": PROJECT_ROOT / paths["processed_dir"],
        "datasets": PROJECT_ROOT / paths["datasets_dir"] / f"{symbol}_{interval}"
    }


# =============== 核心函数 ===============
# 从Binance下载原始数据，最多3次尝试重新下载
def download_binance_klines(symbol, interval, config, logger, timeframe="monthly", max_retries=3):
    dirs = get_data_dirs(symbol, interval, config)
    # 创建保存目录
    save_dir = dirs["raw"]
    save_dir.mkdir(parents=True, exist_ok=True)
    # 解析起止时间
    start = datetime.strptime(config["data"]["start_date"], "%Y-%m")
    end = datetime.strptime(config["data"]["end_date"], "%Y-%m")
    current = start
    logger.info(f"开始下载 {symbol} {interval} 数据")
    failed_files = []  # 记录失败的文件
    while current <= end:
        year, month = current.year, current.month
        filename = f"{symbol}-{interval}-{year}-{month:02d}.zip"
        url = f"https://data.binance.vision/data/spot/{timeframe}/klines/{symbol}/{interval}/{filename}"
        filepath = save_dir / filename

        # 检查文件是否已存在且完整
        if filepath.exists():
            if is_valid_zip(filepath):
                logger.info(f"✅ 已存在且有效: {filename}")
                current += relativedelta(months=1)
                continue
            else:
                logger.warning(f"⚠️ 文件损坏，重新下载: {filename}")
                filepath.unlink()  # 删除损坏文件

        # 尝试下载（带重试）
        success = False
        for attempt in range(max_retries):
            try:
                logger.info(f"📥 下载 {filename} (尝试 {attempt + 1}/{max_retries})")
                response = requests.get(url, stream=True, timeout=30)
                if response.status_code == 200:
                    with open(filepath, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    # 验证下载的文件
                    if is_valid_zip(filepath):
                        logger.info(f"✅ 下载成功: {filename}")
                        success = True
                        break
                    else:
                        logger.warning(f"⚠️ 文件无效，重试: {filename}")
                        if filepath.exists():
                            filepath.unlink()
                else:
                    logger.warning(f"❌ HTTP {response.status_code}: {filename}")
            except Exception as e:
                logger.error(f"⚠️ 下载异常 {filename} (尝试 {attempt + 1}): {e}")
                if filepath.exists():
                    filepath.unlink()  # 删除可能损坏的文件

        if not success:
            logger.error(f"❌ 最终下载失败: {filename}")
            failed_files.append(filename)

        current += relativedelta(months=1)
    # 报告失败情况
    if failed_files:
        logger.warning(f"⚠️ {len(failed_files)} 个文件下载失败: {failed_files}")
    else:
        logger.info("✅ 所有文件下载完成")
    return len(failed_files) == 0  # 返回是否全部成功

def is_valid_zip(filepath):
    """验证 ZIP 文件是否完整"""
    try:
        with zipfile.ZipFile(filepath, 'r') as z:
            # 检查 ZIP 文件完整性
            bad_file = z.testzip()
            return bad_file is None
    except (zipfile.BadZipFile, OSError):
        return False

# 多个.zip文件合并为CSV文件
def merge_binance_klines(symbol, interval, config, logger):
    dirs = get_data_dirs(symbol, interval, config)
    raw_dir = dirs["raw"]
    processed_dir = dirs["processed"]
    processed_dir.mkdir(parents=True, exist_ok=True)

    zip_files = sorted(glob.glob(str(raw_dir / "*.zip")))
    if not zip_files:
        logger.error(f"未找到 ZIP 文件: {raw_dir}")
        return None

    dfs = []
    for zip_path in zip_files:
        with zipfile.ZipFile(zip_path, 'r') as z:
            for csv_name in z.namelist():
                if csv_name.endswith('.csv'):
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f, header=None)
                        dfs.append(df)
                        logger.debug(f"读取: {csv_name}")

    if not dfs:
        logger.error("未找到任何 CSV 文件")
        return None

    full_df = pd.concat(dfs, ignore_index=True)
    full_df.columns = [
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ]
    output_file = processed_dir / f"{symbol}_{interval}_raw.csv"
    full_df.to_csv(output_file, index=False)
    logger.info(f"合并完成: {output_file} ({len(full_df)} 行)")
    return str(output_file)

# 修复时间戳函数 (✅BTCUSDT 2025年之后的数据是微秒级，之前是毫秒级)
def normalize_timestamp(ts):
    """安全的时间戳标准化函数"""
    # 如果是字符串，尝试转换为数值
    if isinstance(ts, str):
        try:
            ts = float(ts)
        except (ValueError, TypeError):
            return pd.NA  # 无法转换的字符串返回 NaN
    # 如果是 NaN 或 None
    if pd.isna(ts):
        return ts
    # 现在确保 ts 是数值类型
    try:
        ts = float(ts)
    except (ValueError, TypeError):
        return pd.NA
    # 标准化时间戳
    if ts > 1e15:  # 微秒 → 毫秒
        return ts // 1000
    elif ts > 1e12:  # 毫秒 → 保留
        return ts
    elif ts > 1e9:  # 秒 → 毫秒
        return ts * 1000
    else:
        return ts

# 修复CSV文件
def data_format_repair(symbol, interval, config, logger):
    dirs = get_data_dirs(symbol, interval, config)
    processed_dir = dirs["processed"]
    input_file = processed_dir / f"{symbol}_{interval}_raw.csv"

    if not input_file.exists():
        logger.error(f"文件不存在: {input_file}")
        return None

    df = pd.read_csv(input_file, header=None)
    df.columns = [
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ]

    df['open_time'] = df['open_time'].apply(normalize_timestamp)
    df['close_time'] = df['close_time'].apply(normalize_timestamp)

    fixed_file = processed_dir / f"{symbol}_{interval}_clean.csv"
    df.to_csv(fixed_file, index=False)
    logger.info(f"修复完成: {fixed_file}")
    return str(fixed_file)

# 设定明确的时间边界（推荐用于金融数据！）例如:
# train_end_date = "2024-01-01"
# val_end_date = "2024-12-31"
def split_data_by_datetime(df, train_end_date, val_end_date):
    # 分割df
    train_df = df[df['open_time'] <= train_end_date]
    val_df = df[(df['open_time'] > train_end_date) & (df['open_time'] <= val_end_date)]
    test_df = df[df['open_time'] > val_end_date]
    print("训练集时间段:", train_df['open_time'].min(), "→", train_df['open_time'].max())
    print("验证集时间段:", val_df['open_time'].min(), "→", val_df['open_time'].max())
    print("测试集时间段:", test_df['open_time'].min(), "→", test_df['open_time'].max())
    return train_df, val_df, test_df


# 画出数据集切分图像
def plot_data_split(crypt_name, train_df, val_df, test_df, datasets_dir):
    plt.figure(figsize=(12, 4))
    plt.plot(train_df['open_time'], train_df['close'], label='Train', color='blue')
    plt.plot(val_df['open_time'], val_df['close'], label='Validation', color='orange')
    plt.plot(test_df['open_time'], test_df['close'], label='Test', color='red')
    print(train_df['close'])
    plt.axvline(val_df['open_time'].min(), color='k', linestyle='--', alpha=0.5)
    plt.axvline(test_df['open_time'].min(), color='k', linestyle='--', alpha=0.5)
    plt.legend()
    plt.title(f'{crypt_name} SPLIT')
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    #plt.show()
    plot_path = Path(datasets_dir) / f"{crypt_name}_split.png"
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close()
    return plot_path


# 数据集切分主函数
def data_split(symbol, interval, config, logger):
    processed_dir = PROJECT_ROOT / config["paths"]["processed_dir"]
    input_file = processed_dir / f"{symbol}_{interval}_clean.csv"

    if not input_file.exists():
        logger.error(f"文件不存在: {input_file}，请先运行修复步骤")
        return

    df = pd.read_csv(input_file)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    train_df, val_df, test_df = split_data_by_datetime(
        df,
        config["split"]["train_end"],
        config["split"]["val_end"]
    )

    dataset_dir = PROJECT_ROOT / config["paths"]["datasets_dir"] / f"{symbol}_{interval}"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    train_df.to_csv(dataset_dir / "train.csv", index=False)
    val_df.to_csv(dataset_dir / "val.csv", index=False)
    test_df.to_csv(dataset_dir / "test.csv", index=False)

    logger.info(f"数据集划分完成 | Train: {len(train_df)} Val: {len(val_df)} Test: {len(test_df)}")

    plot_path = plot_data_split(f"{symbol}_{interval}", train_df, val_df, test_df, dataset_dir)
    logger.info(f"划分图已保存: {plot_path}")