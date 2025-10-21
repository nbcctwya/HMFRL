import sys
from pathlib import Path
import pandas as pd
PROJECT_ROOT = Path(__file__).parent.parent
# 将 PROJECT_ROOT 目录插入到 Python 模块搜索路径的最前面，使得 Python 在导入模块时优先从该目录查找
sys.path.insert(0, str(PROJECT_ROOT))

from data_utils import(
    load_config,
    download_binance_klines,
    merge_binance_klines,
    data_format_repair,
    data_split
)
from tools.log_utils import setup_logger


def main():
    # 读取你的 raw 文件
    df_raw = pd.read_csv("../data/processed/csv/BTCUSDT_1d_raw.csv", header=None)
    print("原始数据前3行:")
    print(df_raw.head(3))
    print("\nopen_time 列的数据类型:", df_raw[0].dtype)
    print("open_time 列的唯一值类型:")
    print(df_raw[0].apply(type).value_counts())

    # 应用修复前检查
    df_raw.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume',
                      'close_time', 'quote_asset_volume', 'number_of_trades',
                      'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore']

    # 检查转换为数值后的结果
    open_time_numeric = pd.to_numeric(df_raw['open_time'], errors='coerce')
    print(f"\n转换为数值后，NaN 数量: {open_time_numeric.isna().sum()}")
    print("前10个数值:")
    print(open_time_numeric.head(10))


    # 加载配置
    config_path = PROJECT_ROOT / "config.yaml"
    # 验证配置文件是否存在
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    config = load_config(str(config_path))

    # 设置日志（使用配置中的 logs_dir）
    logger = setup_logger(config["paths"]["logs_dir"], "pipeline")

    logger.info("🚀 启动数据管道（严格模式：仅当完整下载时才处理）")
    logger.info(f"配置路径: raw={config['paths']['raw_dir']}, processed={config['paths']['processed_dir']}")

    for symbol in config["data"]["symbols"]:
        for interval in config["data"]["intervals"]:
            logger.info(f"🔄 处理 {symbol} {interval}")
            # 1. 下载并检查完整性
            download_result = download_binance_klines(symbol, interval, config, logger)

            if not download_result["is_complete"]:
                logger.warning(
                    f"⚠️ 跳过 {symbol} {interval} 的后续处理（{download_result['failed']} 个文件缺失）"
                )
                raise ValueError(f"⚠️ 跳过 {symbol} {interval} 的后续处理（{download_result['failed']} 个文件缺失）")

            # 2. 合并（现在可以安全合并，因为所有文件都存在）
            merged_file = merge_binance_klines(symbol, interval, config, logger)
            if merged_file is None:
                logger.error(f"❌ 合并失败，跳过后续步骤: {symbol} {interval}")
                raise RuntimeError(f"❌ 合并失败，跳过后续步骤: {symbol} {interval}")

            # 3. 修复
            repaired_file = data_format_repair(symbol, interval, config, logger)
            if repaired_file is None:
                raise RuntimeError("repaired failed")

            # 4. 划分
            data_split(symbol, interval, config, logger)

            logger.info(f"✅ 完成 {symbol} {interval}")
            # try:
            #     # 1. 下载并检查完整性
            #     download_result = download_binance_klines(symbol, interval, config, logger)
            #
            #     if not download_result["is_complete"]:
            #         logger.warning(
            #             f"⚠️ 跳过 {symbol} {interval} 的后续处理（{download_result['failed']} 个文件缺失）"
            #         )
            #         raise ValueError(f"⚠️ 跳过 {symbol} {interval} 的后续处理（{download_result['failed']} 个文件缺失）")
            #
            #     # 2. 合并（现在可以安全合并，因为所有文件都存在）
            #     merged_file = merge_binance_klines(symbol, interval, config, logger)
            #     if merged_file is None:
            #         logger.error(f"❌ 合并失败，跳过后续步骤: {symbol} {interval}")
            #         raise RuntimeError(f"❌ 合并失败，跳过后续步骤: {symbol} {interval}")
            #
            #     # 3. 修复
            #     repaired_file = data_format_repair(symbol, interval, config, logger)
            #     if repaired_file is None:
            #         raise RuntimeError("repaired failed")
            #
            #     # 4. 划分
            #     data_split(symbol, interval, config, logger)
            #
            #     logger.info(f"✅ 完成 {symbol} {interval}")
            #
            # except Exception as e:
            #     logger.error(f"💥 处理 {symbol} {interval} 时发生严重错误: {e}")

    logger.info("✅ 数据管道执行完成")


if __name__ == "__main__":
    main()