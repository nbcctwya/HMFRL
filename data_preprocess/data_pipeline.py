import sys
from pathlib import Path

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
    # 加载配置
    config_path = PROJECT_ROOT / "config.yaml"
    config = load_config(str(config_path))

    # 设置日志（使用配置中的 logs_dir）
    logger = setup_logger(config["paths"]["logs_dir"], "pipeline")

    logger.info("🚀 启动数据管道")
    logger.info(f"配置路径: raw={config['paths']['raw_dir']}, processed={config['paths']['processed_dir']}")

    for symbol in config["data"]["symbols"]:
        for interval in config["data"]["intervals"]:
            logger.info(f"处理 {symbol} {interval}")

            download_binance_klines(symbol, interval, config, logger)
            merge_binance_klines(symbol, interval, config, logger)
            data_format_repair(symbol, interval, config, logger)
            data_split(symbol, interval, config, logger)

    logger.info("✅ 所有任务完成！")


if __name__ == "__main__":
    main()