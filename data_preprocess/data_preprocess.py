import download_data
import data_format_repair
import data_split
import read_csv
import zip2csv

import os
from pathlib import Path

#项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "binance" / "spot"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "csv"



# 下载 BTCUSDT 从2020年1月到2025年10月的日频数据
download_data.download_binance_monthly_klines(
    symbol="BTCUSDT",
    interval="1d",
    start_date="2020-01",
    end_date="2025-09",
    save_dir="./btc_1d_data"
)

zip2csv.merge_binance_klines(data_dir="./btc_1d_data", output_file="BTCUSDT_1d_2020-2025.csv")

