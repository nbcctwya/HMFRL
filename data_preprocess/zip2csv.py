import pandas as pd
import glob
import os
import zipfile

# 多个.zip文件合并为CSV文件
def merge_binance_klines(data_dir="./btc_1m_data", output_file="BTCUSDT_1m_full.csv"):
    all_files = sorted(glob.glob(os.path.join(data_dir, "*.zip")))
    dfs = []

    for zip_path in all_files:
        with zipfile.ZipFile(zip_path, 'r') as z:
            for csv_name in z.namelist():
                if csv_name.endswith('.csv'):
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f, header=None)
                        dfs.append(df)
                        print(f"✅ 已读取: {csv_name}")

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
        # 添加列名（Binance 官方格式）
        full_df.columns = [
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
        ]
        full_df.to_csv(output_file, index=False)
        print(f"🎉 合并完成！总行数: {len(full_df)}，保存为: {output_file}")
    else:
        print("❌ 未找到任何 CSV 文件")


# 执行合并
# merge_binance_klines(data_dir="./btc_1m_data", output_file="BTCUSDT_1m_2020-2025.csv")


# merge_binance_klines(data_dir="./btc_1d_data", output_file="BTCUSDT_1d_2020-2025.csv")