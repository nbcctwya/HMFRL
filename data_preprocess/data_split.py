import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

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

# 将拆分后的数据存入csv文件中
def save2csv(original_filename, train_df, val_df, test_df):
    p = Path(original_filename)
    train_df.to_csv(f'{p.stem}_train{p.suffix}', index=False)
    val_df.to_csv(f'{p.stem}_val{p.suffix}', index=False)
    test_df.to_csv(f'{p.stem}_test{p.suffix}', index=False)

# 画出数据集切分图像
def plot_data_split(crypt_name, train_df, val_df, test_df):
    plt.figure(figsize=(12, 4))
    plt.plot(train_df['open_time'], train_df['close'], label='Train', color='blue')
    plt.plot(val_df['open_time'], val_df['close'], label='Validation', color='orange')
    plt.plot(test_df['open_time'], test_df['close'], label='Test', color='red')
    plt.axvline(val_df['open_time'].min(), color='k', linestyle='--', alpha=0.5)
    plt.axvline(test_df['open_time'].min(), color='k', linestyle='--', alpha=0.5)
    plt.legend()
    plt.title(f'{crypt_name} SPLIT')
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.show()

# 数据集切分主函数
def data_split(filename, symbol="BTCUSDT", interval="1d",train_end_date = "2024-01-01", val_end_date = "2024-12-31"):
    df = pd.read_csv(filename)
    # 将时间戳转为 datetime
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    train_df, val_df, test_df = split_data_by_datetime(df, train_end_date, val_end_date)
    save2csv(filename, train_df, val_df, test_df)
    plot_data_split(filename, train_df, val_df, test_df)


data_split("BTCUSDT_1m_2020-2025_fixed.csv", train_end_date="2024-01-01", val_end_date="2024-12-31")