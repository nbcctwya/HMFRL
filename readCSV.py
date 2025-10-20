import pandas as pd

# 只读取需要的列（比如时间、收盘价）
df = pd.read_csv(
    "BTCUSDT_1d_2020-2025_fixed.csv",
    usecols=["open_time", "close"],
    dtype={"close": "float32"}  # 节省内存
)

# 将时间戳转为 datetime
df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")

# 筛选 2023 年的数据
df_2023 = df[(df["open_time"] >= "2025-01-01") & (df["open_time"] < "2025-06-01")]

print(df_2023)