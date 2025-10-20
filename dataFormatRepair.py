import pandas as pd
from pathlib import Path

# 修复时间戳函数 (✅BTCUSDT 2025年之后的数据是微秒级，之前是毫秒级)
def normalize_timestamp(ts):
    if pd.isna(ts):
        return ts
    if ts > 1e15:       # 微秒 → 毫秒
        return ts // 1000
    elif ts > 1e12:     # 毫秒 → 保留
        return ts
    elif ts > 1e9:      # 秒 → 毫秒
        return ts * 1000
    else:
        return ts

# 修复CSV文件
def data_format_repair(filename):
    df = pd.read_csv(filename)
    # 修复数据
    df['open_time'] = df['open_time'].apply(normalize_timestamp)
    df['close_time'] = df['close_time'].apply(normalize_timestamp)
    # # （可选）过滤异常时间
    # df = df[
    #     (df['open_time'] >= 1262304000000) &
    #     (df['open_time'] <= 1924934400000)
    # ]
    p = Path(filename)
    if p.suffix == '.csv':
        # 如果已有 .csv 后缀，在 .csv 前插入 _fixed
        fixed_filename = p.stem + '_fixed' + p.suffix
    else:
        # 如果没有 .csv 后缀，直接加上 _fixed.csv
        fixed_filename = p.name + '_fixed.csv'
    # 将修改后的数据写入相应_fixed文件 不写索引（index=False）
    df.to_csv(fixed_filename, index=False)
    print(f'✅ 修复完成！已保存为 {fixed_filename}')

data_format_repair('BTCUSDT_1m_2020-2025.csv')