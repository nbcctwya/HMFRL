# 从Binance下载原始数据
import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta


def download_binance_monthly_klines(
        symbol="BTCUSDT",
        interval="1m",
        start_date="2020-01",
        end_date="2025-09",  # 修改为你需要的截止年月
        save_dir="./binance_data"
):
    # 创建保存目录
    os.makedirs(save_dir, exist_ok=True)

    # 解析起止时间
    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")

    current = start
    while current <= end:
        year = current.year
        month = current.month
        day = current.day
        filename = f"{symbol}-{interval}-{year}-{month:02d}.zip"
        url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/{interval}/{filename}"

        filepath = os.path.join(save_dir, filename)

        if os.path.exists(filepath):
            print(f"✅ 已存在，跳过: {filename}")
        else:
            print(f"📥 正在下载: {filename}")
            try:
                response = requests.get(url, stream=True, timeout=30)
                if response.status_code == 200:
                    with open(filepath, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"✔️ 下载完成: {filename}")
                else:
                    print(f"❌ 文件不存在或网络错误: {url} (状态码: {response.status_code})")
            except Exception as e:
                print(f"⚠️ 下载失败 {filename}: {e}")

        # 下一个月
        current += relativedelta(months=1)


# 下载 BTCUSDT 从2020年1月到2025年10月的分钟数据
download_binance_monthly_klines(
    symbol="BTCUSDT",
    interval="1m",
    start_date="2020-01",
    end_date="2025-09",
    save_dir="./btc_1m_data"
)
# 下载 BTCUSDT 从2020年1月到2025年10月的日频数据
download_binance_monthly_klines(
    symbol="BTCUSDT",
    interval="1d",
    start_date="2020-01",
    end_date="2025-09",
    save_dir="./btc_1d_data"
)