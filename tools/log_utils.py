# tools/log_utils.py
import logging
from pathlib import Path
from datetime import datetime


def setup_logger(logs_dir, name="data_pipeline"):
    """
    设置日志系统

    Args:
        logs_dir: 日志目录路径（字符串或 Path）
        name: 日志名称
    """
    logs_path = Path(logs_dir)
    logs_path.mkdir(parents=True, exist_ok=True)  # 自动创建多级目录

    log_file = logs_path / f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(name)