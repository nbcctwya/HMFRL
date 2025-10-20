# tools/utils.py
import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logger(log_dir: str = "logs", name: str = "pipeline"):
    """设置日志系统"""
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True)

    # 日志文件名包含日期
    log_file = log_dir / f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )
    return logging.getLogger(name)