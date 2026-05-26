#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL 数据同步程序
通过伪装成 Slave（从库）的方式连接主库，实现：
  1. 全量初始化同步（mysqldump 方式）
  2. 增量实时同步（Binlog 解析方式）
"""

import argparse
import logging
import logging.handlers
import os
import sys

import yaml

from full_sync import FullSyncManager
from incremental_sync import IncrementalSyncManager


def setup_logging(config: dict):
    """配置日志"""
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file = log_cfg.get("file", "logs/sync.log")
    max_bytes = log_cfg.get("max_bytes", 10 * 1024 * 1024)
    backup_count = log_cfg.get("backup_count", 5)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # 控制台输出
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    root_logger.addHandler(console_handler)

    # 文件输出（滚动）
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    root_logger.addHandler(file_handler)


def load_config(config_path: str) -> dict:
    """加载 YAML 配置文件"""
    if not os.path.exists(config_path):
        print(f"[ERROR] 配置文件不存在: {config_path}")
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(
        description="MySQL 主从伪装同步工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 仅执行全量同步
  python main.py --config config.yaml --mode full

  # 仅执行增量同步（需先完成全量同步）
  python main.py --config config.yaml --mode incremental

  # 先全量同步，完成后自动切换到增量同步
  python main.py --config config.yaml --mode all
        """,
    )
    parser.add_argument(
        "--config", default="config.yaml", help="配置文件路径 (默认: config.yaml)"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "all"],
        default="all",
        help="运行模式: full=仅全量, incremental=仅增量, all=全量后增量 (默认: all)",
    )
    parser.add_argument(
        "--reset-position",
        action="store_true",
        help="重置 Binlog 位点，从当前位置开始增量（谨慎使用）",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config)
    logger = logging.getLogger("main")

    logger.info("=" * 60)
    logger.info("MySQL 同步程序启动")
    logger.info(f"运行模式: {args.mode}")
    logger.info(
        f"源库: {config['source']['host']}:{config['source']['port']}"
    )
    logger.info(
        f"目标库: {config['target']['host']}:{config['target']['port']}"
    )
    logger.info("=" * 60)

    try:
        # 全量同步阶段
        if args.mode in ("full", "all"):
            if config["sync"]["full_sync"].get("enabled", True):
                logger.info("开始全量同步...")
                full_mgr = FullSyncManager(config)
                full_mgr.run()
                logger.info("全量同步完成")
            else:
                logger.info("全量同步已禁用，跳过")

        # 增量同步阶段
        if args.mode in ("incremental", "all"):
            if config["sync"]["incremental"].get("enabled", True):
                logger.info("开始增量同步（监听 Binlog）...")
                inc_mgr = IncrementalSyncManager(config)
                if args.reset_position:
                    logger.warning("--reset-position 已指定，将重置 Binlog 位点")
                    inc_mgr.reset_position()
                inc_mgr.run()
            else:
                logger.info("增量同步已禁用，跳过")

    except KeyboardInterrupt:
        logger.info("收到中断信号，程序退出")
    except Exception as e:
        logger.exception(f"程序异常退出: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
