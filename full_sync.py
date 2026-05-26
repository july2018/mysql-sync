#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全量同步模块
使用 mysqldump 从源库导出数据，再通过 mysql 命令导入到目标库
支持：
  - 多库并发导出（可配置并行数）
  - 导出完成后记录当时的 Binlog 位点（供后续增量同步使用）
"""

import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import pymysql

logger = logging.getLogger("full_sync")


class FullSyncManager:
    def __init__(self, config: dict):
        self.config = config
        self.src = config["source"]
        self.tgt = config["target"]
        self.sync_cfg = config["sync"]
        self.full_cfg = self.sync_cfg["full_sync"]
        self.inc_cfg = self.sync_cfg["incremental"]

        self.mysqldump_bin = self._find_bin(
            self.full_cfg.get("mysqldump_bin", ""), "mysqldump"
        )
        self.mysql_bin = self._find_bin(
            self.full_cfg.get("mysql_bin", ""), "mysql"
        )
        self.parallel = self.full_cfg.get("parallel", 2)
        self.position_file = self.inc_cfg.get(
            "position_file", "logs/binlog_position.json"
        )

    def _find_bin(self, configured_path: str, name: str) -> str:
        """查找可执行文件路径"""
        if configured_path and os.path.isfile(configured_path):
            return configured_path
        found = shutil.which(name)
        if found:
            return found
        # Windows 常见路径
        candidates = [
            rf"C:\Program Files\MySQL\MySQL Server 8.0\bin\{name}.exe",
            rf"C:\Program Files\MySQL\MySQL Server 5.7\bin\{name}.exe",
            rf"C:\xampp\mysql\bin\{name}.exe",
        ]
        for c in candidates:
            if os.path.isfile(c):
                return c
        raise FileNotFoundError(
            f"找不到 {name}，请在配置文件 full_sync.{name}_bin 中指定路径"
        )

    def _get_source_conn(self) -> pymysql.Connection:
        return pymysql.connect(
            host=self.src["host"],
            port=self.src.get("port", 3306),
            user=self.src["user"],
            password=self.src["password"],
            charset=self.src.get("charset", "utf8mb4"),
            connect_timeout=10,
        )

    def _get_target_conn(self, database: Optional[str] = None) -> pymysql.Connection:
        kwargs = dict(
            host=self.tgt["host"],
            port=self.tgt.get("port", 3306),
            user=self.tgt["user"],
            password=self.tgt["password"],
            charset=self.tgt.get("charset", "utf8mb4"),
            connect_timeout=10,
        )
        if database:
            kwargs["database"] = database
        return pymysql.connect(**kwargs)

    def _get_databases(self) -> List[str]:
        """获取需要同步的数据库列表"""
        configured = self.sync_cfg.get("databases", [])
        if configured:
            return configured

        conn = self._get_source_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW DATABASES")
                system_dbs = {
                    "information_schema", "performance_schema",
                    "mysql", "sys"
                }
                return [
                    row[0] for row in cur.fetchall()
                    if row[0] not in system_dbs
                ]
        finally:
            conn.close()

    def _get_binlog_position(self) -> Tuple[str, int]:
        """获取源库当前 Binlog 位点（在全量导出前锁表获取，确保一致性）"""
        conn = self._get_source_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW MASTER STATUS")
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(
                        "无法获取 Binlog 位点，请确认源库已开启 Binlog（log_bin=ON）"
                    )
                return row[0], int(row[1])
        finally:
            conn.close()

    def _save_binlog_position(self, log_file: str, log_pos: int):
        """保存 Binlog 位点到文件"""
        os.makedirs(os.path.dirname(self.position_file), exist_ok=True)
        data = {
            "log_file": log_file,
            "log_pos": log_pos,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "note": "由全量同步完成后自动记录",
        }
        with open(self.position_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Binlog 位点已保存: {log_file}:{log_pos} -> {self.position_file}")

    def _ensure_database_exists(self, db_name: str):
        """在目标库创建数据库（若不存在）"""
        conn = self._get_target_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                    f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            conn.commit()
            logger.debug(f"目标库数据库已就绪: {db_name}")
        finally:
            conn.close()

    def _dump_and_restore_database(self, db_name: str) -> bool:
        """
        对单个数据库执行 mysqldump + mysql 导入
        返回 True 表示成功
        """
        logger.info(f"[{db_name}] 开始全量导出...")
        exclude_tables = self.sync_cfg.get("exclude_tables", [])

        # 构建 mysqldump 命令
        dump_cmd = [
            self.mysqldump_bin,
            f"-h{self.src['host']}",
            f"-P{self.src.get('port', 3306)}",
            f"-u{self.src['user']}",
            f"-p{self.src['password']}",
            "--single-transaction",       # InnoDB 一致性快照
            "--master-data=2",            # 在 dump 文件中注释 CHANGE MASTER TO
            "--set-gtid-purged=OFF",      # 避免 GTID 冲突
            "--skip-lock-tables",
            "--default-character-set=utf8mb4",
            "--hex-blob",
            "--routines",                 # 导出存储过程/函数
            "--triggers",                 # 导出触发器
            "--add-drop-table",           # 先 DROP 再 CREATE（幂等）
        ]

        # 排除特定表（只排除属于当前库的）
        for item in exclude_tables:
            parts = item.split(".", 1)
            if len(parts) == 2 and parts[0] == db_name:
                dump_cmd += [f"--ignore-table={item}"]

        dump_cmd.append(db_name)

        # 构建 mysql 导入命令
        restore_cmd = [
            self.mysql_bin,
            f"-h{self.tgt['host']}",
            f"-P{self.tgt.get('port', 3306)}",
            f"-u{self.tgt['user']}",
            f"-p{self.tgt['password']}",
            "--default-character-set=utf8mb4",
            db_name,
        ]

        try:
            self._ensure_database_exists(db_name)

            with tempfile.TemporaryFile(mode="w+b") as tmp:
                # 导出
                dump_proc = subprocess.Popen(
                    dump_cmd,
                    stdout=tmp,
                    stderr=subprocess.PIPE,
                )
                _, dump_err = dump_proc.communicate()
                if dump_proc.returncode != 0:
                    logger.error(
                        f"[{db_name}] mysqldump 失败: {dump_err.decode('utf-8', errors='replace')}"
                    )
                    return False

                # 获取 dump 文件大小
                tmp.seek(0, 2)
                size_mb = tmp.tell() / 1024 / 1024
                logger.info(f"[{db_name}] 导出完成，大小: {size_mb:.1f} MB，开始导入...")

                # 导入
                tmp.seek(0)
                restore_proc = subprocess.Popen(
                    restore_cmd,
                    stdin=tmp,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                _, restore_err = restore_proc.communicate()
                if restore_proc.returncode != 0:
                    logger.error(
                        f"[{db_name}] mysql 导入失败: {restore_err.decode('utf-8', errors='replace')}"
                    )
                    return False

            logger.info(f"[{db_name}] 全量同步完成 ✓")
            return True

        except Exception as e:
            logger.exception(f"[{db_name}] 全量同步异常: {e}")
            return False

    def run(self):
        """执行全量同步"""
        databases = self._get_databases()
        if not databases:
            logger.warning("没有找到需要同步的数据库，全量同步跳过")
            return

        logger.info(f"需要同步的数据库: {databases}")

        # 在开始前获取 Binlog 位点
        try:
            log_file, log_pos = self._get_binlog_position()
            logger.info(f"当前 Binlog 位点: {log_file}:{log_pos}")
        except Exception as e:
            logger.error(f"获取 Binlog 位点失败: {e}")
            raise

        start_time = time.time()
        success_count = 0
        fail_count = 0

        with ThreadPoolExecutor(max_workers=self.parallel) as executor:
            futures = {
                executor.submit(self._dump_and_restore_database, db): db
                for db in databases
            }
            for future in as_completed(futures):
                db = futures[future]
                try:
                    ok = future.result()
                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    logger.error(f"[{db}] 执行异常: {e}")
                    fail_count += 1

        elapsed = time.time() - start_time
        logger.info(
            f"全量同步完成: 成功 {success_count} 个库, 失败 {fail_count} 个库, "
            f"耗时 {elapsed:.1f}s"
        )

        if fail_count > 0:
            raise RuntimeError(f"有 {fail_count} 个数据库全量同步失败，请检查日志")

        # 全量完成后保存 Binlog 位点
        self._save_binlog_position(log_file, log_pos)
