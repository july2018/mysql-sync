#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增量同步模块
通过 python-mysql-replication 伪装成 MySQL 从库，订阅 Binlog 事件
支持：
  - WriteRowsEvent  → INSERT
  - UpdateRowsEvent → UPDATE
  - DeleteRowsEvent → DELETE
  - 位点持久化，断点续传
  - 自动重连
  - 批量写入（减少目标库 RTT）
  - 多库/表过滤
"""

import json
import logging
import os
import queue
import time
import threading
from typing import Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import QueryEvent, RotateEvent

logger = logging.getLogger("incremental_sync")


# ---------------------------------------------------------------------------
# 位点管理
# ---------------------------------------------------------------------------

class BinlogPosition:
    """Binlog 位点管理器（读写持久化文件）"""

    def __init__(self, position_file: str):
        self.position_file = position_file
        self._log_file: Optional[str] = None
        self._log_pos: Optional[int] = None
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if os.path.exists(self.position_file):
            try:
                with open(self.position_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._log_file = data.get("log_file")
                self._log_pos = data.get("log_pos")
                logger.info(
                    f"已加载 Binlog 位点: {self._log_file}:{self._log_pos}"
                    f"（记录于 {data.get('timestamp', '未知时间')}）"
                )
            except Exception as e:
                logger.warning(f"加载位点文件失败，将从头读取: {e}")

    def get(self) -> Tuple[Optional[str], Optional[int]]:
        with self._lock:
            return self._log_file, self._log_pos

    def update(self, log_file: str, log_pos: int):
        with self._lock:
            self._log_file = log_file
            self._log_pos = log_pos

    def save(self):
        with self._lock:
            if self._log_file is None:
                return
            os.makedirs(os.path.dirname(self.position_file), exist_ok=True)
            data = {
                "log_file": self._log_file,
                "log_pos": self._log_pos,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            # 原子写入（先写临时文件再 rename）
            tmp_path = self.position_file + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.position_file)

    def reset(self):
        """删除位点文件，下次从当前位置开始"""
        with self._lock:
            self._log_file = None
            self._log_pos = None
            if os.path.exists(self.position_file):
                os.remove(self.position_file)
                logger.warning(f"已删除 Binlog 位点文件: {self.position_file}")


# ---------------------------------------------------------------------------
# 目标库写入器
# ---------------------------------------------------------------------------

class TargetWriter:
    """负责将解析出的 DML 事件写入目标库"""

    def __init__(self, config: dict):
        self.tgt = config["target"]
        self._conn: Optional[pymysql.Connection] = None
        self._connect()

    def _connect(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = pymysql.connect(
            host=self.tgt["host"],
            port=self.tgt.get("port", 3306),
            user=self.tgt["user"],
            password=self.tgt["password"],
            charset=self.tgt.get("charset", "utf8mb4"),
            connect_timeout=10,
            autocommit=False,
        )
        logger.debug("目标库连接建立")

    def _ensure_connected(self):
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            logger.warning("目标库连接断开，尝试重连...")
            self._connect()

    def _build_insert(self, schema: str, table: str, row: dict) -> Tuple[str, list]:
        cols = list(row.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(f"`{c}`" for c in cols)
        sql = (
            f"INSERT INTO `{schema}`.`{table}` ({col_names}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE "
            + ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols)
        )
        return sql, list(row.values())

    def _build_update(
        self, schema: str, table: str, before: dict, after: dict, pk_cols: List[str]
    ) -> Tuple[str, list]:
        set_clause = ", ".join(f"`{k}`=%s" for k in after.keys())
        where_clause = " AND ".join(f"`{k}`=%s" for k in pk_cols)
        sql = (
            f"UPDATE `{schema}`.`{table}` SET {set_clause} WHERE {where_clause}"
        )
        params = list(after.values()) + [before[k] for k in pk_cols]
        return sql, params

    def _build_delete(
        self, schema: str, table: str, row: dict, pk_cols: List[str]
    ) -> Tuple[str, list]:
        where_clause = " AND ".join(f"`{k}`=%s" for k in pk_cols)
        sql = f"DELETE FROM `{schema}`.`{table}` WHERE {where_clause}"
        params = [row[k] for k in pk_cols]
        return sql, params

    def _get_primary_keys(self, schema: str, table: str) -> List[str]:
        """从目标库获取主键列（也可以从事件本身推断，这里查询目标库元数据）"""
        self._ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
                "ORDER BY ORDINAL_POSITION",
                (schema, table),
            )
            rows = cur.fetchall()
            return [r[0] for r in rows]

    # 缓存主键，避免频繁查询 information_schema
    _pk_cache: Dict[str, List[str]] = {}

    def get_primary_keys(self, schema: str, table: str) -> List[str]:
        key = f"{schema}.{table}"
        if key not in self._pk_cache:
            self._pk_cache[key] = self._get_primary_keys(schema, table)
        return self._pk_cache[key]

    def apply_batch(self, events: list):
        """批量执行一批 DML 事件"""
        if not events:
            return
        self._ensure_connected()
        try:
            with self._conn.cursor() as cur:
                for event_type, schema, table, row_data in events:
                    if event_type == "INSERT":
                        sql, params = self._build_insert(schema, table, row_data)
                        cur.execute(sql, params)

                    elif event_type == "UPDATE":
                        before, after = row_data
                        pk_cols = self.get_primary_keys(schema, table)
                        if not pk_cols:
                            # 无主键时退化为全列匹配
                            pk_cols = list(before.keys())
                        sql, params = self._build_update(
                            schema, table, before, after, pk_cols
                        )
                        cur.execute(sql, params)

                    elif event_type == "DELETE":
                        pk_cols = self.get_primary_keys(schema, table)
                        if not pk_cols:
                            pk_cols = list(row_data.keys())
                        sql, params = self._build_delete(
                            schema, table, row_data, pk_cols
                        )
                        cur.execute(sql, params)

            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            logger.error(f"批量写入失败，已回滚: {e}")
            raise

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 增量同步主类
# ---------------------------------------------------------------------------

class IncrementalSyncManager:
    def __init__(self, config: dict):
        self.config = config
        self.src = config["source"]
        self.sync_cfg = config["sync"]
        self.inc_cfg = self.sync_cfg["incremental"]

        self.server_id = self.inc_cfg.get("server_id", 9999)
        self.reconnect_interval = self.inc_cfg.get("reconnect_interval", 5)
        self.batch_size = self.inc_cfg.get("batch_size", 100)
        self.batch_timeout = self.inc_cfg.get("batch_timeout", 2)

        position_file = self.inc_cfg.get(
            "position_file", "logs/binlog_position.json"
        )
        self.position = BinlogPosition(position_file)
        self.writer = TargetWriter(config)

        # 过滤配置
        self._only_schemas: Optional[Set[str]] = None
        configured_dbs = self.sync_cfg.get("databases", [])
        if configured_dbs:
            self._only_schemas = set(configured_dbs)

        self._exclude_tables: Set[str] = set(
            self.sync_cfg.get("exclude_tables", [])
        )

        self._running = True
        self._event_queue: queue.Queue = queue.Queue(maxsize=10000)

    def reset_position(self):
        self.position.reset()

    def _should_sync(self, schema: str, table: str) -> bool:
        """判断该表是否需要同步"""
        if self._only_schemas and schema not in self._only_schemas:
            return False
        fqn = f"{schema}.{table}"
        if fqn in self._exclude_tables:
            return False
        return True

    def _build_stream(self) -> BinLogStreamReader:
        log_file, log_pos = self.position.get()
        stream_kwargs = dict(
            connection_settings={
                "host": self.src["host"],
                "port": self.src.get("port", 3306),
                "user": self.src["user"],
                "passwd": self.src["password"],
            },
            server_id=self.server_id,
            blocking=True,
            resume_stream=True,
            only_events=[
                WriteRowsEvent,
                UpdateRowsEvent,
                DeleteRowsEvent,
                QueryEvent,
                RotateEvent,
            ],
        )
        if log_file and log_pos:
            stream_kwargs["log_file"] = log_file
            stream_kwargs["log_pos"] = log_pos
            logger.info(f"从位点 {log_file}:{log_pos} 开始读取 Binlog")
        else:
            logger.info("未找到历史位点，从当前 Binlog 末尾开始读取")

        if self._only_schemas:
            stream_kwargs["only_schemas"] = list(self._only_schemas)

        return BinLogStreamReader(**stream_kwargs)

    def _consumer_thread(self):
        """批量消费队列中的事件并写入目标库"""
        batch = []
        last_flush = time.time()

        while self._running:
            try:
                item = self._event_queue.get(timeout=0.1)
            except queue.Empty:
                item = None

            if item is not None:
                if item == "STOP":
                    # 刷新剩余数据后退出
                    if batch:
                        self._flush_batch(batch)
                    break
                batch.append(item)

            should_flush = (
                len(batch) >= self.batch_size
                or (batch and time.time() - last_flush >= self.batch_timeout)
            )

            if should_flush:
                self._flush_batch(batch)
                batch = []
                last_flush = time.time()

    def _flush_batch(self, batch: list):
        """将批次写入目标库并保存位点"""
        try:
            self.writer.apply_batch(batch)
            self.position.save()
            logger.debug(f"已提交 {len(batch)} 条事件")
        except Exception as e:
            logger.error(f"批量写入失败: {e}")
            # 可在此添加重试逻辑或死信队列

    def run(self):
        """增量同步主循环"""
        # 启动消费者线程
        consumer = threading.Thread(
            target=self._consumer_thread, daemon=True, name="consumer"
        )
        consumer.start()

        while self._running:
            try:
                stream = self._build_stream()
                logger.info(
                    f"已连接到源库 {self.src['host']}:{self.src.get('port', 3306)}，"
                    f"server_id={self.server_id}，开始监听 Binlog..."
                )

                for binlog_event in stream:
                    if not self._running:
                        break

                    # 更新位点（无论是否同步该事件）
                    if hasattr(binlog_event, "packet") and hasattr(
                        binlog_event.packet, "log_pos"
                    ):
                        self.position.update(
                            stream.log_file, binlog_event.packet.log_pos
                        )

                    # 处理 Rotate 事件（切换 Binlog 文件）
                    if isinstance(binlog_event, RotateEvent):
                        logger.info(
                            f"Binlog 文件切换: {binlog_event.next_binlog} "
                            f"pos={binlog_event.position}"
                        )
                        self.position.update(
                            binlog_event.next_binlog, binlog_event.position
                        )
                        continue

                    # 只处理行事件
                    if not isinstance(
                        binlog_event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)
                    ):
                        continue

                    schema = binlog_event.schema
                    table = binlog_event.table

                    if not self._should_sync(schema, table):
                        continue

                    for row in binlog_event.rows:
                        if isinstance(binlog_event, WriteRowsEvent):
                            self._event_queue.put(
                                ("INSERT", schema, table, row["values"])
                            )
                        elif isinstance(binlog_event, UpdateRowsEvent):
                            self._event_queue.put(
                                (
                                    "UPDATE",
                                    schema,
                                    table,
                                    (row["before_values"], row["after_values"]),
                                )
                            )
                        elif isinstance(binlog_event, DeleteRowsEvent):
                            self._event_queue.put(
                                ("DELETE", schema, table, row["values"])
                            )

                stream.close()

            except KeyboardInterrupt:
                logger.info("收到中断信号，停止增量同步")
                self._running = False
                break
            except Exception as e:
                logger.error(
                    f"Binlog 流中断: {e}，{self.reconnect_interval}s 后重连..."
                )
                time.sleep(self.reconnect_interval)

        # 通知消费者退出
        self._event_queue.put("STOP")
        consumer.join(timeout=10)
        self.writer.close()
        logger.info("增量同步已停止")
