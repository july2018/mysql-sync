#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
环境检查工具
运行前验证源库/目标库连接、权限、Binlog 配置等是否满足要求
"""

import sys
import os

import pymysql
import yaml


def check_source(config: dict):
    src = config["source"]
    print(f"\n{'='*50}")
    print(f"检查源库: {src['host']}:{src.get('port', 3306)}")
    print(f"{'='*50}")

    conn = pymysql.connect(
        host=src["host"],
        port=src.get("port", 3306),
        user=src["user"],
        password=src["password"],
        charset=src.get("charset", "utf8mb4"),
        connect_timeout=5,
    )
    print("✓ 连接成功")

    with conn.cursor() as cur:
        # 检查 Binlog 是否开启
        cur.execute("SHOW VARIABLES LIKE 'log_bin'")
        row = cur.fetchone()
        if row and row[1].upper() == "ON":
            print("✓ log_bin = ON")
        else:
            print("✗ log_bin 未开启！请在源库 my.cnf 中设置 log_bin=ON")

        # 检查 binlog_format
        cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
        row = cur.fetchone()
        fmt = row[1] if row else "UNKNOWN"
        if fmt.upper() == "ROW":
            print(f"✓ binlog_format = ROW")
        else:
            print(
                f"✗ binlog_format = {fmt}（需要 ROW 格式）\n"
                f"  请在源库 my.cnf 中设置 binlog_format=ROW"
            )

        # 检查 binlog_row_image
        cur.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        row = cur.fetchone()
        img = row[1] if row else "FULL"
        print(f"✓ binlog_row_image = {img}")
        if img.upper() != "FULL":
            print(f"  建议设置为 FULL 以确保 UPDATE/DELETE 能匹配到完整行数据")

        # 获取 Binlog 位点
        cur.execute("SHOW MASTER STATUS")
        row = cur.fetchone()
        if row:
            print(f"✓ 当前 Binlog: {row[0]}:{row[1]}")
        else:
            print("✗ 无法获取 Binlog 状态")

        # 检查权限
        cur.execute("SHOW GRANTS FOR CURRENT_USER()")
        grants = [r[0] for r in cur.fetchall()]
        has_repl = any(
            "REPLICATION SLAVE" in g or "ALL PRIVILEGES" in g for g in grants
        )
        has_client = any(
            "REPLICATION CLIENT" in g or "ALL PRIVILEGES" in g for g in grants
        )
        print(f"{'✓' if has_repl else '✗'} REPLICATION SLAVE 权限")
        print(f"{'✓' if has_client else '✗'} REPLICATION CLIENT 权限")

        if not has_repl or not has_client:
            print(
                "\n  修复权限 SQL（在源库 root 账号执行）:\n"
                f"  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* "
                f"TO '{src['user']}'@'%';\n"
                f"  FLUSH PRIVILEGES;"
            )

        # server_id
        cur.execute("SHOW VARIABLES LIKE 'server_id'")
        row = cur.fetchone()
        src_server_id = int(row[1]) if row else 0
        sync_server_id = config["sync"]["incremental"].get("server_id", 9999)
        if src_server_id != sync_server_id:
            print(f"✓ server_id 不冲突 (源库={src_server_id}, 伪从库={sync_server_id})")
        else:
            print(
                f"✗ server_id 冲突！源库 server_id={src_server_id}，"
                f"请修改配置中 sync.incremental.server_id"
            )

    conn.close()


def check_target(config: dict):
    tgt = config["target"]
    print(f"\n{'='*50}")
    print(f"检查目标库: {tgt['host']}:{tgt.get('port', 3306)}")
    print(f"{'='*50}")

    conn = pymysql.connect(
        host=tgt["host"],
        port=tgt.get("port", 3306),
        user=tgt["user"],
        password=tgt["password"],
        charset=tgt.get("charset", "utf8mb4"),
        connect_timeout=5,
    )
    print("✓ 连接成功")

    with conn.cursor() as cur:
        cur.execute("SHOW GRANTS FOR CURRENT_USER()")
        grants_raw = [r[0] for r in cur.fetchall()]
        all_grants = " ".join(grants_raw).upper()

        for priv in ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "INDEX", "ALTER"]:
            ok = priv in all_grants or "ALL PRIVILEGES" in all_grants
            print(f"{'✓' if ok else '✗'} {priv} 权限")

    conn.close()


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    if not os.path.exists(config_path):
        print(f"配置文件不存在: {config_path}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    print("MySQL 同步程序 - 环境检查")

    errors = []
    try:
        check_source(config)
    except Exception as e:
        print(f"✗ 源库检查失败: {e}")
        errors.append(str(e))

    try:
        check_target(config)
    except Exception as e:
        print(f"✗ 目标库检查失败: {e}")
        errors.append(str(e))

    print(f"\n{'='*50}")
    if errors:
        print("检查结果: 存在问题，请修复后再运行同步程序")
        sys.exit(1)
    else:
        print("检查结果: 一切正常，可以运行同步程序 ✓")


if __name__ == "__main__":
    main()
