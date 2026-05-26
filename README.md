# MySQL 数据同步程序

通过**伪装成从库（Slave）**的方式连接 MySQL 主库，实现：
- **全量初始化同步**：基于 `mysqldump` 导出 + `mysql` 导入
- **增量实时同步**：解析 Binlog ROW 事件，写入目标库（支持 INSERT/UPDATE/DELETE）

---

## 项目结构

```
mysql-sync/
├── main.py              # 主入口，解析参数，驱动全量/增量流程
├── full_sync.py         # 全量同步模块（mysqldump）
├── incremental_sync.py  # 增量同步模块（Binlog 伪从库）
├── check_env.py         # 环境检查工具
├── config.yaml          # 配置文件
├── requirements.txt     # Python 依赖
└── logs/                # 日志和位点文件（自动创建）
    ├── sync.log
    └── binlog_position.json
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

> 需要 Python 3.8+，以及本机已安装 `mysqldump` / `mysql` 命令行工具

### 2. 配置源库和目标库

编辑 `config.yaml`：

```yaml
source:
  host: "192.168.1.100"
  port: 3306
  user: "repl_user"
  password: "repl_password"

target:
  host: "192.168.1.200"
  port: 3306
  user: "sync_user"
  password: "sync_password"

sync:
  databases:
    - "mydb"         # 指定要同步的数据库（留空则同步全部）
  incremental:
    server_id: 9999  # 必须与主库及其他从库不同
```

### 3. 配置源库权限

在源库以 root 账号执行：

```sql
-- 创建同步账号
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'repl_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;
```

源库 `my.cnf` 必须包含：

```ini
[mysqld]
server-id       = 1
log_bin         = /var/lib/mysql/mysql-bin.log
binlog_format   = ROW
binlog_row_image = FULL
```

### 4. 环境检查

```bash
python check_env.py config.yaml
```

### 5. 运行同步

```bash
# 先全量初始化，完成后自动切换增量（推荐首次使用）
python main.py --config config.yaml --mode all

# 仅全量同步
python main.py --config config.yaml --mode full

# 仅增量同步（必须已完成全量同步）
python main.py --config config.yaml --mode incremental
```

---

## 工作原理

### 全量同步流程

```
1. 查询源库获取当前 Binlog 位点（log_file + log_pos）
2. mysqldump --single-transaction 导出各数据库
3. mysql 导入到目标库（先建库）
4. 将步骤1的位点保存到 logs/binlog_position.json
```

通过在 `--single-transaction` 快照开始前记录位点，确保全量数据与后续增量数据**无缝衔接，不重不漏**。

### 增量同步流程

```
1. 读取 binlog_position.json 中保存的位点
2. 以 server_id=9999 向主库注册（伪装成从库）
3. 接收 Binlog ROW 事件流：
   WriteRowsEvent  → INSERT ... ON DUPLICATE KEY UPDATE
   UpdateRowsEvent → UPDATE ... WHERE <主键>
   DeleteRowsEvent → DELETE ... WHERE <主键>
4. 批量写入目标库（默认每 100 条或 2s 提交一次）
5. 每次提交后更新 binlog_position.json（断点续传）
```

### 关键特性

| 特性 | 说明 |
|------|------|
| **断点续传** | 位点持久化到文件，重启后从上次位置继续 |
| **自动重连** | 网络中断后自动重连主库 |
| **批量写入** | 减少目标库 RTT，提升写入吞吐 |
| **幂等 INSERT** | 使用 `ON DUPLICATE KEY UPDATE`，避免重复执行导致报错 |
| **原子位点更新** | 写临时文件后 rename，防止位点文件损坏 |
| **多库/表过滤** | 可配置只同步指定数据库、排除特定表 |

---

## 常见问题

**Q: 提示 `找不到 mysqldump`？**  
A: 在 `config.yaml` 的 `full_sync.mysqldump_bin` 中指定完整路径，例如：  
`mysqldump_bin: "C:/Program Files/MySQL/MySQL Server 8.0/bin/mysqldump.exe"`

**Q: 增量同步报 `1236 - Could not find first log file name in binary log index file`？**  
A: 位点记录的 Binlog 文件已被主库清理。解决方案：  
1. 删除 `logs/binlog_position.json`
2. 重新执行全量同步：`python main.py --mode all`

**Q: 源库是 MySQL 5.6/5.7/8.0，目标库版本不同，是否支持？**  
A: 支持。增量同步基于 Binlog ROW 事件，与 MySQL 版本无关；全量同步使用 `mysqldump`，有一定的跨版本兼容性，建议目标库版本 ≥ 源库版本。

**Q: 如何重置增量位点？**  
```bash
python main.py --config config.yaml --mode incremental --reset-position
```

---

## 源库 my.cnf 完整推荐配置

```ini
[mysqld]
server-id               = 1
log_bin                 = /var/lib/mysql/mysql-bin.log
binlog_format           = ROW
binlog_row_image        = FULL
expire_logs_days        = 7      # 保留7天 Binlog
max_binlog_size         = 100M
sync_binlog             = 1      # 提高 Binlog 持久性
innodb_flush_log_at_trx_commit = 1
```
