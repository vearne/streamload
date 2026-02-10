"replication_num" = "2"
表示有1个冗余备份

## 方式1：使用主键的明细表

```
-- 创建主键明细表
CREATE TABLE IF NOT EXISTS user_details (
    id INT COMMENT '用户ID',
    name VARCHAR(50) COMMENT '用户姓名',
    age INT COMMENT '用户年龄'
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "2"
);
```
## 方式2：普通明细表（无主键）
```
-- 创建普通明细表
CREATE TABLE users (
    create_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',  -- 分区字段放在第一列
    id INT COMMENT '用户ID',
    name VARCHAR(50) COMMENT '用户姓名',
    age INT COMMENT '用户年龄'
)
DUPLICATE KEY(create_date, id)  -- 分区列必须在排序键的第一位
PARTITION BY RANGE(create_date)  -- 按日期分区
(
    PARTITION p2025 VALUES LESS THAN ("2025-01-01"),
    PARTITION p2026 VALUES LESS THAN ("2026-01-01"),
    PARTITION p2027 VALUES LESS THAN ("2027-01-01")
)
DISTRIBUTED BY HASH(create_date, id) BUCKETS 4
PROPERTIES (
    "replication_num" = "2"
);
```