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
    "replication_num" = "1"
);
```
## 方式2：普通明细表（无主键）
```
-- 创建普通明细表
CREATE TABLE IF NOT EXISTS user_details (
    id INT COMMENT '用户ID',
    name VARCHAR(50) COMMENT '用户姓名',
    age INT COMMENT '用户年龄'
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
```