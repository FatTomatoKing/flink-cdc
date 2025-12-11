# MySQL to DuckDB CDC Migration

这是一个基于 Apache Flink CDC 的数据迁移工具，能够实时捕获 MySQL 数据库的变更并同步到 DuckDB。

## 功能特性

- ✅ **实时数据同步**: 使用 Flink CDC 实时捕获 MySQL binlog 变更
- ✅ **初始快照**: 支持全量数据初始化
- ✅ **增量同步**: 自动同步 INSERT、UPDATE、DELETE 操作
- ✅ **自动建表**: 根据源表结构自动在 DuckDB 中创建目标表
- ✅ **类型推断**: 智能推断并映射 MySQL 到 DuckDB 的数据类型
- ✅ **容错机制**: 支持 Flink checkpoint，保证数据一致性

## 技术栈

- Apache Flink 1.17.2
- Flink CDC 2.4.2
- MySQL 8.0+
- DuckDB 0.9.2
- Java 8+

## 前置条件

### 1. MySQL 配置

确保 MySQL 已启用 binlog，编辑 `my.cnf` 或 `my.ini`:

```ini
[mysqld]
# 启用 binlog
log-bin=mysql-bin
binlog-format=ROW
binlog-row-image=FULL
server-id=1

# 设置过期时间（可选）
expire_logs_days=7
```

重启 MySQL 服务后验证：

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
```

### 2. MySQL 用户权限

创建专用的 CDC 用户并授予必要权限：

```sql
CREATE USER 'cdcuser'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdcuser'@'%';
FLUSH PRIVILEGES;
```

### 3. Java 环境

确保已安装 JDK 8 或更高版本：

```bash
java -version
```

## 快速开始

### 1. 配置文件

编辑 `src/main/resources/application.properties`:

```properties
# MySQL 源数据库配置
mysql.hostname=localhost
mysql.port=3306
mysql.username=cdcuser
mysql.password=your_password
mysql.database=your_database
mysql.tables=table1,table2,table3

# DuckDB 目标数据库配置
duckdb.path=./data/target.duckdb
duckdb.schema=main

# Flink 配置
flink.checkpoint.interval=60000
flink.parallelism=1
```

**配置说明**:
- `mysql.tables`: 要同步的表名，用逗号分隔。留空则同步所有表
- `duckdb.path`: DuckDB 数据库文件路径，会自动创建
- `flink.checkpoint.interval`: 检查点间隔（毫秒）
- `flink.parallelism`: 并行度

### 2. 编译项目

```bash
mvn clean package
```

编译成功后会在 `target` 目录生成 `flink-cdc-1.0-SNAPSHOT.jar`。

### 3. 运行程序

#### 方式一：使用启动脚本（推荐）

**Windows:**
```cmd
run.bat
```

**Linux/Mac:**
```bash
chmod +x run.sh
./run.sh
```

#### 方式二：IDE 中运行

1. 在 IDE 中打开项目
2. 找到 `com.base.MySQLToDuckDBCDC` 类
3. 右键点击 main 方法，选择 "Run" 或 "Debug"

**注意**: 确保已经执行过 `mvn compile` 来下载所有依赖。

#### 方式三：命令行运行

```bash
# 先编译
mvn clean package

# 运行
java -cp target/flink-cdc-1.0-SNAPSHOT.jar com.base.MySQLToDuckDBCDC
```

#### 方式四：提交到 Flink 集群

如果要部署到生产环境的 Flink 集群，需要修改 `pom.xml` 中 Flink 依赖的 scope 为 `provided`：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>  <!-- 添加这行 -->
</dependency>
```

然后提交任务：

```bash
# 下载并启动 Flink
wget https://archive.apache.org/dist/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz
tar -xzf flink-1.17.2-bin-scala_2.12.tgz
cd flink-1.17.2
./bin/start-cluster.sh

# 提交任务
./bin/flink run -c com.base.MySQLToDuckDBCDC /path/to/flink-cdc-1.0-SNAPSHOT.jar
```

## 项目结构

```
flink-cdc/
├── pom.xml                                 # Maven 配置文件
├── README.md                               # 项目说明文档
└── src/
    └── main/
        ├── java/
        │   └── com/
        │       └── base/
        │           ├── MySQLToDuckDBCDC.java      # 主程序
        │           ├── config/
        │           │   └── ConfigLoader.java      # 配置加载器
        │           ├── model/
        │           │   └── CDCEvent.java          # CDC 事件模型
        │           └── sink/
        │               └── DuckDBSink.java        # DuckDB Sink 实现
        └── resources/
            └── application.properties              # 配置文件
```

## 工作原理

1. **初始化阶段**: 
   - 程序启动时，Flink CDC 会对指定的 MySQL 表进行全量快照
   - 将快照数据写入 DuckDB（自动创建表结构）

2. **增量同步阶段**:
   - 持续监听 MySQL binlog
   - 捕获 INSERT、UPDATE、DELETE 操作
   - 实时同步到 DuckDB

3. **容错机制**:
   - 定期创建 checkpoint
   - 故障恢复时从最近的 checkpoint 继续

## 数据类型映射

| MySQL 类型 | DuckDB 类型 |
|-----------|------------|
| TINYINT, SMALLINT, INT | INTEGER |
| BIGINT | BIGINT |
| FLOAT, DOUBLE | DOUBLE |
| BOOLEAN | BOOLEAN |
| DATE | DATE |
| TIME | TIME |
| DATETIME, TIMESTAMP | TIMESTAMP |
| VARCHAR, TEXT, CHAR | VARCHAR |

## 监控和调试

### 查看日志

程序运行时会输出详细日志，包括：
- CDC 事件捕获信息
- 数据写入状态
- 错误和异常信息

### 验证数据同步

使用 DuckDB CLI 验证数据：

```bash
# 安装 DuckDB CLI
# 下载地址: https://duckdb.org/docs/installation/

# 连接到数据库
duckdb ./data/target.duckdb

# 查看表
SHOW TABLES;

# 查询数据
SELECT * FROM your_table LIMIT 10;
```

## 常见问题

### 1. 连接 MySQL 失败

**问题**: `Communications link failure`

**解决方案**:
- 检查 MySQL 服务是否运行
- 验证主机名、端口、用户名、密码是否正确
- 确保防火墙允许连接

### 2. Binlog 未启用

**问题**: `The MySQL server is not configured to use a ROW binlog_format`

**解决方案**:
- 按照前置条件中的说明配置 MySQL binlog
- 重启 MySQL 服务

### 3. 权限不足

**问题**: `Access denied for user`

**解决方案**:
- 确保用户拥有 REPLICATION SLAVE 和 REPLICATION CLIENT 权限
- 执行 `FLUSH PRIVILEGES;`

### 4. 内存不足

**问题**: `OutOfMemoryError`

**解决方案**:
- 增加 JVM 堆内存: `java -Xmx2g -jar ...`
- 降低并行度配置

## 性能优化

1. **调整 checkpoint 间隔**: 根据数据量和延迟要求调整
2. **增加并行度**: 对于大表可以提高并行度
3. **批量写入**: DuckDB Sink 可以改造为批量写入模式
4. **过滤不需要的表**: 只同步必要的表以减少资源消耗

## 扩展开发

### 自定义数据转换

修改 `JsonToCDCEventMapper` 类，添加自定义的数据转换逻辑。

### 支持更多目标数据库

参考 `DuckDBSink` 实现，创建其他数据库的 Sink 类。

### 添加数据过滤

在数据流中添加 filter 操作，过滤不需要的数据。

## 许可证

本项目采用 MIT 许可证。

## 作者

- zard
- 创建日期: 2025/12/10

## 贡献

欢迎提交 Issue 和 Pull Request！

## 参考资料

- [Apache Flink CDC](https://github.com/ververica/flink-cdc-connectors)
- [Apache Flink Documentation](https://flink.apache.org/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [MySQL Binlog](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)

