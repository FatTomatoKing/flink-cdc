#!/bin/bash

echo "========================================"
echo "MySQL to DuckDB CDC Migration"
echo "========================================"
echo ""

# 检查是否已编译
if [ ! -f "target/flink-cdc-1.0-SNAPSHOT.jar" ]; then
    echo "[INFO] 项目未编译，开始编译..."
    mvn clean package -DskipTests
    if [ $? -ne 0 ]; then
        echo "[ERROR] 编译失败！"
        exit 1
    fi
    echo "[INFO] 编译完成！"
    echo ""
fi

echo "[INFO] 启动 Flink CDC 任务..."
echo "[INFO] 配置文件: src/main/resources/application.properties"
echo ""

java -cp "target/flink-cdc-1.0-SNAPSHOT.jar" com.base.MySQLToDuckDBCDC

if [ $? -ne 0 ]; then
    echo ""
    echo "[ERROR] 程序运行失败！"
    exit 1
fi

