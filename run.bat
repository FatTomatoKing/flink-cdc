@echo off
echo ========================================
echo MySQL to DuckDB CDC Migration
echo ========================================
echo.

REM 检查是否已编译
if not exist "target\flink-cdc-1.0-SNAPSHOT.jar" (
    echo [INFO] 项目未编译，开始编译...
    call mvn clean package -DskipTests
    if errorlevel 1 (
        echo [ERROR] 编译失败！
        pause
        exit /b 1
    )
    echo [INFO] 编译完成！
    echo.
)

echo [INFO] 启动 Flink CDC 任务...
echo [INFO] 配置文件: src\main\resources\application.properties
echo.

java -cp "target\flink-cdc-1.0-SNAPSHOT.jar" com.base.MySQLToDuckDBCDC

if errorlevel 1 (
    echo.
    echo [ERROR] 程序运行失败！
    pause
    exit /b 1
)

pause

