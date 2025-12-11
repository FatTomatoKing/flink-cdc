package com.base.sink;

import com.base.model.CDCEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DuckDB Sink实现
 * 将CDC事件写入DuckDB数据库
 * 
 * @author zard
 * @since 2025/12/10
 */
public class DuckDBSink extends RichSinkFunction<CDCEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(DuckDBSink.class);
    
    private final String duckdbPath;
    private final String schema;
    private transient Connection connection;

    public DuckDBSink(String duckdbPath, String schema) {
        this.duckdbPath = duckdbPath;
        this.schema = schema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载DuckDB JDBC驱动
        Class.forName("org.duckdb.DuckDBDriver");
        
        // 建立连接
        connection = DriverManager.getConnection("jdbc:duckdb:" + duckdbPath);
        connection.setAutoCommit(true);
        
        LOG.info("DuckDB connection established: {}", duckdbPath);
    }

    @Override
    public void invoke(CDCEvent event, Context context) throws Exception {
        String tableName = event.getTableName();
        String operation = event.getOperation();
        
        LOG.info("Processing CDC event: operation={}, table={}", operation, tableName);

        try {
            switch (operation.toUpperCase()) {
                case "INSERT":
                case "READ": // 初始快照读取
                    handleInsert(tableName, event.getAfter());
                    break;
                case "UPDATE":
                    handleUpdate(tableName, event.getAfter(), event.getBefore());
                    break;
                case "DELETE":
                    handleDelete(tableName, event.getBefore());
                    break;
                default:
                    LOG.warn("Unknown operation type: {}", operation);
            }
        } catch (SQLException e) {
            LOG.error("Error processing CDC event: {}", event, e);
            // 如果表不存在，尝试创建表
            if (e.getMessage().contains("does not exist")) {
                createTableIfNotExists(tableName, event.getAfter());
                // 重试操作
                invoke(event, context);
            } else {
                throw e;
            }
        }
    }

    private void handleInsert(String tableName, Map<String, Object> data) throws SQLException {
        if (data == null || data.isEmpty()) {
            return;
        }

        // 确保表存在
        createTableIfNotExists(tableName, data);

        String columns = String.join(", ", data.keySet());
        String placeholders = data.keySet().stream()
                .map(k -> "?")
                .collect(Collectors.joining(", "));

        String sql = String.format("INSERT INTO %s.%s (%s) VALUES (%s)", 
                schema, tableName, columns, placeholders);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            int index = 1;
            for (Object value : data.values()) {
                stmt.setObject(index++, value);
            }
            stmt.executeUpdate();
            LOG.debug("Inserted record into {}", tableName);
        }
    }

    private void handleUpdate(String tableName, Map<String, Object> newData, 
                            Map<String, Object> oldData) throws SQLException {
        if (newData == null || newData.isEmpty()) {
            return;
        }

        // 构建SET子句
        String setClause = newData.entrySet().stream()
                .map(e -> e.getKey() + " = ?")
                .collect(Collectors.joining(", "));

        // 构建WHERE子句 (使用旧数据作为条件)
        String whereClause = oldData.entrySet().stream()
                .map(e -> e.getKey() + " = ?")
                .collect(Collectors.joining(" AND "));

        String sql = String.format("UPDATE %s.%s SET %s WHERE %s", 
                schema, tableName, setClause, whereClause);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            int index = 1;
            // 设置SET子句的参数
            for (Object value : newData.values()) {
                stmt.setObject(index++, value);
            }
            // 设置WHERE子句的参数
            for (Object value : oldData.values()) {
                stmt.setObject(index++, value);
            }
            int updated = stmt.executeUpdate();
            LOG.debug("Updated {} record(s) in {}", updated, tableName);
        }
    }

    private void handleDelete(String tableName, Map<String, Object> data) throws SQLException {
        if (data == null || data.isEmpty()) {
            return;
        }

        String whereClause = data.entrySet().stream()
                .map(e -> e.getKey() + " = ?")
                .collect(Collectors.joining(" AND "));

        String sql = String.format("DELETE FROM %s.%s WHERE %s", 
                schema, tableName, whereClause);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            int index = 1;
            for (Object value : data.values()) {
                stmt.setObject(index++, value);
            }
            int deleted = stmt.executeUpdate();
            LOG.debug("Deleted {} record(s) from {}", deleted, tableName);
        }
    }

    private void createTableIfNotExists(String tableName, Map<String, Object> sampleData) 
            throws SQLException {
        // 检查表是否存在
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(null, schema, tableName, null)) {
            if (rs.next()) {
                return; // 表已存在
            }
        }

        // 根据样本数据推断列类型并创建表
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE ").append(schema).append(".").append(tableName).append(" (");

        boolean first = true;
        for (Map.Entry<String, Object> entry : sampleData.entrySet()) {
            if (!first) {
                createTableSql.append(", ");
            }
            createTableSql.append(entry.getKey()).append(" ");
            createTableSql.append(inferDuckDBType(entry.getValue()));
            first = false;
        }
        createTableSql.append(")");

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql.toString());
            LOG.info("Created table: {}.{}", schema, tableName);
        }
    }

    private String inferDuckDBType(Object value) {
        if (value == null) {
            return "VARCHAR";
        }
        
        if (value instanceof Integer) {
            return "INTEGER";
        } else if (value instanceof Long) {
            return "BIGINT";
        } else if (value instanceof Double || value instanceof Float) {
            return "DOUBLE";
        } else if (value instanceof Boolean) {
            return "BOOLEAN";
        } else if (value instanceof java.sql.Date) {
            return "DATE";
        } else if (value instanceof java.sql.Time) {
            return "TIME";
        } else if (value instanceof java.sql.Timestamp) {
            return "TIMESTAMP";
        } else {
            return "VARCHAR";
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("DuckDB connection closed");
        }
    }
}

