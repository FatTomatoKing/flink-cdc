package com.base;

import com.base.config.ConfigLoader;
import com.base.model.CDCEvent;
import com.base.sink.DuckDBSink;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * MySQL CDC 到 DuckDB 数据迁移主程序
 * 使用Flink CDC实时捕获MySQL变更并同步到DuckDB
 * 
 * @author zard
 * @since 2025/12/10
 */
public class MySQLToDuckDBCDC {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLToDuckDBCDC.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 加载配置
        ConfigLoader config = new ConfigLoader("application.properties");
        
        String mysqlHostname = config.get("mysql.hostname", "localhost");
        int mysqlPort = config.getInt("mysql.port", 3306);
        String mysqlUsername = config.get("mysql.username", "root");
        String mysqlPassword = config.get("mysql.password", "");
        String mysqlDatabase = config.get("mysql.database");
        String[] mysqlTables = config.getArray("mysql.tables");
        
        String duckdbPath = config.get("duckdb.path", "./data/target.duckdb");
        String duckdbSchema = config.get("duckdb.schema", "main");
        
        long checkpointInterval = config.getLong("flink.checkpoint.interval", 60000);
        int parallelism = config.getInt("flink.parallelism", 1);

        LOG.info("Starting MySQL to DuckDB CDC migration...");
        LOG.info("MySQL Source: {}:{}/{}", mysqlHostname, mysqlPort, mysqlDatabase);
        LOG.info("DuckDB Target: {}", duckdbPath);
        LOG.info("Tables to sync: {}", String.join(", ", mysqlTables));

        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        
        // 启用检查点
        env.enableCheckpointing(checkpointInterval);

        // 配置MySQL CDC Source
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("decimal.handling.mode", "string");
        debeziumProps.setProperty("bigint.unsigned.handling.mode", "long");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .databaseList(mysqlDatabase)
                .tableList(buildTableList(mysqlDatabase, mysqlTables))
                .username(mysqlUsername)
                .password(mysqlPassword)
                .serverTimeZone("UTC") // 设置服务器时区
                .startupOptions(StartupOptions.initial()) // 从初始快照开始
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(debeziumProps)
                .build();

        // 创建数据流
        DataStream<String> mysqlStream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        // 转换为CDCEvent对象
        DataStream<CDCEvent> cdcEventStream = mysqlStream
                .map(new JsonToCDCEventMapper())
                .filter(event -> event != null);

        // 输出到DuckDB
        cdcEventStream.addSink(new DuckDBSink(duckdbPath, duckdbSchema))
                .name("DuckDB Sink");

        // 打印到控制台（用于调试）
        cdcEventStream.print().name("Console Print");

        // 执行任务
        LOG.info("Starting Flink job...");
        env.execute("MySQL to DuckDB CDC Migration");
    }

    /**
     * 构建表列表字符串
     */
    private static String buildTableList(String database, String[] tables) {
        if (tables == null || tables.length == 0) {
            return database + ".*";
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tables.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(database).append(".").append(tables[i].trim());
        }
        return sb.toString();
    }

    /**
     * JSON到CDCEvent的转换器
     */
    public static class JsonToCDCEventMapper implements MapFunction<String, CDCEvent> {
        @Override
        public CDCEvent map(String json) throws Exception {
            try {
                JsonNode root = objectMapper.readTree(json);
                
                // 提取操作类型
                String operation = root.has("op") ? root.get("op").asText() : "unknown";
                
                // 提取source信息
                JsonNode source = root.get("source");
                String database = source != null && source.has("db") ? 
                        source.get("db").asText() : "";
                String tableName = source != null && source.has("table") ? 
                        source.get("table").asText() : "";
                
                // 提取时间戳
                Long timestamp = source != null && source.has("ts_ms") ? 
                        source.get("ts_ms").asLong() : System.currentTimeMillis();
                
                // 提取before和after数据
                Map<String, Object> before = null;
                Map<String, Object> after = null;
                
                if (root.has("before") && !root.get("before").isNull()) {
                    before = jsonNodeToMap(root.get("before"));
                }
                
                if (root.has("after") && !root.get("after").isNull()) {
                    after = jsonNodeToMap(root.get("after"));
                }
                
                // 映射操作类型
                String mappedOp = mapOperation(operation);
                
                CDCEvent event = new CDCEvent(mappedOp, database, tableName, 
                        before, after, timestamp);
                
                LOG.debug("Parsed CDC event: {}", event);
                return event;
                
            } catch (Exception e) {
                LOG.error("Error parsing JSON: {}", json, e);
                return null;
            }
        }
        
        private Map<String, Object> jsonNodeToMap(JsonNode node) {
            Map<String, Object> map = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = field.getKey();
                JsonNode value = field.getValue();
                
                if (value.isNull()) {
                    map.put(key, null);
                } else if (value.isBoolean()) {
                    map.put(key, value.asBoolean());
                } else if (value.isInt()) {
                    map.put(key, value.asInt());
                } else if (value.isLong()) {
                    map.put(key, value.asLong());
                } else if (value.isDouble()) {
                    map.put(key, value.asDouble());
                } else {
                    map.put(key, value.asText());
                }
            }
            
            return map;
        }
        
        private String mapOperation(String debeziumOp) {
            switch (debeziumOp) {
                case "c": return "INSERT";
                case "r": return "READ";
                case "u": return "UPDATE";
                case "d": return "DELETE";
                default: return debeziumOp.toUpperCase();
            }
        }
    }
}

