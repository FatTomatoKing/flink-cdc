package com.base.model;

import java.io.Serializable;
import java.util.Map;

/**
 * CDC事件数据模型
 * 用于封装从MySQL CDC捕获的变更数据
 * 
 * @author zard
 * @since 2025/12/10
 */
public class CDCEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    // 操作类型: INSERT, UPDATE, DELETE
    private String operation;
    
    // 数据库名
    private String database;
    
    // 表名
    private String tableName;
    
    // 变更前的数据 (UPDATE和DELETE时有值)
    private Map<String, Object> before;
    
    // 变更后的数据 (INSERT和UPDATE时有值)
    private Map<String, Object> after;
    
    // 时间戳
    private Long timestamp;

    public CDCEvent() {
    }

    public CDCEvent(String operation, String database, String tableName, 
                    Map<String, Object> before, Map<String, Object> after, Long timestamp) {
        this.operation = operation;
        this.database = database;
        this.tableName = tableName;
        this.before = before;
        this.after = after;
        this.timestamp = timestamp;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "CDCEvent{" +
                "operation='" + operation + '\'' +
                ", database='" + database + '\'' +
                ", tableName='" + tableName + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", timestamp=" + timestamp +
                '}';
    }
}

