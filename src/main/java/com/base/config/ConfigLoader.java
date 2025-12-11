package com.base.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置文件加载器
 * 
 * @author zard
 * @since 2025/12/10
 */
public class ConfigLoader {
    private Properties properties;

    public ConfigLoader(String configFile) throws IOException {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (input == null) {
                throw new IOException("Unable to find " + configFile);
            }
            properties.load(input);
        }
    }

    public String get(String key) {
        return properties.getProperty(key);
    }

    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    public long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Long.parseLong(value) : defaultValue;
    }

    public String[] getArray(String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return new String[0];
        }
        return value.split(",");
    }
}

